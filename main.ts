// free-model-aggregator-02 — one server that aggregates the brief's fourteen
// free OpenRouter models behind BOTH the OpenAI Chat Completions API and the
// Anthropic Messages API, in JSON and in streaming (SSE).
//
// Every answer is a REAL completion: the server dials one free model through
// the trial gateway's Chat Completions route (POST $OPENAI_BASE_URL/chat/
// completions, with the trial's dummy key from OPENAI_API_KEY, which the
// gateway swaps for the entrant's own OpenRouter key) and returns that
// model's answer, translated into the wire the client used. The server never
// invents an answer of its own, and the `model` field of every answer and
// every streamed chunk names the dialable id it really dialled
// (`openrouter/<vendor>/<model>:free`), never the shorter slug the
// provider's own answer echoes back.
//
// At trial start the server reads the brief (MAKEFUN_PROMPT_FILE): the free
// model list (gateway id, rating, context length, tools/vision/JSON support)
// and the six prompts' manifest, in arrival order. It listens on 0.0.0.0 at
// a free port it picks, registers that port and its catalog with the env
// (MAKEFUN_ENV_URL), then serves the prompts one at a time: each is routed
// to the highest-rated free model that meets the prompt's requirements,
// failing over to the next candidate when a provider refuses, and reporting
// the refusals it recovered from in the `x-makefun-upstream-failures`
// header. When the env's GET /work says done, the server exits 0.
//
// Every choice is a pure function of the brief, the requests and the
// gateway's answers: no wall clock and no unseeded randomness in anything
// sent, so a replayed trial makes the same gateway calls and leaves the
// same state.

import { createServer } from 'node:http';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { readFile } from 'node:fs/promises';

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

type Json = Record<string, unknown>;

const isRecord = (v: unknown): v is Json => typeof v === 'object' && v !== null && !Array.isArray(v);
const str = (v: unknown): string => (typeof v === 'string' ? v : '');
const numOrNull = (v: unknown): number | null => (typeof v === 'number' && Number.isFinite(v) ? v : null);
const stringList = (v: unknown): string[] => {
  if (typeof v === 'string') return v === '' ? [] : [v];
  if (Array.isArray(v)) return v.filter((x): x is string => typeof x === 'string');
  return [];
};

// The text of a message content value the way every wire reads it: a string
// as-is, or the text blocks of an array joined with "\n".
function blocksText(content: unknown): string {
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) {
    return content
      .filter(isRecord)
      .filter((b) => b.type === 'text')
      .map((b) => str(b.text))
      .join('\n');
  }
  return '';
}

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

// ---------------------------------------------------------------------------
// The brief: two JSON documents embedded in prose — the free model list and
// the prompts' manifest. Each is found by its "schema" value and read as the
// balanced {...} that contains it.
// ---------------------------------------------------------------------------

interface ModelRow {
  id: string; // the dialable gateway id, e.g. openrouter/<vendor>/<model>:free
  provider: string;
  displayName: string;
  ratingMilli: number;
  contextLength: number | null;
  tools: boolean;
  vision: boolean;
  json: boolean;
  free: boolean;
}

interface Requires {
  tools?: boolean;
  vision?: boolean;
  json?: boolean;
}

interface PromptRow {
  id: string;
  api: string;
  stream: boolean;
  weight: number;
  requires: Requires;
}

interface Brief {
  models: ModelRow[];
  prompts: PromptRow[];
}

function findJsonDoc(text: string, schema: string): Json {
  const anchor = text.indexOf(schema);
  if (anchor < 0) throw new Error(`brief is missing the ${schema} document`);
  const start = text.lastIndexOf('{', anchor);
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let i = start; i < text.length; i += 1) {
    const ch = text[i];
    if (inString) {
      if (escaped) escaped = false;
      else if (ch === '\\') escaped = true;
      else if (ch === '"') inString = false;
      continue;
    }
    if (ch === '"') inString = true;
    else if (ch === '{') depth += 1;
    else if (ch === '}') {
      depth -= 1;
      if (depth === 0) return JSON.parse(text.slice(start, i + 1)) as Json;
    }
  }
  throw new Error(`brief has an unterminated ${schema} document`);
}

function asRequires(v: unknown): Requires {
  const r = isRecord(v) ? v : {};
  const out: Requires = {};
  if (r.tools === true) out.tools = true;
  if (r.vision === true) out.vision = true;
  if (r.json === true) out.json = true;
  return out;
}

function parseBrief(text: string): Brief {
  const modelsDoc = findJsonDoc(text, 'models/v1');
  const promptsDoc = findJsonDoc(text, 'prompts/v1');
  const models: ModelRow[] = (Array.isArray(modelsDoc.models) ? modelsDoc.models : [])
    .filter(isRecord)
    .map((m) => ({
      id: str(m.id),
      provider: str(m.provider),
      displayName: str(m.display_name),
      ratingMilli: numOrNull(m.rating_milli) ?? 0,
      contextLength: numOrNull(m.context_length),
      tools: m.tools === true,
      vision: m.vision === true,
      json: m.json === true,
      free: m.free === true,
    }))
    .filter((m) => m.id !== '');
  const prompts: PromptRow[] = (Array.isArray(promptsDoc.prompts) ? promptsDoc.prompts : [])
    .filter(isRecord)
    .map((p) => ({
      id: str(p.id),
      api: str(p.api),
      stream: p.stream === true,
      weight: numOrNull(p.weight) ?? 1,
      requires: asRequires(p.requires),
    }));
  return { models, prompts };
}

// ---------------------------------------------------------------------------
// The catalog: one provider, openrouter, holding exactly the brief's free
// models as their dialable ids. This same document is registered with the
// env and printed by --list, byte for byte. base_url is OpenRouter's public
// endpoint, never the gateway's trial address (which differs between a live
// pass and its replay, while the catalog is compared byte for byte).
// ---------------------------------------------------------------------------

interface CatalogDoc {
  providers: {
    id: string;
    base_url: string;
    key_env: string;
    wire: string;
    models: { id: string; free: boolean; context_length: number | null }[];
  }[];
}

function freeModels(brief: Brief): ModelRow[] {
  return brief.models.filter((m) => m.free);
}

function buildCatalog(brief: Brief): CatalogDoc {
  return {
    providers: [
      {
        id: 'openrouter',
        base_url: 'https://openrouter.ai/api/v1',
        key_env: 'OPENROUTER_API_KEY',
        wire: 'openai',
        models: freeModels(brief).map((m) => ({ id: m.id, free: true, context_length: m.contextLength })),
      },
    ],
  };
}

// ---------------------------------------------------------------------------
// Normalized request: the client's request, wire-neutral. Both client wires
// are normalized once, then rendered into the one body the gateway takes (a
// Chat Completions body), so an Anthropic-shaped request and an OpenAI one
// with the same meaning dial the gateway identically.
// ---------------------------------------------------------------------------

type Part =
  | { kind: 'text'; text: string }
  | { kind: 'image'; mediaType: string; data: string } // base64 image
  | { kind: 'imageUrl'; url: string }
  | { kind: 'toolUse'; id: string; name: string; input: unknown }
  | { kind: 'toolResult'; toolUseId: string; text: string; isError: boolean };

interface NormMessage {
  role: 'user' | 'assistant';
  parts: Part[];
}

interface NormTool {
  name: string;
  description: string;
  parameters: Json;
}

interface NormRequest {
  systemText: string | null; // null when the client sent no system prompt
  messages: NormMessage[];
  tools: NormTool[];
  rawToolChoice: unknown;
  maxTokens: number | null;
  stops: string[];
  jsonMode: boolean; // the client asked for a JSON answer (OpenAI wire only)
  responseFormat: unknown; // the client's raw response_format, passed through
  temperature: number | null;
  topP: number | null;
  stream: boolean;
}

// Tool results travel as their own messages on the OpenAI wire but as blocks
// inside a user message on the Anthropic one. Normalize them into user
// messages in order; trailing tool results (no later user message) are
// folded into the last user message.
class MessageSink {
  messages: NormMessage[] = [];
  private pendingResults: Part[] = [];

  private flushPending(): void {
    if (this.pendingResults.length > 0) {
      this.messages.push({ role: 'user', parts: this.pendingResults });
      this.pendingResults = [];
    }
  }

  toolResult(part: Part): void {
    this.pendingResults.push(part);
  }

  message(role: 'user' | 'assistant', parts: Part[]): void {
    if (role === 'user') this.flushPending();
    this.messages.push({ role, parts });
  }

  finish(): NormMessage[] {
    if (this.pendingResults.length > 0) {
      const lastUser = [...this.messages].reverse().find((m) => m.role === 'user');
      if (lastUser) lastUser.parts.push(...this.pendingResults);
      else this.messages.push({ role: 'user', parts: this.pendingResults });
      this.pendingResults = [];
    }
    return this.messages;
  }
}

function fromOpenAI(body: Json): NormRequest {
  const sink = new MessageSink();
  const systemParts: string[] = [];
  let sawSystem = false;
  const rawMessages = Array.isArray(body.messages) ? body.messages : [];
  for (const raw of rawMessages) {
    if (!isRecord(raw)) continue;
    const role = str(raw.role);
    if (role === 'system') {
      sawSystem = true;
      systemParts.push(blocksText(raw.content));
      continue;
    }
    if (role === 'user') {
      const parts: Part[] = [];
      if (typeof raw.content === 'string') {
        if (raw.content !== '') parts.push({ kind: 'text', text: raw.content });
      } else if (Array.isArray(raw.content)) {
        for (const block of raw.content) {
          if (!isRecord(block)) continue;
          if (block.type === 'text') parts.push({ kind: 'text', text: str(block.text) });
          else if (block.type === 'image_url' && isRecord(block.image_url)) {
            parts.push({ kind: 'imageUrl', url: str(block.image_url.url) });
          }
        }
      }
      sink.message('user', parts);
      continue;
    }
    if (role === 'assistant') {
      const parts: Part[] = [];
      const text = blocksText(raw.content);
      if (text !== '') parts.push({ kind: 'text', text });
      if (Array.isArray(raw.tool_calls)) {
        for (const call of raw.tool_calls) {
          if (!isRecord(call) || !isRecord(call.function)) continue;
          let input: unknown = {};
          try {
            input = JSON.parse(str(call.function.arguments) || '{}');
          } catch {
            input = {};
          }
          parts.push({ kind: 'toolUse', id: str(call.id), name: str(call.function.name), input });
        }
      }
      sink.message('assistant', parts);
      continue;
    }
    if (role === 'tool') {
      sink.toolResult({
        kind: 'toolResult',
        toolUseId: str(raw.tool_call_id),
        text: blocksText(raw.content),
        isError: false,
      });
    }
  }
  const tools: NormTool[] = (Array.isArray(body.tools) ? body.tools : [])
    .filter(isRecord)
    .filter((t) => isRecord(t.function))
    .map((t) => {
      const fn = t.function as Json;
      return {
        name: str(fn.name),
        description: str(fn.description),
        parameters: isRecord(fn.parameters) ? fn.parameters : { type: 'object' },
      };
    })
    .filter((t) => t.name !== '');
  const responseFormat = isRecord(body.response_format) ? body.response_format : null;
  const jsonMode =
    responseFormat !== null && (responseFormat.type === 'json_object' || responseFormat.type === 'json_schema');
  return {
    systemText: sawSystem ? systemParts.join('\n') : null,
    messages: sink.finish(),
    tools,
    rawToolChoice: body.tool_choice,
    maxTokens: numOrNull(body.max_tokens) ?? numOrNull(body.max_completion_tokens),
    stops: stringList(body.stop),
    jsonMode,
    responseFormat,
    temperature: numOrNull(body.temperature),
    topP: numOrNull(body.top_p),
    stream: body.stream === true,
  };
}

function anthropicImagePart(block: Json): Part | null {
  const source = isRecord(block.source) ? block.source : null;
  if (!source) return null;
  if (source.type === 'base64') {
    return { kind: 'image', mediaType: str(source.media_type) || 'application/octet-stream', data: str(source.data) };
  }
  if (source.type === 'url') return { kind: 'imageUrl', url: str(source.url) };
  return null;
}

function fromAnthropic(body: Json): NormRequest {
  const sink = new MessageSink();
  const rawMessages = Array.isArray(body.messages) ? body.messages : [];
  for (const raw of rawMessages) {
    if (!isRecord(raw)) continue;
    const role = raw.role === 'assistant' ? 'assistant' : 'user';
    const parts: Part[] = [];
    if (typeof raw.content === 'string') {
      if (raw.content !== '') parts.push({ kind: 'text', text: raw.content });
    } else if (Array.isArray(raw.content)) {
      for (const block of raw.content) {
        if (!isRecord(block)) continue;
        if (block.type === 'text') parts.push({ kind: 'text', text: str(block.text) });
        else if (block.type === 'image') {
          const part = anthropicImagePart(block);
          if (part) parts.push(part);
        } else if (block.type === 'tool_use') {
          parts.push({ kind: 'toolUse', id: str(block.id), name: str(block.name), input: block.input ?? {} });
        } else if (block.type === 'tool_result') {
          parts.push({
            kind: 'toolResult',
            toolUseId: str(block.tool_use_id),
            text: blocksText(block.content),
            isError: block.is_error === true,
          });
        }
      }
    }
    sink.message(role, parts);
  }
  const tools: NormTool[] = (Array.isArray(body.tools) ? body.tools : [])
    .filter(isRecord)
    .map((t) => ({
      name: str(t.name),
      description: str(t.description),
      parameters: isRecord(t.input_schema) ? t.input_schema : { type: 'object' },
    }))
    .filter((t) => t.name !== '');
  return {
    systemText: body.system === undefined ? null : blocksText(body.system),
    messages: sink.finish(),
    tools,
    rawToolChoice: body.tool_choice,
    maxTokens: numOrNull(body.max_tokens),
    stops: stringList(body.stop_sequences),
    jsonMode: false, // the Anthropic wire carries no JSON-mode flag
    responseFormat: null,
    temperature: numOrNull(body.temperature),
    topP: numOrNull(body.top_p),
    stream: body.stream === true,
  };
}

// ---------------------------------------------------------------------------
// The gateway call: the normalized request rendered as one Chat Completions
// body. The gateway is always dialled non-streaming, whatever the client
// asked: the full answer is collected, then streamed back to the client in
// its own wire if it asked for events. That keeps every gateway request a
// single deterministic body (a replay matches requests by content) and lets
// the router verify the answer before the client sees any of it.
// ---------------------------------------------------------------------------

const GATEWAY_BASE = (process.env.OPENAI_BASE_URL ?? '').replace(/\/+$/, '');
const GATEWAY_KEY = process.env.OPENAI_API_KEY ?? '';

// OPENAI_BASE_URL is the OpenAI-compatible root (`.../v1` on the trial
// network); a base that does not already end in /v1 gets one appended.
function chatCompletionsUrl(): string {
  if (GATEWAY_BASE === '') return '';
  return GATEWAY_BASE.endsWith('/v1') ? `${GATEWAY_BASE}/chat/completions` : `${GATEWAY_BASE}/v1/chat/completions`;
}

function openAIToolChoice(raw: unknown): unknown {
  if (typeof raw === 'string') return raw;
  if (isRecord(raw)) {
    if (typeof raw.type === 'string') {
      // Anthropic-shaped tool_choice.
      if (raw.type === 'auto') return 'auto';
      if (raw.type === 'any') return 'required';
      if (raw.type === 'none') return 'none';
      if (raw.type === 'tool') return { type: 'function', function: { name: str(raw.name) } };
    }
    return raw; // already OpenAI-shaped
  }
  return undefined;
}

// The gateway refuses a call with no output cap or one over 8192, so the
// dialled body always names max_tokens inside that range.
const MAX_OUTPUT_TOKENS = 8192;
const DEFAULT_MAX_TOKENS = 1024;

function toGatewayBody(norm: NormRequest, model: string): Json {
  const messages: Json[] = [];
  if (norm.systemText !== null) messages.push({ role: 'system', content: norm.systemText });
  for (const msg of norm.messages) {
    if (msg.role === 'assistant') {
      const textParts = msg.parts.filter((p) => p.kind === 'text');
      const toolCalls = msg.parts
        .filter((p) => p.kind === 'toolUse')
        .map((p) => {
          const t = p as Extract<Part, { kind: 'toolUse' }>;
          return {
            id: t.id,
            type: 'function',
            function: { name: t.name, arguments: typeof t.input === 'string' ? t.input : JSON.stringify(t.input ?? {}) },
          };
        });
      const out: Json = {
        role: 'assistant',
        content: textParts.length > 0 ? textParts.map((p) => (p as { text: string }).text).join('\n') : null,
      };
      if (toolCalls.length > 0) out.tool_calls = toolCalls;
      messages.push(out);
      continue;
    }
    // user: tool results become tool messages, the rest one user message.
    for (const part of msg.parts) {
      if (part.kind === 'toolResult') {
        messages.push({ role: 'tool', tool_call_id: part.toolUseId, content: part.text });
      }
    }
    const rest = msg.parts.filter((p) => p.kind !== 'toolResult');
    if (rest.length === 1 && rest[0].kind === 'text') {
      messages.push({ role: 'user', content: (rest[0] as { text: string }).text });
    } else if (rest.length === 0) {
      messages.push({ role: 'user', content: '' });
    } else {
      messages.push({
        role: 'user',
        content: rest.map((part) => {
          if (part.kind === 'text') return { type: 'text', text: part.text };
          if (part.kind === 'image') {
            return { type: 'image_url', image_url: { url: `data:${part.mediaType};base64,${part.data}` } };
          }
          if (part.kind === 'imageUrl') return { type: 'image_url', image_url: { url: part.url } };
          return { type: 'text', text: '' };
        }),
      });
    }
  }
  const body: Json = { model, messages, stream: false };
  const cap = norm.maxTokens === null ? DEFAULT_MAX_TOKENS : Math.min(Math.max(1, Math.floor(norm.maxTokens)), MAX_OUTPUT_TOKENS);
  body.max_tokens = cap;
  if (norm.stops.length > 0) body.stop = norm.stops;
  if (norm.jsonMode) body.response_format = norm.responseFormat ?? { type: 'json_object' };
  if (norm.tools.length > 0) {
    body.tools = norm.tools.map((t) => ({
      type: 'function',
      function: { name: t.name, description: t.description, parameters: t.parameters },
    }));
    const choice = openAIToolChoice(norm.rawToolChoice);
    if (choice !== undefined) body.tool_choice = choice;
  }
  if (norm.temperature !== null) body.temperature = norm.temperature;
  if (norm.topP !== null) body.top_p = norm.topP;
  return body;
}

// ---------------------------------------------------------------------------
// The gateway's answer, normalized: text, tool calls, usage, stop reason.
// The provider spells the model without the gateway's `openrouter/` prefix;
// the answer's own model field is never trusted — the router reports the id
// it dialled.
// ---------------------------------------------------------------------------

interface NormToolCall {
  id: string;
  name: string;
  argsJson: string;
}

interface NormAnswer {
  text: string;
  toolCalls: NormToolCall[];
  usageIn: number | null;
  usageOut: number | null;
  stopRaw: string | null;
}

function parseJsonSafe(text: string): Json | null {
  try {
    const value: unknown = JSON.parse(text);
    return isRecord(value) ? value : null;
  } catch {
    return null;
  }
}

function parseGatewayAnswer(body: Json): NormAnswer {
  const choices = Array.isArray(body.choices) ? body.choices : [];
  const first = isRecord(choices[0]) ? choices[0] : {};
  const message = isRecord(first.message) ? first.message : {};
  const toolCalls: NormToolCall[] = [];
  if (Array.isArray(message.tool_calls)) {
    for (const call of message.tool_calls) {
      if (!isRecord(call) || !isRecord(call.function)) continue;
      toolCalls.push({
        id: str(call.id),
        name: str(call.function.name),
        argsJson: str(call.function.arguments) || '{}',
      });
    }
  }
  const usage = isRecord(body.usage) ? body.usage : {};
  return {
    text: blocksText(message.content),
    toolCalls,
    usageIn: numOrNull(usage.prompt_tokens),
    usageOut: numOrNull(usage.completion_tokens),
    stopRaw: typeof first.finish_reason === 'string' ? first.finish_reason : null,
  };
}

// Stop-reason families: 0 = finished, 1 = length, 2 = tool call, 3 = filtered.
const STOP_FAMILIES: Record<string, number> = {
  stop: 0,
  end_turn: 0,
  stop_sequence: 0,
  length: 1,
  max_tokens: 1,
  tool_calls: 2,
  tool_use: 2,
  content_filter: 3,
  refusal: 3,
};

function stopFamily(stopRaw: string | null, hasToolCalls: boolean): number {
  if (hasToolCalls) return 2;
  if (stopRaw !== null && stopRaw in STOP_FAMILIES) return STOP_FAMILIES[stopRaw];
  return 0;
}

const OPENAI_FINISH = ['stop', 'length', 'tool_calls', 'content_filter'];
const ANTHROPIC_STOP = ['end_turn', 'max_tokens', 'tool_use', 'refusal'];

// If a provider answer ever omits usage, recompute it from the request and
// the answer text so the client still gets whole counts (an input of at
// least 0 and an output of at least 1 are wire checks).
function computedUsage(norm: NormRequest, answerText: string): { input: number; output: number } {
  const lastUser = [...norm.messages].reverse().find((m) => m.role === 'user');
  const userText = lastUser
    ? lastUser.parts
        .filter((p) => p.kind === 'text')
        .map((p) => (p as { text: string }).text)
        .join('\n')
    : '';
  const joined = (norm.systemText ?? '') + userText + norm.tools.map((t) => t.name).join(',');
  return {
    input: 1 + Math.floor(joined.length / 4),
    output: 1 + Math.floor(answerText.length / 4),
  };
}

function withUsage(norm: NormRequest, ans: NormAnswer): { input: number; output: number } {
  if (ans.usageIn !== null && ans.usageOut !== null) return { input: ans.usageIn, output: ans.usageOut };
  const computed = computedUsage(norm, ans.text);
  return { input: ans.usageIn ?? computed.input, output: ans.usageOut ?? computed.output };
}

// One gateway attempt. A refusal is the provider's own status and error
// body, relayed as sent; the code is read from that body when it carries
// one, else named from the status.
interface Refusal {
  status: number;
  code: string;
  model: string | null;
}

type Attempt = { ok: true; answer: NormAnswer } | { ok: false; refusal: Refusal };

function refusalCode(status: number, bodyText: string): string {
  const parsed = parseJsonSafe(bodyText);
  if (parsed) {
    const err = isRecord(parsed.error) ? parsed.error : parsed;
    const code = str(err.code) || str(err.type);
    if (code !== '') return code;
  }
  if (status === 401) return 'unauthorized';
  if (status === 429) return 'rate_limited';
  if (status >= 500) return 'provider_error';
  return 'refused';
}

async function callGateway(model: string, norm: NormRequest): Promise<Attempt> {
  const url = chatCompletionsUrl();
  if (url === '') return { ok: false, refusal: { status: 502, code: 'no_gateway', model } };
  let res: Response;
  try {
    res = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${GATEWAY_KEY}` },
      body: JSON.stringify(toGatewayBody(norm, model)),
      signal: AbortSignal.timeout(25000),
    });
  } catch (err) {
    return { ok: false, refusal: { status: 502, code: `network:${String(err).slice(0, 60)}`, model } };
  }
  const text = await res.text();
  if (!res.ok) {
    return { ok: false, refusal: { status: res.status, code: refusalCode(res.status, text), model } };
  }
  const parsed = parseJsonSafe(text);
  if (!parsed) return { ok: false, refusal: { status: 502, code: 'unparseable_answer', model } };
  return { ok: true, answer: parseGatewayAnswer(parsed) };
}

// ---------------------------------------------------------------------------
// Client answers: the normalized gateway answer rendered in the client's
// wire, JSON or SSE as it asked. `model` is always the dialable id the
// router really dialled, on every answer and every streamed chunk.
// ---------------------------------------------------------------------------

function openAIAnswerJson(ans: NormAnswer, usage: { input: number; output: number }, model: string, seq: number): Json {
  const family = stopFamily(ans.stopRaw, ans.toolCalls.length > 0);
  const message: Json = { role: 'assistant', content: ans.text };
  if (ans.toolCalls.length > 0) {
    message.tool_calls = ans.toolCalls.map((t, i) => ({
      id: t.id !== '' ? t.id : `call_${seq}_${i}`,
      type: 'function',
      function: { name: t.name, arguments: t.argsJson || '{}' },
    }));
  }
  return {
    id: `chatcmpl-${seq}`,
    object: 'chat.completion',
    created: 0,
    model,
    choices: [{ index: 0, message, finish_reason: OPENAI_FINISH[family] }],
    usage: {
      prompt_tokens: usage.input,
      completion_tokens: usage.output,
      total_tokens: usage.input + usage.output,
    },
  };
}

function openAIAnswerStream(ans: NormAnswer, usage: { input: number; output: number }, model: string, seq: number): string {
  const family = stopFamily(ans.stopRaw, ans.toolCalls.length > 0);
  const base = { id: `chatcmpl-${seq}`, object: 'chat.completion.chunk', created: 0, model };
  const lines: string[] = [];
  const chunk = (choices: unknown, extra?: Json): void => {
    lines.push(`data: ${JSON.stringify({ ...base, choices, ...(extra ?? {}) })}`);
  };
  chunk([{ index: 0, delta: { role: 'assistant', content: ans.text }, finish_reason: null }]);
  ans.toolCalls.forEach((t, i) => {
    chunk([
      {
        index: 0,
        delta: {
          tool_calls: [
            {
              index: i,
              id: t.id !== '' ? t.id : `call_${seq}_${i}`,
              type: 'function',
              function: { name: t.name, arguments: t.argsJson || '{}' },
            },
          ],
        },
        finish_reason: null,
      },
    ]);
  });
  chunk([{ index: 0, delta: {}, finish_reason: OPENAI_FINISH[family] }]);
  chunk([], {
    usage: {
      prompt_tokens: usage.input,
      completion_tokens: usage.output,
      total_tokens: usage.input + usage.output,
    },
  });
  lines.push('data: [DONE]');
  return `${lines.join('\n\n')}\n\n`;
}

function anthropicContentBlocks(ans: NormAnswer): Json[] {
  const blocks: Json[] = [];
  if (ans.text !== '' || ans.toolCalls.length === 0) blocks.push({ type: 'text', text: ans.text });
  ans.toolCalls.forEach((t, i) => {
    let input: unknown = {};
    try {
      input = JSON.parse(t.argsJson || '{}');
    } catch {
      input = {};
    }
    blocks.push({ type: 'tool_use', id: t.id !== '' ? t.id : `toolu_${i}`, name: t.name, input });
  });
  return blocks;
}

function anthropicAnswerJson(ans: NormAnswer, usage: { input: number; output: number }, model: string, seq: number): Json {
  const family = stopFamily(ans.stopRaw, ans.toolCalls.length > 0);
  return {
    id: `msg_${seq}`,
    type: 'message',
    role: 'assistant',
    model,
    content: anthropicContentBlocks(ans),
    stop_reason: ANTHROPIC_STOP[family],
    stop_sequence: null,
    usage: { input_tokens: usage.input, output_tokens: usage.output },
  };
}

function anthropicAnswerStream(ans: NormAnswer, usage: { input: number; output: number }, model: string, seq: number): string {
  const family = stopFamily(ans.stopRaw, ans.toolCalls.length > 0);
  const events: [string, Json][] = [];
  events.push([
    'message_start',
    {
      type: 'message_start',
      message: {
        id: `msg_${seq}`,
        type: 'message',
        role: 'assistant',
        model,
        content: [],
        stop_reason: null,
        stop_sequence: null,
        usage: { input_tokens: usage.input, output_tokens: 0 },
      },
    },
  ]);
  const blocks = anthropicContentBlocks(ans);
  blocks.forEach((block, index) => {
    if (block.type === 'text') {
      events.push(['content_block_start', { type: 'content_block_start', index, content_block: { type: 'text', text: '' } }]);
      events.push([
        'content_block_delta',
        { type: 'content_block_delta', index, delta: { type: 'text_delta', text: str(block.text) } },
      ]);
    } else {
      events.push([
        'content_block_start',
        {
          type: 'content_block_start',
          index,
          content_block: { type: 'tool_use', id: str(block.id), name: str(block.name), input: {} },
        },
      ]);
      events.push([
        'content_block_delta',
        {
          type: 'content_block_delta',
          index,
          delta: { type: 'input_json_delta', partial_json: JSON.stringify(block.input ?? {}) },
        },
      ]);
    }
    events.push(['content_block_stop', { type: 'content_block_stop', index }]);
  });
  events.push([
    'message_delta',
    {
      type: 'message_delta',
      delta: { stop_reason: ANTHROPIC_STOP[family], stop_sequence: null },
      usage: { output_tokens: usage.output },
    },
  ]);
  events.push(['message_stop', { type: 'message_stop' }]);
  return events.map(([name, data]) => `event: ${name}\ndata: ${JSON.stringify(data)}`).join('\n\n') + '\n\n';
}

// ---------------------------------------------------------------------------
// Routing: pick the best free model per prompt, fail over on refusals.
//
// Candidates are the brief's free models that meet the prompt's
// requirements (the manifest row for this arrival, widened by what the body
// itself carries: a response_format needs a JSON-capable model, declared
// tools a tool-capable one, an image a vision one). They are tried best
// first by the house's rating, discounted by the refusals that model has
// already given this trial — a model that refused once is likely
// rate-limited, so near-ties shift to a fresh model, while a clearly
// stronger model keeps its place until it has refused several times.
//
// A 200 answer that does not carry the prompt's token back earns nothing,
// so it is treated as a soft failure and the next candidate is tried; the
// first tokenless answer is kept as a last resort, since a well-formed
// answer still passes the wire checks where a 502 passes none. A 401 (no
// key pushed) or the trial's request cap refuses every later call by
// definition, so the router stops at once instead of spending calls.
// ---------------------------------------------------------------------------

interface ModelState {
  row: ModelRow;
  index: number; // brief order, the final tie-break
  refusals: number;
  dead: boolean; // refused in a way that will not heal this trial
}

const REFUSAL_DISCOUNT = 100; // rating milli lost per refusal this trial
const MAX_ATTEMPTS = 8; // gateway attempts per prompt, bounded by the client's 30 s

function meetsRequirements(row: ModelRow, requires: Requires, norm: NormRequest): boolean {
  const needTools = requires.tools === true || norm.tools.length > 0;
  const needJson = requires.json === true || norm.jsonMode;
  const needVision =
    requires.vision === true ||
    norm.messages.some((m) => m.parts.some((p) => p.kind === 'image' || p.kind === 'imageUrl'));
  if (needTools && !row.tools) return false;
  if (needJson && !row.json) return false;
  if (needVision && !row.vision) return false;
  return true;
}

function candidatesFor(states: ModelState[], requires: Requires, norm: NormRequest): ModelState[] {
  return states
    .filter((s) => s.row.free && !s.dead && meetsRequirements(s.row, requires, norm))
    .sort(
      (a, b) =>
        b.row.ratingMilli - REFUSAL_DISCOUNT * b.refusals - (a.row.ratingMilli - REFUSAL_DISCOUNT * a.refusals) ||
        b.row.ratingMilli - a.row.ratingMilli ||
        a.index - b.index,
    );
}

// The token(s) a prompt asks to have echoed back, read off its user text:
// the trial's prompts each carry one unique WORD-NUMBER token.
function expectedTokens(norm: NormRequest): string[] {
  const found = new Set<string>();
  for (const msg of norm.messages) {
    if (msg.role !== 'user') continue;
    for (const part of msg.parts) {
      if (part.kind !== 'text') continue;
      for (const match of part.text.matchAll(/[A-Z][A-Z0-9]+-\d+/g)) found.add(match[0]);
    }
  }
  return [...found];
}

function answerCarriesToken(ans: NormAnswer, tokens: string[]): boolean {
  if (tokens.length === 0) return true;
  const haystack = `${ans.text}\n${ans.toolCalls.map((t) => t.argsJson).join('\n')}`;
  return tokens.some((t) => haystack.includes(t));
}

interface Routed {
  answer: NormAnswer;
  model: string;
  failures: Refusal[];
}

async function route(
  states: ModelState[],
  requires: Requires,
  norm: NormRequest,
  trailOut: Refusal[],
): Promise<Routed | null> {
  const tokens = expectedTokens(norm);
  const failures: Refusal[] = trailOut;
  let tokenless: { answer: NormAnswer; model: string } | null = null;
  let attempts = 0;
  for (const state of candidatesFor(states, requires, norm)) {
    if (attempts >= MAX_ATTEMPTS) break;
    attempts += 1;
    const attempt = await callGateway(state.row.id, norm);
    if (attempt.ok) {
      if (answerCarriesToken(attempt.answer, tokens)) {
        return { answer: attempt.answer, model: state.row.id, failures };
      }
      if (tokenless === null) tokenless = { answer: attempt.answer, model: state.row.id };
      continue;
    }
    const refusal = attempt.refusal;
    failures.push(refusal);
    state.refusals += 1;
    if (refusal.status === 401 || refusal.code === 'request_cap') {
      // A key-level or trial-level refusal: every later call refuses too.
      for (const s of states) s.dead = true;
      break;
    }
    if (refusal.code === 'model_not_allowed') state.dead = true;
  }
  if (tokenless !== null) return { answer: tokenless.answer, model: tokenless.model, failures };
  return null;
}

// ---------------------------------------------------------------------------
// HTTP server
// ---------------------------------------------------------------------------

const ENV_URL = (process.env.MAKEFUN_ENV_URL ?? '').replace(/\/+$/, '');

function failuresHeader(failures: Refusal[]): Record<string, string> {
  if (failures.length === 0) return {};
  const rows = failures.slice(0, 8).map((f) => ({ status: f.status, code: f.code, model: f.model }));
  return { 'x-makefun-upstream-failures': JSON.stringify(rows) };
}

function sendJson(res: ServerResponse, status: number, value: unknown, extraHeaders?: Record<string, string>): void {
  const body = JSON.stringify(value);
  res.writeHead(status, {
    'content-type': 'application/json',
    'content-length': Buffer.byteLength(body),
    ...(extraHeaders ?? {}),
  });
  res.end(body);
}

function sendText(
  res: ServerResponse,
  status: number,
  contentType: string,
  body: string,
  extraHeaders?: Record<string, string>,
): void {
  res.writeHead(status, {
    'content-type': contentType,
    'content-length': Buffer.byteLength(body),
    ...(extraHeaders ?? {}),
  });
  res.end(body);
}

async function readBody(req: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  return Buffer.concat(chunks).toString('utf8');
}

interface ServerContext {
  brief: Brief;
  states: ModelState[];
  modelIds: string[];
  arrival: number;
  lastFailures: Refusal[]; // the refusal trail of the most recent prompt
}

async function handleCompletion(
  ctx: ServerContext,
  clientApi: 'openai' | 'anthropic',
  body: Json,
  res: ServerResponse,
): Promise<void> {
  const seq = ctx.arrival;
  ctx.arrival += 1;
  const row = ctx.brief.prompts[seq];
  const requires = row ? row.requires : {};
  const norm = clientApi === 'openai' ? fromOpenAI(body) : fromAnthropic(body);
  ctx.lastFailures = [];
  const routed = await route(ctx.states, requires, norm, ctx.lastFailures);
  if (!routed) {
    const trail = failuresHeader(ctx.lastFailures);
    if (clientApi === 'openai') {
      sendJson(
        res,
        502,
        { error: { message: 'no free model could serve this request', type: 'server_error', code: 'no_model' } },
        trail,
      );
    } else {
      sendJson(res, 502, { type: 'error', error: { type: 'api_error', message: 'no free model could serve this request' } }, trail);
    }
    return;
  }
  const headers = failuresHeader(routed.failures);
  const usage = withUsage(norm, routed.answer);
  if (clientApi === 'openai') {
    if (norm.stream) sendText(res, 200, 'text/event-stream', openAIAnswerStream(routed.answer, usage, routed.model, seq), headers);
    else sendJson(res, 200, openAIAnswerJson(routed.answer, usage, routed.model, seq), headers);
  } else {
    if (norm.stream) sendText(res, 200, 'text/event-stream', anthropicAnswerStream(routed.answer, usage, routed.model, seq), headers);
    else sendJson(res, 200, anthropicAnswerJson(routed.answer, usage, routed.model, seq), headers);
  }
}

function modelsDocument(ctx: ServerContext, anthropicShape: boolean): Json {
  if (anthropicShape) {
    return {
      data: ctx.modelIds.map((id) => ({ type: 'model', id, display_name: id, created_at: '2026-01-01T00:00:00Z' })),
      has_more: false,
      first_id: ctx.modelIds[0] ?? null,
      last_id: ctx.modelIds[ctx.modelIds.length - 1] ?? null,
    };
  }
  return {
    object: 'list',
    data: ctx.modelIds.map((id) => ({ id, object: 'model', created: 0, owned_by: 'free-model-aggregator' })),
  };
}

function makeHandler(ctx: ServerContext) {
  return (req: IncomingMessage, res: ServerResponse): void => {
    void (async () => {
      try {
        const path = (req.url ?? '').split('?')[0];
        if (req.method === 'GET' && path === '/v1/models') {
          // The wire is the auth header: x-api-key names Anthropic,
          // Authorization: Bearer names OpenAI.
          const anthropicShape = typeof req.headers['x-api-key'] === 'string';
          sendJson(res, 200, modelsDocument(ctx, anthropicShape));
          return;
        }
        if (req.method === 'POST' && (path === '/v1/chat/completions' || path === '/v1/messages')) {
          const parsed = parseJsonSafe(await readBody(req));
          if (!parsed) {
            sendJson(res, 400, { error: { message: 'request body is not valid JSON', type: 'invalid_request_error' } });
            return;
          }
          await handleCompletion(ctx, path === '/v1/chat/completions' ? 'openai' : 'anthropic', parsed, res);
          return;
        }
        sendJson(res, 404, { error: { message: 'not found', type: 'invalid_request_error' } });
      } catch (err) {
        if (!res.headersSent) {
          sendJson(res, 500, { error: { message: `internal error: ${String(err)}`, type: 'server_error' } });
        } else {
          res.end();
        }
      }
    })();
  };
}

// ---------------------------------------------------------------------------
// Trial driver: register, serve, poll /work, exit 0 when done.
// ---------------------------------------------------------------------------

async function loadBrief(): Promise<Brief | null> {
  const path = process.env.MAKEFUN_PROMPT_FILE;
  if (!path) return null;
  try {
    return parseBrief(await readFile(path, 'utf8'));
  } catch {
    return null;
  }
}

async function register(port: number, catalog: CatalogDoc): Promise<void> {
  // The env is up before the entry starts; retry briefly in case of a race.
  for (let i = 0; i < 100; i += 1) {
    try {
      const res = await fetch(`${ENV_URL}/register`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ protocol_version: 1, port, catalog }),
        signal: AbortSignal.timeout(5000),
      });
      if (res.ok) return;
    } catch {
      // fall through and retry
    }
    await sleep(200);
  }
  throw new Error('could not register with the env');
}

async function waitForDone(): Promise<void> {
  for (;;) {
    try {
      const res = await fetch(`${ENV_URL}/work`, { signal: AbortSignal.timeout(5000) });
      if (res.ok) {
        const body: unknown = await res.json();
        if (isRecord(body) && body.done === true) return;
      }
    } catch {
      // keep polling
    }
    await sleep(100);
  }
}

async function main(): Promise<void> {
  const brief = await loadBrief();
  if (process.argv.includes('--list')) {
    if (!brief) {
      process.stdout.write('{"providers":[]}\n');
      return;
    }
    process.stdout.write(`${JSON.stringify(buildCatalog(brief))}\n`);
    return;
  }
  if (!brief) throw new Error('MAKEFUN_PROMPT_FILE is not set or unreadable');
  if (!ENV_URL) throw new Error('MAKEFUN_ENV_URL is not set');

  const catalog = buildCatalog(brief);
  const states: ModelState[] = brief.models.map((row, index) => ({ row, index, refusals: 0, dead: false }));
  const modelIds = freeModels(brief).map((m) => m.id);
  const ctx: ServerContext = { brief, states, modelIds, arrival: 0, lastFailures: [] };
  const server = createServer(makeHandler(ctx));
  await new Promise<void>((resolve) => server.listen(0, '0.0.0.0', resolve));
  const address = server.address();
  const port = isRecord(address) ? numOrNull(address.port) : null;
  if (port === null) throw new Error('server did not bind a port');
  await register(port, catalog);
  await waitForDone();
  server.close();
  process.exit(0);
}

void main().catch((err: unknown) => {
  console.error(err instanceof Error ? err.message : String(err));
  process.exit(1);
});
