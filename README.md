[![make.fun free-model-aggregator-02](https://make.fun/badge/free-model-aggregator-02.svg)](https://make.fun/bounty/free-model-aggregator-02)
[![rank](https://make.fun/badge/free-model-aggregator-02/maezageryan-del/free-model-aggregator-02/rank.svg)](https://make.fun/bounty/free-model-aggregator-02)
[![reward](https://make.fun/badge/free-model-aggregator-02/maezageryan-del/free-model-aggregator-02/reward.svg)](https://make.fun/bounty/free-model-aggregator-02)

# Free Model Aggregator 02 — live free-model aggregator

One server (`main.ts`, Node 22, standard library only, no dependencies) that
serves both the OpenAI Chat Completions API and the Anthropic Messages API —
JSON and streaming — and answers every request with a **real completion from
a free OpenRouter model**, dialled through the grader's gateway. The server
never answers from itself.

## How it works

At startup the server reads the trial brief (`MAKEFUN_PROMPT_FILE`): the
fourteen free models (dialable id `openrouter/<vendor>/<model>:free`, the
house's quality rating, context length, tools/vision/JSON support) and the
six prompts' manifest in arrival order. It then:

1. listens on `0.0.0.0` at a free port it picks (the env owns 8080),
2. registers the port and its catalog with the env
   (`POST $MAKEFUN_ENV_URL/register`, protocol version 1),
3. serves `GET /v1/models` (OpenAI shape for `Authorization: Bearer`,
   Anthropic shape for `x-api-key`), `POST /v1/chat/completions` and
   `POST /v1/messages`,
4. polls `GET $MAKEFUN_ENV_URL/work` and exits 0 when it says done.

`node main.ts --list` prints the catalog document — one `openrouter`
provider (`base_url` `https://openrouter.ai/api/v1`, `key_env`
`OPENROUTER_API_KEY`, `wire` `openai`) holding exactly the fourteen dialable
free ids — byte for byte as it is registered.

## Routing and failover

- Every request, on either wire, is normalized once and dialled on the
  gateway's Chat Completions route only:
  `POST $OPENAI_BASE_URL/chat/completions` with the trial's dummy key
  (`OPENAI_API_KEY`) as the bearer token; the gateway swaps it for the
  entrant's pushed OpenRouter key, which never enters the sandbox. The
  Anthropic Messages gateway route is never used — the gateway refuses the
  allowed ids there.
- The gateway is always dialled non-streaming; the collected answer is then
  returned in the client's wire, streamed as SSE if the client asked. That
  keeps each gateway request a single deterministic body, so a replayed
  trial makes the same calls.
- Candidates are the free models meeting the prompt's requirements (from
  the manifest row, widened by the body itself: JSON mode needs a
  JSON-capable model, declared tools a tool-capable one, an image a
  vision one), tried best-first by the house rating, discounted by the
  refusals that model has already given this trial — a model that refused
  once is likely rate-limited, so near-ties shift to a fresh model.
- A refusal (a provider's real 429/5xx, relayed as sent) fails over to the
  next candidate, and the refusals a prompt recovered from are reported in
  the `x-makefun-upstream-failures` response header. A 401 (no key pushed)
  or the trial's request cap stops routing at once: every later call would
  refuse too.
- An answer that does not carry the prompt's token back earns nothing, so
  a 200 answer without it is treated as a soft failure and the next
  candidate is tried; the first such answer is kept as a last resort.
- The `model` field of every answer and every streamed chunk names the
  dialable id really dialled — never the shorter slug the provider's own
  answer echoes back.
- The dialled body always names `max_tokens` (the client's cap, clamped to
  the gateway's 8192 ceiling, or a default when the client named none),
  because the gateway refuses uncapped calls.

Every choice is a pure function of the brief, the requests and the
gateway's answers: no wall clock and no unseeded randomness in anything
sent, so a replay leaves the same state export byte for byte.

## The key

The entry needs an OpenRouter key pushed to the competition as the
`OPENROUTER_API_KEY` secret before submitting (`makefun secrets push
free-model-aggregator-02 --env-file .env`). The key itself never enters the
sandbox or this repository; the server only ever sends the trial's dummy
key to the gateway.

## Layout

- `main.ts` — the whole server: brief parsing, catalog, both client wires,
  gateway dialling, routing/failover, registration and the work poll.
- `LICENSE` — MIT.
- `.makefun/owner.json` — the entrant's wallet claim file.
