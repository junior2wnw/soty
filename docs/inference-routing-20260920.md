# Inference routing and client integration

The connector accepts the client's model choice. It preserves application tokens,
agent registrations, both existing chat endpoints, and legacy model aliases.
It no longer rewrites every request to an operator-selected model.

## Client contract

Application base URL: `https://соты.online/api/inference/v1`.
Agent base URL: `https://соты.online/api/connectors/gonka/v1`.
Each uses its existing Bearer credential and its existing authentication scope.

`GET /models` returns the three supported canonical IDs:

| ID | Legacy names |
| --- | --- |
| `deepseek-ai/DeepSeek-V4-Flash-0731` | Existing DeepSeek aliases including `deepseek-chat`, `deepseek-reasoner`, `deepseek-r1` |
| `MiniMaxAI/MiniMax-M2.7` | Existing MiniMax aliases including `MiniMax-M2.7`, `minimax` |
| `zai-org/GLM-5.3-Flash` | `glm`, `GLM-5.3-Flash`, `zai/glm-5.3-flash` |

Send the chosen ID in `POST /chat/completions`. No key replacement is needed.
Missing/unknown model names are rejected before reaching a provider. Legacy
aliases select the current model of their family, not the exact historical version.
Clients needing a particular version should use a canonical ID from `/models`.

`GET /health` is authenticated and free of inference calls. `configured` describes
local configuration; per-model provider states show circuit/queue state. This is
not a live probe or a promise that the next inference will succeed. The site's
root and general readiness remain in data recovery and may return 503 independently.

## Parallel and reserve modes

`SOTY_GONKA_PROVIDER_STRATEGY=race` sends the same canonical model to Gate and
OpenBroker concurrently. It returns the first valid answer and cancels the other
connection. Both providers receive the request and can bill for accepted work;
local cancellation does not guarantee remote cancellation. A cooling-down
provider is temporarily excluded. A shared queue bounds each provider's total
concurrency across all models; circuits are independent for each model.

For reserve mode set `SOTY_GONKA_PROVIDER_STRATEGY=fallback` and
`SOTY_GONKA_FIRST_TOKEN_TIMEOUT_MS=20000` in the existing protected environment
file, then recreate only the app with the existing release/override configuration.
This contacts OpenBroker only after a retryable primary failure before visible
output. Do not replace the environment or application token file with examples.

`SOTY_GONKA_FALLBACK_MODELS=*` enables both routes for all three models. A comma
list of canonical IDs restricts the secondary; `none` disables it. Invalid
policies fail closed. `SOTY_GONKA_UPSTREAM_MODEL` remains legacy default health
metadata and does not override explicit client selection.

## Timeouts and response integrity

The deployment uses a 55-second total budget for non-streaming requests, including
the queue. Race mode allows 45 seconds to initial provider activity; queue wait is
5 seconds and idle timeout is 30 seconds. A bounded JSON error arrives before a
client's 60-second cURL timeout. It is not a guarantee of successful completion.
An ordinary SSE response can continue beyond that budget after valid visible output.
Structured JSON streams are buffered until completion and validated within the budget.

The provider is contacted in stream mode even for JSON clients. The connector
collects the complete result and preserves usage. JSON mode checks parsing, and
`json_object` checks the object root; application-specific JSON Schema validation
and medical-content checking remain the client's responsibility. A complete leading
MiniMax `<think>...</think>` block is moved to `reasoning_content`, leaving the
answer bytes intact. Truncated JSON is rejected rather than repaired.

Tool arguments are validated before forwarding, finish reasons are normalized to
`tool_calls` when a call exists, and no second provider replays a response after
visible output. Explicit refusals remain refusals. Client disconnects release queues.
Upstream 401/402/403 errors are reported as gateway failures, not invalid client keys.

GLM has full reasoning by default. Gate documents `reasoning_effort: "low"` as its
fast mode. Clients may explicitly choose it; this release does not silently
override their reasoning preference. A 2048-token budget includes reasoning.

## Verification and limits

The local inference suite covers authentication scope, unchanged key access,
18 aliases, three model routes, both dispatch modes, shared queue capacity,
timeouts/cancellation, broken JSON, mismatched model metadata, and streamed tools.

Live synthetic tests on 2026-09-20 reproduced upstream instability. A ~22 KB
source-selection request completed with valid DeepSeek JSON in 2.967 seconds;
another with MiniMax completed in 20.600 seconds. Several answer-preparation
requests did not complete in 55 seconds; GLM also timed out. Direct OpenBroker
checks for all three models returned no answer in 56 seconds despite an active
account with spendable balance. These are a small diagnostic sample, not an SLA
or evidence of launch readiness. JSON parsing fixes and parallelism cannot supply
an answer when both external routes stall.

For REG.RU: keep the existing endpoint/key, select a canonical model, and retest
both guide steps on fictional inputs. Record HTTP status, elapsed time, and
`X-Soty-Request-Id` without logging client text or credentials. A 504 with a request
ID identifies a bounded upstream failure; a cURL timeout with no HTTP response
requires correlation with this ID or UTC timestamp and the network path.

## Data handling

Race mode sends content to both Gate and OpenBroker and onward to Gonka compute
nodes. Direct chat content is not persisted by the connector handler. Metrics
contain request IDs, canonical models, route names, durations, byte counts and
result codes, not prompts/answers/keys. Docker local logs rotate at 10 MB × 3,
which is a size limit rather than time-based retention. Current configuration and
registration database backups are encrypted; no timed deletion policy was established.

Gate's published privacy policy states no prompt/answer storage, 90 days for
technical inference logs and 30 days for hashed IPs. This is not a network-wide
independent guarantee. OpenBroker/node retention, processing countries, training
use and a health-data processing contract remain unconfirmed. The owner's earlier
statement of Russia does not independently establish the new Timeweb datacenter
or the countries of external GPU nodes. Gate terms require permission for resale;
no such permission was found in the reviewed materials.

Sources checked 2026-09-20: [Gate privacy](https://gate.joingonka.ai/privacy),
[Gate terms](https://gate.joingonka.ai/terms), [Gate docs](https://gate.joingonka.ai/docs),
[OpenBroker docs](https://openbroker.gonka.gg/docs),
[OpenBroker status](https://openbroker.gonka.gg/status).
