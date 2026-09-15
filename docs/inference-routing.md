# Inference routing on soty

The application and connector-device API paths share the same provider policy.
Client credentials remain in the existing authenticators; they are never sent to
providers. Provider credentials, prompts, reasoning and account balances are
excluded from metrics.

## Parallel mode

`SOTY_GONKA_PROVIDER_STRATEGY=race` is the default. Each accepted request starts
both configured providers concurrently, subject to each provider's bounded
queue and circuit breaker. A healthy primary is not preferred over a faster
secondary. Providers in cooldown are temporarily skipped; one recovery probe
is admitted after cooldown.

- For streaming clients, the first visible answer text selects the winner.
  Role-only events, heartbeat comments and reasoning do not win the race.
- Tool calls are buffered until the provider has completed the response and
  their IDs and JSON arguments have been validated. A partial tool call must
  never escape and cause a duplicate action.
- For JSON clients, the first complete usable response wins. Early partial
  text cannot commit a JSON response.
- Explicit refusals and content-filter terminations are valid outcomes.
- Losing HTTP requests and queued attempts are cancelled. Cancellation does
  not count as provider failure. Disconnecting the client cancels every attempt.
- After committing any output, the relay never splices another provider's
  output into the response. A later failure is reported as an incomplete stream.
- The winning response retains reasoning fields and original chunks needed
  for tool continuations. Applications decide how to hide reasoning in the UI.

Parallel dispatch can consume tokens at both providers before cancellation is
processed. It improves tail latency, not provider capacity. Switch the environment
value to `fallback` to restore sequential routing. A normal service restart is
required after changing environment settings.

## MiniMax auto-tool compatibility

On 2026-09-15 both Gonka routes reproduced a specific failure: with optional
tools supplied, an ordinary answer ended with `finish_reason=stop`, an unclosed
`<think>` block, no visible text and no tool calls. The identical request without
the tool definitions produced an answer. `reasoning_split=true` did not fix the
provider behavior.

`SOTY_GONKA_EMPTY_TOOL_FALLBACK=1` enables one compatibility retry for that
MiniMax condition. The retry keeps the model, messages, context and generation
settings, but omits the optional tool definitions. It runs only before any
output is committed, within the original deadline. Required or explicitly
selected tools, malformed tool calls, refusals, transport errors and truncated
answers do not trigger this retry. It does not remove skills from the project or
change character instructions. Set the value to `0` when the upstream bug is fixed.

## Bounds and verification

The existing defaults remain: 100 simultaneous logical requests per client key,
8 active attempts and 64 queued attempts per provider, 5 seconds maximum queue
wait, 75 seconds for initial response/JSON completion, 20 seconds for first
provider activity and 30 seconds for idle streams. Heartbeats cannot extend a
stalled provider's activity deadline. The separate stream deadline applies only
after an answer has begun. HTTP backpressure pauses the winning reader.

`npm run inference:selftest` verifies provider failures, cooldown and recovery,
timeouts, JSON/SSE behavior, fragmented UTF-8 and reasoning markers, tool
validation, cancellation and queue release, concurrent requests, MiniMax
compatibility and isolation across application and connector credentials.
Tests use synthetic providers and credentials. Production probes should be few,
neutral and report only timing, status, selected provider and counts.

Keep the current recovery override and maintenance marker when deploying this
connector change. Passing inference tests does not authorize restoring unrelated
Soty features or changing recovered job/data state.

References: [MiniMax OpenAI-compatible API](https://platform.minimax.io/docs/api-reference/text-openai-api),
[Node.js AbortSignal](https://nodejs.org/docs/latest-v24.x/api/globals.html#class-abortsignal),
[Node.js stream backpressure](https://nodejs.org/docs/latest-v24.x/api/stream.html).
