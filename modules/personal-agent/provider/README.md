# GLM Chat Completions port

Standalone host-only JavaScript/Node adapter for exact `zai-org/GLM-5.3-Flash`.
**Not integrated with Soty's live controller, account gateway or UI yet.** It accepts a trusted transport,
builds a bounded request and assembles a complete SSE reply. It handles no URL, auth header, API key,
account creation, grant, wallet, model prompt, source effect or business tool executor.

## Controller integration

```js
import { createGlmProvider } from './modules/personal-agent/provider/index.mjs';

const provider = createGlmProvider({
  transport: boundGatewayTransport,
  policy: approvedRoutePolicy,
  tools: currentPinnedTools,
});
const result = await provider.complete({
  requestId: stableControllerRequestId,
  messages: approvedAudienceMessages,
  signal: currentContextAbortSignal,
  onText: provisionalTextObserver, // optional trusted synchronous callback
});
// Recheck current context/lease, then dispatch result.message.toolCalls through existing U1.
// No tool is executed by this adapter.
provider.close();
```

The backend supplies `transport({requestId,requestDigest,body,signal}) → Response` and binds account,
device, audience, exact model route, data/region policy, approved capability/version, expiry and monetary
reserve using existing authority. Do not expose factory configuration or transport credentials to a model
or browser. Model payload cannot select endpoint/model/headers/tenant/limits. The fixed body asks for
streaming and usage, with one allowed model and a bounded `max_tokens`; it adds no provider-specific
hosted search, automatic fallback, SDK retry or business prompt.

The existing `modules/capabilities/server/validation.mjs` canonical JSON helper is reused for request
identity. The body is detached and deeply frozen. `requestDigest` binds exact canonical body + request ID;
callback/signal identities are excluded. A changed body changes the digest. **Deterministic identity is
not durable deduplication.** Two explicit `complete()` calls may perform two transport dispatches. Backend
admission must pin the same request ID/digest to current owner/budget and reconcile an unknown previous
attempt before repeating it. A new random ID on reconnect is not a safe retry policy.

## Trusted tool registry

Each selected tool is a closed descriptor:

```js
{
  name: 'notes_create_draft',
  description: 'The approved concise capability description',
  parameters: approvedClosedInputSchema,
  validateArguments: args => trustedSchemaAndDomainValidator(args) === true,
}
```

Use schemas/validators from the existing trusted pinned registry. `parameters` is bounded closed-object
model guidance; it grants no access and is not fetched as a remote schema. `validateArguments` must be a
pure synchronous validator for that exact schema/profile; this port does not implement arbitrary JSON
Schema or prove that a supplied callback matches its declaration. A permissive/miswired callback is a
host configuration fault. Current source/resource grants and domain authorisation remain the controller/
U1 responsibility, checked again before every consequential effect.

The adapter accepts only function tools, registered names, unique complete call IDs and contiguous tool
indices. Fragmented names/IDs/JSON arguments can interleave. Every call must finish with a complete JSON
object and pass decoded duplicate-key, Unicode, size/depth/node checks. The full set is parsed before any
validator runs; no executor/partial tool callback exists. Validators must return exactly `true`; false,
Promise or exception denies the result. Returned arguments are detached/frozen. The controller supplies
operation IDs; a model tool-call ID is not an authority or an exactly-once effect receipt.

History supports ordered resolved assistant/tool exchanges; IDs can be reused in separate resolved
turns. Unmatched replies/pending calls, unknown tool names, unsafe role fields and reasoning history are
rejected. Old history is controller-owned previously validated data, not permission to replay a tool.

## Stream completion and provisional text

This v1 profile is **SSE only** when `stream:true`; JSON-success compatibility requires a separate qualified
profile. UTF-8 bytes and SSE boundaries may split anywhere. There is a fatal UTF-8 decoder, bounded event
buffer and decoded duplicate-key JSON parser; unknown metadata is not a licence for an unbounded payload.

Successful return requires exact model proof, choice0/assistant, a valid terminal finish reason and
`[DONE]`. Length truncation, missing DONE/finish, sparse/duplicate/unknown tools, malformed JSON, forbidden
role/model, conflicting finish or data after terminal proof fail closed. Tools require `tool_calls` finish;
text requires `stop`. The adapter does not silently reinterpret a provider protocol change.

Optional `onText` sees **provisional content only**, after model identification. It never receives tool
arguments or reasoning fields. The controller must check current recipients/context on every observer
call and use it only for temporary display. It must not execute instructions, save memory or declare
success until the complete result is verified and current authority is checked again. A later error can
invalidate already displayed provisional text; the adapter cannot retract plaintext already delivered.
An observer must be synchronous; errors/Promises are normalised without their message/cause/details.

`reasoning_content` and `reasoning` fields are collected separately and bounded. By default only their
byte count is returned; raw trace requires explicit trusted `policy.returnReasoning=true` and stays in
`diagnostics.reasoning`, outside `message` and history. No memory is written. A route placing private
trace inside ordinary content must be qualified/filtered by its controller; this port does not infer
privacy from arbitrary model text. All content is untrusted data and creates no tool authority.

## Usage, errors and cancellation

Missing usage returns `{status:'unknown'}`, never zero tokens/cost. A reported block must contain valid
nonnegative integer prompt/completion counts and a consistent total. Contradictory/malformed later blocks
demote earlier observations to unknown before error; an optimistic early number cannot settle the attempt.
`reported` means provider-reported tokens, **not a paid invoice or final monetary settlement**. Backend
reserve/reconcile/settle remains the existing commerce gateway's job.

Safe `ProviderError` accounting preserves request ID/digest, dispatchAttempted, current usage status,
bounded completion/provider request IDs and HTTP status. HTTP/provider/transport/validator/observer error
bodies and arbitrary exception messages/cause/details are not returned. IDs resembling known bearer
prefixes are rejected/omitted. The transport still needs its own safe public error envelope and must not
log private request content or credentials.

Cancel/timeout does not prove no provider work or no charge. The caller receives an error promptly;
uncooperative transport/read/cancel retains this handle's bounded slot until the underlying work settles.
Late valid text/tools are discarded, late rejection is handled. Close aborts current invocations and
rejects new ones. The controller must enforce a global pool across handles/processes and backend request
deadlines/watchdogs; close/recreate is not a way to reset concurrency or money limits.

## Software bounds and model qualification

Defaults/hard ceilings:

| Bound | Default | Hard ceiling |
| --- | --- | --- |
| Request | 256KiB | 256KiB |
| Stream / event | 8MiB / 256KiB | same |
| Visible text / reasoning | 256KiB / 512KiB | same |
| Tool argument / selected tools | 64KiB / 12 | same |
| Messages / one message | 128 / 64KiB | same |
| JSON depth / nodes | 16 / 10000 | same |
| SSE events, including keepalives | 65536 | same |
| Completion tokens | 8192 | 32768 |
| Total request deadline | 120s | 1200s |
| Concurrent attempts per handle | 4 | 8 |

These are operational software limits, **not claims about GLM's actual context/output capability**.
Input bytes are not a tokenizer or cost estimate. Host policy must fit the currently qualified route,
actual tokenizer/provider limits, budget and response latency. Progressive tool selection avoids sending
every project's capabilities in every prompt.

On07.10.2026 a public unauthenticated `GET https://api.openbroker.gonka.gg/v1/models` again listed exact
`zai-org/GLM-5.3-Flash`. This read costs no inference and does not prove tools/latency/caps. Primary
[OpenBroker Chat Completions](https://openbroker.gonka.gg/docs/chat-completions) documents the forwarded
OpenAI-style body and usage/request-ID lookups; [Streaming](https://openbroker.gonka.gg/docs/streaming)
documents SSE terminal markers and potentially long first-header waits. The host route, not the adapter,
must decide a suitable bounded deadline. No live paid GLM request was used to validate this component.

The donor inspected was `D:/otsov/agent/agent.mjs:181–228` (read-only): UTF-8/SSE boundary collection,
interleaved tool fragments and DONE/finish proof. This port separates transport/credentials/business
context and adds closed input, complete validation, quotas, exact identity, error/usage and abort guards.

## Verification and outstanding integration

```text
node --test modules/personal-agent/provider/test/provider.test.mjs
```

Synthetic local transports only. Tests cover split UTF-8/SSE, interleaved tools, no partial validator call,
duplicate/invalid/deep JSON, byte/count limits, model/role/finish mismatch, missing DONE, usage conflicts,
reasoning isolation, request pinning, cancellation/deadline/close, ignored transport/reader capacity,
redacted errors and provisional observer abort. Types are supplied in `index.d.mts`.

Still required before user activation: current Connect/device/audience→gateway admission; per-account
monetary reserve and unknown reconciliation; native scoped credentials outside guest; global concurrency;
trusted tool profile pins and U1 effects; context guards around provisional/final delivery; and capped
real GLM text/tool acceptance under an explicitly permitted pilot. Standalone tests are not those proofs.
