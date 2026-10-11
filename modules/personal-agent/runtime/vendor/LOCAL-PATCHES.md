# Local runtime patch

The donor remains `agent-platform` revision
`23717ad30878c9ad7a8d9d614bbd8384004763c9`, path
`packages/runtime/src/agent-runtime-core.ts`. `UPSTREAM.json` retains that
revision/blob and original SHA-256; its `sha256` and `bytes` verify the local
file. `sourceUnmodified: false` explicitly records the divergence.

## bounded-finish-answer-output-budget-2048

Both routed and post-tool requests offer `finish_answer`. Their former output
budgets of 384 and 640 tokens were too small for a useful structured draft.
Both now request at most 2048 tokens, including the answer's JSON envelope and
claims. The host's existing `Math.min(modelPolicy.maxOutputTokens, maxTokens)`
continues to enforce any smaller approved cap. This budget provides headroom
for a roughly 1100-token completion; it does not promise every 8000-character
answer fits, or change the provider's tokenizer or cost accounting.

The change touches no parser, validation, history, deadline, authorization,
provider failure classification or retry behavior. A `length` finish still
fails closed even if its arguments happen to be valid JSON. Failed results
are not saved; an explicit repeat of the same turn requires reconciliation.

Verification: `node --test server/test/personal-agent-http.test.mjs` uses the
actual signed HTTP/composition/runtime/provider/Notes path with synthetic SSE
and billing ports. It exercises a long `finish_answer` with declared fixture
usage of 1100 tokens in either phase, lower/higher host caps, truncated and
valid-JSON `length` finishes, and no retry/history on failure. Synthetic usage
is not a GLM tokenizer measurement, paid receipt or real-model acceptance.

The historical production `agent_model_unknown` failures remain unclassified
by this patch; a small budget is a demonstrated local failure mechanism, not
proof of the historical provider error code. No donor repository, deployed
runtime, configuration or paid model call is changed here.

## terminal-schema-current-evidence-and-applicability

Build each advertised `finish_answer` schema from the current adapter scope and
already obtained tool evidence. Applicability is an exact enum; evidence IDs are
limited to those obtained in this turn. Without source evidence, only inference
and projection claims are offered; source-free facts remain invalid. The system
instruction recommends empty claims and selections for drafts from user text.
After a tool result, fact claims require nonempty evidence references and the
instruction requires those references to be selected. Generic source-free
inference/projection behavior and the complete existing terminal validator stay
unchanged; no invalid response is repaired or silently promoted to a fact.

Verification: `server/test/personal-agent-apps.test.mjs` exercises actual signed
Connect, composition, JSON provider, runtime, Notes and browser conversation with
synthetic model/receipt IO. Two old-schema failures, schema-guided draft and
sourced-fact success, and invalid claims/evidence rejection are covered without
paid calls. The strict JSON, accounting, authorization and replay boundaries are
unchanged. The donor pin records this local patch; deployment is separate.

## installed-exact-effect-capability

The portable runtime still defaults to denying effect names. An optional
`effectCapabilities` list comes only from the installed host adapter. Admission
requires an exact action name equal to its capability, membership in this
explicit effect list, and membership in the ordinary capability allowlist.
Soty supplies only `source_write_1`, when its selected-window tool exists.
This does not issue a grant, choose a window or authorize a Native effect.
The existing Root capability ledger, original selected handle and Native ACL
remain independently necessary. No provider JSON/configuration can extend the
list, and the existing one-tool + terminal cycle and retry rules stay unchanged.

Verification: `server/test/selected-native-source.test.mjs` checks default denial,
missing ordinary capability, and a mismatched action name. The ordinary Native
and HIVE integration tests exercise the signed runtime through real installed
HTTP/WS and Native SQL with synthetic model/billing callbacks. They perform no
paid provider call. Source receipts are independently read; an unknown result
uses the original opaque invocation ID and exact selected input for a readonly
reconciliation, without another Native mutation.
