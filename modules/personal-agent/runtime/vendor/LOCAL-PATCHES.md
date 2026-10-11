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
