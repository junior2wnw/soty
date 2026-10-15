# Browser pilot acceptance receipt — 2026-10-15

Source-only, isolated from Root32/f90. Real model calls0; live/config/grant changes0.

Parent IAB acceptance on88357: authored-enabled app → Root consent → observe exactly Task/Add (password/file absent) → fill Allow → observe → click Allow → task exists in the same iframe → inner reload preserves task and revokes permission → reallow → fill Cancel denied → generation revoked. Browser was operated by Parent, not this agent.

Screenshot: C:\Users\Junio\Documents\Codex\output\BROWSER-PILOT-SAME-FRAME-20261015.jpg
SHA256: c3135d3a780e45392f2359929d7f9ab15be9c7883cd7b637cb51c43927195ee1

## Actual server/runtime continuation

`node --test server/test/personal-agent-apps.test.mjs` —25 PASS.

The new test at line333 reuses the existing fixture: actual Apps database/schema/connector claim/registration/owner inspection; actual Connect challenge/signatures; actual PersonalAgentComposition/runtime and GLM-provider parser/tool schema; existing client pump; actual SDK and Root bridge on an in-memory document. Only connector WebSocket IO, model response/usage receipts and DOM transport are synthetic. No Apps inspection stub in this case. Real DOM and localStorage acceptance are separately established by Parent IAB above.

Two explicit turns on the same document: observe→fill→finish, then observe→click→finish. Two synthetic human approvals, one task created, six deterministic provider responses. These are tool calls in the existing personal-agent runtime, not an MCP transport claim.

An attempted fill+click within one turn was correctly denied after the first effect. Existing runtime `modules/personal-agent/runtime/vendor/agent-runtime-core.ts:377-415` admits one effect/turn, then offers only finish_answer. This limit remains unchanged. Human approval is **up to60s**, not a promised allowance beyond the existing whole-turn deadline. No runtime deadline increase.

## Production admission — explicit remaining blocker

`server/personal-agent-composition.js:30-37` has constructor-only browserPilotEnabled, defaultOFF. `server/personal-agent-configuration.js:58,83` accepts only the existing installed schema fields and does not propagate this flag. `server/startup.js:84-89` passes that configuration onward. The Root toggle cannot turn on the server feature. There is currently no per-app host allowlist; enabling the boolean globally would expose the opt-in pilot to eligible owners, not only one selected app.

Therefore stock startup cannot currently enable exactly one app through existing UI/config without a reviewed host-code admission pin. A future narrowly scoped host patch must pin the existing authorized account+app before opening browser turns and preserve the normal owner/source checks. This receipt does not add that patch, environment variables or grants and does not claim production readiness.

## One real GLM request — not executed

Existing authorized path is the current signed-account assistant `agent.turn`, routed through the already installed accounted gateway. Minimal reviewable input is one own-account request: «Ответь одним словом: готово. Не вызывай инструменты приложений». This needs no exported key, cookie, new model or permission. However a prompt cannot guarantee exactly one upstream model request: runtime can perform multiple rounds. This would test existing inference only while browserPilotEnabled remainsOFF.

An exact one-request browser-pilot smoke cannot honestly be supplied as a stock command yet: it needs the reviewed one-app server admission above and a genuine signed actor, then bounded request accounting. Do not forge an actor or extract a gateway bearer to bypass that seam. Parent must review the eventual live action separately; none was run here.
