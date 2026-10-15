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

## Production wiring — availability, not a grant

Stock `server/startup.js:86` now supplies browserPilotEnabled:true to the existing composition. No new env/config/schema or hardcoded app/account. Standalone constructor default remainsOFF. Feature registration grants no access: installed model/account authorization, signed account/device, participant Apps authority, pinned source/frame, app-author SDK enable and per-app human consent/each-effect confirmation remain required.

Pilot scope is **current signed user of an accessible app**, one effect/turn. Existing private grants or a published public alias authorize entry; author SDK and Root per-app consent still must be enabled. No owner/principal substitution. Native backend continues to enforce its own user session in the same iframe.

Focused actual startup integration constructs normal Root services in a temporary directory with empty explicit env, confirms poll/reply registration, confirms model remains unconfigured and unauthenticated requests fail, never starts the listener, then closes services. Typecheck PASS;42 selected startup/runtime/bridge tests PASS.

Reused authorization port and related existing API:
1. `modules/apps/server/authority-port.mjs:11` / exported `index.mjs:1543`: withAppAuthority({actor,appId,mode:'participant',domainId}, callback) resolves current accessible entry, handles existing public alias fallback, and supplies frozen source revision/digest/origin under actor/Apps fence. Calling with a pure assertion callback performs no application effect or grant.
2. `modules/apps/server/publications.mjs:145-185` / exported `index.mjs:1549-1550`: decideAccess and recheckAccess evaluate real actor grants/public publication, then recheck branded decision, epoch, source tuple and expiry. A revoked grant cannot silently fall back to public. Browser turns reuse withAppAuthority; decideAccess/recheckAccess remain the existing lower-level publication API, not a new browser permission design.

## One real GLM request — not executed

Existing authorized path is the current signed-account assistant `agent.turn`, routed through the already installed accounted gateway. Minimal reviewable input is one own-account request: «Ответь одним словом: готово. Не вызывай инструменты приложений». This needs no exported key, cookie, new model or permission. However a prompt cannot guarantee exactly one upstream model request: runtime can perform multiple rounds. A one-turn smoke still needs the reviewed candidate installed and the existing account authorized; it is not a new permission.

An exact one-upstream-request browser-pilot smoke cannot be guaranteed by a prompt: the shared runtime can make multiple rounds. Use a genuine signed actor and existing accounted gateway after release review; do not claim a one-turn test proves a one-request budget. Do not forge an actor or extract a gateway bearer to bypass that seam. Parent must review the eventual live action separately; none was run here.


## Participant follow-up

Server browser-binding assertion now calls existing withAppAuthority(mode:participant) under the existing signed Connect actor fence. It compares current origin, source revision/digest and freezes policyEpoch/appRevision/domainId for the turn; every poll/reply/tool assertion rechecks them. Product mount supplies exact launcher.entry.domainId (optional wire field for older local fixtures); multiple public aliases therefore do not rely on a guessed address. No grants, publication, owner impersonation, Source/MCP profile or new auth realm.

Typecheck and58 focused tests PASS. Actual Apps signed tests cover foreign private denial; private granted participant success; grant removal during provider wait; public participant success on the SECOND alias; public alias not granting canonical origin; policy epoch change during wait; revoked app; authoritative source revision/digest mismatch. Separate signed bridge test mutates its source fixture while a command is pending and rejects both poll and reply. No real source promotion or production publication was performed.

Existing private demo app281 remains private; browser localStorage is not a reason to publish shared application data. No live/model calls or Root release changes.
