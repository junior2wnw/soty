# P3-C1 — owner settings: client implementation

2026-09-30. Author checkpoint, not the complete C1 release gate. The backend inspection contract and independent service tests have separate owners. This change does not implement C2 source replacement, DNS deployment or a public catalogue.

## Implemented boundary

- `src/world/app-settings.ts` mounts one owner window and `app.ts` supplies a pinned account/app/screen context. Every request carries `expectedAccountId`. Late results may settle their own persisted command, but cannot update a closed window, another account or another screen.
- Inspection drives the current name, grants, aliases, publication and read-only source. `canPreview` is an allowed attempt, not proof of runtime health. Revoked or unsafe-path records remain inspectable without invented share links. Named-only records work without a canonical address.
- Name and community grants have separate CAS saves. A name save omits grants; a community save preserves all existing account grants. Inaccessible old community entries stay visible. Observation never silently cleans up access.
- Address claim and publication are separate commands. Claim receipt acknowledges the historical reservation; only a new inspection describes present activation/retirement. A publication failure does not undo the claimed name. Current active aliases and the unsaved selected aliases are shown separately.
- Public access requires a selected bound alias and explicit acknowledgment of the whole pinned device/port/target/profile. `listed` is preserved for an existing public policy and set false for restriction; this UI does not promise catalogue visibility. A repeated consent check alone does not enable a semantic no-op that would bump the policy epoch and disconnect sessions.
- A selected alias that subsequently disappears or becomes a tombstone creates a visible conflict. The owner can explicitly remove only unavailable selections, retaining the other edits. An epoch/source conflict still requires explicit selection of the current version; no automatic rebase.
- The copy action uses only inspection-provided permanent links, never a boot ticket. Clipboard success is acknowledged only after its promise resolves; failure exposes a read-only copy field. Preview uses a fresh B3 exact-domain/path launch. Existing community context is retained locally and grants no additional authority.

## Pending commands and concurrency

`app-settings-state.mjs` stores one pending claim/retire/publication intent per account and app. A short Web Lock protects local read/write/compare transitions, never a network wait. The exact normalized payload and request ID are saved and read back before dispatch. Unsupported locks or failed storage prevent a new unrecorded command.

Reload does not send anything. The owner explicitly repeats the stored command. Receipt identity, command fields and expected committed revision/epoch are checked before clearing that matching record. Historical receipt and current inspection remain separate. A late acknowledgment cannot erase another intent that was explicitly prepared after the old one was abandoned. A pruned/unknown receipt is not automatically replaced with a new request or epoch.

“Завершить проверку” reads current state before clearing only the matching local record. It explains that this does not cancel a possible server effect. Browser storage deletion, browser failure and writes outside this cooperating storage protocol remain limits; this is not cross-device locking or a promise of permanent browser durability. Server CAS/receipts arbitrate cross-device changes.

The draft model keeps name, grants and publication CAS bases separately. Inspection preserves edited text, selected IDs and their original base. A matching accepted result clears only matching submitted fields, never a newer edit. Nothing infers a successful publication from a lost response.

## Form and lifecycle

The form is built once. Refresh, visibility, source expiry and storage notifications update stable controls in place; they do not replace inputs or `<details>`. Input values are assigned only when different. Alias/group rows are keyed and reused. Buttons awaiting a result remain focusable with enforced `aria-disabled`; they cannot dispatch another command. Destructive confirmations initially focus the safe action, and cancellation returns to its original trigger.

Explicit refresh and visibility restoration only read inspection, retaining drafts. Freshness uses additive server `checkedAt`: `max(0, min(45000, freshUntil − checkedAt) − full monotonic request duration)`. From receipt the UI uses a monotonic deadline. A 44-second-old observation cannot get another 45 seconds, and a browser wall clock behind the server cannot keep “Источник отвечает” indefinitely. Missing/malformed timing fails to unknown. The historical observation time remains visible; this evidence is not a security, DNS/TLS or functional application check.

Header close, Escape, preview and same-document route changes protect unsaved drafts. Native account/screen destruction closes and disposes the old private window without showing its confirmation in a new account. A same-account shell refresh preserves the window and running iframe; a network-error preservation additionally verifies the local account. Reload uses the browser's unsaved-change guard. Prepared commands remain account-scoped after a deliberate close.

Name changes call the existing metadata updater. They update the toolbar and `iframe.title` without recreating the iframe or chat, requesting another ticket or dropping a WebSocket. A preview is a separate deliberate launch. No mutation navigates the user home as a success substitute.

## Local verification

- `node --test src/world/app-settings-state.test.mjs src/world/app-launch.test.mjs`: **31/31 PASS** (12 new author helpers and 19 existing B3 launch cases).
- `node --test src/world/app-settings-state.test.mjs src/world/app-settings.acceptance.test.mjs`: **25/25 PASS**; 13 cases are the independent reviewer's actual-service/deferred/locks/storage suite, not author-owned tests.
- `node node_modules/typescript/bin/tsc -p tsconfig.json --noEmit`: PASS.
- `git diff --check`: PASS for the current checkout at author check.

The root browser examiner separately reported a real name save preserving the app's entered text and live WebSocket, and an Escape draft guard. Those are partial browser observations; final Back/refresh/claim/activation/copy/preview/two-tab/account-switch/mobile evidence belongs in `p3-owner-settings-browser.md`. No author claim of final browser or release readiness is made here.

## C1 account-boundary follow-up

Independent review of optional metadata found an existing offline transition that assigned `deskAccount` without clearing the prior account's caches. A subsequent online response for the same new account would then retain the old application list. The new `transitionAccount` is the single boundary for online, offline and invalid identity: it closes private handles/dialogs, invalidates pending loaders, clears application/device/community/profile/search/note projections and reloads only that account's desk preferences. It does not delete account-scoped durable notes or pending commands.

Refresh reads the available local identity before loading the signed profile, and checks it again before publishing the returned profile. Offline preservation is allowed only for a confirmed matching local account and an unchanged screen, on a network error. Missing, unreadable, mismatched or revoked identity cannot take the old `preserveNote` shortcut. The same-account notes/settings success paths preserve the existing editor, window and iframe.

`accountTask` captures identity plus transition generation, so a late A request also fails after A → B → A. Resources, home projections, community app cards and optional app metadata reject stale completions/errors. The standard Apps list request pins `expectedAccountId`; injected list hooks are still fenced before their result is used. Exact-domain metadata cannot change launch authority or populate the older lossy recent-route store.

The owner dialog handles keyboard Escape explicitly with `preventDefault`, `stopPropagation` and `requestClose`; its native `cancel` listener remains for other close requests. This keeps repeated Escape within the existing draft confirmation rather than relying solely on native CloseWatcher cancellation. Account destruction still bypasses voluntary-leave confirmation.

`node --test src/world/app-account-lifecycle.test.mjs`: **9/9 PASS**. The harness transpiles and executes the actual `WorldApplication` methods with inert UI/network ports; it does not copy the controller logic or assert source regexes. Cases cover A → offline B → online B, old resource results/errors, A → B → A, missing/unreadable/revoked identity, a profile/local-identity mismatch, and same-account notes/settings preservation. This is controller behavior evidence, not a browser or native Escape proof; root performs those checks on the frozen build.

## C1 cross-tab admission and initial-launch ordering

Independent testing subsequently reproduced a deeper identity race with the actual Connect client/service: a transaction snapshot and coalesced notifications could yield profile A, communities B and final local A. The earlier before/after local checks alone were insufficient. `extension(operation, args, { expectedAccountId })` now captures the optional account context before queueing and verifies the actual selected installation inside the serialized task before challenge/signing. Existing current-device checks still run before signing and after a response. The context never changes the signed product arguments; Apps, Notes and capability server account checks are retained unchanged.

The World adapter creates a separate API closure per mounted account. A late callback from the destroyed A controller still requests A, even if B has since mounted; it cannot silently adopt B from a mutable global. The contact callback uses the same scoped API. A null identity cannot enqueue an unchecked World request. Two-argument Connect callers retain their old behavior, so other products must explicitly opt into displayed-account admission. This is account consistency, not an atomic multi-operation server snapshot or cancellation of a committed effect.

The same real queue revealed that starting optional `loadApps` before `launcher.launch`, even without awaiting it, delayed the app behind catalogue loading. Initial launch is now enqueued before catalogue/community/chat reads. These optional results remain guarded metadata and cannot grant launch rights. An account-admission refusal has a clear account-changed recovery message, without automatic retry.

- `node --test modules/connect/test/extension-identity.test.mjs`: **6/6 PASS**. Actual two clients/native signatures/service; old two-argument ABA reproduction, pinned refusal before B traffic, immutable arguments/context, pre-signing/post-response identity checks and compatibility. The storage/channel seam is synthetic and explicitly does not claim real IndexedDB proof.
- `node --test src/world/app-account-lifecycle.test.mjs`: **10/10 PASS**. The tenth case executes actual app-controller code with the real serialized Connect client/service; both held catalogue and held community responses allow the first launch/frame before metadata is released.
- `node --test src/platform/world-adapter-identity.test.mjs`: **1/1 PASS**. Actual adapter code, isolated account/UI ports; old and new mounts retain A/B respectively, late contact stays A, null account sends nothing.
- TypeScript no-emit check: PASS after this extension.

Root owns the final stable browser repeat and full regression. These local results do not claim browser cookie, production deployment or backup/restore readiness.
