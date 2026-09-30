# P4-C1 — actual two-AS process race and crash acceptance

2026-10-01. Test-only increment over accepted production `6fd01a7ec64c644c9aff935bd65652cfb6cd6461`. The implementation, schema, catalog and HTTP APIs are unchanged. This closes the selected two-AS process evidence gap after [T2](p4-oauth-bearer-receipt.md); it does not declare the whole P4/CLI/browser work complete.

## Tested composition and instrumentation

Each case starts two distinct OS processes with the same canonical issuer, cookie scope, artifact/JWK keys and real Connect/Notes/Capabilities SQLite files. Fixture setup explicitly migrates Notes to 2 and Capabilities to 3 before starting the AS processes. Serving constructors reopen those actual formats without migration. The isolated runtime is Node 24.21.0 / SQLite 3.53.4; the installed maintained Provider is `oidc-provider@9.12.2`.

The worker composes the real `createNotesService`, `createCapabilitiesService`, `attachConnectModule`, `createOAuthHostProfile`, `attachCapabilitiesOAuth` and `attachCapabilitiesActions`. Both real native and OAuth readiness checks must pass. The owner account is established through signed Connect HTTP operations. Every tested code/AT/RT comes from an actual authorization, signed owner approval, completion, Provider resume and token HTTP flow. There are no injected actors, readiness booleans, fabricated token rows or fake provider storage.

This is a deliberately small tested host composition, not `createHttpApp` with World/Apps or a browser. The root's separate full-host acceptance is not counted here. Consent HTML is fetched but not executed. The fixture's minimal CSP and transport proxy are not production browser/CSP acceptance.

A synchronous forwarding facade calls the real artifact port and only then emits a private event or blocks at a selected checkpoint. Production `Adapter.upsert` does not await a returned Promise, so the test does not introduce one. The synchronous barrier uses a dedicated child pipe and one parent release byte. At each selected barrier a separate SQLite handle successfully performs `BEGIN IMMEDIATE` / `ROLLBACK` on both Connect and Capabilities: neither fence remains held while the parent interleaves or kills a process. The source deadlines use the actual clock; no fake TTL is used.

The parent selects the worker through a closed socket-port routing map before writing the canonical request. No public routing or forwarded-authority header is trusted. Keys, browser cookies, codes, ATs and RTs remain in memory/IPC and never enter test logs or configuration files. Test-only IPC can observe a just-committed lost AT in two crash cases; this is an assertion seam, not a token-recovery feature available to a client. Child stderr is discarded, so this suite does **not** establish that the production console is free of diagnostics.

Bounds: two live AS processes per fixture, at most four armed checkpoints, at most 512 private events per worker, 2 KiB/event, 16 KiB pending event text, 1 MiB HTTP response buffer, 10 s control/event deadline, 15 s HTTP deadline and 30 s case timeout. A control timeout forces termination. Teardown waits for actual child `close`; restart asserts a new PID. Temporary deletion is confined to the realpath-checked fixture root with its marker.

## Observed invariants

| Case | Actual interleave and required observation |
|---|---|
| Authorization-code race | Both processes finish a real find of the same unused code. A consumes and commits, then pauses. B's CAS detects reuse and commits family revoke before returning `invalid_grant`. A's later AT upsert is refused. Exactly one consume is correlated to that code; no AT/Note is created for that family. Reopen preserves revoke; a sibling connection still creates its Note. |
| Refresh-token race | Both processes find the same RT. A completes a real successful token response and its AT authenticates. B subsequently detects reuse and revokes the family. Reopen retains the original credential IDs/times, and A's previously delivered AT is denied. A sibling connection remains usable. A successful response is not a promise that a later reuse cannot revoke it. |
| Grant/link crash | A pauses after real Grant/link COMMIT and is forcibly killed before successful completion HTTP. After genuine reopen, B resumes the same signed interaction. Provider Grant, connection, client, principal, root grant and budget identities remain single and unchanged; the resulting AT performs a real native Note create. |
| Code consume crash | Forced kill after consume COMMIT, before AT issuance: zero AT/RT credentials. Reopen retains consumption; retry of the old code returns `invalid_grant` and durably revokes the family. |
| Code AT crash | Forced kill after atomic AT artifact + credential + link COMMIT, before RT/response: one AT, zero RT. The committed lost AT is a real right before retry; old-code retry revokes it. |
| RT consume crash | Forced kill after old RT consume COMMIT, before successor RT: only the original AT/RT remain. Reopen does not unconsume or fabricate rollback; old-RT retry revokes the family. |
| Successor RT crash | Forced kill after successor RT COMMIT, before successor AT: one AT and two RT artifacts. Old-RT retry revokes the family after reopen. |
| Refresh AT crash | Forced kill after successor AT triple COMMIT, before token response: two AT and two RT artifacts. The lost committed AT authenticates before retry, then is denied after durable reuse revoke. |

The five token-crash cases verify that credential identity and absolute expiry never change across restart/reuse and that no native Invocation or Note is silently created. Crash alone leaves a committed family active; it is the explicit consumed-source retry that triggers revoke. The fixture proxy's 502/transport-loss result is labelled as fixture transport failure, not an OAuth server response.

The exercised Provider ordering is specific and material: authorization code uses consume → AT save → RT save; refresh uses consume → successor RT save → AT save. No atomic transaction is claimed across these separate library awaits. Reuse can revoke the tokens of a formal winner; the guarantee is at most one consume and no live family right after committed revoke, not one permanently usable winning response.

## Runs and causal oracle correction

All commands used a process-local PATH selecting `var/toolchains/node-v24.21.0-win-x64`, so forked children also ran the isolated runtime. No concurrent suite was run by this owner.

```powershell
node --test --test-concurrency=1 --test-name-pattern='two actual AS processes read one code' server/test/capabilities-oauth-multiprocess.test.mjs
node --test --test-concurrency=1 server/test/capabilities-oauth-multiprocess.test.mjs
node --test --test-concurrency=1 --test-name-pattern='two actual AS processes read one' server/test/capabilities-oauth-multiprocess.test.mjs
```

| Run | Result | Log under `output/implementation-20260930/` |
|---|---|---|
| First code harness | 0/1, 7310.2531 ms; new fixture assertion counted the sibling's preparatory code exchange | `p4-oauth-two-as-harness-first.log` |
| Corrected code harness | 1/1 PASS, 0 skip, 2336.5817 ms | `p4-oauth-two-as-harness-green.log` |
| Full eight-case file | **8/8 PASS, 0 fail, 0 skip, 16033.1878 ms** | `p4-oauth-two-as-first.log` |
| Final two-race oracle repeat | **2/2 PASS, 0 fail, 0 skip, 4279.2556 ms** | `p4-oauth-two-as-races-final.log` |

The first RED was fixture accounting, not double consumption of one product code. After the full green run, source review identified a second oracle issue: HTTP completion and private event delivery use independent pipes. The final delta adds the SHA-256 of each operation's exact source and awaits its correlated event before counting. It removes dependence on a sibling-event baseline or delivery timing. The assertions about HTTP responses, durable family revoke, late upsert, sibling rights and SQLite state are unchanged. Only the affected two races were repeated on the final snapshot, as agreed with root; the six remaining cases retain the preceding full-file evidence and were not rerun under a misleading final-8/8 claim. No production correction was required.

Syntax checks of all three files and whitespace validation pass. Independent source review is recorded separately by the critic; author execution is not described as an independent repeat. This suite does not prove real CLI login, browser consent, production TLS/proxy configuration, Linux process behavior, or deployment capacity.

## Frozen inventory

SHA-256 of the final local bytes; paths relative to repository root:

| File | SHA-256 |
|---|---|
| `server/test/capabilities-oauth-multiprocess.test.mjs` | `e3957ec5fa1f004d0de8d41c74918aa48f7552114567c227e5cf3585fbc0e5b6` |
| `server/test/support/oauth-as-worker.mjs` | `f7c161bc5d91e0cb3d4485d74a0a57e6ae64783417d96222ba294135c4938a74` |
| `server/test/support/oauth-as-processes.mjs` | `db81a66dfe39c53b2c3e3f2dc43a197466f5a587ebe8fe54f35f69843af6cc7b` |

No existing source or root test fixture is edited. The three test files and this receipt are the entire owned change. Production remains the accepted `6fd01a7` snapshot; no DDL/reader/authority/receipt semantics were changed by this test increment.
