# Independent review: community and application boundary

Reviewed during implementation on 2026-09-28. Read-only review of `modules/apps/server` and `scripts/agent-modules/local-apps.mjs` by the community-module owner; fixes remained with the application-module owner.

## Findings and disposition

1. **Oversized HTTP response left an authorized stream pending.** The earlier catch branch only closed streams whose grants had been revoked. The application owner changed the data branch to close the offending oversized stream immediately. Verified in the current source.
2. **Raw WebSocket tunnelling did not enforce the promised per-message bound.** The application owner added incremental RFC 6455 header inspection, direction/mask checks and a cumulative 1 MiB bound across continuation frames. Negotiated compression is omitted in this version. Reviewed the parser and its negative tests.
3. **Live host credential revocation was not checked after channel authentication.** The implementation now offers `invalidateConnector` and periodically revalidates the existing host credential. The immediate hook belongs to the host integration. The regression test revokes an authenticated runtime while a WebSocket is open and verifies closure.
4. **A removed app publisher could retain a community grant.** Live group authorization now requires both viewer membership and continued group-administration permission for the app owner. A committed World membership event invalidates grants after demotion/removal. The regression test covers publisher demotion while a participant uses the app.
5. **Iframe cookie delivery depends on browser site policy.** The reviewed version used `SameSite=Lax`. The application owner reproduced its failure in an embedded `.localhost` app and replaced it with a host-only, HttpOnly, Secure, `SameSite=None; Partitioned` cookie. That owner reports actual-browser iframe/assets/HTTP/WebSocket/revocation success. This review independently checked the server boundary; browser evidence remains with that owner, rather than being inferred from Node tests.

The target is built as `127.0.0.1` plus the explicitly registered port; request paths cannot replace its host. Upstream redirects are inspected without following arbitrary destinations. Session cookies, Authorization headers and Connect/connector credentials are not forwarded to the local application. App origins are separate from the shell's account origin. Those properties were checked in source and exercised by the application suite.

## Verification observed

`node --test modules/apps/test/*.test.mjs`: **8 passed, 0 failed**. Includes real runtime-to-gateway HTTP/assets/JSON POST/WebSocket, outsider denial, live revoke, credential revoke, publisher demotion, SSRF/forged ownership/Origin rejection, bounded streaming, manifest validation and frame/message limits.

`node --test modules/world/test/*.test.mjs`: **18 passed, 0 failed**. Includes signed Connect identities and actual HTTP adapter, visibility projections, role boundaries, request/invite membership, revocation, idempotency, ownership transfer, restart, and privacy-preserving member lists.

These are local implementation observations, not evidence of public deployment or a completed production browser workflow. The parent task owns full UI/connector integration and acceptance of all five stages.

## Follow-up: account-owned agent jobs

Read `server/apps-jobs.js`, the Connect asynchronous-extension boundary, and the new ConnectorStore ownership guards. The proof is consumed before asynchronous external I/O, request arguments are copied from the signed canonical input, and the store rechecks the guard inside its write queue. The owning account and explicit requested connector protect jobs from the old shared-room-link API.

Found a mutation-order defect: cancellation initially checked the selected host only after the store had already cancelled the job. The parent changed store lookup to include `expectedDeviceId` and `connectorId` before mutation. This prevents an account using host A's current permission to cancel an account-owned job on host B through a mismatched request.

Also reported a response-size edge: cancellation/create replay can include all historical events, and result text was character-limited rather than byte-limited. The Connect extension envelope limit is checked after a mutation, so an oversized reply can hide an already-accepted cancellation behind an error. The parent changed account-owned projections to omit event history and return a 32,000-character result preview with an explicit truncation flag; event history remains paged. The reviewed source now bounds even escaped Unicode/control text well below the extension envelope.

A broader inherited scaling concern was reported: anonymous challenges were rate-limited by Origin, which is shared by all users of the website. The parent changed HTTP challenge/discovery/recovery rate keys to a hash of the adapter-provided peer, while keeping the signed-device budget. The adapter uses Express's configured `req.ip` or the socket peer, never an RPC argument; production must configure any trusted reverse proxy boundary explicitly rather than accepting arbitrary forwarded headers.

After avatar schema 2, the independent combined run `node --test modules/world/test/*.test.mjs server/test/apps-jobs.test.mjs` passed **26 tests**: 24 World (including signed HTTP avatar upload/read/hide), and 2 existing app-job integration tests. This result precedes any additional tests the parent adds for the two findings above.

## Final focused regression and runtime correction

Added `server/test/connect-peer.test.mjs` with 4,800 successful real HTTP challenges across the neutral adapter, Express defaults and an explicit proxy allowlist. The checks exhaust one client's budget, then attempt to bypass it through a JSON `peer`, `X-Forwarded-For`, `Forwarded`, `X-Real-IP` and another allowed Origin. Another real loopback client retains its independent budget. With a trusted proxy, the nearest untrusted hop determines the client; changing an earlier forwarded address cannot reset that budget. An untrusted socket cannot supply its own forwarded identity. The old persisted-counter test now checks the counter rather than depending on its internal key namespace.

The expanded signed app-job tests use more than 2 MiB of event history and approximately 5.1 MB of JSON-encoded final text. Cancellation, creation replay and event pages remain bounded. All final text is reconstructed exactly from signed result pages, including Unicode surrogate pairs split across boundaries. A second owned device, a second owned connector and another account cannot read or cancel the job. Revoking ownership while reads/cancellation wait in the store queue prevents both output disclosure and cancellation. When inference is unavailable, no new job is enqueued; existing reads, result pages and cancellation still work.

This review then found a real integration defect beyond the original test scope: the connector's one-second cancellation watcher still used the legacy shared-link `GET /api/connectors/jobs/:id`. Its new account-owner guard correctly rejected account-owned jobs, causing the watcher to abort valid work. The parent authorized a dedicated runtime route, implemented here:

- `GET /api/connectors/jobs/:id/runtime-status` authenticates the existing connector credential and requires the exact assigned link/device/connector.
- Its only job fields are `id`, `status` and `cancelRequested`; responses use `Cache-Control: no-store`.
- A known credential does not authorize an unassigned job. Wrong credentials, device or connector receive the same fixed authentication error. The legacy shared-link route remains closed to account-owned jobs.
- Runtime event/result POST acknowledgements use this same small projection. Existing store-method return contracts remain unchanged. The connector callers only need acknowledgement; they no longer receive every historical event on each write or a duplicate megabyte result.

The HTTP upload boundary had another mismatch: a one-million-character stored result can need roughly six million JSON bytes, while the shared request parser allowed only 1 MiB. The result route now has its own 6 MiB parser; other connector routes retain their smaller limit. Real HTTP regression verifies a maximum escaped result, metadata limits and rejection of a body over 6 MiB before the job is completed.

`server/test/local-app-workspace.test.mjs` uses the actual Windows filesystem. It checks separate directories per server job ID, retained files on lease retries, explicit workspace selection, denied roots/traversal and a junction in either generated path component. Sentinel files remain unchanged. Canonical path checks establish the allowed starting workspace; they are **not an OS sandbox**. The existing agent's shell still has the local user's process permissions. Generated folders are retained on failure/cancellation.

Final independent run after the runtime acknowledgement change: **47 passed, 0 failed, 0 skipped**. The selected suites were:

```text
server/test/app-agent-runtime.test.mjs
server/test/jobs-runtime.test.mjs
server/test/apps-jobs.test.mjs
server/test/connect-peer.test.mjs
server/test/local-app-workspace.test.mjs
modules/connect/test/http.test.mjs
modules/connect/test/server.test.mjs
```

The OpenCode test was enabled through `SOTY_OPENCODE_E2E_PATH` pointing at the already-installed trusted executable. It ran the real connector and CLI, waited across two cancellation-watch intervals, executed the CLI's actual file-writing tool, read a fresh manifest, and registered/launched the local HTTP test application. Its inference responses and sample application were controlled local fixtures. This proves the runtime/control-plane integration; it does **not** claim availability or quality of production model inference.
