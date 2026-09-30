# Soty local applications

An independent web project on a claimed connector becomes a persistent app cell. Browser content uses an isolated per-app origin; the gateway forwards HTTP and WebSocket through the connector's outgoing authenticated channel to a fixed loopback port. Neither connector credentials nor Soty account storage enter the app.

## Host integration

```js
const apps = createAppsService({
  dataDir,
  appOriginTemplate: 'https://{appId}.apps.example.org',
  shellOrigins: ['https://example.org'],
  actorActive: actor => connect.isActorActive(actor),
  canAccessCommunity: world.canAccessCommunity,
  isGroupAdmin: world.isGroupAdmin,
  activeCommunityIds: world.activeCommunityIds,
  subscribeMembership: world.subscribeMembership,
  withAuthorityFence: callback => world.withCommunityAuthorityFence(callback),
  readCommunityAuthority: (actor, ownerAccountId, ids) => world.appCommunityAuthority(actor.accountId, ownerAccountId, ids),
  authenticateConnector: async auth => {
    await connectorStore.writeQueue;
    return Boolean(connectorStore.authenticate(auth));
  },
});
```

Register `apps.operations` and its synchronous `execute({op,args,actor})` as a Connect proof extension. The actor must come from verified account installation state. Install `apps.handleRequest(req,res)` before the shell CSP/static handlers; install `apps.handleUpgrade(req,socket,head)` before other upgrade handlers. Both return a boolean indicating ownership of the request. Call `apps.close()` during shutdown.

`apps.resolveOwnedDevice(actor,hostDeviceId,connectorId?)` returns connector identity only to trusted server code, after ownership verification. It is intended for the account-authenticated jobs bridge. Do not expose its Link ID to community members.

`apps.invalidateAccess({accountId?,deviceId?,appId?})` immediately rechecks and closes affected app sessions. World membership events are already subscribed. `apps.invalidateConnector({hostDeviceId?,connectorId?})` closes a revoked host immediately; periodic token validation provides a further bounded check. A group grant remains valid only while its publisher is a group administrator and its viewer is an active member.

## Signed control operations

| Operation | Arguments | Result |
|---|---|---|
| `apps.devices` | `{}` | `{devices:[{hostDeviceId,connectorId,name,online,claimed}]}` |
| `apps.claim` | `{hostDeviceId,connectorId,claimCode}` | `{device}` |
| `apps.list` | `{communityId?}` | `{configured,apps}` |
| `apps.register` | `{hostDeviceId,connectorId,name,port,entryPath?,grants?}` | `{app}` |
| `apps.update` | `{appId,name?,grants?}` | `{app}` |
| `apps.revoke` | `{appId}` | `{app}` |
| `apps.launch` | `{appId,domainId?,path?}` | `{launchUrl,expiresAt}` |
| `apps.saved.get` | `{appId}` | `{revision,entry}` for the authenticated account |
| `apps.saved.list` | `{limit?,cursor?}` | `{revision,entries,nextCursor}` |
| `apps.saved.set` | `{appId,saved,expectedRevision,requestId,domainId?,path?}` | `{requestId,replayed,receipt,current}` |
| `apps.discussion.context` | app and optional exact entry/conversation | initial permitted context, messages and cursors |
| `apps.discussion.history` | app/conversation/exact entry and optional cursor | bounded older messages |
| `apps.discussion.changes` | app/conversation/exact entry and cursor | current message projections including removals |
| `apps.discussion.archives` | app/exact entry and optional cursor | only permitted past conversations |
| `apps.discussion.send` | app/conversation/exact entry, requestId, body, optional replyTo | immutable receipt and separately authorized current message |
| `apps.discussion.remove` | `{appId,conversationId,messageId}` | repeatable own-message or app-owner redaction |

Grants are `{accountIds:[],communityIds:[]}`. Private is the default. Registration is idempotent for the same owner/connector/port and exact normalized configuration; a different configuration on an existing port is rejected instead of silently overwriting another cell.

Apps schema3 adds [named addresses](../../docs/implementation/p3-domains.md) and an [explicit publication policy](../../docs/implementation/p3-publication-contract.md). Claiming an alias does not activate it. The owner selects active aliases and `restricted` or `anyone`; public exposure requires acknowledgement of the entire fixed loopback port and runtime profile. `listed` is independent of launch permission. Canonical origins retain private grants even when a named alias is public. Named production zones must use a registrable site separate from every trusted Soty shell; use the host's `validateNamedAppZone` integration before opening storage. A domain configuration is not proof of DNS ownership or TLS availability.

Schema4 adds [immutable source targets and revision-bound preparation](../../docs/implementation/p3-source-runtime-integration.md); schema5 adds [account-owned saved entries](../../docs/implementation/p3-saved-model.md). A save is a chosen exact address/path, not a grant, community membership, notification subscription or local pin. The global account revision advances for every accepted new desired state; an exact retry returns its historical receipt separately from current state. Removing a personal saved entry needs no current app access. On access loss only the personal title/address snapshot remains, without fresh private metadata. Limits are200 active entries,128 receipts,50 rows and256KiB per page. Missing host authority fence disables saved operations while leaving older Apps operations compatible.

The synchronous fence spans World authority reads and the bounded Apps transaction in Connect→World→Apps order. It must not await, perform network I/O or call mutating World operations. Temporary contention is reported as typed `apps_saved_busy` or `world_authority_busy`; the HTTP adapter returns503 and never retries a signed mutation itself. Repeat the original intent with fresh transport proof and the same requestId. [Integration evidence](../../docs/implementation/p3-saved-integration.md) distinguishes local acceptance from production and real-browser gates.

Schema6 adds [independent app discussions](../../docs/implementation/p3-discussion-contract.md). Policy changes rotate the current audience atomically; private historical conversations never become public with the app. An unopened app allocates no discussion head, and an empty audience generation creates no archive row. Archives require both current entry access and the original audience predicate. World authority is read under the host fence with one bounded relevant-community query; no World chat history or profile is copied. Explicit owner-administrative reads work after closure but cannot launch or post. Own-message redaction needs no current reading permission.

Message request IDs are account-global, with durable fingerprints retained by tombstones. Retry preserves the original conversation, exact address, body and reply; it never moves a message to a newly opened audience. Encrypted fixed-size cursors bind actor/device/entry/conversation and explicitly reset after a service restart. The single-writer pilot admits bounded conversations/messages/live bytes, without silently evicting history. `apps_discussion_busy` maps to503 and `apps_discussion_rate_limited` to429; other semantic errors retain the native signed RPC400 envelope. See the contract for complete shapes, logical quotas and the distinction between backend, browser and production acceptance.

Launch defaults to the canonical address or uses the exact supplied domainId. Tickets are30s, one-use and bound to app, origin, target and policy epoch. Session exchange rechecks current authority after reading its body. Account sessions have an absolute1h deadline; anonymous public requests get renewable30s leases that cannot revive after expiry. Invalid presented session cookies never silently become anonymous. Continuous checks cover HTTP and WebSocket asynchronous boundaries; audit rechecks idle access every10s by default, subject to event-loop delay. Already delivered bytes and upstream side effects cannot be undone by revocation. See [transport acceptance](../../docs/implementation/p3-runtime-transport.md) and [browser entry acceptance](../../docs/implementation/p3-entry-browser.md) for evidence and outstanding release gates.

Boot removes its ticket fragment before network work. POST `/_soty/session` returns a separate30s `sessionCheck`; boot must confirm that value with `X-Soty-Boot-Check` on a credentialed GET of the same endpoint before navigating. An old accepted cookie cannot confirm a newer rejected cookie. Explicit DELETE of that endpoint requires the exact Origin and current public permission, clears presented sessions for that exact app/domain/origin and their live streams, and expires both partitioned and unpartitioned host cookies. It is never an automatic fallback.

Private top-level GET navigation may redirect to the trusted `#launch/<appId>/<domainId>?path=...` shell route. Asset/fetch/HEAD/unsafe/iframe requests retain status responses. The shell issues a fresh ticket for separate opening; it does not reuse an iframe ticket. Local launch paths preserve query and hash-SPA fragments; raw fragments of an initial private runtime URL are not sent to the HTTP server and cannot be preserved by that302. Share closed hash-SPA apps through the encoded shell launch route. Managed status pages use the same neutral palette and hex geometry as the shell, with nonce CSP and no external assets.

Public app data: `{id,name,ownerAccountId,hostDeviceId,state,createdAt,updatedAt}`. Only the owner receives `{connectorId,port,entryPath,grants}`. States are `starting`, `ready`, `stopped`, `offline`, `revoked`. The registry and normalized grants are SQLite-backed and survive restarts. No HTML/HTTP bodies are persisted in the registry.

## Connector integration

```js
const localApps = createLocalAppsRuntime({
  randomSecret: () => randomBytes(32).toString('base64url'),
  digest: value => createHash('sha256').update(value).digest('hex'),
  createWebSocket: url => new globalThis.WebSocket(url),
  httpRequest, // node:http request
  encodeBase64: value => Buffer.from(value).toString('base64'),
  decodeBase64: value => Buffer.from(value, 'base64'),
}, {
  serverUrl: () => relayBaseUrl,
  identity: () => ({linkId,hostDeviceId:deviceId,connectorId,name:deviceNick}),
  token: () => connectorToken,
  blockedPorts: [connectorPort],
});
localApps.start();
```

Expose `await localApps.claim()` only on an explicit local POST action with an exact Soty Origin allowlist. It returns `{ok:true,hostDeviceId,connectorId,claimCode,expiresAt}` only after the remote service acknowledges the digest. The 32-byte random secret is generated at that action, expires in five minutes and is consumed by `apps.claim`. Connecting the runtime does not generate a claim secret. Stop with `localApps.stop()`.

Runtime source has no imports so the current immutable release bundler can embed it. All filesystem/network operations are injected; the runtime never evaluates a command received through the app protocol.

## Origin and compatibility contract

Production requires wildcard DNS/TLS for the configured app origin. Development supports `http://{appId}.localhost:<port>` without changing OS DNS settings in modern Chromium. Missing origin configuration fails closed. The frontend uses an iframe with `sandbox="allow-scripts allow-forms allow-same-origin"` and `referrerpolicy="no-referrer"`.

A one-use launch ticket in a URL fragment is exchanged on the app origin for `__Host-soty_app_session` with `HttpOnly; Secure; SameSite=None; Partitioned`. The browser does not need unrestricted third-party cookies. This uses the [CHIPS standard](https://developer.mozilla.org/en-US/docs/Web/Privacy/Guides/Third-party_cookies/Partitioned_cookies), which partitions the session by application host and top-level site. Local Chromium acceptance verified secure cookies on `.localhost`; production always uses HTTPS. Older browsers without partitioned-cookie support are outside this initial browser compatibility claim.

Supported: HTML/CSS/JS, same-origin forms/fetch, binary assets, HTTP streaming and WebSocket. Browser requests use relative URLs or `location.origin`. Loopback host/port cannot change during a request. External redirects are rejected. Credentials, cookies and arbitrary browser headers are not forwarded upstream; app `Set-Cookie` is intentionally unsupported in v1. Apps requiring their own cookie login, hardcoded localhost URLs, service workers, cross-origin resources or OAuth need a separate explicit compatibility contract.

Limits: 32 streams/connector, with public-basis traffic limited to24 so eight slots remain available to granted users; 48 KiB chunks with one acknowledged chunk in flight per stream; 8 MiB request body; 64 MiB HTTP response; 1 MiB WebSocket message, including fragmented messages; 30-second head/ack wait; two-minute stream idle timeout. Signing in without a grant still uses public capacity. WebSocket compression is not negotiated. Incremental framing checks enforce mask direction, control-frame validity and message size without storing a whole message. Unsafe HTTP methods and all WebSocket upgrades require the exact application Origin; browser cookies and Authorization are never forwarded as application credentials.

## OpenCode proposal

Only a task explicitly requesting a local app should inspect `.soty/app.json` after success:

```js
const proposal = await readLocalAppProposal({realpath,stat,readFile,join,isWithin,httpRequest}, {
  workspace: cwd,
  allowedRoots,
  jobId: job.id,
  blockedPorts: [connectorPort],
  completedAfter: jobStartMillis,
});
```

Manifest shape: `{schema:'soty.local-app.v1',name:'Покупки',port:3000,entryPath:'/'}`. Exact fields, a maximum of 4096 bytes, real allowed workspace, no symlink/junction escape, freshness and a responding service are required. Result adds `sourceJobId`. No shell commands, arbitrary URLs or grants are accepted. A proposal is not published automatically; the user explicitly registers it through the same app control contract.

## Verification

`node --test modules/apps/test/*.test.mjs`

`node modules/apps/examples/browser-qa.mjs 5306` starts an isolated loopback fixture with a genuinely separate sample application and invented account principals. It does not read production data or installed connector credentials. Open `http://localhost:5306` to exercise owner/member/outsider, shared changes and live revocation. Stop with Ctrl+C.

Research and acceptance details: [contract](../../docs/research/apps-contract-20260928.md), [acceptance](../../docs/research/apps-acceptance-20260928.md).
