# Soty local applications

An independent web project on a claimed connector becomes a persistent app cell. Browser content uses an isolated per-app origin; the gateway forwards HTTP and WebSocket through the connector's outgoing authenticated channel to a fixed loopback port. Neither connector credentials nor Soty account storage enter the app.

## Host integration

```js
const apps = createAppsService({
  dataDir,
  appOriginTemplate: 'https://{appId}.apps.example.org',
  shellOrigins: ['https://example.org'],
  actorActive: actor => connect.isActiveActor(actor),
  canAccessCommunity: world.canAccessCommunity,
  isGroupAdmin: world.isGroupAdmin,
  activeCommunityIds: world.activeCommunityIds,
  subscribeMembership: world.subscribeMembership,
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
| `apps.launch` | `{appId}` | `{launchUrl,expiresAt}` |

Grants are `{accountIds:[],communityIds:[]}`. Private is the default. Registration is idempotent for the same owner/connector/port and exact normalized configuration; a different configuration on an existing port is rejected instead of silently overwriting another cell.

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

Limits: 32 streams/connector; 48 KiB chunks with one acknowledged chunk in flight per stream; 8 MiB request body; 64 MiB HTTP response; 1 MiB WebSocket message, including fragmented messages; 30-second head/ack wait; two-minute stream idle timeout. WebSocket compression is not negotiated. Incremental framing checks enforce mask direction, control-frame validity and message size without storing a whole message.

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
