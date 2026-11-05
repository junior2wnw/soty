# Current application and module discovery

This module adds an owner opt-in **metadata extension** to the existing AppsService. It cannot register an app, change its publication, install code, create credentials or grant app execution. AppsService remains authoritative for app existence, owner, current runtime target, publication, named address and participant ACL.

## Service API and durable state

```js
createAppAgentDirectory({ databasePath, projectId, actorActive,
  withAppAuthority, withPublicAppAuthority, readPublicApps, transport?, now? })
// => {operations, execute, list, get, query, recheckQuery, close}
```

`operations` contains signed Connect owner operations `apps.agentDescriptor.get` and `apps.agentDescriptor.put`. Every operation requires `expectedAccountId` and a real `appId`. Put additionally takes `requestId`, `expectedRevision`, `enabled`, and a `soty.app-agent.v1` descriptor when enabling. A disable can omit the descriptor. The metadata's app ID, source pin and visibility must match the current Apps authority. Target pins name a registered runtime tuple; they do not attest a source-code release or image.

SQLite tables contain only metadata, revisions and idempotency receipts: `directory_identity`, `app_agent_descriptors`, `app_agent_descriptor_receipts`. No tokens, IP/port, host device, connector keys, commands or executable adapters. A failed COMMIT returns no success and rolls back the descriptor and its receipt. Exact retry keeps the historical `receipt`, while the top-level record reflects the **current** enabled/source/revision state. Reusing a request ID with changed input fails. Data survives close/reopen. This is separate storage for one installed module, not another app registry.

`list({actor?,query?,limit?,cursor?})` and `get({actor?,appId})` always inspect current Apps authority. Anonymous reads additionally require the existing public catalog and the new host public authority port (listed + public + current named alias + enabled + exact runtime digest). Source or visibility changes suppress an older opt-in; the owner must approve the new descriptor. A module disable or app revoke suppresses it immediately. Cursors are scoped to account/device/query and pages have a 512KiB budget, leaving room for both JSON-escaped text and structuredContent within Root's2MiB MCP envelope. No global metadata cache or rebuild is needed.

Author descriptions are validated by the existing closed app-contract SDK. References are descriptive; JSON cannot select an arbitrary endpoint, command, credential or Root adapter. Owner-configured callbacks never become trusted code.

## Exact Root wiring seam

Only the composing Root owns the following changes; this module does not modify `http-app.js`, `app.ts` or AppsService.

1. After Connect/Apps/Capabilities exist, create the trusted authority helper and module:

```js
const directoryAuthority = createAppAgentDirectoryAuthority({
  apps: () => apps, connect: () => connect, capabilities: () => capabilities,
});
const appDirectory = createAppAgentDirectory({
  databasePath: path.join(dataDir, 'app-agent-directory', 'metadata.sqlite'),
  projectId: 'soty', ...directoryAuthority,
  // Optional: reviewed host transport below. Omit when none is admitted.
  transport,
});
```

The helper uses the original signed Connect actor through `withActorAuthorityFence` for human owner/participant access. Existing Root **service actors** are mapped only by `capabilities.withOwnerAuthority`, retaining their current credential/grant fence. A service actor may discover a private app only if an existing external contract for that exact app is currently granted and source-authorized. Otherwise it can still discover public metadata through `withPublicAppAuthority`, without owner mapping or private rights. It does not inherit all of its owner's private apps. A read grant does not configure this metadata, and `canManage` is false for service actors. `external.getContract` acquires its own Connect fence: read it before the downstream fence, then recheck that same grant/Source afterward. Do not nest its transaction, manufacture a human actor or add `deviceId` from author JSON.

2. Add `appDirectory` to the signed Connect extensions and close it with other services before closing Apps/Capabilities. Optionally expose `app.locals.appAgentDirectory` to controlled integration tests.

3. Build the live module metadata registry:

```js
const moduleCatalog = createAppModuleCatalog({entries: () => installedModuleDescriptors});
```

Its output is `{schema:'soty.agent-module-catalog.v1', modules, authority}`. Each host-installed entry has closed fields `{id,version,title,description,kind,contract,documentation,readiness,operations,providerKinds}`. Kinds: `service|sdk|provider-contract`; readiness: `available|contract-only`. A universal canvas widget contract is represented as `kind:'provider-contract',providerKinds:['canvas.widget']`; use `contract-only` until an actual admitted provider exists. No app IDs or adapter imports are baked into this registry. The directory's own module is included automatically. The host must register other installed module descriptors; the module does not scan arbitrary local folders or silently claim their readiness.

4. Install HTTP middleware **before** `attachCapabilitiesDiscovery` (which returns404 for other `/agents/*` paths):

```js
const authentication = createExternalCapabilityOperations({service: capabilities, origin: admittedCapabilityAudience});
attachAppAgentDirectory(app, {directory: appDirectory, moduleCatalog,
  origin: admittedDiscoveryOrigin,
  authenticate: req => authentication.authenticate({
    authorization: singleHeader(req, 'authorization'), audience: admittedCapabilityAudience,
  }),
});
```

This reuses existing service credential/OAuth authentication. HTTP is GET/HEAD only; cookies, Host and claimed account IDs do not authenticate. Root `/` receives a `Link` header to `/.well-known/soty-app-platform`. Paths: `/agents/apps`, `/api/app-agent-directory/apps[/appId]`, `/api/app-agent-directory/modules`, docs, a fixed SDK-file whitelist, and two examples. All dynamic responses use `no-store`. No existing OAuth namespace is replaced.

5. Merge `APP_AGENT_DIRECTORY_MCP_TOOLS` or `createAppAgentDirectoryMcpTools(...).tools` into the existing Root `/mcp` tools/list. In tools/call, validate the existing request envelope and tool input, then:

```js
context.current();
const value = await directoryMcp.call({name, args, actor: context.authenticate()});
context.current();
directoryMcp.recheck({name, args, value, actor: context.authenticate()});
return {content:[{type:'text',text:JSON.stringify(value)}],structuredContent:value};
```

Tools: `app_directory_search`, `app_directory_get`, `app_directory_modules`, `app_directory_query`. Post-await recheck is required before sending bytes. It never repeats a query: `recheckQuery` reads the current opted-in descriptor and the existing query-ledger authority. No separate MCP server, OAuth issuer, consent UI or credential vault is introduced.

## Readonly execution through the existing transport

```js
const transport = createAppAgentDirectoryTransport({
  service: () => capabilities, origin: admittedCapabilityAudience,
  resolveBinding({authority, descriptor, capability}) {
    // Reviewed installed HOST CODE returns the current Root reference or null.
    // Match current app/source and the installed binding; never import JSON code.
    return trustedBindings.resolve({authority, descriptor, capability});
  }, appFeedback: appCapabilityGuards,
});
```

The transport verifies same app, approved readonly binding, version, schemas, resources, recipients, current grant and Source. `discoveryReady` means only current metadata. `executionReady` remains false without this independent admission/grant. Calling `query({actor,appId,capabilityId,input,idempotencyKey})` delegates to the **existing** `apps_query` coordinator and its budget, ledger, output validation, Source fences and after-response replay. No network await holds an Apps/Connect/SQLite transaction. The result is discarded if app/source/grant/descriptor opt-in changed during the call. Reusing an idempotency key returns the existing metadata receipt and never repeats the Source read; actual data can be absent on replay by the existing query contract.

This is not arbitrary author MCP proxying. JSON cannot install an app-owned MCP URL or Root binding. Bounded MCP endpoint/origin/protocol/health admission for new app-owned transports remains a separate explicit gate. Named Planner/Perimetrica/Hive grants were not configured by this task.

## Observable requirements and evidence

Run with Node24.22.1 (package engine requires24.15+):

```powershell
& 'C:\Users\Junio\.cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe' --test --test-timeout=12000 modules/app-agent-directory/test/*.test.mjs
& 'C:\Users\Junio\.cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe' --test --test-timeout=30000 modules/app-agent-directory/test/*.test.mjs server/test/app-agent-directory-root.test.mjs
```

| Requirement | Observable criterion | Check |
|---|---|---|
| Dynamic apps, no parallel registry | Two independent real registered app IDs + owner descriptors appear in the same running service; later registration appears without rebuild | `test/service.test.mjs`, first case |
| Explicit opt-in and current ACL | Anonymous excludes private, unlisted link and disabled metadata; scoped member sees only actual Apps access | `service.test.mjs`, first three cases; intentional registry-only and owner-scope negative controls fail the same assertions |
| Current Source and revoke | Same public address with a new immutable target suppresses old metadata; old pin rejected; app revoke removes next read | `service.test.mjs`, source case; intentionally cached static catalog fails the same check |
| Owner CAS/retry/persistence | Changed request ID payload rejected, stale CAS rejected, account/app isolation, source change on late receipt, reopen still stored | `service.test.mjs`, CAS + late replay cases |
| Durable failure | Actual deferred foreign-key error occurs at COMMIT; descriptor and receipt both absent; exact request succeeds after reopen/retry | `service.test.mjs`, COMMIT case |
| Safe SDK/metadata | Unknown executable endpoint/command/adapter fields fail; a clean downloader executes actual app-contract SDK and both examples with random app IDs | `service.test.mjs`, closed JSON; `http.test.mjs`, SDK case |
| Clean external discovery | Root Link→well-known→SDK/modules→public app capability; escaped HTML, GET/HEAD, no cookies/accountId auth | `http.test.mjs`; uses a narrow HTTP directory fixture, not full Root wiring |
| Installed module/provider metadata | Registry changes reflected on next read; `canvas.widget` contract-only stays explicit; executable config rejected | `http.test.mjs`, module case |
| Genuine readonly mechanism | Actual Apps registry + genuine Root grant + existing apps_query coordinator + independent localhost HTTP app gives one Source read; exact replay gives receipt without re-reading | `transport.test.mjs`; a deliberately repeated direct network read is caught by the same dispatch-count assertion |
| Current authorization after await | Source credential/app/grant/owner opt-in revoked before or during HTTP await suppresses result; no unrelated private app leaks | `transport.test.mjs` |
| JSON is not executable admission | Owner changes output schema while retaining approved binding; metadata stays readable but executionReady=false and no network call occurs | `transport.test.mjs`, final case |
| Root MCP response freshness | Changes after call invalidate the previous metadata response; unknown account claim rejected | `http.test.mjs`, MCP case |
| Real Root composition and signed creator flow | Native bootstrap→connector claim→two app registrations→real universal feedback refs→owner opt-in/publication appears without reload; private/unlisted absent | `server/test/app-agent-directory-root.test.mjs`, first two cases; `root-before-ops-fix.log` shows the real old failure |
| Real Root HTTP/MCP transport | Four tools, app-scoped grant and HTTP/MCP audience separation; actual network query, duplicate+restart receipt; Source failure; awaited grant revoke; independent Apps ACL removal after Source restoration | `server/test/app-agent-directory-root.test.mjs`, final case |
| Valid large metadata remains usable |12 valid large descriptors paginate without omissions and the actual MCP text+structuredContent envelope fits2MiB | `service.test.mjs`, final case; original1MiB page fails this criterion in `root-before-pagination-fix.log` |

Current local result: **19/19 pass,0 skip** (16 module +3 actual Root cases), recorded in `root-integration-results.log`. `test-results.log` is the earlier15-case module-only snapshot. The operations were corrected to `apps.agentDescriptor.get/put` to satisfy the existing Connect grammar; no global regex was loosened. No new dependency installed. The network Source is explicitly synthetic and isolated; it is not a production app claim. Local composing Root HTTP/MCP integration is now checked. Real named Planner/Perimetrica/Hive grants, app-owned MCP protocol/health admission, owner-facing opt-in UI, full Source promotion UI and physical Android remain **unverified**.
