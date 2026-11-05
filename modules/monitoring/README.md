# Общий мониторинг, аналитика и управление · v1

Один переносимый SQLite-сервис и browser SDK для `{kind:'platform'}` и `{kind:'app',appId}`. Метрики транспорта не доказывают успех бизнес-сценария. Описание модуля не выдаёт прав. Приложения/личности/Source остаются в существующих реестрах; модуль хранит только собственные числовые измерения, настройки, минимальные receipts/audit.

## Host composition

```js
const monitoring = createMonitoringModule({
  dataDir,
  storageMode: 'reader', // DEFAULT. Explicit writer only after reader/rollback admission.
  connect: () => connect,
  apps: () => apps,
  capabilities: () => capabilities,
  publication: request => (universal?.appsExtension ?? apps).execute(request),
  platformPolicy: {
    readAccountIds: [], configureAccountIds: [], adminAccountIds: [],
  },
  onChange: ({scope}) => { /* content-free hint to current authorized UI */ },
});
```

Import `createMonitoringModule` from `server/monitoring-module.js` in this host, or use standalone `createMonitoringService` from `server/index.mjs` with trusted `currentActor` and synchronous `withAuthority({actor,scope,right},callback)` ports. Preserve the **original signed actor**: serialized fields carry no Connect authority. `withAuthority` must keep existing identity/App ownership current through the module COMMIT. It may return/throw only synchronously and invoke the callback once. App owner is checked with current Apps authority; an app/Page/placement launch grant does not grant app-wide monitoring.

Platform policy is explicitly configured by the operator; it is not inferred from invitation admins, hostname, a project label or client JSON. Read/configure/admin are distinct; explicit custom observations use `record` authority. A deployment needs a legitimate configured operator, not a synthetic test account. Existing canonical invitation administration has its own additional rights, retained independently.

The module creates **no extra Connect extension slot**: replace the existing capabilities entry with `monitoring.composeExtension(capabilities)`, which snapshots its existing operations and dispatches fixed monitoring operations separately. This does not replace Capabilities or disable other modules. Add `monitoring.capabilities.catalog` / `.queries` / `.effects` to the existing Capabilities composition; merge `.beforeCall` and `.recheck` into the current Root external guard **without replacing its existing feedback/Source/referral guards**. Recheck before returning/exporting a response after awaits. `close()` closes only this module; Root owns its lifecycle.

`MONITORING_MODULE` is exported by `server/app-agent-directory-modules.js`; include it in the installed registry. Serve this file as `/agents/monitoring.md`, plus `sdk.mjs`, `sdk.d.mts`, `manifest.json` and the portable example via the existing fixed `/agents` asset map. No new OAuth/MCP/registry or author-controlled executable endpoint.

## Reader before writer

Canonical file: `DATA/monitoring/monitoring.sqlite`. Default `mode:'reader'`: absent DB means `storage.installed:false`, no directory/file initialization; existing DB opens read-only and is checked against exact schema/identity. Module writes are unavailable, actions report disabled, and host observation returns `reader-only`. SQLite may use its normal WAL coordination alongside an already running writer; no logical rows/schema/config are written by reader mode.

`mode:'writer'` explicitly initializes a truly empty DB; logical collection is **on by default** for every scope. A saved `enabled:false` stays false across repeat registration/reopen and prevents new samples. Disabled history remains historical data. Mandatory security audit elsewhere is never switched off.

Buffered collectors call host-only `collectionEnabled(scope)` **before enqueue**, not only at flush. Optional synchronous `beforeCollectionChange(scope)` flushes that scope's eligible pending observations before OFF/ON. It runs after a fresh readonly rights/receipt/CAS check, outside module/Apps transactions; the actual write rechecks current rights/CAS. Hook failure blocks configuration acknowledgement. This preserves pre-OFF observations while events captured during OFF cannot reappear after a fast ON. Reader mode returns false. The host owns its bounded buffer; the module adds no second buffer/registry/history schema.

Release contract exports: `server/schema.mjs` — `MONITORING_DDL`, `MONITORING_FORMAT=1`, `MONITORING_LINEAGE='soty.monitoring.sqlite.v1'`, `inspectMonitoringStorage`, `monitoringSchemaFingerprint`. Metadata is one exact row `{id:1,project_id, lineage,registry_id:UUID}`; `PRAGMA user_version=1`. Unknown version/extra DDL/identity mismatch fail closed. No Core/Native migration is introduced. The external deployment probe must pin the DDL independently, add this family to exact image readers/snapshot and prove rollback. This module alone does not supply that release gate.

Default bounds: 30 days; 32,768 minute bucket rows globally; 1,024 scopes; 32 custom definitions per scope; 16,384 dedup receipts; 4,096 safe audit rows; 32MiB SQLite page budget. Valid receipts are not silently evicted to admit new commands; exhaustion fails. Old samples/receipts are pruned after retention; row capacity prunes oldest buckets, leaving observable missing ranges. WAL pressure also stops writes; file/WAL failure cannot acknowledge a successful config/sample. Limits may be reduced by trusted host configuration. They are not claims of infinite history, exactly-once telemetry delivery or a global disk quota.

## Human/browser SDK

```js
import {createMonitoringClient} from './sdk.mjs';
const monitoring = createMonitoringClient({
  request: (op,args) => authorizedTransport.request(op,args),
  currentAccountId: () => currentIdentity.accountId,
  isCurrent: () => thisMountStillOwnsItsAccountAndGeneration(),
  scope: {kind:'app',appId:currentAllowedApp.id},
});
const description = await monitoring.describe();
const history = await monitoring.query({metricIds:['http.requests'],from,to,maxPoints:120});
await monitoring.setEnabled({requestId:stableCommandId,expectedRevision:description.config.revision,enabled:false});
```

`request` is an already authenticated transport supplied by the consumer. SDK never extracts/transfers Root keys or authenticates with an appId. A Source may use its own authorized backend transport or the existing scoped agent flow; it cannot borrow the parent Root identity. No hardcoded application IDs. Types are in `sdk.d.mts`.

SDK fences before/after responses and writes a metadata-only account/scope journal **before** configuration/admin dispatch. Browser `localStorage` is the default; an equivalent `getItem/setItem/removeItem` adapter can be injected. Missing/quota-failed journal prevents dispatch. Lost ACK: `pending()` → `retryPending()` keeps the same original requestId/CAS payload. Known conflict remains until explicit fresh read and `discardPending(exactId)`; no implicit rebase/new ID. Mount/account-generation ownership is the caller's `isCurrent` contract, including ABA. No credential/password/body in this journal. Configuration and administration intents have separate slots.

## Operations

Every operation requires `expectedAccountId` and exact `scope` through the existing signed transport.

| Operation | Right | Additional args |
|---|---|---|
| `monitoring.describe` | read | none; config/permissions/storage/definitions/latest values/installed action metadata |
| `monitoring.query` | read | `metricIds` (1–4), `from`, `to` (epoch ms), `maxPoints?` (1–120) |
| `monitoring.config.put` | configure | stable `requestId`, `expectedRevision`, `enabled` |
| `monitoring.metric.put` | configure | stable `requestId`, `id:'custom.*'`, `kind`, `unit` |
| `monitoring.metric.record` | record | stable `eventId`, declared `metricId`, numeric `value`, `observedAt` |
| `monitoring.audit.list` | admin | optional monotonic `cursor`, `limit` (1–100) |
| `monitoring.admin.invoke` | admin + canonical provider | `actionId`, stable `requestId`, `expectedRevision`, typed `input`, explicit `retry?` |

Unknown fields/types, non-finite/negative numbers, mismatched units, future >30s/expired observations, invalid scope, wrong event payload or stale CAS are rejected. Custom kind/unit are immutable. Counts/bytes are safe integers; percent 0–100, ratio 0–1, boolean 0/1. A custom observation cannot impersonate `http.*`/other host metrics. IDs/field sets are validated before writing; no arbitrary labels, URLs, actor selectors, commands, body/header/query, text messages or passwords are accepted as automatic telemetry.

Host-only `observe({scope,metricId,value,observedAt?,eventId?})` accepts registered host definitions. Collectors supply actual authorized app scope from the host admission/route, not request JSON. Do not expose this method as RPC/MCP. Catch optional observer failures after the primary business operation; report collection unavailability separately, never roll back an already committed Source or pretend a dropped sample is durable. Providing an eventId deduplicates an observation within retained receipts. Reliable producers retain their own unacknowledged events; no automatic payload capture/replay.

High-rate collectors use host-only `observeBatch({observations:[{scope,metricId,samples,sum,min,max,last,observedAt}]})`, 1–4096 aggregate entries, one transaction/prune. Every entry describes one metric/one minute; buffer keys must include the minute as well as scope/metric, with observedAt the last sample's timestamp. Sum/sample counts/min/max are validated before writing; one invalid/capacity-failed entry cannot partially commit the others. Uneven distributions merge **sum and samples**, gauges choose newest timestamps. Result `{observations,recorded,disabled}` counts accepted/skipped aggregate entries. It is not another RPC or new storage schema; scalar observe uses the same merge.

Optional trusted `collectorStatus(scope)` adds `describe.collector:{running,lastSuccessAt,lastFailureAt,failures,supportedMetricIds}`. It contains only bounded status/codes, never exceptions/URLs. Reader mode cannot claim running. Listed host metric definitions without an installed collector remain no-data; supportedMetricIds distinguishes actual collector support from a reserved definition. Root owns its polling/buffering/start/stop and content-free online hints.

`counter` values are increments; query sums them. `gauge` uses the latest timestamp, including out-of-order observations. `distribution` returns the weighted arithmetic mean, not p95 or an average of quantiles. Minute buckets round the returned actual range outward; responses declare that range. Coarser points aggregate without more than 120 points. `null` means no accepted observations in that bin; an explicitly observed zero remains zero. Coverage counts bins with actual observations, not a guarantee of uninterrupted sampling. `lastAt`, sample counts, oldest retained bucket, stale/disabled/unavailable/no-data/partial are explicit. No business revenue/visitors/RTC quality is inferred from transport counts.

Explicit business instrumentation examples: `examples/instrumentation.mjs` has independent Counter increments and Reading List additions using the same SDK. Call only after the **Source's own successful COMMIT**, with stable event IDs. This example is not a proof that any particular live Source has installed it.

## Actual administration and extensions

Installed default canonical providers:

- `platform.admission`: invitation-only/default quota through host-only `connect.platformAdmission`; it still requires canonical invitation administration **and** separately configured monitoring admin. A service grant is not an original signed Connect actor; it cannot impersonate one for this port.
- `app.listing`: list/unlist an **already public** current app through `universal.appsExtension` update/receipt. It preserves the target/domain/Source pin and feedback reconciliation. It never publishes a private port on the owner's behalf or invents exposure consent. Private/unsupported apps show the action unavailable; normal canonical app settings remain separate.

Trusted new provider contract is `{id,label,scope,fields,read,prepare,execute,receipt}`. Fields are bounded boolean/integer/enum. `read` returns current revision/scalar settings. `prepare` creates the exact canonical metadata command; module commits it before calling `execute`. `execute/receipt` receive that **same saved intent** across Source/policy change/restart, not freshly rebuilt arguments. Canonical operations still enforce original actor/current rights/target and their own CAS/idempotency. These code adapters cannot be installed via author JSON.

Successful admin response requires a fresh durable canonical receipt, followed by module audit COMMIT; a callback returning “success” alone is insufficient. Unknown response stays pending; ordinary retry reads receipts only. `retry:true`/SDK `retryAction({executeIfMissing:true})` sends again only if the canonical provider proves absence under the same command contract. Missing pruned receipt plus advanced domain revision is **unknown**, not proof of no effect. No automatic new request ID or retarget. Admin calls run outside the Apps/module prepare/finalize transaction, maintaining existing lock order; this is a recoverable two-store intent, not a false distributed atomic COMMIT. SDK `pendingAction/retryAction/discardAction` preserve this workflow.

## External agents

Use the existing Root authenticated `apps_catalog_search/get`, `apps_query`, `apps_invoke`/invocation receipt tools, not a new MCP. Each capability ID is the corresponding operation name/version1. Input envelope: `{scope,paramsJson}`; paramsJson is the bounded operation-specific JSON above without expectedAccountId/requestId/eventId. Existing invocation idempotency supplies a stable domain ID. Grant requires static `monitoring`, exact `monitoring:platform` or `monitoring:app:<actual-app-id>`, matching effects/recipient `soty:monitoring`, and current subject policy. Read/configure/admin/record capabilities are distinct. Wrong resource or revoked credential is denied.

Readonly result envelope `{schema:'soty.monitoring.agent-data.v1',dataJson}` contains the **same service DTO**, including null gaps. It accommodates the maintained Capabilities schema subset without weakening it to accept arbitrary JSON/nullable unions. Mutation returns the existing domain-proof envelope; effect/artifact type/id/revision must match. Human transport remains typed JSON, not an agent token workaround.

Tests under `test/` cover real SQLite, genuine signed Connect/Apps/World/Universal/Capabilities, canonical management and grant rejection. Isolated malformed/broken controls are run by `test/negative-controls.mjs <new-output-directory>`. Synthetic tests do not prove live Root UI, charts, two live Source consumers, release/rollback, production/physical Android or all remaining Soty requirements.

## uPlot: bundled third-party notice

The chart UI includes the unmodified uPlot 1.6.32 distribution. Its runtime header credits Copyright (c) 2025, Leon Sorokin. The complete license distributed with that release is retained below and in `vendor/uplot/LICENSE`; this README is also served at `/agents/monitoring.md` in the runtime image.

```text
The MIT License (MIT)

Copyright (c) 2022 Leon Sorokin

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
```
