# Authorized application reads

This source change adds `apps_query` to the same **authorized** catalog as
`apps_invoke` and pinned application guidance. It is an optional trusted host
composition. It does not install an app, accept a new author manifest, enable a
Source key, change a human login, or widen the existing Notes-only agent OAuth
connection/delegation profile. No production configuration or deployment is part
of this receipt.

## Contracts and trusted composition

| Port | Admission and result |
| --- | --- |
| `apps_catalog_search` / `apps_catalog_get` | Existing wire; current grant, Root device, App participant/target, and Source authority. Registered reads have `effects: []`. Availability remains `unprobed`. |
| `apps_query` / `POST /api/capabilities/v1/app-actions/query` | Exact `{reference, idempotencyKey, input}`; no actor, endpoint, key, command, or module field. Returns `soty.authorized-app-query.v1`. |
| `apps_invocation_get` / `apps_invocation_cancel` | Existing receipt wire. Reads never return retained query contents. Cancellation cannot refund an already charged dispatch. |
| `apps_guidance_list` / `apps_guidance_get` | Same current authority and immutable content-addressed references. Skill/document content remains application data. |

`createTrustedReadonlyQueryAdapter({withAuthority, query})` explicitly admits
**host-reviewed code**. An HTTP JSON object or a `readOnlyHint` annotation cannot
mint that in-process adapter. This is a code trust boundary, not cryptographic
attestation that arbitrary host code has no side effects. A handler must
independently use a read-only Source port/credential. The legacy effect adapter
profile remains separate and requires nonempty effects.

`createCapabilitiesService({readonlyQueries, readonlyQueryLimits})` accepts exact
registered `{capabilityId, version, digest}` bindings. `createHttpApp` composes
them through its existing private `externalApplications` configuration: immutable
Root app target + Source binding, current Connect fence, active original grant
creator, App participant ACL, then the synchronous Source authority port. An
app target or Source binding change requires a new semantic contract version.
Nothing in a request chooses this composition.

The query handler receives a frozen `{requestId, input, authorization}`, an
`AbortSignal`, and a private `currentAuthority()` callback. The callback verifies
the original actor, current Root/App/Source authority and current running query
status. The admitted Source handler calls it before and after each asynchronous
boundary. It cannot be used after completion, cancellation, close or timeout to
start another read. These private closures and authorization snapshots are not
HTTP/MCP payload fields.

## Budget, idempotency and privacy

The existing Capabilities SQLite ledger is used unchanged: **no new database,
table, column, schema version or migration**. The original input is captured and
checked against the exact catalog schema before a private marker is written.
The internal row retains only `{schema:'soty.read-query-intent.v1', inputDigest}`,
the existing exact request digest, and authorization/target metadata. The input
digest is private database metadata, not a returned audit field. Query text and
result contents are never stored in the invocation, receipt or query cache.

Admission reserves one shared root-grant `invocations` unit. One transaction
performs the pending-to-dispatching CAS **and spends that unit before network
delivery**. A crash immediately after COMMIT is still charged. The receipt does
not promise that a Source request was actually delivered. Pending cancellation
CAS and dispatch CAS are mutually exclusive across processes; cancellation before
dispatch releases its reservation, cancellation afterwards retains the charge.

An exact repeated key returns current-authorized metadata only, including
`reused:true`, `resultUnavailable:true` and the durable charge. It never repeats
a Source read or recovers a lost result. A changed input/reference under the same
key is a conflict. A fresh read requires a new key and remaining approved budget;
it is not an automatic recovery step. `apps_invoke` and generic dispatch,
settlement, job binding and effect-recovery APIs cannot consume a query marker.

Only a successful live call returns `result`, as `authority:'application-data'`.
Input/output JSON schemas, bounded canonical payloads and the final current
authority check precede its response. Root/App/Source revoke, invalid output,
timeout or cancellation suppress private contents. A valid read receipt proves
the handler's successful read/validation at that time; it is not a retained
snapshot or a promise of future object existence.

Limits are explicit: up to 64 total effect/read contracts; canonical query input
and output up to 64 KiB, 4,096 nodes, depth 18, arrays 128; at most 4 actual pending
Source promises **per process**, timeout at most 8 seconds. Ignored aborts retain
their slots until the actual promises settle, including after close. Late output
is discarded and never enters a response or ledger. Existing persistent limits
remain 4 active invocations per principal, 16 per account, 128 globally,
10 admissions/minute/principal, and 100,000 total retained invocations. There is
no history eviction or automatic Source retry. A process crash can retain an
unfinished metadata row/reservation until an authorized cancellation; these hard
limits fail closed rather than pretending that orphan work was recovered.

## Actual Planner adapter

`createPlannerReadonlyQueryAdapter` pins a single selected workspace, Source
actor, read-only key ID, environment/resource tuple, approved origin, tool
schemas, protocol and host-reviewed Source release metadata. The release hash is
a configuration pin, **not code attestation**. Credential resolution and
destination checking are trusted host closures; no bearer is returned to an
agent. Source requests happen outside SQLite authority fences.

The admitted code calls only the fixed authenticated profile, tools,
`planner_help` and `planner_read` with `collection:'objects'`. It cannot call
`planner_apply`, choose another collection/workspace, or follow redirects. It
checks current Source actor/key/workspace/read-only claims before and after the
read and checks current Root/App/Source authority around every await. The existing
work-item receipt profile endpoint is used **only for its authenticated scope
projection**; a read does not claim its write-effect proof.

The reviewed `PlannerAgentService.read` handler reads domain state and projects
objects without changing objects, revision or `agent_requests`. Source bearer
authentication does update the key's operational `last_used_at`; "read-only"
here refers to application domain data, not a claim of zero metadata writes.
Only bounded object fields `{id,title,version,kind,status}` and resource/revision/
pagination metadata reach the caller. The full Source response is not forwarded.

## Reproducible verification

Run after producing `dist` for tests that load actual consent documents:

```text
node node_modules/vite/bin/vite.js build
node --test modules/capabilities/test/readonly-queries.test.mjs modules/capabilities/test/readonly-ledger-process.test.mjs modules/capabilities/test/external-guidance.test.mjs
node --test modules/capabilities/test/readonly-app-http.test.mjs
node --test --test-concurrency=2 modules/capabilities/test/*.test.mjs server/test/capabilities-*.test.mjs scripts/executor-policy.test.mjs
```

The actual Source gate uses the installed reviewed Planner source at
`D:/соты/output/planner-universal-integration/worktree`, or the explicit
`SOTY_PLANNER_PROOF_SOURCE_ROOT`. It creates fresh synthetic Root and Planner
stores and real loopback services, uses signed Connect App registration and
owner-issued grants, and sends actual HTTP/MCP requests. Acceptance requires this
gate to run with **no Source skips**. An absent explicitly configured Source path
fails before tests; a skipped optional default Source test is not evidence of an
integration.

The separate two-OS-process test deliberately uses a synthetic host actor fence
with the real Capabilities ACL, budget and private SQLite ledger port. It proves
cancel/dispatch serialization, crash-before-delivery charging and metadata-only
replay, not Planner behavior. The signed HTTP/MCP test independently proves
actual Planner read/replay, Root grant/App revocation after a Source response,
Source key revocation, and writer-key rejection while comparing domain objects,
revision and `agent_requests` before/after.

The direct SDK catalog now rechecks every returned contract's Source authority
before returning a page. Its regression uses real Capabilities grants with a
controlled Source callback that withdraws a previous entry; transport-level
rechecking alone did not protect this direct SDK path.

Legacy OAuth document failures on an unbuilt checkout reproduced on untouched
`0d40110f350e4aada255bbe98928c2729a9a5feb`: missing `dist/index.html` caused the
unchanged consent `sendFile` to fail and return HTTP400. The same archived source
with a built `dist` passed the five native-flow tests. No OAuth permission, token
lifetime or fixture assertion was changed to make that check pass.

Validated on Node `24.19.0` with installed `oidc-provider 9.12.2`: the final
Capabilities/HTTP/MCP/executor regression run passed **457/457**, with zero
failures or skips. Focused coordinator + two-process + guidance tests passed
**18/18**, and the actual signed Root/App/Planner HTTP/MCP gate passed **4/4**
(the writer case checks both an honest lease and a falsely read-only host lease
against the real Source claim). TypeScript typecheck and Vite build passed. These
are local source/fixture checks; they do not admit a production Source binding
or prove an image containing this change has been built or deployed.
