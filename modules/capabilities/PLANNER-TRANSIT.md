# Source adapters: Planner and Transit

These are trusted host ports, not public registration endpoints or a second
Capabilities ledger. Their metadata creates no grant. Root admission, invocation
budget, stable internal request IDs, fencing and reconciliation remain in the
existing coordinator. Network/file operations must run outside Apps/Connect
SQLite transactions.

## Planner — one application, one selected workspace

The runtime module is `server/planner-adapter.mjs`. It speaks the real Planner
HTTP agent API (`GET /api/agent/tools`, `POST /api/agent/call`) shared with its MCP
transport. It imports no Planner source code or dependency at runtime.

The host selects a verified source and resource, then supplies these private
ports: `authorize`, `accountId`, `resolveCredential`, `assertDestination`.
`resolveCredential` returns a current actor-bound, single-workspace source key
with expiry. Origin, source actor, workspace and keys never come from tool
arguments or the app descriptor. The approved source's exact tools schema digest
is checked before effects. Separate bounded provider-wire validation preserves
the smaller U1 descriptor limits.

An author can use the existing `createAuthorDraft({title})` after source ownership
and the current selected workspace are verified. Platform-owned profiles provide
IDs, source pins, own-login policy and private feedback automatically. The first
source permission still needs explicit consent; a title cannot manufacture it.
The agent then discovers the app/resource, reads objects and creates one work
item in that resource. `createWorkItem` forwards the exact stable `requestId` and
title. An undated item is a Planner `note` with null start/end, not a schedule or
running job. A lost ACK remains an unknown outcome and retries only the identical
intent with fresh host permission and source ACL.

Methods: `discover(context)`, `readObjects(context,{query?,limit?,cursor?})`,
`createWorkItem(context,{requestId,title})`. The source key and its current
workspace membership are checked by the real source on every request. The host
checks the captured Soty actor before and after requests; a post-commit authority
change remains unknown. No model request or scheduler is started by this module.

Run the actual source acceptance fixture on Node 24.19:

    node --test modules/capabilities/test/planner-adapter.test.mjs

It uses an independently owned Planner checkout via `SOTY_PLANNER_SOURCE_ROOT`
(on this controller, `D:/планировщик`), its own installed TSX API and temporary
SQLite. Scheduler is disabled. Source files and normal queues are untouched.
Five source/HTTP cases prove durable receipt replay across restart, lost ACK,
foreign scope denial, source key/membership revocation, schema/actor fences and
the post-commit authority race. The host authorization callback is an explicit
fixture port, not a claim that full signed Soty dispatch is already wired.
Without the optional source checkout the integration cases are visibly skipped;
an explicitly configured but missing checkout fails.

### Source-owned UI and durable recovery

Planner 1.2.0 currently sends `X-Frame-Options: DENY`. Its local-owner mode also
sees all of that local user's workspaces. A Soty app grant must not become that
source-wide owner permission. Embedding requires a source-owned, selected-
workspace mode with an exact approved parent/origin, its own HttpOnly session,
verified current Soty issuer/subject and current source roles. Existing source
accounts are linked only by two-sided proof, never names/email. Own login remains
available. Header stripping or trusting `X-Soty-Subject` is not an integration.

The maintained HIVE `openid-client` protocol port can be reused for human login;
it supplies no agent or workspace grant. A remote Root server cannot dial a
user's loopback; a connector-owned broker must resolve the approved device/port
and keep the key private. Until these gates pass the UI is external-only and
agent availability is conditional on the actual source key and transport.

The original source has durable `agent_requests` but no read-only receipt lookup.
The preserved original is still unchanged. A separate curated Git worktree at
`D:/соты/output/planner-universal-integration/worktree` now implements source-owned
selected-workspace BFF and `planner.work-item-proof.v1`; provenance of the code-only
snapshot is next to it. See that application's `SOTY.md` for configuration and
boundaries. These local gates do not install the source on a user's device.

`server/planner-effect-adapter.mjs` bridges the real source protocol to the existing
Capabilities external delivery coordinator. It returns `{binding,adapter}`. The
closed binding digest pins origin, resource, original source scope, approved tools
and proof schemas and source release reference; no URL/command/key enters the
manifest. A changed binding needs a new Core capability contract version. Core and
U1 semantic pins are different contracts and are not substituted for one another.

Execution checks the real source actor/key scope, selected workspace and provider
schema. After discovery awaits it re-enters the synchronous host authority fence
immediately before mutation send; fetch runs after that fence. Root's host wrapper
must recheck the original credential/deadline and current app/resource permission.
Per-call source ACL and this local fence do not promise a distributed atomic revoke.
A committed effect followed by a lost ACK/revoke can remain unknown until a current,
source-authorized proof read confirms it.

Recovery never calls execute. The source atomically binds exact Root input digest,
full provider input digest, original key scope and workspace to its real receipt.
Old rows without evidence, mismatched replies, outages and revoked keys remain
unknown. A definitive source absence does not release an already-started Root
reservation: another source request might still be in flight.

Actual separate-source gates (Node 24.19):

    node --test modules/capabilities/test/planner-effect-adapter.test.mjs modules/capabilities/test/planner-embed-http.test.mjs

Set `SOTY_PLANNER_PROOF_SOURCE_ROOT` to the packaged source checkout containing the
new protocol and its own dependencies. A missing optional checkout is a visible
skip; an explicitly configured missing source fails. The gate uses real Caps
grants/budget/SQLite plus actual source HTTP and real Root signed Connect/OIDC. The
loopback browser fixture separately proves native consent, the real timeline,
source restart, profile switch, focus and 390px bounds with synthetic records.
No live app, operational queue, financial endpoint or paid model is called.

## Transit — local source reading only

`server/transit-read-adapter.mjs` reads only approved SHA256 pins for `README.md`,
`AGENTS.md` and `docs/READINESS.md`, with bounded line windows. The root and device
binding are host-owned. Current actor permission and online device status are
checked before and after reading. A changed pin returns no text. Text is source
content, not instructions to the executor. No config, `.transit`, account API,
CLI, network role, receipt settlement, payment or process command is reachable.

`D:/roy` is the actual Transit project. Its current readiness document retains
NO-GO for sales and real payouts. Directory presence and this adapter do not
raise runtime or business readiness. The existing Astro Portal has its own
WebAuthn/account/pilot API; that sensitive API is not used by this source-read
profile. Source files remain read-only and existing Rust/client work is preserved.

    node --test modules/capabilities/test/transit-read-adapter.test.mjs

The test includes an actual pinned read of Transit documentation when its optional
checkout is present and reports a visible skip otherwise. No real operational
configuration or user records are read.
