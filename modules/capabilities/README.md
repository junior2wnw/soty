# Capabilities: P1 server domain

One server-owned access and invocation ledger for future HTTP/MCP adapters. This module does not implement OAuth, MCP, a public executor, or Notes creation. The default Notes contract is discoverable metadata with execution disabled.

```js
import { createCapabilitiesService } from './server/index.mjs';

const capabilities = createCapabilitiesService({
  databasePath: '/server-data/capabilities/capabilities.sqlite',
  actorActive: actor => connect.isActorActive(actor),
  // Optional trusted server registry; callers cannot register executable tools.
  // catalog: [...],
  // limits: { access: { maxGrantTtlMs: 86400000 }, invocations: { pageSize: 20 } }
});
// Register capabilities as a Connect extension. Close it with the server.
```

`Connect` authenticates the human owner through its existing signed RPC. `capabilities.execute({ op, args, actor })` accepts that trusted host context, rechecks `actorActive`, and requires `args.expectedAccountId` to match the verified owner. Never expose this method to a request-supplied actor object.

Supported Connect operations:

| Operation | Arguments in addition to expectedAccountId | Result |
|---|---|---|
| `access.principals.create` | label, optional clientLabel | principal, client |
| `access.principals.list` | optional limit/cursor | principals, cursor |
| `access.principals.revoke` | principalId | principal |
| `access.grants.issue` | principalId, capabilities, resources, effects, recipients, expiresAt, allowDelegation, maxDepth, budget | grant |
| `access.grants.derive` | parentGrantId, optional principalId, attenuated capability/resource/effect/recipient sets, expiry, delegation bounds | grant |
| `access.grants.list` | optional principalId/limit/cursor | grants, cursor |
| `access.grants.revoke` | grantId | grant |
| `access.credentials.issue` | grantId, audience, optional expiresAt | credential, token returned once |
| `access.credentials.revoke` | credentialId | credential |
| `access.events.list` | optional limit/cursor | content-free events, cursor |
| `access.invocations.list` | optional limit/cursor | owner's minimal invocation history, nextCursor |

Capabilities are exact `{ capabilityId, version }` pairs. P1 resources, effects and recipients are bounded exact string sets; no wildcard matching. Grant expiry is exclusive. `maxDepth` is remaining delegation depth; a child has strictly less than its parent. Derivation in P1 is a signed owner operation; a headless delegation route is a P4 adapter task, not an unauthenticated shortcut.

The free-operation budget is `{ unit: 'invocations', limit: integer }`. Descendants share the root budget. Returned grant budget fields are `unit`, `limit`, `reserved`, `spent`, `remaining`, and `uncertain` (part of reserved). This is not a money or compute hard cap. Unknown paid units are rejected. Unknown effects retain their reservation until reconciliation.

Issue a random scoped credential, then the external adapter uses `authenticateCredential({ token, audience })`. The returned actor is frozen and recognized by an internal provenance map. Copied/JSON objects cannot replace it. Existing actors are revalidated against credential, principal, client, complete grant ancestry and the trusted device on each operation. Revoking the device that created the service or grant invalidates the authority; another active owner device can create a new authorization explicitly.

The database stores the token digest only. Never log issuance responses, authorization headers, request input bodies, or raw SQL rows. The content-free audit contains operation/object/actor IDs and time. Credentials require HTTPS resource identifiers, local-only HTTP for development, or explicit `urn:`/`soty:` identifiers; HTTP adapters must choose their fixed canonical audience rather than trust a caller-supplied one.

Public metadata: `catalog.search({ query, limit, cursor })` returns `{ items, total, cursor }`; `catalog.get({ capabilityId, version })` returns `{ capability }`. Private entries are removed before search/count/pagination and are not returned by get. P1 does not expose personalized private catalogue discovery. A grant is needed for execution regardless of public visibility.

The trusted registry supports a bounded JSON Schema subset with closed objects, required fields, enum, bounded strings/arrays and numeric ranges. Unsupported keywords, remote references and non-native executors are rejected. This is not a general OpenAPI import implementation. Descriptor versions/digests are durably pinned; semantic changes require a new version. Changing the operational `executionEnabled` flag does not rewrite the semantic contract.

`authorize({ actor, capabilityId, version, resources?, effects?, recipients?, input? })` checks current policy. It does not reserve quota or authorize a later asynchronous effect indefinitely. `invocations.admit(...)` performs fresh authorization, budget reservation and durable admission in the same SQLite transaction. No Promise/network work belongs in that transaction.

```js
const { invocation, reused } = capabilities.invocations.admit({
  actor,
  capabilityId: 'example.create', version: 1,
  idempotencyKey: 'client-request-0001',
  input: { text: 'Typed input' }
});
// Only an enabled native test/real handler descriptor admits this operation.
```

External get/list/cancel are scoped to current account + client + principal + grant. A new credential of the same grant can inspect its history; a sibling grant cannot. Owner history is separate and available only through signed Connect. Receipts contain stable IDs/revisions and known effects, not request content or subsequent document bodies.

Dispatch/result/reconciliation methods and `listForOwner` are **internal host APIs**. Never map the whole `invocations` object to public HTTP. Existing connector jobs own execution leases; dispatch intents track handoff only. Public adapters will expose explicitly typed domain actions. Cancellation is a request, not rollback. Unknown dispatch/result outcomes must not be replayed automatically.

The single `DatabaseSync` stores contracts, clients, principals, grants, credentials, audit, budgets, reservations, invocations, dispatch intents and receipts. Connect and Notes keep their own domain databases; cross-database work requires stable IDs and reconciliation. The schema is additive to the application; older incompatible capability readers are rejected.

Run the module tests from the repository root:

```text
node --test modules/capabilities/test/*.test.mjs
node --test server/test/capabilities-connect.test.mjs
```

Before P4: concrete OAuth AS and versioned MCP adapter, two real client compatibility checks, independent direct HTTP route, Notes trusted domain method and durable create reconciliation. Default `notes.createDraft@1` uses `notes:new`, `create`, `soty:notes`; issuing a grant does not enable its handler.
