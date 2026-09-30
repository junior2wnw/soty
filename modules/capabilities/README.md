# Capabilities: access, invocations and public discovery

One server-owned access and invocation ledger, with bounded public discovery through HTML and HTTP. This module does not yet implement OAuth, MCP, a public executor, or Notes creation. The default Notes contract is discoverable metadata with execution disabled.

```js
import { createCapabilitiesService } from './server/index.mjs';

const capabilities = createCapabilitiesService({
  databasePath: '/server-data/capabilities/capabilities.sqlite',
  actorActive: actor => connect.isActorActive(actor),
  // Optional trusted server registry; callers cannot register executable tools.
  // catalog: [...],
  // documentation: [...], // Required matching RU/EN sidecars for a custom public catalog.
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

Public metadata: `catalog.search({ query, limit, cursor })` returns `{ scope:'public', revision, items, total, cursor }`; `catalog.get({ capabilityId, version })` returns `{ scope:'public', capability, documentation, links }`. Arguments are exact: an `actor` or `accountId` is rejected. Private entries are removed before search/count/pagination/revision and are not returned by get. This view has no personalized private discovery. A grant is needed for execution regardless of public visibility.

## Public HTTP and documentation

`/agents` and `/agents/capabilities/:id/versions/:version` provide semantic, escaped server-rendered pages. Search, forms and links work without JavaScript. A small optional same-origin script only acknowledges PWA update preparation for that read-only document; it is never loaded by the editable SPA. A missing acknowledgement still blocks update. Offline API/document navigation cannot return a cached application shell.

| GET path | Response |
|---|---|
| `/api/capabilities/v1/catalog?query=save%20note` | Compact public search with stable bounded pagination |
| `/api/capabilities/v1/catalog/notes.createDraft/versions/1` | Exact capability, RU/EN documentation and links |
| `/api/capabilities/v1/catalog/notes.createDraft/versions/1/contract.json` | Raw canonical semantic contract; hash these UTF-8 bytes |
| `/api/capabilities/v1/catalog/notes.createDraft/versions/1/schemas/input` | Exact pinned input schema (`output` is also available) |
| `/api/capabilities/v1/openapi.json` | OpenAPI 3.1.2 for the implemented read routes |
| `/api/capabilities/v1/status` | `{notesCreateEnabled:false,audience:null}` in this stage |
| `/agents/sitemap.xml` | Public page URLs, only with an explicitly configured canonical origin |

All routes also support HEAD. Known read routes reject other methods with 405 and `Allow: GET, HEAD`; unknown namespace routes return 404 JSON rather than a SPA200. Errors contain only an allowlisted code. Public JSON uses CORS `*` without credentials. Cookies and bearer headers do not expand this public view or enable execution. Success responses use `public,no-cache` and an ETag; errors and readiness use `no-store`.

Configure `SOTY_DISCOVERY_ORIGIN` with an explicit configured Connect shell origin: HTTPS, or loopback HTTP for local development. Validation runs before opening application storage. Host, Origin and forwarded headers never choose canonical URLs or the OpenAPI server. Without this setting relative links still work, but sitemap is unavailable and no absolute canonical is invented. `scripts/dev.mjs` supplies its known frontend origin and proxies `/agents`. Apps Host classification still precedes these routes.

Search is lexical across positive public Russian/English metadata: all normalized query tokens must match. It does not execute instructions in descriptions or promise semantic retrieval. Limits: 128 public versions; query 200 UTF-16 units/800 UTF-8 bytes before normalization and 12 tokens after it; default 10/max 20 items; 4KiB/item and 64KiB/page. Follow the returned cursor even for a short page. A query/docs/readiness change invalidates an old cursor with `cursor_invalid`; begin a new search rather than silently rebasing. Duplicate/unknown query fields and malformed percent/UTF-8 encodings are rejected.

Each public version has an explicit server-owned sidecar tied to its semantic digest. Missing or mismatched documentation fails before capability storage opens; private/unknown sidecars do not affect the public projection. Documentation/readiness revisions are separate from the immutable semantic digest. The `notes.createDraft@1` contract SHA-256 is `95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204`. Hash `contract.json`, not the enclosing detail response or an individual schema. Canonicalization is sorted own keys plus ECMAScript JSON serialization, not a claim of JCS conformance.

The legacy runtime limits string length in UTF-16 code units in addition to its bounded JSON Schema subset. Published standard schemas alone do not describe all runtime restrictions; see `documentation.validation`. A1 adds output validation but does not change existing input rules. Legacy lone-surrogate acceptance is documented and must be resolved at the external write boundary before enabling execution. All new public metadata/query strings must be well formed Unicode. There is no automatic schema fetching, package installation, Notes-body lookup or discovery authorization through a grant.

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

Remaining P4 gates: native create-only Notes and crash-safe reconciliation, authorized write/receipt HTTP, OAuth AS and versioned MCP adapter, two real client compatibility checks and external HTTPS release. Default `notes.createDraft@1` uses `notes:new`, `create`, `soty:notes`; issuing a grant does not enable its handler. Public crawling/HTML and an OpenAPI document do not guarantee indexing or installation in another AI product.
