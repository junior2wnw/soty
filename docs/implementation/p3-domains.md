# P3-A: durable app names

Workspace: `soty-platform`, based on the P2 checkpoint. Production publication, DNS changes and named runtime launch are not included in this stage.

## A1: migration and registry

The existing Apps SQLite database remains the source of truth. New tables are `app_domain_zones`, `app_domain_heads`, `app_domains` and `app_domain_receipts`. No second registry, identity store, access policy or queue is introduced.

`migrateAppsSchema(db, {legacyTemplate, now})` recognizes an empty database or the known v1/v2 marker, table/column/primary-key/type layout and `user_version` before persistent changes. It rechecks after `BEGIN IMMEDIATE`. An unsupported schema is not initialized opportunistically. Version 2 writes `soty.apps-registry.v2` and `user_version=2` atomically with its tables and canonical bindings. A failed migration rolls back both the schema and grant-index repair. Apps are iterated without loading the full registry into memory.

Recognition includes the full known table/index DDL: UNIQUE (including the canonical partial index), foreign keys, NOT NULL and CHECK definitions. Only cosmetic SQL differences outside string literals and `IF NOT EXISTS` are normalized. This deliberately does not accept arbitrary semantically equivalent hand-edited schemas. Missing or altered constraints, unknown tables, views and triggers require explicit repair/migration instead of being silently treated as known v2.

The migration preserves every existing app ID, owner, source, state, ACL JSON and app revision. In v1, `grants_json` was authoritative; the normalized lookup table is rebuilt exactly from that source, removing stale rows. This does not create public/unlisted access or a catalog listing. The independent domain revision starts at zero and changes only when an alias is claimed or retired.

V1 does not contain historical origin metadata. The operator must confirm the existing deployment's legacy template before migration. That template is normalized to equivalent browser origins and pinned; subsequent drift fails with `apps_origin_template_changed`. An empty template is pinned as empty and produces `canonicalOrigin: null`. Existing canonical origins are immutable in this stage. Claiming an alias does not change launch URLs, cookies, IndexedDB, local storage or the app's access revision.

`createAppsService` retains its existing constructor and signed operations and adds:

- `namedAppZone: ''`: exact base origin of an approved named zone; empty disables new claims. The first nonempty zone is pinned. Empty can disable new claims, but a different nonempty zone requires a future explicit migration.
- `domainLimits: {perApp: 3, perAccount: 100}`: configurable pilot lifetime alias limits; retired names still consume both limits. Canonical random addresses do not consume the alias budget.

Before opening this database, root integration validates the named zone's registrable-site boundary with the pinned PSL parser. The module additionally validates URL syntax, hostname length, HTTPS (localhost-only HTTP development), and direct trusted-origin conflicts. This registry alone is not evidence of browser/site isolation.

`validateNamedZone(zoneOrigin)` is a synchronous constructor hook for that deployment policy. After read-only schema recognition and before migration/WAL changes, every persisted named zone is checked again against the current trusted shell origins, even when new claims are disabled. The module itself rejects any trusted shell host inside a named namespace; root's hook adds registrable-site checks. The guard repeats inside the registry startup transaction before a new zone can be recorded. A trusted-shell routing exception must never override a retained named namespace.

Signed operations use the existing current Connect actor and retain the P2 `expectedAccountId` guard:

| Operation | Input | Result |
|---|---|---|
| `apps.domains.get` | `appId` | `schema`, domain `revision`, immutable `canonicalOrigin`, bounded domain list and alias budget counts; owner only |
| `apps.names.check` | `slug` | Normalized slug, advisory availability and a generic disabled/reserved/unavailable reason; no owner metadata |
| `apps.domains.claim` | `appId`, `slug`, `requestId`, `expectedDomainsRevision` | Immutable mutation `receipt` and `replayed` |
| `apps.domains.retire` | `appId`, `domainId`, `requestId`, `expectedDomainsRevision` | Immutable mutation `receipt` and `replayed`; aliases only |

Names use 3–48 ASCII letters/digits/internal hyphens and lowercase normalization. System names, `app-*` and `xn--*` are reserved in one module. Availability is not a reservation. Claim rechecks the current actor and owner, app state, domain revision, name uniqueness and both quotas inside one `BEGIN IMMEDIATE` transaction. Alias insertion, revision increment and receipt commit together.

Receipt identity is `(accountId, sha256(requestId))`; its intent digest contains the normalized operation inputs. A reused key with a different intent is a conflict. The same intent returns the original committed receipt after reopen, after named admission is disabled, and after a later retirement; it remains a historical claim/retire fact, not a current-access assertion. Current owner/actor checks still apply. A new key for an already completed action cannot accumulate successful no-op receipts. There are at most two successful mutation receipts per lifetime alias in A1. Tokens, app content and runtime request bodies are not stored in these receipts.

Mutation responses also echo `requestId` from the currently authenticated request, so UI pending work can match its acknowledgement without a second hash implementation. It is not persisted in plaintext. The receipt field is explicitly `requestKeyHash`, not an ambiguous `id`.

Aliases are `status-only` with `runtimeReady: false`. Retirement keeps the original app/owner binding permanently and does not make the name available to a different author. There is no canonical rename, address transfer, reclamation timer or browser-storage migration in A1.

## Evidence and gates

A1 focused checks: `node --test modules/apps/test/newdomains.test.mjs`. They cover migration preservation, rollback, unknown-schema byte preservation, null/pinned origins, owner and account guards, reserved/malformed names, stable receipts, tombstone quotas, and simultaneous independent SQLite workers for uniqueness, account quota and identical retries. Existing real HTTP/assets/POST/WS/revoke relay tests remain part of the regression gate.

## A2: host routing and TLS eligibility

`service.classifyHost(rawHost)` consumes the original `req.headers.host`, never forwarded headers. Results are `{kind:'outside'|'invalid'|'unknown-app-zone'}` or `{kind:'canonical'|'alias',domainId,appId,origin,hostname,state}`. This is an internal routing descriptor, not a public metadata response. Root ingress rejects duplicate raw Host fields before both HTTP and upgrade routing.

Authority parsing accepts valid DNS/IPv4/bracketed-IPv6 syntax and ports 1–65535. It rejects spaces, URL syntax, encoded hosts, userinfo, trailing dots, malformed/zero/padded ports and non-string input. DNS case is normalized; explicit default ports denote the same configured origin. A nondefault configured port must match exactly.

The classifier checks indexed, persisted addresses first. Every other hostname at or below a retained app namespace (including its apex, nested names and alternate ports) is consumed by Apps. An exact configured trusted shell can remain outside only in the legacy namespace and cannot override an allocated app hostname. No trusted-shell exception overrides any retained named namespace, including when `namedAppZone` is disabled.

Canonical runtime launch keeps the existing private grant/session flow. Alias requests are status-only: 503 before named runtime exists, 410 after retirement. Unknown app hosts return 404; malformed authorities return 400. These responses contain no app title, owner, target, cookies, redirect or Soty shell. They set no-store, no-referrer, nosniff and a restrictive status-page CSP. Named/unknown/malformed upgrades terminate before the connector control channel or another WebSocket router.

`allowsTlsDomain(hostname)` uses exact persisted HTTPS address ownership and accepts neither URLs nor ports. A revoked canonical or tombstoned alias can retain a certificate for its safe status page. A certificate does not authorize runtime access. Unknown names and HTTP-only development origins are rejected; the handshake path performs no external DNS/ownership lookup.

A2 focused checks: `node --test modules/apps/test/newdomain-hosts.test.mjs` (6/6 PASS). They include strict Host parsing, port/prefix handling, retained-zone and trusted-shell precedence, TLS/status separation, and real HTTP/upgrade alias/tombstone checks. Existing relay: 9/9 PASS. Root's `server/test/app-host-ingress.test.mjs`: 3/3 PASS through actual HTTP and a separate server process, including duplicate Host and namespace misses before shell/Connect/traffic/connector routes.

Independent reviewer acceptance added `modules/apps/test/domain-acceptance.test.mjs` and `server/test/app-host-acceptance.test.mjs`. It covered altered critical constraints before writes, injected receipt-write rollback, same-app concurrent CAS, historical receipts after revocation, retained aliases with disabled admission, 19 Host variants, forwarded Host/absolute targets, and a real enabled HTTP/WS upstream. The reviewer's combined final gate was 59/59 PASS, zero skips, including Apps legacy/A1/A2, both independent acceptance sets, root policy/ingress and signed HTTP. No remaining blocker was reported in the bounded A1/A2 scope.

Named runtime, origin-bound tickets/sessions, public access and policy epochs remain P3-B. Real DNS/TLS, external zone ownership and browser compatibility are separate release gates.

Production migration requires a stopped old writer, a reader/version guard and a prepared compatible fallback before the first v2 write. A schema marker prevents an old binary from reopening v2; it cannot stop a v1 process that already has an open connection. No compatibility flag or `BEGIN IMMEDIATE` claim replaces process fencing.
