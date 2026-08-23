# Soty experimental Identity adapter threat model

## Assets

- stable external subject and membership bindings;
- organization/project role assignments;
- enrolled device status and recovery state;
- issuer verification keys and their rotation state;
- experimental exchange/event replay records used by the local invariant
  selftest;
- audit history.

Existing Soty room secrets, connector tokens, private device keys, files, and
Gonka credentials are outside this seam and must never enter its artifacts.

## Trust boundaries

1. Canonical machine schemas/vectors are `STABLE`, mirrored unchanged, and run
   directly by the Soty conformance test.
2. The local experimental ES256 adapter is not a cross-product trust boundary.
   It exists to retain security and domain-transition tests.
3. Workflow issuance, hash storage, and atomic redeem exist only in the neutral
   Control Plane. Soty has a typed authenticated client after an existing BFF
   session and no local redeem endpoint/store.
4. Canonical events are RFC 7515 Flattened Ed25519/JCS envelopes. Signing and
   outbox are Control Plane only; Soty verifies through a pinned issuer/`kid`
   registry and persists only local shadow projection/replay/audit state.
5. Existing project authentication remains independent. Future cross-product
   login is only OIDC Authorization Code + PKCE S256 via a confidential BFF and
   local HttpOnly session.

## Threats and controls

| Threat | Control |
| --- | --- |
| Experimental artifact replay | Maximum 60-second expiry, unique issuer + JTI record, and retained replay tombstone |
| Retry creates duplicate link/event | Required idempotency key bound to exact artifact digest |
| Algorithm/key confusion | `alg=ES256`, `typ` pinning, `kid` from configured issuer only; remote key headers rejected |
| Cross-tenant confused deputy | Exact `aud`, exact `project_id`, and binding lookup including issuer, subject, organization, project, and membership |
| Project admin becomes another tenant's admin | Role is stored only inside the exact organization/project binding; cross-binding events fail |
| Account takeover through email/phone match | PII claims are rejected recursively; only opaque issuer `sub` is linkable |
| Secret/token leakage | Token/secret/credential/session/password claim names are rejected; artifact accepted only in POST JSON, never returned or persisted |
| URL/history/referrer leakage | Contract forbids exchange material in URL, query, fragment, localStorage, analytics, and logs |
| Device recovery hijack | Recovery requires signed start + matching recovery ID + signed completion; old device is revoked atomically |
| Unlink leaves live devices | Unlink and membership revocation revoke active devices and clear pending recovery |
| Store corruption silently clears replay state | Corrupt store fails closed with `identity_store_unavailable` |
| Audit deletion/modification by accident | Monotonic sequence and SHA-256 hash chain |
| Product becomes another exchange authority | Manifest ownership validation, no product redeem endpoint/store, and test double confined to the selftest |
| Product signs authoritative events | No signing key, signer, or outbox in runtime; consumer accepts only pinned Control Plane Ed25519 keys |
| Legacy wire interpreted as v1 | Separate parsers and prefixes; copied legacy-negative fixtures must reject both Soty ES256 and Tavysh nested compact artifacts |
| Custom token accidentally becomes login | Capabilities declare login unsupported; login requires OIDC Code + PKCE S256 + confidential BFF/local HttpOnly session |
| Workflow continuation stolen/rebound | Canonical requirement is 32 CSPRNG bytes, exact target client/origin/purpose/state/org/resources, authenticated server redeem, hash-only storage, TTL <=60s, atomic consume |
| Event serialization/signature ambiguity | Canonical requirement is exact RFC 7515 Flattened JSON members, EdDSA/Ed25519, pinned issuer+`kid`, and RFC 8785 JCS UTF-8 payload |
| Public endpoint abuse | 32 KiB body limit and in-process limiter in development; production requires distributed abuse controls |
| Split-brain replay/idempotency/audit | File store is blocked in production; a durable transactional store must commit all three with the state transition |
| Key compromise | Separate issuer keys, short artifacts, key rotation/revocation runbook required in shared service |

## Residual risks

- A host administrator can read or rewrite local bindings and recompute the
  local hash chain; centralized signed audit is still required.
- A compromised issuer signing key can authorize its subjects until consumers
  remove that key.
- The seam does not prove identity or collect consent; the issuing service must
  authenticate the subject and record explicit target-project consent.
- The seam does not create or revoke product login sessions. Using a binding as
  login authority requires a new ADR, session design, CSRF protections, and
  step-up policy.
- File-backed state and the in-process rate limiter are suitable only for
  development/single-instance selftests. Readiness is blocked and capabilities
  report `production_ready=false`.
- Canonical workflow replay/TTL/hash/atomic-consume semantics are exercised
  only by the isolated in-memory Control Plane test double. Production behavior
  remains the external Control Plane's responsibility.
- Canonical event revision, replay/security incident, gap quarantine,
  tenant/resource isolation, unlink, and security epoch are implemented only as
  local shadow projection until cutover approval.

## Required security tests

- invalid signature, unknown issuer/key, wrong `typ`, and algorithm confusion;
- expired, not-yet-valid, excessive-TTL, wrong audience, and wrong project;
- same JTI replay, exact idempotent retry, and idempotency digest conflict;
- PII and secret/token claim rejection;
- cross-organization/project admin event rejection;
- enrollment conflict, revoke, recovery-ID mismatch, recovery completion, and
  action after unlink;
- malformed/oversized JSON and corrupt-store fail-closed behavior;
- audit-chain verification and proof that raw artifacts are not persisted;
- feature flags off, withdrawn false-v1 route, permanently disabled legacy
  POST routes, no canonical product redeem/login endpoint, and production
  file-store refusal;
- direct execution of every upstream `STABLE` positive, negative, authority,
  and legacy-noncanonical vector without a translator.
