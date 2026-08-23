# Canonical Identity/Organization wire v1 adapter

Status: canonical contract `STABLE`; Soty integration is a **disabled adapter**,
not working SSO. It is feature-flagged, shadow-only, and not production-ready.
No deployment is authorized.

## Contract source and local mirror

The authority is the Ufanovostroyka contract bundle. Soty mirrors it unchanged
under [`contracts/identity/v1`](../../contracts/identity/v1): 22 JSON artifacts
plus the upstream Python reference test. The local Node conformance test loads
the copied manifest and vectors directly; it does not translate or rewrite
them.

[`soty-vendor-pin.json`](../../contracts/identity/v1/soty-vendor-pin.json)
records wire version 1, the upstream repository/path, all 23 exact SHA-256
digests, synchronization time, and the canonical manifest SHA-256. The sync
time is explicitly not a freshness or trust claim. Conformance fails on any
missing, extra, or byte-changed contract file. Runtime reads only this local
offline bundle; it never reads `D:\уфановостройка`. Updates are explicit
reviewed diffs and are never auto-merged. A signed/versioned package coordinate
may replace the local upstream path later.

## Authority ownership

The manifest-enforced ownership is exact:

- Identity provider: provider-neutral OIDC;
- Organization Control Plane: separate provider-neutral service;
- workflow exchange issuer, hash store, and atomic redeemer: Control Plane
  only;
- service-event signer and outbox: Control Plane only;
- Soty runtime: relying-party consumer only;
- product exchange store/signer: forbidden;
- exchange test double: isolated nonproduction conformance only.

Soty therefore exposes no workflow issue/redeem endpoint and persists no
workflow code/hash/record. `createIdentityControlPlaneRedeemClient` is a typed
client over an injected authenticated server-to-server transport. It requires
an already existing local OIDC BFF session and a response validator supplied by
the eventual Control Plane client package. The raw code is never logged,
persisted, returned to the browser, or placed in a URL.

## OIDC BFF boundary

Ordinary login is only:

```text
OIDC Core 1.0
Authorization Code
PKCE S256
confidential BFF
server-side Secure HttpOnly SameSite=Lax session cookie
```

The BFF checks issuer, audience, signature, expiration, state, and nonce.
Browser token storage and custom login-token protocols are forbidden. Workflow
redeem cannot create a login session; it may continue only an already
authenticated BFF session. Soty currently has no canonical login endpoint.

## Workflow continuation

The typed client accepts only the canonical binding inputs:

- exactly 32 CSPRNG bytes encoded as unpadded base64url;
- target client and HTTPS origin;
- purpose `account_link`;
- caller state;
- organization;
- sorted exact resources.

The Control Plane owns `SHA-256(code ASCII)` storage, TTL up to 60 seconds,
constant-time comparison, revocation checks, and atomic single consume. Soty's
test suite proves these behaviors with an in-memory serialized fake located
only in the selftest. No equivalent production store exists in this repo.

## Service-event consumer

Soty accepts canonical service events only at:

```text
POST /api/identity/v1/service-events
```

The local API requires an injected authenticated service transport. The body
is the RFC 7515 Flattened JWS JSON envelope itself, with exactly `protected`,
`payload`, and `signature`. Verification requires:

- `alg=EdDSA`, Ed25519;
- `typ=identity-service-event+jws`;
- pinned issuer + `kid` registry;
- RFC 8785 JCS UTF-8 protected header and payload bytes;
- exact consumer audience;
- signature verification before apply;
- closed flat payload schema and no PII/secrets;
- exact local tenant/resource binding.

The consumer persists only local projection, replay digests, aggregate heads,
revision-gap quarantine, security epoch, and audit. Duplicate issuer/event ID
with identical signed bytes is idempotent; different signed bytes is recorded
as a security incident. Revision gaps are quarantined without apply,
predecessor mismatch fails, and security epoch never decreases. Soty has no
event signer or outbox.

In the current application composition, `attachCanonicalIdentityApi` receives
no `authenticateServiceEvent` callback. Consequently service-event POST fails
closed with `503 identity_v1_service_authentication_unavailable`. No Control
Plane redeem transport is wired. OIDC BFF session availability is false and
readiness always returns 503. Capabilities report
`integration_status=disabled-adapter` and `sso_available=false`.

## Exact role and scope mapping

There is deliberately no privilege translation.

| Input | Soty mapping |
| --- | --- |
| Canonical tenant role | Store the same catalog ID in shadow projection only |
| Canonical tenant scope | Store the same catalog ID in shadow projection only |
| Canonical platform role/scope | Reject at the product adapter boundary |
| Legacy Soty roles `member`, `viewer`, `project_admin` | Unmapped and rejected as v1 |
| Legacy scopes `identity:*`, `membership:read`, `device:*` | Unmapped and rejected as v1 |
| Any canonical role/scope | Grants no existing Soty room or connector capability |

Accepted tenant role IDs are `analyst`, `billing_manager`, `developer_editor`,
`feed_manager`, `organization_admin`, `organization_owner`, and
`support_viewer`. Platform-only role IDs rejected by Soty are
`billing_operator`, `platform_operator`, `platform_superadmin`,
`security_operator`, and `verification_reviewer`.

The authoritative role-to-scope lists and all 23 scope definitions are copied
verbatim in
[`authorization-catalog.json`](../../contracts/identity/v1/data/contracts/identity/v1/authorization-catalog.json).
No canonical projection currently changes product authorization.

## Runtime surface and flags

```text
GET  /api/identity/v1/capabilities
GET  /api/identity/v1/readiness
POST /api/identity/v1/service-events
```

There is intentionally no product `/workflow-exchanges/issue`,
`/workflow-exchanges/redeem`, login-token, signer, or outbox route.

```text
SOTY_IDENTITY_V1_ENABLED=1
SOTY_IDENTITY_V1_RUNTIME=development|single-instance|production
SOTY_IDENTITY_V1_CLIENT_ID=soty.online
SOTY_IDENTITY_V1_TARGET_ORIGIN=https://soty.online
SOTY_IDENTITY_V1_ISSUERS_JSON=[...pinned public Ed25519 JWK registry...]
```

The feature flag is off by default. Omitted runtime defaults to `production`,
which fails closed because the current JSON event-consumer store and
in-process limiter are development/single-instance only. Capabilities report
`authorization_effect=shadow-projection-only` and `production_ready=false`.

## Remaining gates

- wire the approved OIDC provider and confidential BFF/session lifecycle;
- provide the Control Plane authenticated redeem transport and its canonical
  response validator/client package;
- replace the file event-consumer state with a durable transactional local
  projection/replay/quarantine/audit store;
- configure and drill pinned issuer/`kid` rotation and emergency revocation;
- approve catalog usage, consent UI, recovery/ownership decisions, and Soty
  cutover gates from the manifest;
- keep authorization effect in shadow until those gates are complete.
