# Soty Identity architecture checkpoint

Status: completed for the compatibility phase. No deployment is authorized by
this document.

## Current Soty security domains

| Existing primitive | What it proves | Why it is not shared human identity |
| --- | --- | --- |
| Browser P-256 device key and derived `dev_*` ID | Possession of one extractable device signing key | No human subject, organization, server session, revocation registry, or tenant authorization |
| Room ID, room secret, and derived room auth | Possession of one room capability | A room secret is bearer authority, not an account or membership |
| Connector Link ID and installation token | Permission for one installed runtime to poll one Link queue | Scoped device transport credential; no organization or project role |
| Account transfer payload | Ability to restore an encrypted browser/device backup | The server stores an opaque ciphertext; it cannot authenticate the owner or revoke a subject |
| TrustLink Kernel | Device identity, pair trust, permissions, sessions, encrypted frames | Reusable crypto/transport primitives, but its identity is explicitly device identity rather than a person/organization directory |

The realtime server accepts possession of room auth and then trusts the
client-provided device ID inside that room. This is appropriate for the current
capability-based room model, but it cannot authorize a person as an
organization member or administrator.

## Decision

Soty is **not** reusable as the shared Identity + Organization authority. A
separate provider-neutral Identity service is required. The accepted boundary
is recorded in [`ADR-0002`](../adr/0002-shared-identity-service-boundary.md),
and the threats are recorded in [`threat-model.md`](threat-model.md).

This repository now contains only an experimental relying-party adapter and a
fail-closed conformance gate:

```text
canonical schemas + vectors (D:\уфановостройка, STABLE)
  -- copied and consumed directly, unchanged -->
Soty relying-party adapter
  -- Soty-local mapping only --> existing Soty account/device model
```

The canonical adapter is disabled unless `SOTY_IDENTITY_V1_ENABLED=1` and has
shadow projection only. Soty owns no exchange issuer/store/redeem endpoint and
no event signer/outbox. It provides a typed authenticated Control Plane client
after an existing OIDC BFF session and a local service-event consumer. The old
ES256 experimental POST routes remain disabled. Neither adapter issues login
sessions, accepts passwords, discovers accounts by email/phone, or makes Soty
an IdP.

## What is reusable

- P-256 and stable-key-ID utilities can inform device enrollment.
- TrustLink pairing, encrypted session, replay, permission, and transport
  primitives can carry identity messages after the identity layer authorizes
  them.
- Existing atomic file-store patterns are sufficient only for development and
  single-instance invariant tests. They are not production identity storage.

These primitives must not be confused with the new domain entities:
`ExternalSubject`, `Organization`, `ProjectMembership`, `IdentityBinding`,
`EnrolledDevice`, `Recovery`, `LinkExchange`, and `AuditEvent`.

## Required separate-service capabilities

Before any project treats the shared layer as login authority, the separate
service needs:

1. Control-Plane-owned durable transactional workflow exchange storage and
   atomic consume; product-owned durable transactional event projection,
   replay, quarantine, revision/security epoch, and audit storage;
2. OIDC Authorization Code + PKCE S256 through a confidential BFF, local
   HttpOnly sessions, explicit account-link consent, step-up for recovery/admin
   actions, and session revocation;
3. Control-Plane-owned managed event signing/outbox, product-side canonical
   pinned issuer/`kid` registry, rotation overlap, emergency key revocation,
   and issuer-specific blast-radius controls;
4. organization/project policy enforcement and immutable tenant-qualified
   authorization checks;
5. append-only centralized audit export and retention policy;
6. abuse controls, operational metrics, backup/restore drills, and privacy/data
   lifecycle policy;
7. an ADR and reviewed threat model in every consuming project before login or
   admin authorization is switched.

## Migration and rollback

There is no automatic migration from existing Soty devices or room links.
Creating a shared subject from a device ID, nickname, matching email, or phone
is forbidden.

Rollout gates:

1. Flag off: code is inert and the reserved route prefix returns a stable 404.
2. Legacy checkpoint: retain the non-canonical local security selftest; legacy
   workflow/event ingress remains blocked.
3. Canonical conformance: run the copied `STABLE` Ufanovostroyka manifest and
   all vectors directly without a Soty/Tavysh translator.
4. Non-production integration: use only the typed Control Plane redeem client
   after an existing BFF session and the authenticated flattened Ed25519/JCS
   event consumer in shadow mode.
5. Production readiness: require durable transactional replay/idempotency/audit
   storage, distributed abuse controls, OIDC+BFF session review, and a new ADR.

Rollback is disabling the flag. Existing Soty room, device, connector, and
account-transfer data remain unchanged. Experimental state lives at
`DATA_DIR/soty-identity-adapter/state.json`; it is not migrated into canonical
identity state automatically. Consumers must fall back to their pre-existing
auth path, never to email/phone matching.
