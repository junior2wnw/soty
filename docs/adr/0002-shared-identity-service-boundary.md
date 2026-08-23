# ADR-0002: Shared Identity service boundary

- Status: Accepted
- Date: 2026-08-12
- Scope: authority boundary and experimental Soty adapter only; no production deployment

## Context

Soty, Tavysh/karaoke, mykosyk, and Ufanovostroyka need a common way to refer to
the same external subject, organization membership, and enrolled device.
Soty currently uses capability secrets and device keys. It has no human account
session, tenant directory, organization policy engine, or centralized
revocation authority.

Making Soty the central IdP would silently turn room/connector credentials into
cross-project authority and create a confused-deputy boundary without the
required account, recovery, audit, and operational controls.

## Decision

Build a separate provider-neutral shared Identity + Organization service. Each
project remains a relying party and owns its local account/domain data. The
only canonical machine schemas and test vectors are owned by the
Ufanovostroyka repository. Their manifest is `STABLE` and mirrored unchanged
in Soty for direct conformance. Soty does not publish a competing shared v1 or
add a Soty-to-Tavysh translator.

Soty currently retains an off-by-default, non-canonical experimental adapter
that:

- accepts only pinned-issuer ES256 JWS artifacts;
- requires exact audience and project identifiers;
- consumes each link exchange once with idempotent retry semantics;
- tests membership/device/recovery/unlink transitions locally, while both
  legacy POST routes remain permanently disabled as non-canonical;
- stores no exchange artifact, PII, provider secret, or login token;
- scopes every binding and role to issuer + subject + organization + project +
  membership;
- does not issue a Soty session or grant existing room/connector access.

Canonical Soty runtime is relying-party-consumer-only. Cross-product login is
only OIDC Authorization Code + PKCE S256 through a
confidential BFF with a local HttpOnly session. Workflow continuation uses a
32-byte CSPRNG opaque base64url code, authenticated server-to-server redeem,
TTL no greater than 60 seconds, exact target/purpose/state/org/resource
binding, hash-only storage, and atomic single consume, all owned by the neutral
Control Plane. Soty contains only a typed authenticated redeem client after an
existing BFF session; it has no exchange store or redeem endpoint. Service
events use the canonical RFC 7515 Flattened Ed25519/JCS envelope. The signer
and outbox are Control Plane only; Soty owns only the authenticated consumer
and local shadow projection.

## Consequences

- A local experimental selftest link is only a compatibility receipt, not
  login. Canonical workflow continuation is delegated to the Control Plane and
  cannot create a session.
- Existing Soty data formats require no migration for this checkpoint.
- Projects do not depend on Soty code or its experimental ES256 wire.
- Manifest ownership fields and negative fixtures prevent a product runtime
  from becoming an exchange authority or event signer.
- The JSON adapter store and in-process limiter are development/single-instance
  only. Capabilities report `production_ready=false`, and production mutation
  fails closed until durable transactional replay/idempotency/audit storage and
  distributed abuse controls exist.
- Key compromise, identity proofing, consent UX, and centralized recovery stay
  responsibilities of the shared service.

## Rejected alternatives

- **Soty as central IdP:** rejected because its current secrets prove room or
  device capability, not a human/organization identity.
- **Match email or phone across databases:** rejected because identifiers can be
  recycled, mistyped, shared, or unverified and would transfer PII.
- **Put a signed token or code in a URL:** rejected because URLs leak through
  history, referrers, screenshots, analytics, proxies, and logs.
- **Share one cross-project admin role:** rejected because authorization must be
  qualified by organization and project at every decision.
