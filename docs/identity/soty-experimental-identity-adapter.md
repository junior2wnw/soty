# Soty legacy experimental Identity adapter

Status: **NON-CANONICAL, DISABLED, DEVELOPMENT TESTS ONLY**.

This document describes the pre-canonical Soty ES256 compact-JWS experiment.
It is not Identity wire v1, not an IdP, and not a format for another product.
Canonical wire v1 is now `STABLE`, mirrored under
[`contracts/identity/v1`](../../contracts/identity/v1), and implemented through
the separate consumer described in
[`canonical-identity-wire-v1-adapter.md`](canonical-identity-wire-v1-adapter.md).

## Legacy formats retained only for invariant tests

- `soty.identity-adapter.experimental.v1`;
- `soty.identity-adapter.link-exchange.experimental.v1`;
- `soty.identity-adapter.event.experimental.v1`;
- `soty-identity-link-experimental+jwt`;
- `soty-identity-event-experimental+jwt`;
- compact JWS, ES256.

The local selftest keeps replay/idempotency, exact issuer/audience/project,
tenant isolation, device enrollment/revoke/recovery/unlink, PII/secret
rejection, and audit-chain tests. These tests do not make the wire acceptable
as canonical v1.

The canonical fixtures
[`legacy-soty-es256-flat.json`](../../contracts/identity/v1/tests/fixtures/identity-wire-v1/legacy-soty-es256-flat.json)
and
[`legacy-tavysh-eddsa-nested.json`](../../contracts/identity/v1/tests/fixtures/identity-wire-v1/legacy-tavysh-eddsa-nested.json)
prove that both legacy formats are rejected by v1. There is no dual parser and
no Soty-to-Tavysh translator.

## Disabled legacy HTTP surface

```text
GET  /api/identity-adapter/experimental/capabilities
GET  /api/identity-adapter/experimental/readiness
POST /api/identity-adapter/experimental/exchanges/consume
POST /api/identity-adapter/experimental/events/apply
```

The two POST routes always fail closed with
`identity_legacy_workflow_disabled` and `identity_legacy_event_disabled`.
Readiness is always blocked. The withdrawn `/api/identity-compat/v1/*` prefix
returns `410 identity_compat_contract_withdrawn`.

The legacy flag remains off by default:

```text
SOTY_IDENTITY_ADAPTER_EXPERIMENTAL_ENABLED=1
```

Enabling it exposes only diagnostics and negative/legacy behavior; it cannot
enable canonical authorization, browser login, workflow exchange, or service
events. The JSON legacy store and in-process limiter are never production
capabilities.

## Ownership boundary

Soty never issues, stores, or atomically redeems canonical workflow exchanges.
It never signs canonical service events and hosts no event outbox. Those
responsibilities belong only to the neutral Organization Control Plane. Soty
is a relying-party consumer with a typed redeem client and a local canonical
event projection.

The non-canonical local guard
[`fixtures/soty-experimental-adapter-conformance.json`](fixtures/soty-experimental-adapter-conformance.json)
records that upstream v1 is `STABLE`, direct adoption is required, and a
translator is forbidden.
