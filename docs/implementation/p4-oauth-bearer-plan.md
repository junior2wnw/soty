# P4-C1c T2 — OAuth bearer and occupied native key

2026-10-01. Implementation boundary approved after T1 `8497650`. The implemented boundary, executable evidence and frozen hashes are recorded in [p4-oauth-bearer-receipt.md](p4-oauth-bearer-receipt.md).

## Ownership and sequence

1. `modules/capabilities/server/access.mjs`: extend the existing private actor WeakMap with an explicit immutable OAuth connection reference. A coordinator-only captured factory resolves an opaque-token digest to an existing credential/link and returns the same service actor shape. No actor/account factory is exposed to callers. Every subsequent resolve checks the exact connection reference, current credential, account/audience, creator and root chain. The `soty_cap_` parser is unchanged.
2. `native-notes.mjs`: after fresh authorization and the existing same-client exact replay, obtain a checked namespace from the captured access resolver. Query the existing Invocation rows by account/request key and immutable connection issuer/static profile/resource. A different connection occupying the key yields only `invocation_request_conflict`, before readiness, reservation and insertion. No receipt, old input, ID, status or current Note existence is returned. Existing expired/revoked/terminal/purged rows still occupy their key. No new ledger or DDL.
3. `oauth-connections.mjs` and minimal `index.mjs` composition: `authenticateBearer({token,audience})` validates the fixed opaque form and configured resource, then invokes the private factory inside the existing Connect→Caps fence. It needs immutable digest/link, not an artifact decryption key. Readiness becomes true only with schema3, key/configured token ports and the real fixed native binding/identities. This binding check ignores operational `executionEnabled`; the host independently gates starting the AS with native operational readiness. Read/replay/bearer have no global readiness guard.
4. New own T2 fixture/tests use signed Connect, real Notes2/Caps3, actual OAuth artifacts and the native coordinator. Tests are serial and use isolated Node24.21.0. No host/provider/UI, Notes, catalog, DDL or reader changes.

## Closed seams

The existing `captureOAuthAuthority` object gains `authenticate({tokenDigest,issuer,audience}) -> branded ServiceActor` and `scope(actor) -> null|{connectionId,accountId,issuer,clientProfile,resource}`. Both require the current Caps transaction and validate the existing credential/chain. Only the coordinator/native composition receives them. OAuth connection identity is held in private actorRefs, never supplied through native admission arguments or copied from actor public fields.

The native factory accepts `oauthScope` (optional closed resolver) and `captureBindingReadiness` (optional composition callback). The latter receives a synchronous fixed-binding/identity check, without effect admission or execution. `index.mjs` wires these internal seams; no new public service method or route is introduced. Public native/OAuth signatures and persisted authorization snapshots remain unchanged.

Current refreshed AT actors may read/replay the same connection's Invocation under current read ACL, including when native execution is off. Original dispatch still re-evaluates the **original** persisted credential and absolute expiry. Refresh cannot extend that deadline. Family/creator/root revoke denies external access; a previously committed Notes proof can still settle internally, and owner signed history remains available.

## Focused acceptance

- Opaque AT resolves to branded actor; forged/copied/stale-instance actors, raw service-token crossover, foreign audience/account, missing link and creator/root/family revoke fail closed. Keyless AS-off read is supported without enabling issuance.
- AT1 admission → expiry → new AT2 on the same Grant: read and exact replay work, original effect cannot newly execute. Terminal receipt after human edit/purge stays historical, with no current-existence lookup.
- Notes COMMIT before Caps receipt → family revoke/cancel → proof-first reconcile completes internally; external actor remains denied.
- New connection, same account/issuer/profile/resource/key: one generic conflict after old success/revoke/purge or with changed valid input, zero new reservation/Note. Same connection refresh replays. Other accounts/profiles/resources and legacy service credentials keep independent namespaces.
- Two actual OS writers on separate connections contest one key under real Connect/Caps locks: one admission, one conflict. Actual query SQL uses the existing account/request/client index; no global history scan under the fence.
- Cleanup of expired transient credentials retains actual unknown/terminal native original links. Readiness checks real schema/identity/fixed binding but is not used to block authorized history or occupied-key denial. Default-off/missing key/operational-off cases remain distinct.

T1 encrypted code/refresh HTTP authentication, complete CLI/MCP and production deployment are separate gates. T2 port tests will not claim those outcomes. No implementation of the full master plan is inferred merely from this checkpoint.
