# Remembered device consent: Root Human3

This slice starts from Root commit `4d33e0d8944b9bc639088d51e8adf7fb6f320dcc`. It changes only the existing Human issuer service/store and Root signed approval presentation. It does not configure or deploy an issuer, alter Native accounts/links, change Source RP49, add an OAuth engine, or claim a tested invisible iframe flow.

## Meaning and boundaries

“Запомнить вход на этом устройстве” records permission to approve a **new** ordinary OIDC interaction after a **new current signed Connect device proof**. The remembered tuple is exact account, device, registered client, client profile/generation, registered callback and sorted requested scopes. Another account/device/client/scopes/profile generation cannot inherit it. Display names, email addresses and caller-supplied roles are not matching inputs.

The preference has no bearer credential. ID tokens remain 60 seconds, access tokens 300 seconds, and each opted-in refresh family ends at its original approval time plus 86400 seconds. Continuing after that deadline creates a new interaction and a new finite family; it never changes an old family deadline, token ciphertext, or Native immutable issuer/subject link. Revoked devices and inactive or changed client generations fail current checks. There is no hardware/passkey assurance claim.

An existing 24h approval is not silently upgraded. The initial remembered choice is a signed `identity.human.approve` decision with `decision:'approve'`, `stayInAppSeconds:86400`, and `rememberForDevice:true`. The UI hides the separate 24h checkbox while remembering is selected; ordinary nonremembered 24h remains available under its existing policy.

## Constructor and profile hooks

```js
createHumanIdentityHostProfile({
  // existing registered clients, issuer and private keys unchanged
  renewal: { admissionEnabled: true, clientIds: [...] },
  deviceConsent: { admissionEnabled: true, clientIds: [...] },
}, { shellOrigins });

createHumanIdentityService({
  // existing private synchronous current-authority ports unchanged
  allowRenewalMigration: false,
  allowDeviceConsentMigration: false,
  renewalLimits: { activeFamilies: 16, familiesPerAccount: 8 },
  ...
});
```

`deviceConsent.clientIds` must be a subset of the existing renewal client IDs. Adding or disabling admission does not alter the existing protocol or client profile digests. Migration and limits are trusted constructor options; there is no body/RPC/author override. The parent bootstrap maps `humanIdentityDeviceConsentMigration` to `allowDeviceConsentMigration` and `humanIdentityRenewalLimits` to `renewalLimits`; that integration is outside this slice.

Default family limits remain 16 globally and 8 per account. The optional closed limits are integers in 1..4096, with per-account <= global. They count active finite **families**, not unique apps; another device/session in one RP can consume another slot. They do not enlarge database/artifact/grant quotas or promise capacity at the maximum. Source RP49's own production per-account bound remains 8 and is separate. Device consent history is bounded at 4096 rows globally and 128 rows per account/device, including inactive historical tuples; revoked rows are never silently evicted or healed.

## Closed signed continuation and logout

`identity.human.continue` accepts only `{expectedAccountId, interactionId, browserNonce, csrf, requestId}`. Its result is `{schema:'soty.human-login-continuation.v1', interactionId, requestId, continued:boolean}`. A false result leaves the fresh interaction pending for ordinary manual approval. A true result approves only the exact current interaction. The same request ID and intent return one pinned durable decision; changed input conflicts, and a revoked pinned consent cannot replay successfully. Unknown acknowledgement is retained for an explicit identical retry, with no background retry loop.

The public context contains only optional presentation `{deviceConsent:{available:boolean, approvedRememberForDevice?:boolean}}`. It includes no consent hash/revision, device key, account authority, redirect, or tokens. An already approved remembered choice remains visible after reload/admission off. The controller checks the current local account before and after awaits and does not automatically create a profile.

Active standard maintained `/revoke` of an RP's own RT/Grant revokes the corresponding device consent. Clearing only a Source cookie cannot notify Root. For an expired family, logout uses a **fresh** ordinary interaction and existing signed `identity.human.approve` with `decision:'deny', forgetForDevice:true`, no duration/remember flags. It works with consent admission off when the client remains registered for this profile. It requires the same current actor/device and exact registered interaction. It revokes that tuple's consents and families, preserves another RP, and records the existing durable decision result. The same old forget intent never performs a new revoke after a later manual reconsent.

Within that transaction, earlier pending interactions in the same client/profile/generation/browser nonce are denied. Earlier approved interactions of this exact account/device retain their immutable approval/JSON but lose their live SDK Interaction proof. Human3 checks that proof before approved completion/new Grant commit. Thus an earlier unpinned pending remember, an already captured unpinned approval, or an old callback cannot mint a postlogout family. Approved interactions of another account/device and requests with a different browser nonce remain intact. This browser-local cancellation is not a global logout or cross-device operation.

The Root controller gets `presentation:'forget'` from exact URL fragment `#forget`. This suppresses automatic continuation and displays “Выйти и забыть на этом устройстве”; the fragment grants no permission and cannot cause an effect without the signed click. It does not reinterpret `prompt:login`, which ordinary RP login already may use. The explicit synthetic Chrome gate starts at real RP `/login#forget`, follows the maintained issuer authorize/302 to Root interaction, and proves retention after that redirect and reload without URL/cookie injection. It uses a new owned browser profile and loopback origins, not the live Native/HIVE HTTPS UI. Native/Source logout hook and natural browser integration remain separate gates. The fragment must reach the fresh Root interaction before mounting.

HIVE has distinct ordinary Native and Source RP clients. Forgetting one registered client does not revoke the other's Root preference. Whole-app logout requires Native durable suppression/pending-login cancellation which Source cannot bypass; if both Root preferences must be cleared, use two ordinary explicit signed scopes. This slice adds no client group, role, app-label inference, or cross-RP revocation authority.

## Storage and rollback

Human3 is required because Human2 verifies its entire exact schema. It adds one table `human_identity_device_consents` and nullable decision columns `consent_hash TEXT`, `consent_revision INTEGER`. The pair must be both NULL or both nonNULL, with a 64-character hash and revision >=1. The consent tuple and creation time are immutable, revisions advance exactly once, and deletion/replacement is forbidden. Manual reconsent is bounded below revision1000000, reserving one final revocation step.

Migration is one SQLite transaction after exact prior layout/metadata/FK validation. It preserves every old data column, result JSON, encrypted artifact bytes and approved absolute deadlines; only lineage/user_version and the additive table/decision references change. Human1 still needs its separate explicit renewal migration before Human3. Default migration flags are false and default Human1/Human2 stores do not gain a consent table. New code reads Human3 with both admissions off; an old Human2 reader refuses it, including a disguised user_version=2 database.

Independent literal public fixtures in `modules/human-identity/test/support/human-layout-v2.mjs` and `human-layout-v3.mjs` have no runtime schema imports or private data. The bounded read-only reader checks exact normalized schema, five metadata keys/lineage/profile and FK integrity.

| Version | Objects | Normalized layout SHA256 |
| --- | ---: | --- |
| Human2 | 33 | `b096f63d2b27fa301d9fd336a57205278495ea80355fd27477f46115d4f8a2c9` |
| Human3 | 38 | `b3fa2360e7db5e44af0fb7f0716ecbb9c6e43793a9771574a73db30c01a3afb9` |

Human3 metadata: user_version3, lineage `soty.human-identity.sqlite.v3`; registry_id, environment_id, issuer and `oidc-provider-9.12.2-human-v1` are unchanged. The normalized pin preserves SQL string literals and removes/lowercases other whitespace as the existing Human2 reader does.

Before first migration/write, rollout needs a fresh encrypted backup and a separately verified compatible new-reader/consent-OFF cold fallback. Historical Human2 image cannot be used as that fallback. Parent owns loader, runtime measurement, Docker reader declarations, independent host probe and rollout guards. This slice supplies their schema fixture, not a completed image/cold/restore gate or a general revocation-after-backup guarantee.

## Evidence

The new signed HTTP tests use real Connect browser keys, maintained oidc-provider9.12.2, independently served BFFs/openid-client6.8.4, real SQLite and controlled time. They cover a new finite login after absolute expiry, exact tuple mismatches, current device revoke, active `/revoke`, 48h expired-family forget, same-intent lost ACK, stale pending/approved/cached Grant denial, other-RP preservation, off admission and nine registered RPs with explicit limits. Storage tests check byte/value preservation and the independent literal Reader2/Reader3 boundary. Emitted real controller tests use synthetic DOM/account/network ports and are not browser SSO/CSS evidence.

Local final gates on bundled Node24.19: complete Human71/71, unchanged Source RP49 22/22, emitted Human UI12/12, context4/4, typecheck and diff whitespace PASS; zero fail/cancel/skip. Existing maintained provider warning about an already parsed body remains unchanged. No test or limiter was weakened; the revoked OP session can be denied first by the maintained SDK with HTTP400 or by the service with403, with exact private denial and zero new Grant checked independently.

Focused commands use bundled Node and the existing dependency tree:

```text
node --test --test-concurrency=1 modules/human-identity/test/*.test.mjs
node --test --test-concurrency=1 modules/source-rp/test/*.test.mjs src/platform/human-context.test.mjs src/world/human-login.acceptance.test.mjs
node node_modules/typescript/bin/tsc -p tsconfig.json --noEmit
node scripts/fixtures/human-device-consent-browser.mjs --cli /approved/local/node_modules/@playwright/cli/playwright-cli.js
```

Live issuer configuration, Native links/data, active browser sessions and deployments are untouched. No automatic iframe availability or forever bearer/session claim is made.
