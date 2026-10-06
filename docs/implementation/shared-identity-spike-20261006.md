# Shared identity spike — source evidence, 6 October 2026

This is a read-only source assessment, not an implemented or deployed SSO flow. Soty source base is `698a75f82ba9780494e10e1bf060781e94095b5e`. Pocket-ID `2.12.0` was identified by the preceding serving audit; this pass read that exact upstream tag, not live administrator configuration, users, credentials or provider storage.

## Registration seam already available

- `modules/connect/server/index.mjs:411` constructs an active, signed installation's actor from stored account/device IDs. Request arguments cannot supply that actor. Async extensions at line 726 commit the consumed proof before external work and require their own final authorization check and durable request ID.
- `modules/apps/server/inspection.mjs:96` reads owner, grants, publication epoch and active target in one Apps snapshot. A host-only registration resolver can use these records with the established Connect → World → Apps fencing order. Do not call a nested Connect fence from a synchronous extension already inside the signed transaction.
- The runtime target digest in `modules/apps/server/publications.mjs:42` and `sources.mjs:99` hashes a routing/ownership tuple. It is not a hash of application files. UI-only registration can pin that tuple; it must not claim an attested executable release or enable agent execution.
- Persisted author registration can accept a closed title-only draft plus app/request IDs, materialize from the verified owner/source view and an operator-approved profile, and retain transactional intent/receipt history. No request-supplied host pins, owner or provider configuration is trusted. A real feedback installation needs its own durable receipt.

## Pocket-ID 2.12.0: two different capabilities

The [exact release](https://github.com/pocket-id/pocket-id/releases/tag/v2.12.0) already includes QR sign-in. In that tag, [device-login approval](https://github.com/pocket-id/pocket-id/blob/v2.12.0/backend/internal/devicelogin/handler.go#L123) and [service verification](https://github.com/pocket-id/pocket-id/blob/v2.12.0/backend/internal/devicelogin/service.go#L134) require fresh passkey reauthentication. This continues an existing Pocket-ID account; it is not Connect device-key enrollment or recovery.

There is also a candidate first-use path that requires an isolated integration test:

- [Signup DTO](https://github.com/pocket-id/pocket-id/blob/v2.12.0/backend/internal/usersignup/dto.go#L9): username required, email optional. An issuer-origin UI could generate the pseudonym without showing a registration form.
- [Native signup service](https://github.com/pocket-id/pocket-id/blob/v2.12.0/backend/internal/usersignup/service.go#L45): open signup or a permitted signup token is required. User and issuer session are created transactionally without requiring an initial passkey. The [native handler](https://github.com/pocket-id/pocket-id/blob/v2.12.0/backend/internal/usersignup/handler.go#L201) sets its own browser cookie.
- [OIDC authorization](https://github.com/pocket-id/pocket-id/blob/v2.12.0/backend/internal/oidc/authorization_service.go#L205) still checks allowed groups; reauthentication depends on client/prompt/max_age requirements. [JWT processing](https://github.com/pocket-id/pocket-id/blob/v2.12.0/backend/internal/service/jwt_service.go#L293) permits an absent authentication-method claim. This supports investigating native issuer signup → ordinary OIDC Code + PKCE + app BFF sessions, not copying issuer cookies or inventing an app login token.

Installed signup/client/group policies were not read. Native signup exposes no application idempotency key, and cookie loss/expiry does not provide Connect-style key recovery. An isolated test must prove first-use creation, lost-ACK handling, two independent clients, old-profile resume, expiry/recovery and denied privileges before this candidate closes U2. Never enable open signup on the existing issuer implicitly.

## Remaining authority boundary

Soty's `server/identity-wire-v1-api.js:49` still reports no OIDC BFF session; its canonical consumer is a shadow projection and rejects production mutation without durable storage. Tavysh's canonical adapter is also a consumer, not an issuer. Old local IDs, ACLs and E2EE remain in their products; linking needs proof of both the old local account and the new `(issuer, subject)` identity, including pairwise-subject handling. An expired or unavailable proof must not silently create a replacement author.

If durable enrolled-device authentication without an initial passkey is required beyond the native-cookie candidate, a maintained neutral issuer needs a verified authentication module. [Keycloak Authentication SPI](https://www.keycloak.org/docs/latest/server_development/index.html#_auth_spi) is a documented extension seam, not an already installed Connect plugin. Any such module authenticates at the issuer's own origin; applications continue to consume standard OIDC. No identity implementation or deployment was performed by this spike.
