# P4-C1 — discovery followed by a real client

2026-10-01. A fresh local PWA stand exposed a production defect before browser authorization: the RFC 8414 metadata alias advertised `/authorize`, `/token`, `/revoke` and `/jwks`, while the actual provider lives under `/oauth`. The ordinary OIDC discovery path described the correct endpoints. Earlier fixtures called known paths directly and did not establish this discovery-follow property.

## Cause and correction

The pinned `oidc-provider@9.12.2` generates URLs from the Express mount: its `oidc_context.urlFor` first infers a prefix from `originalUrl`, then falls back to `req.baseUrl`. The RFC alias does not contain the provider-relative discovery path. The wrapper now explicitly supplies its fixed, trusted `/oauth` base URL before dispatch, preserving `originalUrl` for the bounded ingress. No caller-controlled forwarding header supplies this mount.

The provider also advertised general OIDC scopes/modes which the narrower Soty facade rejects. Its normal middleware extension now projects `scopes_supported:['notes.createDraft']` and `response_modes_supported:['query']` only onto a successful discovery response. Public client authentication is configured through the maintained `clientAuthMethods:['none']` option. Endpoint generation, PKCE, token issuance and revocation remain library operations.

An explicit `response_mode=query` exposed a further mismatch: both ingress and the strict persisted Interaction validator originally rejected that standard equivalent of the default. Both now accept only optional `query`; `form_post`, `fragment`, arrays, empty and unknown fields remain rejected. The full request stays in the canonical stored payload/context. No schema migration or existing-record rewrite occurs. An older application whose validator cannot read this newly admitted optional field is **not** claimed to be a compatible OAuth rollback reader; select a reviewed compatible application image for release.

## Causal and regression evidence

- `p4-oauth-discovery-red.log`: actual alias/canonical endpoint mismatch reproduced.
- `p4-oauth-discovery-jwks-fixture.log`: after mount correction, the new test incorrectly assumed the existing helper parses `application/jwk-set+json`. Only the fixture was corrected to check that media type and parse its bounded text.
- `p4-oauth-discovery-mount-green.log`: actual discovery → signed consent → token exchange → native Note → revocation passed.
- `p4-oauth-discovery-profile-red.log`: unsupported `openid` advertisement reproduced.
- `p4-oauth-discovery-explicit-query-red.log` and `p4-oauth-discovery-query-final.log`: explicit query rejected first by ingress, then by the persisted Interaction allowlist. Both genuine restrictions were corrected, without removing the test.
- **Final 26/26 PASS, 0 skip, 6098.4378 ms**: domain profile/artifact cases plus full HTTP discovery, native flow and account switching. Log `output/implementation-20260930/p4-oauth-discovery-profile-final.log`, SHA256 `bf4573477496191f96d69d5f9b92fd073311511778c22a06f52dea04d4872dec`.

The final discovery test reads PRM, compares both real metadata documents, fetches the advertised public JWKS, uses the advertised authorization endpoint/scope/response mode, signs the owner decision through Connect, exchanges at the advertised token endpoint, creates a real native Note, revokes through the advertised revocation endpoint and confirms that the bearer no longer works. Unsupported response modes fail before consent. Existing wrong-resource/PKCE, family reuse, sibling access, keyless restart, account-switch and expired-document cases still pass. This is HTTP client evidence, not a graphical browser or either selected CLI.

## Frozen source

| File | SHA256 |
|---|---|
|`server/capabilities-oauth.js`|`d20cd27ff668cbe02dd8943ea960dc4d7d8149d15d3c49e1a66b59d7e4a67584`|
|`server/capabilities-oauth-provider.js`|`f4cd38b6174c71085627650a3eb2b8d4a5277d64ec2949b279f5dfdf193e0bc1`|
|`modules/capabilities/server/oauth-profile.mjs`|`e16b589a2cf8c492e7eb567989e465288009aea4bbc3b3c0db98319bd5799e07`|
|`server/test/capabilities-oauth-discovery.test.mjs`|`706563cee7d4594ee426f147b87cf6c6cb5b74ec6c0456f2fce83b1cd718956d`|
|`modules/capabilities/test/oauth-profile.test.mjs`|`130eba4965b10c51b57dc97a52a41d71fcfc654065d67e17fea71afd2ab8d2b9`|

The local stand is restarted into a fresh owned data directory with memory-only keys before browser work. Its original failed smoke remains evidence. P4 C2, actual CLI/PWA acceptance and release/restore gates remain separate.
