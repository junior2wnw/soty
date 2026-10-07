# Source app SDK — S1 Basic300 acceptance

Isolated Root worktree `soty-source-app-sdk/соты`, branch
`codex/soty-source-app-sdk`, base `07e21ed3d570fa3e50db0a4a52bed8866eacb518`.
Original Source/Root repositories and production are unchanged.

## Fixed contract and provenance

One compiled selected Source pin:
`soty.standard-resource`, version1,
`b646578a1cbace022bdcc44147e0ad56b4e9d6239250726c4bb2decc4e4de721`;
kind `soty.resource.v1`. This is a semantic closed route/profile pin, not a
permission, executable source hash, deployment proof or approved provider.
Root must admit the exact Source/target/client/resource profile and install a
connector binary containing the compiled adapter. No Apps format/DDL changes.
Existing selected HIVE and legacy Planner routes/pins are preserved.
Additive Native correlation fix namespaces only Native intent/CSRF by the exact
approved appId; embed cookies/pin/Root DDL are unchanged. A single Native
browser/app has one selected pending intent; replacement explicitly returns
superseded409 for the older form/callback. This is not parallel same-app intent
support. The shared-host parallel gate uses one hostname cookie jar across two
Source apps; port-based cookie isolation is never assumed.

Maintained `openid-client@6.8.4` and exact common Source RP49 are reused.
Root source includes SDK extraction `c511a60` and additive minimum-access
`72aeec2` (reviewed SDK origin `49ead2504485b7b2a81ad192882f79998b369d76`).
Their files are not modified. SHA256:

| Source RP file | SHA256 |
|---|---|
| protocol.mjs | a5a7af0619812c6195fd3f5c0abb95ee4067694dd9c9289a1078b5a21b2cc3dd |
| session-service.mjs | b8e8347d68f460b353dc747c663d6ecb3417fef572dbe5f9340e95e6ec4e5e4f |
| index.mjs | 81dfc15d4bec55db256e3632341d1a09813ef2a3322c4a3c5d330b1c07f7f81a |
| index.d.mts | 6306ee87bfc41c28db34cfe1e2e446395b01185e21eff3e4dda7e6b088049d79 |

Package build uses pinned esbuild0.27.7 and emits source/output SHA provenance.
The standalone server bundle has only Node builtins + pinned openid-client as
runtime dependencies. Browser bundle contains neither Node code nor OIDC
credentials. Package is local preview, not published. Generated `dist` is ignored;
rebuild it from the committed sources before packing or inspecting checksums.

## Actual gates

Node24.19.0 / pnpm10.30.0.

- Source SDK tests: **15/15 PASS, 0 skip**. Constructor/proof brands and callback
  guards; actual Native SQLite commit/revoke/receipt restart; portable package
  loading in another OS process; feedback controller/closed wire; actual PNG and
  >120s Opus rejection; UTF-8 JSON duplicate/nesting/response bounds.
- Existing Apps8 reader/profile/proof selected tests: **11/11 PASS, 0 skip**.
  Combined targeted invocation: **26/26 PASS, 0 skip**.
- Root `typecheck`: PASS. Source package declaration files also pass a separate
  strict NodeNext TypeScript check. Root typecheck alone does not cover the types.
- Maintained BFF HTTP: actual Root OIDC provider + signed Connect consent, real
  PKCE/code/token/userinfo flow. Wrong native CSRF/Origin/callback state/duplicate
  state, body identity claims and Basic reuse on a different Root slot deny.
  This test has a **controlled MAC IPC + RAM Source store**, not installed
  connector/Native durable storage; its passing is part of the 15 tests.
  The additive Source-router closure also directly probes unknown feedback
  suffixes, HEAD mutators, duplicate/unknown query keys, wrong Content-Type,
  byte overflow, invalid UTF-8 and decoded duplicate body keys before dispatch;
  these direct Source negatives do not rely on signed Root route rejection.

Native effect boundaries are real SQLite transactions with synthetic rows:
revoke before final commit produces effects0/receipts0; lost ACK after COMMIT
produces one durable receipt and no automatic apply retry; receipt survives
restart; post-COMMIT revoke is unknown and does not expose the private receipt.
Source support is checked separately and a Native participant is denied reply.
S1 does not prove a multi-process Native domain or an arbitrary existing app.

## Authority and UX bounds

Native hooks are trusted constructor code, not author JSON. They resolve the
real Source account/session/resource and assert current Native role inside their
transaction. Mutators must use the branded synchronous operation-local commit
capability and persist effect + receipt atomically. SDK guards cannot repair a
Native hook which bypasses its own transaction. After an action may have applied,
errors/denials return unknown; post-check is never claimed to roll back a write.
Network remains outside Source DB transactions. There is no distributed
Root–issuer–Source atomicity or instant in-flight cancellation promise.

Basic uses an original Root slot and AT ≤300s. S1 `session-continue` reports actual
deadline and `renewable:false`. Common RP exports do not establish a durable
long session. Native legacy linking requires both old Native proof and new
verified OIDC proof; no email merge or Root-owner promotion. Explicit guest mode
requires an app-owned new-empty policy, not rights on old data.
The Native capture and transactional link hooks own the actual dual-proof
assertion; no separate unused verifyLegacy hook is exposed.

Private feedback draft is bound to its original context; unknown ACK retains
the exact request/body/media. Source support/reporter roles remain Native-owned.
Media is limited to3 attachments/1MiB/120s, actual container/frame validation,
no ASR/OCR. Source canonical DTO contains no Native principal/token or public
author/reputation claim. Public/managed reviews are not enabled by this package.

## S2 gates still required

New ordinary-app Source with Native + encrypted RP + immutable exact receipts
in its own actual SQLite authority; independent Source realm/client/resource
using the same adapter with no Root code/DDL diff; fresh/final Native SQL role
check; owner-only support; Source restart and unknown COMMIT ACK; actual
installed Apps8 channel + HTTP/WS + browser/native consent. Long/resume requires
the actual finite24h RP consumer, renewed proof/alias receipts and real wall-time
gate. Remote Root→user-loopback transport and production/TLS are not accepted
by S1 or by a declaration in metadata.
