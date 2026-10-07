# Standard2 / Native Source2 — actual local installed/browser acceptance

Own checkout `C:/Users/Junio/.codex/worktrees/soty-source-app-sdk/соты`, branch
`codex/soty-source-app-sdk`, base07e21ed. Historical S1/Source1 checkpoint57ad7d6
remains immutable. Original repositories, production, user data and queues are
unchanged. Root secure-resume fix598986a was composed locally asd0436ab; it is
not a Source RP protocol change. Maintained openid-client6.8.4 and SourceRP49
files retain the exact SHA values in `source-app-sdk-acceptance.md`.

## Closed contract

| Profile | Exact semantic pin | Scope |
|---|---|---|
| `soty.standard-resource`1 | b646578a1cbace022bdcc44147e0ad56b4e9d6239250726c4bb2decc4e4de721 | Frozen Native callback/Source1 historical slice |
| `soty.standard-resource`2 | f68eee3d56a1da3269e8586783a457c4eef24e2673088dbd4951cdf0008e2b84 | Native proof before actual embed HTTPS callback |

One compiled route adapter, kind`soty.resource.v1`, handles both independent
Source realms. Connecting realm2 changes no Root code/DDL. Profiles/targets/
clients/resources still need real private admission; metadata/pins are not
Native permission. Old binaries without this compiled pin refuse it. Compose
the registry addition into current Main without replacing HIVE2/Planner routes.

Native independent session/current resource proof is captured BEFORE OIDC.
Source stores the proof encrypted, tied to original H/Native generation/
membership revision/resource incarnation/binding digest/deadline. Source-owned
remember/recover hooks assert current Native SQL authority; caller JSON cannot
provide that proof. The existing maintained RP49 uses persisted PKCE/nonce and
cryptorandom43 H as state, with no vendor edits or new auth protocol.

Code exchange happens once, after durable claimed→exchanging CAS and current
Root/Native fences. Linking, Native consent, Source session and immutable
completion receipt commit in one Source SQL transaction with final Native check.
Unknown exchange requires a NEW explicit login; the old code is never retried.
After COMMIT/lost ACK, explicit login reads the same private H/completion under
the SAME original current Root actor/device/target/source/ref and fresh RP/
Native authority. Root re-registers only its one-use completion correlation;
no code exchange, new link, new grant, new alias or Basic resume occurs.

The callback exposes only a fixed relative
`/api/embed/complete-link?intent=<opaque one-use locator>`. Locator alone is
not authority. HttpOnly Source cookies stay in the exact Root broker slot,
never browser DTO. Original Root mapping/current Native checks govern consume.

## Native Source2 reader and migration

Source2 has17 tables +3 immutable triggers (20 objects). The added
`native_login_proofs` row is encrypted and immutable, FK-bound to Source intent.
Independent literal reader2 accepts exact1,2; frozen literal reader1 refuses2
before writer startup. New empty DB initialization and migration1→2 are explicit
startup options; no request-time ALTER. Migration replaces only Native meta
format constraint and adds the proof table/trigger. Actual preexisting rows,
Native IDs, sessions, ciphertext and receipts compare equal; foreign_key_check
is empty. Cold copy/restart and current compatible profile1 baseline preserve
bytes. Frozen57ad without reader2 is NOT a fallback after write2.

Native content is not encrypted merely because RP fields are encrypted. Whole
database/backup encryption is a separate deployment obligation.

## Actual checks

Pinned Node24.19.0/pnpm10.30.0. Source suite **29/29 PASS,0skip**, Root TSC and
strict public declarations PASS. Existing independent Native two-OS/current SQL
commit/revoke/support/receipt tests remain enabled. Added gates:

- Actual maintained Root signed OIDC + Source pre-OIDC Native proof and Source
  restart before callback; Native revoke prevents link/session creation.
- Exact Standard2 Native consent page only uses Referrer-Policy:origin and
  form-action self + constructor-approved exact issuer.origin. Failure pages/
  profile1 remain self-only. Null/missing/foreign Origin, valid-shaped forged
  Referer, same-host wrong port and extra query cannot extend authority/CSP.
- Native1→2 independent reader/FK/row/cipher/receipt/cold/compatible checks.
- **Installed2/2**: actual signed Apps registration/private admission, real
  locally built connector child HTTP/WS, Root Human, two Native SQL realms;
  exact mutation receipt/restart/replay, support/private/revoke denial.
- Source callback COMMIT/wire loss consumes Root H map; reload403; Source
  restart + explicit same-H readback keeps one interaction/session/link/consent.
  Foreign close, Native revoke and Root restart deny recovery.

Actual Chrome via pinned Playwright CLI0.1.19, fresh isolated vault, no injected
Human/Source cookies, no mocked routes, no artificial user pacing:

- **Happy path13 checks**: Native HTTP loopback consent → real HTTPS Root UI /
  signed browser Connect → HTTPS embed callback / fixed completion → same
  iframe. Two NEW-empty Native realms each principal/link/consent/item1;
  Source private feedback A1/B0; Source restart/private route/revoke checks.
  Mobile390 Parent/Source overflowfalse and visible Source buttons≥44.
- **Recovery6 checks**: actual callback SourceCOMMIT/lost ACK, consumed Root map
  reload403, Source restart, explicit Source connect button → readonly receipt
  with no second identity exchange. Same iframe and exactly one durable intent/
  session/principal/link/consent. Token exchange200=1, userinfo200=9,429=0.
- Natural happy flow token200=2,userinfo200=20,429=0. No ingress limit raised,
  authorization cache added or pacing used to hide traffic.

Safe mobile artifact:`output/playwright/source-app-ordinary-mobile.png`.
Synthetic Root/source authority is real; fixture operator grant/restart/drop/
revoke helpers are DEV-only and never production authorization. Only the Root
app grant is provided by a helper. NEW-empty Native rights are created by the
actual Source policy after consent + OIDC, not by injected Native sessions.
Existing Native account linking is covered separately by the actual independent
Native session/immutable association gates, not claimed from empty-guest proof.

The actual browser found and corrected Native form Origin:null, blocked issuer
303 by form-action self, and send-before-feedback-context. Native form now
permits only the reviewed issuer; Source UI polls real readiness with bounded
backoff in the same iframe and disables sending until context/recipient/right
arrive. It never reports a new Root slot as Basic recovery. Earlier failed
fixture runs are not counted as acceptance; a Vite h2 test-harness Host adapter
failure was removed. TLS uses HTTP/1 fixture front/backend and an ephemeral
trusted certificate, never NODE_TLS_REJECT_UNAUTHORIZED=0 or global OS trust.

## Reproduction and exact bounds

1. `node modules/source-app/scripts/build.mjs` (portable server/browser/example,
   outputs + source SHA provenance in ignored dist).
2. Build the CURRENT combined Root connector with Standard2 compiled before
   `node --test modules/source-app/test/*.test.mjs`. Do not replace Main's
   HIVE2/renewal/FIFO binary with this branch's historical generated artifact.
3. `node scripts/fixtures/source-app-ordinary.mjs`; open the emitted synthetic
   URL with its private ephemeral browserConfig in isolated Playwright Chrome.
   Run `source-app-ordinary.browser.mjs`. A fresh fixture/browser is required for
   `source-app-ordinary-recovery.browser.mjs`. Close the fixture/browser afterward.

**Basic300/original Root slot only.** No long-session/renewal/resume on a new
slot, old Root restart or expired authority; renewable:false. Source account
switch UI, PG async-authority SPI/consumer, feedback agent jobs/ASR/OCR, managed
reviews and production release are separate gates.

**Same-machine Native loopback only.** A remote user's localhost is not the
publisher's SSH Dev localhost. Remote publisher needs browser-reachable reviewed
Native/public origin, Source-owned TLS/entry/Native proof, private broker/admission
and deployment acceptance. Native origin equal to Soty embed does NOT work with
the current compiled router: it does not forward `/soty/connect|authorize`.
The runnable example listens on an explicit local port; it is not a public
deployment pipeline. Ephemeral test TLS is not public DNS/production TLS proof.
