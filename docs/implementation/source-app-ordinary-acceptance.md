# Ordinary Source — S2 Basic300 controlled HTTP/Native acceptance

This is a new example Source with its own SQL authority. It does not migrate
Planner, HIVE, Peremetrika or any user repository/data. Root format/DDL and the
standard adapter do not change when connecting the second realm.

## Implemented artifact

`modules/source-app/examples/ordinary-app` contains the Native domain, encrypted
Source BFF storage, literal independent reader1, runnable server and small
framework-free UI. The package exports `@soty/source-app/example`; its bundled
server runs outside the Root checkout with Node24 + pinned openid-client6.8.4.
Build copies the static example UI and emits source/output SHA provenance.
The bundle's Source store/Native hooks share the same constructor brands as its
included BFF, rather than importing another repository at runtime.

Native source format1:16 tables +2 immutable triggers. Existing unknown schema
is refused before server start; initialization is explicit for a new empty DB.
No HTTP handler runs ALTER/migration. One SQL authority contains Native
principals/sessions/resources/memberships/issuer-sub links/selected consent,
items, private tickets/media/messages and exact actor/resource/request receipts.
Source interactions/sessions and PKCE/AT/cookie/locator state are AES-256-GCM
encrypted with realm/model/id/revision/keyId AAD. Native content belongs to its
own domain; encryption of the whole Native database/backup is a deployment
task, not claimed by encrypted RP fields alone.

Native mutations check current exact session generation/membership revision/
resource incarnation **inside** SQL commit, saving effect + receipt together.
Two actual OS writers with the same selected Native actor/request/input share
that authority and commit one effect/receipt. No JSON authority shadow or
in-memory writer ownership is used. Source support is the Native resource
owner; Root App ownership never supplies that role. Accept belongs to the
reporter and requires explicit ready-to-check state; replay returns its exact
original receipt without repeating the transition.

Default link requires the actual independent Native session + new verified
OIDC proof. Existing immutable issuer-sub association conflict is denied.
Explicit empty-guest policy creates only a new principal/resource owned by
this Source. It cannot grant an existing/private/populated resource.

## Verified gates

Node24.19.0/pnpm10.30.0. S2 **7/7 PASS, 0 skip**:

- Actual maintained Root signed OIDC + two independent Source realm databases
  (`board`,`library`) over HTTP using the same compiled pin; exact same opaque
  Native locator in each realm never merges data/permissions. Root source/DDL
  file hashes are identical before and after the second connection.
- Source restart preserves encrypted sessions, independent Native ACL and
  durable item/ticket/media receipts. Basic continuation is `renewable:false`.
- Actual Source transport loss after COMMIT: readonly exact receipt survives
  restart; one item remains. Explicit same-intent invoke replays the receipt,
  and does not apply twice. Post-COMMIT controlled Root denial returns unknown,
  while private query/result delivery is denied.
- Native participant with Root App owner label cannot manage support status;
  Native owner can send to check, only reporter can accept; PNG bytes persist.
- Revoke during async preparation yields0 items/receipts at final Native SQL
  commit. Native legacy proof/link conflict and new-empty guest boundaries.
- Two real OS Native writers:one receipt/effect. Literal reader1 rejects an
  unexpected shadow table/realm before writer startup with no DDL mutation.

Combined Source SDK +S2 tests are **22/22**; existing Apps8 selected reader/
profile/proof regressions **11/11**, combined **33/33 PASS, 0 skip**. Root
typecheck and separate strict public declarations pass. The standalone
package OS-process gate now also runs the example/public assets and rejects
anonymous protected context. This is a runnable artifact, not a UI screenshot
or synthetic Ready badge.

Native browser cookies in the two-realm gate share one hostname jar, never
port-scoped isolation. Distinct appId intent/CSRF and realm legacy cookie
namespaces permit parallel app logins. Another pending login of the same app
supersedes its older form/callback with explicit409 and does not affect the
other app. Per-intent browser correlation for same-app parallel flows is not
implemented. Embed jars here explicitly model private per-Source broker jars.

## Exact unaccepted boundaries

The Source HTTP test has controlled authenticated MAC IPC; it does **not** prove
an installed connector/channel, real Root Apps registration/admission, browser
UX or production TLS. The callback currently configured by these tests is
Native `/soty/callback`; current Root release policy requires embed HTTPS
`/api/embed/callback`. That admission mismatch is a blocker for Ready. Root
policy is not weakened. A separate reviewed Source-owned pre-OIDC Native proof
capture/state→original-slot correlation will align callback flow before the
installed-channel gate. Native HTTP OAuth callbacks are not released by this
example or its manifest.

Root→remote user-loopback transport, human browser/cold whole Source backup,
finite24h/Root renew-rebind and Source-owned ASR/OCR/triage job grants are still
separate gates. `asr:false` remains truthful. Private text/media is not public
reviews or an executable agent instruction. No production/real account/data
requests, paid calls, external publication or financial effects occur.
