# Planner Source RP and finite slot renewal

Shared maintained SDK is frozen at `725f618a5617b7bb6b17042ab2fe417a3c5f23a0`;
`openid-client@6.8.4` verifies OIDC. Planner owns encrypted durable session heads,
immutable issuer/sub association, current membership/roles and resource consent.
Root account metadata and private continuation locators create no Source rights.

## Human path

Open the approved local app, sign in through Soty, explicitly choose up to24h,
then allow one named Native workspace. Continue in the same embedded project.
Foreground renewal preserves its iframe and does not repeat Native consent.
Disconnect in Planner revokes that resource grant; changing Root profile closes
the original slot/capture. Basic300 stays the default without this opt-in.
Basic readiness on the same fixed route confirms only its current alias and
original slot with fresh userinfo/Native ACL, renewable:false and
deadline=min(actual AT,Root slot). It creates no alias/head/consent and cannot
restore Basic on a new or restarted Root slot.

Root admission/client/source profile and fixed connector routes are host-reviewed
configuration. Metadata does not activate renewal. Missing approval is not-ready.
No arbitrary URL/command proxy, header identity, automatic account merge, manual
MCP configuration or Source-wide workspace permission is introduced.

## Closed continuation and fences

Signed `apps.scoped.renew {appId,handle,requestId}` admits a new five-minute RAM
slot for exact original account+device, current app/admission/target, reviewed
Human identity/client generation, Source/profile and selected resource. No Root
DDL. At most256 renewal requests and256 slots are independently bounded. A lost
ACK retries the same request and existing live committed slot; an expired or
unrecoverable old action never becomes a new intent silently.

Only Root-owned hidden `/_soty/boot?path=/embed&mode=renew` performs the fixed Source
POST `/api/embed/session-continue {requestId}`. Native credentials/cookie stay in
the private broker. Source validates installed-channel MAC/current original
continuation, exact Origin, maintained RP fresh userinfo, own encrypted session,
current immutable link, membership/role and semantic Native grant before/after
await and at its durable CAS/receipt commit. No network call occurs in SQLite tx.
Source encrypted receipt binds one request/session/profile/scope/continuation;
unknown ACK recovery returns that same alias after Source restart.

The child ready message is only a wake-up from the exact hidden WindowProxy and
origin. Swap requires a fresh signed Root context containing the real private
Source channel ACK. An author iframe, same-origin message, declared capability,
locator or cached expiry cannot supply that ACK. On commit old slot/capture closes;
the main iframe DOM stays. Source remains authoritative on every operation.

Finite absolute end is loginStartedAt+24h, never sliding. A RAM client witness
from an actual ACK can permit an attempt when AT expires during preview; it is
bound to the original private slot and source/account generation. It creates no
ready/read/write/swap authority. Explicit not-ready/unknown-refresh, current
authority denial, profile/slot/source change clears it. New ACK must retain the
original absolute end. Unknown started actions retain only their exact intent.
The witness also pins source id/version/digest, opaque handle and available
target revision/digest. Explicit authentication denial clears the pending action
and closes its exact new slot; network unknown retains the requestId. Capture
ensure requires actual ready ACK and BOTH Root/actual AT deadlines; a Root TTL,
missing ACK or historical witness alone never starts a picker.

## Capture and availability

Before picker opens, BOTH actual Source AT and Root slot must have at least190s,
rechecked in the new signed context after any renewal, then capture snapshots
the new slot. A bounded capture lease suppresses only routine automatic rebind
while the180s picker/preview is active; recorder cap120s. It never extends slot
TTL or blocks revoke/profile/source fences. Release is exact once on success,
abort/dispose/timeout, including ignored-abort late media. Old data is discarded
after scope change, never retargeted. Parent selection/Use never submits feedback.

At SDK725 default refresh is only within30s: Source AT with30..190s remaining may
remain too short after a successful Root renewal. Capture then stays denied;
proactive host-only minimum-remaining refresh is a separate SDK/release delta,
not a capability claimed for725 or Source6c.

Foreground tick is30s and can renew below220s remaining. A hidden/background page
is not promised uninterrupted app access: after slot expiry or Root RAM restart,
explicit fresh apps.launch can resume the still-current Source anchor without
another Native resource consent. Absolute24h, logout/revoke/unknown/key change or
missing proof require explicit sign-in. UI-only target promotion requires fresh
Root admission/launch; identical current Native semantic grant can be reused.
Changing resource/realm/authority requires explicit Native consent.

## Source format and release bounds

Planner additive format2 has anchors/heads/rebind receipts/format marker; legacy
Native IDs/history/basic records/agent receipts are not rewritten. Migration is
explicit startup only. Production reader2 and a literal independent reader verify
closed namespace/PK/FK/CHECK, marker and foreign_key_check. The original unguarded
ab378 cannot self-refuse2: external preSTART reader1 guard rejects before START,
and a prepared reader2 baseline is required for fallback/cold restore. Same
approved storage key/profile is required; no backup credential or old RT takeover.

## Actual acceptance layers

Final Root current-byte check with explicit Source6c: world suite1287 total,
1275pass/0fail/12skip (7Windows Unix-listener limitations,5explicit opt-ins;
all8Source-RP HTTP gates included and passed). Current focused client28/28 and
Connect browser21/21, TypeScript/Vite, connector release/update1.4.3 passed.
Connector233169bytes/SHA25613dd286e2ac0e2c81efd67cc5f71641abb734d3be26669d61e783896c46dfb26.
The prior wrong-source full run and its7failures are retained in the local log;
it selected legacy ab without the Source-RP environment and is not passed off as
acceptance. Optional format2 availability is separate from legacy Source tests.

- Shared SDK15/15: actual Root OIDC plus controlled clock/CAS/two OS processes;
  it is not itself a wall-clock browser/Source deployment test.
- Source200/200, MCP32/32, TypeScript/Vite build; reader shape/byte preservation,
  encrypted head/receipt restart, revoke/regrant, key change, bounded GC included.
- Installed actual Source suite8/8,0skip: signed/channel renew/current Source ACK,
  Root+Source restart/fresh launch, foreign/Origin/Native revoke, Root lost signed
  ACK, Source commit-before-wire-loss/restart exact alias recovery, strict Source
  CSP hash over browser-normalized LF script bytes (policy was not relaxed),
  actual Basic same-slot ACK/no-RT/no-new-alias and denial after new slot/Root loss.
- Actual Chrome wall352 from Native-ready: Source GET `/api/embed/state` through
  installed per-app TLS origin200; three signed Root renews and private Source
  receipts, same main iframe. Source restart200 in that iframe. Synthetic only.
- Fake-device actual Chrome120s recording: automatic preview at120046ms,
  readyState4/error=null, native Play currentTime1.335291 after1.4s,1audio/~733KiB,
  iframe retained/private fields absent/submissions0. WebM duration metadata is
  unavailable in Chrome; no fabricated finite metadata duration/ASR is claimed.
- Client lifecycle tests separately exercise capture120/180/AT-expiry, only fresh
  ACK swap, unknown same-intent retry, denied/revoke/profile switch and no sliding.
  Actual Chrome public SDK cross-tab switch removes current preview and original
  iframe before Use/submit. Actual owner Root grant revoke returns Source403
  immediately and cancels preview within its existing30s context tick (observed
  28841ms), not instant visual cancellation. Source-RP gates require its explicit Source format2
  checkout; a legacy Basic checkout can run legacy gates but skips RP gates,
  and a skipped Source layer is never reported as a passed integration.

TLS fixture ignores its ephemeral certificate; production HTTPS ingress, actual
production registry/Native data, candidate/baseline images and encrypted cold
restore are independent gates. No production mutations, user queues or paid
model calls occurred. HIVE/Povédai consumers and typed resource profile evolution
compose through separate reviewed adapters; their release readiness is separate.
