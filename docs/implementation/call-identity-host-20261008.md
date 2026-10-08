# P2.2.1 — browser identity binding, SOURCE only

`bindCallIdentity` in `src/platform/call-identity.ts` connects one existing
trusted `createCallLifecycle` owner to the Connect public view and page lifetime.
It returns only a disposer; it has no call-method proxies, transport/media ports,
keys, admission flags or lifecycle factory. `startWorld(root, {callLifecycle})`
attaches it before the first asynchronous startup operation. The default entry
passes no lifecycle and creates no voice owner, media request or working-call UI.
Real host/media/transport ports are still absent.

The adapter subscribes before its initial `getLocalState` read. A local observed
generation fences late initial/resume reads after newer notifications, suspension
or disposal, including A→B→A. The actual SDK serializes local reads and committed
state delivery and invokes `onState` synchronously without awaiting its Promise.
Public `LocalState` contains no local revision or signer; this binding relies on
that trusted ordered SDK stream. It cannot prove freshness of arbitrary forged
or reordered messages, unobserved storage changes, or remote revocation.

Only bounded own data descriptors are inspected; message getters are not called
and profile lists are not scanned. A consistent public schema, matching current
account/device, active current profile, literal non-revoked state and null
notification error produce copied identity IDs. These IDs are metadata, not
authority. Null, revoked, inconsistent, malformed or errored views clear identity
with `setIdentity(null)`; `revoke()` alone would retain the old identity and permit
another join. Same account/device notifications keep the active call. The shared
core fanout invokes every observer synchronously from one listener snapshot and
returns a rejecting aggregate to the SDK while observing all async outcomes; a
throw or hanging observer cannot starve a later identity observer or delay the
report of another rejection.

`notificationError` is the SDK's sticky observer diagnostic, not proof that the
account was revoked. Clearing local call identity on it is our strict fail-closed
policy. Another `getLocalState` does not reset this diagnostic; a fresh client or
reload may be needed before voice can recover. No new diagnostic-reset mechanism
or false revoked status is added here.

The same lifecycle and its actual retained leases survive identity transitions
and world-screen remounts. A reentrant `call_transition` permits one coalesced
newest-state retry after ending the old call; a second failure is terminal rather
than an everlasting microtask loop. Duplicate binding of the same lifecycle,
including reuse after its terminal disposer, is rejected. Binding lifetime owns
identity updates and terminal cleanup; other controllers must not mutate that
owner's identity behind it.
The confirmed identity cache is invalidated before every potentially effectful
setter call and committed only after its successful current-generation fence.
An older B effect followed by a newer reentrant A notification therefore cannot
be skipped because of a cached A value. The original positive ABA failure is
retained as a synthetic ordered-callback counter, not a live SDK occurrence.

Every `pagehide` ends the call and clears identity. A persisted BFCache hide keeps
the world DOM, page listeners and subscription while identity is suspended.
`pageshow` reads current local data with the same fence; it never joins or unmutes.
Reentrant resume delivered by a close callback is coalesced once until the
synchronous hide/identity-clear barrier finishes. The first actual-source
counter failed before this fix and is retained in research history; it is a
synthetic callback seam, not a claimed live browser occurrence.
Deferred resume carries a separate page-transition epoch, advanced on every
hide. A later hide supersedes an older queued resume; multiple reentrant resume
events retain one newest ticket for the current hide. Nested hide callbacks
retain the hide barrier until the outer stack ends. This page epoch is separate
from identity observation, so finishing its own clear cannot invalidate it.
Nonpersisted hide or explicit disposal removes the binding's own listeners and
terminally disposes the call owner. Startup read/mount failure also cleans this
binding. This is our page policy, not physical Safari/Android, permission/LED or
remote-stop qualification.

Continuity here means same-page world navigation only. Existing `openLegacy`
uses `window.location.assign`, so a full classic/games navigation ends this
document's future call. Retained host ownership across those entries is a separate
P2.2.3 requirement; this change does not promise voice continuity in every app.
Per-owner lease capacity does not establish a global limit across tabs or new
owners; trusted host/gateway must retain actual external work and budget ownership.

Offline tests execute the actual TypeScript binding/core/world through transpile
fixtures and the unchanged call lifecycle. They cover identity/read races,
revocation/error policy, real retained synthetic native work, BFCache events,
reentrant cleanup, startup failure and SDK failure propagation. No microphone,
network, model, engine or paid service is called. Signed participant admission and
consent, continuously enforced revocation, SFU/TURN/media, GLM-only provider and
budget reconciliation, physical devices and live call UI remain later gates.
