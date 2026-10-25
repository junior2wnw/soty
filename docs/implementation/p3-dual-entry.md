# One app, explicitly paired Source and Native entries

This additive contract keeps one app ID, owner, publication and discussion. It
does not change Apps DDL, install a second app, move an alias, grant a Human
identity, or turn `nativeOrigin` in a Source profile into public admission.

## Prepare and commit

An owner may add `nativeEntry:{origin,targetRevision}` to the existing signed
`apps.source.prepare` arguments. The primary target must be an approved
`soty.selected-human-embed.v2` Root-signed workspace; the retained Native target
must be `soty.relay-restricted.v1`, owned by the same app/account and connector,
on a different port. Both origins must be currently bound, published aliases of
that app. `origin` must exactly equal the approved profile's `nativeOrigin`.
The Source and Native **hostnames must differ**. Different ports on one
hostname do not isolate host-only cookies and are rejected.

Both exact targets receive independent existing connector candidate probes.
They share one preparation ID, deadline and bounded registry slot. A failed or
uncancellable probe cannot free that slot while its peer is still outstanding.
Owner, policy/target CAS, domains, port conflicts and both private connector
proofs are checked again at commit. No network operation holds an Apps lock.

`apps.source.promote` then additionally requires a fresh explicit
`nativeExposureAck:{scope:'whole-port',targetRevision,targetDigest,profile}`.
It must acknowledge the exact prepared Native tuple. It is independent of the
primary Source `exposureAck` (`authenticated-workspace`), and an earlier Native
publication ACK is never inherited. Paired promotion currently requires
`launchPolicy:'anyone'`; there is no actor-capable restricted Native admission.

The existing transaction atomically persists the primary target/admission,
publication epoch and one `soty.app-source-receipt.v2` /
`apps.source.promote.v2` receipt with its exact `nativeEntry`. Existing
Source-only calls keep their v1 intent hashes and receipts. Exact retries return
the original immutable receipt, separately from current publication state.

## Current authority, revocation and ports

The reader inspects **only the latest overall Source promotion receipt** for
the app. It requires v2 and the current exact primary target. It never searches
history for an old Native ACK. A subsequent Source-only promotion, even back to
the same Source revision, closes the Native entry. Replaying an older paired
receipt does not reopen it. A Native rollback is another explicit ordinary
Source promotion; it needs its own fresh probe and publication ACK.

Publication changes retain the exact pairing, but Native admission additionally
requires current enabled app state, `anyone`, both published bound aliases,
current approved Source pin, retained Native tuple and connector ownership.
Retiring a required alias, disabling/revoking the app, changing the primary
without a pair, or removing the approved host pin closes Native admission.
While the current primary retains a paired receipt, its Native port remains
reserved even if public policy temporarily closes the entry. Both registration
and source preparation/promotion check that reservation. Explicit unpairing or
app revocation releases it. No historical target reserves a port forever.

Owner publication/inspection reads expose an optional `nativeEntry` summary
with `runtimeReady:false`. This is a configuration record, not a reachability
claim. The existing v1 source editor does not silently preserve or acknowledge
Native exposure. An editor offering paired source replacement must display the
two entries and their independent acknowledgment before its signed mutation.

## Fixed operator ingress and Native SDK guard

For the Native hostname, the operator checks `GET /_soty/ingress-check` on the
fixed trusted Root transport, with fixed configuration headers:

```text
Host: <exact Native alias authority>
x-soty-ingress-surface: native
x-soty-ingress-app: <app ID>
x-soty-ingress-primary: <current Source digest>
x-soty-ingress-target: <separately acknowledged Native digest>
```

The current primary connector must still be online and bound to the exact
Source target. The retained Native target does not acquire a second desired
runtime binding. A successful JSON `soty.native-entry-policy.v1` response
contains exact pins, admission request hash, current policy epoch and
`leaseMs:1000`. It sets no cookie and asserts no actor. Missing, repeated,
unknown, stale or cross-origin headers fail closed. The old ingress route and
Source private frame/broker rules remain unchanged.

`createNativeEntryGuard` in the portable server SDK accepts this endpoint and
pins only from operator configuration. HTTPS is mandatory except for a literal
loopback HTTP endpoint. It sends no incoming cookies/tokens/headers, follows no
redirect, bounds the JSON response to 4096 bytes and brands leases privately in
memory. JSON copies are not leases. Its maximum lease is 1000 ms measured from
request start with a monotonic clock; request latency consumes that budget.

For HTTP, acquire a fresh lease before a new effect, use `assertCurrent(lease)`
at the Native final synchronous boundary, and acquire/check again before a
private response. For WS/streaming, `openStream({onClose})` gives
`beforeEffect()` (fresh Root check), `assertCurrent()` (before each output and
final Native boundary), and `close()`. A <=250 ms single-flight heartbeat plus
a separate hard expiry closes the stream on timeout, policy epoch/admission
change, denial or Root outage. The hard expiry does not wait for a slow
heartbeat. There are at most128 stream slots and128 outstanding Root requests
per guard, reserved before async work. Capacity overflow is503 without evicting
a live lease. Rejected HTTP response bodies are destroyed immediately, including
an error body that never ends. These settings can only be shortened, not widened.

Native applications still independently enforce verified issuer+subject, their
own sessions, CSRF/Origin and domain ACL. Headers and app-publication leases
must never supply a Native UID or authorize an embedded Source frame. A proxy
only checking WS upgrade is insufficient for active stream revocation.

Root and Native databases are not atomic. Root state can change after a fresh
check and before a Native commit; a still-live lease permits a bounded race.
Once a Native effect committed, losing the final check/response is UNKNOWN,
not rollback or permission to replay a write. Already transmitted bytes cannot
be recalled. The 1s lease is a protocol deadline, not a hard real-time promise
under a suspended or blocked process. Connector token audits also retain the
host's separately configured current-channel audit bound.

## Compatibility and promotion gate

An old Root does not understand the new Native surface and its old ingress
cannot admit retained Native1 while Source2 is active. It must fail closed;
it is not an available Native fallback. A compatible rollback can explicitly
promote the original Native target under the current reader, or keep the new
reader while rolling the Source application back to a separately reviewed pin.
Neither rollback edits immutable historical receipts.

Local qualification uses actual Apps8 SQLite, Root HTTP, an authenticated
connector WS and Native HTTP/WS. It covers independent ACK/probes, no-pair
promotion and historical replay, current policy/domain/revocation, port
reservation, forged lease rejection, active stream closure and Root outage.
It does not replace operator TLS/DNS/cutover tests, real Human BFF checks, or
Native SQL integration. Production remains unchanged until those are verified.
