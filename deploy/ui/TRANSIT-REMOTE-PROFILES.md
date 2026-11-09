# Remote TRANSIT profiles: release delta

This change is based on Root revision 75eb1bdf. It changes only two production
backend files: server/transit-access.js and server/http-app.js. The separate UI
and tests retain unrelated behavior from that actual baseline. The previous
Transit usage/fresh-download changes from 454ef35a and f577b564 are included as
small additive code in their original modules.

Enable only after updating the private issuer and checking the current ingress
logger. Keep the old six-field private DATA_DIR/transit-access.json unchanged:
new Root negotiates the private issuer's signed, request-bound AES-GCM features
without allocating a claim and derives the HTTPS subscription base from the
existing issuer origin. Authenticated features cache for 30 seconds through one
shared promise; old/unavailable issuers retry after 2 seconds under a 2.5-second
negotiation deadline. Remote GET lazily negotiates after Root restart. Existing
explicit optional settings retain their overrides, including usageEnabled:false.
Usage is shown only when the actual issuer reports a configured reader; the
private old config needs no extra fields and remains readable by the rollback
image. Old issuers retain signed local file delivery with Remote/usage disabled.
Keep the issuer internal, existing pool, claims, signing/delivery keys, mounts,
app ID and network listeners unchanged.

The public GET route runs before monitoring/session collectors and never emits
the token in a diagnostic. The Root host's existing API path boundary must also
include `/transit/subscriptions/*`. Root's HTTP registration alone does not
publish a path excluded by Caddy. `prepareTransitSubscriptionIngress` in
`transit-ingress.mjs` prepares this exact one-expression extension and verifies
the complete adapted configuration. Qualify actual disk/active provenance,
preserve encrypted rollback, apply through maintained configuration CAS, then
verify a real HTTPS capability GET and all security headers. It changes no
other host, upstream, TLS setting, logging encoder or immutable UI rule. A
repeated or ambiguous boundary is refused instead of guessing.

Also compare the actual public `Referrer-Policy` with Root's response. A general
deferred Caddy security header can overwrite Root's `no-referrer` even when the
GET succeeds. `prepareTransitSubscriptionReferrer` prepends a deferred header
scoped to this path and Root host, retaining the complete existing header,
compression and routing chain. Its complete adapted-tree insertion must match
exactly; qualify it with the real pinned Caddy before encrypted CAS. Ordinary
routes keep their original policy. Public acceptance requires `no-store,
private`, `no-referrer`, `nosniff`, malformed/unknown capability 404 without
metadata, repeated stable credentials and unchanged claim rows. Test raw URI
and token absence in actual readable Root logs and the Caddy journal as well
as the isolated success/unavailable-upstream redaction cases.

If actual Caddy access logging is active, exclude
`/transit/subscriptions/*` with a canonical scoped log_skip rule before enabling
the endpoint. log_skip does not suppress runtime error logs: independently
redact secret path segments in each runtime encoder, including request>uri,
uri and request>headers>Referer. A scoped filter fragment for the canonical
capability path is:

```caddyfile
format filter {
  wrap json
  fields {
    request>uri regexp `(/transit/subscriptions/)[^/?#]+` `${1}[redacted]`
    uri regexp `(/transit/subscriptions/)[^/?#]+` `${1}[redacted]`
    request>headers>Referer regexp `(/transit/subscriptions/)[^/?#]+` `${1}[redacted]`
  }
}
```

Merge it into existing encoders rather than replacing outputs/levels/namespaces.
Validate the exact candidate with the pinned real Caddy and test a synthetic
43-character token against both successful and unavailable upstreams; assert
that no log file/stdout/stderr contains it. The fragment is preparation, not a
claim that actual installed Caddy has accepted it. Apply through the existing
maintained encrypted configuration publisher. Do not replace the live wrapped
UI Caddyfile with the repository example. Preserve the immutable UI overlay.
See [Caddy filtering](https://caddyserver.com/docs/caddyfile/directives/log#filter)
and [log_skip](https://caddyserver.com/docs/caddyfile/directives/log_skip).

UI offers the official sing-box Remote import URI and copy-URL fallback. The
capability remains only in the open dialog, outside the encrypted profile vault.
Opening the panel does not issue access or start an agent. Existing local files
need one Remote import; only Remote clients with automatic updates enabled
fetch future topology. Rotation/revocation disables old update links, not
credentials already downloaded. This limitation is explicit in the panel.

Persistent FakeIP must also remain private to one profile. Issuer projections
now use only `transit-cache-<64 lowercase hex>.db`, derived from network/slot;
the shared client validator accepts precisely that basename or the historical
two-field cache policy, never arbitrary paths. Agent1.4.21 advertises
`runtime.profileCacheIsolation:1`. UI requires that capability before importing
a new file into an older Agent, offers its ordinary updater and preserves
control of an already configured connection. The existing core version check
remains separate. Local historical JSON gains a credential-derived private
cache only in the managed Agent projection. Long-running cache files live in
the Agent's private data directory across core updates; every parallel probe
uses its own temporary private working directory and retains it on unknown
exit. No probe locks, deletes or writes the running profile cache.

The UI publisher excludes Agent artifacts. Therefore this coordinated change
requires a full Root image containing the generated1.4.21 runtime, manifest and
installer, followed by the retained-asset UI publish; a UI-only update does not
publish the new Agent. Publish that compatible client before activating the
issuer's new cache projection. The Root runtime import graph does not itself
import this shared Agent validator. Do not infer artifact delivery from that
import-graph fact or add another static overlay to avoid the normal release.

Before replacing production backend files, freshly compare their actual SHA
against the approved baseline; on drift rebase/review the two-file delta. Ship
UI through deploy/ui/host-publisher.mjs using an immutable build, retaining old
hashed files. Keep encrypted rollback copies and verify public app behavior,
profile isolation, the real HTTPS GET and log suppression. Do not publish a raw
image from an older branch over unrelated Root changes.

Checks run locally: focused Root/Connect and UI controller tests, subscription
HTTP tests, TypeScript validation and Vite production build. The Transit issuer
has separate actual HTTP/topology/restart tests. Device-specific OS deep-link
registration and live public TLS/VPN data-plane checks remain deployment checks.
