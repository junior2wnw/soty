# Remote TRANSIT profiles: release delta

This change is based on Root revision 75eb1bdf. It changes only two production
backend files: server/transit-access.js and server/http-app.js. The separate UI
and tests retain unrelated behavior from that actual baseline. The previous
Transit usage/fresh-download changes from 454ef35a and f577b564 are included as
small additive code in their original modules.

Enable only after updating the private issuer and checking the current ingress
logger. Existing Root config is backward compatible. Add the optional field
subscriptionBaseUrl with the exact value `<issuer-origin>/transit/subscriptions`
to private DATA_DIR/transit-access.json. It must use the existing HTTPS issuer
origin, without a trailing slash. Keep the issuer internal, existing pool,
claims, signing/delivery keys, mounts, app ID and network listeners unchanged.

The public GET route runs before monitoring/session collectors and never emits
the token in a diagnostic. If actual Caddy access logging is active, exclude
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
