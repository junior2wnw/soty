# Returning Soty to dev

The September 2026 return uses the complete original dev data volume and the
newer reviewed inference implementation. A partial Timeweb connector snapshot
must never overwrite the dev room, profile, job or durable-request history.

## Source provenance

- Dev's running image before the return is
  `sha256:c299d87a6f84ed38394f34d876ddeee0ce586562e6a0e66c200749f9fcea329e`.
- Its exact server source is preserved in `e381371` on
  `codex/preserve-dev-runtime-20260924`. It includes the MiniMax hotfixes that
  were present in production beyond the original `6e78bb5` image label.
- The return branch includes `2904528`, the later model selection, streaming,
  validation and bounded provider failover implementation. All dev server
  modules apart from the intentionally superseded inference modules and HTTP
  keepalive settings match this source after line-ending normalization.
- `Dockerfile.return` overlays this complete server source onto the exact
  verified dev image. Dependencies are unchanged; frontend assets and the
  installed traffic binary are preserved byte for byte.

## Data and settings

1. Encrypt both hosts' runtime configuration and recovery backups before
   writing them to disk. Keep the decryption key protected separately.
2. Preserve the existing dev volume. Compare connector identities and token
   bindings in memory. Add a Timeweb-only registration only after checking that
   its identity and binding do not conflict with dev. Never print those values.
3. Preserve every dev job, input, result, event and durable request. Timeweb has
   an older partial job set; do not replay its queued jobs or replace newer dev
   statuses. Merge no conflicting identity or authorization record silently.
4. Use the current complete application-key registry from Timeweb after
   verifying it contains the existing dev keys. Retain dev's traffic addresses
   and credentials; copy the latest inference provider settings without issuing
   new credentials.
5. Before changing the serving container, prove there are no assigned or
   running jobs, drain accepted requests, and take a consistent encrypted full
   data backup. Stop exactly the observed container and retain it for rollback.
   Never run two serving owners against the same volume.
6. Set `SOTY_CONNECTOR_PRESERVE_HISTORY=1` on the recovered dev instance before
   resuming connector writes. The ordinary seven-day cleanup otherwise removes
   older completed jobs and offline connector registrations on the first poll.
   This option retains historical records; grant expiry, stale queued-job
   failure, uncertain execution handling and active queue limits still apply.
   If cleanup has already run, restore only missing terminal jobs and their
   related rows from the encrypted snapshot while offline. Keep every newer
   row, including an expired queued job's terminal outcome, unchanged.

## Validation and cutover

Run the inference, connector persistence and integration suites, type checking
and the build. Validate the image against isolated synthetic storage before
attaching the production volume. Require `/ready`, public HTTPS, frontend
assets, a WebSocket exchange, and authentication with every retained application
key and the migrated connector. Use only neutral synthetic inference probes.

Keep the new release on dev as the sole writer. During DNS propagation, make
the Timeweb Soty virtual hosts forward to dev over verified TLS. Preserve other
virtual hosts and applications on both hosts. Change only Soty's intended DNS
records, then verify authoritative and public resolution. Keep local clients
on the former host working through the same forwarding path where needed.

Record the final commit, immutable image, verified backup receipts, data counts,
key equality results, DNS/TLS and functional checks in the release evidence.
Rollback after acknowledged writes must use the newest dev data, never the
partial Timeweb snapshot.
