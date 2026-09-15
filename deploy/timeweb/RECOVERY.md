# Partial recovery on 15 September 2026

The source `dev` host became unavailable after DNS was changed and before its
Docker data volume was copied. This is a partial recovery, not a completed data
migration. Do not expose the full application or remove the connector maintenance
marker on the strength of a passing inference test.

## What is preserved

- The deployed application image is built from `f033bafb42b1d4f272c7121425030f3583680680`.
- Both application API credentials, upstream credentials, TLS certificates and
  64 traffic bridge credentials were copied before the outage and compared privately.
- A Windows DPAPI backup captured at `2026-09-13T22:38:49Z` was recovered into
  `/srv/soty/data`. It contains 10 connector bindings, 7 access grants and 327 jobs:
  293 succeeded, 16 failed, 16 cancelled and 2 queued.
- SQLite integrity, token bindings, grants, job inputs, native results, event
  history and execution state match that snapshot. Neither queued job was leased.
- Both public chat completion paths work with the existing application and
  recovered agent credentials. DeepSeek and MiniMax aliases use MiniMax-M2.7.

## What is missing

The last source inventory contained 13 connector registrations and a roughly
114 MB data volume with 937 room files and additional account/identity/traffic
state. That volume was not transferred. The older connector snapshot contains no
durable request identity history, so enabling dispatch could replay work accepted
after the snapshot. Later registrations and grant revocations are also unknown.

A separate encrypted browser salvage archive preserves this PC's 128 local
links (126 archived) and 40 text snapshots captured by the app on 14 September.
It is not a complete server backup: it does not
reconstruct original Yjs update identities, attachments or other users' data.
Do not seed live shared documents from plain text and call them exact restores.

## Serving state

`connector-maintenance.json` blocks job creation and leasing. Caddy exposes only
the two inference POST routes; all other routes return 503. This also prevents
stale access grants or missing room ownership records from becoming public.
The recovery health check explicitly requires both inference configurations,
readable storage and the maintenance marker. Full `/ready` remains 503.

Production currently uses `/etc/soty/recovery.override.json` and
`/etc/soty/maintenance.Caddyfile`. The checked-in `compose.recovery.yml` and
`Caddyfile.recovery` describe the equivalent deployment for a fresh checkout:

```sh
docker compose --env-file /etc/soty/release.env \
  -f deploy/timeweb/compose.yml -f deploy/timeweb/compose.recovery.yml up -d
```

Both public domains must resolve to the target. At the last check the primary
domain pointed to Timeweb while `soty.pochinit.online` still pointed to `dev`.
Existing DNS caches can retain the previous address for the old 86400-second TTL.

## Recovery evidence and next step

Encrypted snapshots and safe comparison receipts are in `/srv/soty/recovery`.
Encrypted off-server copies and the DPAPI-protected recovery key are retained on
the operator's Windows account. Never print the decrypted contents or commit them.

If the source disk or provider backup still exists, mount or export it without
starting a second writer. Recover the complete original data volume, including
SQLite WAL files, into a separate directory. Preserve the current recovered state
before reconciliation. Compare connector tokens, grants/revocations, request IDs,
native job results and room/account data before changing the serving mount.
Leave uncertain queued work fenced until its prior execution can be reconciled.
Only then remove maintenance and expose the full application.

Without that source data, complete recovery cannot be claimed. Client exports can
salvage individual accounts and texts but require an explicit recovery decision
and do not prove preservation of all shared history.
