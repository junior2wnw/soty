# Core communication release

World6 remains the default physical format. The compatible reader accepts both
6 and 7; `SOTY_WORLD7_MIGRATION=1` explicitly migrates to 7. After this migration,
fallback must also read 7 on the latest DATA. Do not restore an old snapshot or
start the old World6 reader to roll back code. See `core-reader7.md`.

World7 advertises `profile.chatAttachmentsVersion=1`. Rooms and DM use the same
signed chunk protocol, durable browser outbox, original file hashes and metadata
references. Limits: 10 files / 25 MiB per batch, 25 MiB per file, 256 MiB per
account, 2 GiB per store, 20,000 metadata IDs, 24-hour unfinished-upload TTL.
Terminal IDs remain as tombstones; freeing bytes does not free this record cap.
DM file grants belong to the original contact relationship and are not revived
by removing and adding a contact again. Text-history policy is unchanged.

Background Web Push is OFF unless `SOTY_PUSH_ENABLED=1` and the explicit
`SOTY_PUSH_VAPID_SUBJECT`, `SOTY_PUSH_VAPID_PUBLIC_KEY` and
`SOTY_PUSH_VAPID_PRIVATE_KEY` form a valid matching configuration. OFF creates no
push database. Never print these values or copy them into a source checkout.
Private backups of configuration must be encrypted.

The person enables notifications in Menu → Profile → Notifications. Permission
is requested only from that visible button. Closing settings keeps the
app-lifetime controller; closing the app keeps its background binding. Account
switch clears only the prior binding; same-account SDK revision updates refresh
the controller without weakening an in-flight identity fence.

Only generic notification text is displayed. Every dispatch/retry rechecks
active device, membership/relationship, block, read and mute. Clicking separately
checks the exact current conversation; read/mute do not revoke navigation.
Unfinished work blocks navigation and offers a visible signed retry. Payload IDs
and presentation bindings are never permission grants.

Delivery is best effort, with finite queues, retry/TTL and host-only overflow
counters. A successful synthetic transport, OS-card dedup test or real browser
service-worker controller does not qualify delivery to a closed production PWA.
Check that separately with a configured server and a consenting test device.
