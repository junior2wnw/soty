# Scoped Stage renewal

Stage enables the same bounded load/timer lifecycle for the two launcher-validated
profiles `soty.selected-human-embed.v1` and `.v2`. Arbitrary/future profile strings
never arm it. The timer is one per Stage, runs every30s only while current/visible,
and is disposed with the Stage. A late failure from a retired generation is ignored.

This schedules an attempt only. `createScopedSlotRenewal` still requires the exact
current Root binding and private installed-channel Source ACK before committing
a new slot. A client boot hint is insufficient; expired Basic has no long witness.
Capture still checks BOTH current deadlines with the same trusted190000ms minimum.
There is no TTL extension, new transport/permission, Connector regeneration or DDL.

The change addresses the previous literal v1-only Stage condition. Unit lifecycle
tests use an EventTarget/timer fixture and exercise v2 with a missing Source ACK.
Real Stage/Source renewal, wall>300s, voice120 and current-actor rejection remain
mandatory browser gates on exact immutable builds. An earlier intermittent HTTPS
completion400 remains unknown; this UI change does not claim to fix it.
