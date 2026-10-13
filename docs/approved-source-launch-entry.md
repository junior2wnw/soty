# Opening an admitted Source

Owner app cards and `apps.inspect` resolve the preferred Source entry from the
current `ScopedAdmissionRegistry.require` result. The address must belong to
the same app and owner, match the approved `embedOrigin`, be bound, and remain
active in the current publication if it is an alias. The current Root access
decision must also match the complete active target tuple.

For selected Source profiles, inspection includes `source.launchEntry` with
only `domainId`, `origin`, and `path`, or `null` when no entry qualifies. The
ordinary Settings Open button uses that entry. It never falls back to another
address when the Source declaration or publication is unavailable. Ordinary
relay sites retain their active-alias-first selection.

This field is navigation metadata. Existing explicit-address launch requests,
runtime entry validation, scoped instance creation, current access checks, and
the exact Source origin fence remain unchanged. Cached or explicit links to a
different origin still fail. The patch neither installs a profile nor changes
an app target, publication, Native permission, identity, or Source SDK.

The existing selected-source promotion fixture exercises actual Apps SQLite,
admission, publication, launcher entry validation, and an actual connector
channel. It verifies a standard-resource Source with a canonical approved
origin despite an active alias, HIVE with an approved alias, refusal of the
other same-app origin, missing or foreign declarations, inactive aliases, and
actor/app revocation. Deployment and ordinary-site tests preserve alias-first
behavior. Typecheck and the normal UI build are required; Live rollout is a
separate action.
