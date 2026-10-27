# Soty connector maintenance rollout

This is a separate Soty-only path. It does not import or execute deploy/traffic/rollout-server.sh. It never prints Docker inspect/config/Env/logs, and never writes an env-file. Original Config, HostConfig and requested network endpoint settings are cloned in memory; Image, exact image revision label and transaction ownership labels change. An explicitly reviewed optional application policy adds only its one pathname setting and one read-only nonsecret mount; the complete expected result remains fingerprinted. Anonymous data volumes are explicitly reattached by their inspected volume name. Candidate config is compared again after Docker creation/start, so unexpected daemon defaults fail closed.

No real Docker or production proof is claimed by the unit suite. Before using this code, root/controller must review an exact integrated image (including server/connector-maintenance.js) and run it against disposable synthetic Docker containers/storage, including fault cases. Candidate application image must have org.opencontainers.image.revision equal to the exact reviewed commit. Existing application tokens, auth, traffic, mounts, resource settings, ports, client and 3D processes remain outside rollout mutation scope.

## Operator CLI

The September 14 release used a backend overlay over the inspected current image. That historical `deploy/connector/Dockerfile` now deliberately refuses to build before any source copy or label: the current server requires matching modules, contracts and dependencies. Build the root full application Dockerfile from one reviewed revision. The rollout can use that verified full image; it does not require an overlay image. No future overlay compatibility is implied.

Queued work can be preserved with `--preserve-queued-sha256 EXACT64HEX` on both prepare and promote. Bind the same field in the supervisor receipt. The digest covers the complete normalized set of never-leased queued jobs. Any assigned job, retry history, pending-set drift or missing job prevents admission. The maintenance and rollback helpers receive the same digest; an unchanged count alone is insufficient.

An incident with a saturated legacy writer cannot truthfully claim an empty in-memory write queue. `--legacy-recovery committed-state` is a separate, explicit recovery mode, bound to prepare, promote and supervisor receipt (`legacyRecovery:true`, `legacyNoAssignedJobsObserved:true`). It preserves the final atomically committed legacy file after stopping the exact old process. It does not claim graceful completion of unanswered HTTP calls: those calls may fail during the Soty restart. The old server only acknowledges mutations after persistence, and the same strict offline pending-job checks still reject any lease/admission race. Capture and compare committed job history and authentication bindings in memory across import, returning only equality and counts. Do not stop laptop processes, clear jobs, alter another container or call this a successful write-drain observation.

For ordinary deployments the existing `legacyNoPendingWritesObserved:true` requirement still applies. Recovery mode does not bypass active-job, configuration, storage-readiness, candidate-identity or rollback checks.

Run on the server host with its existing Docker Unix socket and Node24, after supervisor review. Arguments contain IDs/hashes, never credentials:

```
node deploy/connector/cli.mjs prepare --original-id ORIGINAL64HEX --original-image sha256:ORIGINAL64HEX --candidate-image sha256:CANDIDATE64HEX --storage-probe-image sha256:TRUSTEDPROBE64HEX --revision COMMIT40HEX --transaction UNIQUE16TO40HEX --journal /private/rollout-journal.json
node deploy/connector/cli.mjs promote --original-id ORIGINAL64HEX --original-image sha256:ORIGINAL64HEX --candidate-image sha256:CANDIDATE64HEX --storage-probe-image sha256:TRUSTEDPROBE64HEX --revision COMMIT40HEX --transaction UNIQUE16TO40HEX --journal /private/rollout-journal.json --reviewed-receipt /private/supervisor-review.json --health-origin http://127.0.0.1:18182
```

Supervisor receipt fields: approved:true, legacyNoPendingWritesObserved:true, originalId, candidateId, storageProbeImage, configurationSha256, revision, applicationPolicySha256 (null without the optional rule). The CLI checks these against the inspected original, prepared candidate, independently reviewed probe image and exact config fingerprint. This file is an explicit review input, not a cryptographic owner-auth mechanism. Existing owner authorization and controller review are still required. Original must be named soty-online-chat and its exact immutable ID/image must match. Health origin must match the original loopback8080 port binding. The safe journal uses fsync+rename+directory fsync and a single-writer exclusive lock. A retained lock after a crash requires supervised investigation, not automatic deletion or a second rollout.

Prepare creates only the uniquely labelled stopped candidate. It does not enter maintenance or stop the serving container. Promote prechecks no active jobs, stops exact legacy original and confirms stopped, rechecks offline jobs (including queued/leased/running/unknown statuses), then invokes enter. A job created between the first check and stop aborts without migration and restarts the same old container. No job is cancelled. Old c0ca does not honor the marker: confirmed stopped old service is the admission barrier. The legacyNoPendingWritesObserved flag is an external, fresh quiescence prerequisite because legacy c0ca does not expose a pending-write metric; count0 alone does not prove no unacknowledged long write.

Temporary maintenance helpers run the candidate image command `node server/connector-maintenance.js status|enter|leave|rollback`; original Env/mount paths stay in memory. Helpers have no network/ports, readonly image root, no capabilities, 768MiB RAM, 0.5 CPU,64pids, bounded tmpfs. Status must be strictly read-only/no migration. Enter and rollback are offline. Rollback must return compatible history/auth JSON and refuse any accepted durable request IDs or active jobs. Final rollback receipt is validated for legacy-json/count0/bytes/SHA. Candidate readiness requires unchanged ready agentModelProxy/applicationModelProxy default model, transport and application path from the original /health, and /api/connectors/storage-ready `{ok:true,schema:"soty.connector-storage-ready.v1",storageReady:true,maintenance:true}` before leave.

## Code-only release on latest DATA

Use `--rollback-mode latest-data` on both prepare and promote for an already
modern SQLite installation. The prepared journal and supervisor receipt must
both contain exactly `rollbackMode:"latest-data"`; omission or a different mode
refuses promotion before STOP. Omitting the option retains the historical
legacy rollout. Latest-data requires a fresh 32-hex transaction and refuses
queued-job preservation and legacy recovery. It does not enable a schema,
Source, Native, issuer, grant, model, or new configuration input.

Choose this mode only after fresh read-only observation of effective `DATA_DIR`,
the matching mount Destination (`mountedDestination` in operator evidence), exact
serving image/container and all actual store formats. Keep volume/config identities
private; no volume name from an old document is a default. Check real reader
labels and isolated current-data canaries for both original and candidate images.
When all formats/configuration are compatible, this is the ordinary release path
with full backup/restore qualification, not a World migration. A historical World3
bridge is relevant only if fresh actual data still reports World3 and that
separate migration/recovery route has been qualified.

Latest-data approval uses `noPendingConnectorJobsObserved:true` and
`fullStoppedWriterBackupRequired:true`, plus the exact prepared
`originalLifecycle` (StartedAt, restart count and running PID projection).
It does not require or infer `legacyNoPendingWritesObserved`. Connector count0
is not a claim that every Apps/World/Native HTTP request has drained. The
supervised restart may interrupt unanswered calls while retaining acknowledged
durable state. Legacy approval fields and behavior stay unchanged.

This mode runs the pinned host-owned `deploy/connect/maintenance-probe.mjs`
source through the existing bounded maintenance helpers. Only status, enter and
leave are allowed; the marker belongs to that exact transaction. There is no
legacy rollback helper, JSON import/export, SQLite deletion or data restore.
Foreign maintenance, pending work, unknown operations and unsupported readers
retain the existing barrier. Before-admission restoration stops and fences the
candidate, guards the SAME original image against current DATA, checks zero
work and an owned marker, starts it under maintenance, verifies storage and the
original model-readiness hash, then clears only its own marker.

For shell promotion, add `--backup-plan /private/backup.json` to the same
`promote --rollback-mode latest-data` arguments. Use the existing GPG `backup`
plan below: its `containerId` and `operationId` must match the original and transaction.
The file is private mode0600; the qualified helper image and new backup directory
are nonsecret references. Send the existing 64-character key plus newline only
through anonymous stdin (the qualified DPAPI/SSH path); never argv, Env or a key file.
Plan, helper image and key are checked before STOP; the key buffer is cleared on exit.
The backup helper uses the default Docker socket, so this flag refuses a different
`--docker-socket`. Keep the operator's existing fresh capacity/floor/reserve check.

```json
{"operation":"backup","containerId":"EXACT_ORIGINAL64HEX","image":"sha256:QUALIFIED_HELPER64HEX","operationId":"SAME_TRANSACTION32HEX","directory":"/private/release/backup"}
```

Library callers may continue using `runConnectorCli(argv, { fullEncryptedBackup })`;
do not combine that callback with the flag. Without either, promotion refuses
before STOP. Prepare needs no key or backup callback. The callback runs only after
the exact original STOP and confirmed
`RestartPolicy:{Name:"no",MaximumRetryCount:0}`, and before enter or candidate
START. It receives only `containerId`, `originalImage`, `transaction` and
`storageProbeImage`. Wire it directly to the maintained full
[GPG backup](../connect/GPG-RELEASE-BACKUP.md), for example inside the reviewed
existing operator driver:

```js
const result = await runConnectorCli(argv, {
  fullEncryptedBackup: ({ containerId, transaction }) => backupStopped({
    containerId,
    image: independentlyQualifiedBackupImage,
    operationId: transaction,
    directory: freshPrivateBackupDirectory,
    key: memoryOnlyKey,
  }),
});
```

Here `backupStopped` is the export of
`deploy/connect/gpg-release-backup.mjs`, **not** the older `backup.mjs` helper.
The GPG mechanism encrypts/authenticates the complete volume and inspected host
bind sources separately with Docker logging disabled. Verify that the host file
bytes still match the original container's effective file binds: an atomic host
pathname replacement can leave a running file bind on the old inode while the
backup reads the new pathname. Inspect mount names alone do not prove this
equality. Keep old sources intact and use the registry feature input below.
Use its existing
qualified Node24/GPG/GNU-tar and CurrentUser-DPAPI/SSH-stdin custody path; no key
may enter argv, Env, an env-file, logs or an unencrypted artifact. The driver
owns bounded invocation and key-buffer disposal. A valid closed full GPG
receipt for this transaction, with both volume and configuration inventories,
is recorded in the journal. A missing, partial, wrong-transaction or failed
backup prevents candidate START and restores the compatible original. An
unresolved helper or backup outcome (`backup_helper_unresolved` or
`full_encrypted_backup_unresolved`) retains `recovery_required`; reconcile exact
objects before another attempt. A callback that times out must report the
unresolved outcome instead of treating it as a confirmed completed failure.
The callback is an operator trust boundary, not a cryptographic attestation of
arbitrary caller-supplied JSON. Exact-image Linux backup/restore and failure
qualification remain mandatory.

Before backup the exact stopped original must have ExitCode0, OOMKilled:false,
Paused:false, Restarting:false, exited status and PID0, with the same StartedAt
and restart count captured at prepare. The confirmed FinishedAt is also checked
after backup and before restoration. Dirty, missing or changed lifecycle proof
leaves `recovery_required`, restart fenced, without another START. The approval,
prepared journal and fresh running original must agree before STOP.

Leave remains the admission boundary. There is no blind rollback of an admitted
or unresolved transaction. After an admitted release, perform a **new reviewed
latest-data prepare/promote** using the current serving ID/image as original,
the prequalified previous code image/revision as candidate, a fresh transaction,
a fresh full backup and the same current DATA/configuration. Qualify that
reverse handoff on a disposable current-data copy first. Never restore an older
snapshot or use the historical f13/gateway release after account-AI writes.

## Durable storage format gate

Every application START and enabling of automatic restart runs the shared [storage guard](./storage-guard.mjs). After the confirmed original stop, its restart policy is fenced to `no`. Recovery fences/stops the candidate and verifies the original reader against current data before any legacy connector rollback helper, restoration of automatic restart or original START. Failure leaves an explicit `recovery_required` journal and retained containers. Ambiguous application START or restart-policy updates do not race a second writer through recovery.

The maintained source uses full `soty.storage-format.v7` observations and
`soty.storage-start.v7` receipts for Rooms, Apps, Notes, Capabilities,
AppRegistration, Feedback, HumanIdentity, Connect and World. The exact current
source declaration is `currentStorageReaders` in the guard and the root Dockerfile;
the observed original/candidate **image** labels must be inspected independently.
Current source declares World readers6/7 and Apps readers1–14. This does not
automatically qualify an installed image or permit a first incompatible write.
The guard requires explicit `DATA_DIR=/data` and its exact supported sole local
volume mount. Unknown layouts, stores, readers and unresolved operations remain
blocked; a parser's recognition of World3 is not World3 runtime support.

### Historical four-store v3 qualification

The following details record the earlier v3 gate and schema transitions. They
are not current DATA observations or the current nine-store reader declaration.

The historical v3 reader manifest was `io.soty.storage.readers={"version":3,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5,6],"notes":[1,2],"capabilities":[1,2,3]}}`. A full application image must earn its declaration with real tested module/contracts/dependency copies and default-off migration behavior; host support alone does not attest a candidate image. The historical backend-overlay Dockerfile has no reader-label override and unconditionally fails early with a full-build instruction; inherited dependencies and module APIs cannot be inferred from a storage-reader label. Each store has its own nonempty, unique supported-version list. That strict v3 gate rejected older manifests, including on empty stores and when the container carried a newer copied label. An honest v3 `[1]` image was admissible only while those stores were empty/v1. The guard reads the actual immutable image; `createConfig` drops an inherited container reader label. The independently pinned `--storage-probe-image` runs host-owned probe source with one read-only data mount, no copied application environment/config/secrets, no ports/network, a read-only root and bounded resources.

At an earlier local checkpoint, parser recognition of Capabilities3 preceded an actual-image reader3 qualification while images still declared `[1,2]`. This historical boundary does not describe the current Dockerfile. An explicit actual-image declaration still needs the separate full-image/restore gate; parser knowledge never authorizes an incompatible reader.

The exact-key result is `{ok:true,schema:"soty.storage-format.v3",rooms:"empty"|1|2,apps:"empty"|1|2|3|4|5|6,notes:"empty"|1|2,capabilities:"empty"|1|2|3}`. Rooms distinguish fresh empty data, legacy JSON, and v2 SQLite; retained JSON never hides v2. Apps use `/data/apps/registry.sqlite`: marker `soty.apps-registry.v1` requires SQLite `user_version` 0 or 1; markers v2 through v6 require the matching version, known tables/projections and pinned legacy-origin metadata. The host recognizes exact immutable-target, sticky binding-floor, saved-revision and discussion guard definitions. Apps5 adds saved entries; Apps6 adds separate discussion audiences, messages/tombstones, change history, usage and rate records. Apps are empty only if the directory is absent or actually empty; a missing main file beside journal/other files is not empty.

Notes use `/data/notes/notes.sqlite`, versions1/2 with matching lineage `soty.notes.sqlite.v1/v2` and project `soty`. The independent host constants retain the four v1 domain tables, exact FTS5 definition and five shadow tables, columns and two indexes. Notes2 adds the exact STRICT `note_native_creates` table and six immutable-proof/identity guards; its historical tables remain non-STRICT. Capabilities use `/data/capabilities/capabilities.sqlite`, versions1/2/3 with matching lineage `soty.capabilities.sqlite.v1/v2/v3`; twelve v1 STRICT tables and eight indexes remain. Capabilities2 adds the exact STRICT `cap_native_note_intents` table, four indexes and eleven guards. Notes2 and Capabilities2/3 metadata maps require `project_id=soty` and a lowercase32hex `registry_id`, without repair or disclosure. Exact key sets apply also to v1 metadata. Native table constraints and guard/index SQL are independent literal definitions pinned to accepted B1a `5e459abc6afa376861c2032226bd29f78bf0468d`, not candidate imports. Capabilities3 adds four STRICT OAuth tables, ten exact indexes and fourteen guards from committed baseline3 `dc1ae217424b33cca0e9a5b60a6e4e719ea0d991`: 17 tables,22 explicit indexes,25 triggers in total. The literal fixture/provenance separately pin Git bytes, lineage and SQL fingerprints. Missing/changed/extra tables, views, triggers, indexes or columns are refused.

All twelve Notes empty/v1/v2 × Capabilities empty/v1/v2/v3 pairs are independent. An absent/empty store is explicit; orphan sidecars, extra files and symlink/junction/nonregular paths fail closed. Neither a zero-version file nor an unreadable file becomes fresh storage. All databases use normal read-only SQLite WAL semantics, never `immutable=1`. Unknown Rooms3, Apps7, Notes3 and Capabilities4 are refused independently; merely changing a v2 marker to3 is still invalid. The frozen reader1 bridge still refuses genuine v2, including committed WAL. Frozen host2 and domain2 refuse genuine main2/WAL3; an old `[1,2]` image remains incompatible with actual3. Format recognition is not a complete B-tree/row/FTS integrity audit, cross-store effect attestation or authorization proof; malformed uncommitted WAL tails may be ignored by SQLite and are not certified as intact. New table constraint definitions are checked, but application row semantics remain the application's responsibility. OAuth ciphertext authenticity requires a separate AS read/use gate; the helper neither decrypts nor certifies it.

At the historical v3 checkpoint, every successful START/automatic-restart intent recorded `soty.storage-start.v3` with exact container/image, mount hash and all four observed store versions. Old v1/v2 probe output and start receipts cannot authorize resumed operations or be rewritten to manufacture approval. Recovery requires a fresh compatible probe after reconciling the exact Docker operation; an unresolved helper or old receipt stays explicit for operator review. Exactly one explicit `DATA_DIR=/data` is required. Unrecognized or unreadable SQLite, unsupported filesystem topology, volume subpaths, nested mounts, other running writers and unknown/incompatible image capabilities block start.

The reader3 cold fixture distinguishes logical refusal from physical file preservation: frozen domain2 returns `schema_version_unsupported` before its persistent PRAGMAs, but closing the last writable SQLite handle after a crashed writer can checkpoint main2/WAL3 into main3 and truncate WAL. No rows are downgraded; this is still a byte change. Therefore an old application's error code is not a read-only guarantee: the host guard must refuse its START before the application opens the database. Host read-only success/refusal retains strict main/WAL byte checks; SHM equality is not claimed.

Supported storage is a managed single-writer Docker `local` volume with local scope, no driver options and exact matching mountpoint. This profile is checked before and after the probe; bind targets fail closed. The writer scan detects Docker names and lexical source overlap, and does not rule out arbitrary bind/symlink/driver aliases or host/remote writers. Operator inventory must establish that operating assumption. The probe inherits image defaults from Docker, so independently inspect the exact trusted image's Node/SQLite/FTS5 runtime, hooks, healthcheck and image volumes; passing a fresh Env array proves only that the application container environment is not copied. Those historical v3 labels covered Rooms/Apps/Notes/Capabilities only. The current v7 gate explicitly checks Connect and World too; full release/restore qualification remains required.

The historical v3 rollout paths restored the SAME old container/image. Their first v2-to-v3 manifest bridge needed a reviewed bootstrap/recovery route even while Notes/Capabilities remain version1: strict v3 correctly refuses that old image. Normal deployment checks actual old reader admission before stopping the serving container; a separately built fallback is not automatically that old ID. Later native schema2 writes need a genuinely serving reader1/2 baseline without automatic migration, or separately reviewed cold recovery. Before OAuth schema3 writes, a genuine AS-off reader3 fallback must preserve ordinary/original credential authority and existing native effects without requiring an AS key. Full application cold restart, coordinated encrypted backup/restore and a separate Linux3 delta remain gates. Downgrading a marker or relabelling an old binary, disabling an endpoint or rewriting historical receipts cannot satisfy them.

Prior Rooms/Apps limits remain: no JSON-only Rooms reader after Rooms2 writes or Apps5-only image after Apps6 writes; source floor2 also requires real binding-v2 runtime support. Test exact images with synthetic current data before release. Real Linux read-only WAL/FTS5, rootless mount permissions, lost-response reconciliation and cold restore of all related stores remain external gates. No production or Docker execution is implied by local tests. See [the bridge receipt](../../docs/implementation/p4-storage-guard-bridge.md), [preflight](../../docs/implementation/p4-storage-guard-preflight.md) and [Apps6 reader receipt](../../docs/implementation/p3-apps-v6-reader.md).

## Failure boundaries

- Docker request waits have absolute10s bounds, and dropped responses are reconciled by exact owned ID/label plus observed state. Rename/start/stop confirmations never rely solely on response success.
- A missing helper after CREATE is looked up by the same exact name for at most60 polls,250ms apart; CREATE is not repeated. The journal records its nonsecret name, verb, ID when known and safe engine error code. A helper that has already run is never restarted. Unresolved CREATE/START retains the helper boundary, including during leave; no second status helper is launched across it. A late CREATE alone remains stopped with restart disabled and is not evidence of database mutation.
- Before leave, ordinary failure stops the candidate, checks actual rooms and Apps format compatibility, performs checked offline connector rollback when maintenance was entered, restores the original name/restart policy and starts the SAME compatible original ID, verifies its /health, then clears maintenance. Failure to prove this returns recovery_required, not restored.
- An unresolved timed-out Docker stop may still complete later. It is not safe to claim the still-running original is restored. An unresolved maintenance helper might still be mutating storage. Both paths retain the barrier and require supervised exact-ID readback; they do not start another helper or execute blind recovery.
- Leave is the admission commit boundary. If its response drops, status reconciles the marker. Once leave was attempted, this implementation conservatively refuses automatic downgrade on any unresolved outcome; active new jobs/request identities must not be discarded.
- No original/retained candidate container is automatically deleted. Successful maintenance helpers are removed without force/volumes. Storage-format helpers remain stopped as exact-ID evidence. Unknown/failed helper objects remain for supervisor inspection. There is no cleanup of arbitrary names, host services, Docker daemon, Caddy or clients.
- Real Docker endpoint defaults, rootless/usernamespace mount access and static-IP reservations require disposable-container proof. A config fingerprint mismatch or unsupported AutoRemove/container-network original rejects the operation before cutover.

## Checks

The private operator reader creates and starts its fixed read-only Docker exec
once. A complete, validated DTO can arrive before Docker marks that exec stopped.
In that case it polls only `GET /exec/<same-id>/json`, with the original configured
timeout as one operation deadline (10 seconds by default). Every HTTP request and
the metadata-settling wait shares that budget; the server socket deadline stays
unchanged. Terminal exit0, the exact fixed command/identity/privilege/TTY metadata
and the unchanged running container are still required. Invalid or lost stdout,
HTTP ambiguity, nonzero exit and changed metadata never authorize another
CREATE/START or output recovery by replay.

Existing top-level error codes remain. Reader failures additionally carry a
closed `soty.universal-measurement-diagnostic.v1` record: container/exec IDs,
failed stage, observed HTTP status (null if headers did not arrive), safe exit and
identity booleans, error category and bounded timing/poll counts. This metadata
is reader-owned; arbitrary error properties are not serialized. CLI failures
return it and activation failures retain it in the existing journal before
restoration. It contains no stdout/stderr, command arguments, response body,
configuration, Env, credentials or error messages.

The historical feature9 `universal_measurement_unresolved` failure predates this
record. Its purged exec/event metadata cannot establish whether that particular
failure was an exec-state race, transport loss or runtime/socket failure. The
correction does not claim that a historical race was observed. A restored
transaction must not be replayed; a fresh operation requires separate review.

`node --test deploy/connector/*.test.mjs deploy/connect/*.test.mjs` runs synthetic injected-engine and local SQLite tests; the Docker archive test requires its explicit opt-in, and file-symlink coverage requires OS permission. `node --check` on every deploy/connector/*.mjs verifies parsability. There is no dependency install or application rebuild for these standalone Node built-in modules. Root separately builds the integrated application image and tests real synthetic migration/readiness/rollback before controller acceptance.


## Optional reviewed application rule

Pass `--policy-file /home/ai2/.config/soty/application-model-policy.json --policy-sha256 EXACT64HEX` to both prepare and promote only for the reviewed existing `kvartalufa` rule in the [fixed application policy](./application-policy.mjs). This helper accepts exactly that one rule with both existing DeepSeek and MiniMax native IDs. It rejects existing/conflicting policy settings or mounts, symlinks, hash changes and other application scopes. The environment setting names `/run/config/soty-application-model-policy.json`; the one added bind mount is read-only. Existing token bytes/upstream credentials/default model/connector settings remain in their existing configuration. File bytes are checked at prepare, before original stop and before admission. The supervisor receipt and journal bind the policy hash as well as the full resulting configuration hash. A policy file is never generated from, or merged with, credentials.

The original container retains its protected configuration for pre-admission rollback; the rollout explicitly disables its restart policy after stop and restores it only after storage compatibility passes. After admission, restore only a transport- and storage-compatible release/configuration. If a website has switched to MiniMax, restore its matching website configuration before removing the allowed model. A public default-model request is not changed by allowing the candidate model for this one application.

The preservation projection normalizes only two Docker API1.45 defaults proven by disposable roundtrip: OomKillDisable null/false and an absent legacy top-level MAC reflected from the explicitly supplied primary endpoint MAC. An explicit conflicting MAC, endpoint change or enabled OOM-disable remains a mismatch. The candidate reports the SHA256 of the valid nonsecret policy bytes actually parsed at startup; admission checks that digest against the reviewed file hash, in addition to re-reading the host file. No credential digest is published.

## Private personal-agent opt-in

An already enabled installation can prepare a configuration update with the
optional `expectedCurrentPreparednessSha256` field in its `features` plan. Take
this SHA256 from the canonical JSON of the actual current private operator
measurement. Prepare and promote require that exact measurement, Universal ON,
and the original mode0 image. Without the field, the original mode1/OFF baseline
requirements remain unchanged. Existing image-reader, private-file, encrypted
witness, configuration preservation and single-writer checks still apply.
First install a compatible image with the current configuration before enabling
a newer storage format. Keep existing proxy settings in the inspected original
configuration; do not copy unsupported fields from an older operator plan.

An optional closed `installedApplications: { source, sha256 }` uses the existing
installed-application loader. It admits one private, hash-approved `.mjs` file
up to 128 KiB at the fixed read-only target
`/run/config/soty-installed-applications.mjs`, and sets the two existing
`SOTY_INSTALLED_APPLICATIONS_MODULE` / `SOTY_INSTALLED_APPLICATIONS_SHA256`
settings. The real loader validates and closes the module during preparation;
held bytes, identity and the encrypted witness are checked again. This installs
operator-owned code and its catalog; it creates no credentials or grants.

Exact Root-signed workspace2 Source profiles reuse a registered current Human
client without an RP callback. Older Source profiles retain their exact callback
checks. Native actor, app, current child and finite action-grant checks remain
independent of this deployment input.

## Updating an existing selected registry

A registry-only feature update uses the existing `--universal-plan` channel and
the SAME exact mode0 application image for original and candidate. No backend
build or new per-application CLI is needed. Keep `selected.source` at the OLD
private host file, and explicitly approve a distinct NEW private file:

```json
{
  "selected": {
    "source": "/home/operator/.config/soty/selected-current.json",
    "migrationConfigured": true
  },
  "featureSelectedEmbed": {
    "source": "/home/operator/.config/soty/selected-next.json",
    "sha256": "EXACT_SHA256_OF_NEW_FILE_BYTES"
  },
  "expectedCurrentPreparednessSha256": "EXACT_CURRENT_LOADED_OPERATOR_DTO_SHA256"
}
```

These are fields in the complete existing `features` plan; retain its Human,
reviews, personal-agent, account-AI and installed-module inputs. The new field is
closed to `source` and `sha256`. The actual existing startup loader and profile
validators must accept the NEW file. It must append exactly one approved profile
and preserve every prior profile, value and order. Neither file is rewritten.
After a successful switch the NEW file is a live production bind, even if its
filename or parent directory contains `candidate` or `staging`. Retain it and
every other live bind source. Derive the next plan and cleanup exclusions from
the actual current container mounts and loaded registry, not the old filename.

Both files' private bytes, identities and parent chains enter the existing
encrypted witness. Only the NEW file becomes the candidate's fixed RO bind at
`/run/config/soty-selected-embed.json`. The OLD actual bind and loaded registry
must match the witnessed old source and old profile DTO. The entire candidate
configuration must equal its input after projecting just this one Source back;
no Env changes, other mounts, extra mount options or ignored fields are allowed.

Use the existing CLI flags for `prepare`, including `--rollback-mode latest-data`,
`--universal-plan`, separate custody/witness files and reviewed shell origins.
For `promote`, use the same flags, exact supervised receipt and maintained
`--backup-plan` channel or `runConnectorCli(argv, {fullEncryptedBackup})` operator
port above. Without either qualified backup route, promotion refuses before
STOP. Policy preparation/sealing is not runtime admission.

The running server's private operator socket reports its loaded registry, not
a fresh import of the host pathname. The expected-current digest remains OLD
through prepare and promotion preflight. Never CAS the live OLD pathname to
NEW bytes first, or substitute the proposed candidate DTO's digest for current.
After an actual successful new START/admission, take the next operation's current
digest from the newly running operator. Full stopped-writer backup reads OLD
host bytes matching the OLD bind. Pre-admission failed readiness uses the
existing latest-DATA rollback, preserved old configuration and storage guard;
it does not restore DATA, keys, or either registry file.

After a downstream Source2 admission or route promotion depends on the ninth
profile, the old eight-profile Root is no longer a blind operational fallback,
even though its image can read the latest DATA. Review/reverse those admission
dependencies or use a compatible registry/configuration under the normal guards.
Preserve late writes; do not rewind DATA to make a former configuration work.

The existing `soty.universal-rollout-policy.v1` feature plan may explicitly include `"personalAgent": {"source": "/home/operator/.config/soty/personal-agent.json"}`. Omission or `null` sets `SOTY_PERSONAL_AGENT_ENABLED=0`; enabling is refused in `legacy-baseline`. The two-step baseline/features image and storage gates are unchanged.

The operator supplies a private `soty.personal-agent-installed.v1` file for the existing [runtime loader](../../server/personal-agent-configuration.js), with the exact intended account IDs in `accountIds`. The policy reuses that validator without network calls, binds the private bytes and file identity to the existing encrypted witness, and adds only `SOTY_PERSONAL_AGENT_ENABLED=1`, `SOTY_PERSONAL_AGENT_CONFIG_FILE=/run/secrets/soty-personal-agent.json`, and the matching read-only bind mount. Conflicting inherited settings or shadowing mounts are refused. No key, account, credential, provider configuration or authority is generated. Private contents and their digests do not enter the public receipt. Runtime actor/source checks still apply; policy validation does not claim a successful model turn or billing receipt.

## Offline SQLite observation

After the exact single writer has stopped, the probe mounts the inspected /data read-only and copies its bounded recognized store files into private RAM. SQLite reads that snapshot normally with WAL; it may create SHM in RAM. It never uses immutable=1 and never creates SHM in the production volume. Unknown store files and orphan journals remain rejection evidence. Retained legacy room JSON is omitted only when rooms-v2.sqlite is present, matching the original probe priority. File and directory identities, sizes and timestamps are checked again after copying. Symlinks and non-regular files fail closed.

The helper is limited to 256 MiB memory, 64 MiB tmpfs and 48 MiB copied file bytes. Exceeding the budget blocks the rollout; operators must review resource increases as stores grow. This bounded format observation is not a coordinated backup, a restore test, or an integrity audit of the entire production volume. The live probe still reads the existing SQLite view directly. Docker list entries that omit a volume Source are resolved by inspecting that exact container; genuinely unresolved sources and overlapping writers remain blocked.
