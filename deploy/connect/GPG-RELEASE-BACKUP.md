# GPG backup and isolated restore for the Soty release

Status: maintained operator instructions, updated 2026-10-27. These instructions do not create a key,
backup, restore, maintenance window, migration or deployment. Root is the sole
operator. Qualify the exact helper and images on synthetic data before using
production. No helper in this packet stops or starts the production container.

## Scope and the rollback prerequisite

Start with fresh read-only evidence, before choosing a release or recovery path:

1. Pin the actual serving container and immutable image. Establish its effective
   `DATA_DIR` and matching mount Destination (`mountedDestination` in operator
   evidence), volume identity and complete mount/config fingerprint. Keep those
   private. A volume name in an old document is not a production default. The
   maintained guard/helper profile requires explicit `DATA_DIR=/data` and its
   sole supported local volume at `/data`; a different topology needs separate
   qualification and must not be made to match by editing evidence.
2. Use the pinned maintained probe/source to obtain fresh full
   `soty.storage-format.v7` JSON, including World and all nine stores. An old
   observation, parser support or exit0 without that result is not current DATA.
3. Inspect `io.soty.storage.readers` on **both exact immutable images**, original
   and candidate. Match every observed format and qualify the real images on an
   isolated current-data copy. A Dockerfile declaration alone is insufficient.

If both images support all actual formats and configuration, use the ordinary
[latest-data release](../connector/README.md#code-only-release-on-latest-data).
For example, actual World6/Apps14 with compatible readers on both images does
not require a World3 migration or a World3 bridge. Before STOP qualify the key,
exact helper/images, complete encrypted DATA/config backup, authenticated
isolated restore and data/ACL/ID checks. Then take the full stopped-writer backup
for this operation. Latest-data rollback preserves current DATA and requires a
fresh compatible-reader check before the original image can START.

### Conditional historical World3 path

Earlier observations included World3/Apps8. Only if the **fresh actual probe**
still reports World3, and an explicitly reviewed migration is needed, qualify
the offline migration and a genuine compatible World3 recovery image/bridge
before the production window. Its runtime must really support the original
formats and private configuration; an unsupported binary cannot gain readers
through copied labels. Test full backup, authenticated restore, original-data
checks, explicit migration, candidate canary and the qualified recovery route
on isolated owned volumes. This packet does not build or qualify that bridge.

Unknown formats, missing image/restore proof or incompatible original readers
block the release. Do not forge a receipt, relabel an unsupported image or weaken
the guard. `HostController.restore` keeps latest SQLite and cannot substitute
for a complete data/config restore when an actual format migration requires it.
Existing R0 `SOTYBAK1` RSA/AES-GCM helpers remain unchanged. Their envelope,
manifest, callbacks and receipt format are different. This GPG operator helper
is an explicit release checkpoint; it is not silently wired into that controller.

## Host and image qualification

**Require a qualified genuine Node 24 operator executable.** Root reported a
working Node 24.15.0 tool with GPG/gpgconf 2.4.4 and GNU tar 1.35. Select its
explicit absolute path and verify its complete SHA256 against Root's binary
receipt before each qualification. The default host Node 18.19.1 cannot import
the real DockerApi dependency closure: application validation evaluates
`String.isWellFormed()` during module loading. Node18 is unsupported here; do not
polyfill that function or refactor/weaken the application validator to run the
backup. The CLI additionally refuses an unqualified Node major version.

Verify `/usr/bin/gpg`, `/usr/bin/gpgconf`, `/usr/bin/sha256sum`, `/usr/bin/docker` and the Docker API
1.45 endpoint. These are host operator tools, separate from the application
image. A generic Linux image/CI worker need not have GPG; do not attach this
operator qualification to unprepared application-image tests or claim Linux
alone proves tool availability. This packet does not change CI or install GPG
inside the product image.
The helper image is a locally existing immutable `sha256:…` image ID, with GNU
tar supporting POSIX/PAX, nanosecond mtime and the extraction flags below. An
image tag, unexpected image, unavailable GPG or different tar implementation
must fail qualification; do not silently fall back or pull another helper.

Run `<absolute-qualified-node24> --test deploy/connect/gpg-release-backup.test.mjs` from the staged
Source. On Windows, Linux/GPG tests explicitly skip; a Windows-only pass is not
GPG or restore qualification. Root must run the Linux tests with real GPG and
GNU tar. Tests use a publicly hardcoded synthetic passphrase. It must never be
used for real backups. The tests never invoke Docker. Root must separately run
an explicit Docker canary using only new owned synthetic volumes.

The maintained tests also import the exact helper closure in a subprocess and
exercise the actual archive/readback argv and extraction CREATE builders through
closed ports. All three builders explicitly disable Docker logging; the legacy
R0 `archiveHelperArgs` is unchanged and is wrapped only by this new helper.

Docker canary acceptance includes: full bytes, names, numeric owner/group,
permissions and nanosecond mtime after restore; extra directories and empty
files; genuine SQLite reopening; both encrypted artifacts; wrong key, altered
and truncated ciphertext; authenticated traversal/link/bounds negatives; no
Docker call before authentication; existing target refusal; competing writer
and bind-alias refusal; label/identity loss; failed extract/readback quarantine.
Inspect the actual archive, readback and extraction helper logging configuration
as `none` while attached streaming works. A successful stream or `--rm` alone
does not establish confidentiality: default Docker logging can persist plaintext
tar before GPG. Use only public synthetic sentinels for this check, never real
secret logs. Extraction must be configured and inspected as logging `none`
before its START; existing proven-owned helpers are stopped on failure even if
their logging configuration is unexpected.
Capture positive and inverse results rather than treating a helper/import
failure as proof of a property.

## Windows CurrentUser DPAPI custody

`windows-backup-key.py` uses only Python standard libraries and Windows
`CryptProtectData`/`CryptUnprotectData`, CurrentUser scope and UI_FORBIDDEN.
It does not use LOCAL_MACHINE scope. Importing the file has no side effects.
Root must explicitly invoke each operation; this packet has generated no key.

The CLI shapes below are templates; replace bracketed fields with verified
absolute paths and a known SSH alias. They are not an instruction to execute
against production automatically.

```text
python windows-backup-key.py init --blob <new-private-current-user-DPAPI-file>
python windows-backup-key.py check --blob <same-DPAPI-file>
python windows-backup-key.py send --blob <same-DPAPI-file> --ssh <absolute-ssh.exe> --host <known-alias> --node <absolute-qualified-Linux-node24> --node-sha256 <full-public-64-hex-Node24-digest> --helper <absolute-staged-gpg-release-backup.mjs> --plan <absolute-host-plan.json>
```

`init` generates 48 random bytes locally, encodes a 64-character passphrase and
writes only a DPAPI-protected blob to a new file, never overwriting a key file.
Keep the blob in the operator's private profile. Output contains only success,
protection state and the blob hash. `check` proves that the current account can
unwrap a correctly formed key. Root must perform a genuine init/check and a
synthetic remote encrypt/decrypt cycle before relying on it.

`send` requires the explicit qualified Node24 path and its complete public binary
SHA256; neither a missing pin nor bare/default `node` is admitted. Root may copy
its already verified Node24 binary into a new owned mode-0700 operator folder,
recheck the full hash and keep the default Node/global configuration unchanged.
The selected executable and staged reviewed Source must remain under that
coordinated owner-controlled gate throughout the operation.

Before reading/unwrapping the DPAPI blob, two **secretless** SSH calls first run
the fixed `/usr/bin/sha256sum` on that exact path and compare its full digest
locally, then run the pinned executable to require major24 and import the exact
helper closure. The import is not a version string accepted from an unpinned
child. Missing/mismatched digest, wrong major, failed import, unexpected output
or either failed call blocks before DPAPI unwrap and before a key-bearing SSH
operation. Node/helper/plan references use a closed absolute ASCII path grammar
without traversal. These are runtime references, not credentials.

Only after both checks pass, `send` unwraps in memory and sends the passphrase
plus one newline only through SSH stdin. The remote helper forwards it to GPG fd 3, separate from tar stdin;
GPG status uses fd 4. No passphrase enters argv, environment, logs or a plaintext
file. SSH is noninteractive and uses existing verified host trust. Only a strict
allowlist of hashes/counts/booleans is returned; remote stderr is discarded.
There is no automatic retry after a timeout. Resolve the previous operation's
state before another attempt. Mutable key buffers are cleared best effort;
the implementation does not claim to erase every runtime/OS memory copy.

DPAPI custody is tied to the Windows user/profile and its protection keys; the
blob alone is not a portable recovery key. Preserve an independently qualified
recovery route under Root's existing secret policy before deleting any original
volume or retained archive. No extra key export or escrow is created here.

## Explicit plans and offline backup

Stage only reviewed Source and keep the host plan as a regular non-symlink file
with mode 0600, at most 64 KiB. It contains references/IDs, never passphrases,
Env, credentials or full inspect. The CLI accepts only these keys:

| operation | Required fields besides `operation` | Optional |
| --- | --- | --- |
| `backup` | `containerId`, `image`, `operationId`, `directory` | `cap` |
| `verify` | `file`, `expectedSha256` | `cap` |
| `restore-isolated` | `file`, `expectedSha256`, `image`, `operationId` | `cap` |

`containerId` is the exact 64-hex original container ID. `image` is the qualified
local helper image ID (`sha256:` plus 64 hex). `operationId` is a fresh 32-hex
operation ID. Reusing an ID or existing artifact/target refuses instead of
overwriting. `directory` is an absolute **new** private backup directory under
Root's controlled artifact location. Its parent must already exist.
Symlinked parents and any overlap with an original volume/bind source refuse
before directory creation, so an artifact cannot be accidentally written into
the data being protected.

Root first establishes the approved maintenance window, disables restart and
external writers/config rotation through its reviewed operator path, then stops
the original container. The helper requires stopped status, exact `/data` local
volume, no subpath/nested mounts, and no other running writer through the volume
name or an overlapping bind path. It repeats the source/config checks before
success. It is not a lock against a privileged Docker/host administrator; keep
the coordinated writer gate closed for the whole operation.
Docker inspect can return the same mounts in a different array order. The
checkpoint orders a new array by complete serialized mount objects, preserving
every original field/object and duplicate. Only order is canonicalized: RW,
Source, Destination, other mount fields and the full existing container/config/
state/restart checkpoint still compare exactly. Source objects are not mutated.
This profile admits exactly that one data volume plus bind mounts. Additional
named/anonymous volumes, tmpfs or unknown mount types need separate qualified
coverage and refuse here rather than disappearing from a success receipt.

Call `send` with the `backup` plan. Two artifacts are streamed directly to GPG:

* `volume.gpg`: every entry of the original `/data` volume, including all
  application stores, sidecars and non-database files, using a read-only mount.
* `configuration.gpg`: full original inspect including Env, mount/config metadata
  and **all** original bind-mounted regular files/directories. No full inspect,
  config value, file list or private path is printed or stored in an open
  sidecar. Configuration outside the original container's effective Env and
  binds is not discovered automatically; Root must explicitly inventory any
  additional external runtime dependency before relying on rollback.

The original volume archive is never a plaintext file. Private bind bytes are
read into a streaming in-memory tar, preserving relative names and attributes.
`metadata.json` inside the encrypted configuration archive preserves exact
original bind paths and destinations plus their collision-free `binds/000000`
archive mapping. The mapping does not authorize writing to those host paths.
All original bind metadata is checked for change after capture and verification.

Each archive is bounded and validated while encrypting, finalized exclusively,
fsynced, hashed, then fully decrypted/authenticated/validated without extraction.
Only after both pass and the stopped-source checkpoint remains unchanged does
the helper emit success with hashes/counts. Capture that safe receipt separately,
pin both ciphertext hashes and retain the reviewed Source/image identifiers.
No success receipt means no qualified coordinated backup. Encrypted partials
are evidence only; do not promote them by renaming them.

Default limits **per artifact**: ciphertext/plaintext stream 5 GiB, regular file
1 GiB, total regular-file data 4 GiB, 100,000 entries, combined path bytes 8 MiB,
depth 32, component 255 bytes, path 4096 bytes, subprocess wall time 20 minutes.
Configuration metadata is at most 8 MiB. Limits can be explicitly changed in
`cap` only through a reviewed plan; check disk capacity first. Links, devices,
sockets, special permission bits, hardlinked bind files and unsupported tar/PAX
extensions refuse when encountered. This codec supports regular bytes,
directories, numeric owners, basic permissions and mtime only. GNU tar's default
stream and the bind encoder do not capture filesystem ACLs/xattrs, capabilities
or security labels. Root must explicitly qualify that these are absent or not
required for every source tree before accepting this backup profile. If they
are needed, stop and add qualified support first; do not claim this artifact
preserves them. Database rows containing application ACLs are ordinary bytes
and are included; they require the independent reopen/data/ACL checks below.

## Two-pass isolated restore

For each artifact, use a **fresh different** operationId and its receipt-pinned
`expectedSha256` in a `restore-isolated` plan. Root sends the same DPAPI key.
This is the only target model: a new volume named
`soty-isolated-restore-<operationId>` and its isolated extraction helper. There
is no user-supplied output directory, volume name, host bind or production target
option. Both source artifacts remain encrypted and unchanged.

GNU POSIX tar can truncate a PAX metadata header's placeholder name inside a
UTF8 sequence. That metadata name is not extracted. The validator checks the
PAX header's checksum/magic/type/link/size/body and then validates the actual
path: a UTF8-valid PAX `path`, or the USTAR name/prefix when no PAX path exists.
Only unconsumed placeholder bytes are ignored. An unsafe/invalid authoritative
PAX path still refuses even if the USTAR fallback looks safe.
PAX record lengths must consist of literal ASCII decimal bytes before conversion;
USTAR magic/version must match their exact wire bytes. High-bit aliases refuse
even when a lossy ASCII decoder would map them to valid digits or header text.

1. Hash, decrypt and validate the entire archive with no output target. GPG must
   exit successfully and report AES256 + MDC, GOODMDC, DECRYPTION_OKAY and
   END_DECRYPTION. The complete tar must have a checksum-valid POSIX/PAX layout,
   root directory, unique safe paths, declared parents and two zero end blocks.
   Links, devices, absolute/traversing paths, unsupported PAX and bounds refuse.
   The actual ciphertext stream is also hashed; a pathname swap cannot satisfy
   the pinned receipt with different bytes.
2. Only after that proof, require absent volume/container names and create a new
   local volume with a fresh unpredictable ownership label. The volume-create
   response is not ownership proof: inspect it and all mounts. Create the exact
   extraction container through the Docker API, validate its immutable ID,
   image, labels and sole new-volume mount, then start that helper only.
3. Decrypt again into this owned isolated target. Recheck the actual ciphertext
   hash and first-pass plaintext/inventory hashes. A failure can leave partial
   data **only in this new quarantined volume**, never in the production volume.
   First-pass authentication is not permission to accept an unauthenticated
   second-pass result. On failure, stop only a proven-owned helper and retain
   evidence; no automatic deletion, promotion or rollback occurs.
4. Read every restored file through a separate read-only tar helper, compare the
   full sorted content/owner/mode/mtime inventory and recheck ownership/no other
   mounts. Success requires GPG proof, extract exit zero, readback equality and
   the extraction helper stopped. The volume remains isolated after success.

`configuration.gpg` restores to its own **different** new volume. Its sensitive
metadata and bind files stay there until Root explicitly prepares private
runtime configuration. Never print `metadata.json`, recursively copy it to
public evidence, or auto-follow its original host paths.

## Switch, rollback and conditional migration gates

Authenticated restore success is not migration, image acceptance or deployment.
Root must independently open the restored stores using the genuine compatible
reader, compare safe row/ACL/ID fingerprints, check SQLite integrity and literal
schemas. Execute an offline migration only when fresh actual formats require
that separately reviewed change; run it on an isolated copy first. Back up the
complete stopped original again if the source checkpoint changed. Obtain a
fresh full v7 observation and the exact image's reader match before START. Confirm
actual call → persistence → reopen and browser behavior through Root's canary.

For a compatible code-only failure use the maintained latest-data recovery
path with the exact qualified original image and current DATA/configuration.
After admission, a reviewed reverse handoff is a fresh latest-data operation
with its own backup and guards; it is not an unconditional restart or rewind.

If an actual migration needs cold recovery, retain failed data/logs, isolate all
writers, authenticate both checkpoint artifacts and restore them into fresh
owned volumes. Check the actual restored formats, data/ACL/IDs and private
configuration, then select the previously qualified compatible recovery image.
A World3 bridge is relevant only for a checkpoint that actually contains World3.
Account for acknowledged writes and revocations after the checkpoint before any
controlled data/config switch. This helper never overwrites production, starts
an image or weakens a guard. Missing backup, image or restore proof is a blocker.
# Protected Docker volume paths (v5 operator adapter)

Host `realpath` remains mandatory for the artifact parent and every accessible
Source. An `EACCES` for the already validated sole local `/data` volume root can
use a separate physical-isolation proof. Other errors, protected bind Sources,
symlink/special bind roots, unknown filesystem layouts or ambiguous proofs fail.

The proof helper uses the same explicitly qualified pinned helper image, Node24,
no network/ports/logging/socket, read-only rootfs and exact read-only Source plus
artifact-parent mounts. It reads only its kernel mountinfo and root stat records.
Known filesystem types are ext4/xfs/btrfs/tmpfs. Device identity and canonical
filesystem root paths are compared for the Source roots and **parent root plus
the new artifact basename**, so sibling configuration files remain valid. It
does not claim that container `realpath` equals host `realpath`. The artifact
parent's kernel dev/inode must match host lstat; host parent identity and realpath
are checked again before exclusive directory creation. Unknown/escaped/ambiguous
fields are not admitted by a path-string fallback.

The exact helper image, public inline command, nonce label, actual mounts, RO
flags, logging, namespace and resource limits are checked before START and
after exit. The helper is stopped/removed only with its full owned tuple. Any
unresolved helper remains a failed operation. Raw mountinfo roots stay inside
bounded memory/pipe; receipts contain only hashes/counts. GPG, key transport,
archive codec, authentication and ciphertext ownership remain unchanged.

This additional adapter needs a fresh actual Linux qualification, including real
protected-volume admission and real alias geometry controls. A pure unit pass or
a namespace `realpath` observation cannot replace that gate.


## V7: bounded all-container restore ownership inventory

The restore ownership fence now enumerates every stopped/running container in fixed pages of8 instead of one unbounded global metadata response. Docker Engine API1.45 supports all=1, limit and the before filter. Each following page uses filters.before containing the last full64-character container ID. A short page terminates enumeration; an exact-full final page needs one following empty page. No volume/name/label/ancestor/status filter is applied, so foreign bind aliases and stopped consumers remain in scope. Reference: https://docs.docker.com/reference/api/engine/version/v1.45/ and the official version/v1.45.yaml.

The helper checks page/container/mount shape, lowerhex full IDs, duplicates across all pages and cursor progress. It retains only Id+Mounts, discarding Commands, Names, Labels and other unrelated metadata. Missing/null/incomplete mount summaries retain the existing per-container inspect fallback. The caller's original volume/labels/overlap/exact-extractor fence and repeated pre/post-use checks remain unchanged.

Limits are fixed:8 records/page; existing4MiB DockerApi response cap/page;8192 total containers;1025 requests maximum (including final empty page);65536 retained mount records;16MiB UTF8 JSON size for retained Id+Mounts. A monotonic60s deadline is checked before/after each page; the existing request timeout bounds an in-flight request, so this is not a cancellation mechanism. Exceeded/invalid/ambiguous inventory fails closed and returns no partial list. DockerApi global limits and all other calls remain unchanged.

Pagination is not an atomic Docker snapshot. The current restore's repeated ownership/no-consumer checks still apply before/after extraction/readback. Concurrent disappearance, ignored cursor, duplicate IDs, malformed responses or exceeded bounds are refusal/quarantine, not permission to skip records. The helper performs GET only and never deletes/stops/renames any listed foreign resource. No private metadata is logged.

Root measured a separate readonly API page8 pilot:1293 IDs,162 pages, maximum page685287 bytes,21066420 aggregate bytes,9.75s; the ID set matched an independent full-ID docker CLI inventory. This is Root-reported feasibility, not author execution or qualification of this exact helper. Independent review, Node cases and the complete actual Docker/GPG/DPAPI synthetic cycle still qualify V7.

The old62 test assertions/declarations/skips/timeouts are retained exactly; only two extracted-function harness ports now receive the actual new helper and one synthetic endpoint changes to the exact paginated first URL. All29 mount-proof tests and production mount-proof code are byte-identical to sealed V6. New portable inventory tests cover complete enumeration, strict bounds/shapes, dropped irrelevant metadata, ignored cursors, later-page foreign named-volume/bind-alias consumers and the actual restore inspect fallback. No GPG algorithm/key custody/TAR code or capability policy changes.
