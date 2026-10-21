# GPG backup and isolated restore for the Soty release

Status: Source v4 candidate, 2026-10-21. Creating these files does not create a key,
backup, restore, maintenance window, migration or deployment. Root is the sole
operator. Qualify the exact helper and images on synthetic data before using
production. No helper in this packet stops or starts the production container.

## Scope and the rollback prerequisite

The original installation observed by Root has Rooms2, Apps8, Notes1,
Capabilities1, AppRegistration1, Feedback1, Human2, Connect4 and World3 with
field1. These are initial observations, not instructions to assume they remain
current. Recheck them read-only and pin the exact serving container and image
before the maintenance window. Full inspect/Env values must stay private.

The new image declares World6 and needs explicit offline World3→6 and Apps8→14
migrations. Its parser recognizing World3 does not make its runtime a World3
reader. The original image has no World declaration and the current guard must
refuse START on a nonempty World for that image. Do not change its labels to claim
support, forge a storage receipt, select a legacy receipt to evade a fresh
observation, or weaken the guard.

Before any production stop, Root must separately build and qualify:

1. The new release image, including native dependencies and the exact declared
   storage readers.
2. A World-aware baseline bridge that genuinely runs the compatible original
   World3/Apps8 application code and declares only readers it actually supports.
   The bridge is a separately reviewed image, not a relabeled unsupported binary.
   This packet neither builds nor qualifies it.
3. Synthetic originals with all nine actual stores, the known literal World3
   layout, rows/IDs/ACLs, SQLite sidecars and private configuration fixtures.
   Run the whole backup → authenticated isolated restore → data/ACL checks →
   explicit migration → new-image canary and baseline-bridge rollback sequence.

Do not enter the production window until those checks and the recovery key check
pass. Existing `HostController.restore` keeps the migrated SQLite and cannot
substitute for restoring the original complete data volume after a World change.
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

## Migration, switch and rollback gates

Authenticated restore success is not migration, image acceptance or deployment.
Root must independently open the restored stores using the genuine compatible
reader, compare safe row/ACL/ID fingerprints, check SQLite integrity and literal
schemas, then execute only the explicit reviewed offline migrations. Migrations
run on an isolated copy first; back up the complete stopped original again if
the source checkpoint changed. After migration obtain a fresh full v7 storage
observation and the current image's real reader match before START. Confirm
actual call → persistence → reopen and browser behavior through Root's canary.

To roll back a failed new release: retain failed migrated data and logs; isolate
all writers; authenticate both original artifacts and restore each into fresh
owned volumes; check the restored original World3/Apps8 data and private config;
then use the **previously qualified World-aware baseline bridge** with its
truthful readers and a fresh observation. Root's reviewed operator performs the
final controlled volume/config switch and health checks. This helper never
overwrites production, starts an image, changes a guard flag, or pretends that
an unsupported binary can read World3. If the bridge/recovery route was not
qualified, that is a release blocker, not authorization for a bypass.
