# Root UI host publisher

`host-publisher.mjs` installs a finite, sealed public release and atomically
switches `/srv/soty-ui/current`. Routine UI updates do not change Caddy or its
process, need no encryption key and never open Root DATA. Backend replacement
is unnecessary when the UI/API contract remains compatible.

## Normal publication: one plan, one command

Compose `createUiRelease(candidateDist, [currentReleaseRoot])`, write the stage
with `writeUiRelease`, and save `{id,manifest}`. Retain every current immutable
URL with identical bytes. Use one plan:

```json
{
  "schema":"soty.ui.host.v1","action":"publish","transaction":"<32 hex>",
  "releaseId":"<manifest SHA>","receiptDirectory":"<private root-owned directory>",
  "base":"/srv","input":"<public stage>","manifestFile":"<manifest.json>",
  "expectedReleaseId":"<current release SHA>"
}
```

```sh
node deploy/ui/host-publisher.mjs execute publish-plan.json
# Only if the response is lost, or to read the current result:
node deploy/ui/host-publisher.mjs inspect publish-plan.json
```

Execute requires Linux root and `/usr/bin/flock` (util-linux). It verifies or
installs `releases/<id>` with the existing exact inventory, seals files444 and
directories555, checks current release CAS and retained immutable bytes, then
atomically renames one root-owned relative symlink. Only `current` may be a
symlink, exactly to `releases/<64 hex>`; public inventory still forbids symlinks.
The permanent owned `.publish.lock` uses a kernel lock: concurrent execution
fails, process exit/SIGKILL releases the lock. Never unlink the lock inode.

One fsynced transaction journal precedes the switch. `inspect` changes nothing:
it verifies the actual link/release and reports `published`, `not-applied` or
`changed`, including the original journal phases. Never replay an unknown
operation or change its receipt directory. A known failed operation can be
followed by a new reviewed transaction after inspection; partial unsealed
directories are refused. Keep old releases. Rollback uses the same publish
command with old mutable UI composed with **all current immutable URLs**.

`reloadRequired:false` means no reload was needed; it does not claim HTTP or
browser acceptance. Check the public index, new/retained assets and protected
routes after publication. No new backend build or full Root identity packet is
required merely to publish compatible static files.

## One-time stable-root bootstrap

For an existing literal release path, use the encrypted/pinned Caddy mechanism
below once, deriving the candidate from the approved unwrapped canonical with
`root:"/srv/soty-ui/current"`. Initialize that verified pointer using publish
with `expectedReleaseId:null` before applying the bootstrap. Confirm the
pointer still names the intended release immediately before apply. Stable
matchers derive from the same fixed/hashed public-file policy as the installer;
the configuration is independent of release manifests. GET/HEAD, Root200,
dynamic security headers, project/HIVE exclusions and unknown-route fallback
remain intact. This bootstrap preserves TLS/auth/other ingress unchanged.

The retained `install/apply/rollback` operations below support this migration
and ingress repair. They are not part of a routine UI update. `config-swap.mjs`
supplies their authenticated AES-256-GCM backup/readback and atomic replacement.

## Build the operator closure

Use the reviewed full Git revision and its frozen dependencies. Copying only
`release.mjs` is invalid: its Source SDK contract/public-release imports include
the maintained GPG tar inventory. Either stage the exact full source closure or
bundle it, keeping only Node builtins external:

```sh
pnpm exec esbuild deploy/ui/host-publisher.mjs --bundle --platform=node --format=esm --target=node24 --packages=external --outfile="$OWN/ui-publisher.mjs" --metafile="$OWN/ui-publisher.meta.json"
node --input-type=module -e 'import fs from "node:fs";const m=JSON.parse(fs.readFileSync(process.argv[1]));if(Object.values(m.outputs).flatMap(o=>o.imports).some(i=>!i.external||!i.path.startsWith("node:")))process.exit(1)' "$OWN/ui-publisher.meta.json"
sha256sum "$OWN/ui-publisher.mjs"
```

Record exact revision, bundle SHA, Caddy2.11.2 binary SHA and Node24 SHA. All
operation directories must be fresh, private, owned by the operator and outside
every Docker bind containing application/configuration data. Keep key material
in memory/anonymous stdin; never argv, environment, a plan, output or a key file.
Config backups are `.enc` only. Config bytes and Caddy stderr stay private.

## Prepare and review exact bytes

1. Independently acquire the current Caddy PID/UID/startTicks and raw `/proc/PID/cmdline`
   SHA, exact config path, owner/group/mode and disk SHA. Reject `--resume` and
   config environment substitutions. Independently qualify the active JSON
   provenance for this exact process (admin read or its actual fresh autosave).
   `verifyActiveDisk` runs the pinned real `caddy adapt`, compares full JSON and
   rereads both files; it does **not** establish the supplied active file's origin.
   Bind its `expectedUid`, disk/active/binary SHA explicitly. It returns no raw
   configuration and does not claim reload success.
2. Obtain the approved **unwrapped canonical ingress** through encrypted custody.
   Read installed current public files with `createUiRelease(currentRoot)`.
   Build the next release from the immutable candidate image, retaining old
   immutable URLs with `createUiRelease(candidateDist, [currentRoot])`; stage its
   exact finite manifest. Install uses that already composed stage, not a
   second reconstruction with omitted retained files.
3. Call `prepareOverlayReplacement({canonical,currentDisk,activeQualification,currentRelease,
   currentRoot,release,root,adapt,host})`. `adapt` must invoke the qualified actual
   Caddy binary with private stdout/stderr. Both overlays derive from the same
   canonical bytes. The reconstructed current overlay must equal the current
   disk **byte for byte**. `activeQualification` is the separate fresh safe result
   from actual-file `verifyActiveDisk`, acquired for the pinned Caddy process,
   with matching disk SHA and admin disabled. Any changed ingress or unreproducible old
   generator is a refusal. Never feed the current wrapped Caddyfile to
   `createCaddyOverlay`: that nests the new handler below the old public UI.
   A later Native ingress change requires a newly reconciled approved canonical.
   Caddy automatically puts the config filename in `file_server.hide`:
   stdin-adapt JSON must not be compared to actual-file-adapt JSON, and this
   difference must never be hidden with a generic JSON exclusion.
4. Pass the returned `baseline`/`candidate` buffers to
   `writeEncryptedConfigs({directory,baseline,candidate,key})`. It writes fresh
   mode600 baseline/candidate AES-GCM files, fsyncs and authenticates readback.
   The returned descriptors contain paths, ciphertext and plaintext SHA only.
   Wipe key/plaintext buffers after use. Preserve the encrypted baseline.

The reviewed apply plan has exactly this shape (decimal `mode`, no signal field):

```json
{
  "schema":"soty.ui.host.v1","action":"apply","transaction":"<32 hex>",
  "releaseId":"<next manifest SHA>","receiptDirectory":"<private owned directory>",
  "releaseRoot":"/srv/soty-ui/releases/<next manifest SHA>",
  "configPath":"/etc/caddy/Caddyfile","profile":{"uid":0,"gid":0,"mode":420},
  "process":{"pid":123,"uid":998,"startTicks":"12345","argvSha256":"<SHA>"},
  "baseline":{"file":"<absolute baseline.enc>","cipherSha256":"<SHA>","plainSha256":"<current SHA>"},
  "candidate":{"file":"<absolute candidate.enc>","cipherSha256":"<SHA>","plainSha256":"<next SHA>"},
  "overlay":{"canonicalSha256":"<SHA>","currentReleaseId":"<current manifest SHA>",
    "currentRoot":"/srv/soty-ui/releases/<current manifest SHA>",
    "currentSha256":"<current SHA>","nextSha256":"<next SHA>","activeSha256":"<qualified active SHA>"}
}
```

`overlay` is returned by the preparation function and bound to the two encrypted
descriptors. It is a reviewed preparation proof, not an independent assertion
that some caller-provided active JSON belongs to Caddy.

## Bounded host invocations

Install plan keys: `schema,action:"install",transaction,releaseId,receiptDirectory,
input,manifestFile,base`; `manifestFile` is `{id,manifest}` and `base` normally
`/srv`. The resulting directory must be new and stays sealed mode555, files444.
Existing/partial directories are never reused; only manifest-selected public
paths are copied. No signal is sent during install.

```sh
"$NODE24" "$OWN/ui-publisher.mjs" execute "$OWN/install-plan.json"
"$NODE24" "$OWN/ui-publisher.mjs" inspect "$OWN/apply-plan.json"
# Authorized root/native helper receives only {"key":"<canonical 32-byte base64>"}
# on anonymous stdin from the operator's encrypted custody, never from echo/argv/env.
"$NODE24" "$OWN/ui-publisher.mjs" execute "$OWN/apply-plan.json"
"$NODE24" "$OWN/ui-publisher.mjs" inspect "$OWN/apply-plan.json"
```

Execute requires Linux root; its durable receipt directory must be owned by root
with mode700 (a dev-owned mode700 bind is deliberately refused). Encrypted input
files must remain mode600 and readable by that exact native helper profile.
With a Docker native helper, use an exact trusted image ID, host PID namespace
for apply/rollback or read-only process inspection, private labelled container,
read-only rootfs, network none, logging none, no-new-privileges, cap-drop ALL plus
only required KILL for signalling the differently owned Caddy. Mount only the
exact Caddy directory RW for atomic rename, installed public release RO,
encrypted inputs RO and owned receipt directory RW; no Docker socket or DATA.
Prepare root-owned private custody/receipt directories through the existing
authorized native custody path before invocation; never weaken their modes or
grant broad capabilities to compensate for a mismatched mount owner.
Install needs only the reviewed public input RO, finite public destination RW
and receipt directory RW; no host PID namespace or KILL. Preserve exact IDs if
create/start/response is unknown. A shell wrapper must propagate the single safe
JSON result without private stdout/stderr from subcommands.

O_EXCL + fsynced action receipt is created before mutation. Reusing an action/tx
or changing receipt directory to replay after a lost response is forbidden.
`inspect` is read-only and reports disk SHA/state and bounded phases only.
After `replaced`/an unknown signal, keep recovery required; inspect actual process,
disk and independently qualified active configuration. No automatic retry,
rollback, SIGTERM or process restart occurs.

After confirmed apply, independently verify active=disk via the pinned real
Caddy, then public index/worker/manifest, new **and** retained immutable assets,
API denial/HIVE routes, content hashes and security/cache headers. Only those
readbacks establish publication. Apply always returns `activeReloadVerified:false`.
A supervised reversal uses the same encrypted descriptors/proof and process pins,
`action:"rollback"` (its own one-shot receipt); CAS requires current candidate
bytes and restores exact baseline bytes. Review fresh active/disk/process state
before invoking it, then repeat public/active readback.

## Gates

```sh
node --test deploy/ui/host-publisher.test.mjs
SOTY_UI_CADDY_BINARY=/usr/local/bin/soty-test-caddy node --test --test-concurrency=1 deploy/ui/host-publisher.acceptance.mjs deploy/ui/release-caddy.acceptance.mjs deploy/ui/source-sdk-caddy.acceptance.mjs
```

Run the host gate in an owned Linux namespace as root, network none, without
production mounts or credentials. The stable-current gate proves new hash URLs
and rollback while Caddy PID/config/autosave stay unchanged. Unit gates exercise
a real competing CLI, SIGKILL before/after rename, read-only reconciliation,
immutable retention, CAS and tampered/escaping targets. The legacy gate boots
an **old overlay**, installs/replaces
with a new overlay, checks new index + both assets + protected ingress, checks
actual autosave against real adapt, reverses on the same PID and tests strict
process pins and the lost-response barrier. Set `SOTY_UI_PUBLISHER_BUNDLE` to the
reviewed bundle path to exercise its actual CLI instead of the source CLI.
No production CLI target is used.
