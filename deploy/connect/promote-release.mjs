import { constants } from 'node:fs';
import { createPublicKey, randomUUID } from 'node:crypto';
import { mkdir, open, readdir, lstat, realpath, rename, link, unlink } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { verifyRelease, sha256 } from './update-engine.mjs';

const LIMIT = 12 * 1024 * 1024;
const fail = code => { throw Object.assign(new Error(code), { code }); };
const exists = async file => { try { await lstat(file); return true; } catch (error) { if (error.code === 'ENOENT') return false; throw error; } };
const inside = (root, file) => { const relative = path.relative(root, file); return !relative || (relative !== '..' && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative)); };

async function location(file) {
  if (typeof file !== 'string' || !path.isAbsolute(file)) fail('promote_absolute_paths_required');
  let current = path.resolve(file); const missing = [];
  while (!await exists(current)) { missing.unshift(path.basename(current)); current = path.dirname(current); }
  let ancestor = current;
  while (true) {
    if ((await lstat(ancestor)).isSymbolicLink()) fail('promote_symlink');
    const parent = path.dirname(ancestor); if (parent === ancestor) break; ancestor = parent;
  }
  return path.join(await realpath(current), ...missing);
}
async function regularBytes(file, limit = LIMIT) {
  const before = await lstat(file);
  if (!before.isFile() || before.isSymbolicLink() || before.size > limit) fail('promote_file_invalid');
  const handle = await open(file, constants.O_RDONLY | (constants.O_NOFOLLOW || 0));
  try {
    const opened = await handle.stat();
    if (!opened.isFile() || opened.dev !== before.dev || opened.ino !== before.ino || opened.size > limit) fail('promote_file_changed');
    const chunks = []; let total = 0;
    while (true) {
      const buffer = Buffer.alloc(Math.min(64 * 1024, limit + 1 - total));
      const { bytesRead } = await handle.read(buffer, 0, buffer.length, null);
      if (!bytesRead) break;
      total += bytesRead; if (total > limit) fail('promote_input_too_large'); chunks.push(buffer.subarray(0, bytesRead));
    }
    const after = await handle.stat();
    if (after.size !== total || after.size !== opened.size || after.mtimeMs !== opened.mtimeMs) fail('promote_file_changed');
    return Buffer.concat(chunks, total);
  } finally { await handle.close(); }
}
function json(bytes) {
  // The installed updater parses JSON bytes directly; it does not accept a
  // transport-added UTF-8 BOM. Reject it before making an unusable feed current.
  if (bytes[0] === 0xef && bytes[1] === 0xbb && bytes[2] === 0xbf) fail('promote_json_invalid');
  try { return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)); }
  catch { fail('promote_json_invalid'); }
}
async function syncDirectory(directory) {
  if (process.platform === 'win32') return; // Deployment runs on the Linux host.
  const handle = await open(directory, 'r');
  try { await handle.sync(); } finally { await handle.close(); }
}
async function atomicFile(directory, name, bytes, immutable) {
  const temporary = path.join(directory, `.promote-${process.pid}-${randomUUID()}.tmp`);
  const destination = path.join(directory, name);
  const handle = await open(temporary, 'wx', 0o644);
  try { await handle.writeFile(bytes); await handle.sync(); } finally { await handle.close(); }
  try {
    if (immutable) { await link(temporary, destination); await unlink(temporary); }
    else await rename(temporary, destination);
    await syncDirectory(directory);
  } catch (error) { await unlink(temporary).catch(() => {}); throw error; }
}
function pinnedTrust(bytes) {
  const trust = json(bytes);
  if (!trust || !Number.isSafeInteger(trust.threshold) || trust.threshold < 1 || !trust.keys || typeof trust.keys !== 'object' || Array.isArray(trust.keys) || Object.keys(trust.keys).length < trust.threshold) fail('promote_trust_invalid');
  const keys = Object.create(null);
  try {
    for (const [id, pem] of Object.entries(trust.keys)) {
      if (!/^[A-Za-z0-9_-]{1,100}$/.test(id) || typeof pem !== 'string' || pem.includes('PRIVATE KEY')) fail('promote_trust_invalid');
      const key = createPublicKey(pem); if (key.asymmetricKeyType !== 'ed25519') fail('promote_trust_invalid'); keys[id] = key;
    }
  } catch { fail('promote_trust_invalid'); }
  return { threshold: trust.threshold, keys };
}
function checked(bytes, trust, { sequence, now = Date.now(), lastSequence = 0, version = '0.1.0', historic = false } = {}) {
  const release = json(bytes);
  if (sequence !== undefined && release?.signed?.sequence !== sequence) fail('promote_history_invalid');
  try {
    verifyRelease(release, trust, { now: historic ? 0 : now, lastSequence, version: historic ? release.signed.version : version, channel: 'stable' });
  } catch { fail(historic ? 'promote_history_invalid' : 'promote_release_invalid'); }
  return release;
}
async function history(directory, trust) {
  let highest = 0; let latest; let latestBytes; const versions = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    if (!entry.name.startsWith('release-') || !entry.name.endsWith('.json')) continue;
    const match = /^release-([1-9]\d*)\.json$/.exec(entry.name), sequence = Number(match?.[1]);
    if (!match || !Number.isSafeInteger(sequence) || !entry.isFile() || entry.isSymbolicLink()) fail('promote_history_invalid');
    const bytes = await regularBytes(path.join(directory, entry.name));
    const release = checked(bytes, trust, { sequence, historic: true });
    versions.push({ sequence, version: release.signed.version });
    if (sequence > highest) { highest = sequence; latest = release; latestBytes = bytes; }
  }
  versions.sort((a, b) => a.sequence - b.sequence);
  for (let i = 1; i < versions.length; i++) if (compareVersion(versions[i].version, versions[i - 1].version) < 0) fail('promote_history_invalid');
  let stableBytes;
  const stableFile = path.join(directory, 'stable.json');
  if (await exists(stableFile)) {
    stableBytes = await regularBytes(stableFile);
    const stable = checked(stableBytes, trust, { historic: true }), sequence = stable.signed.sequence;
    if (sequence > highest || !(await regularBytes(path.join(directory, `release-${sequence}.json`))).equals(stableBytes)) fail('promote_history_invalid');
  }
  return { highest, latest, latestBytes, stableBytes };
}
function compareVersion(a, b) {
  const left = a.split('.').map(BigInt), right = b.split('.').map(BigInt);
  for (let i = 0; i < 3; i++) if (left[i] !== right[i]) return left[i] > right[i] ? 1 : -1;
  return 0;
}

// Accept an already-signed artifact only. No extraction, import, tests, build,
// Docker operation or private key is involved in promotion.
export async function promoteRelease({ trustFile, releaseDirectory, bytes, artifactFile, now = Date.now() }) {
  trustFile = await location(trustFile); releaseDirectory = await location(releaseDirectory);
  if (inside(releaseDirectory, trustFile)) fail('promote_trust_inside_feed');
  if ((bytes === undefined) === (artifactFile === undefined)) fail('promote_one_input_required');
  if (artifactFile !== undefined) bytes = await regularBytes(await location(artifactFile));
  if (!(bytes instanceof Uint8Array) || !bytes.length || bytes.length > LIMIT) fail('promote_input_too_large');
  bytes = Buffer.from(bytes); // Preserve these exact bytes; never reserialize them.
  const trust = pinnedTrust(await regularBytes(trustFile, 64 * 1024));
  const release = checked(bytes, trust, { now });
  await mkdir(releaseDirectory, { recursive: true, mode: 0o755 });
  // Shared with the local publisher, so the two entry points cannot race.
  const lockPath = path.join(releaseDirectory, '.publish.lock'); let lock;
  try { lock = await open(lockPath, 'wx', 0o600); }
  catch (error) { if (error.code === 'EEXIST') fail('promote_locked'); throw error; }
  const lockIdentity = await lock.stat();
  try {
    await lock.writeFile(JSON.stringify({ pid: process.pid, operation: 'promote', startedAt: new Date(now).toISOString() })); await lock.sync(); await syncDirectory(releaseDirectory);
    const previous = await history(releaseDirectory, trust), sequence = release.signed.sequence;
    if (sequence < previous.highest) fail('promote_sequence_rollback');
    if (sequence === previous.highest) {
      if (!bytes.equals(previous.latestBytes)) fail('promote_sequence_conflict');
      // Repeating the exact bytes also finishes an interrupted immutable→stable step.
      if (!previous.stableBytes?.equals(bytes)) await atomicFile(releaseDirectory, 'stable.json', bytes, false);
      return receipt(release, bytes, previous.stableBytes?.equals(bytes) ? 'current' : 'promoted');
    }
    if (previous.latest && compareVersion(release.signed.version, previous.latest.signed.version) < 0) fail('promote_version_rollback');
    checked(bytes, trust, { now, lastSequence: previous.highest, version: previous.latest?.signed.version || '0.1.0' });
    await atomicFile(releaseDirectory, `release-${sequence}.json`, bytes, true);
    await atomicFile(releaseDirectory, 'stable.json', bytes, false);
    return receipt(release, bytes, 'promoted');
  } finally {
    await lock.close();
    const current = await lstat(lockPath);
    if (!current.isFile() || current.isSymbolicLink() || current.dev !== lockIdentity.dev || current.ino !== lockIdentity.ino) fail('promote_lock_changed');
    await unlink(lockPath); await syncDirectory(releaseDirectory);
  }
}
function receipt(release, bytes, status) {
  return { ok: true, status, sequence: release.signed.sequence, version: release.signed.version, channel: 'stable', releaseFile: `release-${release.signed.sequence}.json`, stableFile: 'stable.json', bytes: bytes.length, sha256: sha256(bytes) };
}
async function stdinBytes(stream) {
  const chunks = []; let total = 0;
  for await (const chunk of stream) { total += chunk.length; if (total > LIMIT) fail('promote_input_too_large'); chunks.push(chunk); }
  return Buffer.concat(chunks, total);
}
export async function main(args = process.argv.slice(2), stream = process.stdin) {
  if (args.length < 2 || args.length > 3) fail('promote_usage');
  return promoteRelease({ trustFile: args[0], releaseDirectory: args[1], ...(args[2] && args[2] !== '-' ? { artifactFile: args[2] } : { bytes: await stdinBytes(stream) }) });
}
if (process.argv[1] && pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url) {
  try { process.stdout.write(JSON.stringify(await main()) + '\n'); }
  catch (error) { const code = /^promote_[a-z_]+$/.test(error?.code || '') ? error.code : 'promote_failed'; process.stderr.write(JSON.stringify({ ok: false, code }) + '\n'); process.exitCode = 1; }
}
