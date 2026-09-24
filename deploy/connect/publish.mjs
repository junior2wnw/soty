import { createPrivateKey, createPublicKey, randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import { mkdir, open, readFile, readdir, lstat, realpath, rename, link, unlink } from 'node:fs/promises';
import path from 'node:path';
import { createRelease, verifyRelease, canonical, sha256 } from '../../modules/connect/update/index.mjs';

const MAX_INPUT = 64 * 1024;
const MAX_RELEASE = 12 * 1024 * 1024;
const LIFETIME_MS = 90 * 24 * 60 * 60 * 1000;
const fail = code => { throw Object.assign(new Error(code), { code }); };
const isObject = value => value !== null && typeof value === 'object' && !Array.isArray(value);
function exact(value, keys) {
  if (!isObject(value) || Object.keys(value).some(key => !keys.includes(key))) fail('publish_input_invalid');
}
function external(a, b) {
  const relative = path.relative(a, b);
  return relative === '..' || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative);
}
async function exists(file) {
  try { await lstat(file); return true; } catch (error) { if (error.code === 'ENOENT') return false; throw error; }
}
async function location(file) {
  if (typeof file !== 'string' || !path.isAbsolute(file)) fail('publish_absolute_paths_required');
  let current = path.resolve(file); const missing = [];
  while (!await exists(current)) { missing.unshift(path.basename(current)); current = path.dirname(current); }
  let ancestor = current;
  while (true) {
    if ((await lstat(ancestor)).isSymbolicLink()) fail('publish_symlink');
    const parent = path.dirname(ancestor); if (parent === ancestor) break; ancestor = parent;
  }
  return path.join(await realpath(current), ...missing);
}
async function regularBytes(file, maxBytes = MAX_RELEASE) {
  const before = await lstat(file);
  if (!before.isFile() || before.isSymbolicLink() || before.size > maxBytes) fail('publish_existing_invalid');
  const handle = await open(file, constants.O_RDONLY | (constants.O_NOFOLLOW || 0));
  try {
    const after = await handle.stat();
    if (!after.isFile() || after.dev !== before.dev || after.ino !== before.ino || after.size > maxBytes) fail('publish_existing_invalid');
    return await handle.readFile();
  } finally { await handle.close(); }
}
async function syncDirectory(directory) {
  if (process.platform === 'win32') return;
  const handle = await open(directory, 'r');
  try { await handle.sync(); } finally { await handle.close(); }
}
async function atomicFile(directory, name, bytes, immutable) {
  const temporary = path.join(directory, `.publish-${process.pid}-${randomUUID()}.tmp`);
  const destination = path.join(directory, name);
  const handle = await open(temporary, 'wx', 0o644);
  try { await handle.writeFile(bytes); await handle.sync(); } finally { await handle.close(); }
  try {
    // link creates the immutable name atomically and never replaces an existing
    // release. stable uses an atomic same-directory rename after that succeeds.
    if (immutable) { await link(temporary, destination); await unlink(temporary); }
    else await rename(temporary, destination);
    await syncDirectory(directory);
  } catch (error) { await unlink(temporary).catch(() => {}); throw error; }
}
function historicRelease(bytes, sequence, trust) {
  try {
    const release = JSON.parse(bytes.toString('utf8'));
    if (release.signed?.sequence !== sequence || release.signed?.channel !== 'stable') fail('publish_existing_tampered');
    // Historical expiration is not a signing error. New releases are checked
    // against the real clock below; historical metadata still requires its pin.
    verifyRelease(release, trust, { now: 0, version: release.signed.version, channel: 'stable' });
    return release;
  } catch { fail('publish_existing_tampered'); }
}
async function history(directory, trust) {
  let highest = 0; let latest;
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const match = /^release-([1-9]\d*)\.json$/.exec(entry.name);
    if (!match) continue;
    const sequence = Number(match[1]);
    if (!Number.isSafeInteger(sequence) || !entry.isFile() || entry.isSymbolicLink()) fail('publish_existing_invalid');
    if (sequence > highest) highest = sequence;
  }
  if (highest) latest = historicRelease(await regularBytes(path.join(directory, `release-${highest}.json`)), highest, trust);
  const stableFile = path.join(directory, 'stable.json');
  if (await exists(stableFile)) {
    const bytes = await regularBytes(stableFile);
    let sequence;
    try { sequence = JSON.parse(bytes.toString('utf8')).signed?.sequence; } catch { fail('publish_existing_tampered'); }
    if (!Number.isSafeInteger(sequence) || sequence < 1 || sequence > highest) fail('publish_existing_tampered');
    historicRelease(bytes, sequence, trust);
    const immutable = await regularBytes(path.join(directory, `release-${sequence}.json`));
    if (!bytes.equals(immutable)) fail('publish_existing_tampered');
  }
  return { highest, latest };
}
async function publish(input) {
  exact(input, ['config', 'privateKeyPem']);
  exact(input.config, ['directory', 'outputDirectory', 'trustFile', 'keyId', 'sequence', 'expiresAt', 'channel', 'rollout']);
  const config = input.config;
  if (typeof input.privateKeyPem !== 'string' || input.privateKeyPem.length > 16 * 1024
      || typeof config.keyId !== 'string' || !/^[A-Za-z0-9_-]{1,100}$/.test(config.keyId)
      || (config.channel !== undefined && config.channel !== 'stable')) fail('publish_input_invalid');
  const directory = await location(config.directory);
  const outputDirectory = await location(config.outputDirectory);
  const trustFile = await location(config.trustFile);
  if (!external(directory, outputDirectory) || !external(outputDirectory, directory)
      || !external(directory, trustFile) || !external(outputDirectory, trustFile)) fail('publish_paths_overlap');
  let trust; let privateKey;
  try {
    trust = JSON.parse((await regularBytes(trustFile, MAX_INPUT)).toString('utf8'));
    if (trust.threshold !== 1 || !isObject(trust.keys) || !Object.hasOwn(trust.keys, config.keyId)) fail('publish_trust_invalid');
    privateKey = createPrivateKey(input.privateKeyPem);
    const pinned = createPublicKey(trust.keys[config.keyId]);
    if (privateKey.asymmetricKeyType !== 'ed25519' || pinned.asymmetricKeyType !== 'ed25519'
        || !createPublicKey(privateKey).export({ type: 'spki', format: 'der' }).equals(pinned.export({ type: 'spki', format: 'der' }))) fail('publish_key_untrusted');
  } catch (error) {
    if (error.code === 'publish_key_untrusted' || error.code === 'publish_trust_invalid') throw error;
    fail('publish_key_or_trust_invalid');
  }
  input.privateKeyPem = '';
  const now = Date.now();
  const expiresAt = config.expiresAt ?? new Date(now + LIFETIME_MS).toISOString();
  if (typeof expiresAt !== 'string' || !Number.isFinite(Date.parse(expiresAt)) || Date.parse(expiresAt) <= now
      || Date.parse(expiresAt) > now + LIFETIME_MS + 60_000) fail('publish_expiration_invalid');
  await mkdir(outputDirectory, { recursive: true, mode: 0o755 });
  const lockPath = path.join(outputDirectory, '.publish.lock');
  let lock;
  try { lock = await open(lockPath, 'wx', 0o600); }
  catch (error) { if (error.code === 'EEXIST') fail('publish_locked'); throw error; }
  try {
    await lock.writeFile(JSON.stringify({ pid: process.pid, startedAt: new Date(now).toISOString() }));
    const previous = await history(outputDirectory, trust);
    const sequence = config.sequence ?? previous.highest + 1;
    if (!Number.isSafeInteger(sequence) || sequence <= previous.highest || sequence < 1) fail('publish_sequence_rollback');
    const release = await createRelease({ directory, privateKey, keyId: config.keyId, sequence, expiresAt,
      channel: 'stable', rollout: config.rollout ?? 100 });
    verifyRelease(release, trust, { now, lastSequence: previous.highest,
      version: previous.latest?.signed.version ?? release.signed.version, channel: 'stable' });
    const bytes = Buffer.from(`${JSON.stringify(release)}\n`);
    const releaseFile = `release-${sequence}.json`;
    await atomicFile(outputDirectory, releaseFile, bytes, true);
    await atomicFile(outputDirectory, 'stable.json', bytes, false);
    return { ok: true, status: 'published', name: release.signed.name, version: release.signed.version,
      sequence, channel: 'stable', expiresAt, keyId: config.keyId, releaseFile, stableFile: 'stable.json',
      releaseSha256: sha256(bytes), manifestSha256: sha256(canonical(release.signed)),
      fileCount: release.signed.files.length, bytes: bytes.length, publishedAt: new Date().toISOString() };
  } finally { await lock.close(); await unlink(lockPath); }
}

// The command line and environment never carry the configuration or signing key.
// Run this as a short-lived process and send {config,privateKeyPem} through stdin.
try {
  if (process.argv.length !== 2) fail('publish_stdin_only');
  const chunks = []; let length = 0;
  for await (const chunk of process.stdin) {
    length += chunk.length; if (length > MAX_INPUT) fail('publish_input_too_large'); chunks.push(chunk);
  }
  const bytes = Buffer.concat(chunks); let input;
  try { input = JSON.parse(bytes.toString('utf8')); } catch { fail('publish_input_invalid'); }
  bytes.fill(0); chunks.forEach(chunk => chunk.fill(0));
  process.stdout.write(`${JSON.stringify(await publish(input))}\n`);
} catch (error) {
  const code = typeof error.code === 'string' && /^publish_[a-z_]{1,80}$/.test(error.code) ? error.code : 'publish_failed';
  process.stderr.write(`${JSON.stringify({ ok: false, error: code })}\n`);
  process.exitCode = 1;
}
