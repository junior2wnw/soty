import { createHash, sign, verify } from 'node:crypto';
import { mkdir, readFile, writeFile, rename, lstat, readdir, open, unlink, realpath } from 'node:fs/promises';
import path from 'node:path';
import { spawn } from 'node:child_process';

const NAME = '@soty/connect';
const FORMAT = 'connect.release.v1';
const LIMIT = 12 * 1024 * 1024;
const fail = (code) => { throw Object.assign(new Error(code), { code }); };
export function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value && typeof value === 'object') return `{${Object.keys(value).sort().map(k => `${JSON.stringify(k)}:${canonical(value[k])}`).join(',')}}`;
  return JSON.stringify(value);
}
export const sha256 = (data) => createHash('sha256').update(data).digest('hex');
const isInt = (v) => Number.isSafeInteger(v) && v >= 0;
function safeName(name) {
  if (typeof name !== 'string' || name.length > 220 || !/^[a-zA-Z0-9_-][a-zA-Z0-9_./-]*$/.test(name)
      || name.split('/').some(p => !p || p === '.' || p === '..' || /^(con|prn|aux|nul|com\d|lpt\d)(\.|$)/i.test(p))
      || name.includes('node_modules/') || !/\.(mjs|js|ts|mts|cts|json|md|css|html)$/.test(name)) fail('release_path_invalid');
  return name;
}
function contained(root, file) {
  const result = path.resolve(root, file);
  if (!result.startsWith(`${path.resolve(root)}${path.sep}`)) fail('update_path_escape');
  return result;
}
async function regularTree(root) {
  const entries = [];
  async function walk(dir, prefix = '') {
    for (const e of await readdir(dir, { withFileTypes: true })) {
      if (e.name === 'node_modules' || e.name.startsWith('.') || ['data', 'output'].includes(e.name)) continue;
      const name = `${prefix}${e.name}`;
      if (e.isSymbolicLink()) fail('release_symlink');
      if (e.isDirectory()) await walk(path.join(dir, e.name), `${name}/`);
      else if (e.isFile()) {
        safeName(name);
        const bytes = await readFile(path.join(dir, e.name));
        entries.push({ path: name, hash: sha256(bytes), bytes: bytes.length, content: bytes.toString('base64') });
      } else fail('release_file_type');
    }
  }
  await walk(root);
  return entries.sort((a, b) => a.path.localeCompare(b.path));
}
export async function createRelease({ directory, privateKey, keyId, sequence, expiresAt, channel = 'stable', rollout = 100 }) {
  const files = await regularTree(directory);
  const pkg = JSON.parse(await readFile(path.join(directory, 'package.json'), 'utf8'));
  if (pkg.name !== NAME) fail('wrong_package');
  const signed = { format: FORMAT, name: NAME, version: pkg.version, sequence, expiresAt, channel, rollout,
    compatibility: pkg.connectCompatibility, files: files.map(({ content, ...descriptor }) => descriptor) };
  validateManifest(signed);
  const release = { signed, signatures: [{ keyId, signature: sign(null, Buffer.from(canonical(signed)), privateKey).toString('base64url') }],
    contents: Object.fromEntries(files.map(f => [f.path, f.content])) };
  if (Buffer.byteLength(JSON.stringify(release)) > LIMIT) fail('release_too_large');
  return release;
}
function validateManifest(m) {
  if (m?.format !== FORMAT || m.name !== NAME || !/^\d+\.\d+\.\d+$/.test(m.version) || !isInt(m.sequence) || m.sequence < 1
      || !Number.isFinite(Date.parse(m.expiresAt)) || !['stable', 'preview'].includes(m.channel)
      || !isInt(m.rollout) || m.rollout > 100 || m.compatibility?.protocol !== 1
      || m.compatibility?.storage !== 1 || m.compatibility?.minReader !== 1
      || !Array.isArray(m.files) || !m.files.length || m.files.length > 500) fail('manifest_invalid');
  const seen = new Set(); let total = 0;
  for (const f of m.files) {
    safeName(f.path);
    const folded = f.path.toLowerCase();
    if (seen.has(folded) || !/^[a-f0-9]{64}$/.test(f.hash) || !isInt(f.bytes)) fail('manifest_file_invalid');
    seen.add(folded); total += f.bytes;
  }
  if (total > LIMIT || !seen.has('package.json')) fail('manifest_size_invalid');
}
export function verifyRelease(release, trust, { now = Date.now(), lastSequence = 0, version = '0.1.0', channel = 'stable' } = {}) {
  validateManifest(release?.signed);
  const m = release.signed;
  if (Date.parse(m.expiresAt) <= now) fail('release_expired');
  if (m.sequence <= lastSequence) fail('release_rollback');
  if (m.channel !== channel) fail('release_channel');
  // Automatic updates stay inside the installed major AND minor during the 0.x period.
  const current = version.split('.'); const next = m.version.split('.');
  if (current[0] !== next[0] || (current[0] === '0' && current[1] !== next[1])) fail('release_requires_migration');
  if (compareVersion(m.version, version) < 0) fail('release_version_rollback');
  const threshold = trust?.threshold ?? 1;
  if (!isInt(threshold) || threshold < 1 || !trust?.keys || !Array.isArray(release.signatures)) fail('trust_invalid');
  const accepted = new Set();
  for (const s of release.signatures) {
    if (!trust.keys[s.keyId] || accepted.has(s.keyId)) continue;
    try { if (verify(null, Buffer.from(canonical(m)), trust.keys[s.keyId], Buffer.from(s.signature, 'base64url'))) accepted.add(s.keyId); } catch { /* Not a trusted signature. */ }
  }
  if (accepted.size < threshold) fail('release_signature');
  if (!release.contents || Object.keys(release.contents).length !== m.files.length) fail('release_contents');
  const checked = new Map();
  for (const f of m.files) {
    const encoded = release.contents[f.path];
    if (typeof encoded !== 'string' || encoded.length > LIMIT * 2 || !/^[A-Za-z0-9+/]*={0,2}$/.test(encoded)) fail('release_contents');
    const bytes = Buffer.from(encoded, 'base64');
    if (bytes.length !== f.bytes || sha256(bytes) !== f.hash) fail('release_hash');
    checked.set(f.path, bytes);
  }
  const pkg = JSON.parse(checked.get('package.json').toString('utf8'));
  if (pkg.name !== NAME || pkg.version !== m.version || canonical(pkg.connectCompatibility) !== canonical(m.compatibility)) fail('release_package_mismatch');
  return { manifest: m, files: checked };
}
function compareVersion(a, b) {
  for (let i = 0; i < 3; i++) { const delta = Number(a.split('.')[i]) - Number(b.split('.')[i]); if (delta) return delta; }
  return 0;
}
async function exists(file) { try { await lstat(file); return true; } catch (e) { if (e.code === 'ENOENT') return false; throw e; } }
async function atomicJson(file, value) {
  const tmp = `${file}.${process.pid}.tmp`;
  const handle = await open(tmp, 'w', 0o600);
  try { await handle.writeFile(`${JSON.stringify(value, null, 2)}\n`); await handle.sync(); } finally { await handle.close(); }
  await rename(tmp, file);
}
async function noSymlinkAncestors(target) {
  let cursor = path.resolve(target);
  while (true) {
    if (await exists(cursor)) { if ((await lstat(cursor)).isSymbolicLink()) fail('update_symlink'); }
    const parent = path.dirname(cursor); if (parent === cursor) break; cursor = parent;
  }
}
function samePath(a, b) { return typeof a === 'string' && typeof b === 'string' && path.relative(a, b) === ''; }
function insideOrEqual(root, file) {
  const relative = path.relative(root, file);
  return !relative || (relative !== '..' && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative));
}
async function canonicalLocation(file) {
  let cursor = path.resolve(file); const missing = [];
  while (!await exists(cursor)) { missing.unshift(path.basename(cursor)); cursor = path.dirname(cursor); }
  // Resolve existing filesystem aliases before comparing paths; path.relative also
  // applies Windows case-insensitive comparisons to not-yet-created descendants.
  return path.join(await realpath(cursor), ...missing);
}
function recoveryError(cause, failures, recovery) {
  return Object.assign(new AggregateError([...(cause ? [cause] : []), ...failures], 'update_recovery_required', { cause }), {
    code: 'update_recovery_required', interventionRequired: true, recovery,
  });
}
export async function fetchRelease(url, fetchImpl = fetch) {
  const u = new URL(url);
  if (u.protocol !== 'https:' || u.username || u.password) fail('update_requires_https');
  const response = await fetchImpl(u.href, { redirect: 'error', signal: AbortSignal.timeout(20_000), headers: { accept: 'application/json' } });
  if (!response.ok || Number(response.headers.get('content-length') || 0) > LIMIT) fail('update_download_failed');
  const reader = response.body.getReader(); const parts = []; let total = 0;
  try { while (true) { const { done, value } = await reader.read(); if (done) break; total += value.length; if (total > LIMIT) fail('release_too_large'); parts.push(value); } }
  finally { await reader.cancel().catch(() => {}); }
  return JSON.parse(Buffer.concat(parts).toString('utf8'));
}
export function runValidation(argv, cwd, timeoutMs = 180_000) {
  if (!Array.isArray(argv) || !argv.length || argv.some(x => typeof x !== 'string')) fail('validation_command_required');
  return new Promise((resolve, reject) => {
    // Trusted local configuration only. No shell expansion and no release-supplied commands.
    const child = spawn(argv[0], argv.slice(1), { cwd, shell: false, stdio: 'ignore', windowsHide: true });
    const timer = setTimeout(() => { child.kill(); reject(new Error('validation_timeout')); }, timeoutMs);
    child.on('error', e => { clearTimeout(timer); reject(e); });
    child.on('exit', code => { clearTimeout(timer); code === 0 ? resolve() : reject(new Error('validation_failed')); });
  });
}
// Restore an interrupted code/host transaction before fetching any newer release.
// Locks are never stolen: a dead process's lock requires an operator to verify it.
export async function recoverRelease({ target, stateDir, activate }) {
  target = path.resolve(target); stateDir = path.resolve(stateDir);
  await noSymlinkAncestors(target); await noSymlinkAncestors(stateDir);
  target = await canonicalLocation(target); stateDir = await canonicalLocation(stateDir);
  if (insideOrEqual(target, stateDir) || insideOrEqual(stateDir, target)) fail('update_state_must_be_external');
  await mkdir(stateDir, { recursive: true, mode: 0o700 });
  const lockPath = path.join(stateDir, 'update.lock'); let lock; let operationError;
  try { lock = await open(lockPath, 'wx', 0o600); } catch (error) { if (error.code === 'EEXIST') fail('update_locked'); throw error; }
  try {
    await lock.writeFile(JSON.stringify({ pid: process.pid, target })); await lock.sync();
    const stateFile = path.join(stateDir, 'state.json');
    if (!await exists(stateFile)) return { status: 'clean' };
    const state = JSON.parse(await readFile(stateFile, 'utf8'));
    if (state.format !== 1 || !samePath(state.target, target)) fail('update_state_invalid');
    if (!state.pending) return { status: 'clean' };
    await rollback({ ...state, target }, stateFile, { activate });
    return { status: 'recovered', restartRequired: !activate };
  } catch (error) { operationError = error; throw error; }
  finally {
    const failures = [];
    try { await lock.close(); } catch (error) { failures.push(error); }
    try { await unlink(lockPath); } catch (error) { failures.push(error); }
    if (failures.length) throw recoveryError(operationError, failures, { ...operationError?.recovery, lockReleased: false });
  }
}
export async function applyRelease({ target, stateDir, release, trust, deploymentId, channel = 'stable', validate, activate, now = Date.now() }) {
  target = path.resolve(target); stateDir = path.resolve(stateDir);
  await noSymlinkAncestors(target); await noSymlinkAncestors(stateDir);
  target = await canonicalLocation(target); stateDir = await canonicalLocation(stateDir);
  if (insideOrEqual(target, stateDir) || insideOrEqual(stateDir, target)) fail('update_state_must_be_external');
  await mkdir(stateDir, { recursive: true, mode: 0o700 });
  const lockPath = path.join(stateDir, 'update.lock');
  let lock;
  try { lock = await open(lockPath, 'wx', 0o600); } catch (e) { if (e.code === 'EEXIST') fail('update_locked'); throw e; }
  let operationError;
  try {
    await lock.writeFile(JSON.stringify({ pid: process.pid, target }));
    const stateFile = path.join(stateDir, 'state.json');
    let state = await exists(stateFile) ? JSON.parse(await readFile(stateFile, 'utf8')) : { format: 1, lastSequence: 0, target };
    if (state.format !== 1 || !samePath(state.target, target)) fail('update_state_invalid');
    state = { ...state, target };
    const recoveredPending = Boolean(state.pending);
    if (state.pending) state = await rollback(state, stateFile, { activate });
    const pkg = JSON.parse(await readFile(path.join(target, 'package.json'), 'utf8'));
    if (pkg.name !== NAME) fail('update_target_not_module');
    const sameRelease = state.lastSequence > 0 && release?.signed?.sequence === state.lastSequence
      && state.releaseHash === sha256(canonical(release.signed));
    const checked = verifyRelease(release, trust, { now, lastSequence: sameRelease ? state.lastSequence - 1 : state.lastSequence, version: pkg.version, channel });
    if (sameRelease) return { status: 'current', version: pkg.version, ...(recoveredPending && !activate ? { restartRequired: true } : {}) };
    const bucket = parseInt(sha256(String(deploymentId || target)).slice(0, 8), 16) % 100;
    if (bucket >= checked.manifest.rollout) return { status: 'deferred', version: pkg.version, ...(recoveredPending && !activate ? { restartRequired: true } : {}) };
    if (typeof validate !== 'function') fail('validation_required');
    const suffix = `${checked.manifest.sequence}-${Date.now()}-${process.pid}`;
    // Paths are siblings on one filesystem so rename is atomic. They contain module code only.
    const stage = `${target}.stage-${suffix}`; const backup = `${target}.previous-${suffix}`;
    for (const file of [stage, backup]) { if (await exists(file)) fail('update_stage_exists'); await noSymlinkAncestors(file); }
    await mkdir(stage, { mode: 0o700 });
    for (const [name, bytes] of checked.files) {
      const file = contained(stage, name); await mkdir(path.dirname(file), { recursive: true }); await writeFile(file, bytes, { flag: 'wx', mode: 0o600 });
    }
    // Module tests are part of the signed candidate; host validation is an independent local gate.
    const tests = checked.manifest.files.filter(f => /^test\/.*\.test\.mjs$/.test(f.path)).map(f => f.path);
    if (!tests.length) fail('release_tests_missing');
    await runValidation([process.execPath, '--test', ...tests], stage);
    state = { ...state, pending: { stage, backup, nextSequence: checked.manifest.sequence } };
    await atomicJson(stateFile, state);
    try {
      await rename(target, backup); await rename(stage, target);
      await validate(target);
      if (activate) await activate(target);
      const committed = { format: 1, target, lastSequence: checked.manifest.sequence, releaseHash: sha256(canonical(release.signed)), version: checked.manifest.version, previous: backup, pending: null };
      // Keep the rollback journal in memory until the durable commit succeeds.
      await atomicJson(stateFile, committed);
      return { status: 'updated', version: committed.version, previous: backup, restartRequired: !activate };
    } catch (e) {
      await rollback(state, stateFile, { activate, cause: e });
      throw e;
    }
  } catch (error) { operationError = error; throw error; }
  finally {
    const failures = [];
    try { await lock.close(); } catch (error) { failures.push(error); }
    try { await unlink(lockPath); } catch (error) { failures.push(error); }
    if (failures.length) throw recoveryError(operationError, failures, { ...operationError?.recovery, lockReleased: false });
  }
}
async function rollback(state, stateFile, { activate, cause } = {}) {
  const { target, pending } = state;
  if (!pending) return state;
  const recovery = { codeRestored: false, hostReactivated: activate ? false : null, journalRestored: false };
  try {
    for (const file of [pending.stage, pending.backup]) {
      if (typeof file !== 'string' || !path.isAbsolute(file) || !samePath(path.dirname(file), path.dirname(target))
          || !path.relative(path.dirname(target), file).toLowerCase().startsWith(`${path.basename(target).toLowerCase()}.`)) fail('update_journal_invalid');
      await noSymlinkAncestors(file);
    }
    if (samePath(pending.stage, pending.backup)) fail('update_journal_invalid');
    if (await exists(pending.backup)) {
      if (await exists(target)) await rename(target, `${pending.stage}.failed`);
      await rename(pending.backup, target);
    } else if (!await exists(target)) fail('update_recovery_required');
    recovery.codeRestored = true;
  } catch (error) { throw recoveryError(cause, [error], recovery); }
  const failures = [];
  // Reactivate restored code even if the journal cannot currently be written.
  // Keep pending durable when activation fails so the next run retries it.
  if (activate) {
    try { await activate(target); recovery.hostReactivated = true; }
    catch (error) { failures.push(error); }
  }
  const result = { ...state, pending: null };
  if (!activate || recovery.hostReactivated) {
    try { await atomicJson(stateFile, result); recovery.journalRestored = true; }
    catch (error) { failures.push(error); }
  }
  if (failures.length) throw recoveryError(cause, failures, recovery);
  return result;
}
// A dead process leaves a lock intentionally. An operator verifies the PID before removing it;
// a timeout alone must never steal a lock from an update that is still validating.
