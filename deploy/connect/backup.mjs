import { createCipheriv, createHash, createPublicKey, publicEncrypt, randomBytes } from 'node:crypto';
import { createWriteStream, createReadStream } from 'node:fs';
import { appendFile, mkdir, open, readFile, rename, unlink } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { pipeline } from 'node:stream/promises';
import path from 'node:path';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { DockerApi } from '../connector/docker-api.mjs';
import { createRestoreParser } from './backup-format.mjs';

const fail = code => { throw Object.assign(new Error(code), { code }); };
export const MAGIC = Buffer.from('SOTYBAK1');
export function archiveHelperArgs({ containerId, helperName, image }) {
  // Existing volumes can contain files owned by several UIDs. Root without
  // DAC_READ_SEARCH cannot archive another owner's 0600 files after cap-drop.
  // This capability grants reads/search only; both rootfs and inherited mounts
  // remain read-only, and the helper has no network or other capabilities.
  return ['run', '--rm', '--name', helperName, '--network', 'none', '--read-only', '--user', '0:0',
    '--cap-drop', 'ALL', '--cap-add', 'DAC_READ_SEARCH', '--security-opt', 'no-new-privileges',
    '--volumes-from', `${containerId}:ro`, '--entrypoint', 'tar', image, '-C', '/data', '-cf', '-', '.'];
}
export async function encryptBackup({ output, publicKey, metadata, stream, durableDirectory = false }) {
  const key = createPublicKey(publicKey);
  if (key.asymmetricKeyType !== 'rsa' || key.asymmetricKeyDetails.modulusLength < 3072) fail('backup_key_invalid');
  const secret = randomBytes(32), iv = randomBytes(12);
  const header = Buffer.from(JSON.stringify({ format: 'soty.encrypted-backup.v1', algorithm: 'RSA-OAEP-SHA256/AES-256-GCM',
    keyId: createHash('sha256').update(key.export({ type: 'spki', format: 'der' })).digest('hex'),
    key: publicEncrypt({ key, oaepHash: 'sha256' }, secret).toString('base64'), iv: iv.toString('base64') }));
  const length = Buffer.alloc(4); length.writeUInt32BE(header.length);
  const prefix = Buffer.concat([MAGIC, length, header]);
  const cipher = createCipheriv('aes-256-gcm', secret, iv); cipher.setAAD(prefix); secret.fill(0);
  const meta = Buffer.from(JSON.stringify(metadata)), metaLength = Buffer.alloc(4); metaLength.writeUInt32BE(meta.length);
  const tmp = `${output}.${process.pid}.partial`;
  const initial = await open(tmp, 'wx', 0o600); await initial.writeFile(prefix); await initial.close();
  try {
    async function* plaintext() { yield metaLength; yield meta; for await (const chunk of stream) yield chunk; }
    await pipeline(plaintext(), cipher, createWriteStream(tmp, { flags: 'a', mode: 0o600 }));
    await appendFile(tmp, cipher.getAuthTag());
    const f = await open(tmp, 'r+'); await f.sync(); await f.close();
    await rename(tmp, output);
    const directory = await open(path.dirname(output), 'r');
    try { if (durableDirectory) await directory.sync(); else await directory.sync().catch(() => {}); }
    finally { await directory.close(); }
    return { format: 'soty.backup-receipt.v1', encrypted: true, file: output, keyId: JSON.parse(header).keyId };
  } catch (e) { await unlink(tmp).catch(() => {}); throw e; }
}

export async function backupStoppedContainer({ containerId, publicKeyFile, directory, cold }, engine = new DockerApi()) {
  if (cold !== undefined) return completeColdBackup({ containerId, publicKeyFile, directory, cold }, engine);
  if (!/^[a-f0-9]{64}$/.test(containerId)) fail('backup_container_invalid');
  const original = await engine.inspect(containerId);
  if (original.Id !== containerId || original.State.Running) fail('backup_requires_stopped_container');
  const volume = original.Mounts.find(m => m.Destination === '/data' && m.Type === 'volume');
  if (!volume) fail('backup_data_volume_missing');
  const assertNoWriters = async () => {
    const running = await engine.request('GET', '/containers/json');
    if (running.some(c => c.Mounts?.some(m => m.Name === volume.Name && m.RW !== false))) fail('backup_other_writer_running');
  };
  await assertNoWriters();
  await mkdir(directory, { recursive: true, mode: 0o700 });
  const secrets = {};
  for (const mount of original.Mounts) {
    if (mount.Destination === '/run/secrets/soty-application-tokens.json' && mount.Type === 'bind' && !mount.RW) {
      secrets[mount.Destination] = (await readFile(mount.Source)).toString('base64');
    }
  }
  const file = path.join(directory, `soty-${new Date().toISOString().replace(/[:.]/g, '-')}-${randomBytes(4).toString('hex')}.enc`);
  const helperName = `soty-connect-backup-${randomBytes(8).toString('hex')}`;
  const helper = spawn('docker', archiveHelperArgs({ containerId, helperName, image: original.Image }),
    { stdio: ['ignore', 'pipe', 'ignore'] });
  const done = new Promise((resolve, reject) => { helper.once('error', () => reject(new Error('backup_helper_start'))); helper.once('exit', code => code === 0 ? resolve() : reject(new Error('backup_helper_failed'))); });
  done.catch(() => {});
  try {
    const receipt = await encryptBackup({ output: file, publicKey: await readFile(publicKeyFile),
      metadata: { createdAt: new Date().toISOString(), original, secrets, dataFormat: 'tar', offline: true }, stream: helper.stdout });
    await done;
    const after = await engine.inspect(containerId);
    if (after.State.Running || after.Image !== original.Image || after.State.StartedAt !== original.State.StartedAt
      || after.State.FinishedAt !== original.State.FinishedAt || after.RestartCount !== original.RestartCount) fail('backup_offline_guard_changed');
    await assertNoWriters();
    const digest = createHash('sha256');
    for await (const chunk of createReadStream(file)) digest.update(chunk);
    return { ...receipt, ok: true, receiptPath: file, sha256: digest.digest('hex') };
  } catch (error) { helper.kill(); await unlink(file).catch(() => {}); throw error; }
}

// One-shot, private cold profile. Quiescence of host/alias writers is a caller
// precondition, not something two Docker snapshots can establish.
const COLD_CODES = new Set(['backup_cold_invalid', 'backup_cold_changed', 'backup_cold_limit_exceeded',
  'backup_cold_timeout', 'backup_cold_io_failed', 'backup_cold_cleanup_pending']);
const coldFailure = error => {
  const code = COLD_CODES.has(error?.code) ? error.code : 'backup_cold_io_failed';
  return Object.assign(new Error(code), { code, stack: `Error: ${code}` });
};
const HEX = /^[a-f0-9]{64}$/, LOGICAL_ID = /^[a-z][a-z0-9._-]{0,95}$/;
const hash = value => createHash('sha256').update(value).digest('hex');
const mountPin = mounts => hash(JSON.stringify(mounts));
const sameWitness = (a, b) => ['generationId', 'checkpointSha256', 'inventorySha256'].every(key => a[key] === b[key]);
function coldArchiveHelperArgs(options) {
  const args = archiveHelperArgs(options);
  // A pipe to this supervisor does not disable the daemon's container logs.
  args.splice(args.indexOf('--entrypoint'), 0, '--log-driver', 'none');
  return args;
}
function coldOptions(value, witnessRequired) {
  const allowed = ['dataRoot', 'generationId', 'checkpointSha256', 'stores', 'externalFiles', 'limits', 'sourceWitness', 'controlMounts'];
  if (!value || typeof value !== 'object' || Array.isArray(value)
      || Reflect.ownKeys(value).some(k => !allowed.includes(k))) fail('backup_cold_invalid');
  // Captured private data is never printed. Subsequent awaits do not reread callers.
  const cold = JSON.parse(JSON.stringify(value));
  if (cold.dataRoot !== '/data' || !/^[a-f0-9]{32}$/.test(cold.generationId || '') || !HEX.test(cold.checkpointSha256 || '')
      || !Array.isArray(cold.stores) || !Array.isArray(cold.externalFiles) || !cold.limits) fail('backup_cold_invalid');
  for (const field of ['wallMs', 'idleMs', 'archiveBytes', 'plaintextBytes'])
    if (!Number.isSafeInteger(cold.limits[field]) || cold.limits[field] <= 0) fail('backup_cold_invalid');
  if (cold.limits.wallMs > 2147483647) fail('backup_cold_limit_exceeded');
  const witness = cold.sourceWitness;
  if (witnessRequired && (!witness || Object.keys(witness).sort().join(',') !== 'checkpointSha256,generationId,inventorySha256'
      || witness.generationId !== cold.generationId || witness.checkpointSha256 !== cold.checkpointSha256
      || !HEX.test(witness.inventorySha256 || ''))) fail('backup_cold_invalid');
  for (const external of cold.externalFiles) {
    if (!external || !LOGICAL_ID.test(external.id || '') || typeof external.required !== 'boolean'
        || external.path !== null && (typeof external.path !== 'string' || !path.isAbsolute(external.path)
          || external.path.length > 4096 || /[,\u0000-\u001f\u007f]/u.test(external.path))) fail('backup_cold_invalid');
  }
  cold.controlMounts ??= [];
  validateColdSourceMounts([], cold, true);
  return cold;
}
// Public-feed contents are NOT copied into B. The trusted operator separately
// attests that this exact RO directory is a recoverable public signed feed,
// not private durable data. Its disposition and mount config stay encrypted.
export function validateColdSourceMounts(mounts, cold, profileOnly = false) {
  const controls = cold.controlMounts ?? [];
  if (!Array.isArray(controls) || controls.length > 4) fail('backup_cold_invalid');
  const destinations = new Set(), sources = new Set();
  for (const mount of controls) {
    if (!mount || Object.keys(mount).sort().join(',') !== 'attestationSha256,destination,purpose,source'
        || mount.purpose !== 'public-signed-release-feed' || !HEX.test(mount.attestationSha256 || '')
        || !['source', 'destination'].every(k => typeof mount[k] === 'string' && mount[k].isWellFormed()
          && Buffer.byteLength(mount[k]) <= 4096 && path.posix.isAbsolute(mount[k])
          && path.posix.normalize(mount[k]) === mount[k] && !/[,\u0000-\u001f\u007f\\]/u.test(mount[k]))
        || mount.destination === '/data' || mount.destination.startsWith('/data/')
        || destinations.has(mount.destination) || sources.has(mount.source)
        || cold.externalFiles.some(file => file.path === mount.source)) fail('backup_cold_invalid');
    destinations.add(mount.destination); sources.add(mount.source);
  }
  if (profileOnly) return;
  const used = new Set();
  for (const mount of mounts) {
    if (mount.Destination === '/data') continue;
    if (mount.Destination.startsWith('/data/') || mount.RW !== false || mount.Type !== 'bind') fail('backup_cold_invalid');
    const control = controls.find(item => item.source === mount.Source && item.destination === mount.Destination);
    if (control) { if (used.has(control.destination)) fail('backup_cold_invalid'); used.add(control.destination); continue; }
    if (!cold.externalFiles.some(file => file.path === mount.Source)) fail('backup_cold_invalid');
  }
  if (used.size !== controls.length) fail('backup_cold_invalid');
}
async function stoppedSource(containerId, cold, engine) {
  if (!/^[a-f0-9]{64}$/.test(containerId)) fail('backup_cold_invalid');
  const original = await engine.inspect(containerId);
  if (original.Id !== containerId || original.State.Running || !HEX.test(original.Image?.replace(/^sha256:/, '') || '')) fail('backup_cold_invalid');
  const volumes = original.Mounts.filter(m => m.Destination === '/data' && m.Type === 'volume');
  if (volumes.length !== 1 || !volumes[0].Name) fail('backup_cold_invalid');
  validateColdSourceMounts(original.Mounts, cold);
  await noVolumeWriters(original, engine); return original;
}
async function noVolumeWriters(original, engine) {
  const running = await engine.request('GET', '/containers/json');
  const source = original.Mounts.find(m => m.Destination === '/data');
  if (running.some(c => c.Mounts?.some(m => m.RW !== false && (m.Name === source.Name || m.Source === source.Source))))
    fail('backup_cold_changed');
}
async function unchangedStopped(original, engine) {
  const after = await engine.inspect(original.Id);
  if (after.Id !== original.Id || after.State.Running || after.Image !== original.Image
      || after.State.StartedAt !== original.State.StartedAt || after.State.FinishedAt !== original.State.FinishedAt
      || after.RestartCount !== original.RestartCount || mountPin(after.Mounts) !== mountPin(original.Mounts)) fail('backup_cold_changed');
  await noVolumeWriters(original, engine);
}
function ownedChild(args, timeoutMs, input = null, capture = false) {
  const child = spawn('docker', args, { stdio: [input === null ? 'ignore' : 'pipe', 'pipe', 'ignore'] });
  let failed = false, timedOut = false, size = 0;
  const buffer = capture ? Buffer.alloc(4 * 1024 * 1024) : null;
  const terminate = () => { failed = true; if (!child.killed) child.kill('SIGTERM'); };
  const timer = setTimeout(() => { timedOut = true; terminate(); }, timeoutMs);
  if (capture) child.stdout.on('data', chunk => {
    if (chunk.length > buffer.length - size) { terminate(); return; }
    chunk.copy(buffer, size); size += chunk.length;
  });
  child.stdout.on('error', terminate);
  child.on('error', () => { failed = true; });
  if (input !== null) {
    child.stdin.on('error', terminate);
    child.stdin.end(input, () => input.fill(0));
  }
  const closed = new Promise((resolve, reject) => child.once('close', (code, signal) => {
    clearTimeout(timer);
    if (failed || code !== 0 || signal !== null) {
      buffer?.fill(0); reject(coldFailure({ code: timedOut ? 'backup_cold_timeout' : 'backup_cold_io_failed' }));
    } else resolve(capture ? buffer.subarray(0, size) : null);
  }));
  closed.catch(() => {});
  return { child, closed, terminate, clear: () => buffer?.fill(0) };
}
async function collectInHelper(original, cold) {
  const helperName = `soty-connect-cold-${randomBytes(8).toString('hex')}`;
  const args = coldArchiveHelperArgs({ containerId: original.Id, helperName, image: original.Image });
  args.splice(args.indexOf('--entrypoint'));
  args.splice(1, 0, '--interactive');
  const moduleFile = fileURLToPath(new URL('./cold-inventory.mjs', import.meta.url));
  if (moduleFile.includes(',')) fail('backup_cold_invalid');
  args.push('--mount', `type=bind,source=${moduleFile},target=/run/soty-cold-inventory.mjs,readonly`);
  const externalFiles = cold.externalFiles.map(file => {
    if (file.path === null) return file;
    const destination = `/run/soty-cold-files/${file.id}`;
    args.push('--mount', `type=bind,source=${file.path},target=${destination},readonly`);
    return { ...file, path: destination };
  });
  args.push('--entrypoint', 'node', original.Image, '/run/soty-cold-inventory.mjs');
  const control = Buffer.from(JSON.stringify({ dataRoot: '/data', generationId: cold.generationId,
    checkpointSha256: cold.checkpointSha256, stores: cold.stores, externalFiles, limits: cold.limits }));
  if (control.length > 65536) { control.fill(0); fail('backup_cold_limit_exceeded'); }
  const task = ownedChild(args, cold.limits.wallMs, control, true);
  try {
    const bytes = await task.closed;
    const result = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
    if (!result?.restoreManifest || !result.restoreFiles || !result.sourceWitness
        || !HEX.test(result.manifestSha256 || '') || result.sourceWitness.generationId !== cold.generationId
        || result.sourceWitness.checkpointSha256 !== cold.checkpointSha256) fail('backup_cold_invalid');
    return result;
  } finally { task.clear(); }
}
// Private control API: root obtains this separate cold witness BEFORE producer.
export async function captureStoppedContainerInventory({ containerId, cold }, engine = new DockerApi()) {
  try {
    const captured = coldOptions(cold, false), original = await stoppedSource(containerId, captured, engine);
    const result = await collectInHelper(original, captured); await unchangedStopped(original, engine); return result;
  } catch (error) { throw coldFailure(error); }
}
async function completeColdBackup({ containerId, publicKeyFile, directory, cold }, engine) {
  let task, file, parser;
  try {
    cold = coldOptions(cold, true);
    const original = await stoppedSource(containerId, cold, engine), captured = await collectInHelper(original, cold);
    if (!sameWitness(captured.sourceWitness, cold.sourceWitness)) fail('backup_cold_changed');
    await unchangedStopped(original, engine);
    await mkdir(directory, { recursive: true, mode: 0o700 });
    file = path.join(directory, `soty-${new Date().toISOString().replace(/[:.]/g, '-')}-${randomBytes(4).toString('hex')}.enc`);
    const metadata = { createdAt: new Date().toISOString(), original, secrets: {}, dataFormat: 'tar', offline: true,
      restoreManifest: captured.restoreManifest, restoreFiles: captured.restoreFiles, controlMounts: cold.controlMounts };
    const bytes = Buffer.from(JSON.stringify(metadata)), length = Buffer.alloc(4);
    if (bytes.length > 4 * 1024 * 1024) fail('backup_cold_limit_exceeded'); length.writeUInt32BE(bytes.length);
    const started = performance.now(); let lastProgress = started, plaintextBytes = 4 + bytes.length;
    const check = advanced => {
      const now = performance.now();
      if (now - started > cold.limits.wallMs || now - lastProgress > cold.limits.idleMs) fail('backup_cold_timeout');
      if (advanced) lastProgress = now;
    };
    parser = createRestoreParser({ expectedManifestSha256: captured.manifestSha256, sourceWitness: cold.sourceWitness, limits: cold.limits, check });
    await parser.feed(length); await parser.feed(bytes); bytes.fill(0);
    const helperName = `soty-connect-backup-${randomBytes(8).toString('hex')}`;
    task = ownedChild(coldArchiveHelperArgs({ containerId, helperName, image: original.Image }), cold.limits.wallMs);
    async function* checkedTar() {
      for await (const chunk of task.child.stdout) {
        check(true); plaintextBytes += chunk.length;
        if (!Number.isSafeInteger(plaintextBytes) || plaintextBytes > cold.limits.plaintextBytes) fail('backup_cold_limit_exceeded');
        await parser.feed(chunk); yield chunk;
      }
      await parser.finish(); await task.closed; check();
    }
    const receipt = await encryptBackup({ output: file, publicKey: await readFile(publicKeyFile), metadata, stream: checkedTar(), durableDirectory: true });
    await task.closed; await unchangedStopped(original, engine);
    const after = await collectInHelper(original, cold);
    if (!sameWitness(after.sourceWitness, cold.sourceWitness)) fail('backup_cold_changed');
    await unchangedStopped(original, engine);
    const digest = createHash('sha256'); let count = 0;
    for await (const chunk of createReadStream(file)) {
      count += chunk.length; if (count > cold.limits.archiveBytes) fail('backup_cold_limit_exceeded'); digest.update(chunk);
    }
    return { ...receipt, ok: true, complete: true, receiptPath: file, sha256: digest.digest('hex'),
      manifestSha256: captured.manifestSha256, sourceWitness: captured.sourceWitness };
  } catch (error) {
    if (task) { task.terminate(); try { await task.closed; } catch { } }
    // Only this newly chosen ciphertext is removed. Unknown live helper cleanup
    // remains the supervisor's responsibility; no receipt is returned early.
    if (file) await unlink(file).catch(() => {});
    throw coldFailure(error);
  } finally { parser?.clear(); }
}
if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  try {
    const [containerId, publicKeyFile, directory, coldControlFile, mode] = process.argv.slice(2);
    let cold;
    if (coldControlFile !== undefined) {
      // Explicit private-controller mode. This output must be captured by the
      // owning controller; the default legacy CLI never emits private pins.
      const handle = await open(coldControlFile, 'r'), bytes = Buffer.alloc(65537);
      try {
        let used = 0;
        for (;;) {
          const { bytesRead } = await handle.read(bytes, used, bytes.length - used, used);
          if (!bytesRead) break; used += bytesRead;
          if (used > 65536) fail('backup_cold_limit_exceeded');
        }
        cold = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes.subarray(0, used)));
      } finally { bytes.fill(0); await handle.close(); }
    }
    if (mode !== undefined && mode !== 'capture') fail('backup_cold_invalid');
    if (mode === 'capture') {
      const captured = await captureStoppedContainerInventory({ containerId, cold });
      // Only the independent cold pin, never restoreFiles/config material.
      console.log(JSON.stringify({ sourceWitness: captured.sourceWitness, manifestSha256: captured.manifestSha256 }));
    } else console.log(JSON.stringify(await backupStoppedContainer({ containerId, publicKeyFile, directory, cold })));
  } catch (error) { console.error(JSON.stringify({ ok: false, code: /^[a-z_]+$/.test(error.code || '') ? error.code : 'backup_failed' })); process.exitCode = 1; }
}
