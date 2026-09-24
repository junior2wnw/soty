import { createCipheriv, createHash, createPublicKey, publicEncrypt, randomBytes } from 'node:crypto';
import { createWriteStream, createReadStream } from 'node:fs';
import { appendFile, mkdir, open, readFile, rename, unlink } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { pipeline } from 'node:stream/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { DockerApi } from '../connector/docker-api.mjs';

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
export async function encryptBackup({ output, publicKey, metadata, stream }) {
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
    const directory = await open(path.dirname(output), 'r'); await directory.sync().catch(() => {}); await directory.close();
    return { format: 'soty.backup-receipt.v1', encrypted: true, file: output, keyId: JSON.parse(header).keyId };
  } catch (e) { await unlink(tmp).catch(() => {}); throw e; }
}

export async function backupStoppedContainer({ containerId, publicKeyFile, directory }, engine = new DockerApi()) {
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
if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  try {
    const [containerId, publicKeyFile, directory] = process.argv.slice(2);
    console.log(JSON.stringify(await backupStoppedContainer({ containerId, publicKeyFile, directory })));
  } catch (error) { console.error(JSON.stringify({ ok: false, code: /^[a-z_]+$/.test(error.code || '') ? error.code : 'backup_failed' })); process.exitCode = 1; }
}
