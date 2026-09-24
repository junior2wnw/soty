import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, privateDecrypt, createDecipheriv, randomBytes } from 'node:crypto';
import { spawn, spawnSync } from 'node:child_process';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { Readable } from 'node:stream';
import { encryptBackup, archiveHelperArgs, MAGIC } from './backup.mjs';
const keys = generateKeyPairSync('rsa', { modulusLength: 3072 });
test('offline backup authenticates both encrypted configuration and streamed data; tampering fails', async () => {
  const dir = await mkdtemp(path.join(tmpdir(), 'connect-backup-'));
  try {
    const file = path.join(dir, 'snapshot.enc');
    const metadata = { env: ['SECRET=synthetic-test-only'], offline: true };
    const bytes = Buffer.from('synthetic archive');
    await encryptBackup({ output: file, publicKey: keys.publicKey.export({ type: 'spki', format: 'pem' }), metadata, stream: Readable.from([bytes]) });
    const encrypted = await readFile(file);
    assert.equal(encrypted.includes(Buffer.from('SECRET')), false);
    const decrypt = input => {
      assert.deepEqual(input.subarray(0, 8), MAGIC);
      const end = 12 + input.readUInt32BE(8), header = JSON.parse(input.subarray(12, end));
      const secret = privateDecrypt({ key: keys.privateKey, oaepHash: 'sha256' }, Buffer.from(header.key, 'base64'));
      const decipher = createDecipheriv('aes-256-gcm', secret, Buffer.from(header.iv, 'base64'));
      decipher.setAAD(input.subarray(0, end)); decipher.setAuthTag(input.subarray(-16));
      return Buffer.concat([decipher.update(input.subarray(end, -16)), decipher.final()]);
    };
    const clear = decrypt(encrypted), length = clear.readUInt32BE(0);
    assert.deepEqual(JSON.parse(clear.subarray(4, 4 + length)), metadata);
    assert.deepEqual(clear.subarray(4 + length), bytes);
    encrypted[encrypted.length - 20] ^= 1;
    assert.throws(() => decrypt(encrypted));
  } finally { await rm(dir, { recursive: true, force: true }); }
});

// Opt-in real Linux/Docker regression. SOTY_BACKUP_TEST_IMAGE must name an
// already available image containing Node and tar; no production volumes are
// used. All test resources have a unique, guarded prefix and are removed.
test('Docker archive reads foreign-owner 0600 files while writes stay forbidden', {
  skip: process.platform !== 'linux' || !process.env.SOTY_BACKUP_TEST_IMAGE,
}, async () => {
  const image = process.env.SOTY_BACKUP_TEST_IMAGE;
  const name = `soty-backup-test-${randomBytes(8).toString('hex')}`;
  const command = args => spawnSync('docker', args, { encoding: 'utf8', timeout: 30_000 });
  const run = async args => new Promise((resolve, reject) => {
    const child = spawn('docker', args, { stdio: ['ignore', 'pipe', 'pipe'] });
    let bytes = 0, stderr = '';
    const timer = setTimeout(() => child.kill('SIGTERM'), 30_000);
    child.stdout.on('data', chunk => { bytes += chunk.length; });
    child.stderr.on('data', chunk => { if (stderr.length < 8192) stderr += chunk.toString(); });
    child.on('error', error => { clearTimeout(timer); reject(error); });
    child.on('close', code => { clearTimeout(timer); resolve({ code, bytes, denied: /Permission denied/.test(stderr), clean: stderr.length === 0 }); });
  });
  let containerId;
  assert.equal(command(['volume', 'create', name]).status, 0, 'create isolated synthetic volume');
  try {
    const setup = "const f=require('node:fs');f.mkdirSync('/data/restricted');f.writeFileSync('/data/restricted/payload','synthetic');f.chmodSync('/data/restricted/payload',0o600);f.chownSync('/data/restricted/payload',22001,22001);f.chmodSync('/data/restricted',0o700);f.chownSync('/data/restricted',22001,22001);";
    assert.equal(command(['run', '--rm', '--network', 'none', '--user', '0:0', '--mount', `type=volume,source=${name},target=/data`, '--entrypoint', 'node', image, '-e', setup]).status, 0, 'create foreign-owner fixture');
    const original = command(['create', '--name', name, '--mount', `type=volume,source=${name},target=/data`, image]);
    assert.equal(original.status, 0, 'create stopped synthetic volume owner');
    containerId = original.stdout.trim(); assert.match(containerId, /^[a-f0-9]{64}$/);
    const args = archiveHelperArgs({ containerId, helperName: name + '-archive', image });
    const legacy = [...args]; legacy.splice(legacy.indexOf('--cap-add'), 2);
    const denied = await run(legacy);
    assert.notEqual(denied.code, 0); assert.equal(denied.denied, true, 'old capability policy must reproduce the real production failure');
    const archived = await run(args);
    assert.equal(archived.code, 0); assert.equal(archived.clean, true); assert.ok(archived.bytes >= 2048);
    const confinement = args.slice(0, args.indexOf('--entrypoint'));
    const writeProbe = "try{require('node:fs').writeFileSync('/data/restricted/mutation','x');process.exitCode=1}catch(e){if(!['EROFS','EACCES'].includes(e.code))process.exitCode=2}";
    const blocked = await run([...confinement, '--entrypoint', 'node', image, '-e', writeProbe]);
    assert.equal(blocked.code, 0, 'read-only confinement must still reject writes');
  } finally {
    assert.match(name, /^soty-backup-test-[a-f0-9]{16}$/);
    command(['rm', '-f', name + '-archive']);
    if (containerId) command(['rm', '-f', containerId]);
    assert.equal(command(['volume', 'rm', name]).status, 0, 'remove isolated synthetic volume');
  }
});
