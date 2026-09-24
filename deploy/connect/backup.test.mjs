import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, privateDecrypt, createDecipheriv } from 'node:crypto';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { Readable } from 'node:stream';
import { encryptBackup, MAGIC } from './backup.mjs';
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
