import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import { request } from 'node:http';
import { mkdtemp, readdir, readFile, rm, utimes, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { attachAccountTransfer } from '../account-transfer.js';

const lookup = index => `audit${String(index).padStart(27, '0')}`;
const envelope = (length = 64) => ({ schema: 'soty.account-phrase-backup.v1', createdAt: '2001-01-01T00:00:00Z',
  kdf: { name: 'PBKDF2', hash: 'SHA-256', iterations: 100_000, salt: 'a'.repeat(24) },
  cipher: { name: 'AES-GCM', nonce: 'b'.repeat(16), ciphertext: 'c'.repeat(length) } });
const size = value => Buffer.byteLength(`${JSON.stringify(value)}\n`);
async function fixture(directory, { limits, clock } = {}) {
  const app = express(); attachAccountTransfer(app, { dataDir: directory, transferLimits: limits, clock });
  const server = app.listen(0, '127.0.0.1'); await new Promise(resolve => server.once('listening', resolve));
  const origin = `http://127.0.0.1:${server.address().port}`;
  return { origin,
    put: (id, value = envelope(), headers = {}) => fetch(`${origin}/api/account-transfer/${id}`, { method: 'PUT', headers: { 'Content-Type': 'application/json', ...headers }, body: JSON.stringify(value) }),
    get: id => fetch(`${origin}/api/account-transfer/${id}`),
    close: async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); } };
}

test('legacy anonymous recovery remains compatible while file quota survives a server restart', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-transfer-count-'));
  let host = await fixture(directory, { limits: { maxFiles: 2, minFreeBytes: 0 } });
  try {
    assert.equal((await host.put(lookup(1))).status, 200);
    assert.equal((await host.put(lookup(2))).status, 200);
    assert.equal((await host.put(lookup(3))).status, 507);
    assert.deepEqual(await (await host.get(lookup(1))).json(), envelope());
    await host.close(); host = await fixture(directory, { limits: { maxFiles: 2, minFreeBytes: 0 } });
    assert.equal((await host.put(lookup(3))).status, 507);
    assert.equal((await host.put(lookup(1), envelope(32))).status, 200);
    assert.deepEqual(await (await host.get(lookup(1))).json(), envelope(32));
    assert.equal((await readdir(join(directory, 'account-transfer'))).length, 2);
  } finally { await host.close(); await rm(directory, { recursive: true, force: true }); }
});

test('byte quota covers replacement growth and concurrent creation without discarding recoverable backups', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-transfer-bytes-'));
  const limit = size(envelope()) * 2;
  let host = await fixture(directory, { limits: { maxTotalBytes: limit, minFreeBytes: 0 } });
  try {
    const responses = await Promise.all([host.put(lookup(1)), host.put(lookup(2)), host.put(lookup(3))]);
    assert.deepEqual(responses.map(value => value.status).sort(), [200, 200, 507]);
    const files = await readdir(join(directory, 'account-transfer'));
    const saved = files[0].replace('.json', '');
    assert.equal((await host.put(saved, envelope(65))).status, 507);
    assert.deepEqual(await (await host.get(saved)).json(), envelope());
    await host.close(); host = await fixture(directory, { limits: { maxTotalBytes: 1, minFreeBytes: 0 } });
    assert.deepEqual(await (await host.get(saved)).json(), envelope());
    assert.equal((await host.put(saved, envelope(1))).status, 200);
    assert.deepEqual(await (await host.get(saved)).json(), envelope(1));
  } finally { await host.close(); await rm(directory, { recursive: true, force: true }); }
});

test('peer limits cannot be reset by forwarding headers and expire without expiring recovery data', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-transfer-rate-'));
  let now = Date.now();
  const host = await fixture(directory, { limits: { peerWritesPerHour: 2, peerReadsPerMinute: 2, minFreeBytes: 0 }, clock: () => now });
  try {
    assert.equal((await host.put(lookup(1))).status, 200);
    assert.equal((await host.put(lookup(2))).status, 200);
    const blocked = await host.put(lookup(3), envelope(), { 'X-Forwarded-For': '198.51.100.123', 'X-Real-IP': '198.51.100.124' });
    assert.equal(blocked.status, 429); assert.equal(blocked.headers.get('cache-control'), 'no-store');
    assert.equal((await host.get(lookup(1))).status, 200);
    assert.equal((await host.get(lookup(1))).status, 200);
    assert.equal((await host.get(lookup(1))).status, 429);
    now += 3_600_001;
    assert.equal((await host.put(lookup(3))).status, 200);
    assert.deepEqual(await (await host.get(lookup(1))).json(), envelope());
  } finally { await host.close(); await rm(directory, { recursive: true, force: true }); }
});

test('incomplete request bodies have bounded admission before parsing and free their slot on abort', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-transfer-admission-'));
  const host = await fixture(directory, { limits: { maxConcurrentWrites: 1, minFreeBytes: 0 } });
  let pending;
  try {
    pending = request(`${host.origin}/api/account-transfer/${lookup(1)}`, { method: 'PUT', headers: { 'Content-Type': 'application/json', 'Content-Length': '1024' } });
    pending.on('error', () => undefined);
    pending.flushHeaders(); pending.write('{');
    await new Promise(resolve => setTimeout(resolve, 30));
    assert.equal((await host.put(lookup(2))).status, 503);
    pending.destroy(); await new Promise(resolve => pending.once('close', resolve));
    await new Promise(resolve => setTimeout(resolve, 30));
    assert.equal((await host.put(lookup(2))).status, 200);
  } finally { pending?.destroy(); await host.close(); await rm(directory, { recursive: true, force: true }); }
});

test('only abandoned staging files expire; valid old recovery files remain readable', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-transfer-staging-'));
  const host = await fixture(directory, { limits: { minFreeBytes: 0 } });
  try {
    assert.equal((await host.put(lookup(1))).status, 200);
    const root = join(directory, 'account-transfer');
    const old = join(root, `${lookup(1)}.json.123.456.tmp`), recent = join(root, `${lookup(2)}.json.still-writing.tmp`);
    await writeFile(old, 'abandoned encrypted staging'); await writeFile(recent, 'pending encrypted staging');
    const before = new Date(Date.now() - 48 * 3_600_000); await utimes(old, before, before);
    assert.equal((await host.put(lookup(2))).status, 200);
    const names = await readdir(root);
    assert.equal(names.includes(`${lookup(1)}.json.123.456.tmp`), false);
    assert.equal(names.includes(`${lookup(2)}.json.still-writing.tmp`), true);
    assert.deepEqual(await (await host.get(lookup(1))).json(), envelope());
  } finally { await host.close(); await rm(directory, { recursive: true, force: true }); }
});

test('malformed and oversized bodies return fixed errors without allocating durable files', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-transfer-invalid-'));
  const host = await fixture(directory, { limits: { minFreeBytes: 0 } });
  try {
    const invalid = await fetch(`${host.origin}/api/account-transfer/${lookup(1)}`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: '{invalid' });
    assert.equal(invalid.status, 400); assert.deepEqual(await invalid.json(), { ok: false, error: 'bad_account_transfer' });
    const huge = await host.put(lookup(2), envelope(9_000_000)); assert.equal(huge.status, 413);
    assert.deepEqual(await huge.json(), { ok: false, error: 'bad_account_transfer' });
    assert.equal((await host.get(lookup(1))).status, 404);
    await assert.rejects(readFile(join(directory, 'account-transfer', `${lookup(2)}.json`)), { code: 'ENOENT' });
  } finally { await host.close(); await rm(directory, { recursive: true, force: true }); }
});
