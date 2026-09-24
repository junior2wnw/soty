import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import { createServer, request } from 'node:http';
import { mkdtemp, mkdir, writeFile, open, symlink } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { attachConnectReleaseSource } from './connect-release-source.js';

async function fixture(t) {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'connect-feed-'));
  const artifact = Buffer.from('{"signed":{"sequence":1},"signatures":[],"contents":{}}\n');
  await writeFile(path.join(directory, 'stable.json'), artifact);
  await writeFile(path.join(directory, 'release-1.json'), artifact);
  await writeFile(path.join(directory, 'private.pem'), 'never serve this file');
  const app = express(); let fallbacks = 0;
  attachConnectReleaseSource(app, { directory });
  app.use((_req, res) => { fallbacks++; res.send('SPA'); });
  const server = createServer(app); await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const port = server.address().port;
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); });
  return { directory, artifact, get fallbacks() { return fallbacks; }, async get(target, method = 'GET') {
    return new Promise((resolve, reject) => {
      const req = request({ hostname: '127.0.0.1', port, path: target, method, agent: false }, res => {
        const chunks = []; res.on('data', chunk => chunks.push(chunk));
        res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, bytes: Buffer.concat(chunks) }));
      });
      req.on('error', reject); req.end();
    });
  } };
}

test('strict feed routes preserve exact bytes and separate stable from immutable cache policy', async t => {
  const f = await fixture(t);
  for (const name of ['stable.json', 'release-1.json']) {
    const result = await f.get(`/releases/connect/${name}`);
    assert.equal(result.status, 200); assert.deepEqual(result.bytes, f.artifact);
    assert.equal(result.headers['content-type'], 'application/json; charset=utf-8');
    assert.equal(result.headers['x-content-type-options'], 'nosniff');
    assert.equal(result.headers.location, undefined);
    assert.equal(result.headers['cache-control'], name === 'stable.json' ? 'no-store' : 'public, max-age=31536000, immutable');
  }
  assert.equal(f.fallbacks, 0);
});

test('HEAD and write methods are rejected explicitly instead of entering the SPA', async t => {
  const f = await fixture(t);
  for (const method of ['HEAD', 'POST', 'PUT', 'DELETE', 'OPTIONS']) {
    const result = await f.get('/releases/connect/stable.json', method);
    assert.equal(result.status, 405); assert.equal(result.headers.allow, 'GET');
    assert.equal(result.headers['cache-control'], 'no-store');
  }
  assert.equal(f.fallbacks, 0);
});

test('unknown, encoded, traversal, case, query and noncanonical sequence paths never expose files or fall back', async t => {
  const f = await fixture(t);
  for (const target of ['/releases/connect', '/releases/connect/', '/releases/connect/private.pem',
    '/releases/connect/../private.pem', '/releases/connect/%2e%2e/private.pem',
    '/releases/%63onnect/stable.json', '/releases/connect/%73table.json',
    '/RELEASES/CONNECT/stable.json', '/releases/connect/release-01.json', '/releases/connect/release-0.json',
    '/releases/connect/release-9007199254740992.json', '/releases/connect/release-2.json',
    '/releases/connect/stable.json?download=1', '/releases/connect/stable.json/',
    'http://localhost/releases/connect/stable.json']) {
    const result = await f.get(target); assert.equal(result.status, 404, target);
    assert.equal(result.headers.location, undefined); assert.equal(result.headers['cache-control'], 'no-store');
    assert.ok(!result.bytes.toString().includes('never serve')); assert.ok(!result.bytes.toString().includes(f.directory));
  }
  assert.equal(f.fallbacks, 0);
  const unrelated = await f.get('/room'); assert.equal(unrelated.status, 200); assert.equal(f.fallbacks, 1);
});

test('directories and oversized artifacts fail closed without a SPA response', async t => {
  const f = await fixture(t);
  await mkdir(path.join(f.directory, 'release-2.json'));
  const large = await open(path.join(f.directory, 'release-3.json'), 'w');
  await large.truncate(12 * 1024 * 1024 + 1); await large.close();
  for (const name of ['release-2.json', 'release-3.json']) {
    const result = await f.get(`/releases/connect/${name}`); assert.equal(result.status, 503);
    assert.equal(result.headers['cache-control'], 'no-store');
  }
  assert.equal(f.fallbacks, 0);
});

test('a symlink named like a release cannot expose its target', async t => {
  const f = await fixture(t);
  try { await symlink(path.join(f.directory, 'private.pem'), path.join(f.directory, 'release-2.json')); }
  catch (error) { if (error.code === 'EPERM' || error.code === 'ENOTSUP') { t.skip('filesystem does not permit test symlinks'); return; } throw error; }
  const result = await f.get('/releases/connect/release-2.json');
  assert.equal(result.status, 503); assert.ok(!result.bytes.toString().includes('never serve'));
  assert.equal(f.fallbacks, 0);
});
