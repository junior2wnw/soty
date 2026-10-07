import test from 'node:test';
import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { createServer } from 'node:http';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, resolve, basename } from 'node:path';
import { fileURLToPath } from 'node:url';
import { randomBytes } from 'node:crypto';
import { sqliteSource, digest } from './support/sqlite-source.mjs';
const random = () => randomBytes(32).toString('base64url');
test('actual two OS processes share Source encrypted CAS head and send one refresh', async t => {
  const directory = mkdtempSync(join(tmpdir(), 'source-rp-processes-')), path = join(directory, 'source.sqlite'), time = Date.now();
  const native = sqliteSource(path), marker = { sessionIdHash: digest(random()), profileDigest: digest('reviewed-profile'),
    bindingDigest: digest('same-selected-resource'), issuer: 'https://root.fixture/human-identity', subject: 'same-sub',
    createdAt: time - 280000, sessionExpiresAt: time - 280000 + 86400000 };
  await native.seed(marker, { accessToken: random(), refreshToken: random(), nonce: random() }, time + 20000);
  let sends = 0;
  const server = createServer(async (req, res) => {
    if (req.method !== 'POST' || req.url !== '/renew') return res.writeHead(404).end();
    let size = 0; for await (const part of req) { size += part.length; if (size > 1024) return res.writeHead(413).end(); }
    sends++; await new Promise(done => setTimeout(done, 100));
    res.writeHead(200, { 'content-type': 'application/json' }); res.end(JSON.stringify({ accessToken: random(), refreshToken: random() }));
  });
  await new Promise(done => server.listen(0, '127.0.0.1', done)); const children = [];
  t.after(async () => { children.forEach(child => { if (child.exitCode === null) child.kill(); }); server.closeAllConnections();
    await new Promise(done => server.close(done)); native.close(); assert.equal(dirname(resolve(directory)), resolve(tmpdir()));
    assert.match(basename(directory), /^source-rp-processes-/u); rmSync(directory, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 }); });
  const run = () => new Promise((resolveResult, reject) => {
    const child = fork(fileURLToPath(new URL('./support/worker.mjs', import.meta.url)), [], { windowsHide: true, silent: true,
      execPath: process.execPath }); children.push(child);
    let result, outputBytes = 0; child.stdout.on('data', data => { outputBytes += data.length; }); child.stderr.on('data', data => { outputBytes += data.length; });
    child.on('message', value => { result = value; }); child.on('error', reject);
    child.on('exit', code => code === 0 && result ? resolveResult(result) : reject(new Error('fixture_process_failed:' + code + ':bytes=' + outputBytes)));
    child.send({ path, marker, time, key: native.key.toString('base64url'), origin: `http://127.0.0.1:${server.address().port}` });
  });
  const results = await Promise.all([run(), run()]);
  assert.equal(results.every(result => result.ok && result.generation === 1), true, 'both private processes resolve same generation');
  assert.equal(sends, 1); assert.equal(native.head(marker.sessionIdHash).revision, 1);
});
