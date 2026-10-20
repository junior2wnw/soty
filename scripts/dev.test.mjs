import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { generateKeyPairSync, sign } from 'node:crypto';
import { mkdir, mkdtemp, rm, stat, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';
import WebSocket from 'ws';
import { startDevelopment } from './dev.mjs';
import { awaitDevelopment } from './dev-lifecycle.mjs';
import { digestArgs } from '../modules/connect/server/index.mjs';

test('fullstack development serves Vite, signed API and origin-checked WS, then closes both listeners', { timeout: 35_000 }, async t => {
  const fetch = (url, options) => ownedFetch(t.signal, url, options);
  const dataDir = await mkdtemp(join(tmpdir(), 'soty-dev-test-'));
  const privateRoot = fileURLToPath(new URL('../var/', import.meta.url));
  await mkdir(privateRoot, { recursive: true });
  const privateDir = await mkdtemp(join(privateRoot, 'dev-deny-test-'));
  await writeFile(join(privateDir, 'probe.txt'), 'Test-only private data');
  const ports = await freePorts(3);
  let dev;
  t.after(async () => {
    await dev?.close();
    for (const [dir, parent] of [[dataDir, tmpdir()], [privateDir, privateRoot]]) {
      assert.equal(dirname(resolve(dir)), resolve(parent)); await rm(dir, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
    }
  });
  dev = await startDevelopment({ port: ports[0], apiPort: ports[1], connectorPort: ports[2], dataDir, quiet: true, signal: t.signal });
  const html = await fetch(dev.origin); assert.equal(html.status, 200); assert.match(await html.text(), /\/src\/entry\.ts/u);
  assert.equal((await fetch(`${dev.origin}/src/platform/world-adapter.ts`)).status, 200);
  assert.equal((await fetch(`${dev.origin}/var/${privateDir.slice(privateRoot.length).replaceAll('\\', '/')}/probe.txt`)).status, 403, 'Vite must not serve server data under the project root');
  assert.equal((await (await fetch(`${dev.origin}/health`)).json()).ok, true);
  const docs = await fetch(`${dev.origin}/agents`), docsHtml = await docs.text();
  assert.equal(docs.status, 200); assert.match(docsHtml, /<form\b/u);
  assert.ok(docsHtml.includes(`${dev.origin}/agents`), 'canonical must use the configured frontend origin');
  assert.ok(!docsHtml.includes('/@vite/client') && !docsHtml.includes('/src/entry.ts'), 'SSR documentation must bypass the SPA transform');
  const missingDocs = await fetch(`${dev.origin}/agents/missing`);
  assert.equal(missingDocs.status, 404); assert.equal((await missingDocs.json()).error.code, 'not_found');
  const machine = await fetch(`${dev.origin}/api/capabilities/v1/catalog?query=save+note`);
  assert.equal(machine.status, 200); assert.equal((await machine.json()).items[0].capabilityId, 'notes.createDraft');
  const sitemap = await fetch(`${dev.origin}/agents/sitemap.xml`);
  assert.equal(sitemap.status, 200); assert.ok((await sitemap.text()).includes(`${dev.origin}/agents`));
  assert.match(await (await fetch(`${dev.origin}/agents-old`)).text(), /\/src\/entry\.ts/u);
  const capabilities = await (await fetch(`${dev.origin}/api/apps/capabilities`)).json();
  assert.equal(capabilities.configured, true); assert.equal(capabilities.localConnectorOrigin, `http://127.0.0.1:${ports[2]}`);
  const signing = generateKeyPairSync('ec', { namedCurve: 'prime256v1' }), encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const rpc = (operation, args) => signedRpc(dev.origin, signing, operation, args, t.signal);
  const boot = await rpc('bootstrap', { label: 'Dev integration', encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }) });
  assert.equal(boot.ok, true, boot.error?.code);
  const profile = await rpc('world.profile.get', {}); assert.equal(profile.ok, true, profile.error?.code);
  assert.equal((await stat(join(dataDir, 'connect', 'accounts.sqlite'))).isFile(), true);
  const forbidden = await fetch(`${dev.origin}/api/connect/rpc`, { method: 'POST', headers: { Origin: 'https://untrusted.invalid', 'Content-Type': 'application/json' }, body: JSON.stringify({ protocol: 1, op: 'challenge', args: { operation: 'world.profile.get', digest: digestArgs({}) } }) });
  assert.equal(forbidden.status, 400); assert.match((await forbidden.json()).error.code, /origin/u);

  const wsUrl = `${dev.origin.replace('http:', 'ws:')}/ws/dev_room_12345678901234567890`;
  const ws = ownedSocket(t, wsUrl, { origin: dev.origin });
  await awaitDevelopment(new Promise((resolveOpen, reject) => { ws.once('open', resolveOpen); ws.once('error', reject); }), t.signal);
  const closed = new Promise(resolveClose => ws.once('close', resolveClose)); ws.close(); await awaitDevelopment(closed, t.signal);
  const denied = ownedSocket(t, wsUrl, { origin: 'https://untrusted.invalid' });
  const deniedOutcome = await awaitDevelopment(new Promise(resolveDenied => { denied.once('open', () => { denied.terminate(); resolveDenied('open'); }); denied.once('error', () => resolveDenied('denied')); }), t.signal);
  assert.equal(deniedOutcome, 'denied', 'the dev proxy must preserve the original WebSocket Origin');

  const appHost = `app-${'0'.repeat(32)}.localhost:${ports[1]}`;
  const isolated = await localRequest(ports[1], appHost, '/', t.signal);
  assert.equal(isolated.status, 404); assert.match(isolated.body, /app_not_found/u);
  assert.equal(isolated.headers['x-frame-options'], undefined, 'app hosts must reach the gateway before the shell CSP');
  const worker = await fetch(`${dev.origin}/sw.js`); assert.equal(worker.headers.get('cache-control'), 'no-store');
  assert.match(await worker.text(), /registration\.unregister/u);
  await dev.close(); assert.equal(await dev.done, null);
  const reclaimed = await Promise.all(ports.slice(0, 2).map(listen));
  await Promise.all(reclaimed.map(server => new Promise(resolveClose => server.close(resolveClose))));
});

test('an occupied API port fails startup and leaves the UI port free', { timeout: 20_000 }, async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'soty-dev-conflict-'));
  const ports = await freePorts(3), occupied = await listen(ports[1]);
  t.after(async () => { await new Promise(resolveClose => occupied.close(resolveClose)); assert.equal(dirname(resolve(dataDir)), resolve(tmpdir())); await rm(dataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 }); });
  await assert.rejects(startDevelopment({ port: ports[0], apiPort: ports[1], connectorPort: ports[2], dataDir, quiet: true, signal: t.signal }), /Cannot start development API/u);
  const unused = await listen(ports[0]); await new Promise(resolveClose => unused.close(resolveClose));
});

test('named-apps development isolates canonical and named zones from the shell', { timeout: 35_000 }, async t => {
  const fetch = (url, options) => ownedFetch(t.signal, url, options);
  const dataDir = await mkdtemp(join(tmpdir(), 'soty-dev-named-'));
  const ports = await freePorts(3);
  let dev;
  t.after(async () => {
    await dev?.close();
    assert.equal(dirname(resolve(dataDir)), resolve(tmpdir()));
    await rm(dataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  });
  dev = await startDevelopment({ port: ports[0], apiPort: ports[1], connectorPort: ports[2], dataDir, namedApps: true, quiet: true, signal: t.signal });
  const capabilities = await (await fetch(`${dev.origin}/api/apps/capabilities`)).json();
  assert.equal(capabilities.configured, true);
  for (const host of [`app-${'0'.repeat(32)}.legacy.localhost:${ports[1]}`, `new-app.named.localhost:${ports[1]}`]) {
    const result = await localRequest(ports[1], host, '/', t.signal);
    assert.equal(result.status, 404); assert.match(result.body, /app_not_found/u);
    assert.equal(result.headers['x-frame-options'], undefined);
  }
  const shell = await localRequest(ports[1], `127.0.0.1:${ports[0]}`, '/health', t.signal);
  assert.equal(shell.status, 200);
  const csp = shell.headers['content-security-policy'];
  assert.ok(csp.includes(`http://*.legacy.localhost:${ports[1]}`), csp);
  assert.ok(csp.includes(`http://*.named.localhost:${ports[1]}`));
  assert.ok(!csp.includes(`http://*.localhost:${ports[1]}`));
});

function listen(port = 0) { return new Promise((resolveListen, reject) => { const server = createServer(); server.once('error', reject); server.listen(port, '127.0.0.1', () => resolveListen(server)); }); }
async function freePorts(count) { const servers = await Promise.all(Array.from({ length: count }, () => listen())); const ports = servers.map(server => server.address().port); await Promise.all(servers.map(server => new Promise(resolveClose => server.close(resolveClose)))); return ports; }
async function signedRpc(origin, principal, op, args, signal) {
  const call = async body => (await ownedFetch(signal, `${origin}/api/connect/rpc`, { method: 'POST', headers: { Origin: origin, 'Content-Type': 'application/json' }, body: JSON.stringify(body) })).json();
  const challenge = await call({ protocol: 1, op: 'challenge', args: { operation: op, digest: digestArgs(args) } }); assert.equal(challenge.ok, true, challenge.error?.code);
  const signature = sign('sha256', Buffer.from(challenge.message), { key: principal.privateKey, dsaEncoding: 'ieee-p1363' }).toString('base64url');
  return call({ protocol: 1, op, args, proof: { challengeId: challenge.challengeId, publicJwk: principal.publicKey.export({ format: 'jwk' }), signature } });
}
function localRequest(port, host, path = '/', signal) { return new Promise((resolveRequest, reject) => { const req = request({ hostname: '127.0.0.1', port, path, headers: { Host: host }, signal }, res => { const bytes = []; res.on('data', chunk => bytes.push(chunk)); res.once('error', reject); res.on('end', () => resolveRequest({ status: res.statusCode, headers: res.headers, body: Buffer.concat(bytes).toString('utf8') })); }); req.once('error', reject); req.end(); }); }

function ownedFetch(signal, url, options = {}) { return globalThis.fetch(url, { ...options, signal }); }
function ownedSocket(t, url, options) {
  const socket = new WebSocket(url, options);
  const stop = () => { if (socket.readyState !== WebSocket.CLOSED) socket.terminate(); };
  socket.on('error', () => {}); // Waiting assertions retain their own error listeners.
  t.signal.addEventListener('abort', stop, { once: true });
  socket.once('close', () => t.signal.removeEventListener('abort', stop));
  t.after(async () => {
    if (socket.readyState === WebSocket.CLOSED) return;
    const closed = new Promise(resolve => socket.once('close', resolve)); stop(); await closed;
  });
  if (t.signal.aborted) stop();
  return socket;
}
