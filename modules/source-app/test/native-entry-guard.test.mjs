import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createNativeEntryGuard } from '../server/native-entry-guard.mjs';
const turn = () => new Promise(done => setImmediate(done));
async function fixture(t, handler, overrides = {}) {
  const sockets = new Set();
  const server = createServer(handler); server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  const options = { rootCheckUrl: 'http://127.0.0.1:' + server.address().port + '/_soty/ingress-check',
    appId: 'app-' + 'a'.repeat(32), origin: 'https://native.apps.test', primaryTargetDigest: 'b'.repeat(64), nativeTargetDigest: 'c'.repeat(64), ...overrides };
  const guard = createNativeEntryGuard(options);
  t.after(async () => { guard.close(); for (const socket of sockets) socket.destroy(); await new Promise(done => server.close(done)); });
  const body = extra => ({ schema: 'soty.native-entry-policy.v1', appId: options.appId, origin: options.origin,
    primaryTargetRevision: 2, primaryTargetDigest: options.primaryTargetDigest, nativeTargetRevision: 1,
    nativeTargetDigest: options.nativeTargetDigest, policyEpoch: 2, admissionKeyHash: 'd'.repeat(64), leaseMs: 1000, ...extra });
  return { guard, sockets, body };
}

test('outstanding fresh HTTP checks are bounded before I/O and release capacity without evicting another request', async t => {
  const responses = [], f = await fixture(t, (_req, res) => responses.push(res), { maxRequests: 2, timeoutMs: 1000 });
  const one = f.guard.acquire(), two = f.guard.acquire();
  await assert.rejects(f.guard.acquire(), { code: 'native_entry_request_capacity', status: 503 });
  while (responses.length < 2) await turn(); assert.equal(f.sockets.size, 2);
  responses[0].writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify(f.body())); await one;
  const three = f.guard.acquire(); while (responses.length < 3) await turn();
  responses[1].writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify(f.body())); await two;
  responses[2].writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify(f.body())); await three;
});

test('nonending Root error and wrong content-type bodies are destroyed, never detached from timeout or close custody', async t => {
  for (const status of [403, 200]) await t.test(String(status), async sub => {
    let aborted;
    const close = new Promise(done => { aborted = done; });
    const f = await fixture(sub, (_req, res) => { res.once('close', aborted); res.writeHead(status, { 'content-type': 'text/plain' }); res.write('error never ends'); });
    await assert.rejects(f.guard.acquire(), { code: 'native_entry_policy_denied' });
    await close; await turn(); assert.equal(f.sockets.size, 0);
  });
});

test('fixed guard sends only exact operator pins, no forwarded cookie, and rejects redirect, oversized and mismatched policy', async t => {
  let observed, mode = 'good', f;
  f = await fixture(t, (req, res) => { observed = req.headers;
    if (mode === 'redirect') { res.writeHead(302, { location: 'https://other.test/_soty/ingress-check' }).end(); return; }
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(mode === 'large' ? ' '.repeat(5000) : JSON.stringify(f.body(mode === 'mismatch' ? { nativeTargetDigest: 'e'.repeat(64) } : {})));
  });
  const lease = await f.guard.acquire(); f.guard.assertCurrent(lease);
  assert.equal(observed.host, 'native.apps.test'); assert.equal(observed.cookie, undefined); assert.equal(observed.authorization, undefined);
  assert.equal(observed['x-soty-ingress-primary'], 'b'.repeat(64));
  assert.throws(() => f.guard.assertCurrent({ ...lease }), { code: 'native_entry_lease_closed' });
  for (const value of ['redirect','large','mismatch']) { mode = value;
    await assert.rejects(f.guard.acquire(), error => ['native_entry_policy_denied','native_entry_policy_invalid'].includes(error.code)); }
});

test('slow successful response consumes its lease and close cancels all owned pending requests', async t => {
  let f;
  f = await fixture(t, (_req, res) => { setTimeout(() => { if (!res.destroyed) res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify(f.body())); }, 120); },
    { leaseMs: 100, heartbeatMs: 25, timeoutMs: 500 });
  await assert.rejects(f.guard.acquire(), { code: 'native_entry_lease_closed' });
  const pending = f.guard.acquire(); const denied = assert.rejects(pending, { code: 'native_entry_policy_unavailable' });
  f.guard.close(); await denied; await turn();
  await assert.rejects(f.guard.acquire(), { code: 'native_entry_closed' });
});
