import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createHash, randomBytes } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import WebSocket from 'ws';
import { createAppsService } from '../server/index.mjs';

const identity = { linkId: 'source_channel_link', hostDeviceId: 'source_channel_host', connectorId: 'source_channel_connector' };
const actor = { accountId: 'source_channel_owner', deviceId: 'source_channel_browser' };
const auth = capabilities => ({ type: 'auth', schema: 'soty.apps-channel.v1', ...identity, token: 'ephemeral-local-test', ...(capabilities ? { capabilities } : {}) });
const delay = ms => new Promise(done => setTimeout(done, ms));
async function until(check, label) {
  const deadline = Date.now() + 4000;
  while (!check()) { if (Date.now() > deadline) throw new Error(`Timed out: ${label}`); await delay(5); }
}
async function fixture(t, authenticate = async () => true) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-source-channel-'));
  const service = createAppsService({ dataDir: directory, shellOrigins: ['http://127.0.0.1'],
    appOriginTemplate: 'http://{appId}.localhost', actorActive: value => value.accountId === actor.accountId,
    authenticateConnector: authenticate });
  const server = createServer((_req, res) => res.writeHead(404).end()), clients = [];
  server.on('upgrade', (req, socket, head) => { if (!service.handleUpgrade(req, socket, head)) socket.destroy(); });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  t.after(async () => {
    for (const client of clients) client.ws.terminate(); service.close();
    await new Promise(done => server.close(done));
    assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(basename(directory), /^soty-source-channel-/u);
    await rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  });
  const peer = async frame => {
    const ws = new WebSocket(`ws://127.0.0.1:${server.address().port}/api/apps/channel`), received = [];
    const value = { ws, received, closed: false }; clients.push(value);
    ws.on('error', () => {}); ws.on('message', bytes => received.push(JSON.parse(bytes.toString()))); ws.once('close', () => { value.closed = true; });
    await new Promise(done => ws.once('open', done)); if (frame) ws.send(JSON.stringify(frame)); return value;
  };
  return { service, peer, call(op, args = {}) { return service.execute({ actor, op, args }); } };
}

test('actual server negotiates v2 explicitly and requires a fresh channel identifier after reconnect', async t => {
  const f = await fixture(t), a = await f.peer(auth({ targetBindingVersions: [1, 2] }));
  await until(() => a.received.some(value => value.type === 'ready'), 'first ready');
  const first = a.received.find(value => value.type === 'ready');
  assert.equal(first.bindingVersion, 2); assert.match(first.channelId, /^[A-Za-z0-9_-]{43}$/u);
  const b = await f.peer(auth({ targetBindingVersions: [2] }));
  await until(() => b.received.some(value => value.type === 'ready') && a.closed, 'replacement ready');
  assert.notEqual(b.received.find(value => value.type === 'ready').channelId, first.channelId);
  b.ws.send(JSON.stringify({ type: 'observation', appId: `app-${'1'.repeat(32)}`, state: 'ready' }));
  await until(() => b.closed, 'legacy observation cannot enter v2');
});

test('a second auth while verification waits closes admission and late success cannot resurrect it', async t => {
  let release, entered = false;
  const f = await fixture(t, () => { entered = true; return new Promise(done => { release = done; }); });
  const peer = await f.peer(auth({ targetBindingVersions: [1, 2] }));
  await until(() => entered, 'authentication pending');
  peer.ws.send(JSON.stringify(auth({ targetBindingVersions: [1, 2] })));
  await until(() => peer.closed, 'second auth refused'); release(true); await delay(20);
  assert.equal(peer.received.some(value => value.type === 'ready'), false);
  assert.deepEqual(f.call('apps.devices').devices, []);
});

test('unsupported and malformed advertised modes never create an implicit legacy channel', async t => {
  const f = await fixture(t);
  for (const targetBindingVersions of [[], [3], ['2'], [1, 1], [1, 2, 0], [1, 2, 3, 4, 5, 6, 7, 8, 9]]) {
    const peer = await f.peer(auth({ targetBindingVersions }));
    await until(() => peer.closed, 'bad version offer refused');
    assert.equal(peer.received.some(value => value.type === 'ready'), false);
  }
});

test('an authenticated legacy device still serves target1 but cannot attest a source change', async t => {
  const f = await fixture(t), peer = await f.peer(auth());
  await until(() => peer.received.some(value => value.type === 'ready'), 'legacy ready');
  const ready = peer.received.find(value => value.type === 'ready'); assert.equal(ready.bindingVersion, undefined);
  const claimCode = randomBytes(32).toString('base64url');
  peer.ws.send(JSON.stringify({ type: 'claim', claimDigest: createHash('sha256').update(claimCode).digest('hex') }));
  await until(() => peer.received.some(value => value.type === 'claim-ready'), 'legacy claim');
  f.call('apps.claim', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, claimCode });
  const app = f.call('apps.register', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, name: 'Legacy source', port: 4201 }).app;
  assert.ok(f.call('apps.launch', { appId: app.id }).launchUrl);
  const policy = f.call('apps.publication.get', { appId: app.id });
  await assert.rejects(f.service.sourcePreparationExtension.executeAsync({ actor, op: 'apps.source.prepare', args: {
    appId: app.id, expectedPolicyEpoch: policy.policyEpoch, expectedTargetRevision: 1,
    source: { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, port: 4202, entryPath: '/' },
  } }), error => error.code === 'app_source_protocol_required');
  const history = f.call('apps.source.history', { appId: app.id });
  assert.equal(history.targets.length, 1); assert.equal(history.requiredBindingVersion, 1);
  assert.equal(peer.received.some(value => value.type === 'target-prepare'), false); assert.equal(peer.closed, false);
});
