import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { createServer, request } from 'node:http';
import { lstat, mkdir, mkdtemp, open, opendir, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { createAppsService } from '../server/index.mjs';
import { createPreparedBrowse } from '../server/prepared-browse.mjs';
import { connectorKey } from '../server/protocol.mjs';
import { createLocalAppsRuntime, normalizeLocalAppManifest, probeLocalApp } from '../../../scripts/agent-modules/local-apps.mjs';
import { createPreparedProjectBrowser } from '../../../scripts/agent-modules/prepared-projects.mjs';
import { parseSourceJson } from '../../source-app/shared/strict-json.mjs';

const secret = () => randomBytes(32).toString('base64url');
const digest = value => createHash('sha256').update(value).digest('hex');
async function until(predicate) {
  const end = Date.now() + 5000;
  while (!predicate()) { if (Date.now() >= end) throw new Error('fixture_timeout'); await delay(10); }
}
const gate = () => {
  let release, enter;
  const wait = new Promise(done => { release = done; }), entered = new Promise(done => { enter = done; });
  return { release, enter, wait, entered };
};

test('Root forwards a bounded busy response as a retryable error', async () => {
  const identity = { linkId: 'busy-link', hostDeviceId: 'busy-host', connectorId: 'busy-connector' }, key = connectorKey(identity);
  const channel = { key, identity, bindingVersion: 2, preparedBrowse: true, channelId: secret(), ws: { readyState: 1 }, auth: {} };
  const channels = new Map([[key, channel]]); let registry;
  registry = createPreparedBrowse({ channels, secret, assertActor: () => {}, resolveOwnedDevice: () => identity,
    authenticateConnector: async () => true, connectorCurrent: () => true, blockedPorts: [],
    send: (_channel, frame) => { queueMicrotask(() => registry.handle(channel,
      { type: 'prepared-browse-result', channelId: channel.channelId, nonce: frame.nonce, ok: false, code: 'prepared_busy' })); return true; } });
  try { await assert.rejects(registry.browse({ accountId: 'owner', deviceId: 'phone' }, { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId }),
    { code: 'prepared_busy', status: 429 }); }
  finally { registry.close(); }
});

test('only the current owner can browse a real permitted project through its authenticated connector', { timeout: 30000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'soty-prepared-channel-'));
  const aliceRoot = join(dir, 'alice'), bobRoot = join(dir, 'bob');
  await mkdir(join(aliceRoot, 'project', '.soty'), { recursive: true }); await mkdir(join(bobRoot, 'private'), { recursive: true });
  const app = createServer((_req, res) => res.writeHead(200).end());
  await new Promise(done => app.listen(0, '127.0.0.1', done));
  await writeFile(join(aliceRoot, 'project', '.soty', 'app.json'), JSON.stringify({ schema: 'soty.local-app.v1', name: 'Личный проект', port: app.address().port }));
  let service, pause, validConnector = true; const inactive = new Set(), runtimes = [];
  const root = createServer((req, res) => service?.handleRequest(req, res) || res.writeHead(404).end());
  root.on('upgrade', (req, socket, head) => { if (!service.handleUpgrade(req, socket, head)) socket.destroy(); });
  await new Promise(done => root.listen(0, '127.0.0.1', done));
  const origin = `http://127.0.0.1:${root.address().port}`;
  const alice = { accountId: 'alice', deviceId: 'alice-phone' }, bob = { accountId: 'bob', deviceId: 'bob-phone' };
  service = createAppsService({ databasePath: join(dir, 'apps.sqlite'), shellOrigins: [origin],
    actorActive: actor => [alice, bob].some(user => user.accountId === actor?.accountId && user.deviceId === actor?.deviceId) && !inactive.has(actor.deviceId),
    authenticateConnector: async auth => validConnector && auth.token === 'fixture-token', connectorCurrent: auth => validConnector && auth.token === 'fixture-token' });
  function runtime(identity, allowedRoot, supported = true) {
    const browser = createPreparedProjectBrowser({ allowedRoots: () => [allowedRoot], lstat, open, opendir, realpath,
      basename, isAbsolute, join, relative, sep, digest, normalizeManifest: normalizeLocalAppManifest, parseJson: parseSourceJson,
      blockedPorts: [], probe: value => probeLocalApp({ httpRequest: request }, value) });
    const instance = createLocalAppsRuntime({ randomSecret: secret, digest, createWebSocket: url => new WebSocket(url), httpRequest: request,
      encodeBase64: bytes => Buffer.from(bytes).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64') },
    { serverUrl: origin, identity, token: 'fixture-token', ...(supported ? { preparedBrowse: async input => {
      if (pause) { pause.enter(); await pause.wait; } return browser.list(input);
    }, preparedInspect: input => browser.inspect(input) } : {}) });
    runtimes.push(instance); instance.start(); return instance;
  }
  t.after(async () => { pause?.release(); for (const item of runtimes) item.stop(); service.close(); root.closeAllConnections(); app.closeAllConnections();
    await Promise.all([new Promise(done => root.close(done)), new Promise(done => app.close(done))]);
    assert.equal(dirname(resolve(dir)), resolve(tmpdir())); assert.match(basename(dir), /^soty-prepared-channel-/u);
    await rm(dir, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 }); });
  const aliceDevice = { linkId: 'link-a', hostDeviceId: 'computer-a', connectorId: 'connector-a', name: 'Alice' };
  const bobDevice = { linkId: 'link-b', hostDeviceId: 'computer-b', connectorId: 'connector-b', name: 'Bob' };
  let aliceRuntime = runtime(aliceDevice, aliceRoot), bobRuntime = runtime(bobDevice, bobRoot);
  await until(() => aliceRuntime.status().connected && bobRuntime.status().connected);
  for (const [actor, device, connector] of [[alice, aliceDevice, aliceRuntime], [bob, bobDevice, bobRuntime]]) {
    const claim = await connector.claim(); service.execute({ op: 'apps.claim', actor, args: { hostDeviceId: device.hostDeviceId, connectorId: device.connectorId, claimCode: claim.claimCode } });
  }
  const browse = (actor, device, input = {}) => service.sourcePreparationExtension.executeAsync({ op: 'apps.prepared.browse', actor,
    args: { expectedAccountId: actor.accountId, hostDeviceId: device.hostDeviceId, connectorId: device.connectorId, ...input } });
  const roots = await browse(alice, aliceDevice), rootId = roots.roots[0].id;
  assert.equal(roots.roots[0].label, 'alice');
  assert.deepEqual((await browse(alice, aliceDevice, { location: { rootId, segments: [] } })).entries, ['project']);
  const preview = await browse(alice, aliceDevice, { location: { rootId, segments: ['project'] } });
  assert.equal(preview.project.name, 'Личный проект'); assert.equal(preview.project.responding, true);
  const inspection = await service.inspectPrepared(alice, { hostDeviceId: aliceDevice.hostDeviceId, connectorId: aliceDevice.connectorId,
    location: { rootId, segments: ['project'] }, digest: preview.project.digest });
  assert.equal(inspection.manifest.name, 'Личный проект'); inspection.assertCurrent();
  validConnector = false; assert.throws(() => inspection.assertCurrent(), { code: 'prepared_device_unavailable' }); validConnector = true;
  const bobRoots = await browse(bob, bobDevice); assert.equal(bobRoots.roots[0].label, 'bob');
  await assert.rejects(browse(bob, aliceDevice), { code: 'apps_device_not_owned' });
  await assert.rejects(browse(alice, bobDevice), { code: 'apps_device_not_owned' });
  await assert.rejects(browse(alice, aliceDevice, { location: { rootId, segments: ['..'] } }), { code: 'prepared_invalid_request' });

  pause = gate(); const duringRevocation = browse(alice, aliceDevice); await pause.entered;
  inactive.add(alice.deviceId); pause.release(); await assert.rejects(duringRevocation, { code: 'apps_authentication_required' });
  pause = null; inactive.clear();
  pause = gate(); const duringConnectorRevocation = browse(alice, aliceDevice); await pause.entered;
  validConnector = false; pause.release(); await assert.rejects(duringConnectorRevocation, { code: 'prepared_device_unavailable' });
  pause = null; validConnector = true;

  pause = gate(); const duringDisconnect = browse(alice, aliceDevice); await pause.entered;
  service.invalidateConnector({ hostDeviceId: aliceDevice.hostDeviceId, connectorId: aliceDevice.connectorId });
  await assert.rejects(duringDisconnect, { code: 'prepared_device_unavailable' }); pause.release(); pause = null;

  aliceRuntime.stop(); aliceRuntime = runtime(aliceDevice, aliceRoot, false);
  await until(() => aliceRuntime.status().connected);
  await assert.rejects(browse(alice, aliceDevice), { code: 'prepared_connector_unsupported' });
});
