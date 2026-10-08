import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { randomBytes } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname, resolve, basename } from 'node:path';
import { pathToFileURL } from 'node:url';
import { createHttpApp } from '../http-app.js';
import { createTavyshNativeDeviceBroker } from '../tavysh-native-device-broker.js';
import { verifyTavyshNativeBrokerDevice } from '../tavysh-device-read-adapter.js';
import { createClientWithStorage } from '../../modules/connect/browser/client.mjs';

const nativeRoot = process.env.SOTY_TAVYSH_DEVICE_SOURCE_ROOT;
assert.ok(nativeRoot, 'An explicitly approved own Native source is an integration prerequisite.');
const { createNativeDeviceReadFixture } = await import(pathToFileURL(join(nativeRoot, 'tests/source-fixtures/native-device-read.mjs')));
const { mountInstalledTavyshSourcePage } = await import(pathToFileURL(join(nativeRoot, 'src/source-host/installed-page.js')));
const NATIVE_ORIGIN = 'https://native.fixture.test';
async function until(check) {
  const deadline = Date.now() + 10000;
  do { if (await check()) return; await new Promise(done => setTimeout(done, 10)); } while (Date.now() < deadline);
  throw Error('synthetic_native_mount_timeout');
}
function domFixture() {
  const nodes = [];
  class Node {
    constructor(tag) { this.tagName = tag; this.children = []; this.listeners = new Map(); this.ownerDocument = doc; nodes.push(this); }
    append(...children) { this.children.push(...children); }
    replaceChildren(...children) { this.children = children; }
    setAttribute() {}
    addEventListener(name, callback) { this.listeners.set(name, callback); }
    remove() { this.removed = true; }
    showModal() { this.open = true; }
  }
  const doc = { createElement: tag => new Node(tag) }; doc.body = new Node('body');
  return { doc, button: text => nodes.find(row => row.tagName === 'button' && row.textContent === text && !row.disabled && !row.removed) };
}
async function mounted(t) {
  const folder = await mkdtemp(join(tmpdir(), 'native-source-mounted-')), dist = join(folder, 'dist');
  await mkdir(dist); await writeFile(join(dist, 'index.html'), '<title>Synthetic mounted host</title>');
  const native = await createNativeDeviceReadFixture(), profileBefore = native.records.get('krug.profile.v2');
  const broker = createTavyshNativeDeviceBroker({ nativeOrigin: NATIVE_ORIGIN, deviceId: native.identity.id, sourceRelease: native.sourceRelease });
  const sockets = new Set(), clients = []; let app, page;
  const server = createServer((req, res) => app(req, res));
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  const origin = 'http://127.0.0.1:' + server.address().port;
  t.after(async () => {
    page?.close(); native.port.close(); clients.forEach(client => client.dispose()); sockets.forEach(socket => socket.destroy());
    await app?.locals.closeServices(); await new Promise(done => server.close(done));
    assert.equal(dirname(resolve(folder)), resolve(tmpdir())); assert.match(basename(folder), /^native-source-mounted-/u);
    await rm(folder, { recursive: true, force: true, maxRetries: 5, retryDelay: 20 });
  });
  app = createHttpApp(dist, { dataDir: join(folder, 'data'), connectOrigins: [origin], appHosting: { domainProfile: 'separate-site' },
    namedAppZone: '', discoveryOrigin: '', appOriginTemplate: '', universalAppsEnabled: true,
    nativeSourceHandlers: [{ handle: broker.handle, close: broker.close }] });
  let stored;
  const client = createClientWithStorage({ projectId: 'soty', endpoint: origin + '/api/connect/rpc',
    fetch: (url, init) => fetch(url, { ...init, headers: { ...init.headers, origin } }) }, {
    async read() { return structuredClone(stored ?? null); }, async claim(next) { stored ??= structuredClone(next); return structuredClone(stored); },
    async compareAndSwap(revision, next) { assert.equal(stored.localRevision, revision); stored = structuredClone(next); return structuredClone(stored); },
  }); clients.push(client);
  const account = await client.bootstrap('Synthetic mounted Root caller');
  const principal = (await client.extension('access.principals.create', { expectedAccountId: account.accountId, label: 'Synthetic Native-approved reader' })).principal;
  const dom = domFixture(), listeners = new Map();
  page = await mountInstalledTavyshSourcePage({ pageWindow: { location: { origin: NATIVE_ORIGIN, protocol: 'https:' },
    addEventListener: (name, callback) => listeners.set(name, callback), removeEventListener: name => listeners.delete(name) }, pageDocument: dom.doc,
    profileStorage: native.storage, fetchImpl: (path, init) => fetch(origin + path, { ...init, headers: { ...init.headers, origin: NATIVE_ORIGIN } }) });
  assert.equal(page.mounted, true);
  const scope = native.scope({ rootPrincipal: { accountId: account.accountId, clientId: principal.clientId, principalId: principal.id } });
  const pending = broker.requestConsent(scope); await until(() => dom.button('Разрешить чтение'));
  const button = dom.button('Разрешить чтение'); await button.listeners.get('click')({ isTrusted: false });
  assert.equal(button.disabled, undefined);
  // Only an OWN Source DOM fixture supplies isTrusted=true. No physical browser,
  // common SSO, production TLS or Native consent ceremony is qualified by this.
  await button.listeners.get('click')({ isTrusted: true }); const consent = await pending;
  assert.equal(native.records.get('krug.profile.v2'), profileBefore);
  const challenge = () => ({ grantId: consent.payload.grantId, scopeDigest: consent.payload.scopeDigest,
    nonce: randomBytes(32).toString('base64url'), operation: 'project.status' });
  return { native, broker, origin, consent, challenge, client, account, profileBefore };
}

test('actual Root HTTP mount carries Native P256 owner consent and signed bounded status; Root Connect still works', async t => {
  const f = await mounted(t), packet = await f.broker.readSigned(f.challenge());
  const verified = await verifyTavyshNativeBrokerDevice(packet, f.native.identity.id);
  assert.equal(verified.payload.status.status, 'ready');
  assert.equal(verified.payload.grantId, f.consent.payload.grantId);
  assert.equal(f.consent.payload.scope.rootPrincipal.accountId, f.account.accountId);
  assert.equal(JSON.stringify(packet).includes('SYNTHETIC PRIVATE'), false);
  assert.equal(f.native.records.get('krug.profile.v2'), f.profileBefore);
  assert.ok((await f.client.extension('world.profile.get')).profile);
  for (const path of ['consent', 'approve', 'read', 'enqueue']) {
    const response = await fetch(f.origin + '/api/source/device/v1/' + path, { method: 'POST', headers: { origin: NATIVE_ORIGIN, 'content-type': 'application/json' }, body: '{}' });
    assert.equal(response.status, 404); assert.equal(Object.hasOwn(await response.json(), 'result'), false);
  }
  const denied = await fetch(f.origin + '/api/source/device/v1/challenge', { method: 'POST', headers: { origin: 'https://wrong.fixture.test', 'content-type': 'application/json' }, body: '{}' });
  assert.equal(denied.status, 403);
});

test('actual Native profile change remains403 through the optional Root mount and returns no status data', async t => {
  const f = await mounted(t); assert.equal((await f.broker.readSigned(f.challenge())).payload.status.status, 'ready');
  f.native.persistence.write(f.native.profile, { scope: 'synthetic-other-account', replace: true });
  await assert.rejects(f.broker.readSigned(f.challenge()), error => error.status === 403 && error.code === 'tavysh_native_cloud_scope_changed');
  assert.notEqual(f.native.records.get('krug.profile.v2'), f.profileBefore);
  assert.equal((await fetch(f.origin + '/health')).status, 200);
});
