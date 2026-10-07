import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { randomBytes, createHash } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve, dirname, basename } from 'node:path';
import { createHttpApp } from '../../../../server/http-app.js';
import { createClientWithStorage } from '../../../connect/browser/client.mjs';
import { createLocalAppsRuntime } from '../../../../scripts/agent-modules/local-apps.mjs';
import { createPeremetrikaReadonlyQueryAdapter } from '../../server/peremetrika-readonly-adapter.mjs';
import { peremetrikaReadCatalog } from '../../server/peremetrika-readonly-contract.mjs';

const PROFILE = Object.freeze({ id: 'peremetrika.selected-read', version: 1,
  digest: '63ed34c548c5cbd64fd35ed8e529a333a9bb82df9c908ac4e5152654a7a98fd1' });
const until = async check => {
  const deadline = Date.now() + 10000;
  do { if (await check()) return; await new Promise(done => setTimeout(done, 10)); } while (Date.now() < deadline);
  throw new Error('root_fixture_not_ready');
};
function memory() {
  let value;
  return { async read() { return structuredClone(value ?? null); },
    async claim(candidate) { value ??= structuredClone(candidate); return structuredClone(value); },
    async compareAndSwap(revision, candidate) { assert.equal(value.localRevision, revision); value = structuredClone(candidate); return structuredClone(value); } };
}
export async function createPeremetrikaSignedRoot(t, source) {
  const folder = await mkdtemp(join(tmpdir(), 'soty-peremetrika-read-')), dataDir = join(folder, 'data'), dist = join(folder, 'dist');
  await mkdir(dist); await writeFile(join(dist, 'index.html'), '<!doctype html><title>Synthetic Root</title>');
  let app, runtime; const sockets = new Set(), clients = [];
  const server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  const origin = 'http://127.0.0.1:' + server.address().port;
  const create = options => createHttpApp(dist, { dataDir, connectOrigins: [origin],
    appOriginTemplate: 'http://{appId}.localhost:' + server.address().port, capabilityAudience: origin, ...options });
  app = create(); server.on('upgrade', (req, socket, head) => { if (!app.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy(); });
  t.after(async () => {
    runtime?.stop(); clients.forEach(client => client.dispose()); await app?.locals.closeServices(); sockets.forEach(socket => socket.destroy());
    await new Promise(done => server.close(done)); assert.equal(dirname(resolve(folder)), resolve(tmpdir()));
    assert.match(basename(folder), /^soty-peremetrika-read-/u); await rm(folder, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  });
  const secret = randomBytes(32).toString('base64url');
  const identity = { linkId: 'peremetrika_link_fixture_123456789012345', hostDeviceId: 'peremetrika_host_fixture',
    connectorId: 'peremetrika_connector_fixture', name: 'Synthetic fixture host' };
  const registered = await fetch(origin + '/api/connectors/register', { method: 'POST', headers: { 'content-type': 'application/json', authorization: 'Bearer ' + secret },
    body: JSON.stringify({ linkId: identity.linkId, deviceId: identity.hostDeviceId, connectorId: identity.connectorId,
      scope: 'Dev', protocol: 2, capabilities: ['apps'] }) });
  assert.equal(registered.status, 200);
  runtime = createLocalAppsRuntime({ randomSecret: () => randomBytes(32).toString('base64url'), digest: value => createHash('sha256').update(value).digest('hex'),
    createWebSocket: url => new WebSocket(url), httpRequest: request, encodeBase64: bytes => Buffer.from(bytes).toString('base64'),
    decodeBase64: value => Buffer.from(value, 'base64') }, { identity, token: secret, serverUrl: origin });
  runtime.start(); await until(() => runtime.status().connected);
  const client = createClientWithStorage({ projectId: 'soty', endpoint: origin + '/api/connect/rpc',
    fetch: (url, options) => fetch(url, { ...options, headers: { ...options.headers, origin } }) }, memory()); clients.push(client);
  const account = await client.bootstrap('Synthetic Peremetrika reader'), claim = await runtime.claim();
  await client.extension('apps.claim', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, claimCode: claim.claimCode });
  const registeredApp = await client.extension('apps.register', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId,
    name: 'Переметрика — выбранный документ', port: Number(new URL(source.origin).port), entryPath: '/',
    grants: { accountIds: [], communityIds: [] } });
  const appId = registeredApp.app.id, target = registeredApp.universalRegistration.descriptor.app.source;
  const ready = () => until(async () => (await client.extension('apps.list')).apps.find(item => item.id === appId)?.state === 'ready');
  await ready();
  return { origin, account, client, appId, async connect(selection, native) {
    const resourceId = 'app.' + appId + ':selected-' + selection.kind, capabilityId = 'app.' + appId + ':' + (selection.kind === 'page' ? 'page.read' : 'library.version.read');
    const resource = { registryId: 'soty', tenantId: account.accountId, appId, environmentId: 'production', resourceId,
      sourceActorId: native.sourceActorId, sourceMode: native.sourceMode };
    const bridge = createPeremetrikaReadonlyQueryAdapter({ origin: source.origin, resource, selection, expectedProfile: PROFILE,
      sourceRelease: { id: 'peremetrika.native-selected-read', version: 1, digest: 'b'.repeat(64) },
      bindingId: 'app.' + appId + ':native-selected-read', withAuthority: (_request, _scope, action) => action(),
      resolveCredential: async () => ({ sotyAccountId: account.accountId, sourceActorId: native.sourceActorId, sourceMode: native.sourceMode,
        expiresAt: native.expiresAt, token: native.token }), assertDestination: async destination => destination === source.origin, allowLoopback: true });
    const catalog = peremetrikaReadCatalog({ capabilityId, appId, resourceId, binding: bridge.binding, kind: selection.kind });
    const options = { externalApplications: [{ appId, target: { revision: target.revision, digest: target.digest }, catalog, adapter: bridge.adapter }] };
    await app.locals.closeServices(); app = create(options); await ready();
    const reference = app.locals.capabilitiesService.external.contracts[0];
    const principal = (await client.extension('access.principals.create', { expectedAccountId: account.accountId, label: 'Synthetic selected reader' })).principal;
    const grant = (await client.extension('access.grants.issue', { expectedAccountId: account.accountId, principalId: principal.id,
      capabilities: [{ capabilityId, version: 1 }], resources: [resourceId], effects: [], recipients: [resourceId],
      expiresAt: Date.now() + 600000, budget: { unit: 'invocations', limit: 20 }, allowDelegation: false, maxDepth: 0 })).grant;
    const credential = (await client.extension('access.credentials.issue', { expectedAccountId: account.accountId, grantId: grant.id, audience: origin })).token;
    return { reference, async query(idempotencyKey, input = {}) {
      const response = await fetch(origin + '/api/capabilities/v1/app-actions/query', { method: 'POST',
        headers: { 'content-type': 'application/json', origin, authorization: 'Bearer ' + credential },
        body: JSON.stringify({ reference, idempotencyKey, input }) });
      return { status: response.status, body: await response.json() };
    }, async revokeRoot() { return client.extension('access.grants.revoke', { expectedAccountId: account.accountId, grantId: grant.id }); },
    async restartRoot() { await app.locals.closeServices(); app = create(options); await ready(); },
    async revokeApp() { return client.extension('apps.update', { appId, grants: { accountIds: [], communityIds: [] }, enabled: false }); },
    };
  } };
}
