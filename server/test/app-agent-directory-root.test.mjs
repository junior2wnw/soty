import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createHash, randomBytes } from 'node:crypto';
import WebSocket from 'ws';
import { nativeHttpFixture, nativeIdentity, good } from './support/native-capability-http.mjs';
import { createHttpApp } from '../http-app.js';
import { createDescriptor } from '../../modules/app-contract/sdk.mjs';
import { createTrustedReadonlyQueryAdapter } from '../../modules/capabilities/server/readonly-queries.mjs';
import { AccessError } from '../../modules/capabilities/server/validation.mjs';
import { captureExternalApplications } from '../external-applications.js';

test('actual Root composes discovery with native signed Connect and exposes its live module catalog', { timeout: 20000 }, async t => {
  const f = await nativeHttpFixture(t, { appOrigins: true }), actor = nativeIdentity('Directory Root smoke');
  const account = await f.bootstrap(actor); assert.ok(account.accountId);
  const modules = await f.http('/api/app-agent-directory/modules'); assert.equal(modules.status, 200);
  assert.ok(modules.body.modules.some(value => value.id === 'soty.app-agent-directory'));
});

const ref = id => ({ id, version: 1, digest: 'a'.repeat(64) });
const object = properties => ({ type: 'object', properties, required: Object.keys(properties), additionalProperties: false });
const until = async read => { const end = Date.now() + 5000; while (!read()) { assert.ok(Date.now() < end, 'Root directory fixture timeout'); await new Promise(done => setTimeout(done, 5)); } };
async function sourceFixture(t, count) {
  let calls = 0, current = true, hold = false, release, reached;
  const token = randomBytes(32).toString('base64url'), server = createServer((req, res) => {
    if (req.url !== '/read' || req.method !== 'GET' || req.headers.authorization !== `Bearer ${token}`) { res.writeHead(403).end(); return; }
    calls++; const reply = () => { if (res.destroyed || res.writableEnded) return; res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({ count })); };
    if (hold) { release = reply; reached?.(); } else reply();
  });
  t.after(async () => { release?.(); server.closeAllConnections(); await new Promise(done => server.close(done)); });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  const origin = `http://127.0.0.1:${server.address().port}`;
  return { port: server.address().port, calls: () => calls, revoke() { current = false; }, restore() { current = true; },
    hold() { hold = true; return new Promise(done => { reached = done; }); }, release() { hold = false; release?.(); },
    adapter: createTrustedReadonlyQueryAdapter({
      withAuthority(_request, callback) { if (!current) throw new AccessError('external_resource_denied'); return callback(); },
      async query(_request, signal, checkCurrent) {
        checkCurrent(); const response = await fetch(origin + '/read', { signal, headers: { authorization: `Bearer ${token}` } });
        assert.equal(response.status, 200); const body = await response.json(); checkCurrent(); return body;
      },
    }) };
}
async function fixture(t) {
  const sources = [await sourceFixture(t, 11), await sourceFixture(t, 22)]; let trustedEntries = [], sequence = 0;
  const factory = (dist, options) => createHttpApp(dist, { ...options, discoveryOrigin: options.connectOrigins[0],
    appOriginTemplate: `http://{appId}.legacy.localhost:${new URL(options.connectOrigins[0]).port}`,
    namedAppZone: `http://apps.localhost:${new URL(options.connectOrigins[0]).port}`, externalApplications: trustedEntries });
  const f = await nativeHttpFixture(t, { appOrigins: true, appFactory: factory });
  f.server.on('upgrade', (req, socket, head) => { if (!f.app.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy(); });
  const owner = nativeIdentity('Directory publisher'), reader = nativeIdentity('Directory allowed member'), outsider = nativeIdentity('Unrelated private user'), accounts = new Map();
  for (const identity of [owner, reader, outsider]) accounts.set(identity, (await f.bootstrap(identity)).accountId);
  const host = { linkId: 'directory_root_link_012345678901234567890', hostDeviceId: 'directory_root_host', connectorId: 'directory_root_connector' },
    token = randomBytes(32).toString('base64url'), claimCode = randomBytes(32).toString('base64url');
  good((await f.http('/api/connectors/register', { method: 'POST', token, body: JSON.stringify({ linkId: host.linkId, deviceId: host.hostDeviceId,
    connectorId: host.connectorId, scope: 'Dev', protocol: 2, capabilities: ['apps'] }) })).body);
  const socket = new WebSocket(f.origin.replace('http:', 'ws:') + '/api/apps/channel'), frames = [];
  socket.on('error', () => {}); socket.on('message', raw => frames.push(JSON.parse(raw))); t.after(() => socket.terminate());
  await until(() => socket.readyState === WebSocket.OPEN);
  socket.send(JSON.stringify({ type: 'auth', schema: 'soty.apps-channel.v1', ...host, token, name: 'Isolated directory connector' }));
  await until(() => frames.some(frame => frame.type === 'ready'));
  socket.send(JSON.stringify({ type: 'claim', claimDigest: createHash('sha256').update(claimCode).digest('hex') }));
  await until(() => frames.some(frame => frame.type === 'claim-ready'));
  good(await f.call(owner, 'apps.claim', { hostDeviceId: host.hostDeviceId, connectorId: host.connectorId, claimCode }));
  socket.close(); await until(() => socket.readyState === WebSocket.CLOSED);
  const apps = [];
  for (let index = 0; index < sources.length; index++) {
    const registration = good(await f.call(owner, 'apps.register', { name: `Independent directory app ${index + 1}`, port: sources[index].port,
      entryPath: '/', hostDeviceId: host.hostDeviceId, connectorId: host.connectorId,
      grants: { accountIds: index === 1 ? [accounts.get(reader)] : [], communityIds: [] } }));
    const appId = registration.app.id;
    const universal = registration.universalRegistration ?? good(await f.call(owner, 'apps.universal.get', { expectedAccountId: accounts.get(owner), appId })).registration;
    assert.ok(universal?.descriptor?.feedback, 'actual universal registration supplies real automatic feedback refs');
    apps.push({ ...registration.app, feedback: universal.descriptor.feedback, source: sources[index] });
  }
  const ownerCall = (op, args) => f.call(owner, op, { expectedAccountId: accounts.get(owner), ...args });
  async function publish(appId, listed = true) {
    const domains = good(await f.call(owner, 'apps.domains.get', { appId }));
    const claim = good(await f.call(owner, 'apps.domains.claim', { appId, slug: `directory-app-${++sequence}`, requestId: `directory-claim-${sequence}`, expectedDomainsRevision: domains.revision })).receipt;
    const policy = good(await f.call(owner, 'apps.publication.get', { appId }));
    return good(await f.call(owner, 'apps.publication.update', { appId, requestId: `directory-publication-${++sequence}`,
      expectedPolicyEpoch: policy.policyEpoch, expectedTargetRevision: policy.activeTargetRevision, launchPolicy: 'anyone', listed,
      activeDomainIds: [claim.domainId], exposureAck: { scope: 'whole-port', targetRevision: policy.target.revision, targetDigest: policy.target.digest, profile: policy.target.profile } }));
  }
  const rootCatalog = new Map();
  async function optIn(app, { enabled = true, descriptor } = {}) {
    const previous = good(await ownerCall('apps.agentDescriptor.get', { appId: app.id }));
    if (enabled && !descriptor) {
      const publication = good(await f.call(owner, 'apps.publication.get', { appId: app.id })), namespace = `app.${app.id}`;
      descriptor = createDescriptor({ app: { id: app.id, namespace, title: app.name, visibility: publication.launchPolicy === 'anyone' ? 'public' : 'private',
        source: previous.source, auth: { mode: 'public' } }, feedback: app.feedback,
        capabilities: [{ id: `${namespace}:read`, version: 1, title: 'Read isolated count', inputSchema: object({}),
          outputSchema: object({ count: { type: 'integer', minimum: 0, maximum: 100 } }), effects: ['read'],
          resources: [ref(`${namespace}:workspace`)], recipients: [ref(`${namespace}:workspace`)], binding: rootCatalog.get(app.id)?.executionBinding.binding ?? ref(`${namespace}:unadmitted`) }] });
    }
    return good(await ownerCall('apps.agentDescriptor.put', { appId: app.id, expectedRevision: previous.revision, requestId: `directory-opt-in-${++sequence}`,
      enabled, ...(descriptor ? { descriptor } : {}) }));
  }
  async function admitTransports() {
    trustedEntries = [];
    for (const app of apps) {
      const publication = good(await f.call(owner, 'apps.publication.get', { appId: app.id })), namespace = `app.${app.id}`;
      trustedEntries.push({ appId: app.id, target: { revision: publication.target.revision, digest: publication.target.digest }, adapter: app.source.adapter,
        catalog: { capabilityId: `${namespace}:read`, version: 1, appId: app.id, title: 'Read isolated count', description: 'Synthetic independent HTTP source for Root wiring verification.',
          visibility: 'private', executionEnabled: true, inputSchema: object({}), outputSchema: object({ count: { type: 'integer', minimum: 0, maximum: 100 } }),
          effects: [], resources: [`${namespace}:workspace`], recipients: [`${namespace}:workspace`],
          executionBinding: { kind: 'registered', handler: `${namespace}:read`, version: 1, binding: ref(`${namespace}:trusted-binding`) } } });
    }
    for (const entry of captureExternalApplications(trustedEntries)) rootCatalog.set(entry.appId, entry.catalog);
    await f.restart({ enabled: true });
    for (const app of apps) await optIn(app);
  }
  async function grant(identity, app, audience = f.origin + '/mcp') {
    const expectedAccountId = accounts.get(identity), capability = rootCatalog.get(app.id);
    const principal = good(await f.call(identity, 'access.principals.create', { expectedAccountId, label: 'Scoped directory agent' })).principal;
    const issued = good(await f.call(identity, 'access.grants.issue', { expectedAccountId, principalId: principal.id,
      capabilities: [{ capabilityId: capability.capabilityId, version: 1 }], resources: capability.resources, effects: [], recipients: capability.recipients,
      expiresAt: Date.now() + 600000, budget: { unit: 'invocations', limit: 20 } })).grant;
    return { ...await f.issue(identity, expectedAccountId, { principalId: principal.id, grantId: issued.id, audience }), identity, expectedAccountId };
  }
  function mcp(identity, name, args = {}, method = 'tools/call') {
    return f.http('/mcp', { method: 'POST', token: identity.token,
      headers: { accept: 'application/json, text/event-stream', 'mcp-protocol-version': '2026-07-28', 'mcp-method': method, ...(method === 'tools/call' ? { 'mcp-name': name } : {}) },
      body: JSON.stringify({ jsonrpc: '2.0', id: ++sequence, method, params: { ...(method === 'tools/call' ? { name, arguments: args } : {}), _meta: {
        'io.modelcontextprotocol/protocolVersion': '2026-07-28', 'io.modelcontextprotocol/clientInfo': { name: 'directory-root-fixture', version: '1' },
        'io.modelcontextprotocol/clientCapabilities': {},
      } } }) });
  }
  const query = (identity, app, idempotencyKey = `directory-read-${++sequence}`) => mcp(identity, 'app_directory_query', {
    appId: app.id, capabilityId: `app.${app.id}:read`, input: {}, idempotencyKey });
  return { ...f, get app() { return f.app; }, owner, reader, outsider, accounts, apps, sources, ownerCall, publish, optIn, admitTransports, grant, mcp, query,
    async revokeGrant(identity) { return good(await f.call(identity.identity, 'access.grants.revoke', { expectedAccountId: identity.expectedAccountId, grantId: identity.grantId })); } };
}
function success(response) {
  assert.equal(response.status, 200, JSON.stringify(response.body)); const result = response.body.result;
  assert.ok(result && !result.isError, JSON.stringify(response.body)); return result.structuredContent ?? JSON.parse(result.content[0].text);
}
function denied(response) {
  assert.ok(response.status !== 200 || response.body.error || response.body.result?.isError, JSON.stringify(response.body));
}

test('actual signed Root register/opt-in/publish two independent apps updates public discovery without reload and preserves private ACL', { timeout: 25000 }, async t => {
  const f = await fixture(t), currentRoot = f.app, [first, second] = f.apps;
  assert.deepEqual((await f.http('/api/app-agent-directory/apps')).body.items, []);
  await f.publish(first.id); await f.optIn(first); await f.optIn(second);
  assert.equal(f.app, currentRoot, 'no rebuild or Root restart was used for registration/publication/opt-in');
  const catalog = await f.http('/api/app-agent-directory/apps'); assert.equal(catalog.status, 200);
  assert.deepEqual(catalog.body.items.map(item => item.appId), [first.id]); assert.equal(catalog.body.items[0].executionReady, false);
  assert.notEqual(first.id, second.id); assert.ok(catalog.body.items[0].capabilities[0].id.includes(first.id));
  const root = await fetch(f.origin + '/'); assert.match(root.headers.get('link'), /soty-app-platform/u);
  const discovery = await f.http('/.well-known/soty-app-platform'); assert.equal(discovery.body.sourceSdk, '/sdk/source-app/index.json');
  assert.equal((await fetch(f.origin + discovery.body.descriptorSdk.entry)).status, 200);
  assert.equal((await f.http(`/api/app-agent-directory/apps/${second.id}`)).status, 403);
  assert.equal((await f.call(f.outsider, 'apps.agentDescriptor.get', { expectedAccountId: f.accounts.get(f.outsider), appId: second.id })).ok, false);
  const unauthorized = await f.issue(f.outsider, f.accounts.get(f.outsider), { audience: f.origin + '/mcp' });
  const list = success(await f.mcp(unauthorized, 'app_directory_search'));
  assert.deepEqual(list.items.map(item => item.appId), [first.id], 'a generic current agent sees public metadata without gaining private access');
  denied(await f.mcp(unauthorized, 'app_directory_get', { appId: second.id }));
  await f.publish(first.id, false);
  assert.equal((await f.http('/api/app-agent-directory/apps')).body.items.length, 0, 'an unlisted direct link is not a public directory admission');
  assert.equal(success(await f.mcp(unauthorized, 'app_directory_search')).items.length, 0);
  await f.publish(first.id);
  const before = good(await f.ownerCall('apps.agentDescriptor.get', { appId: second.id }));
  assert.equal((await f.call(f.reader, 'apps.agentDescriptor.put', { expectedAccountId: f.accounts.get(f.reader), appId: second.id,
    expectedRevision: before.revision, requestId: 'member-cannot-opt-out', enabled: false })).ok, false);
  assert.equal(good(await f.ownerCall('apps.agentDescriptor.get', { appId: second.id })).revision, before.revision);
  await f.optIn(first, { enabled: false }); assert.equal((await f.http('/api/app-agent-directory/apps')).body.items.length, 0);
  await f.optIn(first); await f.restart({ enabled: true });
  assert.deepEqual((await f.http('/api/app-agent-directory/apps')).body.items.map(item => item.appId), [first.id]);
});

test('actual Root MCP uses an exact grant and Source binding; query/replay/restart and Source/grant/Apps ACL revoke are enforced', { timeout: 30000 }, async t => {
  const f = await fixture(t), [first, second] = f.apps;
  await f.publish(first.id); await f.optIn(first); await f.optIn(second); await f.admitTransports();
  const identity = await f.grant(f.reader, second);
  const tools = await f.mcp(identity, '', {}, 'tools/list');
  assert.equal(tools.status, 200); assert.equal(tools.body.result.tools.filter(tool => tool.name.startsWith('app_directory_')).length, 4);
  const serviceActor = f.app.locals.capabilitiesService.authenticateCredential({ token: identity.token, audience: f.origin + '/mcp' });
  assert.equal(f.app.locals.appAgentDirectory.get({ actor: serviceActor, appId: second.id }).appId, second.id,
    'the same genuine credential first proves the direct host seam; the following call also has to cross actual MCP');
  const selected = success(await f.mcp(identity, 'app_directory_get', { appId: second.id }));
  assert.equal(selected.executionReady, true); assert.equal(selected.canManage, false);
  const httpIdentity = await f.grant(f.reader, second, f.origin);
  const privateHttp = await f.http(`/api/app-agent-directory/apps/${second.id}`, { token: httpIdentity.token });
  assert.equal(privateHttp.status, 200, JSON.stringify(privateHttp.body)); assert.equal(privateHttp.body.executionReady, true);
  assert.notEqual((await f.http(`/api/app-agent-directory/apps/${second.id}`, { token: identity.token })).status, 200, 'MCP audience token cannot be replayed at the HTTP audience');
  assert.equal(success(await f.mcp(identity, 'app_directory_get', { appId: first.id })).executionReady, false, 'unrelated public app is metadata only');
  denied(await f.query(identity, first));
  const query = success(await f.query(identity, second, 'actual-root-once')); assert.deepEqual(query.result, { count: 22 }); assert.equal(second.source.calls(), 1);
  const replay = success(await f.query(identity, second, 'actual-root-once')); assert.equal(replay.reused, true); assert.equal(second.source.calls(), 1);
  await f.restart({ enabled: true });
  assert.equal(success(await f.query(identity, second, 'actual-root-once')).invocation.invocationId, query.invocation.invocationId);
  assert.equal(second.source.calls(), 1);
  second.source.revoke(); denied(await f.query(identity, second, 'source-revoked')); assert.equal(second.source.calls(), 1);
  const other = await f.grant(f.owner, first), reached = first.source.hold(), pending = f.query(other, first, 'held-revoke');
  await reached; await f.revokeGrant(other); first.source.release(); denied(await pending); assert.equal(first.source.calls(), 1);
  denied(await f.mcp(other, 'app_directory_get', { appId: first.id }));
  second.source.restore();
  assert.equal(success(await f.mcp(identity, 'app_directory_get', { appId: second.id })).executionReady, true,
    'Source and Root grant are valid again before isolating the Apps ACL removal');
  good(await f.call(f.owner, 'apps.update', { appId: second.id, grants: { accountIds: [], communityIds: [] } }));
  denied(await f.mcp(identity, 'app_directory_get', { appId: second.id }));
  assert.notEqual((await f.http(`/api/app-agent-directory/apps/${second.id}`, { token: httpIdentity.token })).status, 200);
});
