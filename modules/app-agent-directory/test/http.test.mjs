import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import { createServer } from 'node:http';
import { mkdtempSync, mkdirSync, writeFileSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, basename, join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { randomBytes } from 'node:crypto';
import { attachAppAgentDirectory } from '../../../server/app-agent-directory.js';
import { createAppModuleCatalog } from '../../../server/app-agent-directory-modules.js';
import { createAppAgentDirectoryMcpTools } from '../../../server/app-agent-directory-mcp.js';
import { validateDescriptor } from '../../app-contract/index.mjs';

const ref = id => ({ id, version: 1, digest: 'a'.repeat(64) });
async function fixture(t) {
  let items = [], queries = 0, modules = [];
  const actor = { accountId: 'scoped-owner', deviceId: 'scoped-device' }, privateItem = { appId: 'private-app', title: 'Private', capabilities: [] };
  const directory = { list(args = {}) { return { schema: 'soty.app-agent-directory.v1', items: args.actor ? [...items, privateItem] : items, nextCursor: null }; },
    get({ appId, actor: current }) { if (appId === privateItem.appId && current === actor) return privateItem;
      const item = items.find(value => value.appId === appId); if (!item) throw Object.assign(new Error('directory_not_found'), { code: 'directory_not_found', status: 404 }); return item; },
    async query() { queries++; return { count: 7 }; }, recheckQuery() {} };
  const moduleCatalog = createAppModuleCatalog({ entries: () => modules }), app = express();
  attachAppAgentDirectory(app, { directory, moduleCatalog, origin: 'https://root.example', authenticate(req) {
    if (req.headers.authorization !== 'Bearer test-scoped-fixture') throw Object.assign(new Error('directory_authentication_required'), { code: 'directory_authentication_required', status: 401 });
    return actor;
  } });
  // Same namespace ownership as existing capabilities discovery: ordering matters.
  app.use((req, res, next) => { if (req.path.startsWith('/agents')) res.status(404).end(); else next(); });
  app.get('/', (_req, res) => res.type('html').send('<!doctype html><title>Root</title>'));
  const server = createServer(app), sockets = new Set(); server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  t.after(async () => { sockets.forEach(socket => socket.destroy()); await new Promise(done => server.close(done)); });
  const origin = `http://127.0.0.1:${server.address().port}`;
  return { origin, directory, moduleCatalog, actor, get: (path, options) => fetch(origin + path, options),
    items: values => { items = values; }, modules: values => { modules = values; }, queries: () => queries };
}

test('clean external agent follows root link to SDK/modules then a current public capability; HTTP never mutates', async t => {
  const f = await fixture(t), root = await f.get('/');
  assert.match(root.headers.get('link'), /<\/.well-known\/soty-app-platform>; rel="service-desc"/u);
  const discovery = await (await f.get('/.well-known/soty-app-platform')).json();
  assert.equal(discovery.rootMcp, '/mcp'); assert.equal(discovery.sourceSdk, '/sdk/source-app/index.json');
  assert.equal((await (await f.get(discovery.apps)).json()).items.length, 0);
  f.items([{ appId: 'dynamic-public-app', title: '<Visible & current>', discoveryReady: true, executionReady: false,
    capabilities: [{ id: 'public:read', title: '<Read>', executionReady: false }] }]);
  const catalog = await (await f.get(discovery.apps)).json(); assert.equal(catalog.items[0].appId, 'dynamic-public-app');
  const app = await (await f.get(discovery.apps + '/dynamic-public-app')).json(); assert.equal(app.capabilities[0].id, 'public:read');
  const html = await (await f.get(discovery.ui)).text(); assert.match(html, /&lt;Visible &amp; current&gt;/u); assert.doesNotMatch(html, /<Read>/u);
  const head = await f.get(discovery.apps, { method: 'HEAD' }); assert.equal(head.status, 200); assert.equal(await head.text(), '');
  assert.equal(head.headers.get('cache-control'), 'no-store');
  assert.equal((await f.get(discovery.apps, { method: 'POST' })).status, 405); assert.equal(f.queries(), 0);
  assert.equal((await f.get('/api/app-agent-directory/apps?limit=2&limit=3')).status, 400);
  assert.equal((await f.get('/api/app-agent-directory/apps/dynamic-public-app?query=bad')).status, 400);
  assert.equal((await f.get('/api/app-agent-directory/modules')).status, 200);
});

test('a context-free author discovers mandatory window login before any app or role is registered', async t => {
  const f = await fixture(t), discovery = await (await f.get('/.well-known/soty-app-platform')).json();
  assert.equal(discovery.authentication?.default, 'soty-window');
  assert.equal(discovery.authentication?.documentation, '/agents/shared-auth.md');
  assert.equal(discovery.authentication?.capabilities, '/api/apps/capabilities');
  assert.equal(discovery.authentication?.sourceSdk, discovery.sourceSdk);
  const guide = await f.get(discovery.authentication.documentation);
  assert.equal(guide.status, 200); assert.match(guide.headers.get('content-type'), /^text\/markdown/u);
  assert.equal(guide.headers.get('cache-control'), 'no-store');
  const instructions = await guide.text();
  for (const name of ['createRelayIdentityFetch', 'createSqliteRelayIdentity', 'issuer', 'subject']) assert.ok(instructions.includes(name), name);
  assert.match(instructions, /Не создавайте отдельные пароли/u);
  assert.match(instructions, /не переносите вход в отдельную вкладку/u);
  assert.match(instructions, /Остановите подключение/u);
  assert.equal((await f.get(discovery.authentication.documentation, { method: 'HEAD' })).status, 200);
  assert.equal((await f.get(discovery.authentication.documentation, { method: 'POST' })).status, 405);
  assert.equal((await f.get(discovery.authentication.documentation + '?accountId=foreign')).status, 400);
  assert.equal(f.queries(), 0, 'public login guidance grants no app execution or private data');
});

test('HTTP authenticates private metadata by the explicit current credential; cookies and accountId do not authorize', async t => {
  const f = await fixture(t);
  assert.equal((await f.get('/api/app-agent-directory/apps/private-app')).status, 404);
  assert.equal((await f.get('/api/app-agent-directory/apps/private-app', { headers: { cookie: 'accountId=scoped-owner' } })).status, 404);
  const signed = await f.get('/api/app-agent-directory/apps/private-app', { headers: { authorization: 'Bearer test-scoped-fixture' } });
  assert.equal(signed.status, 200); assert.equal((await signed.json()).title, 'Private');
  assert.equal((await f.get('/api/app-agent-directory/apps/private-app', { headers: { authorization: 'Bearer wrong' } })).status, 401);
  assert.equal((await f.get('/api/app-agent-directory/apps?accountId=scoped-owner')).status, 400);
});

test('clean download executes the existing SDK and two independent manifests without hardcoded app IDs', async t => {
  const f = await fixture(t), parent = realpathSync(tmpdir()), directory = mkdtempSync(join(parent, 'soty-discovery-sdk-'));
  t.after(() => { const actual = realpathSync(directory); assert.equal(dirname(actual), parent); assert.match(basename(actual), /^soty-discovery-sdk-/u); rmSync(actual, { recursive: true, force: true }); });
  const discovery = await (await f.get('/.well-known/soty-app-platform')).json();
  for (const path of [...discovery.descriptorSdk.files, ...discovery.examples]) {
    const response = await f.get(path); assert.equal(response.status, 200, path);
    const local = join(directory, path.slice('/agents/'.length)); mkdirSync(dirname(local), { recursive: true }); writeFileSync(local, await response.text());
  }
  const { notesDescriptor } = await import(pathToFileURL(join(directory, 'examples', 'notes-descriptor.mjs')).href),
    { galleryDescriptor } = await import(pathToFileURL(join(directory, 'examples', 'gallery-descriptor.mjs')).href);
  for (const factory of [notesDescriptor, galleryDescriptor]) {
    const appId = `app-${randomBytes(16).toString('hex')}`, namespace = `app.${appId}`, workspace = ref(`${namespace}:workspace`);
    const value = factory({ app: { id: appId, namespace, title: factory.name, visibility: 'private', auth: { mode: 'public' },
      source: { id: `apps:${appId}/target`, revision: 1, digest: 'b'.repeat(64) } }, workspace, collection: workspace, binding: ref(`${namespace}:binding`),
      feedback: { mode: 'required', provider: ref('soty:feedback'), captureProfile: ref('soty:capture'), retentionProfile: ref('soty:retention'),
        submitAudience: 'members', ticketVisibility: 'reporter-and-support' } });
    assert.equal(validateDescriptor(value).app.id, appId); assert.ok(value.capabilities[0].id.startsWith(namespace + ':'));
    assert.equal('url' in value, false); assert.equal('adapter' in value, false);
  }
});

test('module catalog reflects live installed registry and canvas.widget contract metadata; executable configuration is rejected', async t => {
  const f = await fixture(t), widget = { id: 'soty.widget-provider', version: 1, title: 'Canvas widget provider', description: 'UI/runtime admission is separate.',
    kind: 'provider-contract', contract: 'soty.canvas-widget.v1', documentation: '/agents/app-platform.md', readiness: 'contract-only', operations: [], providerKinds: ['canvas.widget'] };
  f.modules([widget]); const catalog = await (await f.get('/api/app-agent-directory/modules')).json();
  assert.equal(catalog.modules.find(item => item.id === widget.id).readiness, 'contract-only');
  f.modules([]); assert.equal((await (await f.get('/api/app-agent-directory/modules')).json()).modules.some(item => item.id === widget.id), false);
  f.modules([{ ...widget, command: 'run arbitrary' }]); assert.equal((await f.get('/api/app-agent-directory/modules')).status, 503);
});

test('MCP uses the same directory and post-await recheck; a changed ACL/result invalidates an old response', async t => {
  const f = await fixture(t), router = createAppAgentDirectoryMcpTools({ directory: f.directory, moduleCatalog: f.moduleCatalog });
  assert.equal(router.tools.length, 4);
  const args = {}, value = await router.call({ name: 'app_directory_search', args, actor: f.actor });
  router.recheck({ name: 'app_directory_search', args, actor: f.actor, value });
  f.items([{ appId: 'new-app', capabilities: [] }]);
  assert.throws(() => router.recheck({ name: 'app_directory_search', args, actor: f.actor, value }), { code: 'directory_source_changed' });
  await assert.rejects(router.call({ name: 'app_directory_get', args: { appId: 'new-app', accountId: 'forged' }, actor: f.actor }), { code: 'directory_invalid_input' });
});
