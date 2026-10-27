import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { execFileSync, spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { nativeHttpFixture, nativeIdentity, good } from './support/native-capability-http.mjs';
import { pinnedHistory } from './support/pinned-history.mjs';

const repo = resolve(import.meta.dirname, '../..');
const oldCheckout = process.env.SOTY_PREPARED_AGENT_OLD_ROOT;
const baseline = 'ff507100b3645f77e6a7fb84cbc0ad913ab3e34c';
async function previousRoot() {
  assert.ok(oldCheckout, 'set SOTY_PREPARED_AGENT_OLD_ROOT to the pinned old Root checkout');
  const tree = revision => execFileSync('git', ['rev-parse', `${revision}^{tree}`], { cwd: oldCheckout, encoding: 'utf8' }).trim();
  assert.equal(tree('HEAD'), tree(baseline), 'old Root has the exact baseline tree');
  assert.equal(execFileSync('git', ['status', '--porcelain', '--untracked-files=no'], { cwd: oldCheckout, encoding: 'utf8' }).trim(), '');
  return import(pathToFileURL(join(oldCheckout, 'server/http-app.js')));
}
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const gate = () => { let release; const promise = new Promise(resolve => { release = resolve; }); return { promise, release }; };
const waitForGate = (reached, pending) => Promise.race([reached, pending.then(() => { throw Error('query_finished_before_expected_gate'); })]);
function holdSdkJsonEof() {
  const descriptor = Object.getOwnPropertyDescriptor(Response.prototype, 'body'), streams = new WeakMap();
  let selected = false, reached, release;
  const atEnd = new Promise(done => { reached = done; }), barrier = new Promise(done => { release = done; });
  Object.defineProperty(Response.prototype, 'body', { ...descriptor, get() {
    const original = descriptor.get.call(this);
    if (!original || !/^application\/json/u.test(this.headers.get('content-type') ?? '')) return original;
    if (streams.has(this)) return streams.get(this);
    if (selected) return original;
    selected = true; const reader = original.getReader(); let stopped = false;
    const held = new ReadableStream({ async pull(controller) {
      try { const next = await reader.read();
        if (next.done) { reached(); await barrier; if (!stopped) controller.close(); }
        else if (!stopped) controller.enqueue(next.value);
      } catch (error) { if (!stopped) controller.error(error); }
    }, async cancel() { stopped = true; release(); await reader.cancel().catch(() => {}); } });
    streams.set(this, held); return held;
  } });
  return { atEnd, release, restore() { release(); Object.defineProperty(Response.prototype, 'body', descriptor); } };
}
async function until(check, timeout = 15000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) { const result = await check(); if (result) return result; await delay(50); }
  throw Error('prepared_manifest_fixture_timeout');
}
async function listen(server) { await new Promise((done, fail) => { server.once('error', fail); server.listen(0, '127.0.0.1', done); }); }
async function freePort() { const server = createServer(); await listen(server); const port = server.address().port; await new Promise(done => server.close(done)); return port; }
function previousConnector() {
  const bytes = pinnedHistory(baseline, 'public/agent/soty-connector.mjs');
  const manifest = JSON.parse(pinnedHistory(baseline, 'public/agent/manifest.json').toString('utf8'));
  assert.equal(manifest.version, '1.4.13'); assert.equal(createHash('sha256').update(bytes).digest('hex'), manifest.sha256);
  return bytes;
}

async function fixture(t, connectorFile = 'scripts/soty-connector.mjs', { manifestSupported = true, appFactory, oldConnectorBytes } = {}) {
  const parent = resolve(tmpdir());
  let child, appServer, root, outputBytes = 0;
  // Close the upgraded connector before nativeHttpFixture closes its server.
  t.after(async () => {
    if (child && child.exitCode === null && child.signalCode === null) { const done = new Promise(resolve => child.once('exit', resolve)); child.kill(); await done; }
    if (appServer) { appServer.closeAllConnections(); if (appServer.listening) await new Promise(done => appServer.close(done)); }
    if (root) {
      assert.equal(dirname(resolve(root)), parent); assert.match(basename(root), /^soty-agent-manifest-/u);
      await rm(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    }
  });
  const f = await nativeHttpFixture(t, { appOrigins: true, ...(appFactory ? { appFactory } : {}) });
  root = await mkdtemp(join(parent, 'soty-agent-manifest-'));
  const connectorDir = join(root, 'connector'), projects = join(root, 'projects'), project = join(projects, 'selected'), other = join(projects, 'other');
  await mkdir(connectorDir); await mkdir(join(project, '.soty'), { recursive: true }); await mkdir(join(other, '.soty'), { recursive: true });
  if (oldConnectorBytes) { connectorFile = join(connectorDir, 'old-connector.mjs'); await writeFile(connectorFile, oldConnectorBytes); }
  await writeFile(join(connectorDir, 'connector-config.json'), JSON.stringify({ workspaceRoot: projects, allowedRoots: [projects] }));
  let applicationRequests = 0;
  appServer = createServer((_request, response) => { applicationRequests++; response.end('fixture app'); });
  await listen(appServer);
  const manifest = { schema: 'soty.local-app.v1', name: 'Selected fixture', port: appServer.address().port, entryPath: '/' };
  await writeFile(join(project, '.soty', 'app.json'), JSON.stringify(manifest));
  await writeFile(join(other, '.soty', 'app.json'), JSON.stringify({ ...manifest, name: 'Different project' }));
  f.server.on('upgrade', (request, socket, head) => { if (!f.app.locals.appsService.handleUpgrade(request, socket, head)) socket.destroy(); });
  const connectorPort = await freePort(), inherited = Object.fromEntries(Object.entries(process.env).filter(([key]) => ['PATH', 'PATHEXT', 'SYSTEMROOT', 'WINDIR', 'COMSPEC'].includes(key.toUpperCase())));
  const childEnv = { ...inherited,
    TEMP: root, TMP: root, USERPROFILE: root, APPDATA: join(root, 'appdata'), LOCALAPPDATA: join(root, 'localappdata'),
    SOTY_CONNECTOR_DATA_DIR: connectorDir, SOTY_CONNECTOR_PORT: String(connectorPort), SOTY_CONNECTOR_SERVER_URL: f.origin,
    SOTY_CONNECTOR_UPDATE_URL: `${f.origin}/agent/manifest.json`,
    SOTY_CONNECTOR_LINK_ID: 'prepared_agent_real_link_123456789012345', SOTY_CONNECTOR_DEVICE_ID: 'prepared_agent_host',
    SOTY_CONNECTOR_SCOPE: 'Dev', SOTY_CONNECTOR_AUTO_UPDATE: '0', SOTY_CONNECTOR_MANAGED: '0', SOTY_OPENCODE_PATH: join(root, 'no-agent'),
  };
  function launch(file) {
    child = spawn(process.execPath, [resolve(repo, file)], { cwd: connectorDir, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'], env: childEnv });
    for (const stream of [child.stdout, child.stderr]) stream.on('data', value => { outputBytes += value.length; });
  }
  launch(connectorFile);
  const claim = await until(async () => {
    assert.equal(child.exitCode, null, `connector exited (${outputBytes} diagnostic bytes)`);
    try { const response = await fetch(`http://127.0.0.1:${connectorPort}/apps/claim`, { method: 'POST',
      headers: { origin: f.origin, 'content-type': 'application/json' }, body: '{}' }); return response.ok ? await response.json() : null; } catch { return null; }
  });
  const alice = nativeIdentity('Manifest owner'), bob = nativeIdentity('Other owner');
  const a = await f.bootstrap(alice), b = await f.bootstrap(bob);
  good(await f.call(alice, 'apps.claim', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, claimCode: claim.claimCode }));
  const selectedDevice = { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId };
  const roots = good(await f.call(alice, 'apps.prepared.browse', { expectedAccountId: a.accountId, ...selectedDevice }));
  const selected = { ...selectedDevice, location: { rootId: roots.roots[0].id, segments: ['selected'] } };
  let scopeResponse;
  try { scopeResponse = await f.call(alice, 'apps.prepared.agentScope', { expectedAccountId: a.accountId, ...selected }); }
  catch (error) {
    if (manifestSupported || !appFactory || error.code !== 'ERR_ASSERTION' || !error.message.startsWith('unsupported_operation')) throw error;
    scopeResponse = { ok: false, error: { code: 'unsupported_operation' } }; // Old Root rejects even the signed challenge.
  }
  const scope = manifestSupported ? good(scopeResponse) : scopeResponse;
  let sequence = 0;
  async function mcp(token, name, args) {
    return f.http('/mcp', { method: 'POST', token, headers: { accept: 'application/json, text/event-stream',
      'mcp-protocol-version': '2026-07-28', 'mcp-method': 'tools/call', 'mcp-name': name }, body: JSON.stringify({ jsonrpc: '2.0', id: ++sequence,
      method: 'tools/call', params: { name, arguments: args, _meta: { 'io.modelcontextprotocol/protocolVersion': '2026-07-28',
        'io.modelcontextprotocol/clientInfo': { name: 'prepared-manifest-fixture', version: '1' }, 'io.modelcontextprotocol/clientCapabilities': {} } } }) });
  }
  async function grant(owner, accountId, resource) {
    const principal = good(await f.call(owner, 'access.principals.create', { expectedAccountId: accountId, label: 'Read selected manifest' })).principal;
    const granted = good(await f.call(owner, 'access.grants.issue', { expectedAccountId: accountId, principalId: principal.id,
      capabilities: [{ capabilityId: 'apps.prepared.manifest.read', version: 1 }], resources: ['apps.prepared.manifest', resource],
      effects: [], recipients: ['soty:prepared-manifest'], expiresAt: Date.now() + 600000,
      allowDelegation: false, maxDepth: 0, budget: { unit: 'invocations', limit: 30 } })).grant;
    return { ...(await f.issue(owner, accountId, { principalId: principal.id, grantId: granted.id, audience: f.origin + '/mcp' })), accountId };
  }
  function ref() { return f.app.locals.capabilitiesService.external.contracts.find(value => value.capabilityId === 'apps.prepared.manifest.read'); }
  const input = { ...selected, manifestDigest: scope.project?.digest ?? '' };
  const query = (identity, key, value = input) => mcp(identity.token, 'apps_query', { reference: ref(), idempotencyKey: key, input: value });
  return { ...f, get app() { return f.app; }, alice, bob, a, b, selected, input, scope, grant, query, mcp, project, manifest,
    rawRestart: f.restart, reference: ref,
    get applicationRequests() { return applicationRequests; },
    async switchToOldConnector() {
      const done = new Promise(resolve => child.once('exit', resolve)); child.kill(); await done;
      const file = join(connectorDir, 'old-connector.mjs'); await writeFile(file, previousConnector()); launch(file);
      await until(async () => { const result = await f.call(alice, 'apps.prepared.browse', { expectedAccountId: a.accountId, ...selectedDevice }); return result.ok ? result : null; });
    },
    async restart() { await f.restart({ enabled: true });
      await until(async () => { const result = await f.call(alice, 'apps.prepared.agentScope', { expectedAccountId: a.accountId, ...selected }); return result.ok ? result : null; }); },
  };
}
function success(response) {
  assert.equal(response.status, 200); assert.ok(response.body.result && !response.body.result.isError);
  return response.body.result.structuredContent;
}
function denied(response) {
  if (response.status !== 200) { assert.ok([401, 403, 404].includes(response.status)); return; }
  assert.equal(response.body.result?.isError, true);
  const code = JSON.parse(response.body.result.content[0].text).error.code;
  assert.ok(['access_denied', 'authorization_required', 'external_resource_denied', 'external_query_not_admitted', 'not_found'].includes(code), code);
}

test('signed owner grants exactly one manifest, actual MCP reads it without HEAD and survives restart', { timeout: 60000 }, async t => {
  const f = await fixture(t), identity = await f.grant(f.alice, f.a.accountId, f.scope.resource);
  assert.equal(f.scope.project.state, 'present'); assert.equal(f.scope.project.name, f.manifest.name);
  const catalog = success(await f.mcp(identity.token, 'apps_catalog_search', { query: 'описание' }));
  assert.equal(catalog.items.length, 1); assert.equal(catalog.items[0].reference.capabilityId, 'apps.prepared.manifest.read');
  const first = success(await f.query(identity, 'selected-once'));
  assert.equal(first.result.name, f.manifest.name); assert.equal(first.result.digest, f.scope.project.digest);
  assert.equal(first.invocation.status, 'succeeded'); assert.equal(first.reused, false);
  const audit = f.sql(f.capsFile, db => db.prepare('SELECT account_id,principal_id,grant_id,capability_id FROM cap_invocations WHERE id=?').get(first.invocation.invocationId));
  assert.equal(audit.account_id, f.a.accountId); assert.equal(audit.principal_id, identity.principalId);
  assert.equal(audit.grant_id, identity.grantId); assert.equal(audit.capability_id, 'apps.prepared.manifest.read');
  assert.notEqual(audit.principal_id, f.a.deviceId, 'agent provenance remains separate from the human signing device');
  assert.equal(f.applicationRequests, 0, 'scope and query must never contact the application');
  assert.equal((good(await f.call(f.alice, 'apps.list'))).apps.length, 0, 'manifest read is not app registration');
  const forbidden = { ...f.input, location: { ...f.input.location, segments: ['other'] } };
  denied(await f.query(identity, 'other-project', forbidden));
  const wrongDigest = { ...f.input, manifestDigest: 'f'.repeat(64) };
  denied(await f.query(identity, 'different-digest', wrongDigest));
  const foreign = await f.call(f.bob, 'apps.prepared.agentScope', { expectedAccountId: f.b.accountId, ...f.selected });
  assert.equal(foreign.ok, false);
  const stolenScope = await f.grant(f.bob, f.b.accountId, f.scope.resource);
  denied(await f.query(stolenScope, 'foreign-owner'));
  const notesOnly = await f.issue(f.alice, f.a.accountId, { audience: f.origin + '/mcp' });
  denied(await f.query(notesOnly, 'notes-only'));
  assert.equal(f.applicationRequests, 0);
  await f.restart();
  const replay = success(await f.query(identity, 'selected-once'));
  assert.equal(replay.reused, true); assert.equal(replay.resultUnavailable, true);
  const current = success(await f.query(identity, 'after-restart'));
  assert.equal(current.result.digest, first.result.digest); assert.equal(f.applicationRequests, 0);
  await writeFile(join(f.project, '.soty', 'app.json'), JSON.stringify({ ...f.manifest, name: 'Changed selected project' }));
  denied(await f.query(identity, 'changed-manifest'));
  assert.equal(f.applicationRequests, 0);
});

test('actual old connector fails closed before any app request', { timeout: 60000 }, async t => {
  const oldConnectorBytes = previousConnector();
  const f = await fixture(t, undefined, { manifestSupported: false, oldConnectorBytes });
  assert.equal(f.scope.ok, false);
  assert.equal(f.scope.error.code, 'prepared_connector_unsupported');
  assert.equal(f.applicationRequests, 0);
  assert.equal((good(await f.call(f.alice, 'apps.list'))).apps.length, 0);
});

test('an existing grant refuses an actual downgraded connector with a clear unsupported error', { timeout: 60000 }, async t => {
  const f = await fixture(t), identity = await f.grant(f.alice, f.a.accountId, f.scope.resource);
  await f.switchToOldConnector();
  const result = await f.query(identity, 'after-connector-downgrade');
  assert.equal(result.status, 200); assert.equal(result.body.result.isError, true);
  assert.equal(JSON.parse(result.body.result.content[0].text).error.code, 'prepared_connector_unsupported');
  assert.equal(f.applicationRequests, 0);
});

test('actual old Root keeps legacy browsing with the new connector and rejects the new operation',
  { timeout: 60000, skip: oldCheckout ? false : 'requires pinned SOTY_PREPARED_AGENT_OLD_ROOT' }, async t => {
  const oldRoot = await previousRoot();
  const f = await fixture(t, 'scripts/soty-connector.mjs', { manifestSupported: false, appFactory: oldRoot.createHttpApp });
  assert.equal(f.scope.ok, false);
  assert.equal(f.applicationRequests, 0);
  assert.equal((good(await f.call(f.alice, 'apps.list'))).apps.length, 0);
});

test('old Root preserves the new grant and receipt on rollback and the newer Root reads them again',
  { timeout: 60000, skip: oldCheckout ? false : 'requires pinned SOTY_PREPARED_AGENT_OLD_ROOT' }, async t => {
  const f = await fixture(t), identity = await f.grant(f.alice, f.a.accountId, f.scope.resource);
  const reference = f.reference(), first = success(await f.query(identity, 'rollback-read'));
  const before = f.sql(f.capsFile, db => db.prepare('SELECT count(*) AS n FROM cap_invocations').get().n);
  const oldRoot = await previousRoot();
  const newRoot = await import('../http-app.js');
  await f.rawRestart({ enabled: true, appFactory: oldRoot.createHttpApp });
  const grants = good(await f.call(f.alice, 'access.grants.list', { expectedAccountId: f.a.accountId }));
  assert.ok(grants.grants.some(grant => grant.id === identity.grantId));
  denied(await f.mcp(identity.token, 'apps_query', { reference, idempotencyKey: 'rollback-read', input: f.input }));
  assert.equal(f.sql(f.capsFile, db => db.prepare('SELECT count(*) AS n FROM cap_invocations').get().n), before);
  await f.rawRestart({ enabled: true, appFactory: newRoot.createHttpApp });
  await until(async () => { const result = await f.call(f.alice, 'apps.prepared.agentScope', { expectedAccountId: f.a.accountId, ...f.selected }); return result.ok ? result : null; });
  const replay = success(await f.query(identity, 'rollback-read'));
  assert.equal(replay.invocation.invocationId, first.invocation.invocationId); assert.equal(replay.reused, true);
  assert.equal(f.applicationRequests, 0);
});

test('revocation after the SDK read but before wire output withholds the private manifest', { timeout: 60000 }, async t => {
  const f = await fixture(t), identity = await f.grant(f.alice, f.a.accountId, f.scope.resource), hold = holdSdkJsonEof();
  try {
    const pending = f.query(identity, 'revoked-before-output'); await waitForGate(hold.atEnd, pending);
    good(await f.call(f.alice, 'access.grants.revoke', { expectedAccountId: f.a.accountId, grantId: identity.grantId }));
    hold.release(); const result = await pending; denied(result);
    assert.equal(JSON.stringify(result.body).includes(f.manifest.name), false);
    assert.equal(f.applicationRequests, 0);
  } finally { hold.restore(); }
});

test('revoking the original grant after connector read withholds the result', { timeout: 60000 }, async t => {
  const f = await fixture(t), identity = await f.grant(f.alice, f.a.accountId, f.scope.resource);
  const entered = gate(), release = gate(), service = f.app.locals.appsService, original = service.readPreparedManifest;
  service.readPreparedManifest = async (...args) => { const result = await original(...args); entered.release(); await release.promise; return result; };
  try {
    const pending = f.query(identity, 'revoked-mid-read'); await waitForGate(entered.promise, pending);
    good(await f.call(f.alice, 'access.grants.revoke', { expectedAccountId: f.a.accountId, grantId: identity.grantId }));
    release.release(); denied(await pending);
    denied(await f.query(identity, 'after-revoke'));
    assert.equal(f.applicationRequests, 0);
  } finally { release.release(); service.readPreparedManifest = original; }
});

test('revoking the grant creator device while the connector response is delayed denies the read', { timeout: 60000 }, async t => {
  const f = await fixture(t), identity = await f.grant(f.alice, f.a.accountId, f.scope.resource);
  const phone = nativeIdentity('Second owner device');
  const started = good(await f.call(phone, 'enrollment.start', { label: 'Second owner device', encryptionPublicJwk: phone.encryptionPublicJwk }));
  good(await f.call(f.alice, 'enrollment.approve', { requestId: started.requestId, wrappedKey: { schema: 'test.encrypted', ciphertext: 'synthetic' } }));
  good(await f.call(phone, 'enrollment.finish', { requestId: started.requestId, expectedAccountId: f.a.accountId }));
  const entered = gate(), release = gate(), service = f.app.locals.appsService, original = service.readPreparedManifest;
  service.readPreparedManifest = async (...args) => { const result = await original(...args); entered.release(); await release.promise; return result; };
  try {
    const pending = f.query(identity, 'revoked-owner-device'); await waitForGate(entered.promise, pending);
    good(await f.call(phone, 'device.revoke', { deviceId: f.a.deviceId }));
    release.release(); denied(await pending);
    assert.equal(f.applicationRequests, 0);
  } finally { release.release(); service.readPreparedManifest = original; }
});
