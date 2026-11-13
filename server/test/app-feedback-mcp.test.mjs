import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import WebSocket from 'ws';
import { nativeHttpFixture, nativeIdentity, good } from './support/native-capability-http.mjs';
import { APP_FEEDBACK_CATALOG, appFeedbackResource, createAppFeedbackCapabilities } from '../app-feedback-capabilities.js';
import { createCatalog } from '../../modules/capabilities/server/catalog.mjs';
import { createCapabilitiesService, BUILTIN_CAPABILITIES } from '../../modules/capabilities/server/index.mjs';
import { fixture as accessPanelFixture, settle } from '../../src/world/test-support/access-panel-delegation.mjs';

const until = async read => {
  const deadline = Date.now() + 5000;
  do { const value = read(); if (value) return value; await new Promise(done => setTimeout(done, 5)); } while (Date.now() < deadline);
  throw new Error('fixture_timeout');
};
async function fixture(t) {
  const sockets = [];
  t.after(() => sockets.forEach(socket => socket.terminate()));
  const f = await nativeHttpFixture(t, { appOrigins: true });
  f.server.on('upgrade', (req, socket, head) => {
    if (!f.app.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy();
  });
  const administrator = nativeIdentity('First account'), owner = nativeIdentity('Independent placement owner'), reporter = nativeIdentity('Reporter');
  const accounts = new Map();
  for (const person of [administrator, owner, reporter]) accounts.set(person, await f.bootstrap(person));
  async function placement(person, suffix, ports = [41001]) {
    const token = randomBytes(32).toString('base64url');
    const identity = { linkId: 'feedback_link_123456789012345_' + suffix, hostDeviceId: 'feedback_host_' + suffix, connectorId: 'feedback_connector_' + suffix };
    const registered = await f.http('/api/connectors/register', { method: 'POST', token, body: JSON.stringify({
      linkId: identity.linkId, deviceId: identity.hostDeviceId, connectorId: identity.connectorId, scope: 'Dev', protocol: 2, capabilities: ['apps'],
    }) });
    assert.equal(registered.body.ok, true, registered.body.error?.code ?? registered.body.error);
    const socket = new WebSocket(f.origin.replace('http:', 'ws:') + '/api/apps/channel'), frames = [];
    sockets.push(socket); socket.on('error', () => {}); socket.on('message', bytes => frames.push(JSON.parse(bytes)));
    await new Promise((done, reject) => { socket.once('open', done); socket.once('error', reject); });
    socket.send(JSON.stringify({ type: 'auth', schema: 'soty.apps-channel.v1', ...identity, token, name: 'Disposable connector' }));
    await until(() => frames.some(frame => frame.type === 'ready'));
    const claimCode = randomBytes(32).toString('base64url');
    socket.send(JSON.stringify({ type: 'claim', claimDigest: createHash('sha256').update(claimCode).digest('hex') }));
    await until(() => frames.some(frame => frame.type === 'claim-ready'));
    good(await f.call(person, 'apps.claim', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, claimCode }));
    const closed = new Promise(done => socket.once('close', done)); socket.close(); await closed;
    return Promise.all(ports.map(async port => good(await f.call(person, 'apps.register', {
      hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, name: 'Disposable ' + port, port, entryPath: '/',
      grants: { accountIds: [accounts.get(reporter).accountId], communityIds: [] },
    })).app.id));
  }
  const [appId, siblingId] = await placement(owner, 'owner', [41001, 41002]);
  const [foreignId] = await placement(administrator, 'first');
  async function grant(person, target, operations = APP_FEEDBACK_CATALOG.map(entry => entry.capabilityId), resources = [appFeedbackResource(target)]) {
    const accountId = accounts.get(person).accountId;
    const principal = good(await f.call(person, 'access.principals.create', { expectedAccountId: accountId, label: 'Disposable external agent' })).principal;
    const effects = [...new Set(APP_FEEDBACK_CATALOG.filter(entry => operations.includes(entry.capabilityId)).flatMap(entry => entry.effects))];
    const issued = good(await f.call(person, 'access.grants.issue', { expectedAccountId: accountId, principalId: principal.id,
      capabilities: operations.map(capabilityId => ({ capabilityId, version: 1 })), resources: ['apps.feedback', ...resources], effects,
      recipients: ['soty:app-feedback'], expiresAt: Date.now() + 600000, allowDelegation: false, maxDepth: 0,
      budget: { unit: 'invocations', limit: 30 } })).grant;
    const credential = await f.issue(person, accountId, { principalId: principal.id, grantId: issued.id, audience: f.origin + '/mcp' });
    return { ...credential, person, accountId };
  }
  let sequence = 0;
  async function mcp(identity, name, args) {
    return f.http('/mcp', { method: 'POST', token: identity.token, headers: { accept: 'application/json, text/event-stream',
      'mcp-protocol-version': '2026-07-28', 'mcp-method': 'tools/call', 'mcp-name': name }, body: JSON.stringify({
      jsonrpc: '2.0', id: ++sequence, method: 'tools/call', params: { name, arguments: args, _meta: {
        'io.modelcontextprotocol/protocolVersion': '2026-07-28',
        'io.modelcontextprotocol/clientInfo': { name: 'isolated-feedback-fixture', version: '1' },
        'io.modelcontextprotocol/clientCapabilities': {},
      } },
    }) });
  }
  const reference = op => f.app.locals.capabilitiesService.external.contracts.find(value => value.capabilityId === 'apps.feedback.' + op);
  const invoke = (identity, op, input, idempotencyKey = 'feedback-' + ++sequence) => mcp(identity, ['list', 'get', 'attachment'].includes(op) ? 'apps_query' : 'apps_invoke',
    { reference: reference(op), input, idempotencyKey });
  const feedbackFile = join(f.directory, 'feedback', 'feedback.sqlite');
  const counts = () => f.sql(feedbackFile, db => Object.fromEntries(['installations', 'tickets', 'messages', 'receipts'].map(table =>
    [table, db.prepare('SELECT count(*) AS n FROM feedback_' + table).get().n])));
  const context = async (person, id = appId) => good(await f.call(person, 'apps.feedback.context', { appId: id })).context;
  return { ...f, get app() { return f.app; }, owner, administrator, reporter, accounts, appId, siblingId, foreignId,
    grant, mcp, invoke, reference, counts, context, feedbackFile,
    restart: options => f.restart(options),
    assertOwnerImmutable(id, accountId) {
      // Existing placement lineage has no transfer API. Do not disable its
      // foreign keys or immutable-target triggers to manufacture a transition.
      const db = new DatabaseSync(join(f.directory, 'apps', 'registry.sqlite'));
      try {
        const before = db.prepare('SELECT owner_account_id FROM local_apps WHERE id=?').get(id);
        assert.throws(() => db.prepare('UPDATE local_apps SET owner_account_id=? WHERE id=?').run(accountId, id), /FOREIGN KEY/u);
        assert.throws(() => db.prepare('UPDATE app_runtime_targets SET owner_account_id=? WHERE app_id=?').run(accountId, id), /immutable/u);
        assert.deepEqual(db.prepare('SELECT owner_account_id FROM local_apps WHERE id=?').get(id), before);
        assert.equal(db.prepare('PRAGMA foreign_key_check').get(), undefined);
      }
      finally { db.close(); }
    },
  };
}
function successful(response) {
  assert.equal(response.status, 200, JSON.stringify(response.body));
  const value = response.body.result;
  assert.ok(value && !value.isError, JSON.stringify(response.body));
  assert.deepEqual(JSON.parse(value.content[0].text), value.structuredContent);
  return value.structuredContent;
}
function denied(response, codes = ['access_denied', 'apps_owner_required', 'apps_access_denied', 'app_unavailable']) {
  if (response.status !== 200) { assert.ok([401, 403].includes(response.status)); return; }
  assert.equal(response.body.result?.isError, true, JSON.stringify(response.body));
  const code = JSON.parse(response.body.result.content[0].text).error.code;
  assert.ok(codes.includes(code), code);
}

test('dedicated owner attachment capability preserves all seven existing v1 catalog bindings', () => {
  const catalog = createCatalog(APP_FEEDBACK_CATALOG).listAll(), attachment = catalog.find(entry => entry.capabilityId === 'apps.feedback.attachment');
  assert.ok(attachment, 'the owner must have an explicit selected-original read capability'); assert.deepEqual(attachment.effects, []);
  const original = {
    context: '92ff669e5452257fc702970528af8059aa17000cf569e1b503aa6434cc662ba2',
    submit: '82fb16d1ae16acd7701db9ca9c90773ed28caa5d1f79d545df497b05b4ff07cc',
    list: '96f196ba897e9ef23597899b9b7c9f7c9ac9d0345c8ff966f705d86423be2b58',
    get: '26ed2b61a0c3db25cf893a727c4da3e2b9ebb62f7cc3b16095bc56cf9f216c83',
    reply: 'f62f4deae46af8a2a9ec20babc3b00b8c42aa32be38cf13b269e38a8e7038529',
    status: '0408193eb74f58934b139186b2e7c4513c67952a52cd23cadf77f24908eed205',
    accept: 'd20701b60d76427bf6b0360b0018f6191a6f44afb28bc3919c9cb65795297fc2' };
  for (const [op, digest] of Object.entries(original)) {
    const entry = catalog.find(value => value.capabilityId === 'apps.feedback.' + op); assert.equal(entry.version, 1); assert.equal(entry.digest, digest);
    assert.notDeepEqual(attachment.executionBinding.binding, entry.executionBinding.binding);
  }
});

test('actual Access consent issues an exact read grant through signed Root, reads only its app and revokes through existing UI', { timeout: 20000 }, async t => {
  const f = await fixture(t), accountId = f.accounts.get(f.owner).accountId;
  const methods = ['apps.feedback.context', 'access.principals.list', 'access.grants.list', 'access.events.list', 'oauth.connections.list',
    'access.invocations.list', 'access.principals.create', 'access.grants.issue', 'access.credentials.issue', 'access.grants.revoke'];
  const handlers = Object.fromEntries(methods.map(method => [method, async args => good(await f.call(f.owner, method, args))]));
  const ui = accessPanelFixture(t, { accountId, handlers, panelOptions: { appFeedback: { accountId, appId: f.appId, name: 'Selected placement' } },
    availability: async () => (await f.http('/api/capabilities/v1/status')).body });
  await until(() => ui.dialog()?.querySelector('form'));
  const form = ui.dialog().querySelector('form'), name = form.querySelectorAll('input').find(node => node.type === 'text');
  name.value = 'Disposable read agent'; name.dispatch('input'); form.dispatch('submit'); await ui.handle.flush(); await settle();
  const secret = ui.dialog()?.querySelector('.sa-secret'); assert.ok(secret && /^soty_cap_[A-Za-z0-9_-]{43}$/u.test(secret.value), 'actual server confirms the UI grant and key');
  const identity = { token: secret.value }, installationId = (await f.context(f.owner)).installationId;
  assert.equal(successful(await f.invoke(identity, 'list', { appId: f.appId, installationId })).result.tickets.length, 0);
  denied(await f.invoke(identity, 'list', { appId: f.siblingId, installationId: (await f.context(f.owner, f.siblingId)).installationId }));
  denied(await f.invoke(identity, 'context', { appId: f.appId }));
  const issued = ui.calls.find(call => call.method === 'access.grants.issue').args;
  const grantId = ui.calls.find(call => call.method === 'access.credentials.issue').args.grantId;
  ui.button('Готово, закрыть', ui.dialog()).click(); assert.equal(secret.value, '');
  await until(() => ui.target('principal-' + issued.principalId)); ui.target('principal-' + issued.principalId).click();
  await until(() => ui.target('revoke-grant-' + grantId));
  ui.target('revoke-grant-' + grantId).click();
  await until(() => ui.dialog()?.querySelectorAll('button').some(node => node.textContent === 'Отозвать доступ'));
  ui.button('Отозвать доступ', ui.dialog()).click();
  await ui.handle.flush(); await settle(); denied(await f.invoke(identity, 'list', { appId: f.appId, installationId }));
  assert.equal(ui.calls.filter(call => call.method === 'access.grants.issue').length, 1);
});
// Delay only a real SDK Response stream at EOF. Authentication, Source reads,
// persistence and the final transport authority gate remain unmodified.
function holdSdkJsonEof() {
  const descriptor = Object.getOwnPropertyDescriptor(Response.prototype, 'body');
  const streams = new WeakMap(); let selected = false, reached, release;
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

test('actual Root MCP maps the non-admin grant creator to their existing queue, replays one effect and reloads', { timeout: 20000 }, async t => {
  const f = await fixture(t), identity = await f.grant(f.owner, f.appId), before = f.counts();
  const catalog = successful(await f.mcp(identity, 'apps_catalog_search', {}));
  assert.equal(catalog.items.length, APP_FEEDBACK_CATALOG.length);
  const ensured = successful(await f.invoke(identity, 'context', { appId: f.appId }, 'context-once'));
  assert.equal(ensured.invocation.status, 'succeeded');
  assert.equal(ensured.invocation.receipt.artifacts[0].type, 'feedback_context');
  const installationId = ensured.invocation.receipt.artifacts[0].id;
  assert.equal(installationId, (await f.context(f.owner)).installationId);
  assert.equal(f.counts().installations, before.installations, 'reuse the original fbi installation');
  const report = good(await f.call(f.reporter, 'apps.feedback.submit', { appId: f.appId, installationId,
    requestId: 'human-report-once', body: 'Private original report', attachments: [] }));
  const selected = { appId: f.appId, installationId, ticketId: report.ticket.id };
  const listed = successful(await f.invoke(identity, 'list', { appId: f.appId, installationId }));
  assert.equal(listed.result.tickets[0].id, report.ticket.id);
  assert.equal(successful(await f.invoke(identity, 'list', { appId: f.appId, installationId, cursor: '' })).result.tickets.length, 1);
  const reply = { ...selected, body: 'One owner response', expectedRevision: 1 };
  const first = successful(await f.invoke(identity, 'reply', reply, 'reply-once'));
  assert.equal(first.invocation.status, 'succeeded');
  const replay = successful(await f.invoke(identity, 'reply', reply, 'reply-once'));
  assert.equal(replay.invocation.invocationId, first.invocation.invocationId);
  assert.equal(replay.reused, true); assert.equal(f.counts().messages, before.messages + 1);
  const details = successful(await f.invoke(identity, 'get', selected));
  assert.equal(details.result.ticket.messages[0].body, reply.body);
  assert.equal(details.result.ticket.canManage, true);
  assert.equal(JSON.stringify(details).includes('dataBase64'), false);
  const state = f.counts(); await f.restart();
  const afterReload = successful(await f.invoke(identity, 'reply', reply, 'reply-once'));
  assert.equal(afterReload.invocation.invocationId, first.invocation.invocationId);
  assert.deepEqual(f.counts(), state);
  const readAgain = successful(await f.invoke(identity, 'get', selected));
  assert.equal(readAgain.result.ticket.messages.length, 1);
  const own = successful(await f.invoke(identity, 'submit', { appId: f.appId, installationId, body: 'Owner reported issue' }, 'owner-report-once'));
  const ownSelected = { appId: f.appId, installationId, ticketId: own.invocation.receipt.artifacts[0].id };
  assert.equal(successful(await f.invoke(identity, 'status', { ...ownSelected, expectedRevision: 1, status: 'ready_to_check' }, 'owner-ready-once')).invocation.status, 'succeeded');
  assert.equal(successful(await f.invoke(identity, 'accept', { ...ownSelected, expectedRevision: 2 }, 'owner-accept-once')).invocation.status, 'succeeded');
  const accepted = good(await f.call(f.owner, 'apps.feedback.get', ownSelected));
  assert.equal(accepted.ticket.status, 'resolved');
  const files = f.sql(f.feedbackFile, db => db.prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").all());
  assert.equal(files.some(row => /project|fp_|central/u.test(row.name)), false, 'the standalone fp service is not this queue');
});

test('real app and grant revocation after admission prevent dispatch, and ownership stays immutable', { timeout: 20000 }, async t => {
  for (const mode of ['app-revoke', 'grant-revoke']) {
    const f = await fixture(t);
    const identity = await f.grant(f.owner, f.appId), service = f.app.locals.capabilitiesService;
    f.assertOwnerImmutable(f.appId, f.accounts.get(f.administrator).accountId);
    const actor = service.authenticateCredential({ token: identity.token, audience: f.origin + '/mcp' });
    const args = { actor, reference: f.reference('context'), idempotencyKey: 'admit-' + mode, input: { appId: f.appId } };
    const before = f.counts();
    const admitted = service.external.admit(args);
    assert.equal(admitted.invocation.status, 'accepted');
    if (mode === 'app-revoke') good(await f.call(f.owner, 'apps.revoke', { appId: f.appId }));
    else good(await f.call(f.owner, 'access.grants.revoke', { expectedAccountId: identity.accountId, grantId: identity.grantId }));
    // No auth override: the retained actor is an actual server-authenticated
    // delegated credential, and the admission is the real durable Root record.
    await service.external.invoke(args).catch(error => assert.ok(['access_denied', 'apps_owner_required', 'apps_access_denied', 'app_unavailable', 'not_found'].includes(error.code), error.code));
    assert.deepEqual(f.counts(), before);
    denied(await f.invoke(identity, 'context', { appId: f.appId }, args.idempotencyKey));
  }
});

test('actual MCP withholds a completed private query after signed app or grant revoke before final wire output', { timeout: 20000 }, async t => {
  for (const mode of ['app-revoke', 'grant-revoke']) {
    const f = await fixture(t), installationId = (await f.context(f.owner)).installationId;
    const report = good(await f.call(f.reporter, 'apps.feedback.submit', { appId: f.appId, installationId,
      requestId: 'race-report', body: 'Never leak this selected private report', attachments: [] }));
    const identity = await f.grant(f.owner, f.appId), hold = holdSdkJsonEof();
    try {
      const pending = f.invoke(identity, 'get', { appId: f.appId, installationId, ticketId: report.ticket.id });
      await hold.atEnd;
      const current = f.sql(f.capsFile, db => db.prepare("SELECT status FROM cap_invocations WHERE capability_id='apps.feedback.get' ORDER BY created_at DESC LIMIT 1").get());
      assert.equal(current.status, 'succeeded', 'Source read completed while wire EOF is held');
      if (mode === 'app-revoke') good(await f.call(f.owner, 'apps.revoke', { appId: f.appId }));
      else good(await f.call(f.owner, 'access.grants.revoke', { expectedAccountId: identity.accountId, grantId: identity.grantId }));
      hold.release(); const response = await pending;
      denied(response);
      assert.equal(JSON.stringify(response.body).includes(report.ticket.body), false);
      assert.equal(JSON.stringify(response.body).includes(report.ticket.id), false);
    } finally { hold.restore(); }
  }
});

test('revocation before wire output hides a committed reply without rolling it back or repeating it', { timeout: 20000 }, async t => {
  for (const mode of ['app-revoke', 'grant-revoke']) {
    const f = await fixture(t), installationId = (await f.context(f.owner)).installationId;
    const report = good(await f.call(f.reporter, 'apps.feedback.submit', { appId: f.appId, installationId,
      requestId: 'effect-race-report', body: 'Selected report', attachments: [] }));
    const identity = await f.grant(f.owner, f.appId), hold = holdSdkJsonEof();
    const args = { appId: f.appId, installationId, ticketId: report.ticket.id, expectedRevision: 1, body: 'Exactly one committed reply' };
    try {
      const pending = f.invoke(identity, 'reply', args, 'reply-revocation-once');
      await hold.atEnd;
      assert.equal(f.counts().messages, 1, 'the lawful COMMIT precedes revocation');
      const current = f.sql(f.capsFile, db => db.prepare("SELECT status FROM cap_invocations WHERE capability_id='apps.feedback.reply'").get());
      assert.equal(current.status, 'succeeded');
      if (mode === 'app-revoke') good(await f.call(f.owner, 'apps.revoke', { appId: f.appId }));
      else good(await f.call(f.owner, 'access.grants.revoke', { expectedAccountId: identity.accountId, grantId: identity.grantId }));
      hold.release(); const response = await pending;
      denied(response); assert.equal(JSON.stringify(response.body).includes(report.ticket.id), false);
      denied(await f.invoke(identity, 'reply', args, 'reply-revocation-once'));
      assert.equal(f.counts().messages, 1, 'neither denial nor exact replay creates another reply');
    } finally { hold.restore(); }
  }
});

test('actual MCP list and get select attachment metadata before byte I/O; the original UI path is unchanged', { timeout: 20000 }, async t => {
  const f = await fixture(t), installationId = (await f.context(f.owner)).installationId;
  const bytes = readFileSync(new URL('../../modules/feedback/test/fixtures/chrome.png', import.meta.url));
  const sent = good(await f.call(f.reporter, 'apps.feedback.submit', { appId: f.appId, installationId,
    requestId: 'media-report-once', body: 'a' + '😀'.repeat(120), attachments: [{ kind: 'image', name: 'selected.png',
      mimeType: 'image/png', dataBase64: bytes.toString('base64') }] }));
  const identity = await f.grant(f.owner, f.appId, ['apps.feedback.list', 'apps.feedback.get']);
  const original = DatabaseSync.prototype.prepare; let metadataQueries = 0;
  DatabaseSync.prototype.prepare = function (sql, ...rest) {
    if (/^\s*SELECT\b[\s\S]*\bFROM feedback_attachments\b/iu.test(sql)) {
      const columns = sql.slice(0, sql.search(/\bFROM\b/iu));
      assert.doesNotMatch(columns, /\*|\bbytes\b/iu, 'refuse the BLOB column before preparing/reading it');
      metadataQueries++;
    }
    return original.call(this, sql, ...rest);
  };
  try {
    for (const [op, input] of [['list', { appId: f.appId, installationId }], ['get', { appId: f.appId, installationId, ticketId: sent.ticket.id }]]) {
      const value = successful(await f.invoke(identity, op, input));
      const ticket = op === 'list' ? value.result.tickets[0] : value.result.ticket;
      assert.equal(ticket.body.isWellFormed(), true);
      assert.equal(ticket.attachments[0].byteLength, bytes.length);
      assert.equal(Object.hasOwn(ticket.attachments[0], 'dataBase64'), false);
      assert.equal(JSON.stringify(value).includes(bytes.toString('base64')), false);
    }
  } finally { DatabaseSync.prototype.prepare = original; }
  assert.equal(metadataQueries, 2);
  const explicit = good(await f.call(f.owner, 'apps.feedback.get', { appId: f.appId, installationId, ticketId: sent.ticket.id }));
  assert.equal(explicit.ticket.attachments[0].dataBase64, bytes.toString('base64'));
});

test('real Root denies another app of the same owner, another owner, missing app resource and readonly writes before metadata', { timeout: 20000 }, async t => {
  const f = await fixture(t), identity = await f.grant(f.owner, f.appId), before = f.counts();
  denied(await f.invoke(identity, 'context', { appId: f.siblingId }));
  const other = await f.grant(f.owner, f.foreignId);
  denied(await f.invoke(other, 'context', { appId: f.foreignId }));
  const baseOnly = await f.grant(f.owner, f.appId, ['apps.feedback.context'], []);
  denied(await f.invoke(baseOnly, 'context', { appId: f.appId }));
  const readonly = await f.grant(f.owner, f.appId, ['apps.feedback.list', 'apps.feedback.get']);
  denied(await f.invoke(readonly, 'context', { appId: f.appId }));
  assert.deepEqual(f.counts(), before, 'denied context creates no installation or receipt');
  const installationId = (await f.context(f.owner)).installationId;
  assert.equal(successful(await f.invoke(readonly, 'list', { appId: f.appId, installationId })).result.tickets.length, 0);
  denied(await f.invoke(identity, 'context', { appId: f.appId, requiredResources: [appFeedbackResource(f.siblingId)] }), ['invalid_input']);
});

test('actual signed Root MCP owner reads durable selected Opus chunks without widening old metadata grants', { timeout: 20000 }, async t => {
  const f = await fixture(t), installationId = (await f.context(f.owner)).installationId;
  const bytes = Buffer.concat([readFileSync(new URL('../../modules/feedback/test/fixtures/chrome-recorder.webm', import.meta.url)), Buffer.from([0xec, 0x21, 0, 0]), Buffer.alloc(65536)]);
  const sent = good(await f.call(f.reporter, 'apps.feedback.submit', { appId: f.appId, installationId, requestId: 'durable-voice', body: 'Selected private voice',
    attachments: [{ kind: 'audio', name: 'voice.webm', mimeType: 'audio/webm', dataBase64: bytes.toString('base64') }] }));
  const input = { appId: f.appId, installationId, ticketId: sent.ticket.id, attachmentId: sent.ticket.attachments[0].id };
  const identity = await f.grant(f.owner, f.appId, ['apps.feedback.attachment']), before = f.counts(), expectedSha256 = createHash('sha256').update(bytes).digest('hex');
  const firstResponse = await f.invoke(identity, 'attachment', input), first = successful(firstResponse).result;
  assert.equal(first.attachment.sha256, expectedSha256); assert.equal(first.dataBase64.length, 49152); assert.equal(first.nextOffset, 49152);
  t.diagnostic(JSON.stringify({ attachmentChunkChars: first.dataBase64.length, typedResultBytes: Buffer.byteLength(JSON.stringify(first)),
    mcpEnvelopeBytes: Buffer.byteLength(JSON.stringify(firstResponse.body)) }));
  assert(Buffer.byteLength(JSON.stringify(first)) < 59000); await f.restart();
  const last = successful(await f.invoke(identity, 'attachment', { ...input, offset: first.nextOffset, expectedSha256 })).result;
  assert.equal(last.nextOffset, -1); assert.equal(last.attachment.sha256, expectedSha256);
  assert.deepEqual(Buffer.from(first.dataBase64 + last.dataBase64, 'base64'), bytes); assert.deepEqual(f.counts(), before);
  const metadataOnly = await f.grant(f.owner, f.appId, ['apps.feedback.list', 'apps.feedback.get']);
  const anotherOwner = await f.grant(f.administrator, f.appId, ['apps.feedback.attachment']);
  const reporter = await f.grant(f.reporter, f.appId, ['apps.feedback.attachment']);
  const baseOnly = await f.grant(f.owner, f.appId, ['apps.feedback.attachment'], []);
  const original = DatabaseSync.prototype.prepare; let byteReads = 0;
  DatabaseSync.prototype.prepare = function (sql, ...rest) {
    if (/^\s*SELECT\b[\s\S]*\bbytes\b[\s\S]*\bFROM feedback_attachments\b/iu.test(sql)) byteReads++;
    return original.call(this, sql, ...rest);
  };
  try {
    denied(await f.invoke(metadataOnly, 'attachment', input)); denied(await f.invoke(anotherOwner, 'attachment', input));
    denied(await f.invoke(reporter, 'attachment', input)); denied(await f.invoke(baseOnly, 'attachment', input));
    denied(await f.invoke(identity, 'attachment', { ...input, appId: f.siblingId }));
  } finally { DatabaseSync.prototype.prepare = original; }
  assert.equal(byteReads, 0, 'failed capability/app/owner fences must precede private byte reads');
  assert.deepEqual(f.counts(), before);
});

test('actual MCP withholds selected audio bytes after app or grant revocation at final wire output', { timeout: 20000 }, async t => {
  for (const mode of ['app-revoke', 'grant-revoke']) {
    const f = await fixture(t), installationId = (await f.context(f.owner)).installationId;
    const bytes = readFileSync(new URL('../../modules/feedback/test/fixtures/chrome-recorder.webm', import.meta.url));
    const sent = good(await f.call(f.reporter, 'apps.feedback.submit', { appId: f.appId, installationId, requestId: 'private-audio-race', body: 'Private voice',
      attachments: [{ kind: 'audio', name: 'private.webm', mimeType: 'audio/webm', dataBase64: bytes.toString('base64') }] }));
    const identity = await f.grant(f.owner, f.appId, ['apps.feedback.attachment']), hold = holdSdkJsonEof();
    try {
      const pending = f.invoke(identity, 'attachment', { appId: f.appId, installationId, ticketId: sent.ticket.id, attachmentId: sent.ticket.attachments[0].id });
      await hold.atEnd;
      assert.equal(f.sql(f.capsFile, db => db.prepare("SELECT status FROM cap_invocations WHERE capability_id='apps.feedback.attachment' ORDER BY created_at DESC LIMIT 1").get()).status, 'succeeded');
      if (mode === 'app-revoke') good(await f.call(f.owner, 'apps.revoke', { appId: f.appId }));
      else good(await f.call(f.owner, 'access.grants.revoke', { expectedAccountId: identity.accountId, grantId: identity.grantId }));
      hold.release(); const response = await pending; denied(response);
      assert.equal(JSON.stringify(response.body).includes(bytes.toString('base64').slice(0, 64)), false);
      assert.equal(JSON.stringify(response.body).includes(sent.ticket.attachments[0].id), false);
    } finally { hold.restore(); }
  }
});

test('the unchanged cold capabilities initializer opens new attachment grants with the old seven-entry feedback registry', { timeout: 20000 }, async t => {
  const f = await fixture(t), oldCatalog = APP_FEEDBACK_CATALOG.filter(entry => entry.capabilityId !== 'apps.feedback.attachment');
  const legacy = await f.grant(f.owner, f.appId, oldCatalog.map(entry => entry.capabilityId));
  const selected = await f.grant(f.owner, f.appId, ['apps.feedback.attachment']);
  const snapshot = () => f.sql(f.capsFile, db => ({ version: db.prepare('PRAGMA user_version').get().user_version,
    grants: db.prepare('SELECT * FROM cap_grants ORDER BY id').all(), credentials: db.prepare('SELECT * FROM cap_credentials ORDER BY id').all() }));
  const before = snapshot(); await f.app.locals.closeServices();
  const unused = () => { throw Error('cold compatibility must not dispatch private content or effects'); };
  const composition = createAppFeedbackCapabilities({ capabilities: unused, connect: unused, apps: unused, feedback: unused });
  const old = createCapabilitiesService({ databasePath: f.capsFile, projectId: 'soty', actorActive: () => true,
    catalog: [...BUILTIN_CAPABILITIES, ...oldCatalog], externalAdapters: composition.effects,
    readonlyQueries: composition.queries.filter(entry => entry.contract.capabilityId !== 'apps.feedback.attachment') });
  try {
    const actor = old.authenticateCredential({ token: legacy.token, audience: f.origin + '/mcp' });
    for (const entry of createCatalog(oldCatalog).listAll()) {
      const authorization = old.authorize({ actor, capabilityId: entry.capabilityId, version: 1 });
      assert.equal(authorization.capabilityDigest, entry.digest);
    }
    const attachmentActor = old.authenticateCredential({ token: selected.token, audience: f.origin + '/mcp' });
    assert.throws(() => old.authorize({ actor: attachmentActor, capabilityId: 'apps.feedback.attachment', version: 1 }), error => error.code === 'not_found');
    assert.equal(old.external.contracts.length, 7); assert.equal(old.external.contracts.some(entry => entry.capabilityId === 'apps.feedback.attachment'), false);
  } finally { old.close(); }
  assert.deepEqual(snapshot(), before, 'old cold startup and denied unknown capability preserve every grant, credential and storage version');
});
