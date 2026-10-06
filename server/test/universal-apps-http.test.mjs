import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { createHash, generateKeyPairSync, randomBytes } from 'node:crypto';
import { mkdtemp, mkdir, readFile, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve, dirname, basename } from 'node:path';
import { createHttpApp } from '../http-app.js';
import { createClientWithStorage } from '../../modules/connect/browser/client.mjs';
import { createLocalAppsRuntime } from '../../scripts/agent-modules/local-apps.mjs';
import { DatabaseSync } from 'node:sqlite';
import { createHumanBffFixture } from '../../modules/human-identity/examples/bff.mjs';
import { parseHumanLoginContext } from '../../src/platform/human-context.mjs';
import { canonicalHash } from '../../modules/capabilities/server/validation.mjs';
import { EXTERNAL_ADAPTER_PROFILE } from '../../modules/capabilities/server/external-adapters.mjs';

const until = async check => {
  const deadline = Date.now() + 5000;
  do { const result = await check(); if (result) return result; await new Promise(done => setTimeout(done, 10)); } while (Date.now() < deadline);
  throw new Error('fixture_readiness_timeout');
};
function memory() {
  let value;
  return { async read() { return structuredClone(value ?? null); },
    async claim(candidate) { value ??= structuredClone(candidate); return structuredClone(value); },
    async compareAndSwap(revision, candidate) { assert.equal(value.localRevision, revision); value = structuredClone(candidate); return structuredClone(value); } };
}
async function fixture(t, { holdFirstFeedback = false, initialUniversalEnabled = true, appName = 'Independent application' } = {}) {
  const folder = await mkdtemp(join(tmpdir(), 'soty-universal-http-')), dist = join(folder, 'dist');
  await mkdir(dist); await writeFile(join(dist, 'index.html'), '<!doctype html><title>Universal test</title>');
  let app, runtime;
  const clients = [], connections = new Set();
  const source = createServer((_req, res) => res.end('Independent application'));
  const server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
  for (const item of [source, server]) item.on('connection', socket => { connections.add(socket); socket.once('close', () => connections.delete(socket)); });
  await Promise.all([source, server].map(item => new Promise(done => item.listen(0, '127.0.0.1', done))));
  const origin = `http://127.0.0.1:${server.address().port}`;
  const dataDir = join(folder, 'data');
  t.after(async () => {
    runtime?.stop(); clients.forEach(client => client.dispose()); await app?.locals.closeServices();
    connections.forEach(socket => socket.destroy());
    await Promise.all([source, server].map(item => new Promise(done => item.close(done))));
    assert.equal(dirname(resolve(folder)), resolve(tmpdir())); assert.match(basename(folder), /^soty-universal-http-/);
    await rm(folder, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  });
  app = createHttpApp(dist, { dataDir, connectOrigins: [origin], appOriginTemplate: `http://{appId}.localhost:${server.address().port}`, universalAppsEnabled: initialUniversalEnabled });
  server.on('upgrade', (req, socket, head) => { if (!app.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy(); });
  const token = randomBytes(32).toString('base64url');
  const identity = { linkId: 'universal_http_link_123456789012345', hostDeviceId: 'universal_http_host', connectorId: 'universal_http_connector', name: 'Fixture device' };
  const registered = await fetch(origin + '/api/connectors/register', { method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
    body: JSON.stringify({ linkId: identity.linkId, deviceId: identity.hostDeviceId, connectorId: identity.connectorId, scope: 'Dev', protocol: 2, capabilities: ['apps'] }) });
  assert.equal((await registered.json()).ok, true);
  runtime = createLocalAppsRuntime({ randomSecret: () => randomBytes(32).toString('base64url'), digest: value => createHash('sha256').update(value).digest('hex'),
    createWebSocket: url => new globalThis.WebSocket(url), httpRequest: request,
    encodeBase64: bytes => Buffer.from(bytes).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64') }, { identity, token, serverUrl: origin });
  runtime.start(); await until(() => runtime.status().connected);
  async function client(label) {
    const value = createClientWithStorage({ projectId: 'soty', endpoint: origin + '/api/connect/rpc',
      fetch: (url, options) => fetch(url, { ...options, headers: { ...options.headers, origin } }) }, memory());
    clients.push(value); const account = await value.bootstrap(label); return { client: value, account };
  }
  const owner = await client('Владелец'), one = await client('Первый участник'), two = await client('Второй участник');
  const claim = await runtime.claim();
  await owner.client.extension('apps.claim', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, claimCode: claim.claimCode });
  const registrationArgs = { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId,
    name: appName, port: source.address().port, entryPath: '/', grants: { accountIds: [one.account.accountId, two.account.accountId], communityIds: [] } };
  const blocked = holdFirstFeedback ? new DatabaseSync(join(dataDir, 'feedback', 'feedback.sqlite')) : null;
  let created;
  try {
    blocked?.exec('BEGIN IMMEDIATE');
    created = await owner.client.extension('apps.register', registrationArgs);
  } finally { if (blocked) { blocked.exec('ROLLBACK'); blocked.close(); } }
  const appId = created.app.id;
  await until(async () => (await owner.client.extension('apps.list')).apps.find(item => item.id === appId)?.state === 'ready');
  return { app, origin, owner, one, two, appId, dataDir, created, registrationArgs,
    async restart(options = {}) { await app.locals.closeServices(); app = createHttpApp(dist, { dataDir, connectOrigins: [origin], appOriginTemplate: `http://{appId}.localhost:${server.address().port}`, ...options }); },
    service() { return app; } };
}
const fails = code => error => error.code === code;

test('real signed Apps/Connect authority and separate HTTP/MCP audiences guard the generic application ledger', { timeout: 20000 }, async t => {
  const f = await fixture(t), source = f.created.universalRegistration.descriptor.app.source;
  const proofs = new Map(); let writes = 0, sourceActive = true;
  const id = 'app.' + f.appId + ':createWorkItem', resource = 'app.' + f.appId + ':workspace-one';
  const adapter = { profile: EXTERNAL_ADAPTER_PROFILE,
    withAuthority(_request, callback) { if (!sourceActive) throw Object.assign(new Error('external_resource_denied'), { code:'external_resource_denied' }); return callback(); },
    async execute(request) { writes++; if (!proofs.has(request.requestId)) proofs.set(request.requestId, {
      requestId:request.requestId,inputDigest:request.inputDigest,outcome:'committed',
      effects:[{kind:'created',resourceType:'planner-object',resourceId:'synthetic_object_one',revision:1}],
      receipt:{verificationMethod:'domain_read',artifacts:[{type:'planner-object',id:'synthetic_object_one',revision:1}]},
    }); },
    async readProof(request) { return proofs.get(request.requestId) ?? {requestId:request.requestId,inputDigest:request.inputDigest,outcome:'not_applied'}; },
  };
  // Deliberately synthetic source; real Planner source persistence is proved by
  // modules/capabilities/test/planner-effect-adapter.test.mjs, not by this map.
  const catalog = {capabilityId:id,version:1,appId:f.appId,title:'Создать один объект',description:'Изолированный HTTP/MCP authority test.',visibility:'private',executionEnabled:true,
    inputSchema:{type:'object',properties:{title:{type:'string',maxLength:500}},required:['title'],additionalProperties:false},
    outputSchema:{type:'object',properties:{objectId:{type:'string',maxLength:160},revision:{type:'integer',minimum:0}},required:['objectId','revision'],additionalProperties:false},
    resources:[resource],effects:['create'],recipients:[resource],executionBinding:{kind:'registered',handler:id,version:1,
      binding:{id:'app.'+f.appId+':source-binding',version:1,digest:canonicalHash({scope:resource,protocol:EXTERNAL_ADAPTER_PROFILE})}}};
  const options = {capabilityAudience:f.origin,externalApplications:[{appId:f.appId,target:{revision:source.revision,digest:source.digest},catalog,adapter}]};
  await f.restart(options);
  const documentResponse = await fetch(f.origin + '/api/capabilities/v1/openapi.json'), document = await documentResponse.json();
  assert.equal(documentResponse.status, 200); assert.equal(document['x-soty-mcp'].tools.length, 9);
  assert.equal(document.paths['/api/capabilities/v1/app-actions/invoke'].post.operationId, 'apps_invoke');
  assert.equal(JSON.stringify(document).includes(f.appId), false, 'public protocol documentation does not expose the private installed application');
  const reference = f.service().locals.capabilitiesService.external.contracts[0];
  const principal = (await f.owner.client.extension('access.principals.create',{expectedAccountId:f.owner.account.accountId,label:'Synthetic generic wire'})).principal;
  const grant = (await f.owner.client.extension('access.grants.issue',{expectedAccountId:f.owner.account.accountId,principalId:principal.id,
    capabilities:[{capabilityId:id,version:1}],resources:[resource],effects:['create'],recipients:[resource],expiresAt:Date.now()+600000,
    allowDelegation:false,maxDepth:0,budget:{unit:'invocations',limit:3}})).grant;
  async function key(audience) { return (await f.owner.client.extension('access.credentials.issue',{expectedAccountId:f.owner.account.accountId,grantId:grant.id,audience})).token; }
  const httpToken=await key(f.origin),mcpToken=await key(f.origin+'/mcp');
  async function http(kind,args,token=httpToken) { const response=await fetch(f.origin+'/api/capabilities/v1/app-actions/'+kind,{method:'POST',
    headers:{'content-type':'application/json',origin:f.origin,...(token?{authorization:'Bearer '+token}:{})},body:typeof args==='string'?args:JSON.stringify(args)});
    return{status:response.status,data:await response.json()}; }
  const page = await http('catalog',{}); assert.equal(page.status,200); assert.equal(page.data.items.length,1);
  assert.equal((await http('catalog',{},mcpToken)).status,401); assert.equal((await http('catalog',{},null)).status,401);
  assert.equal((await http('contract',{reference})).data.binding.digest.length,64);
  assert.equal((await http('invoke','{"reference":'+JSON.stringify(reference)+',"idempotencyKey":"same-intent","input":{"title":"x","title":"y"}}')).status,400);
  const args={reference,idempotencyKey:'same-intention-0001',input:{title:'Synthetic private input'}};
  const first=await http('invoke',args); assert.equal(first.status,201); assert.equal(first.data.invocation.status,'succeeded'); assert.equal(writes,1);
  await f.restart(options); const repeated=await http('invoke',args); assert.equal(repeated.status,200);
  assert.equal(repeated.data.invocation.invocationId,first.data.invocation.invocationId);assert.equal(writes,1);
  assert.equal(JSON.stringify(repeated.data).includes(args.input.title),false);
  async function mcp(method,params) { const response=await fetch(f.origin+'/mcp',{method:'POST',headers:{authorization:'Bearer '+mcpToken,
    accept:'application/json, text/event-stream','content-type':'application/json','mcp-protocol-version':'2026-07-28','mcp-method':method,
    ...(method==='tools/call'?{'mcp-name':params.name}:{})},body:JSON.stringify({jsonrpc:'2.0',id:1,method,params:{...params,_meta:{'io.modelcontextprotocol/protocolVersion':'2026-07-28',
      'io.modelcontextprotocol/clientInfo':{name:'synthetic-universal-wire',version:'1'},'io.modelcontextprotocol/clientCapabilities':{}}}})});
    return{status:response.status,data:await response.json()}; }
  const listed=await mcp('tools/list',{});assert.equal(listed.status,200);assert.equal(listed.data.result.tools.length,9);
  const read=await mcp('tools/call',{name:'apps_invocation_get',arguments:{invocationId:first.data.invocation.invocationId}});
  assert.equal(read.status,200);assert.equal(read.data.result.structuredContent.invocation.invocationId,first.data.invocation.invocationId);
  sourceActive=false; assert.equal((await http('get',{invocationId:first.data.invocation.invocationId})).status,403);
  sourceActive=true;await f.owner.client.extension('apps.revoke',{appId:f.appId});
  assert.equal((await http('get',{invocationId:first.data.invocation.invocationId})).status,404);assert.equal(writes,1);
});

test('actual signed registration commits real feedback and preserves request receipts across HTTP restart', { timeout: 20000 }, async t => {
  const f = await fixture(t);
  const current = await f.owner.client.extension('apps.universal.get', { appId: f.appId, expectedAccountId: f.owner.account.accountId });
  assert.equal(current.registration.state, 'ready');
  const args = { expectedAccountId: f.owner.account.accountId, appId: f.appId, requestId: 'register-one', expectedRevision: current.registration.revision,
    proposal: { kind: 'author-draft', draft: { title: 'Приложение независимого автора' } } };
  const result = await f.owner.client.extension('apps.universal.admit', args);
  assert.equal(result.registration.state, 'ready'); assert.equal(result.registration.gates.ui, 'ready');
  assert.equal(result.registration.gates.agent, 'not-admitted'); assert.equal(result.registration.gates.local, 'not-admitted');
  assert.equal(result.registration.descriptor.capabilities.length, 0);
  assert.match(result.registration.feedback.installationId, /^fbi_/);
  await f.restart();
  const repeated = await f.owner.client.extension('apps.universal.admit', args);
  assert.equal(repeated.replayed, true); assert.deepEqual(repeated.receipt, result.receipt);
  assert.equal(repeated.registration.feedback.installationId, result.registration.feedback.installationId);
  await assert.rejects(() => f.one.client.extension('apps.universal.get', { appId: f.appId, expectedAccountId: f.one.account.accountId }), fails('apps_owner_required'));
  await assert.rejects(() => f.owner.client.extension('apps.universal.admit', { ...args, proposal: { kind: 'author-draft', draft: { title: 'Подмена' } } }), fails('registration_intent_conflict'));
});

test('automatic registration retry reconciles a pending real inbox without creating another app', { timeout: 20000 }, async t => {
  const f = await fixture(t, { holdFirstFeedback: true });
  assert.equal(f.created.universalRegistration.state, 'pending-feedback');
  const repeated = await f.owner.client.extension('apps.register', f.registrationArgs);
  assert.equal(repeated.app.id, f.appId);
  assert.equal(repeated.universalRegistration.state, 'ready');
  assert.equal(repeated.universalRegistration.generation, f.created.universalRegistration.generation);
  assert.equal(repeated.universalRegistration.revision, f.created.universalRegistration.revision + 1);
  assert.equal((await f.owner.client.extension('apps.list')).apps.length, 1);
  assert.equal((await f.owner.client.extension('apps.universal.history', { expectedAccountId: f.owner.account.accountId, appId: f.appId })).items.length, 1);
});

test('a retained legacy app joins through its normalized verified title without a new card or changed ownership', { timeout: 20000 }, async t => {
  const f = await fixture(t, { initialUniversalEnabled: false, appName: ' '.repeat(161) + 'Independent application' });
  assert.equal(f.created.universalRegistration, undefined);
  await f.restart();
  const migrated = await f.owner.client.extension('apps.register', f.registrationArgs);
  assert.equal(migrated.app.id, f.appId); assert.equal(migrated.app.name, 'Independent application');
  assert.equal(migrated.universalRegistration.state, 'ready');
  assert.equal(migrated.universalRegistration.descriptor.app.title, 'Independent application');
  assert.equal((await f.owner.client.extension('apps.list')).apps.length, 1);
});

test('actual signed feedback isolates reporters, survives lost ACK and replies with explicit acceptance', { timeout: 20000 }, async t => {
  const f = await fixture(t), context = (await f.one.client.extension('apps.feedback.context', { appId: f.appId })).context;
  const args = { appId: f.appId, installationId: context.installationId, requestId: 'feedback-one', body: 'Проблема первого участника', attachments: [] };
  const sent = await f.one.client.extension('apps.feedback.submit', args);
  assert.equal((await f.one.client.extension('apps.feedback.submit', args)).replayed, true);
  assert.equal((await f.two.client.extension('apps.feedback.list', { appId: f.appId, installationId: context.installationId })).tickets.length, 0);
  await assert.rejects(() => f.two.client.extension('apps.feedback.get', { appId: f.appId, installationId: context.installationId, ticketId: sent.receipt.ticketId }), fails('feedback_ticket_unavailable'));
  const base = { appId: f.appId, installationId: context.installationId, ticketId: sent.receipt.ticketId };
  const reply = await f.owner.client.extension('apps.feedback.reply', { ...base, requestId: 'reply-one', expectedRevision: 1, body: 'Ответ поддержки' });
  assert.equal(reply.ticket.status, 'in_progress');
  const ready = await f.owner.client.extension('apps.feedback.status', { ...base, requestId: 'ready-one', expectedRevision: reply.ticket.revision, status: 'ready_to_check' });
  const accepted = await f.one.client.extension('apps.feedback.accept', { ...base, requestId: 'accept-one', expectedRevision: ready.ticket.revision });
  assert.equal(accepted.ticket.status, 'resolved');
  const unsigned = await fetch(f.origin + '/api/connect/rpc', { method: 'POST', headers: { 'content-type': 'application/json', origin: f.origin },
    body: JSON.stringify({ protocol: 1, op: 'apps.feedback.get', args: { ...base } }) });
  assert.equal((await unsigned.json()).error.code, 'proof_required');
});

test('compatible fallback disables new mutations while retaining application and committed feedback data', { timeout: 20000 }, async t => {
  const f = await fixture(t), context = (await f.one.client.extension('apps.feedback.context', { appId: f.appId })).context;
  const sent = await f.one.client.extension('apps.feedback.submit', { appId: f.appId, installationId: context.installationId,
    requestId: 'before-fallback', body: 'Не терять при откате', attachments: [] });
  await f.restart({ universalAppsEnabled: false });
  await assert.rejects(() => f.one.client.extension('apps.feedback.list', { appId: f.appId, installationId: context.installationId }), fails('unsupported_operation'));
  assert.equal((await f.owner.client.extension('apps.list')).apps[0].id, f.appId);
  await f.restart();
  const recovered = await f.one.client.extension('apps.feedback.get', { appId: f.appId, installationId: context.installationId, ticketId: sent.receipt.ticketId });
  assert.equal(recovered.ticket.body, 'Не терять при откате');
  assert.equal((await f.one.client.extension('apps.feedback.context', { appId: f.appId })).context.installationId, context.installationId);
});

test('actual signed media survives restart, list omits bytes and invalid duration commits no ticket', { timeout: 20000 }, async t => {
  const f = await fixture(t), context = (await f.one.client.extension('apps.feedback.context', { appId: f.appId })).context;
  const media = async (name, mimeType, kind) => ({ kind, name, mimeType,
    dataBase64: (await readFile(new URL('../../modules/feedback/test/fixtures/' + name, import.meta.url))).toString('base64') });
  const attachments = await Promise.all([media('chrome.png', 'image/png', 'image'), media('chrome-recorder.webm', 'audio/webm;codecs=opus', 'audio')]);
  const base = { appId: f.appId, installationId: context.installationId };
  const sent = await f.one.client.extension('apps.feedback.submit', { ...base, requestId: 'media-one', body: 'Синтетические снимок и звук', attachments });
  await f.restart();
  const listed = await f.one.client.extension('apps.feedback.list', base);
  assert.equal(listed.tickets.length, 1);
  assert.equal(listed.tickets[0].attachments.length, 2);
  assert(listed.tickets[0].attachments.every(item => !Object.hasOwn(item, 'dataBase64')));
  const got = await f.one.client.extension('apps.feedback.get', { ...base, ticketId: sent.receipt.ticketId });
  assert.deepEqual(got.ticket.attachments.map(item => item.dataBase64), attachments.map(item => item.dataBase64));
  const tooLong = await media('ffmpeg-opus-121s.ogg', 'audio/ogg', 'audio');
  await assert.rejects(() => f.one.client.extension('apps.feedback.submit', { ...base, requestId: 'too-long', body: 'Отклонённая запись',
    attachments: [tooLong] }), fails('feedback_audio_duration_limit'));
  assert.equal((await f.one.client.extension('apps.feedback.list', base)).tickets.length, 1);
  await assert.rejects(() => f.two.client.extension('apps.feedback.get', { ...base, ticketId: sent.receipt.ticketId }), fails('feedback_ticket_unavailable'));
});

test('Root host reserves disabled human issuer without a database, then signs the same existing profile into two independent BFFs', { timeout: 30000 }, async t => {
  const f = await fixture(t);
  const disabled = await fetch(f.origin + '/human-identity/.well-known/openid-configuration');
  assert.equal(disabled.status, 503); assert.equal((await disabled.json()).error, 'temporarily_unavailable');
  await assert.rejects(readFile(join(f.dataDir, 'human-identity', 'identity.sqlite')), error => error.code === 'ENOENT');
  const reviews = await f.one.client.extension('apps.reviews.context', { appId: f.appId });
  assert.equal(reviews.ok, true); assert.equal(reviews.mode, 'disabled'); assert.deepEqual(reviews.subjects, []);
  const secrets = [randomBytes(32).toString('base64url'), randomBytes(32).toString('base64url')];
  const rps = await Promise.all(['root-alpha', 'root-beta'].map((clientId, index) => createHumanBffFixture({ clientId, clientSecret: secrets[index] })));
  t.after(() => Promise.all(rps.map(rp => rp.close())));
  const key = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' });
  Object.assign(key, { kid: 'root-fixture', use: 'sig', alg: 'RS256' });
  const issuer = f.origin + '/human-identity';
  const humanIdentity = { enabled: true, issuer, registryId: 'soty', environmentId: 'production',
    clients: rps.map((rp, index) => ({ id: rp.clientId, label: 'Независимое приложение ' + rp.clientId, redirectUri: rp.redirectUri, clientSecret: secrets[index] })),
    jwks: { keys: [key] }, cookieKeys: [randomBytes(32).toString('base64url')], artifactKey: randomBytes(32), artifactKeyId: 'root-fixture' };
  await f.restart({ humanIdentity }); await Promise.all(rps.map(rp => rp.configure(issuer)));
  const jar = new Map();
  async function visit(input, fields) {
    const url = new URL(input); assert.equal(url.hostname, '127.0.0.1');
    const cookie = [...jar.values()].filter(item => item.host === url.hostname && (url.pathname === item.path || url.pathname.startsWith(item.path.endsWith('/') ? item.path : item.path + '/')))
      .map(item => item.pair).join('; ');
    const response = await fetch(url, { redirect: 'manual', signal: AbortSignal.timeout(5000),
      ...(fields ? { method: 'POST', body: new URLSearchParams(fields) } : {}),
      headers: { ...(cookie ? { cookie } : {}), ...(fields ? { origin: url.origin, 'sec-fetch-site': 'same-origin' } : {}) } });
    for (const raw of response.headers.getSetCookie()) {
      const [pair, ...attributes] = raw.split(';'), name = pair.split('=')[0];
      const path = attributes.find(item => item.trim().toLowerCase().startsWith('path='))?.trim().slice(5) || '/';
      jar.set(url.hostname + '\0' + path + '\0' + name, { host: url.hostname, path, pair });
    }
    const text = await response.text(); assert(Buffer.byteLength(text) <= 65536);
    return { status: response.status, text, body: response.headers.get('content-type')?.includes('application/json') ? JSON.parse(text) : null,
      location: response.headers.get('location') ? new URL(response.headers.get('location'), url) : null };
  }
  let serial = 0;
  for (const rp of rps) {
    const start = await visit(rp.origin + '/login'), authorization = await visit(start.location), document = await visit(authorization.location);
    assert.equal(document.status, 200, 'Root interaction safe failure=' + (document.body?.error || 'none'));
    const response = await visit(authorization.location.href + '/context');
    const context = parseHumanLoginContext(response.body, { interactionId: response.body.interactionId, checkedAt: Date.now() });
    assert.equal(context.client.id, rp.clientId);
    const decision = await f.one.client.extension('identity.human.approve', { expectedAccountId: f.one.account.accountId,
      interactionId: context.interactionId, browserNonce: context.browserNonce, csrf: context.csrf, requestId: 'root-human-' + (++serial), decision: 'approve' },
      { expectedAccountId: f.one.account.accountId });
    assert.equal(decision.decision, 'approved');
    let completed = await visit(authorization.location.href + '/complete', { csrf: context.csrf });
    assert.equal(completed.status, 303); completed = await visit(completed.location);
    assert([302, 303].includes(completed.status)); assert.equal(completed.location.origin + completed.location.pathname, rp.redirectUri);
    assert.equal((await visit(completed.location)).status, 200);
  }
  const first = await visit(rps[0].origin + '/me'), second = await visit(rps[1].origin + '/me');
  assert.equal(first.body.identity.sub, f.one.account.accountId); assert.equal(second.body.identity.sub, f.one.account.accountId);
  assert.notEqual(first.body.localAccountId, second.body.localAccountId);
  await f.restart({ humanIdentity });
  assert.equal((await visit(rps[0].origin + '/me')).status, 200);
  await f.restart({ universalAppsEnabled: false, humanIdentity });
  assert.equal((await fetch(issuer + '/.well-known/openid-configuration')).status, 503);
  assert.equal((await f.owner.client.extension('apps.list')).apps[0].id, f.appId);
});
