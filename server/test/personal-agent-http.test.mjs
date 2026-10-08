import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { createHash, generateKeyPairSync, randomUUID, randomBytes, sign } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname, resolve } from 'node:path';
import { createHttpApp, waitForRejectedHttpStart } from '../http-app.js';
import { digestArgs } from '../../modules/connect/server/index.mjs';
import { GLM_MODEL } from '../../modules/personal-agent/provider/index.mjs';
import { createLocalAppsRuntime } from '../../scripts/agent-modules/local-apps.mjs';
import { createTrustedReadonlyQueryAdapter } from '../../modules/capabilities/server/readonly-queries.mjs';
import { captureExternalApplications } from '../external-applications.js';
import { canonicalHash } from '../../modules/capabilities/server/validation.mjs';
import { createClientWithStorage } from '../../modules/connect/browser/client.mjs';
import { createPersonalAgentConversation } from '../../src/platform/personal-agent-client.ts';

const defer = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
const identity = () => { const pair = generateKeyPairSync('ec', { namedCurve: 'prime256v1' }), encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { key: pair.privateKey, publicJwk: pair.publicKey.export({ format: 'jwk' }), encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }) }; };

async function sharedBrowser(t, f) {
  let local = null; const data = new Map();
  const storage = { async read() { return structuredClone(local); }, async claim(candidate) { local ??= structuredClone(candidate); return structuredClone(local); },
    async compareAndSwap(revision, candidate) { assert.equal(local.localRevision, revision); local = structuredClone(candidate); return structuredClone(local); } };
  const client = createClientWithStorage({ projectId: 'soty', endpoint: f.origin + '/api/connect/rpc',
    fetch: (url, options) => fetch(url, { ...options, headers: { ...options.headers, origin: f.origin } }) }, storage);
  t.after(() => client.dispose()); const account = await client.bootstrap('Own shared browser fixture');
  const ui = createPersonalAgentConversation({ accountId: account.accountId, client,
    storage: { getItem: key => data.get(key) ?? null, setItem: (key, value) => data.set(key, value) } });
  return { client, account, ui, snapshot: () => structuredClone(local),
    mergeProfile(other) { local = { ...local, installations: [...local.installations, ...other.snapshot().installations], localRevision: local.localRevision + 1 }; } };
}

async function fixture(t, { enabled = true } = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-agent-http-owned-')), dist = join(directory, 'dist');
  await mkdir(dist); await writeFile(join(dist, 'index.html'), '<!doctype html><title>Owned agent fixture</title>');
  const tenant = 'synthetic_billing', keyRef = Object.freeze({}), receipts = new Map(), calls = [], holds = [];
  let allowed = true, lost = false, hold = null, app, closed = false, settledReceiptReads = 0;
  const connections = new Set(), server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
  server.on('connection', socket => { connections.add(socket); socket.once('close', () => connections.delete(socket)); });
  server.on('upgrade', (req, socket, head) => { if (!app?.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy(); });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  const origin = `http://127.0.0.1:${server.address().port}`;
  // Synthetic billing ports test mounting, not the real commerce reserve/finish implementation.
  // The separately mandatory, explicit integration-test proves the unchanged actual commerce gateway.
  const configuration = { enabled, gateway: { gatewayOrigin: 'https://fixture-commerce.invalid', billingTenant: tenant, keyRef,
    authorizeActor: () => allowed, resolveCredential: async ref => ({ keyRef: ref, tenantId: tenant, bearer: 'synthetic-only-fixture-accounted-key' }),
    readReceipt: async ({ requestId, billingTenant, keyRef: ref }) => { assert.equal(billingTenant, tenant); assert.equal(ref, keyRef);
      const request = receipts.get(requestId); if (request?.status === 'completed') settledReceiptReads++;
      return { tenantId: tenant, request }; },
    fetchImpl: async (url, init) => {
      assert.equal(url, 'https://fixture-commerce.invalid/v1/chat/completions');
      const body = JSON.parse(init.body); assert.equal(body.model, GLM_MODEL); assert.equal(body.autoFallback, false);
      calls.push({ body, key: init.headers['Idempotency-Key'] });
      if (hold) { const barrier = hold; hold = null; barrier.entered.resolve(); await barrier.release.promise; }
      const last = body.messages.at(-1);
      let name = 'finish_answer', args = { answer: 'Подтверждённый ответ', selectedEvidenceIds: [], claims: [] };
      if (last.role === 'user' && last.content === 'Read my notes') { name = 'list_my_notes'; args = {}; }
      if (last.role === 'user' && last.content.startsWith('Read note ')) { name = 'read_my_note'; args = { noteId: last.content.slice(10) }; }
      if (last.role === 'user' && last.content === 'Read source') { name = 'source_read_1'; args = {}; }
      if (last.role === 'user' && last.content.startsWith('Local task ')) { name = 'run_on_my_device'; args = { text: last.content.slice(11) }; }
      if (last.role === 'tool') { const evidence = JSON.parse(last.content).evidence; args = { answer: 'Источник проверен', selectedEvidenceIds: evidence.map(item => item.id),
        claims: [{ kind: 'fact', statement: 'Источник доступен текущему пользователю', evidenceIds: evidence.map(item => item.id),
          freshness: 'current', confidence: 'high', applicability: 'Текущий пользователь и разрешённые источники' }] }; }
      const id = randomUUID(); receipts.set(id, { id, status: 'completed', model_requested: GLM_MODEL, model_resolved: GLM_MODEL,
        input_tokens: 3, output_tokens: 2, charge_urub: '7' });
      if (lost) throw new Error('private_fixture_payload_must_not_escape');
      const packet = `data: ${JSON.stringify({ id: 'chatcmpl_owned', model: GLM_MODEL, choices: [{ index: 0, delta: { tool_calls: [{ index: 0,
        id: 'call_owned', type: 'function', function: { name, arguments: JSON.stringify(args) } }] }, finish_reason: null }] })}\n\n`
        + `data: ${JSON.stringify({ id: 'chatcmpl_owned', model: GLM_MODEL, choices: [{ index: 0, delta: {}, finish_reason: 'tool_calls' }],
          usage: { prompt_tokens: 3, completion_tokens: 2, total_tokens: 5 } })}\n\n` + 'data: [DONE]\n\n';
      return new Response(packet, { status: 200, headers: { 'content-type': 'text/event-stream', 'x-soty-request-id': id } });
    } } };
  const base = { dataDir: join(directory, 'data'), connectOrigins: [origin], appOriginTemplate: `http://{appId}.localhost:${server.address().port}`,
    appHosting: { domainProfile: 'separate-site', namedAppZone: '', discoveryOrigin: '' }, universalAppsEnabled: false,
    gonka: { apiKey: '', baseUrl: 'http://127.0.0.1:1/v1', applicationTokens: {} }, personalAgent: configuration };
  app = createHttpApp(dist, base);
  async function call(actor, op, args = {}) {
    const post = value => fetch(`${origin}/api/connect/rpc`, { method: 'POST', headers: { Origin: origin, 'Content-Type': 'application/json' }, body: JSON.stringify({ protocol: 1, ...value }) }).then(reply => reply.json());
    const challenge = await post({ op: 'challenge', args: { operation: op, digest: digestArgs(args) } }); assert.equal(challenge.ok, true);
    return post({ op, args, proof: { challengeId: challenge.challengeId, publicJwk: actor.publicJwk,
      signature: sign('sha256', Buffer.from(challenge.message), { key: actor.key, dsaEncoding: 'ieee-p1363' }).toString('base64url') } });
  }
  const alice = identity(), bob = identity();
  const bootstrap = actor => call(actor, 'bootstrap', { label: 'Synthetic fixture human', encryptionPublicJwk: actor.encryptionPublicJwk });
  async function stop() {
    if (closed) return; closed = true; holds.forEach(barrier => barrier.release.resolve());
    const stopped = new Promise(done => server.close(done)); connections.forEach(socket => socket.destroy());
    await app.locals.closeServices(); await stopped;
  }
  t.after(async () => { await stop(); assert.equal(dirname(resolve(directory)), resolve(tmpdir()));
    assert.match(directory, /soty-agent-http-owned-/); await rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 }); });
  const a = await bootstrap(alice), b = await bootstrap(bob); assert.equal(a.ok, true); assert.equal(b.ok, true);
  return { directory, dist, origin, alice, bob, a, b, calls, call, base, configuration, settledReceipts: () => settledReceiptReads,
    service: () => app, stop,
    turn: (actor, requestId, prompt = 'Neutral question', extra = {}) => call(actor, 'agent.turn', { requestId, prompt, ...extra }),
    hold() { const barrier = { entered: defer(), release: defer() }; holds.push(barrier); hold = barrier; return barrier; },
    setAllowed(value) { allowed = value; }, setLost(value) { lost = value; },
    async restart(extra = {}) { await app.locals.closeServices(); app = createHttpApp(dist, { ...base, ...extra }); },
    async phone() { const phone = identity(), started = await call(phone, 'enrollment.start', { label: 'Own second device', encryptionPublicJwk: phone.encryptionPublicJwk });
      assert.equal(started.ok, true); assert.equal((await call(alice, 'enrollment.approve', { requestId: started.requestId,
        wrappedKey: { schema: 'synthetic.encrypted', ciphertext: 'opaque' } })).ok, true);
      assert.equal((await call(phone, 'enrollment.finish', { requestId: started.requestId, expectedAccountId: a.accountId })).ok, true); return phone; },
  };
}

async function ownedAgent(t, f) {
  const token = randomBytes(32).toString('base64url'), identity = { linkId: 'personal_owned_link_123456789012345', hostDeviceId: 'personal_owned_host',
    connectorId: 'personal_owned_connector', name: 'Owned synthetic local agent' };
  const response = await fetch(f.origin + '/api/connectors/register', { method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
    body: JSON.stringify({ linkId: identity.linkId, deviceId: identity.hostDeviceId, connectorId: identity.connectorId, scope: 'Dev', protocol: 2,
      capabilities: ['apps', 'agent'], agent: { id: 'opencode', provider: 'gonka', available: true } }) });
  assert.equal((await response.json()).ok, true);
  const runtime = createLocalAppsRuntime({ randomSecret: () => randomBytes(32).toString('base64url'), digest: value => createHash('sha256').update(value).digest('hex'),
    createWebSocket: url => new globalThis.WebSocket(url), httpRequest: request,
    encodeBase64: bytes => Buffer.from(bytes).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64') }, { identity, token, serverUrl: f.origin });
  t.after(() => runtime.stop()); runtime.start(); const deadline = Date.now() + 5000;
  while (!runtime.status().connected) { assert.ok(Date.now() < deadline); await new Promise(done => setTimeout(done, 10)); }
  const claim = await runtime.claim(); assert.equal((await f.call(f.alice, 'apps.claim', { hostDeviceId: claim.hostDeviceId,
    connectorId: claim.connectorId, claimCode: claim.claimCode })).ok, true);
  return { target: { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, cwd: 'C:\\owned-synthetic-workspace' },
    auth: { linkId: identity.linkId, deviceId: identity.hostDeviceId, connectorId: identity.connectorId, token }, runtime };
}

async function historyNote(f, actor, accountId) {
  const list = await f.call(actor, 'notes.list', { expectedAccountId: accountId }); assert.equal(list.ok, true);
  const metadata = list.notes.find(note => note.title === 'Диалог с помощником'); assert.ok(metadata);
  const value = await f.call(actor, 'notes.get', { expectedAccountId: accountId, noteId: metadata.noteId }); assert.equal(value.ok, true); return value.note;
}

test('portable generic runtime is an unchanged qualified source, with no absolute donor import', async () => {
  const root = new URL('../../modules/personal-agent/runtime/vendor/', import.meta.url), pin = JSON.parse(await readFile(new URL('UPSTREAM.json', root))),
    bytes = await readFile(new URL('agent-runtime-core.ts', root));
  assert.equal(pin.upstreamRevision, '23717ad30878c9ad7a8d9d614bbd8384004763c9');
  assert.equal(createHash('sha256').update(bytes).digest('hex'), pin.sha256); assert.equal(bytes.length, pin.bytes);
  const runtime = await import(new URL('agent-runtime-core.ts', root)); assert.equal(typeof runtime.runBoundedRuntimeTurn, 'function');
});

test('actual Root HTTP mounts default-off signed operations and preserves legacy device operations/status', async t => {
  const f = await fixture(t, { enabled: false }); const status = await fetch(f.origin + '/api/apps/capabilities').then(value => value.json());
  assert.equal(status.personalAgent.configured, false); assert.equal(status.personalAgent.missingBinding, 'not_enabled'); assert.equal(status.personalAgent.wholeReady, false);
  assert.equal(typeof status.agentConfigured, 'boolean'); assert.equal((await f.call(f.alice, 'apps.devices')).ok, true);
  const result = await f.turn(f.alice, 'disabled_turn_01'); assert.equal(result.ok, false); assert.equal(f.calls.length, 0);
});

test('actual Root HTTP signed turn uses portable runtime and current own Notes, not another account', async t => {
  const f = await fixture(t);
  assert.equal((await f.call(f.alice, 'notes.put', { expectedAccountId: f.a.accountId, noteId: 'owned_note_01', mutationId: 'mutation_owned_01',
    expectedRevision: 0, title: 'Own source note', body: 'Own fixture text', items: [], color: 'plain', pinned: false, state: 'active' })).ok, true);
  assert.equal((await f.call(f.bob, 'notes.put', { expectedAccountId: f.b.accountId, noteId: 'foreign_note_01', mutationId: 'mutation_other_01',
    expectedRevision: 0, title: 'Foreign private title', body: 'Foreign private body', items: [], color: 'plain', pinned: false, state: 'active' })).ok, true);
  const result = await f.turn(f.alice, 'notes_turn_0001', 'Read my notes'); assert.equal(result.ok, true, result.error?.code);
  assert.equal(result.accounting.length, 2); assert.equal(result.evidence.length, 1);
  assert.equal(result.evidence[0].data.notes.length, 1); assert.equal(result.evidence[0].data.notes[0].noteId, 'owned_note_01');
  assert.equal(JSON.stringify(f.calls).includes('Foreign private'), false);
  const denied = await f.turn(f.alice, 'foreign_note_try', 'Read note foreign_note_01'); assert.equal(denied.ok, false);
  assert.equal(JSON.stringify(denied).includes('Foreign private'), false);
});

test('actual Root HTTP rejects cross-account context/payload authority before model dispatch', async t => {
  const f = await fixture(t); const baseline = await f.turn(f.alice, 'context_good_001'); assert.equal(baseline.ok, true);
  const before = f.calls.length;
  assert.equal((await f.turn(f.bob, 'context_bad_0001', 'Neutral question', { contextId: baseline.contextId })).ok, false);
  assert.equal((await f.turn(f.alice, 'spoofed_actor_01', 'Neutral question', { actor: { accountId: f.b.accountId } })).ok, false);
  assert.equal(f.calls.length, before);
});

test('actual Root HTTP signed cancellation isolates actors and does not append a cancelled turn', async t => {
  const f = await fixture(t); const first = await f.turn(f.alice, 'context_initial'); assert.equal(first.ok, true);
  const barrier = f.hold(), running = f.turn(f.alice, 'cancel_held_001', 'Cancelled prompt', { contextId: first.contextId }); await barrier.entered.promise;
  const foreign = await f.call(f.bob, 'agent.cancel', { requestId: 'cancel_held_001' }); assert.equal(foreign.ok, false);
  const cancel = await f.call(f.alice, 'agent.cancel', { requestId: 'cancel_held_001' }); assert.equal(cancel.cancelRequested, true); assert.equal(cancel.remoteTerminationVerified, false);
  barrier.release.resolve(); assert.equal((await running).ok, false);
  const after = await f.turn(f.alice, 'after_cancel_001', 'Next question', { contextId: first.contextId }); assert.equal(after.ok, true);
  assert.equal(f.calls.at(-1).body.messages.some(item => item.content === 'Cancelled prompt'), false);
});

test('actual Root HTTP current device revoke at provider barrier prevents delivery and further tools', async t => {
  const f = await fixture(t), phone = await f.phone(); const first = await f.turn(f.alice, 'before_revoke_01'); assert.equal(first.ok, true);
  const barrier = f.hold(), running = f.turn(f.alice, 'revoke_held_001', 'Read my notes', { contextId: first.contextId }); await barrier.entered.promise;
  assert.equal((await f.call(phone, 'device.revoke', { deviceId: f.a.deviceId })).ok, true);
  barrier.release.resolve(); assert.equal((await running).ok, false);
  const before = f.calls.length; assert.equal((await f.turn(f.alice, 'revoked_next_01')).ok, false); assert.equal(f.calls.length, before);
  assert.equal((await f.turn(phone, 'phone_current_01', 'Next question', { contextId: first.contextId })).ok, true);
  assert.equal(f.calls.at(-1).body.messages.some(item => item.content === 'Read my notes'), false);
});

test('actual Root HTTP preserves unknown ACK and stable no-redispatch semantics', async t => {
  const f = await fixture(t); f.setLost(true);
  assert.equal((await f.turn(f.alice, 'unknown_turn_001')).ok, false); const before = f.calls.length;
  const retry = await f.turn(f.alice, 'unknown_turn_001'); assert.equal(retry.ok, false); assert.equal(retry.error.code, 'agent_reconcile_required'); assert.equal(f.calls.length, before);
  assert.equal(JSON.stringify(retry).includes('private_fixture_payload'), false);
});

test('actual registered Root Source query tool preserves independent Source denial and foreign app isolation', { timeout: 20000 }, async t => {
  const f = await fixture(t); await f.restart({ universalAppsEnabled: true });
  const source = createServer((_req, res) => res.end('Synthetic Source contract fixture'));
  await new Promise(done => source.listen(0, '127.0.0.1', done));
  const token = randomBytes(32).toString('base64url');
  const sourceIdentity = { linkId: 'agent_source_link_123456789012345', hostDeviceId: 'agent_source_host', connectorId: 'agent_source_connector', name: 'Own Source fixture host' };
  const registered = await fetch(f.origin + '/api/connectors/register', { method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
    body: JSON.stringify({ linkId: sourceIdentity.linkId, deviceId: sourceIdentity.hostDeviceId, connectorId: sourceIdentity.connectorId, scope: 'Dev', protocol: 2, capabilities: ['apps'] }) });
  assert.equal((await registered.json()).ok, true);
  const runtime = createLocalAppsRuntime({ randomSecret: () => randomBytes(32).toString('base64url'), digest: value => createHash('sha256').update(value).digest('hex'),
    createWebSocket: url => new globalThis.WebSocket(url), httpRequest: request,
    encodeBase64: bytes => Buffer.from(bytes).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64') }, { identity: sourceIdentity, token, serverUrl: f.origin });
  t.after(async () => { runtime.stop(); source.closeAllConnections(); await new Promise(done => source.close(done)); });
  runtime.start(); const connectionDeadline = Date.now() + 5000;
  while (!runtime.status().connected) { assert.ok(Date.now() < connectionDeadline); await new Promise(done => setTimeout(done, 10)); }
  const claim = await runtime.claim();
  assert.equal((await f.call(f.alice, 'apps.claim', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, claimCode: claim.claimCode })).ok, true);
  const registration = await f.call(f.alice, 'apps.register', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId,
    name: 'Own synthetic readonly source', port: source.address().port, entryPath: '/', grants: { accountIds: [], communityIds: [] } });
  assert.equal(registration.ok, true); const appId = registration.app.id, pin = registration.universalRegistration.descriptor.app.source;
  const deadline = Date.now() + 5000;
  while ((await f.call(f.alice, 'apps.list')).apps.find(item => item.id === appId)?.state !== 'ready') {
    assert.ok(Date.now() < deadline); await new Promise(done => setTimeout(done, 10));
  }
  let sourceAllowed = true, sourceReads = 0;
  // Synthetic Source current-authority port, not a claim of installed Planner/HIVE acceptance or Native SSO.
  // Root must honor its independent refusal even though this human owns the registered Root app.
  const adapter = createTrustedReadonlyQueryAdapter({ withAuthority(request, callback) {
    assert.equal(request.actor.accountId, f.a.accountId); if (!sourceAllowed) throw new Error('synthetic_native_source_permission_denied'); return callback();
  }, query(_request, _signal, current) { current(); sourceReads++; return { text: 'Private scoped Source result' }; } });
  const capabilityId = `app.${appId}:read`, resource = `app.${appId}:selected`;
  const catalog = { capabilityId, version: 1, appId, title: 'Selected Source read', description: 'Read one independently authorized Source resource',
    visibility: 'private', executionEnabled: true, inputSchema: { type: 'object', properties: {}, additionalProperties: false },
    outputSchema: { type: 'object', properties: { text: { type: 'string', maxLength: 1000 } }, required: ['text'], additionalProperties: false },
    resources: [resource], effects: [], recipients: [resource], executionBinding: { kind: 'registered', handler: capabilityId, version: 1,
      binding: { id: `app.${appId}:readonly`, version: 1, digest: canonicalHash({ source: 'synthetic', resource }) } } };
  const entry = { appId, target: { revision: pin.revision, digest: pin.digest }, catalog, adapter };
  const reference = captureExternalApplications([entry])[0].contract;
  await f.restart({ universalAppsEnabled: true, capabilityAudience: f.origin, externalApplications: [entry] });
  // Actual signed grant/credential consent, kept private in this synthetic fixture. The composition never creates one.
  const principal = (await f.call(f.alice, 'access.principals.create', { expectedAccountId: f.a.accountId, label: 'Explicit fixture reader' })).principal;
  const grant = (await f.call(f.alice, 'access.grants.issue', { expectedAccountId: f.a.accountId, principalId: principal.id,
    capabilities: [{ capabilityId, version: 1 }], resources: [resource], effects: [], recipients: [resource], expiresAt: Date.now() + 600000,
    allowDelegation: false, maxDepth: 0, budget: { unit: 'invocations', limit: 4 } })).grant;
  const issued = await f.call(f.alice, 'access.credentials.issue', { expectedAccountId: f.a.accountId, grantId: grant.id, audience: f.origin + '/mcp' });
  assert.equal(issued.ok, true);
  await f.restart({ universalAppsEnabled: true, capabilityAudience: f.origin, externalApplications: [entry], personalAgent: { ...f.configuration,
    sourceReferences: [reference], sourceCredentialFor: () => ({ token: issued.token, audience: f.origin + '/mcp' }) } });
  const baseline = await f.turn(f.alice, 'source_good_001', 'Read source'); assert.equal(baseline.ok, true, baseline.error?.code);
  assert.equal(sourceReads, 1); assert.equal(baseline.evidence[0].data.text, 'Private scoped Source result');
  const before = f.calls.length;
  assert.equal((await f.turn(f.bob, 'source_other_01', 'Read source')).ok, false); assert.equal(f.calls.length, before);
  sourceAllowed = false;
  assert.equal((await f.turn(f.alice, 'source_denied_1', 'Read source')).ok, false); assert.equal(f.calls.length, before); assert.equal(sourceReads, 1);
  sourceAllowed = true;
  const fresh = await f.turn(f.alice, 'source_fresh_01', 'Read source'); assert.equal(fresh.ok, true); assert.equal(sourceReads, 2);
  const barrier = f.hold(), running = f.turn(f.alice, 'source_revoke_1', 'Read source', { contextId: fresh.contextId }); await barrier.entered.promise;
  assert.equal((await f.call(f.alice, 'access.grants.revoke', { expectedAccountId: f.a.accountId, grantId: grant.id })).ok, true);
  barrier.release.resolve(); assert.equal((await running).ok, false); assert.equal(sourceReads, 2, 'current grant must be checked before any further Source read');
});

test('actual mounted close aborts a held turn before dependent stores close and prevents delivery', async t => {
  const f = await fixture(t); const good = await f.turn(f.alice, 'close_baseline_1'); assert.equal(good.ok, true);
  const barrier = f.hold(), pending = f.turn(f.alice, 'close_pending_01', 'Read my notes', { contextId: good.contextId }); await barrier.entered.promise;
  await f.service().locals.closeServices(); barrier.release.resolve();
  assert.equal((await pending).ok, false); assert.equal(f.service().locals.personalAgent.status().configured, false);
  assert.equal(f.calls.length, 2);
});

test('Root assembly rejects unsafe configuration before opening its data stores', async t => {
  const f = await fixture(t); const dataDir = join(f.directory, 'rejected-data'); let failure;
  try { createHttpApp(f.dist, { ...f.base, dataDir, personalAgent: { enabled: true, authority: 'private_config_sentinel' } }); } catch (error) { failure = error; }
  assert.ok(failure); await waitForRejectedHttpStart(failure); assert.equal(failure.message.includes('private_config_sentinel'), false);
  await assert.rejects(readFile(join(dataDir, 'world/world.sqlite')), { code: 'ENOENT' });
});

test('actual shared browser client dispatches signed UI cancel while the original model RPC is held', { timeout: 10000 }, async t => {
  const f = await fixture(t); let local = null;
  const storage = { async read() { return structuredClone(local); }, async claim(candidate) { local ??= structuredClone(candidate); return structuredClone(local); },
    async compareAndSwap(revision, candidate) { assert.equal(local.localRevision, revision); local = structuredClone(candidate); return structuredClone(local); } };
  const client = createClientWithStorage({ projectId: 'soty', endpoint: f.origin + '/api/connect/rpc',
    fetch: (url, options) => fetch(url, { ...options, headers: { ...options.headers, origin: f.origin } }) }, storage);
  t.after(() => client.dispose());
  const account = await client.bootstrap('Own shared browser fixture'), data = new Map();
  const ui = createPersonalAgentConversation({ accountId: account.accountId, client,
    storage: { getItem: key => data.get(key) ?? null, setItem: (key, value) => data.set(key, value) } });
  const baseline = await client.extension('agent.turn', { requestId: 'shared_baseline_001', prompt: 'Shared client baseline' }, { expectedAccountId: account.accountId });
  assert.equal(baseline.ok, true); assert.equal(baseline.status, 'complete');
  const barrier = f.hold(), turn = ui.turn('Held shared client turn'); await barrier.entered.promise;
  const cancellation = ui.cancel();
  try {
    const observed = await Promise.race([cancellation.then(value => ({ responded: true, value })), new Promise(done => setTimeout(() => done({ responded: false }), 400))]);
    assert.equal(observed.responded, true, 'shared-client cancel must reach the signed server handler before provider release');
    assert.equal(observed.value.cancelRequested, true); assert.equal(observed.value.remoteTerminationVerified, false);
  } finally { barrier.release.resolve(); await Promise.allSettled([turn, cancellation]); }
  assert.equal(ui.view().phase, 'unknown'); assert.equal(f.calls.length, 2);
});

test('actual shared UI accepts the genuine Connect ok envelope and retains its own confirmed context', async t => {
  const f = await fixture(t), browser = await sharedBrowser(t, f);
  const good = await browser.ui.turn('Actual UI baseline'); assert.equal(good.phase, 'complete'); assert.equal(good.unconfirmed, 0);
  assert.match(good.contextId, /^ctx_/); assert.equal(good.answer, 'Подтверждённый ответ');
  const continued = await browser.ui.turn('Actual UI continuation'); assert.equal(continued.contextId, good.contextId); assert.equal(f.calls.length, 2);
});

test('actual shared client profile switch while turn is held denies delivery and cannot cancel another account', async t => {
  const f = await fixture(t), first = await sharedBrowser(t, f), second = await sharedBrowser(t, f);
  const good = await first.ui.turn('Profile switch baseline'); assert.equal(good.phase, 'complete');
  const readNotes = browser => browser.client.extension('notes.list', { expectedAccountId: browser.account.accountId }, { expectedAccountId: browser.account.accountId });
  const ownNotes = await readNotes(first), ownNoteId = ownNotes.notes.find(note => note.title === 'Диалог с помощником').noteId;
  const readOwnNote = () => first.client.extension('notes.get', { expectedAccountId: first.account.accountId, noteId: ownNoteId }, { expectedAccountId: first.account.accountId });
  const before = (await readOwnNote()).note, foreignBefore = await readNotes(second);
  assert.equal(JSON.parse(before.body).history.length, 2); first.mergeProfile(second);
  const barrier = f.hold(), turn = first.ui.turn('Do not deliver to another profile'); await barrier.entered.promise;
  assert.equal(first.ui.view().answer, undefined); assert.deepEqual(first.ui.view().sources, []);
  await first.client.switchProfile(second.account.accountId);
  assert.equal((await first.client.getLocalState()).accountId, second.account.accountId);
  await assert.rejects(first.client.extension('agent.cancel', { requestId: first.ui.view().requestId }, { expectedAccountId: first.account.accountId }), { code: 'ACTIVE_PROFILE_CHANGED' });
  await assert.rejects(first.ui.cancel(), { code: 'authentication_required' });
  barrier.release.resolve(); await assert.rejects(turn, { code: 'ACTIVE_PROFILE_CHANGED' });
  assert.equal(first.ui.view().phase, 'unknown'); assert.equal(first.ui.view().unconfirmed, 1);
  assert.equal(first.ui.view().answer, undefined); assert.deepEqual(first.ui.view().sources, []);
  assert.equal(second.ui.view().answer, undefined); assert.deepEqual(second.ui.view().sources, []); assert.equal(second.ui.view().unconfirmed, 0);
  assert.deepEqual(await readNotes(second), foreignBefore, 'the foreign account receives no new Note/history');
  await assert.rejects(second.client.extension('notes.get', { expectedAccountId: second.account.accountId, noteId: ownNoteId },
    { expectedAccountId: second.account.accountId }), { code: 'notes_note_not_found' });
  // Only restore the fixture's old signing profile for a durable owner read;
  // do not deliver/retry a turn or restore an answer in the foreign UI.
  await first.client.switchProfile(first.account.accountId);
  assert.equal((await first.client.getLocalState()).accountId, first.account.accountId);
  const after = (await readOwnNote()).note;
  assert.equal(after.noteId, before.noteId);
  const confirmed = JSON.parse(before.body).history, current = JSON.parse(after.body).history;
  assert.equal(JSON.stringify(current.slice(0, confirmed.length)), JSON.stringify(confirmed), 'prior own confirmed entries remain byte-equal');
  assert.ok(current.length === confirmed.length || current.length === confirmed.length + 2, 'unknown UI ACK allows at most one whole same-owner pair');
  assert.equal(after.revision, before.revision + (current.length === confirmed.length ? 0 : 1));
  if (current.length > confirmed.length) {
    assert.deepEqual(current.slice(confirmed.length), [
      JSON.stringify({ role: 'user', content: 'Do not deliver to another profile' }),
      JSON.stringify({ role: 'assistant', content: 'Подтверждённый ответ' }),
    ]);
  }
  assert.equal(first.ui.view().unconfirmed, 1); assert.equal(first.ui.view().answer, undefined); assert.deepEqual(first.ui.view().sources, []);
  assert.equal(f.calls.length, 2, 'no foreign cancellation/retry or second model dispatch');
});

test('actual shared client dispose while held denies late delivery without declaring remote rollback', async t => {
  const f = await fixture(t), browser = await sharedBrowser(t, f);
  assert.equal((await browser.ui.turn('Dispose baseline')).phase, 'complete');
  const barrier = f.hold(), turn = browser.ui.turn('Late response'); await barrier.entered.promise;
  browser.client.dispose(); browser.ui.dispose(); barrier.release.resolve(); await assert.rejects(turn, { code: 'CLIENT_DISPOSED' });
  assert.equal(browser.ui.view().unconfirmed, 1); assert.equal(f.calls.length, 2);
});

test('Notes-backed history survives actual Root restart, preserves two owners and blocks confirmed redispatch', async t => {
  const f = await fixture(t), first = await f.turn(f.alice, 'history_first_001', 'Remember my own first question'); assert.equal(first.ok, true, first.error?.code);
  const a = await historyNote(f, f.alice, f.a.accountId), stored = JSON.parse(a.body); assert.equal(stored.history.length, 2); assert.equal(a.revision, 1);
  const before = f.calls.length; assert.equal((await f.turn(f.bob, 'history_foreign_1', 'Question', { contextId: first.contextId })).ok, false);
  assert.equal(f.calls.length, before);
  const other = await f.turn(f.bob, 'history_bob_0001', 'Other private conversation'); assert.equal(other.ok, true);
  assert.equal((await historyNote(f, f.bob, f.b.accountId)).noteId === a.noteId, false);
  await f.restart(); const next = await f.turn(f.alice, 'history_second_1', 'Continue my own question', { contextId: first.contextId }); assert.equal(next.ok, true, next.error?.code);
  assert.equal(f.calls.at(-1).body.messages.some(item => item.content === 'Remember my own first question'), true);
  assert.equal(f.calls.at(-1).body.messages.some(item => item.content === 'Other private conversation'), false);
  const current = await historyNote(f, f.alice, f.a.accountId); assert.equal(current.noteId, a.noteId); assert.equal(current.revision, 2);
  assert.equal(JSON.parse(current.body).history.length, 4);
  const count = f.calls.length; assert.equal((await f.turn(f.alice, 'history_first_001', 'Remember my own first question')).error.code, 'agent_reconcile_required');
  assert.equal(f.calls.length, count); assert.equal((await historyNote(f, f.alice, f.a.accountId)).revision, 2);
});

test('current Notes CAS preserves a legitimate owner edit during a held model result', async t => {
  const f = await fixture(t), first = await f.turn(f.alice, 'memory_edit_first'); assert.equal(first.ok, true);
  const note = await historyNote(f, f.alice, f.a.accountId), barrier = f.hold();
  const pending = f.turn(f.alice, 'memory_edit_held1', 'A stale memory append', { contextId: first.contextId }); await barrier.entered.promise;
  const edit = await f.call(f.alice, 'notes.put', { expectedAccountId: f.a.accountId, noteId: note.noteId, mutationId: 'manual_memory_edit',
    expectedRevision: note.revision, title: 'Диалог с помощником', body: note.body, items: [], color: 'blue', pinned: true, state: 'active' }); assert.equal(edit.ok, true);
  barrier.release.resolve(); const denied = await pending; assert.equal(denied.error.code, 'agent_memory_conflict');
  const current = await historyNote(f, f.alice, f.a.accountId); assert.equal(current.revision, edit.revision); assert.equal(current.body, note.body);
  assert.equal(current.color, 'blue'); assert.equal(current.pinned, true);
});

test('actual signed cancel and device revoke never persist either half of an unconfirmed Notes pair', async t => {
  const f = await fixture(t), first = await f.turn(f.alice, 'memory_current_1'); assert.equal(first.ok, true);
  const note = await historyNote(f, f.alice, f.a.accountId), cancelled = f.hold();
  const one = f.turn(f.alice, 'memory_cancel_01', 'Must not be recorded', { contextId: first.contextId }); await cancelled.entered.promise;
  assert.equal((await f.call(f.alice, 'agent.cancel', { requestId: 'memory_cancel_01' })).cancelRequested, true);
  cancelled.release.resolve(); assert.equal((await one).ok, false); assert.equal((await historyNote(f, f.alice, f.a.accountId)).body, note.body);
  const phone = await f.phone(), revoked = f.hold(); const two = f.turn(f.alice, 'memory_revoke_01', 'Revoked append', { contextId: first.contextId }); await revoked.entered.promise;
  assert.equal((await f.call(phone, 'device.revoke', { deviceId: f.a.deviceId })).ok, true); revoked.release.resolve(); assert.equal((await two).ok, false);
  const after = await historyNote(f, phone, f.a.accountId); assert.equal(after.revision, note.revision); assert.equal(after.body, note.body);
});

test('one approved local task uses current Apps ownership and the existing durable job receipt', async t => {
  const f = await fixture(t), source = await ownedAgent(t, f);
  await f.restart({ gonka: { ...f.base.gonka, apiKey: 'synthetic-only-never-used-local-key' },
    personalAgent: { ...f.configuration, localTargetFor: () => source.target } });
  const denied = await f.turn(f.bob, 'local_foreign_01', 'Local task Inspect my own project'); assert.equal(denied.ok, false); assert.equal(f.calls.length, 0);
  const result = await f.turn(f.alice, 'local_owned_0001', 'Local task Inspect my own project'); assert.equal(result.ok, true, result.error?.code);
  const data = result.evidence[0].data; assert.equal(data.status, 'queued'); assert.equal(data.completionVerified, false);
  const replay = await f.call(f.alice, 'apps.assistant.send', { ...source.target, expectedAccountId: f.a.accountId, requestId: data.requestId,
    conversationId: data.requestId, text: 'Inspect my own project' }); assert.equal(replay.ok, true); assert.equal(replay.job.id, data.jobId);
  assert.equal((await f.call(f.bob, 'apps.agent.read', { ...source.target, expectedAccountId: f.b.accountId, jobId: data.jobId })).ok, false);
  source.runtime.stop(); await f.restart({ personalAgent: { ...f.configuration, localTargetFor: () => source.target } });
  const current = await f.call(f.alice, 'apps.agent.read', { hostDeviceId: source.target.hostDeviceId, connectorId: source.target.connectorId, jobId: data.jobId });
  assert.equal(current.ok, true); assert.equal(current.job.id, data.jobId); assert.equal(current.job.status, 'queued');
});

test('signed cancel while the actual connector write queue is held prevents a local task effect', async t => {
  const f = await fixture(t), source = await ownedAgent(t, f);
  await f.restart({ gonka: { ...f.base.gonka, apiKey: 'synthetic-only-never-used-local-key' }, personalAgent: { ...f.configuration, localTargetFor: () => source.target } });
  const store = f.service().locals.connectorService.store, barrier = defer(), entered = defer(); assert.ok(store); store.writeQueue = barrier.promise;
  t.after(() => barrier.resolve()); const lookup = store.getOwnedRequest.bind(store);
  store.getOwnedRequest = (...args) => { entered.resolve(); return lookup(...args); };
  const running = f.turn(f.alice, 'local_cancel_01', 'Local task Must not dispatch');
  try {
    await Promise.race([entered.promise, running.then(() => { throw Error('local_write_barrier_not_entered'); })]); assert.equal(store.state.jobs.length, 0);
    assert.equal((await f.call(f.alice, 'agent.cancel', { requestId: 'local_cancel_01' })).cancelRequested, true);
    barrier.resolve(); assert.equal((await running).ok, false); assert.equal(store.state.jobs.length, 0);
  } finally { barrier.resolve(); }
});

test('current Connect device revoke at the actual local write barrier admits no second job', async t => {
  const f = await fixture(t), source = await ownedAgent(t, f);
  await f.restart({ gonka: { ...f.base.gonka, apiKey: 'synthetic-only-never-used-local-key' }, personalAgent: { ...f.configuration, localTargetFor: () => source.target } });
  const baseline = await f.turn(f.alice, 'local_revoke_pos', 'Local task Current owner baseline'); assert.equal(baseline.ok, true, baseline.error?.code);
  const store = f.service().locals.connectorService.store, phone = await f.phone(), barrier = defer(), entered = defer();
  assert.equal(store.state.jobs.length, 1); store.writeQueue = barrier.promise; t.after(() => barrier.resolve());
  const lookup = store.getOwnedRequest.bind(store); store.getOwnedRequest = (...args) => { entered.resolve(); return lookup(...args); };
  const running = f.turn(f.alice, 'local_revoke_mid', 'Local task Revoked actor must not dispatch');
  try {
    await Promise.race([entered.promise, running.then(() => { throw Error('local_write_barrier_not_entered'); })]);
    assert.equal((await f.call(phone, 'device.revoke', { deviceId: f.a.deviceId })).ok, true); barrier.resolve();
    assert.equal((await running).ok, false); assert.equal(store.state.jobs.length, 1);
  } finally { barrier.resolve(); }
});

for (const [kind, unit] of [['plain', 'a'], ['escaped', '\\']]) {
  test(`accepted8000 ${kind} signed turn persists exact content and restores it after restart`, async t => {
    const f = await fixture(t), baseline = await f.turn(f.alice, `boundary_${kind}_base`, 'Valid current baseline');
    assert.equal(baseline.ok, true, baseline.error?.code);
    const before = await historyNote(f, f.alice, f.a.accountId), prompt = unit.repeat(8000), dispatches = f.calls.length, receipts = f.settledReceipts();
    const result = await f.turn(f.alice, `boundary_${kind}_max`, prompt, { contextId: baseline.contextId });
    const note = await historyNote(f, f.alice, f.a.accountId);
    // Closed causal facts: genuine module accepted/dispatched/accounted the request, then actual Notes was observed.
    console.info(JSON.stringify({ boundary: kind, acceptedContentLength: prompt.length, modelDispatches: f.calls.length - dispatches,
      completedReceiptReads: f.settledReceipts() - receipts, replyOk: result.ok, replyCode: result.error?.code ?? null,
      actualNotesRevisionDelta: note.revision - before.revision, wholeBodyBelow96KiB: Buffer.byteLength(note.body) < 96 * 1024 }));
    assert.equal(f.calls.length, dispatches + 1); assert.equal(f.settledReceipts(), receipts + 1);
    assert.equal(result.ok, true, 'accepted content must receive ACK after one real Notes pair commit');
    assert.equal(note.revision, before.revision + 1);
    const record = JSON.parse(note.body); assert.equal(record.history.length, 4);
    assert.equal(JSON.parse(record.history[2]).content, prompt);
    assert.equal(record.history[2], JSON.stringify({ role: 'user', content: prompt }));
    const immediate = await f.turn(f.alice, `boundary_${kind}_live`, 'Read the committed snapshot', { contextId: result.contextId });
    assert.equal(immediate.ok, true, immediate.error?.code);
    const afterCommit = JSON.parse((await historyNote(f, f.alice, f.a.accountId)).body).history.map(JSON.parse);
    assert.deepEqual(f.calls.at(-1).body.messages.slice(1, -1), afterCommit.slice(0, -2));
    await f.restart(); const restored = await f.turn(f.alice, `boundary_${kind}_next`, 'Continue exact stored content', { contextId: result.contextId });
    assert.equal(restored.ok, true, restored.error?.code);
    assert.deepEqual(f.calls.at(-1).body.messages.slice(1, -1), afterCommit);
    assert.equal(f.calls.at(-1).body.messages.find(item => item.role === 'user' && item.content.length === 8000)?.content, prompt);
    assert.equal(JSON.parse((await historyNote(f, f.alice, f.a.accountId)).body).history[2], record.history[2]);
  });
}

test('stored content8001 is rejected before dispatch although its JSON envelope fits the finite body', async t => {
  const f = await fixture(t), baseline = await f.turn(f.alice, 'stored_content_base'); assert.equal(baseline.ok, true);
  const note = await historyNote(f, f.alice, f.a.accountId), record = JSON.parse(note.body);
  record.history[0] = JSON.stringify({ role: 'user', content: 'a'.repeat(8001) }); const body = JSON.stringify(record);
  assert.ok(Buffer.byteLength(body) < 96 * 1024);
  const modified = await f.call(f.alice, 'notes.put', { expectedAccountId: f.a.accountId, noteId: note.noteId, mutationId: 'stored_content_over',
    expectedRevision: note.revision, title: note.title, body, items: [], color: note.color, pinned: note.pinned, state: note.state }); assert.equal(modified.ok, true);
  const count = f.calls.length, denied = await f.turn(f.alice, 'stored_content_next', 'Must not dispatch', { contextId: baseline.contextId });
  assert.equal(denied.ok, false); assert.equal(denied.error.code, 'agent_memory_unavailable'); assert.equal(f.calls.length, count);
  assert.equal((await historyNote(f, f.alice, f.a.accountId)).revision, modified.revision);
});
