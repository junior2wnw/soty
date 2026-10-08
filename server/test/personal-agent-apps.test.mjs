import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { createHash, generateKeyPairSync, randomBytes, randomUUID, sign } from 'node:crypto';
import { registerHooks } from 'node:module';
import { createConnectService, digestArgs } from '../../modules/connect/server/index.mjs';
import { createNotesService } from '../../modules/notes/server/index.mjs';
import { createPersonalAgentComposition } from '../personal-agent-composition.js';
import { GLM_MODEL } from '../../modules/personal-agent/provider/index.mjs';
import { CHANNEL_SCHEMA } from '../../modules/apps/server/protocol.mjs';

// Only the WebSocket IO constructor is an in-memory port. Apps schema,
// connector admission, claim, registration, grants and list are actual code.
class MemoryWss {
  clients = new Set();
  handleUpgrade(_request, socket, _head, ready) { this.clients.add(socket); ready(socket); }
  close() { this.clients.clear(); }
}
const appsUrl = new URL('../../modules/apps/server/index.mjs', import.meta.url).href;
const wsSymbol = Symbol.for('soty.personal-agent-apps.test.ws'); globalThis[wsSymbol] = MemoryWss;
const hooks = registerHooks({ resolve(specifier, context, next) {
  return specifier === 'ws' && context.parentURL === appsUrl
    ? { url: `data:text/javascript,${encodeURIComponent("export const WebSocketServer = globalThis[Symbol.for('soty.personal-agent-apps.test.ws')];")}`, shortCircuit: true }
    : next(specifier, context);
} });
let createAppsService;
try { ({ createAppsService } = await import(appsUrl)); } finally { hooks.deregister(); delete globalThis[wsSymbol]; }

const ORIGIN = 'https://apps-component.test';
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
const tick = () => new Promise(done => setImmediate(done));
const entered = (barrier, running) => Promise.race([barrier.entered.promise, running.then(result => {
  throw new Error(`fixture_turn_completed_before_provider:${result.error?.code ?? 'complete'}`);
})]);
const good = value => { assert.equal(value.ok, true, value.error?.code); return value; };
function identity() {
  const pair = generateKeyPairSync('ec', { namedCurve: 'prime256v1' }), encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { key: pair.privateKey, publicJwk: pair.publicKey.export({ format: 'jwk' }), encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }) };
}
class MemorySocket extends EventEmitter {
  readyState = 1; messages = [];
  send(value) { this.messages.push(JSON.parse(value)); }
  input(value) { this.emit('message', Buffer.from(JSON.stringify(value)), false); }
  terminate() { if (this.readyState === 3) return; this.readyState = 3; this.emit('close'); }
  destroy() { this.terminate(); }
  end() { this.terminate(); }
  ping() { this.emit('pong'); }
}

async function fixture(t, { enabled = true, sourceReferences = [], sourceCredentialFor } = {}) {
  let connect, personal, rawApps, selectedInput = {}, selectedTool = 'list_my_apps', hold = null;
  const providerCalls = [], appReads = [], receipts = new Map(), rowsSeen = [], auth = new Map();
  const notes = createNotesService({ databasePath: ':memory:', projectId: 'soty' });
  rawApps = createAppsService({ databasePath: ':memory:', shellOrigins: [ORIGIN], appOriginTemplate: 'https://{appId}.component-app.test',
    actorActive: actor => connect.isActorActive(actor), authenticateConnector: value => auth.get(value.token) === `${value.linkId}:${value.deviceId}:${value.connectorId}` });
  const apps = { execute(request) { assert.equal(request.op, 'apps.list', 'the agent Apps port is read-only');
    appReads.push({ accountId: request.actor.accountId, expectedAccountId: request.args.expectedAccountId });
    const result = rawApps.execute(request); rowsSeen.push(result.apps); return result; } };
  const tenant = 'component_fixture_tenant', keyRef = Object.freeze({});
  const configuration = { enabled, sourceReferences, ...(sourceCredentialFor ? { sourceCredentialFor } : {}), gateway: {
    gatewayOrigin: 'https://component-commerce.invalid', billingTenant: tenant, keyRef,
    authorizeActor: ({ actor }) => connect.isActorActive(actor), resolveCredential: async ref => ({ keyRef: ref, tenantId: tenant, bearer: 'synthetic-component-bearer-with-no-provider' }),
    readReceipt: async ({ requestId, billingTenant, keyRef: ref }) => { assert.equal(billingTenant, tenant); assert.equal(ref, keyRef); return { tenantId: tenant, request: receipts.get(requestId) }; },
    fetchImpl: async (_url, init) => {
      const body = JSON.parse(init.body); assert.equal(body.model, GLM_MODEL); assert.equal(body.autoFallback, false);
      providerCalls.push({ tools: body.tools.map(tool => tool.function.name), lastRole: body.messages.at(-1).role });
      if (hold) { const current = hold; hold = null; current.entered.resolve(); await current.release.promise; }
      const last = body.messages.at(-1); let name = selectedTool, args = selectedInput;
      if (last.role === 'tool') { const evidence = JSON.parse(last.content).evidence; name = 'finish_answer'; args = {
        answer: 'Текущая разрешённая проекция проверена.', selectedEvidenceIds: evidence.map(item => item.id), claims: [] }; }
      const id = randomUUID(); receipts.set(id, { id, status: 'completed', model_requested: GLM_MODEL, model_resolved: GLM_MODEL,
        input_tokens: 3, output_tokens: 2, charge_urub: '0' });
      const packet = `data: ${JSON.stringify({ id: 'fixture_completion', model: GLM_MODEL, choices: [{ index: 0, delta: {
        tool_calls: [{ index: 0, id: 'fixture_tool_call', type: 'function', function: { name, arguments: JSON.stringify(args) } }] }, finish_reason: null }] })}\n\n`
        + `data: ${JSON.stringify({ id: 'fixture_completion', model: GLM_MODEL, choices: [{ index: 0, delta: {}, finish_reason: 'tool_calls' }],
          usage: { prompt_tokens: 3, completion_tokens: 2, total_tokens: 5 } })}\n\n` + 'data: [DONE]\n\n';
      return new Response(packet, { status: 200, headers: { 'content-type': 'text/event-stream', 'x-soty-request-id': id } });
    }
  } };
  personal = createPersonalAgentComposition({ getConnect: () => connect, getServices: () => ({ notes, apps, capabilities: {} }), configuration });
  connect = createConnectService({ databasePath: ':memory:', projectId: 'soty', allowedOrigins: [ORIGIN], extensions: [notes, rawApps, personal] });
  t.after(() => { hold?.release.resolve(); personal.close(); rawApps.close(); notes.close(); connect.close(); });
  async function rpc(actor, op, args = {}) {
    const challenge = good(await connect.handle({ origin: ORIGIN, op: 'challenge', args: { operation: op, digest: digestArgs(args) } }));
    return connect.handle({ origin: ORIGIN, op, args, proof: { challengeId: challenge.challengeId, publicJwk: actor.publicJwk,
      signature: sign('sha256', Buffer.from(challenge.message), { key: actor.key, dsaEncoding: 'ieee-p1363' }).toString('base64url') } });
  }
  const alice = identity(), bob = identity();
  for (const actor of [alice, bob]) actor.account = good(await rpc(actor, 'bootstrap', { label: 'Synthetic component owner', encryptionPublicJwk: actor.encryptionPublicJwk }));
  let hostIndex = 0; const registeredPorts = new Map();
  async function ownedHost(actor) {
    const index = ++hostIndex, host = { linkId: `component_link_${index}`, hostDeviceId: `component_host_${index}`, connectorId: `component_connector_${index}` },
      token = randomBytes(32).toString('base64url'), claimCode = randomBytes(32).toString('base64url'), socket = new MemorySocket();
    auth.set(token, `${host.linkId}:${host.hostDeviceId}:${host.connectorId}`);
    assert.equal(rawApps.handleUpgrade({ url: '/api/apps/channel', headers: { host: new URL(ORIGIN).host } }, socket, Buffer.alloc(0)), true);
    socket.input({ type: 'auth', schema: CHANNEL_SCHEMA, ...host, token }); await tick();
    assert.equal(socket.readyState, 1); assert.equal(socket.messages.some(message => message.type === 'ready'), true);
    socket.input({ type: 'claim', claimDigest: createHash('sha256').update(claimCode).digest('hex') }); await tick();
    good(await rpc(actor, 'apps.claim', { hostDeviceId: host.hostDeviceId, connectorId: host.connectorId, claimCode }));
    return { ...host, socket };
  }
  async function register(actor, host, name, grants = { accountIds: [], communityIds: [] }) {
    const port = registeredPorts.get(host.hostDeviceId) ?? 8080; registeredPorts.set(host.hostDeviceId, port + 1);
    return good(await rpc(actor, 'apps.register', { hostDeviceId: host.hostDeviceId, connectorId: host.connectorId,
      name, port, entryPath: '/', grants })).app;
  }
  return { alice, bob, rpc, register, ownedHost, rawApps, appReads, providerCalls, rowsSeen, notes, personal,
    input(value, tool = 'list_my_apps') { selectedInput = value; selectedTool = tool; },
    turn(actor, requestId = 'component_apps_turn_one') { return rpc(actor, 'agent.turn', { requestId, prompt: 'Current account applications' }); },
    holdNextProvider() { const barrier = { entered: deferred(), release: deferred() }; hold = barrier; return barrier; } };
}

const appData = result => result.evidence.find(item => item.title === 'Мои приложения').data;
test('current signed account receives only its Apps projection through an accounted finish', async t => {
  const f = await fixture(t), a = await f.ownedHost(f.alice), b = await f.ownedHost(f.bob);
  const own = await f.register(f.alice, a, 'Own application'), foreign = await f.register(f.bob, b, 'Other private application');
  const result = await f.turn(f.alice);
  console.error(JSON.stringify({ componentPhase: 'first_turn_result', ok: result.ok, code: result.error?.code ?? null,
    providerCallbackCount: f.providerCalls.length, appsReadCount: f.appReads.length,
    toolOffered: f.providerCalls.some(call => call.tools.includes('list_my_apps')) }));
  assert.equal(result.ok, true, 'the existing personal agent must offer and execute current Apps discovery');
  const data = appData(result); assert.deepEqual(data.apps, [{ appId: own.id, name: own.name, status: own.state }]);
  assert.equal(data.hasMore, false); assert.equal(data.truncated, false);
  assert.equal(data.apps.some(app => app.appId === foreign.id), false);
  assert.deepEqual(f.appReads, [{ accountId: f.alice.account.accountId, expectedAccountId: f.alice.account.accountId }]);
  assert.equal(f.rowsSeen[0].some(row => Object.hasOwn(row, 'connectorId') && Object.hasOwn(row, 'grants')), true);
  assert.deepEqual(Object.keys(data.apps[0]).sort(), ['appId', 'name', 'status']);
  assert.equal(result.accounting.length, 2); assert.equal(f.providerCalls.length, 2);
  assert.equal(f.providerCalls.every(call => call.tools.includes('finish_answer')), true);
});
test('actual Apps account grant is observed, removal hides it, and own revoked status remains truthful', async t => {
  const f = await fixture(t), a = await f.ownedHost(f.alice), b = await f.ownedHost(f.bob);
  const own = await f.register(f.alice, a, 'Own revoked application'), shared = await f.register(f.bob, b, 'Shared application', {
    accountIds: [f.alice.account.accountId], communityIds: [] });
  good(await f.rpc(f.alice, 'apps.revoke', { appId: own.id }));
  const first = good(await f.turn(f.alice)); assert.equal(appData(first).apps.some(row => row.appId === shared.id), true);
  assert.equal(appData(first).apps.find(row => row.appId === own.id).status, 'revoked');
  good(await f.rpc(f.bob, 'apps.update', { appId: shared.id, grants: { accountIds: [], communityIds: [] } }));
  const next = good(await f.turn(f.alice, 'component_apps_turn_after_grant')); assert.equal(appData(next).apps.some(row => row.appId === shared.id), false);
  const other = good(await f.turn(f.bob, 'component_apps_turn_bob')); assert.equal(appData(other).apps.some(row => row.appId === own.id), false);
});
test('query and limit return bounded current metadata with truthful remainder and no private fields', async t => {
  const f = await fixture(t), host = await f.ownedHost(f.alice);
  for (let n = 0; n < 23; n++) await f.register(f.alice, host, `Selected application ${n}`);
  await f.register(f.alice, host, 'Different application');
  f.input({ query: 'SELECTED', limit: 20 }); const result = good(await f.turn(f.alice)); const data = appData(result);
  assert.equal(data.apps.length, 20); assert.equal(data.hasMore, true); assert.equal(data.truncated, true);
  assert.equal(data.apps.every(row => row.name.toLowerCase().includes('selected')), true);
  assert.equal(data.apps.every(row => Object.keys(row).sort().join(',') === 'appId,name,status'), true);
  f.input({ query: 'does not match', limit: 1 }); const empty = appData(good(await f.turn(f.alice, 'component_apps_empty')));
  assert.deepEqual(empty.apps, []); assert.equal(empty.hasMore, false); assert.equal(empty.truncated, false);
});
test('foreign account selector, excessive query, and excessive limits fail before Apps effect', async t => {
  const f = await fixture(t); let count = 0;
  for (const input of [{ accountId: f.bob.account.accountId }, { query: 'a'.repeat(121) }, { limit: 0 }, { limit: 21 }]) {
    f.input(input); const result = await f.turn(f.alice, `component_apps_invalid_${++count}`);
    assert.equal(result.ok, false); assert.equal(f.appReads.length, 0);
  }
});
test('current actor revocation during pending model callback prevents Apps read and final answer', async t => {
  const f = await fixture(t), held = f.holdNextProvider(), running = f.turn(f.alice);
  await entered(held, running);
  const second = identity(), started = good(await f.rpc(second, 'enrollment.start', { label: 'Own second component device', encryptionPublicJwk: second.encryptionPublicJwk }));
  good(await f.rpc(f.alice, 'enrollment.approve', { requestId: started.requestId, wrappedKey: { schema: 'synthetic.wrapped', ciphertext: 'opaque' } }));
  good(await f.rpc(second, 'enrollment.finish', { requestId: started.requestId, expectedAccountId: f.alice.account.accountId }));
  good(await f.rpc(f.alice, 'device.revoke', { deviceId: f.alice.account.deviceId }));
  held.release.resolve(); const result = await running;
  assert.equal(result.ok, false); assert.equal(f.appReads.length, 0); assert.equal(Object.hasOwn(result, 'answer'), false);
});
test('signed cancellation before tool dispatch prevents Apps read without a retry', async t => {
  const f = await fixture(t), held = f.holdNextProvider(), id = 'component_apps_cancelled', running = f.turn(f.alice, id);
  await entered(held, running); const cancel = good(await f.rpc(f.alice, 'agent.cancel', { requestId: id })); assert.equal(cancel.cancelRequested, true);
  held.release.resolve(); const result = await running;
  assert.equal(result.ok, false); assert.equal(f.appReads.length, 0); assert.equal(f.providerCalls.length, 1);
});
test('existing Notes and labelled history remain account-owned alongside the new read tool', async t => {
  const f = await fixture(t); f.input({}, 'list_my_notes');
  const result = good(await f.turn(f.alice)); assert.equal(f.appReads.length, 0); assert.equal(result.accounting.length, 2);
  const mine = good(await f.rpc(f.alice, 'notes.list', { expectedAccountId: f.alice.account.accountId })),
    other = good(await f.rpc(f.bob, 'notes.list', { expectedAccountId: f.bob.account.accountId }));
  assert.ok(mine.notes.length >= 1); assert.equal(other.notes.length, 0); assert.equal(f.personal.status().memory, 'owner_notes_labelled_history');
});
test('default disabled and missing Source credential preserve admission without Apps access', async t => {
  const disabled = await fixture(t, { enabled: false }); assert.equal(disabled.personal.status().configured, false);
  assert.equal((await disabled.turn(disabled.alice)).ok, false); assert.equal(disabled.appReads.length, 0); assert.equal(disabled.providerCalls.length, 0);
  const source = await fixture(t, { sourceReferences: [{ capabilityId: 'fixture.current.source', version: 1, digest: 'a'.repeat(64) }] });
  assert.equal(source.personal.status().configured, false); assert.equal((await source.turn(source.alice)).ok, false);
  assert.equal(source.appReads.length, 0); assert.equal(source.providerCalls.length, 0);
});
