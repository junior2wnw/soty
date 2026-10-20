import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, randomUUID, sign } from 'node:crypto';
import { EventEmitter } from 'node:events';
import { join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = fileURLToPath(new URL('../../', import.meta.url));
const load = name => import(pathToFileURL(join(root, name)).href);
const [{ createConnectService, digestArgs }, { createNotesService }, { createPersonalAgentComposition },
  { createOpenAiOrbTransport }, { createVoiceOrbComposition }, { GLM_MODEL }, { createVoiceOrbQuota }, { createClientWithStorage }] = await Promise.all([
  load('modules/connect/server/index.mjs'), load('modules/notes/server/index.mjs'), load('server/personal-agent-composition.js'),
  load('server/voice-orb-transport.js'), load('server/voice-orb-composition.js'), load('modules/personal-agent/provider/index.mjs'),
  load('server/voice-orb-quota.js'), load('modules/connect/browser/client.mjs'),
]);
const ORIGIN = 'https://general-voice-component.invalid';
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
const good = value => { assert.equal(value.ok, true, value.error?.code); return value; };
const flush = () => new Promise(resolve => setImmediate(resolve));
const entered = (gate, pending) => Promise.race([gate.entered.promise, pending.then(value => { throw new Error('completed_before_model:' + (value.error?.code ?? 'complete')); })]);
const identity = () => { const pair = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { key: pair.privateKey, publicJwk: pair.publicKey.export({ format: 'jwk' }), encryptionPublicJwk: pair.publicKey.export({ format: 'jwk' }) }; };
class SyntheticSocket extends EventEmitter {
  readyState = 0; sent = []; terminations = 0;
  constructor(sessionId) { super(); this.sessionId = sessionId; queueMicrotask(() => { this.readyState = 1; this.emit('open'); }); }
  event(value) { this.emit('message', Buffer.from(JSON.stringify(value))); }
  send(raw) { const event = JSON.parse(raw); this.sent.push(event);
    if (event.type === 'session.update') queueMicrotask(() => this.event({ type: 'session.updated', session: { ...event.session, id: this.sessionId } }));
    if (event.type === 'conversation.item.create') queueMicrotask(() => this.event({ type: 'conversation.item.done', item: event.item }));
  }
  terminate() { if (this.readyState === 3) return; this.terminations++; this.readyState = 3; this.emit('close'); }
}
async function fixture(t, options = {}) {
  let connect, voice, personal, realQuota, modelHold = null, transportHold = null;
  const issuedActors = new Map(), browserClients = [];
  const modelCalls = [], puts = [], receipts = new Map(), http = [], sockets = [], admissions = [], outcomes = [], holds = [], durableClaims = new Map();
  const actualNotes = createNotesService({ databasePath: ':memory:', projectId: 'soty' });
  const notes = { execute(request) { if (request.op === 'notes.put') puts.push(structuredClone(request));
    const result = actualNotes.execute(request);
    return options.corruptReadback && request.op === 'notes.get' && request.args.noteId.startsWith('agent_note_')
      ? { note: { ...result.note, title: 'Concurrent change' } } : result;
  } };
  const keyRef = Object.freeze({}), tenant = 'synthetic_general_voice_billing';
  personal = createPersonalAgentComposition({ getConnect: () => connect, getServices: () => ({ notes, capabilities: {} }),
    configuration: { enabled: true, historyEnabled: false, gateway: {
      gatewayOrigin: 'https://synthetic-voice-glm.invalid', billingTenant: tenant, keyRef,
      authorizeActor: ({ actor }) => connect.isActorActive(actor),
      resolveCredential: async ref => ({ keyRef: ref, tenantId: tenant, bearer: 'synthetic-glm-no-real-provider-key' }),
      readReceipt: async ({ requestId }) => ({ tenantId: tenant, request: receipts.get(requestId) }),
      fetchImpl: async (_url, init) => {
        const body = JSON.parse(init.body); modelCalls.push(body); assert.equal(body.model, GLM_MODEL); assert.equal(body.autoFallback, false);
        if (modelHold) { const gate = modelHold; modelHold = null; gate.entered.resolve(); await gate.release.promise; }
        const last = body.messages.at(-1), name = last.role === 'tool' ? 'finish_answer' : 'create_my_note';
        const args = name === 'finish_answer' ? { answer: 'Записка сохранена и проверена.', selectedEvidenceIds: JSON.parse(last.content).evidence.map(row => row.id), claims: [] }
          : { title: 'Список голосом', body: 'Позвонить в мастерскую.' };
        const id = randomUUID(); receipts.set(id, { id, status: options.receiptStatus ?? 'completed', model_requested: GLM_MODEL,
          model_resolved: GLM_MODEL, input_tokens: 3, output_tokens: 2, charge_urub: '0' });
        return new Response(JSON.stringify({ id: 'synthetic_voice_completion', model: GLM_MODEL,
          choices: [{ index: 0, message: { role: 'assistant', tool_calls: [{ id: 'synthetic_voice_tool', type: 'function', function: { name, arguments: JSON.stringify(args) } }] }, finish_reason: 'tool_calls' }],
          usage: { prompt_tokens: 3, completion_tokens: 2, total_tokens: 5 } }), { status: 200, headers: { 'content-type': 'application/json', 'x-soty-request-id': id } });
      },
    } } });
  const transport = createOpenAiOrbTransport({ resolveApiKey: async () => 'synthetic-openai-no-real-account-key', timeoutMs: 100,
    fetchImpl: async (url, init) => {
      http.push({ url, method: init.method });
      if (url.endsWith('/hangup')) return new Response(null, { status: options.hangupStatus ?? 200 });
      assert.equal(url, 'https://api.openai.com/v1/realtime/calls'); assert.ok(init.body instanceof FormData);
      const session = JSON.parse(init.body.get('session')); assert.equal(session.model, 'gpt-realtime-2.1'); assert.equal(session.type, 'realtime');
      assert.deepEqual(session.tools.map(item => item.name), ['delegate_to_agent']); assert.equal('delegation' in session, false);
      if (transportHold) { const gate = transportHold; transportHold = null; gate.entered.resolve(); await gate.release.promise; }
      const callId = 'rtc_fixture_' + (sockets.length + 1);
      return new Response('synthetic_sdp_answer', { status: 201, headers: { location: '/v1/realtime/calls/' + callId, 'content-type': 'application/sdp' } });
    },
    socketFactory: (url, init) => { assert.ok(url.startsWith('wss://api.openai.com/v1/realtime?call_id=rtc_fixture_')); assert.equal(init.followRedirects, false);
      const socket = new SyntheticSocket('sess_fixture_' + (sockets.length + 1)); sockets.push(socket); return socket; },
  });
  const configuration = options.disabled ? {} : { enabled: true, transport, admitQuota: async intent => {
    admissions.push(intent); if (realQuota) return realQuota.admit(intent); if (options.denyQuota) throw new Error('synthetic_budget_denied');
    const key = intent.actor.accountId + ':' + intent.actor.deviceId + ':' + intent.requestId;
    return { assertCurrent: () => true,
      async claimTransport() { if (durableClaims.has(key)) return false; durableClaims.set(key, intent.intentDigest); return true; },
      async recordOutcome(proof) { outcomes.push(proof); },
    };
  } };
  const observedAgent = options.projectAgentResult ? { ...personal, async executeAsync(request) {
    const result = await personal.executeAsync(request); return request.op === 'agent.turn' ? options.projectAgentResult(result, request) : result;
  } } : personal;
  voice = createVoiceOrbComposition({ getConnect: () => connect, agent: observedAgent, configuration });
  connect = createConnectService({ databasePath: ':memory:', projectId: 'soty', allowedOrigins: [ORIGIN], extensions: [actualNotes, personal, { operations: voice.operations, executeAsync(request) { issuedActors.set(request.actor.accountId, request.actor); return voice.executeAsync(request); } }] });
  t.after(async () => { holds.forEach(gate => gate.release.resolve()); await voice.close(); browserClients.forEach(client => client.dispose()); realQuota?.close(); personal.close(); actualNotes.close(); connect.close(); });
  async function rpc(actor, op, args = {}) {
    const challenge = good(await connect.handle({ origin: ORIGIN, op: 'challenge', args: { operation: op, digest: digestArgs(args) } }));
    return connect.handle({ origin: ORIGIN, op, args, proof: { challengeId: challenge.challengeId, publicJwk: actor.publicJwk,
      signature: sign('sha256', Buffer.from(challenge.message), { key: actor.key, dsaEncoding: 'ieee-p1363' }).toString('base64url') } });
  }
  const alice = identity(), bob = identity();
  for (const actor of [alice, bob]) actor.account = good(await rpc(actor, 'bootstrap', { label: 'Synthetic general voice', encryptionPublicJwk: actor.encryptionPublicJwk }));
  if (options.realQuota) realQuota = createVoiceOrbQuota({ databasePath: ':memory:', profiles: new Map([alice, bob].map(actor => [
    JSON.stringify({ accountId: actor.account.accountId, deviceId: actor.account.deviceId }), Object.freeze({ ...actor.account,
      tenantId: 'synthetic_owned_scale', authorizationId: 'synthetic_scale_authorization', expiresAt: Date.now() + 86400000, maxStarts: 1000 }),
  ])) });
  const response = (callId = 'call_fixture_one', task = 'Создай записку: позвонить в мастерскую.', responseId = 'response_fixture_one', status = 'completed') => ({
    type: 'response.done', response: { id: responseId, status, output: [{ id: 'item_' + callId, type: 'function_call', status: 'completed',
      name: 'delegate_to_agent', call_id: callId, arguments: JSON.stringify({ task }) }], usage: { total_tokens: 5, input_tokens: 3, output_tokens: 2 }, metadata: { ...sockets.at(-1)?.sent.filter(row => row.type === 'response.create' && row.response?.metadata?.soty_turn).at(-1)?.response.metadata } },
  });
  return { alice, bob, rpc, voice, http, sockets, admissions, outcomes, modelCalls, puts, response,
    async hostActor() { good(await rpc(alice, 'voice.orb.status')); return issuedActors.get(alice.account.accountId); },
    rawChallenge: () => connect.handle({ origin: ORIGIN, op: 'challenge', args: { operation: 'voice.orb.status', digest: digestArgs({}) } }),
    quotaStatus: actor => realQuota.statusFor(actor),
    async browser() {
      const makeStorage = () => { let state = null; return { read: async () => state === null ? null : structuredClone(state),
        claim: async value => { state ??= structuredClone(value); return structuredClone(state); },
        compareAndSwap: async (revision, value) => { if (state?.localRevision !== revision) throw Object.assign(new Error('LOCAL_CONFLICT'), { code: 'LOCAL_CONFLICT' }); state = structuredClone(value); return structuredClone(state); } }; };
      const storage = makeStorage(), otherStorage = makeStorage();
      const options = { projectId: 'soty', endpoint: ORIGIN + '/rpc', scopedVoiceCleanup: true,
        createChannel: () => ({ close() {}, postMessage() {} }),
        fetch: async (_url, init) => { const value = await connect.handle({ origin: ORIGIN, ...JSON.parse(init.body) }); return Response.json(value); } };
      const client = createClientWithStorage(options, storage), other = createClientWithStorage(options, otherStorage); browserClients.push(client, other);
      const owner = await client.bootstrap('Actual SDK voice owner'), next = await other.bootstrap('Actual SDK next owner');
      const state = await storage.read(), saved = await otherStorage.read(); await storage.compareAndSwap(state.localRevision, { ...state, localRevision: state.localRevision + 1, installations: [...state.installations, ...saved.installations] });
      return { client, owner, next };
    },
    start: (requestId = 'synthetic_voice_start_one', actor = alice) => rpc(actor, 'voice.orb.start', { requestId, sdp: 'synthetic_offer' }),
    list: async (actor = alice) => good(await rpc(actor, 'notes.list', { expectedAccountId: actor.account.accountId })).notes,
    event(value) { sockets.at(-1).event(value); },
    call(callId = 'call_fixture_one', task, responseId = 'response_fixture_one', userItemId = 'user_fixture_one') {
      const socket = sockets.at(-1); socket.event({ type: 'input_audio_buffer.speech_started', item_id: userItemId });
      socket.event({ type: 'input_audio_buffer.committed', item_id: userItemId });
      socket.event({ type: 'response.created', response: { id: responseId } }); socket.event(response(callId, task, responseId));
    },
    turn(sessionId, extra = {}, actor = alice) { return rpc(actor, 'voice.orb.turn', { sessionId, callId: 'call_fixture_one', ...extra }); },
    holdModel() { const gate = { entered: deferred(), release: deferred() }; modelHold = gate; holds.push(gate); return gate; },
    holdTransport() { const gate = { entered: deferred(), release: deferred() }; transportHold = gate; holds.push(gate); return gate; },
    async secondDevice() {
      const actor = identity(), started = good(await rpc(actor, 'enrollment.start', { label: 'Second voice device', encryptionPublicJwk: actor.encryptionPublicJwk }));
      good(await rpc(alice, 'enrollment.approve', { requestId: started.requestId, wrappedKey: { schema: 'synthetic.wrapped', ciphertext: 'opaque' } }));
      actor.account = good(await rpc(actor, 'enrollment.finish', { requestId: started.requestId, expectedAccountId: alice.account.accountId })); return actor;
    },
  };
}

test('default OFF and rejected quota perform no OpenAI/model calls', async t => {
  const off = await fixture(t, { disabled: true }); assert.equal(good(await off.rpc(off.alice, 'voice.orb.status')).configured, false);
  assert.equal((await off.start()).ok, false); assert.equal(off.http.length, 0); assert.equal(off.modelCalls.length, 0);
  const denied = await fixture(t, { denyQuota: true }); assert.equal((await denied.start()).ok, false);
  assert.equal(denied.admissions.length, 1); assert.equal(denied.http.length, 0); assert.equal(denied.modelCalls.length, 0);
});

test('completed structured voice task delegates to accounted GLM, saves one owned Note, reads it, then returns original call_id output', async t => {
  const f = await fixture(t), started = good(await f.start()); f.call();
  const result = good(await f.turn(started.sessionId, { noteCreation: true })); assert.equal(result.playbackVerified, false);
  assert.equal(f.modelCalls.length, 2); assert.equal(f.modelCalls[0].tools.some(row => row.function.name === 'create_my_note'), true);
  const notes = await f.list(); assert.equal(notes.length, 1); assert.equal(notes[0].revision, 1); assert.equal(f.puts.length, 1);
  const read = good(await f.rpc(f.alice, 'notes.get', { noteId: notes[0].noteId, expectedAccountId: f.alice.account.accountId })).note;
  assert.equal(read.title, 'Список голосом'); assert.equal(read.body, 'Позвонить в мастерскую.');
  assert.equal((await f.list(f.bob)).length, 0);
  const output = f.sockets[0].sent.find(row => row.type === 'conversation.item.create');
  assert.equal(output.item.type, 'function_call_output'); assert.equal(output.item.call_id, 'call_fixture_one');
  assert.equal(JSON.parse(output.item.output).answer, result.answer); assert.equal(f.sockets[0].sent.at(-1).type, 'response.create');
  const ended = good(await f.rpc(f.alice, 'voice.orb.end', { sessionId: started.sessionId }));
  assert.equal(ended.hangupAccepted, true); assert.equal(ended.remoteTerminationVerified, false); assert.equal(ended.accountingFinalized, false);
  assert.equal(f.outcomes.length, 1); assert.equal(f.outcomes[0].accountingFinalized, false); assert.equal(f.outcomes[0].responseUsage.length, 1);
});

test('partial function argument events and browser supplied task/model/permission strings cannot dispatch GLM', async t => {
  const f = await fixture(t), started = good(await f.start());
  f.event({ type: 'response.function_call_arguments.delta', call_id: 'call_fixture_one', delta: '{"task":"Создай' });
  for (const extra of [{ task: 'forged task' }, { model: 'other' }, { noteCreation: 'true' }]) assert.equal((await f.turn(started.sessionId, extra)).ok, false);
  assert.equal(f.modelCalls.length, 0); assert.equal(f.puts.length, 0);
});

test('model cannot give itself Notes permission; foreign account cannot use another session', async t => {
  const f = await fixture(t), started = good(await f.start()); f.call();
  assert.equal((await f.turn(started.sessionId, { noteCreation: true }, f.bob)).ok, false); assert.equal(f.modelCalls.length, 0);
  assert.equal((await f.turn(started.sessionId)).ok, false); assert.equal(f.puts.length, 0);
  assert.equal(f.modelCalls[0].tools.some(row => row.function.name === 'create_my_note'), false);
});

test('duplicate completed event, duplicate turn and changed start reuse do not repeat a Note or paid start', async t => {
  const f = await fixture(t), started = good(await f.start()); f.call(); f.event(f.response());
  good(await f.turn(started.sessionId, { noteCreation: true }));
  assert.equal((await f.turn(started.sessionId, { noteCreation: true })).error.code, 'voice_orb_reconcile_required');
  assert.equal((await f.start()).error.code, 'voice_orb_reconcile_required');
  assert.equal((await f.rpc(f.alice, 'voice.orb.start', { requestId: 'synthetic_voice_start_one', sdp: 'changed_offer' })).error.code, 'voice_orb_intent_conflict');
  assert.equal(f.puts.length, 1); assert.equal(f.modelCalls.length, 2); assert.equal(f.http.filter(row => !row.url.endsWith('/hangup')).length, 1);
});

test('cancel before a queued completed-call dispatch invalidates it without model/tool execution', async t => {
  const f = await fixture(t), started = good(await f.start()); f.call(); good(await f.rpc(f.alice, 'voice.orb.cancel', { sessionId: started.sessionId }));
  assert.equal((await f.turn(started.sessionId, { noteCreation: true })).ok, false); assert.equal(f.modelCalls.length, 0); assert.equal(f.puts.length, 0);
});

test('delayed response.created for pre-cancel user input cannot regain a new control generation', async t => {
  const f = await fixture(t), started = good(await f.start());
  f.event({ type: 'input_audio_buffer.speech_started', item_id: 'user_fixture_one' });
  f.event({ type: 'input_audio_buffer.committed', item_id: 'user_fixture_one' });
  const oldCompleted = f.response();
  good(await f.rpc(f.alice, 'voice.orb.cancel', { sessionId: started.sessionId }));
  f.event({ type: 'response.created', response: { id: 'response_fixture_one' } }); f.event(oldCompleted); await flush();
  assert.equal((await f.turn(started.sessionId, { noteCreation: true })).ok, false);
  assert.equal(f.modelCalls.length, 0); assert.equal(f.puts.length, 0);
});

test('pre-clear pending input cannot create a response token after explicit Cancel', async t => {
  const f = await fixture(t), started = good(await f.start());
  good(await f.rpc(f.alice, 'voice.orb.cancel', { sessionId: started.sessionId }));
  const count = f.sockets[0].sent.filter(row => row.type === 'response.create').length;
  f.event({ type: 'input_audio_buffer.committed', item_id: 'old_pending_item' });
  assert.equal(f.sockets[0].sent.filter(row => row.type === 'response.create').length, count);
  f.event({ type: 'input_audio_buffer.cleared' });
  f.event({ type: 'input_audio_buffer.committed', item_id: 'another_old_pending' });
  assert.equal(f.sockets[0].sent.filter(row => row.type === 'response.create').length, count);
  f.event({ type: 'input_audio_buffer.speech_started', item_id: 'fresh_item' });
  f.event({ type: 'input_audio_buffer.committed', item_id: 'fresh_item' });
  assert.equal(f.sockets[0].sent.filter(row => row.type === 'response.create').length, count + 1);
});

test('signed cancel while GLM is pending prevents Note and speech output', async t => {
  const f = await fixture(t), started = good(await f.start()); f.call(); const gate = f.holdModel(), pending = f.turn(started.sessionId, { noteCreation: true });
  await entered(gate, pending); assert.equal(good(await f.rpc(f.alice, 'voice.orb.cancel', { sessionId: started.sessionId })).cancelRequested, true);
  gate.release.resolve(); assert.equal((await pending).ok, false); assert.equal(f.puts.length, 0);
  assert.equal(f.sockets[0].sent.some(row => row.type === 'conversation.item.create'), false);
});

test('new user speech while GLM is pending cancels the previous task before its Note commit', async t => {
  const f = await fixture(t), started = good(await f.start()); f.call(); const gate = f.holdModel(), pending = f.turn(started.sessionId, { noteCreation: true });
  await entered(gate, pending); f.event({ type: 'input_audio_buffer.speech_started', item_id: 'new_speech' }); await flush();
  gate.release.resolve(); assert.equal((await pending).ok, false); assert.equal(f.puts.length, 0);
});

test('device revocation while GLM is pending closes voice and prevents the Note', async t => {
  const f = await fixture(t), device = await f.secondDevice(), started = good(await f.start()); f.call();
  const gate = f.holdModel(), pending = f.turn(started.sessionId, { noteCreation: true }); await entered(gate, pending);
  good(await f.rpc(f.alice, 'device.revoke', { deviceId: f.alice.account.deviceId })); gate.release.resolve();
  assert.equal((await pending).ok, false); assert.equal(f.puts.length, 0); assert.equal((await f.list(device)).length, 0);
});

test('unconfirmed GLM accounting or changed Note readback never produces successful voice function output', async t => {
  for (const options of [{ receiptStatus: 'usage_pending' }, { corruptReadback: true }]) {
    const f = await fixture(t, options), started = good(await f.start()); f.call();
    assert.equal((await f.turn(started.sessionId, { noteCreation: true })).ok, false);
    assert.equal(f.sockets[0].sent.some(row => row.type === 'conversation.item.create'), false);
    const previousCalls = f.modelCalls.length; assert.equal((await f.turn(started.sessionId, { noteCreation: true })).ok, false);
    assert.equal(f.modelCalls.length, previousCalls);
  }
});

test('a speech response to tool output cannot recursively dispatch another GLM task without fresh committed user input', async t => {
  const f = await fixture(t), started = good(await f.start()); f.call(); good(await f.turn(started.sessionId, { noteCreation: true }));
  f.event({ type: 'response.created', response: { id: 'recursive_response' } });
  f.event(f.response('recursive_call', 'Создай ещё одну записку', 'recursive_response')); await flush();
  assert.equal((await f.turn(started.sessionId, { callId: 'recursive_call', noteCreation: true })).ok, false);
  assert.equal(f.puts.length, 1); assert.equal(f.modelCalls.length, 2);
});

test('voice task is taken from authoritative complete sideband data, never partial/changed caption text', async t => {
  const f = await fixture(t), started = good(await f.start());
  f.event({ type: 'session.input_transcript.delta', delta: 'НЕ СОЗДАВАЙ ЧУЖУЮ ЗАПИСКУ', start_ms: 0, end_ms: 10 });
  f.call(); good(await f.turn(started.sessionId, { noteCreation: true }));
  assert.equal(f.puts.length, 1); assert.equal(f.modelCalls[0].messages.some(row => String(row.content).includes('НЕ СОЗДАВАЙ ЧУЖУЮ')), false);
});

test('129 composition-layer sessions retain signed-derived actor fences and real SQLite quota without changing network rate limits', async t => {
  const f = await fixture(t, { realQuota: true }), actor = await f.hostActor();
  for (let i = 0; i < 129; i++) {
    const result = await f.voice.executeAsync({ op: 'voice.orb.start', actor, args: { requestId: 'synthetic_scale_' + i, sdp: 'synthetic_offer' } });
    await f.voice.executeAsync({ op: 'voice.orb.end', actor, args: { sessionId: result.sessionId } });
  }
  const starts = f.http.filter(row => !row.url.endsWith('/hangup')).length; assert.equal(starts, 129);
  assert.equal(f.quotaStatus(actor).reservedStarts, 129);
  await assert.rejects(f.voice.executeAsync({ op: 'voice.orb.start', actor, args: { requestId: 'synthetic_scale_0', sdp: 'synthetic_offer' } }));
  assert.equal(f.http.filter(row => !row.url.endsWith('/hangup')).length, starts);
});

test('actual production signed-network admission still returns rate_limited at its unchanged bound', async t => {
  const f = await fixture(t); let rejected = null;
  for (let i = 0; i < 300; i++) { const value = await f.rpc(f.alice, 'voice.orb.status'); if (!value.ok) { rejected = value; break; } }
  assert.equal(rejected?.error.code, 'rate_limited'); assert.equal(f.http.length, 0); assert.equal(f.modelCalls.length, 0);
});

test('actual SDK retained owned End after A→B aborts the original GLM before Note commit', async t => {
  const f = await fixture(t), browser = await f.browser(), client = browser.client;
  const started = await client.extension('voice.orb.start', { requestId: 'actual_sdk_owned_start', sdp: 'synthetic_offer' }, { expectedAccountId: browser.owner.accountId });
  const cleanup = client.voiceSessionCleanup(started); assert.equal(typeof cleanup, 'function'); f.call();
  const gate = f.holdModel();
  const pending = client.extension('voice.orb.turn', { sessionId: started.sessionId, callId: 'call_fixture_one', noteCreation: true }, { expectedAccountId: browser.owner.accountId });
  const observed = pending.then(value => ({ value }), error => ({ error }));
  await Promise.race([gate.entered.promise, observed.then(() => { throw new Error('model_not_entered'); })]);
  await client.switchProfile(browser.next.accountId); await cleanup(); gate.release.resolve();
  const stopped = await observed; assert.equal(stopped.value, undefined);
  assert.ok(['ACTIVE_PROFILE_CHANGED', 'agent_cancelled', 'voice_orb_expired'].includes(stopped.error?.code)); assert.equal(f.puts.length, 0);
  assert.equal(f.sockets[0].sent.some(row => row.type === 'conversation.item.create'), false);
  assert.equal(f.http.some(row => row.url.endsWith('/hangup')), true);
  assert.equal((await client.getLocalState()).accountId, browser.next.accountId);
});

// These 5 new projection/race cases use the actual signed Connect -> voice
// composition -> existing personal Notes agent. The injected host result models
// a trusted top-level artifact only. Its real Native receipt producer is tested
// separately by the same maintained OTsov CAS fixture, not claimed here.
const editorArtifact = {schema:'soty.otsov-project-artifact.v1',appId:'app-'+'a'.repeat(32),projectId:'11111111-1111-4111-8111-111111111111',branch:'main',revision:2,invocationId:'native_invocation_001'};
test('editor-handoff signed voice projects only the owned host artifact and original deterministic turn ID', {timeout:5000}, async t=>{
  let seen;const f=await fixture(t,{projectAgentResult(result,request){seen=request.args.requestId;return {...result,editorHandoff:editorArtifact};}});
  const session=good(await f.start());f.call();const reply=good(await f.turn(session.sessionId,{noteCreation:true}));
  assert.deepEqual(reply.editorHandoff,editorArtifact);assert.equal(reply.requestId,seen);assert.match(seen,/^voice_[a-f0-9]{64}$/);
  assert.equal(f.http.filter(row=>row.url.endsWith('/realtime/calls')).length,1);assert.equal(f.http.some(row=>row.url.endsWith('/hangup')),false);
});
test('editor-handoff arbitrary Source evidence cannot be promoted by the voice DTO', {timeout:5000}, async t=>{
  const f=await fixture(t,{projectAgentResult(result){return {...result,evidence:[{id:'synthetic_untrusted',data:{editorHandoff:editorArtifact,projectArtifact:editorArtifact}}]};}});
  const session=good(await f.start());f.call();const reply=good(await f.turn(session.sessionId,{noteCreation:true}));
  assert.equal(reply.editorHandoff,undefined);assert.equal(reply.requestId,undefined);assert.equal('evidence'in reply,false);
});
test('editor-handoff malformed top-level artifact cannot escape the host result validator', {timeout:5000}, async t=>{
  const f=await fixture(t,{projectAgentResult(result){return {...result,editorHandoff:{...editorArtifact,url:'https://not-an-editor.invalid'}};}});
  const session=good(await f.start());f.call();const reply=await f.turn(session.sessionId,{noteCreation:true});
  assert.equal(reply.ok,false);assert.equal(reply.editorHandoff,undefined);
  assert.equal(f.sockets[0].sent.filter(row=>row.type==='conversation.item.create').length,0);
});
test('editor-handoff a foreign personal turn ID is not an owned voice result', {timeout:5000}, async t=>{
  const f=await fixture(t,{projectAgentResult(result){return {...result,requestId:'another_personal_turn',editorHandoff:editorArtifact};}});
  const session=good(await f.start());f.call();const reply=await f.turn(session.sessionId,{noteCreation:true});assert.equal(reply.ok,false);assert.equal(reply.editorHandoff,undefined);
});
test('editor-handoff fresh speech during a held model turn prevents its late artifact from being returned', {timeout:5000}, async t=>{
  const f=await fixture(t,{projectAgentResult(result){return {...result,editorHandoff:editorArtifact};}});
  const session=good(await f.start());f.call();const gate=f.holdModel(),pending=f.turn(session.sessionId,{noteCreation:true});
  await entered(gate,pending);f.event({type:'input_audio_buffer.speech_started',item_id:'new_human_speech'});gate.release.resolve();
  const reply=await pending;assert.equal(reply.ok,false);assert.equal(reply.editorHandoff,undefined);
});
