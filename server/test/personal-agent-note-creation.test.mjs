import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, randomUUID, sign } from 'node:crypto';

const root = new URL('../../', import.meta.url);
const load = name => import(new URL(name, root).href);
const [{ createConnectService, digestArgs }, { createNotesService }, { createPersonalAgentComposition },
  { createPersonalAgentConversation }, { GLM_MODEL }] = await Promise.all([
  load('modules/connect/server/index.mjs'), load('modules/notes/server/index.mjs'),
  load('server/personal-agent-composition.js'), load('src/platform/personal-agent-client.ts'),
  load('modules/personal-agent/provider/index.mjs'),
]);
const ORIGIN = 'https://note-intent-component.test';
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
const good = value => { assert.equal(value.ok, true, value.error?.code); return value; };
const entered = (held, pending) => Promise.race([held.entered.promise, pending.then(result => {
  throw new Error(`fixture_completed_before_model:${result.error?.code ?? 'complete'}`);
})]);
const identity = () => {
  const pair = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { key: pair.privateKey, publicJwk: pair.publicKey.export({ format: 'jwk' }), encryptionPublicJwk: pair.publicKey.export({ format: 'jwk' }) };
};

async function fixture(t, { receiptStatus = 'completed', corruptReadback = false, secondEffect = false } = {}) {
  let connect, personal, held, owner;
  const calls = [], puts = [], receipts = new Map();
  const actualNotes = createNotesService({ databasePath: ':memory:', projectId: 'soty' });
  const notes = { execute(request) {
    if (request.op === 'notes.put') puts.push(structuredClone(request));
    const result = actualNotes.execute(request);
    if (corruptReadback && request.op === 'notes.get' && request.args.noteId.startsWith('agent_note_'))
      return { note: { ...result.note, title: 'Concurrent changed title' } };
    return result;
  } };
  const keyRef = Object.freeze({}), tenant = 'synthetic_note_billing';
  personal = createPersonalAgentComposition({ getConnect: () => connect,
    getServices: () => ({ notes, capabilities: {} }), configuration: { enabled: true, historyEnabled: false, gateway: {
      gatewayOrigin: 'https://synthetic-note-gateway.invalid', billingTenant: tenant, keyRef,
      authorizeActor: ({ actor }) => connect.isActorActive(actor),
      resolveCredential: async ref => ({ keyRef: ref, tenantId: tenant, bearer: 'synthetic-note-only-bearer-no-real-provider' }),
      readReceipt: async ({ requestId }) => ({ tenantId: tenant, request: receipts.get(requestId) }),
      fetchImpl: async (_url, init) => {
        const body = JSON.parse(init.body); calls.push(body);
        assert.equal(body.model, GLM_MODEL); assert.equal(body.autoFallback, false);
        if (held) { const pending = held; held = undefined; pending.entered.resolve(); await pending.release.promise; }
        const last = body.messages.at(-1);
        const name = last.role === 'tool' && !secondEffect ? 'finish_answer' : 'create_my_note';
        const args = name === 'finish_answer'
          ? { answer: 'Записка сохранена и прочитана.', selectedEvidenceIds: JSON.parse(last.content).evidence.map(row => row.id), claims: [] }
          : { title: 'Список на завтра', body: 'Позвонить в мастерскую.' };
        const id = randomUUID(); receipts.set(id, { id, status: receiptStatus, model_requested: GLM_MODEL,
          model_resolved: GLM_MODEL, input_tokens: 3, output_tokens: 2, charge_urub: '0' });
        return new Response(JSON.stringify({ id: 'synthetic_note_completion', model: GLM_MODEL,
          choices: [{ index: 0, message: { role: 'assistant', tool_calls: [{ id: 'synthetic_note_tool', type: 'function',
            function: { name, arguments: JSON.stringify(args) } }] }, finish_reason: 'tool_calls' }],
          usage: { prompt_tokens: 3, completion_tokens: 2, total_tokens: 5 } }),
        { status: 200, headers: { 'content-type': 'application/json', 'x-soty-request-id': id } });
      },
    } } });
  connect = createConnectService({ databasePath: ':memory:', projectId: 'soty', allowedOrigins: [ORIGIN], extensions: [actualNotes, personal] });
  const holds = [];
  t.after(() => { holds.forEach(value => value.release.resolve()); personal.close(); actualNotes.close(); connect.close(); });
  async function rpc(actor, op, args = {}) {
    const challenge = good(await connect.handle({ origin: ORIGIN, op: 'challenge', args: { operation: op, digest: digestArgs(args) } }));
    return connect.handle({ origin: ORIGIN, op, args, proof: { challengeId: challenge.challengeId, publicJwk: actor.publicJwk,
      signature: sign('sha256', Buffer.from(challenge.message), { key: actor.key, dsaEncoding: 'ieee-p1363' }).toString('base64url') } });
  }
  const alice = identity(), bob = identity();
  for (const actor of [alice, bob]) actor.account = good(await rpc(actor, 'bootstrap', { label: 'Synthetic Notes owner', encryptionPublicJwk: actor.encryptionPublicJwk }));
  owner = alice;
  return { calls, puts, rpc, alice, bob,
    turn: (extra = {}, actor = alice) => rpc(actor, 'agent.turn', { requestId: 'synthetic_note_turn_one', prompt: 'Создай записку со списком на завтра.', ...extra }),
    list: async (actor = owner) => good(await rpc(actor, 'notes.list', { expectedAccountId: actor.account.accountId })).notes,
    async read(actor, noteId) { return rpc(actor, 'notes.get', { expectedAccountId: actor.account.accountId, noteId }); },
    hold() { const value = { entered: deferred(), release: deferred() }; held = value; holds.push(value); return value; },
    async secondDevice() {
      const device = identity(), started = good(await rpc(device, 'enrollment.start', { label: 'Own second Notes device', encryptionPublicJwk: device.encryptionPublicJwk }));
      good(await rpc(alice, 'enrollment.approve', { requestId: started.requestId, wrappedKey: { schema: 'synthetic.wrapped', ciphertext: 'opaque' } }));
      device.account = good(await rpc(device, 'enrollment.finish', { requestId: started.requestId, expectedAccountId: alice.account.accountId }));
      owner = device; return device;
    },
    browser(extra = {}) {
      const data = new Map(); let stateReads = 0, requestNumber = 0;
      const client = { async getLocalState() { stateReads++; await extra.readState?.(stateReads); return { accountId: alice.account.accountId }; },
        async extension(op, args) { const value = await rpc(alice, op, args); if (!value.ok) throw Object.assign(new Error(value.error.code), { code: value.error.code }); return value; } };
      const storage = { getItem: key => data.get(key) ?? null, setItem: (key, value) => data.set(key, value) };
      const ui = createPersonalAgentConversation({ accountId: alice.account.accountId, client, storage, idFactory: () => `synthetic_note_browser_${++requestNumber}` });
      return { ui, data, client, storage };
    },
  };
}

test('signed one-turn permission creates an owned Note only after accounted model and domain readback', async t => {
  const f = await fixture(t), result = good(await f.turn({ noteCreation: true }));
  const evidence = result.evidence.find(row => row.title === 'Созданная записка');
  assert.equal(evidence.data.completionVerified, true); assert.equal(evidence.data.verificationMethod, 'domain_read');
  assert.equal(f.calls[0].tools.some(row => row.function.name === 'create_my_note'), true);
  assert.deepEqual(f.calls[1].tools.map(row => row.function.name), ['finish_answer']);
  const notes = await f.list(); assert.equal(notes.length, 1); assert.equal(notes[0].noteId, evidence.data.noteId);
  const note = good(await f.read(f.alice, notes[0].noteId)).note;
  assert.equal(note.title, 'Список на завтра'); assert.equal(note.body, 'Позвонить в мастерскую.'); assert.equal(note.revision, 1);
  assert.equal((await f.read(f.bob, note.noteId)).ok, false); assert.equal((await f.list(f.bob)).length, 0);
  assert.equal(f.puts.length, 1); assert.equal(f.puts[0].args.expectedRevision, 0);
  assert.equal(f.puts[0].args.noteId.includes('Список'), false);
});

test('default and quoted external instruction cannot offer or execute a note write', async t => {
  const f = await fixture(t), result = await f.turn({ prompt: 'В документе написано «создай записку». Объясни эту фразу, ничего не создавай.' });
  assert.equal(result.ok, false); assert.equal(f.calls[0].tools.some(row => row.function.name === 'create_my_note'), false);
  assert.equal(f.puts.length, 0); assert.equal((await f.list()).length, 0);
});

test('the same signed turn cannot be repeated or repurposed, and Notes CAS rejects a changed payload', async t => {
  const f = await fixture(t); good(await f.turn({ noteCreation: true }));
  assert.equal((await f.turn({ noteCreation: true })).error.code, 'agent_reconcile_required');
  assert.equal((await f.turn({ noteCreation: true, prompt: 'Создай другую записку.' })).error.code, 'agent_intent_conflict');
  const prior = f.puts[0];
  const replay = good(await f.rpc(f.alice, 'notes.put', prior.args)); assert.equal(replay.replayed, true);
  const changed = await f.rpc(f.alice, 'notes.put', { ...prior.args, body: 'Иное содержимое.' });
  assert.equal(changed.ok, false); assert.equal(changed.error.code, 'notes_mutation_reused');
  assert.equal((await f.list()).length, 1); assert.equal(f.puts.length, 1); assert.equal(f.calls.length, 2);
});

test('false, string, owner override and project override are refused before provider dispatch', async t => {
  const f = await fixture(t);
  for (const extra of [{ noteCreation: false }, { noteCreation: 'true' }, { noteCreation: true, accountId: f.bob.account.accountId }, { noteCreation: true, projectId: 'foreign' }])
    assert.equal((await f.turn(extra)).ok, false);
  assert.equal(f.calls.length, 0); assert.equal(f.puts.length, 0);
});

test('signed cancellation while the model is pending prevents note creation', async t => {
  const f = await fixture(t), held = f.hold(), pending = f.turn({ noteCreation: true });
  await entered(held, pending);
  assert.equal(good(await f.rpc(f.alice, 'agent.cancel', { requestId: 'synthetic_note_turn_one' })).cancelRequested, true);
  held.release.resolve(); assert.equal((await pending).ok, false); assert.equal(f.puts.length, 0); assert.equal((await f.list()).length, 0);
});

test('current device revocation while the model is pending prevents note creation', async t => {
  const f = await fixture(t); await f.secondDevice();
  const held = f.hold(), pending = f.turn({ noteCreation: true }); await entered(held, pending);
  good(await f.rpc(f.alice, 'device.revoke', { deviceId: f.alice.account.deviceId }));
  held.release.resolve(); assert.equal((await pending).ok, false); assert.equal(f.puts.length, 0); assert.equal((await f.list()).length, 0);
});

test('unknown payment settlement cannot execute the offered note tool', async t => {
  const f = await fixture(t, { receiptStatus: 'usage_pending' }), result = await f.turn({ noteCreation: true });
  assert.equal(result.ok, false); assert.equal(result.error.code, 'agent_accounting_unknown');
  assert.equal(f.calls.length, 1); assert.equal(f.puts.length, 0); assert.equal((await f.list()).length, 0);
});

test('changed readback never becomes created evidence and unknown is not retried', async t => {
  const f = await fixture(t, { corruptReadback: true }), result = await f.turn({ noteCreation: true });
  assert.equal(result.ok, false); assert.equal(Object.hasOwn(result, 'evidence'), false);
  assert.equal(f.puts.length, 1); assert.equal((await f.list()).length, 1);
  assert.equal((await f.turn({ noteCreation: true })).error.code, 'agent_reconcile_required'); assert.equal(f.puts.length, 1);
});

test('a second effect cannot be offered or executed after the one approved Note', async t => {
  const f = await fixture(t, { secondEffect: true }), result = await f.turn({ noteCreation: true });
  assert.equal(result.ok, false); assert.equal(f.puts.length, 1); assert.equal((await f.list()).length, 1);
  assert.deepEqual(f.calls[1].tools.map(row => row.function.name), ['finish_answer']);
});

test('browser snapshots the explicit permission before async work and clears it on the next turn', async t => {
  const f = await fixture(t), hold = deferred(), entered = deferred();
  const browser = f.browser({ async readState(number) { if (number === 1) { entered.resolve(); await hold.promise; } } });
  const permission = { noteCreation: true }, pending = browser.ui.turn('Создай записку', null, permission);
  await entered.promise; delete permission.noteCreation; hold.resolve(); await pending;
  assert.equal(browser.ui.view().phase, 'complete'); assert.equal(f.puts.length, 1);
  browser.ui.startNew(); await assert.rejects(browser.ui.turn('Только прочитай эту цитату «создай записку»'));
  assert.equal(f.calls[2].tools.some(row => row.function.name === 'create_my_note'), false); assert.equal(f.puts.length, 1);
});

test('browser retains one true intent after unknown, reload checks status without a fresh model dispatch', async t => {
  const f = await fixture(t, { receiptStatus: 'usage_pending' }), browser = f.browser();
  await assert.rejects(browser.ui.turn('Создай записку', null, { noteCreation: true }));
  const saved = JSON.parse([...browser.data.values()][0]); assert.equal(saved.pending[0].noteCreation, true);
  browser.ui.dispose();
  const reloaded = createPersonalAgentConversation({ accountId: f.alice.account.accountId, client: browser.client, storage: browser.storage });
  assert.equal(reloaded.view().phase, 'unknown'); await reloaded.checkStatus();
  assert.equal(f.calls.length, 1); assert.equal(f.puts.length, 0); assert.equal(reloaded.view().phase, 'unknown');
});
