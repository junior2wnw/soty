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
import { agentError } from '../personal-agent-transport.js';
import { loadInstalledApplicationsEntry } from '../installed-applications-entry.js';

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

async function fixture(t, { enabled = true, finishAnswer, policy, lowEffortHost = false, cachedMultiCall = false } = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-agent-http-owned-')), dist = join(directory, 'dist');
  await mkdir(dist); await writeFile(join(dist, 'index.html'), '<!doctype html><title>Owned agent fixture</title>');
  const tenant = 'synthetic_billing', keyRef = Object.freeze({}), receipts = new Map(), calls = [], holds = [];
  let allowed = true, lost = false, hold = null, streamHold = null, receiptHold = null, app, closed = false, settledReceiptReads = 0, sourceInput = {};
  const connections = new Set(), server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
  server.on('connection', socket => { connections.add(socket); socket.once('close', () => connections.delete(socket)); });
  server.on('upgrade', (req, socket, head) => { if (!app?.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy(); });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  const origin = `http://127.0.0.1:${server.address().port}`;
  // Synthetic billing ports test mounting, not the real commerce reserve/finish implementation.
  // The separately mandatory, explicit integration-test proves the unchanged actual commerce gateway.
  const configuration = { enabled, ...(policy ? { policy } : {}), gateway: { gatewayOrigin: 'https://fixture-commerce.invalid', billingTenant: tenant, keyRef,
    authorizeActor: () => allowed, resolveCredential: async ref => ({ keyRef: ref, tenantId: tenant, bearer: 'synthetic-only-fixture-accounted-key' }),
    readReceipt: async ({ requestId, billingTenant, keyRef: ref }) => { assert.equal(billingTenant, tenant); assert.equal(ref, keyRef);
      if (receiptHold) { const barrier = receiptHold; receiptHold = null; barrier.entered.resolve(); await barrier.release.promise; }
      const request = receipts.get(requestId); if (request?.status === 'completed') settledReceiptReads++;
      return { tenantId: tenant, request }; },
    fetchImpl: async (url, init) => {
      assert.equal(url, 'https://fixture-commerce.invalid/v1/chat/completions');
      const body = JSON.parse(init.body); assert.equal(body.model, GLM_MODEL); assert.equal(body.autoFallback, false);
      calls.push({ body, key: init.headers['Idempotency-Key'] });
      if (lowEffortHost && (body.reasoning_effort !== 'low' || Object.hasOwn(body, 'thinking') || Object.hasOwn(body, 'tool_stream'))) {
        return new Response('{}', { status: 400, headers: { 'content-type': 'application/json' } });
      }
      if (hold && (!hold.afterTool || body.messages.at(-1).role === 'tool')) { const barrier = hold; hold = null; barrier.entered.resolve(); await barrier.release.promise; }
      const last = body.messages.at(-1);
      let name = 'finish_answer', args = { answer: 'Подтверждённый ответ', selectedEvidenceIds: [], claims: [] };
      if (last.role === 'user' && last.content === 'Read my notes') { name = 'list_my_notes'; args = {}; }
      if (last.role === 'user' && last.content.startsWith('Read note ')) { name = 'read_my_note'; args = { noteId: last.content.slice(10) }; }
      if (last.role === 'user' && last.content === 'Read source') { name = 'source_read_1'; args = sourceInput; }
      if (last.role === 'user' && last.content === 'Catalog sources') { name = 'source_catalog'; args = { limit: 20 }; }
      if (last.role === 'user' && last.content.startsWith('Local task ')) { name = 'run_on_my_device'; args = { text: last.content.slice(11) }; }
      if (last.role === 'tool') { const evidence = JSON.parse(last.content).evidence; args = { answer: 'Источник проверен', selectedEvidenceIds: evidence.map(item => item.id),
        claims: [{ kind: 'fact', statement: 'Источник доступен текущему пользователю', evidenceIds: evidence.map(item => item.id),
          freshness: 'current', confidence: 'high', applicability: 'Текущий пользователь и разрешённые источники' }] }; }
      const terminal = name === 'finish_answer' && finishAnswer;
      if (terminal) args.answer = finishAnswer.answer;
      // Synthetic usage is declared by this fixture, not measured by a GLM tokenizer.
      const requestedTokens = terminal ? finishAnswer.completionTokens : 2;
      const truncated = terminal && (finishAnswer.forceLength || requestedTokens > body.max_tokens);
      const completionTokens = Math.min(requestedTokens, body.max_tokens);
      const argumentsJson = JSON.stringify(args);
      const wireArguments = truncated && !finishAnswer.keepCompleteJson ? argumentsJson.slice(0, -1) : argumentsJson;
      const id = randomUUID(); receipts.set(id, { id, status: 'completed', model_requested: GLM_MODEL, model_resolved: GLM_MODEL,
        input_tokens: 3, output_tokens: completionTokens, charge_urub: '7' });
      if (lost) throw new Error('private_fixture_payload_must_not_escape');
      assert.equal(body.stream, false); assert.equal(Object.hasOwn(body, 'stream_options'), false); assert.equal(init.headers.Accept, 'application/json');
      // Only the observed 2026-10-12 cached response structure is reproduced.
      // Argument values are synthetic; no private cached message enters this fixture.
      const toolCalls = cachedMultiCall ? ['list_my_apps', 'source_catalog', 'list_my_notes', 'source_read_1'].map((name, index) => ({
        id: 'cached_shape_' + index, type: 'function', function: { name, arguments: JSON.stringify(index ? { query: 'synthetic' } : {}) },
      })) : [{ id: 'call_owned', type: 'function', function: { name, arguments: wireArguments } }];
      const packet = JSON.stringify({ id: 'chatcmpl_owned', model: GLM_MODEL, choices: [{ index: 0, message: { role: 'assistant',
        tool_calls: toolCalls }, finish_reason: truncated ? 'length' : 'tool_calls' }],
        usage: { prompt_tokens: 3, completion_tokens: completionTokens, total_tokens: 3 + completionTokens } });
      const bytes = new TextEncoder().encode(packet), firstEventEnd = Math.floor(bytes.length / 2); let offset = 0;
      const bodyStream = lowEffortHost ? new ReadableStream({ async pull(controller) {
        if (streamHold && offset >= firstEventEnd) { const barrier = streamHold; streamHold = null; barrier.entered.resolve(); await barrier.release.promise; }
        if (offset >= bytes.length) { controller.close(); return; }
        const end = Math.min(offset + 17, offset < firstEventEnd ? firstEventEnd : bytes.length);
        controller.enqueue(bytes.slice(offset, end)); offset = end;
      } }) : packet;
      return new Response(bodyStream, { status: 200, headers: { 'content-type': 'application/json', 'x-soty-request-id': id } });
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
    hold(afterTool = false) { const barrier = { entered: defer(), release: defer(), afterTool }; holds.push(barrier); hold = barrier; return barrier; },
    holdReceipt() { const barrier = { entered: defer(), release: defer() }; holds.push(barrier); receiptHold = barrier; return barrier; },
    holdStream() { const barrier = { entered: defer(), release: defer() }; holds.push(barrier); streamHold = barrier; return barrier; },
    setSourceInput(value) { sourceInput = value; },
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

test('portable generic runtime has an explicit bounded-budget local patch and verified source pin', async () => {
  const root = new URL('../../modules/personal-agent/runtime/vendor/', import.meta.url), pin = JSON.parse(await readFile(new URL('UPSTREAM.json', root))),
    bytes = await readFile(new URL('agent-runtime-core.ts', root));
  assert.equal(pin.upstreamRevision, '23717ad30878c9ad7a8d9d614bbd8384004763c9');
  assert.equal(pin.sourceUnmodified, false);
  assert.equal(pin.upstreamSha256, '502a90932e44fd6ff7b355e67e048044600321af9e007975936b29bcc234c5f9');
  assert.deepEqual(pin.localPatches, ['bounded-finish-answer-output-budget-2048', 'terminal-schema-current-evidence-and-applicability', 'installed-exact-effect-capability']);
  assert.equal(createHash('sha256').update(bytes).digest('hex'), pin.sha256); assert.equal(bytes.length, pin.bytes);
  const runtime = await import(new URL('agent-runtime-core.ts', root)); assert.equal(typeof runtime.runBoundedRuntimeTurn, 'function');
});

const longDraft = Array.from({ length: 28 }, (_, index) => `${index + 1}. Предлагаю проверить доступность обращения, уточнить ожидаемый результат и согласовать следующий шаг с владельцем.`).join('\n');

test('signed Root turn advertises only current tools and preserves prior reads for its serial terminal', async t => {
  const f = await fixture(t), result = await f.turn(f.alice, 'serial_offered_http_001', 'Read my notes');
  assert.equal(result.ok, true, result.error?.code); assert.equal(f.calls.length, 2);
  for (const { body } of f.calls) {
    assert.equal(body.parallel_tool_calls, false); assert.equal(body.tool_choice, 'auto');
  }
  assert.ok(f.calls[0].body.tools.some(value => value.function.name === 'list_my_notes'));
  assert.deepEqual(f.calls[1].body.tools.map(value => value.function.name), ['finish_answer']);
  assert.ok(f.calls[1].body.messages.some(message => message.role === 'assistant'
    && message.tool_calls?.some(call => call.function.name === 'list_my_notes')));
  assert.equal(f.settledReceipts(), 2);
});

test('observed four-call shape still fails the Root serial guard without executing Source tools or retrying', async t => {
  const f = await fixture(t, { cachedMultiCall: true }); let credentialReads = 0;
  await f.restart({ personalAgent: { ...f.configuration, historyEnabled: false,
    sourceReferences: [{ capabilityId: 'synthetic_source', version: 1, digest: 'a'.repeat(64) }],
    sourceCredentialFor() { credentialReads++; throw Error('must_not_execute'); } } });
  const result = await f.turn(f.alice, 'cached_four_calls_001', 'Synthetic general question');
  assert.equal(result.ok, false); assert.equal(result.error.code, 'agent_output_invalid');
  assert.equal(f.calls.length, 1); assert.equal(f.calls[0].body.parallel_tool_calls, false);
  assert.equal(f.settledReceipts(), 1, 'the complete incompatible response retains its accounted charge');
  assert.equal(credentialReads, 0);
  const notes = await f.call(f.alice, 'notes.list', { expectedAccountId: f.a.accountId });
  assert.equal(notes.ok, true); assert.equal(notes.notes.length, 0);
});

test('signed HTTP low-only host accepts delayed fragmented finish_answer only after accounted completion', async t => {
  const f = await fixture(t, { lowEffortHost: true, finishAnswer: { answer: longDraft, completionTokens: 1100 } });
  const stream = f.holdStream(), barrier = f.holdReceipt(), pending = f.turn(f.alice, 'low_effort_draft_001', 'Draft a reply');
  let settled = false, receiptEntered = false;
  void pending.then(() => { settled = true; }); void barrier.entered.promise.then(() => { receiptEntered = true; });
  try {
    await Promise.race([stream.entered.promise, pending.then(() => { throw Error('fragment_barrier_not_reached'); })]);
    const partialNotes = await f.call(f.alice, 'notes.list', { expectedAccountId: f.a.accountId });
    assert.equal(partialNotes.ok, true); assert.equal(partialNotes.notes.length, 0);
    assert.equal(f.settledReceipts(), 0); assert.equal(receiptEntered, false);
    assert.equal(f.calls.length, 1); assert.equal(settled, false);
    stream.release.resolve();
    await Promise.race([barrier.entered.promise, pending.then(() => { throw Error('accounting_barrier_not_reached'); })]);
    assert.equal(f.calls.length, 1); assert.equal(f.calls[0].body.reasoning_effort, 'low');
    assert.equal(f.calls[0].body.max_tokens, 2048); assert.equal(f.settledReceipts(), 0);
    const notes = await f.call(f.alice, 'notes.list', { expectedAccountId: f.a.accountId });
    assert.equal(notes.ok, true); assert.equal(notes.notes.length, 0, 'a complete parsed draft is not persisted before accounting');
  } finally { stream.release.resolve(); barrier.release.resolve(); }
  const result = await pending; assert.equal(result.ok, true, result.error?.code); assert.equal(result.answer, longDraft);
  assert.equal(f.settledReceipts(), 1); assert.equal(result.accounting[0].status, 'completed');
  assert.equal(result.accounting[0].usage.completionTokens, 1100); assert.equal(result.accounting[0].chargeMicrorubles, '7');
  const stored = JSON.parse((await historyNote(f, f.alice, f.a.accountId)).body).history.map(JSON.parse);
  assert.deepEqual(stored, [{ role: 'user', content: 'Draft a reply' }, { role: 'assistant', content: longDraft }]);
  assert.equal((await f.turn(f.alice, 'low_effort_draft_001', 'Draft a reply')).error.code, 'agent_reconcile_required');
  assert.equal(f.calls.length, 1);
});

for (const prompt of ['Draft a reply', 'Read my notes']) {
  test(`signed HTTP completes and persists a long finish_answer with bounded output: ${prompt}`, async t => {
    const f = await fixture(t, { finishAnswer: { answer: longDraft, completionTokens: 1100 } });
    const result = await f.turn(f.alice, 'long_draft_001', prompt);
    assert.equal(result.ok, true, result.error?.code); assert.equal(result.answer, longDraft);
    assert.equal(f.calls.length, prompt === 'Read my notes' ? 2 : 1);
    assert.ok(f.calls.every(call => call.body.max_tokens === 2048));
    assert.equal(result.accounting.at(-1).usage.completionTokens, 1100);
    const history = JSON.parse((await historyNote(f, f.alice, f.a.accountId)).body).history.map(JSON.parse);
    assert.deepEqual(history, [{ role: 'user', content: prompt }, { role: 'assistant', content: longDraft }]);
    assert.equal((await f.turn(f.alice, 'long_draft_001', prompt)).error.code, 'agent_reconcile_required');
    assert.equal(f.calls.length, prompt === 'Read my notes' ? 2 : 1);
  });
}

for (const maxOutputTokens of [768, 4096]) {
  test(`signed HTTP retains host policy and runtime caps: ${maxOutputTokens}`, async t => {
    const f = await fixture(t, { policy: { maxOutputTokens } });
    assert.equal((await f.turn(f.alice, 'budget_caps_001', 'Read my notes')).ok, true);
    assert.equal(f.calls.length, 2);
    assert.ok(f.calls.every(call => call.body.max_tokens === Math.min(2048, maxOutputTokens)));
  });
}

for (const prompt of ['Draft a reply', 'Read my notes']) {
  for (const keepCompleteJson of [false, true]) {
    test(`signed HTTP length finish fails closed without retry or history: ${prompt}, complete JSON ${keepCompleteJson}`, async t => {
      const f = await fixture(t, { finishAnswer: { answer: longDraft, completionTokens: 1100, forceLength: true, keepCompleteJson } });
      const result = await f.turn(f.alice, 'length_draft_001', prompt);
      assert.equal(result.ok, false); assert.equal(result.error.code, 'agent_model_unknown');
      assert.equal(result.answer, undefined);
      const count = prompt === 'Read my notes' ? 2 : 1;
      assert.equal(f.calls.length, count, 'length failure must not trigger another model call');
      const notes = await f.call(f.alice, 'notes.list', { expectedAccountId: f.a.accountId });
      assert.equal(notes.ok, true); assert.equal(notes.notes.length, 0, 'neither half of the failed pair is saved');
      assert.equal((await f.turn(f.alice, 'length_draft_001', prompt)).error.code, 'agent_reconcile_required');
      assert.equal(f.calls.length, count);
    });
  }
}

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

async function sourceFixture(t, { count = 1 } = {}) {
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
  const capabilityIds = Array.from({ length: count }, (_, index) => `app.${appId}:read${index + 1}`), resources = capabilityIds.map(id => id + ':selected');
  const entries = capabilityIds.map((capabilityId, index) => ({ appId, target: { revision: pin.revision, digest: pin.digest }, adapter,
    catalog: { capabilityId, version: 1, appId, title: `Selected Source read ${index + 1}`, description: 'Read one independently authorized Source resource',
      visibility: 'private', executionEnabled: true, inputSchema: { type: 'object', properties: {}, additionalProperties: false },
      outputSchema: { type: 'object', properties: { text: { type: 'string', maxLength: 1000 } }, required: ['text'], additionalProperties: false },
      resources: [resources[index]], effects: [], recipients: [resources[index]], executionBinding: { kind: 'registered', handler: capabilityId, version: 1,
        binding: { id: capabilityId + ':readonly', version: 1, digest: canonicalHash({ source: 'synthetic', resource: resources[index] }) } } } }));
  const references = captureExternalApplications(entries).map(value => value.contract), reference = references[0];
  await f.restart({ universalAppsEnabled: true, capabilityAudience: f.origin, externalApplications: entries });
  // Actual signed grant/credential consent, kept private in this synthetic fixture. The composition never creates one.
  const principal = (await f.call(f.alice, 'access.principals.create', { expectedAccountId: f.a.accountId, label: 'Explicit fixture reader' })).principal;
  const grant = (await f.call(f.alice, 'access.grants.issue', { expectedAccountId: f.a.accountId, principalId: principal.id,
    capabilities: capabilityIds.map(capabilityId => ({ capabilityId, version: 1 })), resources, effects: [], recipients: resources, expiresAt: Date.now() + 600000,
    allowDelegation: false, maxDepth: 0, budget: { unit: 'invocations', limit: Math.max(4, count + 4) } })).grant;
  const issued = await f.call(f.alice, 'access.credentials.issue', { expectedAccountId: f.a.accountId, grantId: grant.id, audience: f.origin + '/mcp' });
  assert.equal(issued.ok, true);
  // Genuine Root grant above remains distinct from CODE registry and Native port.
  // Exercise the actual installed loader; it derives refs and captures this private callback.
  const installedFile = join(f.directory, 'installed-source.mjs');
  const held = { entries, sourceCredentialFor: () => ({ token: issued.token, audience: f.origin + '/mcp' }), closed: 0 };
  globalThis.__installedAgentSourceFixture = held; t.after(() => { delete globalThis.__installedAgentSourceFixture; });
  const code = `export default async function(api){const held=globalThis.__installedAgentSourceFixture;
    const close=()=>{held.closed++;};api.ownClose(close);
    return {...await api.loadInstalledExistingApplications({createRegistry:()=>held.entries}),close,sourceCredentialFor:held.sourceCredentialFor};}`;
  await writeFile(installedFile, code);
  const installed = await loadInstalledApplicationsEntry({ SOTY_INSTALLED_APPLICATIONS_MODULE: installedFile,
    SOTY_INSTALLED_APPLICATIONS_SHA256: createHash('sha256').update(code).digest('hex') });
  assert.deepEqual(installed.sourceReferences, references); assert.equal(installed.sourceCredentialFor, held.sourceCredentialFor);
  const personalAgent = { ...f.configuration, sourceReferences: installed.sourceReferences, sourceCredentialFor: installed.sourceCredentialFor };
  assert.equal(personalAgent.enabled, f.configuration.enabled); assert.equal(personalAgent.gateway, f.configuration.gateway);
  await f.restart({ universalAppsEnabled: true, capabilityAudience: f.origin, ...installed.httpOptions, personalAgent });
  return { f, references, reference, entries, grant, issued, reads: () => sourceReads, setNativeAllowed(value) { sourceAllowed = value; } };
}

test('actual registered Root Source query tool preserves independent Source denial and foreign app isolation', { timeout: 20000 }, async t => {
  const source = await sourceFixture(t), { f, grant } = source;
  const baseline = await f.turn(f.alice, 'source_good_001', 'Read source'); assert.equal(baseline.ok, true, baseline.error?.code);
  assert.equal(source.reads(), 1); assert.equal(baseline.evidence[0].data.text, 'Private scoped Source result');
  const before = source.reads();
  assert.equal((await f.turn(f.bob, 'source_other_01', 'Read source')).ok, false); assert.equal(source.reads(), before, 'foreign/denied source gets no read');
  source.setNativeAllowed(false);
  assert.equal((await f.turn(f.alice, 'source_denied_1', 'Read source')).ok, false); assert.equal(source.reads(), before, 'foreign/denied source gets no read'); assert.equal(source.reads(), 1);
  source.setNativeAllowed(true);
  const fresh = await f.turn(f.alice, 'source_fresh_01', 'Read source'); assert.equal(fresh.ok, true); assert.equal(source.reads(), 2);
  const barrier = f.hold(), running = f.turn(f.alice, 'source_revoke_1', 'Read source', { contextId: fresh.contextId }); await barrier.entered.promise;
  assert.equal((await f.call(f.alice, 'access.grants.revoke', { expectedAccountId: f.a.accountId, grantId: grant.id })).ok, true);
  barrier.release.resolve(); assert.equal((await running).ok, false); assert.equal(source.reads(), 2, 'current grant must be checked before any further Source read');
});

test('unused actual Root Source grant revoke leaves current-owner Notes and conversation usable', { timeout: 20000 }, async t => {
  const source = await sourceFixture(t), { f, grant } = source;
  const prior = await f.turn(f.alice, 'unused_baseline_1', 'Read source'); assert.equal(prior.ok, true);
  const own = await historyNote(f, f.alice, f.a.accountId);
  assert.equal((await f.call(f.alice, 'access.grants.revoke', { expectedAccountId: f.a.accountId, grantId: grant.id })).ok, true);
  assert.equal((await f.call(f.alice, 'notes.get', { expectedAccountId: f.a.accountId, noteId: own.noteId })).ok, true);
  const reads = source.reads(), next = await f.turn(f.alice, 'unused_notes_001', 'Read my notes', { contextId: prior.contextId });
  assert.equal(next.ok, true, next.error?.code); assert.equal(next.evidence[0].title, 'Мои записки'); assert.equal(source.reads(), reads);
});

test('selected Source SAME credential and Native predicate remain current through the held next model await', { timeout: 20000 }, async t => {
  const source = await sourceFixture(t), { f, grant } = source;
  const baseline = await f.turn(f.alice, 'selected_baseline', 'Read source'); assert.equal(baseline.ok, true);
  const held = f.hold(true), running = f.turn(f.alice, 'selected_native_1', 'Read source'); await held.entered.promise;
  assert.equal(source.reads(), 2); source.setNativeAllowed(false); held.release.resolve(); assert.equal((await running).ok, false);
  source.setNativeAllowed(true);
  const second = f.hold(true), pending = f.turn(f.alice, 'selected_root_001', 'Read source'); await second.entered.promise;
  assert.equal(source.reads(), 3);
  assert.equal((await f.call(f.alice, 'access.grants.revoke', { expectedAccountId: f.a.accountId, grantId: grant.id })).ok, true);
  second.release.resolve(); assert.equal((await pending).ok, false); assert.equal(source.reads(), 3);
  assert.equal((await f.turn(f.alice, 'selected_notes_1', 'Read my notes')).ok, true, 'later Notes-only turn carries no used Source');
});

test('nine admitted Source contracts use constant tools and an eight-selection turn context without a global app cap', { timeout: 20000 }, async t => {
  const source = await sourceFixture(t, { count: 9 }), { f, references } = source;
  const catalog = await f.turn(f.alice, 'nine_catalog_001', 'Catalog sources'); assert.equal(catalog.ok, true, catalog.error?.code);
  assert.equal(catalog.evidence[0].data.items.length, 8); assert.equal(catalog.evidence[0].data.hasMore, true);
  assert.equal(catalog.evidence[0].data.availability, 'unprobed'); assert.equal(source.reads(), 0);
  const names = f.calls[0].body.tools.map(value => value.function.name); assert.equal(names.filter(name => name.startsWith('source_')).length, 2);
  assert.deepEqual(f.calls.at(-1).body.tools.map(value => value.function.name), ['finish_answer']);
  f.setSourceInput({ reference: references[8], input: {} });
  const ninth = await f.turn(f.alice, 'nine_read_001', 'Read source'); assert.equal(ninth.ok, true, ninth.error?.code); assert.equal(source.reads(), 1);
  f.setSourceInput({ reference: { ...references[8], version: '1' }, input: {} });
  assert.equal((await f.turn(f.alice, 'nine_typed_001', 'Read source')).ok, false); assert.equal(source.reads(), 1);
  const foreign = await f.turn(f.bob, 'nine_foreign_01', 'Read source'); assert.equal(foreign.ok, false); assert.equal(source.reads(), 1);
  f.setSourceInput({ query: 'read9', input: {} });
  const callsBeforeUnique = f.calls.length;
  const unique = await f.turn(f.alice, 'nine_unique_001', 'Read source'); assert.equal(unique.ok, true, unique.error?.code); assert.equal(source.reads(), 2);
  const queryMessages = f.calls.slice(callsBeforeUnique).map(value => value.body.messages.at(-1).role);
  assert.deepEqual(queryMessages, ['user', 'tool'], 'one nonterminal query/read then terminal, without Core policy changes');
  f.setSourceInput({ query: 'Selected Source', input: {} });
  assert.equal((await f.turn(f.alice, 'nine_ambiguous_1', 'Read source')).ok, false); assert.equal(source.reads(), 2);
  f.setSourceInput({ query: 'No matching Source at all', input: {} });
  assert.equal((await f.turn(f.alice, 'nine_absent_001', 'Read source')).ok, false); assert.equal(source.reads(), 2);
});

test('Source callback unknown/getter/Promise/cancel failures remain failed while unused callbacks do not gate Notes', { timeout: 20000 }, async t => {
  const source = await sourceFixture(t), { f, references, entries } = source;
  let getters = 0, traps = 0;
  const getter = new Error('PRIVATE_ERROR_CANARY'); Object.defineProperty(getter, 'code', { get() { getters++; throw Error('PRIVATE_ERROR_CANARY'); } });
  const proxy = new Proxy(new Error('PRIVATE_ERROR_CANARY'), { get() { traps++; throw Error('PRIVATE_ERROR_CANARY'); } });
  const callbacks = [() => { throw getter; }, () => { throw proxy; }, () => Promise.reject(Error('PRIVATE_ERROR_CANARY')), () => { throw agentError('agent_cancelled'); }];
  for (const [index, callback] of callbacks.entries()) {
    await f.restart({ universalAppsEnabled: true, capabilityAudience: f.origin, externalApplications: entries,
      personalAgent: { ...f.configuration, sourceReferences: references, sourceCredentialFor: callback } });
    const catalogue = await f.turn(f.alice, `unknown_catalog_${index}`, 'Catalog sources');
    assert.equal(catalogue.ok, false); assert.equal(JSON.stringify(catalogue).includes('PRIVATE_ERROR_CANARY'), false);
    if (index === 3) assert.equal(catalogue.error.code, 'agent_cancelled', 'cancellation is not hidden permission');
    assert.equal((await f.turn(f.alice, `unknown_notes_${index}`, 'Read my notes')).ok, true);
    assert.equal(source.reads(), 0);
  }
  assert.equal(getters, 0); assert.equal(traps, 0);
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
  const barrier = f.hold(), turn = ui.turn('Held shared client turn');
  // Cancellation may reject the held turn before its own signed ACK returns.
  // Observe that outcome immediately without releasing the provider barrier.
  const turnOutcome = Promise.allSettled([turn]); await barrier.entered.promise;
  const cancellation = ui.cancel();
  try {
    const observed = await Promise.race([cancellation.then(value => ({ responded: true, value })), new Promise(done => setTimeout(() => done({ responded: false }), 400))]);
    assert.equal(observed.responded, true, 'shared-client cancel must reach the signed server handler before provider release');
    assert.equal(observed.value.cancelRequested, true); assert.equal(observed.value.remoteTerminationVerified, false);
  } finally { barrier.release.resolve(); await Promise.allSettled([turnOutcome, cancellation]); }
  const [cancelledTurn] = await turnOutcome;
  assert.equal(cancelledTurn.status, 'rejected'); assert.equal(cancelledTurn.reason.code, 'agent_cancelled');
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
