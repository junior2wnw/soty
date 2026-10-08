import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, sign, randomUUID, createHash } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve, dirname } from 'node:path';
import { pathToFileURL } from 'node:url';
import { createConnectService, digestArgs } from '../../modules/connect/server/index.mjs';
import { createPersonalAgentModule } from '../personal-agent-module.js';
import { createPersonalAgentTransport } from '../personal-agent-transport.js';
import { GLM_MODEL } from '../../modules/personal-agent/provider/index.mjs';

// Explicit readonly donor imports; no source copying, npm install or network. The actual TS runtime is mandatory.
assert.equal(typeof process.env.SOTY_AGENT_RUNTIME_SOURCE === 'string' && typeof process.env.SOTY_AGENT_COMMERCE_SOURCE === 'string', true,
  'qualified_agent_runtime_and_commerce_source_fixtures_required');
const runtimeRoot = resolve(process.env.SOTY_AGENT_RUNTIME_SOURCE);
const gatewayRoot = resolve(process.env.SOTY_AGENT_COMMERCE_SOURCE);
const { runBoundedRuntimeTurn, createIsolatedContextStore } = await import(pathToFileURL(join(runtimeRoot, 'packages/runtime/src/agent-runtime-core.ts')));
const { createApp } = await import(pathToFileURL(join(gatewayRoot, 'apps/gateway/src/app.mjs')));
const defer = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
const tick = () => new Promise(done => setImmediate(done));
const identity = () => {
  const s = generateKeyPairSync('ec', { namedCurve: 'prime256v1' }), e = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { signing: s.privateKey, publicJwk: s.publicKey.export({ format: 'jwk' }), encryptionPublicJwk: e.publicKey.export({ format: 'jwk' }) };
};

async function fixture(t, options = {}) {
  const directory = mkdtempSync(join(resolve(tmpdir()), 'soty-agent-entry-'));
  const flags = { sourceAllowed: true, modelAllowed: true, credentialTenantMatches: true, receiptTenantMatches: true,
    responseLost: false, hold: null, toolEffects: 0, connectorCalls: 0, reserves: 0, finishes: 0, fetchCalls: 0, appendCalls: 0 };
  const tenant = randomUUID(), key = { id: randomUUID(), tenant_id: tenant, allowed_paths: ['/v1'], limits: { maxOutputTokens: 8192 } };
  const bearer = 'synthetic-only-not-a-real-inference-key', session = 'synthetic-only-not-a-real-management-session';
  const records = new Map(), idempotency = new Map(), keyRef = Object.freeze({});
  const store = {
    pepper: 'synthetic-only-pepper-not-a-real-secret', getSession: async value => value === session ? { tenantId: tenant, expiresAt: Date.now() + 60000 } : null,
    authenticate: async value => { assert.equal(value === bearer, true); return key; },
    pool: { query: async (sql, values) => {
      if (sql.includes('SELECT settings FROM tenants')) return { rows: [{ settings: {} }] };
      const record = records.get(values[1]); return { rows: record && values[0] === tenant ? [record] : [], rowCount: record && values[0] === tenant ? 1 : 0 };
    } },
    reserve: async input => {
      flags.reserves++; const body = JSON.stringify(input.body), digest = createHash('sha256').update(body).digest('hex');
      const previous = idempotency.get(input.idempotencyKey);
      if (previous) { assert.equal(previous.digest === digest, true); return { replay: true, request: records.get(previous.id) }; }
      const id = randomUUID(); records.set(id, { id, status: 'running', model_requested: GLM_MODEL, model_resolved: GLM_MODEL,
        input_tokens: null, output_tokens: null, charge_urub: '0' }); idempotency.set(input.idempotencyKey, { digest, id }); return { id };
    },
    finish: async (id, result) => {
      flags.finishes++; if (flags.finishHold) { flags.finishHold.entered.resolve(); await flags.finishHold.release.promise; }
      const record = records.get(id); record.status = result.outcome;
      if (result.usage) { record.input_tokens = result.usage.prompt_tokens; record.output_tokens = result.usage.completion_tokens; }
      record.charge_urub = result.outcome === 'completed' ? '7' : '0'; return { chargeMicrorubles: record.charge_urub };
    },
  };
  const usage = { prompt_tokens: 3, completion_tokens: 2, total_tokens: 5 };
  const connector = {
    models: () => [{ id: GLM_MODEL, inputPriceMicrorublesPerMillion: '1', outputPriceMicrorublesPerMillion: '1' }],
    complete: async ({ body, policy }) => {
      assert.equal(policy.autoFallback, false);
      flags.connectorCalls++;
      if (flags.hold) { flags.hold.entered.resolve(); await flags.hold.release.promise; }
      const last = body.messages.at(-1), read = last.role === 'user' && last.content === 'Read the guide';
      const args = read ? { articleId: 'safe' } : { answer: last.role === 'tool' ? 'Grounded answer' : 'Neutral answer',
        selectedEvidenceIds: last.role === 'tool' ? ['evidence_one'] : [], claims: last.role === 'tool' ? [{ kind: 'fact', statement: 'The guide exists',
          evidenceIds: ['evidence_one'], freshness: 'current', confidence: 'high', applicability: 'Synthetic support' }] : [] };
      const content = `data: ${JSON.stringify({ id: 'chatcmpl_synthetic', model: GLM_MODEL, choices: [{ index: 0,
        delta: { tool_calls: [{ index: 0, id: 'call_synthetic', type: 'function', function: { name: read ? 'lookup_article' : 'finish_answer', arguments: JSON.stringify(args) } }] }, finish_reason: null }] })}\n\n`
        + `data: ${JSON.stringify({ id: 'chatcmpl_synthetic', model: GLM_MODEL, choices: [{ index: 0, delta: {}, finish_reason: 'tool_calls' }], usage })}\n\n` + 'data: [DONE]\n\n';
      return { kind: 'stream', model: GLM_MODEL, providerId: 'synthetic', stream: new ReadableStream({ start(controller) {
        controller.enqueue(new TextEncoder().encode(content)); controller.close(); } }), completion: Promise.resolve({ outcome: 'success', usage }) };
    },
  };
  const app = await createApp({ store, connector, baseUrl: 'https://gateway-fixture.invalid', installEnabled: false });
  // One active owned loopback gateway per sequential fixture. inject() misreports SSE writableFinished;
  // the genuine HTTP writer must settle before we accept an accounting receipt. No provider/network beyond loopback.
  const gatewayOrigin = await app.listen({ port: 0, host: '127.0.0.1' });
  let connect;
  const transport = createPersonalAgentTransport({ gatewayOrigin, allowLoopbackFixture: true, billingTenant: tenant, keyRef,
    authorizeActor: () => flags.modelAllowed,
    resolveCredential: async () => ({ keyRef, tenantId: flags.credentialTenantMatches ? tenant : randomUUID(), bearer }),
    fetchImpl: async (url, init) => {
      flags.fetchCalls++; assert.equal(url === `${gatewayOrigin}/v1/chat/completions`, true);
      const response = await fetch(url, init);
      if (flags.responseLost) throw new Error('private_sentinel');
      return response;
    },
    readReceipt: async ({ requestId, billingTenant, keyRef: ref, signal }) => {
      assert.equal(billingTenant === tenant && ref === keyRef, true);
      const reply = await fetch(`${gatewayOrigin}/api/requests/${requestId}`, { signal, headers: { cookie: `soty_workspace=${session}` } });
      flags.receiptStatus = reply.status;
      assert.equal(reply.status, 200);
      const request = (await reply.json()).request;
      flags.receiptCompleted = request.status === 'completed'; flags.receiptUsageMatches = request.input_tokens === 3 && request.output_tokens === 2;
      if (request.status === 'running') flags.finishHold?.read.resolve();
      flags.receiptModelMatches = request.model_requested === GLM_MODEL && request.model_resolved === GLM_MODEL;
      return { tenantId: flags.receiptTenantMatches ? tenant : randomUUID(), request };
    },
  });
  const runtime = { runTurn: runBoundedRuntimeTurn, createContextStore: options => {
    const value = createIsolatedContextStore(options); return { ...value, append(...args) { flags.appendCalls++; return value.append(...args); } };
  }, outputProfile: 'host-enforced-terminal.v1', adapterFor: ({ tools }) => ({ id: 'soty-synthetic-support', roles: ['human'],
    capabilities: Object.values(tools).map(tool => tool.capability), applicability: 'Synthetic support',
    systemContext: terminal => `Use the exact ${terminal} tool for a complete neutral answer`, isActionHrefAllowed: href => href.startsWith('/help/') }) };
  const moduleOptions = { getConnect: () => connect, runtime, transport: options.disabled ? undefined : transport,
    toolsForActor: () => ({ assertCurrent: () => flags.sourceAllowed, tools: { lookup_article: {
      capability: 'knowledge_read', description: 'Read an approved synthetic guide', inputSchema: { type: 'object', additionalProperties: false,
        properties: { articleId: { type: 'string' } }, required: ['articleId'] },
      validate: args => args.articleId === 'safe', execute: async (_input, signal) => {
        if (flags.toolHold) { flags.toolHold.entered.resolve(); await flags.toolHold.release.promise;
          if (signal.aborted) throw new Error('synthetic_source_cancelled_before_effect'); }
        flags.toolEffects++; return {
        evidence: [{ id: 'evidence_one', title: 'Guide', observedAt: null, sourceStatus: 'current' }],
        cards: [{ kind: 'article', title: 'Guide', href: '/help/guide', freshness: 'current', asOf: null, sourceStatus: 'current', evidenceIds: ['evidence_one'] }], actions: [] };
      },
    } } }), policy: { timeoutMs: 5000 } };
  let module = createPersonalAgentModule(moduleOptions);
  connect = createConnectService({ databasePath: join(directory, 'accounts.sqlite'), projectId: 'soty', allowedOrigins: ['https://root-fixture.invalid'], extensions: [module] });
  async function call(actor, op, args) {
    const challenge = await connect.handle({ origin: 'https://root-fixture.invalid', op: 'challenge', args: { operation: op, digest: digestArgs(args) } });
    assert.equal(challenge.ok, true);
    return connect.handle({ origin: 'https://root-fixture.invalid', op, args, proof: { challengeId: challenge.challengeId, publicJwk: actor.publicJwk,
      signature: sign('sha256', Buffer.from(challenge.message), { key: actor.signing, dsaEncoding: 'ieee-p1363' }).toString('base64url') } });
  }
  async function bootstrap(actor) { const value = await call(actor, 'bootstrap', { label: 'Synthetic human', encryptionPublicJwk: actor.encryptionPublicJwk }); assert.equal(value.ok, true); return value; }
  const alice = identity(), bob = identity(), a = await bootstrap(alice); await bootstrap(bob);
  t.after(async () => {
    module.close(); transport.close(); flags.hold?.release.resolve(); flags.finishHold?.release.resolve(); flags.toolHold?.release.resolve(); await app.close(); connect.close();
    assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.equal(directory.startsWith(join(resolve(tmpdir()), 'soty-agent-entry-')), true);
    rmSync(directory, { recursive: true, force: true });
  });
  return { module, transport, connect, flags, app, alice, bob, a, call, records,
    turn: (actor, id, changes = {}) => call(actor, 'agent.turn', { requestId: id, prompt: 'Neutral question', ...changes }),
    async revoker() { const phone = identity(); const started = await call(phone, 'enrollment.start', { label: 'Synthetic phone', encryptionPublicJwk: phone.encryptionPublicJwk });
      assert.equal(started.ok, true); assert.equal((await call(alice, 'enrollment.approve', { requestId: started.requestId, wrappedKey: { schema: 'synthetic.encrypted', ciphertext: 'opaque' } })).ok, true);
      assert.equal((await call(phone, 'enrollment.finish', { requestId: started.requestId, expectedAccountId: a.accountId })).ok, true); return phone; },
    restartRoot() { module.close(); connect.close(); module = createPersonalAgentModule(moduleOptions);
      connect = createConnectService({ databasePath: join(directory, 'accounts.sqlite'), projectId: 'soty', allowedOrigins: ['https://root-fixture.invalid'], extensions: [module] }); },
  };
}

test('actual signed Connect + actual generic runtime + actual gateway reserve/finish yields an accounted text turn', async t => {
  const f = await fixture(t); const value = await f.turn(f.alice, 'turn_text_0001'); assert.equal(value.ok, true,
    JSON.stringify({ code: value.error?.code, receiptStatus: f.flags.receiptStatus ?? null, completed: f.flags.receiptCompleted,
      usageMatches: f.flags.receiptUsageMatches, modelMatches: f.flags.receiptModelMatches, finishes: f.flags.finishes, appends: f.flags.appendCalls }));
  assert.equal(value.answer, 'Neutral answer'); assert.equal(value.accounting[0].status, 'completed'); assert.equal(value.accounting[0].chargeMicrorubles, '7');
  assert.equal(f.flags.connectorCalls, 1); assert.equal(f.flags.reserves, 1); assert.equal(f.flags.finishes, 1); assert.equal(f.flags.appendCalls, 2);
  assert.equal(f.module.status().wholeReady, false);
});
test('actual runtime uses the current Source read tool then validates terminal evidence and scoped actions', async t => {
  const f = await fixture(t); const value = await f.turn(f.alice, 'turn_tools_0001', { prompt: 'Read the guide' }); assert.equal(value.ok, true, value.error?.code);
  assert.equal(value.answer, 'Grounded answer'); assert.equal(value.evidence.length, 1); assert.equal(value.cards.length, 1);
  assert.equal(f.flags.toolEffects, 1); assert.equal(f.flags.connectorCalls, 2); assert.equal(value.accounting.length, 2);
});
test('foreign context, spoofed actor and model/tenant payload never acquire authority', async t => {
  const f = await fixture(t); const own = await f.turn(f.alice, 'turn_owner_0001'); assert.equal(own.ok, true);
  const foreign = await f.turn(f.bob, 'turn_foreign_001', { contextId: own.contextId }); assert.equal(foreign.ok, false);
  assert.equal(f.flags.connectorCalls, 1); assert.equal(f.flags.appendCalls, 2);
  await assert.rejects(f.module.executeAsync({ op: 'agent.turn', actor: { accountId: f.a.accountId, deviceId: f.a.deviceId }, args: { requestId: 'turn_spoof_0001', prompt: 'Question' } }));
  const spoofed = await f.turn(f.alice, 'turn_payload_01', { tenantId: 'foreign', model: 'foreign' }); assert.equal(spoofed.ok, false); assert.equal(f.flags.connectorCalls, 1);
});
test('missing trusted transport is explicit default-off, even with a genuine signed actor', async t => {
  const f = await fixture(t, { disabled: true }); const value = await f.turn(f.alice, 'turn_disabled_1');
  assert.equal(value.ok, false); assert.equal(value.error.code, 'agent_transport_unavailable'); assert.equal(f.flags.reserves, 0);
  assert.equal(f.module.status().configured, false); assert.equal(f.module.status().wholeReady, false);
});
test('Root device revoke at provider barrier denies delivery, context and tool effects after await', async t => {
  const f = await fixture(t); const owner = await f.revoker(); f.flags.hold = { entered: defer(), release: defer() };
  const pending = f.turn(f.alice, 'turn_revoked_01', { prompt: 'Read the guide' }); await f.flags.hold.entered.promise;
  assert.equal((await f.call(owner, 'device.revoke', { deviceId: f.a.deviceId })).ok, true);
  f.flags.hold.release.resolve(); const value = await pending; assert.equal(value.ok, false); assert.equal(value.error.code, 'agent_actor_denied');
  assert.equal(f.flags.toolEffects, 0); assert.equal(f.flags.appendCalls, 0); await tick();
});
test('Source permission revoke at provider barrier denies before the actual tool effect', async t => {
  const f = await fixture(t); f.flags.hold = { entered: defer(), release: defer() };
  const pending = f.turn(f.alice, 'turn_source_001', { prompt: 'Read the guide' }); await f.flags.hold.entered.promise;
  f.flags.sourceAllowed = false; f.flags.hold.release.resolve(); const value = await pending;
  assert.equal(value.ok, false); assert.equal(value.error.code, 'agent_source_denied'); assert.equal(f.flags.toolEffects, 0); assert.equal(f.flags.appendCalls, 0);
});
test('Source credential and receipt tenant mismatches reject without pretending user/publisher equivalence', async t => {
  const f = await fixture(t); f.flags.credentialTenantMatches = false; const before = await f.turn(f.alice, 'turn_keyscope_1'); assert.equal(before.ok, false); assert.equal(f.flags.reserves, 0);
  f.flags.credentialTenantMatches = true; f.flags.receiptTenantMatches = false; const after = await f.turn(f.alice, 'turn_receipt_01');
  assert.equal(after.ok, false); assert.equal(after.error.code, 'agent_key_binding_invalid'); assert.equal(f.flags.appendCalls, 0); assert.equal(f.flags.connectorCalls, 1);
});
test('lost ACK after actual gateway completion remains unknown and exact retry never redispatches', async t => {
  const f = await fixture(t); f.flags.responseLost = true; const first = await f.turn(f.alice, 'turn_unknown_01'); assert.equal(first.ok, false);
  assert.equal(first.error.code, 'agent_model_unknown'); assert.equal(f.flags.connectorCalls, 1); assert.equal(f.flags.finishes, 1); assert.equal(f.flags.appendCalls, 0);
  const retry = await f.turn(f.alice, 'turn_unknown_01'); assert.equal(retry.error.code, 'agent_reconcile_required'); assert.equal(f.flags.connectorCalls, 1);
  const conflict = await f.turn(f.alice, 'turn_unknown_01', { prompt: 'Changed intent' }); assert.equal(conflict.error.code, 'agent_intent_conflict'); assert.equal(f.flags.connectorCalls, 1);
  assert.equal(JSON.stringify(first).includes('private_sentinel'), false);
});
test('Root restart preserves Source idempotency key: existing gateway refuses replay without a second model call', async t => {
  const f = await fixture(t); const first = await f.turn(f.alice, 'turn_restart_01'); assert.equal(first.ok, true, first.error?.code);
  f.restartRoot(); const again = await f.turn(f.alice, 'turn_restart_01'); assert.equal(again.ok, false); assert.equal(again.error.code, 'agent_model_unknown');
  assert.equal(f.flags.reserves, 2); assert.equal(f.flags.connectorCalls, 1); assert.equal(f.flags.finishes, 1);
});
test('current model ACL denial and module close prevent new model dispatches', async t => {
  const f = await fixture(t); assert.equal((await f.turn(f.alice, 'turn_current_01')).ok, true);
  f.flags.modelAllowed = false; const denied = await f.turn(f.alice, 'turn_acl_00001'); assert.equal(denied.error.code, 'agent_model_access_denied');
  assert.equal(f.flags.connectorCalls, 1); f.module.close(); const closed = await f.turn(f.alice, 'turn_closed_01'); assert.equal(closed.error.code, 'agent_closed');
  assert.equal(f.flags.connectorCalls, 1);
});
test('actual gateway body completes before finish persistence; bounded readonly wait accepts only the later real receipt', async t => {
  const f = await fixture(t); f.flags.finishHold = { entered: defer(), read: defer(), release: defer() };
  const pending = f.turn(f.alice, 'turn_finisher_01'); await f.flags.finishHold.entered.promise; await f.flags.finishHold.read.promise;
  assert.equal(f.flags.receiptCompleted, false); assert.equal(f.flags.appendCalls, 0); assert.equal(f.flags.connectorCalls, 1);
  f.flags.finishHold.release.resolve(); const value = await pending; assert.equal(value.ok, true, value.error?.code);
  assert.equal(value.accounting[0].status, 'completed'); assert.equal(f.flags.connectorCalls, 1); assert.equal(f.flags.finishes, 1);
});
test('actual signed owner cancel stops a held model turn locally; foreign account cannot cancel or inspect it', async t => {
  const f = await fixture(t); f.flags.hold = { entered: defer(), release: defer() };
  const pending = f.turn(f.alice, 'turn_cancel_001', { prompt: 'Read the guide' }); await f.flags.hold.entered.promise;
  const foreign = await f.call(f.bob, 'agent.cancel', { requestId: 'turn_cancel_001' });
  assert.equal(foreign.ok, false); assert.equal(foreign.error.code, 'agent_turn_unavailable');
  const own = await f.call(f.alice, 'agent.cancel', { requestId: 'turn_cancel_001' });
  assert.equal(own.ok, true); assert.equal(own.cancelRequested, true); assert.equal(own.remoteTerminationVerified, false);
  f.flags.hold.release.resolve(); const value = await pending;
  assert.equal(value.ok, false); assert.equal(value.error.code, 'agent_cancelled'); assert.equal(f.flags.appendCalls, 0); assert.equal(f.flags.toolEffects, 0);
  assert.equal(f.flags.connectorCalls, 1); assert.equal(f.flags.reserves, 1);
  const retry = await f.turn(f.alice, 'turn_cancel_001', { prompt: 'Read the guide' }); assert.equal(retry.error.code, 'agent_reconcile_required');
  assert.equal(f.flags.connectorCalls, 1);
});
test('signed cancel at the actual runtime Source tool barrier preserves no-effect and no-second-round boundary', async t => {
  const f = await fixture(t); f.flags.toolHold = { entered: defer(), release: defer() };
  const pending = f.turn(f.alice, 'turn_toolcancel_1', { prompt: 'Read the guide' }); await f.flags.toolHold.entered.promise;
  assert.equal((await f.call(f.alice, 'agent.cancel', { requestId: 'turn_toolcancel_1' })).cancelRequested, true);
  f.flags.toolHold.release.resolve(); const value = await pending;
  assert.equal(value.ok, false); assert.equal(f.flags.toolEffects, 0); assert.equal(f.flags.appendCalls, 0);
  assert.equal(f.flags.connectorCalls, 1); assert.equal(f.flags.reserves, 1);
});
