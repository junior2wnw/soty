import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, realpathSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { createCapabilitiesService } from '../server/index.mjs';
import { createCatalog } from '../server/catalog.mjs';
import { EXTERNAL_ADAPTER_PROFILE } from '../server/external-adapters.mjs';
import { captureExternalAdapters, createExternalAdapterCoordinator } from '../server/external-adapters.mjs';
import { fixtureDocumentation } from './support/documentation.mjs';

const CAP = {
  capabilityId: 'app.planner:createWorkItem', version: 1, appId: 'planner', title: 'Создать рабочий объект',
  description: 'Добавить объект в одно разрешённое пространство Планировщика.', visibility: 'private', executionEnabled: true,
  inputSchema: { type: 'object', properties: { title: { type: 'string', maxLength: 500 } }, required: ['title'], additionalProperties: false },
  outputSchema: { type: 'object', properties: { objectId: { type: 'string', maxLength: 160 }, revision: { type: 'integer', minimum: 0 } }, required: ['objectId', 'revision'], additionalProperties: false },
  resources: ['app.planner:workspace-a'], effects: ['create'], recipients: ['app.planner:workspace-a'],
  executionBinding: { kind: 'registered', handler: 'app.planner:createWorkItem', version: 1,
    binding: { id: 'app.planner:workspace-a-binding', version: 1, digest: 'b'.repeat(64) } },
};
const CONTRACT = Object.freeze({ capabilityId: CAP.capabilityId, version: CAP.version, digest: createCatalog([CAP]).get(CAP.capabilityId, CAP.version).digest });
const OWNER = Object.freeze({ accountId: 'account_a', deviceId: 'device_a' });
const AUDIENCE = 'https://soty.test';

function harness(t, { withAuthority, readProof, execute } = {}) {
  const parent = realpathSync(tmpdir()), directory = realpathSync(mkdtempSync(join(parent, 'soty-external-adapter-')));
  const nonce = randomUUID(), marker = join(directory, 'fixture-owner'); writeFileSync(marker, nonce, { flag: 'wx' });
  const file = join(directory, 'capabilities.sqlite');
  const effects = new Map(); let sourceCalls = 0, sourceReads = 0, sourceAllowed = true, deviceActive = true;
  // This is an explicit synthetic source. Real Planner HTTP integration is a
  // separate suite; these tests exercise the real Caps ACL/budget/SQLite ledger.
  const adapter = {
    profile: EXTERNAL_ADAPTER_PROFILE,
    withAuthority: withAuthority ?? ((_request, callback) => { assert.equal(sourceAllowed, true, 'source_workspace_revoked'); return callback(); }),
    async execute(request, signal) {
      assert.equal(signal.aborted, false); sourceCalls++;
      if (execute) return execute(request, effects);
      const old = effects.get(request.requestId);
      if (old) { assert.equal(old.inputDigest, request.inputDigest); return; }
      effects.set(request.requestId, { requestId: request.requestId, inputDigest: request.inputDigest, outcome: 'committed',
        effects: [{ kind: 'created', resourceType: 'planner-object', resourceId: 'object_a', revision: 1 }],
        receipt: { verificationMethod: 'domain_read', artifacts: [{ type: 'planner-object', id: 'object_a', revision: 1 }] } });
    },
    async readProof(request) {
      sourceReads++;
      if (readProof) return readProof(request, effects);
      return effects.get(request.requestId) ?? { requestId: request.requestId, inputDigest: request.inputDigest, outcome: 'not_applied' };
    },
  };
  const create = (overrides = {}) => createCapabilitiesService({ databasePath: file, projectId: 'external-test',
    actorActive: actor => actor.accountId === OWNER.accountId && actor.deviceId === OWNER.deviceId && deviceActive,
    catalog: [CAP], documentation: fixtureDocumentation([CAP]), externalAdapters: [{ contract: CONTRACT, adapter }], ...overrides });
  let service = create();
  const call = (op, args = {}) => service.execute({ op: `access.${op}`, actor: OWNER, args: { expectedAccountId: OWNER.accountId, ...args } });
  const principal = call('principals.create', { label: 'Синтетический агент' }).principal;
  const grant = call('grants.issue', { principalId: principal.id, capabilities: [{ capabilityId: CAP.capabilityId, version: 1 }],
    resources: CAP.resources, effects: CAP.effects, recipients: CAP.recipients, expiresAt: Date.now() + 600000,
    allowDelegation: false, maxDepth: 0, budget: { unit: 'invocations', limit: 4 } }).grant;
  const issued = call('credentials.issue', { grantId: grant.id, audience: AUDIENCE });
  const actor = () => service.authenticateCredential({ token: issued.token, audience: AUDIENCE });
  const request = () => ({ actor: actor(), reference: CONTRACT, idempotencyKey: 'work_item_0001', input: { title: 'Личная задача' } });
  t.after(() => {
    service.close(); assert.equal(dirname(realpathSync(directory)), parent); assert.match(basename(directory), /^soty-external-adapter-/u);
    assert.equal(readFileSync(marker, 'utf8'), nonce); rmSync(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  });
  return { get service() { return service; }, adapter, effects, actor, request, call, grant, issued,
    calls: () => ({ writes: sourceCalls, reads: sourceReads }),
    revokeSource() { sourceAllowed = false; }, revokeDevice() { deviceActive = false; },
    reopen(overrides) { service.close(); service = create(overrides); },
    sql(fn) { const db = new DatabaseSync(file, { readOnly: true }); try { return fn(db); } finally { db.close(); } },
  };
}

test('existing real grant, quota and ledger commit one source effect; restart/replay keep its identity', async t => {
  const f = harness(t), first = await f.service.external.invoke(f.request());
  assert.equal(first.invocation.status, 'succeeded'); assert.equal(first.invocation.receipt.verificationMethod, 'domain_read');
  assert.equal(f.calls().writes, 1); assert.equal(f.effects.size, 1);
  f.reopen(); const next = await f.service.external.invoke(f.request());
  assert.equal(next.reused, true); assert.equal(next.invocation.invocationId, first.invocation.invocationId);
  assert.equal(f.calls().writes, 1);
  const stored = f.sql(db => ({ receipts: db.prepare('SELECT count(*) AS n FROM cap_receipts').get().n,
    reservations: db.prepare('SELECT count(*) AS n FROM cap_budget_reservations').get().n }));
  assert.deepEqual(stored, { receipts: 1, reservations: 1 });
  const raw = JSON.stringify(next); for (const forbidden of ['Личная задача', issuedToken(f), 'authorization', 'internalRequestId']) assert.equal(raw.includes(forbidden), false);
  await assert.rejects(f.service.external.invoke({ ...f.request(), input: { title: 'Другая задача' } }), /invocation_request_conflict/u);
});
const issuedToken = f => f.issued.token;

test('lost source ACK and process restart recover only durable proof without repeating execution', async t => {
  let down = false;
  const f = harness(t, { execute(request, effects) {
    effects.set(request.requestId, { requestId: request.requestId, inputDigest: request.inputDigest, outcome: 'committed',
      effects: [{ kind: 'created', resourceType: 'planner-object', resourceId: 'object_b', revision: 7 }],
      receipt: { verificationMethod: 'domain_read', artifacts: [{ type: 'planner-object', id: 'object_b', revision: 7 }] } });
    down = true; throw new Error('synthetic_lost_ack');
  }, readProof(request, effects) {
    if (down) throw new Error('source_disconnected');
    return effects.get(request.requestId) ?? { requestId: request.requestId, inputDigest: request.inputDigest, outcome: 'not_applied' };
  } });
  const first = await f.service.external.invoke(f.request()); assert.equal(first.invocation.effectState, 'unknown');
  assert.equal(f.calls().writes, 1); f.reopen(); down = false;
  const recovered = await f.service.external.reconcile({ reference: CONTRACT, invocationId: first.invocation.invocationId });
  assert.equal(recovered.outcome, 'committed'); assert.equal(f.calls().writes, 1);
  assert.equal(f.service.external.get({ actor: f.actor(), invocationId: first.invocation.invocationId }).invocation.status, 'succeeded');
});

test('root revoke before dispatch or source revoke after admission sends no effect', async t => {
  for (const target of ['root', 'source']) {
    const f = harness(t), request = f.request(), first = f.service.external.admit(request);
    if (target === 'root') f.call('grants.revoke', { grantId: f.grant.id }); else f.revokeSource();
    await assert.rejects(f.service.external.invoke(request)); assert.equal(f.calls().writes, 0);
    assert.equal(f.effects.size, 0);
    // Recovery cannot turn a pending admission into a send or fabricate a negative result.
    await f.service.external.reconcile({ reference: CONTRACT, invocationId: first.invocation.invocationId });
    assert.equal(f.calls().writes, 0);
  }
});

test('root revoke after source COMMIT settles historical proof and denies the caller', async t => {
  let f;
  f = harness(t, { execute(request, effects) {
    effects.set(request.requestId, { requestId: request.requestId, inputDigest: request.inputDigest, outcome: 'committed',
      effects: [{ kind: 'created', resourceType: 'planner-object', resourceId: 'object_c', revision: 1 }],
      receipt: { verificationMethod: 'domain_read', artifacts: [{ type: 'planner-object', id: 'object_c', revision: 1 }] } });
    f.call('grants.revoke', { grantId: f.grant.id });
  } });
  await assert.rejects(f.service.external.invoke(f.request()), /access_denied/u);
  assert.equal(f.sql(db => db.prepare('SELECT status FROM cap_invocations').get().status), 'succeeded');
  assert.equal(f.calls().writes, 1);
});

test('cancellation during proof, false receipts and wrong intent cannot create/settle work', async t => {
  let release, f, started;
  const barrier = new Promise(resolve => { release = resolve; });
  const entered = new Promise(resolve => { started = resolve; });
  f = harness(t, { async readProof(request) { started(); await barrier;
    return { requestId: request.requestId, inputDigest: request.inputDigest, outcome: 'not_applied' }; } });
  const request = f.request(), admitted = f.service.external.admit(request), running = f.service.external.invoke(request);
  await entered; f.service.external.cancel({ actor: request.actor, invocationId: admitted.invocation.invocationId }); release();
  const result = await running; assert.equal(result.invocation.cancelRequested, true); assert.equal(result.invocation.effectState, 'unknown'); assert.equal(f.calls().writes, 0);
  for (const kind of ['request', 'digest', 'effect', 'verification']) {
    const g = harness(t, { readProof(context) { return { requestId: kind === 'request' ? 'another_request' : context.requestId,
      inputDigest: kind === 'digest' ? 'f'.repeat(64) : context.inputDigest, outcome: 'committed',
      effects: [{ kind: kind === 'effect' ? 'deleted' : 'created', resourceType: 'planner-object', resourceId: 'object_invalid', revision: 1 }],
      receipt: { verificationMethod: kind === 'verification' ? 'handler_assertion' : 'domain_read', artifacts: [{ type: 'planner-object', id: 'object_invalid', revision: 1 }] } }; } });
    const held = await g.service.external.invoke(g.request()); assert.equal(held.invocation.effectState, 'unknown');
    assert.equal(held.invocation.receipt, undefined); assert.equal(g.calls().writes, 0);
  }
});

test('concurrent same request shares one delivery and cannot replace captured input/actor', async t => {
  let release, started;
  const barrier = new Promise(resolve => { release = resolve; }), entered = new Promise(resolve => { started = resolve; });
  const f = harness(t, { async execute(request, effects) { started(); await barrier;
    assert.equal(request.input.title, 'Личная задача');
    effects.set(request.requestId, { requestId: request.requestId, inputDigest: request.inputDigest, outcome: 'committed',
      effects: [{ kind: 'created', resourceType: 'planner-object', resourceId: 'object_d', revision: 1 }],
      receipt: { verificationMethod: 'domain_read', artifacts: [{ type: 'planner-object', id: 'object_d', revision: 1 }] } }); } });
  const request = f.request(), first = f.service.external.invoke(request); await entered;
  request.input.title = 'Подмена'; request.actor = { accountId: 'intruder' };
  const second = f.service.external.invoke(f.request()); release();
  const [a,b] = await Promise.all([first,second]); assert.equal(a.invocation.invocationId,b.invocation.invocationId); assert.equal(f.calls().writes,1);
});

test('late/missing/async authority closures fail before source effects; committed admission may replay', async t => {
  let late;
  for (const callback of [(_request, next) => { late = next; }, (_request, next) => { const result = next(); next(); return result; },
    (_request, _next) => Promise.resolve({ forged: true })]) {
    const f = harness(t, { withAuthority: callback });
    assert.throws(() => f.service.external.admit(f.request()), /external_authority_invalid/u);
    assert.equal(f.calls().writes, 0);
  }
  assert.throws(() => late(), /external_authority_invalid/u);
});

test('swallowed duplicate authority entry poisons the whole frame and never authorizes source execution', async t => {
  const f = harness(t, { withAuthority(_request, next) { const result = next(); try { next(); } catch { /* adversarial swallowed error */ } return result; } });
  await assert.rejects(f.service.external.invoke(f.request()), /external_authority_invalid/u);
  assert.equal(f.calls().writes, 0);
  assert.equal(f.sql(db => db.prepare('SELECT count(*) AS n FROM cap_receipts').get().n), 0);
});

test('ignored abort retains its source slot after timeout; repeated callers cannot accumulate orphans', async t => {
  let release;
  const blocked = new Promise(resolve => { release = resolve; });
  const f = harness(t, { readProof(request) { return blocked.then(() => ({ requestId: request.requestId, inputDigest: request.inputDigest, outcome: 'not_applied' })); } });
  const coordinator = createExternalAdapterCoordinator({ entries: captureExternalAdapters([{ contract: CONTRACT, adapter: f.adapter }]),
    registry: createCatalog([CAP]), invocations: f.service.invocations, authorize: f.service.authorize, limits: { inflight: 1, callTimeoutMs: 5 } });
  t.after(() => coordinator.close());
  for (let i = 0; i < 5; i++) {
    const result = await coordinator.invoke(f.request()); assert.equal(result.invocation.effectState, 'unknown');
  }
  assert.equal(f.calls().reads, 1); assert.equal(f.calls().writes, 0); release();
  await blocked; await Promise.resolve();
});

test('receipt artifacts must match every proved effect by exact type/id/revision', async t => {
  const f = harness(t, { readProof(request) { return { requestId: request.requestId, inputDigest: request.inputDigest, outcome: 'committed',
    effects: [{ kind: 'created', resourceType: 'planner-object', resourceId: 'object_A', revision: 1 }],
    receipt: { verificationMethod: 'domain_read', artifacts: [{ type: 'planner-object', id: 'object_B', revision: 900 }] } }; } });
  const result = await f.service.external.invoke(f.request()); assert.equal(result.invocation.effectState, 'unknown');
  assert.equal(result.invocation.receipt, undefined); assert.equal(f.calls().writes, 0);
});

test('authorized discovery hides another grant/resource and a changed binding cannot reuse a persisted version', t => {
  const f = harness(t), actor = f.actor(), page = f.service.external.search({ actor });
  assert.equal(page.schema, 'soty.authorized-app-capabilities.v1'); assert.equal(page.items.length, 1);
  const contract = f.service.external.getContract({ actor, reference: CONTRACT });
  assert.equal(contract.binding.digest, CAP.executionBinding.binding.digest); assert.deepEqual(contract.skills, []);
  assert.equal(f.service.catalog.search({}).total, 0, 'private contracts never affect public discovery');
  const principal = f.call('principals.create', { label: 'Другое разрешение' }).principal;
  const grant = f.call('grants.issue', { principalId: principal.id, capabilities: [{ capabilityId: CAP.capabilityId, version: 1 }],
    resources: CAP.resources, effects: CAP.effects, recipients: CAP.recipients, expiresAt: Date.now() + 600000,
    allowDelegation: false, maxDepth: 0, budget: { unit: 'invocations', limit: 1 } }).grant;
  const issued = f.call('credentials.issue', { grantId: grant.id, audience: AUDIENCE });
  const other = f.service.authenticateCredential({ token: issued.token, audience: AUDIENCE });
  assert.equal(f.service.withOwnerAuthority({ actor }, owner => owner.accountId), OWNER.accountId);
  assert.throws(() => f.service.withOwnerAuthority({ actor: { ...actor } }, owner => owner), /authorization_required/u);
  f.call('grants.revoke', { grantId: grant.id }); assert.throws(() => f.service.external.search({ actor: other }), /access_denied/u);
  assert.throws(() => f.reopen({ catalog: [{ ...CAP, executionBinding: { ...CAP.executionBinding, binding: { ...CAP.executionBinding.binding, digest: 'c'.repeat(64) } } }], externalAdapters: [] }), /capability_version_conflict/u);
});

test('a paused pinned adapter replays a committed intent but refuses a new effect', async t => {
  const f = harness(t), first=await f.service.external.invoke(f.request());
  f.reopen({catalog:[{...CAP,executionEnabled:false}]});
  const repeated=await f.service.external.invoke(f.request());assert.equal(repeated.invocation.invocationId,first.invocation.invocationId);
  assert.equal(repeated.reused,true);assert.equal(f.calls().writes,1);
  await assert.rejects(f.service.external.invoke({...f.request(),idempotencyKey:'another-intention'}),/capability_disabled/u);
});

test('finite registered admissions hold source-unknown quota and preserve exact retries at capacity', async t => {
  const f=harness(t,{readProof(request){return{requestId:request.requestId,inputDigest:request.inputDigest,outcome:'unknown'};}});
  const results=[];for(let i=0;i<4;i++)results.push(await f.service.external.invoke({...f.request(),idempotencyKey:'bounded_intent_'+i}));
  // A larger root budget cannot bypass the coordinator's persistent pilot cap.
  const principal=f.call('principals.create',{label:'Finite pilot'}).principal;
  const grant=f.call('grants.issue',{principalId:principal.id,capabilities:[{capabilityId:CAP.capabilityId,version:1}],resources:CAP.resources,
    effects:CAP.effects,recipients:CAP.recipients,expiresAt:Date.now()+600000,allowDelegation:false,maxDepth:0,budget:{unit:'invocations',limit:10}}).grant;
  const issued=f.call('credentials.issue',{grantId:grant.id,audience:AUDIENCE}),actor=f.service.authenticateCredential({token:issued.token,audience:AUDIENCE});
  for(let i=0;i<4;i++)f.service.external.admit({...f.request(),actor,idempotencyKey:'second_bound_'+i});
  assert.throws(()=>f.service.external.admit({...f.request(),actor,idempotencyKey:'second_bound_4'}),/external_admission_limit/u);
  assert.equal((await f.service.external.invoke({...f.request(),idempotencyKey:'bounded_intent_0'})).invocation.invocationId,results[0].invocation.invocationId);
  assert.equal(f.calls().writes,0);
});
