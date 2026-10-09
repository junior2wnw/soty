import test from 'node:test';
import assert from 'node:assert/strict';
import { createManagedRootReviews } from '../../modules/reviews/server/managed-root.mjs';
import { createManagedReviewsNativeBridge } from '../managed-reviews-native-bridge.js';

// Actual Root managed extension/port; Native request, actor, grants and Human
// ports are controlled here. This is not real RP/SQL permission qualification.
const legacy = process.env.SOTY_MANAGED_REVIEWS_BRIDGE_BASELINE === '1';
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
const provider = { id: 'povedai:fixture', version: 1, digest: 'a'.repeat(64), origin: 'https://reviews.example' };
const providerRef = { id: provider.id, version: provider.version, digest: provider.digest };
const scope = { registryId: 'soty', tenantId: 'owner', appId: 'app-one', environmentId: 'production' };
function fixture(t) {
  const config = { providers: [provider], bindings: ['app', 'project', 'person'].map((kind, index) => ({ scope,
    localSubject: { kind, id: { app: 'app-one', project: 'native-project', person: 'owner' }[kind] }, providerRef,
    subjectRef: { id: 'povedai:subject-' + kind, version: 1, digest: String(index + 1).repeat(64) },
    providerSubjectId: 'subject_' + String(index + 1).repeat(20), providerEntityType: { app: 'product', project: 'project', person: 'profile' }[kind], mode: 'public-read' })) };
  const rootActors = new WeakSet(), humans = new WeakMap(), nativeActors = new WeakSet(), originalRequest = Object.freeze({ controlledNativeRequest: true });
  const profile = { issuer: 'https://root.example/human-identity', clientId: 'selected-client' };
  const human = () => ({ ...profile, subject: 'owner', clientProfileDigest: 'f'.repeat(64), clientGeneration: 1 });
  const actor = () => { const value = Object.freeze({ accountId: 'owner', deviceId: 'device' }); rootActors.add(value); humans.set(value, human()); return value; };
  const a = actor(), calls = [], receipts = new Map();
  const state = { active: true, grants: true, resolveGate: null, actorGate: null, operationGate: null, operationEntered: null,
    humanPort: null, duplicateActor: false, throwCollect: false, principalTouches: 0, nativeClose: 0 };
  const expected = (request, callback) => {
    assert.ok(rootActors.has(request.actor)); assert.equal(request.issuer, profile.issuer); assert.equal(request.clientId, profile.clientId);
    if (state.humanPort) return state.humanPort(request, callback);
    return callback(Object.freeze({ ...humans.get(request.actor) }));
  };
  const native = { async withActor(request, callback) {
    assert.equal(request, originalRequest); calls.push('withActor'); state.principalTouches++;
    await state.actorGate; const current = Object.freeze({}); nativeActors.add(current);
    const result = await callback(current);
    if (state.duplicateActor) { try { await callback(current); } catch {} }
    return result;
  }, async context(current, input) {
    assert.ok(nativeActors.has(current)); calls.push('context');
    if (!state.grants) throw Object.assign(new Error('managed_source_grant_required'), { code: 'managed_source_grant_required', status: 403 });
    const binding = config.bindings.find(value => value.localSubject.kind === input.localSubject.kind);
    return { protocol: 'povedai.managed-reviews.v1', localSubject: binding.localSubject, entityType: binding.providerEntityType,
      bindingId: 'native-binding', siteId: 'native-site', siteKey: 'native-site-key', objectId: 'native-object', objectSlug: 'native-object',
      subjectId: binding.providerSubjectId, placementId: 'native-placement', generation: 1, canDisplay: false };
  }, async list(current) {
    assert.ok(nativeActors.has(current)); calls.push('list'); state.operationEntered?.resolve(); await state.operationGate;
    return { items: [], nextCursor: null };
  }, async collect(current, input) {
    assert.ok(nativeActors.has(current)); calls.push('collect');
    const reply = { reviewId: 'review-' + input.localSubject.kind, authorId: 'native-author', rootId: 'review-' + input.localSubject.kind,
      parentId: null, depth: 0, status: 'pending' };
    receipts.set(input.requestId, reply); state.operationEntered?.resolve(); await state.operationGate;
    if (state.throwCollect) throw Object.assign(new Error('managed_source_ack_unknown'), { code: 'managed_source_ack_unknown', status: 503 });
    return reply;
  }, close() { state.nativeClose++; } };
  const sourceApp = { profile, async resolveSourceCredential(request, port) {
    assert.ok(rootActors.has(request.actor)); assert.equal(request.rootActor.accountId, 'owner');
    port({ actor: request.actor, ...profile }, () => {}); calls.push('resolve'); await state.resolveGate; return originalRequest;
  } };
  const bridge = legacy ? { managedReviewsSource: { providerRef, source: { ...native,
    provision() { assert.fail(); }, publish() { assert.fail(); }, moderate() { assert.fail(); } },
    resolveSourceCredential: request => sourceApp.resolveSourceCredential(request, expected) }, close() {} }
    : createManagedReviewsNativeBridge({ nativeApplication: { managedService: native, managedSourceApp: sourceApp }, providerRef, withHumanSubjectAuthority: expected });
  const root = createManagedRootReviews({ configuration: config, managedSource: bridge.managedReviewsSource,
    withRootActorAuthority(value, callback) { assert.ok(rootActors.has(value) && state.active); return callback({ accountId: value.accountId, deviceId: value.deviceId }); },
    withAppAuthority(_request, callback) { return callback({ appId: 'app-one', accountId: 'owner', ownerId: 'owner', canManage: true,
      appRevision: 1, policyEpoch: 1, target: { revision: 1, digest: 'b'.repeat(64) }, entry: { path: '/' } }); } });
  t.after(() => { bridge.close(); root.close(); });
  const execute = (method, kind = 'app', extra = {}, current = a) => {
    const binding = config.bindings.find(value => value.localSubject.kind === kind);
    return root.executeAsync({ op: 'apps.reviews.managed.' + method, actor: current, args: { appId: 'app-one', subjectKind: kind, providerRef,
      subjectRef: binding.subjectRef, providerSubjectId: binding.providerSubjectId, ...extra } });
  };
  return { bridge, root, native, sourceApp, a, actor, humans, human, profile, expected, state, calls, receipts, originalRequest, execute };
}

test('actual Root context/collect routes reuse all three Native subjects and retain pending receipts', async t => {
  const f = fixture(t);
  for (const kind of ['app', 'project', 'person']) {
    const context = await f.execute('context', kind); assert.equal(context.available, true); assert.equal(context.subjectKind, kind);
    const result = await f.execute('collect', kind, { requestId: 'stable-request-' + kind, body: 'Synthetic review' });
    assert.equal(result.status, 'pending'); assert.equal(f.receipts.get('stable-request-' + kind).reviewId, result.reviewId);
  }
  assert.equal(f.receipts.size, 3); assert.equal(f.state.nativeClose, 0);
});

test('distinct single-use tokens sharing the original Native request preserve both original Root actors', async t => {
  const f = fixture(t), second = f.actor(), port = f.bridge.managedReviewsSource;
  const request = actor => ({ actor, rootActor: { accountId: actor.accountId, deviceId: actor.deviceId }, scope });
  const [a, b] = await Promise.all([port.resolveSourceCredential(request(f.a)), port.resolveSourceCredential(request(second))]);
  assert.notEqual(a, b); assert.notEqual(a, f.originalRequest); assert.deepEqual(Object.keys(a), []);
  assert.deepEqual(await Promise.all([port.source.withActor(a, async () => 'a'), port.source.withActor(b, async () => 'b')]), ['a', 'b']);
  assert.equal(f.calls.filter(value => value === 'withActor').length, 2);
  await assert.rejects(port.source.withActor(a, async () => 'replay'));
  await assert.rejects(port.source.withActor({}, async () => 'foreign'));
});

test('full Human changes during actual Source list await deny stale output for each of five fields', async t => {
  for (const field of ['issuer', 'subject', 'clientId', 'clientProfileDigest', 'clientGeneration']) {
    const f = fixture(t), gate = deferred(), entered = deferred(); f.state.operationGate = gate.promise; f.state.operationEntered = entered;
    const pending = f.execute('list'); await entered.promise;
    const current = f.humans.get(f.a), value = field === 'clientGeneration' ? 2 : field === 'clientProfileDigest' ? 'e'.repeat(64) : current[field] + '-changed';
    f.humans.set(f.a, { ...current, [field]: value }); gate.resolve();
    await assert.rejects(pending, error => /^managed_reviews_native_human_/u.test(error.code));
    assert.equal(f.calls.filter(name => name === 'list').length, 1);
  }
});

test('collect has one durable receipt yet late Human change yields unknown with stable requestId', async t => {
  const f = fixture(t), gate = deferred(), entered = deferred(); f.state.operationGate = gate.promise; f.state.operationEntered = entered;
  const pending = f.execute('collect', 'app', { requestId: 'stable-unknown-request', body: 'Keep this draft' }); await entered.promise;
  f.humans.set(f.a, { ...f.humans.get(f.a), clientGeneration: 2 }); gate.resolve();
  await assert.rejects(pending, { code: 'managed_reviews_outcome_unknown', status: 503 });
  assert.equal(f.receipts.size, 1); assert.ok(f.receipts.has('stable-unknown-request'));
  assert.equal(f.calls.filter(name => name === 'collect').length, 1);
});

test('Native collect post-receipt refusal remains unknown; missing grant before collect stays denied', async t => {
  const f = fixture(t); f.state.throwCollect = true;
  await assert.rejects(f.execute('collect', 'app', { requestId: 'native-unknown-request', body: 'Synthetic' }), { code: 'managed_reviews_outcome_unknown' });
  assert.equal(f.receipts.size, 1);
  const denied = fixture(t); denied.state.grants = false;
  await assert.rejects(denied.execute('collect', 'app', { requestId: 'grant-denied-request', body: 'Synthetic' }), { code: 'managed_source_grant_required' });
  assert.equal(denied.calls.includes('collect'), false); assert.equal(denied.receipts.size, 0);
});

test('close during resolver or Native withActor await starts no later Source operation', async t => {
  const f = fixture(t), gate = deferred(); f.state.resolveGate = gate.promise;
  const pending = f.execute('context'); while (!f.calls.includes('resolve')) await new Promise(done => setImmediate(done));
  f.bridge.close(); gate.resolve(); await assert.rejects(pending); assert.equal(f.calls.includes('withActor'), false);
  const other = fixture(t), held = deferred(); other.state.actorGate = held.promise;
  const waiting = other.execute('context'); while (!other.calls.includes('withActor')) await new Promise(done => setImmediate(done));
  other.bridge.close(); held.resolve(); await assert.rejects(waiting); assert.equal(other.calls.includes('context'), false);
  assert.equal(other.state.principalTouches, 1); // Existing Native identity work may precede callback; no effects0 claim.
  assert.equal(other.state.nativeClose, 0);
});

test('duplicate/async/retained Human callback and duplicate Native actor callback cannot acknowledge', async t => {
  for (const mode of ['duplicate', 'async', 'retained']) {
    const f = fixture(t); let retained;
    f.state.humanPort = (request, callback) => {
      const value = Object.freeze({ ...f.humans.get(request.actor) });
      if (mode === 'retained') { retained = callback; return; }
      callback(value); if (mode === 'duplicate') { try { callback(value); } catch {} } else return Promise.reject(new Error('controlled async refusal'));
    };
    await assert.rejects(f.execute('context')); assert.equal(f.calls.includes('withActor'), false);
    if (retained) assert.throws(() => retained(f.human()));
  }
  const f = fixture(t); f.state.duplicateActor = true; await assert.rejects(f.execute('context'));
});

test('a foreign Source actor and administrative methods cannot cross the private call frame', async t => {
  const f = fixture(t), port = f.bridge.managedReviewsSource;
  const token = await port.resolveSourceCredential({ actor: f.a, rootActor: { accountId: 'owner', deviceId: 'device' }, scope });
  await assert.rejects(port.source.withActor(token, async actualActor => { await assert.rejects(port.source.context({ ...actualActor }, {})); return port.source.provision(actualActor, {}); }));
  assert.equal(f.calls.includes('context'), false); assert.equal(f.receipts.size, 0);
});
