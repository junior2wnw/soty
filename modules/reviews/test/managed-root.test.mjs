import test from 'node:test';
import assert from 'node:assert/strict';
import { createReviewsService } from '../server/index.mjs';
import { createManagedReviewsPort } from '../server/managed-port.mjs';
import { createConnectService } from '../../connect/server/index.mjs';
import { createClientWithStorage } from '../../connect/browser/client.mjs';

const legacy = process.env.SOTY_REVIEWS_LEGACY_CONTEXT === '1';
const createManagedRootReviews = legacy ? null : (await import('../server/managed-root.mjs')).createManagedRootReviews;
const provider = { id: 'povedai:fixture', version: 1, digest: 'a'.repeat(64), origin: 'https://reviews.example' };
const ref = value => ({ id: value.id, version: value.version, digest: value.digest });
const scope = { registryId: 'soty', tenantId: 'owner', appId: 'app-one', environmentId: 'production' };
const configuration = () => ({ providers: [provider], bindings: ['app', 'project', 'person'].map((kind, index) => ({
  scope, localSubject: { kind, id: { app: 'app-one', project: 'native-project-one', person: 'owner' }[kind] },
  providerRef: ref(provider), subjectRef: { id: 'povedai:subject-' + kind, version: 1, digest: String(index + 1).repeat(64) },
  providerSubjectId: 'subject_' + String(index + 1).repeat(20), providerEntityType: { app: 'product', project: 'project', person: 'profile' }[kind], mode: 'public-read',
})) });
const selector = binding => ({ appId: binding.scope.appId, subjectKind: binding.localSubject.kind,
  providerRef: binding.providerRef, subjectRef: binding.subjectRef, providerSubjectId: binding.providerSubjectId });
const projection = binding => ({ protocol: 'povedai.managed-reviews.v1', localSubject: binding.localSubject,
  entityType: binding.providerEntityType, bindingId: 'native-binding-' + binding.localSubject.kind,
  siteId: 'native-site', siteKey: 'native-site-key', objectId: 'native-object-' + binding.localSubject.kind,
  objectSlug: 'native-object', subjectId: binding.providerSubjectId, placementId: 'native-placement-' + binding.localSubject.kind,
  generation: 1, canDisplay: false });
const review = (binding, id, body, status = 'approved', name = 'Native alias') => ({ id, objectId: 'native-object-' + binding.localSubject.kind,
  subjectId: binding.providerSubjectId, type: 'review', rootId: id, depth: 0, version: 1, source: 'native', status,
  appearance: 'standard', appearanceMeta: {}, body, author: { name, verified: false }, mediaUrls: [], tags: [], inferredTags: [], excludedTags: [],
  up: 0, down: 0, meta: {}, createdAt: '2026-10-09T00:00:00.000Z', updatedAt: '2026-10-09T00:00:00.000Z' });

function fixture(t, overrides = {}) {
  const config = overrides.configuration ?? configuration(), actor = Object.freeze({ accountId: 'owner', deviceId: 'device' });
  const credential = Object.freeze({}), sourceActor = Object.freeze({}), grants = new Set(['app', 'project', 'person']);
  const calls = [], effects = [], receipts = new Map(), priorReviews = [{ id: 'prior-review', body: 'Synthetic prior content' }];
  const records = config.bindings.map(binding => review(binding, 'prior-' + binding.localSubject.kind, 'Synthetic prior content'));
  let active = true, revision = 1, ownerId = 'owner';
  const context = () => ({ appId: 'app-one', accountId: 'owner', ownerId, canManage: ownerId === 'owner',
    appRevision: revision, policyEpoch: 1, target: { revision: 1, digest: 'b'.repeat(64) }, entry: { path: '/' },
    grants: { accountIds: [], communityIds: ['access-group-is-not-a-project'] } });
  const withRootActorAuthority = (value, callback) => {
    assert.ok(value === actor && active, 'synthetic current original Root actor');
    return callback({ accountId: value.accountId, deviceId: value.deviceId });
  };
  const withAppAuthority = (_request, callback) => callback(context());
  const bindingFor = input => config.bindings.find(value => value.localSubject.kind === input.localSubject.kind && value.localSubject.id === input.localSubject.id);
  const source = {
    async withActor(value, callback) { assert.equal(value, credential); calls.push('withActor'); return callback(sourceActor); },
    async context(value, input) { assert.equal(value, sourceActor); calls.push('context');
      if (!grants.has(input.localSubject.kind)) throw Object.assign(new Error('managed_source_grant_required'), { code: 'managed_source_grant_required', status: 403 });
      return projection(bindingFor(input)); },
    async list(value, input) { assert.equal(value, sourceActor); calls.push('list'); const binding = bindingFor(input);
      return { items: records.filter(item => item.objectId === 'native-object-' + binding.localSubject.kind), nextCursor: null }; },
    async collect(value, input) { assert.equal(value, sourceActor); calls.push('collect');
      if (!grants.has(input.localSubject.kind)) throw Object.assign(new Error('managed_source_grant_required'), { code: 'managed_source_grant_required', status: 403 });
      const prior = receipts.get(input.requestId), intent = JSON.stringify(input);
      if (prior) { assert.equal(prior.intent, intent); return prior.reply; }
      effects.push({ kind: input.localSubject.kind, requestId: input.requestId });
      const reply = { reviewId: 'new-review-' + input.localSubject.kind, authorId: 'native-author', rootId: 'new-review-' + input.localSubject.kind, parentId: null, depth: 0, status: 'pending' };
      records.push(review(bindingFor(input), reply.reviewId, input.body, 'pending', input.authorLabel ?? 'Участник'));
      receipts.set(input.requestId, { intent, reply }); return reply; },
    async provision() { assert.fail('auto-provision forbidden'); }, async publish() { assert.fail('auto-publish forbidden'); }, async moderate() { assert.fail('automatic moderation forbidden'); },
    ...overrides.source,
  };
  const options = { configuration: config, withRootActorAuthority, withAppAuthority,
    managedSource: { providerRef: ref(provider), source, resolveSourceCredential: overrides.resolveSourceCredential ?? (async () => credential),
      ...(overrides.timeoutMs ? { timeoutMs: overrides.timeoutMs } : {}) }, ...overrides };
  const service = legacy ? createReviewsService({ configuration: config, actorActive: () => active, withAppAuthority }) : createManagedRootReviews(options);
  t.after(() => service.close());
  const execute = (op, args, value = actor) => legacy ? Promise.resolve().then(() => service.execute({ op, args, actor: value })) : service.executeAsync({ op, args, actor: value });
  return { service, actor, config, credential, sourceActor, source, calls, effects, receipts, priorReviews, context, execute, grants, options,
    revoke() { active = false; }, revise() { revision++; }, rebindOwner(value = 'new-owner') { ownerId = value; revision++; } };
}

for (const kind of ['app', 'project', 'person']) test('current Native context resolves exactly one actual ' + kind + ' binding', async t => {
  const f = fixture(t), binding = f.config.bindings.find(value => value.localSubject.kind === kind);
  const result = await f.execute('apps.reviews.managed.context', selector(binding));
  assert.equal(result.available, true); assert.equal(result.subjectKind, kind);
  assert.equal(result.providerSubjectId, binding.providerSubjectId);
  assert.deepEqual(f.effects, []); assert.deepEqual(f.priorReviews, [{ id: 'prior-review', body: 'Synthetic prior content' }]);
  assert.ok(!JSON.stringify(result).includes('native-author')); assert.ok(!JSON.stringify(result).includes('device'));
});

test('only exact host selectors reach Source; foreign/freeform IDs and claimed grants remain denied', async t => {
  const f = fixture(t), args = selector(f.config.bindings[0]);
  for (const changed of [{ appId: 'foreign-app' }, { subjectKind: 'project' }, { providerSubjectId: 'subject_' + 'f'.repeat(20) },
    { subjectRef: { ...args.subjectRef, digest: 'f'.repeat(64) } }, { providerRef: { ...args.providerRef, version: 2 } }]) {
    await assert.rejects(f.execute('apps.reviews.managed.context', { ...args, ...changed }), /managed_reviews_binding_not_found/);
  }
  for (const added of [{ localSubject: { kind: 'app', id: 'foreign' } }, { scope: { tenantId: 'foreign' } }, { credential: {} }, { rights: ['collect'] }, { verified: true }]) {
    await assert.rejects(f.execute('apps.reviews.managed.context', { ...args, ...added }), /managed_reviews_invalid/);
  }
  assert.deepEqual(f.calls, []); assert.deepEqual(f.effects, []);
});

test('access group is never substituted for project; missing project binding is unavailable and person must be actual owner', async t => {
  const config = configuration(), missing = config.bindings.splice(1, 1)[0];
  const f = fixture(t, { configuration: config });
  await assert.rejects(f.execute('apps.reviews.managed.context', selector(missing)), /managed_reviews_binding_not_found/);
  config.bindings[1] = { ...config.bindings[1], localSubject: { kind: 'person', id: 'foreign-person' } };
  const g = fixture(t, { configuration: config });
  await assert.rejects(g.execute('apps.reviews.managed.context', selector(config.bindings[1])), /managed_reviews_subject_scope_invalid/);
  assert.deepEqual(g.calls, []); assert.deepEqual(g.effects, []);
});

test('absence of real Source credential port stays unavailable after Root/Apps admission and preserves public data', async t => {
  const f = fixture(t, { managedSource: undefined }), args = selector(f.config.bindings[0]);
  assert.deepEqual(await f.execute('apps.reviews.managed.context', args), { available: false, code: 'managed_reviews_unavailable' });
  await assert.rejects(f.execute('apps.reviews.managed.collect', { ...args, requestId: 'one', body: 'Synthetic' }), /managed_reviews_unavailable/);
  await assert.rejects(f.execute('apps.reviews.managed.context', args, structuredClone(f.actor)), /synthetic current original Root actor/);
  assert.deepEqual(f.calls, []); assert.equal(f.priorReviews.length, 1);
});

test('copied or revoked Root identity and revoked Native grant cannot produce a collect effect', async t => {
  const f = fixture(t), args = { ...selector(f.config.bindings[0]), requestId: 'one', body: 'Synthetic' };
  await assert.rejects(f.execute('apps.reviews.managed.collect', args, structuredClone(f.actor)), /synthetic current original Root actor/);
  f.grants.delete('app'); await assert.rejects(f.execute('apps.reviews.managed.collect', args), /managed_source_grant_required/);
  f.revoke(); await assert.rejects(f.execute('apps.reviews.managed.collect', args), /synthetic current original Root actor/);
  assert.deepEqual(f.effects, []); assert.equal(f.priorReviews.length, 1);
});

test('Native target changed since the UI context is rejected by fresh context before collect', async t => {
  let different = false, f;
  f = fixture(t, { source: { async context(_actor, input) {
    const binding = f.config.bindings.find(item => item.localSubject.kind === input.localSubject.kind);
    return { ...projection(binding), ...(different ? { subjectId: 'subject_' + 'f'.repeat(20) } : {}) };
  } } });
  const args = selector(f.config.bindings[0]); await f.execute('apps.reviews.managed.context', args); different = true;
  await assert.rejects(f.execute('apps.reviews.managed.collect', { ...args, requestId: 'one', body: 'Synthetic' }), /managed_reviews_source_binding_changed/);
  assert.deepEqual(f.effects, []);
});

test('Root/app revoke and revision ABA during credential await deny before Source dispatch', async t => {
  for (const change of ['revoke', 'ABA']) {
    let resolve; const waiting = new Promise(done => { resolve = done; });
    const f = fixture(t, { resolveSourceCredential: () => waiting });
    const pending = f.execute('apps.reviews.managed.collect', { ...selector(f.config.bindings[0]), requestId: 'one', body: 'Synthetic' });
    await Promise.resolve();
    if (change === 'revoke') f.revoke(); else { f.rebindOwner(); f.rebindOwner('owner'); }
    resolve(f.credential); await assert.rejects(pending);
    assert.deepEqual(f.calls, []); assert.deepEqual(f.effects, []);
  }
});

for (const kind of ['app', 'project', 'person']) test('explicit collect for ' + kind + ' stays pending and preserves the Source receipt and prior records', async t => {
  const f = fixture(t), binding = f.config.bindings.find(value => value.localSubject.kind === kind);
  const args = { ...selector(binding), requestId: 'collect-' + kind, body: 'Synthetic new content', authorLabel: 'Chosen alias', rating: 4 };
  const result = await f.execute('apps.reviews.managed.collect', args);
  assert.equal(result.status, 'pending'); assert.equal(result.requestId, args.requestId);
  assert.equal(f.effects.length, 1); assert.equal(f.receipts.size, 1);
  assert.deepEqual(f.priorReviews, [{ id: 'prior-review', body: 'Synthetic prior content' }]);
  assert.deepEqual(Object.keys(result).sort(), ['requestId', 'reviewId', 'status']);
  assert.equal(result.verified, undefined); assert.equal(result.authorId, undefined);
  const listed = await f.execute('apps.reviews.managed.list', selector(binding));
  assert.equal(listed.items.length, 2); assert.equal(listed.items[0].body, 'Synthetic prior content');
  assert.equal(listed.items[1].status, 'pending'); assert.equal(listed.items[1].body, 'Synthetic new content');
  assert.equal(listed.items[1].author.verified, false);
});

test('lost Source ACK creates no automatic write retry; explicit list is read-only and leaves outcome unknown', async t => {
  let f;
  f = fixture(t, { source: { async collect(_actor, input) {
    f.effects.push({ requestId: input.requestId }); throw Object.assign(new Error('managed_source_unknown_ack'), { code: 'managed_source_unknown_ack', status: 503 });
  } } });
  const selectorArgs = selector(f.config.bindings[0]);
  await assert.rejects(f.execute('apps.reviews.managed.collect', { ...selectorArgs, requestId: 'stable-one', body: 'Synthetic' }), /managed_source_unknown_ack/);
  assert.equal(f.effects.length, 1);
  await f.execute('apps.reviews.managed.list', selectorArgs); assert.equal(f.effects.length, 1);
  assert.equal(f.calls.filter(value => value === 'withActor').length, 2);
});

test('Root post-fence denial after Source commit does not pretend to roll back the external effect', async t => {
  let f;
  f = fixture(t, { source: { async collect(_actor, input) {
    f.effects.push({ requestId: input.requestId }); f.revise();
    return { reviewId: 'new-review', authorId: 'native-author', rootId: 'new-review', parentId: null, depth: 0, status: 'pending' };
  } } });
  await assert.rejects(f.execute('apps.reviews.managed.collect', { ...selector(f.config.bindings[0]), requestId: 'one', body: 'Synthetic' }), /managed_reviews_authority_changed/);
  assert.equal(f.effects.length, 1);
});

test('four ignored credential timeouts retain one shared limit across all three kinds, with no redispatch', async t => {
  let resolutions = 0;
  const f = fixture(t, { timeoutMs: 100, resolveSourceCredential: () => { resolutions++; return new Promise(() => {}); } });
  for (let index = 0; index < 4; index++) await assert.rejects(f.execute('apps.reviews.managed.context', selector(f.config.bindings[index % 3])), /managed_reviews_timeout/);
  await assert.rejects(f.execute('apps.reviews.managed.context', selector(f.config.bindings[1])), /managed_reviews_busy/);
  assert.equal(resolutions, 4); assert.deepEqual(f.calls, []);
});

test('async and repeated Apps fences plus accessor selectors cannot acknowledge Native availability', async t => {
  const f = fixture(t, { withAppAuthority(_request, callback) { return Promise.resolve(callback(f.context())); } });
  await assert.rejects(f.execute('apps.reviews.managed.context', selector(f.config.bindings[0])), /managed_reviews_async_authority/);
  let g;
  g = fixture(t, { withAppAuthority(_request, callback) { const value = callback(g.context()); try { callback(g.context()); } catch {} return value; } });
  await assert.rejects(g.execute('apps.reviews.managed.context', selector(g.config.bindings[0])), /managed_reviews_authority_invalid/);
  let reads = 0; const args = { ...selector(g.config.bindings[0]) };
  Object.defineProperty(args, 'subjectKind', { enumerable: true, get() { reads++; return 'app'; } });
  await assert.rejects(g.execute('apps.reviews.managed.context', args), /managed_reviews_invalid/);
  assert.equal(reads, 0); assert.deepEqual(f.calls, []); assert.deepEqual(g.calls, []);
});

test('optional trusted local subject hook is synchronous exactly true and runs in all existing fences', async t => {
  const f = fixture(t), base = { providerRef: ref(provider), source: f.source, resolveSourceCredential: async () => f.credential,
    withRootActorAuthority: f.options.withRootActorAuthority, withAppAuthority: f.options.withAppAuthority };
  for (const hook of [() => false, () => Promise.resolve(true), () => Promise.reject(new Error('Synthetic rejected hook'))]) {
    const port = createManagedReviewsPort({ ...base, validateLocalSubject: hook }); t.after(() => port.close());
    await assert.rejects(port.execute({ op: 'context', actor: f.actor, args: { appId: 'app-one', localSubject: f.config.bindings[0].localSubject } }));
  }
  assert.deepEqual(f.calls, []);
  const phases = [], port = createManagedReviewsPort({ ...base, validateLocalSubject(value) {
    assert.equal(Object.isFrozen(value), true); assert.equal(value.localSubject.id, 'app-one'); phases.push(value.context.appRevision); return true;
  } }); t.after(() => port.close());
  await port.execute({ op: 'context', actor: f.actor, args: { appId: 'app-one', localSubject: f.config.bindings[0].localSubject } });
  assert.deepEqual(phases, [1, 1, 1]);
});

test('actual signed in-memory Connect executeAsync hands original actor to the Native wrapper without a listener', async t => {
  let module, retainedActor, state = null;
  const origin = 'https://shell.example', projectId = 'native-reviews-source-test';
  const service = createConnectService({ databasePath: ':memory:', projectId, allowedOrigins: [origin], extensions: [{
    operations: new Set(['apps.reviews.managed.context']), executeAsync(request) { retainedActor = request.actor; return module.executeAsync(request); },
  }] }); t.after(() => service.close());
  const storage = { async read() { return state === null ? null : structuredClone(state); },
    async claim(value) { state ??= structuredClone(value); return structuredClone(state); },
    async compareAndSwap(revision, value) { assert.equal(revision, state.localRevision); state = structuredClone(value); return structuredClone(state); } };
  const client = createClientWithStorage({ projectId, endpoint: origin + '/rpc', createChannel: () => null,
    fetch: async (_url, options) => { const result = await service.handle({ ...JSON.parse(options.body), origin });
      return new Response(JSON.stringify(result), { status: result.ok ? 200 : 400 }); } }, storage); t.after(() => client.dispose());
  const account = await client.bootstrap('Private test profile'), config = configuration();
  config.bindings = config.bindings.map(binding => ({ ...binding, scope: { ...binding.scope, tenantId: account.accountId },
    localSubject: binding.localSubject.kind === 'person' ? { kind: 'person', id: account.accountId } : binding.localSubject }));
  const credential = {}, sourceActor = {}, source = { async withActor(value, callback) { assert.equal(value, credential); return callback(sourceActor); },
    async context(value, input) { assert.equal(value, sourceActor); return projection(config.bindings.find(binding => binding.localSubject.kind === input.localSubject.kind)); },
    async list() {}, async collect() {}, async provision() {}, async publish() {}, async moderate() {} };
  module = createManagedRootReviews({ configuration: config,
    withRootActorAuthority: (actor, callback) => service.withActorAuthorityFence(actor, () => callback({ accountId: actor.accountId, deviceId: actor.deviceId })),
    withAppAuthority: ({ actor, appId }, callback) => callback({ appId, ownerId: account.accountId, accountId: actor.accountId, canManage: true,
      appRevision: 1, policyEpoch: 1, target: { revision: 1, digest: 'b'.repeat(64) }, entry: { path: '/' } }),
    managedSource: { providerRef: ref(provider), source, resolveSourceCredential: async ({ actor }) => { assert.equal(actor, retainedActor); return credential; } } });
  t.after(() => module.close());
  for (const binding of config.bindings) {
    const result = await client.extension('apps.reviews.managed.context', selector(binding), { expectedAccountId: account.accountId });
    assert.equal(result.available, true); assert.equal(result.subjectKind, binding.localSubject.kind);
  }
  await assert.rejects(module.executeAsync({ op: 'apps.reviews.managed.context', actor: structuredClone(retainedActor), args: selector(config.bindings[0]) }), error => error.code === 'connect_authority_actor_invalid');
});
