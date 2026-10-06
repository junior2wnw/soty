import test from 'node:test';
import assert from 'node:assert/strict';
import { createReviewsService } from '../server/index.mjs';
import { REVIEWS_LIMITS } from '../server/profile.mjs';
import { captureReviewedConfiguration } from '../../app-contract/server/authority.mjs';
import { DEFAULT_FEEDBACK_PROFILE } from '../../feedback/server/profile.mjs';
import { createAdmissionHost, closeAdmissionHost, materializeAuthorDraft, planAdmission } from '../../app-contract/index.mjs';

const provider = { id: 'povedai:public-v1', version: 1, digest: 'a'.repeat(64), origin: 'https://reviews.example' };
const ref = ({ id, version, digest }) => ({ id, version, digest });
const scope = (appId = 'app_one', tenantId = 'owner') => ({ registryId: 'fixture', tenantId, appId, environmentId: 'test' });
const subjectId = index => 'subject_' + String(index).padStart(20, '0');
const binding = (kind = 'app', index = 1, appId = 'app_one', tenantId = 'owner') => ({
  scope: scope(appId, tenantId), localSubject: { kind, id: kind === 'app' ? appId : `${kind}_private_local_${index}` },
  providerRef: ref(provider), subjectRef: { id: `povedai:subject-${index}`, version: 1, digest: String(index).repeat(64).slice(0, 64) },
  providerSubjectId: subjectId(index), providerEntityType: { app: 'product', project: 'project', person: 'profile' }[kind], mode: 'public-read',
});
const actor = (accountId = 'one') => ({ accountId, deviceId: 'device_' + accountId });
const context = (who, appId = 'app_one', ownerId = 'owner') => ({ appId, accountId: who.accountId, ownerId,
  canManage: who.accountId === ownerId, title: 'Synthetic title', entry: null });
const configuration = () => ({ providers: [structuredClone(provider)], bindings: [binding()] });
const host = (config = configuration(), overrides = {}) => ({ registryId: 'fixture', environmentId: 'test',
  configuration: config, actorActive: who => ['owner', 'one', 'two'].includes(who.accountId),
  withAppAuthority: ({ actor: who, appId }, callback) => callback(context(who, appId)), ...overrides });
const make = (t, config, overrides) => { const service = createReviewsService(host(config, overrides)); t.after(() => service.close()); return service; };
const call = (service, who = actor(), args = { appId: 'app_one' }) => service.execute({ op: 'apps.reviews.context', actor: who, args });
const fails = (fn, code) => assert.throws(fn, error => error.code === code);

test('default context is disabled only after verified participant authority, never anonymous identity', t => {
  const service = make(t, { providers: [], bindings: [] });
  assert.deepEqual(call(service), { mode: 'disabled', subjects: [] });
  fails(() => call(service, actor('stranger')), 'reviews_authentication_required');
  fails(() => service.execute({ op: 'apps.reviews.collect', actor: actor(), args: { appId: 'app_one' } }), 'unsupported_operation');
});

test('typed app, project and person contexts expose only pinned public URLs and retain unprobed state', t => {
  const config = { providers: [structuredClone(provider)], bindings: [binding('person', 3), binding('app', 1), binding('project', 2)] };
  const service = make(t, config), result = call(service);
  assert.equal(result.mode, 'public-read'); assert.equal(result.availability, 'unprobed');
  assert.deepEqual(result.subjects.map(value => value.subjectKind), ['app', 'project', 'person']);
  assert.deepEqual(result.subjects.map(value => value.providerEntityType), ['product', 'project', 'profile']);
  for (const subject of result.subjects) {
    assert.equal(subject.api.subject, `${provider.origin}/api/public/v1/subjects/${subject.providerSubjectId}`);
    assert.equal(subject.api.rating, subject.api.subject + '/rating'); assert.equal(subject.api.reviews, subject.api.subject + '/reviews');
    assert.equal(subject.publicPageUrl, `${provider.origin}/subjects/${subject.providerSubjectId}`);
    assert(Object.isFrozen(subject)); assert(Object.isFrozen(subject.api));
    for (const key of ['ready', 'canCollect', 'canReply', 'canModerate', 'localSubject', 'ownerId', 'issuer', 'sub', 'email']) assert(!Object.hasOwn(subject, key));
  }
  assert(!JSON.stringify(result).includes('private_local')); assert(Object.isFrozen(result));
  assert.deepEqual(service.origins(), [provider.origin]);
});

test('registration pins accept the same public-read contract while a different approved provider cannot borrow its subject', t => {
  const config = configuration();
  config.providers.push({ id: 'other:public-v1', version: 1, digest: 'd'.repeat(64), origin: 'https://other.example' });
  const service = make(t, config), approved = service.approvedReferences();
  assert.deepEqual(approved.publicSubjects, [{ ...config.bindings[0].subjectRef, provider: ref(provider) }]);
  assert(approved.providers.every(value => value.kind === 'reviews' && value.publicRead === true));
  const policy = captureReviewedConfiguration(DEFAULT_FEEDBACK_PROFILE, approved);
  const admission = createAdmissionHost({ context: { scope: scope(), namespace: 'app.app_one', ownerId: 'owner',
    authorityRevision: 1, visibility: 'private', source: { id: 'apps:app_one/target', revision: 1, digest: 'b'.repeat(64) }, auth: { mode: 'public' } },
    ...policy.approvedReferences, authorProfile: DEFAULT_FEEDBACK_PROFILE });
  try {
    const draft = structuredClone(materializeAuthorDraft(admission, { title: 'Synthetic' }));
    draft.reviews = { mode: 'public-read', provider: ref(provider), subjects: [config.bindings[0].subjectRef] };
    assert.equal(planAdmission(admission, draft, 'reviews.pin-conformance').reviews.mode, 'public-read');
    fails(() => planAdmission(admission, { ...draft, reviews: { ...draft.reviews, provider: ref(config.providers[1]) } }, 'reviews.foreign-provider'), 'review_not_public');
  } finally { closeAdmissionHost(admission); }
});

test('current Apps access, owner-bound scope and active actor control every repeated context read', t => {
  let allowed = true, active = true, currentOwner = 'owner';
  const service = make(t, configuration(), { actorActive: () => active,
    withAppAuthority({ actor: who, appId }, callback) {
      if (!allowed) throw Object.assign(new Error('apps_access_denied'), { code: 'apps_access_denied' });
      return callback(context(who, appId, currentOwner));
    } });
  assert.equal(call(service).mode, 'public-read');
  assert.equal(call(service, actor(), { appId: 'app_two' }).mode, 'disabled');
  allowed = false; fails(() => call(service), 'apps_access_denied'); allowed = true;
  currentOwner = 'new_owner'; assert.equal(call(service).mode, 'disabled');
  currentOwner = 'owner'; active = false; fails(() => call(service), 'reviews_authentication_required');
});

test('retained, skipped, repeated and foreign-context callbacks fail closed and cannot forge returned readiness', t => {
  let mode = 'skip', retained;
  const service = make(t, configuration(), { withAppAuthority({ actor: who, appId }, callback) {
    retained = callback; const normal = context(who, appId);
    if (mode === 'skip') return { mode: 'public-read', availability: 'ready', subjects: [] };
    if (mode === 'app') return callback({ ...normal, appId: 'app_foreign' });
    if (mode === 'actor') return callback({ ...normal, accountId: 'two' });
    if (mode === 'owner') return callback({ ...normal, canManage: true });
    if (mode === 'double') { callback(normal); return callback(normal); }
    callback(normal); return { mode: 'disabled', subjects: [] };
  } });
  fails(() => call(service), 'reviews_authority_fence_invalid');
  fails(() => retained(context(actor())), 'reviews_authority_fence_invalid');
  for (const bad of ['app', 'actor', 'owner']) { mode = bad; fails(() => call(service), 'reviews_authority_context_invalid'); }
  mode = 'double'; fails(() => call(service), 'reviews_authority_fence_invalid');
  mode = 'normal'; assert.equal(call(service).availability, 'unprobed');
  fails(() => retained(context(actor())), 'reviews_authority_fence_invalid');
});

test('snapshots preserve original actor, arguments and trusted config even if retained host objects change', t => {
  const config = configuration(), who = actor(), args = { appId: 'app_one' };
  let captured;
  const service = make(t, config, { withAppAuthority({ actor: pinned, appId }, callback) {
    captured = pinned; who.accountId = 'two'; args.appId = 'app_elsewhere'; return callback(context(pinned, appId));
  } });
  config.providers[0].origin = 'https://changed.example'; config.bindings[0].providerSubjectId = subjectId(9);
  const result = call(service, who, args);
  assert.equal(captured.accountId, 'one'); assert(Object.isFrozen(captured));
  assert.equal(result.subjects[0].api.subject, `${provider.origin}/api/public/v1/subjects/${subjectId(1)}`);
});

test('HTTPS origins are canonical, and local HTTP is permitted only by explicit synthetic fixture opt-in', t => {
  for (const origin of ['https://reviews.example/', 'https://user:pass@reviews.example', 'https://reviews.example/path',
    'https://reviews.example?subject=x', 'https://reviews.example#fragment', 'http://reviews.example', 'file:///tmp/reviews',
    'http://127.0.0.1:44001', 'http://localhost:44001', 'http://[::1]:44001']) {
    const config = configuration(); config.providers[0].origin = origin;
    fails(() => createReviewsService(host(config)), 'reviews_origin_invalid');
  }
  for (const origin of ['http://127.0.0.1:44001', 'http://localhost:44001', 'http://[::1]:44001']) {
    const config = configuration(); config.providers[0].origin = origin;
    const service = make(t, config, { allowFixtureOrigins: true }); assert.equal(call(service).subjects[0].api.subject.startsWith(origin + '/'), true);
  }
  const external = configuration(); external.providers[0].origin = 'http://external.example:44001';
  fails(() => createReviewsService(host(external, { allowFixtureOrigins: true })), 'reviews_origin_invalid');
});

test('provider resolution requires exact ID, version and digest; metadata cannot add managed authority', () => {
  for (const change of [{ id: 'foreign:public-v1' }, { version: 2 }, { digest: 'f'.repeat(64) }]) {
    const config = configuration(); Object.assign(config.bindings[0].providerRef, change);
    fails(() => createReviewsService(host(config)), 'reviews_provider_pin_mismatch');
  }
  for (const change of [{ mode: 'managed' }, { rights: ['moderate'] }, { canCollect: true }, { origin: 'https://foreign.example' },
    { issuer: 'https://identity.example' }, { providerSubjectId: 'subject_1/../../private' }]) {
    const config = configuration(); Object.assign(config.bindings[0], change);
    fails(() => createReviewsService(host(config)), 'reviews_configuration_invalid');
  }
});

test('typed local subjects cannot inherit another app or conflate a profile, project and product', () => {
  for (const [kind, entityType] of [['person', 'product'], ['app', 'profile'], ['project', 'service']]) {
    const config = configuration(); config.bindings[0] = binding(kind); config.bindings[0].providerEntityType = entityType;
    fails(() => createReviewsService(host(config)), 'reviews_subject_kind_mismatch');
  }
  const otherApp = configuration(); otherApp.bindings[0].localSubject.id = 'app_other';
  fails(() => createReviewsService(host(otherApp)), 'reviews_subject_kind_mismatch');
  for (const change of [{ registryId: 'foreign' }, { environmentId: 'development' }]) {
    const config = configuration(); Object.assign(config.bindings[0].scope, change);
    fails(() => createReviewsService(host(config)), 'reviews_scope_mismatch');
  }
});

test('duplicate providers, same-version subject rewrites and multiple subjects of one local kind are rejected', () => {
  const duplicate = configuration(); duplicate.providers.push({ ...duplicate.providers[0], origin: 'https://other.example' });
  fails(() => createReviewsService(host(duplicate)), 'reviews_provider_pin_conflict');
  for (const change of [{ providerSubjectId: subjectId(2) }, { subjectRef: { ...binding().subjectRef, digest: 'c'.repeat(64) } }]) {
    const config = configuration(), copy = { ...binding('app', 1, 'app_other'), ...change };
    config.bindings.push(copy); fails(() => createReviewsService(host(config)), 'reviews_subject_pin_conflict');
  }
  const kind = configuration(); kind.bindings.push(binding('app', 2));
  fails(() => createReviewsService(host(kind)), 'reviews_binding_conflict');
});

test('config cardinality and compiled lookups are bounded; saturation neither evicts nor cross-links an old app', t => {
  const config = { providers: [structuredClone(provider)], bindings: Array.from({ length: REVIEWS_LIMITS.subjectBindings }, (_, i) => binding('app', 1, 'app_' + i)) };
  const service = make(t, config);
  assert.equal(call(service, actor(), { appId: 'app_0' }).subjects[0].providerSubjectId, subjectId(1));
  assert.equal(call(service, actor(), { appId: 'app_127' }).subjects[0].providerSubjectId, subjectId(1));
  assert.equal(call(service, actor(), { appId: 'app_128' }).mode, 'disabled');
  config.bindings.push(binding('app', 1, 'app_extra'));
  assert.throws(() => createReviewsService(host(config)), error => error.status === 500);
  const providerConfig = { providers: Array.from({ length: REVIEWS_LIMITS.providerPins }, (_, i) => ({ ...provider, id: `povedai:provider-${i}` })), bindings: [] };
  const bounded = make(t, providerConfig); assert.equal(bounded.approvedReferences().providers.length, 128);
  providerConfig.providers.push({ ...provider, id: 'povedai:overflow' });
  assert.throws(() => createReviewsService(host(providerConfig)), error => error.status === 500);
});

test('no accessors or asynchronous fences are accepted, final revocation is checked, and close disposes retained callbacks', t => {
  let invoked = false;
  const config = configuration(); Object.defineProperty(config.providers[0], 'origin', { enumerable: true, get() { invoked = true; return provider.origin; } });
  fails(() => createReviewsService(host(config)), 'reviews_configuration_invalid'); assert.equal(invoked, false);
  assert.throws(() => createReviewsService(host(configuration(), { actorActive: async () => true })), error => error.code === 'reviews_host_required');
  const thenable = make(t, configuration(), { withAppAuthority(_request, _callback) { return Promise.resolve(null); } });
  fails(() => call(thenable), 'reviews_async_authority');
  let active = true;
  const revoked = make(t, configuration(), { actorActive: () => active, withAppAuthority({ actor: who, appId }, callback) {
    const result = callback(context(who, appId)); active = false; return result;
  } });
  fails(() => call(revoked), 'reviews_authentication_required');
  let held;
  const service = make(t, configuration(), { withAppAuthority({ actor: who, appId }, callback) { held = callback; return callback(context(who, appId)); } });
  call(service); service.close(); service.close();
  fails(() => call(service), 'reviews_closed'); fails(() => service.origins(), 'reviews_closed'); fails(() => service.approvedReferences(), 'reviews_closed');
  fails(() => held(context(actor())), 'reviews_authority_fence_invalid');
});

test('request bodies cannot select an identity, provider, subject, remote URL or managed operation', t => {
  const service = make(t);
  for (const extra of [{ providerRef: ref(provider) }, { subjectId: subjectId(1) }, { origin: provider.origin }, { ownerId: 'owner' },
    { localSubject: { kind: 'person', id: 'one' } }, { url: 'https://foreign.example' }, { rights: ['publish'] }]) {
    fails(() => call(service, actor(), { appId: 'app_one', ...extra }), 'reviews_invalid_arguments');
  }
  for (const path of ['//foreign.example', '/\\foreign.example', '/%2f%2fforeign.example']) fails(() => call(service, actor(), { appId: 'app_one', path }), 'reviews_invalid_arguments');
  assert.equal(call(service, actor(), { appId: 'app_one', domainId: 'domain_one', path: '/page?tab=reviews' }).mode, 'public-read');
});

test('scoped host selection includes only its own tenant/app refs and is stable when an unrelated app is added', t => {
  const config = { providers: [structuredClone(provider)], bindings: [binding('app', 1, 'app_one'), binding('app', 2, 'app_two')] };
  const first = make(t, config), selected = first.approvedReferencesFor(scope('app_one'));
  assert.deepEqual(selected.references.providers, [{ ...ref(provider), kind: 'reviews', publicRead: true }]);
  assert.deepEqual(selected.references.publicSubjects, [{ ...config.bindings[0].subjectRef, provider: ref(provider) }]);
  config.bindings.unshift(binding('app', 3, 'app_three'));
  config.providers.unshift({ ...provider, id: 'other:unused', origin: 'https://unused.example' });
  const second = make(t, config);
  assert.deepEqual(second.approvedReferencesFor(scope('app_one')), selected);
  assert.deepEqual(second.approvedReferencesFor(scope('app_one', 'another_owner')).references, { providers: [], publicSubjects: [] });
  assert(Object.isFrozen(selected) && Object.isFrozen(selected.references));
  fails(() => second.approvedReferencesFor({ ...scope(), environmentId: 'production' }), 'reviews_scope_mismatch');
  second.close(); fails(() => second.approvedReferencesFor(scope()), 'reviews_closed');
});

test('operational binding digest changes on its provider origin or private association change while semantic pins remain unchanged', t => {
  const config = { providers: [structuredClone(provider)], bindings: [binding('person', 1)] };
  const before = make(t, config).approvedReferencesFor(scope());
  for (const change of ['origin', 'association']) {
    const modified = structuredClone(config);
    if (change === 'origin') modified.providers[0].origin = 'https://new-approved.example';
    else modified.bindings[0].localSubject.id = 'different_private_person';
    const after = make(t, modified).approvedReferencesFor(scope());
    assert.deepEqual(after.references, before.references);
    assert.notEqual(after.bindingDigest, before.bindingDigest);
    assert(!JSON.stringify(after.references).includes('private_person'));
  }
});
