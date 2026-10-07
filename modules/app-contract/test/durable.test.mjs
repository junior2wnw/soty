import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve, dirname, basename } from 'node:path';
import { spawn } from 'node:child_process';
import { createDescriptor } from '../sdk.mjs';
import { contractDigest, createAdmissionHost, closeAdmissionHost, materializeAuthorDraft, capabilityDigest } from '../index.mjs';
import { createUniversalRegistrationService } from '../server/registration.mjs';
import { buildTrustedAppConfiguration, captureReviewedConfiguration, refPart } from '../server/authority.mjs';
import { inspectRegistrationSchema } from '../server/schema.mjs';
import { createReviewsService } from '../../reviews/server/index.mjs';

const appId = 'app-' + 'a'.repeat(32), actor = { accountId: 'account-' + '1'.repeat(32), deviceId: 'device-01' };
const outsider = { accountId: 'account-' + '2'.repeat(32), deviceId: 'device-02' };
const clone = value => structuredClone(value);
const throws = (fn, code) => assert.throws(fn, error => error.code === code, code);
const pin = (id, digit) => ({ id, version: 1, digest: digit.repeat(64) });
function profile() {
  return { ...pin('platform:author.default', 'a'), feedback: { mode: 'required', provider: pin('feedback:inbox', 'b'),
    captureProfile: pin('feedback:text', 'c'), retentionProfile: pin('feedback:retention', 'd'),
    submitAudience: 'members', ticketVisibility: 'reporter-and-support' } };
}
function source() {
  return { appId, ownerId: actor.accountId, accountId: actor.accountId, appRevision: 1, policyEpoch: 1,
    target: { revision: 1, digest: 'e'.repeat(64), profile: 'web-standard-v1' }, visibility: 'private',
    grants: { accountIds: [], communityIds: [] } };
}
function fixture(t, options = {}) {
  const directory = mkdtempSync(join(tmpdir(), 'soty-registration-')), databasePath = join(directory, 'registry.sqlite');
  const snapshots = new Map([[appId, source()]]), services = [], active = new Set([actor.accountId, outsider.accountId]);
  let boundary = ({ actor: verified, appId: requested }, callback) => {
    const current = snapshots.get(requested);
    if (!current || verified.accountId !== current.ownerId) throw Object.assign(new Error('synthetic owner denied'), { code: 'registration_app_not_owned' });
    return callback({ ...clone(current), accountId: verified.accountId });
  };
  const configuration = { databasePath, registryId: 'registry01', environmentId: 'development',
    reviewedProfile: profile(), ...options };
  const open = (extra = {}) => {
    const service = createUniversalRegistrationService({ ...configuration,
      actorActive: captured => active.has(captured.accountId),
      withReviewedAppAuthority: (...args) => boundary(...args), ...extra });
    services.push(service); return service;
  };
  t.after(() => {
    for (const service of services) service.close();
    assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(basename(directory), /^soty-registration-/u);
    rmSync(directory, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 });
  });
  const args = (changes = {}) => ({ expectedAccountId: actor.accountId, appId, requestId: 'request-01', expectedRevision: 0,
    proposal: { kind: 'author-draft', draft: { title: 'Первое приложение' } }, ...changes });
  const call = (service, op = 'admit', input = args(), as = actor) => service.execute({ op: 'apps.universal.' + op, actor: as, args: input });
  return { ...configuration, directory, snapshots, active, open, args, call, setBoundary(fn) { boundary = fn; },
    readArgs: { expectedAccountId: actor.accountId, appId } };
}
function feedbackPort() {
  const installed = new Map(); let calls = 0, effects = 0, crashAfterCommit = false;
  const key = request => contractDigest(request);
  const port = {
    ensureInstallation(request) {
      calls++;
      if (!installed.has(key(request))) {
        effects++;
        installed.set(key(request), { installationId: 'installation-' + effects, receiptDigest: contractDigest(request),
          provisioningKey: request.provisioningKey, scope: request.scope, source: request.source,
          profile: refPart(request.profile), authorityDigest: request.authorityDigest, generation: request.generation });
      }
      if (crashAfterCommit) { crashAfterCommit = false; throw Object.assign(new Error('synthetic commit ACK lost'), { code: 'synthetic_ack_lost' }); }
      return installed.get(key(request));
    },
    inspectInstallation(request) { return installed.get(key(request)) || null; },
  };
  return { port, installed, get calls() { return calls; }, get effects() { return effects; },
    crashOnce() { crashAfterCommit = true; } };
}

test('durable title-only registration owns its source and never installs grants or fabricates feedback readiness', t => {
  const f = fixture(t), inbox = feedbackPort(), service = f.open({ feedback: inbox.port });
  const plan = f.call(service, 'plan');
  assert.equal(plan.descriptor.app.namespace, 'app.' + appId);
  assert.deepEqual(plan.descriptor.app.auth, { mode: 'public' });
  assert.equal(plan.descriptor.app.visibility, 'private');
  assert.deepEqual(plan.descriptor.capabilities, []);
  assert.deepEqual(plan.descriptor.reviews, { mode: 'disabled' });
  assert.equal('ownerId' in plan, false); assert.equal('grants' in plan, false);
  const saved = f.call(service);
  assert.equal(saved.registration.state, 'pending-feedback');
  assert.deepEqual(saved.registration.gates, { ui: 'pending-feedback', agent: 'not-admitted', local: 'not-admitted' });
  assert.equal(inbox.calls, 0); assert.equal(inbox.effects, 0);
  const db = new DatabaseSync(f.databasePath);
  try {
    assert.equal(db.prepare('SELECT count(*) AS n FROM registration_heads').get().n, 1);
    assert.equal(db.prepare('SELECT count(*) AS n FROM registration_versions').get().n, 1);
    assert.equal(db.prepare('SELECT count(*) AS n FROM registration_receipts').get().n, 1);
    assert.equal(db.prepare('SELECT count(*) AS n FROM registration_feedback_outbox').get().n, 1);
  } finally { db.close(); }
});

test('restart and lost-ACK retry keep the same durable receipt, reject changed intent and cannot rerun provisioning', t => {
  const f = fixture(t), service = f.open(), first = f.call(service); service.close();
  const second = f.open(), replay = f.call(second);
  assert.equal(replay.replayed, true); assert.deepEqual(replay.receipt, first.receipt);
  assert.equal(replay.registration.revision, 1);
  throws(() => f.call(second, 'admit', f.args({ proposal: { kind: 'author-draft', draft: { title: 'Changed intent' } } })), 'registration_intent_conflict');
  throws(() => f.call(second, 'admit', f.args({ expectedRevision: 1 })), 'registration_intent_conflict');
  throws(() => f.call(second, 'admit', f.args({ requestId: 'request-02' })), 'registration_revision_conflict');
});

test('unknown ACK after registration COMMIT is recovered from the immutable request receipt', t => {
  const f = fixture(t), service = f.open(); let fail = true;
  f.setBoundary(({ appId: id }, callback) => {
    const result = callback(f.snapshots.get(id));
    if (fail) { fail = false; throw Object.assign(new Error('synthetic fence-release'), { code: 'synthetic_release_failure' }); }
    return result;
  });
  throws(() => f.call(service), 'synthetic_release_failure');
  assert.equal(f.call(service).replayed, true);
  assert.equal(f.call(service, 'history', f.readArgs).items.length, 1);
});

test('stolen account/app, revoked device, wrong expected account and input authority fields fail closed', t => {
  const f = fixture(t), service = f.open();
  throws(() => f.call(service, 'admit', f.args(), outsider), 'registration_account_mismatch');
  throws(() => f.call(service, 'admit', f.args({ expectedAccountId: outsider.accountId }), outsider), 'registration_app_not_owned');
  throws(() => f.call(service, 'admit', f.args({ appId: 'app-' + 'b'.repeat(32) })), 'registration_app_not_owned');
  for (const field of ['ownerId', 'tenantId', 'environmentId', 'ready', 'reviewedProfile', 'source', 'Host']) {
    throws(() => f.call(service, 'admit', f.args({ [field]: true })), 'registration_fields_invalid');
  }
  f.active.delete(actor.accountId);
  throws(() => f.call(service), 'registration_authentication_required');
  f.active.add(actor.accountId);
  f.setBoundary((_input, callback) => callback({ ...source(), ownerId: outsider.accountId }));
  throws(() => f.call(service), 'registration_app_not_owned');
});

test('source/policy authority changes withdraw descriptor bodies and fence old replay and pending completion', t => {
  const f = fixture(t), inbox = feedbackPort(), service = f.open({ feedback: inbox.port });
  const initial = f.call(service); const current = f.snapshots.get(appId);
  current.target.revision = 2; current.target.digest = 'f'.repeat(64); current.policyEpoch++;
  const read = f.call(service, 'get', f.readArgs).registration;
  assert.equal(read.state, 'authority-stale'); assert.equal(read.gates.ui, 'held'); assert.equal('descriptor' in read, false);
  throws(() => f.call(service), 'registration_authority_changed');
  throws(() => service.reconcileFeedback({ actor, appId, expectedRevision: 1 }), 'registration_authority_changed');
  assert.equal(inbox.effects, 0);
  const next = f.call(service, 'admit', f.args({ expectedRevision: 1, requestId: 'request-02' }));
  assert.equal(next.registration.generation, 2); assert.notEqual(next.receipt.authorityDigest, initial.receipt.authorityDigest);
  assert.equal(next.registration.descriptor.app.source.revision, 2);
});

test('real committed feedback proof unlocks UI only and survives lost ACK between the two stores', t => {
  const f = fixture(t), inbox = feedbackPort(), service = f.open({ feedback: inbox.port }); f.call(service);
  inbox.crashOnce(); throws(() => service.reconcileFeedback({ actor, appId, expectedRevision: 1 }), 'synthetic_ack_lost');
  assert.equal(inbox.effects, 1);
  assert.equal(f.call(service, 'get', f.readArgs).registration.state, 'pending-feedback');
  service.close(); const reopened = f.open({ feedback: inbox.port });
  const ready = reopened.reconcileFeedback({ actor, appId, expectedRevision: 1 }).registration;
  assert.equal(ready.state, 'ready'); assert.equal(ready.revision, 2); assert.equal(ready.generation, 1);
  assert.deepEqual(ready.gates, { ui: 'ready', agent: 'not-admitted', local: 'not-admitted' });
  assert.equal(inbox.calls, 1); assert.equal(inbox.effects, 1);
  assert.equal(f.call(reopened).registration.state, 'ready');
  throws(() => reopened.reconcileFeedback({ actor, appId, expectedRevision: 1 }), 'registration_revision_conflict');
  assert.equal(reopened.reconcileFeedback({ actor, appId, expectedRevision: 2 }).registration.revision, 2);
});

test('persisted ready is held without current committed provider evidence and never trusts a replaced receipt', t => {
  const f = fixture(t), inbox = feedbackPort(), service = f.open({ feedback: inbox.port }); f.call(service);
  service.reconcileFeedback({ actor, appId, expectedRevision: 1 }); service.close();
  const noProvider = f.open(), held = f.call(noProvider, 'get', f.readArgs).registration;
  assert.equal(held.state, 'feedback-held'); assert.equal(held.gates.ui, 'held'); assert.equal('installationId' in held.feedback, false);
  noProvider.close(); const provider = f.open({ feedback: inbox.port });
  const original = [...inbox.installed.entries()][0]; inbox.installed.clear();
  assert.equal(f.call(provider, 'get', f.readArgs).registration.gates.ui, 'held');
  inbox.installed.set(original[0], { ...original[1], installationId: 'replacement' });
  throws(() => f.call(provider, 'get', f.readArgs), 'feedback_receipt_conflict');
  inbox.installed.set(...original); assert.equal(f.call(provider, 'get', f.readArgs).registration.gates.ui, 'ready');
});

test('ensure success without inspect evidence and wrong scope/source/profile/generation receipts cannot attest ready', t => {
  const f = fixture(t); let request;
  const invisible = { ensureInstallation(value) { request = value; return null; }, inspectInstallation() { return null; } };
  const service = f.open({ feedback: invisible }); f.call(service);
  assert.equal(service.reconcileFeedback({ actor, appId, expectedRevision: 1 }).registration.state, 'pending-feedback');
  service.close();
  const good = { installationId: 'proof01', receiptDigest: '1'.repeat(64), provisioningKey: request.provisioningKey,
    scope: request.scope, source: request.source, profile: refPart(request.profile), authorityDigest: request.authorityDigest, generation: 1 };
  for (const mutate of [
    proof => { proof.scope = { ...proof.scope, tenantId: outsider.accountId }; },
    proof => { proof.scope = { ...proof.scope, environmentId: 'production' }; },
    proof => { proof.source = { ...proof.source, digest: '0'.repeat(64) }; },
    proof => { proof.profile = { ...proof.profile, version: 2 }; },
    proof => { proof.generation = 2; },
    proof => { proof.ready = true; },
  ]) {
    const bad = clone(good); mutate(bad);
    const another = f.open({ feedback: { ensureInstallation() { return bad; }, inspectInstallation() { return bad; } } });
    throws(() => another.reconcileFeedback({ actor, appId, expectedRevision: 1 }), 'feedback_receipt_mismatch'); another.close();
  }
});

test('same-version configuration rewrites and remove/readd are rejected durably, while new profile version withdraws prior readiness', t => {
  const f = fixture(t), inbox = feedbackPort(), first = f.open({ feedback: inbox.port }); f.call(first);
  first.reconcileFeedback({ actor, appId, expectedRevision: 1 }); first.close();
  const changed = profile(); changed.feedback.retentionProfile.digest = '8'.repeat(64);
  const wrong = f.open({ reviewedProfile: changed });
  throws(() => f.call(wrong, 'get', f.readArgs), 'immutable_pin_conflict'); wrong.close();
  const next = profile(); next.version = 2; next.digest = '9'.repeat(64); next.feedback.retentionProfile.version = 2; next.feedback.retentionProfile.digest = '8'.repeat(64);
  const latest = f.open({ reviewedProfile: next });
  assert.equal(f.call(latest, 'get', f.readArgs).registration.state, 'authority-stale'); latest.close();
  const readded = f.open({ reviewedProfile: changed });
  throws(() => f.call(readded, 'get', f.readArgs), 'immutable_pin_conflict');
});

test('durable advanced proposals validate approved exact semantic bindings without creating runtime grants', t => {
  const f = fixture(t), policy = captureReviewedConfiguration(profile());
  const built = buildTrustedAppConfiguration({ sourceSnapshot: source(), actor, registryId: f.registryId, environmentId: f.environmentId, ...policy });
  const host = createAdmissionHost(built.configuration), basic = materializeAuthorDraft(host, { title: 'Advanced' }); closeAdmissionHost(host);
  const cap = { id: 'app.' + appId + ':draft', version: 1,
    inputSchema: { type: 'object', properties: { title: { type: 'string', maxLength: 80 } }, additionalProperties: false, required: ['title'] },
    outputSchema: { type: 'object', properties: {}, additionalProperties: false }, resources: [pin('app.' + appId + ':drafts', '2')],
    effects: ['create'], recipients: [pin('platform:storage', '3')], binding: pin('app.' + appId + ':binding', '4') };
  const descriptor = createDescriptor({ ...basic, capabilities: [cap] });
  const unbound = f.open();
  const args = f.args({ proposal: { kind: 'descriptor-json', json: JSON.stringify(descriptor) } });
  const future = { ...descriptor, schema: 'soty.app-agent.v99' };
  throws(() => f.call(unbound, 'admit', f.args({ proposal: { kind: 'descriptor-json', json: JSON.stringify(future) } })), 'unsupported_schema');
  throws(() => f.call(unbound, 'admit', args), 'binding_missing'); unbound.close();
  const binding = { ...cap.binding, scope: built.configuration.context.scope, source: built.configuration.context.source,
    capability: { id: descriptor.capabilities[0].id, version: 1, digest: descriptor.capabilities[0].digest } };
  const bound = f.open({ approvedReferences: { bindings: [binding] } }), saved = f.call(bound, 'admit', args);
  assert.equal(saved.registration.descriptor.capabilities.length, 1); assert.equal(saved.registration.gates.agent, 'not-admitted');
  const changed = clone(descriptor); changed.capabilities[0].effects = ['delete']; changed.capabilities[0].digest = capabilityDigest(changed.capabilities[0]);
  throws(() => f.call(bound, 'admit', f.args({ expectedRevision: 1, requestId: 'request-02',
    proposal: { kind: 'descriptor-json', json: JSON.stringify(changed) } })), 'binding_contract_mismatch');
  changed.app.title = 'Cosmetic'; changed.capabilities[0] = { ...descriptor.capabilities[0], title: 'New prose' };
  assert.equal(f.call(bound, 'admit', f.args({ expectedRevision: 1, requestId: 'request-02',
    proposal: { kind: 'descriptor-json', json: JSON.stringify(changed) } })).registration.generation, 2);
});

test('approved host replacement cannot reinterpret a pinned semantic version through a differently named binding', t => {
  const f = fixture(t), policy = captureReviewedConfiguration(profile());
  const built = buildTrustedAppConfiguration({ sourceSnapshot: source(), actor, registryId: f.registryId, environmentId: f.environmentId, ...policy });
  const capability = pin('app.' + appId + ':effect', '1'), binding = { ...pin('app.' + appId + ':bindingA', '2'),
    scope: built.configuration.context.scope, source: built.configuration.context.source, capability };
  const service = f.open({ approvedReferences: { bindings: [binding] } }); f.call(service); service.close();
  const replacement = { ...binding, ...pin('app.' + appId + ':bindingB', '3'), capability: { ...capability, digest: '4'.repeat(64) } };
  const wrong = f.open({ approvedReferences: { bindings: [replacement] } });
  throws(() => f.call(wrong, 'get', f.readArgs), 'immutable_capability_conflict');
});

test('reference and app quotas roll back rejected authority changes without evicting accepted history', t => {
  const f = fixture(t, { limits: { appsPerOwner: 1, referenceHistory: 4, versionsPerApp: 1 } }), service = f.open(); f.call(service);
  const otherApp = 'app-' + 'b'.repeat(32); f.snapshots.set(otherApp, { ...source(), appId: otherApp });
  throws(() => f.call(service, 'get', { ...f.readArgs, appId: otherApp }), 'registration_app_limit');
  throws(() => f.call(service, 'admit', f.args({ expectedRevision: 1, requestId: 'next-version' })), 'registration_version_limit');
  service.close(); const nextProfile = profile(); nextProfile.version = 2; nextProfile.digest = '5'.repeat(64);
  const full = f.open({ reviewedProfile: nextProfile });
  throws(() => f.call(full, 'get', f.readArgs), 'registration_reference_history_limit'); full.close();
  const unchanged = f.open(); assert.equal(f.call(unchanged).replayed, true);
  const db = new DatabaseSync(f.databasePath);
  try {
    assert.equal(db.prepare('SELECT count(*) AS n FROM registration_reference_history').get().n, 4);
    assert.equal(db.prepare('SELECT count(*) AS n FROM registration_authorities').get().n, 1);
  } finally { db.close(); }
});

test('bounded JSON ingress rejects escaped duplicate keys, future schemas, unsafe numbers and secret payload without echoing input', t => {
  const f = fixture(t), service = f.open(), sentinel = 'NEVER_ECHO_PRIVATE_SENTINEL';
  for (const [json, code] of [
    ['{"schema":"soty.app-agent.v1","sch\\u0065ma":"soty.app-agent.v1"}', 'duplicate_key'],
    ['{"schema":"soty.app-agent.v99"}', 'closed_fields'],
    ['{"value":9007199254740992}', 'unsafe_number'],
    ['{"x":"Bearer ' + sentinel + '"}', 'secret_not_allowed'],
    ['['.repeat(20) + '0' + ']'.repeat(20), 'input_limit'],
    [' '.repeat(65537), 'input_limit'],
  ]) {
    const args = f.args({ proposal: { kind: 'descriptor-json', json } });
    assert.throws(() => f.call(service, 'admit', args), error => error.code === code && !String(error).includes(sentinel));
  }
});

test('no history eviction: quotas preserve exact replay and bounded owner-only keyset pagination', t => {
  const f = fixture(t, { limits: { receiptsPerOwner: 3, versionsPerApp: 3, historyPage: 2 } }), service = f.open();
  const initial = f.call(service);
  for (let revision = 1; revision < 3; revision++) f.call(service, 'admit', f.args({ expectedRevision: revision, requestId: 'request-' + (revision + 1) }));
  assert.deepEqual(f.call(service).receipt, initial.receipt);
  throws(() => f.call(service, 'admit', f.args({ expectedRevision: 3, requestId: 'request-04' })), 'registration_receipt_limit');
  const page = f.call(service, 'history', { ...f.readArgs, limit: 2 });
  assert.deepEqual(page.items.map(item => item.generation), [3, 2]); assert.ok(page.nextCursor);
  assert.equal(JSON.stringify(page).includes('Первое приложение'), false);
  const last = f.call(service, 'history', { ...f.readArgs, limit: 2, cursor: page.nextCursor });
  assert.deepEqual(last.items.map(item => item.generation), [1]); assert.equal(last.nextCursor, null);
  throws(() => f.call(service, 'history', { ...f.readArgs, limit: 3 }), 'registration_revision_invalid');
  const otherApp = 'app-' + 'b'.repeat(32); f.snapshots.set(otherApp, { ...source(), appId: otherApp });
  throws(() => f.call(service, 'history', { ...f.readArgs, appId: otherApp, cursor: page.nextCursor }), 'registration_cursor_invalid');
});

test('durable namespace/source identity and storage environment cannot silently be renamed on reopen', t => {
  const f = fixture(t), service = f.open(); f.call(service); service.close();
  throws(() => f.open({ registryId: 'different-registry' }), 'registration_registry_mismatch');
  throws(() => f.open({ environmentId: 'production' }), 'registration_environment_mismatch');
  const db = new DatabaseSync(f.databasePath);
  try {
    assert.equal(inspectRegistrationSchema(db).schemaVersion, 1);
    assert.throws(() => db.exec("UPDATE registration_metadata SET value='production' WHERE key='environment_id'"));
    assert.throws(() => db.exec('DELETE FROM registration_heads'));
    assert.throws(() => db.exec("UPDATE registration_receipts SET intent_hash='" + '0'.repeat(64) + "'"));
    db.exec('PRAGMA user_version=99');
  } finally { db.close(); }
  throws(() => f.open(), 'registration_schema_unsupported');
});

test('schema recognition rejects missing guards and undeclared additive tables before serving', t => {
  const f = fixture(t), service = f.open(); service.close();
  const db = new DatabaseSync(f.databasePath);
  try { db.exec('DROP TRIGGER registration_heads_no_delete'); } finally { db.close(); }
  throws(() => f.open(), 'registration_schema_invalid');
});

test('source fence must be single, synchronous and active; expired callbacks and promise providers are rejected', t => {
  const f = fixture(t), service = f.open(); let delayed;
  f.setBoundary((_input, callback) => { delayed = callback; return undefined; });
  throws(() => f.call(service), 'registration_authority_fence_invalid');
  throws(() => delayed(source()), 'registration_authority_fence_invalid');
  f.setBoundary((_input, callback) => { const first = callback(source()); callback(source()); return first; });
  throws(() => f.call(service), 'registration_authority_fence_invalid');
  // First callback committed; its unknown ACK is recoverable and the second cannot mutate it.
  f.setBoundary((_input, callback) => callback(source())); assert.equal(f.call(service).replayed, true);
  const asyncPort = f.open({ feedback: { ensureInstallation() { return Promise.resolve(null); }, inspectInstallation() { return Promise.resolve(null); } } });
  throws(() => asyncPort.reconcileFeedback({ actor, appId, expectedRevision: 1 }), 'registration_async_boundary');
});

test('captured host/profile objects are immutable snapshots and default private audience is enforced', t => {
  const f = fixture(t), p = profile(), service = f.open({ reviewedProfile: p }); p.feedback.provider.digest = '0'.repeat(64);
  const saved = f.call(service); assert.equal(saved.registration.descriptor.feedback.provider.digest, 'b'.repeat(64));
  assert.throws(() => { saved.registration.descriptor.app.title = 'mutation'; }, TypeError);
  const publicProfile = profile(); publicProfile.feedback.submitAudience = 'public';
  const wrong = f.open({ reviewedProfile: publicProfile }); throws(() => f.call(wrong), 'audience_privacy');
});

test('close releases resources without deleting durable history or accepting retired service handles', t => {
  const f = fixture(t), service = f.open(); const first = f.call(service);
  assert.equal(service.close(), true); assert.equal(service.close(), false); throws(() => f.call(service), 'registration_closed');
  assert.deepEqual(f.call(f.open()).receipt, first.receipt);
});

function reviewConfiguration(f, ids, { origin = 'https://reviews.example', personOnFirst = false } = {}) {
  const provider = { ...pin('reviews:public', '5'), origin };
  return { providers: [provider], bindings: ids.map((value, index) => ({
    scope: { registryId: f.registryId, tenantId: actor.accountId, appId: value, environmentId: f.environmentId },
    localSubject: personOnFirst && index === 0 ? { kind: 'person', id: 'private_person' } : { kind: 'app', id: value },
    providerRef: refPart(provider), subjectRef: pin('reviews:subject-' + value, '6'),
    providerSubjectId: 'subject_' + String(index + 1).padStart(20, '0'),
    providerEntityType: personOnFirst && index === 0 ? 'profile' : 'product', mode: 'public-read',
  })) };
}
function scopedService(t, f, config, extra = {}) {
  const reviews = createReviewsService({ registryId: f.registryId, environmentId: f.environmentId,
    configuration: config, actorActive: () => true, withAppAuthority() { throw new Error('unused public context'); } });
  t.after(() => reviews.close());
  const service = f.open({ approvedReferences: reviews.approvedReferences(),
    selectApprovedReferences: ({ scope }) => reviews.approvedReferencesFor(scope), ...extra });
  return { reviews, service };
}

test('scoped refs preserve Alpha and Beta exact readiness and receipts when Gamma is onboarded after restart', t => {
  const f = fixture(t), inbox = feedbackPort(), beta = 'app-' + 'b'.repeat(32), gamma = 'app-' + 'c'.repeat(32);
  for (const value of [beta, gamma]) f.snapshots.set(value, { ...source(), appId: value });
  const first = scopedService(t, f, reviewConfiguration(f, [appId, beta]), { feedback: inbox.port }).service;
  const inputs = [appId, beta].map((value, i) => f.args({ appId: value, requestId: 'initial-' + i }));
  const saved = inputs.map(input => f.call(first, 'admit', input));
  for (const value of [appId, beta]) first.reconcileFeedback({ actor, appId: value, expectedRevision: 1 });
  first.close();
  const latest = scopedService(t, f, reviewConfiguration(f, [appId, beta, gamma]), { feedback: inbox.port }).service;
  const admittedGamma = f.call(latest, 'admit', f.args({ appId: gamma, requestId: 'initial-gamma' }));
  assert.equal(admittedGamma.registration.generation, 1);
  for (let i = 0; i < inputs.length; i++) {
    const replay = f.call(latest, 'admit', inputs[i]);
    assert.equal(replay.registration.state, 'ready'); assert.equal(replay.replayed, true);
    assert.deepEqual(replay.receipt, saved[i].receipt);
    assert.equal(f.call(latest, 'history', { ...f.readArgs, appId: inputs[i].appId }).items.length, 1);
  }
});

test('removing or rewriting Alpha refs withdraws Alpha alone, preserves Beta and keeps immutable same-version history', t => {
  const f = fixture(t), beta = 'app-' + 'b'.repeat(32), inbox = feedbackPort(); f.snapshots.set(beta, { ...source(), appId: beta });
  const original = reviewConfiguration(f, [appId, beta]);
  const first = scopedService(t, f, original, { feedback: inbox.port }).service;
  f.call(first); f.call(first, 'admit', f.args({ appId: beta, requestId: 'initial-beta' }));
  first.reconcileFeedback({ actor, appId, expectedRevision: 1 }); first.reconcileFeedback({ actor, appId: beta, expectedRevision: 1 }); first.close();
  const removed = clone(original); removed.bindings.shift();
  const second = scopedService(t, f, removed, { feedback: inbox.port }).service;
  assert.equal(f.call(second, 'get', f.readArgs).registration.state, 'authority-stale');
  assert.equal(f.call(second, 'get', { ...f.readArgs, appId: beta }).registration.state, 'ready'); second.close();
  const rewritten = clone(original); rewritten.bindings[0].subjectRef.digest = '7'.repeat(64);
  const third = scopedService(t, f, rewritten, { feedback: inbox.port }).service;
  throws(() => f.call(third, 'get', f.readArgs), 'immutable_pin_conflict');
  assert.equal(f.call(third, 'get', { ...f.readArgs, appId: beta }).registration.state, 'ready');
});

test('origin or private person association changes hold its old authority while unchanged semantic refs cannot fabricate a current receipt', t => {
  const f = fixture(t), config = reviewConfiguration(f, [appId], { personOnFirst: true });
  const first = scopedService(t, f, config).service, saved = f.call(first); first.close();
  for (const modification of ['origin', 'person']) {
    const changed = clone(config);
    if (modification === 'origin') changed.providers[0].origin = 'https://new-approved.example';
    else changed.bindings[0].localSubject.id = 'different_private_person';
    const service = scopedService(t, f, changed).service;
    const read = f.call(service, 'get', f.readArgs).registration;
    assert.equal(read.state, 'authority-stale'); assert.equal(Object.hasOwn(read, 'descriptor'), false);
    throws(() => f.call(service), 'registration_authority_changed');
    assert.equal(f.call(service, 'history', f.readArgs).items[0].authorityDigest, saved.receipt.authorityDigest); service.close();
  }
});

test('reference selector is a synchronous exact closed subset port; unknown pins, accessors and promises create no authority', t => {
  const f = fixture(t), config = reviewConfiguration(f, [appId]), reviews = scopedService(t, f, config).reviews;
  const selected = reviews.approvedReferencesFor(config.bindings[0].scope);
  throws(() => f.open({ selectApprovedReferences: async () => selected }), 'registration_reference_selector_invalid');
  let getterRan = false;
  const unsafe = { references: selected.references };
  Object.defineProperty(unsafe, 'bindingDigest', { enumerable: true, get() { getterRan = true; return selected.bindingDigest; } });
  for (const [selector, expected] of [
    [() => Promise.resolve(selected), 'registration_async_boundary'],
    [() => ({ ...selected, bindingDigest: 'bad' }), 'registration_reference_selection_invalid'],
    [() => ({ ...selected, url: 'https://foreign.example' }), 'registration_reference_selection_invalid'],
    [() => unsafe, 'invalid_json'],
    [() => ({ ...selected, references: { providers: [{ ...selected.references.providers[0], digest: '8'.repeat(64) }] } }), 'registration_reference_not_approved'],
  ]) {
    const service = f.open({ approvedReferences: reviews.approvedReferences(), selectApprovedReferences: selector });
    throws(() => f.call(service), expected); service.close();
  }
  assert.equal(getterRan, false);
  const db = new DatabaseSync(f.databasePath, { readOnly: true });
  try { for (const table of ['registration_authorities', 'registration_reference_history', 'registration_heads', 'registration_receipts'])
    assert.equal(db.prepare(`SELECT count(*) AS n FROM ${table}`).get().n, 0); } finally { db.close(); }
});

test('source and selector values cannot outlive or mutate their fence, and final actor revocation rolls back all metadata', t => {
  const f = fixture(t), config = reviewConfiguration(f, [appId]); let sourceInput, selectedContext, callback;
  const reviews = scopedService(t, f, config).reviews;
  f.setBoundary((_request, action) => { sourceInput = source(); callback = action; return action(sourceInput); });
  const service = f.open({ approvedReferences: reviews.approvedReferences(), selectApprovedReferences(context) {
    selectedContext = context; sourceInput.target.revision = 7; sourceInput.target.digest = '7'.repeat(64);
    return reviews.approvedReferencesFor(context.scope);
  } });
  const saved = f.call(service);
  assert.equal(saved.registration.descriptor.app.source.revision, 1);
  assert(Object.isFrozen(selectedContext.scope) && Object.isFrozen(selectedContext.source) && Object.isFrozen(selectedContext.actor));
  assert.throws(() => { selectedContext.source.revision = 9; }, TypeError);
  throws(() => callback(source()), 'registration_authority_fence_invalid'); service.close();
  const next = f.open({ approvedReferences: reviews.approvedReferences(), selectApprovedReferences({ scope }) {
    f.active.delete(actor.accountId); return reviews.approvedReferencesFor(scope);
  } });
  throws(() => f.call(next, 'admit', f.args({ expectedRevision: 1, requestId: 'revoked-in-selector' })), 'registration_authentication_required');
  f.active.add(actor.accountId);
  const reopened = f.open({ approvedReferences: reviews.approvedReferences(), selectApprovedReferences: ({ scope }) => reviews.approvedReferencesFor(scope) });
  assert.equal(f.call(reopened, 'history', f.readArgs).items.length, 1);
});

test('a provider flipping the actor during real receipt confirmation cannot commit a ready head', t => {
  const f = fixture(t), inbox = feedbackPort(); let flip = true;
  const service = f.open({ feedback: { inspectInstallation: request => inbox.port.inspectInstallation(request), ensureInstallation(request) {
    const receipt = inbox.port.ensureInstallation(request); if (flip) { flip = false; f.active.delete(actor.accountId); } return receipt;
  } } });
  f.call(service);
  throws(() => service.reconcileFeedback({ actor, appId, expectedRevision: 1 }), 'registration_authentication_required');
  f.active.add(actor.accountId);
  assert.equal(f.call(service, 'get', f.readArgs).registration.state, 'pending-feedback');
  assert.equal(service.reconcileFeedback({ actor, appId, expectedRevision: 1 }).registration.state, 'ready');
  assert.equal(inbox.effects, 1);
});

test('operational SQLite quota holds new writes, retains accepted receipt/history, and allows larger existing stores to reopen readably', t => {
  const f = fixture(t), service = f.open({ maxDatabaseBytes: 256 * 1024 });
  let lastArgs = f.args(), saved = f.call(service), rejected;
  for (let i = 0; i < 255; i++) {
    const next = f.args({ requestId: 'quota-' + i, expectedRevision: saved.registration.revision,
      proposal: { kind: 'author-draft', draft: { title: 'x'.repeat(160) } } });
    try { saved = f.call(service, 'admit', next); lastArgs = next; }
    catch (error) { rejected = error; break; }
  }
  assert.equal(rejected?.code, 'registration_storage_full'); assert.equal(rejected.status, 503);
  assert.deepEqual(f.call(service, 'admit', lastArgs).receipt, saved.receipt); service.close();
  const limited = f.open({ maxDatabaseBytes: 64 * 1024 });
  assert.equal(f.call(limited, 'get', f.readArgs).registration.revision, saved.registration.revision);
  assert.deepEqual(f.call(limited, 'admit', lastArgs).receipt, saved.receipt);
  assert(f.call(limited, 'history', f.readArgs).items.length > 1);
  throws(() => f.call(limited, 'admit', f.args({ requestId: 'after-quota', expectedRevision: saved.registration.revision })), 'registration_storage_full');
  for (const budget of [65535, 16 * 1024 * 1024 + 1, NaN]) throws(() => f.open({ maxDatabaseBytes: budget }), 'registration_database_quota_invalid');
});

const workerCode = `
import { createUniversalRegistrationService } from ${JSON.stringify(new URL('../server/registration.mjs', import.meta.url).href)};
const c = JSON.parse(process.argv[1]);
const service = createUniversalRegistrationService({...c.config,actorActive:()=>true,withReviewedAppAuthority:(_input,fn)=>fn(c.source)});
process.send({ready:true});
process.once('message',()=>{
 try { const value=service.execute({op:'apps.universal.admit',actor:c.actor,args:c.args}); process.send({ok:true,revision:value.registration.revision}); }
 catch(error){process.send({ok:false,code:error.code});}
 finally{service.close();process.disconnect();}
});`;
function worker(config) {
  const child = spawn(process.execPath, ['--input-type=module', '-e', workerCode, JSON.stringify(config)], { stdio: ['ignore', 'pipe', 'pipe', 'ipc'] });
  let stderr = ''; child.stderr.on('data', data => { stderr += data; });
  const messages = [], waiters = [];
  child.on('message', message => { if (waiters.length) waiters.shift()(message); else messages.push(message); });
  const next = () => messages.length ? Promise.resolve(messages.shift()) : new Promise((resolve, reject) => {
    waiters.push(resolve); child.once('error', reject); child.once('exit', code => { if (code) reject(new Error('worker failed ' + code + ':' + stderr)); });
  });
  return { child, next };
}
test('two independent SQLite writers cannot both accept different intents at the same head revision', async t => {
  const f = fixture(t), initialized = f.open(); initialized.close();
  const config = { databasePath: f.databasePath, registryId: f.registryId, environmentId: f.environmentId, reviewedProfile: profile() };
  const intents = [f.args({ requestId: 'concurrent-a' }), f.args({ requestId: 'concurrent-b' })];
  const a = worker({ config, source: source(), actor, args: intents[0] });
  const b = worker({ config, source: source(), actor, args: intents[1] });
  t.after(() => { if (!a.child.killed) a.child.kill(); if (!b.child.killed) b.child.kill(); });
  await Promise.all([a.next(), b.next()]); a.child.send({ go: true }); b.child.send({ go: true });
  const results = await Promise.all([a.next(), b.next()]);
  assert.equal(results.filter(result => result.ok).length, 1);
  const losingIndex = results.findIndex(result => !result.ok);
  // Under CPU/IO pressure the bounded100ms SQLite wait may expire before the
  // winning COMMIT. Both refusals are valid; neither may create a second effect.
  assert.ok(['registration_revision_conflict', 'registration_busy'].includes(results[losingIndex].code));
  const service = f.open(); assert.equal(f.call(service, 'history', f.readArgs).items.length, 1);
  // A later exact retry now sees the committed head, never a second admission.
  throws(() => f.call(service, 'admit', intents[losingIndex]), 'registration_revision_conflict');
  assert.equal(f.call(service, 'history', f.readArgs).items.length, 1);
});

test('a held independent SQLite writer refuses without a receipt and permits the same intent after release', t => {
  const f = fixture(t), service = f.open(), other = new DatabaseSync(f.databasePath);
  const intent = f.args({ requestId: 'bounded-lock' });
  try {
    other.exec('BEGIN IMMEDIATE');
    try { throws(() => f.call(service, 'admit', intent), 'registration_busy'); }
    finally { other.exec('ROLLBACK'); }
  } finally { other.close(); }
  assert.equal(f.call(service, 'history', f.readArgs).items.length, 0);
  const accepted = f.call(service, 'admit', intent);
  assert.equal(accepted.registration.revision, 1);
  assert.equal(f.call(service, 'history', f.readArgs).items.length, 1);
  assert.deepEqual(f.call(service, 'admit', intent).receipt, accepted.receipt);
});
