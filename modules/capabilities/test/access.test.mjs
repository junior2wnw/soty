import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createCapabilitiesService, BUILTIN_CAPABILITIES } from '../server/index.mjs';
import { fixtureDocumentation } from './support/documentation.mjs';

const AUDIENCE = 'https://soty.test/api/capabilities/v1';
const OWNER_A = Object.freeze({ accountId: 'account_a', deviceId: 'device_a' });
const OWNER_B = Object.freeze({ accountId: 'account_b', deviceId: 'device_b' });
const CAP = {
  capabilityId: 'test.create', version: 1, appId: 'test', title: 'Create', description: 'Test contract', visibility: 'public', executionEnabled: true,
  inputSchema: { type: 'object', properties: { text: { type: 'string', maxLength: 100 } }, required: ['text'], additionalProperties: false },
  outputSchema: { type: 'object', properties: {}, additionalProperties: false },
  resources: ['test:new'], effects: ['create'], recipients: ['soty:test'], executionBinding: { kind: 'native', handler: 'test.create', version: 1 }
};

function fixture(t, options = {}) {
  const directory = mkdtempSync(path.join(tmpdir(), 'soty-cap-access-'));
  const databasePath = path.join(directory, 'capabilities.sqlite');
  let time = 1_800_000_000_000;
  const revoked = new Set();
  const actorActive = actor => (actor.accountId === OWNER_A.accountId && actor.deviceId === OWNER_A.deviceId || actor.accountId === OWNER_B.accountId && actor.deviceId === OWNER_B.deviceId) && !revoked.has(actor.deviceId);
  const create = overrides => {
    const config = { databasePath, projectId: 'capabilities-access-test', clock: () => time, actorActive, catalog: [CAP], ...options, ...overrides };
    return createCapabilitiesService({ ...config, documentation: Object.hasOwn(config, 'documentation') ? config.documentation : fixtureDocumentation(config.catalog) });
  };
  let service = create();
  t.after(() => {
    service.close();
    assert.equal(path.dirname(path.resolve(directory)), path.resolve(tmpdir()));
    assert.ok(path.basename(directory).startsWith('soty-cap-access-'));
    rmSync(directory, { recursive: true, force: true });
  });
  const call = (op, args = {}, actor = OWNER_A) => service.execute({ op: `access.${op}`, args: { expectedAccountId: actor.accountId, ...args }, actor });
  function principal(actor = OWNER_A) { return call('principals.create', { label: 'Test service' }, actor).principal; }
  function grant(principalId, overrides = {}, actor = OWNER_A) {
    return call('grants.issue', { principalId, capabilities: [{ capabilityId: CAP.capabilityId, version: 1 }], resources: CAP.resources, effects: CAP.effects,
      recipients: CAP.recipients, expiresAt: time + 10000, allowDelegation: false, maxDepth: 0, budget: { unit: 'invocations', limit: 4 }, ...overrides }, actor).grant;
  }
  function credential(grantId, overrides = {}, actor = OWNER_A) { return call('credentials.issue', { grantId, audience: AUDIENCE, ...overrides }, actor); }
  return { get service() { return service; }, directory, databasePath, now: () => time, advance: ms => { time += ms; }, revokeDevice: id => revoked.add(id), call, principal, grant, credential,
    reopen(overrides = {}) { service.close(); service = create(overrides); return service; } };
}
const hasCode = code => error => error.code === code && error.message === code;
function authorized(f, grant) {
  const issued = f.credential(grant.id);
  return { issued, actor: f.service.authenticateCredential({ token: issued.token, audience: AUDIENCE }) };
}

test('server-created actor is opaque, frozen, account-bound and rechecked after credential revoke', t => {
  const f = fixture(t); const principal = f.principal(); const grant = f.grant(principal.id);
  const { actor, issued } = authorized(f, grant);
  assert.equal(Object.isFrozen(actor), true);
  assert.throws(() => f.service.authorize({ actor: { ...actor }, capabilityId: CAP.capabilityId, version: 1 }), hasCode('authorization_required'));
  assert.throws(() => f.service.authorize({ actor: OWNER_A, capabilityId: CAP.capabilityId, version: 1 }), hasCode('authorization_required'));
  assert.equal(f.service.authorize({ actor, capabilityId: CAP.capabilityId, version: 1 }).accountId, OWNER_A.accountId);
  assert.throws(() => f.call('grants.issue', { expectedAccountId: OWNER_A.accountId }, OWNER_B), hasCode('account_mismatch'));
  assert.throws(() => f.call('credentials.issue', { grantId: grant.id, audience: AUDIENCE }, OWNER_B), hasCode('access_denied'));
  f.call('credentials.revoke', { credentialId: issued.credential.id });
  assert.throws(() => f.service.authorize({ actor, capabilityId: CAP.capabilityId, version: 1 }), hasCode('authorization_required'));
});

test('service token contains entropy but only its digest is persisted; errors and audit do not echo it', t => {
  const f = fixture(t); const grant = f.grant(f.principal().id); const { issued } = authorized(f, grant);
  const db = new DatabaseSync(f.databasePath, { readOnly: true });
  try {
    const row = db.prepare('SELECT * FROM cap_credentials WHERE id=?').get(issued.credential.id);
    assert.match(row.digest, /^[a-f0-9]{64}$/u);
    assert.equal(JSON.stringify(row).includes(issued.token), false);
  } finally { db.close(); }
  const events = f.call('events.list').events;
  assert.equal(events.length, 3);
  assert.equal(JSON.stringify(events).includes(issued.token), false);
  assert.deepEqual(f.call('events.list', {}, OWNER_B).events, []);
  assert.throws(() => f.service.authenticateCredential({ token: `${issued.token}x`, audience: AUDIENCE }), hasCode('authorization_required'));
});

test('audience is a canonical resource identifier and never a URL with credentials or a remote cleartext endpoint', t => {
  const f = fixture(t); const grant = f.grant(f.principal().id);
  for (const invalid of ['https://', 'https://user:password@soty.test', 'https://soty.test/?secret=x', 'https://soty.test/#a', 'http://example.com', 'https://SOTY.test', 'https://soty.test/a/../b', 'soty:', 'https://soty.test\\evil']) {
    assert.throws(() => f.credential(grant.id, { audience: invalid }), hasCode('audience_invalid'));
  }
  for (const valid of [AUDIENCE, 'https://soty.test', 'https://soty.test/', 'http://127.0.0.1:8080/mcp', 'http://localhost:3000', 'http://[::1]:8000/mcp', 'urn:soty:capabilities', 'soty:capabilities']) {
    const result = f.credential(grant.id, { audience: valid });
    assert.ok(f.service.authenticateCredential({ token: result.token, audience: valid }));
    assert.throws(() => f.service.authenticateCredential({ token: result.token, audience: `${valid}/other` }), hasCode('authorization_required'));
  }
});

test('expiry boundary, principal revocation and issuer-device revocation invalidate retained actors', t => {
  for (const revoke of ['expiry', 'principal', 'device']) {
    const f = fixture(t); const principal = f.principal(); const grant = f.grant(principal.id); const { actor } = authorized(f, grant);
    if (revoke === 'expiry') f.advance(10000);
    if (revoke === 'principal') f.call('principals.revoke', { principalId: principal.id });
    if (revoke === 'device') f.revokeDevice(OWNER_A.deviceId);
    assert.throws(() => f.service.authorize({ actor, capabilityId: CAP.capabilityId, version: 1 }));
  }
});

test('child grants attenuate version, resources, effects, recipients, time and delegation depth', t => {
  const f = fixture(t); const principal = f.principal();
  const root = f.grant(principal.id, { allowDelegation: true, maxDepth: 2 });
  const base = { parentGrantId: root.id, principalId: principal.id, capabilities: [{ capabilityId: CAP.capabilityId, version: 1 }],
    resources: CAP.resources, effects: CAP.effects, recipients: CAP.recipients, expiresAt: f.now() + 9000, allowDelegation: false, maxDepth: 0 };
  for (const extra of [{ resources: ['test:new', 'test:old'] }, { effects: ['create', 'delete'] }, { recipients: ['soty:test', 'external:any'] },
    { expiresAt: f.now() + 11000 }, { allowDelegation: true, maxDepth: 2 }]) {
    assert.throws(() => f.call('grants.derive', { ...base, ...extra }), hasCode('delegation_denied'));
  }
  const child = f.call('grants.derive', base).grant;
  assert.equal(child.rootGrantId, root.id); assert.equal(child.depth, 1); assert.equal(child.budget.limit, root.budget.limit);
  const { actor } = authorized(f, child);
  f.call('grants.revoke', { grantId: root.id });
  assert.throws(() => f.service.authorize({ actor, capabilityId: CAP.capabilityId, version: 1 }), hasCode('access_denied'));
});

test('unsupported scopes, malformed DTOs and inherited properties cannot silently widen an action', t => {
  const f = fixture(t); const principal = f.principal(); const grant = f.grant(principal.id); const { actor } = authorized(f, grant);
  assert.throws(() => f.service.authorize({ actor, capabilityId: 'notes.purge', version: 1 }), hasCode('access_denied'));
  assert.throws(() => f.service.authorize({ actor, capabilityId: CAP.capabilityId, version: 1, effects: ['delete'] }), hasCode('access_denied'));
  assert.throws(() => f.service.authorize({ actor, capabilityId: CAP.capabilityId, version: 1, input: { text: 'ok', command: 'dangerous' } }), hasCode('invalid_input'));
  assert.throws(() => f.call('principals.create', { label: 'Bad', accountId: OWNER_B.accountId }), hasCode('invalid_input'));
  assert.throws(() => f.service.execute({ op: 'access.principals.list', actor: { accountId: OWNER_A.accountId, deviceId: OWNER_B.deviceId }, args: { expectedAccountId: OWNER_A.accountId } }), hasCode('authorization_required'));
});

test('catalogue hides private metadata and does not equate discoverability with executable Notes', t => {
  const privateCap = { ...CAP, capabilityId: 'private.secret', visibility: 'private', title: 'Hidden secret' };
  const f = fixture(t, { catalog: [...BUILTIN_CAPABILITIES, privateCap] });
  assert.equal(f.service.catalog.search({ query: 'secret' }).total, 0);
  assert.throws(() => f.service.catalog.get({ capabilityId: privateCap.capabilityId, version: 1 }), hasCode('not_found'));
  const note = f.service.catalog.get({ capabilityId: 'notes.createDraft', version: 1 }).capability;
  assert.equal(note.executionEnabled, false);
  const principal = f.principal();
  const grant = f.grant(principal.id, { capabilities: [{ capabilityId: 'notes.createDraft', version: 1 }], resources: ['notes:new'], effects: ['create'], recipients: ['soty:notes'] });
  const { actor } = authorized(f, grant);
  assert.throws(() => f.service.authorize({ actor, capabilityId: 'notes.createDraft', version: 1 }), hasCode('capability_disabled'));
});

test('capability versions are durably pinned while an operational enable flag can change', t => {
  const f = fixture(t); const digest = f.service.catalog.get({ capabilityId: CAP.capabilityId, version: 1 }).capability.digest;
  f.reopen({ catalog: [{ ...CAP, executionEnabled: false }] });
  assert.equal(f.service.catalog.get({ capabilityId: CAP.capabilityId, version: 1 }).capability.digest, digest);
  assert.throws(() => f.reopen({ catalog: [{ ...CAP, effects: ['delete'] }] }), hasCode('capability_version_conflict'));
  // Reopen the original schema after the rejected update to prove no pin was changed.
  f.reopen();
  assert.equal(f.service.catalog.get({ capabilityId: CAP.capabilityId, version: 1 }).capability.digest, digest);
});

test('bounded owner lists keep stable account/filter-bound pagination and content-free audit', t => {
  const f = fixture(t); const a = f.principal(); const b = f.principal(); f.principal(OWNER_B);
  f.grant(a.id); f.grant(a.id); f.grant(b.id);
  const first = f.call('principals.list', { limit: 1 }); const second = f.call('principals.list', { limit: 1, cursor: first.cursor });
  assert.equal(first.principals.length, 1); assert.equal(second.principals.length, 1); assert.notEqual(first.principals[0].id, second.principals[0].id);
  assert.throws(() => f.call('principals.list', { limit: 1, cursor: first.cursor }, OWNER_B), hasCode('cursor_invalid'));
  const grants = f.call('grants.list', { principalId: a.id, limit: 1 });
  assert.equal(grants.grants.length, 1); assert.ok(grants.cursor);
  assert.throws(() => f.call('grants.list', { principalId: b.id, limit: 1, cursor: grants.cursor }), hasCode('cursor_invalid'));
  const events = f.call('events.list', { limit: 2 }); assert.equal(events.events.length, 2); assert.ok(events.cursor);
});

test('admission shares a finite budget, rollback leaves no extra Invocation, pending cancel releases held capacity', t => {
  const f = fixture(t); const principal = f.principal(); const grant = f.grant(principal.id, { budget: { unit: 'invocations', limit: 1 } }); const { actor } = authorized(f, grant);
  const invoke = key => f.service.invocations.admit({ actor, capabilityId: CAP.capabilityId, version: 1, idempotencyKey: key, input: { text: 'private body' } });
  const first = invoke('request_key_one'); assert.equal(first.reused, false);
  assert.equal(invoke('request_key_one').reused, true);
  assert.throws(() => invoke('request_key_two'), hasCode('budget_exceeded'));
  let budget = f.call('grants.list').grants[0].budget;
  assert.equal(budget.remaining, 0); assert.equal(budget.reserved, 1); assert.equal(budget.spent, 0);
  f.service.invocations.requestCancel({ actor, invocationId: first.invocation.invocationId });
  budget = f.call('grants.list').grants[0].budget;
  assert.equal(budget.remaining, 1); assert.equal(budget.reserved, 0);
  assert.equal(invoke('request_key_two').reused, false);
});

test('constructor bounds each submodule before opening a database; unknown schemas are rejected', t => {
  const f = fixture(t);
  assert.throws(() => f.reopen({ limits: { unsupported: true } }), hasCode('limits_invalid'));
  f.reopen({ limits: { access: { maxGrantTtlMs: 1000 }, invocations: { pageSize: 10 } } });
  assert.throws(() => f.grant(f.principal().id), hasCode('expiry_invalid'));
  assert.throws(() => f.reopen({ catalog: [{ ...CAP, inputSchema: { ...CAP.inputSchema, $ref: 'https://evil.invalid/schema' } }] }), hasCode('schema_unsupported'));
  f.reopen();
});
