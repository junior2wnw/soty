import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createCapabilitiesService } from '../server/index.mjs';
import { createOAuthBaselineGuard } from '../server/oauth-baseline.mjs';
import { tokensFixture } from './support/oauth-tokens.mjs';
import { config } from './support/oauth-artifacts.mjs';

const H = 'https://delegation.test', PROJECT = 'delegation-domain-test';
const OWNER = Object.freeze({ accountId: 'account_owner', deviceId: 'device_owner' });
const SECOND = Object.freeze({ accountId: OWNER.accountId, deviceId: 'device_second' });
const OTHER = Object.freeze({ accountId: 'account_other', deviceId: 'device_other' });
const code = expected => error => error?.code === expected;
const scope = { capabilities: [{ capabilityId: 'notes.createDraft', version: 1 }],
  resources: ['notes:new'], effects: ['create'], recipients: ['soty:notes'] };

/** Model tests deliberately use a synchronous test fence. The companion
 * delegation-native suite proves actual Connect/SQLite authority and effects. */
function fixture(t, initial = {}) {
  const parent = realpathSync(tmpdir()), directory = mkdtempSync(path.join(parent, 'soty-delegation-'));
  const file = path.join(directory, 'caps.sqlite');
  let time = 1000, options = initial, service;
  const revoked = new Set();
  const actorActive = actor => [OWNER, SECOND, OTHER].some(value => actor.accountId === value.accountId
    && actor.deviceId === value.deviceId && !revoked.has(value.deviceId));
  function open() {
    service = createCapabilitiesService({ databasePath: file, projectId: PROJECT, clock: () => time, actorActive,
      ...options, delegation: Object.hasOwn(options, 'delegation') ? options.delegation
        : { audience: H, withAuthorityFence: options.fence ?? (action => action()) } });
  }
  open();
  t.after(() => {
    service.close(); const actual = realpathSync(directory);
    assert.equal(path.dirname(actual), parent); assert.ok(path.basename(actual).startsWith('soty-delegation-'));
    rmSync(actual, { recursive: true });
  });
  const call = (op, args = {}, owner = OWNER) => service.execute({ op: `access.${op}`, actor: owner,
    args: { expectedAccountId: owner.accountId, ...args } });
  const actor = token => service.authenticateCredential({ token, audience: H });
  const sql = action => { const db = new DatabaseSync(file); try { return action(db); } finally { db.close(); } };
  function issue(overrides = {}, owner = OWNER) {
    const principal = call('principals.create', { label: 'Parent' }, owner).principal;
    const grant = call('grants.issue', { principalId: principal.id, ...scope, expiresAt: 20000,
      allowDelegation: true, maxDepth: 1, budget: { unit: 'invocations', limit: 5 }, ...overrides }, owner).grant;
    const issued = call('credentials.issue', { grantId: grant.id, audience: H }, owner);
    return { principal, grant, ...issued, actor: actor(issued.token) };
  }
  return { get service() { return service; }, file, call, issue, actor, sql,
    time(value) { if (value !== undefined) time = value; return time; }, revokeDevice(id) { revoked.add(id); },
    counts() { return sql(db => Object.fromEntries(['clients', 'principals', 'grants', 'credentials', 'audit', 'budgets', 'budget_reservations']
      .map(name => [name, db.prepare(`SELECT count(*) AS n FROM cap_${name}`).get().n]))); },
    derive(actor, extra = {}) { return service.delegation.derive({ actor, label: 'Child 😀', expiresAt: 10000, ...extra }); },
    reopen(next = {}) { service.close(); options = { ...options, ...next }; open(); }
  };
}

test('child is one atomic distinct identity, fixed leaf and shared root; only token digest persists', t => {
  const f = fixture(t), parent = f.issue(), before = f.counts(), child = f.derive(parent.actor);
  assert.equal(Object.isFrozen(f.service.delegation), true);
  assert.equal(Object.isFrozen(child) && Object.isFrozen(child.grant), true);
  assert.equal(f.service.operations.has('service.delegation.derive'), false);
  assert.notEqual(child.principal.id, parent.principal.id); assert.notEqual(child.principal.clientId, parent.principal.clientId);
  assert.equal(child.grant.parentGrantId, parent.grant.id); assert.equal(child.grant.rootGrantId, parent.grant.id);
  assert.deepEqual({ capabilities: child.grant.capabilities, resources: child.grant.resources,
    effects: child.grant.effects, recipients: child.grant.recipients }, scope);
  assert.equal(child.grant.allowDelegation, false); assert.equal(child.grant.maxDepth, 0); assert.equal(child.grant.depth, 1);
  assert.deepEqual(child.grant.budget, parent.grant.budget);
  assert.deepEqual(f.counts(), { ...before, clients: before.clients + 1, principals: before.principals + 1,
    grants: before.grants + 1, credentials: before.credentials + 1, audit: before.audit + 1 });
  assert.equal(typeof child.token === 'string' && /^soty_cap_[A-Za-z0-9_-]{43}$/u.test(child.token), true);
  assert.deepEqual(Object.keys(child.credential).sort(), ['audience', 'createdAt', 'expiresAt', 'grantId', 'id']);
  assert.equal(child.credential.audience, H);
  const persisted = f.sql(db => JSON.stringify(['cap_credentials', 'cap_audit'].map(table => db.prepare(`SELECT * FROM ${table}`).all())));
  assert.equal(persisted.includes(child.token), false);
  const events = f.call('events.list').events.filter(event => event.actorType === 'service');
  assert.equal(events.length, 1); assert.equal(events[0].kind, 'access.grants.derive');
  assert.equal(events[0].actorId, parent.principal.id); assert.equal(events[0].objectId, child.grant.id);
  assert.throws(() => f.derive(f.actor(child.token)), code('delegation_denied'));
});

test('private actor must be authentic, same service and ordinary exact resource', t => {
  const f = fixture(t), other = fixture(t), parent = f.issue(), foreign = other.issue();
  const before = f.counts();
  for (const actor of [undefined, null, OWNER, { ...parent.actor }, foreign.actor]) {
    assert.throws(() => f.derive(actor), code('authorization_required'));
  }
  const wrong = f.call('credentials.issue', { grantId: parent.grant.id, audience: `${H}/mcp` });
  const actor = f.service.authenticateCredential({ token: wrong.token, audience: `${H}/mcp` });
  assert.throws(() => f.derive(actor), code('access_denied'));
  assert.deepEqual(f.counts(), { ...before, credentials: before.credentials + 1, audit: before.audit + 1 });
});

test('exact own scalar request rejects widening, getters, symbols, missing fields and ill-formed UTF-16', t => {
  const f = fixture(t), parent = f.issue(), before = f.counts();
  for (const extra of ['accountId', 'principalId', 'parentGrantId', 'rootGrantId', 'audience', 'budget', 'scope', 'capabilities', 'effects', 'recipients', 'requestId']) {
    assert.throws(() => f.derive(parent.actor, { [extra]: 'injected' }), code('invalid_input'));
  }
  for (const label of ['', 'x'.repeat(101), ['valid'], 1, '\u0000']) assert.throws(() => f.derive(parent.actor, { label }), code('invalid_input'));
  for (const label of ['\ud800', 'a\udfff']) assert.throws(() => f.derive(parent.actor, { label }), code('invalid_unicode'));
  let accessed = false;
  const request = { actor: parent.actor, expiresAt: 10000, get label() { accessed = true; return 'never'; } };
  assert.throws(() => f.service.delegation.derive(request), code('invalid_input')); assert.equal(accessed, false);
  assert.throws(() => f.service.delegation.derive({ actor: parent.actor, label: 'child' }), code('invalid_input'));
  assert.throws(() => f.derive(parent.actor, { [Symbol('extra')]: true }), code('invalid_input'));
  assert.deepEqual(f.counts(), before);
  const child = f.derive(parent.actor, { label: '😀'.repeat(50) });
  assert.equal(child.principal.label.length, 100); assert.equal(child.principal.label.isWellFormed(), true);
});

test('expiry respects the credential, every ancestor, live actor and configured TTL without rounding', t => {
  const f = fixture(t), root = f.issue({ maxDepth: 2 });
  const mid = f.call('grants.derive', { ...scope, parentGrantId: root.grant.id, principalId: root.principal.id,
    expiresAt: 12000, allowDelegation: true, maxDepth: 1 }, SECOND).grant;
  const issued = f.call('credentials.issue', { grantId: mid.id, audience: H, expiresAt: 9000 });
  const actor = f.actor(issued.token), before = f.counts();
  for (const expiresAt of [1000, 999, 9001, 12001, Number.MAX_SAFE_INTEGER, 1001.5, '5000', null]) {
    assert.throws(() => f.derive(actor, { expiresAt }), code('expiry_invalid'));
  }
  assert.deepEqual(f.counts(), before);
  const child = f.derive(actor, { expiresAt: 9000 });
  const inherited = f.sql(db => [db.prepare('SELECT creator_device_id FROM cap_principals WHERE id=?').get(child.principal.id),
    db.prepare('SELECT creator_device_id FROM cap_grants WHERE id=?').get(child.grant.id)]);
  assert.deepEqual(inherited.map(row => row.creator_device_id), [SECOND.deviceId, SECOND.deviceId]);
  f.reopen({ limits: { access: { maxGrantTtlMs: 1000 } } });
  assert.throws(() => f.derive(f.actor(issued.token), { expiresAt: 2001 }), code('expiry_invalid'));
  const retained = f.actor(issued.token); f.time(9000);
  assert.throws(() => f.derive(retained), code('authorization_required'));
});

test('revoking only parent credential leaves issued child; parent grant/principal/creator revocation closes its chain', t => {
  for (const kind of ['credential', 'grant', 'principal', 'creator']) {
    const f = fixture(t), parent = f.issue(), child = f.derive(parent.actor), actor = f.actor(child.token);
    if (kind === 'credential') f.call('credentials.revoke', { credentialId: parent.credential.id });
    else if (kind === 'grant') f.call('grants.revoke', { grantId: parent.grant.id });
    else if (kind === 'principal') f.call('principals.revoke', { principalId: parent.principal.id });
    else f.revokeDevice(OWNER.deviceId);
    assert.throws(() => f.derive(parent.actor));
    const read = () => f.service.authorize({ actor, capabilityId: 'notes.createDraft', version: 1, action: 'read' });
    if (kind === 'credential') assert.equal(read().grantId, child.grant.id);
    else assert.throws(read, code('access_denied'));
  }
});

test('fixed Notes scope and non-delegable parent are checked again from current rows', t => {
  for (const change of ['nondelegable', 'scope']) {
    const f = fixture(t), parent = f.issue(change === 'nondelegable' ? { allowDelegation: false, maxDepth: 0 } : {});
    if (change === 'scope') f.sql(db => db.prepare("UPDATE cap_grants SET recipients_json='[\"soty:other\"]' WHERE id=?").run(parent.grant.id));
    const before = f.counts(); assert.throws(() => f.derive(parent.actor), code('delegation_denied')); assert.deepEqual(f.counts(), before);
  }
});

test('all account quotas are checked before the first INSERT and audit failure rolls back the entire issue', t => {
  for (const quota of ['principals', 'grants', 'credentials']) {
    const f = fixture(t), parent = f.issue();
    f.sql(db => {
      if (quota === 'principals') db.exec(`WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<999)
        INSERT INTO cap_principals(id,account_id,client_id,kind,label,state,creator_device_id,created_at)
        SELECT 'quota_p_'||x,account_id,client_id,kind,label,state,creator_device_id,created_at FROM n,cap_principals WHERE id='${parent.principal.id}'`);
      if (quota === 'grants') db.exec(`WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<9999)
        INSERT INTO cap_grants(id,account_id,client_id,principal_id,parent_id,root_id,creator_device_id,capabilities_json,resources_json,effects_json,recipients_json,allow_delegation,max_depth,depth,not_before,expires_at,created_at)
        SELECT 'quota_g_'||x,account_id,client_id,principal_id,id,id,creator_device_id,capabilities_json,resources_json,effects_json,recipients_json,0,0,1,not_before,expires_at,created_at FROM n,cap_grants WHERE id='${parent.grant.id}'`);
      if (quota === 'credentials') db.exec(`WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<9999)
        INSERT INTO cap_credentials(id,digest,account_id,client_id,principal_id,grant_id,audience,expires_at,created_at)
        SELECT 'quota_k_'||x,printf('%064x',x),account_id,client_id,principal_id,grant_id,audience,expires_at,created_at FROM n,cap_credentials WHERE id='${parent.credential.id}'`);
      db.exec("CREATE TRIGGER assert_preflight BEFORE INSERT ON cap_clients BEGIN SELECT RAISE(ABORT,'insert_before_quota'); END;");
    });
    const before = f.counts(); assert.throws(() => f.derive(parent.actor), code('quota_exceeded')); assert.deepEqual(f.counts(), before);
  }
  const f = fixture(t), parent = f.issue(), before = f.counts();
  f.sql(db => db.exec("CREATE TRIGGER audit_fault BEFORE INSERT ON cap_audit WHEN NEW.actor_type='service' BEGIN SELECT RAISE(ABORT,'synthetic_audit_fault'); END;"));
  assert.throws(() => f.derive(parent.actor), /synthetic_audit_fault/u); assert.deepEqual(f.counts(), before);
  f.sql(db => db.exec('DROP TRIGGER audit_fault'));
  assert.equal(f.derive(parent.actor).grant.parentGrantId, parent.grant.id);
});

test('configuration is snapshotted before opening storage and missing fence closes only delegation', t => {
  const f = fixture(t, { delegation: undefined }), parent = f.issue();
  assert.throws(() => f.derive(parent.actor), code('delegation_unavailable'));
  for (const delegation of [null, {}, { audience: H, withAuthorityFence: async action => action() },
    { audience: `${H}/mcp`, withAuthorityFence: action => action() }, { audience: 'http://remote.test', withAuthorityFence: action => action() }]) {
    assert.throws(() => createCapabilitiesService({ databasePath: ':memory:', projectId: PROJECT, actorActive: () => true, delegation }), code('delegation_configuration_invalid'));
  }
  const composition = { audience: H, withAuthorityFence: action => action() };
  const second = fixture(t, { delegation: composition }), identity = second.issue();
  composition.audience = 'https://other.test'; composition.withAuthorityFence = () => { throw new Error('mutated'); };
  assert.equal(second.derive(identity.actor).credential.audience, H);
});

test('callback discipline rejects absent/thenable/late/reentrant work and captures request before the fence', async t => {
  for (const promise of [false, true]) {
    let later;
    const f = fixture(t, { fence(action) { later = action; return promise ? Promise.resolve() : undefined; } }), parent = f.issue();
    const before = f.counts(); assert.throws(() => f.derive(parent.actor), code('delegation_context_invalid'));
    await Promise.resolve(); assert.throws(() => later(), code('delegation_context_invalid')); assert.deepEqual(f.counts(), before);
    f.service.close(); assert.throws(() => later(), code('delegation_context_invalid'));
  }
  let call;
  const recursive = fixture(t, { fence(action) { call(); return action(); } }), parent = recursive.issue();
  call = () => recursive.derive(parent.actor); const before = recursive.counts();
  assert.throws(call, code('delegation_context_invalid')); assert.deepEqual(recursive.counts(), before);
  let request;
  const captured = fixture(t, { fence(action) { request.label = 'changed'; request.expiresAt = 19999; return action(); } });
  request = { actor: captured.issue().actor, label: 'captured', expiresAt: 9000 };
  const child = captured.service.delegation.derive(request);
  assert.equal(child.principal.label, 'captured'); assert.equal(child.grant.expiresAt, 9000);
});

test('committed issue plus lost fence reply remains one unknown child, visible after reopen and revocable without its secret', t => {
  for (const mode of ['lost', 'twice', 'substitute']) {
    const f = fixture(t, { fence(action) {
      action();
      if (mode === 'twice') return action();
      if (mode === 'substitute') return {};
      throw new Error('synthetic_lost_reply');
    } }), parent = f.issue(), before = f.counts();
    assert.throws(() => f.derive(parent.actor));
    assert.equal(f.counts().credentials, before.credentials + 1); assert.equal(f.counts().audit, before.audit + 1);
    f.reopen({ fence: action => action() });
    const child = f.call('grants.list').grants.find(grant => grant.parentGrantId === parent.grant.id);
    assert.ok(child); assert.equal(Object.hasOwn(child, 'token'), false);
    assert.equal(f.call('grants.revoke', { grantId: child.id }).grant.revokedAt, f.time());
  }
});

test('credential quota uses account lineage indexes and preserves exact OAuth exclusions', async t => {
  for (const allowNativeMigration of [false, true]) {
    const f = fixture(t, { allowNativeMigration }), parent = f.issue(); f.issue({}, OTHER);
    f.sql(db => {
      db.exec(`WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<4096)
        INSERT INTO cap_credentials(id,digest,account_id,client_id,principal_id,grant_id,audience,expires_at,created_at)
        SELECT 'plan_k_'||x,printf('%064x',x),account_id,client_id,principal_id,grant_id,audience,expires_at,created_at FROM n,cap_credentials WHERE id='${parent.credential.id}'; ANALYZE;`);
      const old = db.prepare('EXPLAIN QUERY PLAN SELECT count(*) AS n FROM cap_credentials WHERE account_id=?').all(OWNER.accountId);
      assert.ok(old.some(row => row.detail === 'SCAN cap_credentials'));
      const observed = [];
      const tracking = { prepare(sql) { observed.push(sql); return db.prepare(sql); } };
      const guard = createOAuthBaselineGuard({ db: tracking });
      assert.equal(guard.legacyCredentialCount(OWNER.accountId), 4097);
      assert.equal(guard.legacyCredentialCount(OTHER.accountId), 1);
      const query = observed.find(sql => sql.includes('CROSS JOIN cap_credentials'));
      const plan = db.prepare('EXPLAIN QUERY PLAN ' + query).all(OWNER.accountId, OWNER.accountId).map(row => row.detail);
      assert.ok(plan.some(value => value.includes('cap_grants_account (account_id=?)')));
      assert.ok(plan.some(value => value.includes('cap_credentials_grant (grant_id=?)')));
      assert.equal(plan.some(value => value.startsWith('SCAN ')), false);
    });
  }
  const oauth = await tokensFixture(t), payload = oauth.payload('AccessToken');
  oauth.put(payload);
  const guard = createOAuthBaselineGuard({ db: oauth.db });
  assert.equal(guard.legacyCredentialCount(oauth.owner.accountId), 0);
  // A legitimate extra ordinary grant stays counted in an OAuth-capable store.
  const p = (await oauth.ownerCall('access.principals.create', { label: 'ordinary' })).principal;
  const g = (await oauth.ownerCall('access.grants.issue', { principalId: p.id, ...scope, expiresAt: oauth.now() + 10000,
    allowDelegation: true, maxDepth: 1, budget: { unit: 'invocations', limit: 1 } })).grant;
  await oauth.ownerCall('access.credentials.issue', { grantId: g.id, audience: H });
  assert.equal(guard.legacyCredentialCount(oauth.owner.accountId), 1);
});

test('same-instance OAuth actor cannot derive while ordinary authority works on AS/keyless schemas', async t => {
  const f = await tokensFixture(t), payload = f.payload('AccessToken'); f.put(payload);
  const service = createCapabilitiesService({ databasePath: f.files.caps, projectId: f.caps.projectId, clock: f.now,
    actorActive: actor => f.connect.isActorActive(actor), oauth: { ...config(), withAuthorityFence: action => f.connect.withAuthorityFence(action) },
    delegation: { audience: new URL(f.primary.resource).origin, withAuthorityFence: action => f.connect.withAuthorityFence(action) } });
  try {
    const actor = service.oauth.authenticateBearer({ token: payload.jti, audience: f.primary.resource });
    const before = f.db.prepare('SELECT count(*) AS n FROM cap_principals').get().n;
    assert.throws(() => service.delegation.derive({ actor, label: 'no child', expiresAt: f.now() + 1000 }), code('delegation_denied'));
    assert.equal(f.db.prepare('SELECT count(*) AS n FROM cap_principals').get().n, before);
  } finally { service.close(); }
  const p = (await f.ownerCall('access.principals.create', { label: 'ordinary for AS-off' })).principal;
  const g = (await f.ownerCall('access.grants.issue', { principalId: p.id, ...scope, expiresAt: f.now() + 10000,
    allowDelegation: true, maxDepth: 1, budget: { unit: 'invocations', limit: 1 } })).grant;
  const credential = await f.ownerCall('access.credentials.issue', { grantId: g.id, audience: H });
  const keyless = config(); delete keyless.artifactKey; delete keyless.artifactKeyId;
  for (const oauth of [undefined, keyless]) {
    const current = createCapabilitiesService({ databasePath: f.files.caps, projectId: f.caps.projectId, clock: f.now,
      actorActive: actor => f.connect.isActorActive(actor), oauth,
      delegation: { audience: H, withAuthorityFence: action => f.connect.withAuthorityFence(action) } });
    try {
      const actor = current.authenticateCredential({ token: credential.token, audience: H });
      assert.equal(current.delegation.derive({ actor, label: 'ordinary child', expiresAt: f.now() + 1000 }).grant.rootGrantId, g.id);
    } finally { current.close(); }
  }
});
