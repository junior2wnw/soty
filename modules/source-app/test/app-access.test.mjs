import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
const { createAppAccess } = await import(process.env.SOTY_APP_ACCESS_MODULE ?? '../server/app-access.mjs');

const APP = 'app-' + 'a'.repeat(32), OTHER_APP = 'app-' + 'b'.repeat(32);
const alice = Object.freeze({ issuer: 'https://soty.test/human-identity', subject: 'alice' });
const bob = Object.freeze({ issuer: 'https://soty.test/human-identity', subject: 'bob' });
const stranger = Object.freeze({ issuer: 'https://soty.test/human-identity', subject: 'platform-card-owner' });
const code = expected => error => error.code === expected;
function fixture(t, filename = ':memory:') {
  const db = new DatabaseSync(filename); db.exec('PRAGMA foreign_keys=ON;PRAGMA busy_timeout=1000');
  t.after(() => db.close());
  db.exec('CREATE TABLE IF NOT EXISTS effects(app_id TEXT,resource_id TEXT,principal_id TEXT,value TEXT)');
  const access = createAppAccess({ database: db, appId: APP, initialize: true });
  return { db, access, effects: () => db.prepare('SELECT * FROM effects').all(),
    effect: context => { assert.equal(context.database.isTransaction, true); context.database.prepare('INSERT INTO effects VALUES(?,?,?,?)')
      .run(context.appId, context.resourceId, context.principalId, 'saved'); return 'saved'; } };
}

test('optional initialization adds only its own schema and preserves existing identities and application metadata', t => {
  const db = new DatabaseSync(':memory:'); t.after(() => db.close());
  db.exec("PRAGMA user_version=37;PRAGMA application_id=11;CREATE TABLE users(id TEXT PRIMARY KEY,email TEXT,password TEXT);INSERT INTO users VALUES('old','a@example.test','old-password')");
  const original = db.prepare('SELECT * FROM users').all();
  assert.throws(() => createAppAccess({ database: db, appId: APP }), code('app_access_not_initialized'));
  assert.equal(db.prepare("SELECT count(*) AS n FROM sqlite_schema WHERE name GLOB 'soty_app_access_*'").get().n, 0);
  createAppAccess({ database: db, appId: APP, initialize: true });
  assert.deepEqual(db.prepare('SELECT * FROM users').all(), original);
  assert.equal(db.prepare('PRAGMA user_version').get().user_version, 37);
  assert.equal(db.prepare('PRAGMA application_id').get().application_id, 11);
  assert.equal(db.prepare('SELECT count(*) AS n FROM soty_app_access_memberships').get().n, 0);
});

test('partial or conflicting optional schema is refused without adopting or changing it', t => {
  const db = new DatabaseSync(':memory:'); t.after(() => db.close());
  db.exec('CREATE TABLE soty_app_access_meta(secret TEXT)');
  const before = db.prepare('SELECT name,sql FROM sqlite_schema ORDER BY name').all();
  assert.throws(() => createAppAccess({ database: db, appId: APP, initialize: true }), code('app_access_schema_invalid'));
  assert.deepEqual(db.prepare('SELECT name,sql FROM sqlite_schema ORDER BY name').all(), before);
});

test('only an explicit new resource creates its creator owner; visits and platform/card ownership confer no app rights', t => {
  const f = fixture(t);
  assert.throws(() => f.access.capture(alice, 'team-a'), code('app_access_denied'));
  const owner = f.access.createResource(alice, 'team-a');
  assert.equal(owner.role, 'owner'); assert.equal(owner.revision, 1); assert.ok(Object.isFrozen(owner));
  for (const visitor of [bob, stranger]) {
    assert.throws(() => f.access.capture(visitor, 'team-a'), code('app_access_denied'));
    assert.throws(() => f.access.createResource(visitor, 'team-a'), code('app_access_resource_exists'));
  }
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM soty_app_access_memberships').get().n, 1);
  assert.throws(() => f.access.capture({ ...alice, email: 'old@example.test' }, 'team-a'));
});

test('app, resource, and issuer/subject namespaces stay separate even for otherwise equal identifiers', t => {
  const f = fixture(t), second = createAppAccess({ database: f.db, appId: OTHER_APP });
  const one = f.access.createResource(alice, 'team-a');
  const other = second.createResource(bob, 'team-a');
  assert.throws(() => second.withAccess(one, { roles: ['owner'] }, f.effect), code('app_access_proof_invalid'));
  assert.throws(() => f.access.capture(bob, 'team-a'), code('app_access_denied'));
  assert.throws(() => f.access.capture(alice, 'team-b'), code('app_access_denied'));
  assert.throws(() => f.access.capture({ ...alice, issuer: 'https://another.test/human-identity' }, 'team-a'), code('app_access_denied'));
  f.access.withAccess(one, { roles: ['owner'] }, f.effect);
  second.withAccess(other, { roles: ['owner'] }, f.effect);
  assert.deepEqual(f.effects().map(row => row.app_id), [APP, OTHER_APP]);
  assert.notEqual(f.effects()[0].principal_id, f.effects()[1].principal_id);
});

test('setRole, demotion and revoke require the resource current owner; viewer and editor do not acquire management', t => {
  const f = fixture(t), owner = f.access.createResource(alice, 'team-a');
  for (const role of ['viewer', 'editor']) {
    f.access.setRole(owner, bob, role);
    const member = f.access.capture(bob, 'team-a');
    assert.equal(member.role, role);
    assert.throws(() => f.access.setRole(member, stranger, 'owner'), code('app_access_denied'));
    assert.throws(() => f.access.revoke(member, alice), code('app_access_denied'));
    if (role === 'viewer') assert.throws(() => f.access.withAccess(member, { roles: ['owner', 'editor'] }, f.effect), code('app_access_denied'));
    else assert.equal(f.access.withAccess(member, { roles: ['owner', 'editor'] }, f.effect), 'saved');
  }
  assert.equal(f.effects().length, 1);
});

test('captured editor requests cannot apply an effect after demotion; a fresh viewer can read but cannot write', t => {
  const f = fixture(t), owner = f.access.createResource(alice, 'team-a');
  f.access.setRole(owner, bob, 'editor'); const pending = f.access.capture(bob, 'team-a');
  f.access.setRole(owner, bob, 'viewer');
  assert.throws(() => f.access.withAccess(pending, { roles: ['owner', 'editor'] }, f.effect), code('app_access_changed'));
  assert.equal(f.effects().length, 0);
  const fresh = f.access.capture(bob, 'team-a');
  assert.equal(f.access.withAccess(fresh, { roles: ['viewer'] }, context => context.role), 'viewer');
  assert.throws(() => f.access.withAccess(fresh, { roles: ['editor'] }, f.effect), code('app_access_denied'));
});

test('revocation and later regrant never reactivate an old captured request', t => {
  const f = fixture(t), owner = f.access.createResource(alice, 'team-a');
  f.access.setRole(owner, bob, 'editor'); const pending = f.access.capture(bob, 'team-a');
  f.access.revoke(owner, bob);
  assert.throws(() => f.access.capture(bob, 'team-a'), code('app_access_denied'));
  assert.throws(() => f.access.withAccess(pending, { roles: ['editor'] }, f.effect), code('app_access_denied'));
  f.access.setRole(owner, bob, 'editor');
  assert.throws(() => f.access.withAccess(pending, { roles: ['editor'] }, f.effect), code('app_access_changed'));
  assert.equal(f.access.withAccess(f.access.capture(bob, 'team-a'), { roles: ['editor'] }, f.effect), 'saved');
  assert.equal(f.effects().length, 1);
});

test('owner handover preserves at least one owner and a demoted captured owner cannot change any role', t => {
  const f = fixture(t), owner = f.access.createResource(alice, 'team-a');
  assert.throws(() => f.access.revoke(owner, alice), code('app_access_last_owner'));
  assert.throws(() => f.access.setRole(owner, alice, 'viewer'), code('app_access_last_owner'));
  f.access.setRole(owner, bob, 'owner'); const successor = f.access.capture(bob, 'team-a');
  f.access.setRole(successor, alice, 'viewer');
  assert.throws(() => f.access.setRole(owner, stranger, 'owner'), code('app_access_changed'));
  assert.throws(() => f.access.revoke(successor, bob), code('app_access_last_owner'));
  assert.equal(f.access.capture(bob, 'team-a').role, 'owner');
});

test('authority and application effects share one transaction: exceptions and returned promises roll back writes', t => {
  const f = fixture(t), owner = f.access.createResource(alice, 'team-a');
  const syncThrow = () => { f.effect({ database: f.db, appId: APP, resourceId: 'team-a', principalId: 'test' }); throw Error('abort'); };
  assert.throws(() => f.access.withAccess(owner, { roles: ['owner'] }, syncThrow), /abort/u);
  assert.equal(f.effects().length, 0);
  assert.throws(() => f.access.withAccess(owner, { roles: ['owner'] }, context => { f.effect(context); return Promise.resolve('late'); }), code('source_app_authority_invalid'));
  assert.equal(f.effects().length, 0);
  let called = false;
  assert.throws(() => f.access.withAccess(owner, { roles: ['owner'] }, async () => { called = true; }), code('app_access_callback_invalid'));
  assert.throws(() => f.access.withAccess(owner, { roles: ['owner'] }, function* () { called = true; }), code('app_access_callback_invalid'));
  assert.equal(called, false);
  assert.equal(f.db.isTransaction, false);
});

test('a foreign transaction is preserved, reentrancy is refused, and transaction loss is detected', t => {
  const f = fixture(t), owner = f.access.createResource(alice, 'team-a');
  f.db.exec('BEGIN IMMEDIATE');
  assert.throws(() => f.access.withAccess(owner, { roles: ['owner'] }, f.effect), code('app_access_transaction_active'));
  assert.equal(f.db.isTransaction, true); f.db.exec('ROLLBACK');
  f.access.withAccess(owner, { roles: ['owner'] }, () => {
    assert.throws(() => f.access.withAccess(owner, { roles: ['owner'] }, f.effect), code('app_access_transaction_active'));
  });
  assert.throws(() => f.access.withAccess(owner, { roles: ['owner'] }, () => f.db.exec('ROLLBACK')), code('app_access_transaction_lost'));
  assert.equal(f.effects().length, 0);
});

test('proofs are instance-local opaque objects, not JSON roles, and access options are closed', t => {
  const f = fixture(t), owner = f.access.createResource(alice, 'team-a');
  const reopened = createAppAccess({ database: f.db, appId: APP });
  assert.throws(() => f.access.withAccess(JSON.parse(JSON.stringify(owner)), { roles: ['owner'] }, f.effect), code('app_access_proof_invalid'));
  assert.throws(() => reopened.withAccess(owner, { roles: ['owner'] }, f.effect), code('app_access_proof_invalid'));
  assert.throws(() => f.access.withAccess(owner, { roles: ['admin'] }, f.effect), code('app_access_roles_invalid'));
  assert.throws(() => f.access.withAccess(owner, { roles: ['owner'], email: 'a@example.test' }, f.effect));
  assert.throws(() => f.access.setRole(owner, bob, 'platform-admin'), code('app_access_role_invalid'));
  assert.equal(f.effects().length, 0);
});

test('another SQLite connection can revoke a captured request, and persisted ACL survives a reopen', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'soty-app-access-')); const file = join(dir, 'application.sqlite');
  t.after(() => rm(dir, { recursive: true, force: true }));
  const db = new DatabaseSync(file), second = new DatabaseSync(file);
  try {
    db.exec('PRAGMA busy_timeout=1000'); second.exec('PRAGMA busy_timeout=1000');
    const access = createAppAccess({ database: db, appId: APP, initialize: true }), other = createAppAccess({ database: second, appId: APP });
    const owner = access.createResource(alice, 'team-a'); access.setRole(owner, bob, 'editor');
    const pending = access.capture(bob, 'team-a');
    other.revoke(other.capture(alice, 'team-a'), bob);
    assert.throws(() => access.withAccess(pending, { roles: ['editor'] }, () => { throw Error('must not run'); }), code('app_access_denied'));
  } finally { db.close(); second.close(); }
  const reopened = new DatabaseSync(file);
  try {
    const access = createAppAccess({ database: reopened, appId: APP });
    assert.equal(access.capture(alice, 'team-a').role, 'owner');
    assert.throws(() => access.capture(bob, 'team-a'), code('app_access_denied'));
  } finally { reopened.close(); }
});
