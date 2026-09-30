import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { Worker } from 'node:worker_threads';
import { migrateAppsSchema, inspectAppsSchema, APPS_REGISTRY_SCHEMA, RUNTIME_PROFILE, runtimeTargetDigest } from '../server/schema.mjs';
import { createAppsService } from '../server/index.mjs';
import { createDomainRegistry } from '../server/domains.mjs';

const owner = { accountId: 'account_owner', deviceId: 'device_owner' }, other = { accountId: 'account_other', deviceId: 'device_other' };
const appA = 'app-' + 'a'.repeat(32), appB = 'app-' + 'b'.repeat(32), appC = 'app-' + 'c'.repeat(32);
const aliasA = 'dom_' + 'a'.repeat(32), aliasB = 'dom_' + 'b'.repeat(32), retired = 'dom_' + 'd'.repeat(32);
const legacy = 'https://{appId}.legacy.example', named = 'https://apps.example';
const hash = value => createHash('sha256').update(value).digest('hex');

// Frozen historical DDL from the reviewed Apps v2 checkpoint 635022f. Do not
// generate this fixture with the latest migration or relabel a newer database.
const coreV1 = `CREATE TABLE apps_meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
  CREATE TABLE app_devices (connector_key TEXT PRIMARY KEY,owner_account_id TEXT NOT NULL,identity_json TEXT NOT NULL,name TEXT NOT NULL,created_at INTEGER NOT NULL);
  CREATE TABLE local_apps (id TEXT PRIMARY KEY,owner_account_id TEXT NOT NULL,connector_key TEXT NOT NULL REFERENCES app_devices(connector_key),name TEXT NOT NULL,port INTEGER NOT NULL,entry_path TEXT NOT NULL,grants_json TEXT NOT NULL,state TEXT NOT NULL,revision INTEGER NOT NULL,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL);
  CREATE INDEX local_apps_owner ON local_apps(owner_account_id);
  CREATE TABLE local_app_grants (app_id TEXT NOT NULL REFERENCES local_apps(id) ON DELETE CASCADE,kind TEXT NOT NULL CHECK(kind IN ('account','community')),principal_id TEXT NOT NULL,PRIMARY KEY(app_id,kind,principal_id));
  CREATE INDEX local_app_grants_principal ON local_app_grants(kind,principal_id,app_id);`;
const domainsV2 = `CREATE TABLE app_domain_zones (
    id TEXT PRIMARY KEY, kind TEXT NOT NULL CHECK(kind IN ('legacy','named')), origin_template TEXT NOT NULL UNIQUE,
    suffix TEXT NOT NULL, scheme TEXT NOT NULL CHECK(scheme IN ('https','http')), port TEXT NOT NULL, created_at INTEGER NOT NULL);
  CREATE TABLE app_domain_heads (app_id TEXT PRIMARY KEY REFERENCES local_apps(id),revision INTEGER NOT NULL CHECK(revision>=0));
  CREATE TABLE app_domains (
    id TEXT PRIMARY KEY,zone_id TEXT NOT NULL REFERENCES app_domain_zones(id),hostname TEXT NOT NULL UNIQUE,
    origin TEXT NOT NULL UNIQUE,slug TEXT,app_id TEXT NOT NULL REFERENCES local_apps(id),owner_account_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('canonical','alias')),state TEXT NOT NULL CHECK(state IN ('bound','tombstone')),
    created_at INTEGER NOT NULL,retired_at INTEGER,
    CHECK((role='canonical' AND slug IS NULL AND state='bound' AND retired_at IS NULL) OR
      (role='alias' AND slug IS NOT NULL AND ((state='bound' AND retired_at IS NULL) OR (state='tombstone' AND retired_at IS NOT NULL)))));
  CREATE UNIQUE INDEX app_domain_canonical ON app_domains(app_id) WHERE role='canonical';
  CREATE INDEX app_domain_app ON app_domains(app_id,created_at,id);
  CREATE INDEX app_domain_owner ON app_domains(owner_account_id,role);
  CREATE TABLE app_domain_receipts (
    account_id TEXT NOT NULL,request_key TEXT NOT NULL,intent_hash TEXT NOT NULL,
    action TEXT NOT NULL CHECK(action IN ('claim','retire')),domain_id TEXT NOT NULL REFERENCES app_domains(id),
    committed_revision INTEGER NOT NULL,created_at INTEGER NOT NULL,PRIMARY KEY(account_id,request_key));`;

function historical(db, version = 2) {
  db.exec(coreV1);
  db.prepare('INSERT INTO apps_meta VALUES (?,?)').run('schema', `soty.apps-registry.v${version === 2 ? 2 : 1}`);
  db.exec('PRAGMA user_version=' + version);
  for (const actor of [owner, other]) {
    db.prepare('INSERT INTO app_devices VALUES (?,?,?,?,?)').run(actor.accountId, actor.accountId,
      JSON.stringify({ linkId: `link_${actor.accountId}`, hostDeviceId: `host_${actor.accountId}`, connectorId: `connector_${actor.accountId}` }), 'Device', 1);
  }
  for (const [id, actor, port, state, grants] of [[appA, owner, 9001, 'enabled', { accountIds: [other.accountId], communityIds: [] }],
    [appB, owner, 9002, 'revoked', { accountIds: [], communityIds: [] }], [appC, other, 9003, 'enabled', { accountIds: [], communityIds: [] }]]) {
    db.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)').run(id, actor.accountId, actor.accountId, 'App', port, '/start', JSON.stringify(grants), state, 7, 1, 2);
    for (const principal of grants.accountIds) db.prepare('INSERT INTO local_app_grants VALUES (?,?,?)').run(id, 'account', principal);
  }
  if (version !== 2) return;
  db.exec(domainsV2);
  db.prepare('INSERT INTO apps_meta VALUES (?,?)').run('legacy_origin_template', legacy);
  const legacyZone = 'zone_' + hash(legacy).slice(0, 32), namedTemplate = 'https://{slug}.apps.example', namedZone = 'zone_' + hash(namedTemplate).slice(0, 32);
  db.prepare('INSERT INTO app_domain_zones VALUES (?,?,?,?,?,?,?)').run(legacyZone, 'legacy', legacy, 'legacy.example', 'https', '', 1);
  db.prepare('INSERT INTO app_domain_zones VALUES (?,?,?,?,?,?,?)').run(namedZone, 'named', namedTemplate, 'apps.example', 'https', '', 1);
  for (const [id, actor] of [[appA, owner], [appB, owner], [appC, other]]) {
    db.prepare('INSERT INTO app_domain_heads VALUES (?,?)').run(id, 3);
    const origin = legacy.replace('{appId}', id);
    db.prepare("INSERT INTO app_domains VALUES (?,?,?,?,NULL,?,?,'canonical','bound',?,NULL)")
      .run('dom_' + hash('canonical:' + id).slice(0, 32), legacyZone, new URL(origin).hostname, origin, id, actor.accountId, 1);
  }
  for (const [id, app, actor, slug, state, retiredAt] of [[aliasA, appA, owner, 'alpha', 'bound', null],
    [aliasB, appC, other, 'beta', 'bound', null], [retired, appA, owner, 'old-alpha', 'tombstone', 4]]) {
    db.prepare("INSERT INTO app_domains VALUES (?,?,?,?,?,?,?,'alias',?,?,?)")
      .run(id, namedZone, `${slug}.apps.example`, `https://${slug}.apps.example`, slug, app, actor.accountId, state, 2, retiredAt);
  }
  db.prepare('INSERT INTO app_domain_receipts VALUES (?,?,?,?,?,?,?)').run(owner.accountId, hash('old-name-request'), hash('intent'), 'claim', aliasA, 1, 2);
}

async function fixture(t, { version = 2 } = {}) {
  const directory = await mkdtemp(path.join(tmpdir(), 'soty-publication-'));
  const databasePath = path.join(directory, 'apps.sqlite'), db = new DatabaseSync(databasePath), services = [];
  db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000');
  if (version !== 'empty') historical(db, version);
  t.after(async () => {
    for (const service of services) service.close();
    db.close();
    assert.equal(path.dirname(path.resolve(directory)), path.resolve(tmpdir())); assert.match(path.basename(directory), /^soty-publication-/u);
    await rm(directory, { recursive: true, force: true });
  });
  return { directory, databasePath, db, services };
}

function currentRows(db, table) { return db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(); }

test('historical v2 migrates once to v3 with private publication, initial pinned targets and unchanged apps/domains/grants', async t => {
  const f = await fixture(t);
  assert.equal(inspectAppsSchema(f.db), 'v2');
  const preserved = ['app_devices', 'local_apps', 'local_app_grants', 'app_domain_zones', 'app_domain_heads', 'app_domains', 'app_domain_receipts'];
  const before = Object.fromEntries(preserved.map(table => [table, currentRows(f.db, table)]));
  const result = migrateAppsSchema(f.db, { legacyTemplate: legacy, now: () => 10 });
  assert.equal(result.schema, APPS_REGISTRY_SCHEMA); assert.equal(result.migrated, true);
  assert.equal(inspectAppsSchema(f.db), 'v3'); assert.equal(f.db.prepare('PRAGMA user_version').get().user_version, 3);
  for (const table of preserved) assert.deepEqual(currentRows(f.db, table), before[table]);
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM app_publication_domains').get().n, 0);
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM app_publication_receipts').get().n, 0);
  for (const row of currentRows(f.db, 'app_publications')) {
    assert.equal(row.launch_policy, 'restricted'); assert.equal(row.listed, 0); assert.equal(row.policy_epoch, 1);
    assert.equal(row.active_target_revision, 1); assert.equal(row.exposure_ack_json, null);
    const app = f.db.prepare('SELECT * FROM local_apps WHERE id=?').get(row.app_id), target = f.db.prepare('SELECT * FROM app_runtime_targets WHERE app_id=?').get(row.app_id);
    assert.equal(target.connector_key, app.connector_key); assert.equal(target.port, app.port); assert.equal(target.entry_path, app.entry_path);
    assert.equal(target.profile, RUNTIME_PROFILE); assert.equal(target.digest, runtimeTargetDigest({ appId: app.id, revision: 1,
      ownerAccountId: app.owner_account_id, connectorKey: app.connector_key, port: app.port, entryPath: app.entry_path, profile: RUNTIME_PROFILE }));
  }
  const all = currentRows(f.db, 'app_publications');
  assert.equal(migrateAppsSchema(f.db, { legacyTemplate: legacy, now: () => 20 }).migrated, false);
  assert.deepEqual(currentRows(f.db, 'app_publications'), all);
  const reopened = new DatabaseSync(f.databasePath);
  try { assert.equal(inspectAppsSchema(reopened), 'v3'); assert.deepEqual(currentRows(reopened, 'app_publications'), all); } finally { reopened.close(); }
});

test('empty and historical v1 versions 0/1 initialize v3 without named activation or reset of source/grants', async t => {
  for (const version of ['empty', 0, 1]) {
    const f = await fixture(t, { version }), before = version === 'empty' ? [] : currentRows(f.db, 'local_apps');
    migrateAppsSchema(f.db, { legacyTemplate: legacy });
    assert.equal(inspectAppsSchema(f.db), 'v3'); assert.deepEqual(currentRows(f.db, 'local_apps'), before);
    assert.equal(currentRows(f.db, 'app_runtime_targets').length, before.length);
    assert.deepEqual(currentRows(f.db, 'app_publication_domains'), []);
  }
});

test('migration rejects bad ownership and rolls every new object and marker back to exact v2', async t => {
  for (const mutation of ["UPDATE app_devices SET owner_account_id='different_owner' WHERE connector_key='account_owner'",
    `UPDATE app_domains SET owner_account_id='different_owner' WHERE id='${aliasA}'`]) {
    const f = await fixture(t); f.db.exec(mutation);
    const before = currentRows(f.db, 'local_apps');
    assert.throws(() => migrateAppsSchema(f.db, { legacyTemplate: legacy }), /apps_registry_corrupt/);
    assert.equal(inspectAppsSchema(f.db), 'v2'); assert.deepEqual(currentRows(f.db, 'local_apps'), before);
    assert.equal(f.db.prepare("SELECT count(*) AS n FROM sqlite_schema WHERE name='app_publications'").get().n, 0);
  }
});

test('target rows are immutable and composite foreign keys never select a different app or owner', async t => {
  const f = await fixture(t); migrateAppsSchema(f.db, { legacyTemplate: legacy });
  assert.throws(() => f.db.prepare('UPDATE app_runtime_targets SET port=9010 WHERE app_id=?').run(appA), /app_runtime_target_immutable/);
  assert.throws(() => f.db.prepare('DELETE FROM app_runtime_targets WHERE app_id=?').run(appA), /app_runtime_target_immutable/);
  f.db.prepare('INSERT INTO app_runtime_targets SELECT app_id,2,owner_account_id,connector_key,port,entry_path,profile,digest,created_at FROM app_runtime_targets WHERE app_id=?').run(appC);
  assert.throws(() => f.db.prepare('UPDATE app_publications SET active_target_revision=2 WHERE app_id=?').run(appA), /FOREIGN KEY/);
  assert.throws(() => f.db.prepare('INSERT INTO app_publication_domains VALUES (?,?,?)').run(appA, aliasB, owner.accountId), /FOREIGN KEY/);
  assert.throws(() => f.db.prepare('INSERT INTO app_runtime_targets VALUES (?,?,?,?,?,?,?,?,?)').run(appA, 2, owner.accountId,
    other.accountId, 9010, '/', RUNTIME_PROFILE, '0'.repeat(64), 10), /FOREIGN KEY/);
  assert.throws(() => f.db.prepare("UPDATE app_publications SET listed=1 WHERE app_id=?").run(appA), /CHECK/);
  assert.throws(() => f.db.prepare("UPDATE app_publications SET launch_policy='anyone' WHERE app_id=?").run(appA), /CHECK/);
});

test('unrecognized future schema and altered v3 constraints fail before migration writes', async t => {
  const unknown = await fixture(t); unknown.db.exec("UPDATE apps_meta SET value='soty.apps-registry.v4' WHERE key='schema'; PRAGMA user_version=4");
  const before = await readFile(unknown.databasePath);
  assert.throws(() => migrateAppsSchema(unknown.db, { legacyTemplate: legacy }), /apps_schema_unsupported/);
  assert.deepEqual(await readFile(unknown.databasePath), before);
  for (const mutation of ['DROP INDEX app_domains_identity_owner', 'DROP TRIGGER app_runtime_target_no_update']) {
    const f = await fixture(t); migrateAppsSchema(f.db, { legacyTemplate: legacy }); f.db.exec(mutation);
    const before = await readFile(f.databasePath);
    assert.throws(() => migrateAppsSchema(f.db, { legacyTemplate: legacy }), /apps_schema_unsupported/);
    assert.deepEqual(await readFile(f.databasePath), before);
  }
});

async function serviceFixture(t, options = {}) {
  const f = await fixture(t), active = new Set([owner.deviceId, other.deviceId]);
  let timestamp = 10_000;
  const config = { databasePath: f.databasePath, appOriginTemplate: legacy, namedAppZone: named, shellOrigins: ['https://shell.example'],
    actorActive: actor => active.has(actor?.deviceId), now: () => timestamp, ...options };
  const service = createAppsService(config); f.services.push(service);
  const rpc = (op, args = {}, actor = owner) => service.execute({ op, args, actor });
  const get = (id = appA, actor = owner) => rpc('apps.publication.get', { appId: id }, actor);
  const intent = (requestId, overrides = {}, id = appA) => {
    const current = get(id);
    return { appId: id, requestId, expectedPolicyEpoch: current.policyEpoch, expectedTargetRevision: current.activeTargetRevision,
      launchPolicy: 'restricted', listed: false, activeDomainIds: [], ...overrides };
  };
  const publish = (requestId, overrides = {}) => {
    const current = get();
    return intent(requestId, { launchPolicy: 'anyone', activeDomainIds: [aliasA],
      exposureAck: { scope: 'whole-port', targetRevision: current.target.revision, targetDigest: current.target.digest, profile: current.target.profile }, ...overrides });
  };
  return { ...f, service, rpc, get, intent, publish, active, setTime: value => { timestamp = value; }, config };
}

const code = expected => error => error?.code === expected;

test('publication is explicit, owner/account guarded, namespace separated, and replay distinguishes historical from current state', async t => {
  const f = await serviceFixture(t), initial = f.get();
  assert.equal(initial.runtimeReady, false); assert.equal(initial.launchPolicy, 'restricted');
  assert.throws(() => f.get(appA, other), code('apps_owner_required'));
  assert.throws(() => f.rpc('apps.publication.get', { appId: appA, expectedAccountId: other.accountId }), code('authentication_required'));
  assert.throws(() => f.rpc('apps.publication.update', f.intent('missing-ack', { launchPolicy: 'anyone' })), code('app_exposure_ack_required'));
  const request = f.publish('old-name-request', { listed: true }); // Same key exists in the domain namespace.
  const first = f.rpc('apps.publication.update', request);
  assert.equal(first.requestId, request.requestId); assert.equal(first.replayed, false); assert.equal(first.receipt.policyEpoch, 2);
  assert.equal(first.current.listed, true); assert.deepEqual(first.current.activeDomainIds, [aliasA]);
  assert.equal(first.receipt.namespace, 'apps.publication.update.v1');
  assert.equal(first.receipt.exposureAck.scope, 'whole-port');
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM app_domain_receipts').get().n, 1);
  const closed = f.rpc('apps.publication.update', f.intent('close-again'));
  assert.equal(closed.current.policyEpoch, 3);
  const replay = f.rpc('apps.publication.update', request);
  assert.deepEqual(replay.receipt, first.receipt); assert.equal(replay.replayed, true);
  assert.equal(replay.current.policyEpoch, 3); assert.equal(replay.current.launchPolicy, 'restricted');
  assert.throws(() => f.rpc('apps.publication.update', { ...request, listed: false }), code('app_publication_request_conflict'));
  f.active.delete(owner.deviceId);
  assert.throws(() => f.rpc('apps.publication.update', request), code('apps_authentication_required'));
  f.active.add(owner.deviceId);
  assert.throws(() => f.rpc('apps.publication.update', request, other), code('apps_owner_required'));
  f.service.close(); f.services.splice(f.services.indexOf(f.service), 1);
  const reopened = createAppsService(f.config); f.services.push(reopened);
  assert.deepEqual(reopened.execute({ op: 'apps.publication.update', actor: owner, args: request }).receipt, first.receipt);
});

test('only exact bound aliases and target acknowledgement can activate; a new claim on anyone stays inactive', async t => {
  const f = await serviceFixture(t), canonical = f.db.prepare("SELECT id FROM app_domains WHERE app_id=? AND role='canonical'").get(appA).id;
  for (const bad of [aliasB, retired, canonical, 'dom_' + 'f'.repeat(32)]) {
    assert.throws(() => f.rpc('apps.publication.update', f.publish('bad-' + bad, { activeDomainIds: [bad] })), code('app_publication_domain_unavailable'));
  }
  assert.throws(() => f.rpc('apps.publication.update', f.publish('dupes', { activeDomainIds: [aliasA, aliasA] })), code('invalid_publication_domains'));
  const wrong = f.publish('wrong-target'); wrong.exposureAck.targetDigest = f.get(appC, other).target.digest;
  assert.throws(() => f.rpc('apps.publication.update', wrong), code('app_exposure_ack_required'));
  assert.throws(() => f.rpc('apps.publication.update', f.publish('wrong-revision', { expectedTargetRevision: 2,
    exposureAck: { scope: 'whole-port', targetRevision: 2, targetDigest: wrong.exposureAck.targetDigest, profile: RUNTIME_PROFILE } })), code('app_publication_target_conflict'));
  assert.equal(f.get().policyEpoch, 1);
  f.rpc('apps.publication.update', f.publish('open-one'));
  const claim = f.rpc('apps.domains.claim', { appId: appA, slug: 'second-alpha', expectedDomainsRevision: 3, requestId: 'second-address' });
  assert.deepEqual(f.get().activeDomainIds, [aliasA]); assert.equal(f.get().policyEpoch, 2);
  assert.throws(() => f.service.policy.decideAccess({ domainId: claim.receipt.domainId, origin: claim.receipt.origin }), code('apps_access_denied'));
  assert.equal(f.rpc('apps.domains.get', { appId: appA }).domains.find(d => d.id === claim.receipt.domainId).runtimeReady, false);
  const result = f.rpc('apps.publication.update', f.publish('open-two', { activeDomainIds: [claim.receipt.domainId, aliasA] }));
  assert.deepEqual(result.current.activeDomainIds, [claim.receipt.domainId, aliasA].sort());
});

test('receipts retain last 64 epochs with fixed clocks; late retry never reapplies or blocks emergency restriction', async t => {
  const f = await serviceFixture(t), original = f.publish('first-public');
  f.rpc('apps.publication.update', original);
  for (let i = 0; i < 64; i++) f.rpc('apps.publication.update', f.publish('no-op-' + i));
  assert.equal(f.get().policyEpoch, 66);
  const rows = f.db.prepare('SELECT committed_epoch FROM app_publication_receipts WHERE app_id=? ORDER BY committed_epoch').all(appA);
  assert.equal(rows.length, 64); assert.equal(rows[0].committed_epoch, 3); assert.equal(rows.at(-1).committed_epoch, 66);
  assert.throws(() => f.rpc('apps.publication.update', original), code('app_publication_revision_conflict'));
  assert.equal(f.get().policyEpoch, 66);
  const close = f.rpc('apps.publication.update', f.intent('emergency-close'));
  assert.equal(close.current.policyEpoch, 67); assert.equal(close.current.launchPolicy, 'restricted');
  assert.deepEqual(close.current.activeDomainIds, []);
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM app_publication_receipts WHERE app_id=?').get(appA).n, 64);
});

test('receipt insert and prune faults roll back policy, activated aliases and epoch together', async t => {
  for (const mutation of ["CREATE TRIGGER injected BEFORE INSERT ON app_publication_receipts BEGIN SELECT RAISE(ABORT,'injected'); END",
    "CREATE TRIGGER injected BEFORE DELETE ON app_publication_receipts BEGIN SELECT RAISE(ABORT,'injected'); END"]) {
    const f = await serviceFixture(t);
    if (mutation.includes('DELETE')) for (let i = 0; i < 64; i++) f.rpc('apps.publication.update', f.intent('seed-' + i));
    const tables = ['app_publications', 'app_publication_domains', 'app_publication_receipts'];
    const before = Object.fromEntries(tables.map(table => [table, currentRows(f.db, table)]));
    f.db.exec(mutation); const intent = f.publish('fault-once');
    assert.throws(() => f.rpc('apps.publication.update', intent), /injected/);
    for (const table of tables) assert.deepEqual(currentRows(f.db, table), before[table]);
    f.db.exec('DROP TRIGGER injected');
    assert.equal(f.rpc('apps.publication.update', intent).receipt.policyEpoch, intent.expectedPolicyEpoch + 1);
  }
});

test('new registration creates target/publication atomically and an idempotent retry keeps the same model', async t => {
  const f = await serviceFixture(t), args = { hostDeviceId: 'host_account_owner', connectorId: 'connector_account_owner', name: 'New app', port: 9010 };
  f.db.exec("CREATE TRIGGER injected BEFORE INSERT ON app_runtime_targets BEGIN SELECT RAISE(ABORT,'injected'); END");
  const before = ['local_apps', 'local_app_grants', 'app_domain_heads', 'app_domains'].map(table => currentRows(f.db, table));
  assert.throws(() => f.rpc('apps.register', args), /injected/);
  assert.deepEqual(['local_apps', 'local_app_grants', 'app_domain_heads', 'app_domains'].map(table => currentRows(f.db, table)), before);
  f.db.exec('DROP TRIGGER injected');
  const first = f.rpc('apps.register', args).app, retry = f.rpc('apps.register', args).app;
  assert.equal(first.id, retry.id);
  const publication = f.get(first.id);
  assert.equal(publication.policyEpoch, 1); assert.equal(publication.target.port, 9010); assert.equal(publication.target.entryPath, '/');
  assert.equal(publication.launchPolicy, 'restricted'); assert.equal(publication.listed, false); assert.deepEqual(publication.activeDomainIds, []);
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM app_runtime_targets WHERE app_id=?').get(first.id).n, 1);
});

test('AccessDecision is private branded, exact-host pinned, bounded and freshly checked with rolling public lease', async t => {
  const f = await serviceFixture(t), canonical = f.db.prepare("SELECT id,origin FROM app_domains WHERE app_id=? AND role='canonical'").get(appA);
  assert.throws(() => f.service.policy.decideAccess({ domainId: aliasA, origin: 'https://alpha.apps.example', actor: owner }), code('apps_access_denied'));
  f.rpc('apps.publication.update', f.publish('public'));
  const guest = f.service.policy.decideAccess({ domainId: aliasA, origin: 'https://alpha.apps.example' });
  assert.equal(guest.subject, 'public'); assert.equal(Object.hasOwn(guest, 'actor'), false);
  assert.ok(Object.isFrozen(guest)); assert.ok(Object.isFrozen(guest.route)); assert.equal(guest.expiresAt, 40_000);
  assert.throws(() => f.service.policy.recheckAccess({ ...guest }), code('app_access_decision_required'));
  const independent = createAppsService(f.config); f.services.push(independent);
  assert.throws(() => independent.policy.recheckAccess(guest), code('app_access_decision_required'));
  assert.throws(() => f.service.policy.decideAccess({ domainId: aliasA, origin: 'https://beta.apps.example' }), code('apps_access_denied'));
  assert.throws(() => f.service.policy.decideAccess({ domainId: aliasA, origin: 'https://alpha.apps.example', ttlMs: 30_001 }), code('invalid_app_access_ttl'));
  assert.throws(() => f.service.policy.decideAccess({ domainId: canonical.id, origin: canonical.origin }), code('apps_access_denied'));
  const account = f.service.policy.decideAccess({ domainId: canonical.id, origin: canonical.origin, actor: other, ttlMs: 3_600_000 });
  assert.equal(account.subject, 'account'); assert.ok(Object.isFrozen(account.actor));
  f.setTime(39_999);
  const renewed = f.service.policy.recheckAccess(guest, { ttlMs: 30_000 }); assert.equal(renewed.expiresAt, 69_999);
  assert.equal(f.service.policy.recheckAccess(account).expiresAt, account.expiresAt);
  f.setTime(40_000); assert.throws(() => f.service.policy.recheckAccess(guest, { ttlMs: 30_000 }), code('app_access_expired'));
  assert.equal(f.service.policy.recheckAccess(renewed).expiresAt, 69_999);
  f.active.delete(other.deviceId); assert.throws(() => f.service.policy.recheckAccess(account), code('apps_authentication_required'));
  f.rpc('apps.publication.update', f.intent('private-again'));
  assert.throws(() => f.service.policy.recheckAccess(renewed), code('apps_access_denied'));
});

async function simultaneous(databasePath, intents) {
  const moduleUrl = new URL('../server/index.mjs', import.meta.url).href;
  const workers = intents.map(intent => new Worker(`const { parentPort, workerData } = require('node:worker_threads');
    (async () => { const {createAppsService}=await import(workerData.moduleUrl);
      const service=createAppsService({databasePath:workerData.databasePath,appOriginTemplate:workerData.legacy,namedAppZone:workerData.named,
        shellOrigins:['https://shell.example'],actorActive:()=>true,now:()=>10000});
      parentPort.postMessage({ready:true}); parentPort.once('message',()=>{
        try { const envelope=workerData.intent.op ? workerData.intent : {op:'apps.publication.update',args:workerData.intent};
          parentPort.postMessage({result:service.execute({actor:workerData.owner,...envelope})}); }
        catch(error){parentPort.postMessage({error:error.code||error.message});} finally{service.close();}
      });
    })().catch(error=>{throw error;});`, { eval: true, workerData: { databasePath, moduleUrl, owner, legacy, named, intent } }));
  try {
    const completion = workers.map(worker => new Promise((resolve, reject) => {
      worker.on('error', reject); worker.on('message', message => { if (!message.ready) resolve(message); });
      worker.on('exit', status => { if (status !== 0) reject(new Error('worker exited ' + status)); });
    }));
    await Promise.all(workers.map(worker => new Promise((resolve, reject) => { worker.once('error', reject); worker.once('message', resolve); })));
    for (const worker of workers) worker.postMessage('go');
    return await Promise.all(completion);
  } finally { await Promise.all(workers.map(worker => worker.terminate())); }
}

test('independent SQLite writers serialize CAS and lost-ACK retries to exactly one accepted mutation', { timeout: 15_000 }, async t => {
  const f = await serviceFixture(t), first = f.publish('concurrent-first'), second = { ...first, requestId: 'concurrent-second', listed: true };
  const different = await simultaneous(f.databasePath, [first, second]);
  assert.equal(different.filter(value => value.result).length, 1);
  assert.deepEqual(different.filter(value => value.error).map(value => value.error), ['app_publication_revision_conflict']);
  assert.equal(f.get().policyEpoch, 2);
  const same = f.publish('concurrent-same'), retries = await simultaneous(f.databasePath, [same, same]);
  assert.ok(retries.every(value => value.result)); assert.deepEqual(retries.map(value => value.result.replayed).sort(), [false, true]);
  assert.deepEqual(retries[0].result.receipt, retries[1].result.receipt); assert.equal(f.get().policyEpoch, 3);
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM app_publication_receipts').get().n, 2);
});

test('grants update invalidates old decisions atomically without pretending to ban a public visitor', async t => {
  const f = await serviceFixture(t), canonical = f.db.prepare("SELECT id,origin FROM app_domains WHERE app_id=? AND role='canonical'").get(appA);
  f.rpc('apps.publication.update', f.publish('open'));
  const ownerDecision = f.service.policy.decideAccess({ domainId: canonical.id, origin: canonical.origin, actor: owner });
  const granted = f.service.policy.decideAccess({ domainId: canonical.id, origin: canonical.origin, actor: other });
  const guest = f.service.policy.decideAccess({ domainId: aliasA, origin: 'https://alpha.apps.example' });
  const tables = ['local_apps', 'local_app_grants', 'app_publications'], before = tables.map(table => currentRows(f.db, table));
  f.db.exec("CREATE TRIGGER injected BEFORE UPDATE OF policy_epoch ON app_publications BEGIN SELECT RAISE(ABORT,'injected'); END");
  assert.throws(() => f.rpc('apps.update', { appId: appA, grants: {} }), /injected/);
  assert.deepEqual(tables.map(table => currentRows(f.db, table)), before);
  assert.equal(f.service.policy.recheckAccess(granted).policyEpoch, 2);
  f.db.exec('DROP TRIGGER injected');
  f.rpc('apps.update', { appId: appA, name: 'Only title' });
  assert.equal(f.get().policyEpoch, 2); assert.equal(f.service.policy.recheckAccess(granted).policyEpoch, 2);
  f.rpc('apps.update', { appId: appA, grants: {} });
  assert.equal(f.get().policyEpoch, 3);
  assert.throws(() => f.service.policy.recheckAccess(granted), code('apps_access_denied'));
  assert.throws(() => f.service.policy.recheckAccess(ownerDecision), code('app_access_changed'));
  assert.throws(() => f.service.policy.recheckAccess(guest), code('app_access_changed'));
  const freshPublic = f.service.policy.decideAccess({ domainId: aliasA, origin: 'https://alpha.apps.example' });
  assert.equal(freshPublic.subject, 'public'); assert.equal(freshPublic.policyEpoch, 3);
  f.rpc('apps.update', { appId: appA, grants: { accountIds: [], communityIds: [] } });
  assert.equal(f.get().policyEpoch, 3); // Repeating the same grant set is not a new publication command.
});

test('retire combines domain tombstone, receipt, active set and epoch in one rollback-safe mutation', async t => {
  const f = await serviceFixture(t);
  f.rpc('apps.publication.update', f.publish('open', { listed: true }));
  const guest = f.service.policy.decideAccess({ domainId: aliasA, origin: 'https://alpha.apps.example' });
  const request = { appId: appA, domainId: aliasA, requestId: 'retire-active', expectedDomainsRevision: 3 };
  const tables = ['app_domains', 'app_domain_heads', 'app_domain_receipts', 'app_publications', 'app_publication_domains'];
  const before = tables.map(table => currentRows(f.db, table));
  f.db.exec("CREATE TRIGGER injected BEFORE INSERT ON app_domain_receipts BEGIN SELECT RAISE(ABORT,'injected'); END");
  assert.throws(() => f.rpc('apps.domains.retire', request), /injected/);
  assert.deepEqual(tables.map(table => currentRows(f.db, table)), before);
  assert.equal(f.service.policy.recheckAccess(guest).policyEpoch, 2);
  f.db.exec('DROP TRIGGER injected');
  const result = f.rpc('apps.domains.retire', request);
  assert.equal(result.receipt.state, 'tombstone'); assert.equal(f.get().policyEpoch, 3);
  assert.deepEqual(f.get().activeDomainIds, []); assert.equal(f.get().listed, false);
  assert.throws(() => f.service.policy.recheckAccess(guest), code('apps_access_denied'));
  assert.equal(f.rpc('apps.domains.retire', request).replayed, true); assert.equal(f.get().policyEpoch, 3);
  const claim = f.rpc('apps.domains.claim', { appId: appA, slug: 'not-active', requestId: 'claim-inactive', expectedDomainsRevision: 4 });
  f.rpc('apps.domains.retire', { appId: appA, domainId: claim.receipt.domainId, requestId: 'retire-inactive', expectedDomainsRevision: 5 });
  assert.equal(f.get().policyEpoch, 3); // No admission existed on this inactive address.
});

test('app revoke closes every alias and publication atomically; replayed past publication cannot resurrect it', async t => {
  const f = await serviceFixture(t), original = f.publish('open', { listed: true });
  f.rpc('apps.publication.update', original);
  const guest = f.service.policy.decideAccess({ domainId: aliasA, origin: 'https://alpha.apps.example' });
  const tables = ['local_apps', 'app_publications', 'app_publication_domains'], before = tables.map(table => currentRows(f.db, table));
  f.db.exec("CREATE TRIGGER injected BEFORE UPDATE OF policy_epoch ON app_publications BEGIN SELECT RAISE(ABORT,'injected'); END");
  assert.throws(() => f.rpc('apps.revoke', { appId: appA }), /injected/);
  assert.deepEqual(tables.map(table => currentRows(f.db, table)), before);
  f.db.exec('DROP TRIGGER injected');
  f.rpc('apps.revoke', { appId: appA });
  assert.equal(f.get().appState, 'revoked'); assert.equal(f.get().policyEpoch, 3);
  assert.equal(f.get().launchPolicy, 'restricted'); assert.equal(f.get().listed, false); assert.deepEqual(f.get().activeDomainIds, []);
  assert.throws(() => f.service.policy.recheckAccess(guest), code('apps_access_denied'));
  assert.throws(() => f.rpc('apps.publication.update', f.publish('reopen')), code('app_revoked'));
  const replay = f.rpc('apps.publication.update', original);
  assert.equal(replay.replayed, true); assert.equal(replay.receipt.launchPolicy, 'anyone'); assert.equal(replay.current.appState, 'revoked');
  f.rpc('apps.revoke', { appId: appA }); assert.equal(f.get().policyEpoch, 3);
});

test('concurrent grant removal, active retirement and revoke cannot commit with a stale publication admission', { timeout: 20_000 }, async t => {
  for (const mutation of [
    { op: 'apps.update', args: { appId: appA, grants: {} } },
    { op: 'apps.domains.retire', args: { appId: appA, domainId: aliasA, expectedDomainsRevision: 3, requestId: 'concurrent-retire' } },
    { op: 'apps.revoke', args: { appId: appA } },
  ]) {
    const f = await serviceFixture(t); f.rpc('apps.publication.update', f.publish('open'));
    const pending = f.publish('concurrent-publication', { listed: true });
    const result = await simultaneous(f.databasePath, [pending, mutation]);
    assert.ok(result[1].result);
    assert.ok(result[0].result || result[0].error === 'app_publication_revision_conflict');
    assert.equal(f.get().policyEpoch, result[0].result ? 4 : 3);
    if (mutation.op === 'apps.update') assert.deepEqual(JSON.parse(f.db.prepare('SELECT grants_json FROM local_apps WHERE id=?').get(appA).grants_json), { accountIds: [], communityIds: [] });
    else { assert.deepEqual(f.get().activeDomainIds, []); assert.equal(f.get().listed, false); }
    if (mutation.op === 'apps.revoke') assert.equal(f.get().appState, 'revoked');
  }
});

test('community membership and owner administration are checked again on each branded private admission', async t => {
  let member = true, admin = true;
  const f = await serviceFixture(t, { canAccessCommunity: (id, community) => member && id === other.accountId && community === 'community_one',
    isGroupAdmin: (id, community) => admin && id === owner.accountId && community === 'community_one' });
  f.rpc('apps.update', { appId: appA, grants: { communityIds: ['community_one'] } });
  f.rpc('apps.publication.update', f.intent('private-active', { activeDomainIds: [aliasA] }));
  const decision = f.service.policy.decideAccess({ domainId: aliasA, origin: 'https://alpha.apps.example', actor: other });
  member = false; assert.throws(() => f.service.policy.recheckAccess(decision), code('apps_access_denied'));
  member = true; admin = false; assert.throws(() => f.service.policy.recheckAccess(decision), code('apps_access_denied'));
  assert.throws(() => f.service.policy.decideAccess({ domainId: aliasA, origin: 'https://alpha.apps.example' }), code('apps_access_denied'));
});

test('partial v3 publication is not repaired and domain mutation requires explicit atomic policy integration', async t => {
  const f = await serviceFixture(t);
  assert.throws(() => createDomainRegistry({ db: f.db, assertActor: () => {}, legacyTemplate: legacy,
    namedAppZone: named, shellOrigins: ['https://shell.example'] }), code('apps_policy_validator_required'));
  f.db.prepare('DELETE FROM app_publications WHERE app_id=?').run(appA);
  const before = f.db.prepare('SELECT * FROM local_apps WHERE id=?').get(appA);
  assert.throws(() => f.rpc('apps.revoke', { appId: appA }), code('apps_registry_corrupt'));
  assert.deepEqual(f.db.prepare('SELECT * FROM local_apps WHERE id=?').get(appA), before);
  const reopened = createAppsService(f.config); f.services.push(reopened);
  assert.throws(() => reopened.execute({ actor: owner, op: 'apps.publication.get', args: { appId: appA } }), code('apps_registry_corrupt'));
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM app_publications WHERE app_id=?').get(appA).n, 0);
});

test('account identity is separate from public access basis; loss of community grant never silently downgrades a live decision', async t => {
  let member = true, admin = true;
  const f = await serviceFixture(t, { canAccessCommunity: (id, community) => member && id === other.accountId && community === 'community_one',
    isGroupAdmin: (id, community) => admin && id === owner.accountId && community === 'community_one' });
  f.rpc('apps.update', { appId: appA, grants: { communityIds: ['community_one'] } });
  f.rpc('apps.publication.update', f.publish('public-community'));
  const target = { domainId: aliasA, origin: 'https://alpha.apps.example' };
  const granted = f.service.policy.decideAccess({ ...target, actor: other });
  assert.equal(granted.subject, 'account'); assert.equal(granted.accessBasis, 'grant');
  assert.equal(f.service.policy.decideAccess({ ...target, actor: owner }).accessBasis, 'grant');
  assert.equal(f.service.policy.decideAccess(target).accessBasis, 'public');
  const epoch = f.get().policyEpoch;
  member = false;
  assert.throws(() => f.service.policy.recheckAccess(granted), code('apps_access_denied'));
  const accountPublic = f.service.policy.decideAccess({ ...target, actor: other });
  assert.equal(accountPublic.subject, 'account'); assert.equal(accountPublic.accessBasis, 'public');
  assert.equal(f.service.policy.decideAccess(target).accessBasis, 'public'); assert.equal(f.get().policyEpoch, epoch);
  assert.equal(f.service.policy.decideAccess({ ...target, actor: other, forcedBasis: 'grant' }).accessBasis, 'public');
  assert.throws(() => f.service.policy.recheckAccess(accountPublic, { accessBasis: 'grant' }), code('unexpected_argument'));
  member = true;
  assert.equal(f.service.policy.recheckAccess(accountPublic).accessBasis, 'public');
  assert.equal(f.service.policy.recheckAccess(accountPublic, { ttlMs: 30_000 }).accessBasis, 'public');
  assert.equal(f.service.policy.decideAccess({ ...target, actor: other }).accessBasis, 'grant');
  assert.equal(f.service.policy.decideAccess({ ...target, actor: other, forcedBasis: 'public' }).accessBasis, 'grant');
  assert.equal(f.get().policyEpoch, epoch);
  member = true; admin = false;
  assert.throws(() => f.service.policy.recheckAccess(granted), code('apps_access_denied'));
  assert.equal(f.service.policy.decideAccess({ ...target, actor: other }).accessBasis, 'public');
  f.active.delete(other.deviceId);
  assert.throws(() => f.service.policy.recheckAccess(accountPublic), code('apps_authentication_required'));
});
