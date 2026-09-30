import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { Worker } from 'node:worker_threads';
import { createAppsService } from '../server/index.mjs';
import { createDomainRegistry } from '../server/domains.mjs';
import { createHistoricalAppsV2 } from '../../../deploy/connector/apps-v2.fixture.mjs';

// This independent acceptance suite uses public RPC/policy entrypoints and a
// historical v2 fixture, not the author's test helper or today's migration.
const alice = { accountId: 'acct_alice', deviceId: 'device_alice' };
const bob = { accountId: 'acct_bob', deviceId: 'device_bob' };
const guest = { accountId: 'acct_guest', deviceId: 'device_guest' };
const stranger = { accountId: 'acct_stranger', deviceId: 'device_stranger' };
const appA = 'app-' + '1'.repeat(32), appB = 'app-' + '2'.repeat(32);
const appC = 'app-' + '3'.repeat(32), appR = 'app-' + '4'.repeat(32);
const aliasA = 'dom_' + '1'.repeat(32), aliasA2 = 'dom_' + '2'.repeat(32);
const aliasB = 'dom_' + '3'.repeat(32), aliasC = 'dom_' + '4'.repeat(32), retired = 'dom_' + '5'.repeat(32);
const legacy = 'https://{appId}.legacy.example', named = 'https://apps.example';
const profile = 'soty.relay-restricted.v1';
const hash = value => createHash('sha256').update(value).digest('hex');
const canonical = id => ({ id: 'dom_' + hash('canonical:' + id).slice(0, 32), origin: legacy.replace('{appId}', id) });
const code = value => error => error?.code === value;
const denied = error => ['apps_access_denied', 'app_access_changed', 'app_revoked'].includes(error?.code);
const ownerTables = ['app_devices', 'local_apps', 'local_app_grants'];
const domainTables = ['app_domain_zones', 'app_domain_heads', 'app_domains', 'app_domain_receipts'];
const policyTables = ['app_publications', 'app_publication_domains', 'app_publication_receipts', 'app_runtime_targets'];
const identity = actor => ({ linkId: 'link_' + actor.accountId, hostDeviceId: 'host_' + actor.accountId, connectorId: 'connector_' + actor.accountId });
const rows = (db, table) => db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all();
const snapshot = (db, tables) => Object.fromEntries(tables.map(table => [table, rows(db, table)]));

// Historical core DDL from 635022f. Version1 fixtures are not produced by
// deleting objects from a current database or calling the current migrator.
function coreV1(db, version) {
  db.exec(`CREATE TABLE apps_meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
    CREATE TABLE app_devices (connector_key TEXT PRIMARY KEY,owner_account_id TEXT NOT NULL,identity_json TEXT NOT NULL,name TEXT NOT NULL,created_at INTEGER NOT NULL);
    CREATE TABLE local_apps (id TEXT PRIMARY KEY,owner_account_id TEXT NOT NULL,connector_key TEXT NOT NULL REFERENCES app_devices(connector_key),name TEXT NOT NULL,port INTEGER NOT NULL,entry_path TEXT NOT NULL,grants_json TEXT NOT NULL,state TEXT NOT NULL,revision INTEGER NOT NULL,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL);
    CREATE INDEX local_apps_owner ON local_apps(owner_account_id);
    CREATE TABLE local_app_grants (app_id TEXT NOT NULL REFERENCES local_apps(id) ON DELETE CASCADE,kind TEXT NOT NULL CHECK(kind IN ('account','community')),principal_id TEXT NOT NULL,PRIMARY KEY(app_id,kind,principal_id));
    CREATE INDEX local_app_grants_principal ON local_app_grants(kind,principal_id,app_id);
    INSERT INTO apps_meta VALUES ('schema','soty.apps-registry.v1'); PRAGMA user_version=${version}`);
}

function historical(db, version) {
  if (version === 2) createHistoricalAppsV2(db); else coreV1(db, version);
  for (const actor of [alice, bob]) db.prepare('INSERT INTO app_devices VALUES (?,?,?,?,?)')
    .run(actor.accountId, actor.accountId, JSON.stringify(identity(actor)), 'Synthetic computer', 10);
  for (const [id, actor, port, state] of [[appA, alice, 9101, 'enabled'], [appB, alice, 9102, 'enabled'],
    [appC, bob, 9201, 'enabled'], [appR, alice, 9103, 'revoked']]) {
    const grants = { accountIds: id === appA ? [guest.accountId] : [], communityIds: [] };
    db.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)')
      .run(id, actor.accountId, actor.accountId, 'Private synthetic app', port, '/start?mode=private', JSON.stringify(grants), state, 7, 11, 12);
    for (const accountId of grants.accountIds) db.prepare('INSERT INTO local_app_grants VALUES (?,?,?)').run(id, 'account', accountId);
  }
  if (version !== 2) return;
  db.prepare("UPDATE apps_meta SET value=? WHERE key='legacy_origin_template'").run(legacy);
  const oldZone = 'zone_' + hash(legacy).slice(0, 32), newTemplate = 'https://{slug}.apps.example';
  const newZone = 'zone_' + hash(newTemplate).slice(0, 32);
  db.prepare('INSERT INTO app_domain_zones VALUES (?,?,?,?,?,?,?)').run(oldZone, 'legacy', legacy, 'legacy.example', 'https', '', 10);
  db.prepare('INSERT INTO app_domain_zones VALUES (?,?,?,?,?,?,?)').run(newZone, 'named', newTemplate, 'apps.example', 'https', '', 10);
  for (const [id, actor] of [[appA, alice], [appB, alice], [appC, bob], [appR, alice]]) {
    const address = canonical(id);
    db.prepare('INSERT INTO app_domain_heads VALUES (?,4)').run(id);
    db.prepare("INSERT INTO app_domains VALUES (?,?,?,?,NULL,?,?,'canonical','bound',10,NULL)")
      .run(address.id, oldZone, new URL(address.origin).hostname, address.origin, id, actor.accountId);
  }
  for (const [id, appId, owner, slug, state] of [[aliasA, appA, alice, 'alpha', 'bound'], [aliasA2, appA, alice, 'alpha-two', 'bound'],
    [aliasB, appB, alice, 'bravo', 'bound'], [aliasC, appC, bob, 'charlie', 'bound'], [retired, appA, alice, 'old-alpha', 'tombstone']]) {
    db.prepare("INSERT INTO app_domains VALUES (?,?,?,?,?,?,?,'alias',?,12,?)")
      .run(id, newZone, slug + '.apps.example', 'https://' + slug + '.apps.example', slug, appId, owner.accountId, state, state === 'tombstone' ? 14 : null);
  }
  db.prepare('INSERT INTO app_domain_receipts VALUES (?,?,?,?,?,?,?)')
    .run(alice.accountId, hash('historical-domain-key'), hash('historical-domain-intent'), 'claim', aliasA, 1, 13);
}

async function fixture(t, { version = 2, autoStart = true } = {}) {
  const root = await mkdtemp(path.join(tmpdir(), 'soty-publication-acceptance-'));
  const databasePath = path.join(root, 'registry.sqlite');
  const initial = new DatabaseSync(databasePath); historical(initial, version);
  const preserved = snapshot(initial, [...ownerTables, ...(version === 2 ? domainTables : [])]); initial.close();
  const active = new Set([alice, bob, guest, stranger].map(actor => actor.accountId + ':' + actor.deviceId));
  let timestamp = 100_000, member = true, admin = true;
  const config = { databasePath, appOriginTemplate: legacy, namedAppZone: named, shellOrigins: ['https://soty.example'],
    domainLimits: { perApp: 100, perAccount: 200 }, actorActive: actor => active.has(actor.accountId + ':' + actor.deviceId),
    canAccessCommunity: (id, community) => member && id === guest.accountId && community === 'community_test',
    isGroupAdmin: (id, community) => admin && id === alice.accountId && community === 'community_test', now: () => timestamp };
  const services = [];
  const start = () => { const service = createAppsService(config); services.push(service); return service; };
  let service = autoStart ? start() : null;
  const db = new DatabaseSync(databasePath); db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000');
  t.after(async () => {
    for (const item of services) item.close(); db.close();
    assert.equal(path.dirname(path.resolve(root)), path.resolve(tmpdir()));
    assert.match(path.basename(root), /^soty-publication-acceptance-/u);
    await rm(root, { recursive: true, force: true });
  });
  const rpc = (op, args, actor = alice) => service.execute({ op, args, actor });
  const get = (appId = appA, actor = alice) => rpc('apps.publication.get', { appId }, actor);
  function intent(requestId, { appId = appA, actor = alice, public: open = false, ...change } = {}) {
    const current = get(appId, actor);
    return { appId, requestId, expectedPolicyEpoch: current.policyEpoch, expectedTargetRevision: current.activeTargetRevision,
      launchPolicy: open ? 'anyone' : 'restricted', listed: false, activeDomainIds: [],
      ...(open ? { exposureAck: { scope: 'whole-port', targetRevision: current.activeTargetRevision, targetDigest: current.target.digest, profile } } : {}),
      ...change };
  }
  return { root, databasePath, db, preserved, config, active, rpc, get, intent, start,
    get service() { return service; }, reopen() { service?.close(); service = start(); return service; },
    now: value => { timestamp = value; }, membership: (m, a) => { member = m; admin = a; } };
}

for (const version of [0, 1, 2]) test(`independent historical Apps${version === 0 ? '1/user_version0' : version} preserves data and remains private across migration and reopen`, async t => {
  const f = await fixture(t, { version });
  assert.equal(f.db.prepare('PRAGMA user_version').get().user_version, 5);
  assert.equal(f.db.prepare("SELECT value FROM apps_meta WHERE key='schema'").get().value, 'soty.apps-registry.v5');
  assert.deepEqual(snapshot(f.db, Object.keys(f.preserved)), f.preserved);
  for (const appId of [appA, appB, appC, appR]) {
    const value = f.get(appId, appId === appC ? bob : alice);
    assert.equal(value.launchPolicy, 'restricted'); assert.equal(value.listed, false); assert.equal(value.policyEpoch, 1);
    assert.equal(value.runtimeReady, false); assert.deepEqual(value.activeDomainIds, []);
    assert.equal(value.target.profile, profile); assert.equal(value.target.revision, 1);
    const target = rows(f.db, 'app_runtime_targets').find(row => row.app_id === appId);
    assert.equal(value.target.digest, hash(JSON.stringify(['soty.runtime-target.v1', appId, 1, target.owner_account_id,
      target.connector_key, target.port, target.entry_path, profile])));
    const address = canonical(appId);
    assert.throws(() => f.service.policy.decideAccess({ domainId: address.id, origin: address.origin }), code('apps_access_denied'));
  }
  assert.equal(f.get(appR).appState, 'revoked');
  const migrated = snapshot(f.db, [...ownerTables, ...domainTables, ...policyTables]);
  f.reopen(); assert.deepEqual(snapshot(f.db, Object.keys(migrated)), migrated);
  assert.deepEqual(f.rpc('apps.list', {}, stranger).apps, []);
});

test('malformed historical source aborts the whole migration without leaving v3 objects or rewriting old rows', async t => {
  const f = await fixture(t, { autoStart: false });
  f.db.prepare('UPDATE local_apps SET entry_path=? WHERE id=?').run('/_soty/control', appA);
  const before = snapshot(f.db, ['apps_meta', ...ownerTables, ...domainTables]);
  const definitions = f.db.prepare('SELECT name,sql FROM sqlite_schema ORDER BY name').all();
  assert.throws(() => f.start(), code('apps_registry_corrupt'));
  assert.equal(f.db.prepare('PRAGMA user_version').get().user_version, 2);
  assert.deepEqual(snapshot(f.db, Object.keys(before)), before);
  assert.deepEqual(f.db.prepare('SELECT name,sql FROM sqlite_schema ORDER BY name').all(), definitions);
});

test('v1 stale grant-index rows never become authority and real grants_json remains unchanged', async t => {
  const f = await fixture(t, { version: 1, autoStart: false });
  f.db.prepare('INSERT INTO local_app_grants VALUES (?,?,?)').run(appA, 'account', stranger.accountId);
  const authority = rows(f.db, 'local_apps'); f.reopen();
  assert.deepEqual(rows(f.db, 'local_apps'), authority);
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM local_app_grants WHERE principal_id=?').get(stranger.accountId).n, 0);
  assert.deepEqual(f.rpc('apps.list', {}, stranger).apps, []);
  const address = canonical(appA);
  assert.equal(f.service.policy.decideAccess({ domainId: address.id, origin: address.origin, actor: guest }).accessBasis, 'grant');
  assert.throws(() => f.service.policy.decideAccess({ domainId: address.id, origin: address.origin, actor: stranger }), code('apps_access_denied'));
});

test('registration initializes a private pinned target once and duplicate registration never resets an existing publication', async t => {
  const f = await fixture(t), device = identity(alice);
  f.rpc('apps.publication.update', f.intent('published-original', { public: true, activeDomainIds: [aliasA] }));
  const before = snapshot(f.db, [...ownerTables, ...domainTables, ...policyTables]);
  const duplicate = f.rpc('apps.register', { hostDeviceId: device.hostDeviceId, connectorId: device.connectorId,
    name: 'Private synthetic app', port: 9101, entryPath: '/start?mode=private', grants: { accountIds: [guest.accountId] } });
  assert.equal(duplicate.app.id, appA); assert.deepEqual(snapshot(f.db, Object.keys(before)), before);
  const registered = f.rpc('apps.register', { hostDeviceId: device.hostDeviceId, connectorId: device.connectorId,
    name: 'New private app', port: 9400, entryPath: '/hello', grants: {} });
  const view = f.get(registered.app.id);
  assert.equal(view.launchPolicy, 'restricted'); assert.equal(view.listed, false); assert.deepEqual(view.activeDomainIds, []);
  assert.equal(view.policyEpoch, 1); assert.equal(view.target.revision, 1); assert.equal(view.target.port, 9400);
  assert.equal(view.target.entryPath, '/hello'); assert.equal(view.target.profile, profile);
});

test('migration never exposes named aliases and a new claim is still inactive after explicit public publication', async t => {
  const f = await fixture(t);
  for (const [domainId, origin] of [[aliasA, 'https://alpha.apps.example'], [retired, 'https://old-alpha.apps.example']])
    assert.throws(() => f.service.policy.decideAccess({ domainId, origin, actor: alice }), code('apps_access_denied'));
  f.rpc('apps.publication.update', f.intent('open-alpha', { public: true, activeDomainIds: [aliasA], listed: true }));
  const before = f.get();
  const claim = f.rpc('apps.domains.claim', { appId: appA, slug: 'fresh-alpha', requestId: 'new-alias', expectedDomainsRevision: 4 });
  assert.deepEqual(f.get(), before);
  assert.throws(() => f.service.policy.decideAccess({ domainId: claim.receipt.domainId, origin: claim.receipt.origin }), code('apps_access_denied'));
  const domain = f.rpc('apps.domains.get', { appId: appA }).domains.find(item => item.id === claim.receipt.domainId);
  assert.equal(domain.runtimeReady, false); assert.equal(domain.runtimeMode, 'status-only');
});

test('activation rejects another app of this owner, another owner, canonical, tombstone and unknown addresses without partial state', async t => {
  const f = await fixture(t), before = snapshot(f.db, policyTables);
  for (const domainId of [aliasB, aliasC, canonical(appA).id, retired, 'dom_' + 'f'.repeat(32)]) {
    assert.throws(() => f.rpc('apps.publication.update', f.intent('bad-' + domainId,
      { public: true, activeDomainIds: [aliasA, domainId] })), code('app_publication_domain_unavailable'));
    assert.deepEqual(snapshot(f.db, policyTables), before);
  }
  assert.throws(() => f.rpc('apps.publication.update', f.intent('duplicate', { activeDomainIds: [aliasA, aliasA] })), code('invalid_publication_domains'));
  assert.throws(() => f.rpc('apps.publication.update', f.intent('listed-private', { listed: true, activeDomainIds: [aliasA] })), code('invalid_publication_listing'));
  assert.throws(() => f.rpc('apps.publication.update', f.intent('listed-no-domain', { public: true, listed: true })), code('app_publication_domain_required'));
});

test('public consent binds the whole port, immutable target digest/revision and exact supported runtime profile', async t => {
  const f = await fixture(t), good = f.intent('consent', { public: true, activeDomainIds: [aliasA] });
  for (const exposureAck of [undefined, { ...good.exposureAck, scope: 'page' }, { ...good.exposureAck, profile: 'unrestricted' },
    { ...good.exposureAck, targetDigest: '0'.repeat(64) }, { ...good.exposureAck, targetRevision: 2 },
    { ...good.exposureAck, targetDigest: f.get(appB).target.digest }])
    assert.throws(() => f.rpc('apps.publication.update', { ...good, exposureAck }), code('app_exposure_ack_required'));
  assert.throws(() => f.rpc('apps.publication.update', { ...good, expectedTargetRevision: 2,
    exposureAck: { ...good.exposureAck, targetRevision: 2 } }), code('app_publication_target_conflict'));
  assert.equal(f.get().policyEpoch, 1);
  const accepted = f.rpc('apps.publication.update', good);
  assert.deepEqual(accepted.receipt.exposureAck, good.exposureAck);
  for (const query of ['UPDATE app_runtime_targets SET port=9999 WHERE app_id=?', 'DELETE FROM app_runtime_targets WHERE app_id=?'])
    assert.throws(() => f.db.prepare(query).run(appA), /app_runtime_target_immutable/);
  assert.throws(() => f.rpc('apps.update', { appId: appA, port: 9999 }), code('unexpected_argument'));
  assert.throws(() => f.rpc('apps.update', { appId: appA, entryPath: '/different' }), code('unexpected_argument'));
  assert.equal(f.get().target.digest, good.exposureAck.targetDigest);
});

test('compound target ownership cannot bind another app revision and a corrupted digest is refused at admission', async t => {
  const f = await fixture(t), target = f.db.prepare('SELECT * FROM app_runtime_targets WHERE app_id=?').get(appB);
  f.db.prepare('INSERT INTO app_runtime_targets VALUES (?,?,?,?,?,?,?,?,?)').run(appB, 2, alice.accountId, target.connector_key,
    target.port, target.entry_path, profile, hash(JSON.stringify(['soty.runtime-target.v1', appB, 2, alice.accountId,
      target.connector_key, target.port, target.entry_path, profile])), 15);
  assert.throws(() => f.db.prepare('UPDATE app_publications SET active_target_revision=2 WHERE app_id=?').run(appA), /FOREIGN KEY/);
  f.rpc('apps.publication.update', f.intent('activate-private', { activeDomainIds: [aliasA] }));
  f.db.exec('DROP TRIGGER app_runtime_target_no_update'); // Fault injection, not a supported application operation.
  f.db.prepare('UPDATE app_runtime_targets SET digest=? WHERE app_id=?').run('0'.repeat(64), appA);
  assert.throws(() => f.service.policy.decideAccess({ domainId: aliasA, origin: 'https://alpha.apps.example', actor: alice }), code('apps_registry_corrupt'));
  assert.throws(() => f.get(), code('apps_registry_corrupt'));
});

test('lost response replay survives restart, distinguishes historical/current and checks the current actor first', async t => {
  const f = await fixture(t);
  const request = f.intent('historical-domain-key', { public: true, activeDomainIds: [aliasA2, aliasA], listed: true });
  const first = f.rpc('apps.publication.update', request);
  assert.equal(first.replayed, false); assert.equal(rows(f.db, 'app_domain_receipts').length, 1);
  f.rpc('apps.publication.update', f.intent('close-publication'));
  f.reopen();
  const retry = f.rpc('apps.publication.update', { ...request, activeDomainIds: [aliasA, aliasA2] });
  assert.equal(retry.replayed, true); assert.deepEqual(retry.receipt, first.receipt);
  assert.equal(retry.receipt.launchPolicy, 'anyone'); assert.equal(retry.current.launchPolicy, 'restricted');
  assert.equal(retry.current.policyEpoch, 3); assert.deepEqual(retry.current.activeDomainIds, []);
  assert.throws(() => f.rpc('apps.publication.update', { ...request, listed: false }), code('app_publication_request_conflict'));
  f.active.delete(alice.accountId + ':' + alice.deviceId);
  assert.throws(() => f.rpc('apps.publication.update', request), code('apps_authentication_required'));
  f.active.add(alice.accountId + ':' + alice.deviceId);
  assert.throws(() => f.rpc('apps.publication.update', request, bob), code('apps_owner_required'));
  assert.throws(() => f.rpc('apps.publication.update', { ...request, expectedAccountId: bob.accountId }), code('authentication_required'));
  assert.throws(() => f.rpc('apps.publication.update', f.intent(request.requestId, { appId: appB })), code('app_publication_request_conflict'));
  const unrelated = f.rpc('apps.publication.update', f.intent(request.requestId, { appId: appC, actor: bob }), bob);
  assert.equal(unrelated.replayed, false); assert.equal(unrelated.current.appId, appC);
});

test('receipt retention follows the last64 committed epochs per app with a reversing clock and never rebases a pruned retry', async t => {
  const f = await fixture(t), requests = [], receipts = [];
  f.rpc('apps.publication.update', f.intent('separate-app', { appId: appB }));
  for (let i = 0; i < 70; i++) {
    f.now(100_000 - i * 7);
    const request = f.intent('epoch-' + i); requests.push(request);
    receipts.push(f.rpc('apps.publication.update', request).receipt);
  }
  const retained = f.db.prepare('SELECT committed_epoch FROM app_publication_receipts WHERE app_id=? ORDER BY committed_epoch').all(appA);
  assert.deepEqual(retained.map(item => item.committed_epoch), Array.from({ length: 64 }, (_, i) => i + 8));
  assert.equal(f.get().policyEpoch, 71);
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM app_publication_receipts WHERE app_id=?').get(appB).n, 1);
  const before = snapshot(f.db, policyTables);
  assert.throws(() => f.rpc('apps.publication.update', requests[0]), code('app_publication_revision_conflict'));
  assert.deepEqual(snapshot(f.db, policyTables), before);
  const retainedRetry = f.rpc('apps.publication.update', requests[6]);
  assert.equal(retainedRetry.replayed, true); assert.deepEqual(retainedRetry.receipt, receipts[6]);
  assert.equal(retainedRetry.current.policyEpoch, 71);
  assert.equal(f.rpc('apps.publication.update', f.intent('emergency-restrict')).current.policyEpoch, 72);
});

test('receipt insertion and pruning faults roll back policy, domain set, new receipt and epoch together', async t => {
  const f = await fixture(t);
  for (let i = 0; i < 64; i++) f.rpc('apps.publication.update', f.intent('fill-' + i));
  for (const event of ['INSERT', 'DELETE']) {
    const before = snapshot(f.db, policyTables);
    f.db.exec(`CREATE TRIGGER acceptance_fault BEFORE ${event} ON app_publication_receipts BEGIN SELECT RAISE(ABORT,'acceptance_fault'); END`);
    assert.throws(() => f.rpc('apps.publication.update', f.intent('failed-' + event, { public: true, listed: true, activeDomainIds: [aliasA] })), /acceptance_fault/);
    assert.deepEqual(snapshot(f.db, policyTables), before);
    f.db.exec('DROP TRIGGER acceptance_fault');
  }
});

test('grant removal and its policy epoch are atomic, while a public visitor can still request fresh admission', async t => {
  const f = await fixture(t);
  f.rpc('apps.publication.update', f.intent('open', { public: true, activeDomainIds: [aliasA] }));
  const address = canonical(appA), old = f.service.policy.decideAccess({ domainId: address.id, origin: address.origin, actor: guest });
  const publicDecision = f.service.policy.decideAccess({ domainId: aliasA, origin: 'https://alpha.apps.example' });
  const before = snapshot(f.db, [...ownerTables, ...policyTables]);
  f.db.exec("CREATE TRIGGER acceptance_fault BEFORE UPDATE OF policy_epoch ON app_publications BEGIN SELECT RAISE(ABORT,'acceptance_fault'); END");
  assert.throws(() => f.rpc('apps.update', { appId: appA, grants: {} }), /acceptance_fault/);
  assert.deepEqual(snapshot(f.db, [...ownerTables, ...policyTables]), before);
  assert.equal(f.service.policy.recheckAccess(old).policyEpoch, 2);
  f.db.exec('DROP TRIGGER acceptance_fault');
  f.rpc('apps.update', { appId: appA, grants: {} });
  assert.equal(f.get().policyEpoch, 3); assert.throws(() => f.service.policy.recheckAccess(old), code('apps_access_denied'));
  assert.throws(() => f.service.policy.recheckAccess(publicDecision), code('app_access_changed'));
  assert.equal(f.service.policy.decideAccess({ domainId: aliasA, origin: 'https://alpha.apps.example' }).subject, 'public');
  f.rpc('apps.update', { appId: appA, grants: {} }); assert.equal(f.get().policyEpoch, 3);
});

test('active retirement rollback covers domain receipt/tombstone and policy; confirmed retirement cannot be revived by replay', async t => {
  const f = await fixture(t), opening = f.intent('open', { public: true, listed: true, activeDomainIds: [aliasA] });
  const accepted = f.rpc('apps.publication.update', opening);
  const old = f.service.policy.decideAccess({ domainId: aliasA, origin: 'https://alpha.apps.example' });
  const request = { appId: appA, domainId: aliasA, requestId: 'retire-active', expectedDomainsRevision: 4 };
  const before = snapshot(f.db, [...domainTables, ...policyTables]);
  f.db.exec("CREATE TRIGGER acceptance_fault BEFORE INSERT ON app_domain_receipts BEGIN SELECT RAISE(ABORT,'acceptance_fault'); END");
  assert.throws(() => f.rpc('apps.domains.retire', request), /acceptance_fault/);
  assert.deepEqual(snapshot(f.db, [...domainTables, ...policyTables]), before);
  assert.equal(f.service.policy.recheckAccess(old).policyEpoch, 2); f.db.exec('DROP TRIGGER acceptance_fault');
  assert.equal(f.rpc('apps.domains.retire', request).receipt.state, 'tombstone');
  assert.equal(f.get().policyEpoch, 3); assert.equal(f.get().listed, false); assert.deepEqual(f.get().activeDomainIds, []);
  assert.throws(() => f.service.policy.recheckAccess(old), code('apps_access_denied'));
  const replay = f.rpc('apps.publication.update', opening);
  assert.deepEqual(replay.receipt, accepted.receipt); assert.deepEqual(replay.current.activeDomainIds, []);
  assert.equal(f.rpc('apps.domains.retire', request).replayed, true); assert.equal(f.get().policyEpoch, 3);
  assert.throws(() => f.rpc('apps.domains.claim', { appId: appA, slug: 'alpha', requestId: 'reuse-retired', expectedDomainsRevision: 5 }), code('app_name_unavailable'));
});

test('revocation rollback after policy change preserves access; committed revoke stays closed after historical publication replay', async t => {
  const f = await fixture(t), opening = f.intent('open', { public: true, listed: true, activeDomainIds: [aliasA, aliasA2] });
  f.rpc('apps.publication.update', opening);
  const old = f.service.policy.decideAccess({ domainId: aliasA, origin: 'https://alpha.apps.example' });
  const before = snapshot(f.db, [...ownerTables, ...policyTables]);
  f.db.exec("CREATE TRIGGER acceptance_fault BEFORE DELETE ON app_publication_domains BEGIN SELECT RAISE(ABORT,'acceptance_fault'); END");
  assert.throws(() => f.rpc('apps.revoke', { appId: appA }), /acceptance_fault/);
  assert.deepEqual(snapshot(f.db, [...ownerTables, ...policyTables]), before);
  assert.equal(f.service.policy.recheckAccess(old).policyEpoch, 2); f.db.exec('DROP TRIGGER acceptance_fault');
  f.rpc('apps.revoke', { appId: appA });
  const current = f.get(); assert.equal(current.appState, 'revoked'); assert.equal(current.launchPolicy, 'restricted');
  assert.equal(current.listed, false); assert.deepEqual(current.activeDomainIds, []); assert.equal(current.policyEpoch, 3);
  assert.throws(() => f.service.policy.recheckAccess(old), denied);
  const replay = f.rpc('apps.publication.update', opening);
  assert.equal(replay.receipt.launchPolicy, 'anyone'); assert.equal(replay.current.appState, 'revoked');
  assert.throws(() => f.rpc('apps.publication.update', f.intent('new-publication', { public: true, activeDomainIds: [aliasA] })), code('app_revoked'));
  f.rpc('apps.revoke', { appId: appA }); assert.equal(f.get().policyEpoch, 3);
});

test('branded exact-origin admissions cannot be copied, cross-instance reused or used to open the canonical origin publicly', async t => {
  const f = await fixture(t);
  f.rpc('apps.publication.update', f.intent('open', { public: true, activeDomainIds: [aliasA] }));
  const decision = f.service.policy.decideAccess({ domainId: aliasA, origin: 'https://alpha.apps.example' });
  assert.equal(decision.subject, 'public'); assert.equal(Object.hasOwn(decision, 'actor'), false);
  assert.ok(Object.isFrozen(decision)); assert.ok(Object.isFrozen(decision.route));
  for (const copy of [{ ...decision }, structuredClone(decision), null])
    assert.throws(() => f.service.policy.recheckAccess(copy), code('app_access_decision_required'));
  const second = f.start(); assert.throws(() => second.policy.recheckAccess(decision), code('app_access_decision_required'));
  for (const origin of ['http://alpha.apps.example', 'https://alpha.apps.example:443', 'https://alpha.apps.example.attacker.test',
    'https://alpha.apps.example/', 'https://alpha-two.apps.example', 'https://soty.example'])
    assert.throws(() => f.service.policy.decideAccess({ domainId: aliasA, origin }), code('apps_access_denied'));
  const address = canonical(appA);
  assert.throws(() => f.service.policy.decideAccess({ domainId: address.id, origin: address.origin }), code('apps_access_denied'));
  assert.throws(() => f.service.policy.decideAccess({ domainId: address.id, origin: address.origin, actor: stranger }), code('apps_access_denied'));
  const granted = f.service.policy.decideAccess({ domainId: address.id, origin: address.origin, actor: guest });
  assert.equal(granted.subject, 'account'); assert.deepEqual(granted.actor, guest); assert.ok(Object.isFrozen(granted.actor));
});

test('lease expiry cannot be revived and a default recheck never silently extends its lifetime', async t => {
  const f = await fixture(t);
  f.rpc('apps.publication.update', f.intent('open', { public: true, activeDomainIds: [aliasA] }));
  const input = { domainId: aliasA, origin: 'https://alpha.apps.example' };
  for (const ttlMs of [0, -1, 30_001, Infinity]) assert.throws(() => f.service.policy.decideAccess({ ...input, ttlMs }), code('invalid_app_access_ttl'));
  const original = f.service.policy.decideAccess({ ...input, ttlMs: 100 });
  f.now(100_099); assert.equal(f.service.policy.recheckAccess(original).expiresAt, 100_100);
  assert.equal(f.service.policy.recheckAccess(original, { ttlMs: 100 }).expiresAt, 100_199);
  f.now(100_100); assert.throws(() => f.service.policy.recheckAccess(original), code('app_access_expired'));
  assert.throws(() => f.service.policy.recheckAccess(original, { ttlMs: 30_000 }), code('app_access_expired'));
  assert.throws(() => f.service.policy.decideAccess({ ...input, actor: alice, ttlMs: 3_600_001 }), code('invalid_app_access_ttl'));
});

test('private community admission rechecks membership and owner administration even without a local policy write', async t => {
  const f = await fixture(t);
  f.rpc('apps.update', { appId: appA, grants: { communityIds: ['community_test'] } });
  f.rpc('apps.publication.update', f.intent('private', { activeDomainIds: [aliasA] }));
  const input = { domainId: aliasA, origin: 'https://alpha.apps.example', actor: guest };
  const decision = f.service.policy.decideAccess(input), epoch = f.get().policyEpoch;
  f.membership(false, true); assert.throws(() => f.service.policy.recheckAccess(decision), code('apps_access_denied'));
  f.membership(true, false); assert.throws(() => f.service.policy.recheckAccess(decision), code('apps_access_denied'));
  assert.equal(f.get().policyEpoch, epoch);
  f.membership(true, true); assert.equal(f.service.policy.recheckAccess(decision).policyEpoch, epoch);
  f.active.delete(guest.accountId + ':' + guest.deviceId);
  assert.throws(() => f.service.policy.recheckAccess(decision), code('apps_authentication_required'));
});

test('signed public traffic has public basis and loss of community authority cannot silently downgrade a grant session', async t => {
  const f = await fixture(t);
  f.rpc('apps.update', { appId: appA, grants: { communityIds: ['community_test'] } });
  f.rpc('apps.publication.update', f.intent('open-with-community', { public: true, activeDomainIds: [aliasA] }));
  const input = { domainId: aliasA, origin: 'https://alpha.apps.example' };
  const granted = f.service.policy.decideAccess({ ...input, actor: guest });
  assert.equal(granted.subject, 'account'); assert.equal(granted.accessBasis, 'grant');
  const owner = f.service.policy.decideAccess({ ...input, actor: alice }); assert.equal(owner.accessBasis, 'grant');
  const signedVisitor = f.service.policy.decideAccess({ ...input, actor: stranger });
  assert.equal(signedVisitor.subject, 'account'); assert.equal(signedVisitor.accessBasis, 'public');
  const anonymous = f.service.policy.decideAccess(input); assert.equal(anonymous.subject, 'public'); assert.equal(anonymous.accessBasis, 'public');
  const epoch = f.get().policyEpoch;
  for (const state of [[false, true], [true, false]]) {
    f.membership(...state);
    assert.equal(f.get().policyEpoch, epoch);
    assert.throws(() => f.service.policy.recheckAccess(granted), error => ['apps_access_denied', 'app_access_changed'].includes(error?.code));
    assert.equal(f.service.policy.recheckAccess(signedVisitor).accessBasis, 'public');
    assert.equal(f.service.policy.recheckAccess(anonymous).accessBasis, 'public');
    const fresh = f.service.policy.decideAccess({ ...input, actor: guest });
    assert.equal(fresh.subject, 'account'); assert.equal(fresh.accessBasis, 'public');
    const address = canonical(appA);
    assert.throws(() => f.service.policy.decideAccess({ domainId: address.id, origin: address.origin, actor: guest }), code('apps_access_denied'));
  }
  f.membership(false, true);
  const originallyPublic = f.service.policy.decideAccess({ ...input, actor: guest });
  assert.equal(originallyPublic.accessBasis, 'public');
  f.membership(true, true);
  assert.equal(f.get().policyEpoch, epoch);
  assert.equal(f.service.policy.recheckAccess(originallyPublic).accessBasis, 'public', 'membership gain never promotes an existing public session');
  assert.equal(f.service.policy.decideAccess({ ...input, actor: guest }).accessBasis, 'grant', 'a new admission can reserve grant capacity');
  assert.throws(() => f.service.policy.recheckAccess(originallyPublic, { accessBasis: 'grant' }), code('unexpected_argument'));
});

test('domain coupling is mandatory and a missing publication stays corrupt instead of being silently reconstructed', async t => {
  const f = await fixture(t);
  assert.throws(() => createDomainRegistry({ db: f.db, assertActor: () => {} }), code('apps_policy_validator_required'));
  f.db.prepare('DELETE FROM app_publications WHERE app_id=?').run(appA);
  const before = snapshot(f.db, [...ownerTables, ...policyTables]);
  assert.throws(() => f.rpc('apps.update', { appId: appA, grants: {} }), code('apps_registry_corrupt'));
  assert.deepEqual(snapshot(f.db, [...ownerTables, ...policyTables]), before);
  assert.throws(() => f.reopen(), code('apps_registry_corrupt'));
  assert.deepEqual(snapshot(f.db, [...ownerTables, ...policyTables]), before);
});

async function race(t, databasePath, commands) {
  const source = `const {parentPort,workerData}=require('node:worker_threads');
    (async()=>{ const {createAppsService}=await import(workerData.serviceUrl);
      const service=createAppsService({...workerData.config,actorActive:()=>true});
      parentPort.once('message',()=>{ try { parentPort.postMessage({result:service.execute(workerData.command)}); }
        catch(error){parentPort.postMessage({error:error.code||error.name});} finally{service.close();parentPort.close();} });
      parentPort.postMessage({ready:true});
    })().catch(error=>{parentPort.postMessage({fatal:error.code||error.name});parentPort.close();});`;
  const workers = commands.map(command => new Worker(source, { eval: true, workerData: { command,
    serviceUrl: new URL('../server/index.mjs', import.meta.url).href,
    config: { databasePath, appOriginTemplate: legacy, namedAppZone: named, shellOrigins: ['https://soty.example'], domainLimits: { perApp: 100, perAccount: 200 } } } }));
  const ready = [], completed = [], exits = [];
  for (const worker of workers) {
    let yesReady, noReady, yesResult, noResult;
    ready.push(new Promise((resolve, reject) => { yesReady = resolve; noReady = reject; }));
    completed.push(new Promise((resolve, reject) => { yesResult = resolve; noResult = reject; }));
    const failure = error => { noReady(error); noResult(error); };
    worker.on('error', failure);
    worker.on('message', value => {
      if (value.ready) yesReady(); else if (value.fatal) failure(new Error(value.fatal)); else yesResult(value);
    });
    exits.push(new Promise(resolve => worker.on('exit', status => {
      if (status) failure(new Error('acceptance_worker_exit_' + status)); resolve();
    })));
  }
  // Attach result handlers before waiting for both workers, including failures.
  const results = Promise.all(completed); results.catch(() => {});
  let timeout;
  try {
    return await Promise.race([(async () => {
      await Promise.all(ready); for (const worker of workers) worker.postMessage('go');
      const values = await results; await Promise.all(exits); return values;
    })(), new Promise((_, reject) => { timeout = setTimeout(() => reject(new Error('acceptance_workers_timeout')), 10_000); })]);
  } finally {
    clearTimeout(timeout); await Promise.all(workers.map(worker => worker.terminate()));
  }
}

test('two SQLite connections serialize competing publication intents and deduplicate simultaneous same-key lost-ACK retries', { timeout: 20_000 }, async t => {
  const f = await fixture(t), first = f.intent('racer-a'), second = f.intent('racer-b');
  const call = args => ({ op: 'apps.publication.update', args, actor: alice });
  const outcomes = await race(t, f.databasePath, [call(first), call(second)]);
  assert.equal(outcomes.filter(item => item.result).length, 1);
  assert.deepEqual(outcomes.filter(item => item.error).map(item => item.error), ['app_publication_revision_conflict']);
  assert.equal(f.get().policyEpoch, 2);
  const repeat = f.intent('same-key'), retries = await race(t, f.databasePath, [call(repeat), call(repeat)]);
  assert.ok(retries.every(item => item.result)); assert.deepEqual(retries.map(item => item.result.replayed).sort(), [false, true]);
  assert.deepEqual(retries[0].result.receipt, retries[1].result.receipt); assert.equal(f.get().policyEpoch, 3);
});

test('publication and legacy grants have both valid serial orders, not an artificial exactly-one-success rule', async t => {
  for (const publicationFirst of [true, false]) {
    const f = await fixture(t), pending = f.intent('pending', { public: true, activeDomainIds: [aliasA] });
    const publication = () => f.rpc('apps.publication.update', pending);
    const grant = () => f.rpc('apps.update', { appId: appA, grants: {} });
    if (publicationFirst) { publication(); grant(); assert.equal(f.get().policyEpoch, 3); }
    else { grant(); assert.throws(publication, code('app_publication_revision_conflict')); assert.equal(f.get().policyEpoch, 2); }
    assert.deepEqual(JSON.parse(f.db.prepare('SELECT grants_json FROM local_apps WHERE id=?').get(appA).grants_json), { accountIds: [], communityIds: [] });
  }
});

for (const mutation of ['grants', 'retire', 'revoke']) test(`real concurrent publication versus ${mutation} never leaves stale authority committed`, { timeout: 20_000 }, async t => {
  const f = await fixture(t);
  f.rpc('apps.publication.update', f.intent('open', { public: true, activeDomainIds: [aliasA], listed: true }));
  const pending = f.intent('concurrent-update', { public: true, activeDomainIds: [aliasA], listed: true });
  const command = mutation === 'grants' ? { op: 'apps.update', args: { appId: appA, grants: {} } }
    : mutation === 'retire' ? { op: 'apps.domains.retire', args: { appId: appA, domainId: aliasA, requestId: 'concurrent-retire', expectedDomainsRevision: 4 } }
      : { op: 'apps.revoke', args: { appId: appA } };
  const outcomes = await race(t, f.databasePath, [{ op: 'apps.publication.update', args: pending, actor: alice }, { ...command, actor: alice }]);
  assert.ok(outcomes[1].result); assert.ok(outcomes[0].result || outcomes[0].error === 'app_publication_revision_conflict');
  const current = f.get(); assert.equal(current.policyEpoch, outcomes[0].result ? 4 : 3);
  if (mutation === 'grants') assert.deepEqual(JSON.parse(f.db.prepare('SELECT grants_json FROM local_apps WHERE id=?').get(appA).grants_json), { accountIds: [], communityIds: [] });
  else { assert.deepEqual(current.activeDomainIds, []); assert.equal(current.listed, false); }
  if (mutation === 'retire') assert.equal(f.db.prepare('SELECT state FROM app_domains WHERE id=?').get(aliasA).state, 'tombstone');
  if (mutation === 'revoke') assert.equal(current.appState, 'revoked');
});
