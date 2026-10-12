import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:http';
import WebSocket from 'ws';
import { createAppsService } from '../server/index.mjs';
import { STANDARD_WORKSPACE_SOURCE, STANDARD_ROOT_WORKSPACE_SOURCE } from '../../source-app/server/standard-profile.mjs';
import { migrateAppsSchema, inspectAppsSchema, ensureInitialPublication, ensureCanonicalDomain, runtimeTargetDigest } from '../server/schema.mjs';
import { createPublicationRegistry } from '../server/publications.mjs';
import { createSourceRegistry } from '../server/sources.mjs';
import { createScopedAdmissionRegistry } from '../scoped-embed/admissions.mjs';
import { HIVE_SELECTED_SOURCE } from '../scoped-embed/resource-route-adapters.mjs';

// Actual Apps DDL/SourceRegistry/Publication/ScopedAdmission, all in OWN synthetic SQLite.
// Constructor-owned actor/channel fixture ports are not Native or public-body authority.
function fixture(t, version = 8, profileVersion = 2, options = {}) {
  const db = new DatabaseSync(options.databasePath ?? ':memory:'); db.exec('PRAGMA foreign_keys=ON');
  migrateAppsSchema(db, { legacyTemplate: options.template ?? '', allowScopedEmbedMigration: version >= 7, allowSelectedResourceMigration: version >= 8 });
  assert.equal(inspectAppsSchema(db), 'v' + version);
  const owner = Object.freeze({ accountId: 'synthetic_owner', deviceId: 'synthetic_browser' });
  const appId = 'app-' + 'a'.repeat(32), identity = { linkId: 'synthetic_link', hostDeviceId: 'synthetic_host', connectorId: 'synthetic_connector' };
  const connectorKey = [identity.linkId, identity.hostDeviceId, identity.connectorId].join('|');
  const grants = { accountIds: ['synthetic_reader'], communityIds: [] };
  db.exec('BEGIN IMMEDIATE');
  db.prepare('INSERT INTO app_devices VALUES(?,?,?,?,?)').run(connectorKey, owner.accountId, JSON.stringify(identity), 'Synthetic device', 1);
  db.prepare('INSERT INTO local_apps VALUES(?,?,?,?,?,?,?,?,?,?,?)').run(appId, owner.accountId, connectorKey, 'Preserved project', 63345, '/', JSON.stringify(grants), 'enabled', 1, 1, 1);
  db.prepare('INSERT INTO local_app_grants VALUES(?,?,?)').run(appId, 'account', grants.accountIds[0]);
  const app = db.prepare('SELECT * FROM local_apps WHERE id=?').get(appId);
  ensureCanonicalDomain(db, app, options.template ?? ''); ensureInitialPublication(db, app); db.exec('COMMIT');
  let active = true, clock = 100000, probeCalls = 0, notify;
  const assertActor = actor => {
    if (!active || actor.accountId !== owner.accountId || actor.deviceId !== owner.deviceId) throw Object.assign(Error('apps_authentication_required'), { code: 'apps_authentication_required', status: 401 });
  };
  const target = { appId, revision: 2, ownerAccountId: owner.accountId, connectorKey, port: 63345,
    entryPath: '/embed', profile: 'soty.selected-human-embed.v' + profileVersion };
  target.digest = runtimeTargetDigest(target);
  const profile = { schema: target.profile, appId, connector: identity, target: { revision: target.revision, digest: target.digest },
    sourceProfile: options.pin ?? (profileVersion === 2 ? { ...HIVE_SELECTED_SOURCE } : { id: 'planner.selected-workspace', version: 1, digest: '9'.repeat(64) }),
    resource: { registryId: 'soty', tenantId: owner.accountId, appId, environmentId: 'production', resourceId: 'selected',
      ...(profileVersion === 2 ? { selection: { kind: options.pin ? 'soty.workspace.v1' : 'hive.project.v1', nativeId: 'synthetic_native', incarnationId: 'one' } } : { workspaceId: 'selected' }) },
    issuer: 'https://root.test/human-identity', clientId: 'synthetic-selected', embedOrigin: options.template?.replace('{appId}', appId) ?? 'https://selected.apps.test',
    nativeOrigin: 'https://native.test', parentOrigin: 'https://root.test' };
  const admissions = createScopedAdmissionRegistry({ db, profiles: [profile], clock: () => clock });
  const publications = createPublicationRegistry({ db, now: () => clock, assertActor, canUse: actor => actor.accountId === owner.accountId });
  const channel = Object.freeze({ fixture: 'constructor-owned-current-channel' });
  const openSources = () => createSourceRegistry({ db, now: () => clock, assertActor, publications, scopedAdmission: admissions,
    prepareTarget: async frame => { assert.equal(db.isTransaction, false); probeCalls++; return Object.freeze({ target: frame.target, channel }); },
    verifyPreparedTarget: frame => frame.evidence.channel === channel && frame.evidence.target === frame.target,
    onChanged: event => notify?.(event) });
  let sources = openSources();
  t.after(() => { sources.close(); db.close(); });
  const get = () => publications.execute({ op: 'apps.publication.get', actor: owner, args: { appId } });
  const prepareArgs = () => ({ appId, expectedPolicyEpoch: get().policyEpoch, expectedTargetRevision: get().activeTargetRevision,
    source: { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, port: target.port, entryPath: target.entryPath, profile: target.profile } });
  const prepare = () => sources.execute({ op: 'apps.source.prepare', actor: owner, args: prepareArgs() });
  const intent = prepared => ({ appId, requestId: 'synthetic-selected-promotion', preparationId: prepared.preparationId,
    expectedPolicyEpoch: prepared.expectedPolicyEpoch, expectedTargetRevision: prepared.expectedTargetRevision, launchPolicy: 'restricted', listed: false });
  const promote = args => sources.execute({ op: 'apps.source.promote', actor: owner, args });
  const rows = () => Object.fromEntries(db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*' ORDER BY name").all()
    .map(({ name }) => [name, db.prepare(`SELECT * FROM ${name} ORDER BY rowid`).all()]));
  return { db, admissions, target, appId, profile, owner, identity, get, rows, prepare, intent, promote, probeCalls: () => probeCalls,
    reopen() { sources.close(); sources = openSources(); }, revoke() { active = false; }, advance() { clock += 60000; },
    notify(work) { notify = work; }, history() { return sources.execute({ op: 'apps.source.history', actor: owner, args: { appId } }); } };
}

test('selected v2 on actual Apps6/7 refuses before probe; legacy SQLite promotion failure stays an effect0 counter', async t => {
  for (const version of [6, 7]) {
    const f = fixture(t, version), before = f.rows();
    let prepared, failure, oldPromotionClass = 'not_attempted';
    try { prepared = await f.prepare(); } catch (error) { failure = error; }
    if (prepared) {
      // Diagnostic only on the old unguarded implementation, never an actual owner retry.
      try { f.promote(f.intent(prepared)); oldPromotionClass = 'unexpected_commit'; }
      catch (error) { oldPromotionClass = error.code === 'ERR_SQLITE_ERROR' && error.errcode === 275 ? 'sqlite_check_constraint' : 'other_error'; }
    }
    t.diagnostic(JSON.stringify({ schemaVersion: version, probeCalls: f.probeCalls(), oldPromotionClass,
      policyEpoch: f.get().policyEpoch, activeTargetRevision: f.get().activeTargetRevision,
      historyRows: f.rows().app_runtime_targets.length, sourceReceipts: f.rows().app_source_receipts.length }));
    assert.deepEqual(f.rows(), before);
    assert.equal(failure?.code, 'app_scoped_migration_required'); assert.equal(failure?.status, 503);
    assert.equal(prepared, undefined); assert.equal(f.probeCalls(), 0);
  }
});

test('real Apps8 promotion commits exact target2/admission/floor/epoch/receipt/history; lost ACK replays after reopening', async t => {
  const f = fixture(t), before = f.rows(), prepared = await f.prepare();
  assert.deepEqual(f.rows(), before); assert.equal(prepared.target.revision, 2);
  f.notify(() => { throw Error('synthetic_lost_ACK_after_commit'); });
  const args = f.intent(prepared);
  assert.throws(() => f.promote(args), /synthetic_lost_ACK_after_commit/);
  const after = f.rows();
  assert.equal(f.get().policyEpoch, 2); assert.equal(f.get().activeTargetRevision, 2);
  assert.equal(f.get().requiredBindingVersion, 2); assert.equal(after.app_source_receipts.length, 1);
  assert.equal(after.app_scoped_embed_admissions.length, 1); assert.equal(after.app_runtime_targets.length, 2);
  assert.equal(f.admissions.require(f.target).schema, 'soty.selected-human-embed.v2');
  for (const name of ['local_apps', 'local_app_grants', 'app_devices', 'app_domains', 'app_publication_domains']) assert.deepEqual(after[name], before[name]);
  assert.deepEqual(f.db.prepare('PRAGMA foreign_key_check').all(), []); assert.equal(inspectAppsSchema(f.db), 'v8');
  f.notify(undefined); f.advance(); f.reopen();
  const replay = f.promote(args);
  assert.equal(replay.replayed, true); assert.equal(replay.receipt.targetRevision, 2); assert.equal(replay.current.activeTargetRevision, 2);
  assert.deepEqual(f.rows(), after); assert.equal(f.history().targets.length, 2); assert.equal(f.probeCalls(), 1);
});

test('legacy selected v1 still prepares and promotes on the real Apps7 schema', async t => {
  const f = fixture(t, 7, 1), prepared = await f.prepare(), result = f.promote(f.intent(prepared));
  assert.equal(result.current.activeTargetRevision, 2); assert.equal(f.admissions.require(f.target).schema, 'soty.selected-human-embed.v1');
  assert.equal(inspectAppsSchema(f.db), 'v7'); assert.equal(f.rows().app_scoped_embed_admissions.length, 1);
});

test('after a valid Apps8 preparation, original DDL inspection failure rolls back partial target/floor writes atomically', async t => {
  const f = fixture(t), prepared = await f.prepare();
  f.db.exec('DROP TRIGGER app_scoped_admission_no_update');
  const tampered = f.rows();
  assert.throws(() => f.promote(f.intent(prepared)), { code: 'apps_schema_unsupported' });
  assert.deepEqual(f.rows(), tampered); assert.equal(f.get().activeTargetRevision, 1);
  assert.equal(f.get().policyEpoch, 1); assert.equal(f.get().requiredBindingVersion, 1);
  assert.equal(f.rows().app_source_receipts.length, 0); assert.equal(f.rows().app_scoped_embed_admissions.length, 0);
  assert.equal(f.db.isTransaction, false);
});

test('valid Apps8 preparation still requires current owner and exact target/policy CAS before any effect', async t => {
  const f = fixture(t), prepared = await f.prepare(), args = f.intent(prepared), before = f.rows();
  assert.throws(() => f.promote({ ...args, expectedTargetRevision: 2 }), { code: 'app_publication_target_conflict' });
  assert.throws(() => f.promote({ ...args, expectedPolicyEpoch: 2 }), { code: 'app_publication_revision_conflict' });
  f.revoke(); assert.throws(() => f.promote(args), { code: 'apps_authentication_required', status: 401 });
  assert.deepEqual(f.rows(), before);
});


test('actual Apps launch maps a saved root path only for admitted workspace2, preserving actor and immutable target', async t => {
  for (const pin of [STANDARD_ROOT_WORKSPACE_SOURCE, STANDARD_WORKSPACE_SOURCE]) await t.test('workspace' + pin.version, async sub => {
    const directory = mkdtempSync(join(tmpdir(), 'soty-saved-entry-')), databasePath = join(directory, 'apps.sqlite');
    const template = 'https://{appId}.apps.test', f = fixture(sub, 8, 2, { databasePath, template, pin });
    f.promote(f.intent(await f.prepare())); const before = f.rows();
    let active = true;
    const service = createAppsService({ databasePath, appOriginTemplate: template, shellOrigins: ['https://root.test'],
      allowScopedEmbedMigration: true, allowSelectedResourceMigration: true, scopedEmbedProfiles: [f.profile],
      withAuthorityFence: callback => callback(),
      actorActive: actor => active && actor.accountId === f.owner.accountId && actor.deviceId === f.owner.deviceId,
      authenticateConnector: async auth => auth.token === 'synthetic-channel-token' && auth.linkId === f.identity.linkId
        && auth.deviceId === f.identity.hostDeviceId && auth.connectorId === f.identity.connectorId,
      signSourceAuthority: async () => { throw Error('launch_does_not_issue_identity_witness'); },
      withHumanSubjectAuthority: ({ actor, issuer, clientId }, callback) => {
        assert.equal(actor, f.owner); assert.ok(active); return callback({ issuer, clientId, subject: 'synthetic-subject', clientGeneration: 1, clientProfileDigest: 'c'.repeat(64) }); } });
    const peers = new Set(), server = createServer(); server.on('connection', socket => { peers.add(socket); socket.once('close', () => peers.delete(socket)); }); server.on('upgrade', (req, socket, head) => { if (!service.handleUpgrade(req, socket, head)) socket.destroy(); });
    await new Promise(done => server.listen(0, '127.0.0.1', done));
    const ws = new WebSocket(`ws://127.0.0.1:${server.address().port}/api/apps/channel`);
    sub.after(async () => { ws.terminate(); service.close(); for (const peer of peers) peer.destroy(); await new Promise(done => server.close(done)); rmSync(directory, { recursive: true, force: true }); });
    ws.on('error', () => {});
    // Actual channel with a controlled connector ACK, not an upstream health assertion.
    ws.on('message', bytes => { const frame = JSON.parse(bytes);
      if (frame.type === 'binding-set') ws.send(JSON.stringify({ type: 'binding-ack', channelId: frame.channelId, syncId: frame.syncId,
        appId: frame.target.appId, revision: frame.target.revision, digest: frame.target.digest, profile: frame.target.profile })); });
    await new Promise((done, reject) => { ws.once('open', done); ws.once('error', reject); });
    ws.send(JSON.stringify({ type: 'auth', schema: 'soty.apps-channel.v1', ...f.identity, token: 'synthetic-channel-token',
      capabilities: { targetBindingVersions: [1, 2], runtimeProfiles: ['soty.relay-restricted.v1', f.profile.schema] } }));
    const call = args => service.execute({ op: 'apps.launch', actor: f.owner, args: { appId: f.appId, instanceId: 'main', ...args } });
    const deadline = Date.now() + 3000; let initial;
    while (!initial) { try { initial = call({ path: '/embed' }); } catch (error) {
      if (!['app_offline', 'app_binding_pending'].includes(error.code) || Date.now() >= deadline) throw error;
      await new Promise(done => setTimeout(done, 5)); } }
    if (pin.version === 2) {
      const launched = call({ domainId: initial.entry.domainId, path: '/' });
      assert.deepEqual(launched.entry, initial.entry); assert.equal(launched.entry.path, '/embed');
      assert.deepEqual(launched.scopedSource, pin); assert.equal(new URL(launched.launchUrl).searchParams.get('path'), '/embed');
    } else assert.throws(() => call({ path: '/' }), { code: 'app_scoped_admission_required' });
    for (const path of ['/?q=1', '/board']) assert.throws(() => call({ path }), { code: 'app_scoped_admission_required' });
    active = false; assert.throws(() => call({ path: '/' }), { code: 'apps_authentication_required' });
    assert.deepEqual(f.rows(), before, 'no preference, target, admission or publication mutation');
  });
});
