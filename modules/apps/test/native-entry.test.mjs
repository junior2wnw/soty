import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { execFileSync, spawn } from 'node:child_process';
import { createServer, request } from 'node:http';
import WebSocket, { WebSocketServer } from 'ws';
import { createAppsService } from '../server/index.mjs';
import { migrateAppsSchema, ensureInitialPublication, ensureCanonicalDomain, runtimeTargetDigest, inspectAppsSchema } from '../server/schema.mjs';
import { createPublicationRegistry } from '../server/publications.mjs';
import { createSourceRegistry } from '../server/sources.mjs';
import { createScopedAdmissionRegistry } from '../scoped-embed/admissions.mjs';
import { STANDARD_ROOT_WORKSPACE_SOURCE } from '../../source-app/server/standard-profile.mjs';
import { readPairedNativeEntry, requireNativeEntry, assertNativePortAvailable } from '../server/native-entry.mjs';
import { createNativeEntryGuard } from '../../source-app/server/native-entry-guard.mjs';

const turn = () => new Promise(done => setImmediate(done));
async function until(callback) { const end = Date.now() + 5000; while (!callback()) { assert.ok(Date.now() < end, 'fixture deadline'); await new Promise(done => setTimeout(done, 10)); } }
const ack = target => ({ scope: target.profile === 'soty.relay-restricted.v1' ? 'whole-port' : 'authenticated-workspace',
  targetRevision: target.revision, targetDigest: target.digest, profile: target.profile });
const removal = f => ({ action: 'remove', origin: f.profile.nativeOrigin, targetRevision: 1,
  targetDigest: f.db.prepare('SELECT digest FROM app_runtime_targets WHERE app_id=? AND revision=1').get(f.appId).digest });

function fixture(t, options = {}) {
  const directory = mkdtempSync(join(tmpdir(), 'soty-dual-entry-')), databasePath = join(directory, 'apps.sqlite');
  const db = new DatabaseSync(databasePath); db.exec('PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL');
  migrateAppsSchema(db, { legacyTemplate: 'https://{appId}.apps.test', allowScopedEmbedMigration: true, allowSelectedResourceMigration: true });
  const owner = Object.freeze({ accountId: 'fixture_owner', deviceId: 'fixture_browser' });
  const appId = 'app-' + 'a'.repeat(32), identity = { linkId: 'fixture_link', hostDeviceId: 'fixture_host', connectorId: 'fixture_connector' };
  const connectorKey = Object.values(identity).join('|');
  db.exec('BEGIN IMMEDIATE');
  db.prepare('INSERT INTO app_devices VALUES(?,?,?,?,?)').run(connectorKey, owner.accountId, JSON.stringify(identity), 'Fixture device', 1);
  db.prepare('INSERT INTO local_apps VALUES(?,?,?,?,?,?,?,?,?,?,?)').run(appId, owner.accountId, connectorKey, 'One project', 63345, '/',
    JSON.stringify({ accountIds: [], communityIds: [] }), 'enabled', 1, 1, 1);
  const app = db.prepare('SELECT * FROM local_apps WHERE id=?').get(appId);
  ensureCanonicalDomain(db, app, 'https://{appId}.apps.test'); ensureInitialPublication(db, app);
  const template = 'https://{slug}.apps.test', zone = 'zone_' + createHash('sha256').update(template).digest('hex').slice(0, 32);
  db.prepare('INSERT INTO app_domain_zones VALUES(?,?,?,?,?,?,?)').run(zone, 'named', template, 'apps.test', 'https', '', 1);
  const domains = {};
  for (const [slug, suffix] of [['native', 'b'], ['source', 'c']]) {
    const id = 'dom_' + suffix.repeat(32); domains[slug] = id;
    db.prepare("INSERT INTO app_domains VALUES(?,?,?,?,?,?,?,'alias','bound',?,NULL)").run(id, zone,
      slug + '.apps.test', 'https://' + slug + '.apps.test', slug, appId, owner.accountId, 1);
    db.prepare('INSERT INTO app_publication_domains VALUES(?,?,?)').run(appId, id, owner.accountId);
  }
  db.exec('COMMIT');
  const target = { appId, revision: 2, ownerAccountId: owner.accountId, connectorKey,
    port: options.sourcePort ?? 63346, entryPath: '/embed', profile: 'soty.selected-human-embed.v2' };
  target.digest = runtimeTargetDigest(target);
  const profile = { schema: target.profile, appId, connector: identity, target: { revision: 2, digest: target.digest },
    sourceProfile: { ...STANDARD_ROOT_WORKSPACE_SOURCE }, resource: { registryId: 'soty', tenantId: owner.accountId, appId,
      environmentId: 'test', resourceId: 'workspace', selection: { kind: 'soty.workspace.v1', nativeId: 'fixture_native', incarnationId: 'one' } },
    issuer: 'https://root.test/human-identity', clientId: 'fixture_client', parentOrigin: 'https://root.test',
    embedOrigin: options.embedOrigin ?? 'https://source.apps.test', nativeOrigin: options.nativeOrigin ?? 'https://native.apps.test' };
  let clock = Date.now(), active = true, provider, verifier;
  const authenticate = actor => { assert.equal(active && actor.accountId === owner.accountId && actor.deviceId === owner.deviceId, true); };
  const admissions = createScopedAdmissionRegistry({ db, profiles: [profile], clock: () => clock });
  const publications = createPublicationRegistry({ db, now: () => clock, assertActor: authenticate, canUse: actor => actor.accountId === owner.accountId });
  const probes = [], channel = Object.freeze({ fixture: true }); let sources;
  const open = () => createSourceRegistry({ db, now: () => clock, assertActor: authenticate, publications, scopedAdmission: admissions,
    prepareTarget: async input => { probes.push(input); assert.equal(db.isTransaction, false);
      return provider ? provider(input) : Object.freeze({ target: input.target, channel }); },
    verifyPreparedTarget: input => verifier ? verifier(input) : input.evidence?.target === input.target && input.evidence.channel === channel });
  sources = open(); const cleanups = [];
  t.after(async () => { for (const cleanup of cleanups) await cleanup(); sources.close(); db.close(); assert.equal(join(tmpdir(), directory.slice(tmpdir().length + 1)), directory); rmSync(directory, { recursive: true, force: true }); });
  const get = () => publications.execute({ op: 'apps.publication.get', actor: owner, args: { appId } });
  const prepareArgs = (extra = {}) => ({ appId, expectedPolicyEpoch: get().policyEpoch, expectedTargetRevision: get().activeTargetRevision,
    source: { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, port: target.port, entryPath: '/embed', profile: target.profile },
    nativeEntry: { origin: profile.nativeOrigin, targetRevision: 1 }, ...extra });
  const prepare = extra => sources.execute({ op: 'apps.source.prepare', actor: owner, args: prepareArgs(extra) });
  const intent = (prepared, requestId = 'fixture_pair', extra = {}) => ({ appId, requestId, preparationId: prepared.preparationId,
    expectedPolicyEpoch: prepared.expectedPolicyEpoch, expectedTargetRevision: prepared.expectedTargetRevision,
    launchPolicy: 'anyone', listed: true, exposureAck: ack(prepared.target), nativeExposureAck: ack(prepared.nativeEntry.target), ...(prepared.nativeEntryAction ? { nativeEntryAction: prepared.nativeEntryAction } : {}), ...extra });
  const promote = args => sources.execute({ op: 'apps.source.promote', actor: owner, args });
  const requireEntry = () => requireNativeEntry({ db, appId, origin: profile.nativeOrigin, primaryDigest: target.digest,
    nativeDigest: db.prepare('SELECT digest FROM app_runtime_targets WHERE app_id=? AND revision=1').get(appId).digest, scopedAdmission: admissions });
  const rows = () => Object.fromEntries(['app_publications','app_runtime_targets','app_source_heads','app_source_receipts','app_scoped_embed_admissions']
    .map(name => [name, db.prepare('SELECT * FROM ' + name).all()]));
  return { db, directory, databasePath, appId, owner, identity, target, profile, domains, probes, publications, admissions,
    get, prepare, prepareArgs, intent, promote, requireEntry, rows, clock: () => clock,
    advance(ms) { clock += ms; }, provider(fn) { provider = fn; }, verifier(fn) { verifier = fn; },
    cleanup(fn) { cleanups.push(fn); }, reopen() { sources.close(); sources = open(); } };
}

test('pair independently probes both exact targets and needs a new whole-port ACK before one atomic v2 receipt', async t => {
  const f = fixture(t), before = f.rows(), prepared = await f.prepare();
  assert.deepEqual(f.rows(), before); assert.equal(inspectAppsSchema(f.db), 'v8');
  assert.equal(f.probes.length, 2); assert.notEqual(f.probes[0].target, f.probes[1].target);
  assert.equal(f.probes[0].preparationId, f.probes[1].preparationId);
  const intent = f.intent(prepared);
  for (const extra of [{ nativeExposureAck: undefined }, { nativeExposureAck: ack(prepared.target) },
    { nativeExposureAck: { ...intent.nativeExposureAck, targetDigest: 'f'.repeat(64) } },
    { launchPolicy: 'restricted', listed: false, exposureAck: undefined }]) {
    assert.throws(() => f.promote({ ...intent, ...extra }), /app_native_exposure_ack_required/u);
    assert.deepEqual(f.rows(), before);
  }
  const result = f.promote(intent); assert.equal(result.receipt.schema, 'soty.app-source-receipt.v2');
  assert.equal(result.receipt.nativeEntry.origin, f.profile.nativeOrigin);
  assert.equal(result.current.nativeEntry.origin, f.profile.nativeOrigin);
  assert.equal(result.current.activeTargetRevision, 2); assert.equal(result.current.policyEpoch, 2);
  assert.equal(f.requireEntry().target.revision, 1); assert.equal(f.rows().app_source_receipts.length, 1);
  const saved = f.rows(); f.advance(60000); f.reopen();
  const replay = f.promote(intent); assert.equal(replay.replayed, true); assert.deepEqual(f.rows(), saved);
  assert.throws(() => f.promote({ ...intent, nativeExposureAck: { ...intent.nativeExposureAck, targetRevision: 3 } }),
    { code: 'app_source_request_conflict' });
  assert.deepEqual(f.db.prepare('PRAGMA foreign_key_check').all(), []);
});

test('last overall promotion controls Native entry; historical v2 replay cannot reopen after no-pair promotion or Native rollback', async t => {
  const f = fixture(t), first = await f.prepare(), original = f.intent(first); f.promote(original);
  const next = await f.prepare({ source: undefined, targetRevision: 2, nativeEntry: undefined, nativeEntryAction: removal(f) });
  const removed = f.promote({ ...f.intent({ ...next, nativeEntry: first.nativeEntry }, 'fixture_without_pair'), nativeExposureAck: undefined });
  assert.equal(removed.current.nativeEntry, undefined);
  assert.equal(readPairedNativeEntry(f.db, f.appId), null); assert.throws(f.requireEntry, { code: 'app_native_entry_closed' });
  assert.equal(f.promote(original).replayed, true); assert.equal(readPairedNativeEntry(f.db, f.appId), null);
  const again = await f.prepare({ source: undefined, targetRevision: 2 }); f.promote(f.intent(again, 'fixture_new_pair'));
  assert.equal(f.requireEntry().committedPolicyEpoch, 4);
  const back = await f.prepare({ source: undefined, targetRevision: 1, nativeEntry: undefined, nativeEntryAction: removal(f) });
  f.promote({ ...f.intent({ ...back, nativeEntry: first.nativeEntry }, 'fixture_native_rollback'), nativeExposureAck: undefined });
  assert.equal(readPairedNativeEntry(f.db, f.appId), null); assert.equal(f.get().target.profile, 'soty.relay-restricted.v1');
});

test('an active pair rejects old implicit removal before a probe and requires the exact displayed removal action again at commit', async t => {
  const f = fixture(t), paired = await f.prepare(); f.promote(f.intent(paired));
  const before = f.rows(), probes = f.probes.length;
  await assert.rejects(f.prepare({ source: undefined, targetRevision: 2, nativeEntry: undefined }),
    { code: 'native_pair_action_required', status: 409 });
  await assert.rejects(f.prepare({ source: undefined, targetRevision: 2, nativeEntry: undefined,
    nativeEntryAction: { ...removal(f), origin: 'https://wrong.apps.test' } }), { code: 'native_pair_action_required', status: 409 });
  assert.deepEqual(f.rows(), before); assert.equal(f.probes.length, probes);
  const prepared = await f.prepare({ source: undefined, targetRevision: 2, nativeEntry: undefined, nativeEntryAction: removal(f) });
  assert.deepEqual(prepared.nativeEntryAction, removal(f));
  const intent = { ...f.intent({ ...prepared, nativeEntry: paired.nativeEntry }, 'fixture_explicit_removal'), nativeExposureAck: undefined };
  assert.throws(() => f.promote({ ...intent, nativeEntryAction: undefined }), { code: 'native_pair_action_required', status: 409 });
  assert.throws(() => f.promote({ ...intent, nativeEntryAction: { ...intent.nativeEntryAction, targetDigest: 'e'.repeat(64) } }),
    { code: 'native_pair_action_required', status: 409 });
  assert.deepEqual(f.rows(), before); assert.equal(f.requireEntry().target.revision, 1);
  const result = f.promote(intent);
  assert.equal(result.receipt.schema, 'soty.app-source-receipt.v2'); assert.deepEqual(result.receipt.nativeEntryAction, removal(f));
  assert.equal(result.current.nativeEntry, undefined); assert.equal(readPairedNativeEntry(f.db, f.appId), null);
});

test('current publication, bound aliases, immutable source pin and enable/revoke govern both entries', async t => {
  const f = fixture(t), prepared = await f.prepare(); f.promote(f.intent(prepared));
  const change = (launchPolicy, requestId) => f.publications.execute({ op: 'apps.publication.update', actor: f.owner, args: {
    appId: f.appId, requestId, expectedPolicyEpoch: f.get().policyEpoch, expectedTargetRevision: 2,
    launchPolicy, listed: launchPolicy === 'anyone', activeDomainIds: [f.domains.native, f.domains.source],
    ...(launchPolicy === 'anyone' ? { exposureAck: ack(prepared.target) } : {}) } });
  change('restricted', 'fixture_restrict'); assert.throws(f.requireEntry, { code: 'app_native_entry_closed' });
  change('anyone', 'fixture_republish'); assert.equal(f.requireEntry().policyEpoch, 4);
  f.db.prepare("UPDATE local_apps SET state='revoked' WHERE id=?").run(f.appId);
  assert.throws(f.requireEntry, { code: 'app_native_entry_closed' });
  f.db.prepare("UPDATE local_apps SET state='enabled' WHERE id=?").run(f.appId);
  f.db.prepare('DELETE FROM app_publication_domains WHERE domain_id=?').run(f.domains.source);
  assert.throws(f.requireEntry, { code: 'app_native_entry_domain_required' });
  assert.equal(readPairedNativeEntry(f.db, f.appId).target.port, 63345, 'reserve until explicitly unpaired, even while entry closed');
});

test('pair rejects shared browser hostname, same port, unbound alias, nonhistorical target and a copied ACK before effects', async t => {
  for (const options of [{ embedOrigin: 'https://native.apps.test' }, { sourcePort: 63345 }, { nativeOrigin: 'https://unbound.apps.test' }]) {
    const f = fixture(t, options), before = f.rows();
    await assert.rejects(f.prepare(), error => ['app_native_entry_origin_invalid','app_native_entry_target_invalid','app_native_entry_domain_required'].includes(error.code));
    assert.deepEqual(f.rows(), before); assert.equal(f.probes.length, 0);
  }
  const f = fixture(t), before = f.rows();
  await assert.rejects(f.prepare({ nativeEntry: { origin: f.profile.nativeOrigin, targetRevision: 50 } }), { code: 'apps_source_target_unavailable' });
  await assert.rejects(f.prepare({ nativeEntry: { origin: 'https://source.apps.test', targetRevision: 1 } }), { code: 'app_native_entry_origin_invalid' });
  assert.deepEqual(f.rows(), before);
});

test('both candidate proofs remain mandatory at commit; a paired Native port cannot be reused by another app', async t => {
  const f = fixture(t), prepared = await f.prepare(), intent = f.intent(prepared), before = f.rows();
  f.verifier(input => input.target.revision === 2);
  assert.throws(() => f.promote(intent), { code: 'apps_source_preparation_stale' }); assert.deepEqual(f.rows(), before);
  f.verifier(undefined); f.promote(intent);
  assert.throws(() => assertNativePortAvailable(f.db, 'different-app', f.target.connectorKey, 63345), { code: 'app_port_already_registered' });
  assert.doesNotThrow(() => assertNativePortAvailable(f.db, f.appId, f.target.connectorKey, 63345));
  const noPair = await f.prepare({ source: undefined, targetRevision: 2, nativeEntry: undefined, nativeEntryAction: removal(f) });
  f.promote({ ...f.intent({ ...noPair, nativeEntry: prepared.nativeEntry }, 'fixture_free_native'), nativeExposureAck: undefined });
  assert.doesNotThrow(() => assertNativePortAvailable(f.db, 'different-app', f.target.connectorKey, 63345));
});

async function liveRoot(t, f, factory = createAppsService, { actors = [], profiles = [f.profile] } = {}) {
  const service = factory({ databasePath: f.databasePath, appOriginTemplate: 'https://{appId}.apps.test', shellOrigins: ['https://root.test'],
    allowScopedEmbedMigration: true, allowSelectedResourceMigration: true, scopedEmbedProfiles: profiles, now: f.clock,
    withAuthorityFence: callback => callback(), actorActive: actor => actor?.accountId === f.owner.accountId
      || actors.some(value => value.accountId === actor?.accountId && value.deviceId === actor?.deviceId),
    authenticateConnector: async input => input.token === 'fixture_token',
    signSourceAuthority: () => { throw Error('Native policy must never issue identity witness'); },
    withHumanSubjectAuthority: () => { throw Error('Native policy must never assert Human identity'); } });
  const server = createServer((req, res) => { if (!service.handleRequest(req, res)) res.writeHead(404).end(); });
  const sockets = new Set(); server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  server.on('upgrade', (req, socket, head) => { if (!service.handleUpgrade(req, socket, head)) socket.destroy(); });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  const ws = new WebSocket('ws://127.0.0.1:' + server.address().port + '/api/apps/channel'); ws.on('error', () => {});
  f.cleanup(async () => { try { ws.terminate(); service.close(); } finally {
    for (const socket of sockets) socket.destroy(); server.closeAllConnections(); await new Promise(done => server.close(done)); } });
  let welcome = false; const bound = new Set(), probes = [];
  ws.on('message', bytes => {
    const frame = JSON.parse(bytes);
    if (frame.type === 'ready') welcome = true;
    if (frame.type === 'binding-set') {
      ws.send(JSON.stringify({ type: 'binding-ack', channelId: frame.channelId, syncId: frame.syncId, appId: frame.target.appId,
        revision: frame.target.revision, digest: frame.target.digest, profile: frame.target.profile }));
      bound.add(frame.target.revision);
    }
    if (frame.type === 'target-prepare') { probes.push(frame); ws.send(JSON.stringify({ type: 'target-prepared', channelId: frame.channelId,
      nonce: frame.nonce, appId: frame.target.appId, revision: frame.target.revision, digest: frame.target.digest,
      profile: frame.target.profile, state: 'responding', httpStatus: 200 })); }
  });
  await new Promise(done => ws.once('open', done));
  ws.send(JSON.stringify({ type: 'auth', schema: 'soty.apps-channel.v1', ...f.identity, token: 'fixture_token',
    capabilities: { targetBindingVersions: [1,2], runtimeProfiles: ['soty.relay-restricted.v1', f.profile.schema] }, name: 'Fixture device' }));
  await until(() => welcome); await turn();
  const get = () => service.execute({ op: 'apps.publication.get', actor: f.owner, args: { appId: f.appId } });
  const prepare = extra => service.sourcePreparationExtension.executeAsync({ op: 'apps.source.prepare', actor: f.owner,
    args: Object.fromEntries(Object.entries({ ...f.prepareArgs(), expectedPolicyEpoch: get().policyEpoch,
      expectedTargetRevision: get().activeTargetRevision, ...extra }).filter(([, value]) => value !== undefined)) });
  const promote = args => service.execute({ op: 'apps.source.promote', actor: f.owner,
    args: Object.fromEntries(Object.entries(args).filter(([, value]) => value !== undefined)) });
  const check = (headers = {}, host = 'native.apps.test') => new Promise((done, reject) => {
    const req = request({ hostname: '127.0.0.1', port: server.address().port, path: '/_soty/ingress-check', headers: Object.fromEntries(Object.entries({
      Host: host, 'x-soty-ingress-surface': 'native', 'x-soty-ingress-app': f.appId,
      'x-soty-ingress-primary': f.target.digest,
      'x-soty-ingress-target': f.db.prepare('SELECT digest FROM app_runtime_targets WHERE app_id=? AND revision=1').get(f.appId).digest,
      ...headers }).filter(([, value]) => value !== undefined)) }, res => { const chunks = []; res.on('data', part => chunks.push(part)); res.once('end', () => done({
      status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks).toString('utf8') })); }); req.on('error', reject); req.end();
  });
  return { service, server, ws, probes, get, prepare, promote, check, ready: (revision = 2) => until(() => bound.has(revision)) };
}

test('new directory cards prefer the admitted current Source while exact saved Native links remain unchanged', async t => {
  const f = fixture(t), visitor = Object.freeze({ accountId: 'fixture_visitor', deviceId: 'fixture_visitor_browser' });
  const live = await liveRoot(t, f, createAppsService, { actors: [visitor] }), prepared = await live.prepare();
  live.promote(f.intent(prepared)); await live.ready();
  const call = (actor, op, args = {}) => live.service.execute({ op, actor, args: { ...args, expectedAccountId: actor.accountId } });
  const expected = { appId: f.appId, domainId: f.domains.source, origin: f.profile.embedOrigin, path: '/embed' };
  assert.deepEqual(call(f.owner, 'apps.list').apps.find(value => value.id === f.appId).entry,
    { domainId: f.domains.source, origin: f.profile.embedOrigin, path: '/embed' });
  const legacy = { appId: f.appId, domainId: f.domains.native, origin: f.profile.nativeOrigin, path: '/?draft=kept&tab=old' };
  call(f.owner, 'apps.saved.set', { appId: f.appId, domainId: legacy.domainId, path: legacy.path,
    saved: true, expectedRevision: 0, requestId: 'fixture_save_exact_native' });
  const savedBefore = f.db.prepare('SELECT * FROM app_saved_entries').all();
  for (const actor of [f.owner, visitor]) {
    for (const scope of ['all', 'public', ...(actor === f.owner ? ['mine'] : [])]) {
      assert.deepEqual(call(actor, 'apps.directory.search', { scope }).apps.find(value => value.id === f.appId)?.entry, expected);
    }
    const resolved = call(actor, 'apps.directory.resolve', { appIds: [f.appId] }).items[0];
    assert.equal(resolved.available, true); assert.deepEqual(resolved.app.entry, expected);
  }
  assert.deepEqual(call(f.owner, 'apps.entry.get', { appId: f.appId, domainId: legacy.domainId, path: legacy.path }).entry, legacy);
  const saved = call(f.owner, 'apps.saved.get', { appId: f.appId }).entry;
  assert.equal(saved.domainId, legacy.domainId); assert.equal(saved.origin, legacy.origin); assert.equal(saved.path, legacy.path);
  assert.deepEqual(f.db.prepare('SELECT * FROM app_saved_entries').all(), savedBefore);
});

test('Source directory defaults never bypass public listing, current admission or the exact published Source alias', async t => {
  const f = fixture(t), visitor = Object.freeze({ accountId: 'fixture_visitor', deviceId: 'fixture_visitor_browser' });
  const live = await liveRoot(t, f, createAppsService, { actors: [visitor] }), prepared = await live.prepare();
  live.promote(f.intent(prepared)); await live.ready();
  const call = (actor, op, args = {}) => live.service.execute({ op, actor, args: { ...args, expectedAccountId: actor.accountId } });
  const update = (requestId, listed, activeDomainIds) => call(f.owner, 'apps.publication.update', { appId: f.appId, requestId,
    expectedPolicyEpoch: live.get().policyEpoch, expectedTargetRevision: 2, launchPolicy: 'anyone', listed,
    activeDomainIds, exposureAck: ack(prepared.target) });
  update('fixture_unlist_directory', false, [f.domains.native, f.domains.source]);
  assert.deepEqual(call(visitor, 'apps.directory.search', { scope: 'all' }).apps, []);
  assert.deepEqual(call(f.owner, 'apps.directory.search', { scope: 'public' }).apps, []);
  assert.equal(call(visitor, 'apps.directory.resolve', { appIds: [f.appId] }).items[0].available, false);
  assert.equal(call(f.owner, 'apps.directory.search', { scope: 'mine' }).apps[0].entry.origin, f.profile.embedOrigin);
  update('fixture_restore_directory_listing', true, [f.domains.native, f.domains.source]);
  assert.equal(call(visitor, 'apps.directory.search', { scope: 'public' }).apps[0].entry.origin, f.profile.embedOrigin);
  update('fixture_remove_source_alias', true, [f.domains.native]);
  for (const actor of [f.owner, visitor]) {
    assert.deepEqual(call(actor, 'apps.directory.search').apps, []);
    assert.equal(call(actor, 'apps.directory.resolve', { appIds: [f.appId] }).items[0].available, false);
  }
  update('fixture_restore_source_alias', true, [f.domains.native, f.domains.source]);
  const admittedBefore = f.db.prepare('SELECT * FROM app_scoped_embed_admissions').all();
  live.service.close(); live.ws.terminate();
  const unconfigured = await liveRoot(t, f, createAppsService, { actors: [visitor], profiles: [] });
  for (const actor of [f.owner, visitor]) {
    const execute = (op, args = {}) => unconfigured.service.execute({ op, actor, args: { ...args, expectedAccountId: actor.accountId } });
    assert.deepEqual(execute('apps.directory.search').apps, []);
    assert.equal(execute('apps.directory.resolve', { appIds: [f.appId] }).items[0].available, false);
  }
  assert.deepEqual(f.db.prepare('SELECT * FROM app_scoped_embed_admissions').all(), admittedBefore);
});

test('current Native entry is a closed locator-only read for the latest admitted pair and leaves Source and saved routes exact', async t => {
  const f = fixture(t), visitor = Object.freeze({ accountId: 'fixture_visitor', deviceId: 'fixture_visitor_browser' });
  const live = await liveRoot(t, f, createAppsService, { actors: [visitor] });
  const call = (actor, args) => live.service.execute({ op: 'apps.entry.get', actor, args: { ...args, expectedAccountId: actor.accountId } });
  const args = { appId: f.appId, current: true, surface: 'native' };
  const before = f.rows(); assert.deepEqual(call(f.owner, args), { entry: null }); assert.deepEqual(f.rows(), before);
  assert.throws(() => call(visitor, args), { code: 'app_unavailable', status: 404 });
  for (const invalid of [{ ...args, current: undefined }, { ...args, current: false }, { ...args, surface: 'source' },
    { ...args, domainId: f.domains.native }, { ...args, path: '/' }]) {
    assert.throws(() => call(f.owner, invalid), { code: 'invalid_app_current_entry', status: 400 });
  }
  const prepared = await live.prepare(); live.promote(f.intent(prepared)); await live.ready();
  const after = f.rows(), expected = { appId: f.appId, domainId: f.domains.native, origin: f.profile.nativeOrigin, path: '/' };
  for (const actor of [f.owner, visitor]) assert.deepEqual(call(actor, args), { entry: expected });
  assert.deepEqual(call(f.owner, { appId: f.appId, current: true }).entry,
    { appId: f.appId, domainId: f.domains.source, origin: f.profile.embedOrigin, path: '/embed' });
  const deep = call(f.owner, { appId: f.appId, domainId: f.domains.native, path: '/?draft=keep-native' }).entry;
  assert.deepEqual(deep, { ...expected, path: '/?draft=keep-native' });
  assert.deepEqual(f.rows(), after, 'locator read creates no ticket, source promotion or receipt');
  assert.throws(() => live.service.execute({ op: 'apps.entry.get', actor: visitor,
    args: { ...args, expectedAccountId: f.owner.accountId } }), { code: 'authentication_required', status: 401 });
  const unpaired = await live.prepare({ source: undefined, targetRevision: 2, nativeEntry: undefined, nativeEntryAction: removal(f) });
  live.promote({ ...f.intent({ ...unpaired, nativeEntry: prepared.nativeEntry }, 'fixture_native_locator_unpair'), nativeExposureAck: undefined });
  assert.deepEqual(call(f.owner, args), { entry: null }); assert.deepEqual(call(visitor, args), { entry: null });
});

test('a committed but unavailable Native locator fails closed for current publication, alias, admission and runtime binding', async t => {
  const f = fixture(t), live = await liveRoot(t, f), prepared = await live.prepare(); live.promote(f.intent(prepared)); await live.ready();
  const args = { appId: f.appId, current: true, surface: 'native', expectedAccountId: f.owner.accountId };
  const read = (service = live.service) => service.execute({ op: 'apps.entry.get', actor: f.owner, args });
  const update = (requestId, launchPolicy, domains) => live.service.execute({ op: 'apps.publication.update', actor: f.owner,
    args: { appId: f.appId, requestId, expectedPolicyEpoch: live.get().policyEpoch, expectedTargetRevision: 2,
      launchPolicy, listed: false, activeDomainIds: domains, ...(launchPolicy === 'anyone' ? { exposureAck: ack(prepared.target) } : {}) } });
  update('fixture_native_locator_private', 'restricted', [f.domains.native, f.domains.source]);
  assert.throws(() => read(), { code: 'app_native_entry_closed', status: 403 });
  update('fixture_native_locator_link', 'anyone', [f.domains.native, f.domains.source]);
  assert.equal(read().entry.origin, f.profile.nativeOrigin, 'a known public link does not require public catalog listing');
  update('fixture_native_locator_no_native_alias', 'anyone', [f.domains.source]);
  assert.throws(() => read(), { code: 'app_native_entry_domain_required', status: 403 });
  update('fixture_native_locator_restore_native_alias', 'anyone', [f.domains.native, f.domains.source]);
  f.db.prepare("UPDATE local_apps SET state='revoked' WHERE id=?").run(f.appId);
  assert.throws(() => read(), { code: 'app_unavailable', status: 404 });
  f.db.prepare("UPDATE local_apps SET state='enabled' WHERE id=?").run(f.appId);
  const ended = new Promise(done => live.ws.once('close', done)); live.ws.terminate(); await ended; await turn();
  assert.throws(() => read(), { code: 'app_offline', status: 503 });
  live.service.close();
  const noProfile = await liveRoot(t, f, createAppsService, { profiles: [] });
  assert.throws(() => read(noProfile.service), { code: 'app_scoped_admission_required', status: 503 });
});

test('actual Root HTTP ingress uses current primary binding plus Native receipt, never cookies, frame DTOs or old ingress headers', async t => {
  const f = fixture(t), live = await liveRoot(t, f), prepared = await live.prepare();
  assert.equal(live.probes.length, 2); assert.equal((await live.check()).status, 403);
  live.promote(f.intent(prepared)); await live.ready(); await turn();
  const response = await live.check(); assert.equal(response.status, 200, response.body);
  const policy = JSON.parse(response.body); assert.equal(policy.schema, 'soty.native-entry-policy.v1');
  assert.equal(policy.leaseMs, 1000); assert.equal(policy.policyEpoch, 2); assert.equal(response.headers['set-cookie'], undefined);
  assert.equal((await live.check({}, 'source.apps.test')).status, 403);
  assert.equal((await live.check({ Origin: 'https://source.apps.test' })).status, 403);
  assert.equal((await live.check({ 'x-soty-ingress-primary': 'e'.repeat(64) })).status, 403);
  assert.equal((await live.check({ 'x-soty-ingress-surface': 'unknown' })).status, 403);
  assert.equal((await live.check({ 'x-soty-ingress-surface': undefined })).status, 403, 'old Root ingress contract cannot grant retained Native1 after Source2');
  assert.throws(() => live.service.execute({ op: 'apps.register', actor: f.owner, args: { hostDeviceId: f.identity.hostDeviceId,
    connectorId: f.identity.connectorId, port: 63345, entryPath: '/', name: 'Copied Native', grants: { accountIds: [], communityIds: [] } } }),
  { code: 'app_port_already_registered' });
  const next = await live.prepare({ source: undefined, targetRevision: 2, nativeEntry: undefined, nativeEntryAction: removal(f) });
  live.promote({ ...f.intent({ ...next, nativeEntry: prepared.nativeEntry }, 'fixture_live_unpair'), nativeExposureAck: undefined });
  assert.equal((await live.check()).status, 403);
  assert.equal(live.promote(f.intent(prepared)).replayed, true); assert.equal((await live.check()).status, 403);
});

function nativeGuard(f, live, extra = {}) {
  return createNativeEntryGuard({ rootCheckUrl: 'http://127.0.0.1:' + live.server.address().port + '/_soty/ingress-check',
    appId: f.appId, origin: f.profile.nativeOrigin, primaryTargetDigest: f.target.digest,
    nativeTargetDigest: f.db.prepare('SELECT digest FROM app_runtime_targets WHERE app_id=? AND revision=1').get(f.appId).digest, ...extra });
}

test('real HTTP/WS Native guard stops active streams and commands after Source unpair, with an explicit bounded race window', async t => {
  const f = fixture(t), live = await liveRoot(t, f), prepared = await live.prepare(); live.promote(f.intent(prepared)); await live.ready(); await turn();
  const guard = nativeGuard(f, live), originalLease = await guard.acquire();
  assert.throws(() => guard.assertCurrent(JSON.parse(JSON.stringify(originalLease))), { code: 'native_entry_lease_closed' });
  let effects = 0, dataFrames = 0;
  const sockets = new Set(), wss = new WebSocketServer({ noServer: true });
  const native = createServer((req, res) => {
    void (async () => {
      const lease = await guard.acquire();
      // Real apps separately authorize their Native Human/session/CSRF here.
      guard.assertCurrent(lease); effects++;
      const final = await guard.acquire();
      assert.equal(final.policyEpoch, lease.policyEpoch); guard.assertCurrent(final);
      res.setHeader('content-type', 'application/json'); res.end(JSON.stringify({ effects }));
    })().catch(() => { if (!res.headersSent) res.writeHead(403).end(); else res.destroy(); });
  });
  native.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  native.on('upgrade', (req, socket, head) => {
    void (async () => {
      let ws, timer;
      const stream = await guard.openStream({ onClose: () => { clearInterval(timer); ws?.close(1008, 'app policy closed'); } });
      wss.handleUpgrade(req, socket, head, current => {
        ws = current;
        const push = value => { stream.assertCurrent(); ws.send(JSON.stringify(value)); };
        timer = setInterval(() => { try { push({ type: 'data' }); dataFrames++; } catch { stream.close(); } }, 25);
        ws.on('message', () => { void (async () => { await stream.beforeEffect(); stream.assertCurrent(); effects++;
          push({ type: 'command', effects }); })().catch(() => stream.close()); });
        ws.once('close', () => { clearInterval(timer); stream.close(); });
      });
    })().catch(() => { if (!socket.destroyed) socket.end('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n'); });
  });
  await new Promise(done => native.listen(0, '127.0.0.1', done));
  f.cleanup(async () => { guard.close(); wss.close(); for (const socket of sockets) socket.destroy(); await new Promise(done => native.close(done)); });
  const http = () => new Promise((done, reject) => { const req = request({ hostname: '127.0.0.1', port: native.address().port, path: '/', method: 'POST' },
    res => { res.resume(); res.once('end', () => done(res.statusCode)); }); req.on('error', reject); req.end(); });
  assert.equal(await http(), 200); assert.equal(effects, 1);
  const ws = new WebSocket('ws://127.0.0.1:' + native.address().port + '/ws'); ws.on('error', () => {});
  await new Promise((done, reject) => { ws.once('open', done); ws.once('error', reject); });
  await until(() => dataFrames >= 2);
  ws.send('synthetic Native command'); await until(() => effects === 2);
  const next = await live.prepare({ source: undefined, targetRevision: 2, nativeEntry: undefined, nativeEntryAction: removal(f) });
  const revokedAt = performance.now(), closed = new Promise(done => ws.once('close', done));
  live.promote({ ...f.intent({ ...next, nativeEntry: prepared.nativeEntry }, 'fixture_guard_unpair'), nativeExposureAck: undefined });
  assert.equal(await http(), 403); const effectsAfterRevoke = effects;
  await closed; assert.ok(performance.now() - revokedAt < 1250, 'active WS closes within 1s lease + scheduler allowance');
  const framesAtClose = dataFrames; await new Promise(done => setTimeout(done, 60));
  assert.equal(effects, effectsAfterRevoke); assert.equal(dataFrames, framesAtClose);
  await assert.rejects(guard.openStream({ onClose() {} }), { code: 'native_entry_policy_denied' });
  // The API is honest about non-atomic databases: a pre-revoke lease remains
  // usable only until its deadline. It is not a synchronous Root revocation RPC.
  await new Promise(done => setTimeout(done, 1050));
  assert.throws(() => guard.assertCurrent(originalLease), { code: 'native_entry_lease_closed' });
});

test('Native stream fails closed on paired policy epoch changes, Root outage and unbranded or expired leases', async t => {
  const f = fixture(t), live = await liveRoot(t, f), prepared = await live.prepare(); live.promote(f.intent(prepared)); await live.ready(); await turn();
  const guard = nativeGuard(f, live, { leaseMs: 200, heartbeatMs: 50, timeoutMs: 100 }); f.cleanup(() => guard.close());
  let reason, closed;
  const ended = new Promise(done => { closed = done; });
  const stream = await guard.openStream({ onClose: value => { reason = value; closed(); } });
  live.service.execute({ op: 'apps.publication.update', actor: f.owner, args: { appId: f.appId, requestId: 'fixture_new_epoch',
    expectedPolicyEpoch: live.get().policyEpoch, expectedTargetRevision: 2, launchPolicy: 'anyone', listed: false,
    activeDomainIds: [f.domains.native, f.domains.source], exposureAck: ack(prepared.target) } });
  await ended; assert.equal(reason, 'native_entry_policy_changed'); assert.throws(stream.assertCurrent, { code: 'native_entry_lease_closed' });
  let closedAgain; const endedAgain = new Promise(done => { closedAgain = done; });
  const current = await guard.openStream({ onClose: () => closedAgain() });
  live.ws.terminate(); await endedAgain; assert.throws(current.assertCurrent, { code: 'native_entry_lease_closed' });
  assert.throws(() => nativeGuard(f, live, { rootCheckUrl: 'http://untrusted.test/_soty/ingress-check' }), { code: 'native_entry_configuration_invalid' });
  assert.throws(() => nativeGuard(f, live, { leaseMs: 1001 }), { code: 'native_entry_configuration_invalid' });
});

test('current reader restarts from the durable pair; actual preceding Root ingress fails closed and its v1 promotion unpairs a new reader', async t => {
  const f = fixture(t), prepared = await f.prepare(), pairedIntent = f.intent(prepared); f.promote(pairedIntent);
  const current = await liveRoot(t, f); await current.ready(); await turn();
  assert.equal((await current.check()).status, 200);
  current.service.close(); current.ws.terminate();
  const restarted = await liveRoot(t, f); await restarted.ready(); await turn();
  assert.equal((await restarted.check()).status, 200);
  assert.equal(restarted.service.execute({ op: 'apps.publication.get', actor: f.owner, args: { appId: f.appId } }).nativeEntry.runtimeReady, false);

  // Load the exact preceding ingress/source code beside its unchanged module
  // dependencies. No production checkout, config or historical row is edited.
  const repository = fileURLToPath(new URL('../../../', import.meta.url));
  const directory = fileURLToPath(new URL('../server/', import.meta.url)), tag = 'legacy-dual-fixture-' + Date.now();
  const indexPath = join(directory, tag + '-index.mjs'), sourcesPath = join(directory, tag + '-sources.mjs');
  const historical = path => execFileSync('git', ['show', '1487cb96:' + path], { cwd: repository, maxBuffer: 1048576, windowsHide: true }).toString('utf8');
  writeFileSync(sourcesPath, historical('modules/apps/server/sources.mjs'));
  writeFileSync(indexPath, historical('modules/apps/server/index.mjs').replace("'./sources.mjs'", "'./" + tag + "-sources.mjs'"));
  f.cleanup(() => { rmSync(indexPath, { force: true }); rmSync(sourcesPath, { force: true }); });
  const previous = await import(pathToFileURL(indexPath).href), old = await liveRoot(t, f, previous.createAppsService);
  await old.ready(); await turn();
  assert.equal((await old.check()).status, 403, 'old Root cannot admit retained Native1');
  assert.ok([401,403].includes((await old.check({ 'x-soty-ingress-target': f.target.digest })).status), 'substituting primary pin cannot obtain a Native policy body');
  const preparedOld = await old.prepare({ source: undefined, targetRevision: 2, nativeEntry: undefined });
  const result = old.promote({ ...f.intent({ ...preparedOld, nativeEntry: prepared.nativeEntry }, 'fixture_old_root_v1'), nativeExposureAck: undefined });
  assert.equal(result.receipt.schema, 'soty.app-source-receipt.v1');
  assert.equal((await restarted.check()).status, 403, 'latest v1 receipt closes Native on the new reader too');
  assert.equal(f.promote(pairedIntent).replayed, true); assert.equal((await restarted.check()).status, 403);
});

test('a Root wall clock 8 seconds ahead cannot lengthen or prematurely expire a local monotonic Native lease', async t => {
  const f = fixture(t), prepared = await f.prepare(); f.promote(f.intent(prepared));
  f.advance(8000);
  const live = await liveRoot(t, f); await live.ready(); await turn();
  const guard = nativeGuard(f, live); f.cleanup(() => guard.close());
  const started = performance.now(), lease = await guard.acquire();
  guard.assertCurrent(lease);
  await new Promise(done => setTimeout(done, 100)); guard.assertCurrent(lease);
  await new Promise(done => setTimeout(done, Math.max(1, 1010 - (performance.now() - started))));
  assert.throws(() => guard.assertCurrent(lease), { code: 'native_entry_lease_closed' });
});

test('optional Python SDK speaks the same actual Root policy wire and closes its active stream after explicit removal',
  { skip: !process.env.SOTY_NATIVE_TEST_PYTHON, timeout: 15000 }, async t => {
    const f = fixture(t), live = await liveRoot(t, f), prepared = await live.prepare(); live.promote(f.intent(prepared)); await live.ready(); await turn();
    const python = spawn(process.env.SOTY_NATIVE_TEST_PYTHON, [fileURLToPath(new URL('../../source-app/python/root_native_fixture.py', import.meta.url))],
      { windowsHide: true, stdio: ['pipe','pipe','pipe'] });
    const rows = []; let pending = '', stderr = '';
    python.stdout.on('data', chunk => { pending += chunk; let index; while ((index = pending.indexOf('\n')) >= 0) {
      rows.push(JSON.parse(pending.slice(0, index))); pending = pending.slice(index + 1); } });
    python.stderr.on('data', chunk => { stderr = (stderr + chunk).slice(-2048); });
    const exited = new Promise(done => python.once('exit', code => done(code)));
    f.cleanup(async () => { if (python.exitCode === null) python.kill(); await exited; });
    python.stdin.end(JSON.stringify({ rootCheckUrl: 'http://127.0.0.1:' + live.server.address().port + '/_soty/ingress-check',
      appId: f.appId, origin: f.profile.nativeOrigin, primaryTargetDigest: f.target.digest,
      nativeTargetDigest: prepared.nativeEntry.target.digest }) + '\n');
    await until(() => rows.find(value => value.type === 'ready') || rows.find(value => value.type === 'error'));
    assert.equal(rows[0].type, 'ready', stderr); assert.equal(rows[0].policyEpoch, 2);
    const next = await live.prepare({ source: undefined, targetRevision: 2, nativeEntry: undefined, nativeEntryAction: removal(f) });
    live.promote({ ...f.intent({ ...next, nativeEntry: prepared.nativeEntry }, 'fixture_python_unpair'), nativeExposureAck: undefined });
    assert.equal(await exited, 0, stderr); assert.equal(rows.at(-1).type, 'finished');
    assert.equal(rows.at(-1).closed, true); assert.ok(rows.some(value => value.type === 'closed'));
  });

test('Native-only public policy requires the CURRENT exact whole-port ACK, active alias and primary binding, and survives reader restart', async t => {
  const f = fixture(t), live = await liveRoot(t, f), target = f.get().target;
  await live.ready(1); await turn();
  const check = extra => live.check({ 'x-soty-ingress-primary': target.digest, ...extra });
  assert.equal((await check()).status, 403, 'restricted Native-only entry has no actor-capable public lease');
  f.publications.execute({ op: 'apps.publication.update', actor: f.owner, args: { appId: f.appId, requestId: 'fixture_native_primary_public',
    expectedPolicyEpoch: 1, expectedTargetRevision: 1, launchPolicy: 'anyone', listed: true,
    activeDomainIds: [f.domains.native], exposureAck: ack(target) } });
  const response = await check(); assert.equal(response.status, 200, response.body);
  assert.equal(JSON.parse(response.body).primaryTargetRevision, 1); assert.equal(JSON.parse(response.body).nativeTargetRevision, 1);
  assert.equal((await check({ 'x-soty-ingress-primary': f.target.digest })).status, 403, 'a former Source pin cannot silently become primary Native');
  f.db.prepare('UPDATE app_publications SET exposure_ack_json=? WHERE app_id=?').run(JSON.stringify({ ...ack(target), scope: 'authenticated-workspace' }), f.appId);
  assert.equal((await check()).status, 403);
  f.db.prepare('UPDATE app_publications SET exposure_ack_json=? WHERE app_id=?').run(JSON.stringify(ack(target)), f.appId);
  const restarted = await liveRoot(t, f); await restarted.ready(1); await turn();
  assert.equal((await restarted.check({ 'x-soty-ingress-primary': target.digest })).status, 200);
  const guard = nativeGuard(f, restarted, { primaryTargetDigest: target.digest }); f.cleanup(() => guard.close());
  let close; const ended = new Promise(done => { close = done; });
  const stream = await guard.openStream({ onClose: close });
  f.publications.execute({ op: 'apps.publication.update', actor: f.owner, args: { appId: f.appId, requestId: 'fixture_native_primary_restrict',
    expectedPolicyEpoch: 2, expectedTargetRevision: 1, launchPolicy: 'restricted', listed: false, activeDomainIds: [f.domains.native] } });
  await ended; assert.throws(stream.assertCurrent, { code: 'native_entry_lease_closed' });
  assert.equal((await check()).status, 403, 'the retained publication receipt is not current ACK authority');
});

test('explicit Native-primary rollback closes the paired stream; only a new exact Native-primary guard configuration can reopen', async t => {
  const f = fixture(t), live = await liveRoot(t, f), prepared = await live.prepare(); live.promote(f.intent(prepared)); await live.ready(); await turn();
  const paired = nativeGuard(f, live); f.cleanup(() => paired.close());
  let ended; const closed = new Promise(done => { ended = done; });
  const oldStream = await paired.openStream({ onClose: ended });
  const native = new WebSocketServer({ host: '127.0.0.1', port: 0 });
  await new Promise(done => native.once('listening', done));
  native.on('connection', socket => { void paired.openStream({ onClose: () => socket.close(1008, 'app policy changed') })
    .then(stream => { socket.once('close', () => stream.close()); socket.send('ready under the original pair'); }, () => socket.close(1008)); });
  const browser = new WebSocket('ws://127.0.0.1:' + native.address().port); browser.on('error', () => {});
  await new Promise((done, reject) => { browser.once('message', done); browser.once('error', reject); });
  const browserClosed = new Promise(done => browser.once('close', done));
  f.cleanup(async () => { browser.terminate(); for (const socket of native.clients) socket.terminate(); await new Promise(done => native.close(done)); });
  const rollback = await live.prepare({ source: undefined, targetRevision: 1, nativeEntry: undefined, nativeEntryAction: removal(f) });
  live.promote({ ...f.intent({ ...rollback, nativeEntry: prepared.nativeEntry }, 'fixture_guard_native_primary_rollback'), nativeExposureAck: undefined });
  await Promise.all([closed, browserClosed]); assert.throws(oldStream.assertCurrent, { code: 'native_entry_lease_closed' });
  await assert.rejects(paired.acquire(), { code: 'native_entry_policy_denied' });
  await live.ready(1); await turn();
  const ordinary = nativeGuard(f, live, { primaryTargetDigest: rollback.target.digest }); f.cleanup(() => ordinary.close());
  const current = await ordinary.acquire(); ordinary.assertCurrent(current);
  assert.equal(current.primaryTargetDigest, current.nativeTargetDigest);
  f.db.prepare("UPDATE local_apps SET state='revoked' WHERE id=?").run(f.appId);
  await assert.rejects(ordinary.acquire(), { code: 'native_entry_policy_denied' });
});

