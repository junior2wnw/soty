import { createScopedSlotRenewal } from "../../../src/world/app-scoped-renewal.mjs";
import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer, request } from 'node:http';
import WebSocket from 'ws';
import { createAppsService } from '../server/index.mjs';
import { STANDARD_SELECTED_SOURCE_V3, STANDARD_WORKSPACE_SOURCE, STANDARD_ROOT_WORKSPACE_SOURCE } from '../../source-app/server/standard-profile.mjs';
import { preferredInspectionEntry } from '../../../src/world/app-deployment.mjs';
import { validateAppEntry, parseAppLaunchRoute } from '../../../src/world/app-launch.mjs';
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
      ...(profileVersion === 2 ? { selection: { kind: options.selectionKind ?? (options.pin?.id === 'soty.standard-workspace' ? 'soty.workspace.v1' : 'hive.project.v1'), nativeId: 'synthetic_native', incarnationId: 'one' } } : { workspaceId: 'selected' }) },
    issuer: 'https://root.test/human-identity', clientId: 'synthetic-selected', embedOrigin: options.embedOrigin ?? (options.publicEntry ? 'https://selected.apps.test' : options.template?.replace('{appId}', appId) ?? 'https://selected.apps.test'),
    nativeOrigin: options.nativeOrigin ?? 'https://native.test', parentOrigin: 'https://root.test' };
  if (options.publicEntry) {
    const template = 'https://{slug}.apps.test', zone = 'zone_' + createHash('sha256').update(template).digest('hex').slice(0, 32);
    const alias = 'dom_' + 'b'.repeat(32);
    db.prepare('INSERT INTO app_domain_zones VALUES(?,?,?,?,?,?,?)').run(zone, 'named', template, 'apps.test', 'https', '', 1);
    db.prepare("INSERT INTO app_domains VALUES(?,?,?,?,?,?,?,'alias','bound',?,NULL)").run(alias, zone, 'selected.apps.test', 'https://selected.apps.test', 'selected', appId, owner.accountId, 1);
    db.prepare('INSERT INTO app_publication_domains VALUES(?,?,?)').run(appId, alias, owner.accountId);
  }
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
  return { db, publications, admissions, target, appId, profile, owner, identity, get, rows, prepare, intent, promote, probeCalls: () => probeCalls,
    reopen() { sources.close(); sources = openSources(); }, revoke() { active = false; }, advance() { clock += 60000; },
    notify(work) { notify = work; }, history() { return sources.execute({ op: 'apps.source.history', actor: owner, args: { appId } }); } };
}

test('admitted workspace cannot turn a malformed Native origin into a CSP wildcard or directive', async t => {
  for (const nativeOrigin of ['https://*.example.test', 'https://*', 'https://native.test;connect-src']) await t.test(nativeOrigin, async sub => {
    const directory = mkdtempSync(join(tmpdir(), 'soty-native-policy-'));
    const databasePath = join(directory, 'apps.sqlite'), template = 'https://{appId}.apps.test';
    const f = fixture(sub, 8, 2, { databasePath, template, pin: STANDARD_ROOT_WORKSPACE_SOURCE, nativeOrigin });
    f.promote(f.intent(await f.prepare()));
    const service = createAppsService({ databasePath, appOriginTemplate: template, shellOrigins: ['https://root.test'],
      allowScopedEmbedMigration: true, allowSelectedResourceMigration: true, scopedEmbedProfiles: [f.profile],
      withAuthorityFence: callback => callback(), actorActive: () => true,
      signSourceAuthority: async () => { throw Error('malformed_origin_must_not_issue_witness'); },
      withHumanSubjectAuthority: () => { throw Error('malformed_origin_must_not_resolve_identity'); } });
    const server = createServer((req, res) => { if (!service.handleRequest(req, res)) res.writeHead(404).end(); });
    await new Promise(done => server.listen(0, '127.0.0.1', done));
    sub.after(async () => { service.close(); server.closeAllConnections(); await new Promise(done => server.close(done)); rmSync(directory, { recursive: true, force: true }); });
    const response = await new Promise((done, reject) => {
      const req = request({ hostname: '127.0.0.1', port: server.address().port, path: '/embed', headers: { Host: new URL(f.profile.embedOrigin).host } }, res => {
        let body = ''; res.on('data', chunk => { body += chunk; }); res.once('end', () => done({ status: res.statusCode, body, policy: res.headers['content-security-policy'] }));
      }); req.once('error', reject); req.end();
    });
    assert.equal(response.status, 403);
    assert.equal(JSON.parse(response.body).error, 'app_native_origin_invalid');
    assert.equal(response.policy, undefined);
  });
});

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
  for (const pin of [STANDARD_ROOT_WORKSPACE_SOURCE, STANDARD_WORKSPACE_SOURCE, STANDARD_SELECTED_SOURCE_V3, HIVE_SELECTED_SOURCE]) await t.test(pin.id + ':' + pin.version, async sub => {
    const rootWorkspace = pin === STANDARD_ROOT_WORKSPACE_SOURCE;
    const directory = mkdtempSync(join(tmpdir(), 'soty-saved-entry-')), databasePath = join(directory, 'apps.sqlite');
    const template = 'https://{appId}.apps.test', f = fixture(sub, 8, 2, { databasePath, template, pin, publicEntry: true,
      ...(pin === STANDARD_SELECTED_SOURCE_V3 ? { selectionKind: 'soty.resource.v1' } : {}),
      embedOrigin: [STANDARD_ROOT_WORKSPACE_SOURCE, HIVE_SELECTED_SOURCE].includes(pin) ? 'https://selected.apps.test' : template.replace('{appId}', 'app-' + 'a'.repeat(32)) });
    const prepared = await f.prepare(), intent = f.intent(prepared);
    if (rootWorkspace) Object.assign(intent, { launchPolicy: 'anyone', listed: true, exposureAck: {
      scope: 'authenticated-workspace', targetRevision: f.target.revision, targetDigest: f.target.digest, profile: f.target.profile } });
    f.promote(intent); const before = f.rows();
    const outsider = Object.freeze({ accountId: 'new-ungranted-person', deviceId: 'new-person-browser' });
    const reader = Object.freeze({ accountId: 'synthetic_reader', deviceId: 'reader-browser' });
    const actors = [f.owner, reader, outsider]; let humanSubjects = [], forwardedRequests = 0;
    let active = true, clock=Date.now();
    const service = createAppsService({ databasePath, appOriginTemplate: template, shellOrigins: ['https://root.test'],
      allowScopedEmbedMigration: true, allowSelectedResourceMigration: true, scopedEmbedProfiles: [f.profile], now:()=>clock,
      withAuthorityFence: callback => callback(),
      actorActive: actor => active && actors.some(value => value.accountId === actor?.accountId && value.deviceId === actor?.deviceId),
      authenticateConnector: async auth => auth.token === 'synthetic-channel-token' && auth.linkId === f.identity.linkId
        && auth.deviceId === f.identity.hostDeviceId && auth.connectorId === f.identity.connectorId,
      signSourceAuthority: async () => { throw Error('launch_does_not_issue_identity_witness'); },
      withHumanSubjectAuthority: ({ actor, issuer, clientId }, callback) => {
        assert.ok(actors.includes(actor)); assert.ok(active); humanSubjects.push(actor.accountId); return callback({ issuer, clientId, subject: 'subject-' + actor.accountId, clientGeneration: 1, clientProfileDigest: 'c'.repeat(64) }); } });
    const peers = new Set(), server = createServer((req, res) => { if (!service.handleRequest(req, res)) { res.writeHead(404).end(); } }); server.on('connection', socket => { peers.add(socket); socket.once('close', () => peers.delete(socket)); }); server.on('upgrade', (req, socket, head) => { if (!service.handleUpgrade(req, socket, head)) socket.destroy(); });
    await new Promise(done => server.listen(0, '127.0.0.1', done));
    const ws = new WebSocket(`ws://127.0.0.1:${server.address().port}/api/apps/channel`);
    sub.after(async () => { ws.terminate(); service.close(); for (const peer of peers) peer.destroy(); await new Promise(done => server.close(done)); rmSync(directory, { recursive: true, force: true }); });
    ws.on('error', () => {});
    // Actual channel with a controlled connector ACK, not an upstream health assertion.
    const streams=new Map(),seen=[];let holdNext=false,heldReply;
    ws.on('message', bytes => { const frame = JSON.parse(bytes); if (frame.type === 'open') forwardedRequests++;
      if(frame.type==='bound-open'){forwardedRequests++;streams.set(frame.id,frame);seen.push(frame);}
      if(frame.type==='data')ws.send(JSON.stringify({type:'ack',id:frame.id,seq:frame.seq}));
      if(frame.type==='end'&&streams.has(frame.id)){
        const current=streams.get(frame.id),continued=current.path==='/api/embed/session-continue';
        const ack={ready:true,renewable:true,sessionExpiresAt:current.context.expiresAt,accessExpiresAt:current.context.expiresAt,receiptDigest:'d'.repeat(64)};
        const body=continued?{schema:'soty.source-session-continuation.v1',...ack}:{instanceId:current.context.instanceId,reference:current.context.reference.id,path:current.path};
        const reply=()=>{ws.send(JSON.stringify({type:'head',id:frame.id,status:200,headers:{'content-type':'application/json'},...(continued?{auth:{kind:'continued',ack}}:{})}));
          ws.send(JSON.stringify({type:'data',id:frame.id,seq:1,data:Buffer.from(JSON.stringify(body)).toString('base64')}));};
        if(holdNext){holdNext=false;heldReply=reply;}else reply();
      }
      if(frame.type==='ack'&&streams.has(frame.id)){ws.send(JSON.stringify({type:'end',id:frame.id}));streams.delete(frame.id);}
      if (frame.type === 'binding-set') ws.send(JSON.stringify({ type: 'binding-ack', channelId: frame.channelId, syncId: frame.syncId,
        appId: frame.target.appId, revision: frame.target.revision, digest: frame.target.digest, profile: frame.target.profile })); });
    await new Promise((done, reject) => { ws.once('open', done); ws.once('error', reject); });
    ws.send(JSON.stringify({ type: 'auth', schema: 'soty.apps-channel.v1', ...f.identity, token: 'synthetic-channel-token',
      capabilities: { targetBindingVersions: [1, 2], runtimeProfiles: ['soty.relay-restricted.v1', f.profile.schema] } }));
    const inspection = service.execute({ op: 'apps.inspect', actor: f.owner, args: { appId: f.appId } });
    const preferred = preferredInspectionEntry(inspection);
    assert.equal(preferred.origin, f.profile.embedOrigin, 'ordinary Open chooses the admitted origin, not an arbitrary active alias');
    assert.deepEqual(service.execute({ op: 'apps.list', actor: f.owner }).apps[0].entry, preferred, 'card and settings use the same Source projection');
    const resolved = service.execute({ op: 'apps.entry.get', actor: f.owner, args: { appId: f.appId, domainId: preferred.domainId } }).entry;
    assert.deepEqual(validateAppEntry(resolved, { appId: f.appId, domainId: preferred.domainId }, f.profile.parentOrigin), { appId: f.appId, ...preferred });
    const call = args => service.execute({ op: 'apps.launch', actor: f.owner, args: { appId: f.appId, instanceId: 'main', domainId: preferred.domainId, ...args } });
    const deadline = Date.now() + 3000; let initial;
    while (!initial) { try { initial = call({ path: '/embed' }); } catch (error) {
      if (!['app_offline', 'app_binding_pending'].includes(error.code) || Date.now() >= deadline) throw error;
      await new Promise(done => setTimeout(done, 5)); } }
    assert.equal(initial.entry.origin, f.profile.embedOrigin);
    const beforeDefaultOpen = f.rows(), forwardedBeforeDefault = forwardedRequests;
    const ordinary = service.execute({ op: 'apps.launch', actor: f.owner, args: { appId: f.appId, instanceId: 'main' } });
    assert.deepEqual(ordinary.entry, { appId: f.appId, ...preferred }, 'app-only launch uses the admitted current address before issuing its boot');
    assert.deepEqual(service.execute({ op: 'apps.entry.get', actor: f.owner, args: { appId: f.appId } }).entry,
      ordinary.entry, 'cold app-only discussion uses the same current selector');
    assert.deepEqual(f.rows(), beforeDefaultOpen, 'opening and resolving an app do not rewrite bookmarks, grants or publication state');
    assert.equal(forwardedRequests, forwardedBeforeDefault, 'issuing the default boot never replays an upstream request');
    const subjectsBeforeDenial = humanSubjects.length;
    assert.throws(() => service.execute({ op: 'apps.launch', actor: { ...f.owner, deviceId: 'untrusted-browser' },
      args: { appId: f.appId } }), { code: 'apps_authentication_required' });
    assert.throws(() => service.execute({ op: 'apps.launch', actor: f.owner,
      args: { appId: f.appId, expectedAccountId: reader.accountId } }), { code: 'authentication_required' });
    assert.equal(humanSubjects.length, subjectsBeforeDenial, 'stale profile/device cannot acquire a scoped identity');
    assert.deepEqual(f.rows(), beforeDefaultOpen);
    const wrongAddress = inspection.addresses.canonical.origin === preferred.origin ? inspection.addresses.aliases[0] : inspection.addresses.canonical;
    assert.throws(() => call({ domainId: wrongAddress.id, path: '/embed' }), { code: 'scoped_embed_source_changed' }, 'the other same-app address is still fenced');
    assert.throws(() => call({ domainId: wrongAddress.id }), { code: 'scoped_embed_source_changed' }, 'explicit domain without a path remains exact');
    if (inspection.addresses.canonical.origin !== preferred.origin) assert.throws(() => service.execute({ op: 'apps.launch', actor: f.owner,
      args: { appId: f.appId, path: '/embed' } }), { code: 'scoped_embed_source_changed' }, 'an explicit path without domain keeps its previous canonical meaning');
    for (const actor of [f.owner, reader]) {
      const current = service.execute({ op: 'apps.entry.get', actor, args: { appId: f.appId, current: true } });
      assert.deepEqual(current.entry, { appId: f.appId, ...preferred }, 'explicit current lookup resolves the admitted origin for an allowed participant');
    }
    const currentOutside = () => service.execute({ op: 'apps.entry.get', actor: outsider, args: { appId: f.appId, current: true } });
    if (rootWorkspace) assert.deepEqual(currentOutside().entry, { appId: f.appId, ...preferred }, 'public workspace still requires a fresh access decision');
    else assert.throws(currentOutside, { code: 'app_unavailable' }, 'current lookup leaks no address to an ungranted participant');
    assert.throws(() => service.execute({ op: 'apps.entry.get', actor: f.owner,
      args: { appId: f.appId, current: true, domainId: wrongAddress.id } }), { code: 'invalid_app_current_entry' });
    assert.throws(() => service.execute({ op: 'apps.entry.get', actor: f.owner,
      args: { appId: f.appId, current: true, path: '/embed' } }), { code: 'invalid_app_current_entry' });
    assert.throws(() => service.execute({ op: 'apps.entry.get', actor: f.owner,
      args: { appId: f.appId, current: false } }), { code: 'invalid_app_current_entry' });
    const exactOld = service.execute({ op: 'apps.entry.get', actor: f.owner,
      args: { appId: f.appId, domainId: wrongAddress.id, path: '/embed' } });
    assert.equal(exactOld.entry.domainId, wrongAddress.id, 'ordinary exact lookup never silently substitutes an origin');
    assert.throws(() => call({ domainId: wrongAddress.id, path: '/embed' }), { code: 'scoped_embed_source_changed' }, 'metadata recovery does not weaken launch admission');
    const recover = async (navigate = false) => new Promise((done, reject) => {
      const req = request({ hostname: '127.0.0.1', port: server.address().port, path: '/embed',
        headers: { Host: new URL(wrongAddress.origin).host, Accept: 'text/html',
          ...(navigate ? { 'sec-fetch-mode': 'navigate', 'sec-fetch-dest': 'document' } : {}) } }, res => {
        const chunks=[];res.on('data',chunk=>chunks.push(chunk));res.once('end',()=>done({status:res.statusCode,headers:res.headers,body:Buffer.concat(chunks).toString()})); });
      req.once('error',reject);req.end();
    });
    const recoveryUrl = new URL('/', f.profile.parentOrigin);recoveryUrl.hash = `launch/${f.appId}/${preferred.domainId}?${new URLSearchParams({path:'/embed'})}`;
    const refusal = await recover();assert.ok([401,403].includes(refusal.status));
    const redirect = await recover(true);assert.equal(redirect.status,302);
    // Follow the actual recovery target through the same Apps launch port used by Root.
    const recovered = parseAppLaunchRoute(new URL(redirect.headers.location).hash);
    assert.ok(recovered);
    let followed;
    assert.doesNotThrow(() => { followed = service.execute({ op: 'apps.launch', actor: f.owner,
      args: { ...recovered.target, instanceId: 'main' } }); }, 'recovery must launch the admitted Source without another origin error');
    assert.equal(followed.entry.origin, f.profile.embedOrigin);assert.equal(followed.entry.domainId, preferred.domainId);
    assert.equal(followed.entry.path, '/embed');
    if (preferred.domainId !== inspection.addresses.canonical.id) {
      assert.throws(() => service.execute({ op: 'apps.launch', actor: f.owner,
        args: { appId: f.appId, domainId: inspection.addresses.canonical.id, path: '/embed', instanceId: 'main' } }),
      { code: 'scoped_embed_source_changed' }, 'recovery never admits the unrelated canonical origin');
    }
    assert.ok(refusal.body.includes(recoveryUrl.href.replaceAll('&','&amp;')), 'the content-free Source refusal offers the admitted current-entry route');
    assert.doesNotMatch(refusal.body,new RegExp('#launch/'+f.appId+'/'+wrongAddress.id));assert.equal(refusal.headers['set-cookie'],undefined);
    assert.equal(redirect.headers.location,recoveryUrl.href);assert.equal(redirect.headers['set-cookie'],undefined);
    assert.throws(() => call({ domainId: wrongAddress.id, path: '/embed' }), { code: 'scoped_embed_source_changed' }, 'recovery never authorizes the other origin');
    for (const path of ['/?q=1', '/board']) assert.throws(() => call({ path }), { code: 'app_scoped_admission_required' });
    const policyResponse = await new Promise((done, reject) => {
      const req = request({ hostname: '127.0.0.1', port: server.address().port, path: '/embed',
        headers: { Host: new URL(f.profile.embedOrigin).host } }, res => { res.resume(); res.once('end', () => done(res)); });
      req.once('error', reject); req.end();
    });
    assert.ok([401, 403].includes(policyResponse.statusCode));
    assert.equal(policyResponse.headers['set-cookie'], undefined);
    const policy = policyResponse.headers['content-security-policy'];
    const connect = policy.split(';').map(value => value.trim()).find(value => value.startsWith('connect-src '));
    assert.equal(connect, rootWorkspace ? `connect-src 'self' ${f.profile.nativeOrigin}` : "connect-src 'self'");
    assert.match(policy, /(?:^|;\s*)form-action 'self'(?:;|$)/u);
    assert.match(policy, /(?:^|;\s*)frame-src 'none'(?:;|$)/u);
    assert.doesNotMatch(policy, /'unsafe-eval'|\*/u);
    assert.deepEqual([...new Set(policy.match(/https:\/\/[^\s;]+/gu))].sort(),
      (rootWorkspace ? [f.profile.nativeOrigin, f.profile.parentOrigin] : [f.profile.parentOrigin]).sort());
    if (rootWorkspace) {
      assert.match(policy, /(?:^|;\s*)worker-src 'self'(?:;|$)/u);
      assert.match(policy, /(?:^|;\s*)script-src 'self' 'unsafe-inline' blob: 'wasm-unsafe-eval'(?:;|$)/u);
    } else {
      assert.match(policy, /(?:^|;\s*)worker-src 'none'(?:;|$)/u);
      assert.doesNotMatch(policy, /wasm-unsafe-eval|script-src[^;]*blob:/u);
    }
    if (rootWorkspace) {
      const launched = call({ domainId: initial.entry.domainId, path: '/' });
      assert.deepEqual(launched.entry, initial.entry); assert.equal(launched.entry.path, '/embed');
      assert.deepEqual(launched.scopedSource, pin); assert.equal(new URL(launched.launchUrl).searchParams.get('path'), '/embed');
      const catalog = service.execute({ op: 'apps.catalog', actor: outsider, args: {} });
      assert.equal(catalog.apps[0].id, f.appId); assert.equal(catalog.apps[0].entry.path, '/embed');
      assert.deepEqual(f.rows().local_app_grants, before.local_app_grants, 'no per-user access grant was added');
      const outside = service.execute({ op: 'apps.launch', actor: outsider, args: { appId: f.appId,
        domainId: launched.entry.domainId, path: '/', instanceId: 'main' } });
      assert.deepEqual(outside.scopedSource, pin); assert.notEqual(outside.scopedCloseHandle, launched.scopedCloseHandle);
      assert.ok(humanSubjects.includes(outsider.accountId));
      const authority = service.withAppAuthority({ actor: outsider, appId: f.appId, mode: 'participant', domainId: launched.entry.domainId, path: '/embed' }, value => value);
      assert.equal(authority.accountId, outsider.accountId); assert.equal(authority.ownerId, f.owner.accountId);
      assert.equal(authority.visibility, 'public'); assert.equal(authority.canManage, false);
      assert.throws(() => service.execute({ op: 'apps.launch', actor: undefined, args: { appId: f.appId, domainId: launched.entry.domainId } }), { code: 'apps_authentication_required' });
      for (const path of ['/embed', '/api/embed/query', '/api/embed/feedback/context', '/api/projects']) {
        const response = await new Promise((done, reject) => {
          const req = request({ hostname: '127.0.0.1', port: server.address().port, path,
            headers: { Host: new URL(f.profile.embedOrigin).host } }, res => { res.resume(); res.once('end', () => done(res)); });
          req.once('error', reject); req.end();
        });
        assert.ok([401, 403].includes(response.statusCode), path + ':' + response.statusCode);
        assert.equal(response.headers['set-cookie'], undefined);
      }
      assert.equal(forwardedRequests, 0, 'anonymous requests never reach Native or acquire a Source proof');
      // A browser shares one cookie jar between same-origin main/widget frames.
      // Interleave both POSTs before either GET; separate jars hide this race.
      const jar = new Map();
      const browser = (path, { body, headers = {}, cookie } = {}) => new Promise((done, reject) => {
        const bytes = body === undefined ? null : Buffer.from(JSON.stringify(body));
        const req = request({ hostname: '127.0.0.1', port: server.address().port, path,
          method: bytes ? 'POST' : 'GET', headers: { Host: new URL(f.profile.embedOrigin).host,
            Origin: f.profile.embedOrigin, Cookie: cookie ?? [...jar].map(([k, v]) => k + '=' + v).join('; '),
            ...(bytes ? { 'content-type': 'application/json', 'content-length': bytes.length } : {}), ...headers } }, res => {
          const parts = []; res.on('data', part => parts.push(part)); res.once('end', () => {
            for (const item of res.headers['set-cookie'] ?? []) { const pair = item.split(';')[0], at = pair.indexOf('='); jar.set(pair.slice(0, at), pair.slice(at + 1)); }
            const text=Buffer.concat(parts).toString('utf8');done({ status: res.statusCode, body: text?JSON.parse(text):null, headers: res.headers });
          });
        }); req.once('error', reject); req.end(bytes);
      });
      const navigation = '/embed#document=pag_synthetic&block=hero&invite=' + 'z'.repeat(43);
      const main = call({ instanceId: 'main', path: navigation }), widget = call({ instanceId: 'widget-12345678-1234-1234-1234-123456789abc' });
      assert.equal(new URL(main.launchUrl).searchParams.get('path'), '/embed');
      assert.equal(new URL(main.launchUrl).searchParams.size, 1);
      assert.equal(main.launchUrl.includes('invite'), false, 'private invitation navigation never enters the bootstrap request URL');
      const boot = launch => browser('/_soty/session', { body: { ticket: new URL(launch.launchUrl).hash.slice(1) } });
      const mainBoot = await boot(main), widgetBoot = await boot(widget);
      assert.equal(mainBoot.status, 200); assert.equal(widgetBoot.status, 200);
      assert.equal(mainBoot.body.entryPath, navigation, 'the authenticated ticket response retains the exact document locator');
      const mainCheck = await browser('/_soty/session', { headers: { 'x-soty-frame': mainBoot.body.scopedRequestId, 'x-soty-boot-check': mainBoot.body.sessionCheck } });
      const widgetCheck = await browser('/_soty/session', { headers: { 'x-soty-frame': widgetBoot.body.scopedRequestId, 'x-soty-boot-check': widgetBoot.body.sessionCheck } });
      assert.equal(mainCheck.status, 200, 'main keeps its own session after widget boot in the SAME browser jar');
      assert.equal(widgetCheck.status, 200);
      assert.equal(mainBoot.body.frame,mainBoot.body.scopedRequestId);assert.equal(widgetBoot.body.frame,widgetBoot.body.scopedRequestId);
      const read=frame=>browser('/api/embed/query',{body:{operation:'soty.current_context'},headers:{'x-soty-frame':frame}});
      assert.equal((await read(mainBoot.body.frame)).body.instanceId,'main');
      assert.equal((await read(widgetBoot.body.frame)).body.instanceId,'widget-12345678-1234-1234-1234-123456789abc');
      assert.equal((await browser('/embed?sotyFrame='+mainBoot.body.frame)).body.path,'/embed','Root-only locator is removed before the closed Native route');
      assert.equal(seen.at(-1).headers['x-soty-frame'],undefined);assert.equal(seen.at(-1).headers.cookie,undefined);
      const ownCookie=frame=>'__Host-soty_app_session_'+frame;
      const mainCookie=jar.get(ownCookie(mainBoot.body.frame)),widgetCookie=jar.get(ownCookie(widgetBoot.body.frame));
      const denied=await browser('/api/embed/query',{body:{},headers:{'x-soty-frame':mainBoot.body.frame},cookie:ownCookie(mainBoot.body.frame)+'='+widgetCookie});
      assert.equal(denied.status,403,'renaming a widget bearer under the main public name cannot change its authority');
      assert.equal((await browser('/api/embed/query',{body:{}})).status,403,'last ordinary cookie never selects a private frame');
      assert.equal((await browser('/api/embed/query',{body:{},headers:{'x-soty-frame':mainBoot.body.frame},cookie:''})).status,403);
      assert.equal((await browser('/embed?sotyFrame='+mainBoot.body.frame,{headers:{'x-soty-frame':widgetBoot.body.frame}})).status,403);
      assert.equal((await browser('/embed?sotyFrame='+mainBoot.body.frame+'&sotyFrame='+mainBoot.body.frame)).status,403);
      const renewed=await service.sourcePreparationExtension.executeAsync({op:'apps.scoped.renew',actor:f.owner,args:{appId:f.appId,handle:main.scopedCloseHandle,requestId:'main-frame-renewal-1'}});
      assert.equal(new URL(renewed.launchUrl).searchParams.get('path'), '/embed');assert.equal(renewed.launchUrl.includes('invite'), false);
      const pending=await boot(renewed);assert.equal(pending.status,200);assert.equal(pending.body.frame,mainBoot.body.frame,'renewal retains the visible frame locator');
      assert.equal(pending.body.entryPath, navigation, 'short-session renewal preserves navigation in private server state');
      assert.equal(pending.headers['set-cookie'],undefined);assert.equal(jar.get(ownCookie(mainBoot.body.frame)),mainCookie);
      assert.equal((await read(mainBoot.body.frame)).body.reference,mainBoot.body.scopedRequestId,'visible main remains current before ACK');
      const confirmed=await browser('/api/embed/session-continue',{body:{requestId:pending.body.scopedRequestId},headers:{'x-soty-frame':pending.body.frame,'x-soty-boot-check':pending.body.sessionCheck}});
      assert.equal(confirmed.status,200);assert.equal(confirmed.body.ready,true);
      assert.notEqual(jar.get(ownCookie(mainBoot.body.frame)),mainCookie);assert.equal(jar.get(ownCookie(widgetBoot.body.frame)),widgetCookie);
      assert.equal((await read(mainBoot.body.frame)).body.reference,pending.body.scopedRequestId);
      assert.equal((await read(widgetBoot.body.frame)).body.reference,widgetBoot.body.scopedRequestId);
      service.execute({op:'apps.scoped.close',actor:f.owner,args:{appId:f.appId,handle:main.scopedCloseHandle}});
      assert.equal((await read(mainBoot.body.frame)).status,200,'closing replaced main cannot retire its new family head');
      // The last ordinary/base cookie belongs to a subsequently closed widget.
      const lastWidget=call({instanceId:'widget-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'});
      assert.equal((await boot(lastWidget)).status,200);
      service.execute({op:'apps.scoped.close',actor:f.owner,args:{appId:f.appId,handle:lastWidget.scopedCloseHandle}});
      const closedOwnerBase=jar.get('__Host-soty_app_session'),outsideBoot=await boot(outside);
      assert.equal(outsideBoot.status,200);
      // A retained cookie from a different signed Root actor is not a fallback
      // for the actor selected by the ordinary/base cookie.
      jar.set('__Host-soty_app_session',closedOwnerBase);
      holdNext=true;const late=read(widgetBoot.body.frame);
      const heldDeadline=Date.now()+1500;
      while(!heldReply){assert.ok(Date.now()<heldDeadline,'controlled Native reply was reached');await new Promise(done=>setTimeout(done,1));}
      service.execute({op:'apps.scoped.close',actor:f.owner,args:{appId:f.appId,handle:widget.scopedCloseHandle}});
      heldReply();assert.equal((await late).status,403,'late response cannot reopen the closed widget');
      assert.equal((await read(widgetBoot.body.frame)).status,403);assert.equal((await read(mainBoot.body.frame)).status,200);
      const ingress=()=>browser('/_soty/ingress-check',{headers:{'x-soty-ingress-app':f.appId,'x-soty-ingress-target':f.target.digest}});
      assert.equal((await browser('/assets/lazy.js')).status,200,'a closed last widget cannot break a surviving main lazy asset');
      assert.equal(seen.at(-1).context.instanceId,'main');assert.equal(seen.at(-1).headers.cookie,undefined);
      assert.equal((await ingress()).status,204,'fixed Native ingress also requires a surviving exact-app frame');
      assert.equal((await browser('/_soty/ingress-check',{headers:{'x-soty-ingress-app':'app-'+'f'.repeat(32),'x-soty-ingress-target':f.target.digest}})).status,403);
      assert.equal((await browser('/_soty/ingress-check',{headers:{'x-soty-ingress-app':f.appId,'x-soty-ingress-target':'f'.repeat(64)}})).status,403);
      assert.equal((await browser('/assets/lazy.js',{cookie:'__Host-soty_app_session='+'z'.repeat(43)+'; '+ownCookie(mainBoot.body.frame)+'='+jar.get(ownCookie(mainBoot.body.frame))})).status,403,'unknown base never borrows a valid frame');
      assert.equal((await browser('/api/embed/query',{body:{}})).status,403,'asset/ingress fallback grants no private Source route');
      service.execute({op:'apps.scoped.close',actor:f.owner,args:{appId:f.appId,handle:renewed.scopedCloseHandle}});
      assert.equal((await read(mainBoot.body.frame)).status,403,'both frames stay closed independently');
      assert.equal((await browser('/assets/lazy.js')).status,403);assert.equal((await ingress()).status,403,'closed-all denies ingress');
      assert.equal((await read(outsideBoot.body.frame)).status,200,'another actor is still independently live, but was not borrowed');
      const freshLaunch=call({instanceId:'main'}),fresh=await boot(freshLaunch);assert.equal(fresh.status,200);
      const beforeExpiry=seen.length;clock+=301000;const expired=await read(fresh.body.frame);
      assert.ok([401,403].includes(expired.status),'frame selector never extends the Root lease');assert.equal(seen.length,beforeExpiry);
      // Actual client + Apps/gateway + private connector ACK after full W2 expiry.
      // The connector fixture is constructor-owned; this is not a Native test.
      let binding={handle:freshLaunch.scopedCloseHandle,source:pin,slot:{}},swaps=0,issuedFrame;
      const renewal=createScopedSlotRenewal({clock:()=>clock,readBinding:()=>binding,isCurrent:()=>active,
        readContext:handle=>service.sourcePreparationExtension.executeAsync({op:'apps.scoped.context',actor:f.owner,args:{appId:f.appId,handle}}),
        request:async args=>{
          assert.equal(args.shortSessionHandoff,undefined,'W2 must use the existing Root identity branch');
          const r=await service.sourcePreparationExtension.executeAsync({op:'apps.scoped.renew',actor:f.owner,args:{appId:f.appId,...args}});
          return{handle:r.scopedCloseHandle,source:r.scopedSource,slot:{},launchUrl:r.launchUrl,requestId:args.requestId};},
        bootstrap:async reply=>{const issued=await boot(reply);assert.equal(issued.status,200);issuedFrame=issued.body.frame;
          const ack=await browser('/api/embed/session-continue',{body:{requestId:issued.body.scopedRequestId},headers:{'x-soty-frame':issued.body.frame,'x-soty-boot-check':issued.body.sessionCheck}});
          assert.equal(ack.status,200);assert.equal(ack.body.shortSession,undefined);return'ready';},
        commit:(old,next)=>{assert.equal(old.slot,binding.slot);binding=next;swaps++;return true;}});
      assert.equal(await renewal.refresh(),true);assert.equal(swaps,1);assert.equal(issuedFrame,fresh.body.frame,'same mounted frame locator survives expiry');
      assert.equal((await read(fresh.body.frame)).body.instanceId,'main');
      renewal.dispose();const afterRenewal=seen.length;

      const next=await boot(call({instanceId:'main'}));assert.equal(next.status,200);active=false;
      assert.ok([401,403].includes((await read(next.body.frame)).status));assert.equal(seen.length,afterRenewal,'revoked actor never reaches Native');
    } else assert.throws(() => call({ path: '/' }), { code: 'app_scoped_admission_required' });
    active = false; assert.throws(() => call({ path: '/' }), { code: 'apps_authentication_required' });
    assert.deepEqual(f.rows(), before, 'no preference, target, admission or publication mutation');
  });
});

test('Source preferred entry fails closed for missing declaration, wrong app, inactive alias and revocation', async t => {
  const directory = mkdtempSync(join(tmpdir(), 'soty-source-entry-denied-')), databasePath = join(directory, 'apps.sqlite');
  const template = 'https://{appId}.apps.test', f = fixture(t, 8, 2, { databasePath, template, publicEntry: true });
  f.promote(f.intent(await f.prepare())); const before = f.rows();
  let active = true, service;
  const options = { databasePath, appOriginTemplate: template, shellOrigins: ['https://root.test'],
    allowScopedEmbedMigration: true, allowSelectedResourceMigration: true, withAuthorityFence: callback => callback(),
    actorActive: actor => active && actor?.accountId === f.owner.accountId && actor?.deviceId === f.owner.deviceId };
  t.after(() => { service?.close(); rmSync(directory, { recursive: true, force: true }); });
  const open = profiles => { service?.close(); service = createAppsService({ ...options, scopedEmbedProfiles: profiles }); };
  const call = (op, args = {}) => service.execute({ op, actor: f.owner, args: { ...args, ...(op === 'apps.list' ? {} : { appId: f.appId }) } });
  const unavailable = () => {
    const snapshot = call('apps.inspect'); assert.equal(snapshot.source.launchEntry, null);
    assert.equal(snapshot.actions.canPreview, false); assert.equal(preferredInspectionEntry(snapshot), undefined);
    assert.equal(call('apps.list').apps[0].entry, undefined);
  };
  open([]); unavailable();
  const wrongApp = 'app-' + 'f'.repeat(32);
  open([{ ...f.profile, appId: wrongApp, resource: { ...f.profile.resource, appId: wrongApp } }]); unavailable();
  assert.deepEqual(f.rows(), before, 'navigation metadata changes no app, policy, target, admission or grant');
  open([f.profile]); const preferred = preferredInspectionEntry(call('apps.inspect'));
  assert.equal(preferred.origin, f.profile.embedOrigin);
  active = false;
  assert.throws(() => call('apps.inspect'), { code: 'apps_authentication_required' });
  assert.throws(() => call('apps.entry.get', { domainId: preferred.domainId }), { code: 'apps_authentication_required' });
  active = true;
  const policy = call('apps.publication.get');
  call('apps.publication.update', { requestId: 'remove-admitted-alias', expectedPolicyEpoch: policy.policyEpoch,
    expectedTargetRevision: policy.activeTargetRevision, launchPolicy: 'restricted', listed: false, activeDomainIds: [] });
  unavailable();
  assert.throws(() => call('apps.entry.get', { domainId: preferred.domainId }), { code: 'app_unavailable' });
  call('apps.revoke'); unavailable();
  assert.throws(() => call('apps.entry.get', { domainId: preferred.domainId }), { code: 'app_unavailable' });
});


test('only explicitly admitted Root workspace2 can publish an authenticated entry; restart retains exact ACK and old pins stay private', async t => {
  for (const pin of [STANDARD_ROOT_WORKSPACE_SOURCE, STANDARD_WORKSPACE_SOURCE, HIVE_SELECTED_SOURCE]) await t.test(pin.id + ':' + pin.version, async sub => {
    const f = fixture(sub, 8, 2, { pin, publicEntry: true }), prepared = await f.prepare();
    const args = { ...f.intent(prepared), launchPolicy: 'anyone', listed: true,
      exposureAck: { scope: 'authenticated-workspace', targetRevision: 2, targetDigest: f.target.digest, profile: f.target.profile } };
    const before = f.rows();
    if (pin !== STANDARD_ROOT_WORKSPACE_SOURCE) {
      assert.throws(() => f.promote(args), { code: 'app_scoped_private_required' });
      assert.deepEqual(f.rows(), before); return;
    }
    for (const exposureAck of [ { ...args.exposureAck, scope: 'whole-port' }, { ...args.exposureAck, targetDigest: 'f'.repeat(64) } ]) {
      assert.throws(() => f.promote({ ...args, exposureAck }), { code: 'app_exposure_ack_required' }); assert.deepEqual(f.rows(), before);
    }
    const accepted = f.promote(args); assert.equal(accepted.current.launchPolicy, 'anyone'); assert.equal(accepted.current.listed, true);
    assert.deepEqual(accepted.receipt.exposureAck, args.exposureAck); assert.equal(inspectAppsSchema(f.db), 'v8');
    const committed = f.rows(); f.reopen(); assert.equal(f.promote(args).replayed, true); assert.deepEqual(f.rows(), committed);
    const update = extra => f.publications.execute({ op: 'apps.publication.update', actor: f.owner, args: { appId: f.appId,
      requestId: 'workspace-publication-update', expectedPolicyEpoch: accepted.current.policyEpoch, expectedTargetRevision: 2,
      activeDomainIds: accepted.current.activeDomainIds, launchPolicy: 'anyone', listed: true, exposureAck: args.exposureAck, ...extra } });
    assert.throws(() => update({ exposureAck: { ...args.exposureAck, scope: 'whole-port' } }), { code: 'app_exposure_ack_required' });
    assert.deepEqual(f.rows(), committed);
    assert.deepEqual(update({}).receipt.exposureAck, args.exposureAck);
    migrateAppsSchema(f.db, { allowScopedEmbedMigration: true, allowSelectedResourceMigration: true });
    // A durable public row with another scope must fail cold inspection, not open routes.
    f.db.prepare('UPDATE app_publications SET exposure_ack_json=? WHERE app_id=?').run(JSON.stringify({ ...args.exposureAck, scope: 'whole-port' }), f.appId);
    assert.throws(() => migrateAppsSchema(f.db, { allowScopedEmbedMigration: true, allowSelectedResourceMigration: true }), { code: 'apps_scoped_public_forbidden' });
  });
});
