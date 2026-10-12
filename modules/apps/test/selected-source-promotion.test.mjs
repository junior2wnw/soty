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
      ...(profileVersion === 2 ? { selection: { kind: options.pin?.id === 'soty.standard-workspace' ? 'soty.workspace.v1' : 'hive.project.v1', nativeId: 'synthetic_native', incarnationId: 'one' } } : { workspaceId: 'selected' }) },
    issuer: 'https://root.test/human-identity', clientId: 'synthetic-selected', embedOrigin: options.publicEntry ? 'https://selected.apps.test' : options.template?.replace('{appId}', appId) ?? 'https://selected.apps.test',
    nativeOrigin: 'https://native.test', parentOrigin: 'https://root.test' };
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
    const template = 'https://{appId}.apps.test', f = fixture(sub, 8, 2, { databasePath, template, pin, publicEntry: pin.version === 2 });
    const prepared = await f.prepare(), intent = f.intent(prepared);
    if (pin.version === 2) Object.assign(intent, { launchPolicy: 'anyone', listed: true, exposureAck: {
      scope: 'authenticated-workspace', targetRevision: f.target.revision, targetDigest: f.target.digest, profile: f.target.profile } });
    f.promote(intent); const before = f.rows();
    const outsider = Object.freeze({ accountId: 'new-ungranted-person', deviceId: 'new-person-browser' });
    const actors = [f.owner, outsider]; let humanSubjects = [], forwardedRequests = 0;
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
    const call = args => service.execute({ op: 'apps.launch', actor: f.owner, args: { appId: f.appId, instanceId: 'main', ...(pin.version === 2 ? { domainId: 'dom_' + 'b'.repeat(32) } : {}), ...args } });
    const deadline = Date.now() + 3000; let initial;
    while (!initial) { try { initial = call({ path: '/embed' }); } catch (error) {
      if (!['app_offline', 'app_binding_pending'].includes(error.code) || Date.now() >= deadline) throw error;
      await new Promise(done => setTimeout(done, 5)); } }
    for (const path of ['/?q=1', '/board']) assert.throws(() => call({ path }), { code: 'app_scoped_admission_required' });
    if (pin.version === 2) {
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
            done({ status: res.statusCode, body: JSON.parse(Buffer.concat(parts)), headers: res.headers });
          });
        }); req.once('error', reject); req.end(bytes);
      });
      const main = call({ instanceId: 'main' }), widget = call({ instanceId: 'widget-12345678-1234-1234-1234-123456789abc' });
      const boot = launch => browser('/_soty/session', { body: { ticket: new URL(launch.launchUrl).hash.slice(1) } });
      const mainBoot = await boot(main), widgetBoot = await boot(widget);
      assert.equal(mainBoot.status, 200); assert.equal(widgetBoot.status, 200);
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
      const pending=await boot(renewed);assert.equal(pending.status,200);assert.equal(pending.body.frame,mainBoot.body.frame,'renewal retains the visible frame locator');
      assert.equal(pending.headers['set-cookie'],undefined);assert.equal(jar.get(ownCookie(mainBoot.body.frame)),mainCookie);
      assert.equal((await read(mainBoot.body.frame)).body.reference,mainBoot.body.scopedRequestId,'visible main remains current before ACK');
      const confirmed=await browser('/api/embed/session-continue',{body:{requestId:pending.body.scopedRequestId},headers:{'x-soty-frame':pending.body.frame,'x-soty-boot-check':pending.body.sessionCheck}});
      assert.equal(confirmed.status,200);assert.equal(confirmed.body.ready,true);
      assert.notEqual(jar.get(ownCookie(mainBoot.body.frame)),mainCookie);assert.equal(jar.get(ownCookie(widgetBoot.body.frame)),widgetCookie);
      assert.equal((await read(mainBoot.body.frame)).body.reference,pending.body.scopedRequestId);
      assert.equal((await read(widgetBoot.body.frame)).body.reference,widgetBoot.body.scopedRequestId);
      service.execute({op:'apps.scoped.close',actor:f.owner,args:{appId:f.appId,handle:main.scopedCloseHandle}});
      assert.equal((await read(mainBoot.body.frame)).status,200,'closing replaced main cannot retire its new family head');
      holdNext=true;const late=read(widgetBoot.body.frame);
      const heldDeadline=Date.now()+1500;
      while(!heldReply){assert.ok(Date.now()<heldDeadline,'controlled Native reply was reached');await new Promise(done=>setTimeout(done,1));}
      service.execute({op:'apps.scoped.close',actor:f.owner,args:{appId:f.appId,handle:widget.scopedCloseHandle}});
      heldReply();assert.equal((await late).status,403,'late response cannot reopen the closed widget');
      assert.equal((await read(widgetBoot.body.frame)).status,403);assert.equal((await read(mainBoot.body.frame)).status,200);
      service.execute({op:'apps.scoped.close',actor:f.owner,args:{appId:f.appId,handle:renewed.scopedCloseHandle}});
      assert.equal((await read(mainBoot.body.frame)).status,403,'both frames stay closed independently');
      const fresh=await boot(call({instanceId:'main'}));assert.equal(fresh.status,200);
      const beforeExpiry=seen.length;clock+=301000;const expired=await read(fresh.body.frame);
      assert.ok([401,403].includes(expired.status),'frame selector never extends the Root lease');assert.equal(seen.length,beforeExpiry);
      const next=await boot(call({instanceId:'main'}));assert.equal(next.status,200);active=false;
      assert.ok([401,403].includes((await read(next.body.frame)).status));assert.equal(seen.length,beforeExpiry,'revoked actor never reaches Native');
    } else assert.throws(() => call({ path: '/' }), { code: 'app_scoped_admission_required' });
    active = false; assert.throws(() => call({ path: '/' }), { code: 'apps_authentication_required' });
    assert.deepEqual(f.rows(), before, 'no preference, target, admission or publication mutation');
  });
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
