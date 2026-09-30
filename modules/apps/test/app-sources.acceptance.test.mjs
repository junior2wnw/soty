import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname, resolve, basename } from 'node:path';
import { Worker } from 'node:worker_threads';
import { createHistoricalAppsV3, seedHistoricalPublicationV3 } from '../../../deploy/connector/apps-v3.fixture.mjs';
import { migrateAppsSchema } from '../server/schema.mjs';
import { createPublicationRegistry } from '../server/publications.mjs';
import { createDomainRegistry } from '../server/domains.mjs';
import { createSourceRegistry } from '../server/sources.mjs';
import { AppsError } from '../server/protocol.mjs';

// Independent model acceptance. HTTP/connector v2 is NOT present here. The
// WeakMap proof provider below is an explicitly synthetic authority seam.
const owner = Object.freeze({ accountId: 'review_owner', deviceId: 'review_browser' });
const outsider = Object.freeze({ accountId: 'review_other', deviceId: 'other_browser' });
const guest = Object.freeze({ accountId: 'review_guest', deviceId: 'guest_browser' });
const appA = `app-${'1'.repeat(32)}`, appB = `app-${'2'.repeat(32)}`, appC = `app-${'3'.repeat(32)}`;
const aliasA = `dom_${'a'.repeat(32)}`;
const sourceA = { linkId: 'review_link', hostDeviceId: 'first_host', connectorId: 'first_connector' };
const sourceB = { linkId: 'review_link', hostDeviceId: 'second_host', connectorId: 'second_connector' };
const sourceC = { linkId: 'other_link', hostDeviceId: 'other_host', connectorId: 'other_connector' };
const key = value => [value.linkId, value.hostDeviceId, value.connectorId].join('|');
const actorKey = actor => `${actor?.accountId}|${actor?.deviceId}`;
const turn = () => new Promise(resolveTurn => setImmediate(resolveTurn));
const deferred = () => { let finish; const promise = new Promise(resolvePromise => { finish = resolvePromise; }); return { promise, finish }; };
const code = expected => error => error.code === expected;
const tables = ['local_apps', 'local_app_grants', 'app_runtime_targets', 'app_publications', 'app_publication_domains',
  'app_domains', 'app_domain_heads', 'app_domain_receipts', 'app_source_heads', 'app_source_receipts', 'app_publication_receipts'];
const snapshot = db => Object.fromEntries(tables.map(name => [name, db.prepare(`SELECT * FROM ${name} ORDER BY rowid`).all()]));

function seed(db) {
  createHistoricalAppsV3(db);
  for (const [identity, account] of [[sourceA, owner.accountId], [sourceB, owner.accountId], [sourceC, outsider.accountId]])
    db.prepare('INSERT INTO app_devices VALUES (?,?,?,?,?)').run(key(identity), account, JSON.stringify(identity), identity.hostDeviceId, 1);
  db.exec("INSERT INTO app_domain_zones VALUES ('review_named','named','https://{slug}.apps.example.test','apps.example.test','https','',1)");
  for (const [id, account, port, device, domain] of [[appA, owner.accountId, 8101, sourceA, aliasA],
    [appB, owner.accountId, 8102, sourceA, `dom_${'b'.repeat(32)}`], [appC, outsider.accountId, 8103, sourceC, `dom_${'c'.repeat(32)}`]]) {
    const grants = { accountIds: [guest.accountId], communityIds: ['review_group'] };
    db.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)').run(id, account, key(device), `App ${port}`, port,
      '/#/home', JSON.stringify(grants), 'enabled', 4, 1, 1);
    db.prepare('INSERT INTO local_app_grants VALUES (?,?,?)').run(id, 'account', guest.accountId);
    db.prepare('INSERT INTO local_app_grants VALUES (?,?,?)').run(id, 'community', 'review_group');
    db.prepare('INSERT INTO app_domain_heads VALUES (?,0)').run(id);
    seedHistoricalPublicationV3(db, id);
    const slug = `app-${port}`;
    db.prepare("INSERT INTO app_domains VALUES (?,?,?,?,?,?,?,'alias','bound',1,NULL)")
      .run(domain, 'review_named', `${slug}.apps.example.test`, `https://${slug}.apps.example.test`, slug, id, account);
    db.prepare('INSERT INTO app_publication_domains VALUES (?,?,?)').run(id, domain, account);
  }
  migrateAppsSchema(db);
}

async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-source-independent-'));
  const databasePath = join(directory, 'registry.sqlite');
  const connections = new Set(), registries = new Set();
  const active = new Set([owner, outsider, guest].map(actorKey));
  let timestamp = 100_000, channelGeneration = 1, provider, notifier;
  const observations = [], events = [];
  function connect(initial = false) {
    const db = new DatabaseSync(databasePath);
    db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000');
    if (initial) seed(db); else migrateAppsSchema(db);
    db.exec('PRAGMA journal_mode=WAL'); connections.add(db);
    const authenticate = actor => { if (!active.has(actorKey(actor))) throw new AppsError('apps_authentication_required', 401); };
    const publications = createPublicationRegistry({ db, now: () => timestamp, assertActor: authenticate,
      canUse: actor => active.has(actorKey(actor)) });
    const proofRecords = new WeakMap();
    const sources = createSourceRegistry({ db, now: () => timestamp, assertActor: authenticate, publications,
      prepareTarget: context => {
        assert.equal(db.isTransaction, false, 'network provider is outside SQL transaction'); observations.push(context);
        const mint = () => {
          const evidence = Object.freeze({});
          proofRecords.set(evidence, { generation: channelGeneration, target: context.target, preparationId: context.preparationId,
            actorKey: actorKey(context.actor) });
          return evidence;
        };
        return provider ? provider(context, mint) : mint();
      },
      verifyPreparedTarget: context => {
        assert.equal(db.isTransaction, true, 'current synthetic proof is rechecked inside snapshot/commit');
        const proof = proofRecords.get(context.evidence);
        return Boolean(proof && proof.generation === channelGeneration && proof.target === context.target
          && proof.preparationId === context.preparationId && proof.actorKey === actorKey(context.actor));
      },
      onChanged: event => { events.push(event); return notifier?.(event); },
    });
    registries.add(sources);
    const domains = createDomainRegistry({ db, assertActor: authenticate, now: () => timestamp,
      onRetireInTransaction: value => publications.retireInTransaction(value), onPolicyChanged: () => {} });
    return { db, publications, sources, domains,
      close() { sources.close(); registries.delete(sources); db.close(); connections.delete(db); } };
  }
  const initial = connect(true);
  t.after(async () => {
    for (const registry of registries) registry.close();
    for (const connection of connections) connection.close();
    assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(basename(directory), /^soty-source-independent-/u);
    await rm(directory, { recursive: true, force: true });
  });
  return { ...initial, databasePath, connect, observations, events,
    provider(value) { provider = value; }, notifier(value) { notifier = value; },
    active(actor, value) { if (value) active.add(actorKey(actor)); else active.delete(actorKey(actor)); },
    expire() { timestamp += 40_000; }, reconnect() { channelGeneration++; } };
}
const read = (f, id = appA, actor = owner) => f.publications.execute({ op: 'apps.publication.get', actor, args: { appId: id } });
function prepareArgs(f, { id = appA, historical, port = 9200 } = {}) {
  const current = read(f, id);
  return { appId: id, expectedPolicyEpoch: current.policyEpoch, expectedTargetRevision: current.activeTargetRevision,
    ...(historical === undefined ? { source: { hostDeviceId: sourceB.hostDeviceId, connectorId: sourceB.connectorId, port,
      entryPath: '/next?encoded=%2B%23%2F#section' } } : { targetRevision: historical }) };
}
const prepare = (f, options) => f.sources.execute({ op: 'apps.source.prepare', actor: owner, args: prepareArgs(f, options) });
const intent = (prepared, requestId, extra = {}) => ({ appId: prepared.appId, requestId, preparationId: prepared.preparationId,
  expectedPolicyEpoch: prepared.expectedPolicyEpoch, expectedTargetRevision: prepared.expectedTargetRevision,
  launchPolicy: 'restricted', listed: false, ...extra });
const promote = (f, args, actor = owner) => f.sources.execute({ op: 'apps.source.promote', actor, args });

test('authority revoked during await defeats a previously successful synthetic proof without durable writes', async t => {
  const f = await fixture(t), gate = deferred(), before = snapshot(f.db);
  f.provider(async (_context, mint) => { await gate.promise; return mint(); });
  const pending = prepare(f); await turn(); f.active(owner, false); gate.finish();
  await assert.rejects(pending, code('apps_authentication_required')); assert.deepEqual(snapshot(f.db), before);
  f.active(owner, true); f.provider(null);
  const prepared = await prepare(f); f.reconnect();
  assert.throws(() => promote(f, intent(prepared, 'old-connection')), code('apps_source_preparation_stale'));
  assert.deepEqual(snapshot(f.db), before);
});

test('granted readers, foreign owners and a sibling app cannot borrow source authority', async t => {
  const f = await fixture(t), args = prepareArgs(f), before = snapshot(f.db);
  for (const actor of [guest, outsider]) {
    await assert.rejects(f.sources.execute({ op: 'apps.source.prepare', actor, args }), code('apps_owner_required'));
    assert.throws(() => f.sources.execute({ op: 'apps.source.history', actor, args: { appId: appA } }), code('apps_owner_required'));
  }
  assert.equal(f.observations.length, 0, 'private denial precedes runtime probe');
  const prepared = await prepare(f);
  assert.throws(() => promote(f, { ...intent(prepared, 'borrow'), appId: appB }), code('apps_source_preparation_mismatch'));
  assert.deepEqual(snapshot(f.db), before);
});

test('plain JSON does not count as runtime evidence; candidate data cannot forge a verified connection', async t => {
  const f = await fixture(t), before = snapshot(f.db);
  f.provider(context => ({ target: context.target, channelId: 'invented', ready: true }));
  await assert.rejects(prepare(f), code('apps_source_preparation_stale'));
  assert.deepEqual(snapshot(f.db), before);
});

test('lost notification is a durable commit; full DB reopen replay precedes expired ephemeral proof and current policy differs', async t => {
  const f = await fixture(t), prepared = await prepare(f), args = intent(prepared, 'lost-response');
  f.notifier(() => { throw new Error('synthetic notification lost'); });
  assert.throws(() => promote(f, args), /synthetic notification lost/u);
  assert.equal(read(f).activeTargetRevision, 2); f.close(); f.expire(); f.notifier(null);
  f.provider(() => { throw new Error('replay must not require a new runtime proof'); });
  const reopened = f.connect(), current = read(reopened);
  reopened.publications.execute({ op: 'apps.publication.update', actor: owner, args: {
    appId: appA, requestId: 'later-policy', expectedPolicyEpoch: current.policyEpoch, expectedTargetRevision: current.activeTargetRevision,
    launchPolicy: 'restricted', listed: false, activeDomainIds: [],
  } });
  const beforeReplay = snapshot(reopened.db), replay = promote(reopened, args);
  assert.equal(replay.replayed, true); assert.equal(replay.receipt.policyEpoch, 2); assert.equal(replay.current.policyEpoch, 3);
  assert.equal(replay.current.activeDomainIds.length, 0); assert.deepEqual(snapshot(reopened.db), beforeReplay);
  assert.throws(() => promote(reopened, args, outsider), code('apps_owner_required'));
  f.active(owner, false); assert.throws(() => promote(reopened, args), code('apps_authentication_required'));
});

test('active-alias retirement from another connection wins against prepared CAS without resurrecting access', async t => {
  const f = await fixture(t), prepared = await prepare(f), other = f.connect();
  other.domains.execute({ op: 'apps.domains.retire', actor: owner, args: { appId: appA, domainId: aliasA,
    requestId: 'external-retire', expectedDomainsRevision: 0 } });
  const afterRetire = snapshot(f.db);
  assert.throws(() => promote(f, intent(prepared, 'stale-after-retire')), code('app_publication_revision_conflict'));
  assert.deepEqual(snapshot(f.db), afterRetire);
  assert.equal(f.db.prepare('SELECT state FROM app_domains WHERE id=?').get(aliasA).state, 'tombstone');
  assert.equal(read(f).activeDomainIds.length, 0);
});

test('grant removal and revoke in another transaction conflict with prepared source instead of resurrecting policy', async t => {
  for (const change of ['grants', 'revoke']) {
    const f = await fixture(t), prepared = await prepare(f), other = f.connect();
    other.db.exec('BEGIN IMMEDIATE');
    if (change === 'grants') {
      other.db.prepare('DELETE FROM local_app_grants WHERE app_id=?').run(appA);
      other.db.prepare('UPDATE local_apps SET grants_json=? WHERE id=?').run(JSON.stringify({ accountIds: [], communityIds: [] }), appA);
      other.publications.grantsChangedInTransaction(appA);
    } else {
      other.db.prepare("UPDATE local_apps SET state='revoked' WHERE id=?").run(appA);
      other.publications.revokeInTransaction(appA);
    }
    other.db.exec('COMMIT'); const authoritative = snapshot(f.db);
    assert.throws(() => promote(f, intent(prepared, `after-${change}`)), code('app_publication_revision_conflict'));
    assert.deepEqual(snapshot(f.db), authoritative);
  }
});

test('source request keys are account-wide but separate from publication request keys', async t => {
  const f = await fixture(t), current = read(f), shared = 'same-visible-client-request';
  f.publications.execute({ op: 'apps.publication.update', actor: owner, args: { appId: appA, requestId: shared,
    expectedPolicyEpoch: current.policyEpoch, expectedTargetRevision: 1, launchPolicy: 'restricted', listed: false,
    activeDomainIds: [aliasA] } });
  const first = await prepare(f); assert.equal(promote(f, intent(first, shared)).replayed, false);
  const second = await prepare(f, { id: appB, port: 9300 }), before = snapshot(f.db);
  assert.throws(() => promote(f, intent(second, shared)), code('app_source_request_conflict'));
  assert.deepEqual(snapshot(f.db), before);
  assert.equal(f.db.prepare('SELECT count(*) AS count FROM app_publication_receipts').get().count, 1);
  assert.equal(f.db.prepare('SELECT count(*) AS count FROM app_source_receipts').get().count, 1);
});

test('failed retention prune rolls back the 65th effect; exact retry succeeds and pruned original intent never rebases', async t => {
  const f = await fixture(t); let firstIntent;
  for (let i = 0; i < 64; i++) {
    const prepared = await prepare(f, { historical: 1 }), args = intent(prepared, `retained-${i}`);
    firstIntent ??= args; promote(f, args);
  }
  const prepared = await prepare(f, { port: 9400 }), args = intent(prepared, 'prune-fault'), before = snapshot(f.db);
  f.db.exec("CREATE TRIGGER independent_prune_fault BEFORE DELETE ON app_source_receipts BEGIN SELECT RAISE(ABORT,'independent_prune_fault'); END");
  assert.throws(() => promote(f, args), /independent_prune_fault/u); assert.deepEqual(snapshot(f.db), before);
  f.db.exec('DROP TRIGGER independent_prune_fault');
  const accepted = promote(f, args); assert.equal(accepted.receipt.targetRevision, 2); assert.equal(accepted.receipt.policyEpoch, 66);
  assert.equal(f.db.prepare('SELECT count(*) AS count FROM app_source_receipts WHERE app_id=?').get(appA).count, 64);
  const after = snapshot(f.db);
  assert.throws(() => promote(f, firstIntent), code('app_publication_revision_conflict')); assert.deepEqual(snapshot(f.db), after);
  const reopened = f.connect(); assert.equal(read(reopened).requiredBindingVersion, 2);
});

test('epoch exhaustion rolls back insertion and first floor upgrade rather than leaving a half promotion', async t => {
  const f = await fixture(t);
  f.db.prepare('UPDATE app_publications SET policy_epoch=? WHERE app_id=?').run(Number.MAX_SAFE_INTEGER, appA);
  const prepared = await prepare(f), before = snapshot(f.db);
  assert.throws(() => promote(f, intent(prepared, 'exhausted')), code('apps_policy_epoch_exhausted'));
  assert.deepEqual(snapshot(f.db), before);
  assert.equal(f.db.prepare('SELECT required_binding_version AS floor FROM app_source_heads WHERE app_id=?').get(appA).floor, 1);
});

test('rollback has fresh whole-port consent, preserves initial bytes, and cannot take a port now owned by a sibling', async t => {
  const f = await fixture(t), firstTarget = f.db.prepare('SELECT * FROM app_runtime_targets WHERE app_id=? AND revision=1').get(appA);
  const changed = await prepare(f), active = promote(f, intent(changed, 'first-switch'));
  const rollback = await prepare(f, { historical: 1 });
  const wrongAck = { scope: 'whole-port', targetRevision: 1, targetDigest: changed.target.digest, profile: changed.target.profile };
  const before = snapshot(f.db);
  assert.throws(() => promote(f, intent(rollback, 'bad-public-rollback', { launchPolicy: 'anyone', exposureAck: wrongAck })), code('app_exposure_ack_required'));
  assert.deepEqual(snapshot(f.db), before);
  const correctAck = { ...wrongAck, targetDigest: rollback.target.digest };
  const restored = promote(f, intent(rollback, 'valid-rollback', { launchPolicy: 'anyone', exposureAck: correctAck }));
  assert.equal(restored.current.activeTargetRevision, 1); assert.equal(restored.current.requiredBindingVersion, 2);
  assert.deepEqual(f.db.prepare('SELECT * FROM app_runtime_targets WHERE app_id=? AND revision=1').get(appA), firstTarget);
  assert.equal(active.current.activeTargetRevision, 2);
  const moveAgain = await prepare(f, { historical: 2 }); promote(f, intent(moveAgain, 'move-again'));
  const old = await prepare(f, { historical: 1 });
  const siblingArgs = prepareArgs(f, { id: appB });
  siblingArgs.source = { hostDeviceId: sourceA.hostDeviceId, connectorId: sourceA.connectorId, port: 8101, entryPath: '/' };
  const sibling = await f.sources.execute({ op: 'apps.source.prepare', actor: owner, args: siblingArgs });
  promote(f, intent(sibling, 'take-freed-port'));
  const occupied = snapshot(f.db);
  assert.throws(() => promote(f, intent(old, 'occupied-rollback')), code('app_port_already_registered'));
  assert.deepEqual(snapshot(f.db), occupied);
});

test('history is scoped to current owner/app and cannot cross scopes with an apparently valid cursor', async t => {
  const f = await fixture(t);
  for (let i = 0; i < 3; i++) { const prepared = await prepare(f, { port: 9500 + i }); promote(f, intent(prepared, `history-${i}`)); }
  const history = (args, actor = owner) => f.sources.execute({ op: 'apps.source.history', actor, args });
  const first = history({ appId: appA, limit: 2 }); assert.deepEqual(first.targets.map(target => target.revision), [4, 3]);
  const next = await prepare(f, { port: 9600 }); promote(f, intent(next, 'during-pagination'));
  assert.deepEqual(history({ appId: appA, limit: 2, cursor: first.nextCursor }).targets.map(target => target.revision), [2, 1]);
  assert.throws(() => history({ appId: appB, cursor: first.nextCursor }), code('invalid_source_history_cursor'));
  assert.throws(() => history({ appId: appA, cursor: first.nextCursor }, outsider), code('apps_owner_required'));
  f.active(owner, false); assert.throws(() => history({ appId: appA, cursor: first.nextCursor }), code('apps_authentication_required'));
});

test('two real SQLite writers switching different apps to one free port have exactly one winner', { timeout: 15_000 }, async t => {
  const f = await fixture(t), buffer = new SharedArrayBuffer(4), gate = new Int32Array(buffer);
  const moduleUrls = { sources: new URL('../server/sources.mjs', import.meta.url).href,
    publications: new URL('../server/publications.mjs', import.meta.url).href };
  const script = `const {parentPort,workerData}=require('node:worker_threads');
    (async()=>{const {DatabaseSync}=await import('node:sqlite');
      const {createSourceRegistry}=await import(workerData.moduleUrls.sources);
      const {createPublicationRegistry}=await import(workerData.moduleUrls.publications);
      const db=new DatabaseSync(workerData.databasePath);db.exec('PRAGMA foreign_keys=ON;PRAGMA busy_timeout=5000');
      const proofs=new WeakMap();const assertActor=actor=>{if(actor.accountId!==workerData.actor.accountId||actor.deviceId!==workerData.actor.deviceId)throw new Error('fixture actor mismatch');};
      const publications=createPublicationRegistry({db,assertActor,canUse:()=>true});
      const sources=createSourceRegistry({db,assertActor,publications,
        prepareTarget:context=>{const proof={};proofs.set(proof,context.target);return proof;},
        verifyPreparedTarget:({evidence,target})=>proofs.get(evidence)===target});
      try{const prepared=await sources.execute({op:'apps.source.prepare',actor:workerData.actor,args:workerData.args});
        parentPort.postMessage({ready:true});const gate=new Int32Array(workerData.buffer);Atomics.wait(gate,0,0,8000);
        if(Atomics.load(gate,0)!==1)throw new Error('fixture start deadline');
        try{const result=sources.execute({op:'apps.source.promote',actor:workerData.actor,args:{appId:prepared.appId,requestId:workerData.requestId,
          preparationId:prepared.preparationId,expectedPolicyEpoch:prepared.expectedPolicyEpoch,expectedTargetRevision:prepared.expectedTargetRevision,launchPolicy:'restricted',listed:false}});
          parentPort.postMessage({ok:true,appId:prepared.appId,epoch:result.receipt.policyEpoch});}
        catch(error){parentPort.postMessage({ok:false,appId:prepared.appId,code:error.code});}
      }finally{sources.close();db.close();}
    })().catch(error=>parentPort.postMessage({fatal:error.code||error.message}));`;
  let ready = 0;
  const jobs = [appA, appB].map((id, index) => new Promise((resolveJob, rejectJob) => {
    let outcome;
    const worker = new Worker(script, { eval: true, workerData: { moduleUrls, databasePath: f.databasePath,
      actor: owner, args: prepareArgs(f, { id, port: 9700 }), requestId: `race-${index}`, buffer } });
    t.after(() => worker.terminate()); worker.on('error', rejectJob);
    worker.on('message', value => {
      if (value.ready) { ready++; if (ready === 2) { Atomics.store(gate, 0, 1); Atomics.notify(gate, 0, 2); } }
      else if (value.fatal) rejectJob(new Error(value.fatal)); else outcome = value;
    });
    worker.once('exit', exitCode => {
      if (exitCode !== 0 || !outcome) rejectJob(new Error(`fixture worker exit ${exitCode}`)); else resolveJob(outcome);
    });
  }));
  const results = await Promise.all(jobs);
  assert.equal(results.filter(result => result.ok).length, 1);
  const loser = results.find(result => !result.ok); assert.equal(loser.code, 'app_port_already_registered');
  assert.equal(read(f, loser.appId).activeTargetRevision, 1); assert.equal(read(f, loser.appId).requiredBindingVersion, 1);
  assert.equal(f.db.prepare('SELECT count(*) AS count FROM app_source_receipts').get().count, 1);
  assert.equal(f.db.prepare('SELECT count(*) AS count FROM app_runtime_targets WHERE revision>1').get().count, 1);
  const rows = f.db.prepare(`SELECT t.app_id FROM app_publications p JOIN app_runtime_targets t
    ON t.app_id=p.app_id AND t.revision=p.active_target_revision WHERE t.connector_key=? AND t.port=9700`).all(key(sourceB));
  assert.equal(rows.length, 1);
});
