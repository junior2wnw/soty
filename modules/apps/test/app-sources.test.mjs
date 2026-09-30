import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { Worker } from 'node:worker_threads';
import { migrateAppsSchema, ensureInitialPublication, ensureCanonicalDomain, requiredBindingVersion } from '../server/schema.mjs';
import { createPublicationRegistry } from '../server/publications.mjs';
import { createSourceRegistry } from '../server/sources.mjs';

const owner = { accountId: 'account_A', deviceId: 'browser_A' }, foreign = { accountId: 'account_B', deviceId: 'browser_B' };
const appA = `app-${'a'.repeat(32)}`, appB = `app-${'b'.repeat(32)}`, alias = `dom_${'d'.repeat(32)}`;
const keyA = 'link_A|host_A|connector_A', keyB = 'link_B|host_B|connector_B';
const deferred = () => { let resolve; const promise = new Promise(yes => { resolve = yes; }); return { promise, resolve }; };
const turn = () => new Promise(resolve => setImmediate(resolve));
const code = expected => error => error.code === expected;
const stateRows = db => Object.fromEntries(['local_apps', 'local_app_grants', 'app_runtime_targets', 'app_publications', 'app_source_heads',
  'app_publication_domains', 'app_source_receipts', 'app_domains'].map(table => [table, db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all()]));
function seedApp(db, id, port = 8080, key = keyA) {
  db.exec('BEGIN IMMEDIATE');
  try {
    db.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)').run(id, owner.accountId, key, 'Preserved name', port, '/#/dashboard',
      JSON.stringify({ accountIds: ['friend'], communityIds: ['community_A'] }), 'enabled', 7, 1, 1);
    db.prepare('INSERT INTO local_app_grants VALUES (?,?,?)').run(id, 'account', 'friend');
    db.prepare('INSERT INTO local_app_grants VALUES (?,?,?)').run(id, 'community', 'community_A');
    const app = db.prepare('SELECT * FROM local_apps WHERE id=?').get(id); ensureCanonicalDomain(db, app, ''); ensureInitialPublication(db, app);
    db.exec('COMMIT');
  } catch (error) { db.exec('ROLLBACK'); throw error; }
}
async function fixture(t, options = {}) {
  const directory = await mkdtemp(path.join(tmpdir(), 'soty-source-model-')), databasePath = path.join(directory, 'apps.sqlite');
  const db = new DatabaseSync(databasePath); db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000'); migrateAppsSchema(db); db.exec('PRAGMA journal_mode=WAL');
  for (const [key, host, connector, link] of [[keyA, 'host_A', 'connector_A', 'link_A'], [keyB, 'host_B', 'connector_B', 'link_B']]) {
    db.prepare('INSERT INTO app_devices VALUES (?,?,?,?,?)').run(key, owner.accountId, JSON.stringify({ linkId: link, hostDeviceId: host, connectorId: connector }), host, 1);
  }
  seedApp(db, appA);
  db.exec("INSERT INTO app_domain_zones VALUES ('zone_named','named','https://{slug}.apps.example','apps.example','https','',1)");
  db.prepare("INSERT INTO app_domains VALUES (?,'zone_named','named.apps.example','https://named.apps.example','named',?,?,'alias','bound',1,NULL)").run(alias, appA, owner.accountId);
  db.prepare('INSERT INTO app_publication_domains VALUES (?,?,?)').run(appA, alias, owner.accountId);
  let clock = 10_000, active = true, liveChannel = true, provider = options.prepareTarget, verifier = options.verifyPreparedTarget, notifier = options.onChanged;
  const events = [], contexts = [], registries = [], connections = [db];
  function open(connection = db) {
    const assertActor = actor => { assert.equal(active && [owner.accountId, foreign.accountId].includes(actor?.accountId), true); };
    const publications = createPublicationRegistry({ db: connection, now: () => clock, assertActor, canUse: actor => actor.accountId === owner.accountId });
    const sources = createSourceRegistry({ db: connection, now: () => clock, assertActor, publications, limits: options.limits,
      prepareTarget: async context => { assert.equal(connection.isTransaction, false); contexts.push(context); return provider ? provider(context) : { channel: 'current', target: context.target }; },
      verifyPreparedTarget: context => verifier ? verifier(context) : liveChannel && context.evidence?.target === context.target,
      onChanged: event => { events.push(event); return notifier?.(event); },
    });
    registries.push(sources); return { sources, publications };
  }
  const initial = open();
  t.after(async () => {
    for (const registry of registries) registry.close(); for (const connection of connections) connection.close();
    assert.equal(path.dirname(path.resolve(directory)), path.resolve(tmpdir())); assert.match(path.basename(directory), /^soty-source-model-/u);
    await rm(directory, { recursive: true, force: true });
  });
  const get = () => initial.publications.execute({ op: 'apps.publication.get', actor: owner, args: { appId: appA } });
  const prepareArgs = (extra = {}) => ({ appId: appA, expectedPolicyEpoch: get().policyEpoch, expectedTargetRevision: get().activeTargetRevision,
    source: { hostDeviceId: 'host_B', connectorId: 'connector_B', port: 9000, entryPath: '/next?mode=a%2Bb#part' }, ...extra });
  const prepare = args => initial.sources.execute({ op: 'apps.source.prepare', actor: owner, args: args ?? prepareArgs() });
  const intent = (prepared, requestId = 'switch-1', extra = {}) => ({ appId: appA, requestId, preparationId: prepared.preparationId,
    expectedPolicyEpoch: prepared.expectedPolicyEpoch, expectedTargetRevision: prepared.expectedTargetRevision, launchPolicy: 'restricted', listed: false, ...extra });
  const promote = args => initial.sources.execute({ op: 'apps.source.promote', actor: owner, args });
  const history = (args = {}, actor = owner) => initial.sources.execute({ op: 'apps.source.history', actor, args: { appId: appA, ...args } });
  return { db, databasePath, ...initial, get, prepareArgs, prepare, intent, promote, history, events, contexts,
    open() { const connection = new DatabaseSync(databasePath); connection.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000'); migrateAppsSchema(connection); connections.push(connection); return { db: connection, ...open(connection) }; },
    clock(value) { clock = value; }, active(value) { active = value; }, channel(value) { liveChannel = value; },
    provider(value) { provider = value; }, verifier(value) { verifier = value; }, notifier(value) { notifier = value; },
  };
}

test('prepare is transient; promotion atomically pins target, floor and epoch while preserving names, grants and addresses', async t => {
  const f = await fixture(t), before = stateRows(f.db), prepared = await f.prepare();
  assert.deepEqual(stateRows(f.db), before); assert.equal(prepared.target.revision, 2); assert.equal(prepared.expiresAt, 40_000);
  const result = f.promote(f.intent(prepared));
  assert.equal(result.replayed, false); assert.equal(result.current.policyEpoch, 2); assert.equal(result.current.activeTargetRevision, 2);
  assert.equal(result.current.requiredBindingVersion, 2); assert.equal(requiredBindingVersion(f.db, appA), 2);
  for (const table of ['local_apps', 'local_app_grants', 'app_domains', 'app_publication_domains']) assert.deepEqual(stateRows(f.db)[table], before[table]);
  assert.equal(f.events[0].oldConnectorKey, keyA); assert.equal(f.events[0].newConnectorKey, keyB);
});

test('actor and target are captured before asynchronous probe; name-only update does not invalidate source CAS', async t => {
  const waiting = deferred(), f = await fixture(t, { prepareTarget: context => waiting.promise.then(() => ({ target: context.target })) });
  const args = f.prepareArgs(), actor = { ...owner }, preparing = f.sources.execute({ op: 'apps.source.prepare', actor, args });
  await turn(); actor.accountId = foreign.accountId; args.source.port = 9999;
  assert.equal(Object.isFrozen(f.contexts[0].actor), true); assert.equal(Object.isFrozen(f.contexts[0].target), true);
  f.db.prepare('UPDATE local_apps SET name=?,revision=revision+1 WHERE id=?').run('Newer name', appA);
  waiting.resolve(); const prepared = await preparing;
  assert.equal(prepared.target.port, 9000); f.promote(f.intent(prepared));
  assert.equal(f.db.prepare('SELECT name FROM local_apps WHERE id=?').get(appA).name, 'Newer name');
});

test('fresh authorization/CAS after probe and exact channel evidence inside commit cannot be skipped', async t => {
  for (const mode of ['epoch', 'channel', 'promise']) {
    const f = await fixture(t), prepared = await f.prepare(), before = stateRows(f.db);
    if (mode === 'epoch') { f.db.exec('BEGIN IMMEDIATE'); f.publications.grantsChangedInTransaction(appA); f.db.exec('COMMIT'); }
    else if (mode === 'channel') f.channel(false); else f.verifier(() => Promise.resolve(true));
    assert.throws(() => f.promote(f.intent(prepared)), code(mode === 'epoch' ? 'app_publication_revision_conflict' : 'apps_source_preparation_stale'));
    assert.deepEqual(stateRows(f.db).app_runtime_targets, before.app_runtime_targets); assert.equal(requiredBindingVersion(f.db, appA), 1);
  }
});

test('committed lost ACK replays after a true connection reopen and expired preparation; owner is checked first', async t => {
  const f = await fixture(t), prepared = await f.prepare(), args = f.intent(prepared);
  f.notifier(() => { throw new Error('lost sync acknowledgment'); });
  assert.throws(() => f.promote(args), /lost sync acknowledgment/); assert.equal(f.get().policyEpoch, 2);
  f.sources.close(); f.clock(100_000); f.notifier(null);
  const reopened = f.open(), replay = reopened.sources.execute({ op: 'apps.source.promote', actor: owner, args });
  assert.equal(replay.replayed, true); assert.equal(replay.receipt.policyEpoch, 2); assert.equal(replay.current.policyEpoch, 2);
  assert.throws(() => reopened.sources.execute({ op: 'apps.source.promote', actor: foreign, args }), code('apps_owner_required'));
  assert.throws(() => reopened.sources.execute({ op: 'apps.source.promote', actor: owner, args: { ...args, listed: true, launchPolicy: 'anyone',
    exposureAck: { scope: 'whole-port', targetRevision: prepared.target.revision, targetDigest: prepared.target.digest, profile: prepared.target.profile } } }), code('app_source_request_conflict'));
});

test('rollback reuses immutable target1 but keeps binding2, fresh consent and a new epoch; historical replay reports current separately', async t => {
  const f = await fixture(t), initial = f.db.prepare('SELECT * FROM app_runtime_targets WHERE app_id=? AND revision=1').get(appA);
  const prepared = await f.prepare(), args = f.intent(prepared); f.promote(args);
  const restored = await f.prepare(f.prepareArgs({ source: undefined, targetRevision: 1 }));
  const ack = { scope: 'whole-port', targetRevision: 1, targetDigest: restored.target.digest, profile: restored.target.profile };
  assert.throws(() => f.promote(f.intent(restored, 'rollback', { launchPolicy: 'anyone', exposureAck: { ...ack, targetDigest: prepared.target.digest } })), code('app_exposure_ack_required'));
  const result = f.promote(f.intent(restored, 'rollback', { launchPolicy: 'anyone', listed: true, exposureAck: ack }));
  assert.equal(result.current.activeTargetRevision, 1); assert.equal(result.current.requiredBindingVersion, 2); assert.equal(result.current.policyEpoch, 3);
  assert.deepEqual(f.db.prepare('SELECT * FROM app_runtime_targets WHERE app_id=? AND revision=1').get(appA), initial);
  const replay = f.promote(args); assert.equal(replay.receipt.targetRevision, 2); assert.equal(replay.current.activeTargetRevision, 1);
  assert.equal(f.events.at(-1).newConnectorKey, keyA); assert.equal(f.events.at(-1).policyEpoch, 3);
});

test('expired and backward-clock preparations fail closed without durable target allocation', async t => {
  for (const clock of [9_999, 40_000]) {
    const f = await fixture(t), prepared = await f.prepare(), before = stateRows(f.db); f.clock(clock);
    assert.throws(() => f.promote(f.intent(prepared)), code('apps_source_preparation_expired')); assert.deepEqual(stateRows(f.db), before);
  }
});

test('prepare completion rechecks a changed policy and refuses an occupied route committed by another writer', async t => {
  const waiting = deferred(), f = await fixture(t, { prepareTarget: context => waiting.promise.then(() => ({ target: context.target })) });
  const pending = f.prepare(); await turn();
  f.db.exec('BEGIN IMMEDIATE'); f.publications.grantsChangedInTransaction(appA); f.db.exec('COMMIT'); waiting.resolve();
  await assert.rejects(pending, code('app_publication_revision_conflict'));
  f.provider(null); const prepared = await f.prepare(), other = f.open(); seedApp(other.db, appB, 9000, keyB);
  assert.throws(() => f.promote(f.intent(prepared)), code('app_port_already_registered')); assert.equal(requiredBindingVersion(f.db, appA), 1);
});

test('partial SQL failure rolls back target, head, policy and receipt together; exact prepared request can retry', async t => {
  const f = await fixture(t), prepared = await f.prepare(), before = stateRows(f.db);
  f.db.exec("CREATE TRIGGER source_fault BEFORE INSERT ON app_source_receipts BEGIN SELECT RAISE(ABORT,'injected_receipt_fault'); END");
  assert.throws(() => f.promote(f.intent(prepared)), /injected_receipt_fault/); assert.deepEqual(stateRows(f.db), before);
  f.db.exec('DROP TRIGGER source_fault'); assert.equal(f.promote(f.intent(prepared)).receipt.policyEpoch, 2);
});

test('freshness and current channel are checked again after SQL writes before commit', async t => {
  for (const mode of ['expired', 'channel']) {
    const f = await fixture(t), prepared = await f.prepare(), before = stateRows(f.db);
    f.db.function('source_change_during_sql', () => {
      if (mode === 'expired') f.clock(prepared.expiresAt); else f.channel(false);
      return 1;
    });
    f.db.exec('CREATE TRIGGER source_tail AFTER INSERT ON app_source_receipts BEGIN SELECT source_change_during_sql(); END');
    assert.throws(() => f.promote(f.intent(prepared)), code(mode === 'expired' ? 'apps_source_preparation_expired' : 'apps_source_preparation_stale'));
    assert.deepEqual(stateRows(f.db), before); assert.equal(f.events.length, 0);
  }
});

test('a synchronous verifier cannot return an already expired prepared DTO', async t => {
  const f = await fixture(t);
  f.verifier(() => { f.clock(40_000); return true; });
  await assert.rejects(f.prepare(), code('apps_source_preparation_expired'));
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM app_runtime_targets').get().n, 1);
});

test('in-flight timeout retains bounded admission until provider settles; shutdown stops queued dispatch', async t => {
  const waiting = deferred(), f = await fixture(t, { prepareTarget: context => waiting.promise.then(() => ({ target: context.target })), limits: { perApp: 1, probeMs: 15 } });
  const pending = f.prepare(), rejected = assert.rejects(pending, code('apps_source_probe_timeout'));
  await new Promise(resolve => setTimeout(resolve, 30)); await rejected;
  assert.equal(f.contexts[0].signal.aborted, true);
  await assert.rejects(f.prepare(), code('apps_source_preparation_capacity'));
  waiting.resolve(); await turn(); f.provider(null); await f.prepare();
  const second = await fixture(t); const notDispatched = second.prepare(); second.sources.close();
  await assert.rejects(notDispatched, code('apps_closed')); assert.equal(second.contexts.length, 0);
});

test('committed source receipts keep the latest64 epochs independently of publication receipts and equal timestamps', async t => {
  const f = await fixture(t); let first;
  for (let index = 0; index < 66; index++) {
    const prepared = await f.prepare(f.prepareArgs({ source: undefined, targetRevision: 1 })), args = f.intent(prepared, `switch-${index}`);
    if (!first) first = args; f.promote(args);
  }
  const receipts = f.db.prepare('SELECT committed_epoch FROM app_source_receipts ORDER BY committed_epoch').all();
  assert.equal(receipts.length, 64); assert.equal(receipts[0].committed_epoch, 4); assert.equal(receipts.at(-1).committed_epoch, 67);
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM app_runtime_targets').get().n, 1);
  assert.throws(() => f.promote(first), code('app_publication_revision_conflict'));
});

test('history is owner-only, bounded and anchored across new inserts; target revision grows from maximum after rollback', async t => {
  const f = await fixture(t);
  for (let index = 0; index < 3; index++) { const p = await f.prepare(); f.promote(f.intent(p, `forward-${index}`)); }
  const first = f.history({ limit: 2 }); assert.deepEqual(first.targets.map(value => value.revision), [4, 3]);
  const rollback = await f.prepare(f.prepareArgs({ source: undefined, targetRevision: 1 })); f.promote(f.intent(rollback, 'rollback'));
  const next = await f.prepare(); assert.equal(next.target.revision, 5); f.promote(f.intent(next, 'after-rollback'));
  assert.deepEqual(f.history({ limit: 2, cursor: first.nextCursor }).targets.map(value => value.revision), [2, 1]);
  assert.throws(() => f.history({}, foreign), code('apps_owner_required'));
  assert.throws(() => f.history({ limit: 51 }), code('invalid_source_history_limit'));
  assert.throws(() => f.history({ cursor: 'bad' }), code('invalid_source_history_cursor'));
});

test('two actual SQLite writers can commit only one source change at the same policy epoch', async t => {
  const f = await fixture(t), barrier = new SharedArrayBuffer(8), gate = new Int32Array(barrier);
  const moduleUrl = new URL('../server/sources.mjs', import.meta.url).href, publicationUrl = new URL('../server/publications.mjs', import.meta.url).href;
  const script = `const { parentPort, workerData } = require('node:worker_threads');
    (async()=>{const {DatabaseSync}=await import('node:sqlite');const {createSourceRegistry}=await import(workerData.moduleUrl);const {createPublicationRegistry}=await import(workerData.publicationUrl);
    const db=new DatabaseSync(workerData.databasePath);db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000');
    const actor=workerData.owner;const publications=createPublicationRegistry({db,assertActor:()=>{},canUse:()=>true});
    const sources=createSourceRegistry({db,publications,assertActor:()=>{},prepareTarget:async context=>({target:context.target}),verifyPreparedTarget:({evidence,target})=>evidence.target===target});
    try {const prepared=await sources.execute({op:'apps.source.prepare',actor,args:workerData.args});
      const gate=new Int32Array(workerData.barrier);Atomics.add(gate,0,1);parentPort.postMessage({ready:true});Atomics.wait(gate,1,0);
      try {const value=sources.execute({op:'apps.source.promote',actor,args:{appId:workerData.args.appId,requestId:workerData.requestId,preparationId:prepared.preparationId,expectedPolicyEpoch:1,expectedTargetRevision:1,launchPolicy:'restricted',listed:false}});parentPort.postMessage({ok:true,epoch:value.receipt.policyEpoch});}
      catch(error){parentPort.postMessage({ok:false,code:error.code});}
    }finally{sources.close();db.close();}})().catch(error=>parentPort.postMessage({fatal:error.code||error.message}));`;
  const workers = [];
  try {
    const results = ['one', 'two'].map(requestId => new Promise((resolve, reject) => {
      const worker = new Worker(script, { eval: true, workerData: { moduleUrl, publicationUrl, databasePath: f.databasePath, barrier, owner, args: f.prepareArgs(), requestId } });
      workers.push(worker); let result;
      worker.on('error', reject); worker.on('message', message => {
        if (message.ready) { if (Atomics.load(gate, 0) === 2) { Atomics.store(gate, 1, 1); Atomics.notify(gate, 1, 2); } }
        else { if (message.fatal) reject(new Error(message.fatal)); else result = message; }
      });
      // A result message precedes the worker's finally/db.close. Success is
      // complete only after exit, before the fixture can unlink SQLite WAL/SHM.
      worker.once('exit', exitCode => {
        if (exitCode !== 0 || !result) reject(new Error(`Source writer exited ${exitCode} without a completed result`));
        else resolve(result);
      });
    }));
    const values = await Promise.all(results);
    assert.equal(values.filter(value => value.ok).length, 1); assert.equal(values.find(value => !value.ok).code, 'app_publication_revision_conflict');
    assert.equal(f.get().policyEpoch, 2); assert.equal(f.db.prepare('SELECT count(*) AS n FROM app_runtime_targets').get().n, 2);
    assert.equal(f.db.prepare('SELECT count(*) AS n FROM app_source_receipts').get().n, 1);
  } finally {
    // On one worker's failure, await its sibling's termination here, not in a
    // later t.after hook registered behind the fixture directory cleanup.
    await Promise.all(workers.map(worker => worker.terminate()));
  }
});
