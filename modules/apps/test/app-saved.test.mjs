import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { Worker } from 'node:worker_threads';
import { createSavedRegistry, SAVED_LIMITS } from '../server/saved.mjs';
import { AppsError } from '../server/protocol.mjs';
import { migrateAppsSchema, ensureInitialPublication, ensureCanonicalDomain } from '../server/schema.mjs';

const owner = { accountId: 'saved_A', deviceId: 'browser_A' }, other = { accountId: 'saved_B', deviceId: 'browser_B' };
const template = 'https://{appId}.saved.example';
const id = n => `app-${n.toString(16).padStart(32, '0')}`;
const op = (registry, suffix, args, actor = owner) => registry.execute({ op: `apps.saved.${suffix}`, args, actor });
const code = value => error => error.code === value;
const snapshot = db => Object.fromEntries(['app_saved_heads', 'app_saved_entries', 'app_saved_receipts']
  .map(table => [table, db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all()]));

async function fixture(t) {
  const directory = await mkdtemp(path.join(tmpdir(), 'soty-saved-model-')), filename = path.join(directory, 'apps.sqlite');
  const connections = new Set(), unavailable = new Set(); let active = true, clock = 500, calls = 0;
  const openDb = () => { const db = new DatabaseSync(filename); db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=2000'); connections.add(db); return db; };
  const db = openDb(); migrateAppsSchema(db, { legacyTemplate: template }); db.exec('PRAGMA journal_mode=WAL');
  t.after(async () => {
    for (const connection of connections) connection.close();
    assert.equal(path.dirname(path.resolve(directory)), path.resolve(tmpdir())); assert.match(path.basename(directory), /^soty-saved-model-/u);
    await rm(directory, { recursive: true, force: true });
  });
  function seed(n, entryPath = '/#/dashboard') {
    const app = id(n), account = `maker_${Math.floor(n / 90)}`, key = `link_${account}|host_${account}|connector_${account}`;
    db.exec('BEGIN IMMEDIATE');
    try {
      db.prepare('INSERT OR IGNORE INTO app_devices VALUES (?,?,?,?,?)').run(key, account,
        JSON.stringify({ linkId: `link_${account}`, hostDeviceId: `host_${account}`, connectorId: `connector_${account}` }), 'Source', 1);
      db.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)').run(app, account, key, `Application ${n}`, 8000 + n, entryPath,
        '{"accountIds":["saved_A","saved_B"],"communityIds":[]}', 'enabled', 1, 1, 1);
      const row = db.prepare('SELECT * FROM local_apps WHERE id=?').get(app);
      ensureCanonicalDomain(db, row, template); ensureInitialPublication(db, row); db.exec('COMMIT');
    } catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
    return app;
  }
  function resolver(connection, input) {
    assert.equal(connection.isTransaction, true); calls++;
    if (unavailable.has(input.appId)) return null;
    const app = connection.prepare('SELECT * FROM local_apps WHERE id=?').get(input.appId);
    const domain = input.domainId ? connection.prepare('SELECT * FROM app_domains WHERE id=? AND app_id=?').get(input.domainId, input.appId)
      : connection.prepare("SELECT * FROM app_domains WHERE app_id=? AND role='canonical'").get(input.appId);
    if (!app || app.state !== 'enabled' || domain?.state !== 'bound') return null;
    return { appId: app.id, domainId: domain.id, origin: domain.origin, path: input.path ?? app.entry_path,
      name: app.name, status: 'offline', canManage: app.owner_account_id === input.actor.accountId };
  }
  function registry(connection = db, overrides = {}) {
    return createSavedRegistry({ db: connection, now: () => clock,
      assertActor: actor => { if (!active || ![owner.accountId, other.accountId].includes(actor?.accountId)) throw new AppsError('apps_authentication_required', 401); },
      withAuthorityFence: fn => { assert.equal(connection.isTransaction, false); return fn(); },
      resolveEntry: input => resolver(connection, input), ...overrides });
  }
  const saved = registry();
  function set(appId, savedState = true, extra = {}) {
    const expectedRevision = op(saved, 'get', { appId }).revision;
    return op(saved, 'set', { appId, saved: savedState, expectedRevision, requestId: `request_${expectedRevision + 1}`, ...extra });
  }
  return { db, filename, connections, openDb, registry, saved, seed, set, unavailable, resolver,
    active: value => { active = value; }, clock: value => { clock = value; }, calls: () => calls };
}

test('saved exact entry survives close/reopen; receipt remains historical after removal and access loss', async t => {
  const f = await fixture(t), app = f.seed(1), args = { appId: app, saved: true, expectedRevision: 0, requestId: 'lost-ack', path: '/доска?tag=a%2Bb#пункт' };
  const first = op(f.saved, 'set', args); assert.equal(first.current.entry.path, args.path); assert.equal(first.current.entry.current.status, 'offline');
  f.db.close(); f.connections.delete(f.db);
  const reopened = f.openDb(); migrateAppsSchema(reopened, { legacyTemplate: template }); const saved = f.registry(reopened);
  assert.deepEqual(op(saved, 'set', args), { ...first, replayed: true });
  op(saved, 'set', { appId: app, saved: false, expectedRevision: 1, requestId: 'remove' }); f.unavailable.add(app);
  const replay = op(saved, 'set', args);
  assert.deepEqual(replay.receipt, first.receipt); assert.deepEqual(replay.current, { revision: 2, entry: null });
  assert.equal(reopened.prepare('SELECT count(*) AS n FROM app_saved_entries').get().n, 0);
});

test('unavailable entries keep only their own snapshot and remove works without resolving any app', async t => {
  const f = await fixture(t), app = f.seed(1); const first = f.set(app);
  f.db.prepare('UPDATE local_apps SET name=? WHERE id=?').run('Secret new name', app); f.unavailable.add(app);
  const unavailable = op(f.saved, 'get', { appId: app }).entry;
  assert.equal(unavailable.label, first.current.entry.label); assert.equal(unavailable.current, null);
  assert.equal(JSON.stringify(unavailable).includes('Secret'), false);
  assert.equal(op(f.saved, 'get', { appId: app }, other).entry, null);
  const before = f.calls(); const removed = op(f.saved, 'set', { appId: app, saved: false, expectedRevision: 1, requestId: 'remove' });
  assert.equal(removed.current.entry, null); assert.equal(f.calls(), before);
});

test('retired saved alias stays unavailable and never falls back to the canonical entry', async t => {
  const f = await fixture(t), app = f.seed(1), domain = `dom_${'d'.repeat(32)}`;
  f.db.exec("INSERT INTO app_domain_zones VALUES ('named','named','https://{slug}.named.example','named.example','https','',1)");
  f.db.prepare("INSERT INTO app_domains VALUES (?,'named','chosen.named.example','https://chosen.named.example','chosen',?,'maker_0','alias','bound',1,NULL)").run(domain, app);
  f.set(app, true, { domainId: domain }); f.db.prepare("UPDATE app_domains SET state='tombstone',retired_at=2 WHERE id=?").run(domain);
  const stored = op(f.saved, 'get', { appId: app }).entry;
  assert.equal(stored.domainId, domain); assert.equal(stored.origin, 'https://chosen.named.example'); assert.equal(stored.current, null);
  assert.throws(() => f.set(app, true, { domainId: domain }), code('app_unavailable'));
  assert.equal(op(f.saved, 'get', { appId: app }).revision, 1);
});

test('all accepted desired states advance the account head; payload reuse and wrong actor never reuse receipts', async t => {
  const f = await fixture(t), app = f.seed(1);
  const args = { appId: app, saved: false, expectedRevision: 0, requestId: 'same' };
  assert.equal(op(f.saved, 'set', args).receipt.revision, 1);
  assert.equal(op(f.saved, 'set', { ...args, expectedRevision: 1, requestId: 'noop' }).receipt.revision, 2);
  assert.equal(op(f.saved, 'set', args).replayed, true);
  assert.throws(() => op(f.saved, 'set', { ...args, saved: true }), code('apps_saved_request_conflict'));
  assert.equal(op(f.saved, 'set', args, other).receipt.revision, 1);
  f.active(false); assert.throws(() => op(f.saved, 'set', args), code('apps_authentication_required'));
});

test('receipt retention uses committed revision despite reversed time, and pruned save cannot resurrect', async t => {
  const f = await fixture(t), app = f.seed(1), original = { appId: app, saved: true, expectedRevision: 0, requestId: 'old-save' };
  op(f.saved, 'set', original); f.set(app, false);
  for (let index = 2; index < 130; index++) { f.clock(500 - index); f.set(app, false); }
  const receiptRows = f.db.prepare('SELECT committed_revision FROM app_saved_receipts ORDER BY committed_revision').all();
  assert.equal(receiptRows.length, 128); assert.equal(receiptRows[0].committed_revision, 3);
  assert.throws(() => op(f.saved, 'set', original), code('apps_saved_revision_conflict'));
  assert.equal(op(f.saved, 'get', { appId: app }).entry, null); assert.equal(op(f.saved, 'get', { appId: app }).revision, 130);
});

test('quota200 rejects only new saves; existing updates and removals still commit', async t => {
  const f = await fixture(t);
  for (let n = 1; n <= 201; n++) f.seed(n);
  for (let n = 1; n <= 200; n++) f.set(id(n));
  const before = snapshot(f.db);
  assert.throws(() => f.set(id(201)), code('apps_saved_capacity')); assert.deepEqual(snapshot(f.db), before);
  f.set(id(1), true, { path: '/updated' }); f.set(id(2), false); f.set(id(201));
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM app_saved_entries').get().n, 200);
  assert.equal(op(f.saved, 'get', { appId: id(1) }).entry.path, '/updated');
});

test('pagination packs actual Unicode bytes, is ordered, bounded, account/device scoped and revision stable', async t => {
  const f = await fixture(t), longPath = '/' + 'a'.repeat(7400) + '界'.repeat(50);
  for (let n = 1; n <= 55; n++) f.set(f.seed(n, longPath));
  const first = op(f.saved, 'list', { limit: 50 });
  assert.ok(first.entries.length < 50 && first.entries.length > 0); assert.ok(first.nextCursor);
  let page = first, seen = [];
  do {
    assert.ok(Buffer.byteLength(JSON.stringify(page)) <= SAVED_LIMITS.responseBytes);
    seen.push(...page.entries.map(item => item.appId));
    page = page.nextCursor ? op(f.saved, 'list', { limit: 50, cursor: page.nextCursor }) : null;
  } while (page);
  assert.deepEqual(seen, Array.from({ length: 55 }, (_, index) => id(55 - index)));
  assert.throws(() => op(f.saved, 'list', { cursor: first.nextCursor }, other), code('invalid_saved_cursor'));
  assert.throws(() => op(f.saved, 'list', { cursor: first.nextCursor }, { ...owner, deviceId: 'other_browser' }), code('invalid_saved_cursor'));
  f.set(id(1), false); assert.throws(() => op(f.saved, 'list', { cursor: first.nextCursor }), code('apps_saved_cursor_expired'));
});

test('strict scalar inputs reject coercible arrays and unknown fields without writes', async t => {
  const f = await fixture(t), app = f.seed(1), domain = f.db.prepare('SELECT id FROM app_domains WHERE app_id=?').get(app).id;
  const baseline = { appId: app, saved: true, expectedRevision: 0, requestId: 'valid' };
  const variants = [{ appId: [app] }, { saved: 1 }, { saved: [true] }, { expectedRevision: '0' }, { expectedRevision: [0] },
    { requestId: ['valid'] }, { domainId: [domain] }, { path: ['/path'] }, { path: '//external' }, { path: '/_soty/boot' }, { extra: true }];
  for (const value of variants) assert.throws(() => op(f.saved, 'set', { ...baseline, ...value }));
  for (const value of [{ limit: [20] }, { limit: 0 }, { limit: 51 }, { cursor: ['abc'] }, { cursor: null }]) assert.throws(() => op(f.saved, 'list', value));
  for (const value of [{ domainId: domain }, { domainId: undefined }, { path: '/x' }]) assert.throws(() => op(f.saved, 'set', { ...baseline, saved: false, ...value }));
  assert.deepEqual(snapshot(f.db), { app_saved_heads: [], app_saved_entries: [], app_saved_receipts: [] });
});

test('missing/async/delayed/nested fence cannot dispatch a new effect', async t => {
  const f = await fixture(t), app = f.seed(1), args = { appId: app, saved: true, expectedRevision: 0, requestId: 'guard' };
  const before = snapshot(f.db); let late;
  for (const fence of [undefined, async fn => fn(), () => undefined, fn => { late = fn; return Promise.resolve(); }]) {
    assert.throws(() => op(f.registry(f.db, { withAuthorityFence: fence }), 'set', args));
    assert.deepEqual(snapshot(f.db), before);
  }
  assert.throws(() => late(), code('apps_authority_fence_invalid')); assert.deepEqual(snapshot(f.db), before);
  f.db.exec('BEGIN IMMEDIATE');
  try { assert.throws(() => op(f.saved, 'set', args)); } finally { f.db.exec('ROLLBACK'); }
  assert.deepEqual(snapshot(f.db), before);
});

test('async or malformed resolver and internal failures roll back instead of becoming unavailable', async t => {
  const f = await fixture(t), app = f.seed(1), args = { appId: app, saved: true, expectedRevision: 0, requestId: 'guard' }, before = snapshot(f.db);
  for (const resolveEntry of [async () => null, () => undefined, input => ({ ...f.resolver(f.db, input), domainId: ['bad'] }),
    input => ({ ...f.resolver(f.db, input), status: ['offline'] }), input => ({ ...f.resolver(f.db, input), path: '/_soty/boot' }),
    input => ({ ...f.resolver(f.db, input), path: '/' + '界'.repeat(8000) }), () => { throw new Error('synthetic-storage-failure'); }]) {
    assert.throws(() => op(f.registry(f.db, { resolveEntry }), 'set', args)); assert.deepEqual(snapshot(f.db), before);
  }
});

test('receipt insert and retention failure roll back head, entry and previous receipts atomically', async t => {
  const f = await fixture(t), app = f.seed(1); for (let n = 0; n < 128; n++) f.set(app, false);
  const before = snapshot(f.db);
  f.db.exec("CREATE TRIGGER saved_test_prune BEFORE DELETE ON app_saved_receipts BEGIN SELECT RAISE(ABORT,'synthetic_prune_failure'); END");
  assert.throws(() => f.set(app), /synthetic_prune_failure/u); assert.deepEqual(snapshot(f.db), before); f.db.exec('DROP TRIGGER saved_test_prune');
  f.db.exec("CREATE TRIGGER saved_test_insert BEFORE INSERT ON app_saved_receipts BEGIN SELECT RAISE(ABORT,'synthetic_insert_failure'); END");
  assert.throws(() => f.set(app), /synthetic_insert_failure/u); assert.deepEqual(snapshot(f.db), before); f.db.exec('DROP TRIGGER saved_test_insert');
  assert.equal(f.set(app).receipt.revision, 129);
});

test('host release failure after Apps commit is an unknown ACK; exact retry returns the retained receipt', async t => {
  const f = await fixture(t), app = f.seed(1), args = { appId: app, saved: true, expectedRevision: 0, requestId: 'lost' };
  const registry = f.registry(f.db, { withAuthorityFence: fn => { fn(); throw new Error('synthetic-release-failure'); } });
  assert.throws(() => op(registry, 'set', args), /synthetic-release-failure/u);
  const value = op(f.saved, 'set', args); assert.equal(value.replayed, true); assert.equal(value.current.revision, 1);
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM app_saved_receipts').get().n, 1);
});

test('last safe revision can be listed, then exhaustion refuses another mutation', async t => {
  const f = await fixture(t), app = f.seed(1); f.set(app, false);
  f.db.prepare('UPDATE app_saved_heads SET revision=? WHERE account_id=?').run(Number.MAX_SAFE_INTEGER - 1, owner.accountId);
  f.set(app); assert.equal(op(f.saved, 'list', {}).entries[0].savedRevision, Number.MAX_SAFE_INTEGER);
  assert.throws(() => f.set(app, false), code('apps_saved_revision_exhausted'));
});

test('a competing real writer causes a bounded busy refusal without effects, restores timeout and permits exact retry', async t => {
  const f = await fixture(t), app = f.seed(1), blocker = f.openDb();
  f.db.exec('PRAGMA busy_timeout=1729');
  const before = snapshot(f.db), args = { appId: app, saved: true, expectedRevision: 0, requestId: 'busy-retry' };
  blocker.exec('BEGIN IMMEDIATE');
  try {
    assert.throws(() => op(f.saved, 'set', args), error => error.code === 'apps_saved_busy' && error.status === 503);
    assert.equal(f.db.prepare('PRAGMA busy_timeout').get().timeout, 1729); assert.deepEqual(snapshot(f.db), before);
  } finally { blocker.exec('ROLLBACK'); }
  assert.equal(op(f.saved, 'set', args).receipt.revision, 1);
  assert.equal(f.db.prepare('PRAGMA busy_timeout').get().timeout, 1729);
});

test('two real SQLite writers with one expected account revision commit exactly one save', async t => {
  const f = await fixture(t); f.seed(1); f.seed(2);
  const moduleUrl = new URL('../server/saved.mjs', import.meta.url).href;
  const control = new SharedArrayBuffer(4), gate = new Int32Array(control);
  const workerSource = `const {parentPort,workerData}=require('node:worker_threads');
    (async()=>{const{DatabaseSync}=await import('node:sqlite');const{createSavedRegistry}=await import(workerData.moduleUrl);
    const db=new DatabaseSync(workerData.filename);db.exec('PRAGMA foreign_keys=ON;PRAGMA busy_timeout=2000');
    let result;try{const registry=createSavedRegistry({db,assertActor:()=>true,withAuthorityFence:fn=>fn(),resolveEntry:({appId,path})=>{
      const a=db.prepare('SELECT * FROM local_apps WHERE id=?').get(appId),d=db.prepare('SELECT * FROM app_domains WHERE app_id=?').get(appId);
      return {appId,domainId:d.id,origin:d.origin,path:path??a.entry_path,name:a.name,status:'offline',canManage:false};}});
      parentPort.postMessage({ready:true});Atomics.wait(new Int32Array(workerData.control),0,0);
      try{result={ok:registry.execute({actor:workerData.actor,op:'apps.saved.set',args:workerData.args}).receipt.revision};}
      catch(error){result={code:error.code};}}finally{db.close();}parentPort.postMessage({result});})().catch(error=>{throw error;});`;
  const start = n => new Promise((resolve, reject) => {
    let result, ready = false;
    const worker = new Worker(workerSource, { eval: true, execArgv: [], workerData: { moduleUrl, filename: f.filename, control, actor: owner,
      args: { appId: id(n), saved: true, expectedRevision: 0, requestId: `writer_${n}` } } });
    worker.on('error', reject); worker.on('message', message => {
      if (message.ready) { ready = true; Atomics.add(gate, 0, 0); readiness++; if (readiness === 2) { Atomics.store(gate, 0, 1); Atomics.notify(gate, 0); } }
      if (message.result) result = message.result;
    });
    worker.once('exit', exit => exit === 0 && ready && result ? resolve(result) : reject(new Error(`worker_failed_${exit}`)));
  });
  let readiness = 0;
  const results = await Promise.all([start(1), start(2)]);
  assert.equal(results.filter(value => value.ok === 1).length, 1); assert.equal(results.filter(value => value.code === 'apps_saved_revision_conflict').length, 1);
  assert.equal(op(f.saved, 'list', {}).entries.length, 1); assert.equal(op(f.saved, 'list', {}).revision, 1);
});
