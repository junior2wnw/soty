import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { Worker } from 'node:worker_threads';
import { createAppsService } from '../server/index.mjs';
import { migrateAppsSchema, APPS_REGISTRY_SCHEMA } from '../server/schema.mjs';

const owner = { accountId: 'acct_owner', deviceId: 'dev_owner' };
const other = { accountId: 'acct_other', deviceId: 'dev_other' };
const appA = `app-${'a'.repeat(32)}`, appB = `app-${'b'.repeat(32)}`, appC = `app-${'c'.repeat(32)}`;
const legacy = 'https://{appId}.legacy.example';
const named = 'https://apps.example';
const serviceUrl = new URL('../server/index.mjs', import.meta.url).href;

function v1(db) {
  db.exec(`CREATE TABLE apps_meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
    INSERT INTO apps_meta VALUES ('schema','soty.apps-registry.v1');
    CREATE TABLE app_devices (connector_key TEXT PRIMARY KEY,owner_account_id TEXT NOT NULL,identity_json TEXT NOT NULL,name TEXT NOT NULL,created_at INTEGER NOT NULL);
    CREATE TABLE local_apps (id TEXT PRIMARY KEY,owner_account_id TEXT NOT NULL,connector_key TEXT NOT NULL REFERENCES app_devices(connector_key),name TEXT NOT NULL,port INTEGER NOT NULL,entry_path TEXT NOT NULL,grants_json TEXT NOT NULL,state TEXT NOT NULL,revision INTEGER NOT NULL,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL);
    CREATE INDEX local_apps_owner ON local_apps(owner_account_id);
    CREATE TABLE local_app_grants (app_id TEXT NOT NULL REFERENCES local_apps(id) ON DELETE CASCADE,kind TEXT NOT NULL CHECK(kind IN ('account','community')),principal_id TEXT NOT NULL,PRIMARY KEY(app_id,kind,principal_id));
    CREATE INDEX local_app_grants_principal ON local_app_grants(kind,principal_id,app_id);`);
  for (const actor of [owner, other]) {
    const identity = { linkId: `link_${actor.accountId}`, hostDeviceId: `host_${actor.accountId}`, connectorId: `connector_${actor.accountId}` };
    db.prepare('INSERT INTO app_devices VALUES (?,?,?,?,?)').run(actor.accountId, actor.accountId, JSON.stringify(identity), 'Computer', 1);
  }
  const insert = db.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)');
  insert.run(appA, owner.accountId, owner.accountId, 'Private', 9001, '/', JSON.stringify({ accountIds: [], communityIds: [] }), 'enabled', 7, 1, 2);
  insert.run(appB, other.accountId, other.accountId, 'Shared', 9002, '/start', JSON.stringify({ accountIds: [owner.accountId], communityIds: ['community_test'] }), 'enabled', 2, 2, 3);
  insert.run(appC, owner.accountId, owner.accountId, 'Another private app', 9003, '/', JSON.stringify({ accountIds: [], communityIds: [] }), 'enabled', 1, 3, 3);
  // v1 used grants_json as authority. Stale optimization rows must not become new grants.
  db.prepare('INSERT INTO local_app_grants VALUES (?,?,?)').run(appA, 'account', other.accountId);
}

async function fixture(t, options = {}) {
  const dir = await mkdtemp(join(tmpdir(), 'soty-domains-'));
  const databasePath = join(dir, 'apps.sqlite');
  const db = new DatabaseSync(databasePath); v1(db); db.close();
  const config = { databasePath, appOriginTemplate: legacy, namedAppZone: named,
    shellOrigins: ['https://soty.example'], actorActive: actor => [owner, other].some(item => item.accountId === actor?.accountId && item.deviceId === actor?.deviceId),
    ...options };
  const opened = [], inspectionDbs = [];
  const open = overrides => { const service = createAppsService({ ...config, ...overrides }); opened.push(service); return service; };
  const inspect = () => { const connection = new DatabaseSync(databasePath); connection.exec('PRAGMA busy_timeout=5000;'); inspectionDbs.push(connection); return connection; };
  t.after(async () => { for (const connection of inspectionDbs) connection.close(); for (const service of opened) service.close(); await rm(dir, { recursive: true, force: true }); });
  return { databasePath, config, open, inspect };
}
const call = (service, op, args, actor = owner) => service.execute({ actor, op, args });
const claim = (service, slug, requestId = `req_${slug}`, extras = {}, actor = owner) => call(service, 'apps.domains.claim',
  { appId: appA, slug, requestId, expectedDomainsRevision: 0, ...extras }, actor);

test('v1 migration preserves app identity, exact ACL JSON and revisions; stale grant index is repaired', async t => {
  const f = await fixture(t);
  const beforeDb = new DatabaseSync(f.databasePath); const before = beforeDb.prepare('SELECT * FROM local_apps ORDER BY id').all(); beforeDb.close();
  const service = f.open();
  const db = f.inspect();
  assert.equal(db.prepare("SELECT value FROM apps_meta WHERE key='schema'").get().value, APPS_REGISTRY_SCHEMA);
  assert.equal(db.prepare('PRAGMA user_version').get().user_version, 3);
  assert.deepEqual(db.prepare('SELECT * FROM local_apps ORDER BY id').all(), before);
  assert.deepEqual(db.prepare('SELECT principal_id FROM local_app_grants WHERE app_id=?').all(appA), []);
  assert.equal(db.prepare('SELECT count(*) AS n FROM local_app_grants WHERE app_id=?').get(appB).n, 2);
  const view = call(service, 'apps.domains.get', { appId: appA });
  assert.equal(view.revision, 0); assert.equal(view.canonicalOrigin, `https://${appA}.legacy.example`);
  assert.equal(view.domains.length, 1); assert.equal(view.domains[0].role, 'canonical');
  assert.equal(call(service, 'apps.list', {}, other).apps.some(item => item.id === appA), false);
  service.close();
  const reopened = f.open(); assert.deepEqual(call(reopened, 'apps.domains.get', { appId: appA }), view);
});

test('an empty legacy template preserves null canonical origin and is pinned across reopen', async t => {
  const f = await fixture(t, { appOriginTemplate: '' }); const service = f.open();
  const view = call(service, 'apps.domains.get', { appId: appA });
  assert.equal(view.canonicalOrigin, null); assert.deepEqual(view.domains, []);
  const created = claim(service, 'my-app'); assert.equal(created.receipt.origin, 'https://my-app.apps.example');
  assert.equal(call(service, 'apps.domains.get', { appId: appA }).canonicalOrigin, null);
  service.close();
  assert.throws(() => f.open({ appOriginTemplate: legacy }), /apps_origin_template_changed/u);
  assert.equal(call(f.open(), 'apps.domains.get', { appId: appA }).canonicalOrigin, null);
});

test('unknown and structurally unrecognized schemas fail before schema or journal mutations', async t => {
  for (const mutation of ["UPDATE apps_meta SET value='future.schema'", 'ALTER TABLE local_apps ADD COLUMN unsupported TEXT', 'CREATE TABLE sqliteExtra(value TEXT)']) {
    const f = await fixture(t); const db = new DatabaseSync(f.databasePath); db.exec(mutation); db.close();
    const before = await readFile(f.databasePath);
    assert.throws(() => f.open(), /apps_schema_unsupported/u);
    assert.deepEqual(await readFile(f.databasePath), before);
  }
});

test('failed migration rolls back grants and schema atomically; v2 refuses legacy origin drift', async t => {
  const f = await fixture(t); const db = new DatabaseSync(f.databasePath);
  db.prepare('UPDATE local_apps SET grants_json=? WHERE id=?').run('{bad json', appB);
  assert.throws(() => migrateAppsSchema(db, { legacyTemplate: legacy }), /apps_registry_corrupt/u);
  assert.equal(db.prepare("SELECT value FROM apps_meta WHERE key='schema'").get().value, 'soty.apps-registry.v1');
  assert.equal(db.prepare("SELECT count(*) AS n FROM sqlite_schema WHERE type='table' AND name='app_domains'").get().n, 0);
  assert.equal(db.prepare('SELECT count(*) AS n FROM local_app_grants WHERE app_id=?').get(appA).n, 1); db.close();
  const clean = await fixture(t); clean.open().close();
  assert.throws(() => clean.open({ appOriginTemplate: 'https://{appId}.different.example' }), /apps_origin_template_changed/u);
});

test('claim keeps the canonical private address and produces a stable receipt through lost ACK and retirement', async t => {
  const f = await fixture(t); const service = f.open();
  const initial = claim(service, 'My-App', 'retry-claim');
  assert.equal(initial.requestId, 'retry-claim'); assert.match(initial.receipt.requestKeyHash, /^[a-f0-9]{64}$/u);
  assert.equal(initial.receipt.id, undefined);
  const view = call(service, 'apps.domains.get', { appId: appA });
  assert.equal(view.canonicalOrigin, `https://${appA}.legacy.example`);
  const alias = view.domains.find(item => item.role === 'alias');
  assert.equal(alias.origin, 'https://my-app.apps.example'); assert.equal(alias.runtimeMode, 'status-only'); assert.equal(alias.runtimeReady, false);
  assert.equal(view.revision, 1); assert.equal(initial.replayed, false);
  assert.throws(() => claim(service, 'other-name', 'retry-claim'), /app_domain_request_conflict/u);
  service.close();
  const reopened = f.open({ namedAppZone: '' });
  const replay = claim(reopened, 'my-app', 'retry-claim');
  assert.equal(replay.requestId, 'retry-claim');
  assert.equal(replay.replayed, true); assert.deepEqual(replay.receipt, initial.receipt);
  const retireArgs = { appId: appA, domainId: alias.id, requestId: 'retire-claim', expectedDomainsRevision: 1 };
  const retired = call(reopened, 'apps.domains.retire', retireArgs);
  assert.equal(retired.requestId, 'retire-claim');
  assert.equal(retired.receipt.state, 'tombstone');
  assert.deepEqual(call(reopened, 'apps.domains.retire', retireArgs).receipt, retired.receipt);
  assert.deepEqual(claim(reopened, 'my-app', 'retry-claim').receipt, initial.receipt, 'receipt is a historical mutation fact');
  assert.equal(call(reopened, 'apps.domains.get', { appId: appA }).domains.find(item => item.id === alias.id).state, 'tombstone');
  assert.throws(() => claim(reopened, 'new-name', 'new-claim', { expectedDomainsRevision: 2 }), /apps_named_zone_disabled/u);
});

test('current owner and actor are required; expected account guard precedes all new operations', async t => {
  const f = await fixture(t); const service = f.open();
  for (const op of ['apps.domains.get', 'apps.names.check', 'apps.domains.claim', 'apps.domains.retire']) {
    assert.throws(() => call(service, op, { expectedAccountId: owner.accountId }, other), /authentication_required/u);
  }
  assert.throws(() => claim(service, 'hijacked', 'foreign-claim', {}, other), /apps_owner_required/u);
  assert.throws(() => call(service, 'apps.domains.get', { appId: appA }, other), /apps_owner_required/u);
  assert.throws(() => claim(service, 'hijacked', 'foreign-claim', {}, { ...owner, deviceId: 'forged' }), /apps_authentication_required/u);
  assert.equal(call(service, 'apps.domains.get', { appId: appA }).revision, 0);
  let checks = 0;
  const revoking = f.open({ actorActive: actor => actor.accountId === owner.accountId && ++checks < 3 });
  assert.throws(() => claim(revoking, 'denied-inside', 'inside'), /apps_authentication_required/u);
  assert.equal(call(service, 'apps.domains.get', { appId: appA }).revision, 0);
});

test('reserved/malformed names, stale revision, canonical retirement and unexpected args fail closed', async t => {
  const f = await fixture(t); const service = f.open();
  for (const slug of ['ab', '-abc', 'abc-', 'a.b', 'name/path', 'name@host', ' name', 'имя', 'a'.repeat(49), '', null]) {
    assert.throws(() => claim(service, slug), /invalid_app_slug/u);
  }
  for (const slug of ['api', 'WWW', 'app-custom', 'xn--example']) {
    assert.equal(call(service, 'apps.names.check', { slug }).reason, 'reserved');
    assert.throws(() => claim(service, slug), /app_name_reserved/u);
  }
  assert.throws(() => claim(service, 'good-name', 'bad-rev', { expectedDomainsRevision: 1 }), /app_domains_revision_conflict/u);
  assert.throws(() => claim(service, 'good-name', 'bad-args', { accountId: other.accountId }), /unexpected_argument/u);
  const canonical = call(service, 'apps.domains.get', { appId: appA }).domains[0];
  assert.throws(() => call(service, 'apps.domains.retire', { appId: appA, domainId: canonical.id, requestId: 'retire-canonical', expectedDomainsRevision: 0 }), /app_canonical_domain_immutable/u);
});

test('tombstones cannot be reclaimed by another app and consume both pilot quotas', async t => {
  const f = await fixture(t, { domainLimits: { perApp: 1, perAccount: 1 } }); const service = f.open();
  const created = claim(service, 'reserved-forever');
  call(service, 'apps.domains.retire', { appId: appA, domainId: created.receipt.domainId, requestId: 'retire-name', expectedDomainsRevision: 1 });
  assert.throws(() => claim(service, 'another-name', 'quota-app', { expectedDomainsRevision: 2 }), /apps_domain_limit_reached/u);
  assert.throws(() => claim(service, 'another-name', 'quota-account', { appId: appC }), /apps_domain_limit_reached/u);
  assert.throws(() => claim(service, 'reserved-forever', 'steal-tombstone', { appId: appB }, other), /app_name_unavailable/u);
  assert.deepEqual(call(service, 'apps.names.check', { slug: 'reserved-forever' }, other), { slug: 'reserved-forever', available: false, reason: 'unavailable' });
  assert.equal(call(service, 'apps.domains.get', { appId: appA }).limits.usedByAccount, 1);
});

test('named zone is initially disabled and cannot drift after its first explicit configuration', async t => {
  const f = await fixture(t, { namedAppZone: '' }); const closed = f.open();
  assert.deepEqual(call(closed, 'apps.names.check', { slug: 'test-app' }), { slug: 'test-app', available: false, reason: 'disabled' });
  assert.throws(() => claim(closed, 'test-app'), /apps_named_zone_disabled/u); closed.close();
  f.open({ namedAppZone: named }).close();
  assert.throws(() => f.open({ namedAppZone: 'https://different.example' }), /apps_named_zone_changed/u);
  assert.equal(call(f.open(), 'apps.names.check', { slug: 'test-app' }).reason, 'disabled');
});

function competingCall(databasePath, domainLimits, actor, args) {
  const worker = new Worker(`
    const { parentPort, workerData } = require('node:worker_threads');
    (async () => {
      const { createAppsService } = await import(workerData.serviceUrl);
      const service = createAppsService({ databasePath: workerData.databasePath, appOriginTemplate: workerData.legacy,
        namedAppZone: workerData.named, domainLimits: workerData.domainLimits, shellOrigins: ['https://soty.example'],
        actorActive: actor => actor.accountId === workerData.actor.accountId && actor.deviceId === workerData.actor.deviceId });
      parentPort.postMessage({ ready: true });
      parentPort.once('message', () => {
        let outcome;
        try { outcome = { result: service.execute({ actor: workerData.actor, op: 'apps.domains.claim', args: workerData.args }) }; }
        catch (error) { outcome = { error: error.code || 'unexpected_error', status: error.status }; }
        finally { service.close(); parentPort.postMessage(outcome); parentPort.close(); }
      });
    })().catch(error => { parentPort.postMessage({ fatal: error.code || error.message }); parentPort.close(); });
  `, { eval: true, workerData: { databasePath, domainLimits, actor, args, legacy, named, serviceUrl } });
  const ready = new Promise((resolve, reject) => { worker.once('error', reject); worker.once('message', value => value.ready ? resolve() : reject(new Error(value.fatal || 'worker not ready'))); });
  const result = new Promise((resolve, reject) => { worker.on('message', value => { if (!value.ready) resolve(value); }); worker.once('error', reject); });
  const exited = new Promise((resolve, reject) => { worker.once('exit', code => code === 0 ? resolve() : reject(new Error(`worker exited ${code}`))); worker.once('error', reject); });
  return { worker, ready, result, exited };
}
async function compete(t, f, cases, limits = {}) {
  f.open().close();
  const racers = cases.map(item => competingCall(f.databasePath, limits, item.actor, item.args));
  t.after(async () => { await Promise.all(racers.map(item => item.worker.terminate())); });
  await Promise.all(racers.map(item => item.ready));
  for (const racer of racers) racer.worker.postMessage('go');
  const results = await Promise.all(racers.map(item => item.result));
  await Promise.all(racers.map(item => item.exited));
  return results;
}

test('two independent SQLite workers cannot claim the same normalized hostname', async t => {
  const f = await fixture(t);
  const results = await compete(t, f, [
    { actor: owner, args: { appId: appA, slug: 'One-Name', requestId: 'first-worker', expectedDomainsRevision: 0 } },
    { actor: other, args: { appId: appB, slug: 'one-name', requestId: 'second-worker', expectedDomainsRevision: 0 } },
  ]);
  assert.equal(results.filter(item => item.result).length, 1);
  assert.deepEqual(results.filter(item => item.error).map(item => item.error), ['app_name_unavailable']);
  const db = f.inspect();
  assert.equal(db.prepare("SELECT count(*) AS n FROM app_domains WHERE role='alias'").get().n, 1);
  assert.equal(db.prepare('SELECT count(*) AS n FROM app_domain_receipts').get().n, 1);
});

test('account quota is atomic across separate app heads and SQLite workers', async t => {
  const f = await fixture(t);
  const results = await compete(t, f, [
    { actor: owner, args: { appId: appA, slug: 'quota-one', requestId: 'quota-one', expectedDomainsRevision: 0 } },
    { actor: owner, args: { appId: appC, slug: 'quota-two', requestId: 'quota-two', expectedDomainsRevision: 0 } },
  ], { perApp: 3, perAccount: 1 });
  assert.equal(results.filter(item => item.result).length, 1);
  assert.deepEqual(results.filter(item => item.error).map(item => item.error), ['apps_domain_limit_reached']);
});

test('simultaneous retries share one receipt and a new request key cannot manufacture a no-op receipt', async t => {
  const f = await fixture(t);
  const args = { appId: appA, slug: 'retry-together', requestId: 'one-request', expectedDomainsRevision: 0 };
  const results = await compete(t, f, [{ actor: owner, args }, { actor: owner, args }]);
  assert.ok(results.every(item => item.result));
  assert.deepEqual(results[0].result.receipt, results[1].result.receipt);
  assert.deepEqual(results.map(item => item.result.replayed).sort(), [false, true]);
  const service = f.open();
  assert.throws(() => call(service, 'apps.domains.claim', { ...args, requestId: 'pretend-new', expectedDomainsRevision: 1 }), /app_name_unavailable/u);
  assert.equal(f.inspect().prepare('SELECT count(*) AS n FROM app_domain_receipts').get().n, 1);
});
