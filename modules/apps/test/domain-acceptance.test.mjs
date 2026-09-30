import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { Worker } from 'node:worker_threads';
import { createAppsService } from '../server/index.mjs';
import { ensureCanonicalDomain, migrateAppsSchema } from '../server/schema.mjs';

const alice = { accountId: 'acceptance_alice', deviceId: 'acceptance_alice_device' };
const bob = { accountId: 'acceptance_bob', deviceId: 'acceptance_bob_device' };
const apps = [`app-${'1'.repeat(32)}`, `app-${'2'.repeat(32)}`, `app-${'3'.repeat(32)}`];
const legacy = 'https://{appId}.legacy.example';
const named = 'https://apps.example';
const moduleUrl = new URL('../server/index.mjs', import.meta.url).href;
const call = (service, op, args, actor = alice) => service.execute({ op, args, actor });
const claimArgs = (slug, requestId, appId = apps[0], expectedDomainsRevision = 0) =>
  ({ appId, slug, requestId, expectedDomainsRevision, expectedAccountId: appId === apps[1] ? bob.accountId : alice.accountId });

async function fixture(t, options = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-domain-independent-'));
  const databasePath = join(directory, 'apps.sqlite');
  const connections = [], services = [];
  const db = new DatabaseSync(databasePath);
  try {
    db.exec('PRAGMA foreign_keys=ON');
    migrateAppsSchema(db, { legacyTemplate: legacy });
    for (const actor of [alice, bob]) {
      const identity = { linkId: `link_${actor.accountId}`, hostDeviceId: `host_${actor.accountId}`, connectorId: `connector_${actor.accountId}` };
      db.prepare('INSERT INTO app_devices VALUES (?,?,?,?,?)').run(actor.accountId, actor.accountId, JSON.stringify(identity), 'Acceptance device', 1);
    }
    db.exec('BEGIN IMMEDIATE');
    for (const [index, id] of apps.entries()) {
      const owner = index === 1 ? bob : alice;
      db.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)')
        .run(id, owner.accountId, owner.accountId, `App ${index}`, 9200 + index, '/',
          JSON.stringify({ accountIds: [], communityIds: [] }), 'enabled', 1, 1, 1);
      ensureCanonicalDomain(db, { id, owner_account_id: owner.accountId, created_at: 1 }, legacy);
    }
    db.exec('COMMIT');
  } finally { db.close(); }
  const configuration = { databasePath, appOriginTemplate: legacy, namedAppZone: named,
    shellOrigins: ['https://shell.example'],
    actorActive: actor => [alice, bob].some(expected => actor?.accountId === expected.accountId && actor.deviceId === expected.deviceId),
    ...options };
  const open = overrides => {
    const service = createAppsService({ ...configuration, ...overrides }); services.push(service); return service;
  };
  const inspect = () => {
    const connection = new DatabaseSync(databasePath); connection.exec('PRAGMA busy_timeout=5000'); connections.push(connection); return connection;
  };
  t.after(async () => {
    for (const service of services) service.close();
    for (const connection of connections) connection.close();
    assert.equal(dirname(resolve(directory)), resolve(tmpdir()));
    assert.ok(basename(directory).startsWith('soty-domain-independent-'));
    await rm(directory, { recursive: true, force: true });
  });
  return { databasePath, open, inspect };
}

test('a partial v2 schema cannot be accepted or receive a new named zone', async t => {
  const changes = [
    ['missing canonical uniqueness', 'DROP INDEX app_domain_canonical'],
    ['nonunique canonical index with the right name', `DROP INDEX app_domain_canonical;
      CREATE INDEX app_domain_canonical ON app_domains(app_id) WHERE role='canonical'`],
    ['missing head foreign key', `DROP TABLE app_domain_heads;
      CREATE TABLE app_domain_heads(app_id TEXT PRIMARY KEY,revision INTEGER NOT NULL CHECK(revision>=0));
      INSERT INTO app_domain_heads SELECT id,0 FROM local_apps`],
    ['nullable revision', `DROP TABLE app_domain_heads;
      CREATE TABLE app_domain_heads(app_id TEXT PRIMARY KEY REFERENCES local_apps(id),revision INTEGER CHECK(revision>=0));
      INSERT INTO app_domain_heads SELECT id,0 FROM local_apps`],
  ];
  for (const [description, sql] of changes) {
    await t.test(description, async subtest => {
      const f = await fixture(subtest);
      const db = new DatabaseSync(f.databasePath); db.exec(sql); db.close();
      const before = await readFile(f.databasePath);
      assert.throws(() => f.open(), /apps_schema_unsupported/u);
      assert.deepEqual(await readFile(f.databasePath), before, 'format rejection must not migrate or set WAL');
      assert.equal(f.inspect().prepare("SELECT count(*) AS n FROM app_domain_zones WHERE kind='named'").get().n, 0);
    });
  }
});

test('disabling new claims retains the named-zone admission boundary before any further write', async t => {
  const f = await fixture(t); f.open().close();
  const before = await readFile(f.databasePath);
  assert.throws(() => f.open({ namedAppZone: '', shellOrigins: ['https://child.apps.example'] }),
    /apps_named_zone_shell_overlap/u);
  let checked = 0;
  assert.throws(() => f.open({ namedAppZone: '', validateNamedZone: origin => {
    checked++; assert.equal(origin, named); throw new Error('acceptance_retained_policy_denied');
  } }), /acceptance_retained_policy_denied/u);
  assert.ok(checked > 0, 'the policy must be called for a retained zone when current configuration is empty');
  assert.deepEqual(await readFile(f.databasePath), before);
  assert.throws(() => f.open({ validateNamedZone: () => false }), /invalid_named_app_zone_validator/u);
  assert.throws(() => f.open({ validateNamedZone: () => Promise.resolve(true) }), /invalid_named_app_zone_validator/u);
  assert.deepEqual(await readFile(f.databasePath), before);
  const service = f.open({ namedAppZone: '' });
  assert.equal(call(service, 'apps.domains.get', { appId: apps[0] }).canonicalOrigin, `https://${apps[0]}.legacy.example`);
});

test('receipt storage failure rolls the hostname and revision back; the original request can then retry once', async t => {
  const f = await fixture(t), service = f.open(), db = f.inspect();
  // Fault injection on an already opened connection simulates failure after the
  // alias INSERT, at the final durable receipt write. This is not a schema to admit.
  db.exec(`CREATE TRIGGER acceptance_receipt_failure BEFORE INSERT ON app_domain_receipts
    BEGIN SELECT RAISE(ABORT,'simulated_domain_storage_failure'); END`);
  const args = claimArgs('atomic-receipt', 'receipt-failure-request');
  assert.throws(() => call(service, 'apps.domains.claim', args), /simulated_domain_storage_failure/u);
  assert.equal(db.prepare("SELECT count(*) AS n FROM app_domains WHERE role='alias'").get().n, 0);
  assert.equal(db.prepare('SELECT revision FROM app_domain_heads WHERE app_id=?').get(apps[0]).revision, 0);
  assert.equal(db.prepare('SELECT count(*) AS n FROM app_domain_receipts').get().n, 0);
  db.exec('DROP TRIGGER acceptance_receipt_failure');
  const accepted = call(service, 'apps.domains.claim', args);
  const replayed = call(service, 'apps.domains.claim', args);
  assert.equal(accepted.receipt.revision, 1);
  assert.equal(accepted.replayed, false); assert.equal(replayed.replayed, true);
  assert.equal(accepted.requestId, args.requestId); assert.equal(replayed.requestId, args.requestId);
  assert.deepEqual(replayed.receipt, accepted.receipt);
  assert.equal(db.prepare('SELECT count(*) AS n FROM app_domain_receipts').get().n, 1);
});

test('request identity is account-wide while two different accounts remain independent', async t => {
  const f = await fixture(t), service = f.open();
  const requestId = 'shared-client-request-key';
  const first = call(service, 'apps.domains.claim', claimArgs('alice-first', requestId));
  assert.throws(() => call(service, 'apps.domains.claim', claimArgs('alice-second', requestId, apps[2])),
    /app_domain_request_conflict/u);
  const other = call(service, 'apps.domains.claim', claimArgs('bob-first', requestId, apps[1]), bob);
  assert.equal(other.replayed, false);
  assert.notEqual(first.receipt.domainId, other.receipt.domainId);
  assert.equal(first.receipt.requestKeyHash, other.receipt.requestKeyHash);
  assert.equal(call(service, 'apps.domains.get', { appId: apps[2] }).revision, 0);
  assert.throws(() => call(service, 'apps.domains.get', { appId: apps[0] }, bob), /apps_owner_required/u);
});

test('a receipt remains historical after app revocation but is not returned to a revoked actor', async t => {
  let active = true;
  const f = await fixture(t, { actorActive: actor => active && actor?.accountId === alice.accountId && actor.deviceId === alice.deviceId });
  const service = f.open(), args = claimArgs('historical-name', 'historical-request');
  const original = call(service, 'apps.domains.claim', args);
  call(service, 'apps.revoke', { appId: apps[0], expectedAccountId: alice.accountId });
  assert.deepEqual(call(service, 'apps.domains.claim', args).receipt, original.receipt);
  assert.throws(() => call(service, 'apps.domains.claim', claimArgs('after-revoke', 'new-request', apps[0], 1)), /app_revoked/u);
  const retired = call(service, 'apps.domains.retire', { appId: apps[0], domainId: original.receipt.domainId,
    expectedDomainsRevision: 1, requestId: 'retire-revoked-app-name' });
  assert.equal(retired.receipt.state, 'tombstone');
  assert.equal(call(service, 'apps.domains.claim', args).receipt.state, 'bound');
  assert.equal(call(service, 'apps.domains.get', { appId: apps[0] }).domains.find(item => item.id === original.receipt.domainId).state, 'tombstone');
  active = false;
  for (const [op, input] of [['apps.domains.claim', args], ['apps.domains.get', { appId: apps[0] }], ['apps.names.check', { slug: 'historical-name' }]]) {
    assert.throws(() => call(service, op, input), /apps_authentication_required/u);
  }
});

async function prepareWorker(t, databasePath, args) {
  const worker = new Worker(`
    const { parentPort, workerData } = require('node:worker_threads');
    (async () => {
      const { createAppsService } = await import(workerData.moduleUrl);
      const service = createAppsService({ databasePath: workerData.databasePath,
        appOriginTemplate: workerData.legacy, namedAppZone: workerData.named,
        shellOrigins: ['https://shell.example'],
        actorActive: actor => actor.accountId === workerData.actor.accountId && actor.deviceId === workerData.actor.deviceId });
      parentPort.once('message', () => {
        let result;
        try { result = { value: service.execute({ actor: workerData.actor, op: 'apps.domains.claim', args: workerData.args }) }; }
        catch (error) { result = { error: error.code || error.message }; }
        finally { service.close(); }
        parentPort.postMessage(result); parentPort.close();
      });
      parentPort.postMessage({ ready: true });
    })().catch(error => { parentPort.postMessage({ fatal: error.code || error.message }); parentPort.close(); });
  `, { eval: true, workerData: { moduleUrl, databasePath, legacy, named, actor: alice, args } });
  t.after(() => worker.terminate());
  const exited = new Promise((resolveExit, reject) => { worker.once('exit', code => code === 0 ? resolveExit() : reject(new Error(`worker_exit_${code}`))); worker.once('error', reject); });
  await new Promise((resolveReady, reject) => { worker.once('message', value => value.ready ? resolveReady() : reject(new Error(value.fatal || 'worker_not_ready'))); worker.once('error', reject); });
  return { worker, exited, result: new Promise((resolveResult, reject) => { worker.once('message', resolveResult); worker.once('error', reject); }) };
}

test('independent writers with different names still obey the same app revision', { timeout: 15_000 }, async t => {
  const f = await fixture(t); f.open().close();
  const racers = await Promise.all([
    prepareWorker(t, f.databasePath, claimArgs('cas-left', 'cas-left-request')),
    prepareWorker(t, f.databasePath, claimArgs('cas-right', 'cas-right-request')),
  ]);
  for (const racer of racers) racer.worker.postMessage('go');
  const results = await Promise.all(racers.map(racer => racer.result));
  await Promise.all(racers.map(racer => racer.exited));
  assert.equal(results.filter(result => result.value).length, 1);
  assert.deepEqual(results.filter(result => result.error).map(result => result.error), ['app_domains_revision_conflict']);
  const service = f.open(), view = call(service, 'apps.domains.get', { appId: apps[0] });
  assert.equal(view.revision, 1);
  assert.equal(view.domains.filter(domain => domain.role === 'alias').length, 1);
  assert.equal(f.inspect().prepare('SELECT count(*) AS n FROM app_domain_receipts').get().n, 1);
});
