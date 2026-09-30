import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { readFileSync } from 'node:fs';
import { Worker } from 'node:worker_threads';
import { dirname, join } from 'node:path';
import { createHistoricalCapabilitiesV1 } from '../../../deploy/connector/capabilities-v1.fixture.mjs';
import { initializeCapabilitiesSchema as historicalInitialize } from '../../../deploy/connector/fixtures/capabilities-v1/schema.mjs';
import { initializeCapabilitiesSchema, inspectCapabilitiesSchema, CAPABILITIES_SUPPORTED_SCHEMA_VERSIONS } from '../server/schema-v2.mjs';
import { createAccessStore } from '../server/access.mjs';
import { createCatalog } from '../server/catalog.mjs';
import { createNativeBaselineGuard } from '../server/native-baseline.mjs';
import { migrateNotes } from '../../notes/server/schema-v2.mjs';
import { createNotesService } from '../../notes/server/index.mjs';
import { AccessError } from '../server/validation.mjs';
import { PROJECT, OWNER, AUDIENCE, TEST_CATALOG, code, fileHashes, fixture, transaction,
  access, issue, seedInvocation, startFixture, terminalFixture, ledger } from './support/native-storage.mjs';

const options = Object.freeze({ projectId: PROJECT });
const migrate = db => initializeCapabilitiesSchema(db, { ...options, allowNativeMigration: true });
function domainSnapshot(db) {
  return db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name GLOB 'cap_*' AND name!='cap_metadata' ORDER BY name")
    .all().filter(row => row.name !== 'cap_native_note_intents')
    .map(({ name }) => [name, db.prepare(`SELECT * FROM ${name} ORDER BY rowid`).all().map(row => ({ ...row }))]);
}

test('trusted project and strict migration admission are required; new and literal v1 default to actual v1', t => {
  const f = fixture(t, { version: 0 });
  assert.deepEqual(CAPABILITIES_SUPPORTED_SCHEMA_VERSIONS, [1, 2]);
  for (const invalid of [undefined, {}, { projectId: '' }, { projectId: '../tenant' }, { projectId: PROJECT, other: true },
    ...[null, 0, 1, '', 'true', [], {}].map(allowNativeMigration => ({ projectId: PROJECT, allowNativeMigration }))]) {
    assert.throws(() => initializeCapabilitiesSchema(f.db, invalid));
    assert.equal(f.db.prepare('PRAGMA user_version').get().user_version, 0);
  }
  assert.deepEqual(initializeCapabilitiesSchema(f.db, options), { schemaVersion: 1, registryId: null });
  const memory = new DatabaseSync(':memory:'); t.after(() => memory.close());
  createHistoricalCapabilitiesV1(memory);
  assert.deepEqual(initializeCapabilitiesSchema(memory, options), { schemaVersion: 1, registryId: null });
  assert.equal(f.db.prepare("SELECT count(*) AS n FROM sqlite_schema WHERE name='cap_native_note_intents'").get().n, 0);
});

test('v1 grants, credentials, quota and real generic invocation history survive explicit migration unchanged', t => {
  const f = fixture(t, { version: 1 });
  let service = f.service(); const identity = issue(service);
  const pending = seedInvocation(f, service, identity, { native: false, key: 'historical_pending' });
  const completed = seedInvocation(f, service, identity, { native: false, key: 'historical_completed' });
  service.invocations.beginDispatch({ invocationId: completed });
  service.invocations.recordResult({ invocationId: completed, status: 'succeeded', effectState: 'none', effects: [],
    receipt: { verificationMethod: 'handler_assertion', artifacts: [] }, disposition: 'spent',
    actualCharges: [{ unit: 'invocations', amount: 1 }] });
  f.close(service);
  const before = domainSnapshot(f.db), state = migrate(f.db);
  assert.equal(state.schemaVersion, 2); assert.match(state.registryId, /^[a-f0-9]{32}$/u);
  assert.deepEqual(domainSnapshot(f.db), before);
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM cap_native_note_intents').get().n, 0);
  assert.deepEqual(initializeCapabilitiesSchema(f.db, options), state);
  service = f.service();
  assert.equal(service.schemaVersion, 2); assert.equal(service.registryId, state.registryId);
  const actor = service.authenticateCredential({ token: identity.token, audience: AUDIENCE });
  assert.equal(service.invocations.get({ actor, invocationId: pending }).invocation.status, 'accepted');
  assert.equal(service.invocations.get({ actor, invocationId: completed }).invocation.status, 'succeeded');
  assert.ok(service.invocations.peekDispatch().some(row => row.invocationId === pending), 'legacy Notes invocations are not silently converted to native');
});

test('fault immediately before migration COMMIT rolls back all v2 objects and generated identity', t => {
  const f = fixture(t, { version: 1 }), before = domainSnapshot(f.db);
  const wrapper = { prepare: f.db.prepare.bind(f.db), get isTransaction() { return f.db.isTransaction; },
    exec(sql) { if (sql === 'COMMIT') throw new Error('test_commit_fault'); return f.db.exec(sql); } };
  assert.throws(() => migrate(wrapper), /test_commit_fault/u);
  assert.deepEqual(inspectCapabilitiesSchema(f.db, options), { schemaVersion: 1, registryId: null });
  assert.deepEqual(domainSnapshot(f.db), before);
  assert.equal(f.db.prepare("SELECT count(*) AS n FROM cap_metadata WHERE key IN ('project_id','registry_id')").get().n, 0);
  assert.equal(migrate(f.db).schemaVersion, 2);
});

test('future3 and altered critical objects fail before any persistent journal-mode write', t => {
  for (const damage of ['future', 'index', 'trigger']) {
    const f = fixture(t);
    if (damage === 'future') f.db.exec('PRAGMA user_version=3');
    if (damage === 'index') f.db.exec('DROP INDEX cap_invocations_native_identity');
    if (damage === 'trigger') f.db.exec("DROP TRIGGER cap_native_receipt_no_delete; CREATE TRIGGER cap_native_receipt_no_delete BEFORE DELETE ON cap_receipts BEGIN SELECT 1; END");
    f.db.exec('PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE');
    const before = fileHashes(f.file);
    assert.throws(() => migrate(f.db), code(damage === 'future' ? 'schema_version_unsupported' : 'schema_layout_invalid'));
    assert.equal(f.db.prepare('PRAGMA journal_mode').get().journal_mode, 'delete');
    assert.deepEqual(fileHashes(f.file), before);
  }
});

test('project mismatch and absent mandatory v2 identity cannot generate a new identity', t => {
  const f = fixture(t), original = inspectCapabilitiesSchema(f.db, options);
  assert.throws(() => initializeCapabilitiesSchema(f.db, { projectId: 'other-project', allowNativeMigration: true }), code('capabilities_project_mismatch'));
  assert.deepEqual(inspectCapabilitiesSchema(f.db, options), original);
  const sql = f.db.prepare("SELECT sql FROM sqlite_schema WHERE name='cap_identity_no_delete'").get().sql;
  f.db.exec('DROP TRIGGER cap_identity_no_delete');
  f.db.exec("DELETE FROM cap_metadata WHERE key='registry_id'"); f.db.exec(sql);
  assert.throws(() => migrate(f.db), code('schema_lineage_mismatch'));
  assert.equal(f.db.prepare("SELECT count(*) AS n FROM cap_metadata WHERE key='registry_id'").get().n, 0);
});

test('two actual SQLite writers migrate once and observe the same durable registry identity', { timeout: 10000 }, async t => {
  const f = fixture(t, { version: 1 });
  const gate = new SharedArrayBuffer(4), source = new URL('../server/schema-v2.mjs', import.meta.url).href;
  const work = `const { parentPort, workerData } = require('node:worker_threads');
    (async () => { const { DatabaseSync } = await import('node:sqlite');
      const { initializeCapabilitiesSchema } = await import(workerData.source);
      const db = new DatabaseSync(workerData.file); parentPort.postMessage({ ready: true });
      Atomics.wait(new Int32Array(workerData.gate), 0, 0);
      let value;
      try { value = initializeCapabilitiesSchema(db, { projectId: workerData.project, allowNativeMigration: true }); }
      finally { db.close(); }
      parentPort.postMessage({ value });
    })().catch(error => { throw error; });`;
  const workers = [], values = [], ready = new Set();
  for (let i = 0; i < 2; i++) {
    const worker = new Worker(work, { eval: true, workerData: { source, file: f.file, project: PROJECT, gate } }); workers.push(worker);
    values.push(new Promise((resolve, reject) => {
      let result;
      worker.once('error', reject); worker.on('message', message => {
        if (message.ready) {
          ready.add(worker);
          if (ready.size === 2) { Atomics.store(new Int32Array(gate), 0, 1); Atomics.notify(new Int32Array(gate), 0); }
        } else result = message.value;
      });
      worker.once('exit', code => code === 0 && result ? resolve(result) : reject(new Error(`worker_exit_${code}`)));
    }));
  }
  t.after(async () => { await Promise.all(workers.map(worker => worker.terminate())); });
  const results = await Promise.all(values);
  assert.deepEqual(results[0], results[1]); assert.equal(results[0].schemaVersion, 2);
  assert.deepEqual(initializeCapabilitiesSchema(f.db, options), results[0]);
});

test('mixed store bootstrap is readable and restarting never invents the missing registry ID', t => {
  const caps = fixture(t, { version: 1 }), notes = new DatabaseSync(':memory:'); t.after(() => notes.close());
  const n2 = migrateNotes(notes, PROJECT, { allowNativeMigration: true });
  assert.equal(n2.schemaVersion, 2);
  assert.deepEqual(initializeCapabilitiesSchema(caps.db, options), { schemaVersion: 1, registryId: null });
  const c2 = migrate(caps.db);
  assert.notEqual(c2.registryId, n2.registryId);
  assert.deepEqual(migrateNotes(notes, PROJECT), n2); assert.deepEqual(initializeCapabilitiesSchema(caps.db, options), c2);
  const otherNotes = new DatabaseSync(':memory:'); t.after(() => otherNotes.close());
  assert.deepEqual(migrateNotes(otherNotes, PROJECT), { schemaVersion: 1, registryId: null });
  assert.deepEqual(initializeCapabilitiesSchema(caps.db, options), c2);
});

test('native identity, input and final receipt guards reject overwrite, replacement and deletion', t => {
  const f = fixture(t), service = f.service(), identity = issue(service);
  const id = seedInvocation(f, service, identity), before = ledger(f, id);
  for (const statement of [
    "UPDATE cap_native_note_intents SET notes_store_id='bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'",
    'DELETE FROM cap_native_note_intents',
    'INSERT OR REPLACE INTO cap_native_note_intents SELECT * FROM cap_native_note_intents',
    "UPDATE cap_invocations SET input_json='null'",
    "UPDATE cap_invocations SET input_json='{}'",
    'UPDATE cap_native_note_intents SET input_purged_at=1000',
    "UPDATE cap_metadata SET value='other' WHERE key='project_id'",
    "INSERT OR REPLACE INTO cap_metadata VALUES('registry_id','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb')",
    "DELETE FROM cap_metadata WHERE key='registry_id'",
  ]) assert.throws(() => f.db.exec(statement), /immutable/u);
  assert.deepEqual(ledger(f, id), before);
  startFixture(f, id);
  assert.throws(() => f.db.exec('UPDATE cap_native_note_intents SET started_at=NULL'), /immutable/u);
  terminalFixture(f, id);
  for (const sql of ['UPDATE cap_receipts SET value_json=value_json', 'DELETE FROM cap_receipts',
    'INSERT OR REPLACE INTO cap_receipts SELECT * FROM cap_receipts',
    'UPDATE cap_native_note_intents SET input_purged_at=NULL']) assert.throws(() => f.db.exec(sql), /immutable/u);
  assert.equal(initializeCapabilitiesSchema(f.db, options).schemaVersion, 2);
});

test('reader refuses partial native state rather than repairing a started marker or missing quota', t => {
  for (const damage of ['marker', 'reservation', 'input']) {
    const f = fixture(t), service = f.service(), identity = issue(service), id = seedInvocation(f, service, identity);
    f.close(service);
    if (damage === 'marker') f.db.prepare('UPDATE cap_native_note_intents SET started_at=1000 WHERE invocation_id=?').run(id);
    if (damage === 'reservation') f.db.prepare('DELETE FROM cap_budget_reservations WHERE invocation_id=?').run(id);
    if (damage === 'input') {
      const trigger = f.db.prepare("SELECT sql FROM sqlite_schema WHERE name='cap_native_note_input_guard'").get().sql;
      f.db.exec('DROP TRIGGER cap_native_note_input_guard');
      f.db.prepare("UPDATE cap_invocations SET input_json='null' WHERE id=?").run(id); f.db.exec(trigger);
    }
    const before = ledger(f, id);
    assert.throws(() => initializeCapabilitiesSchema(f.db, options), code('capabilities_storage_corrupt'));
    assert.deepEqual(ledger(f, id), before);
  }
});

test('baseline never sends native records through generic workers and cancellation never refunds an unverified effect', t => {
  for (const started of [false, true]) {
    const f = fixture(t), service = f.service(), identity = issue(service), id = seedInvocation(f, service, identity);
    if (started) startFixture(f, id);
    const before = ledger(f, id);
    assert.deepEqual(service.invocations.peekDispatch(), []);
    for (const action of [
      () => service.invocations.beginDispatch({ invocationId: id }),
      () => service.invocations.bindJob({ invocationId: id, internalRequestId: before.invocation.internal_request_id, jobId: 'job_not_native' }),
      () => service.invocations.markUncertain({ invocationId: id }),
      () => service.invocations.recordResult({ invocationId: id, status: 'cancelled', disposition: 'released',
        receipt: { verificationMethod: 'unverified', artifacts: [] } }),
    ]) assert.throws(action, code('native_reconciliation_required'));
    assert.deepEqual(ledger(f, id), before);
    const result = service.invocations.requestCancel({ actor: identity.actor, invocationId: id }).invocation;
    assert.equal(result.status, 'cancel_requested'); assert.equal(result.cancelRequested, true);
    assert.equal(result.effectState, started ? 'unknown' : 'none');
    const after = ledger(f, id);
    assert.equal(after.invocation.completed_at, null); assert.equal(after.receipt, null);
    assert.equal(after.budget.reserved_amount, 1); assert.equal(after.budget.spent_amount, 0);
    assert.equal(after.invocation.input_json, before.invocation.input_json); assert.equal(after.intent.input_purged_at, null);
    f.close(service); assert.equal(f.service().schemaVersion, 2);
  }
});

test('revocation reconciliation holds native quota and body; a direct internal generic settlement cannot bypass it', t => {
  const f = fixture(t), service = f.service(), identity = issue(service), id = seedInvocation(f, service, identity);
  startFixture(f, id); access(service, 'grants.revoke', { grantId: identity.grant.id });
  assert.throws(() => service.invocations.get({ actor: identity.actor, invocationId: id }));
  const result = service.invocations.reconcileAuthorization({ invocationId: id });
  assert.equal(result.authorized, false); assert.equal(result.invocation.status, 'cancel_requested');
  assert.equal(result.invocation.effectState, 'unknown');
  const before = ledger(f, id);
  assert.equal(before.budget.reserved_amount, 1); assert.equal(before.receipt, null);
  const store = createAccessStore({ db: f.db, clock: f.time, actorActive: () => false, catalog: createCatalog(TEST_CATALOG),
    transaction: fn => transaction(f.db, fn) });
  for (const disposition of ['spent', 'released']) assert.throws(() => transaction(f.db, () => store.settleBudget({
    reservationId: before.invocation.reservation_id, disposition })), code('native_reconciliation_required'));
  assert.deepEqual(ledger(f, id), before);
  f.close(service); assert.equal(f.service().schemaVersion, 2);
});

test('an already opened v1 reader detects a new native marker after another compatible writer migrates', t => {
  const f = fixture(t, { version: 1 }), service = f.service(), identity = issue(service);
  const guard = createNativeBaselineGuard({ db: f.db, error: code => new AccessError(code) });
  assert.equal(guard.available(), false); migrate(f.db);
  const id = seedInvocation(f, service, identity);
  assert.equal(guard.available(), true); assert.equal(guard.find(id).invocation_id, id);
  assert.throws(() => service.invocations.beginDispatch({ invocationId: id }), code('native_reconciliation_required'));
  assert.equal(service.invocations.requestCancel({ actor: identity.actor, invocationId: id }).invocation.status, 'cancel_requested');
  assert.equal(ledger(f, id).budget.reserved_amount, 1);
});

test('terminal native facts remain readable when disabled and are never reopened by generic paths', t => {
  for (const status of ['succeeded', 'failed', 'cancelled']) {
    const f = fixture(t); let service = f.service(); const identity = issue(service), id = seedInvocation(f, service, identity);
    startFixture(f, id); terminalFixture(f, id, status); f.close(service);
    service = f.service({ catalog: undefined });
    const actor = service.authenticateCredential({ token: identity.token, audience: AUDIENCE });
    const before = ledger(f, id), read = service.invocations.get({ actor, invocationId: id }).invocation;
    assert.equal(read.status, status); assert.deepEqual(read.receipt.artifacts.map(item => item.revision), status === 'succeeded' ? [1] : []);
    assert.equal(service.invocations.requestCancel({ actor, invocationId: id }).invocation.status, status);
    assert.equal(service.invocations.reconcileAuthorization({ invocationId: id }).invocation.status, status);
    assert.throws(() => service.invocations.beginDispatch({ invocationId: id }));
    assert.deepEqual(ledger(f, id), before);
    assert.equal(before.invocation.input_json, 'null'); assert.ok(before.intent.input_purged_at !== null);
  }
});

test('new admission indexes cover bounded account/principal/rate/nonterminal queries', t => {
  const f = fixture(t), service = f.service(), identity = issue(service);
  seedInvocation(f, service, identity);
  const cases = [
    ["SELECT id FROM cap_invocations WHERE account_id=? AND created_at>=? ORDER BY created_at,id LIMIT 31", [OWNER.accountId, 0], 'cap_invocations_account_admission'],
    ["SELECT id FROM cap_invocations WHERE account_id=? AND principal_id=? AND created_at>=? ORDER BY created_at,id LIMIT 11", [OWNER.accountId, identity.principal.id, 0], 'cap_invocations_principal_admission'],
    ["SELECT id FROM cap_invocations WHERE account_id=? AND principal_id=? AND status NOT IN ('succeeded','failed','cancelled') ORDER BY created_at,id LIMIT 5", [OWNER.accountId, identity.principal.id], 'cap_invocations_nonterminal'],
  ];
  for (const [sql, args, index] of cases) {
    const plans = f.db.prepare(`EXPLAIN QUERY PLAN ${sql}`).all(...args).map(row => row.detail).join('\n');
    assert.ok(plans.includes(index), plans);
    assert.ok(!plans.includes('USE TEMP B-TREE'), plans);
  }
});

test('real old reader refuses main1 plus committed v2 WAL without changing main/WAL while writer owns WAL', t => {
  const f = fixture(t, { version: 1 });
  f.db.exec('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA wal_checkpoint(TRUNCATE)');
  assert.equal(readFileSync(f.file).readUInt32BE(60), 1); migrate(f.db);
  assert.equal(readFileSync(f.file).readUInt32BE(60), 1);
  const before = fileHashes(f.file), old = f.open();
  assert.throws(() => historicalInitialize(old), code('schema_version_unsupported')); f.closeDb(old);
  assert.deepEqual(fileHashes(f.file), before);
});

test('historical DELETE-mode refusal honestly exposes its persistent pre-refusal WAL pragma', t => {
  const f = fixture(t); f.db.exec('PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE');
  const before = fileHashes(f.file);
  assert.throws(() => historicalInitialize(f.db), code('schema_version_unsupported'));
  assert.equal(f.db.prepare('PRAGMA user_version').get().user_version, 2);
  assert.equal(f.db.prepare('PRAGMA journal_mode').get().journal_mode, 'wal');
  assert.notDeepEqual(fileHashes(f.file), before);
});

test('real historical reader refuses checkpointed v2 main without rewriting it', t => {
  const f = fixture(t); f.db.exec('PRAGMA wal_checkpoint(TRUNCATE)');
  assert.equal(readFileSync(f.file).readUInt32BE(60), 2);
  const before = fileHashes(f.file), old = f.open();
  assert.throws(() => historicalInitialize(old), code('schema_version_unsupported')); f.closeDb(old);
  assert.deepEqual(fileHashes(f.file), before);
});

test('reopened baseline holds the post-Notes-commit/pre-Caps-receipt fixture without pretending a refund or execution', t => {
  const f = fixture(t); let service = f.service(); const identity = issue(service);
  const noteFile = join(dirname(f.file), 'notes.sqlite');
  const notes = createNotesService({ databasePath: noteFile, projectId: PROJECT, clock: f.time, allowNativeMigration: true });
  const id = seedInvocation(f, service, identity, { notesStoreId: notes.registryId });
  startFixture(f, id);
  const native = ledger(f, id).intent;
  // The domain write is real; native proof is an explicitly seeded future-format
  // crash fixture. No opaque native context/executor/reconciler exists in B1.
  notes.execute({ op: 'notes.put', actor: OWNER, args: { expectedAccountId: OWNER.accountId,
    noteId: native.note_id, mutationId: native.mutation_id, expectedRevision: 0, title: 'Личный текст',
    body: 'Точное содержимое 😀', items: [], color: 'plain', pinned: false, state: 'active' } });
  notes.close();
  const noteDb = new DatabaseSync(noteFile);
  try {
    noteDb.exec('PRAGMA foreign_keys=ON');
    noteDb.prepare('INSERT INTO note_native_creates VALUES(?,?,?,?,?,?,?,?,?)').run(service.registryId,
      id, OWNER.accountId, native.note_id, native.mutation_id, native.input_digest,
      '95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204', 1, f.time());
    f.close(service); service = f.service({ catalog: undefined });
    const actor = service.authenticateCredential({ token: identity.token, audience: AUDIENCE });
    assert.equal(service.invocations.get({ actor, invocationId: id }).invocation.effectState, 'unknown');
    assert.equal(service.invocations.requestCancel({ actor, invocationId: id }).invocation.status, 'cancel_requested');
    service.invocations.reconcileAuthorization({ invocationId: id });
    const held = ledger(f, id);
    assert.equal(held.receipt, null); assert.equal(held.budget.reserved_amount, 1);
    assert.equal(held.intent.input_purged_at, null); assert.notEqual(held.invocation.input_json, 'null');
    assert.equal(noteDb.prepare('SELECT count(*) AS n FROM notes').get().n, 1);
    assert.equal(noteDb.prepare('SELECT count(*) AS n FROM note_native_creates').get().n, 1);
    assert.equal(migrateNotes(noteDb, PROJECT).schemaVersion, 2);
  } finally { noteDb.close(); }
});
