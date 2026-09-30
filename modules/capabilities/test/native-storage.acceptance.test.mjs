import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createCapabilitiesService, BUILTIN_CAPABILITIES } from '../server/index.mjs';
import { initializeCapabilitiesSchema, inspectCapabilitiesSchema } from '../server/schema.mjs';

const sha = value => createHash('sha256').update(value).digest('hex');
const stable = value => Array.isArray(value) ? `[${value.map(stable).join(',')}]`
  : value && typeof value === 'object' ? `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${stable(value[key])}`).join(',')}}`
    : JSON.stringify(value);
const fingerprint = value => sha(stable(value));
const fault = code => error => error?.code === code;
const OWNER = Object.freeze({ accountId: 'acct_native_review', deviceId: 'device_native_review' });
const TIME = 1_780_000_000_000, AUDIENCE = 'urn:soty:independent-native-reader';
const INPUT = Object.freeze({ title: 'Исходный заголовок', body: 'Личный текст 🐝 e\u0301' });
const historicalUrl = new URL('../../../deploy/connector/fixtures/capabilities-v1/schema.mjs', import.meta.url);
const historicalSha = '959fdb4938d6b0ebc9359817b001f0eed08be70e437083648e9ac259c56229ad';

async function fixture(t, { migrate = true } = {}) {
  assert.equal(sha(readFileSync(historicalUrl)), historicalSha);
  const old = await import(historicalUrl.href);
  const parent = realpathSync(tmpdir()), root = mkdtempSync(path.join(parent, 'soty-native-caps-independent-'));
  const marker = path.join(root, '.owner'), nonce = randomBytes(20).toString('hex');
  writeFileSync(marker, nonce);
  const databasePath = path.join(root, 'capabilities.sqlite'), handles = new Set();
  t.after(() => {
    for (const handle of [...handles].reverse()) handle.close();
    assert.equal(realpathSync(root), root); assert.equal(path.dirname(root), parent);
    assert.match(path.basename(root), /^soty-native-caps-independent-[A-Za-z0-9_-]+$/u);
    assert.equal(readFileSync(marker, 'utf8'), nonce);
    rmSync(root, { recursive: true, force: true });
  });
  const track = value => { handles.add(value); return value; };
  const close = value => { value.close(); handles.delete(value); };
  const db = track(new DatabaseSync(databasePath));
  old.initializeCapabilitiesSchema(db);
  db.exec('PRAGMA wal_autocheckpoint=0');
  let active = true;
  function open({ enabled = true, allowNativeMigration = false } = {}) {
    return track(createCapabilitiesService({ databasePath, projectId: 'soty', clock: () => TIME,
      actorActive: actor => active && actor.accountId === OWNER.accountId && actor.deviceId === OWNER.deviceId,
      catalog: BUILTIN_CAPABILITIES.map(entry => ({ ...entry, executionEnabled: enabled })), allowNativeMigration }));
  }
  const service = open();
  const execute = (op, args) => service.execute({ op, actor: OWNER, args: { expectedAccountId: OWNER.accountId, ...args } });
  const { principal } = execute('access.principals.create', { label: 'Независимый тест' });
  const { grant } = execute('access.grants.issue', { principalId: principal.id,
    capabilities: [{ capabilityId: 'notes.createDraft', version: 1 }], resources: ['notes:new'], effects: ['create'],
    recipients: ['soty:notes'], expiresAt: TIME + 3_600_000, budget: { unit: 'invocations', limit: 20 } });
  const credential = execute('access.credentials.issue', { grantId: grant.id, audience: AUDIENCE });
  const actor = service.authenticateCredential({ token: credential.token, audience: AUDIENCE });
  const authenticate = current => current.authenticateCredential({ token: credential.token, audience: AUDIENCE });
  if (migrate) initializeCapabilitiesSchema(db, { projectId: 'soty', allowNativeMigration: true });
  const admit = key => service.invocations.admit({ actor, capabilityId: 'notes.createDraft', version: 1,
    idempotencyKey: key, input: INPUT }).invocation.invocationId;
  return { root, databasePath, db, old, service, actor, grant, execute, authenticate, open, close, admit,
    revokeDevice() { active = false; } };
}

// Explicit future-format rows. No native executor, Notes effect or cross-store
// commit is fabricated. Generic APIs supply genuine authority/budget/receipt rows.
function nativeIdentity(f, invocationId) {
  const registryId = f.db.prepare("SELECT value FROM cap_metadata WHERE key='registry_id'").get().value;
  const suffix = fingerprint(['soty.native-note.v1', registryId, invocationId]);
  return { noteId: `n_${suffix}`, mutationId: `m_${suffix}` };
}
function insertNative(f, invocationId, started = true, input = INPUT) {
  const identity = nativeIdentity(f, invocationId);
  f.db.prepare(`INSERT INTO cap_native_note_intents(invocation_id,account_id,notes_store_id,note_id,mutation_id,
    input_digest,input_bytes,started_at,input_purged_at) VALUES(?,?,?,?,?,?,?,?,NULL)`)
    .run(invocationId, OWNER.accountId, '7'.repeat(32), identity.noteId, identity.mutationId,
      fingerprint(input), Buffer.byteLength(stable(input)), started ? TIME : null);
  return identity;
}
function seedPending(f, key = 'reader-pending-one', started = true) {
  const invocationId = f.admit(key);
  f.db.exec('BEGIN IMMEDIATE');
  try {
    if (started) f.db.prepare("UPDATE cap_dispatch_intents SET state='dispatching' WHERE invocation_id=?").run(invocationId);
    insertNative(f, invocationId, started);
    f.db.exec('COMMIT');
  } catch (error) { f.db.exec('ROLLBACK'); throw error; }
  return invocationId;
}
function seedTerminal(f) {
  const invocationId = f.admit('reader-terminal-one'), { noteId } = nativeIdentity(f, invocationId);
  f.service.invocations.beginDispatch({ invocationId });
  f.service.invocations.recordResult({ invocationId, status: 'succeeded', effectState: 'committed',
    effects: [{ kind: 'created', resourceType: 'note', resourceId: noteId, revision: 1 }],
    receipt: { verificationMethod: 'domain_read', artifacts: [{ type: 'note', id: noteId, revision: 1 }] },
    disposition: 'spent', actualCharges: [{ unit: 'invocations', amount: 1 }] });
  f.db.exec('BEGIN IMMEDIATE');
  try {
    insertNative(f, invocationId);
    f.db.prepare("UPDATE cap_invocations SET input_json='null' WHERE id=?").run(invocationId);
    f.db.prepare('UPDATE cap_native_note_intents SET input_purged_at=? WHERE invocation_id=?').run(TIME, invocationId);
    f.db.exec('COMMIT');
  } catch (error) { f.db.exec('ROLLBACK'); throw error; }
  return invocationId;
}
function ledger(f, invocationId) {
  return {
    invocation: f.db.prepare('SELECT * FROM cap_invocations WHERE id=?').get(invocationId),
    intent: f.db.prepare('SELECT * FROM cap_native_note_intents WHERE invocation_id=?').get(invocationId),
    delivery: f.db.prepare('SELECT * FROM cap_dispatch_intents WHERE invocation_id=?').get(invocationId),
    receipt: f.db.prepare('SELECT * FROM cap_receipts WHERE invocation_id=?').get(invocationId),
    budget: f.db.prepare('SELECT reserved_amount,spent_amount FROM cap_budgets WHERE root_grant_id=?').get(f.grant.id),
  };
}
function fileProof(file) {
  const proof = {};
  for (const suffix of ['', '-wal']) {
    try { proof[suffix] = sha(readFileSync(file + suffix)); }
    catch (error) { if (error.code !== 'ENOENT') throw error; proof[suffix] = null; }
  }
  return proof;
}

test('Caps historical populated v1 stays v1 by default; explicit migration preserves rows and checkpoint works', async t => {
  const f = await fixture(t, { migrate: false });
  const id = f.admit('ordinary-historical-admission'), before = f.db.prepare('SELECT * FROM cap_invocations WHERE id=?').get(id);
  assert.equal(f.service.schemaVersion, 1); assert.equal(f.service.registryId, null);
  assert.deepEqual(f.service.supportedSchemaVersions, [1, 2]);
  f.close(f.service);
  f.db.exec('PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE');
  const upgraded = f.open({ allowNativeMigration: true });
  assert.equal(upgraded.schemaVersion, 2); assert.match(upgraded.registryId, /^[a-f0-9]{32}$/u);
  assert.deepEqual(f.db.prepare('SELECT * FROM cap_invocations WHERE id=?').get(id), before);
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM cap_native_note_intents').get().n, 0);
  assert.equal(f.db.prepare('PRAGMA wal_checkpoint(TRUNCATE)').get().busy, 0);
  const registry = upgraded.registryId; f.close(upgraded);
  const reopened = f.open(); assert.equal(reopened.schemaVersion, 2); assert.equal(reopened.registryId, registry);
  assert.equal(reopened.invocations.get({ actor: f.authenticate(reopened), invocationId: id }).invocation.invocationId, id);
  assert.throws(() => createCapabilitiesService({ databasePath: f.databasePath, projectId: 'other', actorActive: () => true }), fault('capabilities_project_mismatch'));
});

test('v1 connection notices later v2 native rows and all generic effects remain held through cancel, revoke and reopen', async t => {
  const f = await fixture(t), id = seedPending(f), before = ledger(f, id);
  assert.equal(f.service.schemaVersion, 1, 'metadata describes opening; guard must inspect later migration');
  assert.equal(f.service.invocations.get({ actor: f.actor, invocationId: id }).invocation.effectState, 'unknown');
  assert.deepEqual(f.service.invocations.peekDispatch({ limit: 1 }), []);
  for (const action of [
    () => f.service.invocations.beginDispatch({ invocationId: id }),
    () => f.service.invocations.bindJob({ invocationId: id, internalRequestId: before.invocation.internal_request_id, jobId: 'job-reader-independent' }),
    () => f.service.invocations.markUncertain({ invocationId: id }),
    () => f.service.invocations.recordResult({ invocationId: id, status: 'failed', effectState: 'none', effects: [],
      receipt: { verificationMethod: 'unverified', artifacts: [] }, disposition: 'released' }),
  ]) assert.throws(action, fault('native_reconciliation_required'));
  assert.deepEqual(ledger(f, id), before, 'refused generic effect paths do not alter the held intent');
  f.service.invocations.requestCancel({ actor: f.actor, invocationId: id });
  f.revokeDevice();
  assert.equal(f.service.invocations.reconcileAuthorization({ invocationId: id }).authorized, false);
  const after = ledger(f, id);
  assert.equal(after.invocation.status, 'cancel_requested'); assert.equal(after.invocation.completed_at, null);
  assert.equal(after.invocation.input_json, before.invocation.input_json); assert.equal(after.intent.input_purged_at, null);
  assert.equal(after.receipt, undefined); assert.equal(after.budget.reserved_amount, 1); assert.equal(after.budget.spent_amount, 0);
  assert.throws(() => f.service.invocations.get({ actor: f.actor, invocationId: id }), fault('access_denied'));
  f.close(f.service); const reopened = f.open({ enabled: false });
  assert.deepEqual(reopened.invocations.peekDispatch(), []);
  assert.equal(ledger(f, id).invocation.effect_state, 'unknown');
});

test('unstarted native cancellation cannot take legacy pending/no-job refund shortcut', async t => {
  const f = await fixture(t), id = seedPending(f, 'reader-not-started', false);
  const cancelled = f.service.invocations.requestCancel({ actor: f.actor, invocationId: id }).invocation;
  assert.equal(cancelled.status, 'cancel_requested'); assert.equal(cancelled.effectState, 'none');
  const state = ledger(f, id);
  assert.equal(state.delivery.state, 'pending'); assert.equal(state.budget.reserved_amount, 1);
  assert.equal(state.receipt, undefined); assert.equal(state.intent.started_at, null);
  assert.notEqual(state.invocation.input_json, 'null');
  f.close(f.service); assert.equal(f.open({ enabled: false }).schemaVersion, 2);
});

test('terminal native receipt stays readable with execution disabled, cannot reopen, and discloses no original input', async t => {
  const f = await fixture(t), id = seedTerminal(f), before = ledger(f, id);
  f.close(f.service); const reopened = f.open({ enabled: false }), actor = f.authenticate(reopened);
  const output = reopened.invocations.get({ actor, invocationId: id }).invocation;
  assert.equal(output.status, 'succeeded'); assert.equal(output.receipt.artifacts[0].revision, 1);
  assert.equal(JSON.stringify(output).includes(INPUT.body), false); assert.equal(JSON.stringify(output).includes(INPUT.title), false);
  assert.equal(reopened.invocations.requestCancel({ actor, invocationId: id }).invocation.status, 'succeeded');
  assert.equal(reopened.invocations.reconcileAuthorization({ invocationId: id }).invocation.status, 'succeeded');
  assert.throws(() => reopened.invocations.beginDispatch({ invocationId: id }), fault('native_reconciliation_required'));
  assert.deepEqual(ledger(f, id), before);
  f.revokeDevice(); assert.throws(() => reopened.invocations.get({ actor, invocationId: id }), fault('access_denied'));
});

test('terminal native receipt with a corrupted completion digest is refused rather than silently trusted', async t => {
  const f = await fixture(t), id = seedTerminal(f);
  // First establish that this exact terminal fixture is accepted.
  f.close(f.service); const valid = f.open({ enabled: false }); f.close(valid);
  const guard = f.db.prepare("SELECT sql FROM sqlite_schema WHERE name='cap_native_receipt_no_update'").get().sql;
  f.db.exec('BEGIN IMMEDIATE; DROP TRIGGER cap_native_receipt_no_update');
  f.db.prepare('UPDATE cap_receipts SET digest=? WHERE invocation_id=?').run('0'.repeat(64), id);
  f.db.exec(guard); f.db.exec('COMMIT');
  const before = fileProof(f.databasePath);
  assert.throws(() => f.open({ enabled: false }), fault('capabilities_storage_corrupt'));
  assert.deepEqual(fileProof(f.databasePath), before);
  assert.equal(f.db.prepare('SELECT digest FROM cap_receipts WHERE invocation_id=?').get(id).digest, '0'.repeat(64));
});

test('native reader accepts a declared-size Unicode input whose request envelope exceeds the input budget', async t => {
  const f = await fixture(t), id = f.admit('reader-near-byte-limit');
  const input = { title: '', body: '中'.repeat(87_350) }, inputJson = stable(input);
  const notesDocument = { ...input, items: [], color: 'plain', pinned: false, state: 'active' };
  assert.equal(Buffer.byteLength(inputJson), 262_072);
  assert.equal(Buffer.byteLength(JSON.stringify(notesDocument)), 262_131);
  const row = f.db.prepare('SELECT capability_digest,authorization_json FROM cap_invocations WHERE id=?').get(id);
  const authority = JSON.parse(row.authorization_json);
  const envelope = { capabilityId: 'notes.createDraft', version: 1, capabilityDigest: row.capability_digest,
    input, target: authority.executionBinding, resources: authority.resources, effects: authority.effects,
    recipients: authority.recipients };
  assert.equal(Buffer.byteLength(stable(envelope)), 262_359);
  // The current generic admit has its legacy aggregate cap. This future native
  // record is explicit, and its fingerprint is computed independently here.
  f.db.exec('BEGIN IMMEDIATE');
  try {
    f.db.prepare('UPDATE cap_invocations SET input_json=?,request_digest=? WHERE id=?')
      .run(inputJson, fingerprint(envelope), id);
    insertNative(f, id, false, input);
    f.db.exec('COMMIT');
  } catch (error) { f.db.exec('ROLLBACK'); throw error; }
  f.close(f.service);
  let reopened;
  assert.doesNotThrow(() => { reopened = f.open({ enabled: false }); });
  assert.equal(reopened.schemaVersion, 2);
  assert.equal(f.db.prepare('SELECT input_json FROM cap_invocations WHERE id=?').get(id).input_json, inputJson);
  assert.deepEqual(reopened.invocations.peekDispatch(), []);
});

test('native account orphan cannot disappear through an inner JOIN in row validation', async t => {
  const f = await fixture(t), id = seedPending(f);
  f.close(f.service);
  const guard = f.db.prepare("SELECT sql FROM sqlite_schema WHERE name='cap_native_note_update_guard'").get().sql;
  f.db.exec('PRAGMA foreign_keys=OFF; BEGIN IMMEDIATE; DROP TRIGGER cap_native_note_update_guard');
  f.db.prepare('UPDATE cap_native_note_intents SET account_id=? WHERE invocation_id=?').run('acct_other_reader', id);
  f.db.exec(guard); f.db.exec('COMMIT; PRAGMA foreign_keys=ON');
  assert.ok(f.db.prepare('PRAGMA foreign_key_check').get());
  const before = fileProof(f.databasePath);
  assert.throws(() => f.open(), fault('capabilities_storage_corrupt'));
  assert.deepEqual(fileProof(f.databasePath), before);
});

test('missing registry, removed guard and future marker are refused before persistent pragmas or repair', async t => {
  for (const kind of ['registry', 'guard', 'future']) {
    const f = await fixture(t); f.close(f.service);
    if (kind === 'registry') {
      const guard = f.db.prepare("SELECT sql FROM sqlite_schema WHERE name='cap_identity_no_delete'").get().sql;
      f.db.exec('DROP TRIGGER cap_identity_no_delete');
      f.db.exec("DELETE FROM cap_metadata WHERE key='registry_id'"); f.db.exec(guard);
    } else if (kind === 'guard') f.db.exec('DROP TRIGGER cap_native_note_input_guard');
    else f.db.exec("UPDATE cap_metadata SET value='soty.capabilities.sqlite.v3' WHERE key='lineage'; PRAGMA user_version=3");
    f.db.exec('PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE');
    const before = fileProof(f.databasePath);
    assert.throws(() => f.open({ allowNativeMigration: true }), fault(kind === 'registry' ? 'schema_lineage_mismatch'
      : kind === 'guard' ? 'schema_layout_invalid' : 'schema_version_unsupported'));
    assert.deepEqual(fileProof(f.databasePath), before);
    assert.equal(f.db.prepare('PRAGMA journal_mode').get().journal_mode, 'delete');
  }
});

test('historical reader rejects a real v2 WAL tail while main header remains v1', async t => {
  const f = await fixture(t, { migrate: false }); f.close(f.service);
  f.db.exec('PRAGMA wal_checkpoint(TRUNCATE)');
  assert.equal(readFileSync(f.databasePath).readUInt32BE(60), 1);
  const migrated = initializeCapabilitiesSchema(f.db, { projectId: 'soty', allowNativeMigration: true });
  assert.equal(migrated.schemaVersion, 2); assert.equal(readFileSync(f.databasePath).readUInt32BE(60), 1);
  assert.ok(readFileSync(f.databasePath + '-wal').length > 32);
  const before = fileProof(f.databasePath), oldConnection = new DatabaseSync(f.databasePath);
  try { assert.throws(() => f.old.initializeCapabilitiesSchema(oldConnection), fault('schema_version_unsupported')); }
  finally { oldConnection.close(); }
  assert.deepEqual(fileProof(f.databasePath), before);
  assert.deepEqual(inspectCapabilitiesSchema(f.db, { projectId: 'soty' }), migrated);
});
