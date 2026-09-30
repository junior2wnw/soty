import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync, rmSync, readFileSync, existsSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { createHistoricalCapabilitiesV1 } from '../../../../deploy/connector/capabilities-v1.fixture.mjs';
import { createCapabilitiesService, BUILTIN_CAPABILITIES } from '../../server/index.mjs';
import { initializeCapabilitiesSchema } from '../../server/schema-v2.mjs';
import { canonicalHash, canonicalJson } from '../../server/validation.mjs';

export const PROJECT = 'native-storage-test';
export const OWNER = Object.freeze({ accountId: 'account_storage_owner', deviceId: 'device_storage_owner' });
export const AUDIENCE = 'https://native-storage.test/api';
export const NOTES_STORE = 'a'.repeat(32);
// The operational test flag changes neither the immutable descriptor nor its digest.
// No handler or native effect is implemented by this fixture.
export const TEST_CATALOG = BUILTIN_CAPABILITIES.map(value => ({ ...value, executionEnabled: true }));
export const code = expected => error => error.code === expected;
const sha = value => createHash('sha256').update(value).digest('hex');
export const fileHashes = file => Object.fromEntries(['', '-wal'].map(suffix =>
  [suffix || 'main', existsSync(file + suffix) ? sha(readFileSync(file + suffix)) : null]));

export function fixture(t, { version = 2 } = {}) {
  const base = resolve(tmpdir()), directory = mkdtempSync(join(base, 'soty-native-caps-'));
  const file = join(directory, 'capabilities.sqlite'), handles = new Set(), services = new Set();
  let time = 1000, ownerActive = true;
  const open = () => { const db = new DatabaseSync(file); handles.add(db); return db; };
  const closeDb = db => { if (handles.delete(db)) db.close(); };
  const db = open();
  if (version) {
    createHistoricalCapabilitiesV1(db);
    if (version === 2) initializeCapabilitiesSchema(db, { projectId: PROJECT, allowNativeMigration: true });
  }
  function service(overrides = {}) {
    const result = createCapabilitiesService({ databasePath: file, projectId: PROJECT, clock: () => time,
      actorActive: actor => ownerActive && actor.accountId === OWNER.accountId && actor.deviceId === OWNER.deviceId,
      catalog: TEST_CATALOG, ...overrides });
    services.add(result); return result;
  }
  const close = service => { if (services.delete(service)) service.close(); };
  t.after(() => {
    for (const service of services) service.close();
    for (const handle of handles) handle.close();
    assert.equal(dirname(resolve(directory)), base); assert.match(basename(directory), /^soty-native-caps-/u);
    rmSync(directory, { recursive: true, force: true });
  });
  return { file, db, open, closeDb, service, close, tick: () => ++time, time: () => time,
    revokeHost() { ownerActive = false; } };
}
export function transaction(db, fn) {
  db.exec('BEGIN IMMEDIATE');
  try { const result = fn(); assert.ok(!result?.then); db.exec('COMMIT'); return result; }
  catch (error) { db.exec('ROLLBACK'); throw error; }
}
export const access = (service, operation, args) => service.execute({ op: `access.${operation}`, actor: OWNER,
  args: { expectedAccountId: OWNER.accountId, ...args } });
export function issue(service) {
  const principal = access(service, 'principals.create', { label: 'Storage test service' }).principal;
  const grant = access(service, 'grants.issue', { principalId: principal.id,
    capabilities: [{ capabilityId: 'notes.createDraft', version: 1 }], resources: ['notes:new'],
    effects: ['create'], recipients: ['soty:notes'], expiresAt: 100000, allowDelegation: false,
    maxDepth: 0, budget: { unit: 'invocations', limit: 10 } }).grant;
  const issued = access(service, 'credentials.issue', { grantId: grant.id, audience: AUDIENCE });
  return { principal, grant, token: issued.token,
    actor: service.authenticateCredential({ token: issued.token, audience: AUDIENCE }) };
}
export function seedInvocation(f, service, identity, { key = 'native_storage_key', native = true,
  notesStoreId = NOTES_STORE, input = { title: 'Личный текст', body: 'Точное содержимое 😀' } } = {}) {
  const admitted = service.invocations.admit({ actor: identity.actor, capabilityId: 'notes.createDraft', version: 1,
    idempotencyKey: key, input });
  const invocationId = admitted.invocation.invocationId;
  if (native) {
    const registryId = f.db.prepare("SELECT value FROM cap_metadata WHERE key='registry_id'").get().value;
    const suffix = canonicalHash(['soty.native-note.v1', registryId, invocationId]);
    // Explicit future-format seed. This is not native admission or evidence of a Notes effect.
    f.db.prepare(`INSERT INTO cap_native_note_intents(invocation_id,account_id,notes_store_id,note_id,
      mutation_id,input_digest,input_bytes) VALUES(?,?,?,?,?,?,?)`).run(invocationId, OWNER.accountId,
      notesStoreId, `n_${suffix}`, `m_${suffix}`, canonicalHash(input), Buffer.byteLength(canonicalJson(input)));
  }
  return invocationId;
}
export function startFixture(f, invocationId) {
  transaction(f.db, () => {
    f.db.prepare('UPDATE cap_native_note_intents SET started_at=? WHERE invocation_id=?').run(f.time(), invocationId);
    f.db.prepare("UPDATE cap_dispatch_intents SET state='dispatching' WHERE invocation_id=?").run(invocationId);
  });
}
export function terminalFixture(f, invocationId, status = 'succeeded') {
  const row = f.db.prepare('SELECT * FROM cap_invocations WHERE id=?').get(invocationId);
  const native = f.db.prepare('SELECT * FROM cap_native_note_intents WHERE invocation_id=?').get(invocationId);
  const success = status === 'succeeded', effectState = success ? 'committed' : 'none';
  const effects = success ? [{ kind: 'created', resourceType: 'note', resourceId: native.note_id, revision: 1 }] : [];
  const receipt = { verificationMethod: 'domain_read', artifacts: success ? [{ type: 'note', id: native.note_id, revision: 1 }] : [] };
  const disposition = success ? 'spent' : 'released', actualCharges = success ? [{ unit: 'invocations', amount: 1 }] : null;
  // A structural future-reader fixture only. It does not claim proof-first reconciliation was executed.
  transaction(f.db, () => {
    f.db.prepare('INSERT INTO cap_receipts VALUES(?,?,?,?)').run(invocationId, canonicalJson(receipt),
      canonicalHash({ status, effectState, effects, receipt, disposition, actualCharges }), f.time());
    f.db.prepare(`UPDATE cap_invocations SET status=?,effect_state=?,effects_json=?,input_json='null',completed_at=? WHERE id=?`)
      .run(status, effectState, canonicalJson(effects), f.time(), invocationId);
    f.db.prepare('UPDATE cap_native_note_intents SET input_purged_at=? WHERE invocation_id=?').run(f.time(), invocationId);
    f.db.prepare('UPDATE cap_budget_reservations SET disposition=?,actual_amount=? WHERE id=?').run(disposition, success ? 1 : 0, row.reservation_id);
    f.db.prepare('UPDATE cap_budgets SET reserved_amount=reserved_amount-1,spent_amount=spent_amount+? WHERE root_grant_id=?')
      .run(success ? 1 : 0, row.root_grant_id);
  });
}
export function ledger(f, invocationId) {
  const invocation = { ...f.db.prepare('SELECT * FROM cap_invocations WHERE id=?').get(invocationId) };
  return { invocation, intent: { ...f.db.prepare('SELECT * FROM cap_native_note_intents WHERE invocation_id=?').get(invocationId) },
    dispatch: { ...f.db.prepare('SELECT * FROM cap_dispatch_intents WHERE invocation_id=?').get(invocationId) },
    reservation: { ...f.db.prepare('SELECT * FROM cap_budget_reservations WHERE id=?').get(invocation.reservation_id) },
    budget: { ...f.db.prepare('SELECT * FROM cap_budgets WHERE root_grant_id=?').get(invocation.root_grant_id) },
    receipt: f.db.prepare('SELECT * FROM cap_receipts WHERE invocation_id=?').get(invocationId) ?? null };
}
