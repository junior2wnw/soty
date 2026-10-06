import { DatabaseSync } from 'node:sqlite';
import { randomBytes } from 'node:crypto';
import { AccessError, canonicalHash, canonicalJson, text } from './validation.mjs';
import { nativeNoteRequestDigest } from './native-note-contract.mjs';

export const CAPABILITIES_SCHEMA_VERSION = 2;
export const CAPABILITIES_LINEAGE = 'soty.capabilities.sqlite.v2';
export const CAPABILITIES_SUPPORTED_SCHEMA_VERSIONS = Object.freeze([1, 2]);
const LINEAGES = Object.freeze({ 1: 'soty.capabilities.sqlite.v1', 2: CAPABILITIES_LINEAGE });
const NOTES_DIGEST = '95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204';
const TERMINAL = new Set(['succeeded', 'failed', 'cancelled']);
const V1_DDL = `CREATE TABLE cap_metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;
    CREATE TABLE cap_contracts(
      capability_id TEXT NOT NULL, version INTEGER NOT NULL, digest TEXT NOT NULL,
      PRIMARY KEY(capability_id,version)
    ) STRICT;
    CREATE TABLE cap_clients(
      id TEXT PRIMARY KEY, account_id TEXT NOT NULL, label TEXT NOT NULL,
      state TEXT NOT NULL CHECK(state IN ('active','revoked')), policy_epoch INTEGER NOT NULL DEFAULT 1,
      created_at INTEGER NOT NULL, revoked_at INTEGER
    ) STRICT;
    CREATE INDEX cap_clients_account ON cap_clients(account_id,created_at,id);
    CREATE TABLE cap_principals(
      id TEXT PRIMARY KEY, account_id TEXT NOT NULL, client_id TEXT NOT NULL REFERENCES cap_clients(id),
      kind TEXT NOT NULL CHECK(kind='service'), label TEXT NOT NULL,
      state TEXT NOT NULL CHECK(state IN ('active','revoked')), creator_device_id TEXT NOT NULL,
      created_at INTEGER NOT NULL, revoked_at INTEGER
    ) STRICT;
    CREATE INDEX cap_principals_account ON cap_principals(account_id,created_at,id);
    CREATE TABLE cap_grants(
      id TEXT PRIMARY KEY, account_id TEXT NOT NULL, client_id TEXT NOT NULL REFERENCES cap_clients(id),
      principal_id TEXT NOT NULL REFERENCES cap_principals(id), parent_id TEXT REFERENCES cap_grants(id),
      root_id TEXT NOT NULL REFERENCES cap_grants(id), creator_device_id TEXT NOT NULL,
      capabilities_json TEXT NOT NULL, resources_json TEXT NOT NULL, effects_json TEXT NOT NULL, recipients_json TEXT NOT NULL,
      allow_delegation INTEGER NOT NULL CHECK(allow_delegation IN (0,1)), max_depth INTEGER NOT NULL CHECK(max_depth BETWEEN 0 AND 8),
      depth INTEGER NOT NULL CHECK(depth BETWEEN 0 AND 8), not_before INTEGER NOT NULL, expires_at INTEGER NOT NULL,
      policy_epoch INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL, revoked_at INTEGER
    ) STRICT;
    CREATE INDEX cap_grants_account ON cap_grants(account_id,created_at,id);
    CREATE INDEX cap_grants_root ON cap_grants(root_id,id);
    CREATE TABLE cap_credentials(
      id TEXT PRIMARY KEY, digest TEXT NOT NULL UNIQUE, account_id TEXT NOT NULL, client_id TEXT NOT NULL REFERENCES cap_clients(id),
      principal_id TEXT NOT NULL REFERENCES cap_principals(id), grant_id TEXT NOT NULL REFERENCES cap_grants(id),
      audience TEXT NOT NULL, expires_at INTEGER NOT NULL, created_at INTEGER NOT NULL, revoked_at INTEGER
    ) STRICT;
    CREATE INDEX cap_credentials_grant ON cap_credentials(grant_id,id);
    CREATE TABLE cap_audit(
      id TEXT PRIMARY KEY, account_id TEXT NOT NULL, kind TEXT NOT NULL,
      object_type TEXT NOT NULL, object_id TEXT NOT NULL, actor_type TEXT NOT NULL,
      actor_id TEXT NOT NULL, created_at INTEGER NOT NULL
    ) STRICT;
    CREATE INDEX cap_audit_account ON cap_audit(account_id,created_at,id);
    CREATE TABLE cap_budgets(
      root_grant_id TEXT NOT NULL REFERENCES cap_grants(id), unit TEXT NOT NULL CHECK(unit='invocations'),
      limit_amount INTEGER NOT NULL CHECK(limit_amount>=0), reserved_amount INTEGER NOT NULL DEFAULT 0 CHECK(reserved_amount>=0),
      spent_amount INTEGER NOT NULL DEFAULT 0 CHECK(spent_amount>=0),
      PRIMARY KEY(root_grant_id,unit), CHECK(reserved_amount+spent_amount<=limit_amount)
    ) STRICT;
    CREATE TABLE cap_budget_reservations(
      id TEXT PRIMARY KEY, invocation_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
      root_grant_id TEXT NOT NULL REFERENCES cap_grants(id), unit TEXT NOT NULL CHECK(unit='invocations'),
      amount INTEGER NOT NULL CHECK(amount>0), actual_amount INTEGER,
      disposition TEXT NOT NULL CHECK(disposition IN ('reserved','spent','released','uncertain')),
      request_digest TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
      UNIQUE(root_grant_id,invocation_id,attempt_id,unit)
    ) STRICT;
    CREATE TABLE cap_invocations(
      id TEXT PRIMARY KEY, account_id TEXT NOT NULL, client_id TEXT NOT NULL, principal_id TEXT NOT NULL,
      grant_id TEXT NOT NULL, root_grant_id TEXT NOT NULL, policy_epoch INTEGER NOT NULL,
      capability_id TEXT NOT NULL, capability_version INTEGER NOT NULL, capability_digest TEXT NOT NULL,
      request_key TEXT NOT NULL, request_digest TEXT NOT NULL, internal_request_id TEXT NOT NULL UNIQUE,
      input_json TEXT NOT NULL, target_json TEXT NOT NULL, authorization_json TEXT NOT NULL,
      status TEXT NOT NULL, effect_state TEXT NOT NULL DEFAULT 'none' CHECK(effect_state IN ('none','committed','partial','unknown')),
      cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK(cancel_requested IN (0,1)), effects_json TEXT NOT NULL DEFAULT '[]',
      reservation_id TEXT, job_id TEXT UNIQUE, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, completed_at INTEGER,
      UNIQUE(account_id,client_id,request_key)
    ) STRICT;
    CREATE INDEX cap_invocations_history ON cap_invocations(account_id,client_id,created_at,id);
    CREATE TABLE cap_dispatch_intents(
      invocation_id TEXT PRIMARY KEY REFERENCES cap_invocations(id), internal_request_id TEXT NOT NULL UNIQUE,
      state TEXT NOT NULL CHECK(state IN ('pending','dispatching','bound','cancelled','uncertain')),
      created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
    ) STRICT;
    CREATE INDEX cap_dispatch_pending ON cap_dispatch_intents(state,created_at,invocation_id);
    CREATE TABLE cap_receipts(
      invocation_id TEXT PRIMARY KEY REFERENCES cap_invocations(id), value_json TEXT NOT NULL, digest TEXT NOT NULL, created_at INTEGER NOT NULL
    ) STRICT;`;
const V2_DDL = `CREATE UNIQUE INDEX cap_invocations_native_identity
  ON cap_invocations(id,account_id);
CREATE INDEX cap_invocations_account_admission
  ON cap_invocations(account_id,created_at,id);
CREATE INDEX cap_invocations_principal_admission
  ON cap_invocations(account_id,principal_id,created_at,id);
CREATE INDEX cap_invocations_nonterminal
  ON cap_invocations(account_id,principal_id,created_at,id)
  WHERE status NOT IN ('succeeded','failed','cancelled');

CREATE TABLE cap_native_note_intents(
  invocation_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  notes_store_id TEXT NOT NULL
    CHECK(length(notes_store_id)=32 AND notes_store_id NOT GLOB '*[^0-9a-f]*'),
  note_id TEXT NOT NULL
    CHECK(length(note_id)=66 AND substr(note_id,1,2)='n_'
      AND substr(note_id,3) NOT GLOB '*[^0-9a-f]*'),
  mutation_id TEXT NOT NULL
    CHECK(length(mutation_id)=66 AND substr(mutation_id,1,2)='m_'
      AND substr(mutation_id,3) NOT GLOB '*[^0-9a-f]*'),
  input_digest TEXT NOT NULL
    CHECK(length(input_digest)=64 AND input_digest NOT GLOB '*[^0-9a-f]*'),
  input_bytes INTEGER NOT NULL CHECK(input_bytes BETWEEN 1 AND 262144),
  started_at INTEGER CHECK(started_at BETWEEN 0 AND 9007199254740991),
  input_purged_at INTEGER CHECK(input_purged_at BETWEEN 0 AND 9007199254740991),
  UNIQUE(account_id,note_id),
  UNIQUE(account_id,mutation_id),
  FOREIGN KEY(invocation_id,account_id) REFERENCES cap_invocations(id,account_id)
) STRICT;

CREATE TRIGGER cap_native_note_admission
BEFORE INSERT ON cap_native_note_intents
WHEN NOT EXISTS(SELECT 1 FROM cap_invocations i
  WHERE i.id=NEW.invocation_id AND i.account_id=NEW.account_id
    AND i.capability_id='notes.createDraft' AND i.capability_version=1
    AND i.capability_digest='95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204'
    AND i.job_id IS NULL AND i.input_json!='null')
BEGIN SELECT RAISE(ABORT,'native_note_binding_invalid'); END;
CREATE TRIGGER cap_native_note_no_replace
BEFORE INSERT ON cap_native_note_intents
WHEN EXISTS(SELECT 1 FROM cap_native_note_intents
  WHERE invocation_id=NEW.invocation_id
     OR (account_id=NEW.account_id AND note_id=NEW.note_id)
     OR (account_id=NEW.account_id AND mutation_id=NEW.mutation_id))
BEGIN SELECT RAISE(ABORT,'native_note_identity_immutable'); END;
CREATE TRIGGER cap_native_note_no_delete
BEFORE DELETE ON cap_native_note_intents
BEGIN SELECT RAISE(ABORT,'native_note_identity_immutable'); END;
CREATE TRIGGER cap_native_note_update_guard
BEFORE UPDATE ON cap_native_note_intents
WHEN NEW.invocation_id IS NOT OLD.invocation_id OR NEW.account_id IS NOT OLD.account_id
  OR NEW.notes_store_id IS NOT OLD.notes_store_id OR NEW.note_id IS NOT OLD.note_id
  OR NEW.mutation_id IS NOT OLD.mutation_id OR NEW.input_digest IS NOT OLD.input_digest
  OR NEW.input_bytes IS NOT OLD.input_bytes
  OR (OLD.started_at IS NOT NULL AND NEW.started_at IS NOT OLD.started_at)
  OR (OLD.input_purged_at IS NOT NULL AND NEW.input_purged_at IS NOT OLD.input_purged_at)
  OR (NEW.input_purged_at IS NOT NULL AND NOT EXISTS(
    SELECT 1 FROM cap_invocations i JOIN cap_receipts r ON r.invocation_id=i.id
    WHERE i.id=NEW.invocation_id AND i.input_json='null'
      AND i.status IN ('succeeded','failed','cancelled')))
BEGIN SELECT RAISE(ABORT,'native_note_identity_immutable'); END;

CREATE TRIGGER cap_native_note_input_guard
BEFORE UPDATE OF input_json ON cap_invocations
WHEN EXISTS(SELECT 1 FROM cap_native_note_intents n WHERE n.invocation_id=OLD.id)
 AND NEW.input_json IS NOT OLD.input_json
 AND (NEW.input_json!='null' OR NEW.status NOT IN ('succeeded','failed','cancelled')
      OR NOT EXISTS(SELECT 1 FROM cap_receipts r WHERE r.invocation_id=OLD.id))
BEGIN SELECT RAISE(ABORT,'native_note_input_immutable'); END;
CREATE TRIGGER cap_native_receipt_no_update
BEFORE UPDATE ON cap_receipts
WHEN EXISTS(SELECT 1 FROM cap_native_note_intents n WHERE n.invocation_id=OLD.invocation_id)
BEGIN SELECT RAISE(ABORT,'native_note_receipt_immutable'); END;
CREATE TRIGGER cap_native_receipt_no_delete
BEFORE DELETE ON cap_receipts
WHEN EXISTS(SELECT 1 FROM cap_native_note_intents n WHERE n.invocation_id=OLD.invocation_id)
BEGIN SELECT RAISE(ABORT,'native_note_receipt_immutable'); END;
CREATE TRIGGER cap_native_receipt_no_replace
BEFORE INSERT ON cap_receipts
WHEN EXISTS(SELECT 1 FROM cap_native_note_intents n WHERE n.invocation_id=NEW.invocation_id)
 AND EXISTS(SELECT 1 FROM cap_receipts r WHERE r.invocation_id=NEW.invocation_id)
BEGIN SELECT RAISE(ABORT,'native_note_receipt_immutable'); END;

CREATE TRIGGER cap_identity_no_update
BEFORE UPDATE ON cap_metadata
WHEN OLD.key IN ('project_id','registry_id') OR NEW.key IN ('project_id','registry_id')
BEGIN SELECT RAISE(ABORT,'cap_identity_immutable'); END;
CREATE TRIGGER cap_identity_no_delete
BEFORE DELETE ON cap_metadata WHEN OLD.key IN ('project_id','registry_id')
BEGIN SELECT RAISE(ABORT,'cap_identity_immutable'); END;
CREATE TRIGGER cap_identity_no_replace
BEFORE INSERT ON cap_metadata
WHEN NEW.key IN ('project_id','registry_id')
 AND EXISTS(SELECT 1 FROM cap_metadata WHERE key=NEW.key)
BEGIN SELECT RAISE(ABORT,'cap_identity_immutable'); END;`;

const fail = code => { throw new AccessError(code); };
const assert = (value, code = 'capabilities_storage_corrupt') => { if (!value) fail(code); };
const safe = value => Number.isSafeInteger(value) && value >= 0;
const digest = value => typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value);
function configuration(options) {
  assert(options && typeof options === 'object' && !Array.isArray(options)
    && [null, Object.prototype].includes(Object.getPrototypeOf(options))
    && Object.keys(options).every(key => ['projectId', 'allowNativeMigration'].includes(key)), 'schema_configuration_invalid');
  const { projectId, allowNativeMigration = false } = options;
  assert(typeof projectId === 'string' && /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u.test(projectId), 'project_id_required');
  assert(typeof allowNativeMigration === 'boolean', 'schema_configuration_invalid');
  return { projectId, allowNativeMigration };
}
function sqlTokens(sql) {
  return (sql || '').match(/'(?:''|[^'])*'|"(?:""|[^"])*"|`(?:``|[^`])*`|\[[^\]]*\]|[^\s'"`\[\]]+/gu)?.join(' ').replace(/;$/u, '') || '';
}
function layout(db) {
  return db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all()
    .map(row => [row.type, row.name, row.tbl_name, sqlTokens(row.sql)]);
}
const expectedLayouts = new Map();
function expectedLayout(version) {
  if (!expectedLayouts.has(version)) {
    const reference = new DatabaseSync(':memory:');
    try {
      reference.exec(V1_DDL);
      if (version === 2) reference.exec(V2_DDL);
      expectedLayouts.set(version, JSON.stringify(layout(reference)));
    } finally { reference.close(); }
  }
  return expectedLayouts.get(version);
}

/** Format recognition only; no persistent PRAGMA, migration or candidate repair. */
export function inspectCapabilitiesSchema(db, { projectId } = {}) {
  configuration({ projectId });
  const version = Number(db.prepare('PRAGMA user_version').get().user_version);
  assert(version === 0 || CAPABILITIES_SUPPORTED_SCHEMA_VERSIONS.includes(version), 'schema_version_unsupported');
  const actual = layout(db);
  if (version === 0) {
    assert(actual.length === 0, 'schema_lineage_mismatch');
    return Object.freeze({ schemaVersion: 0, registryId: null });
  }
  assert(JSON.stringify(actual) === expectedLayout(version), 'schema_layout_invalid');
  const rows = db.prepare('SELECT key,value FROM cap_metadata ORDER BY key').all();
  const metadata = Object.fromEntries(rows.map(row => [row.key, row.value]));
  assert(rows.length === (version === 1 ? 1 : 3) && Object.keys(metadata).length === rows.length
    && metadata.lineage === LINEAGES[version], 'schema_lineage_mismatch');
  if (version === 2) {
    assert(metadata.project_id === projectId, 'capabilities_project_mismatch');
    assert(typeof metadata.registry_id === 'string' && /^[a-f0-9]{32}$/u.test(metadata.registry_id), 'schema_lineage_mismatch');
  }
  return Object.freeze({ schemaVersion: version, registryId: metadata.registry_id ?? null });
}

// Shared by the additive v3 reader; the historical format and its checks stay
// literal here, rather than maintaining a second copy of native invariants.
export function validateNativeRows(db, registryId) {
  const credential = db.prepare(`SELECT 1 FROM cap_credentials WHERE id=? AND account_id=?
    AND client_id=? AND principal_id=? AND grant_id=? AND audience=?`);
  const linkage = db.prepare(`SELECT 1 FROM cap_principals p
    JOIN cap_clients c ON c.id=p.client_id AND c.account_id=p.account_id
    JOIN cap_grants g ON g.id=? AND g.account_id=p.account_id AND g.principal_id=p.id AND g.client_id=c.id
    JOIN cap_grants root ON root.id=g.root_id AND root.account_id=p.account_id
    WHERE p.id=? AND p.account_id=? AND p.client_id=? AND root.id=?`);
  const rows = db.prepare(`SELECT n.*,i.client_id,i.principal_id,i.grant_id,i.root_grant_id,i.policy_epoch,
    i.capability_id,i.capability_version,i.capability_digest,i.input_json,i.target_json,i.authorization_json,
    i.request_key,i.request_digest,i.internal_request_id,i.status,i.effect_state,i.effects_json,i.cancel_requested,
    i.job_id,i.created_at,i.updated_at,i.completed_at,d.state AS dispatch_state,d.internal_request_id AS dispatch_request_id,
    b.invocation_id AS budget_invocation_id,b.root_grant_id AS budget_root_id,b.unit,b.amount,b.disposition,b.actual_amount,
    r.value_json AS receipt_json,r.digest AS receipt_digest
    FROM cap_native_note_intents n JOIN cap_invocations i ON i.id=n.invocation_id AND i.account_id=n.account_id
    LEFT JOIN cap_dispatch_intents d ON d.invocation_id=i.id
    LEFT JOIN cap_budget_reservations b ON b.id=i.reservation_id
    LEFT JOIN cap_receipts r ON r.invocation_id=i.id ORDER BY n.invocation_id`);
  for (const row of rows.iterate()) {
    try {
      const suffix = canonicalHash(['soty.native-note.v1', registryId, row.invocation_id]);
      assert(row.note_id === `n_${suffix}` && row.mutation_id === `m_${suffix}`);
      assert(row.capability_id === 'notes.createDraft' && row.capability_version === 1 && row.capability_digest === NOTES_DIGEST);
      assert(row.target_json === canonicalJson({ kind: 'native', handler: 'notes.createDraft', version: 1 }));
      assert(row.job_id === null && digest(row.request_key) && digest(row.request_digest));
      assert(row.dispatch_request_id === row.internal_request_id
        && row.budget_invocation_id === row.invocation_id && row.budget_root_id === row.root_grant_id
        && row.unit === 'invocations' && row.amount === 1);
      assert(linkage.get(row.grant_id, row.principal_id, row.account_id, row.client_id, row.root_grant_id));
      for (const key of ['created_at', 'updated_at', 'policy_epoch']) assert(safe(row[key]));
      assert(row.policy_epoch > 0 && (row.cancel_requested === 0 || row.cancel_requested === 1));
      const authority = JSON.parse(row.authorization_json);
      assert(Buffer.byteLength(row.authorization_json, 'utf8') <= 32768 && authority && !Array.isArray(authority)
        && Object.keys(authority).every(key => ['accountId','clientId','principalId','credentialId','audience',
          'grantId','rootGrantId','policyEpoch','expiresAt','capabilityId','version','capabilityDigest',
          'resources','effects','recipients','executionBinding','charges'].includes(key))
        && authority.accountId === row.account_id && authority.clientId === row.client_id
        && authority.principalId === row.principal_id && authority.grantId === row.grant_id
        && authority.rootGrantId === row.root_grant_id && authority.capabilityId === 'notes.createDraft'
        && authority.version === 1 && authority.capabilityDigest === NOTES_DIGEST
        && authority.policyEpoch === row.policy_epoch && safe(authority.expiresAt)
        && canonicalJson(authority.resources) === '["notes:new"]' && canonicalJson(authority.effects) === '["create"]'
        && canonicalJson(authority.recipients) === '["soty:notes"]'
        && canonicalHash(authority.executionBinding) === canonicalHash(JSON.parse(row.target_json))
        && canonicalJson(authority.charges) === canonicalJson([{ unit: 'invocations', amount: 1 }]));
      assert(typeof authority.credentialId === 'string' && typeof authority.audience === 'string'
        && credential.get(authority.credentialId, row.account_id, row.client_id, row.principal_id, row.grant_id, authority.audience));
      const effects = JSON.parse(row.effects_json);
      if (!TERMINAL.has(row.status)) {
        assert(['accepted', 'cancel_requested', 'execution_uncertain'].includes(row.status)
          && row.completed_at === null && row.receipt_json === null && row.input_purged_at === null);
        assert(['reserved', 'uncertain'].includes(row.disposition) && row.actual_amount === null
          && ['none', 'unknown'].includes(row.effect_state) && canonicalJson(effects) === '[]');
        assert(row.started_at === null ? row.dispatch_state === 'pending' && row.effect_state === 'none'
          : ['dispatching', 'uncertain'].includes(row.dispatch_state));
        const input = JSON.parse(row.input_json);
        assert(input && typeof input === 'object' && !Array.isArray(input)
          && Object.keys(input).sort().join(',') === 'body,title');
        text(input.title, { min: 0, max: 160 }); text(input.body, { min: 0, max: 100000 });
        assert(input.title.isWellFormed() && input.body.isWellFormed()
          && canonicalJson(input) === row.input_json && canonicalHash(input) === row.input_digest
          && Buffer.byteLength(row.input_json, 'utf8') === row.input_bytes);
        assert(row.request_digest === nativeNoteRequestDigest(input));
        assert(Buffer.byteLength(JSON.stringify({ title: input.title, body: input.body, items: [],
          color: 'plain', pinned: false, state: 'active' }), 'utf8') <= 262144);
      } else {
        assert(safe(row.completed_at) && row.input_json === 'null' && safe(row.input_purged_at)
          && typeof row.receipt_json === 'string' && Buffer.byteLength(row.receipt_json, 'utf8') <= 32768
          && digest(row.receipt_digest));
        const receipt = JSON.parse(row.receipt_json);
        assert(receipt && !Array.isArray(receipt) && Array.isArray(receipt.artifacts)
          && Object.keys(receipt).every(key => ['verificationMethod', 'artifacts', 'errorCode'].includes(key)));
        if (row.status === 'succeeded') {
          assert(row.started_at !== null && ['dispatching', 'uncertain'].includes(row.dispatch_state)
            && row.effect_state === 'committed' && row.disposition === 'spent' && row.actual_amount === 1);
          assert(canonicalJson(effects) === canonicalJson([{ kind: 'created', resourceType: 'note', resourceId: row.note_id, revision: 1 }]));
          assert(canonicalJson(receipt) === canonicalJson({ verificationMethod: 'domain_read',
            artifacts: [{ type: 'note', id: row.note_id, revision: 1 }] }));
        } else {
          assert(['pending', 'dispatching', 'cancelled', 'uncertain'].includes(row.dispatch_state)
            && row.effect_state === 'none' && canonicalJson(effects) === '[]'
            && row.disposition === 'released' && row.actual_amount === 0 && receipt.artifacts.length === 0);
          assert(['domain_read', 'unverified'].includes(receipt.verificationMethod));
          if (receipt.errorCode !== undefined) assert(typeof receipt.errorCode === 'string' && /^[a-z][a-z0-9_]{1,79}$/u.test(receipt.errorCode));
        }
        assert(row.receipt_digest === canonicalHash({ status: row.status, effectState: row.effect_state,
          effects, receipt, disposition: row.disposition,
          actualCharges: row.status === 'succeeded' ? [{ unit: 'invocations', amount: 1 }] : null }));
      }
    } catch { fail('capabilities_storage_corrupt'); }
  }
}
function validateRows(db, state) {
  assert(!db.prepare('PRAGMA foreign_key_check').get());
  if (state.schemaVersion === 2) validateNativeRows(db, state.registryId);
}

export function initializeCapabilitiesSchema(db, options) {
  const { projectId, allowNativeMigration } = configuration(options);
  assert(!db.isTransaction, 'nested_transaction');
  inspectCapabilitiesSchema(db, { projectId });
  db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000; PRAGMA synchronous=FULL;');
  db.exec('BEGIN IMMEDIATE');
  let result;
  try {
    let state = inspectCapabilitiesSchema(db, { projectId });
    if (state.schemaVersion !== 0) validateRows(db, state);
    if (state.schemaVersion === 0) {
      db.exec(V1_DDL);
      db.prepare('INSERT INTO cap_metadata(key,value) VALUES(?,?)').run('lineage', LINEAGES[1]);
      db.exec('PRAGMA user_version=1');
      state = inspectCapabilitiesSchema(db, { projectId });
    }
    if (state.schemaVersion === 1 && allowNativeMigration) {
      const insert = db.prepare('INSERT INTO cap_metadata(key,value) VALUES(?,?)');
      insert.run('project_id', projectId); insert.run('registry_id', randomBytes(16).toString('hex'));
      db.prepare("UPDATE cap_metadata SET value=? WHERE key='lineage'").run(LINEAGES[2]);
      db.exec(V2_DDL);
      db.exec('PRAGMA user_version=2');
    }
    result = inspectCapabilitiesSchema(db, { projectId });
    // Existing rows were checked once before DDL. The migration adds only an
    // empty native table/indexes/guards and never rewrites legacy content.
    db.exec('COMMIT');
  } catch (error) {
    if (db.isTransaction) db.exec('ROLLBACK');
    throw error;
  }
  db.exec('PRAGMA journal_mode=WAL;');
  return result;
}
