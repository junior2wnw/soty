import { DatabaseSync } from 'node:sqlite';
import { AccessError } from './validation.mjs';
import { initializeCapabilitiesSchema as initializeV2, inspectCapabilitiesSchema as inspectV2,
  validateNativeRows } from './schema-v2.mjs';
import { OAUTH_DDL, OAUTH_GUARDS } from './oauth-schema.mjs';
import { validateOAuthStorageRows } from './oauth-baseline.mjs';

export const CAPABILITIES_SCHEMA_VERSION = 3;
export const CAPABILITIES_LINEAGE = 'soty.capabilities.sqlite.v3';
export const CAPABILITIES_SUPPORTED_SCHEMA_VERSIONS = Object.freeze([1, 2, 3]);
const fail = code => { throw new AccessError(code); };
const assert = (value, code) => { if (!value) fail(code); };
function configuration(options) {
  assert(options && typeof options === 'object' && !Array.isArray(options)
    && [Object.prototype, null].includes(Object.getPrototypeOf(options))
    && Object.keys(options).every(key => ['projectId', 'allowNativeMigration', 'allowOAuthMigration'].includes(key)), 'schema_configuration_invalid');
  const { projectId, allowNativeMigration = false, allowOAuthMigration = false } = options;
  assert(typeof projectId === 'string' && /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u.test(projectId), 'project_id_required');
  assert(typeof allowNativeMigration === 'boolean' && typeof allowOAuthMigration === 'boolean', 'schema_configuration_invalid');
  return { projectId, allowNativeMigration, allowOAuthMigration };
}
function layout(db) {
  const tokens = sql => (sql || '').match(/'(?:''|[^'])*'|"(?:""|[^"])*"|`(?:``|[^`])*`|\[[^\]]*\]|[^\s'"`\[\]]+/gu)?.join(' ').replace(/;$/u, '') || '';
  return db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all()
    .map(row => [row.type, row.name, row.tbl_name, tokens(row.sql)]);
}
let expected;
function expectedLayout() {
  if (!expected) {
    const reference = new DatabaseSync(':memory:');
    try {
      initializeV2(reference, { projectId: 'schema-reference', allowNativeMigration: true });
      reference.exec(OAUTH_DDL + OAUTH_GUARDS);
      expected = JSON.stringify(layout(reference));
    } finally { reference.close(); }
  }
  return expected;
}

/** Exact, read-only format recognition. No persistent PRAGMA or row repair. */
export function inspectCapabilitiesSchema(db, { projectId } = {}) {
  configuration({ projectId });
  const version = Number(db.prepare('PRAGMA user_version').get().user_version);
  if ([0, 1, 2].includes(version)) return inspectV2(db, { projectId });
  assert(version === 3, 'schema_version_unsupported');
  assert(JSON.stringify(layout(db)) === expectedLayout(), 'schema_layout_invalid');
  const rows = db.prepare('SELECT key,value FROM cap_metadata ORDER BY key').all();
  const values = Object.fromEntries(rows.map(row => [row.key, row.value]));
  assert(rows.length === 3 && values.lineage === CAPABILITIES_LINEAGE
    && typeof values.registry_id === 'string' && /^[a-f0-9]{32}$/u.test(values.registry_id), 'schema_lineage_mismatch');
  assert(values.project_id === projectId, 'capabilities_project_mismatch');
  return Object.freeze({ schemaVersion: 3, registryId: values.registry_id });
}

export function initializeCapabilitiesSchema(db, options) {
  const { projectId, allowNativeMigration, allowOAuthMigration } = configuration(options);
  assert(!db.isTransaction, 'nested_transaction');
  const before = inspectCapabilitiesSchema(db, { projectId });
  if (allowOAuthMigration) assert([2, 3].includes(before.schemaVersion), 'oauth_migration_requires_native_v2');
  if (before.schemaVersion !== 3 && !allowOAuthMigration) return initializeV2(db, { projectId, allowNativeMigration });
  db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000; PRAGMA synchronous=FULL;');
  db.exec('BEGIN IMMEDIATE');
  let result;
  try {
    const state = inspectCapabilitiesSchema(db, { projectId });
    assert([2, 3].includes(state.schemaVersion), 'schema_lineage_mismatch');
    assert(!db.prepare('PRAGMA foreign_key_check').get(), 'capabilities_storage_corrupt');
    validateNativeRows(db, state.registryId);
    if (state.schemaVersion === 2) {
      // The new reference index must not turn malformed legacy JSON into a
      // partially installed migration. This scan runs only on explicit 2→3.
      assert(!db.prepare('SELECT 1 FROM cap_invocations WHERE NOT json_valid(authorization_json) LIMIT 1').get(), 'capabilities_storage_corrupt');
      db.exec(OAUTH_DDL + OAUTH_GUARDS);
      db.prepare("UPDATE cap_metadata SET value=? WHERE key='lineage'").run(CAPABILITIES_LINEAGE);
      db.exec('PRAGMA user_version=3');
    } else validateOAuthStorageRows(db);
    result = inspectCapabilitiesSchema(db, { projectId });
    db.exec('COMMIT');
  } catch (error) {
    if (db.isTransaction) db.exec('ROLLBACK');
    throw error;
  }
  db.exec('PRAGMA journal_mode=WAL;');
  return result;
}
