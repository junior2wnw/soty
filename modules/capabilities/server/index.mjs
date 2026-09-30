import { mkdirSync } from 'node:fs';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createAccessStore } from './access.mjs';
import { BUILTIN_CAPABILITIES, createCatalog } from './catalog.mjs';
import { createPublicDiscovery } from './discovery.mjs';
import { BUILTIN_DOCUMENTATION } from './documentation.mjs';
import { createInvocationStore } from './invocations.mjs';
import { initializeCapabilitiesSchema, CAPABILITIES_SUPPORTED_SCHEMA_VERSIONS } from './schema.mjs';
import { assert, canonicalHash, exact, integer, newId } from './validation.mjs';

export { ACCESS_OPERATIONS } from './access.mjs';
export { BUILTIN_CAPABILITIES } from './catalog.mjs';
export { AccessError } from './validation.mjs';

export function createCapabilitiesService({ databasePath, projectId, clock = Date.now, actorActive, catalog = BUILTIN_CAPABILITIES,
  documentation = BUILTIN_DOCUMENTATION, limits = {}, allowNativeMigration = false } = {}) {
  assert(typeof databasePath === 'string' && databasePath.length > 0, 'database_path_required');
  assert(typeof projectId === 'string' && /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u.test(projectId), 'project_id_required');
  assert(typeof allowNativeMigration === 'boolean', 'schema_configuration_invalid');
  assert(typeof actorActive === 'function', 'host_auth_required');
  exact(limits, ['access', 'invocations'], 'limits_invalid');
  const accessLimits = limits.access ?? {};
  const invocationLimits = limits.invocations ?? {};
  exact(accessLimits, ['maxGrantTtlMs'], 'limits_invalid');
  if (accessLimits.maxGrantTtlMs !== undefined) integer(accessLimits.maxGrantTtlMs, 1, 365 * 24 * 60 * 60 * 1000, 'limits_invalid');
  exact(invocationLimits, ['inputBytes', 'metadataBytes', 'pageSize'], 'limits_invalid');
  const maximum = { inputBytes: 262144, metadataBytes: 32768, pageSize: 50 };
  for (const [key, value] of Object.entries(invocationLimits)) integer(value, 1, maximum[key], 'limits_invalid');
  const registry = createCatalog(catalog);
  // Validate the complete public projection before creating or pinning storage.
  const publicCatalog = createPublicDiscovery({ catalog: registry, documentation });
  if (databasePath !== ':memory:') mkdirSync(path.dirname(databasePath), { recursive: true });
  const db = new DatabaseSync(databasePath);
  let storage;
  try { storage = initializeCapabilitiesSchema(db, { projectId, allowNativeMigration }); }
  catch (error) { db.close(); throw error; }
  let closed = false;
  let inTransaction = false;
  function transaction(fn) {
    assert(!closed, 'service_closed');
    assert(!inTransaction, 'nested_transaction');
    inTransaction = true;
    try {
      db.exec('BEGIN IMMEDIATE');
      const result = fn();
      assert(!result || typeof result.then !== 'function', 'async_transaction_not_allowed');
      db.exec('COMMIT');
      return result;
    } catch (error) {
      try { db.exec('ROLLBACK'); } catch { /* The failed BEGIN may have no transaction. */ }
      throw error;
    } finally { inTransaction = false; }
  }
  try {
    transaction(() => {
      for (const entry of registry.listAll()) {
        const pinned = db.prepare('SELECT digest FROM cap_contracts WHERE capability_id=? AND version=?').get(entry.capabilityId, entry.version);
        assert(!pinned || pinned.digest === entry.digest, 'capability_version_conflict');
        if (!pinned) db.prepare('INSERT INTO cap_contracts(capability_id,version,digest) VALUES(?,?,?)').run(entry.capabilityId, entry.version, entry.digest);
      }
    });
  } catch (error) { db.close(); closed = true; throw error; }
  const access = createAccessStore({ db, clock, transaction, actorActive, catalog: registry, limits: accessLimits });
  const invocations = createInvocationStore({
    db, clock, transaction, authorize: access.authorizeInvocation,
    reserveBudget: access.reserveBudget, settleBudget: access.settleBudget, canonicalHash, newId, limits: invocationLimits
  });
  const operations = new Set([...access.operations, 'access.invocations.list']);
  function execute(request) {
    if (request.op !== 'access.invocations.list') return access.execute(request);
    exact(request.args, ['expectedAccountId', 'limit', 'cursor']);
    const owner = access.verifyOwner(request);
    return invocations.listForOwner({ accountId: owner.accountId, limit: request.args.limit, cursor: request.args.cursor });
  }
  return Object.freeze({
    projectId, schemaVersion: storage.schemaVersion, registryId: storage.registryId,
    supportedSchemaVersions: CAPABILITIES_SUPPORTED_SCHEMA_VERSIONS,
    operations, execute,
    authenticateCredential: access.authenticateCredential, authorize: access.authorize,
    catalog: publicCatalog, invocations,
    close() { if (!closed) { db.close(); closed = true; } }
  });
}
