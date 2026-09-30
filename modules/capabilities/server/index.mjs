import { mkdirSync } from 'node:fs';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createAccessStore } from './access.mjs';
import { BUILTIN_CAPABILITIES, createCatalog } from './catalog.mjs';
import { createPublicDiscovery } from './discovery.mjs';
import { BUILTIN_DOCUMENTATION } from './documentation.mjs';
import { createInvocationStore } from './invocations.mjs';
import { createNativeNotesCoordinator, normalizeNativeNoteLimits, validateNativeNoteComposition } from './native-notes.mjs';
import { initializeCapabilitiesSchema, CAPABILITIES_SUPPORTED_SCHEMA_VERSIONS } from './schema.mjs';
import { normalizeOAuthConfiguration } from './oauth-profile.mjs';
import { createOAuthConnections, OAUTH_OPERATIONS } from './oauth-connections.mjs';
import { assert, canonicalHash, exact, integer, newId } from './validation.mjs';

export { ACCESS_OPERATIONS } from './access.mjs';
export { BUILTIN_CAPABILITIES } from './catalog.mjs';
export { AccessError } from './validation.mjs';
export { OAUTH_OPERATIONS } from './oauth-connections.mjs';

export function createCapabilitiesService({ databasePath, projectId, clock = Date.now, actorActive, catalog = BUILTIN_CAPABILITIES,
  documentation = BUILTIN_DOCUMENTATION, limits = {}, allowNativeMigration = false, allowOAuthMigration = false, nativeNotes, oauth } = {}) {
  assert(typeof databasePath === 'string' && databasePath.length > 0, 'database_path_required');
  assert(typeof projectId === 'string' && /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u.test(projectId), 'project_id_required');
  assert(typeof allowNativeMigration === 'boolean' && typeof allowOAuthMigration === 'boolean', 'schema_configuration_invalid');
  assert(typeof actorActive === 'function', 'host_auth_required');
  assert(typeof clock === 'function', 'clock_invalid');
  const nativeComposition = validateNativeNoteComposition(nativeNotes);
  const oauthComposition = normalizeOAuthConfiguration(oauth);
  exact(limits, ['access', 'invocations', 'nativeNotes'], 'limits_invalid');
  const nativeLimits = normalizeNativeNoteLimits(limits.nativeNotes);
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
  try { storage = initializeCapabilitiesSchema(db, { projectId, allowNativeMigration, allowOAuthMigration }); }
  catch (error) { db.close(); throw error; }
  let closed = false;
  let inTransaction = false;
  const ensureOpen = () => assert(!closed, 'service_closed');
  function transaction(fn, { busyMs } = {}) {
    assert(!closed, 'service_closed');
    assert(!inTransaction && !db.isTransaction, 'nested_transaction');
    inTransaction = true;
    let result, failure, failed = false, timeout, began = false;
    try {
      if (busyMs !== undefined) { timeout = db.prepare('PRAGMA busy_timeout').get().timeout; db.exec(`PRAGMA busy_timeout=${busyMs}`); }
      db.exec('BEGIN IMMEDIATE'); began = true;
      result = fn();
      assert(!result || typeof result.then !== 'function', 'async_transaction_not_allowed');
      db.exec('COMMIT');
    } catch (error) {
      failed = true; failure = error;
      if (began && db.isTransaction) { try { db.exec('ROLLBACK'); } catch { /* Preserve the primary failure. */ } }
    } finally {
      try { if (timeout !== undefined) db.exec(`PRAGMA busy_timeout=${timeout}`); }
      catch (error) { if (!failed) { failed = true; failure = error; } }
      inTransaction = false;
    }
    if (failed) throw failure;
    return result;
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
  let invocationCore, nativeSettlement, oauthAuthority;
  const access = createAccessStore({ db, clock, transaction, actorActive, catalog: registry, limits: accessLimits,
    captureNativeSettlement: nativeComposition ? value => { nativeSettlement = value; } : undefined,
    captureOAuthAuthority: oauthComposition ? value => { oauthAuthority = value; } : undefined });
  const invocations = createInvocationStore({
    db, clock, transaction, authorize: access.authorizeInvocation,
    reserveBudget: access.reserveBudget, settleBudget: access.settleBudget, canonicalHash, newId, limits: invocationLimits,
    captureInternalCore: nativeComposition ? value => { invocationCore = value; } : undefined,
  });
  let nativeBindingReady = () => false;
  const native = nativeComposition ? createNativeNotesCoordinator({ db, projectId, registryId: storage.registryId, schemaVersion: storage.schemaVersion, clock,
    registry, access, core: invocationCore, settleNativeBudget: nativeSettlement, transaction, ensureOpen,
    composition: nativeComposition, limits: nativeLimits, invocationLimits, oauthScope: oauthAuthority?.scope,
    captureBindingReadiness(value) { nativeBindingReady = value; } }) : null;
  const oauthCoordinator = oauthComposition ? createOAuthConnections({ db, projectId, registryId: storage.registryId,
    schemaVersion: storage.schemaVersion, clock, transaction, ensureOpen, configuration: oauthComposition, access,
    authority: oauthAuthority, nativeBindingReady }) : null;
  const operations = new Set([...access.operations, 'access.invocations.list', ...(oauthCoordinator ? OAUTH_OPERATIONS : [])]);
  function execute(request) {
    if (oauthCoordinator && OAUTH_OPERATIONS.includes(request.op)) return oauthCoordinator.execute(request);
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
    catalog: publicCatalog, invocations, nativeNotes: native,
    ...(oauthCoordinator ? { oauth: oauthCoordinator.oauth } : {}),
    close() { if (!closed) { assert(!inTransaction && !db.isTransaction, 'nested_transaction'); oauthCoordinator?.close();
      db.close(); closed = true; oauthComposition?.artifactKey?.fill(0); } }
  });
}
