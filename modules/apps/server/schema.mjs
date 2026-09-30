import { createHash } from 'node:crypto';
import { AppsError, assertApps, appId, appPort, requestPath, cleanGrants, textId } from './protocol.mjs';
import { canonicalOrigin, legacyZone, normalizeLegacyTemplate } from './domain-policy.mjs';

export const APPS_REGISTRY_SCHEMA = 'soty.apps-registry.v3';
const v1Schema = 'soty.apps-registry.v1';
const v2Schema = 'soty.apps-registry.v2';
export const RUNTIME_PROFILE = 'soty.relay-restricted.v1';
const core = {
  apps_meta: ['key', 'value'],
  app_devices: ['connector_key', 'owner_account_id', 'identity_json', 'name', 'created_at'],
  local_apps: ['id', 'owner_account_id', 'connector_key', 'name', 'port', 'entry_path', 'grants_json', 'state', 'revision', 'created_at', 'updated_at'],
  local_app_grants: ['app_id', 'kind', 'principal_id'],
};
const domains = {
  app_domain_zones: ['id', 'kind', 'origin_template', 'suffix', 'scheme', 'port', 'created_at'],
  app_domain_heads: ['app_id', 'revision'],
  app_domains: ['id', 'zone_id', 'hostname', 'origin', 'slug', 'app_id', 'owner_account_id', 'role', 'state', 'created_at', 'retired_at'],
  app_domain_receipts: ['account_id', 'request_key', 'intent_hash', 'action', 'domain_id', 'committed_revision', 'created_at'],
};
const publications = {
  app_runtime_targets: ['app_id', 'revision', 'owner_account_id', 'connector_key', 'port', 'entry_path', 'profile', 'digest', 'created_at'],
  app_publications: ['app_id', 'owner_account_id', 'launch_policy', 'listed', 'policy_epoch', 'active_target_revision', 'exposure_ack_revision', 'exposure_ack_json', 'updated_at'],
  app_publication_domains: ['app_id', 'domain_id', 'owner_account_id'],
  app_publication_receipts: ['account_id', 'request_key', 'intent_hash', 'app_id', 'committed_epoch', 'value_json', 'created_at'],
};
const digest = value => createHash('sha256').update(value).digest('hex');
export const domainZoneId = zone => `zone_${digest(zone.origin_template ?? zone.template).slice(0, 32)}`;

function normalizedSql(sql) {
  // Only cosmetic differences outside string literals are accepted. This is a
  // known-DDL recognizer, not a claim to understand arbitrary equivalent SQL.
  return sql.split(/('(?:[^']|'')*')/gu).map((part, index) => index % 2 ? part
    : part.replace(/\bIF\s+NOT\s+EXISTS\b/giu, '').replace(/\s+/gu, '').replace(/;$/u, '').toLowerCase()).join('');
}
function definitions(sql) {
  return (Array.isArray(sql) ? sql : sql.split(';')).filter(statement => statement.trim()).map(statement => {
    const name = statement.match(/^\s*CREATE\s+(?:UNIQUE\s+)?(?:TABLE|INDEX|TRIGGER)\s+([A-Za-z_][A-Za-z0-9_]*)\b/u)?.[1];
    assertApps(name, 'apps_schema_definition_invalid', 500);
    return [name, normalizedSql(statement)];
  });
}

export function inspectAppsSchema(db) {
  const objects = db.prepare("SELECT type,name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' AND type IN ('table','index','view','trigger')").all();
  const version = Number(db.prepare('PRAGMA user_version').get().user_version);
  if (objects.length === 0 && version === 0) return 'empty';
  assertApps(objects.some(item => item.name === 'apps_meta' && item.type === 'table'), 'apps_schema_unsupported');
  const meta = db.prepare('PRAGMA table_info(apps_meta)').all();
  assertApps(JSON.stringify(meta.map(item => item.name)) === JSON.stringify(core.apps_meta), 'apps_schema_unsupported');
  const schema = db.prepare("SELECT value FROM apps_meta WHERE key='schema'").get()?.value;
  const state = schema === v1Schema && [0, 1].includes(version) ? 'v1'
    : schema === v2Schema && version === 2 ? 'v2' : schema === APPS_REGISTRY_SCHEMA && version === 3 ? 'v3' : '';
  assertApps(state, 'apps_schema_unsupported');
  const expected = state === 'v1' ? core : { ...core, ...domains, ...(state === 'v3' ? publications : {}) };
  const sqlDefinitions = new Map([...definitions(coreDdl()), ...(state !== 'v1' ? definitions(domainDdl()) : []),
    ...(state === 'v3' ? [...definitions(publicationDdl()), ...definitions(targetGuards())] : [])]);
  assertApps(objects.length === sqlDefinitions.size && objects.every(item => typeof item.sql === 'string'
    && sqlDefinitions.get(item.name) === normalizedSql(item.sql)), 'apps_schema_unsupported');
  for (const [name, columns] of Object.entries(expected)) {
    const actual = db.prepare(`PRAGMA table_info(${name})`).all();
    assertApps(JSON.stringify(actual.map(item => item.name)) === JSON.stringify(columns), 'apps_schema_unsupported');
    const primaryKeys = name === 'local_app_grants' ? ['app_id', 'kind', 'principal_id']
      : ['app_domain_receipts', 'app_publication_receipts'].includes(name) ? ['account_id', 'request_key']
        : name === 'app_runtime_targets' ? ['app_id', 'revision']
          : name === 'app_publication_domains' ? ['app_id', 'domain_id']
            : [name === 'apps_meta' ? 'key' : name === 'app_devices' ? 'connector_key'
              : ['app_domain_heads', 'app_publications'].includes(name) ? 'app_id' : 'id'];
    for (const column of actual) {
      const integer = ['revision', 'created_at', 'updated_at', 'retired_at', 'committed_revision', 'port', 'listed',
        'policy_epoch', 'active_target_revision', 'exposure_ack_revision', 'committed_epoch'].includes(column.name)
        && !(name === 'app_domain_zones' && column.name === 'port');
      assertApps(column.type.toUpperCase() === (integer ? 'INTEGER' : 'TEXT')
        && column.pk === primaryKeys.indexOf(column.name) + 1, 'apps_schema_unsupported');
    }
  }
  return state;
}

function coreDdl() {
  return `CREATE TABLE apps_meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
    CREATE TABLE app_devices (connector_key TEXT PRIMARY KEY,owner_account_id TEXT NOT NULL,identity_json TEXT NOT NULL,name TEXT NOT NULL,created_at INTEGER NOT NULL);
    CREATE TABLE local_apps (id TEXT PRIMARY KEY,owner_account_id TEXT NOT NULL,connector_key TEXT NOT NULL REFERENCES app_devices(connector_key),name TEXT NOT NULL,port INTEGER NOT NULL,entry_path TEXT NOT NULL,grants_json TEXT NOT NULL,state TEXT NOT NULL,revision INTEGER NOT NULL,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL);
    CREATE INDEX local_apps_owner ON local_apps(owner_account_id);
    CREATE TABLE local_app_grants (app_id TEXT NOT NULL REFERENCES local_apps(id) ON DELETE CASCADE,kind TEXT NOT NULL CHECK(kind IN ('account','community')),principal_id TEXT NOT NULL,PRIMARY KEY(app_id,kind,principal_id));
    CREATE INDEX local_app_grants_principal ON local_app_grants(kind,principal_id,app_id);`;
}

function domainDdl() {
  return `CREATE TABLE app_domain_zones (
      id TEXT PRIMARY KEY, kind TEXT NOT NULL CHECK(kind IN ('legacy','named')), origin_template TEXT NOT NULL UNIQUE,
      suffix TEXT NOT NULL, scheme TEXT NOT NULL CHECK(scheme IN ('https','http')), port TEXT NOT NULL, created_at INTEGER NOT NULL);
    CREATE TABLE app_domain_heads (app_id TEXT PRIMARY KEY REFERENCES local_apps(id),revision INTEGER NOT NULL CHECK(revision>=0));
    CREATE TABLE app_domains (
      id TEXT PRIMARY KEY,zone_id TEXT NOT NULL REFERENCES app_domain_zones(id),hostname TEXT NOT NULL UNIQUE,
      origin TEXT NOT NULL UNIQUE,slug TEXT,app_id TEXT NOT NULL REFERENCES local_apps(id),owner_account_id TEXT NOT NULL,
      role TEXT NOT NULL CHECK(role IN ('canonical','alias')),state TEXT NOT NULL CHECK(state IN ('bound','tombstone')),
      created_at INTEGER NOT NULL,retired_at INTEGER,
      CHECK((role='canonical' AND slug IS NULL AND state='bound' AND retired_at IS NULL) OR
        (role='alias' AND slug IS NOT NULL AND ((state='bound' AND retired_at IS NULL) OR (state='tombstone' AND retired_at IS NOT NULL)))));
    CREATE UNIQUE INDEX app_domain_canonical ON app_domains(app_id) WHERE role='canonical';
    CREATE INDEX app_domain_app ON app_domains(app_id,created_at,id);
    CREATE INDEX app_domain_owner ON app_domains(owner_account_id,role);
    CREATE TABLE app_domain_receipts (
      account_id TEXT NOT NULL,request_key TEXT NOT NULL,intent_hash TEXT NOT NULL,
      action TEXT NOT NULL CHECK(action IN ('claim','retire')),domain_id TEXT NOT NULL REFERENCES app_domains(id),
      committed_revision INTEGER NOT NULL,created_at INTEGER NOT NULL,PRIMARY KEY(account_id,request_key));`;
}

function publicationDdl() {
  return `CREATE UNIQUE INDEX local_apps_identity_owner ON local_apps(id,owner_account_id);
    CREATE UNIQUE INDEX app_devices_identity_owner ON app_devices(connector_key,owner_account_id);
    CREATE UNIQUE INDEX app_domains_identity_owner ON app_domains(id,app_id,owner_account_id);
    CREATE TABLE app_runtime_targets (
      app_id TEXT NOT NULL,revision INTEGER NOT NULL CHECK(revision BETWEEN 1 AND 9007199254740991),
      owner_account_id TEXT NOT NULL,connector_key TEXT NOT NULL,port INTEGER NOT NULL CHECK(port BETWEEN 1024 AND 65535),
      entry_path TEXT NOT NULL,profile TEXT NOT NULL CHECK(profile='soty.relay-restricted.v1'),digest TEXT NOT NULL,
      created_at INTEGER NOT NULL,PRIMARY KEY(app_id,revision),
      FOREIGN KEY(app_id,owner_account_id) REFERENCES local_apps(id,owner_account_id),
      FOREIGN KEY(connector_key,owner_account_id) REFERENCES app_devices(connector_key,owner_account_id));
    CREATE TABLE app_publications (
      app_id TEXT PRIMARY KEY,owner_account_id TEXT NOT NULL,
      launch_policy TEXT NOT NULL CHECK(launch_policy IN ('restricted','anyone')),
      listed INTEGER NOT NULL CHECK(listed IN (0,1)),policy_epoch INTEGER NOT NULL CHECK(policy_epoch BETWEEN 1 AND 9007199254740991),
      active_target_revision INTEGER NOT NULL,exposure_ack_revision INTEGER,exposure_ack_json TEXT,updated_at INTEGER NOT NULL,
      FOREIGN KEY(app_id,owner_account_id) REFERENCES local_apps(id,owner_account_id),
      FOREIGN KEY(app_id,active_target_revision) REFERENCES app_runtime_targets(app_id,revision),
      FOREIGN KEY(app_id,exposure_ack_revision) REFERENCES app_runtime_targets(app_id,revision),
      CHECK(listed=0 OR launch_policy='anyone'),
      CHECK((exposure_ack_revision IS NULL AND exposure_ack_json IS NULL) OR (exposure_ack_revision IS NOT NULL AND exposure_ack_json IS NOT NULL)),
      CHECK(launch_policy='restricted' OR (exposure_ack_revision IS NOT NULL AND exposure_ack_revision=active_target_revision AND exposure_ack_json IS NOT NULL)));
    CREATE TABLE app_publication_domains (
      app_id TEXT NOT NULL,domain_id TEXT NOT NULL,owner_account_id TEXT NOT NULL,PRIMARY KEY(app_id,domain_id),
      FOREIGN KEY(app_id) REFERENCES app_publications(app_id),
      FOREIGN KEY(app_id,owner_account_id) REFERENCES local_apps(id,owner_account_id),
      FOREIGN KEY(domain_id,app_id,owner_account_id) REFERENCES app_domains(id,app_id,owner_account_id));
    CREATE TABLE app_publication_receipts (
      account_id TEXT NOT NULL,request_key TEXT NOT NULL,intent_hash TEXT NOT NULL,app_id TEXT NOT NULL,
      committed_epoch INTEGER NOT NULL CHECK(committed_epoch BETWEEN 2 AND 9007199254740991),value_json TEXT NOT NULL,created_at INTEGER NOT NULL,
      PRIMARY KEY(account_id,request_key),FOREIGN KEY(app_id,account_id) REFERENCES local_apps(id,owner_account_id));
    CREATE UNIQUE INDEX app_publication_receipt_epoch ON app_publication_receipts(app_id,committed_epoch);`;
}

function targetGuards() {
  return [
    "CREATE TRIGGER app_runtime_target_no_update BEFORE UPDATE ON app_runtime_targets BEGIN SELECT RAISE(ABORT,'app_runtime_target_immutable'); END",
    "CREATE TRIGGER app_runtime_target_no_delete BEFORE DELETE ON app_runtime_targets BEGIN SELECT RAISE(ABORT,'app_runtime_target_immutable'); END",
  ];
}

export function runtimeTargetDigest(value) {
  return digest(JSON.stringify(['soty.runtime-target.v1', value.appId, value.revision, value.ownerAccountId,
    value.connectorKey, value.port, value.entryPath, value.profile]));
}

// Shared by the migration and registration; source changes are a separate stage.
export function ensureInitialPublication(db, app) {
  assertApps(db.isTransaction === true, 'apps_transaction_required', 500);
  const port = appPort(app.port), entryPath = requestPath(app.entry_path);
  assertApps(!entryPath.startsWith('/_soty/'), 'apps_registry_corrupt', 500);
  const device = db.prepare('SELECT owner_account_id FROM app_devices WHERE connector_key=?').get(app.connector_key);
  assertApps(device?.owner_account_id === app.owner_account_id, 'apps_registry_corrupt', 500);
  const target = { appId: app.id, revision: 1, ownerAccountId: app.owner_account_id, connectorKey: app.connector_key,
    port, entryPath, profile: RUNTIME_PROFILE };
  const targetDigest = runtimeTargetDigest(target);
  db.prepare('INSERT OR IGNORE INTO app_runtime_targets VALUES (?,?,?,?,?,?,?,?,?)')
    .run(app.id, 1, app.owner_account_id, app.connector_key, port, entryPath, RUNTIME_PROFILE, targetDigest, app.created_at);
  const existing = db.prepare('SELECT digest FROM app_runtime_targets WHERE app_id=? AND revision=1').get(app.id);
  assertApps(existing?.digest === targetDigest, 'apps_initial_target_changed', 409);
  db.prepare("INSERT OR IGNORE INTO app_publications VALUES (?,?,'restricted',0,1,1,NULL,NULL,?)")
    .run(app.id, app.owner_account_id, app.updated_at);
}

function initializePublications(db) {
  assertApps(!db.prepare('PRAGMA foreign_key_check').get(), 'apps_registry_corrupt', 500);
  assertApps(!db.prepare(`SELECT 1 FROM app_domains d JOIN local_apps a ON a.id=d.app_id
    WHERE d.owner_account_id<>a.owner_account_id LIMIT 1`).get(), 'apps_registry_corrupt', 500);
  for (const app of db.prepare('SELECT * FROM local_apps').iterate()) {
    try {
      appId(app.id); textId(app.owner_account_id); cleanGrants(JSON.parse(app.grants_json));
      assertApps(['enabled', 'revoked'].includes(app.state) && Number.isSafeInteger(app.revision) && app.revision >= 1, 'apps_registry_corrupt', 500);
      assertApps(db.prepare('SELECT 1 FROM app_domain_heads WHERE app_id=?').get(app.id), 'apps_registry_corrupt', 500);
      ensureInitialPublication(db, app);
    } catch { throw new AppsError('apps_registry_corrupt', 500); }
  }
}

export function insertDomainZone(db, zone, timestamp) {
  const id = domainZoneId(zone);
  db.prepare('INSERT OR IGNORE INTO app_domain_zones VALUES (?,?,?,?,?,?,?)')
    .run(id, zone.kind, zone.template, zone.suffix, zone.scheme, zone.port, timestamp);
  return id;
}

// Called only inside the caller's app registration / migration transaction.
export function ensureCanonicalDomain(db, { id, owner_account_id: ownerAccountId, created_at: createdAt }, legacyTemplate) {
  assertApps(db.isTransaction === true, 'apps_transaction_required', 500);
  db.prepare('INSERT OR IGNORE INTO app_domain_heads VALUES (?,0)').run(id);
  if (!legacyTemplate) return;
  const zoneId = insertDomainZone(db, legacyZone(legacyTemplate), createdAt);
  const origin = canonicalOrigin(legacyTemplate, id);
  db.prepare("INSERT OR IGNORE INTO app_domains VALUES (?,?,?,?,NULL,?,?,'canonical','bound',?,NULL)")
    .run(`dom_${digest(`canonical:${id}`).slice(0, 32)}`, zoneId, new URL(origin).hostname, origin, id, ownerAccountId, createdAt);
  const recorded = db.prepare("SELECT origin,owner_account_id FROM app_domains WHERE app_id=? AND role='canonical'").get(id);
  assertApps(recorded?.origin === origin && recorded.owner_account_id === ownerAccountId, 'apps_canonical_origin_conflict', 409);
}

function validateAndRebuildGrants(db, visit) {
  assertApps(!db.prepare('PRAGMA foreign_key_check').get(), 'apps_registry_corrupt', 500);
  // The migration is atomic but does not materialize every app in process memory.
  db.exec('DELETE FROM local_app_grants');
  const insert = db.prepare('INSERT INTO local_app_grants VALUES (?,?,?)');
  for (const row of db.prepare('SELECT * FROM local_apps').iterate()) {
    let grants;
    try {
      appId(row.id); textId(row.owner_account_id);
      assertApps(['enabled', 'revoked'].includes(row.state) && Number.isSafeInteger(row.revision) && row.revision >= 1, 'apps_registry_corrupt');
      grants = cleanGrants(JSON.parse(row.grants_json));
    } catch { throw new AppsError('apps_registry_corrupt', 500); }
    // grants_json remains the v1 authority; stale optimization rows never broaden access.
    for (const id of grants.accountIds) insert.run(row.id, 'account', id);
    for (const id of grants.communityIds) insert.run(row.id, 'community', id);
    visit(row);
  }
}

export function migrateAppsSchema(db, { legacyTemplate = '', now = Date.now } = {}) {
  const normalizedTemplate = normalizeLegacyTemplate(legacyTemplate);
  // Inspect before any DDL or persistent PRAGMA. Unknown/future schemas are untouched.
  inspectAppsSchema(db);
  db.exec('BEGIN IMMEDIATE');
  try {
    const before = inspectAppsSchema(db); // another process may have migrated while this connection waited
    if (before === 'v2' || before === 'v3') {
      const pinned = db.prepare("SELECT value FROM apps_meta WHERE key='legacy_origin_template'").get();
      assertApps(pinned && pinned.value === normalizedTemplate, 'apps_origin_template_changed', 409);
      if (before === 'v3') {
        db.exec('COMMIT');
        return { schema: APPS_REGISTRY_SCHEMA, migrated: false, legacyTemplate: pinned.value };
      }
    }
    if (before === 'empty') db.exec(coreDdl());
    if (before === 'empty' || before === 'v1') {
      db.exec(domainDdl());
      const timestamp = now();
      if (normalizedTemplate) insertDomainZone(db, legacyZone(normalizedTemplate), timestamp);
      validateAndRebuildGrants(db, app => ensureCanonicalDomain(db, app, normalizedTemplate));
    }
    db.exec(publicationDdl());
    for (const statement of targetGuards()) db.exec(statement);
    initializePublications(db);
    db.prepare("INSERT OR REPLACE INTO apps_meta(key,value) VALUES ('schema',?),('legacy_origin_template',?)")
      .run(APPS_REGISTRY_SCHEMA, normalizedTemplate);
    db.exec('PRAGMA user_version=3; COMMIT');
    return { schema: APPS_REGISTRY_SCHEMA, migrated: true, legacyTemplate: normalizedTemplate };
  } catch (error) {
    if (db.isTransaction) db.exec('ROLLBACK');
    throw error;
  }
}
