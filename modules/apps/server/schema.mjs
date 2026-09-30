import { createHash } from 'node:crypto';
import { AppsError, assertApps, appId, cleanGrants, textId } from './protocol.mjs';
import { canonicalOrigin, legacyZone, normalizeLegacyTemplate } from './domain-policy.mjs';

export const APPS_REGISTRY_SCHEMA = 'soty.apps-registry.v2';
const v1Schema = 'soty.apps-registry.v1';
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
const digest = value => createHash('sha256').update(value).digest('hex');
export const domainZoneId = zone => `zone_${digest(zone.origin_template ?? zone.template).slice(0, 32)}`;

function normalizedSql(sql) {
  // Only cosmetic differences outside string literals are accepted. This is a
  // known-DDL recognizer, not a claim to understand arbitrary equivalent SQL.
  return sql.split(/('(?:[^']|'')*')/gu).map((part, index) => index % 2 ? part
    : part.replace(/\bIF\s+NOT\s+EXISTS\b/giu, '').replace(/\s+/gu, '').replace(/;$/u, '').toLowerCase()).join('');
}
function definitions(sql) {
  return sql.split(';').filter(statement => statement.trim()).map(statement => {
    const name = statement.match(/^\s*CREATE\s+(?:UNIQUE\s+)?(?:TABLE|INDEX)\s+([A-Za-z_][A-Za-z0-9_]*)\b/u)?.[1];
    assertApps(name, 'apps_schema_definition_invalid', 500);
    return [name, normalizedSql(statement)];
  });
}

export function inspectAppsSchema(db) {
  const objects = db.prepare("SELECT type,name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' AND type IN ('table','index','view','trigger')").all();
  const version = Number(db.prepare('PRAGMA user_version').get().user_version);
  if (objects.length === 0 && version === 0) return 'empty';
  assertApps(objects.every(item => ['table', 'index'].includes(item.type)) && objects.some(item => item.name === 'apps_meta'), 'apps_schema_unsupported');
  const meta = db.prepare('PRAGMA table_info(apps_meta)').all();
  assertApps(JSON.stringify(meta.map(item => item.name)) === JSON.stringify(core.apps_meta), 'apps_schema_unsupported');
  const schema = db.prepare("SELECT value FROM apps_meta WHERE key='schema'").get()?.value;
  const state = schema === v1Schema && [0, 1].includes(version) ? 'v1'
    : schema === APPS_REGISTRY_SCHEMA && version === 2 ? 'v2' : '';
  assertApps(state, 'apps_schema_unsupported');
  const expected = state === 'v1' ? core : { ...core, ...domains };
  const sqlDefinitions = new Map([...definitions(coreDdl()), ...(state === 'v2' ? definitions(domainDdl()) : [])]);
  assertApps(objects.length === sqlDefinitions.size && objects.every(item => typeof item.sql === 'string'
    && sqlDefinitions.get(item.name) === normalizedSql(item.sql)), 'apps_schema_unsupported');
  for (const [name, columns] of Object.entries(expected)) {
    const actual = db.prepare(`PRAGMA table_info(${name})`).all();
    assertApps(JSON.stringify(actual.map(item => item.name)) === JSON.stringify(columns), 'apps_schema_unsupported');
    const primaryKeys = name === 'local_app_grants' ? ['app_id', 'kind', 'principal_id']
      : name === 'app_domain_receipts' ? ['account_id', 'request_key']
        : [name === 'apps_meta' ? 'key' : name === 'app_devices' ? 'connector_key' : name === 'app_domain_heads' ? 'app_id' : 'id'];
    for (const column of actual) {
      const integer = ['revision', 'created_at', 'updated_at', 'retired_at', 'committed_revision'].includes(column.name)
        || (name === 'local_apps' && column.name === 'port');
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
    if (before === 'v2') {
      const pinned = db.prepare("SELECT value FROM apps_meta WHERE key='legacy_origin_template'").get();
      assertApps(pinned && pinned.value === normalizedTemplate, 'apps_origin_template_changed', 409);
      db.exec('COMMIT');
      return { schema: APPS_REGISTRY_SCHEMA, migrated: false, legacyTemplate: pinned.value };
    }
    if (before === 'empty') db.exec(coreDdl());
    db.exec(domainDdl());
    const timestamp = now();
    if (normalizedTemplate) insertDomainZone(db, legacyZone(normalizedTemplate), timestamp);
    validateAndRebuildGrants(db, app => ensureCanonicalDomain(db, app, normalizedTemplate));
    db.prepare("INSERT OR REPLACE INTO apps_meta(key,value) VALUES ('schema',?),('legacy_origin_template',?)")
      .run(APPS_REGISTRY_SCHEMA, normalizedTemplate);
    db.exec('PRAGMA user_version=2; COMMIT');
    return { schema: APPS_REGISTRY_SCHEMA, migrated: true, legacyTemplate: normalizedTemplate };
  } catch (error) {
    if (db.isTransaction) db.exec('ROLLBACK');
    throw error;
  }
}
