// This trusted host source is passed to a pinned helper's Node executable.
// Do not import application code: the candidate must not attest its own reader.
import { lstat, opendir } from 'node:fs/promises';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';

const fail = code => { throw Object.assign(new Error(code), { code }); };
const missing = async file => { try { return await lstat(file); } catch (e) { if (e.code === 'ENOENT') return null; throw e; } };
const roomTablesV2 = ['room_state', 'room_files', 'room_receipts', 'room_events', 'room_imports', 'room_deletions'];
const serviceFiles = new Set(['connector-store.json', 'connector-maintenance.json', 'connector-rollback.json', 'traffic-control.json', 'traffic-exit-pool.json']);
const appCore = {
  apps_meta: 'key,value',
  app_devices: 'connector_key,owner_account_id,identity_json,name,created_at',
  local_apps: 'id,owner_account_id,connector_key,name,port,entry_path,grants_json,state,revision,created_at,updated_at',
  local_app_grants: 'app_id,kind,principal_id',
};
const appDomains = {
  app_domain_zones: 'id,kind,origin_template,suffix,scheme,port,created_at',
  app_domain_heads: 'app_id,revision',
  app_domains: 'id,zone_id,hostname,origin,slug,app_id,owner_account_id,role,state,created_at,retired_at',
  app_domain_receipts: 'account_id,request_key,intent_hash,action,domain_id,committed_revision,created_at',
};
const appPublications = {
  app_runtime_targets: 'app_id,revision,owner_account_id,connector_key,port,entry_path,profile,digest,created_at',
  app_publications: 'app_id,owner_account_id,launch_policy,listed,policy_epoch,active_target_revision,exposure_ack_revision,exposure_ack_json,updated_at',
  app_publication_domains: 'app_id,domain_id,owner_account_id',
  app_publication_receipts: 'account_id,request_key,intent_hash,app_id,committed_epoch,value_json,created_at',
};
const appSources = {
  app_source_heads: 'app_id,required_binding_version',
  app_source_receipts: 'account_id,request_key,intent_hash,app_id,committed_epoch,value_json,created_at',
};
const appSaved = {
  app_saved_heads: 'account_id,revision',
  app_saved_entries: 'account_id,app_id,domain_id,origin,path,label,saved_revision,updated_at',
  app_saved_receipts: 'account_id,request_key,intent_hash,app_id,saved,committed_revision,created_at',
};
// Frozen host-side recognition of the two v3 immutable-target guards. Matching
// names alone would also admit a replaced trigger with different behavior.
const appTargetGuards = {
  app_runtime_target_no_update: { table: 'app_runtime_targets', sql: "CREATE TRIGGER app_runtime_target_no_update BEFORE UPDATE ON app_runtime_targets BEGIN SELECT RAISE(ABORT,'app_runtime_target_immutable'); END" },
  app_runtime_target_no_delete: { table: 'app_runtime_targets', sql: "CREATE TRIGGER app_runtime_target_no_delete BEFORE DELETE ON app_runtime_targets BEGIN SELECT RAISE(ABORT,'app_runtime_target_immutable'); END" },
};
// The v4 floor may only increase. These definitions are frozen independently of
// the application migration and do not attest that a runtime speaks binding v2.
const appSourceGuards = {
  app_source_head_no_downgrade: { table: 'app_source_heads', sql: "CREATE TRIGGER app_source_head_no_downgrade BEFORE UPDATE ON app_source_heads WHEN NEW.app_id<>OLD.app_id OR NEW.required_binding_version<OLD.required_binding_version BEGIN SELECT RAISE(ABORT,'app_source_binding_downgrade'); END" },
  app_source_head_no_delete: { table: 'app_source_heads', sql: "CREATE TRIGGER app_source_head_no_delete BEFORE DELETE ON app_source_heads BEGIN SELECT RAISE(ABORT,'app_source_head_required'); END" },
  app_source_head_no_replace_downgrade: { table: 'app_source_heads', sql: "CREATE TRIGGER app_source_head_no_replace_downgrade BEFORE INSERT ON app_source_heads WHEN EXISTS (SELECT 1 FROM app_source_heads WHERE app_id=NEW.app_id AND required_binding_version>NEW.required_binding_version) BEGIN SELECT RAISE(ABORT,'app_source_binding_downgrade'); END" },
  app_runtime_target_no_replace: { table: 'app_runtime_targets', sql: "CREATE TRIGGER app_runtime_target_no_replace BEFORE INSERT ON app_runtime_targets WHEN EXISTS (SELECT 1 FROM app_runtime_targets WHERE app_id=NEW.app_id AND revision=NEW.revision) BEGIN SELECT RAISE(ABORT,'app_runtime_target_immutable'); END" },
};
// Apps5 is saved-only. Its monotonic account revision remains after removal;
// missing or replaced guards must not be mistaken for this accepted format.
const appSavedGuards = {
  app_saved_head_no_downgrade: { table: 'app_saved_heads', sql: "CREATE TRIGGER app_saved_head_no_downgrade BEFORE UPDATE ON app_saved_heads WHEN NEW.account_id<>OLD.account_id OR NEW.revision<=OLD.revision BEGIN SELECT RAISE(ABORT,'app_saved_revision_not_increasing'); END" },
  app_saved_head_no_delete: { table: 'app_saved_heads', sql: "CREATE TRIGGER app_saved_head_no_delete BEFORE DELETE ON app_saved_heads BEGIN SELECT RAISE(ABORT,'app_saved_head_required'); END" },
  app_saved_head_no_replace: { table: 'app_saved_heads', sql: "CREATE TRIGGER app_saved_head_no_replace BEFORE INSERT ON app_saved_heads WHEN EXISTS (SELECT 1 FROM app_saved_heads WHERE account_id=NEW.account_id) BEGIN SELECT RAISE(ABORT,'app_saved_head_immutable'); END" },
};
const normalizedSql = sql => typeof sql === 'string' ? sql.split(/('(?:[^']|'')*')/gu)
  .map((part, index) => index % 2 ? part : part.replace(/\s+/gu, '').replace(/;$/u, '').toLowerCase()).join('') : null;

async function checkedDatabaseFile(filename, info) {
  if (!info.isFile() || info.isSymbolicLink() || info.size < 100) fail('storage_format_unreadable');
  for (const suffix of ['-wal', '-shm', '-journal']) {
    const journal = await missing(filename + suffix);
    if (journal && (!journal.isFile() || journal.isSymbolicLink())) fail('storage_format_unreadable');
  }
}

function inspectDatabase(filename, inspect) {
  let db;
  try {
    // Read WAL normally. immutable=1 could silently ignore committed WAL data.
    db = new DatabaseSync(filename, { readOnly: true });
    db.exec('PRAGMA query_only=ON; PRAGMA busy_timeout=3000; BEGIN');
    return inspect(db);
  } catch (e) {
    if (e.code === 'storage_format_unknown' || e.code === 'storage_format_unreadable') throw e;
    fail('storage_format_unreadable');
  } finally { db?.close(); }
}

async function readRoomsFormat(dataDir) {
  const filename = path.join(dataDir, 'rooms-v2.sqlite');
  const info = await missing(filename);
  if (!info) {
    // An orphan journal is never interpreted as a fresh, empty volume.
    for (const suffix of ['-wal', '-shm', '-journal']) if (await missing(filename + suffix)) fail('storage_format_unreadable');
    let legacy = false;
    for await (const entry of await opendir(dataDir)) {
      if (!serviceFiles.has(entry.name) && /^[A-Za-z0-9_-]{16,96}\.json$/u.test(entry.name)) {
        if (!entry.isFile() || entry.isSymbolicLink()) fail('storage_format_unreadable');
        legacy = true;
      }
    }
    return legacy ? 1 : 'empty';
  }
  await checkedDatabaseFile(filename, info);
  return inspectDatabase(filename, db => {
    if (db.prepare('PRAGMA user_version').get().user_version !== 2) fail('storage_format_unknown');
    const tables = new Set(db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map(r => r.name));
    if (roomTablesV2.some(name => !tables.has(name))) fail('storage_format_unreadable');
    // Prove the essential projection is readable, without enumerating contents.
    db.prepare('SELECT room_id,auth,closed_json,sequence,reserved_bytes,update_bytes FROM room_state LIMIT 0').all();
    return 2;
  });
}

async function readAppsFormat(dataDir) {
  const directory = path.join(dataDir, 'apps'), root = await missing(directory);
  if (!root) return 'empty';
  if (!root.isDirectory() || root.isSymbolicLink()) fail('storage_format_unreadable');
  const filename = path.join(directory, 'registry.sqlite'), info = await missing(filename);
  if (!info) {
    // Only an absent or actually empty Apps directory is a fresh store. An
    // orphan journal, backup or unknown file is evidence requiring review.
    for await (const entry of await opendir(directory)) fail('storage_format_unreadable');
    return 'empty';
  }
  await checkedDatabaseFile(filename, info);
  return inspectDatabase(filename, db => {
    const objects = db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' AND type IN ('table','view','trigger')").all();
    if (!objects.some(row => row.type === 'table' && row.name === 'apps_meta')) fail('storage_format_unreadable');
    const markers = db.prepare("SELECT value FROM apps_meta WHERE key='schema' LIMIT 2").all();
    const version = db.prepare('PRAGMA user_version').get().user_version;
    if (markers.length !== 1) fail('storage_format_unreadable');
    const format = markers[0].value === 'soty.apps-registry.v1' && [0, 1].includes(version) ? 1
      : markers[0].value === 'soty.apps-registry.v2' && version === 2 ? 2
        : markers[0].value === 'soty.apps-registry.v3' && version === 3 ? 3
          : markers[0].value === 'soty.apps-registry.v4' && version === 4 ? 4
            : markers[0].value === 'soty.apps-registry.v5' && version === 5 ? 5 : null;
    if (!format) fail('storage_format_unknown');
    const projections = { ...appCore, ...(format >= 2 ? appDomains : {}), ...(format >= 3 ? appPublications : {}),
      ...(format >= 4 ? appSources : {}), ...(format === 5 ? appSaved : {}) };
    const guards = { ...(format >= 3 ? appTargetGuards : {}), ...(format >= 4 ? appSourceGuards : {}),
      ...(format === 5 ? appSavedGuards : {}) };
    if (objects.length !== Object.keys(projections).length + Object.keys(guards).length || objects.some(row =>
      row.type === 'table' ? !Object.hasOwn(projections, row.name)
        : row.type !== 'trigger' || !Object.hasOwn(guards, row.name) || row.tbl_name !== guards[row.name].table
          || normalizedSql(row.sql) !== normalizedSql(guards[row.name].sql))) fail('storage_format_unreadable');
    // Independent format recognition, not row/constraint integrity attestation.
    // Every identifier below is a trusted constant, never database contents.
    for (const [table, columns] of Object.entries(projections)) db.prepare(`SELECT ${columns} FROM ${table} LIMIT 0`).all();
    if (format >= 2) {
      const pinned = db.prepare("SELECT value FROM apps_meta WHERE key='legacy_origin_template' LIMIT 2").all();
      if (pinned.length !== 1 || typeof pinned[0].value !== 'string') fail('storage_format_unreadable');
    }
    return format;
  });
}

export async function readStorageFormat(dataDir = '/data') {
  const root = await lstat(dataDir);
  if (!root.isDirectory() || root.isSymbolicLink()) fail('storage_directory_invalid');
  const rooms = await readRoomsFormat(dataDir), apps = await readAppsFormat(dataDir);
  return { ok: true, schema: 'soty.storage-format.v2', rooms, apps };
}

if (process.env.SOTY_STORAGE_PROBE === '1') {
  try { process.stdout.write(JSON.stringify(await readStorageFormat('/data'))); }
  catch (e) {
    const code = ['storage_directory_invalid', 'storage_format_unknown', 'storage_format_unreadable'].includes(e.code) ? e.code : 'storage_probe_failed';
    process.stdout.write(JSON.stringify({ ok: false, code })); process.exitCode = 1;
  }
}
