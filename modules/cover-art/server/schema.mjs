export const COVER_SCHEMA_VERSION = 1;
export const COVER_FORMATS = Object.freeze({ app: Object.freeze([12,13,14]), profile: Object.freeze([6]) });
export function coverPrefix(kind) {
  if (kind === 'app') return 'app_cover';
  if (kind === 'profile') return 'profile_avatar';
  throw Object.assign(new Error('cover_kind_invalid'), { code: 'cover_kind_invalid' });
}
const normalize = sql => String(sql).replace(/\s+/gu, '').replace(/;$/u, '');

/** DDL fragments only. The owning Apps/World migration must execute them in its
 * own admitted transaction and commit the matching outer format atomically. */
export function coverSchemaStatements(kind) {
  const p = coverPrefix(kind), foreign = kind === 'app' ? 'local_apps(id)' : 'profiles(account_id)';
  const ownerScope=kind==='app'?'FOREIGN KEY(target_id,owner_id) REFERENCES local_apps(id,owner_account_id)':'CHECK(owner_id=target_id)';
  return [
    ...(kind==='app'?[`CREATE UNIQUE INDEX ${p}_target_owner ON local_apps(id,owner_account_id)`]:[]),
    `CREATE TABLE ${p}_usage (id INTEGER PRIMARY KEY CHECK(id=1),store_id TEXT NOT NULL,encoded_bytes INTEGER NOT NULL CHECK(encoded_bytes>=0),reserved_bytes INTEGER NOT NULL CHECK(reserved_bytes>=0),draft_count INTEGER NOT NULL CHECK(draft_count>=0),receipt_count INTEGER NOT NULL CHECK(receipt_count>=0),logical_now INTEGER NOT NULL,minimum_request_time INTEGER NOT NULL)`,
    `CREATE TABLE ${p}_owner_usage (owner_id TEXT PRIMARY KEY,encoded_bytes INTEGER NOT NULL CHECK(encoded_bytes>=0),reserved_bytes INTEGER NOT NULL CHECK(reserved_bytes>=0),draft_count INTEGER NOT NULL CHECK(draft_count>=0),receipt_count INTEGER NOT NULL CHECK(receipt_count>=0),rate_at INTEGER NOT NULL,rate_count INTEGER NOT NULL CHECK(rate_count>=0))`,
    `CREATE TABLE ${p}_assets (id TEXT PRIMARY KEY,target_id TEXT NOT NULL REFERENCES ${foreign},owner_id TEXT NOT NULL,origin TEXT NOT NULL CHECK(origin IN('upload','legacy')),target_revision INTEGER NOT NULL,policy_epoch INTEGER NOT NULL,recipe TEXT NOT NULL,encoded_bytes INTEGER NOT NULL CHECK(encoded_bytes>0),created_at INTEGER NOT NULL,purged_at INTEGER,UNIQUE(target_id,owner_id,id),${ownerScope})`,
    `CREATE TABLE ${p}_renditions (asset_id TEXT NOT NULL REFERENCES ${p}_assets(id),role TEXT NOT NULL,width INTEGER NOT NULL CHECK(width>0),height INTEGER NOT NULL CHECK(height>0),mime TEXT NOT NULL CHECK(mime IN('image/png','image/jpeg','image/webp')),sha256 TEXT NOT NULL,byte_count INTEGER NOT NULL CHECK(byte_count>0),bytes BLOB NOT NULL CHECK(length(bytes)=byte_count),PRIMARY KEY(asset_id,role))`,
    `CREATE TABLE ${p}_versions (id TEXT PRIMARY KEY,target_id TEXT NOT NULL REFERENCES ${foreign},owner_id TEXT NOT NULL,version INTEGER NOT NULL CHECK(version>0),mode TEXT NOT NULL CHECK(mode IN('inherit','none','custom')),asset_id TEXT,crop_json TEXT NOT NULL,created_at INTEGER NOT NULL,purged_at INTEGER,retain_until INTEGER NOT NULL,CHECK((mode='custom')=(asset_id IS NOT NULL)),UNIQUE(target_id,version),UNIQUE(target_id,owner_id,id),FOREIGN KEY(target_id,owner_id,asset_id) REFERENCES ${p}_assets(target_id,owner_id,id),${ownerScope})`,
    `CREATE TABLE ${p}_heads (target_id TEXT PRIMARY KEY REFERENCES ${foreign},owner_id TEXT NOT NULL,revision INTEGER NOT NULL CHECK(revision>=0),next_version INTEGER NOT NULL CHECK(next_version>0),active_version_id TEXT,previous_version_id TEXT,previous_replaced_at INTEGER,updated_at INTEGER NOT NULL,CHECK((previous_version_id IS NULL)=(previous_replaced_at IS NULL)),CHECK(previous_version_id IS NULL OR previous_version_id<>active_version_id),FOREIGN KEY(target_id,owner_id,active_version_id) REFERENCES ${p}_versions(target_id,owner_id,id),${ownerScope})`,
    `CREATE TABLE ${p}_drafts (id TEXT PRIMARY KEY,owner_id TEXT NOT NULL,device_id TEXT NOT NULL,target_kind TEXT NOT NULL CHECK(target_kind='${kind}'),target_id TEXT NOT NULL REFERENCES ${foreign},prepare_key TEXT NOT NULL,target_revision INTEGER NOT NULL,policy_epoch INTEGER NOT NULL,cover_revision INTEGER NOT NULL,origin TEXT NOT NULL CHECK(origin='upload'),state TEXT NOT NULL CHECK(state IN('processing','ready','committed','cancelled','expired','failed')),asset_id TEXT,reserved_bytes INTEGER NOT NULL CHECK(reserved_bytes>=0),created_at INTEGER NOT NULL,expires_at INTEGER NOT NULL,retain_until INTEGER NOT NULL,UNIQUE(owner_id,prepare_key),FOREIGN KEY(target_id,owner_id,asset_id) REFERENCES ${p}_assets(target_id,owner_id,id),${ownerScope})`,
    `CREATE TABLE ${p}_receipts (owner_id TEXT NOT NULL,request_key TEXT NOT NULL,intent_hash TEXT NOT NULL,action TEXT NOT NULL,target_id TEXT NOT NULL,value_json TEXT NOT NULL,created_at INTEGER NOT NULL,expires_at INTEGER NOT NULL,PRIMARY KEY(owner_id,request_key))`,
    `CREATE INDEX ${p}_drafts_expiry ON ${p}_drafts(state,expires_at)`,
    `CREATE INDEX ${p}_drafts_owner ON ${p}_drafts(owner_id,state,expires_at)`,
    `CREATE INDEX ${p}_versions_target ON ${p}_versions(target_id,version)`,
    `CREATE INDEX ${p}_versions_asset ON ${p}_versions(asset_id,purged_at)`,
    `CREATE INDEX ${p}_versions_gc ON ${p}_versions(purged_at,retain_until,created_at)`,
    `CREATE INDEX ${p}_drafts_asset ON ${p}_drafts(asset_id,state,expires_at)`,
    `CREATE INDEX ${p}_receipts_expiry ON ${p}_receipts(expires_at,owner_id)`,
    `CREATE INDEX ${p}_assets_owner ON ${p}_assets(owner_id,purged_at,created_at)`,
    `CREATE TRIGGER ${p}_asset_identity BEFORE UPDATE OF id,target_id,owner_id,origin,target_revision,policy_epoch,recipe,encoded_bytes,created_at ON ${p}_assets BEGIN SELECT RAISE(ABORT,'cover_asset_immutable'); END`,
    `CREATE TRIGGER ${p}_asset_no_replace BEFORE INSERT ON ${p}_assets WHEN EXISTS(SELECT 1 FROM ${p}_assets WHERE id=NEW.id) BEGIN SELECT RAISE(ABORT,'cover_asset_immutable'); END`,
    `CREATE TRIGGER ${p}_asset_purge_monotonic BEFORE UPDATE OF purged_at ON ${p}_assets WHEN OLD.purged_at IS NOT NULL AND NEW.purged_at IS NOT OLD.purged_at BEGIN SELECT RAISE(ABORT,'cover_asset_immutable'); END`,
    `CREATE TRIGGER ${p}_rendition_identity BEFORE UPDATE ON ${p}_renditions BEGIN SELECT RAISE(ABORT,'cover_rendition_immutable'); END`,
    `CREATE TRIGGER ${p}_rendition_no_replace BEFORE INSERT ON ${p}_renditions WHEN EXISTS(SELECT 1 FROM ${p}_renditions WHERE asset_id=NEW.asset_id AND role=NEW.role) BEGIN SELECT RAISE(ABORT,'cover_rendition_immutable'); END`,
    `CREATE TRIGGER ${p}_rendition_live_delete BEFORE DELETE ON ${p}_renditions WHEN EXISTS(SELECT 1 FROM ${p}_versions WHERE asset_id=OLD.asset_id AND purged_at IS NULL) OR EXISTS(SELECT 1 FROM ${p}_drafts WHERE asset_id=OLD.asset_id AND state IN('processing','ready') AND expires_at>(SELECT logical_now FROM ${p}_usage WHERE id=1)) BEGIN SELECT RAISE(ABORT,'cover_asset_live'); END`,
    `CREATE TRIGGER ${p}_version_identity BEFORE UPDATE OF id,target_id,owner_id,version,mode,asset_id,crop_json,created_at ON ${p}_versions BEGIN SELECT RAISE(ABORT,'cover_version_immutable'); END`,
    `CREATE TRIGGER ${p}_version_no_replace BEFORE INSERT ON ${p}_versions WHEN EXISTS(SELECT 1 FROM ${p}_versions WHERE id=NEW.id OR(target_id=NEW.target_id AND version=NEW.version)) BEGIN SELECT RAISE(ABORT,'cover_version_immutable'); END`,
    `CREATE TRIGGER ${p}_version_purge_monotonic BEFORE UPDATE OF purged_at ON ${p}_versions WHEN OLD.purged_at IS NOT NULL AND NEW.purged_at IS NOT OLD.purged_at BEGIN SELECT RAISE(ABORT,'cover_version_immutable'); END`,
    `CREATE TRIGGER ${p}_version_live_purge BEFORE UPDATE OF purged_at ON ${p}_versions WHEN NEW.purged_at IS NOT NULL AND EXISTS(SELECT 1 FROM ${p}_heads WHERE active_version_id=OLD.id) BEGIN SELECT RAISE(ABORT,'cover_version_live'); END`,
    `CREATE TRIGGER ${p}_version_undo_purge BEFORE UPDATE OF purged_at ON ${p}_versions WHEN NEW.purged_at IS NOT NULL AND EXISTS(SELECT 1 FROM ${p}_heads WHERE target_id=OLD.target_id AND previous_version_id=OLD.id AND previous_replaced_at>(SELECT logical_now-86400000 FROM ${p}_usage WHERE id=1)) BEGIN SELECT RAISE(ABORT,'cover_version_protected'); END`,
    `CREATE TRIGGER ${p}_version_undo_delete BEFORE DELETE ON ${p}_versions WHEN EXISTS(SELECT 1 FROM ${p}_heads WHERE target_id=OLD.target_id AND previous_version_id=OLD.id AND previous_replaced_at>(SELECT logical_now-86400000 FROM ${p}_usage WHERE id=1)) BEGIN SELECT RAISE(ABORT,'cover_version_protected'); END`,
    `CREATE TRIGGER ${p}_version_retention BEFORE UPDATE OF retain_until ON ${p}_versions WHEN NEW.retain_until<OLD.retain_until BEGIN SELECT RAISE(ABORT,'cover_retention_immutable'); END`,
    `CREATE TRIGGER ${p}_version_delete_retention BEFORE DELETE ON ${p}_versions WHEN OLD.retain_until>(SELECT logical_now FROM ${p}_usage WHERE id=1) BEGIN SELECT RAISE(ABORT,'cover_retention_immutable'); END`,
    `CREATE TRIGGER ${p}_head_lineage BEFORE UPDATE ON ${p}_heads WHEN NEW.target_id<>OLD.target_id OR NEW.owner_id<>OLD.owner_id OR NEW.revision<>OLD.revision+1 OR NEW.next_version<>OLD.next_version+1 OR NEW.previous_version_id IS NOT OLD.active_version_id OR NEW.previous_replaced_at IS NOT NEW.updated_at OR NEW.updated_at<OLD.updated_at BEGIN SELECT RAISE(ABORT,'cover_head_lineage'); END`,
    `CREATE TRIGGER ${p}_head_no_delete BEFORE DELETE ON ${p}_heads BEGIN SELECT RAISE(ABORT,'cover_head_lineage'); END`,
    `CREATE TRIGGER ${p}_head_no_replace BEFORE INSERT ON ${p}_heads WHEN EXISTS(SELECT 1 FROM ${p}_heads WHERE target_id=NEW.target_id) BEGIN SELECT RAISE(ABORT,'cover_head_lineage'); END`,
    `CREATE TRIGGER ${p}_draft_identity BEFORE UPDATE OF id,owner_id,device_id,target_kind,target_id,prepare_key,target_revision,policy_epoch,cover_revision,origin,created_at,expires_at ON ${p}_drafts BEGIN SELECT RAISE(ABORT,'cover_draft_immutable'); END`,
    `CREATE TRIGGER ${p}_draft_terminal BEFORE UPDATE OF state ON ${p}_drafts WHEN OLD.state NOT IN('processing','ready') AND NEW.state<>OLD.state BEGIN SELECT RAISE(ABORT,'cover_draft_terminal'); END`,
    `CREATE TRIGGER ${p}_draft_retention BEFORE DELETE ON ${p}_drafts WHEN OLD.retain_until>(SELECT logical_now FROM ${p}_usage WHERE id=1) BEGIN SELECT RAISE(ABORT,'cover_retention_immutable'); END`,
    `CREATE TRIGGER ${p}_receipt_immutable BEFORE UPDATE ON ${p}_receipts BEGIN SELECT RAISE(ABORT,'cover_receipt_immutable'); END`,
    `CREATE TRIGGER ${p}_receipt_no_replace BEFORE INSERT ON ${p}_receipts WHEN EXISTS(SELECT 1 FROM ${p}_receipts WHERE owner_id=NEW.owner_id AND request_key=NEW.request_key) BEGIN SELECT RAISE(ABORT,'cover_receipt_immutable'); END`,
    `CREATE TRIGGER ${p}_receipt_retention BEFORE DELETE ON ${p}_receipts WHEN OLD.expires_at>(SELECT logical_now FROM ${p}_usage WHERE id=1) BEGIN SELECT RAISE(ABORT,'cover_retention_immutable'); END`,
  ];
}
export function coverSchemaSeed(kind,storeId) {
  if(typeof storeId!=='string'||!/^[A-Za-z0-9_-]{3,180}$/u.test(storeId))throw new Error('cover_store_id_invalid');
  return `INSERT INTO ${coverPrefix(kind)}_usage VALUES(1,'${storeId}',0,0,0,0,0,0)`;
}
export function assertCoverSchema(db, kind) {
  const version = Number(db.prepare('PRAGMA user_version').get().user_version);
  if (!COVER_FORMATS[kind]?.includes(version)) throw Object.assign(new Error('cover_schema_required'), { code: 'cover_schema_required' });
  if (kind === 'app' && db.prepare("SELECT value FROM apps_meta WHERE key='schema'").get()?.value !== `soty.apps-registry.v${version}`)
    throw Object.assign(new Error('cover_schema_metadata_mismatch'), { code: 'cover_schema_metadata_mismatch' });
  if (kind === 'profile' && (db.prepare("SELECT value FROM world_meta WHERE key='schema_version'").get()?.value !== '6'
    || db.prepare("SELECT value FROM world_meta WHERE key='lineage'").get()?.value !== 'soty.world.sqlite.v1'))
    throw Object.assign(new Error('cover_schema_metadata_mismatch'), { code: 'cover_schema_metadata_mismatch' });
  for (const sql of coverSchemaStatements(kind)) {
    const name = /^CREATE (?:UNIQUE )?(?:TABLE|INDEX|TRIGGER) ([a-z_]+)/u.exec(sql)?.[1];
    if (!name || normalize(db.prepare('SELECT sql FROM sqlite_schema WHERE name=?').get(name)?.sql) !== normalize(sql))
      throw Object.assign(new Error('cover_schema_metadata_mismatch'), { code: 'cover_schema_metadata_mismatch' });
  }
  if (!db.prepare(`SELECT 1 FROM ${coverPrefix(kind)}_usage WHERE id=1`).get()) throw Object.assign(new Error('cover_storage_corrupt'), { code: 'cover_storage_corrupt' });
}
