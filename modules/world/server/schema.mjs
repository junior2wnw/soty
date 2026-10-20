import { DISCOVERY_MIGRATION } from './discovery.mjs';

// Version4 is reserved for the separate attachment reader; it is not this layout.
export const SCHEMA_VERSION = 5;
const LINEAGE = 'soty.world.sqlite.v1';
const AUDIENCE_COLUMN = "audience TEXT NOT NULL DEFAULT 'private' CHECK(audience IN('private','friends','link','public'))";
const PROFILE_INSERT = `CREATE TRIGGER world_profile_directory_ai AFTER INSERT ON profiles WHEN new.audience='public' BEGIN
  INSERT INTO world_directory(kind,entity_id,name,search_text) VALUES('person',new.account_id,new.display_name,new.search_text);
END`;
const PROFILE_UPDATE = `CREATE TRIGGER world_profile_directory_au AFTER UPDATE OF audience,display_name,search_text ON profiles BEGIN
  DELETE FROM world_directory WHERE kind='person' AND entity_id=old.account_id AND new.audience<>'public';
  INSERT INTO world_directory(kind,entity_id,name,search_text)
    SELECT 'person',new.account_id,new.display_name,new.search_text WHERE new.audience='public'
    ON CONFLICT(kind,entity_id) DO UPDATE SET name=excluded.name,search_text=excluded.search_text;
END`;
const PROFILE_DELETE = `CREATE TRIGGER world_profile_directory_ad AFTER DELETE ON profiles BEGIN
  DELETE FROM world_directory WHERE kind='person' AND entity_id=old.account_id;
END`;
const normalized = sql => String(sql ?? '').replace(/\s+/gu, '').replace(/;$/u, '');
function assertAudienceSchema(db) {
  const columns = db.prepare('PRAGMA table_info(profiles)').all();
  const names = ['account_id','display_name','bio','interests','search_text','discoverable','show_presence','show_memberships',
    'contact_policy','avatar_color','revision','created_at','updated_at','avatar_revision','audience'];
  const audience = columns.at(-1);
  const table = db.prepare("SELECT sql FROM sqlite_schema WHERE type='table' AND name='profiles'").get();
  const trigger = name => db.prepare("SELECT sql FROM sqlite_schema WHERE type='trigger' AND name=?").get(name)?.sql;
  const triggers = db.prepare("SELECT name FROM sqlite_schema WHERE type='trigger' AND tbl_name='profiles' ORDER BY name").all();
  if (columns.length !== names.length || columns.some((column, index) => column.name !== names[index])
    || audience?.type !== 'TEXT' || audience.notnull !== 1 || audience.dflt_value !== "'private'" || audience.pk !== 0
    || !normalized(table?.sql).endsWith(normalized(AUDIENCE_COLUMN) + ')')
    || normalized(trigger('world_profile_directory_ai')) !== normalized(PROFILE_INSERT)
    || normalized(trigger('world_profile_directory_au')) !== normalized(PROFILE_UPDATE)
    || normalized(trigger('world_profile_directory_ad')) !== normalized(PROFILE_DELETE)
    || JSON.stringify(triggers.map(row => row.name)) !== JSON.stringify(['world_profile_directory_ad','world_profile_directory_ai','world_profile_directory_au'])
    || db.prepare("SELECT 1 FROM sqlite_schema WHERE name IN('chat_attachments','chat_attachment_chunks') LIMIT 1").get()
    || db.prepare("SELECT 1 FROM profiles WHERE discoverable<>(audience='public') LIMIT 1").get()) {
    throw new Error('world_schema_metadata_mismatch');
  }
}
const SCHEMA = `
CREATE TABLE world_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE profiles (
  account_id TEXT PRIMARY KEY, display_name TEXT NOT NULL, bio TEXT NOT NULL DEFAULT '',
  interests TEXT NOT NULL DEFAULT '[]', search_text TEXT NOT NULL,
  discoverable INTEGER NOT NULL DEFAULT 0 CHECK(discoverable IN(0,1)),
  show_presence INTEGER NOT NULL DEFAULT 0 CHECK(show_presence IN(0,1)),
  show_memberships INTEGER NOT NULL DEFAULT 1 CHECK(show_memberships IN(0,1)),
  contact_policy TEXT NOT NULL DEFAULT 'everyone' CHECK(contact_policy IN('everyone','members','nobody')),
  avatar_color TEXT NOT NULL DEFAULT '#C89448', revision INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
);
CREATE INDEX profiles_discoverable ON profiles(discoverable,account_id);
CREATE TABLE communities (
  id TEXT PRIMARY KEY, owner_id TEXT NOT NULL REFERENCES profiles(account_id),
  name TEXT NOT NULL, description TEXT NOT NULL DEFAULT '', topics TEXT NOT NULL DEFAULT '[]',
  search_text TEXT NOT NULL, join_policy TEXT NOT NULL CHECK(join_policy IN('open','request','invite')),
  show_members INTEGER NOT NULL DEFAULT 1 CHECK(show_members IN(0,1)),
  showcase TEXT NOT NULL DEFAULT '', symbol TEXT NOT NULL DEFAULT '✦',
  color TEXT NOT NULL DEFAULT '#C89448', revision INTEGER NOT NULL DEFAULT 1,
  state TEXT NOT NULL DEFAULT 'active' CHECK(state IN('active','archived')),
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
);
CREATE INDEX communities_discovery ON communities(state,join_policy,id);
CREATE TABLE memberships (
  community_id TEXT NOT NULL REFERENCES communities(id), account_id TEXT NOT NULL REFERENCES profiles(account_id),
  role TEXT NOT NULL DEFAULT 'member' CHECK(role IN('owner','moderator','member')),
  state TEXT NOT NULL CHECK(state IN('active','requested','invited','left','removed','banned','declined')),
  revision INTEGER NOT NULL DEFAULT 1, joined_at INTEGER, updated_at INTEGER NOT NULL,
  show_in_profile INTEGER NOT NULL DEFAULT 1 CHECK(show_in_profile IN(0,1)),
  pinned INTEGER NOT NULL DEFAULT 0 CHECK(pinned IN(0,1)), muted INTEGER NOT NULL DEFAULT 0 CHECK(muted IN(0,1)),
  last_read_seq INTEGER NOT NULL DEFAULT 0, invited_by TEXT,
  PRIMARY KEY(community_id,account_id)
);
CREATE INDEX memberships_account ON memberships(account_id,state,community_id);
CREATE INDEX memberships_group ON memberships(community_id,state,account_id);
CREATE UNIQUE INDEX memberships_owner ON memberships(community_id) WHERE role='owner' AND state='active';
CREATE TABLE messages (
  seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE,
  community_id TEXT NOT NULL REFERENCES communities(id), author_id TEXT NOT NULL REFERENCES profiles(account_id),
  client_id TEXT NOT NULL, text TEXT NOT NULL, reply_to TEXT REFERENCES messages(id),
  created_at INTEGER NOT NULL, deleted_at INTEGER, deleted_by TEXT,
  UNIQUE(community_id,author_id,client_id)
);
CREATE INDEX messages_community_seq ON messages(community_id,seq);
CREATE TABLE receipts (
  account_id TEXT NOT NULL REFERENCES profiles(account_id), operation TEXT NOT NULL,
  request_id TEXT NOT NULL, digest TEXT NOT NULL, result TEXT NOT NULL, created_at INTEGER NOT NULL,
  PRIMARY KEY(account_id,operation,request_id)
);
CREATE TABLE world_audit (
  seq INTEGER PRIMARY KEY AUTOINCREMENT, account_id TEXT NOT NULL,
  operation TEXT NOT NULL, target_id TEXT NOT NULL, created_at INTEGER NOT NULL
);
CREATE INDEX world_audit_target ON world_audit(target_id,seq);
CREATE TABLE world_rate_limits (key TEXT PRIMARY KEY, bucket INTEGER NOT NULL, count INTEGER NOT NULL);
`;

export function migrateWorld(db, projectId, { allowAudienceMigration = false } = {}) {
  if (typeof allowAudienceMigration !== 'boolean') throw new Error('invalid_audience_migration_mode');
  const initialVersion = Number(db.prepare('PRAGMA user_version').get().user_version);
  if (!Number.isInteger(initialVersion) || initialVersion < 0 || initialVersion === 4 || initialVersion > SCHEMA_VERSION) throw new Error('world_schema_unsupported');
  // Default reader startup must not change an existing format before independent release admission.
  if (initialVersion > 0 && initialVersion < SCHEMA_VERSION && !allowAudienceMigration) throw new Error('world_audience_migration_required');
  db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000; PRAGMA synchronous=FULL;');
  db.exec('BEGIN IMMEDIATE');
  try {
    let version = Number(db.prepare('PRAGMA user_version').get().user_version);
    if (!Number.isInteger(version) || version < 0 || version === 4 || version > SCHEMA_VERSION) throw new Error('world_schema_unsupported');
    if (version > 0 && version < SCHEMA_VERSION && !allowAudienceMigration) throw new Error('world_audience_migration_required');
    if (version === 0) {
      if (db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' LIMIT 1").get()) throw new Error('world_schema_metadata_mismatch');
      db.exec(SCHEMA);
      const insert = db.prepare('INSERT INTO world_meta(key,value) VALUES (?,?)');
      insert.run('project_id', projectId); insert.run('lineage', LINEAGE); insert.run('schema_version', '1');
      db.exec('PRAGMA user_version=1'); version = 1;
    }
    const meta = Object.fromEntries(db.prepare('SELECT key,value FROM world_meta').all().map(row => [row.key, row.value]));
    if (meta.project_id !== projectId) throw new Error('world_project_mismatch');
    if (meta.lineage !== LINEAGE || meta.schema_version !== String(version)) throw new Error('world_schema_metadata_mismatch');
    if (version < 2) {
      db.exec(`ALTER TABLE profiles ADD COLUMN avatar_revision INTEGER;
        CREATE TABLE profile_avatars (account_id TEXT PRIMARY KEY REFERENCES profiles(account_id),
          mime TEXT NOT NULL,image BLOB NOT NULL,thumbnail_mime TEXT NOT NULL,thumbnail BLOB NOT NULL);
        PRAGMA user_version=2;`);
      db.prepare("UPDATE world_meta SET value='2' WHERE key='schema_version'").run();
      version = 2;
    }
    if (version < 3) {
      db.exec(DISCOVERY_MIGRATION);
      db.exec('PRAGMA user_version=3');
      db.prepare("UPDATE world_meta SET value='3' WHERE key='schema_version'").run();
      version = 3;
    }
    if (version < 5) {
      db.exec(`ALTER TABLE profiles ADD COLUMN ${AUDIENCE_COLUMN};
        UPDATE profiles SET audience=CASE WHEN discoverable=1 THEN 'public' ELSE 'private' END;
        DROP TRIGGER world_profile_directory_ai;
        DROP TRIGGER world_profile_directory_au;
        ${PROFILE_INSERT};
        ${PROFILE_UPDATE};
        PRAGMA user_version=5;`);
      db.prepare("UPDATE world_meta SET value='5' WHERE key='schema_version'").run();
    }
    assertAudienceSchema(db);
    if (db.prepare('PRAGMA quick_check').all().some(row => row.quick_check !== 'ok') || db.prepare('PRAGMA foreign_key_check').get()) throw new Error('world_storage_corrupt');
    db.exec('COMMIT');
  } catch (error) { db.exec('ROLLBACK'); throw error; }
  // Admission/schema committed. A WAL setup failure is surfaced as an error;
  // it cannot undo the committed format and must not be reported as rollback.
  db.exec('PRAGMA journal_mode=WAL;');
}
