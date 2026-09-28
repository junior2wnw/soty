import { DISCOVERY_MIGRATION } from './discovery.mjs';

export const SCHEMA_VERSION = 3;
const LINEAGE = 'soty.world.sqlite.v1';
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

export function migrateWorld(db, projectId) {
  db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;');
  db.exec('BEGIN IMMEDIATE');
  try {
    let version = Number(db.prepare('PRAGMA user_version').get().user_version);
    if (version === 0) {
      if (db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' LIMIT 1").get()) throw new Error('world_schema_metadata_mismatch');
      db.exec(SCHEMA);
      const insert = db.prepare('INSERT INTO world_meta(key,value) VALUES (?,?)');
      insert.run('project_id', projectId); insert.run('lineage', LINEAGE); insert.run('schema_version', '1');
      db.exec('PRAGMA user_version=1'); version = 1;
    } else if (!Number.isInteger(version) || version < 1 || version > SCHEMA_VERSION) throw new Error('world_schema_unsupported');
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
    }
    if (db.prepare('PRAGMA quick_check').all().some(row => row.quick_check !== 'ok') || db.prepare('PRAGMA foreign_key_check').get()) throw new Error('world_storage_corrupt');
    db.exec('COMMIT');
  } catch (error) { db.exec('ROLLBACK'); throw error; }
}
