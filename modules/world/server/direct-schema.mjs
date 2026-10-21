import { assert } from './validation.mjs';

export const DIRECT_SCHEMA_SQL = "-- Contract DDL only. Root owns final version/strict admission/migration.\nCREATE TABLE world_direct_threads (\n  seq INTEGER PRIMARY KEY AUTOINCREMENT,\n  id TEXT NOT NULL UNIQUE,\n  account_low TEXT NOT NULL,\n  account_high TEXT NOT NULL,\n  relationship_id TEXT NOT NULL,\n  revision INTEGER NOT NULL DEFAULT 1 CHECK(revision>=1),\n  created_at INTEGER NOT NULL,\n  updated_at INTEGER NOT NULL,\n  UNIQUE(account_low,account_high),\n  CHECK(account_low COLLATE BINARY < account_high COLLATE BINARY)\n);\nCREATE INDEX world_direct_low_seq ON world_direct_threads(account_low,seq);\nCREATE INDEX world_direct_high_seq ON world_direct_threads(account_high,seq);\nCREATE TRIGGER world_direct_pair_immutable BEFORE UPDATE OF seq,id,account_low,account_high,created_at ON world_direct_threads\nWHEN new.seq<>old.seq OR new.id<>old.id OR new.account_low<>old.account_low\n  OR new.account_high<>old.account_high OR new.created_at<>old.created_at\nBEGIN SELECT RAISE(ABORT,'direct_pair_immutable'); END;\n\nCREATE TABLE world_direct_messages (\n  seq INTEGER PRIMARY KEY AUTOINCREMENT,\n  id TEXT NOT NULL UNIQUE,\n  thread_id TEXT NOT NULL REFERENCES world_direct_threads(id),\n  author_id TEXT NOT NULL,\n  relationship_id TEXT NOT NULL,\n  client_id TEXT NOT NULL,\n  text TEXT NOT NULL,\n  reply_to TEXT REFERENCES world_direct_messages(id),\n  created_at INTEGER NOT NULL,\n  deleted_at INTEGER,\n  deleted_by TEXT,\n  UNIQUE(thread_id,author_id,client_id),\n  CHECK((deleted_at IS NULL AND deleted_by IS NULL) OR (deleted_at IS NOT NULL AND deleted_by IS author_id AND text=''))\n);\nCREATE INDEX world_direct_messages_thread_seq ON world_direct_messages(thread_id,seq);\nCREATE TRIGGER world_direct_message_author BEFORE INSERT ON world_direct_messages\nWHEN NOT EXISTS(SELECT 1 FROM world_direct_threads d WHERE d.id=new.thread_id\n  AND new.author_id IN(d.account_low,d.account_high) AND new.relationship_id=d.relationship_id)\nBEGIN SELECT RAISE(ABORT,'direct_author_denied'); END;\nCREATE TRIGGER world_direct_message_reply BEFORE INSERT ON world_direct_messages\nWHEN new.reply_to IS NOT NULL AND NOT EXISTS(SELECT 1 FROM world_direct_messages m WHERE m.id=new.reply_to AND m.thread_id=new.thread_id)\nBEGIN SELECT RAISE(ABORT,'direct_reply_denied'); END;\nCREATE TRIGGER world_direct_message_immutable BEFORE UPDATE OF seq,id,thread_id,author_id,relationship_id,client_id,reply_to,created_at ON world_direct_messages\nWHEN new.seq<>old.seq OR new.id<>old.id OR new.thread_id<>old.thread_id OR new.author_id<>old.author_id\n  OR new.relationship_id<>old.relationship_id OR new.client_id<>old.client_id\n  OR new.reply_to IS NOT old.reply_to OR new.created_at<>old.created_at\nBEGIN SELECT RAISE(ABORT,'direct_message_immutable'); END;\n\nCREATE TABLE world_direct_read_state (\n  thread_id TEXT NOT NULL REFERENCES world_direct_threads(id),\n  account_id TEXT NOT NULL,\n  last_read_seq INTEGER NOT NULL DEFAULT 0 CHECK(last_read_seq>=0),\n  pinned INTEGER NOT NULL DEFAULT 0 CHECK(pinned IN(0,1)),\n  muted INTEGER NOT NULL DEFAULT 0 CHECK(muted IN(0,1)),\n  PRIMARY KEY(thread_id,account_id)\n);\nCREATE TRIGGER world_direct_read_actor BEFORE INSERT ON world_direct_read_state\nWHEN NOT EXISTS(SELECT 1 FROM world_direct_threads d WHERE d.id=new.thread_id AND new.account_id IN(d.account_low,d.account_high))\nBEGIN SELECT RAISE(ABORT,'direct_reader_denied'); END;\nCREATE TRIGGER world_direct_read_identity BEFORE UPDATE OF thread_id,account_id ON world_direct_read_state\nWHEN new.thread_id<>old.thread_id OR new.account_id<>old.account_id\nBEGIN SELECT RAISE(ABORT,'direct_read_identity_immutable'); END;\nCREATE TRIGGER world_direct_read_monotonic BEFORE UPDATE OF last_read_seq ON world_direct_read_state\nWHEN new.last_read_seq<old.last_read_seq\nBEGIN SELECT RAISE(ABORT,'direct_read_regression'); END;\nCREATE TRIGGER world_direct_read_target_insert BEFORE INSERT ON world_direct_read_state\nWHEN new.last_read_seq<>0 AND NOT EXISTS(SELECT 1 FROM world_direct_messages m WHERE m.thread_id=new.thread_id AND m.seq=new.last_read_seq)\nBEGIN SELECT RAISE(ABORT,'direct_read_target_invalid'); END;\nCREATE TRIGGER world_direct_read_target_update BEFORE UPDATE OF last_read_seq ON world_direct_read_state\nWHEN new.last_read_seq<>0 AND NOT EXISTS(SELECT 1 FROM world_direct_messages m WHERE m.thread_id=new.thread_id AND m.seq=new.last_read_seq)\nBEGIN SELECT RAISE(ABORT,'direct_read_target_invalid'); END;\n";

const definitions = [
  {
    "type": "index",
    "name": "world_direct_high_seq",
    "sql": "CREATE INDEX world_direct_high_seq ON world_direct_threads(account_high,seq)"
  },
  {
    "type": "index",
    "name": "world_direct_low_seq",
    "sql": "CREATE INDEX world_direct_low_seq ON world_direct_threads(account_low,seq)"
  },
  {
    "type": "index",
    "name": "world_direct_messages_thread_seq",
    "sql": "CREATE INDEX world_direct_messages_thread_seq ON world_direct_messages(thread_id,seq)"
  },
  {
    "type": "table",
    "name": "world_direct_messages",
    "sql": "CREATE TABLE world_direct_messages (\n  seq INTEGER PRIMARY KEY AUTOINCREMENT,\n  id TEXT NOT NULL UNIQUE,\n  thread_id TEXT NOT NULL REFERENCES world_direct_threads(id),\n  author_id TEXT NOT NULL,\n  relationship_id TEXT NOT NULL,\n  client_id TEXT NOT NULL,\n  text TEXT NOT NULL,\n  reply_to TEXT REFERENCES world_direct_messages(id),\n  created_at INTEGER NOT NULL,\n  deleted_at INTEGER,\n  deleted_by TEXT,\n  UNIQUE(thread_id,author_id,client_id),\n  CHECK((deleted_at IS NULL AND deleted_by IS NULL) OR (deleted_at IS NOT NULL AND deleted_by IS author_id AND text=''))\n)"
  },
  {
    "type": "table",
    "name": "world_direct_read_state",
    "sql": "CREATE TABLE world_direct_read_state (\n  thread_id TEXT NOT NULL REFERENCES world_direct_threads(id),\n  account_id TEXT NOT NULL,\n  last_read_seq INTEGER NOT NULL DEFAULT 0 CHECK(last_read_seq>=0),\n  pinned INTEGER NOT NULL DEFAULT 0 CHECK(pinned IN(0,1)),\n  muted INTEGER NOT NULL DEFAULT 0 CHECK(muted IN(0,1)),\n  PRIMARY KEY(thread_id,account_id)\n)"
  },
  {
    "type": "table",
    "name": "world_direct_threads",
    "sql": "CREATE TABLE world_direct_threads (\n  seq INTEGER PRIMARY KEY AUTOINCREMENT,\n  id TEXT NOT NULL UNIQUE,\n  account_low TEXT NOT NULL,\n  account_high TEXT NOT NULL,\n  relationship_id TEXT NOT NULL,\n  revision INTEGER NOT NULL DEFAULT 1 CHECK(revision>=1),\n  created_at INTEGER NOT NULL,\n  updated_at INTEGER NOT NULL,\n  UNIQUE(account_low,account_high),\n  CHECK(account_low COLLATE BINARY < account_high COLLATE BINARY)\n)"
  },
  {
    "type": "trigger",
    "name": "world_direct_message_author",
    "sql": "CREATE TRIGGER world_direct_message_author BEFORE INSERT ON world_direct_messages\nWHEN NOT EXISTS(SELECT 1 FROM world_direct_threads d WHERE d.id=new.thread_id\n  AND new.author_id IN(d.account_low,d.account_high) AND new.relationship_id=d.relationship_id)\nBEGIN SELECT RAISE(ABORT,'direct_author_denied'); END"
  },
  {
    "type": "trigger",
    "name": "world_direct_message_immutable",
    "sql": "CREATE TRIGGER world_direct_message_immutable BEFORE UPDATE OF seq,id,thread_id,author_id,relationship_id,client_id,reply_to,created_at ON world_direct_messages\nWHEN new.seq<>old.seq OR new.id<>old.id OR new.thread_id<>old.thread_id OR new.author_id<>old.author_id\n  OR new.relationship_id<>old.relationship_id OR new.client_id<>old.client_id\n  OR new.reply_to IS NOT old.reply_to OR new.created_at<>old.created_at\nBEGIN SELECT RAISE(ABORT,'direct_message_immutable'); END"
  },
  {
    "type": "trigger",
    "name": "world_direct_message_reply",
    "sql": "CREATE TRIGGER world_direct_message_reply BEFORE INSERT ON world_direct_messages\nWHEN new.reply_to IS NOT NULL AND NOT EXISTS(SELECT 1 FROM world_direct_messages m WHERE m.id=new.reply_to AND m.thread_id=new.thread_id)\nBEGIN SELECT RAISE(ABORT,'direct_reply_denied'); END"
  },
  {
    "type": "trigger",
    "name": "world_direct_pair_immutable",
    "sql": "CREATE TRIGGER world_direct_pair_immutable BEFORE UPDATE OF seq,id,account_low,account_high,created_at ON world_direct_threads\nWHEN new.seq<>old.seq OR new.id<>old.id OR new.account_low<>old.account_low\n  OR new.account_high<>old.account_high OR new.created_at<>old.created_at\nBEGIN SELECT RAISE(ABORT,'direct_pair_immutable'); END"
  },
  {
    "type": "trigger",
    "name": "world_direct_read_actor",
    "sql": "CREATE TRIGGER world_direct_read_actor BEFORE INSERT ON world_direct_read_state\nWHEN NOT EXISTS(SELECT 1 FROM world_direct_threads d WHERE d.id=new.thread_id AND new.account_id IN(d.account_low,d.account_high))\nBEGIN SELECT RAISE(ABORT,'direct_reader_denied'); END"
  },
  {
    "type": "trigger",
    "name": "world_direct_read_identity",
    "sql": "CREATE TRIGGER world_direct_read_identity BEFORE UPDATE OF thread_id,account_id ON world_direct_read_state\nWHEN new.thread_id<>old.thread_id OR new.account_id<>old.account_id\nBEGIN SELECT RAISE(ABORT,'direct_read_identity_immutable'); END"
  },
  {
    "type": "trigger",
    "name": "world_direct_read_monotonic",
    "sql": "CREATE TRIGGER world_direct_read_monotonic BEFORE UPDATE OF last_read_seq ON world_direct_read_state\nWHEN new.last_read_seq<old.last_read_seq\nBEGIN SELECT RAISE(ABORT,'direct_read_regression'); END"
  },
  {
    "type": "trigger",
    "name": "world_direct_read_target_insert",
    "sql": "CREATE TRIGGER world_direct_read_target_insert BEFORE INSERT ON world_direct_read_state\nWHEN new.last_read_seq<>0 AND NOT EXISTS(SELECT 1 FROM world_direct_messages m WHERE m.thread_id=new.thread_id AND m.seq=new.last_read_seq)\nBEGIN SELECT RAISE(ABORT,'direct_read_target_invalid'); END"
  },
  {
    "type": "trigger",
    "name": "world_direct_read_target_update",
    "sql": "CREATE TRIGGER world_direct_read_target_update BEFORE UPDATE OF last_read_seq ON world_direct_read_state\nWHEN new.last_read_seq<>0 AND NOT EXISTS(SELECT 1 FROM world_direct_messages m WHERE m.thread_id=new.thread_id AND m.seq=new.last_read_seq)\nBEGIN SELECT RAISE(ABORT,'direct_read_target_invalid'); END"
  }
];
const normalized = sql => String(sql ?? '').replace(/\s+/gu,'').replace(/;$/u,'');
/** Fragment admission only. Root owns World6 metadata and its explicit migration. */
export function assertDirectSchema(db) {
  const rows = db.prepare("SELECT type,name,sql FROM sqlite_schema WHERE name GLOB 'world_direct_*' ORDER BY type,name").all();
  assert(rows.length === definitions.length && rows.every((row,index) => row.type === definitions[index].type
    && row.name === definitions[index].name && normalized(row.sql) === normalized(definitions[index].sql)), 'world_direct_schema_mismatch');
  assert(!db.prepare(`SELECT 1 FROM world_direct_messages m LEFT JOIN world_direct_threads d ON d.id=m.thread_id
    WHERE d.id IS NULL OR m.author_id NOT IN(d.account_low,d.account_high) OR length(m.text)>6000
      OR (m.reply_to IS NOT NULL AND NOT EXISTS(SELECT 1 FROM world_direct_messages p WHERE p.id=m.reply_to AND p.thread_id=m.thread_id)) LIMIT 1`).get()
    && !db.prepare(`SELECT 1 FROM world_direct_read_state r LEFT JOIN world_direct_threads d ON d.id=r.thread_id
      WHERE d.id IS NULL OR r.account_id NOT IN(d.account_low,d.account_high)
        OR (r.last_read_seq<>0 AND NOT EXISTS(SELECT 1 FROM world_direct_messages m WHERE m.thread_id=r.thread_id AND m.seq=r.last_read_seq)) LIMIT 1`).get(), 'world_direct_storage_corrupt');
}
