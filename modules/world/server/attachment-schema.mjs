import { assert } from './validation.mjs';

export const ATTACHMENT_SCHEMA = [
  `CREATE TABLE world_chat_attachments (
    id TEXT PRIMARY KEY, scope_kind TEXT NOT NULL CHECK(scope_kind IN('room','direct')), conversation_id TEXT NOT NULL,
    relationship_id TEXT, author_id TEXT NOT NULL REFERENCES profiles(account_id), name TEXT NOT NULL, media_type TEXT NOT NULL,
    size INTEGER NOT NULL CHECK(size>=0 AND size<=26214400), state TEXT NOT NULL CHECK(state IN('pending','complete','published','cancelled','expired','removed')),
    manifest_sha256 TEXT, file_sha256 TEXT, message_id TEXT, position INTEGER CHECK(position>=0 AND position<10),
    created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL,
    CHECK((scope_kind='room' AND relationship_id IS NULL) OR (scope_kind='direct' AND relationship_id IS NOT NULL)),
    UNIQUE(scope_kind,message_id,position)
  )`,
  `CREATE INDEX world_chat_attachments_scope ON world_chat_attachments(scope_kind,conversation_id,author_id,state)`,
  `CREATE INDEX world_chat_attachments_author ON world_chat_attachments(author_id,state)`,
  `CREATE INDEX world_chat_attachments_message ON world_chat_attachments(scope_kind,message_id,position)`,
  `CREATE INDEX world_chat_attachments_expiry ON world_chat_attachments(state,expires_at)`,
  `CREATE TABLE world_chat_attachment_chunks (
    attachment_id TEXT NOT NULL REFERENCES world_chat_attachments(id), chunk_index INTEGER NOT NULL CHECK(chunk_index>=0),
    size INTEGER NOT NULL CHECK(size>0 AND size<=524288), sha256 TEXT NOT NULL, data BLOB NOT NULL,
    PRIMARY KEY(attachment_id,chunk_index), CHECK(length(data)=size)
  )`,
];
const normalized = value => String(value ?? '').replace(/\s+/gu, '').replace(/;$/u, '');
export function assertAttachmentSchema(db) {
  const definitions = ATTACHMENT_SCHEMA.map(sql => ({ sql, name: /CREATE (?:TABLE|INDEX) ([a-z_]+)/u.exec(sql)[1] })).sort((a, b) => a.name.localeCompare(b.name));
  const actual = db.prepare("SELECT name,sql FROM sqlite_schema WHERE (name GLOB 'world_chat_attach*' OR type IN('index','trigger') AND tbl_name GLOB 'world_chat_attach*') AND name NOT LIKE 'sqlite_%' ORDER BY name").all();
  assert(actual.length === definitions.length && actual.every((row, index) => row.name === definitions[index].name && normalized(row.sql) === normalized(definitions[index].sql)), 'world_attachment_schema_mismatch');
  assert(!db.prepare(`SELECT 1 FROM world_chat_attachments a WHERE
    (a.state IN('pending','complete') AND (a.message_id IS NOT NULL OR a.position IS NOT NULL))
    OR (a.state IN('complete','published') AND (length(a.manifest_sha256)<>64 OR length(a.file_sha256)<>64 OR a.manifest_sha256 IS NULL OR a.file_sha256 IS NULL))
    OR (a.state='published' AND (a.message_id IS NULL OR a.position IS NULL
      OR (a.scope_kind='room' AND NOT EXISTS(SELECT 1 FROM messages m WHERE m.id=a.message_id AND m.community_id=a.conversation_id AND m.author_id=a.author_id AND m.deleted_at IS NULL))
      OR (a.scope_kind='direct' AND NOT EXISTS(SELECT 1 FROM world_direct_messages m WHERE m.id=a.message_id AND m.thread_id=a.conversation_id AND m.author_id=a.author_id AND m.relationship_id=a.relationship_id AND m.deleted_at IS NULL))))
    OR (a.state IN('cancelled','expired','removed') AND EXISTS(SELECT 1 FROM world_chat_attachment_chunks c WHERE c.attachment_id=a.id)) LIMIT 1`).get(), 'world_attachment_storage_corrupt');
}
