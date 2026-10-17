import { createHash } from 'node:crypto';
import { attachmentFileName } from '../attachment-file-policy.mjs';
import { ATTACHMENT_LIMITS as L, attachmentManifest } from '../attachment-contract.mjs';
import { assert, exact, identifier, integer } from './validation.mjs';
export { ATTACHMENT_LIMITS } from '../attachment-contract.mjs';
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const digest = value => typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value);
export const ATTACHMENT_OPERATIONS = ['world.attachment.capabilities', 'world.attachment.begin', 'world.attachment.chunk',
  'world.attachment.complete', 'world.attachment.status', 'world.attachment.read', 'world.attachment.cancel'];
export const ATTACHMENT_MIGRATION = `
CREATE TABLE chat_attachments (
  id TEXT PRIMARY KEY, community_id TEXT NOT NULL REFERENCES communities(id), author_id TEXT NOT NULL REFERENCES profiles(account_id),
  name TEXT NOT NULL, media_type TEXT NOT NULL, size INTEGER NOT NULL CHECK(size>=0 AND size<=1048576),
  state TEXT NOT NULL CHECK(state IN('pending','complete','published','cancelled','expired','removed')),
  manifest_sha256 TEXT, file_sha256 TEXT, message_id TEXT REFERENCES messages(id), position INTEGER,
  created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL,
  UNIQUE(message_id,position)
);
CREATE INDEX chat_attachments_scope ON chat_attachments(community_id,author_id,state);
CREATE INDEX chat_attachments_message ON chat_attachments(message_id,position);
CREATE TABLE chat_attachment_chunks (
  attachment_id TEXT NOT NULL REFERENCES chat_attachments(id), chunk_index INTEGER NOT NULL CHECK(chunk_index>=0),
  size INTEGER NOT NULL CHECK(size>0 AND size<=256000), sha256 TEXT NOT NULL, data BLOB NOT NULL,
  PRIMARY KEY(attachment_id,chunk_index), CHECK(length(data)=size)
);
`;

/** Explicit offline host operation, never called by the regular constructor.
 * Caller must close all writers and qualify encrypted restore before enabling writes. */
export function migrateChatAttachments(db, projectId) {
  assert(!db.isTransaction, 'attachment_migration_busy');
  db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=100; BEGIN IMMEDIATE');
  try {
    const version = Number(db.prepare('PRAGMA user_version').get().user_version);
    const meta = Object.fromEntries(db.prepare('SELECT key,value FROM world_meta').all().map(row => [row.key, row.value]));
    assert(version === 3 && meta.schema_version === '3' && meta.lineage === 'soty.world.sqlite.v1'
      && meta.project_id === projectId, 'attachment_migration_baseline_required');
    db.exec(ATTACHMENT_MIGRATION);
    db.prepare("UPDATE world_meta SET value='4' WHERE key='schema_version'").run(); db.exec('PRAGMA user_version=4');
    assert(db.prepare('PRAGMA quick_check').get().quick_check === 'ok' && !db.prepare('PRAGMA foreign_key_check').get(), 'world_storage_corrupt');
    db.exec('COMMIT'); return { from: 3, to: 4, writerEnabled: false };
  } catch (error) { db.exec('ROLLBACK'); throw error; }
}
const publicRef = row => ({ attachmentId: row.id, name: row.name, size: row.size,
  manifestSha256: row.manifest_sha256, fileSha256: row.file_sha256,
  chunks: Math.ceil(row.size / L.chunkBytes), mediaType: 'application/octet-stream' });
export function messageAttachments(m, messageId) {
  return m.schemaVersion !== 4 ? [] : m.all("SELECT * FROM chat_attachments WHERE message_id=? AND state='published' ORDER BY position", messageId).map(publicRef);
}
export function admitMessageAttachments(m, ids, communityId, actorId, now) {
  assert(m.attachmentWrites && m.schemaVersion === 4, 'attachments_disabled');
  assert(Array.isArray(ids) && ids.length <= L.batch && new Set(ids).size === ids.length, 'attachment_batch_limit');
  let total = 0;
  return ids.map(value => {
    const row = m.get('SELECT * FROM chat_attachments WHERE id=?', identifier(value));
    assert(row && row.community_id === communityId && row.author_id === actorId && row.state === 'complete'
      && row.expires_at > now && digest(row.manifest_sha256), 'attachment_not_ready');
    total += row.size; assert(total <= L.batchBytes, 'attachment_batch_limit'); return row;
  });
}
export function attachmentOperation(m, op, args, actor, now) {
  const base = ['communityId', 'expectedAccountId'];
  const shapes = {
    'world.attachment.capabilities': [], 'world.attachment.begin': ['attachmentId', 'name', 'size', 'mediaType'],
    'world.attachment.chunk': ['attachmentId', 'index', 'sha256', 'data'], 'world.attachment.complete': ['attachmentId', 'manifestSha256'],
    'world.attachment.status': ['attachmentId'], 'world.attachment.read': ['attachmentId', 'index'], 'world.attachment.cancel': ['attachmentId'],
  };
  exact(args, [...base, ...shapes[op]]);
  assert(args.expectedAccountId === actor.accountId, 'attachment_account_mismatch');
  const { row: community } = m.requireMember(args.communityId, actor.accountId);
  if (op === 'world.attachment.capabilities') return { schema: 'soty.chat-attachment.capabilities.v1',
    readable: m.schemaVersion === 4, writable: m.attachmentWrites && m.schemaVersion === 4, limits: L };
  assert(m.schemaVersion === 4, 'attachments_disabled');
  const read = ['world.attachment.status', 'world.attachment.read'].includes(op);
  if (!read && op !== 'world.attachment.cancel') assert(m.attachmentWrites, 'attachments_disabled');
  const attachmentId = identifier(args.attachmentId);
  let item = m.get('SELECT * FROM chat_attachments WHERE id=?', attachmentId);
  if (op === 'world.attachment.begin') {
    assert(typeof args.name === 'string' && args.name.length <= 4096 && typeof args.mediaType === 'string'
      && args.mediaType.length <= 128 && !/[\u0000-\u001f\u007f]/u.test(args.mediaType), 'invalid_attachment_metadata');
    const size = integer(args.size, 0, L.fileBytes), name = attachmentFileName(args.name), media = args.mediaType;
    if (item) {
      assert(item.author_id === actor.accountId && item.community_id === community.id && item.name === name
        && item.size === size && item.media_type === media, 'attachment_request_conflict');
      assert(['pending','complete','published'].includes(item.state), 'attachment_unavailable');
      assert(item.state === 'published' || item.expires_at > now, 'attachment_unavailable');
      return { attachment: { ...publicRef(item), state: item.state } };
    }
    // Bounded foreground collection never touches published refs; tombstones prohibit resurrection.
    const expired = m.all("SELECT id FROM chat_attachments WHERE state IN('pending','complete') AND expires_at<=? LIMIT 40", now);
    for (const entry of expired) {
      m.run('DELETE FROM chat_attachment_chunks WHERE attachment_id=?', entry.id);
      m.run("UPDATE chat_attachments SET state='expired' WHERE id=?", entry.id);
    }
    assert(m.get('SELECT COUNT(*) AS n FROM chat_attachments').n < L.records, 'attachment_store_capacity');
    assert(m.get("SELECT COUNT(*) AS n FROM chat_attachments WHERE author_id=? AND state IN('pending','complete')", actor.accountId).n < L.batch, 'attachment_batch_limit');
    const reserved = scope => m.get("SELECT COALESCE(SUM(size),0) AS n FROM chat_attachments WHERE state IN('pending','complete','published')" + (scope ? ' AND author_id=?' : ''), ...(scope ? [actor.accountId] : [])).n;
    assert(reserved(false) + size <= L.storeBytes && reserved(true) + size <= L.accountBytes, 'attachment_storage_quota');
    assert(m.get('PRAGMA page_count').page_count * m.get('PRAGMA page_size').page_size + size <= L.databaseBytes, 'attachment_storage_quota');
    m.rate('attachments:' + actor.accountId, 30, 60_000, now);
    m.run("INSERT INTO chat_attachments(id,community_id,author_id,name,media_type,size,state,created_at,expires_at) VALUES (?,?,?,?,?,?,'pending',?,?)",
      attachmentId, community.id, actor.accountId, name, media, size, now, now + L.pendingMs);
    item = m.get('SELECT * FROM chat_attachments WHERE id=?', attachmentId);
    return { attachment: { ...publicRef(item), state: item.state } };
  }
  assert(item && item.community_id === community.id, 'attachment_unavailable');
  if (read) {
    const published = item.state === 'published' && m.get('SELECT 1 FROM messages WHERE id=? AND community_id=? AND deleted_at IS NULL', item.message_id, community.id);
    assert(published || item.author_id === actor.accountId && ['pending','complete'].includes(item.state) && item.expires_at > now, 'attachment_unavailable');
  } else assert(item.author_id === actor.accountId, 'attachment_unavailable');
  if (op === 'world.attachment.status') return { attachment: { ...publicRef(item), state: item.state },
    receivedChunks: m.get('SELECT COUNT(*) AS n FROM chat_attachment_chunks WHERE attachment_id=?', attachmentId).n };
  if (op === 'world.attachment.cancel') {
    assert(item.state !== 'published', 'attachment_already_published');
    if (['pending','complete'].includes(item.state)) {
      m.run('DELETE FROM chat_attachment_chunks WHERE attachment_id=?', attachmentId);
      m.run("UPDATE chat_attachments SET state='cancelled' WHERE id=?", attachmentId);
    }
    return { cancelled: true };
  }
  if (op === 'world.attachment.read') {
    assert(['complete','published'].includes(item.state), 'attachment_not_ready');
    const index = integer(args.index, 0, Math.max(0, Math.ceil(item.size / L.chunkBytes) - 1));
    const part = m.get('SELECT * FROM chat_attachment_chunks WHERE attachment_id=? AND chunk_index=?', attachmentId, index);
    assert(part, 'attachment_chunk_missing'); assert(sha(part.data) === part.sha256, 'attachment_integrity_failed');
    return { index, size: part.size, sha256: part.sha256, data: Buffer.from(part.data).toString('base64') };
  }
  assert(['pending','complete'].includes(item.state) && item.expires_at > now, 'attachment_unavailable');
  if (op === 'world.attachment.chunk') {
    const index = integer(args.index, 0, Math.ceil(item.size / L.chunkBytes) - 1);
    assert(digest(args.sha256) && typeof args.data === 'string' && args.data.length <= Math.ceil(L.chunkBytes / 3) * 4
      && /^[A-Za-z0-9+/]*={0,2}$/u.test(args.data), 'invalid_attachment_chunk');
    const data = Buffer.from(args.data, 'base64');
    assert(data.toString('base64') === args.data && data.length === Math.min(L.chunkBytes, item.size - index * L.chunkBytes)
      && sha(data) === args.sha256, 'attachment_integrity_failed');
    const previous = m.get('SELECT * FROM chat_attachment_chunks WHERE attachment_id=? AND chunk_index=?', attachmentId, index);
    if (previous) assert(previous.sha256 === args.sha256 && Buffer.from(previous.data).equals(data), 'attachment_request_conflict');
    else {
      assert(item.state === 'pending' && index === m.get('SELECT COUNT(*) AS n FROM chat_attachment_chunks WHERE attachment_id=?', attachmentId).n, 'attachment_chunk_order');
      m.run('INSERT INTO chat_attachment_chunks(attachment_id,chunk_index,size,sha256,data) VALUES (?,?,?,?,?)', attachmentId, index, data.length, args.sha256, data);
    }
    return { accepted: true, index, sha256: args.sha256 };
  }
  assert(op === 'world.attachment.complete' && digest(args.manifestSha256), 'invalid_attachment_manifest');
  if (item.state === 'complete') {
    assert(item.manifest_sha256 === args.manifestSha256, 'attachment_request_conflict');
    return { attachment: { ...publicRef(item), state: item.state } };
  }
  const chunks = [], hash = createHash('sha256');
  for (const part of m.db.prepare('SELECT * FROM chat_attachment_chunks WHERE attachment_id=? ORDER BY chunk_index').iterate(attachmentId)) {
    assert(part.chunk_index === chunks.length && sha(part.data) === part.sha256, 'attachment_integrity_failed');
    chunks.push({ bytes: part.size, sha256: part.sha256 }); hash.update(part.data);
  }
  assert(chunks.length === Math.ceil(item.size / L.chunkBytes), 'attachment_chunk_missing');
  assert(sha(attachmentManifest(item.size, chunks)) === args.manifestSha256, 'attachment_integrity_failed');
  m.run("UPDATE chat_attachments SET state='complete',manifest_sha256=?,file_sha256=? WHERE id=?", args.manifestSha256, hash.digest('hex'), attachmentId);
  return { attachment: { ...publicRef(m.get('SELECT * FROM chat_attachments WHERE id=?', attachmentId)), state: 'complete' } };
}
