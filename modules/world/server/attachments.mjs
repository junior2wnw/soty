// Reuses M02's signed chunks/manifest (bff053b0), adapted to canonical7 and DM.
import { createHash } from 'node:crypto';
import { attachmentFileName } from '../attachment-file-policy.mjs';
import { ATTACHMENT_LIMITS as L, attachmentManifest } from '../attachment-contract.mjs';
import { assert, exact, identifier, integer } from './validation.mjs';
import { requireDirectThread } from './direct-scope.mjs';
export { ATTACHMENT_LIMITS } from '../attachment-contract.mjs';
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const digest = value => typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value);
export const ATTACHMENT_OPERATIONS = ['world.attachment.capabilities', 'world.attachment.begin', 'world.attachment.chunk',
  'world.attachment.complete', 'world.attachment.status', 'world.attachment.read', 'world.attachment.cancel'];
export const ATTACHMENT_READ_OPERATIONS = ['world.attachment.capabilities', 'world.attachment.status', 'world.attachment.read'];
const publicRef = row => ({ attachmentId: row.id, name: row.name, size: row.size, manifestSha256: row.manifest_sha256,
  fileSha256: row.file_sha256, chunks: Math.ceil(row.size / L.chunkBytes), mediaType: 'application/octet-stream' });
export function messageAttachments(m, messageId, kind) {
  return m.schemaVersion !== 7 ? [] : m.all("SELECT id,name,size,manifest_sha256,file_sha256 FROM world_chat_attachments WHERE scope_kind=? AND message_id=? AND state='published' ORDER BY position", kind, messageId).map(publicRef);
}
export function admitMessageAttachments(m, ids, scope, actorId, now) {
  assert(m.schemaVersion === 7, 'attachments_disabled');
  assert(Array.isArray(ids) && ids.length <= L.batch && new Set(ids).size === ids.length, 'attachment_batch_limit');
  let total = 0;
  return ids.map(value => {
    const row = m.get('SELECT * FROM world_chat_attachments WHERE id=?', identifier(value));
    assert(row && row.scope_kind === scope.kind && row.conversation_id === scope.id && row.relationship_id === (scope.relationshipId ?? null)
      && row.author_id === actorId && row.state === 'complete' && row.expires_at > now && digest(row.manifest_sha256), 'attachment_not_ready');
    total += row.size; assert(total <= L.batchBytes, 'attachment_batch_limit'); return row;
  });
}
export function publishMessageAttachments(m, rows, messageId) {
  rows.forEach((row, position) => m.run("UPDATE world_chat_attachments SET state='published',message_id=?,position=? WHERE id=?", messageId, position, row.id));
}
export function removeMessageAttachments(m, messageId, kind) {
  if (m.schemaVersion !== 7) return;
  m.run('DELETE FROM world_chat_attachment_chunks WHERE attachment_id IN(SELECT id FROM world_chat_attachments WHERE scope_kind=? AND message_id=?)', kind, messageId);
  m.run("UPDATE world_chat_attachments SET state='removed' WHERE scope_kind=? AND message_id=?", kind, messageId);
}
export function attachmentOperation(m, op, args, actor, now, authority) {
  const direct = args?.threadId !== undefined, base = ['expectedAccountId', ...(direct ? ['threadId', 'expectedRelationshipId'] : ['communityId'])];
  const shapes = { 'world.attachment.capabilities': [], 'world.attachment.begin': ['attachmentId', 'name', 'size', 'mediaType'],
    'world.attachment.chunk': ['attachmentId', 'index', 'sha256', 'data'], 'world.attachment.complete': ['attachmentId', 'manifestSha256'],
    'world.attachment.status': ['attachmentId'], 'world.attachment.read': ['attachmentId', 'index'], 'world.attachment.cancel': ['attachmentId'] };
  assert(Object.hasOwn(shapes, op), 'unsupported_operation'); exact(args, [...base, ...shapes[op]]);
  assert(args.expectedAccountId === actor.accountId, 'attachment_account_mismatch');
  const row = direct ? requireDirectThread(m, args, actor, authority) : m.requireMember(args.communityId, actor.accountId).row;
  const scope = { kind: direct ? 'direct' : 'room', id: row.id, relationshipId: direct ? row.relationship_id : null };
  if (op === 'world.attachment.capabilities') return { schema: 'soty.chat-attachment.capabilities.v1', readable: m.schemaVersion === 7, writable: m.schemaVersion === 7, limits: L };
  assert(m.schemaVersion === 7, 'attachments_disabled');
  const reading = ATTACHMENT_READ_OPERATIONS.includes(op), attachmentId = identifier(args.attachmentId);
  let item = m.get('SELECT * FROM world_chat_attachments WHERE id=?', attachmentId);
  if (op === 'world.attachment.begin') {
    assert(typeof args.name === 'string' && args.name.length <= 4096 && typeof args.mediaType === 'string' && args.mediaType.length <= 128 && !/[\u0000-\u001f\u007f]/u.test(args.mediaType), 'invalid_attachment_metadata');
    const size = integer(args.size, 0, L.fileBytes), name = attachmentFileName(args.name), media = args.mediaType;
    if (item) {
      assert(item.author_id === actor.accountId && item.scope_kind === scope.kind && item.conversation_id === scope.id && item.relationship_id === scope.relationshipId && item.name === name && item.size === size && item.media_type === media, 'attachment_request_conflict');
      assert(['pending', 'complete', 'published'].includes(item.state) && (item.state === 'published' || item.expires_at > now), 'attachment_unavailable');
      return { attachment: { ...publicRef(item), state: item.state } };
    }
    for (const entry of m.all("SELECT id FROM world_chat_attachments WHERE state IN('pending','complete') AND expires_at<=? LIMIT 40", now)) {
      m.run('DELETE FROM world_chat_attachment_chunks WHERE attachment_id=?', entry.id); m.run("UPDATE world_chat_attachments SET state='expired' WHERE id=?", entry.id);
    }
    assert(m.get('SELECT COUNT(*) AS n FROM world_chat_attachments').n < L.records, 'attachment_store_capacity');
    assert(m.get("SELECT COUNT(*) AS n FROM world_chat_attachments WHERE author_id=? AND state IN('pending','complete') AND expires_at>?", actor.accountId, now).n < L.batch, 'attachment_batch_limit');
    const reserved = own => m.get("SELECT COALESCE(SUM(size),0) AS n FROM world_chat_attachments WHERE (state='published' OR state IN('pending','complete') AND expires_at>?)" + (own ? ' AND author_id=?' : ''), now, ...(own ? [actor.accountId] : [])).n;
    assert(reserved(false) + size <= L.storeBytes && reserved(true) + size <= L.accountBytes, 'attachment_storage_quota');
    const pages = m.get('PRAGMA page_count').page_count - m.get('PRAGMA freelist_count').freelist_count;
    assert(pages * m.get('PRAGMA page_size').page_size + size <= L.databaseBytes, 'attachment_storage_quota');
    m.rate('attachments:' + actor.accountId, 30, 60_000, now);
    m.run("INSERT INTO world_chat_attachments(id,scope_kind,conversation_id,relationship_id,author_id,name,media_type,size,state,created_at,expires_at) VALUES (?,?,?,?,?,?,?,?,'pending',?,?)", attachmentId, scope.kind, scope.id, scope.relationshipId, actor.accountId, name, media, size, now, now + L.pendingMs);
    return { attachment: { ...publicRef(m.get('SELECT * FROM world_chat_attachments WHERE id=?', attachmentId)), state: 'pending' } };
  }
  assert(item && item.scope_kind === scope.kind && item.conversation_id === scope.id, 'attachment_unavailable');
  if (reading) {
    const table = direct ? 'world_direct_messages' : 'messages', column = direct ? 'thread_id' : 'community_id';
    const published = item.state === 'published' && m.get(`SELECT 1 FROM ${table} WHERE id=? AND ${column}=? AND deleted_at IS NULL`, item.message_id, scope.id);
    assert(published || item.author_id === actor.accountId && item.relationship_id === scope.relationshipId && ['pending', 'complete'].includes(item.state) && item.expires_at > now, 'attachment_unavailable');
  } else assert(item.author_id === actor.accountId && item.relationship_id === scope.relationshipId, 'attachment_unavailable');
  if (op === 'world.attachment.status') return { attachment: { ...publicRef(item), state: item.state }, receivedChunks: m.get('SELECT COUNT(*) AS n FROM world_chat_attachment_chunks WHERE attachment_id=?', attachmentId).n };
  if (op === 'world.attachment.cancel') {
    assert(item.state !== 'published', 'attachment_already_published');
    if (['pending', 'complete'].includes(item.state)) { m.run('DELETE FROM world_chat_attachment_chunks WHERE attachment_id=?', attachmentId); m.run("UPDATE world_chat_attachments SET state='cancelled' WHERE id=?", attachmentId); }
    return { cancelled: true };
  }
  if (op === 'world.attachment.read') {
    assert(['complete', 'published'].includes(item.state), 'attachment_not_ready');
    const index = integer(args.index, 0, Math.max(0, Math.ceil(item.size / L.chunkBytes) - 1));
    const part = m.get('SELECT * FROM world_chat_attachment_chunks WHERE attachment_id=? AND chunk_index=?', attachmentId, index);
    assert(part, 'attachment_chunk_missing'); assert(sha(part.data) === part.sha256, 'attachment_integrity_failed');
    return { index, size: part.size, sha256: part.sha256, data: Buffer.from(part.data).toString('base64') };
  }
  assert(['pending', 'complete'].includes(item.state) && item.expires_at > now, 'attachment_unavailable');
  if (op === 'world.attachment.chunk') {
    const index = integer(args.index, 0, Math.ceil(item.size / L.chunkBytes) - 1);
    assert(digest(args.sha256) && typeof args.data === 'string' && args.data.length <= Math.ceil(L.chunkBytes / 3) * 4 && /^[A-Za-z0-9+/]*={0,2}$/u.test(args.data), 'invalid_attachment_chunk');
    const data = Buffer.from(args.data, 'base64');
    assert(data.toString('base64') === args.data && data.length === Math.min(L.chunkBytes, item.size - index * L.chunkBytes) && sha(data) === args.sha256, 'attachment_integrity_failed');
    const previous = m.get('SELECT * FROM world_chat_attachment_chunks WHERE attachment_id=? AND chunk_index=?', attachmentId, index);
    if (previous) assert(previous.sha256 === args.sha256 && Buffer.from(previous.data).equals(data), 'attachment_request_conflict');
    else { assert(item.state === 'pending' && index === m.get('SELECT COUNT(*) AS n FROM world_chat_attachment_chunks WHERE attachment_id=?', attachmentId).n, 'attachment_chunk_order'); m.run('INSERT INTO world_chat_attachment_chunks(attachment_id,chunk_index,size,sha256,data) VALUES (?,?,?,?,?)', attachmentId, index, data.length, args.sha256, data); }
    return { accepted: true, index, sha256: args.sha256 };
  }
  assert(op === 'world.attachment.complete' && digest(args.manifestSha256), 'invalid_attachment_manifest');
  if (item.state === 'complete') { assert(item.manifest_sha256 === args.manifestSha256, 'attachment_request_conflict'); return { attachment: { ...publicRef(item), state: item.state } }; }
  const chunks = [], hash = createHash('sha256');
  for (const part of m.db.prepare('SELECT * FROM world_chat_attachment_chunks WHERE attachment_id=? ORDER BY chunk_index').iterate(attachmentId)) {
    assert(part.chunk_index === chunks.length && sha(part.data) === part.sha256, 'attachment_integrity_failed'); chunks.push({ bytes: part.size, sha256: part.sha256 }); hash.update(part.data);
  }
  assert(chunks.length === Math.ceil(item.size / L.chunkBytes), 'attachment_chunk_missing'); assert(sha(attachmentManifest(item.size, chunks)) === args.manifestSha256, 'attachment_integrity_failed');
  m.run("UPDATE world_chat_attachments SET state='complete',manifest_sha256=?,file_sha256=? WHERE id=?", args.manifestSha256, hash.digest('hex'), attachmentId);
  return { attachment: { ...publicRef(m.get('SELECT * FROM world_chat_attachments WHERE id=?', attachmentId)), state: 'complete' } };
}
