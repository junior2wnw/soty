import { assert, exact, identifier, id, integer, text, limit } from './validation.mjs';
import { admitMessageAttachments, messageAttachments, publishMessageAttachments, removeMessageAttachments } from './attachments.mjs';

export const CHAT_OPERATIONS = ['world.chat.list', 'world.chat.send', 'world.chat.remove', 'world.chat.read'];
export function chatOperation(m, op, args, actor, now) {
  let readThrough;
  function message(row) {
    const own = row.author_id === actor.accountId;
    const attachments = row.deleted_at === null ? messageAttachments(m, row.id, 'room') : [];
    if (own && readThrough === undefined) readThrough = m.get("SELECT MAX(last_read_seq) AS seq FROM memberships WHERE community_id=? AND account_id<>? AND state='active'", row.community_id, actor.accountId)?.seq ?? 0;
    return { messageId: row.id, seq: row.seq, text: row.deleted_at === null ? row.text : '',
      author: m.identity(m.person(row.author_id)), createdAt: row.created_at,
      replyTo: row.reply_to, removed: row.deleted_at !== null,
      ...(own ? { clientId: row.client_id, read: row.deleted_at === null && readThrough >= row.seq } : {}), ...(attachments.length ? { attachments } : {}) };
  }
  if (op === 'world.chat.list') {
    exact(args, ['communityId'], ['after', 'before', 'limit']);
    const { row } = m.requireMember(args.communityId, actor.accountId); const size = limit(args.limit);
    assert(args.after === undefined || args.before === undefined, 'invalid_cursor');
    const after = args.after === undefined ? 0 : integer(args.after);
    const before = args.before === undefined ? Number.MAX_SAFE_INTEGER : integer(args.before, 1);
    const ascending = args.after !== undefined;
    const rows = m.all(`SELECT * FROM messages WHERE community_id=? AND seq>? AND seq<? ORDER BY seq ${ascending ? 'ASC' : 'DESC'} LIMIT ?`, row.id, after, before, size + 1);
    const page = rows.slice(0, size);
    return { messages: (ascending ? page : page.reverse()).map(message), hasMore: rows.length > size };
  }
  if (op === 'world.chat.send') {
    exact(args, ['communityId', 'clientId', 'text'], ['replyTo', 'attachments', 'expectedAccountId']);
    const { row } = m.requireMember(args.communityId, actor.accountId);
    const refs = args.attachments ?? []; assert(Array.isArray(refs) && refs.length <= 10 && new Set(refs).size === refs.length, 'attachment_batch_limit'); refs.forEach(identifier);
    if (refs.length || args.expectedAccountId !== undefined) assert(args.expectedAccountId === actor.accountId, 'attachment_account_mismatch');
    const clientId = identifier(args.clientId); const body = text(args.text, 6000, { empty: refs.length > 0, multiline: true });
    const replyTo = args.replyTo === undefined || args.replyTo === null ? null : identifier(args.replyTo);
    if (replyTo) assert(m.get('SELECT 1 FROM messages WHERE id=? AND community_id=?', replyTo, row.id), 'message_not_found');
    const previous = m.get('SELECT * FROM messages WHERE community_id=? AND author_id=? AND client_id=?', row.id, actor.accountId, clientId);
    if (previous) {
      // The original body is deliberately removed after moderation. A replay can return its tombstone,
      // but can never restore the removed content or create a new message.
      assert(previous.deleted_at !== null || previous.text === body && previous.reply_to === replyTo && JSON.stringify(messageAttachments(m, previous.id, 'room').map(item => item.attachmentId)) === JSON.stringify(refs), 'request_conflict');
      return { message: message(previous) };
    }
    m.rate('chat:' + actor.accountId, 60, 60_000, now);
    const messageId = id('msg');
    const attachments = refs.length ? admitMessageAttachments(m, refs, { kind: 'room', id: row.id }, actor.accountId, now) : [];
    m.run('INSERT INTO messages(id,community_id,author_id,client_id,text,reply_to,created_at) VALUES (?,?,?,?,?,?,?)', messageId, row.id, actor.accountId, clientId, body, replyTo, now);
    publishMessageAttachments(m, attachments, messageId);
    m.emit({ kind: 'room', conversationId: row.id, messageId });
    return { message: message(m.get('SELECT * FROM messages WHERE id=?', messageId)) };
  }
  if (op === 'world.chat.remove') {
    exact(args, ['communityId', 'messageId']);
    const { row, participation } = m.requireMember(args.communityId, actor.accountId);
    const item = m.get('SELECT * FROM messages WHERE id=? AND community_id=?', identifier(args.messageId), row.id);
    assert(item, 'message_not_found');
    assert(item.author_id === actor.accountId || ['owner', 'moderator'].includes(participation.role), 'community_permission_denied');
    if (item.deleted_at === null) {
      m.run("UPDATE messages SET text='',deleted_at=?,deleted_by=? WHERE id=?", now, actor.accountId, item.id);
      removeMessageAttachments(m, item.id, 'room');
      m.audit(actor.accountId, op, item.id, now);
    }
    return { removed: true };
  }
  if (op === 'world.chat.read') {
    exact(args, ['communityId', 'throughSeq']);
    const { row } = m.requireMember(args.communityId, actor.accountId); const through = integer(args.throughSeq);
    assert(through === 0 || m.get('SELECT 1 FROM messages WHERE community_id=? AND seq=?', row.id, through), 'message_not_found');
    m.run('UPDATE memberships SET last_read_seq=MAX(last_read_seq,?) WHERE community_id=? AND account_id=?', through, row.id, actor.accountId);
    return { unreadCount: m.unread(row.id, actor.accountId) };
  }
}
