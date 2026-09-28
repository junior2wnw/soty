import { assert, exact, identifier, id, integer, text, limit } from './validation.mjs';

export const CHAT_OPERATIONS = ['world.chat.list', 'world.chat.send', 'world.chat.remove', 'world.chat.read'];
export function chatOperation(m, op, args, actor, now) {
  function message(row) {
    return { messageId: row.id, seq: row.seq, text: row.deleted_at === null ? row.text : '',
      author: m.profile(m.person(row.author_id)), createdAt: row.created_at,
      replyTo: row.reply_to, removed: row.deleted_at !== null };
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
    exact(args, ['communityId', 'clientId', 'text'], ['replyTo']);
    const { row } = m.requireMember(args.communityId, actor.accountId);
    const clientId = identifier(args.clientId); const body = text(args.text, 6000, { empty: false, multiline: true });
    const replyTo = args.replyTo === undefined || args.replyTo === null ? null : identifier(args.replyTo);
    if (replyTo) assert(m.get('SELECT 1 FROM messages WHERE id=? AND community_id=?', replyTo, row.id), 'message_not_found');
    const previous = m.get('SELECT * FROM messages WHERE community_id=? AND author_id=? AND client_id=?', row.id, actor.accountId, clientId);
    if (previous) {
      // The original body is deliberately removed after moderation. A replay can return its tombstone,
      // but can never restore the removed content or create a new message.
      assert(previous.deleted_at !== null || previous.text === body && previous.reply_to === replyTo, 'request_conflict');
      return { message: message(previous) };
    }
    m.rate('chat:' + actor.accountId, 60, 60_000, now);
    const messageId = id('msg');
    m.run('INSERT INTO messages(id,community_id,author_id,client_id,text,reply_to,created_at) VALUES (?,?,?,?,?,?,?)', messageId, row.id, actor.accountId, clientId, body, replyTo, now);
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
