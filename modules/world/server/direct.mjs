import { createHash } from 'node:crypto';
import { assert, identifier, id, integer, text, limit, WorldError } from './validation.mjs';

export const DIRECT_OPERATIONS = Object.freeze(['world.direct.open','world.direct.get','world.direct.list',
  'world.direct.chat.list','world.direct.chat.send','world.direct.chat.remove','world.direct.chat.read','world.direct.preferences']);
export const DIRECT_READ_OPERATIONS = Object.freeze(['world.direct.get','world.direct.list','world.direct.chat.list']);
export const DIRECT_LIMITS = Object.freeze({ threadsPerAccount: 1000, page: 60, text: 6000 });
const schema = 'soty.world-direct.v1';
const hash = value => createHash('sha256').update(JSON.stringify(value)).digest('base64url');
function closed(value, required, optional = []) {
  assert(value && typeof value === 'object' && [Object.prototype,null].includes(Object.getPrototypeOf(value)), 'invalid_arguments');
  const descriptors = Object.getOwnPropertyDescriptors(value), allowed = new Set([...required,...optional]);
  assert(Reflect.ownKeys(descriptors).every(key => typeof key === 'string' && allowed.has(key) && Object.hasOwn(descriptors[key],'value'))
    && required.every(key => Object.hasOwn(descriptors,key)), 'invalid_arguments');
}

/** Called only inside the normal World transaction and its live Connect fence.
 * authority is a trusted synchronous port, never a browser-provided DTO. */
export function directOperation(m, op, args, actor, now, authority) {
  assert(DIRECT_OPERATIONS.includes(op), 'unsupported_operation');
  assert(m.db.isTransaction, 'direct_transaction_required'); integer(now);
  const required = ['expectedAccountId'];
  if (op === 'world.direct.list') closed(args, required, ['limit','cursor']);
  else if (op === 'world.direct.open') closed(args, [...required,'peerAccountId','expectedRelationshipId','requestId']);
  else {
    const common = [...required,'threadId','expectedRelationshipId'];
    if (op === 'world.direct.get') closed(args,common);
    else if (op === 'world.direct.chat.list') closed(args,common,['before','after','limit']);
    else if (op === 'world.direct.chat.send') closed(args,[...common,'clientId','text'],['replyTo']);
    else if (op === 'world.direct.chat.remove') closed(args,[...common,'messageId']);
    else if (op === 'world.direct.chat.read') closed(args,[...common,'throughSeq']);
    else closed(args,common,['pinned','muted']);
  }
  identifier(args.expectedAccountId); identifier(actor?.accountId); identifier(actor?.deviceId);
  assert(args.expectedAccountId === actor.accountId, 'direct_account_changed');
  assert(authority && typeof authority.relationshipId === 'function' && typeof authority.projectId === 'string'
    && authority.projectId.length > 0 && authority.projectId.length <= 128, 'direct_authority_unavailable');
  const relationship = peer => {
    const value = authority.relationshipId(peer);
    if (value === null) return null;
    return identifier(value);
  };
  const peerId = row => row.account_low === actor.accountId ? row.account_high : row.account_high === actor.accountId ? row.account_low : null;
  const current = row => {
    const peer = row && peerId(row);
    return Boolean(peer && relationship(peer) === row.relationship_id);
  };
  const find = () => {
    identifier(args.threadId); identifier(args.expectedRelationshipId);
    const row = m.get('SELECT * FROM world_direct_threads WHERE id=?',args.threadId);
    assert(current(row) && row.relationship_id === args.expectedRelationshipId,'direct_not_found');
    return row;
  };
  const readState = row => m.get('SELECT * FROM world_direct_read_state WHERE thread_id=? AND account_id=?',row.id,actor.accountId);
  const unread = row => m.get(`SELECT COUNT(*) AS n FROM world_direct_messages
    WHERE thread_id=? AND seq>? AND author_id<>? AND deleted_at IS NULL`,row.id,readState(row)?.last_read_seq ?? 0,actor.accountId).n;
  const project = row => {
    const peer = peerId(row), profile = m.person(peer), prefs = readState(row);
    return {schema,threadId:row.id,revision:row.revision,relationshipId:row.relationship_id,
      peer:{accountId:peer,label:profile?.display_name ?? null,avatarRevision:profile?.avatar_revision ?? null},
      unreadCount:unread(row),preferences:{pinned:Boolean(prefs?.pinned),muted:Boolean(prefs?.muted)},createdAt:row.created_at};
  };
  const message = row => {
    const profile = m.person(row.author_id);
    return {messageId:row.id,seq:row.seq,text:row.deleted_at === null ? row.text : '',
      author:profile ? m.identity(profile) : {profileId:row.author_id,displayName:null,avatarColor:null,avatarRevision:null,revision:null},
      createdAt:row.created_at,replyTo:row.reply_to,removed:row.deleted_at !== null};
  };
  if (op === 'world.direct.open') {
    const peer = identifier(args.peerAccountId), expected = identifier(args.expectedRelationshipId); identifier(args.requestId);
    assert(peer !== actor.accountId && relationship(peer) === expected,'direct_contact_required');
    const digest = hash([actor.accountId,peer,expected,args.requestId]);
    const accepted = m.get('SELECT * FROM receipts WHERE account_id=? AND operation=? AND request_id=?',actor.accountId,op,args.requestId);
    if (accepted) {
      assert(accepted.digest === digest,'direct_request_conflict');
      const receipt = JSON.parse(accepted.result), row = m.get('SELECT * FROM world_direct_threads WHERE id=?',receipt.threadId);
      assert(current(row) && row.relationship_id === expected,'direct_not_found');
      return {receipt,replayed:true,current:project(row)};
    }
    const [low,high] = [actor.accountId,peer].sort();
    let row = m.get('SELECT * FROM world_direct_threads WHERE account_low=? AND account_high=?',low,high);
    if (!row) {
      assert(m.get('SELECT COUNT(*) AS n FROM world_direct_threads WHERE account_low=? OR account_high=?',actor.accountId,actor.accountId).n < DIRECT_LIMITS.threadsPerAccount,'direct_limit');
      assert(m.get('SELECT COUNT(*) AS n FROM world_direct_threads WHERE account_low=? OR account_high=?',peer,peer).n < DIRECT_LIMITS.threadsPerAccount,'direct_limit');
      m.rate('direct-create:'+actor.accountId,10,60_000,now);
      const threadId = id('direct');
      m.run(`INSERT INTO world_direct_threads(id,account_low,account_high,relationship_id,created_at,updated_at)
        VALUES (?,?,?,?,?,?)`,threadId,low,high,expected,now,now);
      row = m.get('SELECT * FROM world_direct_threads WHERE id=?',threadId);
    } else if (row.relationship_id !== expected) {
      assert(row.revision < Number.MAX_SAFE_INTEGER,'direct_revision_exhausted');
      m.run('UPDATE world_direct_threads SET relationship_id=?,revision=revision+1,updated_at=? WHERE id=?',expected,now,row.id);
      row = m.get('SELECT * FROM world_direct_threads WHERE id=?',row.id);
    }
    const receipt = {schema:'soty.world-direct-open.receipt.v1',requestId:args.requestId,accountId:actor.accountId,
      threadId:row.id,relationshipId:expected,acceptedRevision:row.revision};
    m.run('INSERT INTO receipts(account_id,operation,request_id,digest,result,created_at) VALUES (?,?,?,?,?,?)',
      actor.accountId,op,args.requestId,digest,JSON.stringify(receipt),now);
    m.audit(actor.accountId,op,row.id,now);
    return {receipt,replayed:false,current:project(row)};
  }
  if (op === 'world.direct.list') {
    const size = limit(args.limit), scope = hash([schema,authority.projectId,actor.accountId,actor.deviceId]); let after = 0;
    if (args.cursor !== undefined) {
      assert(typeof args.cursor === 'string' && args.cursor.length <= 768 && /^[A-Za-z0-9_-]+$/u.test(args.cursor),'invalid_direct_cursor');
      try {
        const parsed = JSON.parse(Buffer.from(args.cursor,'base64url').toString('utf8')); closed(parsed,['schema','scope','after']);
        assert(parsed.schema === schema && parsed.scope === scope,'invalid_direct_cursor'); after = integer(parsed.after,1);
        assert(Buffer.from(JSON.stringify(parsed)).toString('base64url') === args.cursor,'invalid_direct_cursor');
      } catch { throw new WorldError('invalid_direct_cursor'); }
    }
    const visible = []; let scanned = after;
    while (visible.length <= size) {
      const rows = m.all(`SELECT * FROM world_direct_threads WHERE (account_low=? OR account_high=?) AND seq>?
        ORDER BY seq LIMIT 60`,actor.accountId,actor.accountId,scanned);
      for (const row of rows) { scanned = row.seq; if (current(row)) visible.push(row); if (visible.length > size) break; }
      if (visible.length > size || rows.length < 60) break;
    }
    const page = visible.slice(0,size), cursor = visible.length > size
      ? Buffer.from(JSON.stringify({schema,scope,after:page.at(-1).seq})).toString('base64url') : null;
    return {schema,threads:page.map(project),nextCursor:cursor};
  }
  const row = find();
  if (op === 'world.direct.get') return {thread:project(row)};
  if (op === 'world.direct.chat.list') {
    assert(args.before === undefined || args.after === undefined,'invalid_cursor');
    const size = limit(args.limit), after = args.after === undefined ? 0 : integer(args.after),
      before = args.before === undefined ? Number.MAX_SAFE_INTEGER : integer(args.before,1), ascending = args.after !== undefined;
    const rows = m.all(`SELECT * FROM world_direct_messages WHERE thread_id=? AND seq>? AND seq<?
      ORDER BY seq ${ascending ? 'ASC' : 'DESC'} LIMIT ?`,row.id,after,before,size+1), page = rows.slice(0,size);
    return {messages:(ascending ? page : page.reverse()).map(message),hasMore:rows.length>size};
  }
  if (op === 'world.direct.chat.send') {
    const clientId = identifier(args.clientId), body = text(args.text,DIRECT_LIMITS.text,{empty:false,multiline:true});
    assert(body.isWellFormed(),'invalid_text');
    const replyTo = args.replyTo === undefined || args.replyTo === null ? null : identifier(args.replyTo);
    if (replyTo) assert(m.get('SELECT 1 FROM world_direct_messages WHERE id=? AND thread_id=?',replyTo,row.id),'message_not_found');
    const old = m.get('SELECT * FROM world_direct_messages WHERE thread_id=? AND author_id=? AND client_id=?',row.id,actor.accountId,clientId);
    if (old) {
      assert(old.relationship_id === args.expectedRelationshipId && (old.deleted_at !== null || old.text === body && old.reply_to === replyTo),'request_conflict');
      return {message:message(old)};
    }
    m.rate('chat:'+actor.accountId,60,60_000,now);
    const messageId = id('directmsg');
    m.run(`INSERT INTO world_direct_messages(id,thread_id,author_id,relationship_id,client_id,text,reply_to,created_at)
      VALUES (?,?,?,?,?,?,?,?)`,messageId,row.id,actor.accountId,row.relationship_id,clientId,body,replyTo,now);
    return {message:message(m.get('SELECT * FROM world_direct_messages WHERE id=?',messageId))};
  }
  if (op === 'world.direct.chat.remove') {
    const item = m.get('SELECT * FROM world_direct_messages WHERE id=? AND thread_id=?',identifier(args.messageId),row.id);
    assert(item,'message_not_found'); assert(item.author_id === actor.accountId,'direct_permission_denied');
    if (item.deleted_at === null) {
      m.run("UPDATE world_direct_messages SET text='',deleted_at=?,deleted_by=? WHERE id=?",now,actor.accountId,item.id);
      m.audit(actor.accountId,op,item.id,now);
    }
    return {removed:true};
  }
  const prefs = readState(row);
  if (op === 'world.direct.chat.read') {
    const through = integer(args.throughSeq);
    assert(through === 0 || m.get('SELECT 1 FROM world_direct_messages WHERE thread_id=? AND seq=?',row.id,through),'message_not_found');
    const next = Math.max(prefs?.last_read_seq ?? 0,through);
    m.run(`INSERT INTO world_direct_read_state(thread_id,account_id,last_read_seq) VALUES (?,?,?)
      ON CONFLICT(thread_id,account_id) DO UPDATE SET last_read_seq=excluded.last_read_seq`,row.id,actor.accountId,next);
    return {unreadCount:unread(row)};
  }
  assert(args.pinned !== undefined || args.muted !== undefined,'empty_update');
  for (const key of ['pinned','muted']) if (args[key] !== undefined) assert(typeof args[key] === 'boolean','invalid_boolean');
  const pinned = args.pinned === undefined ? prefs?.pinned ?? 0 : Number(args.pinned), muted = args.muted === undefined ? prefs?.muted ?? 0 : Number(args.muted);
  m.run(`INSERT INTO world_direct_read_state(thread_id,account_id,pinned,muted) VALUES (?,?,?,?)
    ON CONFLICT(thread_id,account_id) DO UPDATE SET pinned=excluded.pinned,muted=excluded.muted`,row.id,actor.accountId,pinned,muted);
  return {thread:project(row)};
}
