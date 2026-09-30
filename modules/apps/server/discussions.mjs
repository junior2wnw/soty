import { createHash, randomBytes, createCipheriv, createDecipheriv } from 'node:crypto';
import { AppsError, assertApps, appId, cleanGrants, textId } from './protocol.mjs';
import { createLaunchPath } from './launch-path.mjs';
import { readDiscussionAudience } from './schema.mjs';
import { createEngagementTransaction, synchronous } from './engagement-transaction.mjs';

export const discussionOperations = new Set(['context', 'history', 'changes', 'archives', 'send', 'remove'].map(name => `apps.discussion.${name}`));
export const DISCUSSION_LIMITS = Object.freeze({ heads: 8192, conversations: 8192, conversationsPerApp: 1000,
  messages: 1_000_000, messagesPerApp: 10_000, bodyBytes: 1024 ** 3, bodyBytesPerApp: 32 * 1024 ** 2, changesRetained: 2048 });
const RESPONSE_BYTES = 256 * 1024, CURSOR_TTL = 60 * 60 * 1000;
const hash = value => createHash('sha256').update(value).digest('hex');
const bytes = value => Buffer.byteLength(JSON.stringify(value), 'utf8');
const freshId = prefix => `${prefix}_${randomBytes(16).toString('hex')}`;
const exact = (value, keys) => assertApps(value && typeof value === 'object' && !Array.isArray(value)
  && Object.keys(value).every(key => keys.includes(key)), 'unexpected_argument');
function identifier(value, prefix) {
  assertApps(typeof value === 'string' && new RegExp(`^${prefix}_[a-f0-9]{32}$`, 'u').test(value), `invalid_discussion_${prefix}`); return value;
}
function timestamp(value) { assertApps(Number.isSafeInteger(value) && value >= 0, 'apps_discussion_clock_invalid', 500); return value; }
function bodyText(value) {
  assertApps(typeof value === 'string' && value.trim().length > 0 && value.length <= 4000 && value.isWellFormed()
    && !/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/u.test(value)
    && Buffer.byteLength(value, 'utf8') <= 16384, 'invalid_discussion_body'); return value;
}
function pageLimit(value, fallback, maximum) {
  assertApps(value === undefined || (Number.isSafeInteger(value) && value >= 1 && value <= maximum), 'invalid_discussion_limit'); return value ?? fallback;
}

// This hook is part of the caller's publication transaction. Empty generations
// replace one head; they never consume an archive/message admission slot.
export function syncDiscussionAudienceInTransaction(db, id, at) {
  assertApps(db.isTransaction, 'apps_transaction_required', 500); appId(id);
  const head = db.prepare('SELECT * FROM app_discussion_heads WHERE app_id=?').get(id);
  if (!head) return false;
  const current = readDiscussionAudience(db, id);
  assertApps(head.owner_account_id === current.ownerAccountId, 'apps_registry_corrupt', 500);
  if (head.audience_hash === current.hash) return false;
  assertApps(head.generation < Number.MAX_SAFE_INTEGER, 'apps_discussion_generation_exhausted', 409);
  db.prepare(`UPDATE app_discussion_heads SET current_id=?,generation=generation+1,mode=?,grants_json=?,audience_hash=?,updated_at=? WHERE app_id=?`)
    .run(freshId('conv'), current.mode, JSON.stringify(current.grants), current.hash, timestamp(at), id);
  return true;
}

export function createDiscussionRegistry({ db, now = Date.now, assertActor, withAuthorityFence, resolveEntry,
  canUse, readCommunityAuthority, authorLabel, limits: configured = {} }) {
  assertApps(db && [now, assertActor, resolveEntry, canUse, authorLabel].every(value => typeof value === 'function'),
    'apps_discussion_dependencies_required', 500);
  exact(configured, Object.keys(DISCUSSION_LIMITS));
  const limits = { ...DISCUSSION_LIMITS, ...configured };
  for (const [key, value] of Object.entries(limits)) assertApps(Number.isSafeInteger(value) && value > 0 && value <= DISCUSSION_LIMITS[key], 'invalid_discussion_limits');
  const run = createEngagementTransaction({ db, assertActor, withAuthorityFence, responseBytes: RESPONSE_BYTES,
    busyCode: 'apps_discussion_busy', timeoutCode: 'apps_discussion_timeout_invalid', responseCode: 'apps_discussion_response_too_large' });
  const cursorKey = randomBytes(32), instance = randomBytes(8).toString('hex');
  let closed = false;
  const usage = () => {
    const row = db.prepare('SELECT * FROM app_discussion_usage WHERE id=1').get();
    assertApps(row, 'apps_registry_corrupt', 500); return row;
  };
  function headFor(id, create) {
    const audience = readDiscussionAudience(db, id);
    let head = db.prepare('SELECT * FROM app_discussion_heads WHERE app_id=?').get(id);
    if (!head && create) {
      assertApps(usage().head_count < limits.heads, 'apps_discussion_capacity', 409);
      const at = timestamp(now());
      db.prepare('INSERT INTO app_discussion_heads VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)')
        .run(id, freshId('conv'), 1, audience.ownerAccountId, audience.mode, JSON.stringify(audience.grants), audience.hash, 0, 0, 0, at, 15000, at);
      db.prepare('UPDATE app_discussion_usage SET head_count=head_count+1 WHERE id=1').run();
      head = db.prepare('SELECT * FROM app_discussion_heads WHERE app_id=?').get(id);
    }
    assertApps(!head || (head.audience_hash === audience.hash && head.owner_account_id === audience.ownerAccountId), 'apps_registry_corrupt', 500);
    return head;
  }
  function entryFor(actor, input) {
    const value = synchronous(resolveEntry({ actor, appId: input.appId,
      ...(input.domainId === undefined ? {} : { domainId: input.domainId }), ...(input.path === undefined ? {} : { path: input.path }) }), 'apps_async_authority');
    if (value === null) return null;
    assertApps(value && typeof value === 'object' && !Array.isArray(value) && value.appId === input.appId
      && (input.domainId === undefined || value.domainId === input.domainId) && (input.path === undefined || value.path === input.path), 'apps_discussion_entry_invalid', 500);
    identifier(value.domainId, 'dom'); createLaunchPath(value.path);
    assertApps(typeof value.origin === 'string' && value.origin.length <= 512, 'apps_discussion_entry_invalid', 500);
    const domain = db.prepare('SELECT app_id,origin FROM app_domains WHERE id=?').get(value.domainId);
    assertApps(domain?.app_id === input.appId && domain.origin === value.origin, 'apps_discussion_entry_invalid', 500);
    return { appId: input.appId, domainId: value.domainId, origin: value.origin, path: value.path };
  }
  function access(actor, input, create = false) {
    const app = db.prepare('SELECT * FROM local_apps WHERE id=?').get(input.appId);
    assertApps(app, 'app_unavailable', 404);
    const administrative = input.administrative === true;
    let entry = null;
    if (administrative) assertApps(app.owner_account_id === actor.accountId, 'app_unavailable', 404);
    else { entry = entryFor(actor, input); assertApps(app.state === 'enabled' && entry, 'app_unavailable', 404); }
    return { actor, app, entry, administrative, head: headFor(input.appId, create) };
  }
  function currentConversation(base) {
    const head = base.head;
    if (!head) return null;
    return db.prepare('SELECT * FROM app_discussion_conversations WHERE id=?').get(head.current_id)
      ?? { ...head, id: head.current_id, message_seq: 0, change_seq: 0, created_at: head.updated_at };
  }
  function authority(base, rows) {
    if (base.administrative || base.actor.accountId === base.app.owner_account_id) return () => true;
    const candidates = [...new Set(rows.flatMap(row => cleanGrants(JSON.parse(row.grants_json)).communityIds))];
    assertApps(candidates.length <= 65536, 'apps_registry_corrupt', 500);
    let communities = new Set();
    if (candidates.length) {
      assertApps(typeof readCommunityAuthority === 'function', 'apps_authority_fence_required', 503);
      const result = synchronous(readCommunityAuthority(base.actor, base.app.owner_account_id, Object.freeze(candidates)), 'apps_async_authority');
      const allowed = new Set(candidates);
      assertApps(Array.isArray(result) && result.length <= candidates.length && result.every(id => typeof id === 'string' && allowed.has(id))
        && new Set(result).size === result.length, 'apps_discussion_authority_invalid', 500);
      communities = new Set(result);
    }
    let currentGrant;
    return row => {
      if (row.mode === 'anyone') return true;
      if (currentGrant === undefined) currentGrant = synchronous(canUse(base.actor, base.app), 'apps_async_authority') === true;
      if (!currentGrant) return false;
      const grants = cleanGrants(JSON.parse(row.grants_json));
      return grants.accountIds.includes(base.actor.accountId) || grants.communityIds.some(id => communities.has(id));
    };
  }
  function selected(base, id) {
    assertApps(base.head, 'app_unavailable', 404);
    const row = id === base.head.current_id ? currentConversation(base)
      : db.prepare('SELECT * FROM app_discussion_conversations WHERE app_id=? AND id=?').get(base.app.id, id);
    assertApps(row && authority(base, [row])(row), 'app_unavailable', 404); return row;
  }
  function contextView(base, row) {
    const current = row.id === base.head?.current_id, owner = base.actor.accountId === base.app.owner_account_id;
    const grants = cleanGrants(JSON.parse(row.grants_json));
    return { appId: base.app.id, entry: base.entry, conversationId: row.id,
      mode: current && !base.administrative && base.app.state === 'enabled' ? 'current' : 'archive', isCurrent: current,
      ownerAdministrative: base.administrative, audience: row.mode === 'anyone' ? 'public'
        : grants.accountIds.length || grants.communityIds.length ? 'shared' : 'owner',
      canPost: current && !base.administrative && base.app.state === 'enabled', canModerate: owner };
  }
  function messageView(base, row) {
    return { id: row.id, conversationId: row.conversation_id, author: { accountId: row.author_account_id, label: row.author_label },
      body: row.body, replyTo: row.reply_to_id, createdAt: row.created_at, removedAt: row.removed_at,
      canRemove: row.removed_at === null && (row.author_account_id === base.actor.accountId || base.app.owner_account_id === base.actor.accountId) };
  }
  function scope(base, kind, conversationId) {
    return hash(JSON.stringify([kind, base.actor.accountId, base.actor.deviceId, base.app.id, base.entry, conversationId ?? null, base.administrative]));
  }
  function cursor(base, kind, conversationId, state) {
    const iv = randomBytes(12), cipher = createCipheriv('aes-256-gcm', cursorKey, iv);
    const encoded = Buffer.from(JSON.stringify({ scope: scope(base, kind, conversationId), at: timestamp(now()), state }), 'utf8');
    assertApps(encoded.length <= 512, 'apps_discussion_cursor_invalid', 500);
    // Encryption alone leaks plaintext length (including the number of digits
    // in a hidden generation). All cursor kinds use one fixed-size block.
    const block = Buffer.alloc(512, 0x20); encoded.copy(block);
    const encrypted = Buffer.concat([cipher.update(block), cipher.final()]);
    return `${instance}.${Buffer.concat([iv, encrypted, cipher.getAuthTag()]).toString('base64url')}`;
  }
  function readCursor(token, base, kind, conversationId) {
    assertApps(typeof token === 'string' && token.length <= 2048 && /^[a-f0-9]{16}\.[A-Za-z0-9_-]+$/u.test(token), 'invalid_discussion_cursor');
    const [owner, payload] = token.split('.');
    if (owner !== instance) return null;
    let decoded;
    try {
      const raw = Buffer.from(payload, 'base64url');
      assertApps(raw.length === 540 && raw.toString('base64url') === payload, 'invalid_discussion_cursor');
      const decipher = createDecipheriv('aes-256-gcm', cursorKey, raw.subarray(0, 12)); decipher.setAuthTag(raw.subarray(-16));
      decoded = JSON.parse(Buffer.concat([decipher.update(raw.subarray(12, -16)), decipher.final()]).toString('utf8'));
    } catch { throw new AppsError('invalid_discussion_cursor'); }
    assertApps(decoded?.scope === scope(base, kind, conversationId) && Number.isSafeInteger(decoded.at), 'invalid_discussion_cursor');
    const at = timestamp(now());
    if (at < decoded.at || at - decoded.at > CURSOR_TTL) return null;
    return decoded.state;
  }
  function history(base, row, input) {
    const context = contextView(base, row), limit = input.limit ?? 30;
    const state = input.cursor === undefined ? { upper: row.message_seq, before: row.message_seq + 1 }
      : readCursor(input.cursor, base, 'history', row.id);
    if (!state) return { context, messages: [], nextCursor: null, resetRequired: true };
    assertApps(Number.isSafeInteger(state.upper) && state.upper >= 0 && state.upper <= row.message_seq
      && Number.isSafeInteger(state.before) && state.before >= 1 && state.before <= state.upper + 1, 'invalid_discussion_cursor');
    const rows = db.prepare('SELECT * FROM app_discussion_messages WHERE conversation_id=? AND seq<=? AND seq<? ORDER BY seq DESC LIMIT ?')
      .all(row.id, state.upper, state.before, limit + 1);
    const messages = []; let nextCursor = null;
    for (let i = 0; i < Math.min(rows.length, limit); i++) {
      const following = i + 1 < rows.length ? cursor(base, 'history', row.id, { upper: state.upper, before: rows[i].seq }) : null;
      const value = messageView(base, rows[i]);
      if (bytes({ context, messages: [value, ...messages], nextCursor: following, resetRequired: false }) > RESPONSE_BYTES - 2048) {
        assertApps(messages.length, 'apps_discussion_response_too_large', 500);
        nextCursor = cursor(base, 'history', row.id, { upper: state.upper, before: rows[i - 1].seq }); break;
      }
      messages.unshift(value); nextCursor = following;
    }
    return { context, messages, nextCursor, resetRequired: false };
  }
  function initial(actor, input) {
    const base = access(actor, input, input.conversationId === undefined);
    const row = selected(base, input.conversationId ?? base.head?.current_id);
    const value = history(base, row, {});
    return { context: value.context, messages: value.messages, historyCursor: value.nextCursor,
      changeCursor: cursor(base, 'changes', row.id, { after: row.change_seq }) };
  }
  function changes(base, row, input) {
    const context = contextView(base, row), state = readCursor(input.cursor, base, 'changes', row.id);
    const reset = () => ({ context, changes: [], nextCursor: null, hasMore: false, resetRequired: true });
    if (!state) return reset();
    assertApps(Number.isSafeInteger(state.after) && state.after >= 0 && state.after <= row.change_seq, 'invalid_discussion_cursor');
    const earliest = db.prepare('SELECT min(seq) AS n FROM app_discussion_changes WHERE conversation_id=?').get(row.id).n;
    if (earliest !== null && state.after < earliest - 1) return reset();
    const rows = db.prepare(`SELECT c.seq AS event_seq,c.kind,m.* FROM app_discussion_changes c
      JOIN app_discussion_messages m ON m.conversation_id=c.conversation_id AND m.id=c.message_id
      WHERE c.conversation_id=? AND c.seq>? ORDER BY c.seq LIMIT ?`).all(row.id, state.after, input.limit + 1);
    const values = []; let after = state.after, hasMore = false;
    for (let i = 0; i < Math.min(rows.length, input.limit); i++) {
      const value = { type: rows[i].removed_at === null ? 'message' : 'removed', message: messageView(base, rows[i]) };
      const nextCursor = cursor(base, 'changes', row.id, { after: rows[i].event_seq });
      if (bytes({ context, changes: [...values, value], nextCursor, hasMore: i + 1 < rows.length, resetRequired: false }) > RESPONSE_BYTES) {
        assertApps(values.length, 'apps_discussion_response_too_large', 500); hasMore = true; break;
      }
      values.push(value); after = rows[i].event_seq; hasMore = i + 1 < rows.length;
    }
    return { context, changes: values, nextCursor: cursor(base, 'changes', row.id, { after }), hasMore, resetRequired: false };
  }
  function archives(actor, input) {
    const base = access(actor, input), head = base.head;
    if (!head) return { entries: [], nextCursor: null, resetRequired: false };
    const state = input.cursor === undefined ? { upper: head.generation - 1, before: head.generation }
      : readCursor(input.cursor, base, 'archives');
    if (!state) return { entries: [], nextCursor: null, resetRequired: true };
    assertApps(Number.isSafeInteger(state.upper) && state.upper >= 0 && state.upper < head.generation
      && Number.isSafeInteger(state.before) && state.before >= 1 && state.before <= state.upper + 1, 'invalid_discussion_cursor');
    const all = db.prepare('SELECT * FROM app_discussion_conversations WHERE app_id=? ORDER BY generation DESC LIMIT ?')
      .all(input.appId, DISCUSSION_LIMITS.conversationsPerApp + 1);
    assertApps(all.length <= DISCUSSION_LIMITS.conversationsPerApp, 'apps_registry_corrupt', 500);
    // Filter the entire bounded audience set before pagination. Hidden rows
    // never produce an empty continuation or a numeric gap in the response.
    const allowed = authority(base, [...all, currentConversation(base)]);
    const visible = all.filter(row => row.id !== head.current_id && row.generation <= state.upper && row.generation < state.before && allowed(row));
    const entries = []; let nextCursor = null;
    for (let i = 0; i < Math.min(visible.length, input.limit); i++) {
      const value = contextView(base, visible[i]);
      const following = i + 1 < visible.length ? cursor(base, 'archives', undefined, { upper: state.upper, before: visible[i].generation }) : null;
      if (bytes({ entries: [...entries, value], nextCursor: following, resetRequired: false }) > RESPONSE_BYTES) {
        assertApps(entries.length, 'apps_discussion_response_too_large', 500);
        nextCursor = cursor(base, 'archives', undefined, { upper: state.upper, before: visible[i - 1].generation }); break;
      }
      entries.push(value); nextCursor = following;
    }
    return { entries, nextCursor, resetRequired: false };
  }
  function prune(conversationId, latest) {
    db.prepare('DELETE FROM app_discussion_changes WHERE conversation_id=? AND seq<=?').run(conversationId, latest - limits.changesRetained);
  }
  function replyView(actor, input, row, replayed) {
    let message = null;
    try { const base = access(actor, input); selected(base, row.conversation_id); message = messageView(base, row); }
    catch (error) { if (!(error instanceof AppsError) || error.code !== 'app_unavailable') throw error; }
    return { requestId: input.requestId, replayed, receipt: { id: row.id, conversationId: row.conversation_id, createdAt: row.created_at },
      ownCurrent: { removed: row.removed_at !== null }, message };
  }
  function send(actor, input) {
    const requestKey = hash(input.requestId), fingerprint = hash(JSON.stringify(['soty.app-discussion.send.v1', input.appId,
      input.conversationId, input.domainId, input.path, input.body, input.replyTo ?? null]));
    const prior = db.prepare('SELECT * FROM app_discussion_messages WHERE author_account_id=? AND request_key=?').get(actor.accountId, requestKey);
    if (prior) { assertApps(prior.intent_hash === fingerprint, 'apps_discussion_request_conflict', 409); return replyView(actor, input, prior, true); }
    const base = access(actor, input);
    assertApps(base.head?.current_id === input.conversationId, 'apps_discussion_changed', 409);
    const conversation = selected(base, input.conversationId), before = usage(), size = Buffer.byteLength(input.body, 'utf8');
    if (input.replyTo !== undefined) assertApps(db.prepare('SELECT 1 FROM app_discussion_messages WHERE conversation_id=? AND id=?')
      .get(input.conversationId, input.replyTo), 'invalid_discussion_reply');
    const materialize = conversation.message_seq === 0;
    assertApps(before.message_count < limits.messages && base.head.message_count < limits.messagesPerApp
      && before.body_bytes + size <= limits.bodyBytes && base.head.body_bytes + size <= limits.bodyBytesPerApp
      && (!materialize || (before.conversation_count < limits.conversations && base.head.conversation_count < limits.conversationsPerApp)), 'apps_discussion_capacity', 409);
    const at = timestamp(now()), rate = db.prepare('SELECT * FROM app_discussion_rates WHERE account_id=?').get(actor.accountId);
    assertApps(at >= base.head.rate_at && at >= base.head.updated_at && (!rate || at >= rate.at), 'apps_discussion_clock_invalid', 500);
    const accountCredit = rate ? Math.min(20000, rate.credit + Math.min(20000, at - rate.at)) : 20000;
    const appCredit = Math.min(15000, base.head.rate_credit + Math.min(15000, at - base.head.rate_at));
    assertApps(accountCredit >= 2000 && appCredit >= 250, 'apps_discussion_rate_limited', 429);
    const label = synchronous(authorLabel(actor), 'apps_async_authority');
    assertApps(typeof label === 'string' && label === label.trim() && label.length > 0 && label.length <= 80 && label.isWellFormed()
      && !/[\u0000-\u001f\u007f]/u.test(label), 'apps_discussion_author_invalid', 500);
    const seq = conversation.message_seq + 1, change = conversation.change_seq + 1, id = freshId('msg');
    if (materialize) db.prepare('INSERT INTO app_discussion_conversations VALUES (?,?,?,?,?,?,?,?,?,?)')
      .run(conversation.id, input.appId, base.head.generation, base.head.owner_account_id, base.head.mode, base.head.grants_json, base.head.audience_hash, at, seq, change);
    else db.prepare('UPDATE app_discussion_conversations SET message_seq=?,change_seq=? WHERE id=?').run(seq, change, conversation.id);
    db.prepare('INSERT INTO app_discussion_messages VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)')
      .run(id, input.appId, conversation.id, seq, actor.accountId, label, requestKey, fingerprint, input.body, size, input.replyTo ?? null, at, null, null);
    db.prepare('INSERT INTO app_discussion_changes VALUES (?,?,?,?,?)').run(conversation.id, change, id, 'message', at);
    db.prepare(`UPDATE app_discussion_heads SET message_count=message_count+1,body_bytes=body_bytes+?,conversation_count=conversation_count+?,rate_at=?,rate_credit=? WHERE app_id=?`)
      .run(size, materialize ? 1 : 0, at, appCredit - 250, input.appId);
    db.prepare('UPDATE app_discussion_usage SET message_count=message_count+1,body_bytes=body_bytes+?,conversation_count=conversation_count+? WHERE id=1')
      .run(size, materialize ? 1 : 0);
    db.prepare('INSERT INTO app_discussion_rates VALUES (?,?,?) ON CONFLICT(account_id) DO UPDATE SET at=excluded.at,credit=excluded.credit')
      .run(actor.accountId, at, accountCredit - 2000);
    prune(conversation.id, change);
    return replyView(actor, input, db.prepare('SELECT * FROM app_discussion_messages WHERE id=?').get(id), false);
  }
  function remove(actor, input) {
    const row = db.prepare('SELECT * FROM app_discussion_messages WHERE app_id=? AND conversation_id=? AND id=?')
      .get(input.appId, input.conversationId, input.messageId);
    const owner = db.prepare('SELECT owner_account_id FROM local_apps WHERE id=?').get(input.appId)?.owner_account_id;
    assertApps(row && (row.author_account_id === actor.accountId || owner === actor.accountId), 'app_unavailable', 404);
    if (row.removed_at === null) {
      const at = Math.max(timestamp(now()), row.created_at);
      const conversation = db.prepare('SELECT * FROM app_discussion_conversations WHERE id=?').get(row.conversation_id), change = conversation.change_seq + 1;
      db.prepare('UPDATE app_discussion_messages SET body=NULL,body_bytes=0,removed_at=?,removed_by=? WHERE id=?').run(at, actor.accountId, row.id);
      db.prepare('UPDATE app_discussion_conversations SET change_seq=? WHERE id=?').run(change, row.conversation_id);
      db.prepare('INSERT INTO app_discussion_changes VALUES (?,?,?,?,?)').run(row.conversation_id, change, row.id, 'removed', at);
      db.prepare('UPDATE app_discussion_heads SET body_bytes=body_bytes-? WHERE app_id=?').run(row.body_bytes, input.appId);
      db.prepare('UPDATE app_discussion_usage SET body_bytes=body_bytes-? WHERE id=1').run(row.body_bytes);
      prune(row.conversation_id, change);
    }
    return { id: row.id, conversationId: row.conversation_id, removed: true };
  }
  function normalize(op, args) {
    const action = op.slice('apps.discussion.'.length);
    const common = ['appId', 'domainId', 'path', 'administrative'];
    exact(args, action === 'remove' ? ['appId', 'conversationId', 'messageId']
      : action === 'send' ? ['appId', 'conversationId', 'domainId', 'path', 'requestId', 'body', 'replyTo']
        : [...common, ...(action === 'archives' ? [] : ['conversationId']), ...(action === 'context' ? [] : ['cursor', 'limit'])]);
    const input = { appId: appId(args.appId) };
    if (action === 'remove') return { ...input, conversationId: identifier(args.conversationId, 'conv'), messageId: identifier(args.messageId, 'msg') };
    if (Object.hasOwn(args, 'administrative')) assertApps(args.administrative === true, 'invalid_discussion_mode');
    if (args.administrative === true) {
      assertApps(!Object.hasOwn(args, 'domainId') && !Object.hasOwn(args, 'path'), 'unexpected_argument'); input.administrative = true;
    } else {
      if (args.domainId !== undefined) input.domainId = identifier(args.domainId, 'dom');
      if (args.path !== undefined) input.path = createLaunchPath(args.path).entryPath;
      if (action !== 'context') assertApps(input.domainId !== undefined && input.path !== undefined, 'invalid_discussion_entry');
    }
    if (action !== 'archives' && (action !== 'context' || args.conversationId !== undefined)) input.conversationId = identifier(args.conversationId, 'conv');
    if (action === 'send') return { ...input, requestId: textId(args.requestId), body: bodyText(args.body),
      ...(args.replyTo === undefined ? {} : { replyTo: identifier(args.replyTo, 'msg') }) };
    if (action !== 'context') {
      input.limit = pageLimit(args.limit, action === 'changes' ? 100 : action === 'archives' ? 20 : 30, action === 'changes' ? 100 : 50);
      if (args.cursor !== undefined) { assertApps(typeof args.cursor === 'string' && args.cursor.length <= 2048, 'invalid_discussion_cursor'); input.cursor = args.cursor; }
      if (action === 'changes') assertApps(input.cursor !== undefined, 'invalid_discussion_cursor');
    }
    return input;
  }
  return Object.freeze({ execute({ op, args = {}, actor }) {
    assertApps(!closed, 'apps_discussion_closed', 503); assertApps(discussionOperations.has(op), 'unknown_operation');
    const input = normalize(op, args);
    return run(actor, captured => {
      assertApps(!closed, 'apps_discussion_closed', 503);
      if (op === 'apps.discussion.context') return initial(captured, input);
      if (op === 'apps.discussion.archives') return archives(captured, input);
      if (op === 'apps.discussion.send') return send(captured, input);
      if (op === 'apps.discussion.remove') return remove(captured, input);
      const base = access(captured, input), row = selected(base, input.conversationId);
      return op === 'apps.discussion.history' ? history(base, row, input) : changes(base, row, input);
    });
  }, close() { closed = true; cursorKey.fill(0); } });
}
