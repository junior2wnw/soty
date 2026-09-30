import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { createDiscussionRegistry, syncDiscussionAudienceInTransaction, DISCUSSION_LIMITS } from '../server/discussions.mjs';
import { migrateAppsSchema, ensureCanonicalDomain, ensureInitialPublication } from '../server/schema.mjs';
import { AppsError, cleanGrants } from '../server/protocol.mjs';

const owner = { accountId: 'owner', deviceId: 'owner_browser', label: 'Owner' };
const reader = { accountId: 'reader', deviceId: 'reader_browser', label: 'Reader' };
const template = 'https://{appId}.discussion.example';
const id = number => `app-${number.toString(16).padStart(32, '0')}`;
const call = (registry, op, args, actor = owner) => registry.execute({ op: `apps.discussion.${op}`, args, actor });
const code = expected => error => error.code === expected;
const tables = ['app_discussion_heads', 'app_discussion_conversations', 'app_discussion_messages', 'app_discussion_changes', 'app_discussion_usage', 'app_discussion_rates'];
const data = db => Object.fromEntries(tables.map(table => [table, db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all()]));

function fixture(t, options = {}) {
  const db = new DatabaseSync(':memory:'); db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=1234');
  migrateAppsSchema(db, { legacyTemplate: template }); t.after(() => db.close());
  let clock = 1000, serial = 0, active = true, reads = 0, lastCandidates = [];
  const memberships = new Set();
  const canUse = (actor, app) => actor.accountId === app.owner_account_id || JSON.parse(app.grants_json).accountIds.includes(actor.accountId)
    || JSON.parse(app.grants_json).communityIds.some(value => memberships.has(`${actor.accountId}:${value}`));
  function registry(extra = {}) {
    return createDiscussionRegistry({ db, now: () => clock,
      assertActor: actor => { if (!active || ![owner.accountId, reader.accountId].includes(actor.accountId)) throw new AppsError('apps_authentication_required', 401); },
      withAuthorityFence: fn => fn(), canUse, authorLabel: actor => actor.label ?? 'Участник',
      readCommunityAuthority: (actor, _owner, candidates) => { reads++; lastCandidates = [...candidates]; return candidates.filter(value => memberships.has(`${actor.accountId}:${value}`)); },
      resolveEntry: input => {
        assert.equal(db.isTransaction, true);
        const app = db.prepare('SELECT * FROM local_apps WHERE id=?').get(input.appId);
        const domain = input.domainId ? db.prepare('SELECT * FROM app_domains WHERE id=? AND app_id=?').get(input.domainId, input.appId)
          : db.prepare("SELECT * FROM app_domains WHERE app_id=? AND role='canonical'").get(input.appId);
        const policy = db.prepare('SELECT launch_policy FROM app_publications WHERE app_id=?').get(input.appId);
        if (!app || app.state !== 'enabled' || domain?.state !== 'bound' || (!canUse(input.actor, app) && policy.launch_policy !== 'anyone')) return null;
        return { appId: input.appId, domainId: domain.id, origin: domain.origin, path: input.path ?? app.entry_path };
      }, ...options, ...extra });
  }
  const model = registry();
  function seed(n = 1) {
    db.exec('BEGIN IMMEDIATE');
    try {
      db.prepare('INSERT OR IGNORE INTO app_devices VALUES (?,?,?,?,?)').run('link|host|connector', owner.accountId,
        '{"linkId":"link","hostDeviceId":"host","connectorId":"connector"}', 'Source', 1);
      db.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)').run(id(n), owner.accountId, 'link|host|connector', 'App', 8000 + n, '/#/board',
        '{"accountIds":["reader"],"communityIds":[]}', 'enabled', 1, 1, 1);
      const row = db.prepare('SELECT * FROM local_apps WHERE id=?').get(id(n)); ensureCanonicalDomain(db, row, template); ensureInitialPublication(db, row);
      db.exec('COMMIT');
    } catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
    return { appId: id(n), domainId: db.prepare('SELECT id FROM app_domains WHERE app_id=?').get(id(n)).id, path: '/thread#entry' };
  }
  function rotate(scope, grants, mode = 'restricted') {
    db.exec('BEGIN IMMEDIATE');
    try {
      db.prepare('UPDATE local_apps SET grants_json=? WHERE id=?').run(JSON.stringify(cleanGrants(grants)), scope.appId);
      db.prepare('UPDATE app_publications SET launch_policy=?,listed=0 WHERE app_id=?').run(mode, scope.appId);
      syncDiscussionAudienceInTransaction(db, scope.appId, clock); db.exec('COMMIT');
    } catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
  }
  function post(scope, body = 'message', actor = owner, extra = {}) {
    const context = call(model, 'context', scope, actor).context;
    const args = { ...scope, conversationId: context.conversationId, requestId: `request_${++serial}`, body, ...extra };
    return { args, result: call(model, 'send', args, actor) };
  }
  return { db, model, registry, seed, rotate, post, memberships, readCount: () => reads, candidates: () => lastCandidates,
    clock(value) { clock = value; }, advance(ms = 2500) { clock += ms; }, active(value) { active = value; } };
}

test('lazy lineage replaces empty heads; source/name changes keep the conversation and materialization waits for first send', t => {
  const f = fixture(t), scope = f.seed();
  assert.equal(data(f.db).app_discussion_heads.length, 0);
  f.rotate(scope, { accountIds: [reader.accountId, 'next'] }); assert.equal(data(f.db).app_discussion_heads.length, 0);
  const first = call(f.model, 'context', scope).context.conversationId;
  for (let i = 0; i < 80; i++) f.rotate(scope, { accountIds: [reader.accountId, `next_${i}`] });
  const current = call(f.model, 'context', scope).context.conversationId;
  assert.notEqual(current, first); assert.equal(data(f.db).app_discussion_heads.length, 1);
  assert.equal(data(f.db).app_discussion_conversations.length, 0);
  f.db.exec('BEGIN'); f.db.prepare('UPDATE local_apps SET name=? WHERE id=?').run('New name', scope.appId);
  assert.equal(syncDiscussionAudienceInTransaction(f.db, scope.appId, 1000), false); f.db.exec('COMMIT');
  assert.equal(call(f.model, 'context', scope).context.conversationId, current);
  const accepted = f.post(scope, 'first'); assert.equal(accepted.result.receipt.conversationId, current);
  assert.equal(data(f.db).app_discussion_conversations.length, 1);
  assert.equal(migrateAppsSchema(f.db, { legacyTemplate: template }).migrated, false);
});

test('strict shapes, unsafe launch paths and malformed Unicode do not create heads, rates or messages', t => {
  const f = fixture(t), scope = f.seed(), conversationId = call(f.model, 'context', scope).context.conversationId;
  const args = { ...scope, conversationId, requestId: 'post', body: 'hello' }, before = data(f.db);
  for (const patch of [{ appId: [scope.appId] }, { domainId: [scope.domainId] }, { conversationId: [conversationId] }, { requestId: ['post'] },
    { body: ['hello'] }, { body: '\ud800' }, { body: 'x'.repeat(4001) }, { body: '  \n' }, { body: 'bad\u0000text' },
    { path: '/_soty/session' }, { path: '/界'.repeat(2000) }, { replyTo: ['msg_' + 'a'.repeat(32)] }, { administrative: true }]) {
    assert.throws(() => call(f.model, 'send', { ...args, ...patch })); assert.deepEqual(data(f.db), before);
  }
  assert.throws(() => call(f.model, 'context', { ...scope, administrative: true }), code('unexpected_argument'));
  const valid = f.post(scope, 'Привет\n<literal> 😀'); assert.equal(valid.result.message.body, 'Привет\n<literal> 😀');
});

test('first-send transaction failure rolls back counters, materialized audience, request fingerprint and both buckets', t => {
  const f = fixture(t), scope = f.seed(), conversationId = call(f.model, 'context', scope).context.conversationId;
  const intent = { ...scope, conversationId, requestId: 'unknown_ack', body: 'committed once' }, before = data(f.db);
  f.db.exec("CREATE TEMP TRIGGER fail_change BEFORE INSERT ON app_discussion_changes BEGIN SELECT RAISE(ABORT,'change_write_failed'); END");
  assert.throws(() => call(f.model, 'send', intent), /change_write_failed/u); assert.deepEqual(data(f.db), before);
  assert.equal(f.db.prepare('PRAGMA busy_timeout').get().timeout, 1234); f.db.exec('DROP TRIGGER fail_change');
  const first = call(f.model, 'send', intent); assert.equal(first.replayed, false);
  assert.deepEqual(call(f.model, 'send', intent).receipt, first.receipt);
});

test('redaction and changes pruning failure roll back text, usage and tombstone before exact retry', t => {
  const f = fixture(t, { limits: { changesRetained: 1 } }), scope = f.seed(), sent = f.post(scope, 'private text');
  const args = { appId: scope.appId, conversationId: sent.result.receipt.conversationId, messageId: sent.result.receipt.id }, before = data(f.db);
  f.db.exec("CREATE TEMP TRIGGER fail_prune BEFORE DELETE ON app_discussion_changes BEGIN SELECT RAISE(ABORT,'prune_failed'); END");
  assert.throws(() => call(f.model, 'remove', args), /prune_failed/u); assert.deepEqual(data(f.db), before);
  f.db.exec('DROP TRIGGER fail_prune'); call(f.model, 'remove', args); const removed = data(f.db);
  assert.equal(removed.app_discussion_messages[0].body, null); assert.equal(removed.app_discussion_usage[0].body_bytes, 0);
  assert.equal(removed.app_discussion_usage[0].message_count, 1);
  call(f.model, 'remove', args); assert.deepEqual(data(f.db), removed);
  assert.equal(migrateAppsSchema(f.db, { legacyTemplate: template }).migrated, false);
});

test('account and app rate buckets count only committed first sends; exact retry and removal never consume credit', t => {
  const f = fixture(t), scope = f.seed(); let sent;
  for (let i = 0; i < 10; i++) sent = f.post(scope, `burst ${i}`);
  const before = data(f.db);
  assert.throws(() => f.post(scope, '11'), code('apps_discussion_rate_limited')); assert.deepEqual(data(f.db), before);
  call(f.model, 'send', sent.args);
  call(f.model, 'remove', { appId: scope.appId, conversationId: sent.result.receipt.conversationId, messageId: sent.result.receipt.id });
  assert.deepEqual(data(f.db).app_discussion_rates, before.app_discussion_rates);
  f.advance(1999); assert.throws(() => f.post(scope), code('apps_discussion_rate_limited'));
  f.advance(1); assert.equal(f.post(scope).result.replayed, false);
  f.clock(1); assert.throws(() => f.post(scope), code('apps_discussion_clock_invalid'));
  assert.equal(call(f.model, 'send', sent.args).replayed, true);
});

test('history snapshot holds its original upper bound while later creates and old deletions remain in changes', t => {
  const f = fixture(t), scope = f.seed(); const ids = [];
  for (let i = 0; i < 40; i++) { f.advance(); ids.push(f.post(scope, `message ${i}`).result.receipt.id); }
  const initial = call(f.model, 'context', scope); assert.equal(initial.messages.length, 30);
  f.advance(); f.post(scope, 'later');
  call(f.model, 'remove', { appId: scope.appId, conversationId: initial.context.conversationId, messageId: ids[0] });
  const older = call(f.model, 'history', { ...scope, conversationId: initial.context.conversationId, cursor: initial.historyCursor });
  assert.equal(older.messages.length, 10); assert.equal(older.messages[0].body, null); assert.equal(older.nextCursor, null);
  const changed = call(f.model, 'changes', { ...scope, conversationId: initial.context.conversationId, cursor: initial.changeCursor });
  assert.deepEqual(changed.changes.map(value => value.type), ['message', 'removed']); assert.equal(changed.changes[1].message.id, ids[0]);
  const foreign = { ...owner, deviceId: 'different_device' };
  assert.throws(() => call(f.model, 'history', { ...scope, conversationId: initial.context.conversationId, cursor: initial.historyCursor }, foreign), code('invalid_discussion_cursor'));
  f.advance(3600001);
  assert.equal(call(f.model, 'history', { ...scope, conversationId: initial.context.conversationId, cursor: initial.historyCursor }).resetRequired, true);
});

test('read cursor and async authority are never trusted admission; closed/missing/async fences cannot add data', t => {
  const f = fixture(t), scope = f.seed(), before = data(f.db);
  for (const fence of [undefined, async fn => fn(), fn => Promise.resolve(fn())]) {
    const model = f.registry({ withAuthorityFence: fence });
    assert.throws(() => call(model, 'archives', scope), error => ['apps_authority_fence_required', 'apps_authority_fence_invalid'].includes(error.code));
    assert.deepEqual(data(f.db), before);
  }
  const later = f.registry({ resolveEntry: async () => ({}) });
  assert.throws(() => call(later, 'context', scope), code('apps_async_authority')); assert.deepEqual(data(f.db), before);
  f.model.close(); assert.throws(() => call(f.model, 'context', scope), code('apps_discussion_closed'));
  f.active(false); assert.throws(() => call(f.registry(), 'remove', { appId: scope.appId, conversationId: 'conv_' + 'a'.repeat(32), messageId: 'msg_' + 'b'.repeat(32) }), code('apps_authentication_required'));
});

test('all pilot capacities restrict admission only; accepted send and empty rotation survive lower limits', t => {
  const f = fixture(t, { limits: { heads: 1, conversations: 1, conversationsPerApp: 1, messages: 2, messagesPerApp: 2, bodyBytes: 2, bodyBytesPerApp: 2 } });
  const scope = f.seed(), second = f.seed(2), a = f.post(scope, 'a'); f.advance(); f.post(scope, 'b');
  assert.throws(() => call(f.model, 'context', second), code('apps_discussion_capacity'));
  assert.throws(() => f.post(scope, 'c'), code('apps_discussion_capacity'));
  call(f.model, 'remove', { appId: scope.appId, conversationId: a.result.receipt.conversationId, messageId: a.result.receipt.id });
  assert.throws(() => f.post(scope, 'c'), code('apps_discussion_capacity')); // tombstones remain in message admission count
  f.rotate(scope, { accountIds: [reader.accountId, 'new'] });
  assert.notEqual(call(f.model, 'context', scope).context.conversationId, a.result.receipt.conversationId);
  assert.equal(call(f.model, 'send', a.args).replayed, true);
  assert.equal(call(f.model, 'archives', scope).entries.length, 1);
  assert.equal(migrateAppsSchema(f.db, { legacyTemplate: template }).migrated, false);
});

test('bounded archive pass uses exactly one relevant-community authority callback for 1000 full predicates', t => {
  const f = fixture(t), scope = f.seed();
  for (let n = 0; n < DISCUSSION_LIMITS.conversationsPerApp; n++) {
    const ids = Array.from({ length: 64 }, (_, i) => `group_${n}_${i}`);
    f.rotate(scope, { accountIds: [reader.accountId], communityIds: ids }); f.advance(); f.post(scope, `archive ${n}`);
  }
  f.rotate(scope, { accountIds: [reader.accountId], communityIds: Array.from({ length: 64 }, (_, i) => `latest_${i}`) });
  const before = f.readCount(), start = performance.now();
  const archives = call(f.model, 'archives', scope, reader);
  const elapsed = performance.now() - start;
  assert.equal(archives.entries.length, 20); assert.ok(archives.nextCursor);
  assert.equal(f.readCount() - before, 1); assert.equal(f.candidates().length, 64064);
  assert.equal(JSON.stringify(archives).includes('group_'), false);
  t.diagnostic(`1000 x 64 predicates plus current64; one authority callback; ${elapsed.toFixed(1)}ms local SQLite/JS (host join measured separately)`);
});
