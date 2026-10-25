import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync, rmSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve, sep } from 'node:path';
import { createDiscussionRegistry, syncDiscussionAudienceInTransaction } from '../server/discussions.mjs';
import { createDiscussionReadStore } from '../server/discussion-reads.mjs';
import { migrateAppsSchema, ensureCanonicalDomain, ensureInitialPublication } from '../server/schema.mjs';
import { AppsError } from '../server/protocol.mjs';

const owner = { accountId: 'owner', deviceId: 'owner_browser', label: 'Owner' };
const reader = { accountId: 'reader', deviceId: 'reader_browser', label: 'Reader' };
const stranger = { accountId: 'stranger', deviceId: 'stranger_browser' };
const id = number => `app-${number.toString(16).padStart(32, '0')}`;
const call = (model, op, args, actor = owner) => model.execute({ op: 'apps.discussion.' + op, args, actor });
const code = expected => error => error.code === expected;

function temporary(t) {
  const parent = resolve(tmpdir()), directory = mkdtempSync(join(parent, 'soty-discussion-inbox-'));
  return { directory, cleanup() { assert(directory.startsWith(parent + sep)); assert(/^soty-discussion-inbox-/u.test(directory.slice(parent.length + 1))); rmSync(directory, { recursive: true, force: true }); } };
}

function fixture(t, { filename = ':memory:' } = {}) {
  const db = new DatabaseSync(':memory:'); db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=1234');
  const template = 'https://{appId}.inbox.test'; migrateAppsSchema(db, { legacyTemplate: template });
  let clock = 1000, serial = 0, active = true;
  const models = [], canUse = (actor, app) => actor.accountId === app.owner_account_id || JSON.parse(app.grants_json).accountIds.includes(actor.accountId);
  function registry() {
    const model = createDiscussionRegistry({ db, now: () => clock,
      assertActor: actor => { if (!active || !['owner', 'reader', 'stranger'].includes(actor.accountId)) throw new AppsError('apps_authentication_required', 401); },
      withAuthorityFence: fn => fn(), canUse, authorLabel: actor => actor.label ?? 'Участник',
      readStore: createDiscussionReadStore({ filename, now: () => clock }),
      resolveEntry: input => {
        assert.equal(db.isTransaction, true);
        const app = db.prepare('SELECT * FROM local_apps WHERE id=?').get(input.appId);
        const domain = db.prepare('SELECT * FROM app_domains WHERE app_id=? AND id=?').get(input.appId, input.domainId);
        return app && domain && canUse(input.actor, app) ? { appId: app.id, domainId: domain.id, origin: domain.origin, path: input.path } : null;
      } });
    models.push(model); return model;
  }
  const model = registry();
  t.after(() => { for (const item of models) item.close(); db.close(); });
  function seed(n = 1) {
    db.exec('BEGIN');
    try {
      db.prepare('INSERT OR IGNORE INTO app_devices VALUES (?,?,?,?,?)').run('link|host|connector', owner.accountId,
        '{"linkId":"link","hostDeviceId":"host","connectorId":"connector"}', 'Source', 1);
      db.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)').run(id(n), owner.accountId, 'link|host|connector', 'App ' + n, 8100 + n, '/#/board',
        '{"accountIds":["reader"],"communityIds":[]}', 'enabled', 1, 1, 1);
      const app = db.prepare('SELECT * FROM local_apps WHERE id=?').get(id(n)); ensureCanonicalDomain(db, app, template); ensureInitialPublication(db, app);
      db.exec('COMMIT');
    } catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
    return { appId: id(n), domainId: db.prepare('SELECT id FROM app_domains WHERE app_id=?').get(id(n)).id, path: '/thread#exact' };
  }
  function post(entry, body, actor = owner) {
    clock += 2500;
    const context = call(model, 'context', entry, actor).context;
    return call(model, 'send', { ...entry, conversationId: context.conversationId, requestId: 'request_' + ++serial, body }, actor).message;
  }
  function rotate(entry, accounts) {
    db.exec('BEGIN');
    try {
      db.prepare('UPDATE local_apps SET grants_json=? WHERE id=?').run(JSON.stringify({ accountIds: accounts, communityIds: [] }), entry.appId);
      syncDiscussionAudienceInTransaction(db, entry.appId, clock); db.exec('COMMIT');
    } catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
  }
  return { db, model, registry, seed, post, rotate, active(value) { active = value; } };
}

test('inbox reads exact accessible entries without creating conversation heads; duplicate and hidden app ids are neutral', t => {
  const f = fixture(t), entry = f.seed(), other = f.seed(2);
  const result = call(f.model, 'inbox', { entries: [entry, { ...entry }, other] }, reader);
  assert.equal(result.schema, 'soty.app-discussion.inbox.v1'); assert.equal(result.discussions.length, 2);
  assert.equal(result.discussions[0].entry.path, '/thread#exact');
  assert.equal(result.discussions[0].conversationId, null); assert.equal(result.discussions[0].unreadCount, 0);
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM app_discussion_heads').get().n, 0);
  assert.deepEqual(call(f.model, 'inbox', { entries: [entry, { ...entry, appId: id(999) }] }, stranger).discussions, []);
  for (const args of [{ entries: [entry], administrative: true }, { entries: Array(51).fill(entry) }, { entries: [{ ...entry, path: '/_soty/session' }] }, { entries: [{ ...entry, domainId: undefined }] }, { entries: [null] }]) {
    assert.throws(() => call(f.model, 'inbox', args, reader));
  }
  f.active(false); assert.throws(() => call(f.model, 'inbox', { entries: [entry] }, reader), code('apps_authentication_required'));
});

test('read state is monotonic, account scoped and shared by devices; own messages do not become unread', t => {
  const f = fixture(t), entry = f.seed(), a = f.post(entry, 'Первое'), b = f.post(entry, 'Второе');
  f.post(entry, 'Мой ответ', reader);
  const inbox = actor => call(f.model, 'inbox', { entries: [entry] }, actor).discussions[0];
  assert.equal(inbox(reader).unreadCount, 2); assert.equal(inbox(owner).unreadCount, 1);
  const read = message => call(f.model, 'read', { ...entry, conversationId: message.conversationId, messageId: message.id }, reader);
  assert.equal(read(a).unreadCount, 1); assert.equal(read(b).unreadCount, 0); assert.equal(read(a).unreadCount, 0);
  assert.equal(inbox({ ...reader, deviceId: 'reader_tablet' }).unreadCount, 0);
  assert.equal(inbox(owner).unreadCount, 1);
  assert.throws(() => call(f.model, 'read', { ...entry, conversationId: b.conversationId, messageId: b.id }, stranger), code('app_unavailable'));
  assert.throws(() => read({ ...b, id: 'msg_' + 'f'.repeat(32) }), code('app_unavailable'));
});

test('inbox and read recheck current audience and generation; revoked access never exposes an old preview', t => {
  const f = fixture(t), entry = f.seed(), message = f.post(entry, 'Private old preview');
  f.rotate(entry, []);
  assert.deepEqual(call(f.model, 'inbox', { entries: [entry] }, reader).discussions, []);
  assert.throws(() => call(f.model, 'read', { ...entry, conversationId: message.conversationId, messageId: message.id }, reader), code('app_unavailable'));
  f.rotate(entry, ['reader']);
  const current = call(f.model, 'inbox', { entries: [entry] }, reader).discussions[0];
  assert.equal(current.lastMessage, null); assert.equal(current.unreadCount, 0); assert.notEqual(current.conversationId, message.conversationId);
  assert.throws(() => call(f.model, 'read', { ...entry, conversationId: message.conversationId, messageId: message.id }, owner), code('apps_discussion_stale'));
});

test('removed messages do not leak preview or unread and Unicode previews stay well formed', t => {
  const f = fixture(t), entry = f.seed(), a = f.post(entry, 'a' + '😀'.repeat(161)), b = f.post(entry, 'Removed secret');
  call(f.model, 'remove', { appId: entry.appId, conversationId: b.conversationId, messageId: b.id });
  const row = call(f.model, 'inbox', { entries: [entry] }, reader).discussions[0];
  assert.equal(row.lastMessage.id, a.id); assert.equal(row.unreadCount, 1);
  assert.equal(row.lastMessage.preview.isWellFormed(), true); assert.equal(Array.from(row.lastMessage.preview).length, 160);
  assert.equal(JSON.stringify(row).includes('Removed secret'), false);
});

test('read receipt survives close/reopen without changing app registry schema or messages', t => {
  const { directory, cleanup } = temporary(t), filename = join(directory, 'reads.sqlite');
  const f = fixture(t, { filename }), entry = f.seed(), message = f.post(entry, 'Persisted');
  t.after(cleanup);
  call(f.model, 'read', { ...entry, conversationId: message.conversationId, messageId: message.id }, reader);
  const before = f.db.prepare('SELECT * FROM app_discussion_messages').all(); f.model.close();
  const reopened = f.registry(); assert.equal(call(reopened, 'inbox', { entries: [entry] }, reader).discussions[0].unreadCount, 0);
  assert.deepEqual(f.db.prepare('SELECT * FROM app_discussion_messages').all(), before);
  assert.equal(migrateAppsSchema(f.db, { legacyTemplate: 'https://{appId}.inbox.test' }).migrated, false);
});

test('bounded read metadata replaces old generations without evicting another account', () => {
  const store = createDiscussionReadStore({ maxRows: 2, maxPerAccount: 1, now: () => 123 });
  try {
    const a = 'conv_' + 'a'.repeat(32), b = 'conv_' + 'b'.repeat(32);
    store.mark('reader', id(1), a, 4); assert.equal(store.mark('reader', id(1), a, 1).readSequence, 4);
    store.mark('reader', id(1), b, 1); assert.equal(store.get('reader', id(1), a), 0);
    assert.throws(() => store.mark('reader', id(2), a, 1), code('apps_discussion_read_capacity'));
    store.mark('owner', id(1), a, 2);
    assert.throws(() => store.mark('stranger', id(1), a, 1), code('apps_discussion_read_capacity'));
    assert.equal(store.get('reader', id(1), b), 1); assert.equal(store.get('owner', id(1), a), 2);
  } finally { store.close(); }
});

test('an unknown metadata schema is refused without changing its bytes', t => {
  const { directory, cleanup } = temporary(t), filename = join(directory, 'future.sqlite'); t.after(cleanup);
  const db = new DatabaseSync(filename); db.exec("CREATE TABLE future (value TEXT); INSERT INTO future VALUES ('keep')"); db.close();
  const before = readFileSync(filename);
  assert.throws(() => createDiscussionReadStore({ filename }), code('apps_discussion_read_schema_unsupported'));
  assert.deepEqual(readFileSync(filename), before);
});
