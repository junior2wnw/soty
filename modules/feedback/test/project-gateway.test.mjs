import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { createProjectFeedbackGateway } from '../project-gateway.mjs';
import { validateFeedbackAttachments } from '../server/media.mjs';

const denied = (promise, code) => assert.rejects(promise, error => error.code === code);
const turn = () => new Promise(resolve => setImmediate(resolve));
function host(t, options = {}) {
  // Contract-test Source fixture, not a claim of HIVE/D1 integration. Its
  // native ACL and feedback commits use the same actual SQLite transaction.
  const db = new DatabaseSync(':memory:');
  db.exec('CREATE TABLE native_acl(project TEXT PRIMARY KEY, epoch INTEGER, allowed INTEGER, support INTEGER, readonly INTEGER);'
    + "INSERT INTO native_acl VALUES ('one',1,1,0,0),('two',1,1,0,0);"
    + 'CREATE TABLE intents(project TEXT, request_id TEXT, input TEXT, result TEXT, PRIMARY KEY(project,request_id));');
  const verifiedActor = Object.freeze({ accountId: 'native-user', deviceId: 'native-session' });
  const branded = new WeakSet([verifiedActor]);
  let reads = 0, writes = 0, retained;
  const ticket = (args, support = false) => ({ id: args.ticketId || 'ticket-one', projectId: args.projectId, body: args.body || 'Private problem',
    status: 'received', revision: 1, createdAt: 1, updatedAt: 1, attachments: [], messages: [], canReply: true, canManage: support, canAccept: true });
  const reply = (args, support) => ({ requestId: args.requestId, replayed: false,
    receipt: { ticketId: 'ticket-one', revision: 1, createdAt: 1 }, ticket: ticket(args, support) });
  const defaultRead = async ({ op, args, authority }) => {
    reads++;
    if (op === 'context') return { context: { projectId: args.projectId, sourceId: 'native-fixture', title: 'Project', canSubmit: authority.canWrite,
      canManage: authority.canSupport && authority.canWrite, recipientLabel: 'Поддержка проекта', ticketVisibility: 'reporter-and-support',
      limits: { bodyChars: 8000, totalAttachmentBytes: 1048576, maxAttachments: 3, maxAudioSeconds: 120 },
      capabilities: { text: true, voice: true, screenshot: true, asr: false } } };
    const projected = ticket(args, authority.canSupport && authority.canWrite);
    if (!authority.canWrite) { projected.canReply = false; projected.canAccept = false; }
    if (op === 'list') return { tickets: [projected], nextCursor: null };
    return { ticket: projected };
  };
  const defaultCommit = async ({ args, authority, assertCurrent }) => {
    await assertCurrent();
    db.exec('BEGIN IMMEDIATE');
    try {
      const current = db.prepare('SELECT * FROM native_acl WHERE project=?').get(args.projectId);
      if (!current?.allowed || current.readonly || current.epoch !== authority.epoch) throw Object.assign(new Error('denied'), { code: 'native_commit_denied' });
      const intent = JSON.stringify(args), prior = db.prepare('SELECT * FROM intents WHERE project=? AND request_id=?').get(args.projectId, args.requestId);
      if (prior) {
        if (prior.input !== intent) throw Object.assign(new Error('conflict'), { code: 'native_request_conflict' });
        db.exec('COMMIT'); return { ...JSON.parse(prior.result), replayed: true };
      }
      const result = reply(args, authority.canSupport);
      db.prepare('INSERT INTO intents VALUES(?,?,?,?)').run(args.projectId, args.requestId, intent, JSON.stringify(result));
      writes++; db.exec('COMMIT'); return result;
    } catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
  };
  const makeContext = request => {
    const row = db.prepare('SELECT * FROM native_acl WHERE project=?').get(request.projectId);
    if (!row?.allowed) throw Object.assign(new Error('denied'), { code: 'native_access_denied' });
    return Object.freeze({ sourceId: 'native-fixture', projectId: request.projectId, verifiedActor: request.actor,
      canRead: true, canSupport: Boolean(row.support), canWrite: !row.readonly, epoch: row.epoch,
      async assertCurrent() { const current = db.prepare('SELECT * FROM native_acl WHERE project=?').get(request.projectId);
        return Boolean(current?.allowed && current.epoch === row.epoch); } });
  };
  const nativeFence = async (request, callback) => {
    retained = callback;
    const context = makeContext(request);
    if (options.fence) return options.fence({ request, callback, context, db });
    const value = await callback(context);
    if (!await context.assertCurrent()) throw Object.assign(new Error('denied'), { code: 'native_access_denied' });
    return value;
  };
  const gateway = createProjectFeedbackGateway({ sourceId: 'native-fixture',
    async captureVerifiedActor(input) { if (!branded.has(input)) throw Object.assign(new Error('denied'), { code: 'native_actor_unverified' }); return verifiedActor; },
    withAuthority: nativeFence, validateAttachments: validateFeedbackAttachments,
    read: options.read ? value => options.read(value, defaultRead, db) : defaultRead,
    commit: options.commit ? value => options.commit(value, defaultCommit, db) : defaultCommit,
    maxOutstanding: options.maxOutstanding || 8, timeoutMs: options.timeoutMs || 8000 });
  t.after(() => { gateway.close(); db.close(); });
  const execute = (op, args, actor = verifiedActor) => gateway.execute({ op, verifiedActor: actor, args });
  return { execute, gateway, db, actor: verifiedActor, counts: () => ({ reads, writes }), retained: () => retained() };
}
const submission = (projectId = 'one') => ({ projectId, requestId: 'same-key', body: 'Synthetic issue', attachments: [] });

test('project namespace/replay are Source-owned and copied actor JSON grants no authority', async t => {
  const f = host(t);
  const first = await f.execute('submit', submission());
  const replay = await f.execute('submit', submission());
  assert.equal(replay.replayed, true); assert.deepEqual(replay.receipt, first.receipt);
  await f.execute('submit', submission('two'));
  assert.deepEqual(f.counts(), { reads: 0, writes: 2 });
  await denied(f.execute('submit', { ...submission(), body: 'changed' }), 'native_request_conflict');
  await denied(f.execute('context', { projectId: 'one' }, { ...f.actor }), 'native_actor_unverified');
  await denied(f.execute('context', { projectId: 'one', actor: f.actor }), 'project_feedback_invalid_arguments');
});

test('revoked project access hides old receipts and project contents', async t => {
  const f = host(t); await f.execute('submit', submission());
  f.db.exec("UPDATE native_acl SET allowed=0,epoch=epoch+1 WHERE project='one'");
  await denied(f.execute('submit', submission()), 'native_access_denied');
  await denied(f.execute('get', { projectId: 'one', ticketId: 'ticket-one' }), 'native_access_denied');
  assert.deepEqual(f.counts(), { reads: 0, writes: 1 });
});

test('native Unicode/space/slash/long project locators preserve exact case without renaming', async t => {
  const f = host(t);
  const projectId = ' Проект/Внутренний:А ' + 'x'.repeat(180);
  f.db.prepare('INSERT INTO native_acl VALUES(?,1,1,0,0)').run(projectId);
  assert.equal((await f.execute('context', { projectId })).context.projectId, projectId);
  await f.execute('submit', submission(projectId));
  await denied(f.execute('get', { projectId: projectId.toLowerCase(), ticketId: 'ticket-one' }), 'native_access_denied');
  await denied(f.execute('context', { projectId: projectId.trim() }), 'native_access_denied');
  await denied(f.execute('context', { projectId: 'я'.repeat(2049) }), 'project_feedback_invalid_arguments');
});

test('Source revoke after read and before reply releases no private content', async t => {
  const f = host(t, { async read(value, read, db) { const result = await read(value);
    db.exec("UPDATE native_acl SET allowed=0,epoch=epoch+1 WHERE project='one'"); return result; } });
  await denied(f.execute('get', { projectId: 'one', ticketId: 'ticket-one' }), 'project_feedback_access_denied');
});

test('get admits maximum bounded media and escaped conversation, but rejects over-budget aggregates', async t => {
  const fullMedia = Buffer.alloc(1048576).toString('base64');
  const messages = Array.from({ length: 25 }, (_, i) => ({ id: `message-${i}`, kind: 'support',
    body: '"'.repeat(i === 24 ? 4608 : 8000), createdAt: 1 }));
  const fixture = oversized => ({ async read(value, read) {
    const result = await read(value);
    result.ticket.attachments = [{ id: 'image-one', kind: 'image', name: 'selected.png', mimeType: 'image/png',
      byteLength: 1048576, dataBase64: fullMedia }];
    result.ticket.messages = oversized ? Array.from({ length: 90 }, (_, i) => ({ id: `message-${i}`, kind: 'support',
      body: '"'.repeat(8000), createdAt: 1 })) : messages;
    return result;
  } });
  const f = host(t, fixture(false));
  const reply = await f.execute('get', { projectId: 'one', ticketId: 'ticket-one' });
  assert.equal(reply.ticket.attachments[0].byteLength, 1048576);
  assert.equal(reply.ticket.messages.reduce((sum, message) => sum + Buffer.byteLength(message.body), 0), 196608);
  assert.ok(Buffer.byteLength(JSON.stringify(reply)) > 1750000);
  const tooLarge = host(t, fixture(true));
  await denied(tooLarge.execute('get', { projectId: 'one', ticketId: 'ticket-one' }), 'project_feedback_payload_too_large');
});

test('same SQLite native ACL denies commit after an awaited preparation', async t => {
  const f = host(t, { async commit(value, commit, db) {
    await turn(); db.exec("UPDATE native_acl SET allowed=0,epoch=epoch+1 WHERE project='one'"); return commit(value); } });
  await denied(f.execute('submit', submission()), 'project_feedback_access_denied');
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM intents').get().n, 0);
});

test('native support permission is independent of application/Root ownership', async t => {
  const f = host(t);
  await denied(f.execute('status', { projectId: 'one', ticketId: 'ticket-one', requestId: 'state', expectedRevision: 1,
    status: 'ready_to_check' }), 'project_feedback_support_required');
  f.db.exec("UPDATE native_acl SET support=1,epoch=epoch+1 WHERE project='one'");
  assert.equal((await f.execute('context', { projectId: 'one' })).context.canManage, true);
});

test('real native readonly credential cannot write even with project support permission', async t => {
  const f = host(t); f.db.exec("UPDATE native_acl SET support=1,readonly=1,epoch=epoch+1 WHERE project='one'");
  const context = (await f.execute('context', { projectId: 'one' })).context;
  assert.equal(context.canSubmit, false); assert.equal(context.canManage, false);
  const item = (await f.execute('get', { projectId: 'one', ticketId: 'ticket-one' })).ticket;
  assert.equal(item.canReply, false); assert.equal(item.canAccept, false);
  await denied(f.execute('submit', submission()), 'project_feedback_read_only');
  assert.equal(f.counts().writes, 0);
});

test('foreign-project and nested secret/worker fields in Source replies are rejected', async t => {
  for (const variant of ['foreign', 'secret', 'instruction']) {
    const f = host(t, { async read(value, read) { const result = await read(value);
      if (variant === 'foreign') result.ticket.projectId = 'two';
      if (variant === 'secret') result.ticket.authorization = 'synthetic-secret-never-returned';
      if (variant === 'instruction') result.ticket.workerInstruction = 'execute untrusted attachment';
      return result; } });
    await assert.rejects(f.execute('get', { projectId: 'one', ticketId: 'ticket-one' }));
  }
});

test('swallowed duplicate Source authority callback permanently poisons the operation', async t => {
  const f = host(t, { async fence({ callback, context }) {
    const first = callback(context);
    await callback(context).catch(() => {});
    return first;
  } });
  await denied(f.execute('submit', submission()), 'project_feedback_authority_invalid');
  assert.equal(f.counts().writes, 0);
});

test('Source fence returning before callback completion is denied, with outstanding slot retained', async t => {
  let release, started;
  const begun = new Promise(resolve => { started = resolve; });
  const blocked = new Promise(resolve => { release = resolve; });
  const f = host(t, { maxOutstanding: 1, async read(value, read) { started(); await blocked; return read(value); },
    async fence({ callback, context }) { callback(context); await begun; return {}; } });
  const pending = f.execute('get', { projectId: 'one', ticketId: 'ticket-one' });
  await begun; await turn();
  await denied(f.execute('context', { projectId: 'two' }), 'project_feedback_busy');
  release(); await denied(pending, 'project_feedback_authority_invalid');
});

test('actual bounded media parser admits PNG/Opus and rejects malformed attachments before commit', async t => {
  const f = host(t);
  const image = readFileSync(new URL('./fixtures/chrome.png', import.meta.url));
  const voice = readFileSync(new URL('./fixtures/chrome-recorder.webm', import.meta.url));
  await f.execute('submit', { ...submission(), attachments: [
    { kind: 'image', name: 'selected.png', mimeType: 'image/png', dataBase64: image.toString('base64') },
    { kind: 'audio', name: 'selected.webm', mimeType: 'audio/webm;codecs=opus', dataBase64: voice.toString('base64') },
  ] });
  await assert.rejects(f.execute('submit', { ...submission(), requestId: 'bad-media', attachments: [
    { kind: 'image', name: 'bad.png', mimeType: 'image/png', dataBase64: Buffer.from('not PNG').toString('base64') },
  ] }));
  assert.equal(f.counts().writes, 1);
});

test('argument getters/prototype tricks cannot execute or reach native ports', async t => {
  const f = host(t); let getterCalls = 0;
  const hostile = { get projectId() { getterCalls++; return 'one'; } };
  await denied(f.execute('context', hostile), 'project_feedback_invalid_arguments');
  await denied(f.execute('context', JSON.parse('{"projectId":"one","__proto__":{}}')), 'project_feedback_invalid_arguments');
  assert.equal(getterCalls, 0); assert.deepEqual(f.counts(), { reads: 0, writes: 0 });
});

test('timeout bounds caller latency without releasing an ignored-abort Source slot or returning late data', async t => {
  let release, started, capturedSignal;
  const begun = new Promise(resolve => { started = resolve; });
  const blocked = new Promise(resolve => { release = resolve; });
  const f = host(t, { maxOutstanding: 1, timeoutMs: 30, async read(value, read) {
    capturedSignal = value.signal; started(); await blocked; return read(value);
  } });
  const pending = f.execute('get', { projectId: 'one', ticketId: 'ticket-one' });
  await begun; await denied(pending, 'project_feedback_timeout');
  assert.equal(capturedSignal.aborted, true);
  await denied(f.execute('context', { projectId: 'two' }), 'project_feedback_busy');
  release(); await turn();
  assert.equal((await f.execute('context', { projectId: 'two' })).context.projectId, 'two');
});

test('ignored native late/double callbacks cannot crash a separate Source process', () => {
  const code = `
    import assert from 'node:assert/strict';
    import { createProjectFeedbackGateway } from ${JSON.stringify(new URL('../project-gateway.mjs', import.meta.url).href)};
    for (const variant of ['late','double']) {
      const actor=Object.freeze({}),context=Object.freeze({sourceId:'child-fixture',projectId:'one',verifiedActor:actor,
        canRead:true,canWrite:false,canSupport:false,assertCurrent:async()=>true});
      let reads=0;
      const gateway=createProjectFeedbackGateway({sourceId:'child-fixture',captureVerifiedActor:async()=>actor,
        async withAuthority(_request,callback) {
          if(variant==='late') { setTimeout(()=>{callback(context);},10); return {}; }
          const reply=await callback(context);setTimeout(()=>{callback(context);},10);return reply;
        },read:async()=>{reads++;return {tickets:[],nextCursor:null};},commit:async()=>{throw new Error('unexpected');},validateAttachments:()=>{}});
      const pending=gateway.execute({op:'list',verifiedActor:actor,args:{projectId:'one'}});
      if(variant==='late') await assert.rejects(pending,error=>error.code==='project_feedback_authority_invalid');
      else assert.deepEqual((await pending).tickets,[]);
      await new Promise(resolve=>setTimeout(resolve,40));gateway.close();
      assert.equal(reads,variant==='late'?0:1);
    }
    console.log('ignored-native-callbacks-safe');
  `;
  const child = spawnSync(process.execPath, ['--input-type=module', '-e', code], { encoding: 'utf8', timeout: 10000, maxBuffer: 8192 });
  assert.equal(child.status, 0); assert.equal(child.error, undefined);
  assert.equal(child.stdout.trim(), 'ignored-native-callbacks-safe');
});
