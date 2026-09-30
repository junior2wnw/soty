import test from 'node:test';
import assert from 'node:assert/strict';
import { createAppDiscussionDraftState, dispatchAppDiscussionIntent, listAppDiscussionDrafts, createAppDiscussionFeed } from './app-discussion-state.mjs';

const app = `app-${'a'.repeat(32)}`, domain = `dom_${'b'.repeat(32)}`, conv = `conv_${'c'.repeat(32)}`;
const entry = { appId: app, domainId: domain, origin: 'https://app.example', path: '/board?q=a%2Bb#item' };
const scope = { accountId: 'A', appId: app, conversationId: conv, entry };
const msg = n => `msg_${n.toString(16).padStart(32, '0')}`;
const context = (conversationId = conv, extra = {}) => ({ appId: app, entry, conversationId, mode: 'current', isCurrent: true,
  ownerAdministrative: false, audience: 'shared', canPost: true, canModerate: true, ...extra });
const message = (n, extra = {}) => ({ id: msg(n), conversationId: conv, author: { accountId: 'A', label: 'Author' }, body: `Body ${n}`,
  replyTo: null, createdAt: n, removedAt: null, canRemove: true, ...extra });
const storage = () => { const values = new Map(); return { getItem: key => values.get(key) ?? null, setItem: (key, value) => values.set(key, value) }; };
const locks = () => { let tail = Promise.resolve(); return { request(_key, fn) { const result = tail.then(fn); tail = result.catch(() => {}); return result; } }; };
const deferred = () => { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; };
const code = expected => error => error?.code === expected;
function fixture(t, overrides = {}) {
  const disk = storage(), lock = locks(); let counter = 0;
  const factory = options => { const state = createAppDiscussionDraftState({ scope, storage: disk, locks: lock, randomId: () => `local_${++counter}`, ...overrides, ...options });
    t.after(() => state.dispose()); return state; };
  return { disk, lock, factory, state: factory() };
}
const reply = (pending, extra = {}) => ({ requestId: pending.args.requestId, replayed: false,
  receipt: { id: msg(1), conversationId: pending.scope.conversationId, createdAt: 1 }, ownCurrent: { removed: false },
  message: message(1, { conversationId: pending.scope.conversationId, body: pending.args.body, replyTo: pending.args.replyTo ?? null }), ...extra });
const flushText = async (state, value) => { state.edit({ text: value, replyTo: null }); assert.equal(await state.flush(), true); };

test('discussion lost ACK survives reload with immutable exact entry and resolves after lost read without showing body', async t => {
  const f = fixture(t); await flushText(f.state, 'Private text'); let args;
  await assert.rejects(dispatchAppDiscussionIntent({ state: f.state, expectedDraft: f.state.read().draft, beforeCreate: () => true,
    isCurrent: () => true, api: { async request(_op, value) { args = value; throw new Error('lost_ack'); } } }), /lost_ack/u);
  const reopened = f.factory(), pending = reopened.read().pending;
  const result = await dispatchAppDiscussionIntent({ state: reopened, expectedPending: pending, isCurrent: () => true, api: { async request(_op, value) {
    assert.deepEqual(value, args); return reply(pending, { replayed: true, message: null }); } } });
  assert.equal(result.status, 'accepted'); assert.equal(result.response.message, null); assert.equal(reopened.read().pending, null);
  assert.equal(reopened.read().draft.text, ''); assert.deepEqual(f.state.read().draft.text, '');
});

test('queued prepare captures the clicked draft while a newer local edit and late ACK remain intact', async t => {
  const f = fixture(t), gate = deferred(); await flushText(f.state, 'first');
  const held = f.lock.request('unused', () => gate.promise);
  const work = f.state.prepareSend(f.state.read().draft, () => true);
  f.state.edit({ text: 'second', replyTo: msg(2) }); gate.resolve(); await held;
  const pending = await work; assert.equal(pending.args.body, 'first'); assert.equal(pending.args.replyTo, undefined);
  assert.equal(f.state.read().draft.text, 'second');
  await f.state.acknowledge(pending, reply(pending)); assert.equal(f.state.read().draft.text, 'second'); assert.equal(f.state.read().draft.replyTo, msg(2));
  assert.equal(f.factory().read().draft.text, 'second');
});

test('cross-tab dirty conflict retains mine and remote; explicit keepMine validates the displayed remote revision', async t => {
  const f = fixture(t), other = f.factory();
  f.state.edit({ text: 'Mine', replyTo: null }); other.edit({ text: 'Their first', replyTo: null });
  assert.equal(await other.flush(), true); assert.equal(f.state.read().conflict, true); assert.equal(f.state.read().draft.text, 'Mine');
  const shown = f.state.read().remoteDraft.revision;
  await flushText(other, 'Their newer');
  await assert.rejects(f.state.keepMine(shown), code('app_discussion_draft_changed')); assert.equal(other.read().draft.text, 'Their newer');
  await f.state.keepMine(f.state.read().remoteDraft.revision); assert.equal(f.factory().read().draft.text, 'Mine');
  assert.equal(f.state.read().conflict, false);
});

test('a pending command is not replaced by a different tab or stale rendered retry, including same request id', async t => {
  const f = fixture(t); await flushText(f.state, 'first'); const pending = await f.state.prepareSend(f.state.read().draft, () => true);
  const other = f.factory(); await other.abandon(pending); await flushText(other, 'next');
  const replacement = await other.prepareSend(other.read().draft, () => true); let calls = 0;
  await assert.rejects(dispatchAppDiscussionIntent({ state: f.state, expectedPending: pending, isCurrent: () => true,
    api: { async request() { calls++; } } }), code('app_discussion_pending_changed'));
  assert.equal(calls, 0); assert.deepEqual(other.read().pending, replacement);
  assert.equal(await f.state.acknowledge(pending, reply(pending)), false); assert.deepEqual(other.read().pending, replacement);
});

test('storage denial retains live text, blocks new send, and recovers via explicit flush without truncation', async t => {
  const disk = storage(); let denied = true, calls = 0;
  const f = fixture(t, { storage: { getItem: disk.getItem, setItem(key, value) { if (denied) throw new Error('quota'); disk.setItem(key, value); } } });
  f.state.edit({ text: 'Не потерять 😀', replyTo: null }); assert.equal(await f.state.flush(), false);
  assert.equal(f.state.hasUnsavedChanges(), true); assert.equal(f.state.read().draft.text, 'Не потерять 😀');
  await assert.rejects(dispatchAppDiscussionIntent({ state: f.state, expectedDraft: f.state.read().draft, beforeCreate: () => true,
    isCurrent: () => true, api: { async request() { calls++; } } }), code('app_discussion_storage_unavailable'));
  assert.equal(calls, 0); denied = false; assert.equal(await f.state.flush(), true); assert.equal(f.state.hasUnsavedChanges(), false);
});

test('thirty-two nonempty scopes never evict; the thirty-third remains volatile and retained lookup is exact', async t => {
  const f = fixture(t), states = [];
  for (let i = 0; i < 33; i++) {
    const state = f.factory({ scope: { ...scope, conversationId: `conv_${i.toString(16).padStart(32, '0')}` } }); states.push(state);
    state.edit({ text: `Local ${i}`, replyTo: null }); assert.equal(await state.flush(), i < 32);
  }
  assert.equal(states[32].read().draft.text, 'Local 32'); assert.equal(states[32].hasUnsavedChanges(), true);
  const old = listAppDiscussionDrafts({ accountId: 'A', ...entry, storage: f.disk }); assert.equal(old.length, 32);
  assert.deepEqual(listAppDiscussionDrafts({ accountId: 'B', ...entry, storage: f.disk }), []);
  assert.deepEqual(listAppDiscussionDrafts({ accountId: 'A', ...entry, path: '/different', storage: f.disk }), []);
  const withoutOrigin = listAppDiscussionDrafts({ accountId: 'A', appId: app, domainId: domain, path: entry.path, storage: f.disk });
  assert.equal(withoutOrigin.length, 32); assert.equal(withoutOrigin[0].scope.entry.origin, entry.origin);
  await states[0].discardDraft(states[0].read().draft); assert.equal(await states[32].flush(), true);
});

test('the account byte cap counts UTF-8 and refuses admission without evicting drafts or dispatching', async t => {
  const f = fixture(t), longEntry = { ...entry, path: `/${'x'.repeat(7800)}` }; let blocked = null;
  for (let i = 0; i < 32; i++) {
    const state = f.factory({ scope: { ...scope, entry: longEntry, conversationId: `conv_${i.toString(16).padStart(32, '0')}` } });
    state.edit({ text: '界'.repeat(4000), replyTo: null });
    if (!await state.flush()) { blocked = state; break; }
    try { await state.prepareSend(state.read().draft, () => true); }
    catch (error) { assert.equal(error.code, 'app_discussion_local_capacity'); blocked = state; break; }
  }
  assert.ok(blocked, 'the byte ceiling must be reached before the 32-slot ceiling');
  const before = f.disk.getItem(blocked.key), record = JSON.parse(before);
  assert.ok(record.slots.length < 32); assert.ok(Buffer.byteLength(before, 'utf8') <= 1024 * 1024);
  assert.ok(Buffer.byteLength(before, 'utf8') > before.length); let calls = 0;
  await assert.rejects(dispatchAppDiscussionIntent({ state: blocked, expectedDraft: blocked.read().draft, beforeCreate: () => true,
    isCurrent: () => true, api: { async request() { calls++; } } }), code('app_discussion_local_capacity'));
  assert.equal(calls, 0); assert.equal(f.disk.getItem(blocked.key), before);
  assert.equal(blocked.read().draft.text, '界'.repeat(4000)); assert.equal(blocked.hasUnsavedChanges(), true);
});

test('a failed local ACK write retains the exact pending and the newer draft for explicit recovery', async t => {
  const disk = storage(); let denied = false;
  const shared = { getItem: disk.getItem, setItem(key, value) { if (denied) throw new Error('quota'); disk.setItem(key, value); } };
  const f = fixture(t, { storage: shared }); await flushText(f.state, 'accepted remotely');
  const pending = await f.state.prepareSend(f.state.read().draft, () => true); await flushText(f.state, 'typed after send');
  const before = disk.getItem(f.state.key); denied = true;
  await assert.rejects(f.state.acknowledge(pending, reply(pending)), code('app_discussion_storage_unavailable'));
  assert.equal(disk.getItem(f.state.key), before); assert.equal(f.state.read().draft.text, 'typed after send');
  assert.deepEqual(f.state.read().pending, pending); denied = false;
  const reopened = f.factory(); assert.deepEqual(reopened.read().pending, pending);
  await reopened.acknowledge(pending, reply(pending, { replayed: true }));
  assert.equal(reopened.read().draft.text, 'typed after send'); assert.equal(reopened.read().pending, null);
});

test('missing Web Locks leaves the draft visibly volatile and never starts an untracked send', async t => {
  const f = fixture(t, { locks: undefined }); f.state.edit({ text: 'Keep this text', replyTo: null });
  assert.equal(await f.state.flush(), false); assert.equal(f.state.read().durable, false); let calls = 0;
  await assert.rejects(dispatchAppDiscussionIntent({ state: f.state, expectedDraft: f.state.read().draft, beforeCreate: () => true,
    isCurrent: () => true, api: { async request() { calls++; } } }), code('app_discussion_lock_unavailable'));
  assert.equal(calls, 0); assert.equal(f.state.read().draft.text, 'Keep this text'); assert.equal(f.state.hasUnsavedChanges(), true);
});

test('audience/account/entry scopes remain separate, and a late accepted ACK is hidden from a new screen', async t => {
  const f = fixture(t); await flushText(f.state, 'Original audience'); const gate = deferred(); let current = true, started;
  const work = dispatchAppDiscussionIntent({ state: f.state, expectedDraft: f.state.read().draft, beforeCreate: () => true, isCurrent: () => current,
    api: { request(_op, args) { started = args; return gate.promise; } } });
  while (!started) await new Promise(done => setImmediate(done));
  const oldPending = f.state.read().pending, next = f.factory({ scope: { ...scope, conversationId: `conv_${'d'.repeat(32)}` } });
  await flushText(next, 'New audience'); current = false; gate.resolve(reply(oldPending));
  assert.equal((await work).status, 'stale'); assert.equal(next.read().draft.text, 'New audience');
  assert.equal(f.factory({ scope: { ...scope, accountId: 'B' } }).read().draft.text, '');
  assert.equal(f.factory({ scope: { ...scope, entry: { ...entry, path: '/other' } } }).read().draft.text, '');
});

test('wrong receipt fields and malformed scalar shapes cannot clear pending; missing proof cannot start a new send', async t => {
  const f = fixture(t); await flushText(f.state, 'send'); const pending = await f.state.prepareSend(f.state.read().draft, () => true);
  for (const mutate of [r => { r.requestId = ['wrong']; }, r => { r.receipt.conversationId = `conv_${'0'.repeat(32)}`; },
    r => { r.message.body = 'different'; }, r => { r.message.author.accountId = 'foreign'; }, r => { r.ownCurrent.removed = true; }]) {
    const value = reply(pending); mutate(value); await assert.rejects(f.state.acknowledge(pending, value)); assert.deepEqual(f.state.read().pending, pending);
  }
  await f.state.abandon(pending); await assert.rejects(f.state.prepareSend(f.state.read().draft, () => false), code('app_discussion_context_changed'));
  assert.throws(() => f.state.edit({ text: '\ud800', replyTo: null }));
  assert.throws(() => f.state.edit({ text: 'x'.repeat(4001), replyTo: null }));
  assert.throws(() => f.state.edit({ text: 'okay', replyTo: [msg(1)] }));
});

function feed(t) { const model = createAppDiscussionFeed({ accountId: 'A', appId: app, entry }); t.after(() => model.dispose()); return model; }
const options = value => ({ isCurrent: () => true, api: { async request() { return value; } } });
const initial = (messages = [], extra = {}) => ({ context: context(), messages, historyCursor: 'older', changeCursor: 'changes', ...extra });

test('a feed validates the full entry/context and fences a delayed response across selection changes', async t => {
  const model = feed(t), gate = deferred(); const olderRequest = model.load({ isCurrent: () => true, api: { request: () => gate.promise } });
  const next = `conv_${'d'.repeat(32)}`; await model.load({ ...options(initial([], { context: context(next) })), conversationId: next });
  gate.resolve(initial([message(1)])); assert.equal(await olderRequest, 'stale'); assert.equal(model.read().context.conversationId, next);
  assert.deepEqual(model.read().messages, []);
  await assert.rejects(model.load(options(initial([message(1)], { context: context(conv, { entry: { ...entry, origin: 'https://foreign.example' } }) }))));
  assert.equal(model.read().context, null);
});

test('confirmed removal purges body before a failed refresh and blocks a previously fetched history body', async t => {
  const model = feed(t); await model.load(options(initial([message(1)])));
  const gate = deferred(), pending = model.older({ isCurrent: () => true, api: { request: () => gate.promise } });
  model.acceptRemoval({ id: msg(1), conversationId: conv, removed: true });
  assert.equal(model.read().messages[0].body, null); assert.equal(model.read().messages[0].removedAt, null);
  assert.equal(model.read().messages[0].removalConfirmed, true);
  gate.resolve({ context: context(), messages: [message(1)], nextCursor: null, resetRequired: false });
  assert.equal(await pending, 'stale'); assert.equal(model.read().messages[0].body, null);
  await assert.rejects(model.poll({ isCurrent: () => true, api: { async request() { throw new Error('offline'); } } }));
  assert.equal(model.read().messages[0].body, null);
});

test('initial/read history and changes are separate from drafts; reset never erases local text', async t => {
  const f = fixture(t); await flushText(f.state, 'Unsent'); const model = feed(t);
  await model.load(options(initial([message(1)])));
  const result = await model.poll(options({ context: context(), changes: [], nextCursor: null, hasMore: false, resetRequired: true }));
  assert.equal(result, 'reset'); assert.equal(model.read().messages.length, 0); assert.equal(f.state.read().draft.text, 'Unsent');
});

test('poll is bounded, retains its fetched cursor, applies old deletes and never confuses archive audience with public basis', async t => {
  const model = feed(t); await model.load(options(initial([message(1)]))); let calls = 0;
  await model.poll({ isCurrent: () => true, api: { async request(_op, args) {
    assert.equal(args.cursor, calls ? `next_${calls}` : 'changes'); calls++;
    return { context: context(conv, { mode: 'archive', isCurrent: false, canPost: false }),
      changes: [{ type: 'removed', message: message(1, { body: null, removedAt: 9, canRemove: false }) }], nextCursor: `next_${calls}`, hasMore: true, resetRequired: false };
  } } });
  assert.equal(calls, 4); assert.equal(model.read().changeCursor, 'next_4'); assert.equal(model.read().context.canPost, false);
  assert.equal(model.read().messages[0].body, null);
});

test('bounded history window navigates older without silently evicting focused rows during poll', async t => {
  const model = feed(t); await model.load(options(initial(Array.from({ length: 30 }, (_, i) => message(1000 + i)))));
  for (let page = 0; page < 9; page++) await model.older(options({ context: context(), messages: Array.from({ length: 50 }, (_, i) => message(500 - page * 50 + i)), nextCursor: `old_${page}`, resetRequired: false }));
  assert.equal(model.read().messages.length, 480);
  await model.older(options({ context: context(), messages: Array.from({ length: 50 }, (_, i) => message(1 + i)), nextCursor: 'oldest', resetRequired: false }));
  assert.equal(model.read().messages.length, 50); assert.equal(model.read().window, 'history'); assert.equal(model.read().hasNewer, true);
  const before = model.read().messages.map(row => row.id);
  await model.poll(options({ context: context(), changes: [{ type: 'message', message: message(2000) }], nextCursor: 'newest', hasMore: false, resetRequired: false }));
  assert.deepEqual(model.read().messages.map(row => row.id), before);
  await model.load(options(initial([message(2000)]))); assert.equal(model.read().window, 'latest'); assert.equal(model.read().hasNewer, false);
});

test('oversized/wrong-conversation network pages cannot pollute the scoped feed', async t => {
  const model = feed(t);
  await assert.rejects(model.load(options(initial([message(1, { conversationId: `conv_${'e'.repeat(32)}` })]))));
  await assert.rejects(model.load(options({ ...initial(), unexpected: 'x'.repeat(256 * 1024) })), code('app_discussion_invalid_page'));
  assert.deepEqual(model.read().messages, []);
  await model.load(options(initial([message(1)])));
  await assert.rejects(model.poll({ isCurrent: () => true, api: { async request() { throw Object.assign(new Error('denied'), { code: 'app_unavailable' }); } } }));
  assert.equal(model.read().context, null); assert.deepEqual(model.read().messages, []);
});

test('authority denial from archives fences an already fetched message page, and vice versa', async t => {
  for (const deniedKind of ['archives', 'poll']) {
    const model = feed(t); await model.load(options(initial([message(1)])));
    const gate = deferred();
    const delayed = deniedKind === 'archives'
      ? model.older({ isCurrent: () => true, api: { request: () => gate.promise } })
      : model.archives({ isCurrent: () => true, api: { request: () => gate.promise } });
    await assert.rejects(model[deniedKind]({ isCurrent: () => true, api: { async request() {
      throw Object.assign(new Error('identity changed'), { code: 'authentication_required' });
    } } }), code('authentication_required'));
    gate.resolve(deniedKind === 'archives'
      ? { context: context(), messages: [message(0)], nextCursor: null, resetRequired: false }
      : { entries: [context(`conv_${'d'.repeat(32)}`, { mode: 'archive', isCurrent: false, canPost: false })], nextCursor: null, resetRequired: false });
    assert.equal(await delayed, 'stale'); assert.equal(model.read().context, null);
    assert.deepEqual(model.read().messages, []); assert.deepEqual(model.read().archives, []);
    assert.equal(model.read().loading, false);
  }
});

test('ACK hints clear only after the latest chronological change stream has caught up', async t => {
  const f = fixture(t), model = feed(t); await model.load(options(initial()));
  await flushText(f.state, 'My later message'); const pending = await f.state.prepareSend(f.state.read().draft, () => true);
  const received = reply(pending, { receipt: { id: msg(2), conversationId: conv, createdAt: 2 }, message: message(2, { body: pending.args.body }) });
  model.acceptSend(pending, received);
  assert.deepEqual(model.read().messages, []); assert.equal(model.read().hasNewer, true);
  await model.poll(options({ context: context(), changes: [{ type: 'message', message: message(1) }, { type: 'message', message: received.message }],
    nextCursor: 'caught_up', hasMore: false, resetRequired: false }));
  assert.deepEqual(model.read().messages.map(row => row.id), [msg(1), msg(2)]); assert.equal(model.read().hasNewer, false);
});
