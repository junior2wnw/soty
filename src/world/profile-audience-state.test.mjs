import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, sign } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createProfileAudienceState } from './profile-audience-state.mjs';
import { createConnectService, digestArgs } from '../../modules/connect/server/index.mjs';
import { createWorldService } from '../../modules/world/server/index.mjs';

const profile = (audience = 'private', revision = 4) => ({ profileId: 'synthetic-owner', audience, revision, audienceWriteVersion: 1 });
const deferred = () => { let resolve, reject; const promise = new Promise((a, b) => { resolve = a; reject = b; }); return { promise, resolve, reject }; };
const turn = () => new Promise(resolve => setImmediate(resolve));
const failure = code => Object.assign(new Error(code), { code });
function storagePort() {
  const values = new Map();
  return { values, getItem: key => values.get(key) ?? null, setItem: (key, value) => values.set(key, value), removeItem: key => values.delete(key) };
}
// A deterministic LockManager port. Different controllers share the same FIFO;
// production requests use navigator.locks and do not use this test-only manager.
function lockPort() {
  const tails = new Map();
  return { request(name, callback) {
    const previous = tails.get(name) ?? Promise.resolve(), released = deferred(); tails.set(name, released.promise);
    return previous.then(callback).finally(() => { released.resolve(); if (tails.get(name) === released.promise) tails.delete(name); });
  } };
}
function receiptReply(args, current = profile(args.audience, args.expectedRevision + 1), replayed = false) {
  return { profile: current, replayed, receipt: { schema: 'soty.world-profile-audience.receipt.v1', requestId: args.requestId,
    accountId: args.expectedAccountId, audience: args.audience, expectedRevision: args.expectedRevision, acceptedRevision: args.expectedRevision + 1 } };
}
function harness(request, options = {}) {
  let epoch = 0; const captured = epoch, calls = [], observed = [];
  const storage = options.storage ?? storagePort(), locks = Object.hasOwn(options, 'locks') ? options.locks : lockPort();
  const editor = createProfileAudienceState({ profile: options.profile ?? profile(), storage, locks, isCurrent: () => epoch === captured,
    api: { request(op, args) { calls.push({ op, args: structuredClone(args) }); return request(op, args); } },
    onChange(value) { observed.push(value); options.onChange?.(value); } });
  return { editor, calls, observed, storage, locks, invalidate() { epoch++; }, aba() { epoch += 2; } };
}

// The original nine subjects are retained. The new protocol changes explicit
// Check from readonly get to SAME request-ID replay followed by owner get.
// Unit API ports cover controller validation only; signed SQLite cases below
// separately prove the real server receipts, CAS and restart behavior.
test('audience selection is only a draft; saving uses displayed CAS and a signed owner readback', async () => {
  const h = harness(async (op, args) => op === 'world.profile.update' ? receiptReply(args) : { profile: profile('friends', 5) });
  assert.equal(h.editor.choose('friends'), true); assert.equal(h.calls.length, 0);
  assert.equal(h.editor.read().canSave, true); await h.editor.save();
  const requestId = h.calls[0].args.requestId;
  assert.match(requestId, /^[0-9a-f-]{36}$/u);
  assert.deepEqual(h.calls, [{ op: 'world.profile.update', args: { expectedAccountId: 'synthetic-owner', requestId, expectedRevision: 4, audience: 'friends' } }, { op: 'world.profile.get', args: {} }]);
  assert.equal(h.editor.read().dirty, false); assert.equal(h.editor.read().profile.audience, 'friends');
  assert.equal(h.editor.read().pendingExists, false); assert.equal(h.storage.getItem(h.editor.key), null);
});

test('a lost update response is resolved by exact replay without a fresh write intent', async () => {
  let attempts = 0;
  const h = harness(async (op, args) => { if (op === 'world.profile.update') { if (!attempts++) throw failure('NETWORK_TIMEOUT'); return receiptReply(args, profile('link', 5), true); } return { profile: profile('link', 5) }; });
  h.editor.choose('link'); assert.equal(await h.editor.save(), false);
  assert.equal(h.editor.read().phase, 'unknown'); assert.equal(h.editor.read().pendingExists, true);
  await h.editor.check();
  assert.equal(h.editor.read().phase, 'ready'); assert.equal(h.editor.read().profile.audience, 'link');
  const writes = h.calls.filter(call => call.op === 'world.profile.update');
  assert.equal(writes.length, 2); assert.deepEqual(writes[0].args, writes[1].args);
  assert.equal(h.editor.read().pendingExists, false);
});

test('unknown outcome holds the exact draft and blocks fresh writes until a successful recheck', async () => {
  let available = false;
  const h = harness(async (op, args) => { if (!available) throw failure('NETWORK_TIMEOUT'); return op === 'world.profile.update' ? receiptReply(args, profile('public', 5), true) : { profile: profile('public', 5) }; });
  h.editor.choose('public'); assert.equal(await h.editor.save(), false);
  assert.equal(h.editor.read().phase, 'unknown'); assert.equal(h.editor.choose('private'), false);
  assert.equal(await h.editor.save(), false); assert.equal(h.calls.length, 2);
  const before = h.editor.read().pending;
  available = true; await h.editor.check();
  assert.equal(h.editor.read().phase, 'ready'); assert.equal(h.editor.read().dirty, false);
  const writes = h.calls.filter(call => call.op === 'world.profile.update'); assert.equal(writes.length, 2);
  assert.equal(writes[1].args.requestId, before.requestId); assert.deepEqual(writes[0].args, writes[1].args);
});

test('conflicting server state keeps the draft and requires a new explicit save with fresh CAS', async () => {
  let reads = 0;
  const h = harness(async (op, args) => {
    if (op === 'world.profile.update') { if (!reads) throw failure('revision_conflict'); return receiptReply(args, profile('link', 9)); }
    return { profile: ++reads === 1 ? profile('friends', 8) : profile('link', 9) };
  });
  h.editor.choose('link'); await h.editor.save();
  assert.equal(h.editor.read().phase, 'conflict'); assert.equal(h.editor.read().draft, 'link');
  assert.equal(h.editor.read().profile.audience, 'friends'); assert.equal(h.editor.read().canSave, true);
  await h.editor.save();
  const writes = h.calls.filter(call => call.op === 'world.profile.update');
  assert.deepEqual(writes.map(call => call.args.expectedRevision), [4, 8]); assert.notEqual(writes[0].args.requestId, writes[1].args.requestId);
  assert.equal(h.editor.read().profile.audience, 'link');
});

test('invalid or wrong-actor readback cannot confirm a privacy change', async () => {
  for (const invalid of [profile('friends', 3), { ...profile('friends', 5), profileId: 'other-owner' }, { ...profile('friends', 5), audience: undefined }]) {
    const h = harness(async (op, args) => op === 'world.profile.update' ? receiptReply(args) : { profile: invalid });
    h.editor.choose('friends'); await h.editor.save();
    assert.deepEqual(h.calls.map(call => call.op), ['world.profile.update', 'world.profile.get']);
    assert.equal(h.editor.read().phase, 'unknown'); assert.equal(h.editor.read().profile.revision, 4);
    assert.equal(h.editor.read().canSave, false); assert.equal(h.editor.read().pendingExists, true);
  }
});

test('account change while the write is pending prevents later reads and callbacks', async () => {
  const held = deferred(), h = harness(async () => held.promise);
  h.editor.choose('link'); const saving = h.editor.save(); await turn(); const before = h.observed.length;
  h.invalidate(); held.resolve({}); await assert.rejects(saving, { code: 'ACTIVE_PROFILE_CHANGED' });
  assert.equal(h.calls.length, 1); assert.equal(h.observed.length, before); assert.notEqual(h.storage.getItem(h.editor.key), null);
});

test('account change during readback never adopts a late profile', async () => {
  const held = deferred(), h = harness(async (op, args) => op === 'world.profile.update' ? receiptReply(args) : held.promise);
  h.editor.choose('public'); const saving = h.editor.save(); await turn();
  assert.equal(h.calls.length, 2); const before = h.observed.length;
  h.invalidate(); held.resolve({ profile: profile('public', 5) });
  await assert.rejects(saving, { code: 'ACTIVE_PROFILE_CHANGED' });
  assert.equal(h.editor.read().profile.audience, 'private'); assert.equal(h.observed.length, before); assert.notEqual(h.storage.getItem(h.editor.key), null);
});

test('duplicate save and dispose cannot create a second write or late confirmation', async () => {
  const held = deferred(), h = harness(async () => held.promise);
  h.editor.choose('friends'); const saving = h.editor.save(); await turn(); assert.equal(await h.editor.save(), false);
  assert.equal(h.calls.length, 1); h.editor.dispose(); held.resolve({});
  await assert.rejects(saving, { code: 'ACTIVE_PROFILE_CHANGED' });
  assert.equal(h.editor.read().profile.audience, 'private'); assert.notEqual(h.storage.getItem(h.editor.key), null);
});

test('old servers expose an unsupported control and cannot receive a four-way mutation', async () => {
  for (const old of [{ profileId: 'synthetic-owner', revision: 4, discoverable: true }, { ...profile(), audienceWriteVersion: undefined }]) {
    let calls = 0; const editor = createProfileAudienceState({ profile: old, storage: storagePort(), locks: lockPort(),
      api: { request() { calls++; throw new Error('must not call'); } }, isCurrent: () => true });
    assert.equal(editor.read().phase, 'unsupported'); if (old.discoverable === true) assert.equal(editor.read().draft, 'public');
    assert.equal(editor.choose('private'), false);
    assert.equal(await editor.save(), false); assert.equal(await editor.check(), false); assert.equal(calls, 0);
  }
});

test('intent is durable and read back before dispatch; storage failure or absent locks sends no RPC', async () => {
  for (const mode of ['throw', 'no-op', 'missing-lock']) {
    const storage = storagePort(); if (mode === 'throw') storage.setItem = () => { throw new Error('quota'); };
    if (mode === 'no-op') storage.setItem = () => {};
    const h = harness(async () => { throw new Error('unexpected RPC'); }, { storage, ...(mode === 'missing-lock' ? { locks: null } : {}) });
    h.editor.choose('friends'); assert.equal(await h.editor.save(), false); assert.equal(h.calls.length, 0, mode);
    assert.equal(h.editor.read().canSave, false, mode);
  }
  let h;
  h = harness(async (op, args) => {
    if (op === 'world.profile.update') {
      const value = JSON.parse(h.storage.getItem(h.editor.key)); assert.equal(value.requestId, args.requestId);
      assert.equal(value.accountId, args.expectedAccountId); assert.equal(value.audience, args.audience); return receiptReply(args);
    }
    return { profile: profile('friends', 5) };
  });
  h.editor.choose('friends'); await h.editor.save(); assert.equal(h.editor.read().phase, 'ready');
});

test('corrupt or foreign scoped journal cannot be overwritten or dispatched', async () => {
  for (const value of ['invalid json', JSON.stringify({ schema: 'soty.profile-audience.intent.v1', profileId: 'foreign-owner', accountId: 'foreign-owner', requestId: 'prior-request', expectedRevision: 4, audience: 'public' })]) {
    const storage = storagePort(), key = 'soty.profile-audience.v1:synthetic-owner'; storage.setItem(key, value);
    const h = harness(async () => { throw new Error('unexpected RPC'); }, { storage });
    assert.equal(h.editor.read().phase, 'storage-error'); assert.equal(h.editor.choose('friends'), false);
    assert.equal(await h.editor.save(), false); assert.equal(await h.editor.check(), false);
    assert.equal(h.calls.length, 0); assert.equal(storage.getItem(key), value);
  }
});

test('unchanged read after uncertain dispatch keeps exact pending; no private choice can falsely settle it', async () => {
  const h = harness(async op => { if (op === 'world.profile.update') throw failure('NETWORK_TIMEOUT'); return { profile: profile() }; });
  h.editor.choose('public'); await h.editor.save(); const before = h.editor.read().pending;
  assert.equal(h.editor.read().phase, 'unknown'); assert.equal(h.editor.choose('private'), false); assert.equal(await h.editor.save(), false);
  await h.editor.check(); const writes = h.calls.filter(call => call.op === 'world.profile.update');
  assert.equal(writes.length, 2); assert.deepEqual(writes[0].args, writes[1].args); assert.deepEqual(h.editor.read().pending, before);
});

test('malformed historical receipt cannot release the journal even if current mode matches', async () => {
  for (const field of ['requestId', 'accountId', 'audience', 'expectedRevision', 'acceptedRevision', 'schema']) {
    const h = harness(async (op, args) => {
      assert.equal(op, 'world.profile.update'); const result = receiptReply(args); result.receipt[field] = field.endsWith('Revision') ? 999 : 'wrong'; return result;
    });
    h.editor.choose('friends'); await h.editor.save();
    assert.equal(h.calls.length, 1); assert.equal(h.editor.read().phase, 'unknown'); assert.equal(h.editor.read().pendingExists, true);
  }
});

test('failed cleanup preserves exact request for replay instead of a fresh authority-changing action', async () => {
  const storage = storagePort(), remove = storage.removeItem; let failRemoval = true, attempts = 0;
  storage.removeItem = key => { if (failRemoval) throw new Error('storage unavailable'); remove(key); };
  const h = harness(async (op, args) => op === 'world.profile.update' ? receiptReply(args, profile('friends', 5), attempts++ > 0) : { profile: profile('friends', 5) }, { storage });
  h.editor.choose('friends'); await h.editor.save(); const pending = h.editor.read().pending;
  assert.equal(h.editor.read().phase, 'storage-error'); assert.equal(h.editor.read().canSave, false);
  failRemoval = false; await h.editor.check();
  const writes = h.calls.filter(call => call.op === 'world.profile.update'); assert.equal(writes.length, 2);
  assert.equal(writes[1].args.requestId, pending.requestId); assert.deepEqual(writes[0].args, writes[1].args); assert.equal(h.editor.read().pendingExists, false);
});

test('queued lock admission cannot dispatch after an account ABA generation change', async () => {
  const locks = lockPort(), held = deferred(), h = harness(async () => { throw new Error('unexpected RPC'); }, { locks });
  const blocker = locks.request(h.editor.key, () => held.promise); await turn();
  h.editor.choose('friends'); const saving = h.editor.save(); h.aba(); held.resolve(); await blocker;
  await assert.rejects(saving, { code: 'ACTIVE_PROFILE_CHANGED' });
  assert.equal(h.calls.length, 0); assert.equal(h.storage.getItem(h.editor.key), null);
});

test('storage notification only refreshes the journal and never dispatches a write', () => {
  const storage = storagePort(), h = harness(async () => { throw new Error('unexpected RPC'); }, { storage });
  h.editor.choose('friends'); storage.setItem(h.editor.key, JSON.stringify({ schema: 'soty.profile-audience.intent.v1', accountId: 'synthetic-owner', profileId: 'synthetic-owner', requestId: 'another-window', expectedRevision: 4, audience: 'public' }));
  h.editor.refreshPending(); assert.equal(h.editor.read().pendingExists, true); assert.equal(h.editor.read().draft, 'friends');
  assert.equal(h.editor.read().canSave, false); assert.equal(h.calls.length, 0);
});

test('Check without a pending intent only reads the owner and preserves an edited unsaved choice', async () => {
  const h = harness(async op => { assert.equal(op, 'world.profile.get'); return { profile: profile() }; });
  h.editor.choose('friends'); await h.editor.check();
  assert.deepEqual(h.calls, [{ op: 'world.profile.get', args: {} }]);
  assert.equal(h.editor.read().draft, 'friends'); assert.equal(h.editor.read().canSave, true);
});

test('same-ID payload conflict cannot clear the journal or mint a replacement request', async () => {
  const h = harness(async op => { if (op === 'world.profile.update') throw failure('profile_request_conflict'); return { profile: profile('friends', 7) }; });
  h.editor.choose('public'); await h.editor.save(); const pending = h.editor.read().pending;
  assert.equal(h.editor.read().phase, 'unknown'); assert.equal(await h.editor.save(), false); assert.equal(h.editor.choose('private'), false);
  await h.editor.check(); const writes = h.calls.filter(call => call.op === 'world.profile.update');
  assert.equal(writes.length, 2); assert.deepEqual(writes[0].args, writes[1].args); assert.deepEqual(h.editor.read().pending, pending);
});

test('a stale/disposed owner at the last pending notification cannot dispatch the stored intent', async () => {
  let h;
  h = harness(async () => { throw new Error('unexpected RPC'); }, { onChange(value) {
    if (value.phase === 'saving' && value.pendingExists) h.editor.dispose();
  } });
  h.editor.choose('friends'); await assert.rejects(h.editor.save(), { code: 'ACTIVE_PROFILE_CHANGED' });
  assert.equal(h.calls.length, 0); assert.notEqual(h.storage.getItem(h.editor.key), null);
});

// Real public Connect challenge/P-256 dispatcher and World SQLite. There is no
// HTTP/browser simulation claim: all authority, CAS, effects and receipts below
// are actual public module calls; only timing/response-loss ports are controlled.
async function signedFixture(t) {
  const directory = mkdtempSync(join(tmpdir(), 'soty-profile-editor-')), projectId = 'profile-editor-test', origin = 'https://profile-editor.test';
  let world, connect;
  const open = () => {
    world = createWorldService({ databasePath: join(directory, 'world.sqlite'), projectId,
      withCurrentContacts: (actor, callback) => connect.withCurrentContacts(actor, callback) });
    connect = createConnectService({ databasePath: join(directory, 'connect.sqlite'), projectId, allowedOrigins: [origin], extensions: [world] });
  };
  open(); t.after(() => { connect.close(); world.close(); rmSync(directory, { recursive: true, force: true }); });
  const signing = generateKeyPairSync('ec', { namedCurve: 'prime256v1' }), encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const publicJwk = signing.publicKey.export({ format: 'jwk' });
  async function prepare(op, args = {}) {
    const challenge = await connect.handle({ op: 'challenge', args: { operation: op, digest: digestArgs(args) }, origin });
    assert.equal(challenge.ok, true, challenge.error?.code);
    return { op, args: structuredClone(args), origin, proof: { challengeId: challenge.challengeId, publicJwk,
      signature: sign('sha256', Buffer.from(challenge.message), { key: signing.privateKey, dsaEncoding: 'ieee-p1363' }).toString('base64url') } };
  }
  const send = async input => {
    const result = await connect.handle(input); if (!result.ok) throw failure(result.error.code); return result;
  };
  const call = async (op, args = {}) => send(await prepare(op, args));
  const account = await call('bootstrap', { label: 'Synthetic owner', encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }) });
  const initial = (await call('world.profile.get')).profile;
  assert.equal(initial.profileId, account.accountId); assert.equal(initial.audienceWriteVersion, 1, 'real World v2 feature must be installed, never an inert fake profile');
  const storage = storagePort(), locks = lockPort();
  return { call, prepare, send, storage, locks, initial,
    reopen() { connect.close(); world.close(); open(); },
    editor(api = { request: call }, base = initial, extra = {}) { return createProfileAudienceState({ profile: base, api, storage, locks, isCurrent: () => true, ...extra }); } };
}

test('signed SQLite: storage failure prevents any real World update', async t => {
  const f = await signedFixture(t), storage = storagePort(), calls = [];
  storage.setItem = () => { throw new Error('quota'); };
  const editor = f.editor({ request: async (op, args) => { calls.push(op); return f.call(op, args); } }, f.initial, { storage });
  editor.choose('public'); assert.equal(await editor.save(), false); assert.deepEqual(calls, []);
  const current = (await f.call('world.profile.get')).profile; assert.equal(current.revision, f.initial.revision); assert.equal(current.audience, 'private');
});

test('signed SQLite: lost ACK and database/controller reopen replay one exact persisted intent', async t => {
  const f = await signedFixture(t), writes = []; let drop = true;
  const api = { request: async (op, args) => {
    const reply = await f.call(op, args);
    if (op === 'world.profile.update') { writes.push({ args: structuredClone(args), reply }); if (drop) { drop = false; throw failure('NETWORK_TIMEOUT'); } }
    return reply;
  } };
  const first = f.editor(api); first.choose('public'); assert.equal(await first.save(), false);
  assert.equal(first.read().phase, 'unknown'); assert.equal(first.read().pendingExists, true); const pending = first.read().pending; first.dispose();
  f.reopen(); const current = (await f.call('world.profile.get')).profile;
  const second = f.editor(api, current); assert.equal(second.read().phase, 'unknown'); await second.check();
  assert.equal(writes.length, 2); assert.deepEqual(writes[0].args, writes[1].args);
  assert.equal(writes[1].args.requestId, pending.requestId); assert.equal(writes[0].reply.replayed, false); assert.equal(writes[1].reply.replayed, true);
  assert.deepEqual(writes[0].reply.receipt, writes[1].reply.receipt);
  assert.equal(second.read().pendingExists, false); assert.equal(f.storage.getItem(second.key), null);
  assert.equal(second.read().profile.revision, f.initial.revision + 1); assert.equal(second.read().profile.audience, 'public');
});

test('signed SQLite: same-revision GET cannot settle a signed request held before actual commit', async t => {
  const f = await signedFixture(t), writes = []; let original = null;
  const api = { request: async (op, args) => {
    if (op === 'world.profile.update') {
      writes.push(structuredClone(args));
      if (!original) { original = await f.prepare(op, args); throw failure('NETWORK_TIMEOUT'); }
    }
    return f.call(op, args);
  } };
  const first = f.editor(api); first.choose('public'); assert.equal(await first.save(), false);
  assert.equal(first.read().phase, 'unknown'); assert.equal(first.read().profile.revision, f.initial.revision);
  assert.equal(first.choose('private'), false); assert.equal(await first.save(), false); const pending = first.read().pending;
  const late = await f.send(original); assert.equal(late.replayed, false); assert.equal(late.profile.audience, 'public');
  first.dispose(); const second = f.editor(api, (await f.call('world.profile.get')).profile); await second.check();
  assert.equal(writes.length, 2); assert.deepEqual(writes[0], writes[1]); assert.equal(writes[1].requestId, pending.requestId);
  assert.equal(second.read().phase, 'ready'); assert.equal(second.read().profile.revision, f.initial.revision + 1); assert.equal(second.read().pendingExists, false);
});

test('signed SQLite: competing tabs cannot replace a pending intent and preserve the second unsaved choice', async t => {
  const f = await signedFixture(t), held = deferred(), committed = deferred(), writes = [], replies = [];
  t.after(() => held.resolve());
  let first = true;
  const api = { request: async (op, args) => {
    const reply = await f.call(op, args);
    if (op === 'world.profile.update') {
      writes.push(structuredClone(args)); replies.push(reply);
      if (first) { first = false; committed.resolve(); await held.promise; throw failure('NETWORK_TIMEOUT'); }
    }
    return reply;
  } };
  const a = f.editor(api), b = f.editor(api); a.choose('public'); b.choose('friends');
  const savingA = a.save(); await committed.promise; const pending = JSON.parse(f.storage.getItem(a.key));
  const savingB = b.save(); await turn(); assert.equal(writes.length, 1); assert.equal(JSON.parse(f.storage.getItem(a.key)).requestId, pending.requestId);
  held.resolve(); await savingA; assert.equal(await savingB, false);
  assert.equal(writes.length, 1); assert.equal(b.read().draft, 'friends'); assert.equal(b.read().pendingExists, true);
  await b.check(); assert.equal(writes.length, 2); assert.deepEqual(writes[0], writes[1]); assert.equal(replies[1].replayed, true);
  assert.equal(b.read().pendingExists, false); assert.equal(b.read().draft, 'friends'); assert.equal(b.read().canSave, true);
  await b.save(); assert.equal(writes.length, 3); assert.notEqual(writes[2].requestId, writes[0].requestId);
  assert.equal(b.read().profile.audience, 'friends'); assert.equal(b.read().profile.revision, f.initial.revision + 2);
});


test('signed SQLite: explicit same-ID Check can win before the delayed original and the later proof only replays', async t => {
  const f = await signedFixture(t), writes = []; let original = null;
  const api = { request: async (op, args) => {
    if (op === 'world.profile.update') {
      writes.push(structuredClone(args));
      if (!original) { original = await f.prepare(op, args); throw failure('NETWORK_TIMEOUT'); }
    }
    return f.call(op, args);
  } };
  const editor = f.editor(api); editor.choose('link'); assert.equal(await editor.save(), false);
  assert.equal(editor.read().phase, 'unknown'); assert.equal(editor.read().profile.revision, f.initial.revision);
  const pending = editor.read().pending; await editor.check();
  assert.equal(editor.read().phase, 'ready'); assert.equal(editor.read().pendingExists, false);
  assert.equal(editor.read().profile.audience, 'link'); assert.equal(editor.read().profile.revision, f.initial.revision + 1);
  const late = await f.send(original); assert.equal(late.replayed, true);
  assert.equal(late.receipt.requestId, pending.requestId); assert.equal(late.profile.revision, f.initial.revision + 1);
  assert.equal(writes.length, 2); assert.deepEqual(writes[0], writes[1]);
});


test('journal arguments outside the real World identifier and revision contract never dispatch or erase', async () => {
  const valid = { schema: 'soty.profile-audience.intent.v1', profileId: 'synthetic-owner', accountId: 'synthetic-owner', requestId: 'valid-request', expectedRevision: 4, audience: 'public' };
  for (const mutation of [{ requestId: 'x.y' }, { requestId: 'ab' }, { requestId: 'x'.repeat(161) }, { requestId: 'request:bad' }, { expectedRevision: 0 }, { expectedRevision: -1 }, { expectedRevision: Number.MAX_SAFE_INTEGER }]) {
    const storage = storagePort(), key = 'soty.profile-audience.v1:synthetic-owner', value = JSON.stringify({ ...valid, ...mutation }); storage.setItem(key, value);
    const h = harness(async () => { throw new Error('unexpected RPC'); }, { storage });
    assert.equal(h.editor.read().phase, 'storage-error', JSON.stringify(mutation));
    assert.equal(h.editor.choose('friends'), false); assert.equal(await h.editor.save(), false); assert.equal(await h.editor.check(), false);
    assert.equal(h.calls.length, 0); assert.equal(storage.getItem(key), value);
  }
});
