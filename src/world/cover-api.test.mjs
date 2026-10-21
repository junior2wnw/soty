import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createCoverApi, coverScope, bytesToBase64, base64Bytes } from './cover-api.mjs';
const accountId = 'account_owner', targetId = 'app_one', now = 1900000000000, requestId = `cover1.${now}.${'1'.repeat(32)}`;
const head = { revision: 2, mode: 'custom', version: 'coverversion_two' };
const scope = kind => ({ storeInstanceId: 'store_real', kind, targetId: kind === 'profile' ? accountId : targetId, ownerAccountId: accountId, targetRevision: 8, policyEpoch: 3, coverRevision: 2 });
const base = { requestId, expectedTargetRevision: 8, expectedPolicyEpoch: 3, expectedCoverRevision: 2 };
const bytes = Uint8Array.from(Buffer.from('524946460800000057454250', 'hex'));
const hash = createHash('sha256').update(bytes).digest('hex');
const committed = { current: { ...head, revision: 3 }, receipt: { action: 'committed', targetId, committedRevision: 3, versionId: 'coverversion_three' }, replayed: false };
const prepared = { current: head, receipt: { action: 'prepared', draftId: 'coverdraft_real' }, draft: { id: 'coverdraft_real', state: 'ready', expiresAt: now + 1000 }, replayed: false };
for (const kind of ['app', 'profile']) test(`SDK maps all nine real ${kind} operations and their image fields`, async () => {
  const id = kind === 'profile' ? accountId : targetId, calls = [];
  const api = { async request(op, args) { calls.push({ op, args });
    if (op.endsWith('.status')) return { current: head, scope: scope(kind), serverTime: now, receipt: null, draft: null };
    if (op.endsWith('.read')) return { current: head, versionId: head.version, image: { mime: 'image/webp', width: 320, height: 320, byteCount: bytes.length, sha256: hash, [kind === 'app' ? 'base64' : 'dataBase64']: bytesToBase64(bytes) } };
    if (op.endsWith('.versions')) return { current: head, versions: [{ id: head.version, version: 2, mode: 'custom', createdAt: now, restorable: true }] };
    if (op.endsWith('.prepare') || op.endsWith('.prepareFromVersion')) return prepared;
    if (op.endsWith('.cancel')) return { current: head, receipt: { action: 'cancelled', draftId: 'coverdraft_real' } };
    return { ...committed, receipt: { ...committed.receipt, targetId: id } };
  } };
  const sdk = createCoverApi({ api, kind, accountId, targetId: id, isCurrent: () => true });
  await sdk.call('status'); await sdk.call('prepare', { ...base, mime: 'image/webp', base64: bytesToBase64(bytes), crop: { x: .5, y: .2, zoom: 2 } });
  await sdk.call('prepareFromVersion', { ...base, versionId: head.version }); await sdk.call('commit', { requestId, draftId: 'coverdraft_real' });
  await sdk.call('restore', { ...base, versionId: head.version }); await sdk.call('remove', { ...base, mode: 'inherit' });
  assert.deepEqual((await sdk.call('read', { width: 320 })).image.bytes, bytes);
  await sdk.call('versions', { limit: 20 }); await sdk.call('cancel', { requestId, draftId: 'coverdraft_real' });
  assert.equal(calls.length, 9); for (const call of calls) { assert.equal(call.args.expectedAccountId, accountId); assert.equal(call.args[kind === 'app' ? 'appId' : 'targetId'], id); assert.equal(call.args[kind === 'app' ? 'targetId' : 'appId'], undefined); }
  const upload = calls[1].args; assert.equal(upload[kind === 'app' ? 'bytesBase64' : 'dataBase64'], bytesToBase64(bytes)); assert.equal(upload.base64, undefined);
});
test('SDK refuses injected args and accessor/non-enumerable required scope without invoking accessors', async () => {
  let calls = 0, getter = 0; const sdk = createCoverApi({ api: { request: async () => { calls++; return {}; } }, kind: 'app', targetId, accountId, isCurrent: () => true });
  for (const args of [{ actor: accountId }, { appId: 'foreign' }, Object.defineProperty({}, 'requestId', { get() { getter++; return requestId; }, enumerable: true })])
    await assert.rejects(() => sdk.call('status', args), { code: 'cover_arguments_invalid' });
  assert.equal(calls, 0); assert.equal(getter, 0);
  for (const descriptor of [{ value: 8, enumerable: false }, { get() { getter++; return 8; }, enumerable: true }]) {
    const value = scope('app'); Object.defineProperty(value, 'targetRevision', descriptor);
    assert.throws(() => coverScope(value, { kind: 'app', targetId, accountId }), { code: 'cover_response_invalid' });
  }
  assert.equal(getter, 0);
});
test('SDK await fence rejects ABA generation and a late digest before returning private bytes', async () => {
  let generation = 1, release; const pending = new Promise(r => release = r);
  const sdk = createCoverApi({ api: { request: () => pending }, kind: 'app', targetId, accountId, isCurrent: () => generation === 1 });
  const request = sdk.call('status'); generation = 3; release({ current: head, scope: scope('app'), serverTime: now, draft: null });
  await assert.rejects(request, { code: 'cover_context_changed' });
  generation = 1; let digestRelease; const digest = new Promise(r => digestRelease = r);
  const read = createCoverApi({ api: { request: async () => ({ current: head, versionId: head.version, image: { mime: 'image/webp', width: 320, height: 320, byteCount: bytes.length, sha256: hash, base64: bytesToBase64(bytes) } }) },
    digest: () => digest, kind: 'app', targetId, accountId, isCurrent: () => generation === 1 }).call('read');
  await Promise.resolve(); generation = 3; digestRelease(createHash('sha256').update(bytes).digest()); await assert.rejects(read, { code: 'cover_context_changed' });
});
test('private image count/hash and foreign commit receipt are independently verified', async () => {
  for (const change of [{ byteCount: bytes.length + 1 }, { sha256: '0'.repeat(64) }, { width: 10000 }, { base64: 'AA==' }]) {
    const sdk = createCoverApi({ api: { request: async () => ({ current: head, versionId: head.version, image: { mime: 'image/webp', width: 320, height: 320, byteCount: bytes.length, sha256: hash, base64: bytesToBase64(bytes), ...change } }) }, kind: 'app', targetId, accountId, isCurrent: () => true });
    await assert.rejects(() => sdk.call('read'), { code: 'cover_image_invalid' });
  }
  const sdk = createCoverApi({ api: { request: async () => ({ ...committed, receipt: { ...committed.receipt, targetId: 'foreign_app' } }) }, kind: 'app', targetId, accountId, isCurrent: () => true });
  await assert.rejects(() => sdk.call('commit', { requestId, draftId: 'draft' }), { code: 'cover_response_invalid' });
  assert.throws(() => base64Bytes('AB=='), { code: 'cover_image_invalid' });
});
