import test from 'node:test';
import assert from 'node:assert/strict';
import { deflateSync } from 'node:zlib';
import { createSourceNativeAuthorityPort, createNativeAuthorityRuntime } from '../server/native-authority.mjs';
import { createSourceFeedbackClient } from '../browser/feedback-client.mjs';
import { feedbackOutput, SOURCE_FEEDBACK_LIMITS } from '../shared/feedback-wire.mjs';
import { validateSourceFeedbackInput } from '../server/feedback-wire.mjs';
import { STANDARD_SELECTED_SOURCE, STANDARD_SELECTED_SOURCE_V2, STANDARD_SELECTED_SOURCE_V3,
  STANDARD_SOURCE_CONTRACT, STANDARD_SOURCE_CONTRACT_2, STANDARD_SOURCE_CONTRACT_3, standardSourceContract } from '../server/standard-profile.mjs';
import { sourceEmbedRoute } from '../server/router.mjs';
import { selectedRoute, selectedClientRedirect } from '../../apps/scoped-embed/resource-route-adapters.mjs';
import { createOrdinaryHttpFixture } from './support/ordinary-http.mjs';

// Valid synthetic PNG and worst escaped conversation reproduce the independent
// capacity counter. They carry no user media, authority or network credentials.
const table = Uint32Array.from({ length: 256 }, (_, n) => { let c = n; for (let j = 0; j < 8; j++) c = (c >>> 1) ^ ((c & 1) ? 0xedb88320 : 0); return c >>> 0; });
function crc(bytes) { let c = 0xffffffff; for (const b of bytes) c = (c >>> 8) ^ table[(c ^ b) & 255]; return (c ^ 0xffffffff) >>> 0; }
function chunk(type, body) { const t = Buffer.from(type), out = Buffer.alloc(body.length + 12); out.writeUInt32BE(body.length); t.copy(out, 4); body.copy(out, 8); out.writeUInt32BE(crc(Buffer.concat([t, body])), body.length + 8); return out; }
function png(total) {
  const h = Buffer.alloc(13); h.writeUInt32BE(1); h.writeUInt32BE(1, 4); h[8] = 8; h[9] = 6;
  const sig = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]), ihdr = chunk('IHDR', h), idat = chunk('IDAT', deflateSync(Buffer.from([0, 0, 0, 0, 255]))), end = chunk('IEND', Buffer.alloc(0));
  const n = total - Buffer.concat([sig, ihdr, idat, end]).length - 12, text = Buffer.alloc(n, 65); Buffer.from('Fixture\0').copy(text);
  return Buffer.concat([sig, ihdr, chunk('tEXt', text), idat, end]);
}
const media = bytes => ({ kind: 'image', name: 'synthetic.png', mimeType: 'image/png', dataBase64: png(bytes).toString('base64') });
function messages() {
  const rows = Array.from({ length: 12 }, (_, i) => ({ id: 'message-' + i, kind: i % 2 ? 'support' : 'reporter', body: '\\'.repeat(8000), createdAt: 1 }));
  let lo = 1, hi = 8000, best;
  while (lo <= hi) { const n = Math.floor((lo + hi) / 2), next = [...rows, { id: 'message-last', kind: 'support', body: '\\'.repeat(n), createdAt: 1 }];
    if (Buffer.byteLength(JSON.stringify(next)) <= 196608) { best = next; lo = n + 1; } else hi = n - 1; }
  return best;
}
const image = media(1048576), input = { requestId: 'synthetic-report-000001', body: 'Synthetic report', attachments: [image] };
const large = { ticket: { id: 'synthetic-ticket', revision: 1, status: 'received', body: input.body, createdAt: 1, updatedAt: 1,
  canReply: true, canManage: false, canAccept: false, attachments: [{ ...image, byteLength: 1048576, width: 1, height: 1 }], messages: messages() } };
const envelope = { ok: true, data: large };
function runtime(pin, output = large) {
  const state = { active: true, calls: 0 }, proof = Object.freeze({ fixture: true });
  const read = () => { state.calls++; return output; };
  const port = createSourceNativeAuthorityPort({ capture: () => proof, withCurrent: (native, _binding, action) => { assert.equal(native, proof); if (!state.active) throw new Error('revoked'); return action(); },
    feedback: { context: read, list: read, get: read, submit: (_p, _b, _i, commit) => commit.commit(read) } });
  return { state, runtime: createNativeAuthorityRuntime(port, { sourceProfile: pin }) };
}
const profile = pin => ({ schema: 'soty.selected-human-embed.v2', appId: 'app-' + 'a'.repeat(32), connector: { linkId: 'link', hostDeviceId: 'device', connectorId: 'connector' },
  target: { revision: 1, digest: '1'.repeat(64) }, sourceProfile: pin, resource: { registryId: 'soty', environmentId: 'fixture', tenantId: 'root-owner', appId: 'app-' + 'a'.repeat(32), resourceId: 'native:locator', selection: { kind: 'soty.resource.v1', nativeId: 'Case/A', incarnationId: 'scope-uuid' } },
  issuer: 'https://root.test/human-identity', clientId: 'standard.selected', embedOrigin: 'https://source.root.test', nativeOrigin: 'https://native.test', parentOrigin: 'https://root.test' });

test('standard3 changes exactly the detail GET budget; standard1/2 digest and every other rule remain fixed', () => {
  assert.equal(STANDARD_SELECTED_SOURCE.digest, 'b646578a1cbace022bdcc44147e0ad56b4e9d6239250726c4bb2decc4e4de721');
  assert.equal(STANDARD_SELECTED_SOURCE_V2.digest, 'f68eee3d56a1da3269e8586783a457c4eef24e2673088dbd4951cdf0008e2b84');
  assert.notEqual(STANDARD_SELECTED_SOURCE_V3.digest, STANDARD_SELECTED_SOURCE_V2.digest);
  const restored = structuredClone(STANDARD_SOURCE_CONTRACT_3); restored.rules.find(r => r.path === '/api/embed/feedback/ticket').responseBytes = 1500000;
  assert.deepEqual(restored, STANDARD_SOURCE_CONTRACT_2); assert.ok(Object.isFrozen(STANDARD_SOURCE_CONTRACT_3.rules));
  for (const old of [STANDARD_SOURCE_CONTRACT, STANDARD_SOURCE_CONTRACT_2]) assert.equal(old.rules.find(r => r.path === '/api/embed/feedback/ticket').responseBytes, 1500000);
});
test('real media parser accepts the full PNG and wire accepts the bounded escaped conversation', () => {
  assert.equal(validateSourceFeedbackInput('submit', input).attachments.length, 1);
  assert.equal(feedbackOutput('get', large).ticket.messages.length, 13);
  assert.equal(Buffer.byteLength(JSON.stringify(large.ticket.messages)), 196608);
  assert.equal(Buffer.byteLength(JSON.stringify(envelope)), 1595055); assert.ok(Buffer.byteLength(JSON.stringify(input)) < 1500000);
  assert.equal(SOURCE_FEEDBACK_LIMITS.totalAttachmentBytes, 1048576);
});
test('actual runtime standard1/2 still rejects lawful large detail; standard3 returns it under current proof', async () => {
  for (const pin of [STANDARD_SELECTED_SOURCE, STANDARD_SELECTED_SOURCE_V2, STANDARD_SELECTED_SOURCE_V3]) {
    const f = runtime(pin); try { const proof = await f.runtime.capture({ fixture: 'host-stub' }, null);
      if (pin.version < 3) await assert.rejects(f.runtime.feedback(proof, 'get', { ticketId: 'synthetic-ticket' }), e => e.code === 'source_app_payload_limit' && e.status === 413);
      else assert.deepEqual(await f.runtime.feedback(proof, 'get', { ticketId: 'synthetic-ticket' }), large);
      assert.equal(f.state.calls, 1);
    } finally { f.runtime.close(); }
  }
});
test('standard3 never widens list/context/mutation output and refuses detail over 2MiB', async () => {
  for (const operation of ['context', 'list', 'submit', 'get']) {
    const f = runtime(STANDARD_SELECTED_SOURCE_V3, operation === 'get' ? { padding: 'A'.repeat(2097152) } : large);
    try { const proof = await f.runtime.capture({}, null); await assert.rejects(f.runtime.feedback(proof, operation, {}), e => operation === 'submit' ? e.code === 'source_app_effect_unknown' : e.code === 'source_app_payload_limit'); }
    finally { f.runtime.close(); }
  }
});
test('fresh Native revoke denies before get and the new byte budget supplies no authority', async () => {
  const f = runtime(STANDARD_SELECTED_SOURCE_V3); try { const proof = await f.runtime.capture({}, null); f.state.active = false;
    await assert.rejects(f.runtime.feedback(proof, 'get', {}), /revoked/); assert.equal(f.state.calls, 0); }
  finally { f.runtime.close(); }
});
test('unknown pin, added byte option and accessor cannot choose a wider Native contract', () => {
  assert.throws(() => standardSourceContract({ ...STANDARD_SELECTED_SOURCE_V3, digest: 'f'.repeat(64) }));
  assert.throws(() => standardSourceContract({ ...STANDARD_SELECTED_SOURCE_V3, version: 4 }));
  let reads = 0; assert.throws(() => standardSourceContract({ get id() { reads++; return 'soty.standard-resource'; }, version: 3, digest: STANDARD_SELECTED_SOURCE_V3.digest })); assert.equal(reads, 0);
  const port = createSourceNativeAuthorityPort({ capture: () => { reads++; return {}; }, withCurrent: (_p, _b, action) => action() });
  assert.throws(() => createNativeAuthorityRuntime(port, { sourceProfile: STANDARD_SELECTED_SOURCE_V3, responseBytes: 4000000 })); assert.equal(reads, 0);
});
test('browser standard3 reads lawful detail; defaults and versions1/2 keep the old bound', async () => {
  for (const version of [undefined, 1, 2, 3]) {
    let calls = 0; const c = createSourceFeedbackClient({ ...(version ? { profileVersion: version } : {}), fetch: async () => { calls++; return new Response(JSON.stringify(envelope), { headers: { 'content-type': 'application/json' } }); } });
    if (version === 3) assert.deepEqual(await c.get('synthetic-ticket'), large);
    else await assert.rejects(c.get('synthetic-ticket'), e => e.code === 'source_feedback_response_invalid'); assert.equal(calls, 1);
  }
});
test('browser GET-only cap rejects oversized detail and does not widen list response', async () => {
  for (const operation of ['get', 'list']) { const c = createSourceFeedbackClient({ profileVersion: 3, fetch: async () => new Response(JSON.stringify(operation === 'get' ? { padding: 'A'.repeat(2097152) } : envelope), { headers: { 'content-type': 'application/json' } }) });
    await assert.rejects(operation === 'get' ? c.get('synthetic-ticket') : c.list(), e => e.code === 'source_feedback_response_invalid'); }
  for (const profileVersion of [null, 0, 4, '3', NaN]) assert.throws(() => createSourceFeedbackClient({ profileVersion }));
  let reads = 0; assert.throws(() => createSourceFeedbackClient({ get profileVersion() { reads++; return 3; } })); assert.equal(reads, 0);
});
test('media or conversation beyond its own cap still fails under version3', () => {
  assert.throws(() => validateSourceFeedbackInput('submit', { ...input, attachments: [media(1048577)] }));
  const tooMuch = structuredClone(large); tooMuch.ticket.messages.push({ id: 'extra', kind: 'support', body: 'AA', createdAt: 1 }); assert.throws(() => feedbackOutput('get', tooMuch));
});
test('Source router and Root compiled adapter agree for all3; old callback behavior and body budgets stay fixed', () => {
  const path = '/api/embed/feedback/ticket?ticketId=synthetic-ticket';
  for (const pin of [STANDARD_SELECTED_SOURCE, STANDARD_SELECTED_SOURCE_V2, STANDARD_SELECTED_SOURCE_V3]) {
    const source = sourceEmbedRoute({ url: path, method: 'GET' }, new URL(path, 'https://fixed.test'), pin), root = selectedRoute(profile(pin), 'GET', path);
    assert.equal(source.responseBytes, pin.version === 3 ? 2097152 : 1500000); assert.equal(source.responseBytes, root.responseBytes);
    assert.equal(selectedRoute(profile(pin), 'POST', '/api/embed/feedback').requestBytes, 1500000);
    assert.equal(selectedClientRedirect(profile(pin)), pin.version === 1 ? 'https://native.test/soty/callback' : 'https://source.root.test/api/embed/callback');
  }
  assert.throws(() => sourceEmbedRoute({ url: path, method: 'GET' }, new URL(path, 'https://fixed.test'), { ...STANDARD_SELECTED_SOURCE_V3, version: 4 }));
});

test('actual standard3 maintained OIDC/BFF reads the full allowed Native ticket after restart; Native revoke denies', { timeout: 30000 }, async t => {
  const f = await createOrdinaryHttpFixture(t, { embedOidc: true, standardVersion3: true }), [realm] = f.realms;
  const path = '/api/embed/feedback/ticket?ticketId=synthetic-ticket';
  assert.equal((await realm.request(path)).status, 401);
  const authorized = await realm.authorizeNative(await realm.beginNative());
  await realm.finishNative((await realm.completeOidc(authorized)).callback);
  const created = await realm.request('/api/embed/feedback', { method: 'POST', data: input });
  assert.equal(created.status, 200); assert.equal(created.value.data.ticket.attachments[0].dataBase64, undefined);
  const ticketId = created.value.data.ticket.id;
  // Only this new synthetic fixture is seeded. The real route subsequently
  // proves current Root, RP and Native scope and reads its persisted media.
  realm.store.tx(() => { for (const row of large.ticket.messages)
    realm.store.db.prepare('INSERT INTO native_ticket_messages VALUES(?,?,?,?,?)').run(row.id, ticketId, row.kind, row.body, row.createdAt); });
  realm.restart();
  const read = await realm.request('/api/embed/feedback/ticket?ticketId=' + ticketId);
  assert.equal(read.status, 200); assert.ok(Buffer.byteLength(read.text) > 1500000 && Buffer.byteLength(read.text) < 2097152);
  assert.equal(read.value.data.ticket.attachments[0].dataBase64, image.dataBase64);
  assert.deepEqual(read.value.data.ticket.messages, [...large.ticket.messages].sort((a, b) => a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  realm.store.revokeMembership('selected', 'native-participant');
  assert.equal((await realm.request('/api/embed/feedback/ticket?ticketId=' + ticketId)).status, 403);
});
