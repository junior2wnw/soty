import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, sign } from 'node:crypto';
import { createServer } from 'node:http';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { createConnectService, digestArgs } from '../../connect/server/index.mjs';
import { createConnectHandler } from '../../connect/server/http.mjs';
import { createNotesService } from '../server/index.mjs';
const ORIGIN = 'https://notes.test';
function identity(label) {
  const signing = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { label, signing: signing.privateKey, publicJwk: signing.publicKey.export({ format: 'jwk' }), encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }) };
}
function good(result) { assert.equal(result.ok, true, result.error?.code); return result; }
function error(result, code) { assert.equal(result.ok, false); assert.equal(result.error.code, code); }
test('real HTTP/signed accounts and a second installation cannot bypass note ownership, CAS or revocation', async t => {
  const base = resolve(tmpdir()); const directory = mkdtempSync(join(base, 'soty-notes-http-test-'));
  const options = { projectId: 'notes-http-test' };
  const notes = createNotesService({ ...options, databasePath: join(directory, 'notes.sqlite') });
  const connect = createConnectService({ ...options, databasePath: join(directory, 'connect.sqlite'), allowedOrigins: [ORIGIN], extensions: [notes] });
  const handler = createConnectHandler(connect);
  const server = createServer((req, res) => { handler(req, res).catch(() => { res.statusCode = 500; res.end(); }); });
  await new Promise(resolveListen => server.listen(0, '127.0.0.1', resolveListen));
  t.after(async () => { await new Promise(resolveClose => server.close(resolveClose)); connect.close(); notes.close();
    assert.equal(dirname(resolve(directory)), base); assert.ok(directory.startsWith(join(base, 'soty-notes-http-test-'))); rmSync(directory, { recursive: true, force: true }); });
  const endpoint = `http://127.0.0.1:${server.address().port}/api/connect/rpc`;
  const send = async input => {
    const response = await fetch(endpoint, { method: 'POST', headers: { 'Content-Type': 'application/json', Origin: ORIGIN }, body: JSON.stringify({ protocol: 1, ...input }) });
    assert.equal(response.headers.get('cache-control'), 'no-store'); return response.json();
  };
  async function proof(actor, op, args) {
    const challenge = good(await send({ op: 'challenge', args: { operation: op, digest: digestArgs(args) } }));
    return { op, args, proof: { challengeId: challenge.challengeId, publicJwk: actor.publicJwk,
      signature: sign('sha256', Buffer.from(challenge.message), { key: actor.signing, dsaEncoding: 'ieee-p1363' }).toString('base64url') } };
  }
  const call = async (actor, op, args = {}) => send(await proof(actor, op, args));
  const bootstrap = async actor => good(await call(actor, 'bootstrap', { label: actor.label, encryptionPublicJwk: actor.encryptionPublicJwk }));
  const alice = identity('Алиса'), bob = identity('Боб'), phone = identity('Телефон');
  const a = await bootstrap(alice), b = await bootstrap(bob);
  const first = { expectedAccountId: a.accountId, noteId: 'notes_http_first', mutationId: 'notes_http_create', expectedRevision: 0,
    title: 'Только моя', body: 'Два устройства, один аккаунт', items: [], pinned: true, state: 'active', color: 'honey' };
  good(await call(alice, 'notes.put', first));
  error(await call(bob, 'notes.get', { expectedAccountId: b.accountId, noteId: first.noteId }), 'notes_note_not_found');
  error(await call(bob, 'notes.get', { expectedAccountId: a.accountId, noteId: first.noteId }), 'notes_account_changed');
  assert.equal(good(await call(bob, 'notes.list', { expectedAccountId: b.accountId, query: 'устройства' })).notes.length, 0);
  const enrollment = good(await call(phone, 'enrollment.start', { label: phone.label, encryptionPublicJwk: phone.encryptionPublicJwk }));
  good(await call(alice, 'enrollment.approve', { requestId: enrollment.requestId, wrappedKey: { schema: 'test.encrypted', ciphertext: 'opaque-test-data' } }));
  const joined = good(await call(phone, 'enrollment.finish', { requestId: enrollment.requestId, expectedAccountId: a.accountId }));
  assert.equal(good(await call(phone, 'notes.get', { expectedAccountId: a.accountId, noteId: first.noteId })).note.body, first.body);
  const changed = { ...first, mutationId: 'notes_phone_change', expectedRevision: 1, body: 'С телефона' };
  good(await call(phone, 'notes.put', changed));
  error(await call(alice, 'notes.put', { ...first, mutationId: 'notes_stale_change', expectedRevision: 1 }), 'notes_revision_conflict');
  assert.equal(good(await call(alice, 'notes.put', first)).replayed, true);
  const tampered = await proof(alice, 'notes.get', { expectedAccountId: a.accountId, noteId: first.noteId }); tampered.args.noteId = 'notes_other_note';
  error(await send(tampered), 'challenge_mismatch');
  good(await call(alice, 'device.revoke', { deviceId: joined.deviceId }));
  error(await call(phone, 'notes.put', { ...changed, mutationId: 'notes_revoked_try', expectedRevision: 2 }), 'device_revoked');
  assert.equal(good(await call(alice, 'notes.get', { expectedAccountId: a.accountId, noteId: first.noteId })).note.body, 'С телефона');
});
