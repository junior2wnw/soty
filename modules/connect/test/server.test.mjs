import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, sign, randomBytes, createHash } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createConnectService, canonicalJson, digestArgs, deviceIdForKey, SCHEMA_VERSION } from '../server/index.mjs';
import { MIGRATIONS, migrateDatabase } from '../server/schema.mjs';

const ORIGIN = 'https://connect.test';
const OTHER_ORIGIN = 'https://other.test';
const PROJECT = 'connect-test';
test('account database cannot be placed in the replaceable module directory', () => {
  const databasePath = fileURLToPath(new URL('../data/do-not-create.sqlite', import.meta.url));
  assert.throws(() => createConnectService({ databasePath, projectId: PROJECT, allowedOrigins: [ORIGIN] }), { code: 'database_must_be_outside_module' });
  if (process.platform === 'win32') assert.throws(() => createConnectService({ databasePath: databasePath.toUpperCase(), projectId: PROJECT, allowedOrigins: [ORIGIN] }), { code: 'database_must_be_outside_module' });
});
const wrapped = { schema: 'test.encrypted.v1', iv: 'opaque-test-iv', ciphertext: 'opaque-test-ciphertext' };
function identity(name = 'Device') {
  const keys = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { name, privateKey: keys.privateKey, publicJwk: keys.publicKey.export({ format: 'jwk' }), encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }) };
}
function fixture(t, options = {}) {
  const base = resolve(tmpdir());
  const directory = mkdtempSync(join(base, 'soty-connect-test-'));
  const databasePath = join(directory, 'connect.sqlite');
  let time = 1_800_000_000_000;
  const services = [];
  const open = (overrides = {}) => {
    const service = createConnectService({ databasePath, projectId: PROJECT, allowedOrigins: [ORIGIN, OTHER_ORIGIN], clock: () => time, ...options, ...overrides });
    services.push(service); return service;
  };
  t.after(() => {
    for (const service of services) service.close();
    // Cleanup is restricted to the exact temporary child created by this test.
    assert.equal(dirname(resolve(directory)), base);
    assert.ok(resolve(directory).startsWith(join(base, 'soty-connect-test-')));
    rmSync(directory, { recursive: true, force: true });
  });
  return { databasePath, open, service: open(), advance(ms) { time += ms; }, now: () => time };
}
async function signed(service, actor, op, args = {}, origin = ORIGIN) {
  const challenge = await service.handle({ op: 'challenge', args: { operation: op, digest: digestArgs(args) }, origin });
  assert.equal(challenge.ok, true, challenge.error?.code);
  const signature = sign('sha256', Buffer.from(challenge.message, 'utf8'), { key: actor.privateKey, dsaEncoding: 'ieee-p1363' }).toString('base64url');
  return { op, args, origin, proof: { challengeId: challenge.challengeId, publicJwk: actor.publicJwk, signature } };
}
async function invoke(service, actor, op, args = {}, origin = ORIGIN) {
  return service.handle(await signed(service, actor, op, args, origin));
}
async function boot(service, actor) {
  const result = await invoke(service, actor, 'bootstrap', { label: actor.name, encryptionPublicJwk: actor.encryptionPublicJwk });
  assert.equal(result.ok, true, result.error?.code); return result;
}
function expectError(result, code) { assert.equal(result.ok, false); assert.equal(result.error.code, code); }
async function enroll(service, source, target) {
  const started = await invoke(service, target, 'enrollment.start', { label: target.name, encryptionPublicJwk: target.encryptionPublicJwk });
  assert.equal(started.ok, true, started.error?.code);
  const approved = await invoke(service, source, 'enrollment.approve', { requestId: started.requestId, wrappedKey: wrapped });
  assert.equal(approved.ok, true, approved.error?.code);
  const preview = await invoke(service, target, 'enrollment.preview', { requestId: started.requestId });
  const result = await invoke(service, target, 'enrollment.finish', { requestId: started.requestId, expectedAccountId: preview.account.accountId });
  assert.equal(result.ok, true, result.error?.code);
  return { ...result, requestId: started.requestId };
}
async function prepareRecovery(service, actor) {
  const secret = randomBytes(32).toString('base64url');
  const verifier = createHash('sha256').update(secret).digest('base64url');
  const prepared = await invoke(service, actor, 'recovery.set', { verifier, wrappedKey: wrapped });
  assert.equal(prepared.ok, true, prepared.error?.code);
  return { secret, verifier, recoveryId: prepared.recoveryId };
}
async function confirmRecovery(service, actor, recovery) {
  const result = await invoke(service, actor, 'recovery.confirm', { recoveryId: recovery.recoveryId, verifier: recovery.verifier });
  assert.equal(result.ok, true, result.error?.code);
}
function recoveryArgs(account, recovery, actor) {
  return { accountId: account.accountId, recoveryId: recovery.recoveryId, secret: recovery.secret, label: actor.name, encryptionPublicJwk: actor.encryptionPublicJwk };
}

test('canonical JSON is deterministic and rejects ambiguous/non-JSON input', () => {
  assert.equal(canonicalJson({ z: 1, a: { y: 2, b: [false, null, 'x'] } }), '{"a":{"b":[false,null,"x"],"y":2},"z":1}');
  assert.equal(digestArgs({ a: 1, b: 2 }), digestArgs({ b: 2, a: 1 }));
  for (const value of [undefined, NaN, Infinity, { a: undefined }, new Date(), Array(1)]) assert.throws(() => canonicalJson(value));
  const cycle = {}; cycle.self = cycle; assert.throws(() => canonicalJson(cycle));
});

test('requires explicit origins and keeps a database within one project', (t) => {
  const f = fixture(t);
  assert.throws(() => createConnectService({ databasePath: ':memory:', projectId: PROJECT, allowedOrigins: [] }), /allowed_origins_required/u);
  assert.throws(() => createConnectService({ databasePath: ':memory:', projectId: PROJECT, allowedOrigins: ['http://untrusted.example'] }), /origin_not_allowed/u);
  assert.throws(() => f.open({ projectId: 'different-project' }), /connect_project_mismatch/u);
});

test('proof binds args, operation, origin, project, expiry and exact challenge', async (t) => {
  const f = fixture(t); const actor = identity();
  const args = { label: actor.name, encryptionPublicJwk: actor.encryptionPublicJwk };
  const input = await signed(f.service, actor, 'bootstrap', args);
  const challengeRow = await f.service.handle({ op: 'challenge', origin: ORIGIN, args: { operation: 'status', digest: digestArgs({}) } });
  const message = JSON.parse(challengeRow.message);
  assert.equal(message.projectId, PROJECT); assert.equal(message.origin, ORIGIN);
  assert.equal(message.operation, 'status'); assert.equal(message.digest, digestArgs({}));
  assert.equal(message.challengeId, challengeRow.challengeId); assert.equal(message.expiresAt, challengeRow.expiresAt);
  expectError(await f.service.handle({ ...input, args: { ...args, label: 'changed' } }), 'challenge_mismatch');
  expectError(await f.service.handle({ ...input, op: 'status' }), 'challenge_mismatch');
  expectError(await f.service.handle({ ...input, origin: OTHER_ORIGIN }), 'challenge_mismatch');
  expectError(await f.service.handle({ ...input, origin: 'https://attacker.test' }), 'origin_not_allowed');
  expectError(await f.service.handle({ ...input, origin: undefined }), 'origin_not_allowed');
  assert.equal((await f.service.handle(input)).ok, true);
  expectError(await f.service.handle(input), 'challenge_consumed');
  const expired = await signed(f.service, actor, 'status');
  f.advance(90_000);
  expectError(await f.service.handle(expired), 'challenge_expired');
});

test('rejects invalid signing material without exposing secrets', async (t) => {
  const f = fixture(t); const actor = identity(); const impostor = identity();
  const request = await signed(f.service, actor, 'bootstrap', { label: actor.name, encryptionPublicJwk: actor.encryptionPublicJwk });
  expectError(await f.service.handle({ ...request, proof: { ...request.proof, publicJwk: impostor.publicJwk } }), 'invalid_signature');
  expectError(await f.service.handle({ ...request, proof: { ...request.proof, signature: 'invalid' } }), 'invalid_signature');
  const privateJwk = actor.privateKey.export({ format: 'jwk' });
  expectError(await f.service.handle({ ...request, proof: { ...request.proof, publicJwk: privateJwk } }), 'invalid_public_key');
  assert.equal((await f.service.handle(request)).ok, true);
});

test('valid proof is consumed even for a failed operation', async (t) => {
  const f = fixture(t); const actor = identity();
  const request = await signed(f.service, actor, 'status');
  expectError(await f.service.handle(request), 'authentication_required');
  expectError(await f.service.handle(request), 'challenge_consumed');
});

test('bootstrap is stable across retries/restart and encryption key cannot silently change', async (t) => {
  const f = fixture(t); const actor = identity('Phone');
  const first = await boot(f.service, actor);
  const repeated = await boot(f.service, actor);
  assert.equal(first.accountId, repeated.accountId); assert.equal(first.deviceId, repeated.deviceId);
  const other = identity();
  expectError(await invoke(f.service, actor, 'bootstrap', { label: actor.name, encryptionPublicJwk: other.encryptionPublicJwk }), 'encryption_key_mismatch');
  f.service.close(); const restarted = f.open();
  const result = await boot(restarted, actor);
  assert.equal(result.accountId, first.accountId);
  assert.equal((await invoke(restarted, actor, 'status')).devices.length, 1);
});

test('two independent connections share transactional proof consumption and vault CAS', async (t) => {
  const f = fixture(t); const actor = identity(); await boot(f.service, actor);
  const second = f.open();
  const request = await signed(f.service, actor, 'vault.put', { expectedRevision: 0, envelope: wrapped });
  const results = await Promise.all([f.service.handle(request), second.handle(request)]);
  assert.equal(results.filter((r) => r.ok).length, 1);
  assert.equal(results.find((r) => !r.ok).error.code, 'challenge_consumed');
  const one = await signed(f.service, actor, 'vault.put', { expectedRevision: 1, envelope: { ...wrapped, n: 1 } });
  const two = await signed(second, actor, 'vault.put', { expectedRevision: 1, envelope: { ...wrapped, n: 2 } });
  const cas = await Promise.all([f.service.handle(one), second.handle(two)]);
  assert.equal(cas.filter((r) => r.ok).length, 1);
  assert.equal(cas.find((r) => !r.ok).error.code, 'revision_conflict');
  assert.equal((await invoke(second, actor, 'vault.get')).revision, 2);
});

test('vault is account-scoped, opaque, capped and rejects plaintext private keys', async (t) => {
  const f = fixture(t); const alice = identity('Alice'); const bob = identity('Bob');
  const a = await boot(f.service, alice); await boot(f.service, bob);
  assert.equal((await invoke(f.service, alice, 'vault.put', { expectedRevision: 0, envelope: wrapped })).ok, true);
  assert.equal((await invoke(f.service, bob, 'vault.get')).envelope, null);
  expectError(await invoke(f.service, bob, 'vault.get', { accountId: a.accountId }), 'invalid_arguments');
  expectError(await invoke(f.service, alice, 'vault.put', { expectedRevision: 1, envelope: { privateJwk: alice.privateKey.export({ format: 'jwk' }) } }), 'private_key_forbidden');
  expectError(await invoke(f.service, alice, 'vault.put', { expectedRevision: 1, envelope: { ciphertext: 'x'.repeat(2 * 1024 * 1024) } }), 'envelope_too_large');
  assert.equal((await invoke(f.service, alice, 'vault.get')).revision, 1);
});

test('enrollment uses a separate installation, source approval and repeatable recipient receipt', async (t) => {
  const f = fixture(t); const phone = identity('Phone'); const tv = identity('TV'); const stranger = identity('Stranger');
  const source = await boot(f.service, phone); await boot(f.service, stranger);
  const start = await invoke(f.service, tv, 'enrollment.start', { label: tv.name, encryptionPublicJwk: tv.encryptionPublicJwk });
  const inspect = await invoke(f.service, phone, 'enrollment.inspect', { requestId: start.requestId });
  assert.equal(inspect.deviceId, deviceIdForKey(tv.publicJwk));
  assert.deepEqual(inspect.encryptionPublicJwk, tv.encryptionPublicJwk);
  expectError(await invoke(f.service, tv, 'enrollment.finish', { requestId: start.requestId, expectedAccountId: source.accountId }), 'enrollment_not_approved');
  assert.equal((await invoke(f.service, phone, 'enrollment.approve', { requestId: start.requestId, wrappedKey: wrapped })).ok, true);
  expectError(await invoke(f.service, stranger, 'enrollment.approve', { requestId: start.requestId, wrappedKey: wrapped }), 'enrollment_conflict');
  expectError(await invoke(f.service, stranger, 'enrollment.finish', { requestId: start.requestId, expectedAccountId: source.accountId }), 'enrollment_unavailable');
  const finish = await invoke(f.service, tv, 'enrollment.finish', { requestId: start.requestId, expectedAccountId: source.accountId });
  assert.equal(finish.accountId, source.accountId); assert.notEqual(finish.deviceId, source.deviceId);
  assert.deepEqual(finish.wrappedKey, wrapped);
  f.advance(6 * 60_000);
  const repeat = await invoke(f.service, tv, 'enrollment.finish', { requestId: start.requestId, expectedAccountId: source.accountId });
  assert.deepEqual(repeat, finish);
  assert.equal((await invoke(f.service, phone, 'status')).devices.length, 2);
  f.advance(24 * 60 * 60_000);
  assert.deepEqual(await invoke(f.service, tv, 'enrollment.finish', { requestId: start.requestId, expectedAccountId: source.accountId }), finish);
});

test('recipient-only enrollment preview is read-only and finish binds the account that was displayed', async t => {
  const f = fixture(t), source = identity('Source phone'), target = identity('New screen'), stranger = identity('Another profile');
  const owner = await boot(f.service, source), other = await boot(f.service, stranger);
  const request = await invoke(f.service, target, 'enrollment.start', { label: target.name, encryptionPublicJwk: target.encryptionPublicJwk });
  const pending = await invoke(f.service, target, 'enrollment.preview', { requestId: request.requestId });
  assert.equal(pending.status, 'pending'); assert.equal(pending.account, null);
  await invoke(f.service, source, 'enrollment.approve', { requestId: request.requestId, wrappedKey: wrapped });
  expectError(await invoke(f.service, stranger, 'enrollment.preview', { requestId: request.requestId }), 'enrollment_unavailable');
  const preview = await invoke(f.service, target, 'enrollment.preview', { requestId: request.requestId });
  assert.equal(preview.status, 'approved'); assert.equal(preview.account.accountId, owner.accountId);
  assert.deepEqual(preview.source, { deviceId: owner.deviceId, label: source.name });
  assert.equal(Object.hasOwn(preview, 'wrappedKey'), false);
  expectError(await invoke(f.service, target, 'status'), 'authentication_required');
  expectError(await invoke(f.service, target, 'enrollment.finish', { requestId: request.requestId }), 'invalid_arguments');
  expectError(await invoke(f.service, target, 'enrollment.finish', { requestId: request.requestId, expectedAccountId: other.accountId }), 'enrollment_account_mismatch');
  expectError(await invoke(f.service, target, 'status'), 'authentication_required');
  assert.equal((await invoke(f.service, target, 'enrollment.finish', { requestId: request.requestId, expectedAccountId: owner.accountId })).ok, true);
  f.advance(24 * 60 * 60_000);
  const completed = await invoke(f.service, target, 'enrollment.preview', { requestId: request.requestId });
  assert.equal(completed.status, 'finished'); assert.equal(completed.account.accountId, owner.accountId);
  expectError(await invoke(f.service, target, 'enrollment.finish', { requestId: request.requestId, expectedAccountId: other.accountId }), 'enrollment_account_mismatch');
});

test('expired enrollment and revoked source cannot grant a device', async (t) => {
  const f = fixture(t); const source = identity('Source'); const backup = identity('Backup'); const target = identity('Target');
  const account = await boot(f.service, source); await enroll(f.service, source, backup);
  const started = await invoke(f.service, target, 'enrollment.start', { label: target.name, encryptionPublicJwk: target.encryptionPublicJwk });
  await invoke(f.service, source, 'enrollment.approve', { requestId: started.requestId, wrappedKey: wrapped });
  await invoke(f.service, backup, 'device.revoke', { deviceId: account.deviceId });
  expectError(await invoke(f.service, target, 'enrollment.finish', { requestId: started.requestId, expectedAccountId: account.accountId }), 'enrollment_unavailable');
  const another = identity('Another');
  const next = await invoke(f.service, another, 'enrollment.start', { label: another.name, encryptionPublicJwk: another.encryptionPublicJwk });
  f.advance(5 * 60_000);
  expectError(await invoke(f.service, backup, 'enrollment.approve', { requestId: next.requestId, wrappedKey: wrapped }), 'enrollment_expired');
});

test('revocation retains tombstone and prevents bootstrap, status, vault and enrollment replay', async (t) => {
  const f = fixture(t); const phone = identity('Phone'); const tv = identity('TV');
  const owner = await boot(f.service, phone);
  expectError(await invoke(f.service, phone, 'device.revoke', { deviceId: owner.deviceId }), 'last_device_requires_recovery');
  const target = await enroll(f.service, phone, tv);
  assert.equal((await invoke(f.service, phone, 'device.revoke', { deviceId: target.deviceId })).ok, true);
  expectError(await invoke(f.service, tv, 'bootstrap', { label: tv.name, encryptionPublicJwk: tv.encryptionPublicJwk }), 'device_revoked');
  expectError(await invoke(f.service, tv, 'status'), 'device_revoked');
  expectError(await invoke(f.service, tv, 'vault.get'), 'device_revoked');
  expectError(await invoke(f.service, tv, 'enrollment.finish', { requestId: target.requestId, expectedAccountId: owner.accountId }), 'device_revoked');
  assert.equal((await invoke(f.service, phone, 'status')).devices.find((d) => d.deviceId === target.deviceId).state, 'revoked');
  f.service.close(); const second = f.open();
  expectError(await invoke(second, tv, 'bootstrap', { label: tv.name, encryptionPublicJwk: tv.encryptionPublicJwk }), 'device_revoked');
});

test('recovery preparation is not verified and cannot replace the active method before confirmation', async (t) => {
  const f = fixture(t); const actor = identity(); const owner = await boot(f.service, actor);
  const first = await prepareRecovery(f.service, actor);
  assert.equal((await invoke(f.service, actor, 'status')).recovery.verified, false);
  expectError(await invoke(f.service, actor, 'device.revoke', { deviceId: owner.deviceId }), 'last_device_requires_recovery');
  await confirmRecovery(f.service, actor, first);
  const second = await prepareRecovery(f.service, actor);
  assert.equal((await invoke(f.service, actor, 'status')).recovery.recoveryId, first.recoveryId);
  expectError(await invoke(f.service, actor, 'recovery.confirm', { recoveryId: second.recoveryId, verifier: first.verifier }), 'recovery_unavailable');
  assert.equal((await invoke(f.service, actor, 'status')).recovery.recoveryId, first.recoveryId);
  await confirmRecovery(f.service, actor, second);
  assert.equal((await invoke(f.service, actor, 'status')).recovery.recoveryId, second.recoveryId);
  const replacement = identity('Replacement');
  expectError(await invoke(f.service, replacement, 'recovery.use', recoveryArgs(owner, first, replacement)), 'recovery_unavailable');
});

test('one-use recovery survives lost response for the same recipient and rejects other recipients', async (t) => {
  const f = fixture(t); const actor = identity('Old'); const owner = await boot(f.service, actor);
  const recovery = await prepareRecovery(f.service, actor); await confirmRecovery(f.service, actor, recovery);
  assert.equal((await invoke(f.service, actor, 'device.revoke', { deviceId: owner.deviceId })).ok, true);
  const replacement = identity('New'); const args = recoveryArgs(owner, recovery, replacement);
  const result = await invoke(f.service, replacement, 'recovery.use', args);
  assert.equal(result.ok, true, result.error?.code); assert.equal(result.accountId, owner.accountId);
  assert.notEqual(result.deviceId, owner.deviceId); assert.deepEqual(result.wrappedKey, wrapped);
  f.service.close(); const reopened = f.open();
  assert.deepEqual(await invoke(reopened, replacement, 'recovery.use', args), result);
  const attacker = identity('Other');
  expectError(await invoke(reopened, attacker, 'recovery.use', recoveryArgs(owner, recovery, attacker)), 'recovery_unavailable');
  expectError(await invoke(reopened, replacement, 'recovery.use', { ...args, label: 'Changed' }), 'recovery_unavailable');
  assert.equal((await invoke(reopened, replacement, 'status')).recovery.verified, false);
  f.advance(24 * 60 * 60_000);
  assert.deepEqual(await invoke(reopened, replacement, 'recovery.use', args), result);
  assert.equal((await invoke(reopened, replacement, 'status')).ok, true);
});

test('friendship requires the recipient, grants no other account access, and handles opposite requests', async (t) => {
  const f = fixture(t); const alice = identity('Alice'); const bob = identity('Bob'); const eve = identity('Eve');
  await boot(f.service, alice); await boot(f.service, bob); await boot(f.service, eve);
  const aCard = await invoke(f.service, alice, 'card.get'); const bCard = await invoke(f.service, bob, 'card.get');
  const publicCard = await f.service.handle({ op: 'card.resolve', args: { cardId: aCard.cardId }, origin: ORIGIN });
  assert.equal(publicCard.ok, true); assert.equal(publicCard.label, 'Alice'); assert.equal(publicCard.accountId, undefined);
  assert.equal((await invoke(f.service, bob, 'contacts.list')).requests.incoming.length, 0);
  const request = await invoke(f.service, alice, 'contacts.request', { cardId: bCard.cardId });
  const reverse = await invoke(f.service, bob, 'contacts.request', { cardId: aCard.cardId });
  assert.equal(reverse.requestId, request.requestId); assert.equal(reverse.direction, 'incoming');
  expectError(await invoke(f.service, alice, 'contacts.accept', { requestId: request.requestId }), 'contact_unavailable');
  expectError(await invoke(f.service, eve, 'contacts.accept', { requestId: request.requestId }), 'contact_unavailable');
  const accepted = await invoke(f.service, bob, 'contacts.accept', { requestId: request.requestId });
  assert.equal(accepted.ok, true);
  assert.deepEqual(await invoke(f.service, bob, 'contacts.accept', { requestId: request.requestId }), accepted);
  assert.equal((await invoke(f.service, alice, 'contacts.list')).contacts.length, 1);
  assert.equal((await invoke(f.service, bob, 'status')).devices.length, 1);
  await invoke(f.service, alice, 'vault.put', { expectedRevision: 0, envelope: wrapped });
  assert.equal((await invoke(f.service, bob, 'vault.get')).envelope, null);
  await invoke(f.service, alice, 'contacts.remove', { relationshipId: accepted.relationshipId });
  expectError(await invoke(f.service, bob, 'contacts.accept', { requestId: request.requestId }), 'contact_unavailable');
  assert.equal((await invoke(f.service, bob, 'contacts.list')).contacts.length, 0);
});

test('rotate cancels pending requests but retains friends, cancellation and block cannot be bypassed', async (t) => {
  const f = fixture(t); const alice = identity('Alice'); const bob = identity('Bob'); const charlie = identity('Charlie');
  const a = await boot(f.service, alice); const b = await boot(f.service, bob); await boot(f.service, charlie);
  const card = await invoke(f.service, bob, 'card.get');
  const pending = await invoke(f.service, alice, 'contacts.request', { cardId: card.cardId });
  await invoke(f.service, alice, 'contacts.cancel', { requestId: pending.requestId });
  expectError(await invoke(f.service, bob, 'contacts.accept', { requestId: pending.requestId }), 'contact_unavailable');
  const again = await invoke(f.service, alice, 'contacts.request', { cardId: card.cardId });
  const friendship = await invoke(f.service, bob, 'contacts.accept', { requestId: again.requestId });
  const incoming = await invoke(f.service, charlie, 'contacts.request', { cardId: card.cardId });
  const rotated = await invoke(f.service, bob, 'card.rotate');
  assert.notEqual(rotated.cardId, card.cardId);
  expectError(await f.service.handle({ op: 'card.resolve', args: { cardId: card.cardId }, origin: ORIGIN }), 'card_unavailable');
  expectError(await invoke(f.service, bob, 'contacts.accept', { requestId: incoming.requestId }), 'contact_unavailable');
  assert.equal((await invoke(f.service, alice, 'contacts.list')).contacts[0].relationshipId, friendship.relationshipId);
  assert.equal((await invoke(f.service, bob, 'contacts.block', { peerAccountId: a.accountId })).ok, true);
  assert.equal((await invoke(f.service, alice, 'contacts.list')).contacts.length, 0);
  expectError(await invoke(f.service, alice, 'contacts.request', { cardId: rotated.cardId }), 'contact_unavailable');
  const aCard = await invoke(f.service, alice, 'card.get');
  expectError(await invoke(f.service, bob, 'contacts.request', { cardId: aCard.cardId }), 'contact_unavailable');
  expectError(await invoke(f.service, bob, 'contacts.accept', { requestId: again.requestId }), 'contact_unavailable');
  assert.equal((await invoke(f.service, alice, 'contacts.list')).blocked.length, 0);
  assert.equal((await invoke(f.service, bob, 'contacts.list')).blocked[0].peerAccountId, a.accountId);
  assert.notEqual(a.accountId, b.accountId);
});

test('contact expiry is enforced on acceptance', async (t) => {
  const f = fixture(t); const alice = identity('Alice'); const bob = identity('Bob');
  await boot(f.service, alice); await boot(f.service, bob);
  const card = await invoke(f.service, bob, 'card.get');
  const request = await invoke(f.service, alice, 'contacts.request', { cardId: card.cardId });
  f.advance(7 * 24 * 60 * 60_000);
  expectError(await invoke(f.service, bob, 'contacts.accept', { requestId: request.requestId }), 'contact_unavailable');
});

test('rename changes profile/card labels without changing account, installation, or relation IDs', async (t) => {
  const f = fixture(t); const actor = identity('Initial'); const original = await boot(f.service, actor);
  const card = await invoke(f.service, actor, 'card.get');
  const renamed = await invoke(f.service, actor, 'profile.rename', { label: 'New name' });
  assert.equal(renamed.accountId, original.accountId); assert.equal(renamed.deviceId, original.deviceId);
  const status = await invoke(f.service, actor, 'status');
  assert.equal(status.label, 'New name'); assert.equal(status.devices[0].label, 'Initial');
  const after = await invoke(f.service, actor, 'card.get');
  assert.equal(after.cardId, card.cardId); assert.equal(after.label, 'New name');
  expectError(await invoke(f.service, actor, 'profile.rename', { label: '   ' }), 'invalid_label');
});

test('friend invitations are limited to same-origin room links, explicit and revoked with relationship', async (t) => {
  const f = fixture(t); const alice = identity('Alice'); const bob = identity('Bob'); const eve = identity('Eve');
  const owner = await boot(f.service, alice); await boot(f.service, bob); await boot(f.service, eve);
  const card = await invoke(f.service, bob, 'card.get');
  const request = await invoke(f.service, alice, 'contacts.request', { cardId: card.cardId });
  const relation = await invoke(f.service, bob, 'contacts.accept', { requestId: request.requestId });
  const args = { relationshipId: relation.relationshipId, url: `${ORIGIN}/?j=room_test.person`, label: 'New room' };
  for (const url of [`https://attacker.test/?j=room_test.person`, `${ORIGIN}/?j=room_test.person&connector=secret`, `${ORIGIN}/?j=room_test.person#extra`, `${ORIGIN}/admin?j=room_test.person`, `${OTHER_ORIGIN}/?j=room_test.person`, `${ORIGIN}/?j=one&j=two`]) {
    expectError(await invoke(f.service, alice, 'contacts.sendInvite', { ...args, url }), 'invalid_invitation_url');
  }
  expectError(await invoke(f.service, eve, 'contacts.sendInvite', args), 'contact_unavailable');
  const invite = await invoke(f.service, alice, 'contacts.sendInvite', args);
  assert.equal(invite.ok, true); assert.deepEqual(await invoke(f.service, alice, 'contacts.sendInvite', args), invite);
  const inbox = await invoke(f.service, bob, 'contacts.list');
  assert.equal(inbox.invitations.length, 1); assert.equal(inbox.invitations[0].url, args.url);
  assert.equal(inbox.invitations[0].peerAccountId, owner.accountId);
  assert.equal((await invoke(f.service, bob, 'status')).devices.length, 1);
  expectError(await invoke(f.service, eve, 'contacts.dismissInvite', { invitationId: invite.invitationId }), 'invitation_unavailable');
  assert.equal((await invoke(f.service, bob, 'contacts.dismissInvite', { invitationId: invite.invitationId })).ok, true);
  assert.equal((await invoke(f.service, bob, 'contacts.list')).invitations.length, 0);
  await invoke(f.service, alice, 'contacts.sendInvite', args);
  await invoke(f.service, bob, 'contacts.block', { peerAccountId: owner.accountId });
  assert.equal((await invoke(f.service, bob, 'contacts.list')).invitations.length, 0);
  expectError(await invoke(f.service, alice, 'contacts.sendInvite', args), 'contact_unavailable');
  await invoke(f.service, bob, 'contacts.unblock', { peerAccountId: owner.accountId });
  assert.equal((await invoke(f.service, bob, 'contacts.list')).blocked.length, 0);
  assert.equal((await invoke(f.service, bob, 'contacts.list')).contacts.length, 0);
  expectError(await invoke(f.service, alice, 'contacts.sendInvite', args), 'contact_unavailable');
});

test('recipient decline and sender cancellation remain distinct', async (t) => {
  const f = fixture(t); const alice = identity('Alice'); const bob = identity('Bob');
  await boot(f.service, alice); await boot(f.service, bob);
  const card = await invoke(f.service, bob, 'card.get');
  const request = await invoke(f.service, alice, 'contacts.request', { cardId: card.cardId });
  expectError(await invoke(f.service, alice, 'contacts.decline', { requestId: request.requestId }), 'contact_unavailable');
  assert.equal((await invoke(f.service, bob, 'contacts.decline', { requestId: request.requestId })).declined, true);
  expectError(await invoke(f.service, bob, 'contacts.accept', { requestId: request.requestId }), 'contact_unavailable');
  assert.equal((await invoke(f.service, alice, 'contacts.list')).requests.outgoing.length, 0);
});

test('enrollment start idempotency persists across connections without changing recipient keys', async (t) => {
  const f = fixture(t); const target = identity('Target');
  const args = { label: target.name, encryptionPublicJwk: target.encryptionPublicJwk };
  const first = await invoke(f.service, target, 'enrollment.start', args);
  assert.deepEqual(await invoke(f.open(), target, 'enrollment.start', args), first);
  expectError(await invoke(f.service, target, 'enrollment.start', { ...args, label: 'Changed' }), 'enrollment_conflict');
});

test('failed public card lookup still consumes the persisted abuse counter', async (t) => {
  const f = fixture(t);
  expectError(await f.service.handle({ op: 'card.resolve', args: { cardId: 'card_unknown' }, origin: ORIGIN }), 'card_unavailable');
  expectError(await f.service.handle({ op: 'card.resolve', args: { cardId: 'card_unknown' }, origin: ORIGIN }), 'card_unavailable');
  const db = new DatabaseSync(f.databasePath);
  assert.equal(db.prepare('SELECT count FROM rate_limits WHERE key=?').get('card-resolve:origin:' + ORIGIN).count, 2);
  db.close();
});

test('rate limits persist across connections and restart', async (t) => {
  const f = fixture(t); const actor = identity(); await boot(f.service, actor);
  for (let count = 0; count < 239; count += 1) assert.equal((await invoke(f.service, actor, 'status')).ok, true);
  expectError(await invoke(f.open(), actor, 'status'), 'rate_limited');
  f.service.close();
  const restarted = f.open();
  expectError(await invoke(restarted, actor, 'status'), 'rate_limited');
  f.advance(60_000);
  assert.equal((await invoke(restarted, actor, 'status')).ok, true);
});

test('additive migration preserves existing state and future versions fail without reset', (t) => {
  const f = fixture(t); f.service.close();
  const oldPath = join(dirname(f.databasePath), 'old.sqlite');
  const old = new DatabaseSync(oldPath);
  old.exec(MIGRATIONS[0]); old.exec('PRAGMA user_version=1');
  old.prepare('INSERT INTO connect_meta(key,value) VALUES (?,?)').run('project_id', PROJECT);
  old.prepare('INSERT INTO connect_meta(key,value) VALUES (?,?)').run('min_reader', '1');
  old.prepare('INSERT INTO connect_meta(key,value) VALUES (?,?)').run('schema_version', '1');
  old.prepare('INSERT INTO connect_meta(key,value) VALUES (?,?)').run('schema_lineage', 'connect.sqlite.local.v1');
  old.prepare('INSERT INTO accounts(id,label,created_at) VALUES (?,?,?)').run('acct_fixture', 'Preserved', 123);
  old.close();
  const migrated = f.open({ databasePath: oldPath }); migrated.close();
  const inspect = new DatabaseSync(oldPath);
  assert.equal(inspect.prepare('PRAGMA user_version').get().user_version, SCHEMA_VERSION);
  assert.equal(inspect.prepare('SELECT label FROM accounts WHERE id=?').get('acct_fixture').label, 'Preserved');
  assert.ok(inspect.prepare('PRAGMA table_info(recovery_methods)').all().some((column) => column.name === 'receipt_json'));
  inspect.exec('PRAGMA user_version=999'); inspect.close();
  assert.throws(() => f.open({ databasePath: oldPath }), /connect_schema_metadata_mismatch/u);
  const future = new DatabaseSync(oldPath);
  assert.equal(future.prepare('SELECT label FROM accounts WHERE id=?').get('acct_fixture').label, 'Preserved');
  future.exec(`PRAGMA user_version=${SCHEMA_VERSION}`);
  future.prepare("UPDATE connect_meta SET value='999' WHERE key='min_reader'").run(); future.close();
  assert.throws(() => f.open({ databasePath: oldPath }), /connect_reader_too_old/u);
});

test('reader epoch supports rollback over additive schema without accepting an unsafe or corrupted layout', async (t) => {
  const f = fixture(t); const actor = identity('Preserved'); const account = await boot(f.service, actor); f.service.close();
  const db = new DatabaseSync(f.databasePath);
  migrateDatabase(db, PROJECT, { supportedVersion: 2, readerEpoch: 1 });
  assert.equal(db.prepare('PRAGMA user_version').get().user_version, SCHEMA_VERSION);
  assert.equal(db.prepare("SELECT value FROM connect_meta WHERE key='min_reader'").get().value, '1');
  db.exec('ALTER TABLE accounts ADD COLUMN future_optional_note TEXT');
  db.exec(`PRAGMA user_version=${SCHEMA_VERSION + 1}`);
  db.prepare("UPDATE connect_meta SET value=? WHERE key='schema_version'").run(String(SCHEMA_VERSION + 1));
  db.close();
  const compatible = f.open();
  assert.equal((await invoke(compatible, actor, 'status')).accountId, account.accountId);
  compatible.close();
  const corrupt = new DatabaseSync(f.databasePath);
  corrupt.exec('DROP INDEX relationships_active_pair'); corrupt.close();
  assert.throws(() => f.open(), /connect_schema_layout_invalid/u);
});
