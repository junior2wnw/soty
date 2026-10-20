import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, sign } from 'node:crypto';
import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { DatabaseSync } from 'node:sqlite';
import { createConnectService, digestArgs } from '../../connect/server/index.mjs';
import { createWorldService } from '../server/index.mjs';
import { migrateWorld as migrateV3 } from './support/legacy-world-schema-v3.mjs';
import { removeDiscoveryProjection } from './schema-fixtures.mjs';

const PROJECT = 'world-profile-receipts';
const alice = Object.freeze({ accountId: 'receipt_alice', deviceId: 'receipt_alice_device', label: 'Алиса' });
const bob = Object.freeze({ accountId: 'receipt_bob', deviceId: 'receipt_bob_device', label: 'Борис' });
const schema = 'soty.world-profile-audience.receipt.v1';
const denied = (callback, code) => assert.throws(callback, error => error.code === code);
function fixture(t) {
  const directory = mkdtempSync(join(tmpdir(), 'soty-profile-receipt-')), databasePath = join(directory, 'world.sqlite'), services = [];
  const open = () => { const service = createWorldService({ databasePath, projectId: PROJECT }); services.push(service); return service; };
  let service = open();
  const call = (actor, op, args = {}, target = service) => target.execute({ actor, op: 'world.' + op, args });
  for (const actor of [alice, bob]) call(actor, 'profile.get');
  const intent = (requestId, audience, expectedRevision = call(alice, 'profile.get').profile.revision, account = alice.accountId) =>
    Object.freeze({ expectedAccountId: account, requestId, expectedRevision, audience });
  const inspect = callback => { const db = new DatabaseSync(databasePath); try { return callback(db); } finally { db.close(); } };
  t.after(() => { for (const item of services) item.close(); rmSync(directory, { recursive: true, force: true }); });
  return { call, intent, inspect, databasePath, open,
    reopen() { service.close(); service = open(); },
  };
}
function receiptFor(args, acceptedRevision) {
  return { schema, requestId: args.requestId, accountId: args.expectedAccountId, audience: args.audience,
    expectedRevision: args.expectedRevision, acceptedRevision };
}

test('own durable audience feature is explicit; all existing no-requestId profile edits retain their reply shape', t => {
  const f = fixture(t), initial = f.call(alice, 'profile.get').profile;
  assert.equal(initial.audienceWriteVersion, 1);
  const legacy = f.call(alice, 'profile.update', { expectedRevision: initial.revision, discoverable: true, bio: 'Обычное редактирование' });
  assert.deepEqual(Object.keys(legacy), ['profile']); assert.equal(legacy.profile.audience, 'public');
  assert.equal(legacy.profile.revision, initial.revision + 1); assert.equal(legacy.profile.audienceWriteVersion, 1);
  assert.equal(Object.hasOwn(f.call(bob, 'profile.view', { profileId: alice.accountId }).profile, 'audienceWriteVersion'), false);
  assert.equal(f.inspect(db => db.prepare("SELECT COUNT(*) AS n FROM receipts WHERE operation='world.profile.update'").get().n), 0);
});

test('accepted audience receipt is immutable intent metadata, not a cached full profile; replay precedes CAS and projects current own state', t => {
  const f = fixture(t), args = f.intent('profile_receipt_001', 'public');
  const first = f.call(alice, 'profile.update', args);
  assert.equal(first.replayed, false); assert.deepEqual(first.receipt, receiptFor(args, args.expectedRevision + 1));
  assert.equal(first.profile.revision, first.receipt.acceptedRevision);
  const reordered = { audience: args.audience, expectedRevision: args.expectedRevision, requestId: args.requestId, expectedAccountId: args.expectedAccountId };
  assert.equal(f.call(alice, 'profile.update', reordered).replayed, true);
  const current = f.call(alice, 'profile.update', { expectedRevision: first.profile.revision, audience: 'private', bio: 'Текущее описание' }).profile;
  const replay = f.call(alice, 'profile.update', args);
  assert.equal(replay.replayed, true); assert.deepEqual(replay.receipt, first.receipt); assert.deepEqual(replay.profile, current);
  assert.equal(replay.profile.audience, 'private'); assert.equal(replay.profile.bio, 'Текущее описание');
  f.inspect(db => {
    const rows = db.prepare("SELECT * FROM receipts WHERE operation='world.profile.update'").all(); assert.equal(rows.length, 1);
    assert.deepEqual(JSON.parse(rows[0].result), first.receipt);
    assert.equal(Object.hasOwn(JSON.parse(rows[0].result), 'profile'), false);
    assert.doesNotMatch(rows[0].result, /bio|interests|Текущее описание/u);
    assert.equal(db.prepare("SELECT COUNT(*) AS n FROM world_audit WHERE operation='world.profile.update'").get().n, 2);
  });
});

test('lost reply survives service restart and same-ID retry without an additional profile revision or audit write', t => {
  const f = fixture(t), args = f.intent('profile_receipt_restart', 'friends');
  const accepted = f.call(alice, 'profile.update', args); f.reopen();
  const replay = f.call(alice, 'profile.update', args);
  assert.equal(replay.replayed, true); assert.deepEqual(replay.receipt, accepted.receipt); assert.deepEqual(replay.profile, accepted.profile);
  assert.equal(f.inspect(db => db.prepare("SELECT COUNT(*) AS n FROM world_audit WHERE operation='world.profile.update'").get().n), 1);
});

test('held original delivery cannot reopen public after a same-intent retry and a confirmed newer private write across two real World connections', async t => {
  const f = fixture(t), original = f.intent('profile_receipt_held_public', 'public'), second = f.open();
  let release; const held = new Promise(resolve => { release = resolve; });
  const lateOriginal = held.then(() => f.call(alice, 'profile.update', original));
  try {
    const unchanged = f.call(alice, 'profile.get').profile;
    assert.equal(unchanged.revision, original.expectedRevision); assert.equal(unchanged.audience, 'private');
    const resolved = f.call(alice, 'profile.update', original, second);
    const closing = f.intent('profile_receipt_new_private', 'private', resolved.receipt.acceptedRevision);
    const closed = f.call(alice, 'profile.update', closing, second);
    assert.equal(closed.profile.audience, 'private'); assert.equal(closed.profile.revision, original.expectedRevision + 2);
    release(); const late = await lateOriginal;
    assert.equal(late.replayed, true); assert.deepEqual(late.receipt, resolved.receipt);
    assert.equal(late.profile.audience, 'private'); assert.equal(late.profile.revision, closed.profile.revision);
    assert.deepEqual(f.call(alice, 'profile.get').profile, closed.profile);
    f.inspect(db => {
      assert.equal(db.prepare("SELECT n FROM world_directory_counts WHERE kind='person'").get().n, 0);
      assert.equal(db.prepare("SELECT COUNT(*) AS n FROM receipts WHERE operation='world.profile.update'").get().n, 2);
      assert.equal(db.prepare("SELECT COUNT(*) AS n FROM world_audit WHERE operation='world.profile.update'").get().n, 2);
    });
  } finally { release(); await lateOriginal.catch(() => {}); }
});

test('v2 is a closed account-bound four-field intent; same-ID changed args conflict before stale-revision handling', t => {
  const f = fixture(t), args = f.intent('profile_receipt_conflict', 'public');
  const accepted = f.call(alice, 'profile.update', args);
  for (const next of [{ ...args, audience: 'private' }, { ...args, expectedRevision: accepted.profile.revision }]) {
    denied(() => f.call(alice, 'profile.update', next), 'profile_request_conflict');
  }
  denied(() => f.call(bob, 'profile.update', args), 'profile_account_changed');
  for (const invalid of [
    { expectedRevision: 1, audience: 'private', requestId: 'missing_account' },
    { expectedRevision: 1, audience: 'private', expectedAccountId: alice.accountId },
    { ...args, requestId: 'extra_field', bio: 'Not this intent' },
  ]) denied(() => f.call(alice, 'profile.update', invalid), 'invalid_arguments');
  denied(() => f.call(alice, 'profile.update', { ...args, requestId: 'bad_audience', audience: 'strangers' }), 'invalid_choice');
  denied(() => f.call(alice, 'profile.update', { ...args, requestId: null }), 'invalid_identifier');
  denied(() => f.call(alice, 'profile.update', null), 'invalid_arguments');
  assert.deepEqual(f.call(alice, 'profile.get').profile, accepted.profile);
  const b = f.call(bob, 'profile.update', f.intent(args.requestId, 'link', 1, bob.accountId));
  assert.equal(b.receipt.accountId, bob.accountId); assert.equal(b.profile.profileId, bob.accountId);
});

test('definitive CAS refusal does not persist an acceptance or undo a newer profile', t => {
  const f = fixture(t), stale = f.intent('profile_receipt_stale', 'public');
  const newer = f.call(alice, 'profile.update', { expectedRevision: stale.expectedRevision, bio: 'Новее' }).profile;
  denied(() => f.call(alice, 'profile.update', stale), 'revision_conflict');
  denied(() => f.call(alice, 'profile.update', stale), 'revision_conflict');
  assert.deepEqual(f.call(alice, 'profile.get').profile, newer);
  assert.equal(f.inspect(db => db.prepare('SELECT COUNT(*) AS n FROM receipts WHERE request_id=?').get(stale.requestId).n), 0);
});

test('receipt persistence failure rolls back profile, audit and public index together; the same intent remains retryable', t => {
  const f = fixture(t), args = f.intent('profile_receipt_atomic', 'public'), before = f.call(alice, 'profile.get').profile;
  f.inspect(db => db.exec("CREATE TRIGGER owned_receipt_failure BEFORE INSERT ON receipts WHEN new.operation='world.profile.update' BEGIN SELECT RAISE(ABORT,'owned_receipt_failure'); END"));
  assert.throws(() => f.call(alice, 'profile.update', args), /owned_receipt_failure/u);
  assert.deepEqual(f.call(alice, 'profile.get').profile, before);
  f.inspect(db => {
    assert.equal(db.prepare("SELECT COUNT(*) AS n FROM receipts WHERE operation='world.profile.update'").get().n, 0);
    assert.equal(db.prepare("SELECT COUNT(*) AS n FROM world_audit WHERE operation='world.profile.update'").get().n, 0);
    assert.equal(db.prepare("SELECT n FROM world_directory_counts WHERE kind='person'").get().n, 0);
    db.exec('DROP TRIGGER owned_receipt_failure');
  });
  const retried = f.call(alice, 'profile.update', args); assert.equal(retried.replayed, false); assert.equal(retried.receipt.acceptedRevision, before.revision + 1);
});

test('accepted receipt replay still requires a current genuine signed installation/account before any receipt disclosure', async t => {
  const directory = mkdtempSync(join(tmpdir(), 'soty-profile-receipt-connect-')), worldPath = join(directory, 'world.sqlite'), connectPath = join(directory, 'connect.sqlite');
  const origin = 'https://profile-receipt.test'; let connect;
  const world = createWorldService({ databasePath: worldPath, projectId: PROJECT,
    withCurrentContacts: (actor, callback) => connect.withCurrentContacts(actor, callback) });
  connect = createConnectService({ databasePath: connectPath, projectId: PROJECT, allowedOrigins: [origin], extensions: [world] });
  t.after(() => { connect.close(); world.close(); rmSync(directory, { recursive: true, force: true }); });
  const identity = label => {
    const signing = generateKeyPairSync('ec', { namedCurve: 'prime256v1' }), encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
    return { label, signing: signing.privateKey, publicJwk: signing.publicKey.export({ format: 'jwk' }), encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }) };
  };
  const good = value => { assert.equal(value.ok, true, value.error?.code); return value; };
  const call = async (actor, op, args = {}) => {
    const challenge = good(await connect.handle({ origin, op: 'challenge', args: { operation: op, digest: digestArgs(args) } }));
    return connect.handle({ origin, op, args, proof: { challengeId: challenge.challengeId, publicJwk: actor.publicJwk,
      signature: sign('sha256', Buffer.from(challenge.message), { key: actor.signing, dsaEncoding: 'ieee-p1363' }).toString('base64url') } });
  };
  const boot = async actor => good(await call(actor, 'bootstrap', { label: actor.label, encryptionPublicJwk: actor.encryptionPublicJwk }));
  const owner = identity('Автор'), foreign = identity('Другой'), phone = identity('Телефон');
  const a = await boot(owner), b = await boot(foreign);
  const enrollment = good(await call(phone, 'enrollment.start', { label: phone.label, encryptionPublicJwk: phone.encryptionPublicJwk }));
  good(await call(owner, 'enrollment.approve', { requestId: enrollment.requestId, wrappedKey: { schema: 'test.encrypted', ciphertext: 'owned-test-only' } }));
  const mobile = good(await call(phone, 'enrollment.finish', { requestId: enrollment.requestId, expectedAccountId: a.accountId }));
  const initial = good(await call(owner, 'world.profile.get')).profile;
  const args = { expectedAccountId: a.accountId, requestId: 'profile_receipt_signed', expectedRevision: initial.revision, audience: 'public' };
  const first = good(await call(owner, 'world.profile.update', args));
  assert.deepEqual(good(await call(phone, 'world.profile.update', args)).receipt, first.receipt);
  good(await call(owner, 'device.revoke', { deviceId: mobile.deviceId }));
  const revoked = await call(phone, 'world.profile.update', args); assert.equal(revoked.ok, false); assert.equal(revoked.error.code, 'device_revoked');
  const wrong = await call(foreign, 'world.profile.update', args); assert.equal(wrong.ok, false); assert.equal(wrong.error.code, 'profile_account_changed');
  const db = new DatabaseSync(connectPath);
  try { db.prepare('UPDATE installations SET account_id=? WHERE id=?').run(b.accountId, a.deviceId); } finally { db.close(); }
  const moved = await call(owner, 'world.profile.update', args); assert.equal(moved.ok, false); assert.equal(moved.error.code, 'profile_account_changed');
  denied(() => world.execute({ actor: { accountId: a.accountId, deviceId: a.deviceId, label: owner.label }, op: 'world.profile.update', args }), 'connect_authority_actor_mismatch');
});

function logicalSnapshot(db) {
  return { schema: db.prepare('SELECT name,type,sql FROM sqlite_schema ORDER BY name').all(),
    version: db.prepare('PRAGMA user_version').get().user_version,
    journal: db.prepare('PRAGMA journal_mode').get().journal_mode,
    meta: db.prepare('SELECT * FROM world_meta ORDER BY key').all(),
    profiles: db.prepare('SELECT * FROM profiles ORDER BY account_id').all() };
}

test('existing formats1/2/3 refuse default audience migration before journal/schema/data writes; explicit synthetic opt-in alone upgrades', async t => {
  for (const version of [1, 2, 3]) await t.test('format' + version, tt => {
    const directory = mkdtempSync(join(tmpdir(), 'soty-profile-migration-optin-')), databasePath = join(directory, 'world.sqlite');
    let service; tt.after(() => { service?.close(); rmSync(directory, { recursive: true, force: true }); });
    const db = new DatabaseSync(databasePath); migrateV3(db, PROJECT);
    db.prepare('INSERT INTO profiles(account_id,display_name,bio,search_text,revision,created_at,updated_at) VALUES(?,?,?,?,?,?,?)')
      .run(alice.accountId, alice.label, 'Существующий профиль', 'алиса', 17, 11, 22);
    if (version < 3) removeDiscoveryProjection(db);
    if (version < 2) db.exec('DROP TABLE profile_avatars; ALTER TABLE profiles DROP COLUMN avatar_revision');
    db.exec(`PRAGMA user_version=${version}; UPDATE world_meta SET value='${version}' WHERE key='schema_version'; PRAGMA journal_mode=DELETE`);
    const before = logicalSnapshot(db); db.close();
    let unexpected;
    try { assert.throws(() => { unexpected = createWorldService({ databasePath, projectId: PROJECT }); }, /world_audience_migration_required/u); }
    finally { unexpected?.close(); }
    const observed = new DatabaseSync(databasePath, { readOnly: true });
    try { assert.deepEqual(logicalSnapshot(observed), before); } finally { observed.close(); }
    service = createWorldService({ databasePath, projectId: PROJECT, allowAudienceMigration: true });
    const profile = service.execute({ actor: alice, op: 'world.profile.get' }).profile;
    assert.equal(service.schemaVersion, 5); assert.equal(profile.revision, 17); assert.equal(profile.bio, 'Существующий профиль'); assert.equal(profile.audience, 'private');
  });
});

test('invalid migration opt-in rejects before creating a database; fresh format0 still starts format5 by default', t => {
  const directory = mkdtempSync(join(tmpdir(), 'soty-profile-migration-invalid-')), databasePath = join(directory, 'world.sqlite');
  let service; t.after(() => { service?.close(); rmSync(directory, { recursive: true, force: true }); });
  let unexpected;
  try { denied(() => { unexpected = createWorldService({ databasePath, projectId: PROJECT, allowAudienceMigration: 'true' }); }, 'invalid_audience_migration_mode'); }
  finally { unexpected?.close(); }
  assert.equal(existsSync(databasePath), false);
  service = createWorldService({ databasePath, projectId: PROJECT }); assert.equal(service.schemaVersion, 5);
});
