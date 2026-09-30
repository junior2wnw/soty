import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, randomBytes, sign } from 'node:crypto';
import { mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createConnectService, digestArgs } from '../../connect/server/index.mjs';
import { createNotesService, NOTES_OPERATIONS } from '../../notes/server/index.mjs';
import { createCapabilitiesService, ACCESS_OPERATIONS, BUILTIN_CAPABILITIES } from '../server/index.mjs';

const PROJECT = 'independent-native-effect';
const ORIGIN = 'https://independent-native.test';
const errorCode = expected => error => error?.code === expected;
const result = value => { assert.equal(value.ok, true, value.error?.code); return value; };
const signingIdentity = () => {
  const signing = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { privateKey: signing.privateKey, publicJwk: signing.publicKey.export({ format: 'jwk' }),
    encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }) };
};

// Independent composition: actual signed Connect owner, real authority fence,
// and separate SQLite stores. No author test fixture or synthetic owner ACL.
async function fixture(t, initial = {}) {
  const parent = realpathSync(tmpdir()), prefix = 'soty-native-effect-independent-';
  const directory = mkdtempSync(path.join(parent, prefix)), marker = randomBytes(20).toString('hex');
  writeFileSync(path.join(directory, '.owner'), marker, { flag: 'wx' });
  const paths = Object.fromEntries(['connect', 'notes', 'caps'].map(name => [name, path.join(directory, name + '.sqlite')]));
  let caps, notes, connect, time = 1_890_000_000_000, settings = { enabled: true, ...initial };
  const readers = new Map();
  t.after(() => {
    for (const db of readers.values()) db.close();
    caps?.close(); notes?.close(); connect?.close();
    const resolved = realpathSync(directory);
    assert.equal(resolved, directory); assert.equal(path.dirname(resolved), parent);
    assert.ok(path.basename(resolved).startsWith(prefix));
    assert.equal(readFileSync(path.join(resolved, '.owner'), 'utf8'), marker);
    rmSync(resolved, { recursive: true });
  });
  connect = createConnectService({ databasePath: paths.connect, projectId: PROJECT, allowedOrigins: [ORIGIN], clock: () => time,
    extensions: [{ operations: new Set(ACCESS_OPERATIONS), execute: request => caps.execute(request) },
      { operations: new Set(NOTES_OPERATIONS), execute: request => notes.execute(request) }] });
  notes = createNotesService({ databasePath: paths.notes, projectId: PROJECT, allowNativeMigration: true, clock: () => time,
    verifyNativeContext: (token, mode) => caps.nativeNotes.verifyContext(token, mode) });
  function openCaps() {
    caps = createCapabilitiesService({ databasePath: paths.caps, projectId: PROJECT, allowNativeMigration: true, clock: () => time,
      actorActive: actor => connect.isActorActive(actor),
      catalog: BUILTIN_CAPABILITIES.map(entry => ({ ...entry, executionEnabled: settings.enabled })),
      nativeNotes: { notes: { ...notes.native, ...settings.port },
        withAuthorityFence: callback => connect.withAuthorityFence(callback) } });
  }
  openCaps();
  async function call(identity, op, args = {}) {
    const challenge = result(await connect.handle({ op: 'challenge', args: { operation: op, digest: digestArgs(args) }, origin: ORIGIN }));
    return result(await connect.handle({ op, args, origin: ORIGIN, proof: {
      publicJwk: identity.publicJwk, challengeId: challenge.challengeId,
      signature: sign('sha256', Buffer.from(challenge.message),
        { key: identity.privateKey, dsaEncoding: 'ieee-p1363' }).toString('base64url') } }));
  }
  const owner = signingIdentity();
  const account = await call(owner, 'bootstrap', { label: 'Independent native owner', encryptionPublicJwk: owner.encryptionPublicJwk });
  const ownerCall = (op, args = {}) => call(owner, op, { expectedAccountId: account.accountId, ...args });
  function sql(store) {
    if (!readers.has(store)) readers.set(store, new DatabaseSync(paths[store], { readOnly: true }));
    return readers.get(store);
  }
  return { get caps() { return caps; }, get notes() { return notes; }, get native() { return caps.nativeNotes; },
    account, ownerCall, sql, paths, now: () => time, time: value => { time = value; },
    reopen(next) { caps.close(); settings = { ...settings, ...next }; openCaps(); },
    actor: token => caps.authenticateCredential({ token, audience: ORIGIN }),
    async issue({ credentialTtlMs } = {}) {
      const { principal } = await ownerCall('access.principals.create', { label: 'Independent external agent' });
      const { grant } = await ownerCall('access.grants.issue', { principalId: principal.id,
        capabilities: [{ capabilityId: 'notes.createDraft', version: 1 }], resources: ['notes:new'],
        effects: ['create'], recipients: ['soty:notes'], expiresAt: time + 3_600_000,
        budget: { unit: 'invocations', limit: 20 } });
      const credential = await ownerCall('access.credentials.issue', { grantId: grant.id, audience: ORIGIN,
        ...(credentialTtlMs === undefined ? {} : { expiresAt: time + credentialTtlMs }) });
      return { principal, grant, credential: credential.credential, token: credential.token,
        actor: caps.authenticateCredential({ token: credential.token, audience: ORIGIN }) };
    }
  };
}

test('temporary operational disable through generic reconciliation cannot become a native user cancellation', async t => {
  const f = await fixture(t), identity = await f.issue();
  const input = { title: 'Отложенная записка', body: 'Текст должен дождаться включения обработчика' };
  const invocationId = f.native.admit({ actor: identity.actor, idempotencyKey: 'temporary_disable_independent', input }).invocation.invocationId;
  f.native.beginAttempt({ invocationId });
  f.reopen({ enabled: false });
  assert.equal(f.caps.invocations.reconcileAuthorization({ invocationId }).authorized, false);
  const held = f.native.reconcile({ invocationId });
  const persisted = f.sql('caps').prepare('SELECT status,cancel_requested,input_json FROM cap_invocations WHERE id=?').get(invocationId);
  t.diagnostic(`disabled reconciliation: outcome=${held.outcome}, status=${persisted.status}, cancel=${persisted.cancel_requested}, inputPurged=${persisted.input_json === 'null'}`);
  assert.equal(held.outcome, 'held', 'operational disable is not a request to cancel the admitted intent');
  assert.equal(persisted.cancel_requested, 0);
  assert.notEqual(persisted.input_json, 'null');
  assert.equal(f.sql('caps').prepare('SELECT count(*) AS n FROM cap_receipts').get().n, 0);
  assert.deepEqual({ ...f.sql('caps').prepare('SELECT reserved_amount,spent_amount FROM cap_budgets').get() },
    { reserved_amount: 1, spent_amount: 0 });
  assert.equal(f.sql('notes').prepare('SELECT count(*) AS n FROM notes').get().n, 0);
  f.reopen({ enabled: true });
  assert.equal(f.native.execute({ invocationId }).outcome, 'committed');
  assert.equal(f.sql('notes').prepare('SELECT count(*) AS n FROM notes').get().n, 1);
  assert.equal(f.native.admit({ actor: f.actor(identity.token), idempotencyKey: 'temporary_disable_independent', input }).reused, true);
});

test('committed Notes proof wins over cancellation and revoked execution credential after an ambiguous return, even after human purge', async t => {
  const lost = new Error('independent_port_return_lost');
  let notesPort, captured;
  const f = await fixture(t, { port: { createDraftForInvocation(args) {
    captured = args.context;
    notesPort.createDraftForInvocation(args); // Real Notes transaction commits before the simulated return loss.
    throw lost;
  } } });
  notesPort = f.notes.native;
  const identity = await f.issue(), input = { title: 'Сохранённая попытка', body: 'Личный исходный текст 🐝 e\u0301' };
  const replacement = await f.ownerCall('access.credentials.issue', { grantId: identity.grant.id, audience: ORIGIN });
  const invocationId = f.native.admit({ actor: identity.actor, idempotencyKey: 'lost_return_proof_independent', input }).invocation.invocationId;
  f.native.beginAttempt({ invocationId });
  assert.throws(() => f.native.execute({ invocationId }), error => error === lost);
  assert.throws(() => f.native.verifyContext(captured, 'create'), errorCode('native_context_invalid'));
  assert.throws(() => notesPort.createDraftForInvocation({ context: captured, input }), errorCode('native_context_invalid'));
  const proof = f.sql('notes').prepare('SELECT * FROM note_native_creates').get();
  assert.ok(proof); assert.equal(f.sql('caps').prepare('SELECT count(*) AS n FROM cap_receipts').get().n, 0);
  assert.equal(f.native.get({ actor: identity.actor, invocationId }).invocation.effectState, 'unknown');
  assert.deepEqual({ ...f.sql('caps').prepare('SELECT reserved_amount,spent_amount FROM cap_budgets').get() },
    { reserved_amount: 1, spent_amount: 0 });
  await f.ownerCall('notes.put', { noteId: proof.note_id, mutationId: 'human_changed_and_trashed', expectedRevision: 1,
    title: 'Правка человеком', body: 'Новый текст не часть квитанции агента', items: [], color: 'plain', pinned: false, state: 'trashed' });
  await f.ownerCall('notes.purge', { noteId: proof.note_id, mutationId: 'human_permanent_purge', expectedRevision: 2 });
  f.caps.invocations.requestCancel({ actor: identity.actor, invocationId });
  await f.ownerCall('access.credentials.revoke', { credentialId: identity.credential.id });
  const completed = f.native.reconcile({ invocationId });
  assert.equal(completed.outcome, 'committed'); assert.equal(completed.invocation.status, 'succeeded');
  assert.equal(completed.invocation.cancelRequested, true);
  assert.deepEqual(completed.invocation.receipt, { verificationMethod: 'domain_read', artifacts: [{ type: 'note', id: proof.note_id, revision: 1 }] });
  assert.throws(() => f.native.get({ actor: identity.actor, invocationId }), errorCode('authorization_required'));
  assert.equal(f.sql('notes').prepare('SELECT state FROM notes WHERE id=?').get(proof.note_id).state, 'deleted');
  assert.equal(f.sql('notes').prepare('SELECT count(*) AS n FROM notes_fts').get().n, 0);
  assert.deepEqual(f.sql('notes').prepare('SELECT * FROM note_native_creates').get(), proof);
  assert.equal(f.sql('caps').prepare('SELECT input_json FROM cap_invocations WHERE id=?').get(invocationId).input_json, 'null');
  assert.deepEqual({ ...f.sql('caps').prepare('SELECT reserved_amount,spent_amount FROM cap_budgets').get() },
    { reserved_amount: 0, spent_amount: 1 });
  // After completion the caller can recover its historical result without reading any Notes state.
  f.reopen({ enabled: false, port: { storageIdentity() { throw new Error('independent_notes_offline'); } } });
  const actor = f.actor(replacement.token);
  const replay = f.native.admit({ actor, idempotencyKey: 'lost_return_proof_independent', input });
  assert.equal(replay.reused, true); assert.deepEqual(replay.invocation, completed.invocation);
  assert.deepEqual(f.native.get({ actor, invocationId }).invocation, completed.invocation);
  assert.throws(() => f.native.admit({ actor, idempotencyKey: 'lost_return_proof_independent',
    input: { ...input, body: 'Другое намерение' } }), errorCode('invocation_request_conflict'));
  const projection = JSON.stringify(replay);
  for (const privateValue of [input.title, input.body, identity.token, replacement.token, proof.source_store_id, proof.input_digest]) {
    assert.equal(projection.includes(privateValue), false);
  }
  assert.equal(f.sql('notes').prepare('SELECT count(*) AS n FROM notes').get().n, 1);
});

test('a new same-grant credential reads the old intent but cannot extend its original effect deadline', async t => {
  const f = await fixture(t), identity = await f.issue({ credentialTtlMs: 1000 });
  const admittedAt = f.now(), input = { title: '', body: 'Не создавать по новому удостоверению' };
  const invocationId = f.native.admit({ actor: identity.actor, idempotencyKey: 'original_deadline_independent', input }).invocation.invocationId;
  f.native.beginAttempt({ invocationId });
  f.time(admittedAt + 1500);
  const replacement = await f.ownerCall('access.credentials.issue', { grantId: identity.grant.id, audience: ORIGIN });
  const actor = f.actor(replacement.token);
  assert.equal(f.native.admit({ actor, idempotencyKey: 'original_deadline_independent', input }).reused, true);
  assert.equal(f.native.get({ actor, invocationId }).invocation.effectState, 'unknown');
  const completed = f.native.execute({ invocationId });
  assert.equal(completed.outcome, 'not_applied'); assert.equal(completed.invocation.status, 'failed');
  assert.equal(completed.invocation.effectState, 'none');
  assert.equal(f.native.get({ actor, invocationId }).invocation.status, 'failed');
  assert.equal(f.sql('notes').prepare('SELECT count(*) AS n FROM note_native_creates').get().n, 0);
  assert.equal(f.sql('notes').prepare('SELECT count(*) AS n FROM notes').get().n, 0);
  assert.deepEqual({ ...f.sql('caps').prepare('SELECT reserved_amount,spent_amount FROM cap_budgets').get() },
    { reserved_amount: 0, spent_amount: 0 });
  f.time(admittedAt + 500);
  assert.equal(f.native.execute({ invocationId }).outcome, 'not_applied', 'a terminal negative never reopens after a clock rollback');
  assert.equal(f.sql('notes').prepare('SELECT count(*) AS n FROM notes').get().n, 0);
});
