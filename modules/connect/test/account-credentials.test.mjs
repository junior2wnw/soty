import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { createClientWithStorage } from '../browser/client.mjs';
import { ConnectError, base64url, credentialLookup, normalizeCredentialInput, unlockCredential } from '../browser/crypto.mjs';
import { validateState } from '../browser/storage.mjs';
import { createConnectService } from '../server/index.mjs';
import { migrateDatabase } from '../server/schema.mjs';

const projectId = 'credentials-test', origin = 'https://credentials.test', endpoint = origin + '/rpc';
const password = { login: 'synthetic-user', secret: 'Moonlit-birch-67-lighthouse' };
const replacement = { ...password, secret: 'Copper-canyon-82-observatory' };
const phrase = { secret: '朝の静かな海と山の向こうに広がる明るい世界を歩いていく' };
function storage() {
  let value = null;
  return { read: async () => value === null ? null : structuredClone(value),
    claim: async candidate => { value ??= structuredClone(candidate); return structuredClone(value); },
    compareAndSwap: async (revision, candidate, options) => {
      if (value?.localRevision !== revision) throw new ConnectError('LOCAL_CONFLICT', 'A synthetic concurrent change occurred.');
      validateState(candidate, { projectId, endpoint }); options?.beforeCommit?.(); value = structuredClone(candidate); return structuredClone(value);
    }, inspect: () => structuredClone(value) };
}
function fixture(t) {
  const service = createConnectService({ databasePath: ':memory:', projectId, allowedOrigins: [origin] });
  const traffic = [], replies = [], clients = []; let lose = null, after = null;
  const fetcher = async (_url, options) => {
    const body = JSON.parse(options.body); traffic.push(body);
    const result = await service.handle({ ...body, origin, peer: 'synthetic-peer' }); replies.push({ op: body.op, ...result });
    if (after) await after(body, result);
    if (lose === body.op && result.ok) { lose = null; throw new TypeError('Synthetic lost acknowledgement'); }
    return new Response(JSON.stringify(result), { status: result.ok ? 200 : 400, headers: { 'Content-Type': 'application/json' } });
  };
  t.after(() => { clients.forEach(client => client.dispose()); service.close(); });
  return { traffic, replies, service, lose(op) { lose = op; }, after(fn) { after = fn; },
    client(store = storage(), options = {}) { const client = createClientWithStorage({ projectId, endpoint, fetch: fetcher, ...options }, store); clients.push(client); return { client, store }; } };
}

test('password entry retains both devices, old local profile, data and independent recovery file', async t => {
  const f = fixture(t), owner = f.client(), recipient = f.client();
  const first = await owner.client.bootstrap('Owner'), before = await recipient.client.bootstrap('Automatic profile');
  await owner.client.saveVault({ marker: 'owner data' }); await recipient.client.saveVault({ marker: 'automatic data' });
  const kit = await owner.client.prepareRecovery(); await owner.client.confirmRecovery(kit);
  const status = await owner.client.setAccountCredential('password', password, first.accountId);
  assert.equal(status.password.enabled, true); assert.equal(status.phrase.enabled, false);
  const joined = await recipient.client.loginWithCredential('password', password, 'Second device');
  assert.equal(joined.accountId, first.accountId); assert.notEqual(joined.deviceId, first.deviceId);
  assert.equal((await owner.client.status()).accountId, first.accountId);
  assert.equal((await owner.client.status()).recovery.recoveryId, kit.recoveryId);
  assert.deepEqual((await recipient.client.loadVault()).payload, { marker: 'owner data' });
  assert.equal((await recipient.client.getLocalState()).profiles.length, 2);
  const reloaded = f.client(recipient.store).client;
  const resumed = await reloaded.loginWithCredential('password', password, 'Second device');
  assert.equal(resumed.deviceId, joined.deviceId); assert.equal(resumed.alreadyCompleted, true);
  await reloaded.switchProfile(before.accountId);
  assert.deepEqual((await reloaded.loadVault()).payload, { marker: 'automatic data' });
  assert.equal(JSON.stringify(f.traffic).includes(password.secret), false);
  assert.equal(JSON.stringify(recipient.store.inspect()).includes(password.secret), false);
});

test('Unicode phrase entry supports unspaced text, NFC and whitespace normalization without an English dictionary', async t => {
  const f = fixture(t), owner = f.client(), recipient = f.client();
  const first = await owner.client.bootstrap('Owner');
  await owner.client.setAccountCredential('phrase', phrase, first.accountId);
  assert.equal((await recipient.client.loginWithCredential('phrase', phrase)).accountId, first.accountId);
  const changed = { secret: '  lumière\t paisible  au bord du lac après la pluie  ' };
  await owner.client.setAccountCredential('phrase', changed, first.accountId);
  const another = f.client();
  assert.equal((await another.client.loginWithCredential('phrase', { secret: changed.secret.normalize('NFD').replaceAll('\t', '\n') })).accountId, first.accountId);
  assert.equal(JSON.stringify(f.traffic).includes(changed.secret), false);
});

test('bad credentials and unknown names fail identically before recipient registration', async t => {
  const f = fixture(t), owner = f.client(), recipient = f.client();
  const first = await owner.client.bootstrap('Owner'), before = await recipient.client.bootstrap('Automatic');
  await owner.client.setAccountCredential('password', password, first.accountId);
  const count = recipient.store.inspect().installations.length;
  for (const input of [{ ...password, secret: 'Incorrect-but-long-secret' }, { ...password, login: 'unknown-person' }]) {
    await assert.rejects(recipient.client.loginWithCredential('password', input), { code: 'credential_unavailable' });
    assert.equal((await recipient.client.getLocalState()).accountId, before.accountId);
    assert.equal(recipient.store.inspect().installations.length, count);
  }
  const beforeTraffic = f.traffic.length;
  await assert.rejects(owner.client.setAccountCredential('password', { ...password, secret: 'passwordpassword' }, first.accountId), { code: 'weak_password' });
  await assert.rejects(owner.client.setAccountCredential('phrase', { secret: '短い' }, first.accountId), { code: 'weak_phrase' });
  assert.equal(f.traffic.length, beforeTraffic);
});

test('another account cannot replace a reserved login or act for a different displayed owner', async t => {
  const f = fixture(t), owner = f.client(), other = f.client();
  const first = await owner.client.bootstrap('Owner'), second = await other.client.bootstrap('Other');
  await owner.client.setAccountCredential('password', password, first.accountId);
  await assert.rejects(other.client.setAccountCredential('password', replacement, second.accountId), { code: 'credential_exists' });
  assert.equal((await other.client.setAccountCredential('password', { ...replacement, login: 'different-person' }, second.accountId)).password.enabled, true);
  await assert.rejects(other.client.removeAccountCredential('password', first.accountId), { code: 'ACTIVE_PROFILE_CHANGED' });
  const recipient = f.client(); assert.equal((await recipient.client.loginWithCredential('password', password)).accountId, first.accountId);
});

test('password rotation and removal leave phrase, file and existing devices usable', async t => {
  const f = fixture(t), owner = f.client(); const first = await owner.client.bootstrap('Owner');
  const kit = await owner.client.prepareRecovery(); await owner.client.confirmRecovery(kit);
  await owner.client.setAccountCredential('password', password, first.accountId);
  await owner.client.setAccountCredential('phrase', phrase, first.accountId);
  const active = f.client(); await active.client.loginWithCredential('password', password);
  await owner.client.setAccountCredential('password', replacement, first.accountId);
  await assert.rejects(f.client().client.loginWithCredential('password', password), { code: 'credential_unavailable' });
  assert.equal((await f.client().client.loginWithCredential('password', replacement)).accountId, first.accountId);
  const removed = await owner.client.removeAccountCredential('password', first.accountId);
  assert.equal(removed.password.enabled, false); assert.equal(removed.phrase.enabled, true);
  await assert.rejects(f.client().client.loginWithCredential('password', replacement), { code: 'credential_unavailable' });
  assert.equal((await active.client.status()).accountId, first.accountId);
  assert.equal((await f.client().client.loginWithCredential('phrase', phrase)).accountId, first.accountId);
  assert.equal((await f.client().client.recover(kit, 'File device')).accountId, first.accountId);
  const byQr = f.client(), request = await byQr.client.startEnrollment('QR device');
  await owner.client.approveEnrollment(request.requestId, first.accountId);
  await byQr.client.previewEnrollment(request.requestId);
  assert.equal((await byQr.client.finishEnrollment(request.requestId, first.accountId)).accountId, first.accountId);
});

test('lost setup and login acknowledgements resume the same durable intents and recipient after reload', async t => {
  const f = fixture(t), owner = f.client(), recipient = f.client();
  const first = await owner.client.bootstrap('Owner'), previous = await recipient.client.bootstrap('Automatic');
  f.lose('credentials.set'); await assert.rejects(owner.client.setAccountCredential('password', password, first.accountId), { code: 'NETWORK_ERROR' });
  const reloadOwner = f.client(owner.store).client;
  assert.equal((await reloadOwner.setAccountCredential('password', password, first.accountId)).password.revision, 1);
  const set = f.traffic.filter(item => item.op === 'credentials.set'); assert.equal(set.length, 2);
  assert.deepEqual(set[0].args, set[1].args);
  f.lose('credentials.login'); await assert.rejects(recipient.client.loginWithCredential('password', password), { code: 'NETWORK_ERROR' });
  assert.equal((await recipient.client.getLocalState()).accountId, previous.accountId);
  const before = recipient.store.inspect(), pending = before.extensions['connect.browser.credentials.v1'].logins[0];
  f.lose('credentials.remove'); await assert.rejects(reloadOwner.removeAccountCredential('password', first.accountId), { code: 'NETWORK_ERROR' });
  await f.client(owner.store).client.removeAccountCredential('password', first.accountId);
  const removes = f.traffic.filter(item => item.op === 'credentials.remove'); assert.equal(removes.length, 2); assert.deepEqual(removes[0].args, removes[1].args);
  const reloaded = f.client(recipient.store).client, opened = await reloaded.loginWithCredential('password', password);
  assert.equal(opened.deviceId, pending.deviceId); assert.equal(opened.accountId, first.accountId);
  assert.equal(recipient.store.inspect().installations.length, before.installations.length);
  const logins = f.traffic.filter(item => item.op === 'credentials.login'); assert.equal(logins.length, 2);
  assert.deepEqual(logins[0].args, logins[1].args); assert.deepEqual(logins[0].proof.publicJwk, logins[1].proof.publicJwk);
  await reloadOwner.revokeDevice(opened.deviceId);
  await assert.rejects(reloaded.loginWithCredential('password', password), { code: 'credential_unavailable' });
});

test('the authenticated owner can replace an acknowledged-lost setup with a different password without knowing the previous secret', async t => {
  const f = fixture(t), owner = f.client(); const account = await owner.client.bootstrap('Owner');
  await owner.client.saveVault({ marker: 'owner data' });
  f.lose('credentials.set');
  await assert.rejects(owner.client.setAccountCredential('password', password, account.accountId), { code: 'NETWORK_ERROR' });
  const previous = owner.store.inspect().extensions['connect.browser.credentials.v1'].actions[0];
  const reloaded = f.client(owner.store).client;
  const changed = await reloaded.setAccountCredential('password', replacement, account.accountId);
  assert.equal(changed.password.revision, previous.args.expectedRevision + 2);
  assert.equal(owner.store.inspect().extensions['connect.browser.credentials.v1'].actions.length, 0);
  const changes = f.traffic.filter(item => item.op === 'credentials.set');
  assert.equal(changes.length, 2); assert.notEqual(changes[1].args.requestId, previous.args.requestId);
  assert.equal(changes[1].args.expectedRevision, previous.args.expectedRevision + 1);
  await assert.rejects(reloaded.extension('credentials.set', previous.args, { expectedAccountId: account.accountId }), { code: 'revision_conflict' });
  await assert.rejects(f.client().client.loginWithCredential('password', password), { code: 'credential_unavailable' });
  assert.equal((await f.client().client.loginWithCredential('password', replacement)).accountId, account.accountId);
  assert.deepEqual((await reloaded.loadVault()).payload, { marker: 'owner data' });
});

test('an acknowledged-lost removal can be replaced by a new owner setup even when the disabled method has no remove button', async t => {
  const f = fixture(t), owner = f.client(), recipient = f.client();
  const account = await owner.client.bootstrap('Owner');
  await owner.client.setAccountCredential('password', password, account.accountId);
  f.lose('credentials.remove');
  await assert.rejects(owner.client.removeAccountCredential('password', account.accountId), { code: 'NETWORK_ERROR' });
  assert.equal((await owner.client.credentialStatus(account.accountId)).password.enabled, false);
  const replacement = { ...password, secret: 'A separate long replacement credential' };
  const reloaded = f.client(owner.store).client;
  await reloaded.setAccountCredential('password', replacement, account.accountId);
  assert.equal((await recipient.client.loginWithCredential('password', replacement)).accountId, account.accountId);
});

test('a different password cannot replace an uncertain setup before its signed revision advances', async t => {
  const f = fixture(t), owner = f.client(); const account = await owner.client.bootstrap('Owner');
  f.after((body, result) => {
    if (body.op === 'challenge' && body.args.operation === 'credentials.set' && result.ok) {
      f.after(null); throw new TypeError('Synthetic lost challenge before the setup was sent');
    }
  });
  await assert.rejects(owner.client.setAccountCredential('password', password, account.accountId), { code: 'NETWORK_ERROR' });
  const previous = owner.store.inspect().extensions['connect.browser.credentials.v1'].actions[0];
  const reloaded = f.client(owner.store).client;
  await assert.rejects(reloaded.setAccountCredential('password', replacement, account.accountId), { code: 'credential_unavailable' });
  assert.deepEqual(owner.store.inspect().extensions['connect.browser.credentials.v1'].actions[0], previous);
  assert.equal(f.traffic.some(item => item.op === 'credentials.set'), false);
  assert.equal((await reloaded.credentialStatus(account.accountId)).password.revision, previous.args.expectedRevision);
  assert.equal((await reloaded.setAccountCredential('password', password, account.accountId)).password.revision, previous.args.expectedRevision + 1);
  assert.deepEqual(f.traffic.find(item => item.op === 'credentials.set').args, previous.args);
});

test('closing during credential registration keeps the old selection and resumes the same remotely finished recipient', async t => {
  const f = fixture(t), owner = f.client(), recipient = f.client(), close = new AbortController();
  const first = await owner.client.bootstrap('Owner'), old = await recipient.client.bootstrap('Automatic');
  await owner.client.setAccountCredential('password', password, first.accountId);
  f.after((body, result) => { if (body.op === 'credentials.login' && result.ok) close.abort(); });
  await assert.rejects(recipient.client.loginWithCredential('password', password, 'Second', { signal: close.signal }), { code: 'ACTIVE_PROFILE_CHANGED' });
  assert.equal((await recipient.client.getLocalState()).accountId, old.accountId);
  const pending = recipient.store.inspect().extensions['connect.browser.credentials.v1'].logins[0]; f.after(null);
  const opened = await f.client(recipient.store).client.loginWithCredential('password', password, 'Second');
  assert.equal(opened.deviceId, pending.deviceId); assert.equal(opened.accountId, first.accountId);
});

test('a cross-tab A-B-A during credential lookup cannot select another account or register a recipient', async t => {
  const f = fixture(t), owner = f.client(), recipient = f.client();
  const first = await owner.client.bootstrap('Owner'), old = await recipient.client.bootstrap('Automatic');
  await owner.client.setAccountCredential('password', password, first.accountId);
  const other = f.client(recipient.store).client;
  const kit = await owner.client.prepareRecovery(); await owner.client.confirmRecovery(kit); await other.recover(kit, 'Other saved profile');
  await other.switchProfile(old.accountId);
  const count = recipient.store.inspect().installations.length;
  f.after(async (body, result) => { if (body.op === 'credentials.lookup' && result.ok) { f.after(null); await other.switchProfile(first.accountId); await other.switchProfile(old.accountId); } });
  await assert.rejects(recipient.client.loginWithCredential('password', password), { code: 'ACTIVE_PROFILE_CHANGED' });
  assert.equal((await recipient.client.getLocalState()).accountId, old.accountId);
  assert.equal(recipient.store.inspect().installations.length, count);
  assert.equal(f.traffic.some(item => item.op === 'credentials.login'), false);
});

test('credential envelopes reject altered project, account, kind, lookup, salt, iterations and ciphertext', async t => {
  const f = fixture(t), owner = f.client(); const first = await owner.client.bootstrap('Owner');
  await owner.client.setAccountCredential('password', password, first.accountId);
  const input = normalizeCredentialInput('password', password), lookup = await credentialLookup(input, projectId);
  const record = await owner.client.extension('credentials.lookup', { kind: 'password', lookup });
  for (const [key, value] of [
    ['projectId', 'other-project'], ['accountId', 'acct_other'], ['kind', 'phrase'],
    ['lookup', base64url(new Uint8Array(32))], ['salt', base64url(new Uint8Array(32))], ['iterations', 1],
    ['wrappedKey', { ...record.wrappedKey, ciphertext: base64url(new Uint8Array(48)) }],
  ]) await assert.rejects(unlockCredential({ ...record, [key]: value }, input, { projectId }), { code: 'credential_unavailable' });
});

test('real and unknown credential descriptors have the same shape and unknown records are stable', async t => {
  const f = fixture(t), owner = f.client(); const first = await owner.client.bootstrap('Owner');
  await owner.client.setAccountCredential('password', password, first.accountId);
  const knownLookup = await credentialLookup(normalizeCredentialInput('password', password), projectId);
  const unknownLookup = await credentialLookup(normalizeCredentialInput('password', { ...password, login: 'nobody-here' }), projectId);
  const known = await owner.client.extension('credentials.lookup', { kind: 'password', lookup: knownLookup });
  const missing = await owner.client.extension('credentials.lookup', { kind: 'password', lookup: unknownLookup });
  const again = await owner.client.extension('credentials.lookup', { kind: 'password', lookup: unknownLookup });
  assert.deepEqual(missing, again); assert.deepEqual(Object.keys(known).sort(), Object.keys(missing).sort());
  assert.deepEqual(Object.keys(known.wrappedKey).sort(), Object.keys(missing.wrappedKey).sort());
  for (const key of ['credentialId', 'accountId', 'salt']) assert.equal(known[key].length, missing[key].length);
  assert.equal('verifier' in known, false); assert.equal('verifier' in missing, false);
});

test('signed invalid credential attempts consume persisted abuse counters outside the rolled-back action', async t => {
  const f = fixture(t), owner = f.client(); const first = await owner.client.bootstrap('Owner');
  await owner.client.setAccountCredential('password', password, first.accountId);
  const input = normalizeCredentialInput('password', password), lookup = await credentialLookup(input, projectId);
  const record = await owner.client.extension('credentials.lookup', { kind: 'password', lookup });
  const args = { kind: 'password', lookup, credentialId: record.credentialId, authentication: base64url(new Uint8Array(32)),
    encryptionPublicJwk: owner.store.inspect().installations[0].encryptionPublicJwk, label: 'Invalid sign-in' };
  for (let index = 0; index < 39; index++) await assert.rejects(owner.client.extension('credentials.login', args), { code: 'credential_unavailable' });
  await assert.rejects(owner.client.extension('credentials.login', args), { code: 'credential_rate_limited' });
  await assert.rejects(owner.client.extension('credentials.login', args), { code: 'credential_rate_limited' });
  assert.equal((await owner.client.status()).accountId, first.accountId);
});

test('a stale unacknowledged setup cannot roll back a newer password from another owner device', async t => {
  const f = fixture(t), owner = f.client(), linked = f.client(); const first = await owner.client.bootstrap('Owner');
  const request = await linked.client.startEnrollment('Other owner device');
  await owner.client.approveEnrollment(request.requestId, first.accountId); await linked.client.previewEnrollment(request.requestId);
  await linked.client.finishEnrollment(request.requestId, first.accountId);
  f.lose('credentials.set'); await assert.rejects(owner.client.setAccountCredential('password', password, first.accountId), { code: 'NETWORK_ERROR' });
  await linked.client.setAccountCredential('password', replacement, first.accountId);
  await assert.rejects(f.client(owner.store).client.setAccountCredential('password', password, first.accountId), { code: 'revision_conflict' });
  assert.equal(owner.store.inspect().extensions['connect.browser.credentials.v1'].actions.length, 0);
  assert.equal((await f.client().client.loginWithCredential('password', replacement)).accountId, first.accountId);
  await assert.rejects(f.client().client.loginWithCredential('password', password), { code: 'credential_unavailable' });
});

test('closing while the local compare-and-swap is queued cannot activate a remotely finished QR recipient', async t => {
  const f = fixture(t), owner = f.client(), close = new AbortController(), base = storage();
  let queued, release;
  const entered = new Promise(resolve => { queued = resolve; }), waiting = new Promise(resolve => { release = resolve; });
  const delayed = { ...base, async compareAndSwap(revision, candidate, options) {
    if (options?.beforeCommit) { queued(); await waiting; }
    return base.compareAndSwap(revision, candidate, options);
  } };
  const recipient = f.client(delayed), first = await owner.client.bootstrap('Owner'), old = await recipient.client.bootstrap('Automatic');
  const request = await recipient.client.startEnrollment('QR device');
  await owner.client.approveEnrollment(request.requestId, first.accountId); await recipient.client.previewEnrollment(request.requestId);
  const connecting = recipient.client.finishEnrollment(request.requestId, first.accountId, { signal: close.signal });
  await entered; close.abort(); release();
  await assert.rejects(connecting, { code: 'ACTIVE_PROFILE_CHANGED' });
  assert.equal((await recipient.client.getLocalState()).accountId, old.accountId);
  assert.equal((await recipient.client.previewEnrollment(request.requestId)).status, 'finished');
  const resumed = f.client(base).client; await resumed.previewEnrollment(request.requestId);
  const opened = await resumed.finishEnrollment(request.requestId, first.accountId);
  assert.equal(opened.accountId, first.accountId); assert.equal((await owner.client.status()).accountId, first.accountId);
});

test('an abort from committed account notification does not turn successful activation into failure', async t => {
  const f = fixture(t), owner = f.client(), close = new AbortController(); const first = await owner.client.bootstrap('Owner');
  const recipient = f.client(storage(), { onState: state => { if (state.accountId === first.accountId) close.abort(); } });
  await recipient.client.bootstrap('Automatic');
  const request = await recipient.client.startEnrollment('QR device');
  await owner.client.approveEnrollment(request.requestId, first.accountId); await recipient.client.previewEnrollment(request.requestId);
  const opened = await recipient.client.finishEnrollment(request.requestId, first.accountId, { signal: close.signal });
  assert.equal(close.signal.aborted, true); assert.equal(opened.accountId, first.accountId);
  assert.equal((await recipient.client.getLocalState()).accountId, first.accountId);
});

test('a remotely revoked unacknowledged credential recipient cannot be reactivated by its saved receipt', async t => {
  const f = fixture(t), owner = f.client(), recipient = f.client();
  const first = await owner.client.bootstrap('Owner'), old = await recipient.client.bootstrap('Automatic');
  await owner.client.setAccountCredential('password', password, first.accountId);
  f.lose('credentials.login'); await assert.rejects(recipient.client.loginWithCredential('password', password), { code: 'NETWORK_ERROR' });
  const pending = recipient.store.inspect().extensions['connect.browser.credentials.v1'].logins[0];
  await owner.client.revokeDevice(pending.deviceId);
  await assert.rejects(f.client(recipient.store).client.loginWithCredential('password', password), { code: 'device_revoked' });
  assert.equal((await recipient.client.getLocalState()).accountId, old.accountId);
});

test('QR preview and remote finish reject A-B-A local selection changes and preserve one explicit resume', async t => {
  const f = fixture(t), owner = f.client(), recipient = f.client(), other = f.client();
  const first = await owner.client.bootstrap('Owner'), old = await recipient.client.bootstrap('Automatic'), third = await other.client.bootstrap('Other saved account');
  const kit = await other.client.prepareRecovery(); await other.client.confirmRecovery(kit);
  await recipient.client.recover(kit, 'Other saved account here'); await recipient.client.switchProfile(old.accountId);
  const parallel = f.client(recipient.store).client, request = await recipient.client.startEnrollment('QR device');
  await owner.client.approveEnrollment(request.requestId, first.accountId);
  const change = async () => { f.after(null); await parallel.switchProfile(third.accountId); await parallel.switchProfile(old.accountId); };
  f.after((body, result) => body.op === 'enrollment.preview' && result.ok ? change() : undefined);
  await assert.rejects(recipient.client.previewEnrollment(request.requestId), { code: 'ACTIVE_PROFILE_CHANGED' });
  await recipient.client.previewEnrollment(request.requestId);
  f.after((body, result) => body.op === 'enrollment.finish' && result.ok ? change() : undefined);
  await assert.rejects(recipient.client.finishEnrollment(request.requestId, first.accountId), { code: 'ACTIVE_PROFILE_CHANGED' });
  assert.equal((await recipient.client.getLocalState()).accountId, old.accountId);
  const resumed = f.client(recipient.store).client;
  assert.equal((await resumed.previewEnrollment(request.requestId)).status, 'finished');
  const opened = await resumed.finishEnrollment(request.requestId, first.accountId);
  assert.equal(opened.accountId, first.accountId); assert.equal((await owner.client.status()).accountId, first.accountId);
});

test('credential layout rejects same-name lookup indexes with wrong columns or predicate and a partial credential ID index', () => {
  for (const replacement of [
    'CREATE UNIQUE INDEX credential_lookup ON credential_methods(account_id) WHERE enabled=1',
    'CREATE UNIQUE INDEX credential_lookup ON credential_methods(kind,lookup) WHERE enabled=0',
  ]) {
    const db = new DatabaseSync(':memory:');
    try { migrateDatabase(db, projectId); db.exec('DROP INDEX credential_lookup; ' + replacement);
      assert.throws(() => migrateDatabase(db, projectId), /connect_schema_layout_invalid/u); }
    finally { db.close(); }
  }
  const db = new DatabaseSync(':memory:');
  try {
    migrateDatabase(db, projectId);
    const ddl = db.prepare("SELECT sql FROM sqlite_schema WHERE name='credential_methods'").get().sql;
    db.exec('DROP TABLE credential_methods; ' + ddl.replace('id TEXT NOT NULL UNIQUE', 'id TEXT NOT NULL')
      + '; CREATE UNIQUE INDEX credential_lookup ON credential_methods(kind,lookup) WHERE enabled=1;'
      + 'CREATE UNIQUE INDEX partial_credential_id ON credential_methods(id) WHERE enabled=1;');
    assert.throws(() => migrateDatabase(db, projectId), /connect_schema_layout_invalid/u);
  } finally { db.close(); }
});

test('closing file recovery after its remote receipt preserves the old selection and the same recipient on explicit resume', async t => {
  const f = fixture(t), owner = f.client(), recipient = f.client(), close = new AbortController();
  const first = await owner.client.bootstrap('Owner'), old = await recipient.client.bootstrap('Automatic');
  const kit = await owner.client.prepareRecovery(); await owner.client.confirmRecovery(kit);
  f.after((body, result) => { if (body.op === 'recovery.use' && result.ok) close.abort(); });
  await assert.rejects(recipient.client.recover(kit, 'File device', { signal: close.signal }), { code: 'ACTIVE_PROFILE_CHANGED' });
  assert.equal((await recipient.client.getLocalState()).accountId, old.accountId);
  const saved = recipient.store.inspect().pendingRecovery; assert.ok(saved?.deviceId);
  f.after(null);
  const opened = await f.client(recipient.store).client.recover(kit, 'File device');
  assert.equal(opened.accountId, first.accountId); assert.equal(opened.deviceId, saved.deviceId);
  assert.equal((await owner.client.status()).accountId, first.accountId);
});
