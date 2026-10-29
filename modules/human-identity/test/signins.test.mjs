import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { environment } from './support/renewal-fixture.mjs';
import { createHumanIdentityService } from '../service.mjs';
import { createHumanSigninManagement } from '../signins.mjs';

const LIST = 'identity.human.signins.list', REVOKE = 'identity.human.signins.revoke', CHECK = 'identity.human.signins.checkRevoke';
const list = (f, signer = f.actor, extra = {}) => signer.client.extension(LIST,
  { expectedAccountId: signer.account.accountId, scope: 'current-device', ...extra }, { expectedAccountId: signer.account.accountId });
const intent = (signer, entry, scope = 'current-device') => ({ expectedAccountId: signer.account.accountId, scope,
  entryId: entry.entryId, expectedRevision: entry.revision, requestId: entry.revokeRequestId });
const revoke = (f, args, signer = f.actor) => signer.client.extension(REVOKE, args, { expectedAccountId: signer.account.accountId });
const check = (f, args, signer = f.actor) => signer.client.extension(CHECK, args, { expectedAccountId: signer.account.accountId });
const code = name => error => error.code === name;
const rows = (f, action) => { const db = new DatabaseSync(f.identityPath, { readOnly: true }); try { return action(db); } finally { db.close(); } };
async function remember(f, signer = f.actor, rp = f.rps[0]) {
  const flow = await f.begin(rp, f.freshWire());
  await f.approve(flow, signer, { stayInAppSeconds: 86400, rememberForDevice: true });
  const finished = await f.complete(flow); assert.equal((await flow.session.request(finished.callback)).status, 200);
  return flow;
}

for (const deviceConsent of [false, true]) test(`ordinary short OIDC expiry uses actual artifacts in schema ${deviceConsent ? 3 : 2}`, { timeout: 20000 }, async t => {
  const f = await environment(t, { deviceConsent });
  await f.login(f.rps[0], f.actor, f.freshWire());
  const entry = (await list(f)).entries[0];
  const stored = rows(f, db => db.prepare(`SELECT b.stay_in_app_seconds,b.session_expires_at,max(a.expires_at) AS expires
    FROM human_identity_grant_bindings b JOIN human_identity_artifacts a ON a.grant_hash=b.grant_hash
    WHERE a.model IN ('Grant','AccessToken') GROUP BY b.grant_hash`).get());
  assert.equal(stored.stay_in_app_seconds, 0); assert.equal(stored.session_expires_at, null);
  assert.equal(entry.renewable, false); assert.equal(entry.status, 'active');
  assert.equal(entry.expiresAt, stored.expires * 1000); assert.ok(entry.expiresAt > Date.now());
});

test('list fingerprints the bounded ledger but projects only its requested page, with no expiry SQL per family', { timeout: 20000 }, async t => {
  const f = await environment(t);
  for (let i = 0; i < 4; i++) await f.login(f.rps[0], f.actor, f.freshWire());
  const db = new DatabaseSync(f.identityPath, { readOnly: true }), encryption = f.profile.encryptionKey();
  let statements = 0, clients = 0, devices = 0;
  const port = { get isTransaction() { return db.isTransaction; }, prepare(sql) { statements++; return db.prepare(sql); } };
  const management = createHumanSigninManagement({ db: port, key: encryption.key, keyId: encryption.keyId,
    issuer: f.issuer, schemaVersion: 2, epoch: () => Math.floor(Date.now() / 1000), rowLimits: { grantBindings: 4096, consents: 4096 },
    clientAuthority() { clients++; return null; }, actorActive() { devices++; return true; } });
  try {
    db.exec('BEGIN');
    const page = management.execute(LIST, f.actor.account, { expectedAccountId: f.actor.account.accountId, scope: 'current-device', limit: 1 });
    assert.equal(page.entries.length, 1); assert.ok(page.nextCursor);
    assert.equal(statements, 2, 'one joined family expiry read and one current-head snapshot read, independent of family count');
    assert.equal(clients, 1); assert.equal(devices, 1, 'live metadata is projected only for the requested page');
  } finally { if (db.isTransaction) db.exec('ROLLBACK'); db.close(); encryption.key.fill(0); }
});

test('a full20row UTF8 page fits its own response budget without widening OAuth artifact limits', { timeout: 30000 }, async t => {
  const f = await environment(t);
  await f.configureClients(clients => clients.map(client => ({ ...client, version: client.version + 1, label: '界'.repeat(120) })));
  for (let i = 0; i < 20; i++) await f.login(f.rps[0], f.actor, f.freshWire());
  const page = await list(f);
  assert.equal(page.entries.length, 20); assert.equal(page.nextCursor, null);
  const bytes = Buffer.byteLength(JSON.stringify(page));
  assert.ok(bytes > 16384 && bytes <= 32768, 'large API DTO is distinct from a16KiB OAuth token artifact');
});

test('real signed Human management lists only issuer authorization metadata and closes one exact family', { timeout: 20000 }, async t => {
  t.mock.timers.enable({ apis: ['Date'], now: Math.floor(Date.now() / 1000) * 1000 });
  const f = await environment(t);
  await f.login(f.rps[0], f.actor, f.freshWire(), undefined, 86400);
  await f.login(f.rps[1], f.actor, f.freshWire(), undefined, 86400);
  const before = await list(f);
  assert.equal(before.schema, 'soty.human-signins.v1'); assert.equal(before.entries.length, 2);
  assert.equal(before.capabilities.available, true); assert.equal(before.capabilities.rememberedConsents, false);
  assert.equal(before.capabilities.sourceRootSessions, false);
  const selected = before.entries.find(entry => entry.clientId === f.rps[0].clientId);
  assert.equal(selected.revokeScope, 'exact-session'); assert.equal(selected.renewable, true);
  assert.equal(selected.expiresAt - selected.createdAt, 86400000);
  assert.doesNotMatch(JSON.stringify(before), /grant_hash|grantId|accessToken|refreshToken|clientSecret|browserNonce|cookieToken|privateJwk/u);
  const secret = f.rps[0].verificationFixture().refreshToken;
  assert.equal(JSON.stringify(before).includes(secret), false);
  assert.equal((await list(f, f.outsider)).entries.length, 0);
  await assert.rejects(revoke(f, intent(f.outsider, selected), f.outsider), code('human_identity_decision_conflict'));
  const args = intent(f.actor, selected), closed = await revoke(f, args);
  assert.equal(closed.outcome, 'revoked'); assert.equal(closed.entryId, selected.entryId);
  assert.equal((await f.refresh(f.rps[0], secret)).body.error, 'invalid_grant');
  assert.equal((await f.refresh(f.rps[1], f.rps[1].verificationFixture().refreshToken)).status, 200);
  assert.equal((await list(f)).entries.some(entry => entry.entryId === selected.entryId), false);
  await f.restart();
  const replay = await revoke(f, args);
  assert.equal(replay.outcome, 'already_revoked'); assert.equal(replay.revokedAt, closed.revokedAt);
  await assert.rejects(revoke(f, { ...args, entryId: before.entries.find(entry => entry !== selected).entryId }), code('human_identity_decision_conflict'));
});

test('session expiry stays anchored to approval when grant issuance crosses a second', { timeout: 20000 }, async t => {
  const start = Math.floor(Date.now() / 1000) * 1000;
  t.mock.timers.enable({ apis: ['Date'], now: start });
  const f = await environment(t), flow = await f.begin(f.rps[0], f.freshWire());
  await f.approve(flow, f.actor, { stayInAppSeconds: 86400 });
  t.mock.timers.tick(1000);
  const completed = await f.complete(flow);
  assert.equal((await flow.session.request(completed.callback)).status, 200);
  const selected = (await list(f)).entries[0];
  assert.equal(selected.createdAt, start + 1000);
  assert.equal(selected.expiresAt, start + 86400000);
  assert.equal(selected.expiresAt - selected.createdAt, 86399000);
});

test('current-device and explicit account scopes preserve the original caller while managing its other device', { timeout: 20000 }, async t => {
  const f = await environment(t, { deviceConsent: true }), other = await f.backupDevice();
  await remember(f); await remember(f, other, f.rps[1]);
  const own = await list(f), all = await list(f, f.actor, { scope: 'account' });
  assert.ok(own.entries.every(entry => entry.currentDevice && entry.deviceId === f.actor.account.deviceId));
  const foreignDevice = all.entries.find(entry => entry.kind === 'remembered-consent' && !entry.currentDevice);
  assert.ok(foreignDevice); assert.equal(foreignDevice.clientId, f.rps[1].clientId);
  await assert.rejects(revoke(f, intent(f.actor, foreignDevice)), code('human_identity_decision_conflict'));
  assert.equal((await revoke(f, intent(f.actor, foreignDevice, 'account'))).revokeScope, 'app-device-version');
  const after = await list(f, f.actor, { scope: 'account' });
  assert.ok(after.entries.some(entry => entry.currentDevice && entry.kind === 'remembered-consent'));
  assert.equal(after.entries.some(entry => entry.deviceId === other.account.deviceId && entry.clientId === f.rps[1].clientId), false);
  const counts = rows(f, db => db.prepare('SELECT account_id,device_id,revoked_at FROM human_identity_device_consents ORDER BY client_id').all());
  assert.equal(counts[0].revoked_at, null); assert.ok(counts[1].revoked_at);
  await f.actor.client.revokeDevice(other.account.deviceId, f.actor.account.accountId);
  await assert.rejects(list(f, other), error => ['device_revoked', 'revoked_device'].includes(error.code));
});

test('exact family close retains remembered consent; explicit forget closes tuple families and staged old approvals, never another RP', { timeout: 20000 }, async t => {
  const f = await environment(t, { deviceConsent: true }); await remember(f);
  const firstFamily = (await list(f)).entries.find(entry => entry.kind === 'oidc-session' && entry.clientId === f.rps[0].clientId);
  await remember(f); const secondToken = f.rps[0].verificationFixture().refreshToken;
  await remember(f, f.actor, f.rps[1]);
  await revoke(f, intent(f.actor, firstFamily));
  assert.equal((await f.refresh(f.rps[0], secondToken)).status, 200, 'another family of the very same RP remains usable');
  assert.ok((await list(f)).entries.some(entry => entry.kind === 'remembered-consent' && entry.clientId === f.rps[0].clientId));
  const flow = await f.begin(f.rps[0], f.freshWire());
  const continued = await f.actor.client.extension('identity.human.continue', { expectedAccountId: f.actor.account.accountId,
    interactionId: flow.context.interactionId, browserNonce: flow.context.browserNonce, csrf: flow.context.csrf, requestId: 'remembered-stage-before-forget' },
  { expectedAccountId: f.actor.account.accountId });
  assert.equal(continued.continued, true);
  const entries = (await list(f)).entries;
  const consent = entries.find(entry => entry.kind === 'remembered-consent' && entry.clientId === f.rps[0].clientId);
  assert.equal((await revoke(f, intent(f.actor, consent))).outcome, 'revoked');
  const completion = await flow.session.request(flow.location.href + '/complete', { fields: { csrf: flow.context.csrf } });
  assert.ok([400, 403].includes(completion.status));
  assert.equal((await f.refresh(f.rps[0], f.rps[0].verificationFixture().refreshToken)).body.error, 'invalid_grant');
  assert.ok((await list(f)).entries.some(entry => entry.clientId === f.rps[1].clientId));
  const again = await f.begin(f.rps[0], f.freshWire());
  const decision = await f.actor.client.extension('identity.human.continue', { expectedAccountId: f.actor.account.accountId,
    interactionId: again.context.interactionId, browserNonce: again.context.browserNonce, csrf: again.context.csrf, requestId: 'remembered-after-forget' },
  { expectedAccountId: f.actor.account.accountId });
  assert.equal(decision.continued, false);
});

test('forgotten consent replay after regrant and client version change never closes a new entry', { timeout: 20000 }, async t => {
  const f = await environment(t, { deviceConsent: true }); await remember(f);
  const entry = (await list(f)).entries.find(value => value.kind === 'remembered-consent'), args = intent(f.actor, entry);
  await revoke(f, args); assert.equal((await revoke(f, args)).outcome, 'already_revoked');
  await remember(f);
  await assert.rejects(revoke(f, args), error => ['human_identity_signin_unavailable', 'human_identity_signins_changed'].includes(error.code));
  assert.ok((await list(f)).entries.some(value => value.kind === 'remembered-consent' && value.entryId !== entry.entryId));
  const oldVersion = (await list(f)).entries.find(value => value.kind === 'remembered-consent');
  const oldVersionArgs = intent(f.actor, oldVersion);
  await f.clientVersion(0, 3);
  const before = (await list(f)).entries;
  assert.ok(before.some(value => value.status === 'client-changed'));
  await remember(f);
  const fresh = (await list(f)).entries.filter(value => value.clientGeneration > entry.clientGeneration);
  assert.ok(fresh.length);
  await assert.rejects(revoke(f, args), error => ['human_identity_signin_unavailable', 'human_identity_signins_changed'].includes(error.code));
  assert.deepEqual((await list(f)).entries.filter(value => value.clientGeneration > entry.clientGeneration), fresh);
  await revoke(f, oldVersionArgs);
  assert.deepEqual((await list(f)).entries.filter(value => value.clientGeneration > entry.clientGeneration), fresh,
    'closing an older still-retained client version cannot affect its new scope');
});

test('a lost revocation ACK after restart reads its exact tombstone, while a compacted family remains unavailable', { timeout: 20000 }, async t => {
  const now = Date.now, start = now(); let advance = 0;
  Date.now = () => start + advance * 1000; t.after(() => { Date.now = now; });
  let drop = false;
  const f = await environment(t, { serviceDecorator(service) { return { ...service, execute(request) {
    const result = service.execute(request);
    if (drop && request.op === REVOKE) { drop = false; throw Object.assign(Error('synthetic lost ACK'), { code: 'ack_lost' }); }
    return result;
  } }; } });
  await f.login(f.rps[0], f.actor, f.freshWire(), undefined, 86400);
  const entry = (await list(f)).entries[0], args = intent(f.actor, entry);
  drop = true; await assert.rejects(revoke(f, args), code('ack_lost'));
  assert.equal(rows(f, db => db.prepare('SELECT revoked_at FROM human_identity_grant_bindings').get().revoked_at) !== null, true);
  await f.restart(); assert.equal((await revoke(f, args)).outcome, 'already_revoked');
  advance = 90401;
  for (let i = 0; i < 4; i++) f.identity.compactExpiredRuntime();
  assert.equal(rows(f, db => db.prepare('SELECT count(*) AS n FROM human_identity_grant_bindings').get().n), 0);
  await assert.rejects(revoke(f, args), code('human_identity_signin_unavailable'));
  assert.equal((await check(f, args)).status, 'unavailable', 'GC is not a revocation receipt');
});

test('exact unknown-intent check is read-only before commit, after lost ACK, and after a consent ABA', { timeout: 20000 }, async t => {
  const f = await environment(t, { deviceConsent: true }); await remember(f);
  const entry = (await list(f)).entries.find(value => value.kind === 'remembered-consent'), args = intent(f.actor, entry);
  const snapshot = () => rows(f, db => JSON.stringify(['human_identity_grant_bindings', 'human_identity_device_consents',
    'human_identity_interactions', 'human_identity_artifacts', 'human_identity_decisions']
    .map(table => [table, db.prepare('SELECT * FROM ' + table + ' ORDER BY rowid').all()])));
  const before = snapshot();
  assert.equal((await check(f, args)).status, 'active'); assert.equal((await check(f, args)).status, 'active');
  assert.equal(snapshot(), before, 'readback cannot apply the original revoke');
  await revoke(f, args);
  const closed = snapshot();
  const result = await check(f, args); assert.equal(result.schema, 'soty.human-signin-status.v1');
  assert.equal(result.status, 'revoked'); assert.ok(result.revokedAt);
  assert.equal(snapshot(), closed, 'tombstone read cannot change a family, consent, request or SDK artifact');
  await assert.rejects(check(f, args, f.outsider), code('human_identity_account_mismatch'));
  await remember(f);
  const granted = snapshot(); assert.equal((await check(f, args)).status, 'unavailable');
  assert.equal(snapshot(), granted, 'old original intent cannot close or confirm the new grant');
});

test('loss of the original actor at the final synchronous Human fence rolls back all revocation effects', { timeout: 20000 }, async t => {
  const f = await environment(t, { deviceConsent: true }); await remember(f);
  const entry = (await list(f)).entries.find(value => value.kind === 'remembered-consent'), args = intent(f.actor, entry);
  let checks = 0;
  // Actor proof/HTTP is exercised above. Here the real SQLite service receives
  // controlled private host ports solely to force its final transaction fence.
  const service = createHumanIdentityService({ databasePath: f.identityPath, profile: f.profile,
    actorActive: () => ++checks === 1, withAuthorityFence: action => action(), readProfile: () => ({}) });
  try {
    assert.throws(() => service.execute({ op: REVOKE, actor: f.actor.account, args }), code('human_identity_actor_revoked'));
    assert.equal(checks, 2);
    assert.equal(rows(f, db => db.prepare('SELECT count(*) AS n FROM human_identity_device_consents WHERE revoked_at IS NOT NULL').get().n), 0);
    assert.equal(rows(f, db => db.prepare('SELECT count(*) AS n FROM human_identity_grant_bindings WHERE revoked_at IS NOT NULL').get().n), 0);
    assert.equal((await check(f, args)).status, 'active');
  } finally { service.close(); }
});

test('pagination is bounded and bound to actor, device, scope and current ledger snapshot; grammar stays closed', { timeout: 20000 }, async t => {
  const f = await environment(t, { deviceConsent: true }); await remember(f); await remember(f, f.actor, f.rps[1]);
  const first = await list(f, f.actor, { limit: 1 }); assert.equal(first.entries.length, 1); assert.ok(first.nextCursor);
  const second = await list(f, f.actor, { limit: 1, cursor: first.nextCursor }); assert.notEqual(second.entries[0].entryId, first.entries[0].entryId);
  await assert.rejects(list(f, f.outsider, { cursor: first.nextCursor }), code('human_identity_signins_cursor_invalid'));
  const other = await f.backupDevice();
  await assert.rejects(list(f, other, { cursor: first.nextCursor }), code('human_identity_signins_cursor_invalid'));
  await assert.rejects(list(f, f.actor, { scope: 'account', cursor: first.nextCursor }), code('human_identity_signins_cursor_invalid'));
  for (const tail of ['.', '..', '.ignored']) await assert.rejects(list(f, f.actor, { cursor: first.nextCursor + tail }), code('human_identity_signins_cursor_invalid'));
  await revoke(f, intent(f.actor, first.entries[0]));
  await assert.rejects(list(f, f.actor, { cursor: first.nextCursor }), code('human_identity_signins_changed'));
  for (const args of [{ scope: 'all' }, { limit: 0 }, { limit: 21 }, { cursor: 'x'.repeat(1025) }, { grantId: 'untrusted' }, { expectedAccountId: f.outsider.account.accountId }])
    await assert.rejects(list(f, f.actor, args));
  const entry = (await list(f)).entries[0];
  for (const change of [{ requestId: 'x'.repeat(43) }, { expectedRevision: 'f'.repeat(64) }, { grantId: 'untrusted' }, { deviceId: f.actor.account.deviceId }, { scope: 'all' }])
    await assert.rejects(revoke(f, { ...intent(f.actor, entry), ...change }));
});

test('v1 has no management operation/capability and no new schema is migrated; empty Source-only OIDC list invents no Native session', { timeout: 20000 }, async t => {
  const old = await environment(t, { renewal: false });
  assert.equal(old.identity.signinCapabilities.available, false);
  assert.equal(old.identity.operations.has(LIST), false); assert.equal(old.identity.schemaVersion, 1);
  await assert.rejects(list(old), code('unsupported_operation'));
  const fresh = await environment(t);
  const result = await list(fresh);
  assert.deepEqual(result.entries, []); assert.equal(result.capabilities.sourceRootSessions, false);
  assert.equal(fresh.identity.schemaVersion, 2);
  fresh.identity.operations.add('identity.human.unknown');
  assert.throws(() => fresh.identity.execute({ op: 'identity.human.unknown', actor: fresh.actor.account, args: {} }), code('unsupported_operation'));
});
