import test from 'node:test';
import assert from 'node:assert/strict';
import { Provider } from 'oidc-provider';
import { generateKeyPairSync } from 'node:crypto';
import { initializeCapabilitiesSchema } from '../server/schema.mjs';
import { connectionsFixture, good, code, id, PROJECT, NOW, ORIGIN } from './support/oauth-connections.mjs';

const count = (f, table) => f.db.prepare(`SELECT count(*) AS n FROM ${table}`).get().n;

test('real signed decision creates one fixed authority atomically; owner fields and read clock are safe', async t => {
  const f = await connectionsFixture(t), p = f.prepare();
  assert.equal(p.context.checkedAt, NOW); assert.equal(p.context.decidedAccountId, null);
  assert.equal(p.context.scope, 'notes.createDraft'); assert.equal(p.context.decision, 'pending');
  assert.equal(JSON.stringify(p.context).includes('private-state-plaintext-canary'), false);
  const accepted = good(await f.ownerCall('oauth.connections.approve', p.args));
  const again = good(await f.ownerCall('oauth.connections.approve', p.args));
  assert.equal(again.connectionId, accepted.connectionId); assert.equal(again.replayed, true);
  for (const table of ['cap_clients', 'cap_principals', 'cap_grants', 'cap_budgets', 'cap_oauth_connections']) assert.equal(count(f, table), 1);
  const grant = f.db.prepare('SELECT * FROM cap_grants').get();
  assert.equal(grant.allow_delegation, 0); assert.equal(grant.max_depth, 0); assert.equal(grant.parent_id, null);
  assert.equal(grant.expires_at, NOW + 86400000); assert.equal(grant.capabilities_json, '[{"capabilityId":"notes.createDraft","version":1}]');
  const presentation = f.oauth.readInteraction(p.bindingArgs);
  assert.equal(presentation.decidedAccountId, f.owner.accountId); assert.equal(presentation.checkedAt, f.now());
  const list = good(await f.ownerCall('oauth.connections.list'));
  assert.deepEqual(Object.keys(list.connections[0]).sort(), ['active', 'budget', 'clientProfile', 'createdAt', 'expiresAt', 'id', 'resource', 'revokedAt'].sort());
  assert.equal(list.connections[0].active, true); assert.equal(list.connections[0].budget.remaining, 20);
  assert.equal(f.oauth.readiness().available, false, 'token stage is still deliberately unavailable');
});

test('signed denial creates no authority; mismatched browser/digest/account and changed params cannot approve', async t => {
  const f = await connectionsFixture(t), p = f.prepare();
  const denial = good(await f.ownerCall('oauth.connections.deny', p.args)); assert.equal(denial.replayed, false);
  assert.equal(good(await f.ownerCall('oauth.connections.deny', p.args)).replayed, true);
  assert.equal(count(f, 'cap_clients'), 0); assert.equal(f.oauth.readInteraction(p.bindingArgs).decidedAccountId, f.owner.accountId);
  assert.equal((await f.ownerCall('oauth.connections.approve', p.args)).ok, false);
  const next = f.prepare(), changed = { ...next.payload, params: { ...next.payload.params, state: 'changed after display' } };
  assert.equal((await f.ownerCall('oauth.connections.approve', { ...next.args, contextDigest: '0'.repeat(64) })).ok, false);
  assert.equal((await f.ownerCall('oauth.connections.approve', { ...next.args, browserNonce: 'b'.repeat(43) })).ok, false);
  assert.equal((await f.ownerCall('oauth.connections.approve', { ...next.args, expectedAccountId: 'other' })).ok, false);
  f.oauth.artifactStore.upsert({ model: 'Interaction', id: changed.jti, payload: changed });
  assert.equal((await f.ownerCall('oauth.connections.approve', next.args)).ok, false);
  assert.throws(() => f.oauth.readInteraction(next.bindingArgs), code('oauth_interaction_conflict'));
  assert.equal(count(f, 'cap_oauth_connections'), 0);
});

test('lost outer signed response after Caps COMMIT replays the decision without repeating client/root/budget', async t => {
  const f = await connectionsFixture(t), p = f.prepare(); let fail = true;
  f.afterOwner(request => { if (request.op === 'oauth.connections.approve' && fail) { fail = false; throw new Error('synthetic lost outer commit/response'); } });
  assert.equal((await f.ownerCall('oauth.connections.approve', p.args)).ok, false);
  assert.equal(count(f, 'cap_oauth_connections'), 1);
  const replay = good(await f.ownerCall('oauth.connections.approve', p.args)); assert.equal(replay.replayed, true);
  for (const table of ['cap_clients', 'cap_principals', 'cap_grants', 'cap_budgets', 'cap_oauth_connections']) assert.equal(count(f, table), 1);
  f.reopen({ keyless: true });
  assert.equal(good(await f.ownerCall('oauth.connections.approve', p.args)).replayed, true);
  assert.equal(good(await f.ownerCall('oauth.connections.list')).connections.length, 1);
});

test('a SQLite failure inside approval or Grant linking rolls back every dependent row', async t => {
  const f = await connectionsFixture(t), p = f.prepare();
  f.db.exec(`CREATE TRIGGER test_fail_decision AFTER UPDATE ON cap_oauth_interactions
    WHEN NEW.decision='approved' BEGIN SELECT RAISE(ABORT,'test decision failure'); END`);
  assert.equal((await f.ownerCall('oauth.connections.approve', p.args)).ok, false);
  for (const table of ['cap_clients', 'cap_principals', 'cap_grants', 'cap_budgets', 'cap_oauth_connections', 'cap_audit']) {
    assert.equal(count(f, table), 0, table);
  }
  assert.equal(f.oauth.readInteraction(p.bindingArgs).decision, 'pending');
  f.db.exec('DROP TRIGGER test_fail_decision');
  const accepted = await f.approve(p), binding = f.oauth.beginGrantBinding(accepted.bindingArgs), payload = f.grant(binding);
  f.db.exec(`CREATE TRIGGER test_fail_binding BEFORE UPDATE ON cap_oauth_connections
    WHEN NEW.provider_grant_id IS NOT NULL BEGIN SELECT RAISE(ABORT,'test binding failure'); END`);
  assert.throws(() => f.save(binding, payload), code('capabilities_storage_corrupt'));
  assert.equal(f.db.prepare("SELECT count(*) AS n FROM cap_oauth_artifacts WHERE model='Grant'").get().n, 0);
  assert.equal(f.db.prepare('SELECT provider_grant_id FROM cap_oauth_connections').get().provider_grant_id, null);
  f.db.exec('DROP TRIGGER test_fail_binding');
  f.save(binding, payload); f.oauth.endGrantBinding(binding.context);
  assert.equal(f.db.prepare('SELECT provider_grant_id FROM cap_oauth_connections').get().provider_grant_id, payload.jti);
});

test('Grant seconds may precede non-aligned connection creation; expiry never exceeds its unchanged millisecond boundary', async t => {
  const f = await connectionsFixture(t), accepted = await f.approve(), binding = f.oauth.beginGrantBinding(accepted.bindingArgs);
  const payload = f.grant(binding);
  assert.ok(payload.iat * 1000 < NOW); assert.ok(payload.exp * 1000 < binding.connection.expiresAt);
  assert.throws(() => f.save(binding, { ...payload, exp: payload.exp + 1 }), code('oauth_invalid_artifact'));
  assert.equal(count(f, 'cap_oauth_artifacts'), 1, 'no Grant from the rejected boundary');
  f.save(binding, payload); f.oauth.endGrantBinding(binding.context);
  const stored = f.db.prepare("SELECT * FROM cap_oauth_artifacts WHERE model='Grant'").get();
  assert.equal(stored.created_at, NOW); assert.equal(stored.expires_at, payload.exp * 1000); assert.equal(stored.retain_until, NOW + 86400000);
  f.reopen(); assert.equal(initializeCapabilitiesSchema(f.db, { projectId: PROJECT }).schemaVersion, 3);
  const recovered = f.oauth.beginGrantBinding(accepted.bindingArgs);
  assert.equal(recovered.providerGrantId, payload.jti); f.oauth.endGrantBinding(recovered.context);
  assert.deepEqual(f.oauth.artifactStore.find({ model: 'Grant', id: payload.jti }), payload);
});

test('private Grant staging rejects forged/ended/foreign/expired contexts, and concurrent choices cannot leave an orphan', async t => {
  const f = await connectionsFixture(t), accepted = await f.approve(), first = f.oauth.beginGrantBinding(accepted.bindingArgs);
  const a = f.grant(first, 'grant-a');
  assert.throws(() => f.save({ ...first, context: {} }, a), code('oauth_context_invalid'));
  const second = f.oauth.beginGrantBinding(accepted.bindingArgs), b = f.grant(second, 'grant-b');
  f.save(first, a);
  assert.throws(() => f.save(second, b), code('oauth_grant_conflict'));
  assert.equal(f.db.prepare("SELECT count(*) AS n FROM cap_oauth_artifacts WHERE model='Grant'").get().n, 1);
  f.oauth.endGrantBinding(first.context);
  assert.throws(() => f.save(first, a), code('oauth_context_invalid'));
  f.oauth.endGrantBinding(second.context);
  let tick = 100; t.mock.method(performance, 'now', () => tick);
  const expiring = f.oauth.beginGrantBinding(accepted.bindingArgs); tick += 30000;
  assert.throws(() => f.save(expiring, a), code('oauth_context_invalid'));
  const held = f.oauth.beginGrantBinding(accepted.bindingArgs); f.reopen();
  assert.throws(() => f.save(held, a), code('oauth_context_invalid'));
});

test('staging has sixteen live slots and cleanup shares one work budget across artifacts and proposals', async t => {
  const f = await connectionsFixture(t), accepted = await f.approve(), contexts = [];
  for (let i = 0; i < 16; i++) contexts.push(f.oauth.beginGrantBinding(accepted.bindingArgs));
  assert.throws(() => f.oauth.beginGrantBinding(accepted.bindingArgs), code('oauth_quota_exceeded'));
  f.oauth.endGrantBinding(contexts.pop().context);
  const replacement = f.oauth.beginGrantBinding(accepted.bindingArgs);
  f.oauth.endGrantBinding(replacement.context);
  for (const binding of contexts) f.oauth.endGrantBinding(binding.context);
  for (let i = 0; i < 64; i++) f.prepare(`cleanup-${i}`);
  assert.equal(count(f, 'cap_oauth_artifacts'), 65); assert.equal(count(f, 'cap_oauth_interactions'), 65);
  f.advance(600001);
  const work = [];
  for (let i = 0; i < 3; i++) {
    const result = f.oauth.cleanup({ limit: 64 });
    work.push(result.artifactsDeleted + result.interactionsDeleted + result.credentialsDeleted);
  }
  assert.deepEqual(work, [64, 64, 2]);
  assert.equal(count(f, 'cap_oauth_artifacts'), 0); assert.equal(count(f, 'cap_oauth_interactions'), 0);
  assert.equal(count(f, 'cap_oauth_connections'), 1, 'cleanup never deletes durable authority pins');
  assert.equal(good(await f.ownerCall('oauth.connections.list')).connections.length, 1);
});

test('late outer fence failure after Grant COMMIT preserves the exact durable binding for retry', async t => {
  const f = await connectionsFixture(t), p = await f.approve(), binding = f.oauth.beginGrantBinding(p.bindingArgs), payload = f.grant(binding);
  let fail = true; f.afterFence(() => { if (fail) { fail = false; throw new Error('synthetic post-commit delivery failure'); } });
  assert.throws(() => f.save(binding, payload), /synthetic post-commit/u);
  f.oauth.endGrantBinding(binding.context);
  const recovered = f.oauth.beginGrantBinding(p.bindingArgs);
  assert.equal(recovered.providerGrantId, payload.jti);
  f.save(recovered, payload); f.oauth.endGrantBinding(recovered.context);
  assert.equal(f.db.prepare("SELECT count(*) AS n FROM cap_oauth_artifacts WHERE model='Grant'").get().n, 1);
});

test('creator revocation is checked again at Grant write; revoking an unrelated sibling leaves this connection live', async t => {
  const f = await connectionsFixture(t), phone = await f.enroll(), unrelated = await f.enroll();
  const p = await f.approve(f.prepare(), phone), binding = f.oauth.beginGrantBinding(p.bindingArgs);
  good(await f.call(f.owner.identity, 'device.revoke', { deviceId: unrelated.deviceId }));
  const stillLive = f.oauth.beginGrantBinding(p.bindingArgs); f.oauth.endGrantBinding(stillLive.context);
  good(await f.call(f.owner.identity, 'device.revoke', { deviceId: phone.deviceId }));
  assert.throws(() => f.save(binding), code('access_denied'));
  f.oauth.endGrantBinding(binding.context);
  assert.equal(f.db.prepare("SELECT count(*) AS n FROM cap_oauth_artifacts WHERE model='Grant'").get().n, 0);
  assert.equal(good(await f.ownerCall('oauth.connections.list')).connections[0].active, false);
  good(await f.ownerCall('oauth.connections.revoke', { connectionId: p.decision.connectionId }));
});

test('different account cannot replay/read/revoke an approved decision; list cursor stays account scoped', async t => {
  const f = await connectionsFixture(t), a = await f.approve(), b = await f.approve(), other = await f.account();
  assert.equal((await f.ownerCall('oauth.connections.approve', a.args, other)).ok, false);
  const first = good(await f.ownerCall('oauth.connections.list', { limit: 1 }));
  assert.ok(first.nextCursor); assert.equal(first.connections.length, 1);
  assert.equal((await f.ownerCall('oauth.connections.list', { limit: 1, cursor: first.nextCursor }, other)).ok, false);
  assert.deepEqual(good(await f.ownerCall('oauth.connections.list', {}, other)).connections, []);
  assert.equal((await f.ownerCall('oauth.connections.revoke', { connectionId: a.decision.connectionId }, other)).ok, false);
  const second = good(await f.ownerCall('oauth.connections.list', { limit: 1, cursor: first.nextCursor }));
  assert.equal(second.connections.length, 1); assert.notEqual(second.connections[0].id, first.connections[0].id);
  assert.equal(new Set([...first.connections, ...second.connections].map(row => row.id)).size, 2);
  assert.notEqual(a.decision.connectionId, b.decision.connectionId);
});

test('capacity blocks only new approval, never exact replay, denial, keyless list/revoke, or immutable legacy safeguards', async t => {
  const f = await connectionsFixture(t), accepted = [];
  for (let i = 0; i < 16; i++) accepted.push(await f.approve());
  const pending = f.prepare();
  assert.equal((await f.ownerCall('oauth.connections.approve', pending.args)).ok, false);
  assert.equal(count(f, 'cap_oauth_connections'), 16);
  assert.equal(good(await f.ownerCall('oauth.connections.approve', accepted[0].args)).replayed, true);
  good(await f.ownerCall('oauth.connections.deny', pending.args));
  const row = f.db.prepare('SELECT * FROM cap_oauth_connections WHERE id=?').get(accepted[0].decision.connectionId);
  assert.equal((await f.ownerCall('access.credentials.issue', { grantId: row.root_grant_id, audience: ORIGIN })).ok, false);
  f.reopen({ keyless: true });
  assert.equal(good(await f.ownerCall('oauth.connections.list')).connections.length, 16);
  good(await f.ownerCall('oauth.connections.revoke', { connectionId: row.id }));
  good(await f.ownerCall('oauth.connections.revoke', { connectionId: row.id }));
  assert.equal(f.db.prepare('SELECT state FROM cap_oauth_connections WHERE id=?').get(row.id).state, 'revoked');
  assert.notEqual(f.db.prepare('SELECT revoked_at FROM cap_grants WHERE id=?').get(row.root_grant_id).revoked_at, null);
});

test('successful Interaction completion must reference the signed account and exact bound Grant', async t => {
  const f = await connectionsFixture(t), p = await f.approve(), binding = f.oauth.beginGrantBinding(p.bindingArgs), grant = f.save(binding);
  f.oauth.endGrantBinding(binding.context);
  const complete = result => f.oauth.artifactStore.upsert({ model: 'Interaction', id: p.payload.jti, payload: { ...p.payload, result } });
  assert.throws(() => complete({ login: { accountId: 'other' }, consent: { grantId: grant.jti } }), code('oauth_context_invalid'));
  assert.throws(() => complete({ login: { accountId: f.owner.accountId }, consent: { grantId: id('other-grant') } }), code('oauth_context_invalid'));
  complete({ login: { accountId: f.owner.accountId }, consent: { grantId: grant.jti } });
  assert.equal(f.oauth.artifactStore.find({ model: 'Interaction', id: p.payload.jti }).result.login.accountId, f.owner.accountId);
  const pending = f.prepare();
  assert.throws(() => f.oauth.artifactStore.upsert({ model: 'Interaction', id: pending.payload.jti,
    payload: { ...pending.payload, result: { login: { accountId: f.owner.accountId }, consent: { grantId: grant.jti } } } }), code('oauth_context_invalid'));
});

test('actual Provider Grant save uses existing signed connection and second precision through the encrypted adapter', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: NOW });
  const f = await connectionsFixture(t), p = await f.approve(), binding = f.oauth.beginGrantBinding(p.bindingArgs);
  const key = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' });
  Object.assign(key, { kid: 'test', use: 'sig', alg: 'RS256' });
  class Adapter {
    constructor(model) { this.model = model; }
    async upsert(id, payload, expiresIn) { f.oauth.artifactStore.upsert({ model: this.model, id, payload, expiresIn, stagedGrant: binding.context }); }
    async find(id) { return f.oauth.artifactStore.find({ model: this.model, id }); }
  }
  const provider = new Provider(ORIGIN + '/oauth', { adapter: Adapter, clients: [], jwks: { keys: [key] },
    cookies: { keys: ['only-test-cookie-secret-no-production'] }, features: { devInteractions: { enabled: false } },
    ttl: { Grant: () => Math.floor(binding.connection.expiresAt / 1000) - Math.floor(Date.now() / 1000) }, expiresWithSession: () => false });
  const grant = new provider.Grant({ accountId: binding.connection.accountId, clientId: binding.connection.staticClientId });
  grant.addResourceScope(binding.connection.resource, 'notes.createDraft');
  const providerGrantId = await grant.save();
  f.oauth.endGrantBinding(binding.context);
  const restored = await provider.Grant.find(providerGrantId);
  assert.equal(restored.accountId, f.owner.accountId); assert.equal(restored.exp * 1000, Math.floor(binding.connection.expiresAt / 1000) * 1000);
  assert.equal(count(f, 'cap_credentials'), 0, 'no token issuance in this increment');
});
