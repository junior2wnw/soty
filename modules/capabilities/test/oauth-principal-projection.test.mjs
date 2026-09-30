import test from 'node:test';
import assert from 'node:assert/strict';
import { createCapabilitiesService } from '../server/index.mjs';
import { connectionsFixture, good, PROJECT } from './support/oauth-connections.mjs';

test('owner projection distinguishes an OAuth principal from an identically named key client', async t => {
  const f = await connectionsFixture(t);
  const keyClient = good(await f.ownerCall('access.principals.create', { label: 'Codex CLI' })).principal;
  assert.equal(Object.hasOwn(keyClient, 'managedBy'), false);
  await f.approve();
  const own = good(await f.ownerCall('access.principals.list')).principals;
  assert.equal(own.length, 2);
  const oauth = own.find(value => value.id !== keyClient.id);
  assert.equal(oauth.label, keyClient.label); assert.equal(oauth.managedBy, 'oauth');
  assert.equal(Object.hasOwn(own.find(value => value.id === keyClient.id), 'managedBy'), false);
  assert.deepEqual(Object.keys(oauth).sort(), [...Object.keys(keyClient), 'managedBy'].sort(), 'no provider IDs or secrets added');
  const other = await f.account();
  assert.deepEqual(good(await f.ownerCall('access.principals.list', {}, other)).principals, []);
});

test('the owner type marker survives expiry, keyless reopen and explicit connection revoke', async t => {
  const f = await connectionsFixture(t), accepted = await f.approve();
  f.reopen({ keyless: true }); f.advance(86400001);
  const before = good(await f.ownerCall('access.principals.list')).principals[0];
  assert.equal(before.managedBy, 'oauth');
  good(await f.ownerCall('oauth.connections.revoke', { connectionId: accepted.decision.connectionId }));
  assert.equal(good(await f.ownerCall('access.principals.list')).principals[0].managedBy, 'oauth');
});

test('AS-absent reader still marks and safely disables the exact managed principal', async t => {
  const f = await connectionsFixture(t), accepted = await f.approve();
  const reader = createCapabilitiesService({ databasePath: f.files.caps, projectId: PROJECT, clock: f.now,
    actorActive: actor => f.connect.isActorActive(actor) });
  try {
    assert.equal(reader.oauth, undefined, 'no issuer or AS composition exists');
    // The host supplies the actual fixture owner through its existing fence.
    // This reader-only assertion does not pretend to create a second signed login.
    const call = (op, args = {}) => f.connect.withAuthorityFence(() => reader.execute({ op, actor: f.owner,
      args: { ...args, expectedAccountId: f.owner.accountId } }));
    const principal = call('access.principals.list').principals[0];
    assert.equal(principal.managedBy, 'oauth');
    const result = call('access.principals.revoke', { principalId: principal.id });
    assert.equal(result.principal.managedBy, 'oauth'); assert.equal(result.principal.state, 'revoked');
    const list = good(await f.ownerCall('oauth.connections.list')).connections;
    assert.equal(list.find(item => item.id === accepted.decision.connectionId).active, false);
    assert.equal(f.db.prepare('SELECT count(*) AS n FROM cap_credentials').get().n, 0, 'marker never creates credentials');
  } finally { reader.close(); }
});
