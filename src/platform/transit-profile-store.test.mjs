import test from 'node:test';
import assert from 'node:assert/strict';
import { loadTransitAccountProfile, saveTransitAccountProfile, saveTransitController } from './transit-profile-store.mjs';

test('saving access preserves all existing vault fields and binds the exact account and revision', async () => {
  const profile = { outbounds: [{ uuid: 'synthetic-private-access' }] };
  let result;
  const client = {
    async loadVault(owner) { assert.equal(owner, 'account-A'); return { revision: 7, payload: { notes: { accepted: true }, arbitrary: ['keep'] } }; },
    async saveVault(payload, revision, owner) { assert.equal(revision, 7); assert.equal(owner, 'account-A'); result = payload; },
  };
  await saveTransitAccountProfile(client, 'account-A', profile);
  assert.deepEqual(result.notes, { accepted: true }); assert.deepEqual(result.arbitrary, ['keep']);
  assert.deepEqual(result.transitClient.profile, profile);
  assert.deepEqual(await loadTransitAccountProfile({ async loadVault() { return { payload: result }; } }, 'account-A'), profile);
});

test('an unknown vault shape cannot be replaced with a new empty object', async () => {
  let writes = 0;
  await assert.rejects(saveTransitAccountProfile({ async loadVault() { return { revision: 1, payload: ['existing-data'] }; }, async saveVault() { writes++; } }, 'account-A', {}), /transit-vault-shape/u);
  assert.equal(writes, 0);
});

test('concurrent write and account replacement remain errors rather than blind overwrites', async () => {
  for (const code of ['VAULT_CONFLICT', 'ACTIVE_ACCOUNT_CHANGED']) {
    const client = { async loadVault(owner) { assert.equal(owner, 'account-A'); return { revision: 2, payload: null }; },
      async saveVault(_payload, revision, owner) { assert.equal(revision, 2); assert.equal(owner, 'account-A'); throw new Error(code); } };
    await assert.rejects(saveTransitAccountProfile(client, 'account-A', {}), new RegExp(code));
  }
});

test('OS control enrollment preserves profile and other devices without placing the token in the installer hash slot', async () => {
  const token = Buffer.alloc(32, 9).toString('base64url'); let saved;
  const previous = { transitClient: { schema: 'existing', profile: { keep: true } }, transitControllers: { other: 'keep' }, notes: 'keep' };
  await saveTransitController({ async loadVault() { return { payload: previous, revision: 5 }; }, async saveVault(payload, revision, owner) {
    assert.equal(revision, 5); assert.equal(owner, 'account-A'); saved = payload;
  } }, 'account-A', 'owned-machine', token);
  assert.equal(saved.transitControllers['owned-machine'], token);
  assert.equal(saved.transitControllers.other, 'keep');
  assert.deepEqual(saved.transitClient, previous.transitClient); assert.equal(saved.notes, 'keep');
});
