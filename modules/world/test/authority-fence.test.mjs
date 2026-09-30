import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createWorldService } from '../server/index.mjs';

function fixture(t) {
  const base = resolve(tmpdir()), directory = mkdtempSync(join(base, 'soty-world-authority-'));
  const databasePath = join(directory, 'world.sqlite');
  const world = createWorldService({ databasePath, projectId: 'authority-test' });
  const reader = new DatabaseSync(databasePath, { readOnly: true });
  t.after(() => {
    reader.close(); world.close();
    assert.equal(dirname(resolve(directory)), base);
    assert.ok(resolve(directory).startsWith(join(base, 'soty-world-authority-')));
    rmSync(directory, { recursive: true, force: true });
  });
  const profiles = () => reader.prepare('SELECT COUNT(*) AS total FROM profiles').get().total;
  return { world, profiles };
}

test('host authority reads do not provision a first-use World profile or become a wire operation', t => {
  const { world, profiles } = fixture(t);
  assert.equal(profiles(), 0);
  const value = world.withCommunityAuthorityFence(() => ({
    member: world.canAccessCommunity('acct_first', 'community_missing'),
    admin: world.isGroupAdmin('acct_first', 'community_missing'),
    groups: world.activeCommunityIds('acct_first'),
  }));
  assert.deepEqual(value, { member: false, admin: false, groups: [] });
  assert.equal(profiles(), 0);
  assert.equal([...world.operations].some(op => /fence|authority/i.test(op)), false);
});

test('invalid or asynchronous host callbacks cannot leave the authority transaction held', async t => {
  const { world, profiles } = fixture(t);
  const code = value => error => error.code === value;
  let called = false;
  assert.throws(() => world.withCommunityAuthorityFence(async () => { called = true; }), code('world_authority_callback_invalid'));
  assert.equal(called, false);
  assert.throws(() => world.withCommunityAuthorityFence(() => Promise.reject(new Error('synthetic rejection'))), code('world_authority_callback_async'));
  // The rejected result is consumed by the boundary; a later ordinary operation
  // must still work and must be the first operation to provision the profile.
  await Promise.resolve();
  assert.equal(profiles(), 0);
  assert.equal(world.withCommunityAuthorityFence(() => 'ready'), 'ready');
  const result = world.execute({ op: 'world.profile.get', actor: { accountId: 'acct_first', deviceId: 'device_first', label: 'Первый' } });
  assert.equal(result.profile.profileId, 'acct_first');
  assert.equal(profiles(), 1);
});

test('nested work, World writes, close and callback errors fail without changing World data', t => {
  const { world, profiles } = fixture(t);
  const check = (callback, expected) => {
    assert.throws(() => world.withCommunityAuthorityFence(callback), error => error.code === expected);
    assert.equal(profiles(), 0);
    assert.equal(world.withCommunityAuthorityFence(() => true), true);
  };
  check(() => world.withCommunityAuthorityFence(() => true), 'world_authority_fence_nested');
  check(() => world.execute({ op: 'world.profile.get', actor: { accountId: 'acct_first', deviceId: 'device_first', label: 'Первый' } }), 'world_authority_mutation_forbidden');
  check(() => world.close(), 'world_authority_fence_active');
  const failure = new Error('downstream failed');
  assert.throws(() => world.withCommunityAuthorityFence(() => { throw failure; }), error => error === failure);
  assert.equal(world.withCommunityAuthorityFence(() => 7), 7);
  assert.equal(profiles(), 0);
});
