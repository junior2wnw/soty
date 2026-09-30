import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createWorldService } from '../server/index.mjs';

const publisher = { accountId: 'publisher', deviceId: 'publisher_device', label: 'Автор' };
const reader = { accountId: 'reader', deviceId: 'reader_device', label: 'Читатель' };
const other = { accountId: 'other_owner', deviceId: 'other_device', label: 'Другой автор' };
function fixture(t) {
  const directory = mkdtempSync(join(tmpdir(), 'soty-app-authority-'));
  const databasePath = join(directory, 'world.sqlite'), world = createWorldService({ databasePath, projectId: 'app_authority' });
  t.after(() => { world.close(); assert.equal(dirname(resolve(directory)), resolve(tmpdir()));
    assert.match(basename(directory), /^soty-app-authority-/u); rmSync(directory, { recursive: true, force: true }); });
  let serial = 0;
  const execute = (op, args, actor = publisher) => world.execute({ op, args, actor });
  const create = (actor = publisher) => execute('world.community.create', { requestId: `create_${++serial}`, name: 'Группа', joinPolicy: 'open' }, actor).community.communityId;
  const joinGroup = (id, actor) => execute('world.membership.join', { communityId: id }, actor);
  const read = (ids, owner = publisher.accountId, account = reader.accountId) => world.withCommunityAuthorityFence(() => world.appCommunityAuthority(account, owner, ids));
  return { world, databasePath, execute, create, joinGroup, read };
}

test('app authority intersects only relevant active memberships with publisher management, without profile writes', t => {
  const f = fixture(t), allowed = f.create(), unrelated = f.create(), managed = f.create(other);
  f.joinGroup(allowed, reader); f.joinGroup(unrelated, reader);
  f.joinGroup(managed, reader); f.joinGroup(managed, publisher);
  assert.deepEqual(f.read([allowed, managed, 'group_missing', allowed]), [allowed]);
  f.execute('world.membership.role', { communityId: managed, profileId: publisher.accountId, role: 'moderator' }, other);
  assert.deepEqual(f.read([allowed, managed]), [allowed, managed].sort());
  f.execute('world.membership.role', { communityId: managed, profileId: publisher.accountId, role: 'member' }, other);
  assert.deepEqual(f.read([managed]), []);
  f.execute('world.membership.leave', { communityId: allowed }, reader);
  assert.deepEqual(f.read([allowed]), []);
  const db = new DatabaseSync(f.databasePath, { readOnly: true });
  try {
    const before = db.prepare('SELECT count(*) AS count FROM profiles').get().count;
    assert.deepEqual(f.read([unrelated], publisher.accountId, 'brand_new_reader'), []);
    assert.equal(db.prepare('SELECT count(*) AS count FROM profiles').get().count, before);
  } finally { db.close(); }
  assert.equal([...f.world.operations].some(op => /authority/iu.test(op)), false);
});

test('archived groups cannot satisfy historical discussion authority and no other group is substituted', t => {
  const f = fixture(t), archived = f.create(), active = f.create(); f.joinGroup(archived, reader); f.joinGroup(active, reader);
  const group = f.execute('world.community.get', { communityId: archived }).community;
  f.execute('world.community.archive', { communityId: archived, expectedRevision: group.revision });
  assert.deepEqual(f.read([archived]), []);
  assert.deepEqual(f.read([archived, active]), [active]);
});

test('bounded host authority rejects missing fence and oversized or malformed candidates without truncating', t => {
  const f = fixture(t), allowed = f.create(); f.joinGroup(allowed, reader);
  assert.throws(() => f.world.appCommunityAuthority(reader.accountId, publisher.accountId, [allowed]), { code: 'world_authority_fence_required' });
  for (const ids of [null, {}, [null], ['bad/id'], Array(65537).fill(allowed)]) {
    assert.throws(() => f.read(ids), error => ['world_authority_candidates_invalid', 'invalid_identifier'].includes(error.code));
  }
  const relevant = Array.from({ length: 64064 }, (_, index) => `group_missing_${index}`); relevant[relevant.length - 1] = allowed;
  const started = performance.now();
  assert.deepEqual(f.read(relevant), [allowed]);
  t.diagnostic(`64064 relevant candidates, one real World JOIN: ${(performance.now() - started).toFixed(1)}ms on this local run; not a deadline guarantee`);
  assert.deepEqual(f.read([]), []);
});
