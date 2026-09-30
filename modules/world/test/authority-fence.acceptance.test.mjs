import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync, readFileSync, writeFileSync, existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createWorldService } from '../server/index.mjs';

// These tests use the public World boundary and a second OS process. Files are
// synchronization markers, not a replacement for SQLite locking or authority.
const owner = Object.freeze({ accountId: 'fence_owner', deviceId: 'fence_owner_device', label: 'Fence owner' });
const member = Object.freeze({ accountId: 'fence_member', deviceId: 'fence_member_device', label: 'Fence member' });
const newcomer = Object.freeze({ accountId: 'fence_newcomer', deviceId: 'fence_newcomer_device', label: 'No World profile' });
const waitBuffer = new Int32Array(new SharedArrayBuffer(4));
const pause = ms => new Promise(done => setTimeout(done, ms));
const code = expected => error => { assert.equal(error?.code, expected); return true; };

function fixture(t) {
  const directory = mkdtempSync(join(tmpdir(), 'soty-authority-independent-'));
  const databasePath = join(directory, 'world.sqlite');
  const world = createWorldService({ databasePath, projectId: 'authority_acceptance' });
  const children = new Set();
  t.after(async () => {
    for (const child of children) if (child.exitCode === null) child.kill();
    await Promise.all([...children].map(child => child.finished));
    world.close();
    assert.equal(dirname(resolve(directory)), resolve(tmpdir()));
    assert.match(basename(directory), /^soty-authority-independent-/u);
    rmSync(directory, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
  });
  const execute = (op, args = {}, actor = owner) => world.execute({ op, args, actor });
  const community = execute('world.community.create', { requestId: 'fence_create', name: 'Authority specimen', joinPolicy: 'open' }).community;
  execute('world.membership.join', { communityId: community.communityId }, member);
  function inspect(fn) { const db = new DatabaseSync(databasePath, { readOnly: true }); try { return fn(db); } finally { db.close(); } }
  function state() {
    return inspect(db => Object.fromEntries(['profiles', 'communities', 'memberships', 'messages', 'receipts', 'world_audit', 'world_rate_limits']
      .map(table => [table, db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all()])));
  }
  return { directory, databasePath, world, execute, inspect, state, communityId: community.communityId, children };
}

const childSource = String.raw`
  import { writeFileSync, existsSync } from 'node:fs';
  import { join } from 'node:path';
  import { DatabaseSync } from 'node:sqlite';
  const c = JSON.parse(process.argv[1]);
  const mark = (name, value = {}) => writeFileSync(join(c.directory, name), JSON.stringify(value));
  const sleep = ms => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
  function awaitStart() {
    const end = Date.now() + 5000;
    while (!existsSync(join(c.directory, 'start'))) {
      if (Date.now() > end) throw new Error('parent_start_timeout'); sleep(5);
    }
  }
  let world, db;
  try {
    if (c.mode === 'revoke') {
      const { createWorldService } = await import(c.worldModule);
      world = createWorldService({ databasePath: c.databasePath, projectId: 'authority_acceptance' });
      mark('ready'); awaitStart(); mark('attempt');
      world.execute({ op: 'world.membership.remove', args: { communityId: c.communityId, profileId: c.member.accountId }, actor: c.owner });
      mark('completed', { allowed: world.canAccessCommunity(c.member.accountId, c.communityId) });
    } else {
      db = new DatabaseSync(c.databasePath);
      db.exec('PRAGMA busy_timeout=5000; BEGIN IMMEDIATE'); mark('ready'); awaitStart();
      sleep(c.holdMs); db.exec('COMMIT'); mark('completed');
    }
  } catch (error) { mark('failed', { code: error.code || error.message }); process.exitCode = 1; }
  finally { world?.close(); db?.close(); }
`;

function launchChild(f, mode, options = {}) {
  const env = Object.fromEntries(['SystemRoot', 'WINDIR', 'TEMP', 'TMP'].filter(key => process.env[key] !== undefined).map(key => [key, process.env[key]]));
  const child = spawn(process.execPath, ['--input-type=module', '-e', childSource, JSON.stringify({ mode,
    directory: f.directory, databasePath: f.databasePath, communityId: f.communityId, owner, member,
    worldModule: new URL('../server/index.mjs', import.meta.url).href, ...options })],
  { windowsHide: true, env, stdio: ['ignore', 'ignore', 'pipe'] });
  let stderr = '';
  child.stderr.on('data', value => { stderr = (stderr + value.toString()).slice(-8000); });
  child.finished = new Promise((done, reject) => {
    child.once('error', reject); child.once('exit', (status, signal) => done({ status, signal, stderr }));
  });
  f.children.add(child);
  return child;
}

async function marker(f, name) {
  const end = Date.now() + 5000;
  while (!existsSync(join(f.directory, name))) {
    if (existsSync(join(f.directory, 'failed'))) assert.fail(readFileSync(join(f.directory, 'failed'), 'utf8'));
    if (Date.now() >= end) assert.fail(`Timed out waiting for child ${name}`);
    await pause(5);
  }
  return JSON.parse(readFileSync(join(f.directory, name), 'utf8'));
}

function markerSync(f, name) {
  const end = Date.now() + 3000;
  while (!existsSync(join(f.directory, name))) {
    if (existsSync(join(f.directory, 'failed'))) assert.fail(readFileSync(join(f.directory, 'failed'), 'utf8'));
    if (Date.now() >= end) assert.fail(`Child did not attempt ${name} while fence held`);
    Atomics.wait(waitBuffer, 0, 0, 5);
  }
}

test('D1 independent fence returns a synchronous decision without provisioning profiles or changing World rows', t => {
  const f = fixture(t), before = f.state();
  const result = f.world.withCommunityAuthorityFence(() => ({
    allowed: f.world.canAccessCommunity(member.accountId, f.communityId),
    admin: f.world.isGroupAdmin(owner.accountId, f.communityId),
    newcomer: f.world.canAccessCommunity(newcomer.accountId, f.communityId),
    ids: f.world.activeCommunityIds(member.accountId),
  }));
  assert.deepEqual(result, { allowed: true, admin: true, newcomer: false, ids: [f.communityId] });
  assert.deepEqual(f.state(), before);
  assert.equal(f.inspect(db => db.prepare('SELECT COUNT(*) AS n FROM profiles WHERE account_id=?').get(newcomer.accountId).n), 0);
});

test('D1 independent fence rejects reentrancy, World operations, close and async callbacks, then remains reusable', t => {
  const f = fixture(t), before = f.state();
  let asyncInvoked = false;
  assert.throws(() => f.world.withCommunityAuthorityFence(async () => { asyncInvoked = true; }), code('world_authority_callback_invalid'));
  assert.equal(asyncInvoked, false);
  assert.throws(() => f.world.withCommunityAuthorityFence(null), code('world_authority_callback_invalid'));
  f.world.withCommunityAuthorityFence(() => {
    assert.throws(() => f.world.withCommunityAuthorityFence(() => true), code('world_authority_fence_nested'));
    assert.throws(() => f.execute('world.profile.get', {}, newcomer), code('world_authority_mutation_forbidden'));
    assert.throws(() => f.world.close(), code('world_authority_fence_active'));
    assert.equal(f.world.canAccessCommunity(member.accountId, f.communityId), true);
  });
  assert.throws(() => f.world.withCommunityAuthorityFence(() => Promise.resolve('too late')), code('world_authority_callback_async'));
  const failure = new Error('downstream failed before commit');
  assert.throws(() => f.world.withCommunityAuthorityFence(() => { throw failure; }), error => error === failure);
  assert.equal(f.world.withCommunityAuthorityFence(() => 17), 17);
  assert.deepEqual(f.state(), before);
  // A leaked lock/transaction would make an independent writer fail here.
  const second = new DatabaseSync(f.databasePath);
  try { second.exec('PRAGMA busy_timeout=1; BEGIN IMMEDIATE; ROLLBACK'); } finally { second.close(); }
});

test('D1 actual second World process cannot revoke membership inside the authority fence; the next decision denies', { timeout: 15000 }, async t => {
  const f = fixture(t), child = launchChild(f, 'revoke');
  await marker(f, 'ready');
  f.world.withCommunityAuthorityFence(() => {
    assert.equal(f.world.canAccessCommunity(member.accountId, f.communityId), true);
    writeFileSync(join(f.directory, 'start'), 'start'); markerSync(f, 'attempt');
    Atomics.wait(waitBuffer, 0, 0, 180);
    assert.equal(existsSync(join(f.directory, 'completed')), false, 'the real membership mutation must wait for World authority release');
    assert.equal(f.world.canAccessCommunity(member.accountId, f.communityId), true);
  });
  assert.deepEqual(await marker(f, 'completed'), { allowed: false });
  assert.equal((await child.finished).status, 0);
  assert.equal(f.world.withCommunityAuthorityFence(() => f.world.canAccessCommunity(member.accountId, f.communityId)), false);
});

test('D1 busy World authority skips downstream work and restores the normal writer wait policy', { timeout: 15000 }, async t => {
  const f = fixture(t), child = launchChild(f, 'hold', { holdMs: 600 });
  await marker(f, 'ready'); writeFileSync(join(f.directory, 'start'), 'start');
  let downstreamCalls = 0;
  assert.throws(() => f.world.withCommunityAuthorityFence(() => { downstreamCalls++; }), code('world_authority_busy'));
  assert.equal(downstreamCalls, 0);
  // This ordinary mutation must wait through the remaining lock. If the short
  // 100ms policy leaked out of the failed fence, it would fail instead.
  const changed = f.execute('world.profile.update', { expectedRevision: 1, bio: 'Writer works after contention' });
  assert.equal(changed.profile.bio, 'Writer works after contention');
  assert.equal((await child.finished).status, 0);
  assert.equal(f.world.withCommunityAuthorityFence(() => 'released'), 'released');
});
