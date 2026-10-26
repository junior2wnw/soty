import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createAppBuilderRegistration } from './app-builder-registration.mjs';

const pending = { hostDeviceId: 'device-1', connectorId: 'connector-1', jobId: 'job-1' };
const proposal = { name: 'Личный проект', port: 5111, entryPath: '/board' };
function fixture() {
  const data = new Map(), queues = new Map();
  const storage = { getItem: key => data.get(key) ?? null, setItem: (key, value) => data.set(key, value), removeItem: key => data.delete(key) };
  const locks = { request(key, action) {
    const task = (queues.get(key) ?? Promise.resolve()).then(action);
    queues.set(key, task.catch(() => {}));
    return task;
  } };
  return { data, storage, locks, create: (overrides = {}) => createAppBuilderRegistration({ accountId: 'owner-1', pending, proposal, storage, locks, randomId: randomUUID, ...overrides }) };
}

test('one registration intent wins across concurrent tabs and survives reopening', async () => {
  const f = fixture(), first = f.create(), otherTab = f.create();
  const [a, b] = await Promise.all([first.begin(), otherTab.begin()]);
  assert.equal(a.dispatch, true); assert.equal(b.dispatch, false); assert.equal(a.attempt, b.attempt);
  assert.equal((await f.create().begin()).dispatch, false);
  await otherTab.clear(a.attempt);
  assert.equal((await f.create().begin()).dispatch, true);
});

test('different accounts and jobs have separate intents, while changed proposals fail closed', async () => {
  const f = fixture(); await f.create().begin();
  assert.equal((await f.create({ accountId: 'owner-2' }).begin()).dispatch, true);
  assert.equal((await f.create({ pending: { ...pending, jobId: 'job-2' } }).begin()).dispatch, true);
  await assert.rejects(f.create({ proposal: { ...proposal, port: 5112 } }).begin(), { code: 'app_registration_conflict' });
  await assert.rejects(f.create({ proposal: { ...proposal, port: 5112 } }).inspect(), { code: 'app_registration_conflict' });
  assert.equal((await f.create().begin()).dispatch, false);
});

test('explicit retry recovers a crash before send without allowing competing retries or stale cleanup', async () => {
  const f = fixture(), first = f.create(), second = f.create();
  const old = await first.begin(); // The page may close before it calls apps.register.
  const [retry, race] = await Promise.all([first.retry(old.attempt), second.retry(old.attempt)]);
  assert.equal(retry.dispatch, true); assert.equal(race.dispatch, false);
  assert.equal(race.attempt, retry.attempt); assert.notEqual(retry.attempt, old.attempt);
  await first.clear(old.attempt); assert.equal(await first.inspect(), retry.attempt);
  await second.clear(retry.attempt); assert.equal(await first.inspect(), null);
});

test('unavailable locks or storage cannot result in a dispatch', async () => {
  const f = fixture();
  await assert.rejects(f.create({ locks: undefined }).begin(), { code: 'app_create_lock_unavailable' });
  await assert.rejects(f.create({ storage: { ...f.storage, setItem() { throw Error('quota'); } } }).begin(), { code: 'app_create_storage_unavailable' });
  assert.equal(f.data.size, 0);
});
