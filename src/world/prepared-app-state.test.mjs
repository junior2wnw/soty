import test from 'node:test';
import assert from 'node:assert/strict';
import { createPreparedAppState } from './prepared-app-state.mjs';

function fixture() {
  const data = new Map(); let queue = Promise.resolve(), serial = 0;
  const storage = { getItem: key => data.get(key) ?? null, setItem: (key, value) => data.set(key, value) };
  const locks = { request(_key, action) { const result = queue.then(action); queue = result.catch(() => {}); return result; } };
  const make = (accountId = 'account-a', deviceId = 'device-a') => createPreparedAppState({ accountId, deviceId, storage, locks, randomId: () => `request-${++serial}` });
  const value = { expectedAccountId: 'account-a', hostDeviceId: 'host-a', connectorId: 'connector-a',
    location: { rootId: 'a'.repeat(64), segments: ['project'] }, manifestDigest: 'b'.repeat(64) };
  return { data, storage, make, value };
}
test('two tabs persist a single exact intent before sending and clear only its confirmed receipt', async () => {
  const f = fixture(), a = f.make(), b = f.make();
  const [first, second] = await Promise.all([a.prepare(f.value), b.prepare(f.value)]);
  assert.deepEqual(first.payload, second.payload); assert.equal(first.created, true); assert.equal(second.created, false); assert.equal(a.read().length, 1);
  const other = await b.prepare({ ...f.value, location: { ...f.value.location, segments: ['other'] } });
  assert.notEqual(other.payload.requestId, first.payload.requestId); assert.equal(a.read().length, 2);
  assert.equal(await a.acknowledge({ ...first.payload, manifestDigest: 'c'.repeat(64) }), false);
  assert.equal(await b.acknowledge(first.payload), true); assert.deepEqual(a.read(), [other.payload]);
  assert.deepEqual(f.make('account-b').read(), []); assert.deepEqual(f.make('account-a', 'device-b').read(), []);
});
test('unavailable locks, storage and corrupt markers fail before a new request can be sent', async () => {
  const f = fixture();
  const unavailable = createPreparedAppState({ accountId: 'account-a', deviceId: 'device-a', storage: f.storage });
  await assert.rejects(unavailable.prepare(f.value), { code: 'prepared_local_unavailable' });
  const state = f.make(); await state.prepare(f.value);
  f.data.set([...f.data.keys()][0], '{broken');
  assert.throws(() => state.read(), { code: 'prepared_local_unavailable' });
  await assert.rejects(state.prepare(f.value), { code: 'prepared_local_unavailable' });
});
