import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:http';
import express from 'express';
import { attachConnectorApi } from '../connector-api.js';
import { createConnectorStore } from '../connector-store.js';
import { ConnectorPersistence } from '../connector-persistence.js';
import { readConnectorState } from '../connector-registry.js';

const registration = {
  linkId: 'c'.repeat(43), deviceId: 'close-test-device', connectorId: 'close:test',
  scope: 'CurrentUser', capabilities: ['command'],
  agent: { id: 'opencode', provider: 'gonka', available: true },
};
const token = 'd'.repeat(48);
const closed = error => error?.code === 'STORE_CLOSED';

test('shutdown drains an admitted write and refuses late writes and reads before closing SQLite', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-connector-close-'));
  let store = createConnectorStore(directory);
  t.after(async () => {
    await store.close();
    await rm(directory, { recursive: true, force: true });
  });
  await store.ready;
  const admitted = store.register(registration, token);
  const stopped = store.close();
  await assert.rejects(store.register({ ...registration, deviceId: 'late' }, token), closed);
  await assert.rejects(store.readable(), closed);
  assert.equal((await admitted).ok, true);
  await stopped;
  await Promise.all([store.close(), store.close()]);
  const persisted = await readConnectorState(directory);
  assert.equal(persisted.connectors.length, 1);
  assert.equal(persisted.connectors[0].deviceId, registration.deviceId);
  store = createConnectorStore(directory);
  await store.ready;
  assert.equal(store.state.connectors.length, 1);
});

test('direct persistence shutdown is idempotent and refuses calls as soon as close starts', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-persistence-close-'));
  const persistence = new ConnectorPersistence(join(directory, 'connector-store.json'));
  t.after(async () => {
    await persistence.close();
    await rm(directory, { recursive: true, force: true });
  });
  await persistence.call('load');
  const stopped = persistence.close();
  await assert.rejects(persistence.call('commit', {}), closed);
  await stopped;
  await Promise.all([persistence.close(), persistence.close()]);
  await assert.rejects(persistence.call('load'), closed);
});

test('a late real HTTP request receives a bounded storage-unavailable response with no extra durable row', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-connector-http-close-'));
  const app = express();
  const { store } = attachConnectorApi(app, { dataDir: directory, gonka: { apiKey: '', baseUrl: 'http://127.0.0.1:1' } });
  const server = createServer(app);
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  t.after(async () => {
    server.closeAllConnections();
    await new Promise(done => server.close(done));
    await store.close();
    await rm(directory, { recursive: true, force: true });
  });
  await store.ready;
  await store.close();
  const response = await fetch(`http://127.0.0.1:${server.address().port}/api/connectors/register`, {
    method: 'POST', headers: { 'content-type': 'application/json', authorization: 'Bearer ' + token },
    body: JSON.stringify(registration), signal: AbortSignal.timeout(3000),
  });
  assert.equal(response.status, 503);
  assert.deepEqual(await response.json(), { ok: false, error: 'connector-storage-unavailable' });
  assert.equal((await readConnectorState(directory)).connectors.length, 0);
});
