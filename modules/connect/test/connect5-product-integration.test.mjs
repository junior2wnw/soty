import test from 'node:test';
import assert from 'node:assert/strict';
import { pathToFileURL } from 'node:url';
import { mkdtemp, mkdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';

const root = process.env.SOTY_CONNECT_INTEGRATION_ROOT
  ? pathToFileURL(process.env.SOTY_CONNECT_INTEGRATION_ROOT.replace(/\\/gu, '/') + '/')
  : new URL('../', import.meta.url);
const { createClientWithStorage } = await import(new URL('browser/client.mjs', root));
const { createConnectService } = await import(new URL('server/index.mjs', root));
const { migrateDatabase } = await import(new URL('server/schema.mjs', root));
const origin = 'https://connect5-integration.invalid', endpoint = origin + '/rpc', projectId = 'connect5-integration';

function fixture(t, options = {}) {
  const service = createConnectService({ databasePath: ':memory:', projectId, allowedOrigins: [origin], ...options });
  let state = null;
  const store = { read: async () => structuredClone(state), claim: async value => structuredClone(state ??= value),
    compareAndSwap: async (revision, value, controls) => {
      assert.equal(state.localRevision, revision); controls?.beforeCommit?.(); state = structuredClone(value); return structuredClone(state);
    } };
  const client = createClientWithStorage({ projectId, endpoint, fetch: async (_url, request) => {
    const result = await service.handle({ ...JSON.parse(request.body), origin, peer: 'own-synthetic-peer' });
    return new Response(JSON.stringify(result), { status: result.ok ? 200 : 400 });
  } }, store);
  t.after(() => { client.dispose(); service.close(); });
  return { client, service };
}

test('Connect5 SDK preserves the serving credential entry API', t => {
  const { client } = fixture(t);
  for (const method of ['credentialStatus', 'setAccountCredential', 'removeAccountCredential', 'loginWithCredential']) {
    assert.equal(typeof client[method], 'function', `required public credential method ${method}`);
  }
});

test('Connect5 owner status is a genuine signed SDK/server operation', async t => {
  const { client } = fixture(t), owner = await client.bootstrap('Own synthetic owner');
  let result, failure;
  try { result = await client.extension('credentials.status', {}, { expectedAccountId: owner.accountId }); }
  catch (error) { failure = error.code; }
  assert.equal(failure, undefined, 'verified owner must retain the deployed credential status operation');
  assert.equal(result.schema, 'connect.credentials.v1'); assert.equal(result.accountId, owner.accountId);
  assert.equal(result.password.enabled, false); assert.equal(result.phrase.enabled, false);
});

test('explicit schema3 service retains ordinary account behavior and refuses credential writes', async t => {
  const { client } = fixture(t, { initializePlatformInvitations: false });
  const owner = await client.bootstrap('Own schema3 account');
  assert.equal((await client.status()).accountId, owner.accountId);
  await assert.rejects(client.extension('credentials.status', {}, { expectedAccountId: owner.accountId }), error => error.code === 'unsupported_operation');
  assert.equal((await client.getLocalState()).accountId, owner.accountId);
});

test('compatibility-only service reads genuine schema4 without creating credential tables or downgrading metadata', async t => {
  const directory = await mkdtemp(path.join(tmpdir(), 'soty-connect5-reader4-'));
  const file = path.join(directory, 'accounts.sqlite');
  const db = new DatabaseSync(file); migrateDatabase(db, projectId, { supportedVersion: 4 }); db.close();
  const f = fixture(t, { databasePath: file, initializePlatformInvitations: false });
  t.after(async () => {
    f.client.dispose(); f.service.close();
    assert.equal(path.dirname(path.resolve(directory)), path.resolve(tmpdir()));
    assert.match(path.basename(directory), /^soty-connect5-reader4-/u); await rm(directory, { recursive: true, force: true });
  });
  const owner = await f.client.bootstrap('Own schema4 account');
  assert.equal((await f.client.status()).accountId, owner.accountId);
  await assert.rejects(f.client.extension('credentials.status', {}, { expectedAccountId: owner.accountId }), error => error.code === 'unsupported_operation');
  const view = new DatabaseSync(file, { readOnly: true });
  try {
    assert.equal(view.prepare('PRAGMA user_version').get().user_version, 4);
    assert.equal(view.prepare("SELECT value FROM connect_meta WHERE key='schema_version'").get().value, '4');
    assert.equal(view.prepare("SELECT count(*) AS n FROM sqlite_schema WHERE name LIKE 'credential_%'").get().n, 0);
  } finally { view.close(); }
});
