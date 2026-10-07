import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, mkdir, readFile, readdir, writeFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { DatabaseSync } from 'node:sqlite';
import { createHistoricalNotesV1 } from './notes-v1.fixture.mjs';
import { createHistoricalNotesV2 } from './notes-v2.fixture.mjs';
import { createHistoricalCapabilitiesV1 } from './capabilities-v1.fixture.mjs';
import { createHistoricalCapabilitiesV2 } from './capabilities-v2.fixture.mjs';
import { createHistoricalCapabilitiesV3, upgradeHistoricalCapabilitiesV3, oauthCapabilitiesV3DDL } from './capabilities-v3.fixture.mjs';
import { readStorageFormat } from './storage-probe.mjs';
import { historicalSourceBytes, gitBlobHash } from './historical-source.fixture.mjs';
import { assertStorageCompatible, checkedStorageFormat, currentStorageReaders, guardStorageStart,
  requireStorageStartReceipt, storageReaderLabel } from './storage-guard.mjs';

const baseline3 = 'dc1ae217424b33cca0e9a5b60a6e4e719ea0d991';
const host2 = '0c8db3dbdc4a428c68ff98be9597223abc4da699';
const domain2 = 'cc7f65b84e0a8f8b4fd41cf44e498ee3f21015e7';
const sha = value => createHash('sha256').update(value).digest('hex');
const canonical = value => JSON.stringify(value, (_key, item) => item && typeof item === 'object' && !Array.isArray(item)
  ? Object.fromEntries(Object.keys(item).sort().map(key => [key, item[key]])) : item);
const normalized = value => value.split(/('(?:[^']|'')*')/gu).map((part, index) => index % 2 ? part
  : part.replace(/\s+/gu, '').replace(/;$/u, '').toLowerCase()).join('');
const layout = db => db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all()
  .map(row => ({ ...row, sql: normalized(row.sql) }));
const put = (db, table, values) => db.prepare(`INSERT INTO ${table}(${Object.keys(values).join(',')}) VALUES(${Object.keys(values).map(() => '?').join(',')})`).run(...Object.values(values));
const file = (root, store = 'capabilities') => path.join(root, store, store + '.sqlite');
const format = (notes, capabilities) => ({ ok: true, schema: 'soty.storage-format.v3', rooms: 'empty', apps: 'empty', notes, capabilities });
const reader2 = JSON.stringify({ version: 3, readers: { rooms: [1, 2], apps: [1, 2, 3, 4, 5, 6], notes: [1, 2], capabilities: [1, 2] } });
const reader3 = JSON.stringify({ version: 3, readers: { rooms: [1, 2], apps: [1, 2, 3, 4, 5, 6], notes: [1, 2], capabilities: [1, 2, 3] } });
const image = (readers = reader3) => ({ Id: 'sha256:' + '2'.repeat(64), Config: { Labels: { [storageReaderLabel]: readers } } });
const contractDigest = '95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204';
const git = historicalSourceBytes;
const json = url => readFile(url, 'utf8').then(JSON.parse);

async function directory(t) {
  const root = await mkdtemp(path.join(os.tmpdir(), 'soty-oauth-reader3-'));
  t.after(async () => {
    assert.equal(path.dirname(path.resolve(root)), path.resolve(os.tmpdir()));
    assert.match(path.basename(root), /^soty-oauth-reader3-/u);
    await rm(root, { recursive: true, force: true });
  });
  return root;
}
async function database(root, version = 3, store = 'capabilities') {
  await mkdir(path.join(root, store), { recursive: true });
  const db = new DatabaseSync(file(root, store));
  const create = store === 'notes' ? { 1: createHistoricalNotesV1, 2: createHistoricalNotesV2 }
    : { 1: createHistoricalCapabilitiesV1, 2: createHistoricalCapabilitiesV2, 3: createHistoricalCapabilitiesV3 };
  try { db.exec('PRAGMA foreign_keys=ON'); create[version](db); return db; }
  catch (error) { db.close(); throw error; }
}
async function persistent(root, store = 'capabilities') {
  const result = {};
  for (const suffix of ['', '-wal']) {
    try { result[suffix] = sha(await readFile(file(root, store) + suffix)); }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
  }
  return result;
}
async function pinnedModule(t, version) {
  const provenance = await json(version === 3 ? new URL('./fixtures/capabilities-v3/provenance.json', import.meta.url)
    : new URL('../../modules/capabilities/test/fixtures/capabilities-v2/provenance.json', import.meta.url));
  assert.equal(provenance.commit, version === 3 ? baseline3 : domain2);
  const files = version === 3 ? Object.entries(provenance.files) : provenance.files.map(item => [item.source, item]);
  const root = await directory(t);
  for (const [source, expected] of files) {
    assert.match(source, /^modules\/capabilities\/server\/[a-z0-9-]+\.mjs$/u);
    const bytes = git(provenance.commit, source);
    assert.equal(bytes.length, expected.bytes); assert.equal(sha(bytes), expected.sha256, source);
    assert.equal(gitBlobHash(bytes), expected.gitBlob);
    const target = path.join(root, source); await mkdir(path.dirname(target), { recursive: true }); await writeFile(target, bytes);
  }
  return { module: await import(pathToFileURL(path.join(root, 'modules/capabilities/server/schema.mjs')).href), provenance };
}
async function oldHostProbe(t) {
  const provenance = await json(new URL('./fixtures/capabilities-v3/provenance.json', import.meta.url));
  const expected = provenance.oldHost;
  assert.equal(expected.commit, host2);
  const bytes = git(host2, expected.sourcePath);
  assert.equal(bytes.length, expected.bytes); assert.equal(sha(bytes), expected.sha256);
  const root = await directory(t), target = path.join(root, 'reader2-probe.mjs'); await writeFile(target, bytes);
  return (await import(pathToFileURL(target).href)).readStorageFormat;
}
function dropForSeed(db, table, action) {
  const guards = db.prepare("SELECT name,sql FROM sqlite_schema WHERE type='trigger' AND tbl_name=?").all(table);
  for (const guard of guards) db.exec(`DROP TRIGGER ${guard.name}`);
  try { action(); } finally { for (const guard of guards) db.exec(guard.sql); }
}
function baseWitness(db) {
  return Object.fromEntries(['cap_metadata', 'cap_clients', 'cap_principals', 'cap_grants', 'cap_credentials', 'cap_budgets',
    'cap_budget_reservations', 'cap_invocations', 'cap_dispatch_intents', 'cap_native_note_intents', 'cap_receipts']
    .map(table => [table, db.prepare(`SELECT * FROM ${table}${table === 'cap_metadata' ? " WHERE key!='lineage'" : ''} ORDER BY rowid`).all()]));
}

// Synthetic storage witnesses only: no external client, real token, AS key or
// assertion that these inserts executed a native effect/OAuth authorization.
function seedNative(db, notes) {
  const owner = 'reader3_owner', registry = db.prepare("SELECT value FROM cap_metadata WHERE key='registry_id'").get().value;
  put(db, 'cap_clients', { id: 'native_client', account_id: owner, label: 'Synthetic native', state: 'active', created_at: 100 });
  put(db, 'cap_principals', { id: 'native_principal', account_id: owner, client_id: 'native_client', kind: 'service', label: 'Synthetic', state: 'active', creator_device_id: 'reader3_device', created_at: 100 });
  put(db, 'cap_grants', { id: 'native_root', account_id: owner, client_id: 'native_client', principal_id: 'native_principal', root_id: 'native_root',
    creator_device_id: 'reader3_device', capabilities_json: '[{"capabilityId":"notes.createDraft","version":1}]', resources_json: '["notes:new"]',
    effects_json: '["create"]', recipients_json: '["soty:notes"]', allow_delegation: 0, max_depth: 0, depth: 0, not_before: 100, expires_at: 100000, created_at: 100 });
  put(db, 'cap_credentials', { id: 'native_credential', digest: sha('synthetic native credential'), account_id: owner, client_id: 'native_client',
    principal_id: 'native_principal', grant_id: 'native_root', audience: 'https://reader3.test', expires_at: 100000, created_at: 100 });
  put(db, 'cap_budgets', { root_grant_id: 'native_root', unit: 'invocations', limit_amount: 5, reserved_amount: 1, spent_amount: 1 });
  for (const completed of [false, true]) {
    const id = completed ? 'native_completed' : 'native_uncertain', suffix = sha(canonical(['soty.native-note.v1', registry, id]));
    const input = { title: 'Synthetic original', body: 'Initial synthetic text' }, inputJson = canonical(input);
    const target = { kind: 'native', handler: 'notes.createDraft', version: 1 };
    const authority = { accountId: owner, clientId: 'native_client', principalId: 'native_principal', credentialId: 'native_credential', audience: 'https://reader3.test',
      grantId: 'native_root', rootGrantId: 'native_root', policyEpoch: 1, expiresAt: 100000, capabilityId: 'notes.createDraft', version: 1,
      capabilityDigest: contractDigest, resources: ['notes:new'], effects: ['create'], recipients: ['soty:notes'], executionBinding: target,
      charges: [{ unit: 'invocations', amount: 1 }] };
    const requestDigest = sha(canonical({ capabilityId: 'notes.createDraft', version: 1, capabilityDigest: contractDigest, input, target,
      resources: ['notes:new'], effects: ['create'], recipients: ['soty:notes'] }));
    put(db, 'cap_budget_reservations', { id: 'reservation_' + id, invocation_id: id, attempt_id: 'attempt_' + id, root_grant_id: 'native_root',
      unit: 'invocations', amount: 1, actual_amount: completed ? 1 : null, disposition: completed ? 'spent' : 'uncertain', request_digest: requestDigest, created_at: 100, updated_at: 110 });
    put(db, 'cap_invocations', { id, account_id: owner, client_id: 'native_client', principal_id: 'native_principal', grant_id: 'native_root', root_grant_id: 'native_root', policy_epoch: 1,
      capability_id: 'notes.createDraft', capability_version: 1, capability_digest: contractDigest, request_key: sha(id), request_digest: requestDigest,
      internal_request_id: 'internal_' + id, input_json: inputJson, target_json: canonical(target), authorization_json: canonical(authority),
      status: 'execution_uncertain', effect_state: 'unknown', reservation_id: 'reservation_' + id, created_at: 100, updated_at: 110 });
    put(db, 'cap_dispatch_intents', { invocation_id: id, internal_request_id: 'internal_' + id, state: 'uncertain', created_at: 100, updated_at: 110 });
    put(db, 'cap_native_note_intents', { invocation_id: id, account_id: owner, notes_store_id: 'a'.repeat(32), note_id: 'n_' + suffix, mutation_id: 'm_' + suffix,
      input_digest: sha(inputJson), input_bytes: Buffer.byteLength(inputJson), started_at: 105 });
    if (completed) {
      const effects = [{ kind: 'created', resourceType: 'note', resourceId: 'n_' + suffix, revision: 1 }];
      const receipt = { verificationMethod: 'domain_read', artifacts: [{ type: 'note', id: 'n_' + suffix, revision: 1 }] };
      put(db, 'cap_receipts', { invocation_id: id, value_json: canonical(receipt), digest: sha(canonical({ status: 'succeeded', effectState: 'committed', effects, receipt,
        disposition: 'spent', actualCharges: [{ unit: 'invocations', amount: 1 }] })), created_at: 110 });
      db.prepare("UPDATE cap_invocations SET status='succeeded',effect_state='committed',effects_json=?,input_json='null',completed_at=110 WHERE id=?").run(canonical(effects), id);
      db.prepare('UPDATE cap_native_note_intents SET input_purged_at=110 WHERE invocation_id=?').run(id);
      if (notes) {
        put(notes, 'note_accounts', { account_id: owner, bytes: 0, identities: 1, active: 0, archived: 0, trashed: 0 });
        put(notes, 'notes', { rowid: 1, account_id: owner, id: 'n_' + suffix, title: '', body: '', items: '[]', preview: '', color: 'plain', pinned: 0,
          state: 'deleted', revision: 3, bytes: 0, created_at: 105, updated_at: 130 });
        put(notes, 'note_native_creates', { source_store_id: registry, invocation_id: id, account_id: owner, note_id: 'n_' + suffix, mutation_id: 'm_' + suffix,
          input_digest: sha(inputJson), capability_digest: contractDigest, revision: 1, created_at: 105 });
      }
    }
  }
}
function seedOAuth(db) {
  const owner = 'reader3_owner', device = 'reader3_device', issuer = 'https://reader3.test/oauth', resource = 'https://reader3.test/mcp';
  const connection = 'oauth_connection', provider = 'synthetic_provider_grant_12345678', created = 10000, expires = 110000, tokenDigest = sha('synthetic opaque token');
  db.exec('BEGIN IMMEDIATE');
  try {
    put(db, 'cap_clients', { id: 'oauth_client', account_id: owner, label: 'Synthetic OAuth', state: 'active', created_at: created });
    put(db, 'cap_principals', { id: 'oauth_principal', account_id: owner, client_id: 'oauth_client', kind: 'service', label: 'Synthetic', state: 'active', creator_device_id: device, created_at: created });
    put(db, 'cap_grants', { id: 'oauth_root', account_id: owner, client_id: 'oauth_client', principal_id: 'oauth_principal', root_id: 'oauth_root', creator_device_id: device,
      capabilities_json: '[{"capabilityId":"notes.createDraft","version":1}]', resources_json: '["notes:new"]', effects_json: '["create"]', recipients_json: '["soty:notes"]',
      allow_delegation: 0, max_depth: 0, depth: 0, not_before: created, expires_at: expires, created_at: created });
    put(db, 'cap_budgets', { root_grant_id: 'oauth_root', unit: 'invocations', limit_amount: 4 });
    put(db, 'cap_oauth_connections', { id: connection, account_id: owner, client_id: 'oauth_client', principal_id: 'oauth_principal', root_grant_id: 'oauth_root', creator_device_id: device,
      issuer, static_client_id: 'soty-codex-cli', resource, scope: 'notes.createDraft', consent_digest: sha('synthetic consent'), state: 'active', created_at: created, expires_at: expires });
    const artifact = (model, idHash, expiry, linked = true) => put(db, 'cap_oauth_artifacts', { model, id_hash: idHash, issuer,
      profile: 'oidc-provider-9.12.2-c1', key_id: 'synthetic-reader-witness', payload_cipher: Buffer.alloc(30, 7), payload_digest: sha('synthetic payload'),
      connection_id: linked ? connection : null, provider_grant_id: linked ? provider : null,
      session_uid_hash: model === 'Session' ? sha('synthetic session uid') : null, created_at: created, expires_at: expiry,
      retain_until: model === 'AccessToken' || !linked ? expiry : expires });
    artifact('Grant', sha(provider), expires);
    db.prepare('UPDATE cap_oauth_connections SET provider_grant_id=? WHERE id=?').run(provider, connection);
    artifact('AccessToken', tokenDigest, 20000);
    put(db, 'cap_credentials', { id: 'oauth_credential', digest: tokenDigest, account_id: owner, client_id: 'oauth_client', principal_id: 'oauth_principal',
      grant_id: 'oauth_root', audience: resource, expires_at: 20000, created_at: created });
    put(db, 'cap_oauth_credentials', { credential_id: 'oauth_credential', connection_id: connection, token_digest: tokenDigest, created_at: created, expires_at: 20000 });
    artifact('AuthorizationCode', sha('synthetic code'), 30000); artifact('RefreshToken', sha('synthetic refresh'), 60000);
    artifact('Session', sha('synthetic session'), 50000, false); artifact('Interaction', sha('synthetic artifact interaction'), 50000, false);
    put(db, 'cap_oauth_interactions', { uid_hash: sha('synthetic interaction'), issuer, static_client_id: 'soty-codex-cli', resource,
      redirect_uri: 'http://127.0.0.1:8765/callback', request_digest: sha('synthetic consent'), browser_nonce_hash: sha('synthetic browser nonce'),
      duration_ms: 100000, budget_limit: 4, created_at: 9900, expires_at: 30000, decision: 'pending' });
    db.prepare("UPDATE cap_oauth_interactions SET decision='approved',decided_at=?,decided_account_id=?,decided_device_id=?,connection_id=? WHERE uid_hash=?")
      .run(created, owner, device, connection, sha('synthetic interaction'));
    db.exec('COMMIT');
  } catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
}

test('literal3 has all 64 pinned SQL objects and matches the committed baseline3 import closure', async t => {
  const { module, provenance } = await pinnedModule(t, 3), historical = await pinnedModule(t, 2);
  assert.equal(sha(oauthCapabilitiesV3DDL), provenance.ddl.sha256); assert.equal(Buffer.byteLength(oauthCapabilitiesV3DDL), provenance.ddl.bytes);
  const actual = new DatabaseSync(':memory:'), literal = new DatabaseSync(':memory:');
  try {
    historical.module.initializeCapabilitiesSchema(actual, { projectId: 'soty', allowNativeMigration: true });
    assert.equal(module.initializeCapabilitiesSchema(actual, { projectId: 'soty', allowOAuthMigration: true }).schemaVersion, 3);
    createHistoricalCapabilitiesV3(literal);
    assert.equal(layout(literal).length, 64); assert.deepEqual(layout(literal), layout(actual));
    assert.equal(Object.keys(provenance.objects).length, 28);
    for (const [name, expected] of Object.entries(provenance.objects)) {
      const row = layout(literal).find(item => item.name === name);
      assert.equal(row.type, expected.type, name); assert.equal(row.tbl_name, expected.table, name);
      assert.equal(sha(row.sql), expected.normalizedSha256, name);
    }
  } finally { actual.close(); literal.close(); }
});

test('all twelve independent store pairs are read-only; current image reads3 while image2 remains incompatible', async t => {
  const declared = JSON.parse(currentStorageReaders);
  assert.equal(declared.version, 5);
  assert.deepEqual(declared.readers.apps, [1, 2, 3, 4, 5, 6, 7]);
  const independent = JSON.parse(reader3).readers;
  for (const key of ['rooms', 'notes', 'capabilities']) assert.deepEqual(declared.readers[key], independent[key]);
  for (const notes of ['empty', 1, 2]) for (const capabilities of ['empty', 1, 2, 3]) {
    const root = await directory(t);
    if (notes !== 'empty') (await database(root, notes, 'notes')).close();
    if (capabilities !== 'empty') (await database(root, capabilities)).close();
    const before = await Promise.all(['notes', 'capabilities'].map(store => persistent(root, store)));
    const result = await readStorageFormat(root); assert.deepEqual(result, format(notes, capabilities));
    assertStorageCompatible(image(currentStorageReaders), result);
    if (capabilities === 3) assert.throws(() => assertStorageCompatible(image(reader2), result), /storage_reader_incompatible/u);
    else assertStorageCompatible(image(reader2), result);
    assert.deepEqual(await Promise.all(['notes', 'capabilities'].map(store => persistent(root, store))), before);
    assert.deepEqual(Object.keys(result).sort(), ['apps', 'capabilities', 'notes', 'ok', 'rooms', 'schema']);
  }
});

test('real pinned 2→3 COMMIT in WAL preserves native proofs/budgets; old host/domain2 refuse without writes', async t => {
  const latest = await pinnedModule(t, 3), old = await pinnedModule(t, 2), oldProbe = await oldHostProbe(t);
  const root = await directory(t), db = await database(root, 2), notes = await database(root, 2, 'notes');
  try {
    seedNative(db, notes); const native = baseWitness(db), proofs = notes.prepare('SELECT * FROM note_native_creates').all();
    const notesBefore = await persistent(root, 'notes');
    db.exec('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA synchronous=FULL; PRAGMA wal_checkpoint(TRUNCATE)');
    const beforeMigrationRegistry = db.prepare("SELECT value FROM cap_metadata WHERE key='registry_id'").get().value;
    const state = latest.module.initializeCapabilitiesSchema(db, { projectId: 'soty', allowOAuthMigration: true });
    assert.equal(state.schemaVersion, 3); assert.equal(state.registryId, beforeMigrationRegistry);
    assert.deepEqual(baseWitness(db), native); seedOAuth(db);
    assert.equal((await readFile(file(root))).readUInt32BE(60), 2);
    assert.equal(db.prepare('PRAGMA user_version').get().user_version, 3);
    assert.deepEqual(db.prepare('PRAGMA foreign_key_check').all(), []);
    const before = await persistent(root); assert.ok(before['-wal']);
    await assert.rejects(oldProbe(root), /storage_format_unknown/u);
    const oldHandle = new DatabaseSync(file(root));
    try { assert.throws(() => old.module.initializeCapabilitiesSchema(oldHandle, { projectId: 'soty' }), { code: 'schema_version_unsupported' }); }
    finally { oldHandle.close(); }
    assert.deepEqual(await readStorageFormat(root), format(2, 3));
    assert.deepEqual(await persistent(root), before); assert.deepEqual(await persistent(root, 'notes'), notesBefore);
    assert.equal(latest.module.initializeCapabilitiesSchema(db, { projectId: 'soty' }).schemaVersion, 3, 'default-off, no AS key');
    assert.deepEqual(notes.prepare('SELECT * FROM note_native_creates').all(), proofs);
    assert.equal(notes.prepare('SELECT state FROM notes').get().state, 'deleted');
    assert.deepEqual({ ...db.prepare("SELECT reserved_amount,spent_amount FROM cap_budgets WHERE root_grant_id='native_root'").get() }, { reserved_amount: 1, spent_amount: 1 });
    assert.equal(db.prepare('SELECT count(*) AS n FROM cap_receipts').get().n, 1);
  } finally { db.close(); notes.close(); }
});

test('default-off domain3 preserves exact 1/2/3, and lawful retained links survive expired artifacts and revocation', async t => {
  const { module } = await pinnedModule(t, 3);
  for (const version of [1, 2, 3]) {
    const root = await directory(t), db = await database(root, version);
    try {
      if (version === 3) seedOAuth(db);
      const before = layout(db);
      assert.equal(module.initializeCapabilitiesSchema(db, { projectId: 'soty' }).schemaVersion, version);
      assert.deepEqual(layout(db), before);
      if (version !== 3) continue;
      db.exec("DELETE FROM cap_oauth_artifacts WHERE model='AccessToken'; UPDATE cap_clients SET state='revoked',revoked_at=40000 WHERE id='oauth_client'");
      assert.equal(module.initializeCapabilitiesSchema(db, { projectId: 'soty' }).schemaVersion, 3);
      assert.equal(db.prepare('SELECT count(*) AS n FROM cap_oauth_credentials').get().n, 1);
      assert.equal(db.prepare("SELECT state FROM cap_oauth_connections").get().state, 'active', 'ordinary client revoke is a lawful retained tuple');
      db.exec("UPDATE cap_credentials SET revoked_at=40000 WHERE id='oauth_credential'; UPDATE cap_grants SET revoked_at=40000 WHERE id='oauth_root'; UPDATE cap_oauth_connections SET state='revoked',revoked_at=40000");
      assert.equal(module.initializeCapabilitiesSchema(db, { projectId: 'soty' }).schemaVersion, 3);
      assert.equal((await readStorageFormat(root)).capabilities, 3);
    } finally { db.close(); }
  }
});

test('host exact layout is not a row/decryption certificate: default-off domain3 rejects a corrupt creator tuple', async t => {
  const { module } = await pinnedModule(t, 3), root = await directory(t), db = await database(root);
  try {
    seedOAuth(db);
    dropForSeed(db, 'cap_oauth_connections', () => db.exec("UPDATE cap_oauth_connections SET creator_device_id='wrong_device'"));
    const before = await persistent(root);
    assert.equal((await readStorageFormat(root)).capabilities, 3, 'structural host scope is explicit');
    assert.throws(() => module.initializeCapabilitiesSchema(db, { projectId: 'soty' }), { code: 'capabilities_storage_corrupt' });
    assert.deepEqual(await persistent(root), before);
  } finally { db.close(); }
});

test('all fourteen new guards require exact SQL and table; all ten indexes require their exact predicates', async t => {
  const reference = new DatabaseSync(':memory:'); createHistoricalCapabilitiesV3(reference);
  const objects = reference.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE type IN ('index','trigger') AND name NOT GLOB 'sqlite_*'").all()
    .filter(row => oauthCapabilitiesV3DDL.includes(row.name)); reference.close();
  assert.equal(objects.filter(row => row.type === 'trigger').length, 14);
  assert.equal(objects.filter(row => row.type === 'index').length, 10);
  const root = await directory(t), db = await database(root);
  try {
    for (const object of objects) {
      db.exec(`DROP ${object.type} ${object.name}`);
      let before = await persistent(root);
      await assert.rejects(readStorageFormat(root), /storage_format_unreadable/u, object.name + '/missing');
      assert.deepEqual(await persistent(root), before);
      // Same object type/name/table; the recognized behavior cannot be replaced
      // by a benign trigger or an index with another expression/predicate.
      db.exec(object.type === 'trigger'
        ? `CREATE TRIGGER ${object.name} BEFORE INSERT ON ${object.tbl_name} BEGIN SELECT 1; END`
        : `CREATE INDEX ${object.name} ON ${object.tbl_name}((1))`);
      before = await persistent(root);
      await assert.rejects(readStorageFormat(root), /storage_format_unreadable/u, object.name + '/behavior');
      assert.deepEqual(await persistent(root), before);
      db.exec(`DROP ${object.type} ${object.name}`);
      if (object.type === 'trigger') {
        db.exec(object.sql.replace(new RegExp(`\\bON ${object.tbl_name}\\b`, 'u'), 'ON cap_metadata'));
        before = await persistent(root);
        await assert.rejects(readStorageFormat(root), /storage_format_unreadable/u, object.name + '/target');
        assert.deepEqual(await persistent(root), before);
        db.exec(`DROP TRIGGER ${object.name}`);
      }
      db.exec(object.sql);
    }
    assert.equal((await readStorageFormat(root)).capabilities, 3);
  } finally { db.close(); }
});

test('v3 table STRICT, FK, PK, CHECK, columns and extra SQL objects cannot imitate the frozen schema', async t => {
  const variations = [
    oauthCapabilitiesV3DDL.replace(') STRICT;', ');'),
    oauthCapabilitiesV3DDL.replace('payload_cipher BLOB', 'payload_cipher TEXT'),
    oauthCapabilitiesV3DDL.replace('PRIMARY KEY(model,id_hash)', 'PRIMARY KEY(id_hash,model)'),
    oauthCapabilitiesV3DDL.replace('REFERENCES cap_grants(id)', 'REFERENCES cap_clients(id)'),
    oauthCapabilitiesV3DDL.replace('BETWEEN 30 AND 16412', 'BETWEEN 29 AND 16412'),
    oauthCapabilitiesV3DDL.replace('scope TEXT NOT NULL', 'extra TEXT, scope TEXT NOT NULL'),
    oauthCapabilitiesV3DDL.replace('scope TEXT NOT NULL', "extra TEXT GENERATED ALWAYS AS ('x') VIRTUAL, scope TEXT NOT NULL"),
    oauthCapabilitiesV3DDL + '\nCREATE TABLE extra(value TEXT);',
    oauthCapabilitiesV3DDL + '\nCREATE VIEW extra AS SELECT id FROM cap_oauth_connections;',
    oauthCapabilitiesV3DDL + '\nCREATE INDEX extra ON cap_oauth_connections(id);',
  ];
  for (const ddl of variations) {
    assert.notEqual(ddl, oauthCapabilitiesV3DDL);
    const root = await directory(t), db = await database(root, 2);
    try {
      db.exec(ddl); db.exec("UPDATE cap_metadata SET value='soty.capabilities.sqlite.v3' WHERE key='lineage'; PRAGMA user_version=3");
      const before = await persistent(root);
      await assert.rejects(readStorageFormat(root), /storage_format_unreadable/u);
      assert.deepEqual(await persistent(root), before);
    } finally { db.close(); }
  }
});

test('v3 metadata remains exact and private, with no repair of a missing or malformed identity', async t => {
  for (const damage of ['missing-registry', 'uppercase-registry', 'numeric-registry', 'oversized-registry', 'missing-project', 'project', 'lineage', 'extra']) {
    const root = await directory(t), db = await database(root);
    try {
      dropForSeed(db, 'cap_metadata', () => {
        if (damage === 'missing-registry') db.exec("DELETE FROM cap_metadata WHERE key='registry_id'");
        if (damage === 'uppercase-registry') db.exec("UPDATE cap_metadata SET value=upper(value) WHERE key='registry_id'");
        if (damage === 'numeric-registry') db.exec("UPDATE cap_metadata SET value=1 WHERE key='registry_id'");
        if (damage === 'oversized-registry') db.prepare("UPDATE cap_metadata SET value=? WHERE key='registry_id'").run('f'.repeat(129));
        if (damage === 'missing-project') db.exec("DELETE FROM cap_metadata WHERE key='project_id'");
        if (damage === 'project') db.exec("UPDATE cap_metadata SET value='foreign' WHERE key='project_id'");
        if (damage === 'lineage') db.exec("UPDATE cap_metadata SET value='soty.capabilities.sqlite.v2' WHERE key='lineage'");
        if (damage === 'extra') db.exec("INSERT INTO cap_metadata VALUES('extra','private fixture')");
      });
      const before = await persistent(root);
      await assert.rejects(readStorageFormat(root), /storage_format_unknown/u, damage);
      assert.deepEqual(await persistent(root), before);
    } finally { db.close(); }
  }
});

test('marker-only3 is still invalid; future4 is refused on main and committed WAL by host and actual reader3', async t => {
  const { module } = await pinnedModule(t, 3);
  const markerRoot = await directory(t), marker = await database(markerRoot, 2);
  try {
    marker.exec('PRAGMA user_version=3');
    await assert.rejects(readStorageFormat(markerRoot), /storage_format_unknown/u);
    marker.exec("UPDATE cap_metadata SET value='soty.capabilities.sqlite.v3' WHERE key='lineage'");
    await assert.rejects(readStorageFormat(markerRoot), /storage_format_unreadable/u, 'marker + lineage still has no v3 SQL');
  } finally { marker.close(); }
  for (const wal of [false, true]) {
    const root = await directory(t), db = await database(root);
    try {
      if (wal) db.exec('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA wal_checkpoint(TRUNCATE)');
      db.exec('BEGIN IMMEDIATE; PRAGMA user_version=4; COMMIT');
      assert.equal((await readFile(file(root))).readUInt32BE(60), wal ? 3 : 4);
      const before = await persistent(root);
      await assert.rejects(readStorageFormat(root), /storage_format_unknown/u);
      assert.throws(() => module.initializeCapabilitiesSchema(db, { projectId: 'soty' }), { code: 'schema_version_unsupported' });
      assert.deepEqual(await persistent(root), before);
    } finally { db.close(); }
  }
});

test('fresh START probes actual3 after prior receipt2; only an explicit actual-image reader3 declaration may admit it', async t => {
  const root = await directory(t), db = await database(root, 2), actions = [];
  let declared = reader2;
  const runtime = { Id: '1'.repeat(64), Image: image().Id, State: { Running: false }, Config: { Env: ['DATA_DIR=/data'], Labels: { [storageReaderLabel]: reader3 } },
    Mounts: [{ Type: 'volume', RW: true, Name: 'synthetic-reader3', Source: '/volumes/synthetic-reader3/_data', Destination: '/data' }] };
  const context = { engine: { inspect: async () => runtime, image: async () => image(declared),
    start: async () => { actions.push('START'); assert.fail('guard has no permission to start'); },
    request: async (method, route) => { assert.equal(method, 'GET'); return route === '/containers/json' ? []
      : { Name: 'synthetic-reader3', Driver: 'local', Scope: 'local', Options: null, Mountpoint: runtime.Mounts[0].Source }; },
  }, probe: () => readStorageFormat(root) };
  try {
    const previous = await guardStorageStart(context, runtime); assert.equal(previous.capabilities, 2);
    db.exec('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0'); upgradeHistoricalCapabilitiesV3(db);
    const before = await persistent(root);
    await assert.rejects(guardStorageStart(context, runtime), /storage_reader_incompatible/u, 'a container label and prior receipt cannot replace actual image capability');
    assert.deepEqual(actions, []); assert.deepEqual(await persistent(root), before);
    declared = currentStorageReaders;
    const current = await guardStorageStart(context, runtime);
    assert.equal(current.capabilities, 3); assert.equal(current.notes, 'empty');
    requireStorageStartReceipt(current, runtime.Id); assert.deepEqual(actions, []);
    assert.deepEqual(await persistent(root), before);
    assert.deepEqual(checkedStorageFormat(format('empty', 3)), format('empty', 3));
    for (const value of [4, '3', [3], { valueOf: () => 3 }]) {
      assert.throws(() => checkedStorageFormat({ ...format('empty', 3), capabilities: value }), /storage_probe_invalid/u);
      assert.throws(() => requireStorageStartReceipt({ ...current, capabilities: value }, runtime.Id), /storage_start_guard_missing/u);
    }
    assert.throws(() => checkedStorageFormat(format(3, 3)), /storage_probe_invalid/u, 'Notes has no format3');
  } finally { db.close(); }
});

async function crashedWriter(t) {
  const root = await directory(t), original = await database(root, 2);
  try { seedNative(original); } finally { original.close(); }
  const script = `import { DatabaseSync } from 'node:sqlite'; import { upgradeHistoricalCapabilitiesV3 } from ${JSON.stringify(new URL('./capabilities-v3.fixture.mjs', import.meta.url).href)};
    const db=new DatabaseSync(process.argv[1]); db.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=0');
    upgradeHistoricalCapabilitiesV3(db);
    db.prepare("INSERT INTO cap_oauth_artifacts(model,id_hash,issuer,profile,key_id,payload_cipher,payload_digest,session_uid_hash,created_at,expires_at,retain_until) VALUES('Session',?,'https://reader3.test/oauth','oidc-provider-9.12.2-c1','synthetic-reader-witness',?,?,?,?,?,?)")
      .run('c'.repeat(64),Buffer.alloc(30,7),'d'.repeat(64),'e'.repeat(64),10000,11000,11000);
    process.stdout.write('ready\\n'); setInterval(()=>{},1000);`;
  const child = spawn(process.execPath, ['--input-type=module', '-e', script, file(root)], { stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
  const exited = once(child, 'exit');
  try {
    await new Promise((resolve, reject) => {
      let text = '';
      const timeout = setTimeout(() => finish(new Error('reader3_writer_ready_timeout')), 5000);
      const earlyExit = () => finish(new Error('reader3_writer_early_exit'));
      const finish = error => { clearTimeout(timeout); child.stdout.removeAllListeners('data'); child.off('error', finish); child.off('exit', earlyExit); error ? reject(error) : resolve(); };
      child.once('error', finish); child.once('exit', earlyExit);
      child.stdout.on('data', chunk => { text += chunk.toString(); if (text === 'ready\n') finish(); else if (text.length > 32) finish(new Error('reader3_writer_invalid_ready')); });
    });
    child.kill('SIGKILL'); await exited;
    assert.equal((await readFile(file(root))).readUInt32BE(60), 2);
    return root;
  } finally {
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    await exited;
  }
}
function coldWitness(db) {
  return { schema: layout(db), native: baseWitness(db), oauth: db.prepare('SELECT * FROM cap_oauth_artifacts').all() };
}
test('a real exited writer leaves populated main2/WAL3; host RO readers and old2 RO refusal preserve bytes', async t => {
  const root = await crashedWriter(t), oldProbe = await oldHostProbe(t), old = await pinnedModule(t, 2);
  const before = await persistent(root), names = await readdir(path.dirname(file(root))); assert.ok(before['-wal']);
  await assert.rejects(oldProbe(root), /storage_format_unknown/u);
  const oldHandle = new DatabaseSync(file(root), { readOnly: true });
  try {
    assert.throws(() => old.module.initializeCapabilitiesSchema(oldHandle, { projectId: 'soty' }), { code: 'schema_version_unsupported' });
    assert.equal(oldHandle.prepare('SELECT count(*) AS n FROM cap_receipts').get().n, 1);
    assert.equal(oldHandle.prepare('SELECT count(*) AS n FROM cap_oauth_artifacts').get().n, 1);
  } finally { oldHandle.close(); }
  assert.deepEqual(await readStorageFormat(root), format('empty', 3));
  assert.deepEqual(await persistent(root), before); assert.deepEqual(await readdir(path.dirname(file(root))), names);
});

test('cold old2 writable refusal is logical only: last RW close checkpoints WAL without downgrading rows', async t => {
  const root = await crashedWriter(t), old = await pinnedModule(t, 2), before = await persistent(root);
  const oldHandle = new DatabaseSync(file(root));
  let witness;
  try {
    assert.throws(() => old.module.initializeCapabilitiesSchema(oldHandle, { projectId: 'soty' }), { code: 'schema_version_unsupported' });
    assert.deepEqual(await persistent(root), before, 'old application refusal itself precedes persistent PRAGMA/write');
    witness = coldWitness(oldHandle);
  } finally { oldHandle.close(); }
  const afterClose = await persistent(root);
  assert.notEqual(afterClose[''], before[''], 'observed SQLite last-writable-close checkpoint after crashed writer');
  assert.equal((await readFile(file(root))).readUInt32BE(60), 3, 'checkpoint retains current schema, never downgrade to2');
  assert.ok(afterClose['-wal'] === undefined || afterClose['-wal'] === sha(Buffer.alloc(0)));
  const observer = new DatabaseSync(file(root), { readOnly: true });
  try {
    assert.deepEqual(coldWitness(observer), witness);
    assert.equal(observer.prepare('PRAGMA user_version').get().user_version, 3);
    assert.equal(witness.native.cap_receipts.length, 1); assert.equal(witness.oauth.length, 1);
  } finally { observer.close(); }
  // On a writable temp directory SQLite may create an empty WAL while opening
  // the separate observer after the checkpoint removed the old sidecars. That
  // observer is not the host probe; compare exactly around the latter's call.
  const beforeProbe = await persistent(root);
  assert.equal(beforeProbe[''], afterClose['']);
  assert.ok(beforeProbe['-wal'] === undefined || beforeProbe['-wal'] === sha(Buffer.alloc(0)));
  assert.equal((await readStorageFormat(root)).capabilities, 3);
  assert.deepEqual(await persistent(root), beforeProbe, 'subsequent host RO stays byte preserving');
});

test('existing malformed/truncated files and orphan sidecars never become empty format', async t => {
  for (const damage of ['zero', 'truncated', 'orphan', 'extra']) {
    const root = await directory(t), store = path.dirname(file(root)); await mkdir(store);
    if (damage === 'zero') await writeFile(file(root), Buffer.alloc(0));
    if (damage === 'truncated') await writeFile(file(root), Buffer.from('SQLite format 3\0' + 'x'.repeat(84)));
    if (damage === 'orphan') await writeFile(file(root) + '-wal', Buffer.alloc(64));
    if (damage === 'extra') { (await database(root)).close(); await writeFile(path.join(store, 'unexpected'), 'synthetic'); }
    const before = await persistent(root), names = await readdir(store);
    await assert.rejects(readStorageFormat(root), /storage_format_unreadable/u, damage);
    assert.deepEqual(await persistent(root), before); assert.deepEqual(await readdir(store), names);
  }
});
