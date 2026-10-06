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
import { createHistoricalCapabilitiesV1 } from './capabilities-v1.fixture.mjs';
import { createHistoricalNotesV2, upgradeHistoricalNotesV2, nativeNotesV2DDL } from './notes-v2.fixture.mjs';
import { createHistoricalCapabilitiesV2, upgradeHistoricalCapabilitiesV2, nativeCapabilitiesV2DDL } from './capabilities-v2.fixture.mjs';
import { readStorageFormat } from './storage-probe.mjs';
import { historicalSourceBytes } from './historical-source.fixture.mjs';
import { assertStorageCompatible, currentStorageReaders, guardStorageStart, storageReaderLabel } from './storage-guard.mjs';

const baseline = '5e459abc6afa376861c2032226bd29f78bf0468d';
const bridge = 'ae914f55e6d8d64628a7279189d2d55dfd05de45';
const bridgeGitHash = 'd0aed27ce790502f36df20689663a43aee0ed28266e6b424a002cde7eb2f4e17';
const bridgeWindowsHash = 'd51b36c12e316e660fd2b3ab6d8fe2350833aff242f2ae95d296fb63409afd59';
const bridgeReaders = '{"version":3,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5,6],"notes":[1],"capabilities":[1]}}';
const digest = value => createHash('sha256').update(value).digest('hex');
const sql = value => value.split(/('(?:[^']|'')*')/gu).map((part, index) => index % 2 ? part
  : part.replace(/\s+/gu, '').replace(/;$/u, '').toLowerCase()).join('');
const layout = db => db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all()
  .map(row => ({ ...row, sql: sql(row.sql) }));
const format = (notes, capabilities) => ({ ok: true, schema: 'soty.storage-format.v3', rooms: 'empty', apps: 'empty', notes, capabilities });
const stores = {
  notes: { main: 'notes.sqlite', meta: 'notes_meta', create1: createHistoricalNotesV1, create2: createHistoricalNotesV2,
    upgrade: upgradeHistoricalNotesV2, ddl: nativeNotesV2DDL, native: 'note_native_creates', count: 19,
    migrate: (module, db, enabled = false) => module.migrateNotes(db, 'soty', { allowNativeMigration: enabled }) },
  capabilities: { main: 'capabilities.sqlite', meta: 'cap_metadata', create1: createHistoricalCapabilitiesV1, create2: createHistoricalCapabilitiesV2,
    upgrade: upgradeHistoricalCapabilitiesV2, ddl: nativeCapabilitiesV2DDL, native: 'cap_native_note_intents', count: 36,
    migrate: (module, db, enabled = false) => module.initializeCapabilitiesSchema(db, { projectId: 'soty', allowNativeMigration: enabled }) },
};
const file = (root, store) => path.join(root, store, stores[store].main);
const notesDigest = '95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204';
const canonical = value => JSON.stringify(value, (_key, item) => item && typeof item === 'object' && !Array.isArray(item)
  ? Object.fromEntries(Object.keys(item).sort().map(key => [key, item[key]])) : item);
const put = (db, table, values) => db.prepare(`INSERT INTO ${table}(${Object.keys(values).join(',')}) VALUES(${Object.keys(values).map(() => '?').join(',')})`).run(...Object.values(values));
const git = (pin, sourcePath) => historicalSourceBytes(pin, sourcePath).toString('utf8');

async function directory(t) {
  const root = await mkdtemp(path.join(os.tmpdir(), 'soty-native-reader2-'));
  t.after(async () => {
    assert.equal(path.dirname(path.resolve(root)), path.resolve(os.tmpdir()));
    assert.match(path.basename(root), /^soty-native-reader2-/u);
    await rm(root, { recursive: true, force: true });
  });
  return root;
}
async function database(root, store, version = 2) {
  await mkdir(path.join(root, store), { recursive: true });
  const db = new DatabaseSync(file(root, store));
  try { db.exec('PRAGMA foreign_keys=ON'); stores[store][`create${version}`](db); return db; }
  catch (error) { db.close(); throw error; }
}
async function persistent(root, store) {
  const hashes = {};
  for (const suffix of ['', '-wal']) {
    try { hashes[suffix] = digest(await readFile(file(root, store) + suffix)); }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
  }
  return hashes;
}
async function historicalModule(t, store) {
  const provenance = JSON.parse(await readFile(new URL(`./fixtures/${store}-v2/provenance.json`, import.meta.url), 'utf8'));
  assert.equal(provenance.commit, baseline);
  const root = await directory(t);
  for (const [sourcePath, expected] of Object.entries(provenance.files)) {
    assert.match(sourcePath, new RegExp(`^modules/${store}/server/[a-z0-9-]+\\.mjs$`, 'u'));
    const value = git(baseline, sourcePath);
    assert.equal(digest(value), expected.sha256); assert.equal(Buffer.byteLength(value), expected.bytes);
    const target = path.join(root, sourcePath); await mkdir(path.dirname(target), { recursive: true }); await writeFile(target, value);
  }
  return { module: await import(pathToFileURL(path.join(root, provenance.sourcePath)).href), provenance };
}
async function historicalProbe(t) {
  const source = git(bridge, 'deploy/connector/storage-probe.mjs');
  assert.equal(digest(source), bridgeGitHash);
  assert.equal(digest(source.replaceAll('\n', '\r\n')), bridgeWindowsHash);
  const root = await directory(t), target = path.join(root, 'bridge-probe.mjs'); await writeFile(target, source);
  return (await import(pathToFileURL(target).href)).readStorageFormat;
}

function seedNotes(db) {
  const owner = 'reader2_owner', source = 'b'.repeat(32);
  const document = { title: 'Synthetic café', body: 'Human edited after native create', items: [], color: 'plain', pinned: false, state: 'active' };
  const bytes = Buffer.byteLength(JSON.stringify(document));
  put(db, 'note_accounts', { account_id: owner, bytes, identities: 2, active: 1, archived: 0, trashed: 0 });
  for (const [index, removed] of [false, true].entries()) {
    const invocation = 'reader2_notes_' + index, suffix = digest(JSON.stringify(['soty.native-note.v1', source, invocation]));
    const id = 'n_' + suffix, mutation = 'm_' + suffix;
    put(db, 'notes', { rowid: index + 1, account_id: owner, id, title: removed ? '' : document.title, body: removed ? '' : document.body,
      items: '[]', preview: removed ? '' : document.body, color: 'plain', pinned: 0, state: removed ? 'deleted' : 'active', revision: removed ? 3 : 2,
      bytes: removed ? 0 : bytes, created_at: 100, updated_at: 110 });
    if (!removed) db.prepare('INSERT INTO notes_fts(rowid,scope,title,body,items) VALUES(?,?,?,?,?)').run(index + 1, digest(owner), document.title, document.body, '');
    put(db, 'note_native_creates', { source_store_id: source, invocation_id: invocation, account_id: owner, note_id: id, mutation_id: mutation,
      input_digest: digest('synthetic original input'), capability_digest: notesDigest, revision: 1, created_at: 100 });
  }
}
function seedCapabilities(db) {
  const owner = 'reader2_owner', storeId = 'b'.repeat(32);
  put(db, 'cap_clients', { id: 'reader2_client', account_id: owner, label: 'Synthetic', state: 'active', policy_epoch: 1, created_at: 100 });
  put(db, 'cap_principals', { id: 'reader2_principal', account_id: owner, client_id: 'reader2_client', kind: 'service', label: 'Synthetic', state: 'active', creator_device_id: 'reader2_device', created_at: 100 });
  put(db, 'cap_grants', { id: 'reader2_grant', account_id: owner, client_id: 'reader2_client', principal_id: 'reader2_principal', parent_id: null,
    root_id: 'reader2_grant', creator_device_id: 'reader2_device', capabilities_json: '[]', resources_json: '[]', effects_json: '[]', recipients_json: '[]',
    allow_delegation: 0, max_depth: 0, depth: 0, not_before: 100, expires_at: 10000, policy_epoch: 1, created_at: 100 });
  put(db, 'cap_credentials', { id: 'reader2_credential', digest: digest('synthetic credential digest'), account_id: owner, client_id: 'reader2_client', principal_id: 'reader2_principal',
    grant_id: 'reader2_grant', audience: 'synthetic', expires_at: 10000, created_at: 100 });
  put(db, 'cap_budgets', { root_grant_id: 'reader2_grant', unit: 'invocations', limit_amount: 5, reserved_amount: 1, spent_amount: 1 });
  for (const completed of [false, true]) {
    const id = completed ? 'reader2_completed' : 'reader2_pending', suffix = digest(canonical(['soty.native-note.v1', storeId, id]));
    const input = { title: 'Synthetic', body: 'Retained private synthetic input' }, inputJson = canonical(input);
    const target = { kind: 'native', handler: 'notes.createDraft', version: 1 };
    const authority = { accountId: owner, clientId: 'reader2_client', principalId: 'reader2_principal', credentialId: 'reader2_credential', audience: 'synthetic',
      grantId: 'reader2_grant', rootGrantId: 'reader2_grant', policyEpoch: 1, expiresAt: 10000, capabilityId: 'notes.createDraft', version: 1, capabilityDigest: notesDigest,
      resources: ['notes:new'], effects: ['create'], recipients: ['soty:notes'], executionBinding: target, charges: [{ unit: 'invocations', amount: 1 }] };
    const requestDigest = digest(canonical({ capabilityId: 'notes.createDraft', version: 1, capabilityDigest: notesDigest, input, target,
      resources: ['notes:new'], effects: ['create'], recipients: ['soty:notes'] }));
    put(db, 'cap_budget_reservations', { id: 'reservation_' + id, invocation_id: id, attempt_id: 'attempt_' + id, root_grant_id: 'reader2_grant', unit: 'invocations',
      amount: 1, actual_amount: completed ? 1 : null, disposition: completed ? 'spent' : 'uncertain', request_digest: requestDigest, created_at: 100, updated_at: 110 });
    put(db, 'cap_invocations', { id, account_id: owner, client_id: 'reader2_client', principal_id: 'reader2_principal', grant_id: 'reader2_grant', root_grant_id: 'reader2_grant', policy_epoch: 1,
      capability_id: 'notes.createDraft', capability_version: 1, capability_digest: notesDigest, request_key: digest(id), request_digest: requestDigest, internal_request_id: 'internal_' + id,
      input_json: inputJson, target_json: canonical(target), authorization_json: canonical(authority), status: 'execution_uncertain', effect_state: 'unknown', cancel_requested: 0,
      effects_json: '[]', reservation_id: 'reservation_' + id, created_at: 100, updated_at: 110 });
    put(db, 'cap_dispatch_intents', { invocation_id: id, internal_request_id: 'internal_' + id, state: 'uncertain', created_at: 100, updated_at: 110 });
    put(db, 'cap_native_note_intents', { invocation_id: id, account_id: owner, notes_store_id: 'a'.repeat(32), note_id: 'n_' + suffix, mutation_id: 'm_' + suffix,
      input_digest: digest(inputJson), input_bytes: Buffer.byteLength(inputJson), started_at: 105, input_purged_at: null });
    if (completed) {
      const effects = [{ kind: 'created', resourceType: 'note', resourceId: 'n_' + suffix, revision: 1 }];
      const receipt = { verificationMethod: 'domain_read', artifacts: [{ type: 'note', id: 'n_' + suffix, revision: 1 }] };
      put(db, 'cap_receipts', { invocation_id: id, value_json: canonical(receipt), digest: digest(canonical({ status: 'succeeded', effectState: 'committed', effects, receipt,
        disposition: 'spent', actualCharges: [{ unit: 'invocations', amount: 1 }] })), created_at: 110 });
      db.prepare("UPDATE cap_invocations SET status='succeeded',effect_state='committed',effects_json=?,input_json='null',completed_at=110 WHERE id=?").run(canonical(effects), id);
      db.prepare('UPDATE cap_native_note_intents SET input_purged_at=110 WHERE invocation_id=?').run(id);
    }
  }
}

for (const [store, spec] of Object.entries(stores)) test(`${store}2 literal matches all pinned B1a SQL objects and default-off reopen keeps both versions`, async t => {
  const { module, provenance } = await historicalModule(t, store);
  assert.equal(digest(spec.ddl), provenance.ddl.sha256); assert.equal(Buffer.byteLength(spec.ddl), provenance.ddl.bytes);
  const reference = new DatabaseSync(':memory:'), literal = new DatabaseSync(':memory:');
  try {
    assert.equal(spec.migrate(module, reference, true).schemaVersion, 2); spec.create2(literal);
    assert.equal(layout(literal).length, spec.count); assert.deepEqual(layout(literal), layout(reference));
    for (const [name, expected] of Object.entries(provenance.objects)) {
      assert.equal(digest(layout(literal).find(row => row.name === name).sql), expected, name);
    }
  } finally { reference.close(); literal.close(); }
  for (const version of [1, 2]) {
    const root = await directory(t), db = await database(root, store, version);
    try {
      const before = db.prepare(`SELECT key,value FROM ${spec.meta} ORDER BY key`).all();
      const result = spec.migrate(module, db);
      assert.equal(result.schemaVersion, version); assert.equal(result.registryId === null, version === 1);
      assert.deepEqual(db.prepare(`SELECT key,value FROM ${spec.meta} ORDER BY key`).all(), before);
    } finally { db.close(); }
  }
});

test('all nine independent empty/v1/v2 pairs are read-only and disclose only format fields', async t => {
  for (const notes of ['empty', 1, 2]) for (const capabilities of ['empty', 1, 2]) {
    const root = await directory(t);
    for (const [store, version] of Object.entries({ notes, capabilities })) if (version !== 'empty') (await database(root, store, version)).close();
    const before = await Promise.all(Object.keys(stores).map(store => persistent(root, store)));
    const result = await readStorageFormat(root); assert.deepEqual(result, format(notes, capabilities));
    assertStorageCompatible({ Id: 'sha256:' + '1'.repeat(64), Config: { Labels: { [storageReaderLabel]: currentStorageReaders } } }, result);
    assert.deepEqual(await Promise.all(Object.keys(stores).map(store => persistent(root, store))), before);
    assert.deepEqual(Object.keys(result).sort(), ['apps', 'capabilities', 'notes', 'ok', 'rooms', 'schema']);
  }
});

for (const [store, spec] of Object.entries(stores)) test(`${store}: populated native proof/ledger seed survives host read and exact default-off B1a reopen`, async t => {
  const { module } = await historicalModule(t, store), root = await directory(t), db = await database(root, store);
  try {
    // Synthetic storage states, not a claim that native admission/effect ran.
    (store === 'notes' ? seedNotes : seedCapabilities)(db);
    assert.deepEqual(db.prepare('PRAGMA foreign_key_check').all(), []);
    const before = await persistent(root, store);
    assert.equal((await readStorageFormat(root))[store], 2);
    assert.deepEqual(await persistent(root, store), before);
    const records = () => db.prepare(`SELECT * FROM ${spec.native} ORDER BY ${store === 'notes' ? 'source_store_id,' : ''}invocation_id`).all();
    const retained = records(), identity = db.prepare(`SELECT value FROM ${spec.meta} WHERE key='registry_id'`).get().value;
    const state = spec.migrate(module, db); assert.equal(state.schemaVersion, 2); assert.equal(state.registryId, identity);
    assert.deepEqual(records(), retained);
    if (store === 'notes') assert.equal(db.prepare("SELECT count(*) AS n FROM notes_fts WHERE notes_fts MATCH 'cafe'").get().n, 1);
    else {
      assert.deepEqual({ ...db.prepare('SELECT reserved_amount,spent_amount FROM cap_budgets').get() }, { reserved_amount: 1, spent_amount: 1 });
      assert.equal(db.prepare('SELECT count(*) AS n FROM cap_receipts').get().n, 1);
    }
  } finally { db.close(); }
});

for (const [store, spec] of Object.entries(stores)) test(`${store}: genuine main1/WAL2 is recognized; frozen bridge and actual reader1 remain refused byte-for-byte`, async t => {
  const oldProbe = await historicalProbe(t), root = await directory(t), db = await database(root, store, 1);
  try {
    db.exec('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA synchronous=FULL');
    spec.upgrade(db);
    assert.equal((await readFile(file(root, store))).readUInt32BE(60), 1);
    assert.equal(db.prepare('PRAGMA user_version').get().user_version, 2);
    const before = await persistent(root, store); assert.ok(before['-wal']);
    await assert.rejects(oldProbe(root), /storage_format_unknown/u);
    const result = await readStorageFormat(root);
    assert.deepEqual(result, format(store === 'notes' ? 2 : 'empty', store === 'capabilities' ? 2 : 'empty'));
    const image = { Id: 'sha256:' + '1'.repeat(64), Config: { Labels: { [storageReaderLabel]: bridgeReaders } } };
    assert.throws(() => assertStorageCompatible(image, result), /storage_reader_incompatible/u);
    assert.deepEqual(await persistent(root, store), before);
  } finally { db.close(); }
});

for (const [store, spec] of Object.entries(stores)) test(`${store}: every v2 guard and index is required with its exact table and SQL`, async t => {
  const base = new DatabaseSync(':memory:'); spec.create2(base);
  const additions = base.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE type IN ('index','trigger') AND name NOT GLOB 'sqlite_*'").all()
    .filter(row => spec.ddl.includes(row.name)); base.close();
  assert.equal(additions.length, store === 'notes' ? 6 : 15);
  for (const object of additions) for (const damage of ['missing', 'behavior', ...(object.type === 'trigger' ? ['target'] : [])]) {
    const root = await directory(t), db = await database(root, store);
    try {
      db.exec(`DROP ${object.type} ${object.name}`);
      if (damage === 'behavior') db.exec(object.type === 'trigger'
        ? `CREATE TRIGGER ${object.name} BEFORE INSERT ON ${object.tbl_name} BEGIN SELECT 1; END`
        : `CREATE INDEX ${object.name} ON ${object.tbl_name}(id)`);
      if (damage === 'target') db.exec(object.sql.replace(new RegExp(`\\bON ${object.tbl_name}\\b`, 'u'),
        `ON ${object.tbl_name === spec.meta ? spec.native : spec.meta}`));
    } finally { db.close(); }
    const before = await persistent(root, store);
    await assert.rejects(readStorageFormat(root), /storage_format_unreadable/u, `${object.name}/${damage}`);
    assert.deepEqual(await persistent(root, store), before);
  }
});

for (const [store, spec] of Object.entries(stores)) test(`${store}: v2 table keys/FK/CHECK/STRICT and extra objects cannot imitate an accepted layout`, async t => {
  const variations = [
    spec.ddl.replace(') STRICT;', ');'),
    spec.ddl.replace('input_digest TEXT', 'input_digest BLOB'),
    spec.ddl.replace('UNIQUE(account_id,note_id)', 'UNIQUE(account_id,note_id,mutation_id)'),
    spec.ddl.replace(store === 'notes' ? 'REFERENCES notes(account_id,id)' : 'REFERENCES cap_invocations(id,account_id)',
      store === 'notes' ? 'REFERENCES notes(id,account_id)' : 'REFERENCES cap_invocations(account_id,id)'),
    spec.ddl.replace(store === 'notes' ? 'CHECK(revision=1)' : 'CHECK(input_bytes BETWEEN 1 AND 262144)',
      store === 'notes' ? 'CHECK(revision>=1)' : 'CHECK(input_bytes BETWEEN 1 AND 262145)'),
    spec.ddl + '\nCREATE TABLE extra(value TEXT);', spec.ddl + `\nCREATE VIEW extra AS SELECT * FROM ${spec.native};`,
    spec.ddl + `\nCREATE INDEX extra ON ${spec.native}(account_id);`,
  ];
  if (store === 'capabilities') variations.push(spec.ddl.replace("WHERE status NOT IN ('succeeded','failed','cancelled')", "WHERE status NOT IN ('succeeded','failed')"));
  for (const ddl of variations) {
    assert.notEqual(ddl, spec.ddl);
    const root = await directory(t), db = await database(root, store, 1);
    try {
      db.exec('PRAGMA foreign_keys=OFF');
      db.prepare(`INSERT INTO ${spec.meta}(key,value) VALUES('registry_id',?)`).run('c'.repeat(32));
      if (store === 'capabilities') db.prepare(`INSERT INTO ${spec.meta} VALUES('project_id','soty')`).run();
      db.prepare(`UPDATE ${spec.meta} SET value=? WHERE key='lineage'`).run(`soty.${store}.sqlite.v2`);
      db.exec(ddl); db.exec('PRAGMA user_version=2');
    } finally { db.close(); }
    const before = await persistent(root, store);
    await assert.rejects(readStorageFormat(root), /storage_format_unreadable/u);
    assert.deepEqual(await persistent(root, store), before);
  }
});

for (const [store, spec] of Object.entries(stores)) test(`${store}: exact metadata/identity is mandatory, never backfilled or exposed`, async t => {
  for (const damage of ['missing-registry', 'wrong-registry', 'uppercase-registry', 'oversize-registry', 'blob-registry', 'project', 'missing-project', 'lineage', 'extra', 'null-key']) {
    const root = await directory(t), db = await database(root, store);
    try {
      // A malformed external database retains all literal guards: only this
      // fixture temporarily removes/reinstalls guards to seed corrupt metadata.
      const guards = db.prepare("SELECT name,sql FROM sqlite_schema WHERE type='trigger' AND tbl_name=?").all(spec.meta);
      for (const guard of guards) db.exec(`DROP TRIGGER ${guard.name}`);
      if (damage === 'missing-registry') db.exec(`DELETE FROM ${spec.meta} WHERE key='registry_id'`);
      if (damage === 'wrong-registry') db.exec(`UPDATE ${spec.meta} SET value='invalid' WHERE key='registry_id'`);
      if (damage === 'uppercase-registry') db.exec(`UPDATE ${spec.meta} SET value=upper(value) WHERE key='registry_id'`);
      if (damage === 'oversize-registry') db.prepare(`UPDATE ${spec.meta} SET value=? WHERE key='registry_id'`).run('a'.repeat(100000));
      if (damage === 'blob-registry') {
        if (store === 'capabilities') { db.close(); continue; } // STRICT rejects the insertion itself; Notes TEXT affinity permits BLOB.
        db.prepare(`UPDATE ${spec.meta} SET value=? WHERE key='registry_id'`).run(Buffer.from('a'.repeat(32)));
      }
      if (damage === 'project') db.exec(`UPDATE ${spec.meta} SET value='other-project' WHERE key='project_id'`);
      if (damage === 'missing-project') db.exec(`DELETE FROM ${spec.meta} WHERE key='project_id'`);
      if (damage === 'lineage') db.exec(`UPDATE ${spec.meta} SET value='soty.unknown.v2' WHERE key='lineage'`);
      if (damage === 'extra') db.exec(`INSERT INTO ${spec.meta} VALUES('unexpected','x')`);
      if (damage === 'null-key') {
        if (store === 'capabilities') { db.close(); continue; }
        db.exec(`INSERT INTO ${spec.meta} VALUES(NULL,'x')`);
      }
      for (const guard of guards) db.exec(guard.sql);
    } finally { if (db.isOpen) db.close(); }
    const before = await persistent(root, store);
    await assert.rejects(readStorageFormat(root), /storage_format_unknown/u, damage);
    assert.deepEqual(await persistent(root, store), before);
  }
  for (const version of [1, 2]) {
    const root = await directory(t), db = await database(root, store, version);
    try { db.exec(`INSERT INTO ${spec.meta} VALUES('unknown-key','x')`); } finally { db.close(); }
    await assert.rejects(readStorageFormat(root), /storage_format_unknown/u);
  }
});

for (const store of Object.keys(stores)) test(`${store}: later marker-only3 in WAL invalidates a prior reader2 START receipt without changing main/WAL`, async t => {
  const root = await directory(t), db = await database(root, store);
  const runtime = { Id: '1'.repeat(64), Image: 'sha256:' + '2'.repeat(64), State: { Running: false }, Config: { Env: ['DATA_DIR=/data'] },
    Mounts: [{ Type: 'volume', RW: true, Name: 'synthetic-reader2', Source: '/volumes/synthetic-reader2/_data', Destination: '/data' }] };
  const context = { engine: { inspect: async () => runtime,
    image: async () => ({ Id: runtime.Image, Config: { Labels: { [storageReaderLabel]: currentStorageReaders } } }),
    request: async (method, route) => { assert.equal(method, 'GET'); return route === '/containers/json' ? []
      : { Name: 'synthetic-reader2', Driver: 'local', Scope: 'local', Options: null, Mountpoint: runtime.Mounts[0].Source }; },
  }, probe: () => readStorageFormat(root) };
  try {
    const receipt = await guardStorageStart(context, runtime); assert.equal(receipt[store], 2);
    db.exec('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; BEGIN IMMEDIATE; PRAGMA user_version=3; COMMIT');
    assert.equal((await readFile(file(root, store))).readUInt32BE(60), 2);
    const before = await persistent(root, store);
    await assert.rejects(guardStorageStart(context, runtime), /storage_format_unknown/u);
    assert.deepEqual(await persistent(root, store), before);
  } finally { db.close(); }
});

for (const store of Object.keys(stores)) test(`${store}: exited real writer leaves main1/WAL2 readable through reader2, not frozen bridge`, async t => {
  const root = await directory(t); (await database(root, store, 1)).close();
  const oldProbe = await historicalProbe(t);
  const exported = store === 'notes' ? 'upgradeHistoricalNotesV2' : 'upgradeHistoricalCapabilitiesV2';
  const script = `import { DatabaseSync } from 'node:sqlite'; import { ${exported} as upgrade } from ${JSON.stringify(new URL(`./${store}-v2.fixture.mjs`, import.meta.url).href)};
    const db=new DatabaseSync(process.argv[1]); db.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=0');
    upgrade(db); process.stdout.write('ready\\n'); setInterval(()=>{},1000);`;
  const child = spawn(process.execPath, ['--input-type=module', '-e', script, file(root, store)], { stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
  const exited = once(child, 'exit');
  try {
    await new Promise((resolve, reject) => {
      let text = '';
      const timeout = setTimeout(() => finish(new Error('reader2_writer_ready_timeout')), 5000);
      const earlyExit = () => finish(new Error('reader2_writer_early_exit'));
      const finish = error => { clearTimeout(timeout); child.stdout.removeAllListeners('data'); child.off('error', finish); child.off('exit', earlyExit); error ? reject(error) : resolve(); };
      child.once('error', finish); child.once('exit', earlyExit);
      child.stdout.on('data', chunk => { text += chunk.toString(); if (text === 'ready\n') finish(); else if (text.length > 32) finish(new Error('reader2_writer_invalid_ready')); });
    });
    child.kill('SIGKILL'); await exited;
    assert.equal((await readFile(file(root, store))).readUInt32BE(60), 1);
    const before = await persistent(root, store), names = await readdir(path.join(root, store));
    assert.ok(before['-wal']); assert.ok(names.includes(stores[store].main + '-shm'));
    await assert.rejects(oldProbe(root), /storage_format_unknown/u);
    assert.equal((await readStorageFormat(root))[store], 2);
    assert.deepEqual(await persistent(root, store), before); assert.deepEqual(await readdir(path.join(root, store)), names);
  } finally {
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    await exited;
  }
});
