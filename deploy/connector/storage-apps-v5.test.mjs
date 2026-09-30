import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, mkdir, readFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createHistoricalAppsV4, seedHistoricalRollbackV4 } from './apps-v4.fixture.mjs';
import { seedHistoricalPublicationV3 } from './apps-v3.fixture.mjs';
import { migrateAppsSchema as oldAppsV4Migrator } from './fixtures/apps-v4/schema.mjs';
import { migrateAppsSchema } from '../../modules/apps/server/schema.mjs';
import { readStorageFormat } from './storage-probe.mjs';
import { assertStorageCompatible, currentStorageReaders, storageReaderLabel } from './storage-guard.mjs';

const format = apps => ({ ok: true, schema: 'soty.storage-format.v2', rooms: 'empty', apps });
const image = readers => ({ Id: 'sha256:' + '7'.repeat(64), Config: { Labels: { [storageReaderLabel]: readers } } });
const oldReaders = '{"version":2,"readers":{"rooms":[1,2],"apps":[1,2,3,4]}}';
const appId = letter => `app-${letter.repeat(32)}`;
const liveDomain = 'dom_' + 'c'.repeat(32), retiredDomain = 'dom_' + 'd'.repeat(32);
const normalizedSql = sql => sql.split(/('(?:[^']|'')*')/gu).map((part, index) => index % 2 ? part
  : part.replace(/\s+/gu, '').replace(/;$/u, '').toLowerCase()).join('');
const objects = db => db.prepare("SELECT type,name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all()
  .map(row => ({ ...row, sql: normalizedSql(row.sql) }));

async function directory(t) {
  const root = await mkdtemp(path.join(os.tmpdir(), 'soty-apps5-reader-'));
  t.after(async () => {
    assert.equal(path.dirname(path.resolve(root)), path.resolve(os.tmpdir()));
    assert.match(path.basename(root), /^soty-apps5-reader-/u);
    await rm(root, { recursive: true, force: true });
  });
  await mkdir(path.join(root, 'apps'));
  return { root, filename: path.join(root, 'apps', 'registry.sqlite') };
}

function seedHistoricalApps(db) {
  createHistoricalAppsV4(db);
  db.exec('PRAGMA foreign_keys=ON');
  db.prepare('INSERT INTO app_devices VALUES (?,?,?,?,?)').run('connector-owner', 'account-owner', '{"synthetic":true}', 'Fixture device', 1);
  for (const [letter, state, grants] of [['a', 'enabled', { accountIds: ['account-reader'], communityIds: ['community-original'] }],
    ['b', 'revoked', { accountIds: [], communityIds: [] }], ['c', 'enabled', { accountIds: ['account-private'], communityIds: [] }]]) {
    const id = appId(letter);
    db.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)').run(id, 'account-owner', 'connector-owner',
      `Historical ${letter}`, 9001, '/board?tag=a%2Bb#item', JSON.stringify(grants), state, 3, 1, 3);
    for (const account of grants.accountIds) db.prepare("INSERT INTO local_app_grants VALUES (?,'account',?)").run(id, account);
    for (const group of grants.communityIds) db.prepare("INSERT INTO local_app_grants VALUES (?,'community',?)").run(id, group);
    db.prepare('INSERT INTO app_domain_heads VALUES (?,0)').run(id);
    seedHistoricalPublicationV3(db, id);
    if (letter === 'a') seedHistoricalRollbackV4(db, id); else db.prepare('INSERT INTO app_source_heads VALUES (?,1)').run(id);
  }
  db.exec("INSERT INTO app_domain_zones VALUES ('zone_historical','named','https://{slug}.apps.example','apps.example','https','',1)");
  db.prepare("INSERT INTO app_domains VALUES (?,'zone_historical','live.apps.example','https://live.apps.example','live',?,'account-owner','alias','bound',1,NULL)").run(liveDomain, appId('a'));
  db.prepare("INSERT INTO app_domains VALUES (?,'zone_historical','retired.apps.example','https://retired.apps.example','retired',?,'account-owner','alias','tombstone',1,2)").run(retiredDomain, appId('b'));
  db.prepare("INSERT INTO app_domain_receipts VALUES ('account-owner','historical-domain-key','historical-domain-hash','retire',?,1,2)").run(retiredDomain);
  db.prepare('INSERT INTO app_publication_domains VALUES (?,?,?)').run(appId('a'), liveDomain, 'account-owner');
  const target = db.prepare('SELECT digest,profile FROM app_runtime_targets WHERE app_id=? AND revision=1').get(appId('a'));
  const ack = JSON.stringify({ scope: 'whole-port', targetRevision: 1, targetDigest: target.digest, profile: target.profile });
  db.prepare("UPDATE app_publications SET launch_policy='anyone',listed=1,exposure_ack_revision=1,exposure_ack_json=? WHERE app_id=?").run(ack, appId('a'));
  db.prepare('INSERT INTO app_publication_receipts VALUES (?,?,?,?,?,?,?)').run('account-owner', 'historical-policy-key',
    'historical-policy-hash', appId('a'), 4, '{"synthetic":"historical-policy"}', 4);
}

test('the literal historical Apps4 fixture matches the exact committed old migrator, independently of current source', async () => {
  const provenance = JSON.parse(await readFile(new URL('./fixtures/apps-v4/provenance.json', import.meta.url), 'utf8'));
  assert.equal(provenance.commit, '4fa5f722403dfbb1dc9fdf67965071a398c3537c');
  assert.deepEqual(Object.keys(provenance.files).sort(), ['domain-policy.mjs', 'protocol.mjs', 'schema.mjs']);
  for (const [name, entry] of Object.entries(provenance.files)) {
    const source = await readFile(new URL(`./fixtures/apps-v4/${name}`, import.meta.url), 'utf8');
    // Git may check text out with CRLF; no code other than line endings varies.
    assert.equal(createHash('sha256').update(source.replaceAll('\r\n', '\n')).digest('hex'), entry.sha256, name);
    assert.equal(entry.sourcePath, `modules/apps/server/${name}`);
  }
  const literal = new DatabaseSync(':memory:'), actual = new DatabaseSync(':memory:'), seeded = new DatabaseSync(':memory:');
  try {
    createHistoricalAppsV4(literal); oldAppsV4Migrator(actual);
    assert.equal(objects(literal).length, 30);
    assert.deepEqual(objects(literal), objects(actual));
    assert.equal(literal.prepare('PRAGMA user_version').get().user_version, 4);
    assert.equal(literal.prepare("SELECT value FROM apps_meta WHERE key='schema'").get().value, 'soty.apps-registry.v4');
    seedHistoricalApps(seeded);
    assert.equal(oldAppsV4Migrator(seeded).migrated, false);
    assert.equal(seeded.prepare('SELECT required_binding_version FROM app_source_heads WHERE app_id=?').get(appId('a')).required_binding_version, 2);
  } finally { literal.close(); actual.close(); seeded.close(); }
});

test('Apps4 main plus committed Apps5 WAL preserves old state and makes the actual old migrator refuse without writes', async t => {
  const { root, filename } = await directory(t);
  const db = new DatabaseSync(filename);
  try {
    seedHistoricalApps(db);
    db.exec('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0');
    const mainBefore = await readFile(filename);
    assert.equal(mainBefore.readUInt32BE(60), 4);
    assert.deepEqual(await readStorageFormat(root), format(4));
    const oldObjects = objects(db);
    const preserved = db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name<>'apps_meta' AND name NOT GLOB 'sqlite_*' ORDER BY name").all().map(row => row.name);
    const oldRows = new Map(preserved.map(name => [name, db.prepare(`SELECT * FROM ${name} ORDER BY rowid`).all()]));
    const migrated = migrateAppsSchema(db);
    assert.equal(migrated.schema, 'soty.apps-registry.v5'); assert.equal(migrated.migrated, true);
    for (const [name, rows] of oldRows) assert.deepEqual(db.prepare(`SELECT * FROM ${name} ORDER BY rowid`).all(), rows, name);
    const nextObjects = new Map(objects(db).map(row => [row.name, row]));
    for (const object of oldObjects) assert.deepEqual(nextObjects.get(object.name), object, object.name);
    for (const table of ['app_saved_heads', 'app_saved_entries', 'app_saved_receipts']) {
      assert.equal(db.prepare(`SELECT count(*) AS n FROM ${table}`).get().n, 0, 'migration does not save old local pins');
    }
    assert.equal(db.prepare('SELECT active_target_revision FROM app_publications WHERE app_id=?').get(appId('a')).active_target_revision, 1);
    assert.equal(db.prepare('SELECT required_binding_version FROM app_source_heads WHERE app_id=?').get(appId('a')).required_binding_version, 2);
    assert.equal(db.prepare('SELECT launch_policy FROM app_publications WHERE app_id=?').get(appId('c')).launch_policy, 'restricted');
    assert.equal(db.prepare('SELECT state FROM local_apps WHERE id=?').get(appId('b')).state, 'revoked');
    // Exercise nonempty Apps5 storage, not only the new marker. This is a
    // synthetic storage fixture, not a proof of the signed saved API.
    db.exec('BEGIN');
    db.prepare('INSERT INTO app_saved_heads VALUES (?,2)').run('account-reader');
    db.prepare('INSERT INTO app_saved_entries VALUES (?,?,?,?,?,?,2,8)').run('account-reader', appId('a'), liveDomain,
      'https://live.apps.example', '/board?tag=a%2Bb#item', 'Saved personal title');
    db.prepare('INSERT INTO app_saved_receipts VALUES (?,?,?,?,1,2,8)').run('account-reader', '1'.repeat(64), '2'.repeat(64), appId('a'));
    db.exec('COMMIT');
    const walBefore = await readFile(filename + '-wal');
    assert.ok(walBefore.length > 32); assert.deepEqual(await readFile(filename), mainBefore);
    const observed = await readStorageFormat(root);
    assert.deepEqual(observed, format(5));
    assert.doesNotMatch(JSON.stringify(observed), /account-|Historical|Saved personal|apps\.example|9001|community-original/u);
    assert.throws(() => assertStorageCompatible(image(oldReaders), observed), /storage_reader_incompatible/u);
    assertStorageCompatible(image(currentStorageReaders), observed);
    const oldDb = new DatabaseSync(filename);
    try {
      assert.throws(() => oldAppsV4Migrator(oldDb), error => error.code === 'apps_schema_unsupported');
      assert.equal(oldDb.isTransaction, false);
      assert.equal(oldDb.prepare('PRAGMA user_version').get().user_version, 5);
    } finally { oldDb.close(); }
    assert.deepEqual(await readFile(filename), mainBefore, 'old migrator and host probe never checkpoint the writer');
    assert.deepEqual(await readFile(filename + '-wal'), walBefore, 'old migrator and host probe never append, truncate or rewrite the WAL');
    assert.equal(db.prepare('SELECT count(*) AS n FROM app_saved_entries').get().n, 1);
    assert.equal(db.prepare('SELECT count(*) AS n FROM app_saved_receipts').get().n, 1);
  } finally { db.close(); }
  assert.deepEqual(await readStorageFormat(root), format(5));
  const reopened = new DatabaseSync(filename);
  try {
    assert.equal(migrateAppsSchema(reopened).migrated, false);
    assert.equal(reopened.prepare('SELECT path FROM app_saved_entries').get().path, '/board?tag=a%2Bb#item');
    assert.equal(reopened.prepare('SELECT required_binding_version FROM app_source_heads WHERE app_id=?').get(appId('a')).required_binding_version, 2);
  } finally { reopened.close(); }
});

test('Apps5 refuses missing saved projections and a version5 marker over historical Apps4', async t => {
  const cases = [
    'DROP TABLE app_saved_heads', 'DROP TABLE app_saved_entries', 'DROP TABLE app_saved_receipts',
    'ALTER TABLE app_saved_heads RENAME COLUMN revision TO old_revision',
    'ALTER TABLE app_saved_entries RENAME COLUMN domain_id TO old_domain_id',
    'ALTER TABLE app_saved_entries RENAME COLUMN origin TO old_origin',
    'ALTER TABLE app_saved_entries RENAME COLUMN path TO old_path',
    'ALTER TABLE app_saved_entries RENAME COLUMN saved_revision TO old_saved_revision',
    'ALTER TABLE app_saved_receipts RENAME COLUMN intent_hash TO old_intent_hash',
    'ALTER TABLE app_saved_receipts RENAME COLUMN committed_revision TO old_committed_revision',
    'CREATE TABLE unexpected_saved_data(value TEXT)',
    'CREATE VIEW unexpected_saved_view AS SELECT 1',
  ];
  for (const sql of cases) {
    const { root, filename } = await directory(t), db = new DatabaseSync(filename);
    try { migrateAppsSchema(db); db.exec('PRAGMA foreign_keys=OFF'); db.exec(sql); } finally { db.close(); }
    await assert.rejects(readStorageFormat(root), /storage_format_unreadable/u, sql);
  }
  const { root, filename } = await directory(t), db = new DatabaseSync(filename);
  try { createHistoricalAppsV4(db); db.exec("UPDATE apps_meta SET value='soty.apps-registry.v5' WHERE key='schema'; PRAGMA user_version=5"); }
  finally { db.close(); }
  await assert.rejects(readStorageFormat(root), /storage_format_unreadable/u);
});

test('Apps5 recognizes each exact monotonic saved guard and retains the historical source guards', async t => {
  for (const name of ['app_saved_head_no_downgrade', 'app_saved_head_no_delete', 'app_saved_head_no_replace',
    'app_source_head_no_downgrade', 'app_runtime_target_no_replace']) {
    for (const variant of ['missing', 'changed-body', 'changed-table', 'changed-literal']) {
      const { root, filename } = await directory(t), db = new DatabaseSync(filename);
      try {
        migrateAppsSchema(db);
        const record = db.prepare('SELECT sql,tbl_name FROM sqlite_schema WHERE name=?').get(name);
        db.exec(`DROP TRIGGER ${name}`);
        if (variant === 'changed-body') db.exec(`CREATE TRIGGER ${name} BEFORE UPDATE ON ${record.tbl_name} BEGIN SELECT 1; END`);
        if (variant === 'changed-table') db.exec(record.sql.replace(` ON ${record.tbl_name} `, ' ON local_apps '));
        if (variant === 'changed-literal') db.exec(record.sql.replace(/RAISE\(ABORT,'([^']+)'\)/u, (_, message) => `RAISE(ABORT,'${message.toUpperCase()}')`));
      } finally { db.close(); }
      await assert.rejects(readStorageFormat(root), /storage_format_unreadable/u, `${name}:${variant}`);
    }
  }
  const { root, filename } = await directory(t), db = new DatabaseSync(filename);
  try {
    migrateAppsSchema(db);
    const record = db.prepare("SELECT sql FROM sqlite_schema WHERE name='app_saved_head_no_downgrade'").get();
    db.exec('DROP TRIGGER app_saved_head_no_downgrade');
    db.exec(record.sql.replace('NEW.revision<=OLD.revision', 'NEW.revision<OLD.revision'));
  } finally { db.close(); }
  await assert.rejects(readStorageFormat(root), /storage_format_unreadable/u, 'equal-revision updates must not become an accepted guard');
});

test('the host probe remains independent of candidate source and test-only historical migrators', async () => {
  const source = await readFile(new URL('./storage-probe.mjs', import.meta.url), 'utf8');
  const imports = [...source.matchAll(/\bfrom\s+['"]([^'"]+)['"]/gu)].map(match => match[1]);
  assert.deepEqual(imports, ['node:fs/promises', 'node:path', 'node:sqlite']);
  assert.doesNotMatch(source, /\b(?:import\s*\(|require\s*\(|eval\s*\()/u);
  assert.doesNotMatch(source, /fixtures\/apps|modules\/apps|migrateAppsSchema/u);
  assert.match(source, /new DatabaseSync\(filename, \{ readOnly: true \}\)/u);
});
