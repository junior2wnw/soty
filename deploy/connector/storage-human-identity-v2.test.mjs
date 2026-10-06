import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import { readFileSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, resolve, basename } from 'node:path';
import { inspectHumanIdentityV2, humanIdentityV2StoragePin } from './storage-human-identity-v2-probe.mjs';
import { initializeHumanIdentitySchema } from '../../modules/human-identity/schema.mjs';
import { assertBaselineHumanV1 } from '../../modules/human-identity/test/support/baseline-human-v1-reader.mjs';

const sql = readFileSync(new URL('./fixtures/human-identity-v2/identity.sql', import.meta.url), 'utf8');
const v1 = readFileSync(new URL('./fixtures/human-identity-v1/identity.sql', import.meta.url), 'utf8');
const provenance = JSON.parse(readFileSync(new URL('./fixtures/human-identity-v2/provenance.json', import.meta.url), 'utf8'));
const issuer = 'https://soty.example/human-identity', profile = { enabled: true, issuer, registryId: 'REG.soty', environmentId: 'fixture' };
const sha = value => createHash('sha256').update(value).digest('hex');
const normalized = sql => sql.split(/('(?:[^']|'')*')/gu).map((part, i) => i % 2 ? part
  : part.replace(/\s+/gu, '').replace(/;$/u, '').toLowerCase()).join('');
const layout = db => db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all()
  .map(row => [row.type, row.name, row.tbl_name, normalized(row.sql)]);
function seed(db, version = 2) {
  db.function('human_identity_gc_epoch', () => 0); db.exec((version === 2 ? sql : v1) + `PRAGMA user_version=${version}`);
  const insert = db.prepare('INSERT INTO human_identity_meta VALUES(?,?)');
  for (const entry of Object.entries({ lineage: `soty.human-identity.sqlite.v${version}`, registry_id: profile.registryId,
    environment_id: profile.environmentId, issuer, profile: 'oidc-provider-9.12.2-human-v1' })) insert.run(...entry);
}
function fixture(t) {
  const root = mkdtempSync(join(tmpdir(), 'soty-human-v2-vector-')), path = join(root, 'identity.sqlite');
  t.after(() => { assert.equal(dirname(resolve(root)), resolve(tmpdir())); assert.match(basename(root), /^soty-human-v2-vector-/u);
    rmSync(root, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 }); }); return path;
}

test('literal schema2 pins all 33 objects independently; fresh/migrated candidate layouts match exactly', () => {
  const literal = new DatabaseSync(':memory:'), fresh = new DatabaseSync(':memory:'), migrated = new DatabaseSync(':memory:');
  try {
    seed(literal); assert.equal(sha(sql), provenance.ddlSha256); assert.equal(sha(JSON.stringify(layout(literal))), provenance.layoutSha256);
    assert.deepEqual({ ...humanIdentityV2StoragePin }, { version: 2, objects: 33, lineage: 'soty.human-identity.sqlite.v2',
      profile: 'oidc-provider-9.12.2-human-v1', layoutSha256: provenance.layoutSha256, ddlSha256: provenance.ddlSha256 });
    fresh.function('human_identity_gc_epoch', () => 0); assert.equal(initializeHumanIdentitySchema(fresh, profile, { allowRenewalMigration: true }).schemaVersion, 2);
    seed(migrated, 1); assert.equal(initializeHumanIdentitySchema(migrated, profile, { allowRenewalMigration: true }).schemaVersion, 2);
    assert.deepEqual(layout(fresh), layout(literal)); assert.deepEqual(layout(migrated), layout(literal));
    assert.equal(inspectHumanIdentityV2(literal).version, 2);
  } finally { literal.close(); fresh.close(); migrated.close(); }
});

test('schema2-compatible admission-off reader accepts literal schema2 without migration; frozen v1 refuses', t => {
  const path = fixture(t), seeded = new DatabaseSync(path); seed(seeded); seeded.close(); const before = sha(readFileSync(path));
  const reader = new DatabaseSync(path, { readOnly: true });
  try { assert.deepEqual(inspectHumanIdentityV2(reader), { version: 2, registryId: profile.registryId, environmentId: profile.environmentId }); }
  finally { reader.close(); }
  assert.equal(sha(readFileSync(path)), before); assert.throws(() => assertBaselineHumanV1(path), /human_identity_storage_unknown/u);
  const runtime = new DatabaseSync(path); runtime.function('human_identity_gc_epoch', () => 0);
  try { assert.equal(initializeHumanIdentitySchema(runtime, { ...profile, renewalAdmissionEnabled: false }).schemaVersion, 2); }
  finally { runtime.close(); }
  assert.equal(sha(readFileSync(path)), before);
});

test('v1 stays byte-for-byte unchanged by default; exact migration keeps opaque cipher bytes and refuses damaged v1 atomically', t => {
  const path = fixture(t), db = new DatabaseSync(path); seed(db, 1);
  const cipher = Buffer.alloc(80, 17), fields = ['Interaction', 'a'.repeat(64), cipher, 'b'.repeat(64), 'private-test-key',
    null, null, 'client', null, null, 'c'.repeat(64), 9000000000, null, 1];
  try {
    db.prepare('INSERT INTO human_identity_artifacts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)').run(...fields);
    assert.equal(initializeHumanIdentitySchema(db, profile).schemaVersion, 1); assertBaselineHumanV1(path);
    assert.equal(initializeHumanIdentitySchema(db, profile, { allowRenewalMigration: true }).schemaVersion, 2);
    const row = db.prepare('SELECT payload_cipher,expires_at,retain_until FROM human_identity_artifacts').get();
    assert.equal(Buffer.from(row.payload_cipher).equals(cipher), true); assert.equal(row.expires_at, row.retain_until);
    const before = sha(readFileSync(path)); assert.throws(() => initializeHumanIdentitySchema(db, { ...profile, registryId: 'other' }, { allowRenewalMigration: true }));
    assert.equal(sha(readFileSync(path)), before);
  } finally { db.close(); }
  for (const damaged of ['DROP INDEX human_identity_artifact_pending', 'DROP TRIGGER human_identity_grant_pin', 'CREATE TABLE extra(value TEXT)']) {
    const candidate = new DatabaseSync(':memory:');
    try { seed(candidate, 1); candidate.exec(damaged); const before = layout(candidate);
      assert.throws(() => initializeHumanIdentitySchema(candidate, profile, { allowRenewalMigration: true }));
      assert.equal(candidate.prepare('PRAGMA user_version').get().user_version, 1); assert.deepEqual(layout(candidate), before);
    } finally { candidate.close(); }
  }
});

test('future versions/altered schema2 objects/metadata refuse; probe never reads ciphertext or private artifact fields', () => {
  for (const change of ['PRAGMA user_version=3', 'DROP INDEX human_identity_artifact_retention', 'DROP INDEX human_identity_grant_family_expiry',
    'DROP TRIGGER human_identity_grant_pin', 'CREATE VIEW extra AS SELECT 1']) {
    const db = new DatabaseSync(':memory:'); try { seed(db); db.exec(change); assert.throws(() => inspectHumanIdentityV2(db), /storage_format_unknown/u); } finally { db.close(); }
  }
  const db = new DatabaseSync(':memory:');
  try {
    seed(db); const calls = [], wrapped = { prepare(statement) { calls.push(statement); return db.prepare(statement); } };
    inspectHumanIdentityV2(wrapped); assert.equal(calls.some(statement => /SELECT.*human_identity_artifacts/iu.test(statement)), false);
    const source = readFileSync(new URL('./storage-human-identity-v2-probe.mjs', import.meta.url), 'utf8');
    assert.doesNotMatch(source, /from\s+['"][^'"]*modules\//u);
    db.exec('DROP TRIGGER human_identity_meta_no_update'); db.prepare("UPDATE human_identity_meta SET value=? WHERE key='issuer'").run('https://bad.example/other');
    assert.throws(() => inspectHumanIdentityV2(db), /storage_format_unknown/u);
  } finally { db.close(); }
});

test('malformed volumes are bounded at SQL ingress before schema or metadata values reach the probe', () => {
  for (const mutation of [db => { for (let i = 0; i < 100; i++) db.exec(`CREATE TABLE extra_${i}(value TEXT)`); },
    db => db.prepare('INSERT INTO human_identity_meta VALUES(?,?)').run('extra', 'x'.repeat(1024 * 1024)),
    db => { db.exec('DROP TRIGGER human_identity_meta_no_update'); db.prepare("UPDATE human_identity_meta SET value=? WHERE key='issuer'").run('x'.repeat(1024 * 1024)); }]) {
    const db = new DatabaseSync(':memory:');
    try { seed(db); mutation(db); assert.throws(() => inspectHumanIdentityV2(db), error => error.message === 'storage_format_unknown'); }
    finally { db.close(); }
  }
  const db = new DatabaseSync(':memory:');
  try {
    seed(db); const calls = [], wrapped = { prepare(sql) { calls.push(sql); return db.prepare(sql); } };
    inspectHumanIdentityV2(wrapped);
    assert.equal(calls.some(sql => sql.includes('FROM sqlite_schema') && sql.includes('LIMIT 34') && sql.includes('length(CAST(sql AS BLOB))<=16384')), true);
    assert.equal(calls.some(sql => sql.includes('FROM human_identity_meta') && sql.includes('LIMIT 6') && sql.includes('length(CAST(value AS BLOB))<=512')), true);
  } finally { db.close(); }
});
