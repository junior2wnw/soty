import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { generateKeyPairSync, randomBytes, randomUUID } from 'node:crypto';
import { mkdtemp, mkdir, readFile, rm, writeFile, realpath } from 'node:fs/promises';
import { join, dirname, basename } from 'node:path';
import { tmpdir } from 'node:os';
import { HUMAN_LAYOUT_V3_SQL } from '../../modules/human-identity/test/support/human-layout-v3.mjs';
import { readStorageFormat } from './storage-probe.mjs';
import { inspectHumanIdentityV2 } from './storage-human-identity-v2-probe.mjs';
import { currentStorageReaders, storageReaderLabel, assertStorageCompatible } from './storage-guard.mjs';
import { loadUniversalConfiguration } from '../../server/universal-configuration.js';
import { createHttpApp } from '../../server/http-app.js';

const v2Sql = await readFile(new URL('./fixtures/human-identity-v2/identity.sql', import.meta.url), 'utf8');
const origin = 'https://human3.fixture.invalid', issuer = origin + '/human-identity';
const image = readers => ({ Id: 'sha256:' + '1'.repeat(64), Config: { Labels: { [storageReaderLabel]: readers } } });
async function fixture(t, version) {
  const parent = await realpath(tmpdir()), root = await realpath(await mkdtemp(join(parent, 'soty-human-v3-host-'))), owner = randomUUID();
  await writeFile(join(root, 'owner'), owner, { flag: 'wx' });
  t.after(async () => {
    assert.equal(await realpath(root), root); assert.equal(dirname(root), parent); assert.match(basename(root), /^soty-human-v3-host-/u);
    assert.equal(await readFile(join(root, 'owner'), 'utf8'), owner); await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  });
  const data = join(root, 'data'), dist = join(root, 'dist'); await mkdir(join(data, 'human-identity'), { recursive: true }); await mkdir(dist);
  await writeFile(join(dist, 'index.html'), '<!doctype html><title>Human3 fixture</title>');
  const file = join(data, 'human-identity', 'identity.sqlite'), db = new DatabaseSync(file);
  try {
    db.function('human_identity_gc_epoch', () => 0); db.exec(version === 3 ? HUMAN_LAYOUT_V3_SQL : v2Sql); db.exec(`PRAGMA user_version=${version}`);
    for (const pair of Object.entries({ lineage: `soty.human-identity.sqlite.v${version}`, registry_id: 'soty', environment_id: 'production',
      issuer, profile: 'oidc-provider-9.12.2-human-v1' })) db.prepare('INSERT INTO human_identity_meta VALUES(?,?)').run(...pair);
  } finally { db.close(); }
  return { root, data, dist, file };
}
function read(file, callback) { const db = new DatabaseSync(file, { readOnly: true }); try { return callback(db); } finally { db.close(); } }

test('actual host probe reads independent literal Human3 without writes; actual reader2 fallback is refused', async t => {
  const f = await fixture(t, 3), before = await readFile(f.file), format = await readStorageFormat(f.data);
  assert.equal(format.humanIdentity, 3); assert.deepEqual(await readFile(f.file), before);
  const old = JSON.parse(currentStorageReaders); old.readers.humanIdentity = [1, 2];
  assert.throws(() => assertStorageCompatible(image(JSON.stringify(old)), format), /storage_reader_incompatible/u);
  assert.equal(assertStorageCompatible(image(currentStorageReaders), format).humanIdentity, 3);
  assert.throws(() => read(f.file, inspectHumanIdentityV2), /storage_format_unknown/u);
  const db = new DatabaseSync(f.file); try { db.exec('DROP INDEX human_identity_device_consent_actor'); } finally { db.close(); }
  const damaged = await readFile(f.file); await assert.rejects(readStorageFormat(f.data), /storage_format_unreadable/u);
  assert.deepEqual(await readFile(f.file), damaged);
});

test('actual host entry preserves Human2 until explicit migration, then cold boots Human3 with old normal-login config and consent off', async t => {
  const f = await fixture(t, 2), random = () => randomBytes(32).toString('base64url');
  const key = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' });
  const configuration = { clients: [{ id: 'native-app', label: 'Synthetic Native', version: 2,
    redirectUri: 'https://native.fixture.invalid/callback', clientSecret: random() }],
    jwks: { keys: [{ ...key, kid: 'fixture-human3', alg: 'RS256', use: 'sig' }] }, cookieKeys: [random()], artifactKey: random(), artifactKeyId: 'fixture-human3',
    renewal: { admissionEnabled: true, clientIds: ['native-app'] }, renewalLimits: { activeFamilies: 256, familiesPerAccount: 64 } };
  const filename = join(f.root, 'synthetic-private.json'), env = { SOTY_HUMAN_IDENTITY_ENABLED: '1', SOTY_HUMAN_IDENTITY_ISSUER: issuer, SOTY_HUMAN_IDENTITY_KEYS_FILE: filename };
  const open = () => createHttpApp(f.dist, { dataDir: f.data, connectOrigins: [origin], appHosting: {}, appOriginTemplate: 'https://{appId}.apps.fixture.invalid',
    namedAppZone: '', discoveryOrigin: '', ...loadUniversalConfiguration(env) });
  const seed = new DatabaseSync(f.file);
  try { seed.prepare('INSERT INTO human_identity_artifacts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)').run('RefreshToken', 'a'.repeat(64), Buffer.alloc(80, 29),
    'b'.repeat(64), 'old-synthetic-key', 'old-account', 'old-device', 'native-app', null, null, 'c'.repeat(64), 9000000000, 1000, 1, 9000000000); }
  finally { seed.close(); }
  const preserved = read(f.file, db => db.prepare('SELECT * FROM human_identity_artifacts').get());
  await writeFile(filename, JSON.stringify(configuration), { mode: 0o600 });
  const normal = open(); try { assert.equal(normal.locals.humanIdentityService.schemaVersion, 2); } finally { normal.locals.closeServices(); }
  assert.equal((await readStorageFormat(f.data)).humanIdentity, 2);
  await writeFile(filename, JSON.stringify({ ...configuration, deviceConsent: { admissionEnabled: true, clientIds: ['native-app'] } }), { mode: 0o600 });
  const before = await readFile(f.file);
  assert.throws(open, error => error.code === 'human_identity_device_consent_migration_required');
  assert.deepEqual(await readFile(f.file), before);
  env.SOTY_HUMAN_DEVICE_CONSENT_MIGRATION = '1';
  const enabled = open(); try { assert.equal(enabled.locals.humanIdentityService.schemaVersion, 3); assert.equal(enabled.locals.humanIdentityStatus.enabled, true); }
  finally { enabled.locals.closeServices(); }
  assert.equal((await readStorageFormat(f.data)).humanIdentity, 3);
  assert.deepEqual(read(f.file, db => db.prepare('SELECT * FROM human_identity_artifacts').get()), preserved);
  await writeFile(filename, JSON.stringify(configuration), { mode: 0o600 }); delete env.SOTY_HUMAN_DEVICE_CONSENT_MIGRATION;
  const fallback = open(); try {
    assert.equal(fallback.locals.humanIdentityService.schemaVersion, 3); assert.equal(fallback.locals.humanIdentityStatus.enabled, true);
    assert.equal(fallback.locals.captureUniversalPreparedness().human.deviceConsent, undefined);
  } finally { fallback.locals.closeServices(); }
  assert.deepEqual(read(f.file, db => db.prepare('SELECT * FROM human_identity_artifacts').get()), preserved);
});
