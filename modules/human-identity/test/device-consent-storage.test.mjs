import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { generateKeyPairSync, randomBytes } from 'node:crypto';
import { mkdtempSync, rmSync, readFileSync } from 'node:fs';
import { join, dirname, resolve, basename } from 'node:path';
import { tmpdir } from 'node:os';
import { createHumanIdentityHostProfile, digest } from '../profile.mjs';
import { createHumanIdentityService } from '../service.mjs';
import { environment } from './support/renewal-fixture.mjs';
import { assertHumanLayout } from './support/human-layout-reader.mjs';
import { HUMAN_LAYOUT_V3_SQL } from './support/human-layout-v3.mjs';

const random = () => randomBytes(32).toString('base64url');
const key = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' });
Object.assign(key, { kid: 'device-consent-storage', alg: 'RS256', use: 'sig' });
function fixture(t) {
  const root = mkdtempSync(join(tmpdir(), 'soty-device-consent-store-')), path = join(root, 'identity.sqlite'), services = [];
  t.after(() => { services.forEach(service => service.close()); assert.equal(dirname(resolve(root)), resolve(tmpdir()));
    assert.match(basename(root), /^soty-device-consent-store-/u); rmSync(root, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 }); });
  const origin = 'http://127.0.0.1:19595';
  const config = { enabled: true, issuer: origin + '/human-identity', registryId: 'REG.soty', environmentId: 'fixture',
    clients: [{ id: 'native-app', version: 2, label: 'Synthetic Native', redirectUri: 'http://127.0.0.1:19696/oidc/callback', clientSecret: random() }],
    cookieKeys: [random()], artifactKey: randomBytes(32), artifactKeyId: 'device-consent-storage', jwks: { keys: [key] },
    renewal: { admissionEnabled: true, clientIds: ['native-app'] } };
  const open = ({ options = config, ...changes } = {}) => {
    const profile = createHumanIdentityHostProfile(options, { shellOrigins: [origin] });
    const service = createHumanIdentityService({ databasePath: path, profile, actorActive: () => true, withAuthorityFence: callback => callback(), ...changes });
    services.push(service); return service;
  };
  return { root, path, config, open, origin };
}
const read = (path, callback) => { const db = new DatabaseSync(path, { readOnly: true }); try { return callback(db); } finally { db.close(); } };
const throws = (fn, code) => assert.throws(fn, error => error.code === code);

test('Human2 default has no consent table; explicit consent migration is required and refusal does not alter the store', t => {
  const f = fixture(t), baseline = f.open({ allowRenewalMigration: true }); baseline.close();
  assert.equal(assertHumanLayout(f.path, { maximumVersion: 2 }), 2);
  const off = { ...f.config, deviceConsent: { admissionEnabled: false, clientIds: ['native-app'] } };
  assert.equal(f.open({ options: off }).schemaVersion, 2);
  const enabled = { ...off, deviceConsent: { ...off.deviceConsent, admissionEnabled: true } }, before = digest(readFileSync(f.path));
  throws(() => f.open({ options: enabled }), 'human_identity_device_consent_migration_required');
  assert.equal(digest(readFileSync(f.path)), before);
  assert.equal(read(f.path, db => db.prepare("SELECT count(*) AS n FROM sqlite_schema WHERE name='human_identity_device_consents'").get().n), 0);
});

test('migration preserves every old table value, decision JSON, cipher and absolute deadline; old reader2 refuses before use', async t => {
  const f = await environment(t); await f.login(f.rps[0], f.actor, f.wire, undefined, 86400);
  const names = ['artifacts', 'interactions', 'decisions', 'grant_bindings', 'client_versions', 'client_heads'];
  const before = read(f.identityPath, db => Object.fromEntries(names.map(name => [name, db.prepare(`SELECT * FROM human_identity_${name} ORDER BY rowid`).all()])));
  assert.equal(assertHumanLayout(f.identityPath, { maximumVersion: 2 }), 2);
  await f.enableConsent();
  for (const name of names) {
    const oldColumns = Object.keys(before[name][0] || {});
    if (oldColumns.length) assert.deepEqual(read(f.identityPath, db => db.prepare(`SELECT ${oldColumns.join(',')} FROM human_identity_${name} ORDER BY rowid`).all()), before[name]);
  }
  assert.equal(assertHumanLayout(f.identityPath), 3);
  assert.throws(() => assertHumanLayout(f.identityPath, { maximumVersion: 2 }), /human_identity_storage_unknown/u);
  assert.equal((await f.wire.request(f.rps[0].origin + '/me')).status, 200);
  await f.consentAdmission(false); assert.equal(f.identity.schemaVersion, 3);
  assert.equal((await f.wire.request(f.rps[0].origin + '/me')).status, 200);
});

test('consent migration never silently upgrades Human1 or enables renewal when its explicit migration is absent', t => {
  const f = fixture(t), short = { ...f.config, clients: f.config.clients.map(client => ({ ...client, version: 1 })) }; delete short.renewal;
  f.open({ options: short }).close();
  const enabled = { ...f.config, deviceConsent: { admissionEnabled: true, clientIds: ['native-app'] } };
  throws(() => f.open({ options: enabled, allowDeviceConsentMigration: true }), 'human_identity_renewal_migration_required');
  assert.equal(read(f.path, db => db.prepare('PRAGMA user_version').get().user_version), 1);
  assert.equal(f.open({ options: enabled, allowDeviceConsentMigration: true, allowRenewalMigration: true }).schemaVersion, 3);
});

test('independent literal Human3 fixture is readable with both admissions off and cannot be disguised as Human2', t => {
  const f = fixture(t), db = new DatabaseSync(f.path); db.exec(HUMAN_LAYOUT_V3_SQL);
  const insert = db.prepare('INSERT INTO human_identity_meta VALUES(?,?)');
  for (const [name, value] of Object.entries({ lineage: 'soty.human-identity.sqlite.v3', registry_id: 'REG.soty', environment_id: 'fixture',
    issuer: f.config.issuer, profile: 'oidc-provider-9.12.2-human-v1' })) insert.run(name, value);
  db.exec('PRAGMA user_version=3'); db.close();
  const off = { ...f.config, renewal: { ...f.config.renewal, admissionEnabled: false }, deviceConsent: { admissionEnabled: false, clientIds: ['native-app'] } };
  assert.equal(f.open({ options: off }).schemaVersion, 3); assert.equal(assertHumanLayout(f.path), 3);
  const changed = new DatabaseSync(f.path); changed.exec('PRAGMA user_version=2'); changed.close();
  assert.throws(() => assertHumanLayout(f.path, { maximumVersion: 2 }), /human_identity_storage_unknown/u);
  throws(() => f.open({ options: off }), 'human_identity_storage_unknown');
});

test('Human3 decision consent references are a closed pair and its tuple/history cannot be rewritten or deleted', t => {
  const f = fixture(t); f.open({ allowRenewalMigration: true, allowDeviceConsentMigration: true }).close();
  const db = new DatabaseSync(f.path);
  try {
    const decision = db.prepare('INSERT INTO human_identity_decisions VALUES(?,?,?,?,?,?,?,?)');
    assert.throws(() => decision.run('account', 'r1', 'i', 'u', '{}', 1, 'a'.repeat(64), null));
    assert.throws(() => decision.run('account', 'r2', 'i', 'u', '{}', 1, null, 1));
    db.prepare('INSERT INTO human_identity_device_consents VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)')
      .run('a'.repeat(64), 'account', 'device', 'native-app', 'b'.repeat(64), 1, f.config.clients[0].redirectUri, '["openid"]', 1, 1, 1, null, 'u');
    assert.throws(() => db.exec("UPDATE human_identity_device_consents SET client_id='foreign',revision=2"));
    assert.throws(() => db.exec('UPDATE human_identity_device_consents SET revision=1'));
    assert.throws(() => db.exec('DELETE FROM human_identity_device_consents'));
  } finally { db.close(); }
});

test('host-only family bounds reject accessors, extra fields and invalid ceilings before opening a writer', t => {
  const f = fixture(t); let getterCalls = 0;
  const accessor = { familiesPerAccount: 8 }; Object.defineProperty(accessor, 'activeFamilies', { enumerable: true, get() { getterCalls++; return 16; } });
  for (const renewalLimits of [accessor, { activeFamilies: 0, familiesPerAccount: 0 }, { activeFamilies: 4097, familiesPerAccount: 8 },
    { activeFamilies: 8, familiesPerAccount: 9 }, { activeFamilies: '16', familiesPerAccount: 8 }, { activeFamilies: 16, familiesPerAccount: 8, fromPayload: true }]) {
    assert.throws(() => f.open({ renewalLimits }));
  }
  assert.equal(getterCalls, 0);
});
