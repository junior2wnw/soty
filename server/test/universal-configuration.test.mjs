import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { randomBytes } from 'node:crypto';
import { join, dirname, resolve, basename } from 'node:path';
import { tmpdir } from 'node:os';
import { loadUniversalConfiguration } from '../universal-configuration.js';
const issuer = 'https://soty.fixture.invalid/human-identity';
const fails = error => error.code === 'universal_configuration_invalid' && error.message === 'universal_configuration_invalid' && !error.cause;
function fixture(t) {
  const folder = mkdtempSync(join(tmpdir(), 'soty-universal-config-'));
  t.after(() => { assert.equal(dirname(resolve(folder)), resolve(tmpdir())); assert.match(basename(folder), /^soty-universal-config-/); rmSync(folder, { recursive: true, force: true }); });
  return join(folder, 'synthetic-private.json');
}
test('human issuance defaults off and compatible fallback never opens new private configuration', () => {
  assert.deepEqual(loadUniversalConfiguration({}), {});
  assert.deepEqual(loadUniversalConfiguration({ SOTY_HUMAN_IDENTITY_ISSUER: issuer, SOTY_HUMAN_IDENTITY_KEYS_FILE: 'not-read', SOTY_HUMAN_IDENTITY_ENABLED: '0' }),
    { humanIdentity: { enabled: false, issuer, registryId: 'soty', environmentId: 'production' } });
  const deniedRead = new Proxy({}, { get() { throw new Error('must-not-read-configuration'); } });
  assert.deepEqual(loadUniversalConfiguration(deniedRead, { legacyMode: true }), {});
  assert.deepEqual(loadUniversalConfiguration({ SOTY_UNIVERSAL_APPS_ENABLED: 'false', SOTY_HUMAN_IDENTITY_KEYS_FILE: 'not-read' }), {});
  for (const value of [{ SOTY_HUMAN_IDENTITY_ENABLED: 'true' }, { SOTY_HUMAN_IDENTITY_ENABLED: '1', SOTY_HUMAN_IDENTITY_ISSUER: issuer },
    { SOTY_HUMAN_IDENTITY_KEYS_FILE: 'unbound-private-file' }]) assert.throws(() => loadUniversalConfiguration(value), fails);
});
test('the human private file yields only trusted host options and never a guessed redirect or identity', t => {
  const filename = fixture(t), key = randomBytes(32).toString('base64url');
  writeFileSync(filename, JSON.stringify({ clients: [], jwks: { keys: [] }, cookieKeys: [], artifactKey: key, artifactKeyId: 'fixture' }), { mode: 0o600 });
  const value = loadUniversalConfiguration({ SOTY_HUMAN_IDENTITY_ENABLED: '1', SOTY_HUMAN_IDENTITY_ISSUER: issuer, SOTY_HUMAN_IDENTITY_KEYS_FILE: filename });
  assert.equal(value.humanIdentity.artifactKey.length, 32);
  assert.equal(value.humanIdentity.artifactKey.toString('base64url') === key, true);
  assert.equal(value.humanIdentity.registryId, 'soty'); assert.equal(value.humanIdentity.environmentId, 'production');
  // Complete protocol/key/client validation happens in the single host profile,
  // before any domain database is opened; the loader does not infer it.
});
test('malformed files and hidden configuration fields fail with one safe error', t => {
  const filename = fixture(t), env = { SOTY_HUMAN_IDENTITY_ENABLED: '1', SOTY_HUMAN_IDENTITY_ISSUER: issuer, SOTY_HUMAN_IDENTITY_KEYS_FILE: filename };
  for (const bytes of [Buffer.alloc(65537), Buffer.from([0xff]), Buffer.from('{"PRIVATE-SENTINEL":'), Buffer.from('[]'), Buffer.from('{}')]) {
    writeFileSync(filename, bytes); assert.throws(() => loadUniversalConfiguration(env), fails);
  }
  assert.throws(() => loadUniversalConfiguration({ ...env, SOTY_HUMAN_IDENTITY_KEYS_FILE: 'relative.json' }), fails);
  writeFileSync(filename, JSON.stringify({ providers: [], bindings: [] }));
  assert.deepEqual(loadUniversalConfiguration({ SOTY_REVIEWS_BINDINGS_FILE: filename }), { reviewsConfiguration: { providers: [], bindings: [] } });
});
