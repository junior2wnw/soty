import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, randomBytes } from 'node:crypto';
import { mkdtempSync, realpathSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { loadCapabilityConfiguration } from '../capabilities-configuration.js';
import { createOAuthHostProfile } from '../capabilities-oauth-profile.js';

const origin = 'https://soty.fixture.invalid';
const denied = error => error?.code === 'capability_configuration_invalid'
  && error.message === 'capability_configuration_invalid' && !error.cause;
function fixture(t) {
  const parent = realpathSync(tmpdir()), directory = mkdtempSync(path.join(parent, 'soty-oauth-config-'));
  t.after(() => {
    const real = realpathSync(directory); assert.equal(path.dirname(real), parent);
    assert.match(path.basename(real), /^soty-oauth-config-/u); rmSync(real, { recursive: true });
  });
  const file = path.join(directory, 'fixture.json');
  const env = { SOTY_CAPABILITY_AUDIENCE: origin, SOTY_NATIVE_NOTES_ENABLED: '1', SOTY_OAUTH_ENABLED: '1',
    SOTY_OAUTH_ISSUER: origin + '/oauth', SOTY_OAUTH_KEYS_FILE: file };
  return { directory, file, env };
}

test('process flags are explicit, independent and default off without reading a secret file', () => {
  assert.deepEqual(loadCapabilityConfiguration({}), { capabilityAudience: '', nativeNotesEnabled: false });
  const disabled = loadCapabilityConfiguration({ SOTY_CAPABILITY_AUDIENCE: origin, SOTY_OAUTH_ISSUER: origin + '/oauth' });
  assert.equal(disabled.nativeNotesEnabled, false); assert.equal(disabled.oauth.enabled, false);
  assert.equal(Object.hasOwn(disabled.oauth, 'artifactKey'), false);
  for (const env of [{ SOTY_NATIVE_NOTES_ENABLED: 'true' }, { SOTY_OAUTH_ENABLED: ' 1' },
    { SOTY_OAUTH_ENABLED: '1' }, { SOTY_NATIVE_NOTES_ENABLED: '1' },
    { SOTY_OAUTH_KEYS_FILE: '/never-read-without-issuer' }, { SOTY_OAUTH_ISSUER: [] }]) {
    assert.throws(() => loadCapabilityConfiguration(env), denied);
  }
});

test('bounded private file feeds the single canonical profile without logging or generating a key', t => {
  const f = fixture(t), key = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' });
  Object.assign(key, { kid: 'fixture', use: 'sig', alg: 'RS256' });
  const secret = { jwks: { keys: [key] }, cookieKeys: [randomBytes(32).toString('base64url')],
    artifactKey: randomBytes(32).toString('base64url'), artifactKeyId: 'fixture' };
  writeFileSync(f.file, JSON.stringify(secret), { mode: 0o600 });
  const config = loadCapabilityConfiguration(f.env);
  assert.equal(config.nativeNotesEnabled, true); assert.equal(config.oauth.enabled, true);
  assert.equal(Buffer.from(config.oauth.artifactKey).toString('base64url') === secret.artifactKey, true);
  const profile = createOAuthHostProfile(config.oauth, { shellOrigins: [origin], audience: origin });
  assert.equal(profile.issuer, origin + '/oauth');
  assert.equal(JSON.stringify(profile.protectedResource(origin)).includes(secret.artifactKey), false);
});

test('configuration refuses malformed or oversized files with one safe error and no path/secret dump', t => {
  const f = fixture(t);
  assert.throws(() => loadCapabilityConfiguration(f.env), denied);
  assert.throws(() => loadCapabilityConfiguration({ ...f.env, SOTY_OAUTH_KEYS_FILE: 'relative.json' }), denied);
  assert.throws(() => loadCapabilityConfiguration({ ...f.env, SOTY_OAUTH_KEYS_FILE: f.directory }), denied);
  for (const bytes of [Buffer.alloc(0), Buffer.alloc(32769, 65), Buffer.from([0xff, 0xfe]),
    Buffer.from('{"secret-canary":'), Buffer.from('[]'), Buffer.from('{}'),
    Buffer.from(JSON.stringify({ jwks: {}, cookieKeys: [], artifactKey: 'A'.repeat(42) + 'B', artifactKeyId: 'fixture' }))]) {
    writeFileSync(f.file, bytes); assert.throws(() => loadCapabilityConfiguration(f.env), denied);
  }
});
