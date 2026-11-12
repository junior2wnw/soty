import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { randomBytes } from 'node:crypto';
import { join, dirname, resolve, basename } from 'node:path';
import { tmpdir } from 'node:os';
import { loadUniversalConfiguration, validateHumanIdentityRenewalLimits, runtimeExecutionPolicyPath } from '../universal-configuration.js';
const issuer = 'https://soty.fixture.invalid/human-identity';
const fails = error => error.code === 'universal_configuration_invalid' && error.message === 'universal_configuration_invalid' && !error.cause;

test('frame continuity migration is explicit for existing storage and rejects misspelled permissions', () => {
  const name = 'SOTY_APPS_FRAME_CONTINUITY_MIGRATION';
  for (const value of [undefined, '', '0']) assert.deepEqual(loadUniversalConfiguration({ [name]: value }), {});
  assert.deepEqual(loadUniversalConfiguration({ [name]: '1' }), { allowFrameContinuityMigration: true });
  for (const value of ['true', 'yes', '2', true, 1, null]) assert.throws(() => loadUniversalConfiguration({ [name]: value }), fails);
  assert.deepEqual(loadUniversalConfiguration({ [name]: '1', SOTY_UNIVERSAL_APPS_ENABLED: 'false' }), {});
});
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

test('remembered device consent and capacity are separate trusted host options; migration remains explicit', t => {
  const filename = fixture(t), env = { SOTY_HUMAN_IDENTITY_ENABLED: '1', SOTY_HUMAN_IDENTITY_ISSUER: issuer, SOTY_HUMAN_IDENTITY_KEYS_FILE: filename };
  const configuration = { clients: [], jwks: { keys: [] }, cookieKeys: [], artifactKey: randomBytes(32).toString('base64url'), artifactKeyId: 'fixture',
    renewal: { admissionEnabled: true, clientIds: [] }, deviceConsent: { admissionEnabled: true, clientIds: [] },
    renewalLimits: { activeFamilies: 256, familiesPerAccount: 64 } };
  writeFileSync(filename, JSON.stringify(configuration), { mode: 0o600 });
  const loaded = loadUniversalConfiguration(env);
  assert.deepEqual(loaded.humanIdentity.deviceConsent, configuration.deviceConsent);
  assert.equal(Object.hasOwn(loaded.humanIdentity, 'renewalLimits'), false);
  assert.deepEqual(loaded.humanIdentityRenewalLimits, configuration.renewalLimits);
  assert.equal(Object.isFrozen(loaded.humanIdentityRenewalLimits), true);
  assert.equal(loaded.humanIdentityDeviceConsentMigration, undefined);
  assert.equal(loadUniversalConfiguration({ ...env, SOTY_HUMAN_DEVICE_CONSENT_MIGRATION: '1' }).humanIdentityDeviceConsentMigration, true);
  for (const input of [{ ...env, SOTY_HUMAN_DEVICE_CONSENT_MIGRATION: 'true' }, { SOTY_HUMAN_DEVICE_CONSENT_MIGRATION: '1' }])
    assert.throws(() => loadUniversalConfiguration(input), fails);
  for (const renewalLimits of [{ activeFamilies: 4097, familiesPerAccount: 1 }, { activeFamilies: 2, familiesPerAccount: 3 },
    { activeFamilies: 1, familiesPerAccount: 0 }, { activeFamilies: 1, familiesPerAccount: 1, clientId: 'untrusted' }]) {
    writeFileSync(filename, JSON.stringify({ ...configuration, renewalLimits })); assert.throws(() => loadUniversalConfiguration(env), fails);
  }
  let called = false;
  assert.throws(() => validateHumanIdentityRenewalLimits({ get activeFamilies() { called = true; return 256; }, familiesPerAccount: 64 }), fails);
  assert.equal(called, false);
});

const executionPolicy = () => ({
  schema: 'soty.app-execution-policy.v1',
  admissions: [{ appId: 'app-' + '1'.repeat(32), policyEpoch: 4,
    target: { revision: 1, digest: '2'.repeat(64), profile: 'soty.relay-restricted.v1' },
    capabilities: { wasm: true, workers: 'self', connectSrc: ['https://tiles.openfreemap.org'], imgSrc: [] } }]
});

test('optional execution policy defaults to the existing RO release directory and missing default preserves old options', t => {
  assert.equal(runtimeExecutionPolicyPath, '/run/connect-releases/app-execution-policy.json');
  const filename = fixture(t), options = { executionPolicyDefaultFile: filename };
  assert.deepEqual(loadUniversalConfiguration({}, options), {});
  assert.deepEqual(loadUniversalConfiguration({ SOTY_APP_EXECUTION_POLICY_FILE: '' }, options), {});
  // A configured path is required even while the conventional default is absent.
  assert.throws(() => loadUniversalConfiguration({ SOTY_APP_EXECUTION_POLICY_FILE: filename }, options), fails);
});

test('present execution policy is parsed, captured and frozen without network or unrelated environment reads', t => {
  const filename = fixture(t), policy = executionPolicy();
  writeFileSync(filename, JSON.stringify(policy), { mode: 0o600 });
  let secretRead = false, network = false;
  t.mock.method(globalThis, 'fetch', () => { network = true; throw new Error('must-not-request-policy-sources'); });
  const env = { SOTY_APP_EXECUTION_POLICY_FILE: filename };
  Object.defineProperty(env, 'UNRELATED_PRIVATE_SENTINEL', { enumerable: true, get() { secretRead = true; throw new Error('must-not-read-unrelated-env'); } });
  const loaded = loadUniversalConfiguration(env, { executionPolicyDefaultFile: filename + '.absent' });
  assert.deepEqual(loaded, { runtimeExecutionPolicy: policy });
  assert.equal(Object.isFrozen(loaded.runtimeExecutionPolicy), true);
  assert.equal(Object.isFrozen(loaded.runtimeExecutionPolicy.admissions), true);
  assert.equal(Object.isFrozen(loaded.runtimeExecutionPolicy.admissions[0].capabilities.connectSrc), true);
  writeFileSync(filename, JSON.stringify({ schema: policy.schema, admissions: [] }));
  assert.equal(loaded.runtimeExecutionPolicy.admissions.length, 1);
  assert.equal(secretRead, false); assert.equal(network, false);
});

test('present conventional execution policy is an opt-in and explicit override never falls back', t => {
  const defaultFile = fixture(t), explicitFile = fixture(t), policy = executionPolicy();
  const options = { executionPolicyDefaultFile: defaultFile };
  writeFileSync(defaultFile, JSON.stringify(policy));
  assert.deepEqual(loadUniversalConfiguration({}, options), { runtimeExecutionPolicy: policy });
  assert.throws(() => loadUniversalConfiguration({ SOTY_APP_EXECUTION_POLICY_FILE: explicitFile }, options), fails);
  writeFileSync(explicitFile, JSON.stringify({ schema: policy.schema, admissions: [] }));
  writeFileSync(defaultFile, '{"PRIVATE-SENTINEL":');
  assert.deepEqual(loadUniversalConfiguration({ SOTY_APP_EXECUTION_POLICY_FILE: explicitFile }, options),
    { runtimeExecutionPolicy: { schema: policy.schema, admissions: [] } });
  assert.throws(() => loadUniversalConfiguration({}, options), fails);
});

test('execution policy enforces regular files, exact 64KiB bound, UTF8, duplicate keys and the closed registry schema', t => {
  const filename = fixture(t), env = { SOTY_APP_EXECUTION_POLICY_FILE: filename }, options = { executionPolicyDefaultFile: filename };
  const policy = executionPolicy(), text = JSON.stringify(policy);
  writeFileSync(filename, text + ' '.repeat(65536 - Buffer.byteLength(text)));
  assert.deepEqual(loadUniversalConfiguration(env, options), { runtimeExecutionPolicy: policy });
  for (const value of [Buffer.alloc(0), Buffer.alloc(65537), Buffer.from([0xff]), Buffer.from('{"PRIVATE-SENTINEL":'),
    Buffer.from('[]'), Buffer.from('{}'), Buffer.from('{"schema":"soty.app-execution-policy.v1","admissions":[],"admissions":[]}'),
    Buffer.from(JSON.stringify({ ...policy, extra: true })), Buffer.from(JSON.stringify({ ...policy, admissions: [{ ...policy.admissions[0], capabilities: { ...policy.admissions[0].capabilities, wasm: 'true' } }] }))]) {
    writeFileSync(filename, value);
    assert.throws(() => loadUniversalConfiguration(env, options), fails);
    assert.throws(() => loadUniversalConfiguration({}, options), fails);
  }
  assert.throws(() => loadUniversalConfiguration({ SOTY_APP_EXECUTION_POLICY_FILE: dirname(filename) }, options), fails);
  assert.throws(() => loadUniversalConfiguration({ SOTY_APP_EXECUTION_POLICY_FILE: 'relative-policy.json' }, options), fails);
  for (const value of [null, true, 1, {}]) assert.throws(() => loadUniversalConfiguration({ SOTY_APP_EXECUTION_POLICY_FILE: value }, options), fails);
});

test('legacy and disabled universal modes do not open an execution policy', t => {
  const filename = fixture(t), options = { executionPolicyDefaultFile: filename };
  writeFileSync(filename, '{"PRIVATE-SENTINEL":');
  assert.deepEqual(loadUniversalConfiguration({ SOTY_APP_EXECUTION_POLICY_FILE: filename }, { ...options, legacyMode: true }), {});
  assert.deepEqual(loadUniversalConfiguration({ SOTY_UNIVERSAL_APPS_ENABLED: 'false', SOTY_APP_EXECUTION_POLICY_FILE: filename }, options), {});
});
