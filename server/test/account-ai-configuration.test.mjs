import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync } from 'node:crypto';
import { mkdtemp, writeFile, rm, realpath } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { captureAccountAiConfiguration, loadAccountAiConfiguration, ROOT_AI_ISSUER, GATEWAY_AI_ISSUER, isAccountAiConfiguration } from '../account-ai-configuration.js';

function fixture() {
  const root = generateKeyPairSync('ed25519'), gateway = generateKeyPairSync('ed25519');
  return { schema: 'soty.account-ai-installed.v1', issuer: ROOT_AI_ISSUER, gatewayOrigin: 'http://inference-commerce-gateway-1:8080',
    signing: { kid: 'fixture-root', privateKey: root.privateKey.export({ format: 'pem', type: 'pkcs8' }) },
    gatewaySigner: { issuer: GATEWAY_AI_ISSUER, kid: 'fixture-gateway', publicKey: gateway.publicKey.export({ format: 'pem', type: 'spki' }) } };
}
const invalid = error => error.code === 'ai_configuration_invalid' && error.message === error.code && !error.cause;

test('installed account bridge is explicit, canonical and contains no serializable signing material', () => {
  assert.deepEqual(loadAccountAiConfiguration({}), {});
  const value = fixture(), configuration = captureAccountAiConfiguration(value);
  assert.equal(isAccountAiConfiguration(configuration), true); assert.equal(isAccountAiConfiguration({ ...configuration }), false);
  assert.equal(configuration.issuer, ROOT_AI_ISSUER); assert.equal(configuration.gatewayOrigin, value.gatewayOrigin);
  assert.equal(JSON.stringify(configuration).includes('PRIVATE KEY'), false); assert.equal(JSON.stringify(configuration).includes('PUBLIC KEY'), false);
  const headers = configuration.signer().headers({ method: 'GET', path: '/internal/soty/v1/account', body: '', operation: 'account.status', accountId: 'fixture-account', deviceId: 'fixture-device', requestId: 'fixture-request' });
  assert.match(headers['x-soty-bridge'], /^[\w-]+\.[\w-]+$/u);
  for (const change of [
    data => ({ ...data, issuer: 'https://host-claim.invalid' }), data => ({ ...data, schema: 'unknown' }),
    data => ({ ...data, gatewayOrigin: 'http://untrusted-service:8080' }), data => ({ ...data, gatewayOrigin: 'http://127.0.0.1:8080' }),
    data => ({ ...data, gatewayOrigin: data.gatewayOrigin + '/' }), data => ({ ...data, callbackUrl: 'http://caller.invalid' }),
    data => ({ ...data, signing: { ...data.signing, algorithm: 'none' } }),
    data => ({ ...data, gatewaySigner: { ...data.gatewaySigner, issuer: 'urn:foreign' } }),
    data => ({ ...data, signing: { ...data.signing, kid: 'x'.repeat(81) } }),
    data => ({ ...data, applications: [{ appId: 'fixture', accountId: 'foreign', models: ['glm'] }] }),
    data => ({ ...data, applications: [{ appId: 'fixture', accountId: 'acct_' + 'A'.repeat(32), models: [] }] }),
  ]) assert.throws(() => captureAccountAiConfiguration(change(value)), invalid);
});

test('sponsor bindings are immutable, model bounded and never account payload authority', () => {
  const value = fixture(), accountId = 'acct_' + 'A'.repeat(32);
  value.applications = [{ appId: 'fixture-app', accountId, models: ['zai-org/GLM-5.3-Flash'] }];
  const configuration = captureAccountAiConfiguration(value);
  value.applications[0].accountId = 'acct_' + 'B'.repeat(32); value.applications[0].models.push('other');
  assert.equal(configuration.applicationFor('fixture-app').accountId, accountId);
  assert.deepEqual(configuration.applicationFor('fixture-app').models, ['zai-org/GLM-5.3-Flash']);
  assert.equal(configuration.applicationFor('unknown'), null);
  assert.throws(() => { configuration.applicationFor('fixture-app').models.push('other'); });
  const ec = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  assert.throws(() => captureAccountAiConfiguration({ ...fixture(), signing: { kid: 'ec', privateKey: ec.privateKey.export({ format: 'pem', type: 'pkcs8' }) } }), invalid);
});

test('service accounts occupy a distinct namespace and exist only in explicit installed sponsor bindings', () => {
  const value = fixture(), sponsorId = 'svc_' + 'S'.repeat(32);
  value.applications = [{ appId: 'service-app', accountId: sponsorId, models: ['zai-org/GLM-5.3-Flash'] }];
  const configuration = captureAccountAiConfiguration(value);
  assert.deepEqual(configuration.sponsorAccountIds(), [sponsorId]); assert.equal(configuration.isSponsorAccount(sponsorId), true);
  assert.equal(configuration.isSponsorAccount('svc_' + 'F'.repeat(32)), false);
  value.applications[0].accountId = 'svc_' + 'F'.repeat(32);
  assert.equal(configuration.applicationFor('service-app').accountId, sponsorId);
  assert.throws(() => configuration.sponsorAccountIds().push('svc_' + 'F'.repeat(32)));
});

test('private file loader bounds bytes and shape without exposing invalid contents', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-account-ai-config-')), filename = join(directory, 'private.json');
  t.after(async () => { const target = await realpath(directory); assert.equal(dirname(target), await realpath(tmpdir())); assert.match(target, /soty-account-ai-config-/u); await rm(target, { recursive: true, force: true }); });
  await writeFile(filename, JSON.stringify(fixture()), { mode: 0o600 });
  assert.equal(loadAccountAiConfiguration({ SOTY_ACCOUNT_AI_CONFIG_FILE: filename }).accountAi.issuer, ROOT_AI_ISSUER);
  await writeFile(filename, '{"sentinel":"synthetic-invalid-private-sentinel"');
  assert.throws(() => loadAccountAiConfiguration({ SOTY_ACCOUNT_AI_CONFIG_FILE: filename }), invalid);
  await writeFile(filename, 'x'.repeat(65537)); assert.throws(() => loadAccountAiConfiguration({ SOTY_ACCOUNT_AI_CONFIG_FILE: filename }), invalid);
  assert.throws(() => loadAccountAiConfiguration({ SOTY_ACCOUNT_AI_CONFIG_FILE: 'relative.json' }), invalid);
});
