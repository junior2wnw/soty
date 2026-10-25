import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync } from 'node:crypto';
import { validateSotyBridgeConfig } from '../../apps/gateway/src/soty-bridge.mjs';
import { sotyAllowance, publicSotyModels } from '../../apps/gateway/src/soty-policy.mjs';

test('permanent included allowance is finite and private service trust is pinned', () => {
  const root = generateKeyPairSync('ed25519'); const gateway = generateKeyPairSync('ed25519');
  const config = { schema: 'soty.ai-gateway-bridge.v1', audience: 'urn:soty:ai-gateway:v1', issuers: [{ issuer: 'https://root.soty.invalid', kid: 'root-key', publicKey: root.publicKey }],
    signing: { issuer: 'urn:soty:ai-gateway:production', kid: 'gateway-key', privateKey: gateway.privateKey }, root: { origin: 'http://soty-online-chat:8080', audience: 'urn:soty:root-ai-dispatch:v1', allowPrivateHttp: true } };
  assert.equal(validateSotyBridgeConfig(config).allowance.grantedMicrorubles, '50000000');
  assert.equal(sotyAllowance().limits.dailyMicrorubles, '5000000');
  for (const origin of ['http://public.example', 'http://soty-online-chat:8080/redirect', 'https://user:pass@root.invalid', 'https://root.invalid/?callback=other']) assert.throws(() => validateSotyBridgeConfig({ ...config, root: { ...config.root, origin } }));
  assert.throws(() => validateSotyBridgeConfig({ ...config, root: { ...config.root, allowPrivateHttp: false } }));
  assert.throws(() => validateSotyBridgeConfig({ ...config, audience: 'urn:untrusted' }));
  for (const allowance of [{ limits: { dailyMicrorubles: null } }, { grantedMicrorubles: '0' }, { grantedMicrorubles: '1' }, { limits: { concurrency: null } }, { limits: { totalMicrorubles: '9000000000000000001' } }, { unlimited: true }]) assert.throws(() => sotyAllowance(allowance));
});

test('account model projection is bounded, retains supported models and excludes provider diagnostics', () => {
  const model = id => ({ id, name: 'Я'.repeat(160), status: 'healthy', contextWindow: 100000, inputPriceMicrorublesPerMillion: '1', outputPriceMicrorublesPerMillion: '2', capabilities: { text: true, tools: 'tested', untrusted: true }, routes: [{ providerId: 'private-provider', transport: 'must-not-leak' }], raw: 'x'.repeat(10000) });
  const catalog = Array.from({ length: 1200 }, (_, index) => model(`${'Я'.repeat(240)}${String(index).padStart(5, '0')}`));
  catalog.push(model('zai-org/GLM-5.3-Flash'), model('deepseek-ai/DeepSeek-V4-Flash-0731'), model('MiniMaxAI/MiniMax-M2.7'));
  const projected = publicSotyModels(catalog); const serialized = JSON.stringify(projected);
  assert.ok(projected.length <= 300); assert.ok(Buffer.byteLength(serialized) <= 384 * 1024);
  assert.deepEqual(projected.slice(0, 3).map(entry => entry.id), ['deepseek-ai/DeepSeek-V4-Flash-0731', 'MiniMaxAI/MiniMax-M2.7', 'zai-org/GLM-5.3-Flash']);
  assert.equal(serialized.includes('must-not-leak'), false); assert.equal(serialized.includes('private-provider'), false); assert.equal(serialized.includes('untrusted'), false);
  assert.equal(catalog.length, 1203);
});
