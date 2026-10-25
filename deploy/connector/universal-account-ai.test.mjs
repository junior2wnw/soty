import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync } from 'node:crypto';
import { mkdtemp, writeFile, rm, realpath, rename } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { prepareUniversalPolicy, publicUniversalPolicy, assertUniversalPolicyCurrent, disposeUniversalPolicy,
  applyUniversalPolicy, assertUniversalImagePrerequisites, sealUniversalPolicy, restoreUniversalPolicy,
  UNIVERSAL_POLICY_SCHEMA, universalModeLabel, accountAiPrivateTarget } from './universal-policy.mjs';
import { currentStorageReaders, storageReaderLabel } from './storage-guard.mjs';

const code = expected => error => error.code === expected && error.message === expected && !error.cause;
const base = () => ({ Env: ['DATA_DIR=/data'], HostConfig: { Mounts: [] } });
const image = label => ({ Id: 'sha256:' + 'a'.repeat(64), Config: { Labels: {
  [storageReaderLabel]: currentStorageReaders, [universalModeLabel]: '0', ...(label ? { 'io.soty.account-ai': label } : {}) } } });
function configuration() {
  const root = generateKeyPairSync('ed25519'), gateway = generateKeyPairSync('ed25519');
  return { schema: 'soty.account-ai-installed.v1', issuer: 'https://4-2.xn--p1ai',
    gatewayOrigin: 'http://inference-commerce-gateway-1:8080',
    signing: { kid: 'test-root', privateKey: root.privateKey.export({ type: 'pkcs8', format: 'pem' }) },
    gatewaySigner: { issuer: 'urn:soty:ai-gateway:production', kid: 'test-gateway', publicKey: gateway.publicKey.export({ type: 'spki', format: 'pem' }) } };
}
async function fixture(t) {
  const root = await mkdtemp(path.join(await realpath(tmpdir()), 'soty-universal-policy-account-ai-'));
  const source = path.join(root, 'account-ai.json'), handles = [], value = configuration();
  const options = { shellOrigins: ['https://4-2.xn--p1ai'], fixtureRoot: root };
  const empty = { schema: UNIVERSAL_POLICY_SCHEMA, phase: 'features', human: null, reviews: null };
  const plan = { ...empty, accountAi: { source }, expectedCurrentPreparednessSha256: 'b'.repeat(64) };
  const save = input => writeFile(source, JSON.stringify(input), { mode: 0o600 }); await save(value);
  t.after(async () => { for (const handle of handles) try { disposeUniversalPolicy(handle); } catch {}
    assert.equal(path.dirname(path.resolve(root)), await realpath(tmpdir())); assert.match(path.basename(root), /^soty-universal-policy-account-ai-/u);
    await rm(root, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 }); });
  return { root, source, value, options, empty, plan, save,
    async prepare(input = plan) { const handle = await prepareUniversalPolicy(input, options); handles.push(handle); return handle; }, handles };
}

test('account AI is an optional private feature, with no secrets in public receipt and no legacy enablement', async t => {
  const f = await fixture(t), disabled = applyUniversalPolicy(base(), await f.prepare(f.empty));
  assert.equal(disabled.Env.some(value => value.startsWith('SOTY_ACCOUNT_AI_CONFIG_FILE=')), false);
  const handle = await f.prepare(), receipt = publicUniversalPolicy(handle), result = applyUniversalPolicy(base(), handle);
  assert.equal(result.Env.includes('SOTY_ACCOUNT_AI_CONFIG_FILE=' + accountAiPrivateTarget), true);
  assert.deepEqual(result.HostConfig.Mounts, [{ Type: 'bind', Source: f.source, Target: accountAiPrivateTarget, ReadOnly: true }]);
  assert.equal(JSON.stringify(receipt).includes('PRIVATE KEY'), false);
  assert.equal(JSON.stringify(receipt).includes(f.value.signing.privateKey), false);
  await assert.rejects(f.prepare({ ...f.empty, phase: 'legacy-baseline', accountAi: { source: f.source } }), code('universal_policy_invalid'));
  await assert.rejects(f.prepare({ ...f.plan, accountAi: { source: f.source, accountId: 'forged' } }), code('universal_policy_invalid'));
  const bad = { ...f.value, gatewayOrigin: 'http://caller-selected.invalid' }; await f.save(bad);
  await assert.rejects(f.prepare(), code('universal_policy_account_ai_invalid'));
});

test('private account AI key rotation or inode replacement invalidates a prepared encrypted release witness', async t => {
  const f = await fixture(t), handle = await f.prepare(), key = Buffer.alloc(32, 19), keyId = 'test-custody';
  const sealed = await sealUniversalPolicy(handle, { key, keyId });
  const restored = await restoreUniversalPolicy(f.plan, sealed.packet, f.options, { key, keyId, expectedWitnessId: sealed.witnessId });
  f.handles.push(restored); assert.equal(publicUniversalPolicy(restored).policyDigest, sealed.policyDigest);
  await f.save(configuration());
  await assert.rejects(assertUniversalPolicyCurrent(handle), code('universal_policy_file_changed'));
  await assert.rejects(restoreUniversalPolicy(f.plan, sealed.packet, f.options, { key, keyId, expectedWitnessId: sealed.witnessId }), code('universal_policy_witness_mismatch'));
  const fresh = await f.prepare(), next = await sealUniversalPolicy(fresh, { key, keyId });
  const replacement = path.join(f.root, 'replacement.json'); await writeFile(replacement, JSON.stringify(configuration()), { mode: 0o600 });
  await rename(f.source, path.join(f.root, 'old.json')); await rename(replacement, f.source);
  await assert.rejects(restoreUniversalPolicy(f.plan, next.packet, f.options, { key, keyId, expectedWitnessId: next.witnessId }), code('universal_policy_witness_mismatch'));
  key.fill(0); sealed.packet.fill(0); next.packet.fill(0);
});

test('account AI requires its qualified image capability and rejects shadowed file or inherited authority', async t => {
  const f = await fixture(t), handle = await f.prepare();
  assert.throws(() => assertUniversalImagePrerequisites(handle, { candidateImage: image(), originalImage: image() }), code('universal_policy_account_ai_image_required'));
  assert.doesNotThrow(() => assertUniversalImagePrerequisites(handle, { candidateImage: image('1'), originalImage: image() }));
  assert.throws(() => applyUniversalPolicy({ ...base(), Env: ['SOTY_ACCOUNT_AI_CONFIG_FILE=/unapproved'] }, handle), code('universal_policy_preexisting_configuration'));
  assert.throws(() => applyUniversalPolicy({ ...base(), HostConfig: { Mounts: [{ Type: 'bind', Source: f.root, Target: '/run/secrets', ReadOnly: true }] } }, handle), code('universal_policy_preexisting_configuration'));
});
