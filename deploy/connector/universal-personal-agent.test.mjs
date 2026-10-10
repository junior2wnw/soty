import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, rm, realpath, rename, link, chmod } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { prepareUniversalPolicy, publicUniversalPolicy, assertUniversalPolicyCurrent, disposeUniversalPolicy,
  applyUniversalPolicy, sealUniversalPolicy, restoreUniversalPolicy, UNIVERSAL_POLICY_SCHEMA, personalAgentPrivateTarget } from './universal-policy.mjs';
import { loadPersonalAgentConfiguration } from '../../server/personal-agent-configuration.js';

const closed = code => error => error.code === code && error.message === code && !error.cause;
const profile = () => ({ schema: 'soty.personal-agent-installed.v1', gateway: {
  origin: 'https://fixture-commerce.invalid', billingTenant: 'fixture_private_tenant',
  inferenceBearer: 'synthetic-only-private-inference-key', managementSession: 'synthetic_only_private_session',
}, accountIds: ['fixture_owner_A', 'fixture_owner_B'] });
const base = () => ({ Env: ['DATA_DIR=/data'], HostConfig: { Mounts: [] } });
async function fixture(t) {
  const root = await mkdtemp(path.join(await realpath(tmpdir()), 'soty-universal-policy-'));
  const source = path.join(root, 'private-agent.json'), options = { shellOrigins: ['https://shell.fixture.invalid'], fixtureRoot: root }, handles = [];
  const empty = { schema: UNIVERSAL_POLICY_SCHEMA, phase: 'features', human: null, reviews: null };
  const plan = { ...empty, personalAgent: { source } };
  const save = value => writeFile(source, JSON.stringify(value), { mode: 0o600 }); await save(profile());
  t.after(async () => {
    for (const handle of handles) { try { disposeUniversalPolicy(handle); } catch (error) { assert.equal(error.code, 'universal_policy_handle_invalid'); } }
    assert.equal(path.dirname(path.resolve(root)), await realpath(tmpdir())); assert.match(path.basename(root), /^soty-universal-policy-/u);
    await rm(root, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 });
  });
  return { root, source, options, empty, plan, save, handles,
    async prepare(input = plan) { const handle = await prepareUniversalPolicy(input, options); handles.push(handle); return handle; } };
}

test('personal agent defaults OFF and cannot be enabled in the legacy-baseline phase', async t => {
  const f = await fixture(t);
  for (const plan of [f.empty, { ...f.empty, personalAgent: null }]) {
    const config = applyUniversalPolicy(base(), await f.prepare(plan));
    assert.equal(config.Env.includes('SOTY_PERSONAL_AGENT_ENABLED=0'), true);
    assert.equal(config.Env.some(value => value.startsWith('SOTY_PERSONAL_AGENT_CONFIG_FILE=')), false);
    assert.deepEqual(config.HostConfig.Mounts, []);
    assert.deepEqual(loadPersonalAgentConfiguration(Object.fromEntries(config.Env.map(value => value.split('=')))), { personalAgent: { enabled: false } });
  }
  await assert.rejects(f.prepare({ ...f.plan, phase: 'legacy-baseline' }), closed('universal_policy_invalid'));
  const baseline = applyUniversalPolicy(base(), await f.prepare({ ...f.empty, phase: 'legacy-baseline' }));
  assert.deepEqual(baseline.Env, ['DATA_DIR=/data', 'SOTY_UNIVERSAL_OPERATOR_ENABLED=1']);
});

test('explicit private agent file creates only fixed env and read-only mount while the runtime retains exact account allowlist', async t => {
  const f = await fixture(t), handle = await f.prepare(), receipt = publicUniversalPolicy(handle), before = base();
  const config = applyUniversalPolicy(before, handle);
  assert.deepEqual(before, base()); assert.deepEqual(applyUniversalPolicy(config, handle), config);
  assert.equal(config.Env.includes('SOTY_PERSONAL_AGENT_ENABLED=1'), true);
  assert.equal(config.Env.includes('SOTY_PERSONAL_AGENT_CONFIG_FILE=' + personalAgentPrivateTarget), true);
  assert.deepEqual(config.HostConfig.Mounts, [{ Type: 'bind', Source: f.source, Target: personalAgentPrivateTarget, ReadOnly: true }]);
  assert.deepEqual(receipt.mounts, [{ source: f.source, target: personalAgentPrivateTarget, readOnly: true, visibility: 'private' }]);
  for (const secret of [...Object.values(profile().gateway), ...profile().accountIds]) assert.equal(JSON.stringify(receipt).includes(secret), false);
  let requests = 0;
  const loaded = loadPersonalAgentConfiguration({ SOTY_PERSONAL_AGENT_ENABLED: '1', SOTY_PERSONAL_AGENT_CONFIG_FILE: f.source },
    { fetchImpl: async () => { requests++; throw new Error('unexpected_network'); } }).personalAgent;
  assert.equal(loaded.enabled, true); assert.equal(loaded.historyEnabled, true);
  for (const accountId of [...profile().accountIds, 'foreign_account']) assert.equal(loaded.gateway.authorizeActor({ actor: { accountId },
    billingTenant: profile().gateway.billingTenant, keyRef: loaded.gateway.keyRef }), profile().accountIds.includes(accountId));
  assert.equal(requests, 0); assert.strictEqual(await assertUniversalPolicyCurrent(handle), receipt);
});

test('closed agent plan and installed configuration reject unsupported authority and invalid allowlists with safe errors', async t => {
  const f = await fixture(t);
  for (const personalAgent of [true, { source: f.source, enabled: true }, { source: f.source, accountIds: ['forged'] }])
    await assert.rejects(f.prepare({ ...f.empty, personalAgent }), closed('universal_policy_invalid'));
  await assert.rejects(f.prepare({ ...f.empty, personalAgent: { source: 'relative.json' } }), closed('universal_policy_path_invalid'));
  for (const change of [value => ({ ...value, accountIds: [] }), value => ({ ...value, accountIds: ['duplicate', 'duplicate'] }),
    value => ({ ...value, accountIds: ['*'] }), value => ({ ...value, gateway: { ...value.gateway, model: 'unsupported' } }),
    value => ({ ...value, gateway: { ...value.gateway, origin: 'http://fixture.invalid' } }),
    value => ({ ...value, localTargets: [{ accountId: 'foreign_account', hostDeviceId: 'fixture_device', connectorId: 'fixture_connector', cwd: '/fixture' }] })]) {
    await f.save(change(profile())); await assert.rejects(f.prepare(), closed('universal_policy_personal_agent_invalid'));
  }
});

test('agent private bytes and inode stay bound through the existing encrypted prepare/promote witness', async t => {
  const f = await fixture(t), handle = await f.prepare(), key = Buffer.alloc(32, 7), keyId = 'fixture-custody';
  const sealed = await sealUniversalPolicy(handle, { key, keyId }), approval = { key, keyId, expectedWitnessId: sealed.witnessId };
  const restored = await restoreUniversalPolicy(f.plan, sealed.packet, f.options, approval); f.handles.push(restored);
  assert.equal((await assertUniversalPolicyCurrent(restored)).policyDigest, sealed.policyDigest);
  const changed = profile(); changed.gateway.managementSession = 'synthetic_only_rotated_session'; await f.save(changed);
  assert.equal(publicUniversalPolicy(await f.prepare()).policyDigest, sealed.policyDigest);
  await assert.rejects(assertUniversalPolicyCurrent(handle), closed('universal_policy_file_changed'));
  await assert.rejects(restoreUniversalPolicy(f.plan, sealed.packet, f.options, approval), closed('universal_policy_witness_mismatch'));
  const current = await f.prepare(), next = await sealUniversalPolicy(current, { key, keyId }), replacement = path.join(f.root, 'replacement.json');
  await writeFile(replacement, JSON.stringify(changed), { mode: 0o600 }); await rename(f.source, path.join(f.root, 'old.json')); await rename(replacement, f.source);
  await assert.rejects(restoreUniversalPolicy(f.plan, next.packet, f.options, { key, keyId, expectedWitnessId: next.witnessId }), closed('universal_policy_witness_mismatch'));
  key.fill(0); sealed.packet.fill(0); next.packet.fill(0);
});

test('agent environment and mount cannot be inherited ambiguously or shadow the approved private file', async t => {
  const f = await fixture(t), handle = await f.prepare();
  for (const extra of [['SOTY_PERSONAL_AGENT_ENABLED=0'], ['SOTY_PERSONAL_AGENT_ENABLED'],
    ['SOTY_PERSONAL_AGENT_ENABLED=1', 'SOTY_PERSONAL_AGENT_ENABLED=1'], ['SOTY_PERSONAL_AGENT_CONFIG_FILE=/unapproved']])
    assert.throws(() => applyUniversalPolicy({ ...base(), Env: [...base().Env, ...extra] }, handle), closed('universal_policy_preexisting_configuration'));
  for (const mount of [{ Type: 'bind', Source: f.source, Target: personalAgentPrivateTarget, ReadOnly: false },
    { Type: 'bind', Source: f.root, Target: '/run/secrets', ReadOnly: true }])
    assert.throws(() => applyUniversalPolicy({ ...base(), HostConfig: { Mounts: [mount] } }, handle), closed('universal_policy_preexisting_configuration'));
  const disabled = await f.prepare(f.empty);
  assert.throws(() => applyUniversalPolicy({ ...base(), Env: ['SOTY_PERSONAL_AGENT_ENABLED=1'] }, disabled), closed('universal_policy_preexisting_configuration'));
  assert.throws(() => applyUniversalPolicy({ ...base(), Env: ['SOTY_PERSONAL_AGENT_CONFIG_FILE=' + f.source] }, disabled), closed('universal_policy_preexisting_configuration'));
});

test('agent private file uses the shared hardlink and private-permission checks', async t => {
  const f = await fixture(t), alias = path.join(f.root, 'hardlink.json'); await link(f.source, alias);
  await assert.rejects(f.prepare(), closed('universal_policy_file_invalid')); await rm(alias);
  if (process.platform === 'linux') {
    await chmod(f.source, 0o644); await assert.rejects(f.prepare(), closed('universal_policy_file_permissions'));
  }
});
