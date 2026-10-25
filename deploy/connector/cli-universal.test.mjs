import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomBytes, generateKeyPairSync, createHash } from 'node:crypto';
import { mkdtemp, writeFile, readFile, realpath, rm, lstat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { dockerHttpFixture } from './test-support/docker-http.mjs';
import { args } from './test-support/rollout-engine.mjs';
import { captureUniversalPreparedness, prepareUniversalPolicy, applyUniversalPolicy, disposeUniversalPolicy, selectedTarget, universalModeLabel } from './universal-policy.mjs';
import { createConfig, hash } from './rollout.mjs';
import { STANDARD_ROOT_WORKSPACE_SOURCE } from '../../modules/source-app/server/standard-profile.mjs';
import { createHumanIdentityHostProfile } from '../../modules/human-identity/profile.mjs';

const cli = fileURLToPath(new URL('./cli.mjs', import.meta.url)), random = () => randomBytes(32).toString('base64url');
async function fixture(t, { human = false, sameImage = false } = {}) {
  const root = await mkdtemp(path.join(await realpath(tmpdir()), 'soty-universal-policy-')), engine = await dockerHttpFixture(root);
  t.after(async () => { await engine.close(); assert.equal(path.dirname(path.resolve(root)), await realpath(tmpdir()));
    assert.match(path.basename(root), /^soty-universal-policy-/u); await rm(root, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 }); });
  const files = Object.fromEntries(['plan', 'custody', 'witness', 'journal', 'approval', 'human'].map(name => [name, path.join(root, name + '.json')]));
  const custody = { keyId: 'separate-operator-key', key: random() }, origin = 'https://soty.fixture.invalid';
  const plan = { schema: 'soty.universal-rollout-policy.v1', phase: human ? 'features' : 'legacy-baseline', human: human ? { issuer: origin + '/human-identity', source: files.human } : null, reviews: null };
  let configuration;
  if (human) {
    const jwk = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' }); Object.assign(jwk, { kid: 'fixture-cli', alg: 'RS256', use: 'sig' });
    configuration = { clients: [{ id: 'app.alpha', label: 'App alpha', redirectUri: 'https://alpha.fixture.invalid/callback', clientSecret: random() }],
      jwks: { keys: [jwk] }, cookieKeys: [random()], artifactKey: random(), artifactKeyId: 'fixture-cli' };
    await writeFile(files.human, JSON.stringify(configuration), { mode: 0o600 }); engine.setCandidateMode('0');
    const profile = createHumanIdentityHostProfile({ enabled: true, issuer: plan.human.issuer, registryId: 'soty', environmentId: 'production', ...configuration,
      artifactKey: Buffer.from(configuration.artifactKey, 'base64url') }, { shellOrigins: [origin] });
    engine.setMeasurement(captureUniversalPreparedness({ compiledLegacyMode: false, universalConfigured: true, reviewsConfigured: true, humanProfile: profile, humanHttpEnabled: true }));
  }
  await writeFile(files.plan, JSON.stringify(plan), { mode: 0o600 }); await writeFile(files.custody, JSON.stringify(custody), { mode: 0o600 });
  const flags = ['--journal', files.journal, '--original-id', args.originalId, '--original-image', args.originalImage, '--candidate-image', sameImage ? args.originalImage : args.candidateImage,
    '--storage-probe-image', args.storageProbeImage, '--revision', args.revision, '--transaction', args.transaction,
    '--health-origin', engine.healthOrigin, '--docker-socket', engine.socket, '--universal-plan', files.plan,
    '--universal-custody-file', files.custody, '--universal-witness-file', files.witness, '--universal-shell-origins', origin,
    '--fixture-root', root, '--fixture-mode', 'synthetic-local'];
  async function run(action, extra = []) {
    const child = spawn(process.execPath, [cli, action, ...flags, ...extra], { stdio: ['ignore', 'pipe', 'pipe'] }); let output = '', errors = '';
    child.stdout.on('data', chunk => { output += chunk; assert.equal(Buffer.byteLength(output) <= 65536, true); });
    child.stderr.on('data', chunk => { errors += chunk; assert.equal(Buffer.byteLength(errors) <= 65536, true); });
    const exit = await new Promise((resolve, reject) => { child.once('error', reject); child.once('exit', resolve); });
    assert.equal(errors.includes(custody.key), false); assert.equal(output.includes(custody.key), false);
    return { exit, body: JSON.parse(output.trim()), output };
  }
  async function approve(change = {}) { const prior = JSON.parse(await readFile(files.journal, 'utf8'));
    await writeFile(files.approval, JSON.stringify({ ...prior, approved: true, legacyNoPendingWritesObserved: true, ...change }), { mode: 0o600 }); return prior;
  }
  return { root, engine, files, custody, plan, configuration, run, approve };
}

test('real CLI prepares a distinct-source selected feature and seals both files without touching running old8', async t => {
  const f = await fixture(t, { human: true, sameImage: true }), source = path.join(f.root, 'old8.json'), nextSource = path.join(f.root, 'new9.json');
  const origin = 'https://soty.fixture.invalid';
  const profiles = Array.from({ length: 9 }, (_, i) => {
    const appId = 'app-' + String(i + 1).padStart(32, '0');
    return { schema: 'soty.selected-human-embed.v2', appId,
      connector: { linkId: 'fixture-link', hostDeviceId: 'fixture-device', connectorId: 'fixture-connector' },
      target: { revision: 2, digest: '5'.repeat(64) }, sourceProfile: STANDARD_ROOT_WORKSPACE_SOURCE,
      resource: { registryId: 'soty', tenantId: 'fixture-owner', environmentId: 'production', appId, resourceId: 'personal',
        selection: { kind: 'soty.workspace.v1', nativeId: 'fixture-' + i, incarnationId: 'v1' } },
      issuer: origin + '/human-identity', clientId: f.configuration.clients[0].id, parentOrigin: origin,
      embedOrigin: 'https://source.fixture.invalid', nativeOrigin: 'https://native.fixture.invalid' };
  });
  const oldBytes = JSON.stringify({ schema: 'soty.selected-embed-registry.v1', profiles: profiles.slice(0, 8) });
  const nextBytes = JSON.stringify({ schema: 'soty.selected-embed-registry.v1', profiles });
  await writeFile(source, oldBytes, { mode: 0o600 }); await writeFile(nextSource, nextBytes, { mode: 0o600 });
  const oldPlan = { ...f.plan, selected: { source, migrationConfigured: true } };
  const policy = await prepareUniversalPolicy(oldPlan, { shellOrigins: [origin], fixtureRoot: f.root });
  const old = f.engine.map.get(args.originalId);
  try {
    const { HostConfig, NetworkingConfig, ...Config } = applyUniversalPolicy(createConfig(old, args.originalImage, args.transaction, args.revision), policy);
    old.Config = Config; old.HostConfig = HostConfig;
    old.Mounts.push(...HostConfig.Mounts.filter(m => m.Type === 'bind').map(m => ({ Type: 'bind', Source: m.Source, Destination: m.Target, RW: !m.ReadOnly })));
  } finally { disposeUniversalPolicy(policy); }
  const human = createHumanIdentityHostProfile({ enabled: true, issuer: origin + '/human-identity', registryId: 'soty', environmentId: 'production',
    ...f.configuration, artifactKey: Buffer.from(f.configuration.artifactKey, 'base64url') }, { shellOrigins: [origin] });
  const current = captureUniversalPreparedness({ compiledLegacyMode: false, universalConfigured: true, reviewsConfigured: true,
    humanProfile: human, humanHttpEnabled: true, selectedProfiles: profiles.slice(0, 8), selectedRegistryConfigured: true,
    selectedMigrationConfigured: true, selectedResourceMigrationConfigured: true });
  const inspectImage = f.engine.engine.image;
  f.engine.engine.image = async id => { const image = await inspectImage(id); image.Config.Labels[universalModeLabel] = '0'; return image; };
  f.engine.setOriginalMeasurement(current);
  const plan = { ...oldPlan, expectedCurrentPreparednessSha256: hash(current), featureSelectedEmbed: { source: nextSource,
    sha256: createHash('sha256').update(nextBytes).digest('hex') } };
  await writeFile(f.files.plan, JSON.stringify(plan), { mode: 0o600 });
  const result = await f.run('prepare'); assert.equal(result.body.ok, true, result.body.code);
  assert.equal(result.body.phase, 'prepared'); assert.equal((await readFile(f.files.witness)).includes(Buffer.from(oldBytes)), false);
  assert.equal(old.State.Running, true); assert.equal(await readFile(source, 'utf8'), oldBytes);
  assert.equal(f.engine.map.get(result.body.candidateId).HostConfig.Mounts.find(m => m.Target === selectedTarget).Source, nextSource);
  await f.approve(); await writeFile(nextSource, nextBytes + '\n');
  const changed = await f.run('promote', ['--reviewed-receipt', f.files.approval]);
  assert.equal(changed.body.ok, false); assert.equal(f.engine.events.some(event => event.startsWith('stop:')), false);
});

test('real CLI processes prepare/seal and supervised promote with one exact encrypted private witness', async t => {
  const f = await fixture(t), prepared = await f.run('prepare'); assert.equal(prepared.exit, 0, prepared.body.code); assert.equal(prepared.body.phase, 'prepared');
  assert.equal(/^[A-Za-z0-9_-]{32}$/u.test(prepared.body.universalWitnessId), true); const bytes = await readFile(f.files.witness);
  assert.equal(bytes.includes(Buffer.from('preservationFingerprint')), false); assert.equal(bytes.includes(Buffer.from('synthetic-sensitive')), false);
  assert.equal((await lstat(f.files.witness)).nlink, 1); await f.approve();
  const promoted = await f.run('promote', ['--reviewed-receipt', f.files.approval]); assert.equal(promoted.exit, 0); assert.equal(promoted.body.phase, 'committed');
  assert.equal(f.engine.calls.filter(call => call.route.endsWith('/exec') && call.method === 'POST').length, 1);
  assert.equal(promoted.output.includes('synthetic-sensitive'), false); assert.equal(f.engine.map.get(args.originalId).State.Running, false);
});

test('wrong reviewed witness/public digest and inherited secret changes are denied before actual CLI STOP', async t => {
  for (const fault of ['witness', 'digest', 'both-secrets']) {
    const f = await fixture(t); assert.equal((await f.run('prepare')).body.ok, true);
    const prior = await f.approve(fault === 'witness' ? { universalWitnessId: randomBytes(24).toString('base64url') } : fault === 'digest' ? { universalPolicyDigest: '0'.repeat(64) } : {});
    if (fault === 'both-secrets') for (const id of [args.originalId, prior.candidateId]) f.engine.map.get(id).Config.Env[1] = 'TOKEN=another-private-value';
    const promoted = await f.run('promote', ['--reviewed-receipt', f.files.approval]); assert.equal(promoted.exit, 1); assert.equal(promoted.body.ok, false);
    assert.equal(f.engine.events.some(value => value.startsWith('stop:')), false); assert.equal(f.engine.map.get(args.originalId).State.Running, true);
  }
});

test('changed private Human config with identical public pin fails exact witness restore in another CLI process', async t => {
  const f = await fixture(t, { human: true }); assert.equal((await f.run('prepare')).body.ok, true); await f.approve();
  f.configuration.cookieKeys = [random()]; await writeFile(f.files.human, JSON.stringify(f.configuration), { mode: 0o600 });
  const promoted = await f.run('promote', ['--reviewed-receipt', f.files.approval]); assert.equal(promoted.body.code, 'universal_policy_witness_mismatch');
  assert.equal(f.engine.events.some(value => value.startsWith('stop:')), false);
});

test('witness exclusive publication cannot overwrite an earlier packet or create a candidate without a private custody key', async t => {
  const f = await fixture(t); await writeFile(f.files.witness, 'EXISTING_ENCRYPTED_PACKET', { mode: 0o600 });
  const duplicate = await f.run('prepare'); assert.equal(duplicate.body.ok, false);
  assert.equal(await readFile(f.files.witness, 'utf8'), 'EXISTING_ENCRYPTED_PACKET'); assert.equal(f.engine.events.some(value => value.startsWith('create:')), false);
  await writeFile(f.files.custody, JSON.stringify({ keyId: 'invalid-no-key' }), { mode: 0o600 }); const missing = await f.run('prepare');
  assert.equal(missing.body.code, 'universal_cli_custody_invalid'); assert.equal(f.engine.events.some(value => value.startsWith('create:')), false);
});
