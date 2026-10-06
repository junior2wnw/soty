import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, readFile, rm, realpath } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { randomBytes, generateKeyPairSync } from 'node:crypto';
import { fixture as engineFixture, args } from './test-support/rollout-engine.mjs';
import { Rollout, createConfig, preservationHash } from './rollout.mjs';
import { prepareUniversalPolicy, publicUniversalPolicy, sealUniversalPolicy, restoreUniversalPolicy, disposeUniversalPolicy,
  captureUniversalPreparedness, universalModeLabel, bindUniversalRollout } from './universal-policy.mjs';
import { createHumanIdentityHostProfile } from '../../modules/human-identity/profile.mjs';
import { SafeError } from './docker-api.mjs';
import { storageReaderLabel, currentStorageReaders } from './storage-guard.mjs';

const origin = 'https://soty.fixture.invalid', random = () => randomBytes(32).toString('base64url');
const privateJwk = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' });
Object.assign(privateJwk, { kid: 'rollout-fixture', alg: 'RS256', use: 'sig' });
async function fixture(t, { phase = 'features', fault = {}, human = false } = {}) {
  const root = await mkdtemp(path.join(await realpath(tmpdir()), 'soty-universal-policy-')), handles = [], f = engineFixture(fault);
  t.after(async () => { for (const handle of handles) try { disposeUniversalPolicy(handle); } catch {}
    assert.equal(path.dirname(path.resolve(root)), await realpath(tmpdir())); assert.match(path.basename(root), /^soty-universal-policy-/u);
    await rm(root, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 }); });
  const humanSource = path.join(root, 'human.json'), key = randomBytes(32), options = { shellOrigins: [origin], fixtureRoot: root };
  const configuration = { clients: [{ id: 'app.alpha', label: 'App alpha', redirectUri: 'https://alpha.fixture.invalid/callback', clientSecret: random(), version: 2 }],
    jwks: { keys: [privateJwk] }, cookieKeys: [random()], artifactKey: random(), artifactKeyId: 'rollout-fixture' };
  if (human) await writeFile(humanSource, JSON.stringify(configuration), { mode: 0o600 });
  const plan = { schema: 'soty.universal-rollout-policy.v1', phase, human: human ? { issuer: origin + '/human-identity', source: humanSource } : null, reviews: null };
  const handle = await prepareUniversalPolicy(plan, options); handles.push(handle);
  const image = f.engine.image;
  f.engine.image = async id => { const value = await image(id); value.Config.Labels[universalModeLabel] = id === args.candidateImage ? phase === 'legacy-baseline' ? '1' : '0' : '1'; return value; };
  const humanProfile = human ? createHumanIdentityHostProfile({ enabled: true, issuer: plan.human.issuer, registryId: 'soty', environmentId: 'production', ...configuration,
    artifactKey: Buffer.from(configuration.artifactKey, 'base64url') }, { shellOrigins: [origin] }) : null;
  f.run.allowUniversalFixture = true;
  f.run.universalMeasurement = async id => { if(id===args.originalId)return captureUniversalPreparedness({compiledLegacyMode:true,universalConfigured:false,reviewsConfigured:false,humanHttpEnabled:false,humanProfile:null});
    f.events.push('measure:' + id); return captureUniversalPreparedness({ compiledLegacyMode: phase === 'legacy-baseline',
    universalConfigured: phase !== 'legacy-baseline', reviewsConfigured: phase !== 'legacy-baseline', humanProfile, humanHttpEnabled: Boolean(humanProfile) }); };
  const request = { ...args, universalPolicy: handle, universalWitnessId: randomBytes(24).toString('base64url') };
  return { ...f, get migrated() { return f.migrated; }, root, handle, request, options, plan, configuration, humanSource, key, handles };
}

test('both phases require only the approved delta and actual private measurement before maintenance leave', async t => {
  for (const phase of ['legacy-baseline', 'features']) {
    const f = await fixture(t, { phase }); await f.run.prepare(f.request); await f.run.promote();
    assert.equal(f.run.state.phase, 'committed'); assert.equal(f.run.config.Env.includes('SOTY_UNIVERSAL_OPERATOR_ENABLED=1'), true);
    assert.equal(f.events.indexOf('measure:' + f.run.candidate.Id) < f.events.indexOf('helper:leave'), true);
    assert.equal(f.map.get(args.originalId).Config.Env.includes('SOTY_UNIVERSAL_OPERATOR_ENABLED=1'), false);
    assert.equal(f.map.get('sentinel').State.Running, true);
    assert.equal(f.run.state.configurationSha256 === f.run.fingerprint, false);
    assert.equal(JSON.stringify(f.records).includes(f.run.fingerprint), false);
    if (phase === 'legacy-baseline') assert.deepEqual(f.run.config.Env, [...f.run.original.Config.Env, 'SOTY_UNIVERSAL_OPERATOR_ENABLED=1']);
  }
});

test('opaque policy, wrong image/mode and production fixture rejection fail before CREATE/STOP', async t => {
  for (const fault of ['forged', 'image', 'fixture', 'measurement', 'baselineDto']) {
    const f = await fixture(t);
    if (fault === 'forged') f.request.universalPolicy = JSON.parse(JSON.stringify(publicUniversalPolicy(f.handle)));
    if (fault === 'image') { const image = f.engine.image; f.engine.image = async id => { const value = await image(id); value.Config.Labels[universalModeLabel] = '1'; return value; }; }
    if (fault === 'fixture') f.run.allowUniversalFixture = false;
    if (fault === 'measurement') f.run.universalMeasurement = undefined;
    if (fault === 'baselineDto') f.run.universalMeasurement = async () => captureUniversalPreparedness({compiledLegacyMode:false,universalConfigured:true,reviewsConfigured:true,humanHttpEnabled:false,humanProfile:null});
    await assert.rejects(f.run.prepare(f.request)); assert.deepEqual(f.events, []);
  }
});

test('changed secret file with unchanged public policy pins is rejected before stopping the original', async t => {
  const f = await fixture(t, { human: true }); await f.run.prepare(f.request);
  const digest = publicUniversalPolicy(f.handle).policyDigest;
  f.configuration.clients[0].clientSecret = random(); await writeFile(f.humanSource, JSON.stringify(f.configuration), { mode: 0o600 });
  const updated = await prepareUniversalPolicy(f.plan, f.options); f.handles.push(updated); assert.equal(publicUniversalPolicy(updated).policyDigest, digest);
  await assert.rejects(f.run.promote(), /universal_policy_file_changed/u);
  assert.equal(f.events.some(value => value.startsWith('stop:')), false); assert.equal(f.map.get(args.originalId).State.Running, true);
});

test('encrypted binding rejects equal inherited-secret changes to original and candidate after a fresh process restore', async t => {
  const f = await fixture(t); await f.run.prepare(f.request);
  const sealed = await sealUniversalPolicy(f.handle, { key: f.key, keyId: 'external-custody' });
  const publicHash = f.run.state.configurationSha256;
  for (const id of [args.originalId, f.run.candidate.Id]) f.map.get(id).Config.Env[1] = 'TOKEN=another-private-synthetic-value';
  const restored = await restoreUniversalPolicy(f.plan, sealed.packet, f.options, { key: f.key, keyId: 'external-custody', expectedWitnessId: sealed.witnessId }); f.handles.push(restored);
  assert.equal(publicUniversalPolicy(restored).policyDigest, sealed.policyDigest);
  const fresh = new Rollout({ engine: f.engine, maintenance: f.run.maintenance, ready: f.run.ready, universalMeasurement: f.run.universalMeasurement,
    allowUniversalFixture: true, storageProbe: f.run.storageProbe, sleep: async () => {} });
  await assert.rejects(fresh.guard({ ...f.request, universalPolicy: restored, universalWitnessId: sealed.witnessId }), /universal_policy_rollout_mismatch/u);
  assert.equal(f.events.some(value => value.startsWith('stop:')), false); assert.equal(JSON.stringify(f.records).includes(f.run.fingerprint), false);
  assert.equal(publicHash, f.run.state.configurationSha256);
});

test('bound handle cannot be rebound to another transaction and unbound restored packet cannot acquire a rollout binding', async t => {
  const f = await fixture(t); await f.run.prepare(f.request);
  await assert.rejects(f.run.guard({ ...f.request, transaction: '1'.repeat(20) }), /universal_policy_rollout_mismatch/u);
  const unbound = await prepareUniversalPolicy(f.plan, f.options); f.handles.push(unbound);
  const sealed = await sealUniversalPolicy(unbound, { key: f.key, keyId: 'custody' });
  const restored = await restoreUniversalPolicy(f.plan, sealed.packet, f.options, { key: f.key, keyId: 'custody', expectedWitnessId: sealed.witnessId }); f.handles.push(restored);
  assert.throws(() => bindUniversalRollout(restored, f.run.universalBinding(f.run.fingerprint)), /universal_policy_rollout_mismatch/u);
});

test('private runtime mismatch or ambiguous read never leaves maintenance and restores the same compatible original', async t => {
  for (const fault of ['wrong', 'ambiguous']) {
    const f = await fixture(t); await f.run.prepare(f.request);
    f.run.universalMeasurement = async () => { if (fault === 'ambiguous') throw new SafeError('universal_measurement_unresolved'); return { ok: true, storageReady: true }; };
    await assert.rejects(f.run.promote()); assert.equal(f.run.state.phase, 'restored'); assert.equal(f.map.get(args.originalId).State.Running, true);
    assert.equal(f.run.state.universalPreparedness, undefined); assert.equal(f.events.includes('helper:rollback'), true);
    assert.equal(f.events.filter(value => value === 'helper:leave').length, 1);
  }
});

test('late secret drift before leave restores old exact environment; a changed original is fenced instead of auto-started', async t => {
  for (const changeOriginal of [false, true]) {
    const f = await fixture(t, { human: true }), record = f.run.record; await f.run.prepare(f.request);
    f.run.record = async state => { await record(state); if (state.phase === 'candidate_ready') {
      f.configuration.cookieKeys = [random()]; await writeFile(f.humanSource, JSON.stringify(f.configuration), { mode: 0o600 });
      if (changeOriginal) f.map.get(args.originalId).Config.Env[1] = 'TOKEN=unapproved-private-value';
    } };
    await assert.rejects(f.run.promote()); assert.equal(f.run.state.phase, changeOriginal ? 'recovery_required' : 'restored');
    assert.equal(f.map.get(args.originalId).State.Running, !changeOriginal);
    if (changeOriginal) assert.equal(f.events.includes('start-old'), false);
  }
});

test('candidate never inherits the previous image universal mode label', async t => {
  const f = await fixture(t); f.map.get(args.originalId).Config.Labels[universalModeLabel] = 'forged-mode';
  await f.run.prepare(f.request); assert.equal(Object.hasOwn(f.run.config.Labels, universalModeLabel), false);
  assert.equal(Object.hasOwn(createConfig(f.run.original, args.candidateImage, args.transaction).Labels, universalModeLabel), false);
});

test('new Human2 data fences a reader1-only fallback before any downgrade helper or original restart', async t => {
  const f = await fixture(t), image = f.engine.image;
  f.engine.image = async id => { const value = await image(id); if (id === args.originalImage) {
    const readers = JSON.parse(currentStorageReaders); readers.readers.humanIdentity = [1]; value.Config.Labels[storageReaderLabel] = JSON.stringify(readers);
  } return value; };
  f.run.storageProbe = async () => ({ ok: true, schema: 'soty.storage-format.v5', rooms: 1, apps: 'empty', notes: 'empty', capabilities: 'empty',
    appRegistration: 'empty', feedback: 'empty', humanIdentity: f.migrated ? 2 : 'empty' });
  await f.run.prepare(f.request); f.run.universalMeasurement = async () => ({ ok: true, storageReady: true });
  await assert.rejects(f.run.promote(), /recovery_required/u); assert.equal(f.run.state.failureCode, 'storage_reader_incompatible');
  assert.equal(f.map.get(args.originalId).State.Running, false); assert.equal(f.map.get(args.originalId).HostConfig.RestartPolicy.Name, 'no');
  assert.equal(f.events.includes('helper:rollback'), false); assert.equal(f.events.includes('start-old'), false);
});
