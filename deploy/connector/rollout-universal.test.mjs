import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, readFile, rm, realpath } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { randomBytes, generateKeyPairSync } from 'node:crypto';
import { fixture as engineFixture, args } from './test-support/rollout-engine.mjs';
import { Rollout, createConfig, preservationHash, hash } from './rollout.mjs';
import { prepareUniversalPolicy, publicUniversalPolicy, sealUniversalPolicy, restoreUniversalPolicy, disposeUniversalPolicy,
  captureUniversalPreparedness, universalModeLabel, bindUniversalRollout } from './universal-policy.mjs';
import { createHumanIdentityHostProfile } from '../../modules/human-identity/profile.mjs';
import { SafeError } from './docker-api.mjs';
import { storageReaderLabel, currentStorageReaders } from './storage-guard.mjs';
import { STANDARD_ROOT_WORKSPACE_SOURCE } from '../../modules/source-app/server/standard-profile.mjs';

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
  return { ...f, get migrated() { return f.migrated; }, root, handle, request, options, plan, configuration, humanSource, humanProfile, key, handles };
}

async function activeFeaturesUpdate(t) {
  const f = await fixture(t, { human: true }), old = f.map.get(args.originalId);
  const inherited = createConfig(old, args.originalImage, args.transaction, args.revision, undefined, f.handle);
  old.Config.Env = [...inherited.Env, 'SOTY_TRUST_PROXY=192.0.2.1']; old.HostConfig = inherited.HostConfig;
  const image = f.engine.image;
  f.engine.image = async id => { const value = await image(id); value.Config.Labels[universalModeLabel] = '0'; return value; };
  const current = captureUniversalPreparedness({ compiledLegacyMode: false, universalConfigured: true, reviewsConfigured: true,
    humanProfile: f.humanProfile, humanHttpEnabled: true });
  const appId = 'app-' + '4'.repeat(32), selectedSource = path.join(f.root, 'workspace-registry.json');
  const profiles = [{ schema: 'soty.selected-human-embed.v2', appId, connector: { linkId: 'source-link', hostDeviceId: 'source-device', connectorId: 'source-connector' },
    target: { revision: 2, digest: '5'.repeat(64) }, sourceProfile: STANDARD_ROOT_WORKSPACE_SOURCE,
    resource: { registryId: 'soty', tenantId: 'owner.fixture', environmentId: 'production', appId, resourceId: 'hive:workspace',
      selection: { kind: 'soty.workspace.v1', nativeId: 'hive', incarnationId: 'native-incarnation' } },
    issuer: f.plan.human.issuer, clientId: f.configuration.clients[0].id, parentOrigin: origin,
    embedOrigin: 'https://hive.fixture.invalid', nativeOrigin: 'https://hive.fixture.invalid' }];
  await writeFile(selectedSource, JSON.stringify({ schema: 'soty.selected-embed-registry.v1', profiles }), { mode: 0o600 });
  f.plan = { ...f.plan, selected: { source: selectedSource, migrationConfigured: true }, expectedCurrentPreparednessSha256: hash(current) };
  f.handle = await prepareUniversalPolicy(f.plan, f.options); f.handles.push(f.handle); f.request.universalPolicy = f.handle;
  const target = captureUniversalPreparedness({ compiledLegacyMode: false, universalConfigured: true, reviewsConfigured: true,
    humanProfile: f.humanProfile, humanHttpEnabled: true, selectedProfiles: profiles,
    selectedRegistryConfigured: true, selectedMigrationConfigured: true, selectedResourceMigrationConfigured: true });
  f.run.universalMeasurement = async id => id === args.originalId ? current : target;
  return { ...f, current, target };
}

test('features CAS adds an approved workspace registry without disabling the current Human/profile or inherited proxy settings', async t => {
  const f = await activeFeaturesUpdate(t), before = structuredClone(f.map.get(args.originalId));
  await f.run.prepare(f.request); await f.run.promote();
  assert.equal(f.run.state.phase, 'committed'); assert.equal(f.run.state.universalPhase, 'features');
  assert.deepEqual(f.run.original.Config.Env, before.Config.Env);
  for (const value of before.Config.Env) assert.ok(f.run.config.Env.includes(value));
  assert.deepEqual(f.run.config.HostConfig.Binds, before.HostConfig.Binds);
  assert.deepEqual(f.run.config.HostConfig.Mounts.slice(0, before.HostConfig.Mounts.length), before.HostConfig.Mounts);
  assert.equal(f.run.config.Env.includes('SOTY_SELECTED_EMBED_MIGRATION=1'), true);
  assert.equal(f.run.config.Env.includes('SOTY_SELECTED_RESOURCE_MIGRATION=1'), true);
  assert.equal(f.events.filter(value => value === 'start-candidate').length, 1);
  const sealed = await sealUniversalPolicy(f.handle, { key: f.key, keyId: 'update-custody' });
  const restored = await restoreUniversalPolicy(f.plan, sealed.packet, f.options, { key: f.key, keyId: 'update-custody', expectedWitnessId: sealed.witnessId });
  f.handles.push(restored); assert.equal(publicUniversalPolicy(restored).expectedCurrentPreparednessSha256, hash(f.current));
});

test('features CAS rejects stale, OFF, unknown measurement and wrong original mode/reader before CREATE or STOP', async t => {
  for (const fault of ['drift', 'off', 'unknown', 'mode', 'reader']) await t.test(fault, async t => {
    const f = await activeFeaturesUpdate(t);
    if (fault === 'drift') f.run.universalMeasurement = async () => ({ ...f.current, human: { ...f.current.human, signingPublicKeysDigest: '0'.repeat(64) } });
    if (fault === 'off') {
      const off = captureUniversalPreparedness({ compiledLegacyMode: true, universalConfigured: false,
        reviewsConfigured: false, humanHttpEnabled: false, humanProfile: null });
      const handle = await prepareUniversalPolicy({ ...f.plan, expectedCurrentPreparednessSha256: hash(off) }, f.options);
      f.handles.push(handle); f.request.universalPolicy = handle; f.run.universalMeasurement = async () => off;
    }
    if (fault === 'unknown') f.run.universalMeasurement = async () => { throw new SafeError('universal_measurement_unresolved'); };
    if (fault === 'mode' || fault === 'reader') { const image = f.engine.image; f.engine.image = async id => {
      const value = await image(id); if (id === args.originalImage) {
        if (fault === 'mode') value.Config.Labels[universalModeLabel] = '1';
        else { const readers = JSON.parse(currentStorageReaders); readers.readers.apps = [1, 2, 3, 4, 5, 6]; value.Config.Labels[storageReaderLabel] = JSON.stringify(readers); }
      } return value;
    }; }
    await assert.rejects(f.run.prepare(f.request)); assert.deepEqual(f.events, []);
    assert.equal(f.map.get(args.originalId).State.Running, true);
  });
});

test('features update cannot use an ON runtime without a CAS or survive current runtime drift between prepare and promote', async t => {
  const f = await activeFeaturesUpdate(t), noCas = { ...f.plan }; delete noCas.expectedCurrentPreparednessSha256;
  const unbound = await prepareUniversalPolicy(noCas, f.options); f.handles.push(unbound);
  await assert.rejects(f.run.prepare({ ...f.request, universalPolicy: unbound })); assert.deepEqual(f.events, []);
  await f.run.prepare(f.request);
  f.run.universalMeasurement = async id => id === args.originalId ? { ...f.current, human: { ...f.current.human, signingPublicKeysDigest: '0'.repeat(64) } } : f.target;
  const fresh = new Rollout({ engine: f.engine, maintenance: f.run.maintenance, ready: f.run.ready, universalMeasurement: f.run.universalMeasurement,
    allowUniversalFixture: true, storageProbe: f.run.storageProbe, sleep: async () => {} });
  await assert.rejects(fresh.guard(f.request), /universal_current_runtime_mismatch/u);
  assert.equal(f.events.some(value => value.startsWith('stop:')), false);
});

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

const generatedBridgeMac='02:42:ac:11:00:03';
function clearBridgeMacOnStop(f,{mode='bridge',afterStop=()=>{}}={}){
  const old=f.map.get(args.originalId);old.HostConfig.NetworkMode=mode;
  old.NetworkSettings.Networks={bridge:{IPAMConfig:null,Aliases:null,Links:null,DriverOpts:null,GwPriority:0,MacAddress:generatedBridgeMac}};
  const stop=f.engine.stop;
  f.engine.stop=async id=>{await stop(id);if(id===args.originalId){old.NetworkSettings.Networks.bridge.MacAddress='';afterStop(old);}};
  return old;
}

test('universal promotion preserves generated bridge MAC intent after its confirmed original STOP',async t=>{
  for(const phase of ['legacy-baseline','features'])for(const mode of ['bridge','default']){
    const f=await fixture(t,{phase}),old=clearBridgeMacOnStop(f,{mode});
    await f.run.prepare(f.request);
    const config=structuredClone(f.run.config),original=structuredClone(f.run.original);
    await f.run.promote();
    assert.equal(f.run.state.phase,'committed');assert.equal(old.NetworkSettings.Networks.bridge.MacAddress,'');
    assert.deepEqual(f.run.config,config);assert.deepEqual(f.run.original,original);
    assert.equal(f.map.get(f.run.candidate.Id).NetworkSettings.Networks.bridge.MacAddress,generatedBridgeMac);
    assert.ok(f.events.indexOf('stop:'+args.originalId)<f.events.indexOf('start-candidate'));
    assert.equal(f.events.includes('helper:rollback'),false);
  }
});

test('universal failed readiness can restore the same original after Docker cleared its bridge MAC',async t=>{
  const f=await fixture(t,{fault:{op:'readiness',when:'before'}}),old=clearBridgeMacOnStop(f);
  await f.run.prepare(f.request);await assert.rejects(f.run.promote(),/injected_failure/u);
  assert.equal(f.run.state.phase,'restored');assert.equal(old.State.Running,true);
  assert.equal(old.Id,args.originalId);assert.equal(old.Image,args.originalImage);
  assert.equal(old.HostConfig.RestartPolicy.Name,'unless-stopped');
  assert.equal(f.events.filter(x=>x==='start-old').length,1);
  assert.equal(f.map.get(f.run.candidate.Id).State.Running,false);
});

test('API1.45 reflected bridge MAC survives confirmed STOP for features CAS and restoration',async t=>{
  for(const fail of [false,true]){
    const f=await activeFeaturesUpdate(t),old=clearBridgeMacOnStop(f,{mode:'default'});
    old.Config.MacAddress=generatedBridgeMac;
    await f.run.prepare(f.request);const config=structuredClone(f.run.config),original=structuredClone(f.run.original);
    if(fail)failBeforeCandidateStart(f);
    if(fail)await assert.rejects(f.run.promote(),/universal_policy_rollout_mismatch/u);else await f.run.promote();
    assert.equal(f.run.state.phase,fail?'restored':'committed');
    assert.deepEqual(f.run.config,config);assert.deepEqual(f.run.original,original);
    assert.equal(old.Config.MacAddress,generatedBridgeMac);
    assert.equal(old.NetworkSettings.Networks.bridge.MacAddress,'');
    assert.equal(f.map.get(f.run.candidate.Id).NetworkSettings.Networks.bridge.MacAddress,generatedBridgeMac);
    assert.equal(f.events.includes('helper:rollback'),false);
    assert.equal(f.events.includes('start-old'),fail);assert.equal(f.events.includes('start-candidate'),!fail);
  }
});

test('reflected MAC reconciliation rejects modified top address, endpoint, or custom network',async t=>{
  for(const [name,afterStop] of Object.entries({
    topChanged:old=>old.Config.MacAddress='02:42:ac:11:00:04',
    topRemoved:old=>delete old.Config.MacAddress,
    endpointChanged:old=>old.NetworkSettings.Networks.bridge.MacAddress='02:42:ac:11:00:04',
    customNetwork:old=>old.HostConfig.NetworkMode='custom',
  }))await t.test(name,async t=>{
    const f=await activeFeaturesUpdate(t),old=clearBridgeMacOnStop(f,{afterStop});old.Config.MacAddress=generatedBridgeMac;
    await f.run.prepare(f.request);await assert.rejects(f.run.promote());
    assert.equal(f.run.state.phase,'recovery_required');
    assert.equal(f.events.includes('start-old'),false);assert.equal(f.events.includes('start-candidate'),false);
  });
});

test('post-STOP MAC reconciliation still fences configuration and identity drift before either application START',async t=>{
  const changes={
    nonemptyMac:old=>old.NetworkSettings.Networks.bridge.MacAddress='02:42:ac:11:00:04',
    nullMac:old=>old.NetworkSettings.Networks.bridge.MacAddress=null,
    absentMac:old=>delete old.NetworkSettings.Networks.bridge.MacAddress,
    env:old=>old.Config.Env.push('UNAPPROVED=1'),
    ipam:old=>old.NetworkSettings.Networks.bridge.IPAMConfig={IPv4Address:'172.17.0.4'},
    alias:old=>old.NetworkSettings.Networks.bridge.Aliases=['unapproved'],
    links:old=>old.NetworkSettings.Networks.bridge.Links=['other:alias'],
    driver:old=>old.NetworkSettings.Networks.bridge.DriverOpts={unapproved:'1'},
    priority:old=>old.NetworkSettings.Networks.bridge.GwPriority=1,
    explicitMac:old=>old.Config.MacAddress=generatedBridgeMac,
    identity:old=>old.Id='9'.repeat(64),
    image:old=>old.Image='sha256:'+'9'.repeat(64),
  };
  for(const [name,afterStop] of Object.entries(changes))await t.test(name,async t=>{
    const f=await fixture(t);clearBridgeMacOnStop(f,{afterStop});await f.run.prepare(f.request);
    await assert.rejects(f.run.promote());
    assert.equal(f.run.state.phase,'recovery_required');
    assert.equal(f.events.includes('start-candidate'),false);assert.equal(f.events.includes('start-old'),false);
  });
});

test('cleared MAC cannot be normalized before our STOP, on a candidate, or for configured network intent',async t=>{
  for(const state of ['running','externally-stopped']){
    const f=await fixture(t),old=clearBridgeMacOnStop(f);await f.run.prepare(f.request);
    old.NetworkSettings.Networks.bridge.MacAddress='';
    if(state==='externally-stopped')Object.assign(old.State,{Running:false,Status:'exited'});
    await assert.rejects(f.run.checkUniversalCurrent(),/universal_policy_rollout_mismatch/u);
    await assert.rejects(f.run.checkOriginalPreserved(),/original_configuration_changed/u);
    assert.equal(f.events.some(x=>x.startsWith('stop:')),false);
  }
  {
    const f=await fixture(t);clearBridgeMacOnStop(f);await f.run.prepare(f.request);
    f.map.get(f.run.candidate.Id).NetworkSettings.Networks.bridge.MacAddress='';
    await assert.rejects(f.run.promote(),/candidate_configuration_mismatch/u);
    assert.equal(f.events.some(x=>x.startsWith('stop:')),false);
  }
  const settings=[old=>old.Config.MacAddress='02:42:ac:11:00:04',
    old=>old.NetworkSettings.Networks.bridge.IPAMConfig={IPv4Address:'172.17.0.3'},
    old=>old.NetworkSettings.Networks.bridge.Aliases=['configured'],
    old=>old.NetworkSettings.Networks.bridge.Links=['other:configured'],
    old=>old.NetworkSettings.Networks.bridge.DriverOpts={configured:'1'}];
  for(const configure of settings){
    const f=await fixture(t),old=clearBridgeMacOnStop(f);configure(old);await f.run.prepare(f.request);
    await assert.rejects(f.run.promote());assert.equal(f.run.state.phase,'recovery_required');
    assert.equal(f.events.includes('start-candidate'),false);assert.equal(f.events.includes('start-old'),false);
  }
});

function failBeforeCandidateStart(f,{error=new SafeError('universal_policy_rollout_mismatch'),change=()=>{}}={}){
  const candidate=f.map.get(f.run.candidate.Id),check=f.run.checkUniversalCurrent.bind(f.run);
  Object.assign(candidate.State,{Status:'created',Running:false,Restarting:false,Pid:0,StartedAt:'0001-01-01T00:00:00Z'});
  let failed=false;
  f.run.checkUniversalCurrent=async()=>{
    await check();
    if(!failed&&candidate.Name==='/soty-online-chat'){
      failed=true;change(candidate.State);throw error;
    }
  };
}

test('confirmed never-started candidate restores the original without downgrading durable storage',async t=>{
  for(const phase of ['legacy-baseline','features']){
    const f=await fixture(t,{phase,fault:{rollbackFailure:true}}),old=clearBridgeMacOnStop(f);
    await f.run.prepare(f.request);failBeforeCandidateStart(f);
    await assert.rejects(f.run.promote(),/universal_policy_rollout_mismatch/u);
    assert.equal(f.run.state.phase,'restored');assert.equal(f.run.state.activationFailureCode,'universal_policy_rollout_mismatch');
    assert.equal(old.Id,args.originalId);assert.equal(old.Image,args.originalImage);assert.equal(old.Name,'/soty-online-chat');
    assert.equal(old.State.Running,true);assert.equal(old.HostConfig.RestartPolicy.Name,'unless-stopped');
    assert.equal(f.events.includes('start-candidate'),false);assert.equal(f.events.includes('helper:rollback'),false);
    assert.equal(f.events.filter(x=>x==='helper:status').length,3);assert.equal(f.events.filter(x=>x==='start-old').length,1);
    assert.ok(f.events.lastIndexOf('ready:original')<f.events.indexOf('helper:leave'));
    assert.equal(f.map.get(f.run.candidate.Id).State.Status,'created');assert.equal(f.map.get('sentinel').State.Running,true);
  }
});

test('incomplete or contrary never-started evidence retains the existing rollback barrier',async t=>{
  const changes={
    statusMissing:s=>delete s.Status,statusExited:s=>s.Status='exited',runningMissing:s=>delete s.Running,runningTrue:s=>s.Running=true,
    restartingMissing:s=>delete s.Restarting,restartingTrue:s=>s.Restarting=true,restartingString:s=>s.Restarting='false',
    pidMissing:s=>delete s.Pid,pidNonzero:s=>s.Pid=7,pidString:s=>s.Pid='0',
    startedAtMissing:s=>delete s.StartedAt,startedAtNull:s=>s.StartedAt=null,startedAtEmpty:s=>s.StartedAt='',
    startedAtNonzero:s=>s.StartedAt='2026-10-11T00:00:00Z',startedAtMalformed:s=>s.StartedAt='0001-01-01',
  };
  for(const [name,change] of Object.entries(changes))await t.test(name,async t=>{
    const f=await fixture(t,{fault:{rollbackFailure:true}});await f.run.prepare(f.request);failBeforeCandidateStart(f,{change});
    await assert.rejects(f.run.promote(),/recovery_required/u);
    assert.equal(f.run.state.phase,'recovery_required');assert.equal(f.run.state.failureCode,'rollback_failed');
    assert.equal(f.run.state.activationFailureCode,'universal_policy_rollout_mismatch');
    assert.equal(f.events.includes('helper:rollback'),true);assert.equal(f.events.includes('start-old'),false);
    assert.equal(f.map.get(args.originalId).State.Running,false);
  });
});

test('START intent and a completed candidate START both require rollback even when restoration would fail',async t=>{
  for(const intent of ['starting','confirmed']){
    const f=await fixture(t,{fault:{rollbackFailure:true}});await f.run.prepare(f.request);
    failBeforeCandidateStart(f,{change:()=>{f.run.state.applicationStart={id:f.run.candidate.Id,state:intent};}});
    await assert.rejects(f.run.promote(),/recovery_required/u);
    assert.equal(f.events.includes('helper:rollback'),true);assert.equal(f.events.includes('start-old'),false);
    assert.equal(f.run.state.failureCode,'rollback_failed');assert.equal(f.run.state.activationFailureCode,'universal_policy_rollout_mismatch');
  }
  const f=await fixture(t,{fault:{op:'readiness',when:'before',rollbackFailure:true}});await f.run.prepare(f.request);
  Object.assign(f.map.get(f.run.candidate.Id).State,{Restarting:false,Pid:0,StartedAt:'0001-01-01T00:00:00Z'});
  await assert.rejects(f.run.promote(),/recovery_required/u);
  assert.equal(f.events.includes('start-candidate'),true);assert.equal(f.run.state.applicationStart.state,'confirmed');
  assert.equal(f.events.includes('helper:rollback'),true);assert.equal(f.events.includes('start-old'),false);
  assert.equal(f.run.state.activationFailureCode,'injected_failure');assert.equal(f.run.state.failureCode,'rollback_failed');
});

test('never-started status still requires unchanged pending work and the maintenance marker',async t=>{
  for(const mismatch of ['pending','maintenance']){
    const f=await fixture(t,{fault:{rollbackFailure:true}});await f.run.prepare(f.request);failBeforeCandidateStart(f);
    const maintenance=f.run.maintenance;
    f.run.maintenance=async verb=>{
      const result=await maintenance(verb);
      return verb==='status'&&f.run.state.activationFailureCode
        ?{...result,...(mismatch==='pending'?{count:1,activeJobs:[{id:'new-job',status:'queued'}]}:{maintenance:false})}:result;
    };
    await assert.rejects(f.run.promote(),/recovery_required/u);
    assert.equal(f.run.state.failureCode,'rollback_not_quiescent');assert.equal(f.events.includes('helper:rollback'),false);
    assert.equal(f.events.includes('start-old'),false);assert.equal(f.map.get(args.originalId).State.Running,false);
  }
});

test('activation failure is journaled once and survives secondary rollback failure',async t=>{
  for(const error of [new SafeError('primary_activation_failure'),Object.assign(new Error('private diagnostic detail'),{code:'not_a_safe_error'})]){
    const f=await fixture(t,{fault:{rollbackFailure:true}});await f.run.prepare(f.request);
    failBeforeCandidateStart(f,{error,change:s=>delete s.StartedAt});
    await assert.rejects(f.run.promote(),/recovery_required/u);
    const expected=error instanceof SafeError?'primary_activation_failure':'activation_failed';
    assert.equal(f.run.state.activationFailureCode,expected);assert.equal(f.run.state.failureCode,'rollback_failed');
    const first=f.records.findIndex(s=>s.activationFailureCode!==undefined);
    assert.ok(first>=0);assert.equal(f.records[first].phase,'maintenance_entered');
    assert.equal(f.records.slice(first).every(s=>s.activationFailureCode===expected),true);
    assert.equal(JSON.stringify(f.records).includes('private diagnostic detail'),false);
  }
  const f=await fixture(t,{fault:{rollbackFailure:true}});await f.run.prepare(f.request);
  await f.run.note('prepared',{activationFailureCode:'first_activation_failure'});
  failBeforeCandidateStart(f,{change:s=>delete s.StartedAt});
  await assert.rejects(f.run.promote(),/recovery_required/u);
  assert.equal(f.run.state.activationFailureCode,'first_activation_failure');assert.equal(f.run.state.failureCode,'rollback_failed');
});
