import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, readFile, rm, realpath, rename, mkdir, symlink, link, chmod, lstat } from 'node:fs/promises';
import { generateKeyPairSync, randomBytes, createHash } from 'node:crypto';
import { tmpdir } from 'node:os';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { prepareUniversalPolicy, publicUniversalPolicy, assertUniversalPolicyCurrent, disposeUniversalPolicy,
  applyUniversalPolicy, assertUniversalImagePrerequisites, captureUniversalPreparedness, assertUniversalPreparedness,
  sealUniversalPolicy, restoreUniversalPolicy,
  UNIVERSAL_POLICY_SCHEMA, universalModeLabel, humanPrivateTarget, reviewsTarget, selectedTarget } from './universal-policy.mjs';
import { Rollout, createConfig, preservationHash } from './rollout.mjs';
import { currentStorageReaders, storageReaderLabel, assertStorageCompatible } from './storage-guard.mjs';
import { readStorageFormat } from './storage-probe.mjs';
import { loadUniversalConfiguration } from '../../server/universal-configuration.js';
import { createHumanIdentityHostProfile } from '../../modules/human-identity/profile.mjs';
import { createHttpApp } from '../../server/http-app.js';
import { HIVE_SELECTED_KERNEL_SOURCE, HIVE_SELECTED_SOURCE, STANDARD_SELECTED_SOURCE, STANDARD_SELECTED_SOURCE_V2 } from '../../modules/apps/scoped-embed/resource-route-adapters.mjs';
import { STANDARD_ROOT_WORKSPACE_SOURCE, STANDARD_WORKSPACE_SOURCE } from '../../modules/source-app/server/standard-profile.mjs';
import { loadInstalledApplicationsEntry } from '../../server/installed-applications-entry.js';
import { fixture as engineFixture, args as rolloutArgs } from './test-support/rollout-engine.mjs';

const origin = 'https://soty.fixture.invalid', issuer = origin + '/human-identity';
const installedApplicationsTarget = '/run/config/soty-installed-applications.mjs';
const random = () => randomBytes(32).toString('base64url');
const secretKey = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' });
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const failCode = code => error => error.code === code && error.message === code && !error.cause;
const reader3 = '{"version":3,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5,6],"notes":[1,2],"capabilities":[1,2,3]}}';
const image = (mode, readers = currentStorageReaders, character = 'a') => ({ Id: 'sha256:' + character.repeat(64),
  Config: { Labels: { [storageReaderLabel]: readers, [universalModeLabel]: mode } } });
const baselinePlan = () => ({ schema: UNIVERSAL_POLICY_SCHEMA, phase: 'legacy-baseline', human: null, reviews: null });
const emptyFeatures = () => ({ ...baselinePlan(), phase: 'features' });
function privateConfiguration() {
  return { clients: [{ id: 'rp.alpha', label: 'Приложение', redirectUri: 'https://alpha.fixture.invalid/callback', clientSecret: random(), version: 1 }],
    jwks: { keys: [{ ...secretKey, kid: 'fixture-key', alg: 'RS256', use: 'sig' }] }, cookieKeys: [random()], artifactKey: random(), artifactKeyId: 'fixture-artifact' };
}
function reviewConfiguration() {
  const providerRef = { id: 'reviews:povedai', version: 1, digest: '1'.repeat(64) }, subjectRef = { id: 'subject:app-alpha', version: 1, digest: '2'.repeat(64) };
  return { providers: [{ ...providerRef, origin: 'https://reviews.fixture.invalid' }], bindings: [{
    scope: { registryId: 'soty', tenantId: 'owner.fixture', appId: 'app.alpha', environmentId: 'production' },
    localSubject: { kind: 'app', id: 'app.alpha' }, providerRef, subjectRef, providerSubjectId: 'subject_' + '3'.repeat(32), providerEntityType: 'product', mode: 'public-read' }] };
}
function selectedConfiguration(f) {
  const appId = 'app-' + '4'.repeat(32), embedOrigin = 'https://alpha.fixture.invalid';
  return { schema: 'soty.selected-embed-registry.v1', profiles: [{ schema: 'soty.selected-human-embed.v1', appId,
    connector: { linkId: 'source-link', hostDeviceId: 'source-device', connectorId: 'source-connector' },
    target: { revision: 2, digest: '5'.repeat(64) }, sourceProfile: { id: 'planner.selected-workspace', version: 1, digest: '6'.repeat(64) },
    resource: { registryId: 'soty', tenantId: 'owner.fixture', environmentId: 'production', appId,
      resourceId: 'planner:selected', workspaceId: 'selected-workspace' }, issuer, clientId: f.human.clients[0].id,
    parentOrigin: origin, embedOrigin, nativeOrigin: 'https://native.fixture.invalid' }] };
}
async function fixture(t) {
  const root = await mkdtemp(path.join(await realpath(tmpdir()), 'soty-universal-policy-'));
  const beforeRemove = [];
  t.after(async () => { for (const close of beforeRemove.reverse()) await close();
    assert.equal(path.dirname(path.resolve(root)), await realpath(tmpdir())); assert.match(path.basename(root), /^soty-universal-policy-/u);
    await rm(root, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 }); });
  const humanSource = path.join(root, 'private-human.json'), reviewsSource = path.join(root, 'reviews.json');
  const human = privateConfiguration(), reviews = reviewConfiguration();
  await writeFile(humanSource, JSON.stringify(human), { mode: 0o600 }); await writeFile(reviewsSource, JSON.stringify(reviews), { mode: 0o644 });
  const options = { shellOrigins: [origin], fixtureRoot: root };
  const plan = { schema: UNIVERSAL_POLICY_SCHEMA, phase: 'features', human: { issuer, source: humanSource }, reviews: { source: reviewsSource, sha256: sha(await readFile(reviewsSource)) } };
  return { root, humanSource, reviewsSource, human, reviews, options, plan, beforeRemove };
}
async function prepared(t, f, plan = f.plan) { const handle = await prepareUniversalPolicy(plan, f.options);
  t.after(() => { try { disposeUniversalPolicy(handle); } catch (error) { assert.equal(error.code, 'universal_policy_handle_invalid'); } }); return handle; }
function original() {
  return { Id: '1'.repeat(64), Config: { Image: 'sha256:' + 'b'.repeat(64), Env: ['DATA_DIR=/data', 'FIXTURE_TOKEN=private-existing-value'],
    Labels: { owner: 'retained' }, User: '0', Cmd: ['node', 'server/index.js'], WorkingDir: '/app', Volumes: { '/data': {} }, Healthcheck: { Test: ['CMD', 'node', 'probe.js'] } },
  HostConfig: { Binds: ['/fixture/existing:/run/connect-releases:ro'], RestartPolicy: { Name: 'unless-stopped' }, Memory: 123456789,
    NanoCpus: 500000000, PidsLimit: 100, PortBindings: { '8080/tcp': [{ HostIp: '127.0.0.1', HostPort: '18182' }] }, NetworkMode: 'soty', CapDrop: ['ALL'] },
  Mounts: [{ Type: 'volume', Name: 'existing-data', Source: '/docker/volumes/existing-data', Destination: '/data', RW: true }],
  NetworkSettings: { Networks: { soty: { Aliases: ['old-alias'], IPAMConfig: { IPv4Address: '172.28.0.3' } } } } };
}
function runtime(f, app, profile) {
  return captureUniversalPreparedness({ compiledLegacyMode: false, universalConfigured: Boolean(app.locals.universalApps),
    reviewsConfigured: Boolean(app.locals.universalApps?.reviews), humanProfile: profile, humanHttpEnabled: app.locals.humanIdentityStatus.enabled,
    reviewsConfiguration: f.reviews });
}

async function installedFixture(t) {
  const f = await fixture(t), source = path.join(f.root, 'installed.mjs'), appId = 'app-' + 'f'.repeat(32);
  const scope = { appId, target: { revision: 2, digest: '5'.repeat(64) }, sourceProfile: STANDARD_ROOT_WORKSPACE_SOURCE,
    resource: { registryId: 'soty', tenantId: 'owner.fixture', environmentId: 'production', appId, resourceId: 'hive:workspace',
      selection: { kind: 'soty.workspace.v1', nativeId: 'hive', incarnationId: 'native-incarnation' } } };
  globalThis.__universalInstalledPolicy = { created: 0, closed: 0, credentials: 0 };
  f.beforeRemove.push(() => { delete globalThis.__universalInstalledPolicy; });
  const code = `export default async function(api){
    const state=globalThis.__universalInstalledPolicy;state.created++;
    const close=()=>{state.closed++;};api.ownClose(close);
    const composition=await api.loadInstalledExistingApplications({createRegistry:()=>[api.createCurrentContextSourceApplication(${JSON.stringify(scope)})]});
    return {...composition,close,sourceCredentialFor(){state.credentials++;throw Error('must_not_request_a_credential');}};
  }\n`;
  const save = text => writeFile(source, text, { mode: 0o600 }); await save(code);
  return { ...f, source, code, save, plan: { ...f.plan, installedApplications: { source, sha256: sha(code) } } };
}

test('installed operator CODE is admitted by the actual existing loader, closed, and bound to one fixed RO mount and two settings', async t => {
  const f = await installedFixture(t), handle = await prepared(t, f), receipt = publicUniversalPolicy(handle);
  assert.deepEqual(globalThis.__universalInstalledPolicy, { created: 1, closed: 1, credentials: 0 });
  const base = { Env: ['DATA_DIR=/data'], HostConfig: { Mounts: [] } }, applied = applyUniversalPolicy(base, handle);
  assert.deepEqual(base, { Env: ['DATA_DIR=/data'], HostConfig: { Mounts: [] } });
  assert.ok(applied.Env.includes('SOTY_INSTALLED_APPLICATIONS_MODULE=' + installedApplicationsTarget));
  assert.ok(applied.Env.includes('SOTY_INSTALLED_APPLICATIONS_SHA256=' + sha(f.code)));
  assert.deepEqual(applied.HostConfig.Mounts.at(-1), { Type: 'bind', Source: f.source, Target: installedApplicationsTarget, ReadOnly: true });
  assert.deepEqual(receipt.mounts.at(-1), { source: f.source, target: installedApplicationsTarget, readOnly: true, visibility: 'private' });
  assert.deepEqual(applyUniversalPolicy(applied, handle), applied);
  assert.equal(JSON.stringify(receipt).includes('must_not_request_a_credential'), false);
  const entry = await loadInstalledApplicationsEntry({ SOTY_INSTALLED_APPLICATIONS_MODULE: f.source, SOTY_INSTALLED_APPLICATIONS_SHA256: sha(f.code) });
  assert.equal(entry.sourceReferences.length, 1); await entry.close(); await entry.close();
  assert.deepEqual(globalThis.__universalInstalledPolicy, { created: 2, closed: 2, credentials: 0 });
  const key = randomBytes(32), keyId = 'fixture-installed-custody', sealed = await sealUniversalPolicy(handle, { key, keyId });
  const restored = await restoreUniversalPolicy(f.plan, sealed.packet, f.options, { key, keyId, expectedWitnessId: sealed.witnessId });
  f.beforeRemove.push(() => disposeUniversalPolicy(restored)); assert.deepEqual(publicUniversalPolicy(restored), receipt);
  assert.deepEqual(globalThis.__universalInstalledPolicy, { created: 3, closed: 3, credentials: 0 });
});

test('installed operator omission preserves inherited settings and baseline wire without loading a module', async t => {
  const f = await installedFixture(t), omitted = { ...f.plan }; delete omitted.installedApplications;
  const before = { Env: ['DATA_DIR=/data', 'SOTY_INSTALLED_APPLICATIONS_MODULE=/existing/installed.mjs',
    'SOTY_INSTALLED_APPLICATIONS_SHA256=' + 'b'.repeat(64)], HostConfig: { Mounts: [] } };
  const handle = await prepared(t, f, omitted), applied = applyUniversalPolicy(before, handle);
  assert.deepEqual(applied.Env.slice(0, before.Env.length), before.Env);
  assert.equal(publicUniversalPolicy(handle).mounts.some(value => value.target === installedApplicationsTarget), false);
  assert.deepEqual(applyUniversalPolicy(before, await prepared(t, f, baselinePlan())),
    { ...before, Env: [...before.Env, 'SOTY_UNIVERSAL_OPERATOR_ENABLED=1'] });
  assert.deepEqual(globalThis.__universalInstalledPolicy, { created: 0, closed: 0, credentials: 0 });
});

test('installed operator input rejects extra authority, malformed hash, wrong bytes and baseline enablement before calling the factory', async t => {
  const f = await installedFixture(t);
  for (const installedApplications of [null, true, { ...f.plan.installedApplications, env: ['FORGED=1'] },
    { ...f.plan.installedApplications, actor: { accountId: 'forged' } }, { ...f.plan.installedApplications, sha256: 'A'.repeat(64) }])
    await assert.rejects(prepareUniversalPolicy({ ...f.plan, installedApplications }, f.options), failCode('universal_policy_invalid'));
  await assert.rejects(prepareUniversalPolicy({ ...baselinePlan(), installedApplications: f.plan.installedApplications }, f.options), failCode('universal_policy_invalid'));
  await assert.rejects(prepareUniversalPolicy({ ...f.plan, installedApplications: { ...f.plan.installedApplications, sha256: '0'.repeat(64) } }, f.options),
    failCode('universal_policy_installed_applications_hash_mismatch'));
  let accessed = false; const getter = { sha256: sha(f.code) };
  Object.defineProperty(getter, 'source', { enumerable: true, get() { accessed = true; return f.source; } });
  await assert.rejects(prepareUniversalPolicy({ ...f.plan, installedApplications: getter }, f.options), failCode('universal_policy_invalid'));
  assert.equal(accessed, false); assert.deepEqual(globalThis.__universalInstalledPolicy, { created: 0, closed: 0, credentials: 0 });
});

test('installed operator code is bounded and the existing loader rejects a JSON catalog or unsupported factory while closing owned resources', async t => {
  const f = await installedFixture(t);
  for (const code of ['{"catalog":[{"role":"admin"}]}',
    `export default function(api){api.ownClose(()=>{globalThis.__universalInstalledPolicy.closed++;});throw {get message(){throw Error('private_error');}};}`]) {
    await f.save(code);
    await assert.rejects(prepareUniversalPolicy({ ...f.plan, installedApplications: { source: f.source, sha256: sha(code) } }, f.options),
      failCode('universal_policy_installed_applications_invalid'));
  }
  assert.equal(globalThis.__universalInstalledPolicy.closed, 1);
  const code = f.code + '\n/*' + ' '.repeat(131072 - Buffer.byteLength(f.code) - 6) + '*/\n';
  assert.equal(Buffer.byteLength(code), 131072); await f.save(code);
  await prepared(t, f, { ...f.plan, installedApplications: { source: f.source, sha256: sha(code) } });
  await f.save(code + ' ');
  await assert.rejects(prepareUniversalPolicy({ ...f.plan, installedApplications: { source: f.source, sha256: sha(code + ' ') } }, f.options),
    failCode('universal_policy_file_invalid'));
});

test('installed operator code cannot use a symlink or junction, a hardlink or nonprivate Linux permissions', async t => {
  const f = await installedFixture(t), alias = path.join(f.root, 'alias.mjs'); await link(f.source, alias);
  await assert.rejects(prepareUniversalPolicy(f.plan, f.options), failCode('universal_policy_file_invalid')); await rm(alias);
  if (process.platform === 'linux') {
    await chmod(f.source, 0o644); await assert.rejects(prepareUniversalPolicy(f.plan, f.options), failCode('universal_policy_file_permissions'));
    await chmod(f.source, 0o600);
  }
  const actual = path.join(f.root, 'actual-module'), shortcut = path.join(f.root, 'module-shortcut'); await mkdir(actual, { mode: 0o700 });
  await rename(f.source, path.join(actual, 'installed.mjs')); await symlink(actual, shortcut, process.platform === 'win32' ? 'junction' : 'dir');
  await assert.rejects(prepareUniversalPolicy({ ...f.plan, installedApplications: { source: path.join(shortcut, 'installed.mjs'), sha256: sha(f.code) } }, f.options),
    failCode('universal_policy_file_invalid'));
  assert.deepEqual(globalThis.__universalInstalledPolicy, { created: 0, closed: 0, credentials: 0 });
});

test('installed operator settings and mount refuse conflicting old values, duplicates and shadowing', async t => {
  const f = await installedFixture(t), handle = await prepared(t, f), base = { Env: ['DATA_DIR=/data'], HostConfig: { Mounts: [] } };
  for (const extra of [['SOTY_INSTALLED_APPLICATIONS_MODULE=/foreign'], ['SOTY_INSTALLED_APPLICATIONS_SHA256=' + '0'.repeat(64)],
    ['SOTY_INSTALLED_APPLICATIONS_MODULE'], ['SOTY_INSTALLED_APPLICATIONS_SHA256=' + sha(f.code), 'SOTY_INSTALLED_APPLICATIONS_SHA256=' + sha(f.code)]])
    assert.throws(() => applyUniversalPolicy({ ...base, Env: [...base.Env, ...extra] }, handle), failCode('universal_policy_preexisting_configuration'));
  for (const mount of [{ Type: 'bind', Source: f.source, Target: installedApplicationsTarget, ReadOnly: false },
    { Type: 'bind', Source: f.root, Target: '/run/config', ReadOnly: true }])
    assert.throws(() => applyUniversalPolicy({ ...base, HostConfig: { Mounts: [mount] } }, handle), failCode('universal_policy_preexisting_configuration'));
});

test('installed operator byte drift after prepare is fenced before existing rollout STOP and cannot restore an old encrypted witness', async t => {
  const f = await installedFixture(t), handle = await prepared(t, f), key = randomBytes(32), keyId = 'fixture-installed-custody';
  const sealed = await sealUniversalPolicy(handle, { key, keyId }), engine = engineFixture();
  const imageRead = engine.engine.image;
  engine.engine.image = async id => { const image = await imageRead(id); image.Config.Labels[universalModeLabel] = id === rolloutArgs.candidateImage ? '0' : '1'; return image; };
  engine.run.allowUniversalFixture = true;
  engine.run.universalMeasurement = async () => captureUniversalPreparedness({ compiledLegacyMode: true,
    universalConfigured: false, reviewsConfigured: false, humanHttpEnabled: false, humanProfile: null });
  await engine.run.prepare({ ...rolloutArgs, universalPolicy: handle, universalWitnessId: sealed.witnessId });
  const changed = f.code + '\n// changed approved code\n'; await f.save(changed);
  await assert.rejects(engine.run.promote(), failCode('universal_policy_file_changed'));
  assert.equal(engine.events.some(value => value.startsWith('stop:')), false);
  assert.equal(engine.map.get(rolloutArgs.originalId).State.Running, true);
  await assert.rejects(restoreUniversalPolicy(f.plan, sealed.packet, f.options, { key, keyId, expectedWitnessId: sealed.witnessId }),
    failCode('universal_policy_witness_invalid'));
  await assert.rejects(restoreUniversalPolicy({ ...f.plan, installedApplications: { source: f.source, sha256: sha(changed) } }, sealed.packet, f.options,
    { key, keyId, expectedWitnessId: sealed.witnessId }), failCode('universal_policy_witness_mismatch'));
});

test('Root signed workspace policy needs no Human RP client; legacy workspace still requires its exact client and callback', async t => {
  const f = await fixture(t), base = selectedConfiguration(f).profiles[0], source = path.join(f.root, 'root-workspace.json');
  const profile = { ...base, schema: 'soty.selected-human-embed.v2', sourceProfile: STANDARD_ROOT_WORKSPACE_SOURCE,
    resource: { registryId: 'soty', tenantId: 'owner.fixture', environmentId: 'production', appId: base.appId,
      resourceId: 'hive:workspace', selection: { kind: 'soty.workspace.v1', nativeId: 'hive', incarnationId: 'native-incarnation' } },
    nativeOrigin: 'https://hive.fixture.invalid' };
  const nativeCallback = profile.nativeOrigin + '/account/soty/callback';
  f.human.clients[0].redirectUri = nativeCallback;
  await writeFile(f.humanSource, JSON.stringify(f.human), { mode: 0o600 });
  const plan = { ...f.plan, selected: { source, migrationConfigured: false } };
  async function configured(value) {
    await writeFile(source, JSON.stringify({ schema: 'soty.selected-embed-registry.v1', profiles: [value] }), { mode: 0o600 });
  }
  await configured(profile);
  const accepted = await prepared(t, f, plan); assert.equal(publicUniversalPolicy(accepted).selected.profileCount, 1);
  for (const clientId of ['rp.missing', 'rp.foreign']) {
    await configured({ ...profile, clientId });
    const withoutRp = await prepared(t, f, plan);
    assert.equal(publicUniversalPolicy(withoutRp).selected.profileCount, 1);
  }
  for (const changed of [
    { ...profile, issuer: 'https://foreign.fixture.invalid/human-identity' },
    { ...profile, parentOrigin: 'https://foreign.fixture.invalid' },
    { ...profile, embedOrigin: 'http://localhost:43123' },
    { ...profile, nativeOrigin: 'http://native.fixture.invalid:43123' },
    { ...profile, sourceProfile: { ...STANDARD_ROOT_WORKSPACE_SOURCE, digest: '0'.repeat(64) } },
  ]) {
    await configured(changed); await assert.rejects(prepareUniversalPolicy(plan, f.options), error =>
      typeof error.code === 'string' && error.code.startsWith('universal_policy_') && !error.cause);
  }
  const legacy = { ...profile, sourceProfile: STANDARD_WORKSPACE_SOURCE };
  for (const clientId of ['rp.missing', 'rp.foreign']) {
    await configured({ ...legacy, clientId });
    await assert.rejects(prepareUniversalPolicy(plan, f.options), failCode('universal_policy_selected_human_required'));
  }
  await configured(legacy);
  await assert.rejects(prepareUniversalPolicy(plan, f.options), failCode('universal_policy_selected_human_required'));
  f.human.clients[0].redirectUri = profile.embedOrigin + '/api/embed/callback';
  await writeFile(f.humanSource, JSON.stringify(f.human), { mode: 0o600 });
  const oldAccepted = await prepared(t, f, plan); assert.equal(publicUniversalPolicy(oldAccepted).selected.profileCount, 1);
});

test('remembered consent requires an actual reader3 fallback and explicit host migration before starting', async t => {
  const f = await fixture(t);
  f.human.renewal = { admissionEnabled: true, clientIds: [f.human.clients[0].id] };
  f.human.deviceConsent = { admissionEnabled: true, clientIds: [f.human.clients[0].id] };
  f.human.renewalLimits = { activeFamilies: 256, familiesPerAccount: 64 };
  await writeFile(f.humanSource, JSON.stringify(f.human), { mode: 0o600 });
  const plan = { ...f.plan, human: { ...f.plan.human, deviceConsentMigrationConfigured: true } };
  const handle = await prepared(t, f, plan), receipt = publicUniversalPolicy(handle);
  assert.equal(receipt.human.deviceConsent.admissionEnabled, true);
  assert.equal(receipt.human.deviceConsent.eligibleClientCount, 1);
  assert.equal(Object.hasOwn(receipt.human, 'renewalLimits'), false);
  assert.equal(receipt.environment.find(value => value.name === 'SOTY_HUMAN_DEVICE_CONSENT_MIGRATION').value, '1');
  const v2 = JSON.parse(currentStorageReaders); v2.readers.humanIdentity = [1, 2];
  for (const side of ['candidate', 'original']) assert.throws(() => assertUniversalImagePrerequisites(handle, {
    candidateImage: image('0', side === 'candidate' ? JSON.stringify(v2) : currentStorageReaders),
    originalImage: image('1', side === 'original' ? JSON.stringify(v2) : currentStorageReaders, 'b'),
  }), failCode('universal_policy_reader_baseline_required'));
  assert.equal(assertUniversalImagePrerequisites(handle, { candidateImage: image('0'), originalImage: image('1', currentStorageReaders, 'b') }).phase, 'features');
  const loaded = loadUniversalConfiguration({ SOTY_HUMAN_IDENTITY_ENABLED: '1', SOTY_HUMAN_IDENTITY_ISSUER: issuer,
    SOTY_HUMAN_IDENTITY_KEYS_FILE: f.humanSource, SOTY_HUMAN_DEVICE_CONSENT_MIGRATION: '1' });
  const profile = createHumanIdentityHostProfile(loaded.humanIdentity, { shellOrigins: [origin] });
  assert.deepEqual(captureUniversalPreparedness({ compiledLegacyMode: false, universalConfigured: true, reviewsConfigured: true,
    humanProfile: profile, humanHttpEnabled: true, reviewsConfiguration: f.reviews }).human, receipt.human);
  for (const secret of [f.human.artifactKey, f.human.cookieKeys[0], f.human.clients[0].clientSecret, secretKey.d])
    assert.equal(JSON.stringify(receipt).includes(secret), false);
  await assertUniversalPolicyCurrent(handle);
  delete f.human.deviceConsent;
  await writeFile(f.humanSource, JSON.stringify(f.human), { mode: 0o600 });
  const migrationOnly = await prepared(t, f, plan);
  assert.equal(publicUniversalPolicy(migrationOnly).human.deviceConsent, undefined);
  for (const side of ['candidate', 'original']) assert.throws(() => assertUniversalImagePrerequisites(migrationOnly, {
    candidateImage: image('0', side === 'candidate' ? JSON.stringify(v2) : currentStorageReaders),
    originalImage: image('1', side === 'original' ? JSON.stringify(v2) : currentStorageReaders, 'b'),
  }), failCode('universal_policy_reader_baseline_required'));
});

test('approved private/public files produce only nonsecret receipt; opaque handle cannot be reconstructed', async t => {
  const f = await fixture(t), handle = await prepared(t, f), receipt = publicUniversalPolicy(handle);
  assert.equal(receipt.human.configured, true); assert.equal(receipt.human.clientCount, 1); assert.equal(receipt.reviews.bindingCount, 1);
  assert.deepEqual(receipt.mounts.map(value => value.target), [humanPrivateTarget, reviewsTarget]);
  assert.equal(Object.isFrozen(receipt.mounts[0]), true); assert.equal(JSON.stringify(handle), '{}');
  const text = JSON.stringify(receipt);
  for (const value of [f.human.clients[0].clientSecret, f.human.cookieKeys[0], f.human.artifactKey,
    f.human.jwks.keys[0].d, sha(await readFile(f.humanSource)), 'private-existing-value']) assert.equal(text.includes(value), false);
  assert.throws(() => publicUniversalPolicy(structuredClone(handle)), failCode('universal_policy_handle_invalid'));
  assert.throws(() => applyUniversalPolicy({}, JSON.parse(JSON.stringify(receipt))), failCode('universal_policy_handle_invalid'));
  assert.strictEqual(await assertUniversalPolicyCurrent(handle), receipt);
});

test('closed plan rejects commands, credentials, future schema, getters and baseline enablement before admitting a file', async t => {
  const f = await fixture(t);
  for (const plan of [{ ...f.plan, command: 'inert' }, { ...f.plan, actor: { accountId: 'forged' } }, { ...f.plan, schema: 'soty.universal-rollout-policy.v2' },
    { ...f.plan, human: { ...f.plan.human, sha256: '1'.repeat(64) } }, { ...f.plan, phase: 'legacy-baseline' }])
    await assert.rejects(prepareUniversalPolicy(plan, f.options), failCode('universal_policy_invalid'));
  let accessed = false;
  const getter = { ...f.plan }; Object.defineProperty(getter, 'human', { enumerable: true, get() { accessed = true; throw new Error('private sentinel'); } });
  await assert.rejects(prepareUniversalPolicy(getter, f.options), failCode('universal_policy_invalid')); assert.equal(accessed, false);
  await assert.rejects(prepareUniversalPolicy({ ...f.plan, human: { issuer, source: 'relative.json' } }, f.options), failCode('universal_policy_path_invalid'));
});

test('features update CAS is closed and bound to the exact public policy and encrypted witness', async t => {
  const f = await fixture(t), expected = 'a'.repeat(64), plan = { ...f.plan, expectedCurrentPreparednessSha256: expected };
  const handle = await prepared(t, f, plan), receipt = publicUniversalPolicy(handle);
  assert.equal(receipt.expectedCurrentPreparednessSha256, expected);
  assert.notEqual(receipt.policyDigest, publicUniversalPolicy(await prepared(t, f)).policyDigest);
  for (const value of [null, true, 1, '', 'a'.repeat(63), 'A'.repeat(64)])
    await assert.rejects(prepareUniversalPolicy({ ...plan, expectedCurrentPreparednessSha256: value }, f.options), failCode('universal_policy_invalid'));
  await assert.rejects(prepareUniversalPolicy({ ...baselinePlan(), expectedCurrentPreparednessSha256: expected }, f.options), failCode('universal_policy_invalid'));
  let accessed = false;
  const getter = { ...f.plan };
  Object.defineProperty(getter, 'expectedCurrentPreparednessSha256', { enumerable: true, get() { accessed = true; return expected; } });
  await assert.rejects(prepareUniversalPolicy(getter, f.options), failCode('universal_policy_invalid')); assert.equal(accessed, false);
  const key = randomBytes(32), keyId = 'fixture-update-custody', sealed = await sealUniversalPolicy(handle, { key, keyId });
  const restored = await restoreUniversalPolicy(plan, sealed.packet, f.options, { key, keyId, expectedWitnessId: sealed.witnessId });
  f.beforeRemove.push(() => disposeUniversalPolicy(restored));
  assert.deepEqual(publicUniversalPolicy(restored), receipt);
  await assert.rejects(restoreUniversalPolicy({ ...plan, expectedCurrentPreparednessSha256: 'b'.repeat(64) }, sealed.packet, f.options,
    { key, keyId, expectedWitnessId: sealed.witnessId }), failCode('universal_policy_witness_mismatch'));
});

test('private-only rotation leaves public digest unchanged but retires the exact prior witness', async t => {
  const f = await fixture(t), handle = await prepared(t, f), before = publicUniversalPolicy(handle).policyDigest;
  f.human.clients[0].clientSecret = random(); f.human.cookieKeys[0] = random(); f.human.artifactKey = random();
  await writeFile(f.humanSource, JSON.stringify(f.human));
  const next = await prepared(t, f); assert.equal(publicUniversalPolicy(next).policyDigest, before);
  await assert.rejects(assertUniversalPolicyCurrent(handle), failCode('universal_policy_file_changed'));
  assert.throws(() => publicUniversalPolicy(handle), failCode('universal_policy_handle_invalid'));
});

test('encrypted witness restores a separate process-local handle without publishing a secret fingerprint', async t => {
  const f = await fixture(t), handle = await prepared(t, f), key = randomBytes(32), keyId = 'operator-fixture-key';
  const sealed = await sealUniversalPolicy(handle, { key, keyId }); disposeUniversalPolicy(handle);
  const parsed = JSON.parse(sealed.packet);
  assert.equal(parsed.witnessId, sealed.witnessId); assert.match(sealed.witnessId, /^[A-Za-z0-9_-]{32}$/u);
  assert.equal(Object.hasOwn(parsed, 'files'), false); assert.equal(Object.hasOwn(parsed, 'bytesDigest'), false);
  for (const value of [f.human.artifactKey, f.human.clients[0].clientSecret, sha(await readFile(f.humanSource))]) assert.equal(sealed.packet.includes(value), false);
  const restored = await restoreUniversalPolicy(f.plan, sealed.packet, f.options, { key, keyId, expectedWitnessId: sealed.witnessId });
  t.after(() => disposeUniversalPolicy(restored)); assert.equal((await assertUniversalPolicyCurrent(restored)).policyDigest, sealed.policyDigest);
  const repeated = await sealUniversalPolicy(restored, { key, keyId }); assert.notEqual(repeated.witnessId, sealed.witnessId); assert.equal(repeated.policyDigest, sealed.policyDigest);
});

test('sealed approval rejects same-public-policy secret changes and byte-identical file replacement', async t => {
  const f = await fixture(t), handle = await prepared(t, f), key = randomBytes(32), keyId = 'operator-fixture-key';
  const sealed = await sealUniversalPolicy(handle, { key, keyId }), keyInput = { key, keyId, expectedWitnessId: sealed.witnessId };
  f.human.cookieKeys[0] = random(); await writeFile(f.humanSource, JSON.stringify(f.human));
  const fresh = await prepared(t, f); assert.equal(publicUniversalPolicy(fresh).policyDigest, sealed.policyDigest);
  await assert.rejects(restoreUniversalPolicy(f.plan, sealed.packet, f.options, keyInput), failCode('universal_policy_witness_mismatch'));
  const before = await sealUniversalPolicy(fresh, { key, keyId }), replacement = path.join(f.root, 'replacement.json');
  await writeFile(replacement, await readFile(f.humanSource), { mode: 0o600 }); await rename(f.humanSource, path.join(f.root, 'retired.json')); await rename(replacement, f.humanSource);
  await assert.rejects(restoreUniversalPolicy(f.plan, before.packet, f.options, { ...keyInput, expectedWitnessId: before.witnessId }), failCode('universal_policy_witness_mismatch'));
});

test('wrong custody key, changed packet/AAD, wrong reviewed witness ID and unknown fields fail with one safe error', async t => {
  const f = await fixture(t), handle = await prepared(t, f), key = randomBytes(32), keyId = 'operator-fixture-key';
  const sealed = await sealUniversalPolicy(handle, { key, keyId }), inputs = { key, keyId, expectedWitnessId: sealed.witnessId };
  await assert.rejects(restoreUniversalPolicy(f.plan, sealed.packet, f.options, { ...inputs, key: randomBytes(32) }), failCode('universal_policy_witness_invalid'));
  await assert.rejects(restoreUniversalPolicy(f.plan, sealed.packet, f.options, { ...inputs, expectedWitnessId: randomBytes(24).toString('base64url') }), failCode('universal_policy_witness_invalid'));
  for (const change of [value => ({ ...value, keyId: 'foreign-key' }), value => ({ ...value, policyDigest: '9'.repeat(64) }),
    value => ({ ...value, ciphertext: (value.ciphertext[0] === 'a' ? 'b' : 'a') + value.ciphertext.slice(1) }), value => ({ ...value, extra: 'inert' })])
    await assert.rejects(restoreUniversalPolicy(f.plan, Buffer.from(JSON.stringify(change(JSON.parse(sealed.packet)))), f.options, inputs), failCode('universal_policy_witness_invalid'));
  let accessed = false;
  const getter = { keyId }; Object.defineProperty(getter, 'key', { enumerable: true, get() { accessed = true; return key; } });
  await assert.rejects(sealUniversalPolicy(handle, getter), failCode('universal_policy_witness_invalid')); assert.equal(accessed, false);
});

test('a fresh Node process restores the exact encrypted approval with a private fixture custody file', async t => {
  const f = await fixture(t), handle = await prepared(t, f), key = randomBytes(32), keyId = 'operator-fixture-key';
  const sealed = await sealUniversalPolicy(handle, { key, keyId }); disposeUniversalPolicy(handle);
  const packetPath = path.join(f.root, 'encrypted-witness.json'), keyPath = path.join(f.root, 'private-custody.bin'), requestPath = path.join(f.root, 'request.json'), scriptPath = path.join(f.root, 'child.mjs');
  await writeFile(packetPath, sealed.packet, { mode: 0o600 }); await writeFile(keyPath, key, { mode: 0o600 });
  await writeFile(requestPath, JSON.stringify({ plan: f.plan, options: f.options, keyId, expectedWitnessId: sealed.witnessId }), { mode: 0o600 });
  const source = `import {readFile} from 'node:fs/promises';
import {restoreUniversalPolicy,assertUniversalPolicyCurrent,disposeUniversalPolicy} from ${JSON.stringify(import.meta.resolve('./universal-policy.mjs'))};
let key,handle;
try { const request=JSON.parse(await readFile(process.argv[2],'utf8'));key=await readFile(process.argv[3]);
handle=await restoreUniversalPolicy(request.plan,await readFile(process.argv[4]),request.options,{key,keyId:request.keyId,expectedWitnessId:request.expectedWitnessId});
const receipt=await assertUniversalPolicyCurrent(handle);process.stdout.write(JSON.stringify({ok:true,fixtureOnly:receipt.fixtureOnly,policyDigest:receipt.policyDigest}));
} catch {process.stdout.write(JSON.stringify({ok:false,code:'fixture_restore_failed'}));process.exitCode=1;}
finally {key?.fill(0);if(handle)disposeUniversalPolicy(handle);}`;
  await writeFile(scriptPath, source);
  const result = await promisify(execFile)(process.execPath, [scriptPath, requestPath, keyPath, packetPath], { timeout: 10000, maxBuffer: 4096, windowsHide: true });
  const observed = JSON.parse(result.stdout); assert.equal(observed.ok, true); assert.equal(observed.fixtureOnly, true); assert.equal(observed.policyDigest, sealed.policyDigest);
});

test('replacement with identical bytes, hardlink aliases and symlink source do not inherit file authority', async t => {
  const f = await fixture(t), handle = await prepared(t, f), bytes = await readFile(f.humanSource), replacement = path.join(f.root, 'replacement.json');
  await writeFile(replacement, bytes, { mode: 0o600 }); await rename(f.humanSource, path.join(f.root, 'former.json')); await rename(replacement, f.humanSource);
  await assert.rejects(assertUniversalPolicyCurrent(handle), failCode('universal_policy_file_changed'));
  const alias = path.join(f.root, 'hardlink.json'); await link(f.humanSource, alias);
  await assert.rejects(prepareUniversalPolicy(f.plan, f.options), failCode('universal_policy_file_invalid')); await rm(alias);
  await t.test('file symlink cannot carry private file authority', async child => {
    const symlinkPath = path.join(f.root, 'symlink.json');
    try { await symlink(f.humanSource, symlinkPath, 'file'); } catch (error) { if (process.platform === 'win32' && error.code === 'EPERM') { child.skip('Windows file symlink privilege unavailable'); return; } throw error; }
    await assert.rejects(prepareUniversalPolicy({ ...f.plan, human: { issuer, source: symlinkPath } }, f.options), failCode('universal_policy_file_invalid'));
  });
});

test('ancestor junction/symlink and escaped fixture root are rejected, not followed', async t => {
  const f = await fixture(t), actual = path.join(f.root, 'actual'), shortcut = path.join(f.root, 'shortcut');
  await mkdir(actual); await writeFile(path.join(actual, 'private.json'), JSON.stringify(f.human), { mode: 0o600 });
  await symlink(actual, shortcut, process.platform === 'win32' ? 'junction' : 'dir');
  await assert.rejects(prepareUniversalPolicy({ ...f.plan, human: { issuer, source: path.join(shortcut, 'private.json') } }, f.options), failCode('universal_policy_file_invalid'));
  await assert.rejects(prepareUniversalPolicy({ ...f.plan, human: { issuer, source: path.join(path.dirname(f.root), 'outside.json') } }, f.options), failCode('universal_policy_fixture_invalid'));
});

test('malformed, oversized, duplicate JSON, invalid keys and foreign issuer never expose parser or key details', async t => {
  const f = await fixture(t);
  for (const bytes of [Buffer.alloc(65537), Buffer.from([0xff]), Buffer.from('{"private sentinel":'), Buffer.from('[]'),
    Buffer.from('{"clients":[],"clients":[]}')]) {
    await writeFile(f.humanSource, bytes); await assert.rejects(prepareUniversalPolicy(f.plan, f.options), error =>
      ['universal_policy_file_invalid', 'universal_policy_invalid'].includes(error.code) && error.message === error.code && !error.cause);
  }
  for (const value of [{ ...f.human, password: 'must-be-rejected' }, { ...f.human, artifactKey: 'short' }, { ...f.human, clients: [] }]) {
    await writeFile(f.humanSource, JSON.stringify(value)); await assert.rejects(prepareUniversalPolicy(f.plan, f.options), error =>
      ['universal_policy_invalid', 'universal_policy_human_invalid'].includes(error.code) && error.message === error.code);
  }
  await writeFile(f.humanSource, JSON.stringify(f.human));
  await assert.rejects(prepareUniversalPolicy({ ...f.plan, human: { issuer: 'https://foreign.fixture.invalid/human-identity', source: f.humanSource } }, f.options), failCode('universal_policy_human_invalid'));
});

test('valid private base64url with a token-looking prefix follows the actual host profile rather than descriptor secret detection', async t => {
  const f = await fixture(t); f.human.cookieKeys[0] = 'sk-' + 'A'.repeat(40); f.human.clients[0].clientSecret = 'sk-' + 'B'.repeat(40);
  await writeFile(f.humanSource, JSON.stringify(f.human));
  const handle = await prepared(t, f); assert.equal(publicUniversalPolicy(handle).human.configured, true);
});

test('reviews need both exact byte approval and validated typed host pins; public-read does not become managed rights', async t => {
  const f = await fixture(t);
  await assert.rejects(prepareUniversalPolicy({ ...f.plan, reviews: { ...f.plan.reviews, sha256: '0'.repeat(64) } }, f.options), failCode('universal_policy_reviews_hash_mismatch'));
  for (const config of [{ ...f.reviews, script: 'inert' }, { ...f.reviews, bindings: [{ ...f.reviews.bindings[0], mode: 'managed' }] },
    { ...f.reviews, bindings: [{ ...f.reviews.bindings[0], providerRef: { ...f.reviews.bindings[0].providerRef, digest: '9'.repeat(64) } }] }]) {
    const bytes = JSON.stringify(config); await writeFile(f.reviewsSource, bytes);
    await assert.rejects(prepareUniversalPolicy({ ...f.plan, reviews: { source: f.reviewsSource, sha256: sha(bytes) } }, f.options), failCode('universal_policy_reviews_invalid'));
  }
});

test('delta preserves inherited values, data volume, labels, networks and limits; second application adds no duplicates', async t => {
  const f = await fixture(t), handle = await prepared(t, f), before = original(), originalBytes = JSON.stringify(before);
  const base = createConfig(before, 'sha256:' + 'a'.repeat(64), '1'.repeat(20), '2'.repeat(40));
  const next = applyUniversalPolicy(base, handle);
  assert.equal(JSON.stringify(before), originalBytes); assert.deepEqual(next.Env.slice(0, base.Env.length), base.Env);
  assert.deepEqual(next.Labels, base.Labels); assert.deepEqual(next.NetworkingConfig, base.NetworkingConfig);
  const stripped = structuredClone(next); stripped.Env = stripped.Env.slice(0, base.Env.length); stripped.HostConfig.Mounts = stripped.HostConfig.Mounts.slice(0, base.HostConfig.Mounts.length);
  assert.equal(preservationHash(stripped), preservationHash(base));
  assert.deepEqual(applyUniversalPolicy(next, handle), next);
  assert.deepEqual(next.HostConfig.Mounts.slice(-2).map(value => ({ target: value.Target, readonly: value.ReadOnly, type: value.Type })),
    [{ target: humanPrivateTarget, readonly: true, type: 'bind' }, { target: reviewsTarget, readonly: true, type: 'bind' }]);
});

test('closed delta refuses conflicting or duplicated env and any shadowing mount, including ancestors and image volumes', async t => {
  const f = await fixture(t), handle = await prepared(t, f), base = createConfig(original(), 'sha256:' + 'a'.repeat(64), '1'.repeat(20));
  for (const addition of [['SOTY_HUMAN_IDENTITY_ENABLED=0'], ['SOTY_HUMAN_IDENTITY_ENABLED'], ['SOTY_HUMAN_IDENTITY_ENABLED=1', 'SOTY_HUMAN_IDENTITY_ENABLED=1'], ['SOTY_HUMAN_IDENTITY_KEYS_FILE=/foreign']])
    assert.throws(() => applyUniversalPolicy({ ...base, Env: base.Env.concat(addition) }, handle), failCode('universal_policy_preexisting_configuration'));
  for (const mount of [{ Type: 'bind', Source: f.humanSource, Target: humanPrivateTarget, ReadOnly: false }, { Type: 'volume', Source: 'foreign', Target: '/run' },
    { Type: 'bind', Source: '/foreign', Target: '/run/secrets', ReadOnly: true }, { Type: 'bind', Source: '/foreign', Target: '/other/../run/secrets', ReadOnly: true }])
    assert.throws(() => applyUniversalPolicy({ ...base, HostConfig: { ...base.HostConfig, Mounts: [...base.HostConfig.Mounts, mount] } }, handle), failCode('universal_policy_preexisting_configuration'));
  assert.throws(() => applyUniversalPolicy({ ...base, Volumes: { ...base.Volumes, '/run': {} } }, handle), failCode('universal_policy_preexisting_configuration'));
  assert.throws(() => applyUniversalPolicy({ ...base, HostConfig: { ...base.HostConfig, Binds: [...base.HostConfig.Binds, '/foreign:/run/secrets:ro'] } }, handle), failCode('universal_policy_preexisting_configuration'));
});

test('image modes enforce legacy v5 baseline before feature writes; copied container or wrong/future reader cannot authorize it', async t => {
  const f = await fixture(t), baseline = await prepared(t, f, baselinePlan()), features = await prepared(t, f);
  assert.equal(assertUniversalImagePrerequisites(baseline, { candidateImage: image('1'), originalImage: image('0', reader3, 'b') }).phase, 'legacy-baseline');
  assert.equal(assertUniversalImagePrerequisites(features, { candidateImage: image('0'), originalImage: image('1', currentStorageReaders, 'b') }).phase, 'features');
  assert.throws(() => assertUniversalImagePrerequisites(features, { candidateImage: image('0'), originalImage: image('1', reader3, 'b') }), failCode('universal_policy_reader_baseline_required'));
  assert.throws(() => assertUniversalImagePrerequisites(features, { candidateImage: image('1'), originalImage: image('1', currentStorageReaders, 'b') }), failCode('universal_policy_image_mode_mismatch'));
  assert.throws(() => assertUniversalImagePrerequisites(baseline, { candidateImage: image(undefined), originalImage: image('0', reader3, 'b') }), failCode('universal_policy_image_mode_mismatch'));
  assert.throws(() => assertUniversalImagePrerequisites(features, { candidateImage: image('0', reader3), originalImage: image('1', currentStorageReaders, 'b') }), failCode('universal_policy_reader_baseline_required'));
  const future = JSON.parse(currentStorageReaders); future.version = 8;
  assert.throws(() => assertUniversalImagePrerequisites(features, { candidateImage: image('0', JSON.stringify(future)), originalImage: image('1', currentStorageReaders, 'b') }), failCode('storage_reader_unknown'));
});

test('baseline ignores inherited enablement/private paths and adds only the explicit private operator flag', async t => {
  const f = await fixture(t), handle = await prepared(t, f, baselinePlan()), config = createConfig(original(), 'sha256:' + 'a'.repeat(64), '1'.repeat(20));
  const applied = applyUniversalPolicy(config, handle);
  assert.deepEqual(applied.Env, [...config.Env, 'SOTY_UNIVERSAL_OPERATOR_ENABLED=1']);
  assert.deepEqual(applied.HostConfig, config.HostConfig);
  const neverRead = new Proxy({}, { get() { throw new Error('private sentinel'); } });
  assert.deepEqual(loadUniversalConfiguration(neverRead, { legacyMode: true }), {});
  const dto = captureUniversalPreparedness({ compiledLegacyMode: true, universalConfigured: false, reviewsConfigured: false,
    humanProfile: null, humanHttpEnabled: false });
  assert.equal(assertUniversalPreparedness(handle, dto, { allowFixture: true }).ok, true);
  assert.throws(() => assertUniversalPreparedness(handle, dto), failCode('universal_policy_fixture_not_production'));
});

test('actual host factories initialize feature stores/HTTP; storageReady alone and partial/wrong issuer or client readiness are rejected', async t => {
  const f = await fixture(t), handle = await prepared(t, f), dist = path.join(f.root, 'dist'), data = path.join(f.root, 'data'); await mkdir(dist); await writeFile(path.join(dist, 'index.html'), '<!doctype html><title>Fixture</title>');
  const loaded = loadUniversalConfiguration({ SOTY_UNIVERSAL_APPS_ENABLED: 'true', SOTY_HUMAN_IDENTITY_ENABLED: '1',
    SOTY_HUMAN_IDENTITY_ISSUER: issuer, SOTY_HUMAN_IDENTITY_KEYS_FILE: f.humanSource, SOTY_REVIEWS_BINDINGS_FILE: f.reviewsSource });
  const profile = createHumanIdentityHostProfile(loaded.humanIdentity, { shellOrigins: [origin] });
  const app = createHttpApp(dist, { dataDir: data, connectOrigins: [origin], appHosting: {}, appOriginTemplate: 'https://{appId}.apps.fixture.invalid',
    namedAppZone: '', discoveryOrigin: '', universalAppsEnabled: true, ...loaded });
  f.beforeRemove.push(() => app.locals.closeServices());
  const dto = runtime(f, app, profile); assert.equal(assertUniversalPreparedness(handle, dto, { allowFixture: true }).ok, true);
  const format = await readStorageFormat(data); assert.equal(format.schema, 'soty.storage-format.v7'); assert.equal(format.humanIdentity, 1);
  assert.equal(format.connect, 5);
  const connectDb = new DatabaseSync(path.join(data, 'connect', 'accounts.sqlite'), { readOnly: true });
  try {
    assert.equal(connectDb.prepare("SELECT value FROM connect_meta WHERE key='schema_version'").get().value, '5');
    assert.equal(connectDb.prepare("SELECT value FROM connect_meta WHERE key='min_reader'").get().value, '2');
  } finally { connectDb.close(); }
  assertStorageCompatible(image('1'), format); assert.throws(() => assertStorageCompatible(image('0', reader3), format), failCode('storage_reader_incompatible'));
  assert.throws(() => assertUniversalPreparedness(handle, { ok: true, storageReady: true }, { allowFixture: true }), failCode('universal_policy_invalid'));
  for (const dtoChange of [{ ...dto, universalConfigured: false }, { ...dto, reviewsConfigured: false }, { ...dto, humanHttpEnabled: false },
    { ...dto, human: { ...dto.human, issuer: 'https://foreign.fixture.invalid/human-identity' } },
    { ...dto, human: { ...dto.human, clientsDigest: '9'.repeat(64) } }, { ...dto, reviews: { ...dto.reviews, configurationDigest: '9'.repeat(64) } }])
    assert.throws(() => assertUniversalPreparedness(handle, dtoChange, { allowFixture: true }), failCode('universal_policy_runtime_mismatch'));
});

test('fixture witnesses are not production, disposed/later callbacks cannot authorize and disabled feature config is closed', async t => {
  const f = await fixture(t), handle = await prepared(t, f, emptyFeatures());
  const dto = captureUniversalPreparedness({ compiledLegacyMode: false, universalConfigured: true, reviewsConfigured: true, humanProfile: null, humanHttpEnabled: false });
  assert.throws(() => assertUniversalPreparedness(handle, dto), failCode('universal_policy_fixture_not_production'));
  assert.equal(assertUniversalPreparedness(handle, dto, { allowFixture: true }).ok, true);
  const waiting = assertUniversalPolicyCurrent(await prepared(t, f));
  // A separate live filesystem witness is deliberately disposed while its read is pending.
  const pendingHandle = await prepared(t, f), pending = assertUniversalPolicyCurrent(pendingHandle); disposeUniversalPolicy(pendingHandle);
  await assert.rejects(pending, error => ['universal_policy_file_changed', 'universal_policy_handle_invalid'].includes(error.code)); await waiting;
  disposeUniversalPolicy(handle); assert.throws(() => applyUniversalPolicy({ HostConfig: {} }, handle), failCode('universal_policy_handle_invalid'));
});

test('production owner/mode gates and Windows fixture limitation are explicit', async t => {
  const f = await fixture(t);
  if (process.platform !== 'linux') {
    await assert.rejects(prepareUniversalPolicy(baselinePlan(), { shellOrigins: [origin] }), failCode('universal_policy_posix_required')); return;
  }
  await chmod(f.humanSource, 0o644); await assert.rejects(prepareUniversalPolicy(f.plan, f.options), failCode('universal_policy_file_permissions')); await chmod(f.humanSource, 0o600);
  await chmod(f.root, 0o777); await assert.rejects(prepareUniversalPolicy(f.plan, f.options), failCode('universal_policy_file_permissions')); await chmod(f.root, 0o700);
  const wrongOwner = Number((await lstat(f.humanSource)).uid) + 1;
  await assert.rejects(prepareUniversalPolicy(f.plan, { ...f.options, ownerUid: wrongOwner }), failCode('universal_policy_file_permissions'));
});

test('renewal operational policy is explicit public measurement and requires a reader2 baseline before feature admission', async t => {
  const f = await fixture(t); f.human.clients[0].version = 2;
  f.human.renewal = { admissionEnabled: false, clientIds: [f.human.clients[0].id] };
  await writeFile(f.humanSource, JSON.stringify(f.human), { mode: 0o600 });
  const handle = await prepared(t, f), receipt = publicUniversalPolicy(handle);
  assert.equal(receipt.human.renewal.admissionEnabled, false); assert.equal(receipt.human.renewal.maximumSessionSeconds, 86400);
  const v1 = JSON.parse(currentStorageReaders); v1.readers.humanIdentity = [1];
  const v2 = JSON.parse(currentStorageReaders); v2.readers.humanIdentity = [1, 2];
  assert.throws(() => assertUniversalImagePrerequisites(handle, { candidateImage: image('0', JSON.stringify(v1)), originalImage: image('1', JSON.stringify(v1), 'b') }), failCode('universal_policy_reader_baseline_required'));
  assert.throws(() => assertUniversalImagePrerequisites(handle, { candidateImage: image('0', JSON.stringify(v2)), originalImage: image('1', JSON.stringify(v1), 'b') }), failCode('universal_policy_reader_baseline_required'));
  assert.equal(assertUniversalImagePrerequisites(handle, { candidateImage: image('0', JSON.stringify(v2)), originalImage: image('1', JSON.stringify(v2), 'b') }).phase, 'features');
  const human = createHumanIdentityHostProfile({ enabled: true, issuer, registryId: 'soty', environmentId: 'production', ...f.human,
    artifactKey: Buffer.from(f.human.artifactKey, 'base64url') }, { shellOrigins: [origin] });
  const dto = captureUniversalPreparedness({ compiledLegacyMode: false, universalConfigured: true, reviewsConfigured: true,
    humanHttpEnabled: true, humanProfile: human, reviewsConfiguration: f.reviews });
  assert.equal(assertUniversalPreparedness(handle, dto, { allowFixture: true }).ok, true);
  const changed = { ...dto, human: { ...dto.human, renewal: { ...dto.human.renewal, admissionEnabled: true } } };
  assert.throws(() => assertUniversalPreparedness(handle, changed, { allowFixture: true }), failCode('universal_policy_runtime_mismatch'));
});

test('selected Source policy binds private exact file, approved Human redirect and reader7 baseline', async t => {
  const f = await fixture(t); f.human.clients[0].redirectUri = 'https://alpha.fixture.invalid/api/embed/callback';
  await writeFile(f.humanSource, JSON.stringify(f.human));
  const selected = selectedConfiguration(f), source = path.join(f.root, 'selected.json');
  await writeFile(source, JSON.stringify(selected), { mode: 0o600 });
  const plan = { ...f.plan, selected: { source, migrationConfigured: true } }, handle = await prepared(t, f, plan);
  const receipt = publicUniversalPolicy(handle);
  assert.equal(receipt.selected.profileCount, 1); assert.equal(receipt.selected.migrationConfigured, true);
  assert.equal(receipt.mounts.at(-1).target, selectedTarget); assert.equal(receipt.mounts.at(-1).visibility, 'private');
  const config = applyUniversalPolicy(createConfig(original(), image('0').Id, 'c'.repeat(40)), handle);
  assert.ok(config.Env.includes('SOTY_SELECTED_EMBED_REGISTRY_FILE=' + selectedTarget));
  assert.ok(config.Env.includes('SOTY_SELECTED_EMBED_MIGRATION=1'));
  const loaded = loadUniversalConfiguration({ SOTY_HUMAN_IDENTITY_ENABLED: '1', SOTY_HUMAN_IDENTITY_ISSUER: issuer,
    SOTY_HUMAN_IDENTITY_KEYS_FILE: f.humanSource, SOTY_SELECTED_EMBED_REGISTRY_FILE: source, SOTY_SELECTED_EMBED_MIGRATION: '1' });
  const human = createHumanIdentityHostProfile(loaded.humanIdentity, { shellOrigins: [origin] });
  const dto = captureUniversalPreparedness({ compiledLegacyMode: false, universalConfigured: true, reviewsConfigured: true,
    humanProfile: human, humanHttpEnabled: true, reviewsConfiguration: f.reviews,
    selectedProfiles: loaded.scopedEmbedProfiles, selectedMigrationConfigured: loaded.allowScopedEmbedMigration });
  assert.equal(assertUniversalPreparedness(handle, dto, { allowFixture: true }).ok, true);
  assert.throws(() => assertUniversalPreparedness(handle, { ...dto, selected: { ...dto.selected, registryDigest: '0'.repeat(64) } },
    { allowFixture: true }), failCode('universal_policy_runtime_mismatch'));
  const old = JSON.parse(currentStorageReaders); old.readers.apps = old.readers.apps.filter(version => version !== 7);
  assert.throws(() => assertUniversalImagePrerequisites(handle, { candidateImage: image('0'), originalImage: image('1', JSON.stringify(old), 'b') }),
    failCode('universal_policy_reader_baseline_required'));
  assert.throws(() => assertUniversalImagePrerequisites(handle, { candidateImage: image('0', JSON.stringify(old)), originalImage: image('1', undefined, 'b') }),
    failCode('universal_policy_reader_baseline_required'));
  assert.equal(assertUniversalImagePrerequisites(handle, { candidateImage: image('0'), originalImage: image('1', undefined, 'b') }).phase, 'features');
  const publicText = JSON.stringify(receipt);
  assert.equal(publicText.includes(selected.profiles[0].resource.workspaceId), false);
  assert.equal(publicText.includes(sha(await readFile(source))), false);
  const key = randomBytes(32), sealed = await sealUniversalPolicy(handle, { key, keyId: 'selected-custody' });
  const restored = await restoreUniversalPolicy(plan, sealed.packet, f.options, { key, keyId: 'selected-custody', expectedWitnessId: sealed.witnessId });
  t.after(() => { try { disposeUniversalPolicy(restored); } catch {} });
  // Semantic JSON is unchanged; exact bytes/identity are still privately fenced.
  await writeFile(source, JSON.stringify(selected, null, 2));
  await assert.rejects(assertUniversalPolicyCurrent(restored), failCode('universal_policy_file_changed'));
});

test('selected HIVE callback follows its exact compiled Native HTTPS pin; embed substitution and unknown pins deny', async t => {
  const f = await fixture(t), source = path.join(f.root, 'hive-selected.json');
  const base = selectedConfiguration(f).profiles[0];
  const profile = { ...base, schema: 'soty.selected-human-embed.v2', sourceProfile: HIVE_SELECTED_SOURCE,
    resource: { registryId: 'soty', tenantId: 'owner.fixture', environmentId: 'production', appId: base.appId,
      resourceId: 'hive:selected', selection: { kind: 'hive.project.v1', nativeId: ' Project / Exact ', incarnationId: 'native-incarnation' } },
    nativeOrigin: 'https://hive.fixture.invalid' };
  const plan = { ...f.plan, selected: { source, migrationConfigured: true } };
  async function configured(value, redirect) {
    f.human.clients[0].redirectUri = redirect;
    await writeFile(f.humanSource, JSON.stringify(f.human));
    await writeFile(source, JSON.stringify({ schema: 'soty.selected-embed-registry.v1', profiles: [value] }), { mode: 0o600 });
  }
  const callback = profile.nativeOrigin + '/account/soty/callback';
  for (const pin of [HIVE_SELECTED_KERNEL_SOURCE, HIVE_SELECTED_SOURCE]) {
    await configured({ ...profile, sourceProfile: pin }, callback);
    const handle = await prepared(t, f, plan);
    assert.equal(publicUniversalPolicy(handle).selected.profileCount, 1);
  }
  for (const redirect of [profile.embedOrigin + '/api/embed/callback', profile.nativeOrigin + '/callback', 'https://foreign.fixture.invalid/account/soty/callback']) {
    await configured(profile, redirect);
    await assert.rejects(prepareUniversalPolicy(plan, f.options), failCode('universal_policy_selected_human_required'));
  }
  await configured({ ...profile, sourceProfile: { ...HIVE_SELECTED_SOURCE, digest: '0'.repeat(64) } }, callback);
  await assert.rejects(prepareUniversalPolicy(plan, f.options), failCode('universal_policy_selected_human_required'));
  // A loopback Native rehearsal does not become a production HTTP callback.
  await configured({ ...profile, nativeOrigin: 'http://127.0.0.1:4317' }, profile.embedOrigin + '/api/embed/callback');
  await assert.rejects(prepareUniversalPolicy(plan, f.options), failCode('universal_policy_selected_human_required'));
  await configured({ ...profile, nativeOrigin: 'http://127.0.0.1:4317' }, 'http://127.0.0.1:4317/account/soty/callback');
  await assert.rejects(prepareUniversalPolicy(plan, f.options), failCode('universal_policy_human_invalid'));
});

test('standard Source2 callback is the exact approved embed HTTPS route while Source1 retains its Native callback', async t => {
  const f = await fixture(t), source = path.join(f.root, 'standard-selected.json');
  const base = selectedConfiguration(f).profiles[0];
  const profile = { ...base, schema: 'soty.selected-human-embed.v2', sourceProfile: STANDARD_SELECTED_SOURCE_V2,
    resource: { registryId: 'soty', tenantId: 'owner.fixture', environmentId: 'production', appId: base.appId,
      resourceId: 'standard:selected', selection: { kind: 'soty.resource.v1', nativeId: ' Resource / Exact ', incarnationId: 'native-incarnation' } },
    nativeOrigin: 'https://native.fixture.invalid' };
  const plan = { ...f.plan, selected: { source, migrationConfigured: true } };
  async function configured(value, redirect) {
    f.human.clients[0].redirectUri = redirect;
    await writeFile(f.humanSource, JSON.stringify(f.human));
    await writeFile(source, JSON.stringify({ schema: 'soty.selected-embed-registry.v1', profiles: [value] }), { mode: 0o600 });
  }
  for (const [pin, redirect] of [[STANDARD_SELECTED_SOURCE, profile.nativeOrigin + '/soty/callback'],
    [STANDARD_SELECTED_SOURCE_V2, profile.embedOrigin + '/api/embed/callback']]) {
    await configured({ ...profile, sourceProfile: pin }, redirect);
    const handle = await prepared(t, f, plan);
    assert.equal(publicUniversalPolicy(handle).selected.profileCount, 1);
  }
  for (const redirect of [profile.nativeOrigin + '/api/embed/callback', profile.nativeOrigin + '/soty/callback',
    profile.embedOrigin + '/soty/callback', 'https://foreign.fixture.invalid/api/embed/callback']) {
    await configured(profile, redirect);
    await assert.rejects(prepareUniversalPolicy(plan, f.options), failCode('universal_policy_selected_human_required'));
  }
  await configured({ ...profile, sourceProfile: { ...STANDARD_SELECTED_SOURCE_V2, digest: '0'.repeat(64) } }, profile.embedOrigin + '/api/embed/callback');
  await assert.rejects(prepareUniversalPolicy(plan, f.options), failCode('universal_policy_selected_human_required'));
});

test('selected policy rejects foreign/missing Human clients and inherited unapproved registry', async t => {
  const f = await fixture(t), registry = selectedConfiguration(f), source = path.join(f.root, 'selected.json');
  await writeFile(source, JSON.stringify(registry), { mode: 0o600 });
  const plan = { ...f.plan, selected: { source, migrationConfigured: false } };
  await assert.rejects(prepareUniversalPolicy(plan, f.options), failCode('universal_policy_selected_human_required'));
  await assert.rejects(prepareUniversalPolicy({ ...plan, human: null }, f.options), failCode('universal_policy_selected_human_required'));
  await assert.rejects(prepareUniversalPolicy({ ...baselinePlan(), selected: plan.selected }, f.options), failCode('universal_policy_invalid'));
  const handle = await prepared(t, f);
  const inherited = createConfig(original(), image('0').Id, 'c'.repeat(40)); inherited.Env.push('SOTY_SELECTED_EMBED_REGISTRY_FILE=/unapproved/file');
  assert.throws(() => applyUniversalPolicy(inherited, handle), failCode('universal_policy_preexisting_configuration'));
});

test('reviewed installed Native consent permits explicit loopback ports and rejects external HTTP', async t => {
  const f = await fixture(t); f.human.clients[0].redirectUri = 'https://alpha.fixture.invalid/api/embed/callback';
  await writeFile(f.humanSource, JSON.stringify(f.human));
  const registry = selectedConfiguration(f), source = path.join(f.root, 'local-native.json');
  const plan = { ...f.plan, selected: { source, migrationConfigured: true } };
  for (const nativeOrigin of ['http://localhost:43123', 'http://127.0.0.1:43123', 'http://[::1]:43123']) {
    registry.profiles[0].nativeOrigin = nativeOrigin; await writeFile(source, JSON.stringify(registry), { mode: 0o600 });
    const handle = await prepared(t, f, plan); assert.equal(publicUniversalPolicy(handle).selected.profileCount, 1);
  }
  registry.profiles[0].nativeOrigin = 'http://localhost:81'; await writeFile(source, JSON.stringify(registry));
  await assert.rejects(prepareUniversalPolicy(plan, f.options), failCode('universal_policy_invalid'));
  registry.profiles[0].nativeOrigin = 'http://native.fixture.invalid:43123'; await writeFile(source, JSON.stringify(registry));
  await assert.rejects(prepareUniversalPolicy(plan, f.options));
});

test('empty selected registry initializes Apps13/base7 while feature-off initializes Apps12/base6', async t => {
  const f = await fixture(t), source = path.join(f.root, 'selected-empty.json');
  await writeFile(source, JSON.stringify({ schema: 'soty.selected-embed-registry.v1', profiles: [] }), { mode: 0o600 });
  const plan = { ...emptyFeatures(), selected: { source, migrationConfigured: true } }, handle = await prepared(t, f, plan);
  const dist = path.join(f.root, 'dist'); await mkdir(dist); await writeFile(path.join(dist, 'index.html'), '<!doctype html>');
  const dataDir = path.join(f.root, 'feature-data');
  const app = createHttpApp(dist, { dataDir, connectOrigins: [origin], appHosting: {}, appOriginTemplate: 'https://{appId}.apps.fixture.invalid',
    namedAppZone: '', discoveryOrigin: '', universalAppsEnabled: true, allowScopedEmbedMigration: true });
  f.beforeRemove.push(() => app.locals.closeServices());
  assert.equal((await readStorageFormat(dataDir)).apps, 13); // fresh covers over preserved base7
  assert.equal(assertUniversalPreparedness(handle, app.locals.captureUniversalPreparedness(), { allowFixture: true }).ok, true);
  const offDir = path.join(f.root, 'baseline-data');
  const off = createHttpApp(dist, { dataDir: offDir, connectOrigins: [origin], appHosting: {}, appOriginTemplate: 'https://{appId}.apps.fixture.invalid',
    namedAppZone: '', discoveryOrigin: '', universalAppsEnabled: false, allowScopedEmbedMigration: true, scopedEmbedProfiles: [{}] });
  f.beforeRemove.push(() => off.locals.closeServices());
  assert.equal((await readStorageFormat(offDir)).apps, 12); // fresh covers over preserved base6
  assert.equal(off.locals.captureUniversalPreparedness().selected, undefined);
  const disabledPlan = { ...emptyFeatures(), selected: { source, migrationConfigured: false } };
  const disabled = await prepared(t, f, disabledPlan);
  const loaded = loadUniversalConfiguration({ SOTY_SELECTED_EMBED_REGISTRY_FILE: source, SOTY_SELECTED_EMBED_MIGRATION: '0' });
  const noMigration = createHttpApp(dist, { ...loaded, dataDir: path.join(f.root, 'disabled-registry-data'), connectOrigins: [origin],
    appHosting: {}, appOriginTemplate: 'https://{appId}.apps.fixture.invalid', namedAppZone: '', discoveryOrigin: '' });
  f.beforeRemove.push(() => noMigration.locals.closeServices());
  assert.equal(noMigration.locals.captureUniversalPreparedness().selected.profileCount, 0);
  assert.equal(noMigration.locals.captureUniversalPreparedness().selected.migrationConfigured, false);
  assert.equal(assertUniversalPreparedness(disabled, noMigration.locals.captureUniversalPreparedness(), { allowFixture: true }).ok, true);
});

test('resource policy proposal: v1 keeps exact selected DTO and environment byte behavior', async t => {
  const f = await fixture(t), registry = selectedConfiguration(f), source = path.join(f.root, 'v1-resource-control.json');
  f.human.clients[0].redirectUri = registry.profiles[0].embedOrigin + '/api/embed/callback';
  await writeFile(f.humanSource, JSON.stringify(f.human));
  await writeFile(source, JSON.stringify(registry), { mode: 0o600 });
  const handle = await prepared(t, f, { ...f.plan, selected: { source, migrationConfigured: true } });
  const receipt = publicUniversalPolicy(handle), selected = receipt.selected;
  assert.deepEqual(Object.keys(selected).sort(), ['configured', 'migrationConfigured', 'profileCount', 'registryDigest']);
  const env = applyUniversalPolicy(createConfig(original(), image('0').Id, 'd'.repeat(40)), handle).Env;
  assert.equal(env.some(value => value.startsWith('SOTY_SELECTED_RESOURCE_MIGRATION=')), false);
  assert.equal(env.filter(value => value.startsWith('SOTY_SELECTED_EMBED_MIGRATION='))[0], 'SOTY_SELECTED_EMBED_MIGRATION=1');
  const reader7 = JSON.parse(currentStorageReaders); reader7.readers.apps = reader7.readers.apps.filter(v => v !== 8);
  assert.equal(assertUniversalImagePrerequisites(handle, { candidateImage: image('0', JSON.stringify(reader7)),
    originalImage: image('1', JSON.stringify(reader7), 'b') }).phase, 'features');
});

test('resource policy proposal: v2 derives explicit resource flag and requires actual reader8 on both images', async t => {
  for (const migrationConfigured of [false, true]) {
    const f = await fixture(t), base = selectedConfiguration(f).profiles[0], source = path.join(f.root, 'v2-resource-control.json');
    const profile = { ...base, schema: 'soty.selected-human-embed.v2', sourceProfile: HIVE_SELECTED_SOURCE,
      resource: { registryId: 'soty', tenantId: 'owner.fixture', environmentId: 'production', appId: base.appId,
        resourceId: 'hive:selected', selection: { kind: 'hive.project.v1', nativeId: ' Project / Exact ', incarnationId: 'native-incarnation' } },
      nativeOrigin: 'https://hive.fixture.invalid' };
    f.human.clients[0].redirectUri = profile.nativeOrigin + '/account/soty/callback';
    await writeFile(f.humanSource, JSON.stringify(f.human));
    await writeFile(source, JSON.stringify({ schema: 'soty.selected-embed-registry.v1', profiles: [profile] }), { mode: 0o600 });
    const plan = { ...f.plan, selected: { source, migrationConfigured } }, handle = await prepared(t, f, plan);
    assert.equal(publicUniversalPolicy(handle).selected.resourceMigrationConfigured, migrationConfigured);
    const config = applyUniversalPolicy(createConfig(original(), image('0').Id, 'e'.repeat(40)), handle);
    assert.deepEqual(config.Env.filter(value => value.startsWith('SOTY_SELECTED_RESOURCE_MIGRATION=')),
      ['SOTY_SELECTED_RESOURCE_MIGRATION=' + (migrationConfigured ? '1' : '0')]);
    const loaded = loadUniversalConfiguration({ SOTY_HUMAN_IDENTITY_ENABLED: '1', SOTY_HUMAN_IDENTITY_ISSUER: issuer,
      SOTY_HUMAN_IDENTITY_KEYS_FILE: f.humanSource, SOTY_SELECTED_EMBED_REGISTRY_FILE: source,
      SOTY_SELECTED_EMBED_MIGRATION: migrationConfigured ? '1' : '0', SOTY_SELECTED_RESOURCE_MIGRATION: migrationConfigured ? '1' : '0' });
    assert.equal(loaded.allowSelectedResourceMigration === true, migrationConfigured);
    const human = createHumanIdentityHostProfile(loaded.humanIdentity, { shellOrigins: [origin] });
    const dto = captureUniversalPreparedness({ compiledLegacyMode: false, universalConfigured: true, reviewsConfigured: true,
      humanProfile: human, humanHttpEnabled: true, reviewsConfiguration: f.reviews, selectedProfiles: loaded.scopedEmbedProfiles,
      selectedMigrationConfigured: loaded.allowScopedEmbedMigration === true, selectedResourceMigrationConfigured: loaded.allowSelectedResourceMigration === true });
    assert.equal(assertUniversalPreparedness(handle, dto, { allowFixture: true }).ok, true);
    for (const resourceMigrationConfigured of [!migrationConfigured, undefined]) {
      const selected = { ...dto.selected }; if (resourceMigrationConfigured === undefined) delete selected.resourceMigrationConfigured;
      else selected.resourceMigrationConfigured = resourceMigrationConfigured;
      assert.throws(() => assertUniversalPreparedness(handle, { ...dto, selected }, { allowFixture: true }), failCode('universal_policy_runtime_mismatch'));
    }
    const reader7 = JSON.parse(currentStorageReaders); reader7.readers.apps = reader7.readers.apps.filter(v => v !== 8);
    for (const side of ['candidate', 'original']) assert.throws(() => assertUniversalImagePrerequisites(handle, {
      candidateImage: image('0', side === 'candidate' ? JSON.stringify(reader7) : currentStorageReaders),
      originalImage: image('1', side === 'original' ? JSON.stringify(reader7) : currentStorageReaders, 'b'),
    }), failCode('universal_policy_reader_baseline_required'));
    assert.equal(assertUniversalImagePrerequisites(handle, { candidateImage: image('0'), originalImage: image('1', currentStorageReaders, 'b') }).phase, 'features');
    for (const value of [migrationConfigured ? '0' : '1', 'true']) {
      const foreign = createConfig(original(), image('0').Id, 'f'.repeat(40)); foreign.Env.push('SOTY_SELECTED_RESOURCE_MIGRATION=' + value);
      assert.throws(() => applyUniversalPolicy(foreign, handle), failCode('universal_policy_preexisting_configuration'));
    }
    const duplicate = createConfig(original(), image('0').Id, 'f'.repeat(40));
    duplicate.Env.push('SOTY_SELECTED_RESOURCE_MIGRATION=' + (migrationConfigured ? '1' : '0'), 'SOTY_SELECTED_RESOURCE_MIGRATION=' + (migrationConfigured ? '1' : '0'));
    assert.throws(() => applyUniversalPolicy(duplicate, handle), failCode('universal_policy_preexisting_configuration'));
    await assert.rejects(prepareUniversalPolicy({ ...plan, selected: { ...plan.selected, resourceMigrationConfigured: true } }, f.options), failCode('universal_policy_invalid'));
    const rebound = { ...profile, schema: 'soty.selected-human-embed.v1' };
    await writeFile(source, JSON.stringify({ schema: 'soty.selected-embed-registry.v1', profiles: [rebound] }));
    await assert.rejects(assertUniversalPolicyCurrent(handle), failCode('universal_policy_file_changed'));
  }
});
