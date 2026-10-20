import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, rm, realpath } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, basename } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { fileURLToPath, pathToFileURL } from 'node:url';

const rootPath = fileURLToPath(new URL('../../', import.meta.url));
const [{ loadVoiceOrbConfiguration }, { createRootStartup }] = await Promise.all([
  import(pathToFileURL(join(rootPath, 'server/voice-orb-configuration.js')).href),
  import(pathToFileURL(join(rootPath, 'server/startup.js')).href),
]);
const profile = () => ({ schema: 'soty.voice-orb-installed.v1', maxDurationMs: 15000, profiles: [{
  tenantId: 'synthetic_own_tenant', accountId: 'synthetic_owner', deviceId: 'synthetic_device',
  apiKey: 'synthetic-private-openai-key-no-real-provider', projectId: 'proj_synthetic_own', authorizationId: 'synthetic_explicit_authorization',
  validUntil: '2099-01-01T00:00:00.000Z', maxStarts: 2,
}] });
const actor = { accountId: 'synthetic_owner', deviceId: 'synthetic_device' };
const intent = requestId => ({ actor, requestId, intentDigest: 'a'.repeat(64), maxDurationMs: 15000, signal: new AbortController().signal });
const unknown = { transportAttempted: true, providerCallId: 'rtc_synthetic_one', hangupAccepted: true,
  terminationVerified: false, accountingFinalized: false, responseUsage: [], reason: 'hangup_accepted' };
async function files(t) {
  const dir = await mkdtemp(join(tmpdir(), 'soty-own-voice-install-')), dataDir = join(dir, 'data'), dist = join(dir, 'dist'), file = join(dir, 'own-voice.json');
  await mkdir(dataDir); await mkdir(dist); await writeFile(join(dist, 'index.html'), '<!doctype html><title>Synthetic own voice</title>');
  const stoppers = [];
  t.after(async () => { const outcomes = await Promise.allSettled(stoppers.map(stop => stop()));
    const absolute = await realpath(dir); assert.equal(dirname(absolute), await realpath(tmpdir())); assert.ok(basename(absolute).startsWith('soty-own-voice-install-'));
    await rm(absolute, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
    for (const result of outcomes) if (result.status === 'rejected') throw result.reason;
  });
  return { dir, dataDir, dist, file, stoppers,
    env: { SOTY_VOICE_ORB_ENABLED: '1', SOTY_VOICE_ORB_CONFIG_FILE: file },
    save: value => writeFile(file, JSON.stringify(value), { mode: 0o600 }) };
}

test('actual process-entry voice loader defaults OFF and does not even inspect private filename', () => {
  const disabled = { SOTY_VOICE_ORB_ENABLED: '0' };
  Object.defineProperty(disabled, 'SOTY_VOICE_ORB_CONFIG_FILE', { get() { throw new Error('must_not_read_private_config'); } });
  assert.equal(loadVoiceOrbConfiguration(disabled).voiceOrb.enabled, false);
  assert.equal(loadVoiceOrbConfiguration({}).voiceOrb.enabled, false);
  for (const value of ['true', ' 1', '2']) assert.throws(() => loadVoiceOrbConfiguration({ SOTY_VOICE_ORB_ENABLED: value }), error => error.code === 'voice_orb_installation_invalid');
});

test('real installed own-account/profile binds private key/project/tenant without serializing credentials or making a provider call', async t => {
  const f = await files(t); await f.save(profile()); let network = 0;
  const config = loadVoiceOrbConfiguration(f.env, { dataDir: f.dataDir, fetchImpl: async () => { network++; throw new Error('provider_forbidden'); } }); f.stoppers.push(config.close);
  assert.equal(config.voiceOrb.enabled, true); assert.equal(network, 0);
  assert.equal(JSON.stringify(config).includes(profile().profiles[0].apiKey), false);
  assert.equal(config.voiceOrb.quotaStatus({ actor }).remainingStarts, 2);
  assert.equal(config.voiceOrb.quotaStatus({ actor: { ...actor, accountId: 'foreign' } }).allowed, false);
  await assert.rejects(config.voiceOrb.admitQuota({ ...intent('foreign_request'), actor: { ...actor, deviceId: 'foreign_device' } }));
  assert.equal(network, 0);
});

test('actual SQLite session quota and paid-start dedupe survive close/reopen; unknown usage never refunds starts', async t => {
  const f = await files(t); await f.save(profile());
  let config = loadVoiceOrbConfiguration(f.env, { dataDir: f.dataDir });
  const first = await config.voiceOrb.admitQuota(intent('durable_request_1')); assert.equal(await first.claimTransport(), true);
  assert.equal(await first.claimTransport(), false); await first.recordOutcome(unknown); config.close();
  config = loadVoiceOrbConfiguration(f.env, { dataDir: f.dataDir }); f.stoppers.push(config.close);
  const replay = await config.voiceOrb.admitQuota(intent('durable_request_1')); assert.equal(await replay.claimTransport(), false);
  await assert.rejects(config.voiceOrb.admitQuota({ ...intent('durable_request_1'), intentDigest: 'b'.repeat(64) }));
  const second = await config.voiceOrb.admitQuota(intent('durable_request_2')); assert.equal(await second.claimTransport(), true);
  await second.recordOutcome({ ...unknown, providerCallId: 'rtc_synthetic_two', hangupAccepted: false, reason: 'hangup_unknown' });
  await assert.rejects(config.voiceOrb.admitQuota(intent('durable_request_3')));
  const status = config.voiceOrb.quotaStatus({ actor }); assert.equal(status.reservedStarts, 2); assert.equal(status.remainingStarts, 0); assert.equal(status.usageFinalized, false);
});

test('configuration rejects model/backend/permission overrides and sharing an OpenAI key or project across user accounts', async t => {
  const f = await files(t);
  for (const change of [value => ({ ...value, model: 'other' }), value => ({ ...value, noteCreation: true }), value => ({ ...value, schema: 'future' }),
    value => ({ ...value, profiles: [{ ...value.profiles[0], budgetDollars: 1 }] }),
    value => ({ ...value, profiles: [value.profiles[0], { ...value.profiles[0], accountId: 'foreign' }] }),
    value => ({ ...value, profiles: [value.profiles[0], { ...value.profiles[0], accountId: 'foreign', apiKey: 'different-synthetic-private-key-no-provider' }] }),
    value => ({ ...value, profiles: [{ ...value.profiles[0], maxStarts: 0 }] }),
    value => ({ ...value, profiles: [{ ...value.profiles[0], projectId: 'https://foreign.invalid' }] })]) {
    await f.save(change(profile())); assert.throws(() => loadVoiceOrbConfiguration(f.env, { dataDir: f.dataDir }), error => error.code === 'voice_orb_installation_invalid' && error.message === error.code && !error.cause);
  }
});

test('real startup entry loads enabled own voice profile separately from existing GLM gateway and owns its cleanup without listening', async t => {
  const f = await files(t); await f.save(profile());
  const glmFile = join(f.dir, 'synthetic-glm.json');
  await writeFile(glmFile, JSON.stringify({ schema: 'soty.personal-agent-installed.v1', accountIds: [actor.accountId],
    gateway: { origin: 'https://synthetic-own-glm.invalid', billingTenant: 'synthetic_glm_tenant',
      inferenceBearer: 'synthetic-gonka-inference-no-real-provider', managementSession: 'synthetic_private_glm_session' } }), { mode: 0o600 });
  const root = await createRootStartup({ env: { ...f.env, HOST: '127.0.0.1', PORT: '0', DATA_DIR: f.dataDir, SOTY_DIST_DIR: f.dist,
    SOTY_PERSONAL_AGENT_ENABLED: '1', SOTY_PERSONAL_AGENT_CONFIG_FILE: glmFile } });
  f.stoppers.push(() => root.close()); assert.equal(root.server.listening, false);
  assert.equal(root.app.locals.personalAgent.status().configured, true); assert.equal(root.app.locals.voiceOrb.status().configured, true);
  assert.equal(root.app.locals.voiceOrb.status().voiceModel, 'gpt-realtime-2.1'); assert.equal(root.app.locals.voiceOrb.status().brainModel, 'zai-org/GLM-5.3-Flash');
  assert.equal(root.app.locals.voiceOrb.status().paidActivationVerified, false);
  await root.close(); assert.equal(root.app.locals.voiceOrb.status().configured, false);
  const reloaded = loadVoiceOrbConfiguration(f.env, { dataDir: f.dataDir }); f.stoppers.push(reloaded.close);
  assert.equal(reloaded.voiceOrb.quotaStatus({ actor }).remainingStarts, 2);
});


test('failed replay lease cannot overwrite original attempted-but-unknown outcome after restart', async t => {
  const f = await files(t); await f.save(profile());
  let config = loadVoiceOrbConfiguration(f.env, { dataDir: f.dataDir });
  const original = await config.voiceOrb.admitQuota(intent('crashed_unknown_request')); assert.equal(await original.claimTransport(), true);
  config.close(); // The provider could have received POST; no outcome was recorded.
  config = loadVoiceOrbConfiguration(f.env, { dataDir: f.dataDir }); f.stoppers.push(config.close);
  const replay = await config.voiceOrb.admitQuota(intent('crashed_unknown_request')); assert.equal(await replay.claimTransport(), false);
  await assert.rejects(replay.recordOutcome({ ...unknown, transportAttempted: false, providerCallId: null }), error => error.code === 'voice_orb_denied');
  const db = new DatabaseSync(join(f.dataDir, 'voice-orb-quota.sqlite'), { readOnly: true });
  try { const rows = db.prepare('SELECT attempted,outcome_json FROM voice_orb_starts').all();
    assert.equal(rows.length, 1); assert.equal(rows[0].attempted, 1); assert.equal(rows[0].outcome_json, null);
  } finally { db.close(); }
  assert.equal(config.voiceOrb.quotaStatus({ actor }).reservedStarts, 1);
});
