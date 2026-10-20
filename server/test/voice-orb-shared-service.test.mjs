import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, rm, realpath } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, basename } from 'node:path';
import { createHash, generateKeyPairSync, randomUUID, sign } from 'node:crypto';
import { EventEmitter } from 'node:events';
import { Worker } from 'node:worker_threads';
import { DatabaseSync } from 'node:sqlite';
import { loadVoiceOrbConfiguration } from '../voice-orb-configuration.js';
import { createVoiceOrbQuota } from '../voice-orb-quota.js';
import { createVoiceOrbComposition } from '../voice-orb-composition.js';
import { createPersonalAgentComposition } from '../personal-agent-composition.js';
import { loadPersonalAgentConfiguration } from '../personal-agent-configuration.js';
import { createConnectService, digestArgs } from '../../modules/connect/server/index.mjs';

// All credentials/domains/SDP/provider events below are synthetic. fetchImpl and
// socketFactory are injected: this suite never contacts OpenAI or GLM.
const ORIGIN = 'https://shared-voice-service.invalid';
const actor = { accountId: 'synthetic_shared_account_a', deviceId: 'synthetic_shared_device_1' };
const service = () => ({ tenantId: 'synthetic_operator_billing', serviceId: 'synthetic_soty_voice_service',
  apiKey: 'synthetic-operator-openai-service-no-provider-key', projectId: 'proj_synthetic_operator_service',
  authorizationId: 'synthetic_operator_authorization_1', validUntil: '2099-01-01T00:00:00.000Z', maxStarts: 3, maxAccountStarts: 2 });
const document = () => ({ schema: 'soty.voice-orb-installed.v2', maxDurationMs: 15000, service: service(),
  admission: { policy: 'soty-authenticated-accounts', connectProjectId: 'soty' } });
const ownDocument = () => ({ schema: 'soty.voice-orb-installed.v1', maxDurationMs: 15000, profiles: [{
  ...actor, tenantId: 'synthetic_legacy_tenant', apiKey: 'synthetic-legacy-own-openai-no-provider-key', projectId: 'proj_synthetic_legacy',
  authorizationId: 'synthetic_legacy_authorization', validUntil: '2099-01-01T00:00:00.000Z', maxStarts: 2,
}] });
const intent = (requestId, owner = actor) => ({ actor: owner, requestId, intentDigest: 'a'.repeat(64), maxDurationMs: 15000, signal: new AbortController().signal });
const unknown = { transportAttempted: true, providerCallId: 'rtc_shared_synthetic', hangupAccepted: true,
  terminationVerified: false, accountingFinalized: false, responseUsage: [], reason: 'hangup_accepted' };
const good = value => { assert.equal(value.ok, true, value.error?.code); return value; };
const identity = () => { const pair = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { key: pair.privateKey, publicJwk: pair.publicKey.export({ format: 'jwk' }), encryptionPublicJwk: pair.publicKey.export({ format: 'jwk' }) }; };
async function files(t) {
  const dir = await mkdtemp(join(tmpdir(), 'soty-shared-voice-service-')), dataDir = join(dir, 'data'), file = join(dir, 'synthetic-service.json');
  await mkdir(dataDir); const stoppers = [];
  t.after(async () => {
    const outcomes = await Promise.allSettled(stoppers.map(stop => stop()));
    const absolute = await realpath(dir); assert.equal(dirname(absolute), await realpath(tmpdir())); assert.ok(basename(absolute).startsWith('soty-shared-voice-service-'));
    await rm(absolute, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
    for (const outcome of outcomes) if (outcome.status === 'rejected') throw outcome.reason;
  });
  return { dir, dataDir, file, stoppers, env: { SOTY_VOICE_ORB_ENABLED: '1', SOTY_VOICE_ORB_CONFIG_FILE: file },
    save: value => writeFile(file, JSON.stringify(value), { mode: 0o600 }) };
}
function inspect(databasePath, fn) {
  const db = new DatabaseSync(databasePath, { readOnly: true }); try { return fn(db); } finally { db.close(); }
}
const rows = databasePath => inspect(databasePath, db => db.prepare('SELECT request_key,grant_key,intent_digest,attempted,created_at,outcome_json,outcome_digest FROM voice_orb_starts ORDER BY request_key').all());
class SyntheticSocket extends EventEmitter {
  readyState = 0; sent = [];
  constructor(index) { super(); this.index = index; queueMicrotask(() => { this.readyState = 1; this.emit('open'); }); }
  send(raw) { const event = JSON.parse(raw); this.sent.push(event);
    if (event.type === 'session.update') queueMicrotask(() => this.emit('message', Buffer.from(JSON.stringify({ type: 'session.updated', session: { ...event.session, id: 'sess_shared_' + this.index } }))));
  }
  terminate() { if (this.readyState === 3) return; this.readyState = 3; this.emit('close'); }
}
async function signedFixture(t, { value = document(), projectId = 'soty', personalAccounts = 'both', eligibilityPort = 'actual' } = {}) {
  const f = await files(t); await f.save(value);
  const posts = [], sockets = [], authorizationChecks = [], issuedActors = new Map(); let connect, voice, personal, agentCalls = 0, credentialReads = 0, receiptReads = 0, glmNetwork = 0, quotaReads = 0, admissions = 0;
  const installed = loadVoiceOrbConfiguration(f.env, { dataDir: f.dataDir,
    fetchImpl: async (url, init) => {
      if (url.endsWith('/hangup')) return new Response(null, { status: 200 });
      assert.equal(url, 'https://api.openai.com/v1/realtime/calls');
      posts.push({ project: init.headers['OpenAI-Project'], credentialMatches: init.headers.authorization === 'Bearer ' + value.service.apiKey,
        safety: init.headers['OpenAI-Safety-Identifier'], model: JSON.parse(init.body.get('session')).model });
      return new Response('synthetic_shared_sdp', { status: 201, headers: { location: '/v1/realtime/calls/rtc_shared_' + posts.length } });
    }, socketFactory: () => { const socket = new SyntheticSocket(sockets.length + 1); sockets.push(socket); return socket; },
  });
  const originalQuotaStatus = installed.voiceOrb.quotaStatus, originalAdmit = installed.voiceOrb.admitQuota;
  installed.voiceOrb.quotaStatus = input => { quotaReads++; return originalQuotaStatus(input); };
  installed.voiceOrb.admitQuota = input => { admissions++; return originalAdmit(input); };
  // Bootstrap genuine accounts first, then install the exact existing GLM
  // account allowlist. Forward only the existing five voice RPC operations.
  connect = createConnectService({ databasePath: ':memory:', projectId, allowedOrigins: [ORIGIN], extensions: [{
    operations: new Set(['voice.orb.status', 'voice.orb.start', 'voice.orb.turn', 'voice.orb.cancel', 'voice.orb.end']),
    executeAsync(request) { issuedActors.set(request.actor.deviceId, request.actor); return voice.executeAsync(request); },
  }] });
  f.stoppers.push(async () => { await voice?.close(); personal?.close(); connect.close(); installed.close(); });
  async function rpc(owner, op, args = {}) {
    const challenge = good(await connect.handle({ origin: ORIGIN, op: 'challenge', args: { operation: op, digest: digestArgs(args) } }));
    return connect.handle({ origin: ORIGIN, op, args, proof: { challengeId: challenge.challengeId, publicJwk: owner.publicJwk,
      signature: sign('sha256', Buffer.from(challenge.message), { key: owner.key, dsaEncoding: 'ieee-p1363' }).toString('base64url') } });
  }
  async function bootstrap() { const owner = identity(); owner.account = good(await rpc(owner, 'bootstrap', { label: 'Synthetic shared voice', encryptionPublicJwk: owner.encryptionPublicJwk })); return owner; }
  const alice = await bootstrap(), bob = await bootstrap(), glmFile = join(f.dir, 'synthetic-existing-glm-profile.json');
  await writeFile(glmFile, JSON.stringify({ schema: 'soty.personal-agent-installed.v1',
    accountIds: personalAccounts === 'alice-only' ? [alice.account.accountId] : [alice.account.accountId, bob.account.accountId],
    gateway: { origin: 'https://synthetic-shared-glm.invalid', billingTenant: 'synthetic_existing_glm_tenant',
      inferenceBearer: 'synthetic-existing-glm-no-real-provider-key', managementSession: 'synthetic_glm_session_no_provider' } }), { mode: 0o600 });
  const glm = loadPersonalAgentConfiguration({ SOTY_PERSONAL_AGENT_ENABLED: '1', SOTY_PERSONAL_AGENT_CONFIG_FILE: glmFile },
    { fetchImpl: async () => { glmNetwork++; throw new Error('no_glm_network_for_eligibility'); } });
  const gateway = glm.personalAgent.gateway, originalAuthorize = gateway.authorizeActor, originalResolve = gateway.resolveCredential, originalReceipt = gateway.readReceipt;
  gateway.authorizeActor = input => { authorizationChecks.push({ accountId: input.actor.accountId, model: input.model,
    exactTenant: input.billingTenant === gateway.billingTenant, exactKeyRef: input.keyRef === gateway.keyRef }); return originalAuthorize(input); };
  gateway.resolveCredential = (...args) => { credentialReads++; return originalResolve(...args); };
  gateway.readReceipt = (...args) => { receiptReads++; return originalReceipt(...args); };
  personal = createPersonalAgentComposition({ getConnect: () => connect, getServices: () => ({}), configuration: glm.personalAgent });
  const observedPersonal = { ...personal, async executeAsync(request) { agentCalls++; return personal.executeAsync(request); } };
  if (eligibilityPort === 'absent') delete observedPersonal.modelEligible;
  if (eligibilityPort === 'false') observedPersonal.modelEligible = () => false;
  if (eligibilityPort === 'async') observedPersonal.modelEligible = async () => { throw new Error('unexpected_async_eligibility'); };
  voice = createVoiceOrbComposition({ getConnect: () => connect, agent: observedPersonal, configuration: installed.voiceOrb });
  return { ...f, installed, voice, connect, personal, alice, bob, posts, sockets, rpc, authorizationChecks,
    counters: () => ({ credentialReads, receiptReads, glmNetwork, agentCalls, quotaReads, admissions }), agentCalls: () => agentCalls,
    async hostActor(owner = alice) { good(await rpc(owner, 'voice.orb.status')); return issuedActors.get(owner.account.deviceId); },
    start: (owner, requestId = randomUUID(), extra = {}) => rpc(owner, 'voice.orb.start', { requestId, sdp: 'synthetic_shared_offer', ...extra }),
    async secondDevice() { const owner = identity();
      const started = good(await rpc(owner, 'enrollment.start', { label: 'Another shared device', encryptionPublicJwk: owner.encryptionPublicJwk }));
      good(await rpc(alice, 'enrollment.approve', { requestId: started.requestId, wrappedKey: { schema: 'synthetic.wrapped', ciphertext: 'opaque' } }));
      owner.account = good(await rpc(owner, 'enrollment.finish', { requestId: started.requestId, expectedAccountId: alice.account.accountId })); return owner;
    },
  };
}


test('shared installation stays OFF unless explicitly enabled and rejects mixed or caller permission schemas', { timeout: 10000 }, async t => {
  const off = { SOTY_VOICE_ORB_ENABLED: '0' };
  Object.defineProperty(off, 'SOTY_VOICE_ORB_CONFIG_FILE', { get() { throw new Error('must_not_read_config'); } });
  assert.equal(loadVoiceOrbConfiguration(off).voiceOrb.enabled, false);
  const f = await files(t);
  for (const change of [value => ({ ...value, schema: 'soty.voice-orb-installed.v3' }), value => ({ ...value, profiles: ownDocument().profiles }),
    value => ({ ...value, noteCreation: true }), value => ({ ...value, service: { ...value.service, accountId: actor.accountId } }),
    value => ({ ...value, service: { ...value.service, projectId: 'soty' } }), value => ({ ...value, service: { ...value.service, model: 'other' } }),
    value => ({ ...value, service: { ...value.service, maxStarts: 0 } }), value => ({ ...value, service: { ...value.service, maxAccountStarts: 4 } }),
    value => ({ ...value, service: { ...value.service, validUntil: 'invalid' } }), value => ({ ...value, service: { ...value.service, validUntil: '2000-01-01T00:00:00.000Z' } }),
    value => ({ ...value, admission: { ...value.admission, policy: 'all' } }), value => ({ ...value, admission: { ...value.admission, connectProjectId: 'foreign' } }),
    value => ({ ...value, admission: { ...value.admission, tenantId: 'caller_tenant' } })]) {
    await f.save(change(document()));
    assert.throws(() => loadVoiceOrbConfiguration(f.env, { dataDir: f.dataDir }), error => error.code === 'voice_orb_installation_invalid'
      && error.message === error.code && !error.cause);
  }
});

test('actual signed Soty accounts share the installed operator project while a second device keeps the same account allowance', { timeout: 10000 }, async t => {
  const value = document(); value.service.maxAccountStarts = 1;
  const f = await signedFixture(t, { value });
  assert.equal(JSON.stringify(f.installed).includes(value.service.apiKey), false);
  const first = good(await f.start(f.alice));
  assert.equal(first.voiceModel, 'gpt-realtime-2.1'); assert.equal(first.brainModel, 'zai-org/GLM-5.3-Flash');
  good(await f.rpc(f.alice, 'voice.orb.end', { sessionId: first.sessionId }));
  const secondDevice = await f.secondDevice();
  assert.equal(secondDevice.account.accountId, f.alice.account.accountId);
  assert.equal((await f.start(secondDevice)).ok, false); assert.equal(f.posts.length, 1);
  const second = good(await f.start(f.bob));
  assert.equal(f.posts.length, 2); assert.equal(f.posts.every(row => row.credentialMatches && row.project === 'proj_synthetic_operator_service'), true);
  assert.notEqual(f.posts[0].safety, f.posts[1].safety);
  good(await f.rpc(f.bob, 'voice.orb.end', { sessionId: second.sessionId }));
  assert.equal(good(await f.rpc(secondDevice, 'voice.orb.status')).quota.allowed, false);
  assert.equal(f.agentCalls(), 0); // Starting voice grants no GLM task or app write.
  const observed = rows(join(f.dataDir, 'voice-orb-quota.sqlite'));
  assert.equal(observed.length, 2); assert.equal(observed.every(row => row.attempted === 1), true);
  assert.equal(observed.every(row => JSON.parse(row.outcome_json).accountingFinalized === false), true);
});

test('shared admission still requires an original issued Connect actor and the exact existing Soty namespace', { timeout: 10000 }, async t => {
  const f = await signedFixture(t);
  await assert.rejects(f.voice.executeAsync({ op: 'voice.orb.status', args: {}, actor: { ...f.alice.account } }), error => error.code === 'connect_authority_actor_invalid');
  for (const extra of [{ tenantId: 'foreign' }, { accountId: f.bob.account.accountId }, { apiKey: 'caller' }, { noteCreation: true }, { model: 'other' }])
    assert.equal((await f.start(f.alice, randomUUID(), extra)).ok, false);
  assert.equal(f.posts.length, 0); assert.equal(rows(join(f.dataDir, 'voice-orb-quota.sqlite')).length, 0);
  const unsigned = await f.connect.handle({ origin: ORIGIN, op: 'voice.orb.start', args: { requestId: randomUUID(), sdp: 'synthetic' } });
  assert.equal(unsigned.ok, false); assert.equal(f.posts.length, 0);
  const foreign = await signedFixture(t, { projectId: 'synthetic-other-root' });
  assert.equal((await foreign.rpc(foreign.alice, 'voice.orb.status')).ok, false);
  assert.equal((await foreign.start(foreign.alice)).ok, false); assert.equal(foreign.posts.length, 0);
});

test('global and account starts survive reopening, device changes and operator credential or authorization rotation', { timeout: 10000 }, async t => {
  const f = await files(t); await f.save(document());
  let installed = loadVoiceOrbConfiguration(f.env, { dataDir: f.dataDir });
  const secondDevice = { ...actor, deviceId: 'synthetic_shared_device_2' }, bob = { accountId: 'synthetic_shared_account_b', deviceId: 'synthetic_shared_device_b' };
  await installed.voiceOrb.admitQuota(intent('same_request_id', actor));
  await installed.voiceOrb.admitQuota(intent('same_request_id', secondDevice));
  await assert.rejects(installed.voiceOrb.admitQuota(intent('third_account_start', { ...actor, deviceId: 'synthetic_new_device' })));
  await installed.voiceOrb.admitQuota(intent('global_last_start', bob)); installed.close();
  const rotated = document(); rotated.service.apiKey = 'synthetic-rotated-operator-openai-key-no-provider';
  rotated.service.projectId = 'proj_synthetic_rotated'; rotated.service.authorizationId = 'synthetic_new_authorization'; rotated.service.validUntil = '2099-02-01T00:00:00.000Z';
  await f.save(rotated); installed = loadVoiceOrbConfiguration(f.env, { dataDir: f.dataDir }); f.stoppers.push(installed.close);
  assert.equal(installed.voiceOrb.quotaStatus({ actor: secondDevice }).reservedStarts, 2);
  assert.equal(installed.voiceOrb.quotaStatus({ actor: bob }).remainingStarts, 0);
  await assert.rejects(installed.voiceOrb.admitQuota(intent('global_cannot_reset', { accountId: 'synthetic_shared_c', deviceId: 'synthetic_shared_c1' })));
  const observed = rows(join(f.dataDir, 'voice-orb-quota.sqlite'));
  assert.equal(observed.length, 3); assert.equal(new Set(observed.map(row => row.grant_key)).size, 2);
  assert.equal(observed.every(row => row.grant_key.startsWith('shared.v2:') && !row.grant_key.includes(actor.accountId)), true);
});

test('two actual independent SQLite writers cannot each consume the last shared global start', { timeout: 15000 }, async t => {
  const f = await files(t), databasePath = join(f.dataDir, 'voice-orb-quota.sqlite');
  const entry = { ...service(), expiresAt: Date.parse(service().validUntil), maxStarts: 1, maxAccountStarts: 1 };
  // Initialize the unchanged schema before starting concurrent real connections.
  createVoiceOrbQuota({ databasePath, service: entry }).close();
  const sourceDigest = createHash('sha256').update(createVoiceOrbQuota.toString()).digest('hex');
  const workerCode = `const { parentPort, workerData } = require('node:worker_threads');
    (async () => {
      // Eval workers explicitly establish the same Source-under-test loader.
      // An applied repository needs no overlay; the function pin still checks it.
      if (workerData.loaderUrl) { const { register } = require('node:module'); register(workerData.loaderUrl); }
      const { createVoiceOrbQuota } = await import(workerData.moduleUrl);
      const sourceDigest = require('node:crypto').createHash('sha256').update(createVoiceOrbQuota.toString()).digest('hex');
      if (sourceDigest !== workerData.sourceDigest) { parentPort.postMessage({ fatal: 'worker_source_mismatch', sourceDigest }); parentPort.close(); return; }
      const quota = createVoiceOrbQuota({ databasePath: workerData.databasePath, service: workerData.service });
      parentPort.postMessage({ ready: true, sourceDigest }); parentPort.once('message', async () => {
        try { const lease = await quota.admit({ actor: workerData.actor, requestId: 'concurrent_last_start', intentDigest: 'a'.repeat(64), maxDurationMs: 15000, signal: new AbortController().signal });
          parentPort.postMessage({ admitted: true, claimed: await lease.claimTransport() }); }
        catch (error) { parentPort.postMessage({ admitted: false, code: error.code }); }
        finally { quota.close(); }
      });
    })().catch(error => { parentPort.postMessage({ fatal: typeof error.code === 'string' && /^(?:voice_orb_[a-z_]+|ERR_SQLITE_[A-Z_]+)$/.test(error.code) ? error.code : 'worker_setup_failed',
      sqliteErrcode: Number.isSafeInteger(error.errcode) ? error.errcode : null }); });`;
  const workers = [1, 2].map(index => new Worker(workerCode, { eval: true, workerData: { moduleUrl: new URL('../voice-orb-quota.js', import.meta.url).href,
    databasePath, service: entry, sourceDigest, loaderUrl: process.env.SOTY_SHARED_VOICE_TEST_WORKER_LOADER ?? null,
    actor: { accountId: 'synthetic_worker_' + index, deviceId: 'synthetic_device_' + index } } }));
  // A result message precedes the worker's finally/quota.close. Observe the
  // actual owned thread exit before opening the independent readOnly DB.
  const exited = workers.map(worker => new Promise(resolve => worker.once('exit', resolve)));
  f.stoppers.push(async () => { await Promise.allSettled(workers.map(worker => worker.terminate())); });
  const ready = workers.map(worker => new Promise((resolve, reject) => {
    worker.once('error', reject); worker.once('message', value => value.ready && value.sourceDigest === sourceDigest ? resolve() : reject(new Error('worker_setup_failed:' + value.fatal
      + (Number.isSafeInteger(value.sqliteErrcode) ? ':sqlite=' + value.sqliteErrcode : ''))));
  }));
  await Promise.all(ready);
  const outcomes = workers.map(worker => new Promise((resolve, reject) => { worker.once('error', reject); worker.once('message', resolve); }));
  workers.forEach(worker => worker.postMessage('start'));
  const result = await Promise.all(outcomes); assert.equal(result.some(row => row.fatal), false);
  assert.equal(result.filter(row => row.admitted && row.claimed).length, 1);
  assert.equal(result.filter(row => !row.admitted && row.code === 'voice_orb_denied').length, 1);
  assert.deepEqual(await Promise.all(exited), [0, 0]);
  assert.equal(rows(databasePath).length, 1);
});

test('shared replay cannot erase attempted unknown evidence and a changed exact intent still conflicts after restart', { timeout: 10000 }, async t => {
  const f = await files(t); await f.save(document()); let installed = loadVoiceOrbConfiguration(f.env, { dataDir: f.dataDir });
  const original = await installed.voiceOrb.admitQuota(intent('crashed_shared_request')); assert.equal(await original.claimTransport(), true); installed.close();
  installed = loadVoiceOrbConfiguration(f.env, { dataDir: f.dataDir }); f.stoppers.push(installed.close);
  const replay = await installed.voiceOrb.admitQuota(intent('crashed_shared_request')); assert.equal(await replay.claimTransport(), false);
  await assert.rejects(replay.recordOutcome({ ...unknown, transportAttempted: false, providerCallId: null }));
  await assert.rejects(installed.voiceOrb.admitQuota({ ...intent('crashed_shared_request'), intentDigest: 'b'.repeat(64) }));
  const observed = rows(join(f.dataDir, 'voice-orb-quota.sqlite'));
  assert.equal(observed.length, 1); assert.equal(observed[0].attempted, 1); assert.equal(observed[0].outcome_json, null);
  assert.equal(installed.voiceOrb.quotaStatus({ actor }).reservedStarts, 1);
});

test('finite shared authorization must cover the entire requested session and expiry prevents credential resolution', { timeout: 10000 }, async t => {
  const value = document(), now = Date.now(); value.service.validUntil = new Date(now + 5000).toISOString();
  const f = await files(t); await f.save(value); let network = 0;
  const installed = loadVoiceOrbConfiguration(f.env, { dataDir: f.dataDir, fetchImpl: async () => { network++; throw new Error('provider_forbidden'); } }); f.stoppers.push(installed.close);
  await assert.rejects(installed.voiceOrb.admitQuota(intent('insufficient_authorization_duration')));
  assert.equal(rows(join(f.dataDir, 'voice-orb-quota.sqlite')).length, 0);
  t.mock.method(Date, 'now', () => now + 6000);
  assert.equal(installed.voiceOrb.quotaStatus({ actor }).allowed, false);
  await assert.rejects(installed.voiceOrb.transport.start({ actor, sdp: 'synthetic_offer', signal: new AbortController().signal, onEvent() {} }));
  assert.equal(network, 0);
});

test('V1 and V2 use the same unchanged SQLite schema and retain independent old profile counters under rollback', { timeout: 10000 }, async t => {
  const f = await files(t), databasePath = join(f.dataDir, 'voice-orb-quota.sqlite');
  const schema = () => inspect(databasePath, db => ({ version: db.prepare('PRAGMA user_version').get(),
    ddl: db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name").all() }));
  await f.save(ownDocument()); let installed = loadVoiceOrbConfiguration(f.env, { dataDir: f.dataDir });
  const first = await installed.voiceOrb.admitQuota(intent('legacy_before_shared')); assert.equal(await first.claimTransport(), true); await first.recordOutcome(unknown); installed.close();
  const before = schema(), originalRow = rows(databasePath)[0];
  await f.save(document()); installed = loadVoiceOrbConfiguration(f.env, { dataDir: f.dataDir });
  await installed.voiceOrb.admitQuota(intent('shared_after_legacy')); installed.close();
  assert.deepEqual(schema(), before); assert.deepEqual(rows(databasePath).find(row => row.request_key === originalRow.request_key), originalRow);
  await f.save(ownDocument()); installed = loadVoiceOrbConfiguration(f.env, { dataDir: f.dataDir }); f.stoppers.push(installed.close);
  assert.equal(installed.voiceOrb.quotaStatus({ actor }).reservedStarts, 1); assert.equal(installed.voiceOrb.quotaStatus({ actor }).remainingStarts, 1);
  await assert.rejects(installed.voiceOrb.admitQuota(intent('shared_after_legacy'))); // Same request cannot cross authorization modes.
  await installed.voiceOrb.admitQuota(intent('legacy_after_rollback'));
  await assert.rejects(installed.voiceOrb.admitQuota(intent('legacy_third_denied')));
  assert.equal(rows(databasePath).length, 3); assert.deepEqual(schema(), before);
});


test('shared status and start deny an actual signed account outside the installed GLM allowlist before reading quota or contacting OpenAI', { timeout: 10000 }, async t => {
  const f = await signedFixture(t, { personalAccounts: 'alice-only' });
  const denied = good(await f.rpc(f.bob, 'voice.orb.status')), startDenied = await f.start(f.bob);
  assert.deepEqual({ canStart: denied.canStart, startOk: startDenied.ok, providerPosts: f.posts.length,
    admissions: f.counters().admissions, reservedRows: rows(join(f.dataDir, 'voice-orb-quota.sqlite')).length },
    { canStart: false, startOk: false, providerPosts: 0, admissions: 0, reservedRows: 0 });
  assert.equal('quota' in denied, false);
  assert.deepEqual(f.counters(), { credentialReads: 0, receiptReads: 0, glmNetwork: 0, agentCalls: 0, quotaReads: 0, admissions: 0 });
  assert.equal(f.posts.length, 0); assert.equal(rows(join(f.dataDir, 'voice-orb-quota.sqlite')).length, 0);
  const allowed = good(await f.rpc(f.alice, 'voice.orb.status')); assert.equal(allowed.canStart, true);
  assert.equal(f.counters().quotaReads, 1);
  const started = good(await f.start(f.alice)); assert.equal(f.posts.length, 1);
  good(await f.rpc(f.alice, 'voice.orb.end', { sessionId: started.sessionId }));
  assert.equal(f.authorizationChecks.some(row => row.accountId === f.bob.account.accountId), true);
  assert.equal(f.authorizationChecks.every(row => row.model === 'zai-org/GLM-5.3-Flash' && row.exactTenant && row.exactKeyRef), true);
  const counters = f.counters(); assert.equal(counters.admissions, 1);
  assert.deepEqual([counters.credentialReads, counters.receiptReads, counters.glmNetwork, counters.agentCalls], [0, 0, 0, 0]);
});

test('shared mode fails closed if its trusted read-only eligibility port is absent, false or unexpectedly async', { timeout: 10000 }, async t => {
  for (const eligibilityPort of ['absent', 'false', 'async']) {
    const f = await signedFixture(t, { eligibilityPort });
    assert.equal(good(await f.rpc(f.alice, 'voice.orb.status')).canStart, false);
    assert.equal((await f.start(f.alice)).ok, false);
    assert.deepEqual(f.counters(), { credentialReads: 0, receiptReads: 0, glmNetwork: 0, agentCalls: 0, quotaReads: 0, admissions: 0 });
    assert.equal(f.posts.length, 0); assert.equal(rows(join(f.dataDir, 'voice-orb-quota.sqlite')).length, 0);
  }
});

test('internal model eligibility is read-only, rejects copied or revoked actors, and adds no RPC operation', { timeout: 10000 }, async t => {
  const f = await signedFixture(t, { personalAccounts: 'alice-only' }), retained = await f.hostActor();
  const before = f.counters(); assert.equal(f.personal.modelEligible(retained), true);
  assert.equal(f.personal.modelEligible({ ...retained }), false);
  assert.equal(f.personal.operations.has('agent.modelEligible'), false); assert.equal(f.voice.operations.has('agent.modelEligible'), false);
  const next = await f.secondDevice();
  good(await f.rpc(next, 'device.revoke', { deviceId: f.alice.account.deviceId }));
  assert.equal(f.personal.modelEligible(retained), false);
  f.personal.close(); assert.equal(f.personal.modelEligible(retained), false);
  assert.deepEqual(f.counters(), before);
  assert.equal(f.posts.length, 0); assert.equal(rows(join(f.dataDir, 'voice-orb-quota.sqlite')).length, 0);
});
