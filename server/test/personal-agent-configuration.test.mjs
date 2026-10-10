import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, mkdir, rm, realpath } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname, resolve } from 'node:path';
import { fork } from 'node:child_process';
import { loadPersonalAgentConfiguration } from '../personal-agent-configuration.js';

const profile = () => ({ schema: 'soty.personal-agent-installed.v1', gateway: { origin: 'https://fixture-commerce.invalid', billingTenant: 'fixture_tenant',
  inferenceBearer: 'synthetic-only-private-inference-key', managementSession: 'synthetic_only_private_session' }, accountIds: ['fixture_owner_A'],
  localTargets: [{ accountId: 'fixture_owner_A', hostDeviceId: 'fixture_device', connectorId: 'fixture_connector', cwd: 'C:\\fixture-workspace' }] });
const closed = error => error.code === 'agent_gateway_configuration_invalid' && error.message === error.code && !error.cause;
async function files(t) {
  const dir = await mkdtemp(join(tmpdir(), 'soty-agent-installed-owned-')), file = join(dir, 'private.json'), dist = join(dir, 'dist');
  const stoppers = []; await mkdir(dist); await writeFile(join(dist, 'index.html'), '<!doctype html><title>Own source fixture</title>');
  t.after(async () => { const results = await Promise.allSettled(stoppers.map(stop => stop()));
    const current = await realpath(dir); assert.equal(dirname(current), await realpath(tmpdir()));
    assert.match(current, /soty-agent-installed-owned-/); await rm(current, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
    for (const result of results) if (result.status === 'rejected') throw result.reason; });
  return { dir, file, dist, stoppers, env: { SOTY_PERSONAL_AGENT_ENABLED: '1', SOTY_PERSONAL_AGENT_CONFIG_FILE: file },
    save: value => writeFile(file, JSON.stringify(value), { mode: 0o600 }) };
}

test('private process entry defaults off without reading any config or activating a credential', () => {
  assert.deepEqual(loadPersonalAgentConfiguration({}), { personalAgent: { enabled: false } });
  const env = { SOTY_PERSONAL_AGENT_ENABLED: '0' }; Object.defineProperty(env, 'SOTY_PERSONAL_AGENT_CONFIG_FILE', { get() { throw Error('must_not_read'); } });
  assert.deepEqual(loadPersonalAgentConfiguration(env), { personalAgent: { enabled: false } });
  for (const value of ['true', ' 1', '2']) assert.throws(() => loadPersonalAgentConfiguration({ SOTY_PERSONAL_AGENT_ENABLED: value }), closed);
});

test('installed billing/account/device options are exact private closures and never payload authority', async t => {
  const f = await files(t); await f.save(profile()); const { personalAgent: config } = loadPersonalAgentConfiguration(f.env);
  assert.equal(config.enabled, true); assert.equal(config.historyEnabled, true);
  const gateway = config.gateway, credential = await gateway.resolveCredential(gateway.keyRef);
  assert.equal(credential.keyRef, gateway.keyRef); assert.equal(credential.tenantId, 'fixture_tenant');
  assert.equal(gateway.authorizeActor({ actor: { accountId: 'fixture_owner_A' }, billingTenant: 'fixture_tenant', keyRef: gateway.keyRef }), true);
  assert.equal(gateway.authorizeActor({ actor: { accountId: 'another_owner' }, billingTenant: 'fixture_tenant', keyRef: gateway.keyRef }), false);
  assert.equal(gateway.authorizeActor({ actor: { accountId: 'fixture_owner_A' }, billingTenant: 'another_tenant', keyRef: gateway.keyRef }), false);
  assert.equal(gateway.authorizeActor({ actor: { accountId: 'fixture_owner_A' }, billingTenant: 'fixture_tenant', keyRef: {} }), false);
  assert.equal(config.localTargetFor({ actor: { accountId: 'another_owner' } }), null);
  assert.equal(config.localTargetFor({ actor: { accountId: 'fixture_owner_A' } }).cwd, 'C:\\fixture-workspace');
  const encoded = JSON.stringify(config); assert.equal(encoded.includes(profile().gateway.inferenceBearer), false);
  assert.equal(encoded.includes(profile().gateway.managementSession), false);
});

test('missing/oversized/unsupported/HTTP or extra installed authority fails closed with no sentinel output', async t => {
  const f = await files(t); assert.throws(() => loadPersonalAgentConfiguration(f.env), closed);
  for (const change of [value => ({ ...value, schema: 'future' }), value => ({ ...value, actor: 'private_sentinel' }),
    value => ({ ...value, gateway: { ...value.gateway, origin: 'http://127.0.0.1:1' } }),
    value => ({ ...value, gateway: { ...value.gateway, model: 'other_model' } }),
    value => ({ ...value, localTargets: [{ ...value.localTargets[0], accountId: 'foreign_account' }] }),
    value => ({ ...value, localTargets: [{ ...value.localTargets[0], cwd: '../other-project' }] })]) {
    await f.save(change(profile())); assert.throws(() => loadPersonalAgentConfiguration(f.env), closed);
  }
  await writeFile(f.file, ' '.repeat(65537)); assert.throws(() => loadPersonalAgentConfiguration(f.env), closed);
});

test('receipt Source uses the existing exact management GET, current cancellation and closed byte cap', async t => {
  const f = await files(t); await f.save(profile()); const id = '7ace962a-51a2-4f20-b911-c1ef69fc5d1b'; let called = 0;
  const fetchImpl = async (url, init) => {
    called++; assert.equal(url, `https://fixture-commerce.invalid/api/requests/${id}`); assert.equal(init.redirect, 'error');
    assert.equal(init.signal instanceof AbortSignal, true); assert.equal(init.headers.cookie === `soty_workspace=${profile().gateway.managementSession}`, true);
    return Response.json({ request: { id, status: 'completed' } });
  };
  const config = loadPersonalAgentConfiguration(f.env, { fetchImpl }).personalAgent, gateway = config.gateway;
  const proof = await gateway.readReceipt({ requestId: id, billingTenant: 'fixture_tenant', keyRef: gateway.keyRef, signal: new AbortController().signal });
  assert.equal(proof.request.id, id); assert.equal(proof.tenantId, 'fixture_tenant');
  await assert.rejects(gateway.readReceipt({ requestId: '../other', billingTenant: 'fixture_tenant', keyRef: gateway.keyRef }), error => error.code === 'agent_key_binding_invalid');
  assert.equal(called, 1);
  const cancelled = new AbortController(); cancelled.abort();
  await assert.rejects(gateway.readReceipt({ requestId: id, billingTenant: 'fixture_tenant', keyRef: gateway.keyRef, signal: cancelled.signal }), error => error.code === 'agent_cancelled');
  assert.equal(called, 1);
  const large = loadPersonalAgentConfiguration(f.env, { fetchImpl: async () => Response.json({ request: { body: 'x'.repeat(32768) } }) }).personalAgent.gateway;
  await assert.rejects(large.readReceipt({ requestId: id, billingTenant: 'fixture_tenant', keyRef: large.keyRef }), error => error.code === 'agent_accounting_unknown');
  const rejected = loadPersonalAgentConfiguration(f.env, { fetchImpl: async () => new Response('private_server_error', { status: 403 }) }).personalAgent.gateway;
  await assert.rejects(rejected.readReceipt({ requestId: id, billingTenant: 'fixture_tenant', keyRef: rejected.keyRef }), error => error.code === 'agent_accounting_unknown');
});

test('receipt Source adapts canonical PostgreSQL bigint counters without changing billing metadata', async t => {
  const f = await files(t); await f.save(profile());
  const id = '7ace962a-51a2-4f20-b911-c1ef69fc5d1b';
  let wire = { id, status: 'completed', model_requested: 'zai-org/GLM-5.3-Flash', model_resolved: 'zai-org/GLM-5.3-Flash',
    input_tokens: '27', output_tokens: '85', charge_urub: '2576', error_code: null };
  const gateway = loadPersonalAgentConfiguration(f.env, { fetchImpl: async () => Response.json({ request: wire }) }).personalAgent.gateway;
  const read = () => gateway.readReceipt({ requestId: id, billingTenant: 'fixture_tenant', keyRef: gateway.keyRef });
  assert.deepEqual((await read()).request, { ...wire, input_tokens: 27, output_tokens: 85 });
  assert.equal(wire.input_tokens, '27');
  wire = { ...wire, input_tokens: '0', output_tokens: 0 };
  assert.deepEqual((await read()).request, { ...wire, input_tokens: 0 });
  wire = { ...wire, status: 'usage_pending', input_tokens: null, output_tokens: null };
  assert.deepEqual((await read()).request, wire);
  for (const value of ['01', '-1', ' 27', '+27', '2.7', '2e1', '', '9007199254740992', '99999999999999999']) {
    wire = { ...wire, status: 'completed', input_tokens: value, output_tokens: '85' };
    await assert.rejects(read(), error => error.code === 'agent_accounting_unknown');
  }
});

async function entry(t, fixture, enabled) {
  const environment = Object.fromEntries(['SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'PATH'].filter(name => process.env[name] !== undefined).map(name => [name, process.env[name]]));
  Object.assign(environment, { NODE_NO_WARNINGS: '1', HOST: '127.0.0.1', PORT: '0', DATA_DIR: join(fixture.dir, 'data'), SOTY_DIST_DIR: fixture.dist,
    SOTY_UNIVERSAL_APPS_ENABLED: 'false', SOTY_GONKA_API_KEY: '', SOTY_PERSONAL_AGENT_ENABLED: enabled ? '1' : '0', SOTY_PERSONAL_AGENT_CONFIG_FILE: fixture.file });
  const child = fork(new URL('../index.js', import.meta.url), [], { execPath: process.execPath, env: environment, stdio: ['ignore', 'pipe', 'pipe', 'ipc'] });
  let out = ''; child.stdout.on('data', data => { out += data.toString(); }); child.stderr.on('data', data => { out += data.toString(); });
  const exited = new Promise(done => child.once('exit', (code, signal) => done({ code, signal })));
  const ready = new Promise((done, reject) => { child.on('message', value => { if (value?.type === 'soty:ready') done(value.port); });
    child.once('error', reject); child.once('exit', () => reject(Error('owned_entry_exited_before_ready'))); });
  let timer;
  const deadline = new Promise((_, reject) => { timer = setTimeout(() => reject(Error('owned_entry_start_deadline')), 10000); });
  let stopping = false;
  fixture.stoppers.push(async () => {
    if (!stopping) { stopping = true; if (child.connected) child.disconnect(); else child.kill('SIGTERM'); }
    let timer;
    const failed = new Promise((_, reject) => { timer = setTimeout(() => { child.kill('SIGTERM'); reject(Error('owned_entry_stop_deadline')); }, 5000); });
    let result; try { result = await Promise.race([exited, failed]); } finally { clearTimeout(timer); }
    assert.equal(result.code, 0);
    assert.equal(out.includes(profile().gateway.inferenceBearer), false); assert.equal(out.includes(profile().gateway.managementSession), false); });
  try { return `http://127.0.0.1:${await Promise.race([ready, deadline])}`; } finally { clearTimeout(timer); }
}

test('the actual server/index CLI mounts installed private options, while default remains OFF', { timeout: 15000 }, async t => {
  const off = await files(t), offOrigin = await entry(t, off, false);
  const offStatus = await fetch(offOrigin + '/api/apps/capabilities').then(value => value.json()); assert.equal(offStatus.personalAgent.configured, false);
  assert.equal(offStatus.personalAgent.missingBinding, 'not_enabled');
  const on = await files(t); await on.save(profile()); const onOrigin = await entry(t, on, true);
  const onStatus = await fetch(onOrigin + '/api/apps/capabilities').then(value => value.json()); assert.equal(onStatus.personalAgent.configured, true);
  assert.equal(onStatus.personalAgent.wholeReady, false); assert.equal(onStatus.personalAgent.memory, 'owner_notes_labelled_history');
  assert.equal(JSON.stringify(onStatus).includes(profile().gateway.inferenceBearer), false);
  assert.equal(JSON.stringify(onStatus).includes('fixture_tenant'), false);
});
