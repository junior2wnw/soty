import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, readFile, copyFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve, dirname, basename } from 'node:path';
import { spawnSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';
import { diagnoseAppRelease, appAuthenticationGuide } from '../../modules/apps/deployment-guidance.mjs';

const diagnose = observations => diagnoseAppRelease({ observations });
const codes = observations => diagnose(observations).findings.map(item => item.code);
test('preconnection stalls are hypotheses, and blank frames alone never authorize a CSP change', () => {
  const value = diagnose({ embedded: 'blank', standalone: 'works', document: 'stalled_before_connection', debugger: 'attached' });
  assert.equal(value.liveVerified, false); assert.equal(value.releaseReadiness, 'not_assessed');
  assert.equal(value.findings[0].code, 'debugger_interference_suspected'); assert.equal(value.findings[0].certainty, 'hypothesis');
  assert.deepEqual(codes({ embedded: 'blank', document: 'stalled_before_connection', debugger: 'unknown' }), ['preconnection_stall_unclassified']);
  assert.deepEqual(codes({ embedded: 'blank' }), ['blank_frame_unclassified']);
  assert.deepEqual(codes({ embedded: 'visible', persistence: 'verified', identity: 'verified' }), ['no_conclusive_fault']);
});
test('access and source failures survive simultaneous transport or unsupported readiness observations', () => {
  assert.deepEqual(codes({ access: 'denied', sourcePin: 'changed', document: 'received', readiness: 'unsupported' }),
    ['source_pin_changed', 'access_denied', 'readiness_not_implemented']);
  assert.deepEqual(codes({ readiness: 'pending' }), ['no_conclusive_fault']);
  assert.deepEqual(codes({ readiness: 'denied' }), ['access_denied']);
});
test('known transport, boot, identity and persistence faults get specific bounded recovery', () => {
  for (const [observations, expected] of [
    [{ websocket: 'auth_upgrade_rejected' }, 'auth_subrequest_upgrade'], [{ websocket: 'origin_rejected' }, 'websocket_origin_rejected'],
    [{ framing: 'ancestor_blocked' }, 'frame_ancestor_policy'], [{ framing: 'child_frame_blocked' }, 'nested_frame_policy'],
    [{ boot: 'expired' }, 'boot_ticket_expired'], [{ boot: 'cookie_rejected' }, 'boot_cookie_rejected'], [{ boot: 'failed' }, 'boot_failed'],
    [{ identity: 'wrong_subject' }, 'app_identity_required'], [{ persistence: 'lost_on_reload' }, 'persistence_failed'], [{ document: 'http_error' }, 'document_http_error'],
  ]) assert.deepEqual(codes(observations), [expected]);
});
test('guidance refuses URLs, token values, extra fields, objects and getters without evaluating or reflecting them', () => {
  for (const value of [{}, null, [], { observations: {} }, { observations: { url: 'SECRET_URL' } },
    { observations: { embedded: 'SECRET_TOKEN' } }, { observations: { debugger: {} } },
    { observations: { embedded: 'blank' }, cookie: 'SECRET_COOKIE' }, { observations: Object.create({ embedded: 'blank' }) }]) {
    assert.throws(() => diagnoseAppRelease(value), { code: 'invalid_release_guidance' });
  }
  let calls = 0;
  const getter = Object.defineProperty({}, 'embedded', { get() { calls++; throw new Error('SECRET'); } });
  assert.throws(() => diagnose(getter), { code: 'invalid_release_guidance' }); assert.equal(calls, 0);
  for (const value of [{ mode: 'SECRET' }, { mode: 'human-oidc', secret: 'SECRET' }, { mode: '__proto__' }])
    assert.throws(() => appAuthenticationGuide(value), { code: 'invalid_release_guidance' });
});
test('the default Soty login and explicit alternatives reference maintained integrations without activating authority', async () => {
  assert.deepEqual(appAuthenticationGuide({}), appAuthenticationGuide({ mode: 'soty' }));
  assert.match(appAuthenticationGuide({}).steps.join(' '), /403.*receipt.*Do not blindly repeat/u);
  assert.match(appAuthenticationGuide({}).boundaries.join(' '), /Idempotency-Key/u);
  for (const mode of ['soty', 'human-oidc', 'private-owner', 'agent']) {
    const value = appAuthenticationGuide({ mode });
    assert.equal(value.applied, false); assert.equal(value.productionEnabled, 'not_checked');
    for (const file of value.references) await readFile(resolve(file));
  }
  const human = appAuthenticationGuide({ mode: 'human-oidc' }); human.steps.length = 0;
  assert.ok(appAuthenticationGuide({ mode: 'human-oidc' }).steps.length);
  assert.match(appAuthenticationGuide({ mode: 'private-owner' }).identity, /single-owner/u);
  assert.match(appAuthenticationGuide({ mode: 'agent' }).identity, /OAuth\/MCP/u);
});
test('CLI and core guidance agree, never echo secrets, and refuse to overwrite an earlier result', async t => {
  const folder = await mkdtemp(join(tmpdir(), 'soty-guidance-test-'));
  t.after(async () => {
    assert.equal(dirname(resolve(folder)), resolve(tmpdir())); assert.match(basename(folder), /^soty-guidance-test-/u);
    await rm(folder, { recursive: true, force: true });
  });
  const cli = resolve('scripts/soty-app-release.mjs'), input = join(folder, 'observations.json'), output = join(folder, 'diagnosis.json');
  const run = args => spawnSync(process.execPath, [cli, ...args], { encoding: 'utf8' });
  const observations = { embedded: 'blank', document: 'stalled_before_connection', debugger: 'attached' };
  await writeFile(input, JSON.stringify(observations));
  const args = ['doctor', '--observations', input, '--output', output];
  const result = run(args); assert.equal(result.status, 0, result.stdout); assert.deepEqual(JSON.parse(result.stdout), diagnose(observations));
  assert.equal(JSON.parse(run(args).stdout).code, 'release_output_exists');
  const guide = run(['auth-guide', '--mode', 'human-oidc', '--output', join(folder, 'auth.json')]);
  assert.equal(guide.status, 0); assert.deepEqual(JSON.parse(guide.stdout), appAuthenticationGuide({ mode: 'human-oidc' }));
  const common = run(['auth-guide', '--output', join(folder, 'soty-auth.json')]);
  assert.equal(common.status, 0); assert.deepEqual(JSON.parse(common.stdout), appAuthenticationGuide({}));
  await writeFile(input, JSON.stringify({ embedded: 'blank', authorization: 'DO_NOT_PRINT_SECRET' }));
  const refused = run(['doctor', '--observations', input, '--output', join(folder, 'refused.json')]);
  assert.equal(refused.status, 1); assert.equal((refused.stdout + refused.stderr).includes('DO_NOT_PRINT_SECRET'), false);
  assert.equal(JSON.parse(refused.stdout).code, 'invalid_release_guidance');
  const gate = spawnSync(process.execPath, [resolve('scripts/check-app-release.mjs')], { encoding: 'utf8', env: { ...process.env, SOTY_CADDY_BIN: '' } });
  assert.equal(gate.status, 1); assert.equal(JSON.parse(gate.stdout).code, 'pipeline_caddy_required');
  // Final Docker image copies modules, not src/deploy/scripts. Exercise the
  // shared module in isolation so a test-only import cannot break MCP startup.
  const runtimeModule = join(folder, 'deployment-guidance.mjs');
  await copyFile(resolve('modules/apps/deployment-guidance.mjs'), runtimeModule);
  const runtime = spawnSync(process.execPath, ['--input-type=module', '-e',
    'const m=await import(process.argv[1]);console.log(JSON.stringify(m.appAuthenticationGuide({mode:"human-oidc"})))',
    pathToFileURL(runtimeModule).href], { encoding: 'utf8' });
  assert.equal(runtime.status, 0, runtime.stderr); assert.deepEqual(JSON.parse(runtime.stdout), appAuthenticationGuide({ mode: 'human-oidc' }));
});
