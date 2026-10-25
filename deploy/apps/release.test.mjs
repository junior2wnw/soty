import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, readFile, rm, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve, dirname, basename } from 'node:path';
import { spawnSync } from 'node:child_process';
import { initAppManifest, createReleasePlan, nativeIngress, writeReleasePlan, verifyRelease, namedZoneIngress } from './release.mjs';
import { deployment, pairedDeployment, nativePolicy, manifest } from './fixtures.mjs';

async function folder(t) {
  const path = await mkdtemp(join(tmpdir(), 'soty-release-test-'));
  t.after(async () => {
    assert.equal(dirname(resolve(path)), resolve(tmpdir())); assert.match(basename(path), /^soty-release-test-/u);
    await rm(path, { recursive: true, force: true });
  }); return path;
}
test('manifest init is idempotent for the same intent and refuses to overwrite another project', async t => {
  const path = await folder(t), args = { project: path, name: 'Project', port: 8111 };
  const first = await initAppManifest(args); assert.equal(first.unchanged, false);
  assert.equal((await initAppManifest(args)).unchanged, true);
  await assert.rejects(initAppManifest({ ...args, port: 8112 }), { code: 'manifest_already_exists_different' });
  assert.equal(JSON.parse(await readFile(first.file, 'utf8')).port, 8111);
});
test('a .soty junction or symlink cannot write a manifest outside its project', async t => {
  const root = await folder(t), project = join(root, 'project'), outside = join(root, 'outside');
  await mkdir(project); await mkdir(outside);
  await symlink(outside, join(project, '.soty'), process.platform === 'win32' ? 'junction' : 'dir');
  await assert.rejects(initAppManifest({ project, name: 'Project', port: 8111 }), { code: 'manifest_workspace_escape' });
  await assert.rejects(readFile(join(outside, 'app.json')), { code: 'ENOENT' });
});
test('release planning selects the exact enabled named address and defaults to isolated transport', () => {
  const plan = createReleasePlan({ manifest: manifest(), deployment: deployment() });
  assert.equal(plan.mode, 'isolated'); assert.equal(plan.origin, 'https://project.example');
  assert.match(plan.shellLaunchUrl, /#launch\/app-[a-f0-9]{32}\/dom_[a-f0-9]{32}\?path=/u);
  assert.equal(plan.probes[1].path, '/?project=fixture&soty_release_probe=1');
});
test('stale, revoked, ambiguous or mismatched inputs fail before writing ingress', () => {
  const cases = [
    [value => { value.checkedAt -= 600001; }, 'deployment_snapshot_stale'],
    [value => { value.app.state = 'revoked'; }, 'app_revoked'],
    [value => { value.source.port++; }, 'manifest_source_mismatch'],
    [value => { value.addresses.aliases[0].active = false; }, 'select_named_address'],
    [value => { value.addresses.aliases.push({ ...value.addresses.aliases[0], id: 'dom_' + 'e'.repeat(32), origin: 'https://another.example' }); }, 'select_named_address'],
  ];
  for (const [mutate, code] of cases) {
    const value = deployment(); mutate(value);
    assert.throws(() => createReleasePlan({ manifest: manifest(), deployment: value }), { code });
  }
});
test('explicit address IDs never fall back to a canonical or a different enabled alias', () => {
  const value = deployment();
  assert.throws(() => createReleasePlan({ manifest: manifest(), deployment: value, domainId: value.addresses.canonical.id }), { code: 'active_named_address_required' });
});
test('native ingress pins the source, clears only the auth query and preserves application CSP', () => {
  const plan = createReleasePlan({ manifest: manifest(), deployment: deployment(), mode: 'native', frameOrigins: ['https://retained.example'] });
  const text = nativeIngress(plan);
  assert.match(text, /uri \/_soty\/ingress-check\?\n/u); assert.match(text, new RegExp(plan.source.digest));
  assert.match(text, /\+Content-Security-Policy/u); assert.match(text, /https:\/\/retained\.example/u);
  assert.equal(text.includes('canonical.example'), false);
  assert.throws(() => nativeIngress({ ...plan, origin: 'https://name.example/{injection}' }));
});

test('paired Native planning selects its exact port/address while the Soty link retains the Source entry', () => {
  const value = pairedDeployment(), plan = createReleasePlan({ manifest: manifest(), deployment: value, mode: 'native' });
  assert.equal(plan.schema, 'soty.app-release.v2'); assert.equal(plan.origin, value.nativeEntry.origin);
  assert.equal(plan.domainId, value.nativeEntry.domainId); assert.deepEqual(plan.source, value.source);
  assert.deepEqual(plan.nativeEntry, value.nativeEntry);
  assert.match(plan.shellLaunchUrl, new RegExp(value.source.launchEntry.domainId)); assert.match(plan.shellLaunchUrl, /path=%2Fembed/u);
  assert.equal(plan.probes[0].path, value.nativeEntry.target.entryPath);
  assert.equal(plan.probes[2].path, '/_soty/ingress-check');
  assert.deepEqual(plan.nativeGuard, { rootCheckUrl: value.nativeEntry.origin + '/_soty/ingress-check', appId: value.app.id,
    origin: value.nativeEntry.origin, primaryTargetDigest: value.source.digest, nativeTargetDigest: value.nativeEntry.target.digest });
  const source = createReleasePlan({ manifest: { ...manifest(), port: value.source.port, entryPath: '/embed' }, deployment: value });
  assert.equal(source.origin, value.source.launchEntry.origin);
  assert.throws(() => createReleasePlan({ manifest: manifest(), deployment: value, mode: 'native', domainId: value.source.launchEntry.domainId }),
    { code: 'native_entry_address_required' });
});

test('closed inspected pairs, missing Source or Native aliases, wrong manifests and a Source-only Native request never create an ingress', () => {
  for (const mutate of [value => { value.publication.launchPolicy = 'restricted'; value.publication.listed = false; },
    value => { value.addresses.aliases[0].active = false; value.addresses.aliases[0].state = 'tombstone'; },
    value => { value.source.launchEntry = null; value.addresses.aliases[1].active = false; }]) {
    const value = pairedDeployment(); mutate(value);
    assert.throws(() => createReleasePlan({ manifest: manifest(), deployment: value, mode: 'native' }), { code: 'paired_native_entry_unavailable' });
  }
  const value = pairedDeployment(); value.nativeEntry = null;
  assert.throws(() => createReleasePlan({ manifest: manifest(), deployment: value, mode: 'native' }), { code: 'paired_native_entry_required' });
  assert.throws(() => createReleasePlan({ manifest: { ...manifest(), port: 8112 }, deployment: pairedDeployment(), mode: 'native' }), { code: 'manifest_source_mismatch' });
  assert.throws(() => createReleasePlan({ manifest: manifest(), deployment: pairedDeployment(), mode: 'native', frameOrigins: ['https://source.example'] }), { code: 'invalid_frame_origins' });
});

test('every Native ingress has an exact Root policy GET route; paired data authentication pins both approved targets', () => {
  const primary = createReleasePlan({ manifest: manifest(), deployment: deployment(), mode: 'native' }), primaryText = nativeIngress(primary);
  assert.match(primaryText, /@soty_native_policy \{\n        method GET\n        path \/_soty\/ingress-check/u);
  assert.match(primaryText, /header_up X-Soty-Ingress-Surface native/u);
  assert.match(primaryText, new RegExp('header_up X-Soty-Ingress-Primary ' + primary.source.digest));
  const value = pairedDeployment(), plan = createReleasePlan({ manifest: manifest(), deployment: value, mode: 'native' }), text = nativeIngress(plan);
  assert.equal((text.match(/header_up X-Soty-Ingress-Surface native/gu) ?? []).length, 2);
  assert.equal((text.match(new RegExp('header_up X-Soty-Ingress-Primary ' + value.source.digest, 'gu')) ?? []).length, 2);
  assert.equal((text.match(new RegExp('header_up X-Soty-Ingress-Target ' + value.nativeEntry.target.digest, 'gu')) ?? []).length, 2);
  assert.match(text, /reverse_proxy 127\.0\.0\.1:8111/u); assert.equal(text.includes('reverse_proxy 127.0.0.1:8112'), false);
  assert.match(text, /header_up -X-Soty-Ingress-Surface/u); assert.match(text, /header_up -X-Soty-Ingress-Primary/u);
  assert.match(text, /handle @soty_boot \{\n            respond 404/u);
  assert.ok(text.indexOf('handle @soty_native_policy') < text.indexOf('forward_auth'));
  for (const mutate of [value => { value.source.digest = { toString: () => 'd'.repeat(64) }; },
    value => { value.source.revision = '2'; }, value => { value.source.port = '8112'; },
    value => { value.source.launchEntry.path = '/other'; }, value => { value.publication.policyEpoch = '3'; },
    value => { value.origin = value.nativeEntry.origin = 'https://project.example:8443'; }]) {
    const invalid = structuredClone(plan); mutate(invalid); assert.throws(() => nativeIngress(invalid));
  }
});

test('paired HTTP verification requires the exact bounded current Root policy and never calls a Native guard or Human login ready', async () => {
  const plan = createReleasePlan({ manifest: manifest(), deployment: pairedDeployment(), mode: 'native' });
  const verify = mutate => verifyRelease(plan, { fetcher: async (url, options) => {
    assert.equal(url.origin, plan.origin); assert.equal(options.redirect, 'manual'); assert.equal(options.credentials, 'omit');
    if (url.pathname !== '/_soty/ingress-check') return new Response('Native login UI');
    const policy = nativePolicy(plan); mutate?.(policy);
    return new Response(JSON.stringify(policy), { headers: { 'content-type': 'application/json' } });
  } });
  const good = await verify(); assert.equal(good.ok, true); assert.equal(good.nativePolicyChecked, true);
  assert.equal(good.httpOnly, true); assert.equal(good.browserAndDataChecksRequired, true);
  assert.equal(Object.hasOwn(good, 'nativeGuardReady'), false);
  for (const mutate of [value => { value.primaryTargetDigest = '0'.repeat(64); }, value => { value.nativeTargetDigest = '0'.repeat(64); },
    value => { value.policyEpoch++; }, value => { value.admissionKeyHash = '0'.repeat(64); }, value => { value.leaseMs = 1001; },
    value => { value.nativeTargetRevision++; }, value => { value.origin = 'https://source.example'; }, value => { value.extra = true; }]) {
    assert.equal((await verify(mutate)).ok, false);
  }
  const html = await verifyRelease(plan, { fetcher: async () => new Response('<html>Login</html>', { headers: { 'content-type': 'text/html' } }) });
  assert.equal(html.ok, false);
  await assert.rejects(verifyRelease({ ...plan, probes: plan.probes.slice(0, 2) }, { fetcher: async () => new Response('') }), { code: 'invalid_release_plan' });
  await assert.rejects(verifyRelease(plan, { fetcher: async url => new Response(url.pathname === '/_soty/ingress-check' ? ' '.repeat(4097) : '',
    { headers: { 'content-type': 'application/json' } }) }), { code: 'probe_response_too_large' });
});
test('plans and ingress are stored only in a fresh run directory with an immutable snippet digest', async t => {
  const root = await folder(t), output = join(root, 'run');
  const plan = createReleasePlan({ manifest: manifest(), deployment: deployment(), mode: 'native' });
  const saved = await writeReleasePlan(output, plan);
  assert.match(saved.plan.files['native-ingress.caddy'], /^[a-f0-9]{64}$/u);
  await assert.rejects(writeReleasePlan(output, plan), { code: 'EEXIST' });
});
test('HTTP verification follows no redirects and reports query failures instead of declaring the release complete', async () => {
  const plan = createReleasePlan({ manifest: manifest(), deployment: deployment() }), requests = [];
  const result = await verifyRelease(plan, { fetcher: async (url, options) => {
    requests.push({ url: String(url), options }); return new Response('', { status: String(url).includes('soty_release_probe') ? 403 : 200 });
  } });
  assert.equal(result.ok, false); assert.equal(result.httpOnly, true); assert.equal(result.browserAndDataChecksRequired, true);
  assert.equal(requests.every(item => item.options.redirect === 'manual' && item.options.credentials === 'omit'), true);
  assert.equal(result.results[1].status, 403);
});
test('restricted publication requires authenticated browser validation; probe URLs cannot escape the named origin', async () => {
  const plan = createReleasePlan({ manifest: manifest(), deployment: deployment() });
  await assert.rejects(verifyRelease({ ...plan, publication: { ...plan.publication, launchPolicy: 'restricted' } }), { code: 'authenticated_browser_verification_required' });
  await assert.rejects(verifyRelease(plan, { probes: [{ path: '//evil.example', status: 200 }], fetcher: async () => new Response('') }), { code: 'invalid_release_probe' });
});
test('real CLI init and plan work on this OS, fail without dumping supplied secrets, and never publish', async t => {
  const root = await folder(t), cli = resolve('scripts/soty-app-release.mjs');
  const run = args => spawnSync(process.execPath, [cli, ...args], { encoding: 'utf8' });
  const first = run(['init', '--project', root, '--name', 'Project', '--port', '8111', '--entry-path', '/?project=fixture']);
  assert.equal(first.status, 0, first.stdout); assert.equal(JSON.parse(first.stdout).ok, true);
  const file = join(root, 'deployment.json'); await writeFile(file, JSON.stringify(deployment()));
  const planned = run(['plan', '--project', root, '--deployment', file, '--output', join(root, 'run')]);
  assert.equal(planned.status, 0, planned.stdout); assert.equal(JSON.parse(planned.stdout).applied, false);
  const paired = pairedDeployment(); await writeFile(file, JSON.stringify(paired));
  const pairedPlan = run(['plan', '--project', root, '--deployment', file, '--mode', 'native', '--output', join(root, 'paired')]);
  assert.equal(pairedPlan.status, 0, pairedPlan.stdout); assert.equal(JSON.parse(pairedPlan.stdout).applied, false);
  const pairedStored = JSON.parse(await readFile(join(root, 'paired/release-plan.json'), 'utf8'));
  assert.equal(pairedStored.origin, paired.nativeEntry.origin); assert.equal(pairedStored.schema, 'soty.app-release.v2');
  assert.equal(pairedStored.nativeGuard.primaryTargetDigest, paired.source.digest);
  assert.match(pairedStored.shellLaunchUrl, new RegExp(paired.source.launchEntry.domainId));
  const bad = deployment(); bad.source.secret = 'DO_NOT_PRINT_TOKEN_VALUE'; await writeFile(file, JSON.stringify(bad));
  const refused = run(['plan', '--project', root, '--deployment', file, '--output', join(root, 'bad')]);
  assert.equal(refused.status, 1); assert.equal(refused.stdout.includes('DO_NOT_PRINT_TOKEN_VALUE'), false);
});
test('named zone setup uses a single registry gated wildcard route and cannot inject a Caddyfile', () => {
  const text = namedZoneIngress({ origin: 'https://4-2.xn--p1ai' });
  assert.match(text, /https:\/\/ \{/u); assert.match(text, /host\('\*\.4-2\.xn--p1ai'\)/u); assert.match(text, /127\.0\.0\.1:18182/u);
  const retained = namedZoneIngress({ origin: 'https://4-2.xn--p1ai', additionalOrigins: ['https://old.example', 'https://4-2.xn--p1ai'] });
  assert.match(retained, /host\('\*\.4-2\.xn--p1ai', '\*\.old\.example'\)/u);
  for (const origin of ['https://user:password@example.com', 'https://example.com/path', 'http://example.com', 'https://127.0.0.1'])
    assert.throws(() => namedZoneIngress({ origin }));
});
