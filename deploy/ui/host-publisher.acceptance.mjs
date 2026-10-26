import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { tmpdir } from 'node:os';
import { createServer } from 'node:http';
import { randomBytes } from 'node:crypto';
import { execFileSync, spawn, spawnSync } from 'node:child_process';
import { once } from 'node:events';
import { fileURLToPath } from 'node:url';
import { createUiRelease, writeUiRelease, createCaddyOverlay } from './release.mjs';
import { fixtureDist } from './test-support.mjs';
import { sha256, writeEncryptedConfigs } from './config-swap.mjs';
import { prepareOverlayReplacement, executeHostPlan, inspectHostPlan, verifyActiveDisk, verifyCaddyProcess } from './host-publisher.mjs';
import { sourceSdkArchiveName } from '../../modules/source-app/package-contract.mjs';

const binary = process.env.SOTY_UI_CADDY_BINARY;
assert.ok(binary, 'explicit qualified Caddy binary required');
assert.equal(process.platform, 'linux'); assert.equal(process.getuid(), 0, 'run in an owned isolated namespace as root');
assert.match(execFileSync(binary, ['version'], { encoding: 'utf8' }), /^v2\.11\.2\b/u);
const binarySha256 = sha256(fs.readFileSync(binary));
const adapt = text => JSON.parse(execFileSync(binary, ['adapt', '--config', '-', '--adapter', 'caddyfile'],
  { input: text, maxBuffer: 8 * 1024 * 1024, stdio: ['pipe', 'pipe', 'pipe'] }));
const delay = () => new Promise(resolve => setTimeout(resolve, 20));

test('real Caddy old overlay -> new overlay -> old on one process; finite assets, ingress, CAS and lost-response barrier', { timeout: 20000 }, async t => {
  const directory = fs.realpathSync(fs.mkdtempSync(path.join(tmpdir(), 'soty-ui-hostreal-'))); fs.chmodSync(directory, 0o700);
  let caddy, backend; const key = randomBytes(32);
  t.after(async () => {
    key.fill(0);
    if (caddy?.exitCode === null) { const exited = once(caddy, 'exit'); caddy.kill('SIGTERM'); await exited; }
    if (backend?.listening) { backend.closeAllConnections(); await new Promise(resolve => backend.close(resolve)); }
    const unseal = current => { fs.chmodSync(current, 0o700); for (const entry of fs.readdirSync(current, { withFileTypes: true })) if (entry.isDirectory()) unseal(path.join(current, entry.name)); };
    unseal(directory); fs.rmSync(directory, { recursive: true, force: true });
  });
  // The production verifier pins comm=caddy. CI names its qualified input
  // soty-test-caddy; keep the executable on its qualified mount when TMPDIR is
  // noexec, while using the production basename for this fixture's process.
  const runtimeBinary = path.join(directory, 'caddy');
  fs.symlinkSync(fs.realpathSync(binary), runtimeBinary);
  assert.equal(fs.realpathSync(runtimeBinary), fs.realpathSync(binary), 'fixture Caddy resolves to the qualified executable');
  assert.equal(sha256(fs.readFileSync(runtimeBinary)), binarySha256, 'staged Caddy bytes match the qualified binary');
  backend = createServer((req, res) => {
    res.setHeader('Content-Security-Policy', "default-src 'self'");
    if (req.url.startsWith('/api/')) { res.writeHead(403); res.end('original API denied'); return; }
    res.end('BACKEND ORIGINAL');
  });
  backend.listen(18182, '127.0.0.1'); await once(backend, 'listening');
  const reserve = createServer(); reserve.listen(0, '127.0.0.1'); await once(reserve, 'listening');
  const port = reserve.address().port; await new Promise(resolve => reserve.close(resolve));
  const canonical = Buffer.from(`{\n admin off\n auto_https off\n}\nhttp://127.0.0.1:${port} {\n @private query project=*\n handle @private {\n respond "HIVE original"\n }\n handle {\n reverse_proxy 127.0.0.1:18182 {\n flush_interval -1\n transport http {\n dial_timeout 5s\n }\n }\n }\n}`);
  const oldDist = fixtureDist(path.join(directory, 'old'), { 'index.html': '<script src="/assets/old-12345678.js"></script>OLD ACCEPTED UI',
    'assets/old-12345678.js': 'OLD ACCEPTED ASSET' });
  const old = createUiRelease(oldDist), oldRoot = path.join(directory, old.id); writeUiRelease(old, oldRoot);
  const next = createUiRelease(fixtureDist(path.join(directory, 'input')), [oldRoot]), staged = path.join(directory, 'staged'); writeUiRelease(next, staged);
  const manifestFile = path.join(directory, 'manifest.json'); fs.writeFileSync(manifestFile, JSON.stringify({ id: next.id, manifest: next.manifest }));
  const receipts = path.join(directory, 'receipts'); fs.mkdirSync(receipts, { mode: 0o700 });
  const operator = process.env.SOTY_UI_PUBLISHER_BUNDLE || fileURLToPath(new URL('./host-publisher.mjs', import.meta.url));
  const invoke = (value, command = 'execute', expectedExit = 0) => {
    const planFile = path.join(directory, value.action + '-plan.json');
    if (!fs.existsSync(planFile)) fs.writeFileSync(planFile, JSON.stringify(value), { flag: 'wx', mode: 0o600 });
    const result = spawnSync(process.execPath, [operator, command, planFile], { input: command === 'execute' && value.action !== 'install' ? JSON.stringify({ key: key.toString('base64') }) : '',
      encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'], timeout: 15000, maxBuffer: 1024 * 1024 });
    let output; try { output = JSON.parse(result.stdout); } catch { /* Never echo private child stdout/stderr. */ }
    const safeCodes = new Set(['host_plan_invalid', 'host_linux_required', 'host_profile_changed', 'host_directory_invalid', 'host_journal_invalid',
      'public_inventory_changed', 'public_install_changed', 'installed_release_changed', 'caddy_process_changed', 'encrypted_file_invalid',
      'encrypted_file_changed', 'encrypted_config_invalid', 'decrypted_config_mismatch', 'config_environment_requires_explicit_binding',
      'config_file_invalid', 'current_config_changed', 'candidate_bytes_changed', 'installed_config_changed', 'swap_arguments_invalid',
      'host_operation_already_exists', 'bounded_host_action_failed']);
    const responseCode = output?.ok === true ? 'ok' : safeCodes.has(output?.code) ? output.code : 'publisher_response_invalid';
    assert.equal(result.status, expectedExit, `bounded publisher CLI status (${responseCode})`);
    assert.ok(output && typeof output === 'object' && !Array.isArray(output), 'bounded publisher CLI response');
    assert.equal(output.activeReloadVerified, false); return output;
  };
  const installed = invoke({ schema: 'soty.ui.host.v1', action: 'install', transaction: 'a'.repeat(32),
    releaseId: next.id, receiptDirectory: receipts, input: staged, manifestFile, base: directory });
  const previous = await createCaddyOverlay({ caddyfile: canonical.toString(), release: old, root: oldRoot, adapt, host: '127.0.0.1' });
  const configPath = path.join(directory, 'Caddyfile'); fs.writeFileSync(configPath, previous.caddyfile, { mode: 0o644 });
  const xdg = path.join(directory, 'xdg'); fs.mkdirSync(xdg);
  caddy = spawn(runtimeBinary, ['run', '--config', configPath, '--adapter', 'caddyfile'], { stdio: ['ignore', 'ignore', 'pipe'],
    env: { PATH: '/usr/bin:/bin', HOME: directory, XDG_CONFIG_HOME: xdg, XDG_DATA_HOME: path.join(directory, 'data') } });
  caddy.stderr.resume(); const base = 'http://127.0.0.1:' + port, activeFile = path.join(xdg, 'caddy', 'autosave.json');
  const waitBody = async expected => {
    for (let attempt = 0; ; attempt++) {
      try { if ((await (await fetch(base)).text()).includes(expected)) return; } catch { /* Own process only. */ }
      assert.ok(attempt < 100 && caddy.exitCode === null, 'bounded own Caddy readiness'); await delay();
    }
  };
  const waitActive = async diskSha256 => {
    for (let attempt = 0; ; attempt++) {
      try {
        return verifyActiveDisk({ configPath, activeFile, binary, expectedBinarySha256: binarySha256,
          expectedDiskSha256: diskSha256, expectedActiveSha256: sha256(fs.readFileSync(activeFile)), expectedUid: 0 });
      } catch { assert.ok(attempt < 100, 'bounded actual active/disk readback'); await delay(); }
    }
  };
  await waitBody('OLD ACCEPTED UI'); const activeQualification = await waitActive(sha256(Buffer.from(previous.caddyfile)));
  const proc = '/proc/' + caddy.pid, fields = fs.readFileSync(proc + '/stat', 'utf8').split(') ').at(-1).split(' ');
  assert.equal(fs.readFileSync(proc + '/comm', 'utf8').trim(), 'caddy');
  const processPin = { pid: caddy.pid, uid: fs.statSync(proc).uid, startTicks: fields[19], argvSha256: sha256(fs.readFileSync(proc + '/cmdline')) };
  const prepared = await prepareOverlayReplacement({ canonical, currentDisk: fs.readFileSync(configPath), activeQualification,
    currentRelease: old, currentRoot: oldRoot, release: next, root: installed.root, adapt, host: '127.0.0.1' });
  // Current wrappers cannot be guessed away or nested, even when syntactically valid.
  for (const changed of [{ canonical: Buffer.from(canonical.toString().replace('HIVE original', 'changed ingress')) },
    { currentDisk: Buffer.from(previous.caddyfile + '\n') }, { activeQualification: { ...activeQualification, activeDiskEqual: false } }]) {
    await assert.rejects(prepareOverlayReplacement({ canonical, currentDisk: fs.readFileSync(configPath), activeQualification,
      currentRelease: old, currentRoot: oldRoot, release: next, root: installed.root, adapt, host: '127.0.0.1', ...changed }));
  }
  const encrypted = path.join(directory, 'encrypted'); fs.mkdirSync(encrypted, { mode: 0o700 });
  const pins = writeEncryptedConfigs({ directory: encrypted, ...prepared, key }); prepared.baseline.fill(0); prepared.candidate.fill(0);
  const plan = { schema: 'soty.ui.host.v1', action: 'apply', transaction: 'b'.repeat(32), releaseId: next.id, receiptDirectory: receipts,
    releaseRoot: installed.root, configPath, profile: { uid: 0, gid: 0, mode: 0o644 }, process: processPin, ...pins, overlay: prepared.overlay };
  for (const change of [value => value.process.uid++, value => value.process.startTicks += '1', value => value.process.argvSha256 = '0'.repeat(64), value => value.configPath += '-wrong']) {
    const changed = structuredClone(plan); change(changed); assert.throws(() => verifyCaddyProcess(changed), { code: 'caddy_process_changed' });
  }
  const applied = invoke(plan); assert.equal(applied.ok, true); await waitBody('NEW UI');
  const activeNext = await waitActive(pins.candidate.plainSha256); assert.equal(activeNext.activeDiskEqual, true);
  for (const [url, status, expected] of [['/assets/index-abcdefgh.js', 200, 'export const version = "new";'],
    ['/assets/old-12345678.js', 200, 'OLD ACCEPTED ASSET'], ['/api/test', 403, 'original API denied'], ['/?project=owned', 200, 'HIVE original']]) {
    const response = await fetch(base + url); assert.equal(response.status, status); assert.equal(await response.text(), expected);
  }
  assert.throws(() => executeHostPlan(plan, key), { code: 'EEXIST' }); // Lost response cannot cause a second signal.
  assert.equal(invoke(plan, 'execute', 1).code, 'host_operation_already_exists');
  assert.equal(invoke(plan, 'inspect').diskState, 'candidate');
  const inspection = inspectHostPlan(plan); assert.equal(inspection.diskState, 'candidate'); assert.equal(inspection.signalsSent, 0);
  const changedActive = path.join(directory, 'wrong-active.json'); fs.writeFileSync(changedActive, '{}');
  assert.throws(() => verifyActiveDisk({ configPath, activeFile: changedActive, binary, expectedBinarySha256: binarySha256,
    expectedDiskSha256: pins.candidate.plainSha256, expectedActiveSha256: sha256(Buffer.from('{}')), expectedUid: 0 }), { code: 'active_disk_mismatch' });
  assert.equal(invoke({ ...plan, action: 'rollback' }).ok, true); await waitBody('OLD ACCEPTED UI'); await waitActive(pins.baseline.plainSha256);
  assert.equal(caddy.exitCode, null); assert.equal(caddy.pid, processPin.pid); assert.equal(verifyCaddyProcess(plan), true);
});

test('real Caddy stable current serves new hash URLs and rollback without any config reload', { timeout: 20000 }, async t => {
  const directory = fs.realpathSync(fs.mkdtempSync(path.join(tmpdir(), 'soty-ui-stable-real-'))); fs.chmodSync(directory, 0o700);
  let caddy, backend;
  t.after(async () => {
    if (caddy?.exitCode === null) { const exited = once(caddy, 'exit'); caddy.kill('SIGTERM'); await exited; }
    if (backend?.listening) { backend.closeAllConnections(); await new Promise(resolve => backend.close(resolve)); }
    const unseal = current => { fs.chmodSync(current, 0o700); for (const entry of fs.readdirSync(current, { withFileTypes: true })) if (entry.isDirectory()) unseal(path.join(current, entry.name)); };
    unseal(directory); fs.rmSync(directory, { recursive: true, force: true });
  });
  backend = createServer((req, res) => {
    res.setHeader('Content-Security-Policy', "default-src 'self'; frame-src https://dynamic.example"); res.setHeader('X-Frame-Options', 'DENY');
    if (req.url.startsWith('/api/') || req.url.includes('blocked=1')) { res.writeHead(403); res.end('original denied'); return; }
    if (req.url.includes('redirect=1')) { res.writeHead(302, { Location: '/original' }); res.end(); return; }
    res.end('BACKEND ORIGINAL');
  });
  backend.listen(18182, '127.0.0.1'); await once(backend, 'listening');
  const reserve = createServer(); reserve.listen(0, '127.0.0.1'); await once(reserve, 'listening');
  const port = reserve.address().port; await new Promise(resolve => reserve.close(resolve));
  // Production-shaped finite legacy path boundary: stable bootstrap must add
  // the SAME public policy, not freeze the old inventory or widen to a glob.
  const canonical = `{\n admin off\n auto_https off\n}\nhttp://127.0.0.1:${port} {
    @private query project=*
    handle @private {\n respond "HIVE original"\n }
    handle /api/* {\n respond "original denied" 403\n }
    @ui path / /assets/* /sw.js /manifest.webmanifest /icon.svg
    handle @ui {\n reverse_proxy 127.0.0.1:18182 {\n flush_interval -1\n transport http {\n dial_timeout 5s\n }\n }\n }
    handle {\n respond "unknown original" 404\n }
  }`;
  const root = path.join(directory, 'soty-ui/current');
  const planFor = (release, input, expectedReleaseId, counter) => {
    const manifestFile = path.join(directory, counter + '-manifest.json'); fs.writeFileSync(manifestFile, JSON.stringify({ id: release.id, manifest: release.manifest }));
    return { schema: 'soty.ui.host.v1', action: 'publish', transaction: counter.repeat(32), releaseId: release.id,
      receiptDirectory: directory, base: directory, input, manifestFile, expectedReleaseId };
  };
  const oldInput = fixtureDist(path.join(directory, 'old'), { 'index.html': 'OLD STABLE UI' }), old = createUiRelease(oldInput);
  executeHostPlan(planFor(old, oldInput, null, '1'));
  const nextInput = fixtureDist(path.join(directory, 'next'), { 'index.html': 'NEXT STABLE UI', 'assets/new-12345678.js': 'NEW HASH BYTES',
    'fonts/manrope.woff2': 'NEW FONT', 'app-art/example/v001-123456abcdef/cover-160.123456abcdef.webp': 'NEW COVER' });
  const next = createUiRelease(nextInput, [path.join(directory, 'soty-ui/releases', old.id)]);
  const overlay = await createCaddyOverlay({ caddyfile: canonical, release: old, root, adapt, host: '127.0.0.1' });
  assert.equal((await createCaddyOverlay({ caddyfile: canonical, release: next, root, adapt, host: '127.0.0.1' })).caddyfile, overlay.caddyfile, 'stable configuration is independent of both release manifests');
  const policyPatterns = [];
  const findPatterns = value => { if (!value || typeof value !== 'object') return;
    if (['soty_ui_public', 'soty_ui_immutable'].includes(value.path_regexp?.name)) policyPatterns.push(new RegExp(value.path_regexp.pattern));
    Object.values(value).forEach(child => Array.isArray(child) ? child.forEach(findPatterns) : findPatterns(child)); };
  findPatterns(overlay.config); assert.equal(policyPatterns.length, 2);
  for (const name of ['a'.repeat(64) + '.tgz', 'A'.repeat(64) + '.tgz', 'a'.repeat(63) + '.tgz', 'a'.repeat(65) + '.tgz', 'a'.repeat(64) + '.zip', 'operator.json'])
    for (const pattern of policyPatterns) assert.equal(pattern.test('/sdk/source-app/' + name), sourceSdkArchiveName('sdk/source-app/' + name), 'stable SDK grammar matches frozen authority');
  const configPath = path.join(directory, 'Caddyfile'); fs.writeFileSync(configPath, overlay.caddyfile);
  const xdg = path.join(directory, 'xdg'); fs.mkdirSync(xdg);
  caddy = spawn(binary, ['run', '--config', configPath, '--adapter', 'caddyfile'], { stdio: ['ignore', 'ignore', 'pipe'],
    env: { PATH: '/usr/bin:/bin', HOME: directory, XDG_CONFIG_HOME: xdg, XDG_DATA_HOME: path.join(directory, 'data') } }); caddy.stderr.resume();
  const base = 'http://127.0.0.1:' + port, activeFile = path.join(xdg, 'caddy/autosave.json');
  for (let attempt = 0; ; attempt++) {
    try { if ((await (await fetch(base)).text()) === 'OLD STABLE UI' && fs.existsSync(activeFile)) break; } catch { /* Own process only. */ }
    assert.ok(attempt < 100 && caddy.exitCode === null, 'bounded stable Caddy readiness'); await delay();
  }
  const configBefore = fs.readFileSync(configPath), activeBefore = fs.readFileSync(activeFile), pid = caddy.pid;
  const nextPlan = planFor(next, nextInput, old.id, '2'); assert.equal(executeHostPlan(nextPlan).reloadRequired, false);
  for (const [url, body] of [['/', 'NEXT STABLE UI'], ['/assets/new-12345678.js', 'NEW HASH BYTES'], ['/fonts/manrope.woff2', 'NEW FONT'],
    ['/app-art/example/v001-123456abcdef/cover-160.123456abcdef.webp', 'NEW COVER']]) {
    const response = await fetch(base + url); assert.equal(response.status, 200); assert.equal(await response.text(), body);
    assert.equal(response.headers.get('content-security-policy'), "default-src 'self'; frame-src https://dynamic.example");
    assert.equal(response.headers.get('x-frame-options'), 'DENY');
    assert.equal(response.headers.get('cache-control'), url.includes('123456') ? 'public, max-age=31536000, immutable' : 'no-store');
  }
  for (const [url, method, status, body] of [['/?project=owned', 'GET', 200, 'HIVE original'], ['/?hive=owned', 'GET', 200, 'BACKEND ORIGINAL'],
    ['/', 'POST', 200, 'BACKEND ORIGINAL'], ['/fonts/manrope.woff2', 'POST', 404, 'unknown original'],
    ['/fonts/manrope.woff2?hive=owned', 'GET', 404, 'unknown original'], ['/api/test', 'GET', 403, 'original denied'],
    ['/admin', 'GET', 404, 'unknown original'], ['/.env', 'GET', 404, 'unknown original'], ['/skills/private/SKILL.md', 'GET', 404, 'unknown original'],
    ['/?blocked=1', 'GET', 403, 'original denied'], ['/', 'HEAD', 200, '']]) {
    const response = await fetch(base + url, { method }); assert.equal(response.status, status, method + ' ' + url); assert.equal(await response.text(), body);
  }
  assert.equal((await fetch(base + '/?redirect=1', { redirect: 'manual' })).status, 302);
  const response = await fetch(base + '/assets/new-12345678.js', { headers: { Range: 'bytes=0-2' } }); assert.equal(response.status, 206); assert.equal(await response.text(), 'NEW');
  const rollbackInput = path.join(directory, 'rollback'); const rollback = createUiRelease(oldInput, [path.join(directory, 'soty-ui/releases', next.id)]); writeUiRelease(rollback, rollbackInput);
  executeHostPlan(planFor(rollback, rollbackInput, next.id, '3'));
  assert.equal(await (await fetch(base)).text(), 'OLD STABLE UI'); assert.equal(await (await fetch(base + '/assets/new-12345678.js')).text(), 'NEW HASH BYTES');
  assert.equal(caddy.pid, pid); assert.equal(caddy.exitCode, null); assert.deepEqual(fs.readFileSync(configPath), configBefore); assert.deepEqual(fs.readFileSync(activeFile), activeBefore);
});
