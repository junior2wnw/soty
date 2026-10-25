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
