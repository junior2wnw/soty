import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { readFile } from 'node:fs/promises';
import { EventEmitter } from 'node:events';
import { isAbsolute, join } from 'node:path';
import { pathToFileURL } from 'node:url';

const variant = process.env.SOTY_TRANSIT_SOURCE_VARIANT || 'normal';
assert.ok(['normal', 'parent-route', 'parent-stop'].includes(variant));
const parent = process.env.SOTY_TRANSIT_PARENT_ROOT;
if (variant !== 'normal') assert.ok(typeof parent === 'string' && isAbsolute(parent));
const routeFile = variant === 'parent-route' ? pathToFileURL(join(parent, 'scripts/soty-connector.mjs')) : new URL('./soty-connector.mjs', import.meta.url);
const clientFile = variant === 'parent-stop' ? pathToFileURL(join(parent, 'scripts/agent-modules/transit-client.mjs')) : new URL('./agent-modules/transit-client.mjs', import.meta.url);
const connector = (await readFile(routeFile, 'utf8')).replace(/\r\n/gu, '\n');
const { createTransitClientRuntime } = await import(clientFile);
function actual(name) {
  const marker = 'async function ' + name + '(';
  const start = connector.indexOf(marker), end = connector.indexOf('\nasync function ', start + marker.length);
  assert.ok(start >= 0 && end > start); assert.equal(connector.indexOf(marker, start + marker.length), -1);
  return connector.slice(start, end);
}
function route(result = { exitCode: 0, stdout: 'no\n' }) {
  const calls = [];
  const context = vm.createContext({
    process: { platform: 'win32' }, windowsBuiltInPowerShellPath: () => 'controlled-powershell',
    runChild: async (file, args, options) => { calls.push({ file, args, options }); return result; },
  });
  new vm.Script(['transitCommand', 'transitPowerShell', 'transitRoutePresent'].map(actual).join('\n') +
    '\nglobalThis.present=transitRoutePresent;').runInContext(context);
  return { present: context.present, calls };
}
test('route query preserves all six prefixes and requests all routes with terminating provider errors', async () => {
  const r = route(); assert.equal(await r.present(), false); assert.equal(r.calls.length, 1);
  const { file, args, options } = r.calls[0];
  assert.equal(file, 'controlled-powershell'); assert.equal(options.timeoutMs, 15000);
  assert.deepEqual(Array.from(args.slice(0, -1)), ['-NoLogo', '-NoProfile', '-NonInteractive', '-Command']);
  const query = args.at(-1);
  assert.match(query, /Get-NetRoute -ErrorAction Stop \| Where-Object \{\$_\.InterfaceAlias -eq 'SotyTransit' -and \$_\.DestinationPrefix -in/u);
  assert.doesNotMatch(query, /SilentlyContinue|-InterfaceAlias/u);
  assert.deepEqual(query.match(/DestinationPrefix -in @\(([^)]*)\)/u)[1].match(/'[^']*'/gu),
    ["'0.0.0.0/0'", "'0.0.0.0/1'", "'128.0.0.0/1'", "'::/0'", "'::/1'", "'8000::/1'"]);
});
test('healthy empty and present route outputs retain false and true', async () => {
  assert.equal(await route({ exitCode: 0, stdout: 'no\r\n' }).present(), false);
  assert.equal(await route({ exitCode: 0, stdout: 'yes\r\n' }).present(), true);
});
test('unexpected route output is denied rather than reported OFF', async () => {
  for (const stdout of ['', 'True', 'yes\nno', 'provider-warning\nno']) {
    await assert.rejects(route({ exitCode: 0, stdout }).present(), /transit-route-unconfirmed/u);
  }
});
test('provider process failure is propagated through the actual PowerShell command chain', async () => {
  await assert.rejects(route({ exitCode: 1, stdout: '' }).present(), /transit-command-failed/u);
});
const profile = () => ({
  outbounds: [{ type: 'vless', tag: 'entry', server: 'entry.example', server_port: 443,
    uuid: '11111111-1111-4111-8111-111111111111', tls: { enabled: true, insecure: false } }],
  dns: { servers: [{ tag: 'dns', detour: 'entry' }], final: 'dns' }, route: { final: 'entry' },
});
function fixture() {
  let confirm = async () => true, confirms = 0, spawns = 0, kills = 0;
  const runtime = createTransitClientRuntime({
    installCore: async () => ({}), verifyCore: async () => true,
    readProfile: async () => profile(), underlayInterface: async () => 'Ethernet',
    probe: async () => ({}), writeRunProfile: async () => 'controlled-run.json',
    spawnCore: () => {
      spawns++; const child = new EventEmitter(); child.exitCode = null; child.pid = 321;
      child.kill = () => { kills++; child.exitCode = 0; queueMicrotask(() => child.emit('exit', 0)); return true; };
      return child;
    },
    confirmRunning: async () => true,
    confirmStopped: async () => { confirms++; return confirm(); },
    wait: async () => {}, onceExit: child => child.exitCode === null ? new Promise(resolve => child.once('exit', resolve)) : Promise.resolve(),
  });
  return { runtime, setConfirm: value => { confirm = value; }, get confirms() { return confirms; },
    get spawns() { return spawns; }, get kills() { return kills; } };
}
for (const active of [false, true]) for (const failure of ['false', 'throw']) {
  test((active ? 'active' : 'inactive') + ' stop confirmation ' + (failure === 'throw' ? 'provider failure' : 'false') + ' remains UNKNOWN', async () => {
    const f = fixture(); if (active) await f.runtime.start();
    f.setConfirm(failure === 'false' ? async () => false : route({ exitCode: 1, stdout: '' }).present);
    await assert.rejects(f.runtime.stop(), /transit-stop-unconfirmed/u);
    const result = await f.runtime.status();
    assert.equal(result.phase, 'unknown'); assert.equal(result.active, false); assert.equal(result.running, false);
    assert.equal(result.lastError, 'transit-stop-unconfirmed'); assert.equal(f.confirms, 1);
    assert.equal(f.spawns, active ? 1 : 0); assert.equal(f.kills, active ? 1 : 0);
  });
}
test('confirmed stops retain OFF for both no-child and owned-child paths', async () => {
  for (const active of [false, true]) {
    const f = fixture(); if (active) await f.runtime.start();
    const result = await f.runtime.stop();
    assert.equal(result.phase, 'off'); assert.equal(result.lastError, ''); assert.equal(result.running, false);
    assert.equal(f.kills, active ? 1 : 0);
  }
});
