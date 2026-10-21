import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { createHash, timingSafeEqual } from 'node:crypto';
import { createTransitClientRuntime, executeTransitClientAction, normalizeTransitClientProfile, transitCheckedProfile, transitUnderlayProfile } from './agent-modules/transit-client.mjs';

const profile = () => ({
  inbounds: [{ type: 'tun', tag: 'existing-phone' }],
  outbounds: [{ type: 'vless', tag: 'entry', server: 'entry.example', server_port: 443,
    uuid: '11111111-1111-4111-8111-111111111111', tls: { enabled: true, insecure: false } }],
  dns: { servers: [{ type: 'tcp', tag: 'exit', server: '1.1.1.1', detour: 'entry' }], final: 'exit' },
  route: { final: 'entry', rules: [{ ip_is_private: true, action: 'reject' }] },
});

function fixture(overrides = {}) {
  let stored = profile(), spawns = 0, saved = 0, runWrites = 0;
  const children = [];
  const deps = {
    installCore: async () => ({ executable: 'pinned-synthetic-core' }), verifyCore: async () => true,
    readProfile: async () => structuredClone(stored),
    saveCheckedProfile: async (_core, value) => { saved++; stored = structuredClone(value); },
    underlayInterface: async () => 'Ethernet', probe: async () => ({ exitIp: '203.0.113.7' }),
    writeRunProfile: async () => { runWrites++; return 'owned-private-config'; },
    spawnCore: () => {
      spawns++;
      const child = new EventEmitter(); child.pid = spawns; child.exitCode = null;
      child.kill = () => { child.exitCode = 0; queueMicrotask(() => child.emit('exit', 0)); return true; };
      children.push(child); return child;
    },
    confirmRunning: async () => true, confirmStopped: async () => true,
    wait: async () => {}, onceExit: child => child.exitCode !== null ? Promise.resolve() : new Promise(resolve => child.once('exit', resolve)),
    ...overrides,
  };
  return { runtime: createTransitClientRuntime(deps), children, get spawns() { return spawns; }, get saved() { return saved; },
    get runWrites() { return runWrites; }, get stored() { return stored; } };
}

test('adoption preserves the existing access identity and DNS while discarding imported listeners', () => {
  const original = profile(), before = JSON.stringify(original);
  const normalized = normalizeTransitClientProfile(original);
  assert.deepEqual(normalized.outbounds, original.outbounds);
  assert.deepEqual(normalized.dns, original.dns);
  assert.deepEqual(normalized.route, original.route);
  assert.equal(normalized.inbounds.length, 1);
  assert.equal(normalized.inbounds[0].interface_name, 'SotyTransit');
  assert.equal(JSON.stringify(original), before);
  for (const bad of [
    { ...profile(), outbounds: [{ type: 'direct', tag: 'entry' }] },
    { ...profile(), experimental: { clash_api: { external_controller: '0.0.0.0:9000' } } },
  ]) assert.throws(() => normalizeTransitClientProfile(bad), /transit-profile-invalid/u);
  const insecure = profile(); insecure.outbounds[0].tls.insecure = true;
  assert.throws(() => normalizeTransitClientProfile(insecure), /transit-profile-invalid/u);
});

test('a failed HTTPS leaf cannot win a later urltest and filtering never rewrites the saved access', () => {
  const original = profile();
  original.outbounds.push({ ...original.outbounds[0], tag: 'working', uuid: '22222222-2222-4222-8222-222222222222' });
  original.outbounds.push({ type: 'urltest', tag: 'auto', outbounds: ['entry', 'working'], url: 'https://example.com/' });
  original.route.final = 'auto'; original.dns.servers[0].detour = 'auto';
  const before = JSON.stringify(original);
  const checked = transitCheckedProfile(transitUnderlayProfile(original, 'Ethernet'), ['working']);
  assert.deepEqual(checked.outbounds.map(row => row.tag), ['working', 'auto']);
  assert.deepEqual(checked.outbounds[1].outbounds, ['working']);
  assert.equal(checked.outbounds[0].bind_interface, 'Ethernet');
  assert.equal(checked.outbounds[0].uuid, original.outbounds[1].uuid);
  assert.equal(JSON.stringify(original), before);
  assert.throws(() => transitCheckedProfile(original, []), /transit-probe-failed/u);
});

test('two concurrent enables create one owned process; stop/resume retains the access credential', async () => {
  const f = fixture(); const identity = f.stored.outbounds[0].uuid;
  await Promise.all([f.runtime.start(), f.runtime.start()]);
  assert.equal(f.spawns, 1); assert.equal((await f.runtime.status()).active, true);
  await assert.rejects(f.runtime.configure(profile()), /transit-stop-before-import/u);
  await f.runtime.stop(); await f.runtime.stop();
  assert.equal((await f.runtime.status()).active, false);
  await f.runtime.start();
  assert.equal(f.spawns, 2); assert.equal(f.saved, 0);
  assert.equal(f.stored.outbounds[0].uuid, identity);
  await f.runtime.stop();
});

test('failed preflight cannot write a system profile or spawn a tunnel and never exposes credentials', async () => {
  const secret = 'private-profile-capability-never-in-status';
  const f = fixture({ probe: async () => { throw new Error(secret); } });
  await assert.rejects(f.runtime.start(), /transit-client-failed/u);
  assert.equal(f.spawns, 0); assert.equal(f.runWrites, 0);
  assert.equal((await f.runtime.status()).active, false);
  assert.ok(!JSON.stringify(await f.runtime.status()).includes(secret));
});

test('a tunnel failing OS/IP confirmation is stopped and is never reported connected', async () => {
  const f = fixture({ confirmRunning: async () => false });
  await assert.rejects(f.runtime.start(), /transit-start-unconfirmed/u);
  assert.equal(f.children[0].exitCode, 0);
  const result = await f.runtime.status();
  assert.equal(result.active, false); assert.equal(result.running, false);
  assert.equal(result.phase, 'error');
});

test('a failed stop retains UNKNOWN and a live process instead of announcing OFF', async () => {
  const f = fixture(); await f.runtime.start();
  f.children[0].kill = () => false;
  await assert.rejects(f.runtime.stop(), /transit-stop-unconfirmed/u);
  const result = await f.runtime.status();
  assert.equal(result.phase, 'unknown'); assert.equal(result.running, true); assert.equal(result.active, false);
  f.children[0].exitCode = 0; f.children[0].emit('exit', 0);
});

test('leftover system routes also prevent a repeated stop from silently clearing UNKNOWN', async () => {
  let stopped = true;
  const f = fixture({ confirmStopped: async () => stopped });
  await f.runtime.start(); stopped = false;
  await assert.rejects(f.runtime.stop(), /transit-stop-unconfirmed/u);
  await assert.rejects(f.runtime.stop(), /transit-stop-unconfirmed/u);
  assert.equal((await f.runtime.status()).phase, 'unknown');
});

test('unverified binary cannot import a profile or start a process', async () => {
  const f = fixture({ verifyCore: async () => false });
  await assert.rejects(f.runtime.configure(profile()), /transit-core-pin-mismatch/u);
  await assert.rejects(f.runtime.start(), /transit-core-pin-mismatch/u);
  assert.equal(f.spawns, 0); assert.equal(f.saved, 0);
});

test('a signal-terminated owned process is stopped; inherited routes are UNKNOWN after agent restart', async () => {
  const f = fixture(); await f.runtime.start();
  f.children[0].kill = () => {
    f.children[0].signalCode = 'SIGTERM';
    queueMicrotask(() => f.children[0].emit('exit', null, 'SIGTERM')); return true;
  };
  assert.equal((await f.runtime.stop()).phase, 'off');
  const orphan = fixture({ confirmStopped: async () => false });
  assert.equal((await orphan.runtime.status()).phase, 'unknown');
  assert.equal((await orphan.runtime.status()).active, false);
});

test('a restarted agent can stop only the process returned by verified ownership recovery', async () => {
  const owned = new EventEmitter(); owned.pid = 88; owned.exitCode = null;
  let killed = 0, recovered = 0;
  owned.kill = () => { killed++; owned.exitCode = 0; queueMicrotask(() => owned.emit('exit', 0)); };
  const f = fixture({ recoverOwned: async () => { recovered++; return owned; } });
  assert.equal((await f.runtime.status()).phase, 'unknown');
  assert.equal((await f.runtime.status()).running, true);
  assert.equal((await f.runtime.stop()).phase, 'off');
  assert.equal(killed, 1); assert.equal(recovered, 1); assert.equal(f.spawns, 0);
});

test('public device metadata and an installer hash cannot grant start or export access', async () => {
  const token = Buffer.alloc(32, 7).toString('base64url');
  const digest = value => createHash('sha256').update(value).digest();
  const hash = digest(token); let calls = 0;
  const authority = { deviceId: 'owned-machine', verifyControlToken: value => timingSafeEqual(digest(value), hash),
    runtime: { async status() { calls++; return { active: false }; }, async exportProfile() { calls++; return profile(); }, async start() { calls++; } } };
  for (const controlToken of [undefined, hash.toString('hex'), Buffer.alloc(32, 8).toString('base64url')]) {
    for (const action of ['start', 'export']) await assert.rejects(executeTransitClientAction({ action, deviceId: 'owned-machine', controlToken }, authority), /transit-control-not-paired/u);
  }
  assert.equal(calls, 0);
  await assert.rejects(executeTransitClientAction({ action: 'export', deviceId: 'foreign-machine', controlToken: token }, authority), /transit-device-mismatch/u);
  assert.equal(calls, 0);
  assert.deepEqual(await executeTransitClientAction({ action: 'authorize', deviceId: 'owned-machine', controlToken: token }, authority), { runtime: { active: false } });
  assert.equal(calls, 1);
});
