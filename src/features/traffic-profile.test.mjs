import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';
import { buildSingBoxProfile } from './sing-box-profile.mjs';

const profile = 'vless://11111111-1111-4111-8111-111111111111@gateway.example:443?security=tls&type=ws&path=%2Fapi%2Ftraffic%2Fws&host=cdn.example&sni=gateway.example#Phone';

test('shared-internet profile preserves TLS/WS identity and tunnels application DNS without a direct outbound', () => {
  const result = buildSingBoxProfile(profile);
  assert.equal(result.outbounds.length, 1);
  assert.equal(result.outbounds[0].type, 'vless');
  assert.equal(result.outbounds[0].tls.insecure, false);
  assert.equal(result.outbounds[0].transport.path, '/api/traffic/ws');
  assert.equal(result.outbounds[0].transport.headers.Host, 'cdn.example');
  assert.equal(result.dns.final, 'exit-dns');
  assert.equal(result.dns.servers.find(server => server.tag === 'exit-dns').detour, result.route.final);
  assert.equal(result.outbounds[0].domain_resolver.server, 'entry-bootstrap');
  assert.ok(result.route.rules.some(rule => rule.ip_is_private && rule.action === 'reject'));
});

test('unsupported or unsafe profiles fail without echoing credentials or silently changing transport', () => {
  for (const value of [profile.replace('security=tls', 'security=none'), profile.replace('type=ws', 'type=xhttp'), 'not a profile']) {
    assert.throws(() => buildSingBoxProfile(value), error => !error.message.includes('11111111') && /profile|transport/u.test(error.message));
  }
  assert.throws(() => buildSingBoxProfile(profile.replace('#Phone', '&flow=bad#Phone')), /invalid-sing-box-profile/u);
});

async function apiFixture(response) {
  const source = await readFile(new URL('./traffic-fabric.ts', import.meta.url), 'utf8');
  const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } }).outputText;
  const module = { exports: {} }, actions = [], storage = new Map();
  const location = { href: 'https://shell.example/', origin: 'https://shell.example' };
  vm.runInNewContext(compiled, { module, exports: module.exports, URL, TextEncoder, TextDecoder, Uint8Array, btoa, atob, AbortController,
    localStorage: { getItem: key => storage.get(key) ?? null, setItem: (key, value) => storage.set(key, value) },
    window: { location, history: { replaceState(_state, _title, path) { location.href = new URL(path, location.origin).href; } }, setTimeout, clearTimeout },
    fetch: async (_url, init) => { actions.push(JSON.parse(init.body)); return { ok: response.ok !== false, status: response.status ?? 200, json: async () => response }; },
  });
  return { api: module.exports, actions, storage, location };
}

test('resume sends start only; process confirmation is required for both state changes', async () => {
  const started = await apiFixture({ runtime: { active: true } });
  await started.api.startTraffic();
  assert.deepEqual(started.actions, [{ action: 'start' }]);
  const stillRunning = await apiFixture({ runtime: { active: true } });
  await assert.rejects(stillRunning.api.stopTraffic(), /traffic-stop-unconfirmed/u);
  const missing = await apiFixture({});
  await assert.rejects(missing.api.startTraffic(), /traffic-start-unconfirmed/u);
  await assert.rejects(missing.api.stopTraffic(), /traffic-stop-unconfirmed/u);
  const stopped = await apiFixture({ runtime: { active: false } });
  await stopped.api.stopTraffic();
  const failed = await apiFixture({ ok: false, status: 503, error: 'operation-unavailable' });
  await assert.rejects(failed.api.revokeTrafficClient('phone'), /operation-unavailable/u);
});

test('a generated QR invite adopts the same credential once and removes it from the address', async () => {
  const f = await apiFixture({});
  f.location.href = f.api.trafficProfileInviteUrl(profile);
  assert.equal(f.api.adoptTrafficProfileFromUrl(), true);
  assert.equal(f.api.readTrafficProfile(), profile);
  assert.equal(new URL(f.location.href).searchParams.has('trafficProfile'), false);
  assert.equal(f.api.adoptTrafficProfileFromUrl(), false);
});
