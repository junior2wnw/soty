import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { webcrypto } from 'node:crypto';
import vm from 'node:vm';
import ts from 'typescript';
import * as profiles from '../../scripts/agent-modules/transit-client.mjs';
import * as accountProfiles from './transit-profile-store.mjs';

// The actual TypeScript controller runs against inert UI and Agent ports. These
// tests create no listener, real browser identity, downloaded file or OS process.
const source = await readFile(process.env.SOTY_TRANSIT_OWNER_SOURCE ?? new URL('./transit-client.ts', import.meta.url), 'utf8');
const compiled = ts.transpileModule(source, {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS },
}).outputText;
const deferred = () => { let resolve, settled = false; const promise = new Promise(yes => { resolve = value => { settled = true; yes(value); }; }); return { promise, resolve, get settled() { return settled; } }; };
const turn = () => new Promise(resolve => setImmediate(resolve));
const token = Buffer.alloc(32, 7).toString('base64url');
const profile = () => ({
  outbounds: [{ type: 'vless', tag: 'entry', server: 'entry.example', server_port: 443,
    uuid: '11111111-1111-4111-8111-111111111111', tls: { enabled: true, insecure: false } }],
  dns: { servers: [{ type: 'tcp', tag: 'exit', server: '1.1.1.1', detour: 'entry' }], final: 'exit' },
  route: { final: 'entry' },
});
class ElementPort {
  constructor(tagName) { this.tagName = tagName; }
  children = []; handlers = new Map(); isConnected = true; textContent = ''; className = ''; dataset = {};
  classList = { add() {} };
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.children = [...children]; }
  setAttribute() {}
  addEventListener(type, handler) { this.handlers.set(type, handler); }
  emit(type) { this.handlers.get(type)?.(); }
  remove() { this.isConnected = false; }
  click() { if (!this.disabled) this.emit('click'); }
}
function fixture({ paired = true, fresh = false, issuing = false } = {}) {
  let accountId = 'account-A', selectedRead, stateReads = 0, digestRead, importRead, revision = 1, observer;
  let payload = { transitClient: { schema: 'soty.transit-profile.v1', profile: profile() },
    transitControllers: paired ? { 'owned-host': token } : {} };
  if (fresh) delete payload.transitClient;
  let heldProfile;
  const issuedProfiles = [];
  const calls = [], installers = [], downloads = [], intervals = new Map(); let nextTimer = 0, dialog;
  const state = { configured: false, running: false, active: false, phase: 'off', version: '1.13.15', entryCount: 1 };
  const response = value => ({ ok: true, async json() { return value; } });
  const client = {
    async extension(operation, args, context) {
      assert.equal(args.expectedAccountId, accountId); assert.equal(context.expectedAccountId, accountId);
      if (operation === 'transit.access.status') return { enabled: issuing };
      assert.equal(operation, 'transit.access.profile'); issuedProfiles.push(accountId);
      if (heldProfile) { heldProfile.entered.resolve(); await heldProfile.release.promise; }
      return { profile: profile() };
    },
    async getLocalState() {
      const captured = { accountId };
      if (++stateReads === selectedRead?.at) { const wait = selectedRead; selectedRead = null; wait.entered.resolve(); await wait.release.promise; }
      return captured;
    },
    async loadVault(expected) { assert.equal(expected, accountId); return { revision, payload: structuredClone(payload) }; },
    async saveVault(value, expectedRevision, expected) {
      assert.equal(expected, accountId); assert.equal(expectedRevision, revision);
      payload = structuredClone(value); revision++; return { revision };
    },
  };
  const ports = {
    '../core/connect-client': { accountClient: client, observeAccount(fn) { observer = fn; return () => { observer = null; }; } },
    '../world/dialogs': { createDialog(_title, onClose) {
      const element = new ElementPort('dialog'), body = new ElementPort('div');
      dialog = { element, body, close() { if (!element.isConnected) return; element.remove(); onClose(); } };
      return dialog;
    } },
    '../features/connector': { async checkLocalCompanionAgent() { return { deviceId: 'owned-host' }; },
      downloadAgentInstallerForDevice(...args) { installers.push(args); } },
    '../../scripts/agent-modules/transit-client.mjs': profiles,
    './transit-profile-store.mjs': accountProfiles,
    './transit-client.css': {},
  };
  const exported = { exports: {} };
  const context = vm.createContext({ module: exported, exports: exported.exports,
    require(name) { assert.ok(Object.hasOwn(ports, name), `unexpected controller import ${name}`); return ports[name]; },
    document: { visibilityState: 'visible', body: new ElementPort('body'), createElement: tag => new ElementPort(tag) },
    structuredClone, Blob, URL: { createObjectURL(blob) { downloads.push(blob); return 'blob:profile-fixture'; }, revokeObjectURL() {} },
    window: { setTimeout() {} },
    navigator: { userAgent: 'Windows', platform: 'Win32' },
    setInterval(fn) { const id = ++nextTimer; intervals.set(id, fn); return id; }, clearInterval(id) { intervals.delete(id); },
    AbortSignal: { timeout() {} }, TextEncoder, Uint8Array, btoa: value => Buffer.from(value, 'binary').toString('base64'),
    crypto: { randomUUID: () => 'synthetic-id', getRandomValues: array => webcrypto.getRandomValues(array),
      subtle: { async digest(...args) {
        if (digestRead) { const wait = digestRead; digestRead = null; wait.entered.resolve(); await wait.release.promise; }
        return webcrypto.subtle.digest(...args);
      } } },
    async fetch(_url, options) {
      if (options.method === 'GET') return response({ ok: true, deviceId: 'owned-host', supported: true, runtime: { ...state } });
      const body = JSON.parse(options.body); calls.push(body);
      assert.equal(body.deviceId, 'owned-host'); assert.equal(body.controlToken, paired ? token : payload.transitControllers['owned-host']);
      if (body.action === 'import') {
        if (importRead) { const wait = importRead; importRead = null; wait.entered.resolve(); await wait.release.promise; }
        state.configured = true;
      } else if (body.action === 'start') { state.running = state.active = true; state.phase = 'connected'; }
      else if (body.action === 'stop') { state.running = state.active = false; state.phase = 'off'; }
      else assert.equal(body.action, 'authorize');
      return response({ ok: true, runtime: { ...state } });
    },
  });
  new vm.Script(compiled, { filename: 'actual-transit-client.ts' }).runInContext(context);
  function hold(slot) { const wait = { entered: deferred(), release: deferred() }; slot(wait); return wait; }
  const descendants = node => [node, ...node.children.flatMap(descendants)];
  return {
    calls, installers, downloads, state, open: () => exported.exports.openTransitClient(),
    issuedProfiles, get payload() { return payload; }, holdProfile: () => hold(wait => { heldProfile = wait; }),
    close: () => dialog.close(),
    select(next) { accountId = next; observer?.({ accountId }); },
    holdImport: () => hold(wait => { importRead = wait; }),
    holdStateAt: at => hold(wait => { selectedRead = { ...wait, at }; }),
    holdDigest: () => hold(wait => { digestRead = wait; }),
    button(label) { const value = descendants(dialog.body).find(node => node.tagName === 'button' && node.textContent === label); assert.ok(value, `missing actual ${label} control`); return value; },
    async settle(predicate = () => true) { for (let index = 0; index < 30; index++) { await turn(); if (predicate()) return; } assert.fail('actual controller did not settle'); },
    get closed() { return !dialog.element.isConnected; },
    get observing() { return typeof observer === 'function'; },
    get intervalCount() { return intervals.size; },
  };
}

test('actual TRANSIT controller imports then starts and explicitly stops with the current capability', async () => {
  const f = fixture(); await f.open();
  const held = f.holdImport(); f.button('Включить').click(); await f.settle(() => held.entered.settled);
  assert.equal(f.calls.filter(call => call.action === 'start').length, 0);
  held.release.resolve(); await f.settle(() => f.state.active);
  await f.settle(); f.button('Выключить').click(); await f.settle(() => !f.state.running); await f.settle();
  assert.deepEqual(f.calls.filter(call => call.action !== 'authorize').map(call => call.action), ['import', 'start', 'stop']);
  assert.equal(f.state.phase, 'off'); f.close();
});

test('a new person obtains their profile without enrolling, importing into or starting a computer', async () => {
  const f = fixture({ paired: false, fresh: true, issuing: true }); await f.open();
  f.button('Получить профиль sing-box').click();
  await f.settle(() => !!f.payload.transitClient); await f.settle();
  assert.deepEqual(f.issuedProfiles, ['account-A']); assert.equal(f.installers.length, 0);
  assert.equal(f.calls.length, 0); assert.ok(f.button('Скачать профиль sing-box')); f.close();
});

test('downloading an already saved gVisor profile uses the system stack without changing its credentials or vault', async () => {
  const f = fixture({ paired: false });
  f.payload.transitClient.profile.inbounds = [{ type: 'tun', tag: 'transit-soty', interface_name: 'SotyTransit', stack: 'mixed' }];
  const before = structuredClone(f.payload);
  await f.open(); f.button('Скачать профиль sing-box').click();
  const downloaded = JSON.parse(await f.downloads[0].text());
  assert.equal(downloaded.inbounds[0].stack, 'system');
  assert.equal(downloaded.inbounds[0].interface_name, undefined);
  assert.deepEqual(downloaded.outbounds, before.transitClient.profile.outbounds);
  assert.deepEqual(downloaded.dns, before.transitClient.profile.dns);
  assert.deepEqual(downloaded.route, before.transitClient.profile.route);
  assert.deepEqual(f.payload, before);
  assert.equal(f.calls.length, 0); f.close();
});

test('a delayed new profile cannot be saved after account A-B-A', async () => {
  const f = fixture({ paired: false, fresh: true, issuing: true }); await f.open();
  const held = f.holdProfile(); f.button('Получить профиль sing-box').click(); await f.settle(() => held.entered.settled);
  f.select('account-B'); f.select('account-A'); held.release.resolve(); await f.settle();
  assert.equal(f.payload.transitClient, undefined); assert.equal(f.closed, true); assert.equal(f.calls.length, 0);
});

for (const retire of ['account-switch', 'dialog-close', 'account-ABA']) {
  test(`actual TRANSIT paused import cannot start after ${retire}`, async () => {
    const f = fixture(); await f.open();
    const held = f.holdImport(); f.button('Включить').click(); await f.settle(() => held.entered.settled);
    if (retire === 'dialog-close') f.close();
    else { f.select('account-B'); if (retire === 'account-ABA') f.select('account-A'); }
    assert.equal(f.closed, true); held.release.resolve();
    for (let index = 0; index < 10; index++) await turn();
    assert.equal(f.calls.filter(call => call.action === 'start').length, 0, 'retired owner must not send a late start');
    assert.equal(f.state.running, false);
    assert.deepEqual(f.calls.filter(call => call.action !== 'authorize').map(call => call.action), ['import']);
  });
}

test('actual TRANSIT account read cannot authorize after its dialog was disposed while awaiting durable state', async () => {
  const f = fixture();
  // Owner is the first state read; the next read happens inside actual refresh.
  const held = f.holdStateAt(2), opening = f.open();
  await f.settle(() => held.entered.settled); f.close(); held.release.resolve(); await opening;
  assert.equal(f.closed, true); assert.equal(f.calls.length, 0, 'no capability POST may follow a stale current() result');
});

test('actual TRANSIT installer is not downloaded after closure during capability hashing', async () => {
  const f = fixture({ paired: false }); await f.open();
  const held = f.holdDigest(); f.button('Разрешить управление этим компьютером').click();
  await f.settle(() => held.entered.settled); f.close(); held.release.resolve();
  for (let index = 0; index < 10; index++) await turn();
  assert.equal(f.installers.length, 0); assert.equal(f.calls.length, 0);
});

test('actual TRANSIT closure during initial account read does not register a retired observer', async () => {
  const f = fixture(), held = f.holdStateAt(1), opening = f.open();
  await f.settle(() => held.entered.settled); f.close(); held.release.resolve(); await opening;
  assert.equal(f.closed, true); assert.equal(f.observing, false); assert.equal(f.intervalCount, 0);
  assert.equal(f.calls.length, 0);
});
