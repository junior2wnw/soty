import test from 'node:test';
import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, join, resolve, sep } from 'node:path';
import WebSocket from 'ws';
import { createClientWithStorage } from '../../modules/connect/browser/client.mjs';

const origin = 'http://localhost:8080';
const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(predicate) {
  const deadline = Date.now() + 3000;
  while (Date.now() < deadline) { const value = predicate(); if (value) return value; await wait(10); }
  throw new Error('human_call_test_wait');
}
async function fixture(t, { origins = [origin] } = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-human-call-entry-'));
  await writeFile(join(directory, 'index.html'), '<!doctype html><title>Owned synthetic call entry</title>');
  const environment = Object.fromEntries(['SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'Path', 'PATH']
    .filter(key => process.env[key] !== undefined).map(key => [key, process.env[key]]));
  const child = fork(new URL('../index.js', import.meta.url), [], { execArgv: [], silent: true, env: {
    ...environment, HOST: '127.0.0.1', PORT: '0', DATA_DIR: directory, SOTY_DIST_DIR: directory,
    SOTY_CONNECT_ORIGINS: origins.join(','), SOTY_UNIVERSAL_APPS_ENABLED: 'false',
  } });
  child.stdout.resume(); let startupDiagnostics = '';
  child.stderr.on('data', bytes => { if (startupDiagnostics.length < 16000) startupDiagnostics += bytes.toString(); });
  const sockets = new Set(), clients = [];
  t.after(async () => {
    for (const socket of sockets) socket.terminate();
    for (const client of clients) client.dispose();
    if (child.exitCode === null && child.signalCode === null) {
      const stopped = new Promise(resolve => child.once('exit', resolve));
      child.disconnect(); const deadline = setTimeout(() => child.kill('SIGKILL'), 3000);
      await stopped; clearTimeout(deadline);
    }
    assert.equal(child.exitCode, 0, 'the owned real server must shut down normally');
    assert.ok(resolve(directory).startsWith(resolve(tmpdir()) + sep) && basename(directory).startsWith('soty-human-call-entry-'));
    await rm(directory, { recursive: true, force: true });
  });
  const port = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('human_call_entry_not_ready')), 6000);
    child.on('message', value => { if (value?.type === 'soty:ready') { clearTimeout(timer); resolve(value.port); } });
    child.once('exit', () => {
      clearTimeout(timer);
      const code = startupDiagnostics.match(/code: ['"]([A-Z_a-z0-9]+)['"]/)?.[1]
        ?? startupDiagnostics.match(/(?:Error|TypeError): ([a-z][a-z0-9_]+)\b/)?.[1] ?? 'unknown';
      reject(new Error(`human_call_entry_exited:${code}`));
    });
  });
  async function browser() {
    let local = null;
    const storage = { async read() { return structuredClone(local); },
      async claim(candidate) { local ??= structuredClone(candidate); return structuredClone(local); },
      async compareAndSwap(revision, candidate) {
        assert.equal(local.localRevision, revision); local = structuredClone(candidate); return structuredClone(local);
      } };
    const client = createClientWithStorage({ projectId: 'soty', endpoint: `http://127.0.0.1:${port}/api/connect/rpc`,
      fetch: (url, options) => fetch(url, { ...options, headers: { ...options.headers, origin } }) }, storage);
    clients.push(client); const account = await client.bootstrap('Owned synthetic call user');
    await client.extension('world.profile.get');
    const rpc = (operation, args) => client.extension(operation, args, { expectedAccountId: account.accountId });
    return { client, account, rpc };
  }
  async function socket(admission, extra = {}) {
    const ws = new WebSocket(`ws://127.0.0.1:${port}/ws/human-call/${admission.callId}`, { headers: { origin } });
    sockets.add(ws); const messages = [];
    ws.on('message', raw => messages.push(JSON.parse(raw.toString())));
    await new Promise((resolve, reject) => { ws.once('open', resolve); ws.once('error', reject); });
    ws.send(JSON.stringify({ type: 'call.bind', callId: admission.callId, epoch: admission.epoch,
      grantRef: admission.grantRef, ...extra }));
    return { ws, messages, input: value => ws.send(JSON.stringify({ callId: admission.callId, epoch: admission.epoch, ...value })) };
  }
  return { child, port, browser, socket, sockets };
}

test('real server entry admits two signed contacts, relays canonical signaling and closes publication on block', { timeout: 15000 }, async t => {
  const f = await fixture(t), a = await f.browser(), b = await f.browser();
  const capabilities = await fetch(`http://127.0.0.1:${f.port}/api/calls/capabilities`).then(response => response.json());
  assert.deepEqual(capabilities, { configured: true, participantLimit: 4, turnConfigured: false, remoteMediaQualified: false });
  const card = await b.client.card(); const request = await a.client.requestContact(card.cardId);
  await b.client.acceptContact(request.requestId);
  const leftAdmission = await a.rpc('call.join', { requestId: 'actual_entry_left', mode: 'one-to-one', targetId: b.account.accountId });
  const rightAdmission = await b.rpc('call.join', { requestId: 'actual_entry_right', mode: 'one-to-one', targetId: a.account.accountId });
  assert.equal(leftAdmission.callId, rightAdmission.callId);
  const left = await f.socket(leftAdmission), right = await f.socket(rightAdmission);
  const ready = await until(() => left.messages.find(value => value.type === 'call.ready'));
  assert.equal(ready.self.deviceId, a.account.deviceId); assert.equal(ready.self.microphone, false);
  await until(() => left.messages.some(value => value.type === 'call.peers' && value.peers.length === 2));
  left.input({ type: 'p2p.offer', signal: { id: 'entry_offer_one', targetDeviceId: b.account.deviceId,
    deviceId: b.account.deviceId, kind: 'offer', sdp: 'v=0\r\n' } });
  const offer = await until(() => right.messages.find(value => value.type === 'p2p.offer'));
  assert.equal(offer.signal.deviceId, a.account.deviceId);
  const mic = await a.rpc('call.microphone', { requestId: 'entry_explicit_mic', callId: leftAdmission.callId, epoch: leftAdmission.epoch });
  left.input({ type: 'call.publish', grantRef: mic.grantRef });
  await until(() => left.messages.some(value => value.type === 'call.published'));
  left.input({ type: 'call.unpublish' });
  await until(() => left.messages.at(-1)?.type === 'call.peers' && left.messages.at(-1).peers.every(peer => !peer.microphone));
  const stale = await a.rpc('call.microphone', { requestId: 'entry_mic_before_block', callId: leftAdmission.callId, epoch: leftAdmission.epoch });
  await b.client.blockContact(a.account.accountId);
  left.input({ type: 'call.publish', grantRef: stale.grantRef });
  await until(() => left.ws.readyState === WebSocket.CLOSED);
  assert.equal(left.messages.filter(value => value.type === 'call.published').length, 1);
  await assert.rejects(b.rpc('call.join', { requestId: 'entry_blocked_rejoin', mode: 'one-to-one', targetId: a.account.accountId }),
    { code: 'human_call_denied' });
});

test('real entry rejects absent or foreign call origin and claimed identity while legacy channel still opens', { timeout: 15000 }, async t => {
  const f = await fixture(t), a = await f.browser();
  const group = (await a.rpc('world.community.create', { requestId: 'entry_group_one', name: 'Owned synthetic group', joinPolicy: 'open' })).community;
  const admission = await a.rpc('call.join', { requestId: 'entry_group_join', mode: 'group', targetId: group.communityId });
  for (const suppliedOrigin of [undefined, 'https://foreign.test']) {
    const ws = new WebSocket(`ws://127.0.0.1:${f.port}/ws/human-call/${admission.callId}`,
      suppliedOrigin ? { headers: { origin: suppliedOrigin } } : {});
    f.sockets.add(ws); let opened = false; ws.on('open', () => { opened = true; }); ws.on('error', () => {});
    await until(() => ws.readyState === WebSocket.CLOSED); assert.equal(opened, false);
  }
  const forged = await f.socket(admission, { deviceId: 'claimed_foreign_device' });
  await until(() => forged.ws.readyState === WebSocket.CLOSED); assert.equal(forged.messages.length, 0);
  for (const roomId of ['room_123456789abcdef', 'human-call_123456']) {
    const legacy = new WebSocket(`ws://127.0.0.1:${f.port}/ws/${roomId}`);
    f.sockets.add(legacy); await new Promise((resolve, reject) => { legacy.once('open', resolve); legacy.once('error', reject); });
    assert.equal(legacy.readyState, WebSocket.OPEN);
  }
});

test('call mount preserves the existing Connect server configuration with sixty-four allowed origins', { timeout: 10000 }, async t => {
  const origins = [origin, ...Array.from({ length: 63 }, (_, index) => `https://owned-call-origin-${index}.test`)];
  const f = await fixture(t, { origins });
  const state = await fetch(`http://127.0.0.1:${f.port}/api/calls/capabilities`).then(response => response.json());
  assert.equal(state.configured, true);
  const browser = await f.browser(); assert.ok(browser.account.accountId);
});
