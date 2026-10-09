import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
const { build } = createRequire(import.meta.url)(process.env.SOTY_ESBUILD_MODULE || 'esbuild');
const built = await build({ stdin: { resolveDir: process.cwd(), contents: "export {createHumanCallPorts,createP2pPeer} from './src/sync.ts';export {createCallLifecycle} from './modules/personal-agent/call/lifecycle.mjs';" },
  bundle: true, write: false, format: 'esm', platform: 'node' });
const runtimePath = resolve(process.env.SOTY_HUMAN_CALL_TEST_OUTPUT || 'output/human-call-test-runtime.mjs');
mkdirSync(dirname(runtimePath), { recursive: true }); writeFileSync(runtimePath, built.outputFiles[0].contents);
const { createHumanCallPorts, createP2pPeer, createCallLifecycle } = await import(pathToFileURL(runtimePath));
const tick = () => new Promise(resolve => setImmediate(resolve));
const deferred = () => { let resolve, reject; const promise = new Promise((done, no) => { resolve = done; reject = no; }); return { promise, resolve, reject }; };
async function until(check) { for (let n = 0; n < 80; n++) { if (check()) return; await tick(); } throw new Error('fixture_wait_timeout'); }
const track = () => ({ kind: 'audio', enabled: true, readyState: 'live', stops: 0, stop() { this.stops++; this.readyState = 'ended'; }, addEventListener() {}, removeEventListener() {} });
class Socket {
  readyState = 0; sent = []; closes = 0;
  send(raw) { this.sent.push(JSON.parse(raw)); }
  open() { this.readyState = 1; this.onopen?.(); }
  receive(value) { this.onmessage?.({ data: JSON.stringify({ callId: 'call-one', epoch: 1, ...value }) }); }
  close() { this.closes++; this.readyState = 3; this.onclose?.(); }
}
class RTC {
  connectionState = 'connected'; iceConnectionState = 'connected'; signalingState = 'stable'; remoteDescription = null;
  senders = []; addCalls = 0; removeCalls = 0; closes = 0; localCalls = 0; channels = [];
  localDescription = null; operations = Promise.resolve(); generatingLocal = false;
  operation(action) { const result = this.operations.then(action); this.operations = result.catch(() => {}); return result; }
  async createOffer() {
    const generate = () => { assert.ok(['stable', 'have-local-offer'].includes(this.signalingState)); return { type: 'offer', sdp: 'v=0\r\n' }; };
    return this.generatingLocal ? generate() : this.operation(generate);
  }
  async createAnswer() {
    const generate = () => { assert.equal(this.signalingState, 'have-remote-offer'); return { type: 'answer', sdp: 'v=0\r\n' }; };
    return this.generatingLocal ? generate() : this.operation(generate);
  }
  setLocalDescription(value) { return this.operation(async () => {
    if (value === undefined) {
      // Existing deferred generation hooks still exercise the native no-arg SLD
      // operation. No queued description may install after the PC was closed.
      this.generatingLocal = true;
      try { value = await (this.signalingState === 'have-remote-offer' ? this.createAnswer() : this.createOffer()); }
      finally { this.generatingLocal = false; }
    }
    if (this.connectionState === 'closed') throw new DOMException('controlled_closed', 'InvalidStateError');
    if (value.type === 'rollback') {
      if (this.signalingState === 'stable') throw new DOMException('controlled_rollback_stable', 'InvalidStateError');
      this.signalingState = 'stable'; this.localDescription = null; return;
    }
    assert.ok(value.type === 'offer' ? ['stable', 'have-local-offer'].includes(this.signalingState) : this.signalingState === 'have-remote-offer');
    this.localCalls++; this.localDescription = value;
    this.signalingState = value.type === 'offer' ? 'have-local-offer' : 'stable';
  }); }
  setRemoteDescription(value) { return this.operation(() => {
    if (this.connectionState === 'closed') throw new DOMException('controlled_closed', 'InvalidStateError');
    if (value.type === 'offer' && this.signalingState === 'have-local-offer') { this.signalingState = 'stable'; this.localDescription = null; }
    assert.ok(value.type === 'offer' ? this.signalingState === 'stable' : this.signalingState === 'have-local-offer');
    this.remoteDescription = value; this.signalingState = value.type === 'offer' ? 'have-remote-offer' : 'stable';
  }); }
  addIceCandidate() { return this.operation(() => {}); }
  addTransceiver() {}
  createDataChannel() { const value = { closes: 0, close() { this.closes++; } }; this.channels.push(value); return value; }
  addTrack(value) { this.addCalls++; const sender = { track: value }; this.senders.push(sender); return sender; }
  removeTrack(sender) { this.removeCalls++; if (this.throwRemove) throw new Error('synthetic_remove_failed'); this.senders = this.senders.filter(value => value !== sender); }
  close() { this.closes++; if (this.throwClose) throw new Error('synthetic_close_failed'); this.connectionState = 'closed'; }
}
function fixture(t, { peers = 1, holdMicrophone, acquireHook } = {}) {
  const sockets = [], pcs = [], outputs = [], localTrack = track(); let captures = 0, life, grant = 0;
  const client = { async extension(op) {
    if (op === 'call.leave') return { left: true };
    if (op === 'call.microphone' && holdMicrophone) await holdMicrophone;
    return { schema: 'soty.human-call-admission.v1', purpose: op === 'call.join' ? 'join' : 'microphone', callId: 'call-one', epoch: 1,
      accountId: 'account-self', deviceId: 'device-self', grantRef: 'reference-' + ++grant, expiresAt: Date.now() + 30000 };
  } };
  const ports = createHumanCallPorts({ client, socketFactory() { const value = new Socket(); sockets.push(value); return value; },
    rtcFactory() { const value = new RTC(); pcs.push(value); return value; }, acquire: async () => { captures++; return acquireHook ? acquireHook() : { getTracks: () => [localTrack] }; },
    playRemote() { const output = { plays: 0, closes: 0, async play() { this.plays++; if (this.blocked) throw new Error('synthetic_play_blocked'); }, close() { this.closes++; if (this.throwClose) throw new Error('synthetic_output_close_failed'); } }; outputs.push(output); return output; },
    onInterrupted(reason) { if (reason === 'signaling') life?.end(); else life?.mute(); } });
  life = createCallLifecycle({ ...ports }); life.setIdentity({ accountId: 'account-self', deviceId: 'device-self' });
  const roster = mic => [{ accountId: 'account-self', deviceId: 'device-self', label: 'Self', microphone: false },
    ...Array.from({ length: peers }, (_, n) => ({ accountId: 'account-peer-' + n, deviceId: 'device-peer-' + n, label: 'Peer', microphone: mic }))];
  async function join(microphone = false) {
    const result = life.join({ roomId: 'group-one', mode: 'group' }); await until(() => sockets.length === 1);
    const socket = sockets[0]; socket.open(); socket.receive({ type: 'call.ready', self: roster(false)[0], peers: roster(microphone) });
    assert.equal((await result).ok, true); return socket;
  }
  t.after(() => life.dispose());
  return { life, ports, sockets, pcs, outputs, localTrack, captures: () => captures, join, roster };
}

test('join/listen never captures or publishes and does not close an unrelated legacy file PC', async t => {
  const f = fixture(t), legacy = new RTC(); createP2pPeer('legacy', { candidate() {}, channel() {}, state() {}, iceFailed() {} }, () => legacy);
  await f.join(); assert.equal(f.captures(), 0); assert.equal(f.pcs[0].addCalls, 0);
  f.life.end(); assert.equal(legacy.closes, 0); assert.equal(f.pcs[0].closes, 1);
});
test('explicit microphone stays disabled until exact publish ACK then mute stops it and removes its sender', async t => {
  const f = fixture(t), socket = await f.join(); const pending = f.life.enableMicrophone();
  await until(() => socket.sent.some(value => value.type === 'call.publish'));
  assert.equal(f.captures(), 1); assert.equal(f.localTrack.enabled, false); assert.equal(f.pcs[0].addCalls, 0);
  const request = socket.sent.find(value => value.type === 'call.publish'); socket.receive({ type: 'call.published', grantRef: request.grantRef });
  assert.equal((await pending).ok, true); assert.equal(f.localTrack.enabled, true); assert.equal(f.pcs[0].addCalls, 1);
  f.life.mute(); assert.equal(f.localTrack.stops, 1); assert.equal(f.pcs[0].removeCalls, 1);
  assert.equal(socket.sent.some(value => value.type === 'call.unpublish'), true);
});
test('micOFF receiver track remains gated and can play after current permission becomesON', async t => {
  const f = fixture(t), socket = await f.join(false), received = track();
  f.pcs[0].ontrack({ track: received });
  assert.equal(received.stops, 0); assert.equal(f.outputs.length, 0);
  socket.receive({ type: 'call.peers', peers: f.roster(true) }); await tick();
  assert.equal(f.outputs.length, 1); assert.equal(f.outputs[0].plays, 1);
});
test('current peer microphoneON-OFF-ON gates playback without permanently stopping the receiver', async t => {
  const f = fixture(t), socket = await f.join(true), received = track(); f.pcs[0].ontrack({ track: received });
  assert.equal(f.outputs[0].plays, 1);
  socket.receive({ type: 'call.peers', peers: f.roster(false) });
  assert.equal(f.outputs[0].closes, 1); assert.equal(received.stops, 0); assert.equal(received.enabled, false);
  socket.receive({ type: 'call.peers', peers: f.roster(true) }); await tick();
  assert.equal(f.outputs.length, 2); assert.equal(f.outputs[1].plays, 1); assert.equal(received.enabled, true);
});
test('removing a peer closes exactly its owned playback and receiver track before whole-call end', async t => {
  const f = fixture(t), socket = await f.join(true), received = track(); f.pcs[0].ontrack({ track: received });
  socket.receive({ type: 'call.peers', peers: [f.roster(false)[0]] });
  assert.equal(f.outputs[0].closes, 1); assert.equal(received.stops, 1); assert.equal(f.pcs[0].closes, 1);
});
test('one throwing removeTrack cannot skip cleanup of other owned senders, PCs or socket', async t => {
  const f = fixture(t, { peers: 3 }), socket = await f.join(); const pending = f.life.enableMicrophone();
  await until(() => socket.sent.some(value => value.type === 'call.publish'));
  socket.receive({ type: 'call.published', grantRef: socket.sent.find(value => value.type === 'call.publish').grantRef }); await pending;
  f.pcs[0].throwRemove = true;
  f.life.end();
  assert.equal(f.localTrack.stops, 1); assert.equal(f.pcs.every(pc => pc.removeCalls >= 1), true);
  assert.equal(f.pcs.every(pc => pc.closes === 1), true); assert.equal(socket.closes, 1);
});

test('cancel during Native microphone admission never starts late capture or publication', async t => {
  const gate = deferred(), f = fixture(t, { holdMicrophone: gate.promise }), socket = await f.join();
  const pending = f.life.enableMicrophone(); await tick();
  assert.equal(f.life.snapshot().microphone, 'permission');
  f.life.mute(); assert.equal((await pending).ok, false);
  gate.resolve(); await tick(); await tick();
  assert.equal(f.captures(), 0); assert.equal(f.life.snapshot().microphone, 'off');
  assert.equal(socket.sent.some(value => value.type === 'call.publish'), false);
});

test('cancel during capture adopts and stops the late track without publishing', async t => {
  const gate = deferred(), lateTrack = track(), f = fixture(t, { acquireHook: () => gate.promise }), socket = await f.join();
  const pending = f.life.enableMicrophone(); await until(() => f.captures() === 1);
  f.life.mute(); assert.equal((await pending).ok, false);
  gate.resolve({ getTracks: () => [lateTrack] }); await tick(); await tick();
  assert.equal(lateTrack.readyState, 'ended'); assert.equal(lateTrack.enabled, false);
  assert.equal(f.pcs[0].addCalls, 0); assert.equal(socket.sent.some(value => value.type === 'call.publish'), false);
});

test('old publication ACK after cancel cannot revive or complete a newer explicit microphone request', async t => {
  const first = track(), second = track(); let n = 0;
  const f = fixture(t, { acquireHook: async () => ({ getTracks: () => [n++ === 0 ? first : second] }) }), socket = await f.join();
  const cancelled = f.life.enableMicrophone(); await until(() => socket.sent.filter(value => value.type === 'call.publish').length === 1);
  const old = socket.sent.find(value => value.type === 'call.publish');
  f.life.mute(); assert.equal((await cancelled).ok, false);
  const current = f.life.enableMicrophone(); await until(() => socket.sent.filter(value => value.type === 'call.publish').length === 2);
  const newest = socket.sent.filter(value => value.type === 'call.publish')[1];
  socket.receive({ type: 'call.published', grantRef: old.grantRef }); await tick();
  assert.equal(f.life.snapshot().microphone, 'publishing'); assert.equal(second.enabled, false); assert.equal(f.pcs[0].addCalls, 0);
  socket.receive({ type: 'call.published', grantRef: newest.grantRef }); assert.equal((await current).ok, true);
  assert.equal(second.enabled, true); assert.equal(first.readyState, 'ended'); assert.equal(first.enabled, false);
});

test('profile handoff during capture terminates old session and closes late capture in the old epoch', async t => {
  const gate = deferred(), lateTrack = track(), f = fixture(t, { acquireHook: () => gate.promise }), socket = await f.join();
  const pending = f.life.enableMicrophone(); await until(() => f.captures() === 1);
  f.life.setIdentity({ accountId: 'account-next', deviceId: 'device-next' }); assert.equal((await pending).ok, false);
  assert.equal(socket.closes, 1); assert.equal(f.pcs[0].closes, 1);
  gate.resolve({ getTracks: () => [lateTrack] }); await tick(); await tick();
  assert.equal(lateTrack.readyState, 'ended'); assert.equal(lateTrack.enabled, false); assert.equal(f.pcs[0].addCalls, 0);
});

test('late offer ICE and receiver callbacks after leave cannot signal or play in a retired call', async t => {
  const f = fixture(t), socket = await f.join(), gate = deferred(), received = track();
  const pc = f.pcs[0], ice = pc.onicecandidate, ontrack = pc.ontrack;
  pc.createOffer = () => gate.promise; pc.onnegotiationneeded();
  f.life.end(); gate.resolve({ type: 'offer', sdp: 'v=0\r\n' });
  ice({ candidate: { toJSON: () => ({ candidate: 'candidate:synthetic' }) } }); ontrack({ track: received }); await tick();
  assert.equal(socket.sent.filter(value => value.type.startsWith('p2p.')).length, 0);
  assert.equal(received.readyState, 'ended'); assert.equal(f.outputs.length, 0); assert.equal(pc.localCalls, 0);
});

test('peer removal while answer creation is pending cannot emit a late answer to that removed peer', async t => {
  const f = fixture(t), socket = await f.join(), gate = deferred();
  f.pcs[0].createAnswer = () => gate.promise;
  socket.receive({ type: 'p2p.offer', signal: { kind: 'offer', deviceId: 'device-peer-0', targetDeviceId: 'device-self', sdp: 'v=0\r\n' } }); await tick();
  socket.receive({ type: 'call.peers', peers: [f.roster(false)[0]] });
  gate.resolve({ type: 'answer', sdp: 'v=0\r\n' }); await tick();
  assert.equal(socket.sent.some(value => value.type === 'p2p.answer'), false); assert.equal(f.pcs[0].closes, 1);
});

test('RTC interruption mutes actual local capture without automatic microphone resume on network recovery', async t => {
  const f = fixture(t), socket = await f.join(); const pending = f.life.enableMicrophone();
  await until(() => socket.sent.some(value => value.type === 'call.publish'));
  socket.receive({ type: 'call.published', grantRef: socket.sent.find(value => value.type === 'call.publish').grantRef }); await pending;
  f.pcs[0].connectionState = 'disconnected'; f.pcs[0].onconnectionstatechange();
  assert.equal(f.localTrack.readyState, 'ended'); assert.equal(f.life.snapshot().microphone, 'off'); assert.equal(f.ports.snapshot().connection, 'reconnecting');
  f.pcs[0].connectionState = 'connected'; f.pcs[0].onconnectionstatechange();
  assert.equal(f.ports.snapshot().connection, 'connected'); assert.equal(f.life.snapshot().microphone, 'off'); assert.equal(f.captures(), 1);
});

test('throwing peer and playback close retain unknown cleanup while every other owned resource is attempted', async t => {
  const f = fixture(t, { peers: 2 }), socket = await f.join(true), received = track();
  f.pcs[0].ontrack({ track: received }); f.outputs[0].throwClose = true; f.pcs[0].throwClose = true;
  f.life.end(); await tick(); await tick();
  assert.equal(f.pcs.every(pc => pc.closes >= 1), true); assert.equal(socket.closes, 1); assert.equal(received.readyState, 'ended');
  assert.equal(f.life.snapshot().state, 'ended'); assert.equal(f.life.snapshot().capacity.occupied >= 1, true);
  assert.equal(f.life.snapshot().localEvidence.disposalFailures >= 1, true);
});

test('late rejected answer and ICE failure of a removed peer cannot mute the remaining current group', async t => {
  const f = fixture(t, { peers: 2 }), socket = await f.join(), gate = deferred();
  const pending = f.life.enableMicrophone(); await until(() => socket.sent.some(value => value.type === 'call.publish'));
  socket.receive({ type: 'call.published', grantRef: socket.sent.find(value => value.type === 'call.publish').grantRef }); await pending;
  const removed = f.pcs[0], ice = removed.oniceconnectionstatechange;
  removed.createAnswer = () => gate.promise;
  socket.receive({ type: 'p2p.offer', signal: { kind: 'offer', deviceId: 'device-peer-0', targetDeviceId: 'device-self', sdp: 'v=0\r\n' } }); await tick();
  socket.receive({ type: 'call.peers', peers: f.roster(false).filter(peer => peer.deviceId !== 'device-peer-0') });
  gate.reject(new Error('synthetic_retired_peer_failure')); await tick();
  assert.equal(f.life.snapshot().microphone, 'on'); assert.equal(f.localTrack.enabled, true);
  removed.iceConnectionState = 'failed'; ice();
  assert.equal(f.life.snapshot().microphone, 'on'); assert.equal(f.localTrack.enabled, true); assert.equal(f.pcs[1].closes, 0);
});

test('reentrant leave during session construction releases observed clean socket slots across later calls', async t => {
  const f = fixture(t);
  const remove = f.ports.subscribe(state => { if (state.connection === 'connecting') f.life.end(); }); t.after(remove);
  for (let n = 0; n < 17; n++) {
    const pending = f.life.join({ roomId: 'group-one', mode: 'group' });
    assert.equal((await pending).ok, false); await tick(); await tick();
    assert.equal(f.sockets.length, n + 1, 'a fully observed clean close must not consume a retained socket slot');
  }
  assert.equal(f.sockets.every(socket => socket.closes === 1), true); assert.equal(f.captures(), 0);
});
