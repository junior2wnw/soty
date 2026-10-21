import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { mkdirSync, mkdtempSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { createMediaCaptureCoordinator } from '../platform/media-capture-lease.mjs';

const { build } = createRequire(import.meta.url)(process.env.SOTY_ESBUILD_MODULE || 'esbuild');
const sourceRoot = fileURLToPath(new URL('../../', import.meta.url));
const bundlePlugins = process.env.SOTY_MEDIA_BUNDLE_PLUGIN ? [(await import(pathToFileURL(resolve(process.env.SOTY_MEDIA_BUNDLE_PLUGIN)))).mediaBundlePlugin()] : [];
const built = await build({ plugins: bundlePlugins, stdin: { resolveDir: sourceRoot, contents: "export {createHumanCallPorts,createP2pPeer} from './src/sync.ts';export {createCallLifecycle} from './modules/personal-agent/call/lifecycle.mjs';" },
  bundle: true, write: false, format: 'esm', platform: 'node' });
const runtimePath = process.env.SOTY_MEDIA_COMPOSITION_OUTPUT ? resolve(process.env.SOTY_MEDIA_COMPOSITION_OUTPUT)
  : join(mkdtempSync(join(tmpdir(), 'soty-media-composition-')), 'actual-runtime.mjs');
mkdirSync(dirname(runtimePath), { recursive: true }); writeFileSync(runtimePath, built.outputFiles[0].contents);
const { createHumanCallPorts, createP2pPeer, createCallLifecycle } = await import(pathToFileURL(runtimePath));
globalThis.MediaStream ??= class { constructor(tracks) { this.tracks = tracks; } getTracks() { return this.tracks; } };
const tick = () => new Promise(resolve => setImmediate(resolve));
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
async function until(check) { for (let n = 0; n < 120; n++) { if (check()) return; await tick(); } throw Error('actual_media_composition_wait'); }
function track(kind = 'video') { return { kind, readyState: 'live', enabled: true, stops: 0, handlers: new Set(),
  getSettings() { return { width: 960, height: 540, frameRate: 20 }; }, stop() { this.stops++; this.readyState = 'ended'; },
  addEventListener(_name, fn) { this.handlers.add(fn); }, removeEventListener(_name, fn) { this.handlers.delete(fn); } }; }
class Socket {
  readyState = 0; sent = []; closes = 0;
  open() { this.readyState = 1; this.onopen?.(); }
  send(raw) { this.sent.push(JSON.parse(raw)); }
  receive(value) { this.onmessage?.({ data: JSON.stringify({ callId: 'call-one', epoch: 1, ...value }) }); }
  close() { this.closes++; this.readyState = 3; this.onclose?.(); }
}
class RTC {
  connectionState = 'connected'; iceConnectionState = 'connected'; signalingState = 'stable'; localDescription = null; remoteDescription = null;
  closes = 0; senders = []; receivers = []; parameterWrites = 0; attachedVideo = 0; configurations = []; restarts = 0;
  constructor(config, ignoreCaps) { this.config = config; this.ignoreCaps = ignoreCaps; }
  async setLocalDescription() { if (this.connectionState === 'closed') throw Error('controlled_closed');
    const type = this.signalingState === 'have-remote-offer' ? 'answer' : 'offer'; this.localDescription = { type, sdp: 'v=0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\n' }; this.signalingState = type === 'offer' ? 'have-local-offer' : 'stable'; }
  async setRemoteDescription(value) { this.remoteDescription = value; this.signalingState = value.type === 'offer' ? 'have-remote-offer' : 'stable'; }
  async addIceCandidate() {} createDataChannel() { return { close() {} }; }
  addTransceiver(kind, options) {
    this.kind = kind; if (kind === 'audio') return {};
    const pc = this, sender = { track: null, params: { encodings: structuredClone(options.sendEncodings) },
      getParameters() { const value = structuredClone(this.params); if (pc.ignoreCaps && pc.parameterWrites) value.encodings[0].maxBitrate = 9000000; return value; },
      async setParameters(value) { pc.parameterWrites++; this.params = structuredClone(value); },
      async replaceTrack(value) { this.track = value; if (value) pc.attachedVideo++; pc.transceiver.currentDirection = value ? 'sendrecv' : 'recvonly'; } };
    this.senders.push(sender); this.transceiver = { sender, direction: options.direction, currentDirection: 'recvonly' }; return this.transceiver;
  }
  addTrack(value) { const sender = { track: value }; this.senders.push(sender); return sender; }
  removeTrack(sender) { this.senders = this.senders.filter(value => value !== sender); }
  getReceivers() { return this.receivers; }
  setConfiguration(value) { this.configurations.push(value); } restartIce() { this.restarts++; }
  close() { this.closes++; this.connectionState = 'closed'; }
}
function fixture(t, options = {}) {
  const sockets = [], pcs = [], operations = [], coordinator = createMediaCaptureCoordinator();
  const localVideo = track(), localAudio = track('audio'); let videoCaptures = 0, microphoneCaptures = 0, cameraGeneration = 0, grantNumber = 0, owner;
  const admission = purpose => ({ ok: true, schema: 'soty.human-call-admission.v1', purpose, accountId: 'a-self', deviceId: 'd-self', callId: 'call-one', epoch: 1,
    grantRef: purpose === 'join' ? 'join-self' : 'mic-' + (++grantNumber), expiresAt: Date.now() + 30000,
    ...(purpose === 'join' ? { mediaProtocol: 2, mediaInstance: 'media-self', cameraConfigured: true, iceConfigured: true } : {}) });
  const roster = (camera = false, microphone = false) => [
    { accountId: 'a-self', deviceId: 'd-self', microphone, mediaProtocol: 2, mediaInstance: 'media-self', camera, cameraGeneration },
    { accountId: 'a-peer', deviceId: 'd-peer', microphone: false, mediaProtocol: 2, mediaInstance: 'media-peer', camera: false, cameraGeneration: 0 }];
  const client = { async extension(op, args) {
    operations.push({ op, args });
    if (op === 'call.join') return admission('join'); if (op === 'call.microphone') return admission('microphone');
    if (op === 'call.ice.get') {
      if (options.iceGate) await options.iceGate.promise;
      if (options.failIce?.(args)) throw Error('synthetic_ice_refresh_failed');
      return { ok: true, schema: 'soty.human-call-ice.v1', accountId: 'a-self', deviceId: 'd-self', callId: 'call-one', epoch: 1,
        joinRef: 'join-self', revision: options.replayIce && args.expectedRevision > 0 ? 1 : args.expectedRevision + 1, expiresAt: Date.now() + 120000,
        iceServers: [{ urls: ['turn:relay.invalid:3478?transport=udp'], username: 'synthetic', credential: 'synthetic' }] };
    }
    if (op === 'call.camera') {
      if (options.cameraGate) await options.cameraGate.promise;
      assert.equal(args.expectedJoinRef, 'join-self'); assert.equal(args.expectedCameraGeneration, cameraGeneration); cameraGeneration++;
      sockets[0]?.receive({ type: 'call.peers', peers: roster(false) }); // roster may precede RPC result
      return { ok: true, schema: 'soty.human-camera-admission.v1', accountId: 'a-self', deviceId: 'd-self', callId: 'call-one', epoch: 1,
        grantRef: 'camera-' + cameraGeneration, cameraGeneration, expiresAt: Date.now() + 30000 };
    }
    if (op === 'call.camera.cancel') return { ok: true, withdrawn: true, cameraGeneration: args.cameraGeneration };
    if (op === 'call.leave') return { ok: true, left: true };
    throw Error('unexpected_fixture_operation');
  } };
  const ports = createHumanCallPorts({ client, mediaProtocol: 2,
    socketFactory() { const value = new Socket(); sockets.push(value); return value; },
    rtcFactory(config) { const value = new RTC(config, options.ignoreCaps); pcs.push(value); return value; },
    acquire: async () => { microphoneCaptures++; return { getTracks: () => [localAudio] }; }, playRemote: () => ({ async play() {}, close() {} }),
    camera: { enabled: true, coordinator, async acquire(_constraints, context) {
      videoCaptures++; owner = context.owner; if (options.captureGate) await options.captureGate.promise;
      return options.foreignCapsule ?? { owner, stream: { getTracks: () => [localVideo] } };
    } }, onInterrupted(reason) { if (reason === 'signaling') life.end(); else life.mute(); } });
  const life = createCallLifecycle({ ...ports }); life.setIdentity({ accountId: 'a-self', deviceId: 'd-self' });
  t.after(() => { life.dispose(); ports.close(); });
  async function joinCall() {
    const pending = life.join({ roomId: 'group-one', mode: 'group' }); await until(() => sockets.length === 1);
    sockets[0].open(); sockets[0].receive({ type: 'call.ready', self: roster()[0], peers: roster() }); assert.equal((await pending).ok, true); return sockets[0];
  }
  function ackVideo(extra = {}) {
    const frame = sockets[0].sent.filter(value => value.type === 'call.video.publish').at(-1);
    sockets[0].receive({ type: 'call.video.published', grantRef: frame.grantRef, cameraGeneration: frame.cameraGeneration, ...extra });
  }
  async function enableMicrophone() {
    const pending = life.enableMicrophone(); await until(() => sockets[0].sent.some(value => value.type === 'call.publish'));
    sockets[0].receive({ type: 'call.published', grantRef: sockets[0].sent.find(value => value.type === 'call.publish').grantRef }); assert.equal((await pending).ok, true);
  }
  return { ports, life, coordinator, sockets, pcs, operations, localVideo, localAudio, joinCall, ackVideo, enableMicrophone, roster,
    captures: () => ({ video: videoCaptures, microphone: microphoneCaptures }) };
}

test('actual sync join listens with signed call ICE and allocates no video/capture; unrelated legacy PC survives', async t => {
  const f = fixture(t), legacy = new RTC(); createP2pPeer('legacy-file', { candidate() {}, channel() {}, state() {}, iceFailed() {} }, () => legacy);
  await f.joinCall(); assert.deepEqual(f.captures(), { video: 0, microphone: 0 }); assert.equal(f.pcs.length, 1); assert.equal(f.pcs[0].kind, 'audio');
  assert.equal(f.pcs[0].config.iceServers[0].urls[0], 'turn:relay.invalid:3478?transport=udp'); assert.equal(f.ports.snapshot().turnConfigured, true);
  f.life.end(); assert.equal(legacy.closes, 0); assert.equal(f.pcs[0].closes, 1);
});
test('actual owner waits for exact video ACK and native sender caps; camera OFF leaves microphone and navigation alive', async t => {
  const f = fixture(t); await f.joinCall(); await f.enableMicrophone();
  const pending = f.ports.enableCamera(); await until(() => f.sockets[0].sent.some(value => value.type === 'call.video.publish'));
  assert.equal(f.localVideo.enabled, false); assert.equal(f.pcs.filter(pc => pc.kind === 'video').length, 0); f.ackVideo({ cameraGeneration: 90 });
  await tick(); assert.equal(f.localVideo.enabled, false); f.ackVideo(); assert.equal((await pending).ok, true);
  const audio = f.pcs.find(pc => pc.kind === 'audio'), video = f.pcs.find(pc => pc.kind === 'video');
  assert.equal(f.localVideo.enabled, true); assert.equal(video.parameterWrites, 1); assert.equal(video.attachedVideo, 1);
  f.life.navigate(); assert.equal(audio.closes, 0); assert.equal(f.localVideo.enabled, true);
  f.ports.disableCamera(); await tick(); assert.equal(f.localVideo.stops, 1); assert.equal(video.closes, 1); assert.equal(audio.closes, 0);
  assert.equal(f.localAudio.enabled, true); assert.equal(f.localAudio.stops, 0); assert.equal(f.life.snapshot().microphone, 'on');
});
test('shared QR lease blocks camera; cancel pending own capture stops only late own stream and keeps audio', async t => {
  const gate = deferred(), f = fixture(t, { captureGate: gate }); await f.joinCall();
  const qr = f.coordinator.acquire('camera', {}); assert.equal((await f.ports.enableCamera()).code, 'camera_busy'); assert.equal(f.captures().video, 0);
  qr.release(); const pending = f.ports.enableCamera(); await until(() => f.captures().video === 1); f.ports.disableCamera();
  assert.equal((await pending).ok, false); assert.equal(f.coordinator.busy('camera'), true); assert.equal(f.localVideo.stops, 0);
  gate.resolve(); await until(() => f.localVideo.stops === 1); await until(() => !f.coordinator.busy('camera'));
  assert.equal(f.sockets[0].sent.some(value => value.type === 'call.video.publish'), false); assert.equal(f.pcs[0].closes, 0);
});
test('identity change during signed camera admission cancels late grant without capture or lease transfer', async t => {
  const gate = deferred(), f = fixture(t, { cameraGate: gate }); await f.joinCall(); const pending = f.ports.enableCamera();
  await until(() => f.operations.some(value => value.op === 'call.camera')); f.life.setIdentity({ accountId: 'other-account', deviceId: 'other-device' });
  assert.equal((await pending).ok, false); assert.equal(f.coordinator.busy('camera'), true); gate.resolve();
  await until(() => f.operations.some(value => value.op === 'call.camera.cancel')); await until(() => !f.coordinator.busy('camera'));
  assert.equal(f.captures().video, 0); assert.equal(f.sockets[0].sent.some(value => value.type === 'call.video.publish'), false);
});
test('foreign capture capsule stays untouched and quarantines own camera lease without ending audio', async t => {
  let enumerated = 0; const foreign = track(), f = fixture(t, { foreignCapsule: { owner: {}, stream: { getTracks() { enumerated++; return [foreign]; } } } });
  await f.joinCall(); const result = await f.ports.enableCamera(); assert.equal(result.ok, false); await tick();
  assert.equal(enumerated, 0); assert.equal(foreign.stops, 0); assert.equal(foreign.enabled, true); assert.equal(f.ports.cameraSnapshot().cleanupFailed, true);
  assert.equal(f.coordinator.busy('camera'), true); assert.equal(f.pcs[0].closes, 0);
});
test('closing a pending initial ICE join fences late socket creation', async t => {
  const gate = deferred(), f = fixture(t, { iceGate: gate }); const scope = Object.freeze({ accountId: 'a-self', deviceId: 'd-self', roomId: 'group-one', mode: 'group', generation: 1 });
  const abort = new AbortController(), admission = await f.ports.host.authorize(scope, 'join', { signal: abort.signal });
  const pending = f.ports.transport.join({ scope, admission, signal: abort.signal }); void pending.catch(() => {}); await until(() => f.operations.some(value => value.op === 'call.ice.get'));
  f.ports.close(); gate.resolve(); await tick(); await tick(); assert.equal(f.sockets.length, 0); await assert.rejects(pending); assert.deepEqual(f.captures(), { video: 0, microphone: 0 });
  assert.equal(f.operations.some(value => value.op === 'call.leave'), true);
});
test('actual call ICE refresh reuses audio PC, caps replacement video PC and never captures again', async t => {
  const f = fixture(t); await f.joinCall(); const pending = f.ports.enableCamera(); await until(() => f.sockets[0].sent.some(value => value.type === 'call.video.publish'));
  f.ackVideo(); assert.equal((await pending).ok, true); const audio = f.pcs[0], firstVideo = f.pcs[1];
  assert.equal(await f.ports.refreshCallIce(), true); await until(() => f.pcs.at(-1).attachedVideo === 1);
  assert.equal(audio.closes, 0); assert.equal(audio.restarts, 1); assert.equal(audio.configurations.length, 1); assert.equal(firstVideo.closes, 1);
  assert.equal(f.pcs.at(-1).parameterWrites, 1); assert.equal(f.captures().video, 1); assert.equal(f.localVideo.enabled, true);
});
test('failed video caps stop own camera while microphone sender and call remain active', async t => {
  const f = fixture(t, { ignoreCaps: true }); await f.joinCall(); await f.enableMicrophone(); const pending = f.ports.enableCamera();
  await until(() => f.sockets[0].sent.some(value => value.type === 'call.video.publish')); f.ackVideo(); assert.equal((await pending).ok, false);
  await until(() => f.localVideo.stops === 1); assert.equal(f.localVideo.enabled, false); assert.equal(f.pcs.find(pc => pc.kind === 'video').attachedVideo, 0);
  assert.equal(f.pcs[0].closes, 0); assert.equal(f.localAudio.stops, 0); assert.equal(f.life.snapshot().microphone, 'on');
});


test('review: expired configured ICE waits for a new peer without legacy fallback or interrupting established media', async t => {
  const realNow = Date.now; let at = realNow(), failRefresh = true;
  Date.now = () => at;
  const f = fixture(t, { failIce: args => failRefresh && args.expectedRevision > 0 });
  try {
    await f.joinCall(); await f.enableMicrophone();
    const enabling = f.ports.enableCamera();
    await until(() => f.sockets[0].sent.some(value => value.type === 'call.video.publish'));
    f.ackVideo(); assert.equal((await enabling).ok, true);
    const audio = f.pcs.find(pc => pc.kind === 'audio'), video = f.pcs.find(pc => pc.kind === 'video');
    assert.equal(await f.ports.refreshCallIce(), false);
    // This assertion also catches the sealed candidate's eager video teardown.
    assert.equal(video.closes, 0); assert.equal(audio.closes, 0);
    assert.equal(f.localVideo.enabled, true); assert.equal(f.localVideo.stops, 0); assert.equal(f.localAudio.stops, 0);
    at += 120001;
    const extra = { accountId: 'a-new-synthetic', deviceId: 'd-new-synthetic', microphone: false,
      mediaProtocol: 2, mediaInstance: 'media-new-synthetic', camera: false, cameraGeneration: 0 };
    f.sockets[0].receive({ type: 'call.peers', peers: [...f.roster(true, true), extra] });
    await tick();
    assert.equal(f.pcs.length, 2, 'new configured-call PC must wait until fresh ICE');
    assert.equal(f.pcs.some(pc => pc.config === undefined), false);
    assert.equal(audio.closes, 0); assert.equal(video.closes, 0);
    assert.equal(f.localVideo.enabled, true); assert.equal(f.localAudio.stops, 0);
    assert.equal(f.ports.snapshot().iceWaiting, true);
    assert.equal(f.ports.cameraSnapshot().state, 'on');
    failRefresh = false;
    assert.equal(await f.ports.refreshCallIce(), true);
    await until(() => f.pcs.filter(pc => pc.kind === 'audio').length === 2);
    assert.equal(f.pcs.every(pc => pc.config?.iceServers?.[0]?.urls?.[0] === 'turn:relay.invalid:3478?transport=udp'), true);
    assert.equal(f.ports.snapshot().iceWaiting, false);
    assert.equal(audio.closes, 0);
    assert.deepEqual(f.captures(), { video: 1, microphone: 1 });
  } finally {
    f.life.dispose(); f.ports.close(); Date.now = realNow;
  }
});

// Append to a NEW Root copy of the actual sync composition test.
// Kept separate so the already handed-off proposal/test hashes stay unchanged.
// Requires the failIce/roster fixture additions in regression.diff.
test('review: camera-OFF expired configured ICE cannot create an audio PC with undefined or legacy ICE', async t => {
  const realNow = Date.now; let at = realNow();
  Date.now = () => at;
  const f = fixture(t, { failIce: args => args.expectedRevision > 0 });
  try {
    await f.joinCall(); await f.enableMicrophone();
    const audio = f.pcs[0];
    assert.equal(await f.ports.refreshCallIce(), false);
    assert.equal(audio.closes, 0);
    at += 120001;
    f.sockets[0].receive({ type: 'call.peers', peers: [...f.roster(false, true), {
      accountId: 'a-expiry-only', deviceId: 'd-expiry-only', microphone: false,
      mediaProtocol: 2, mediaInstance: 'media-expiry-only', camera: false, cameraGeneration: 0,
    }] });
    await tick();
    // On the original sealed Source this directly reaches rtcFactory(undefined).
    assert.equal(f.pcs.some(pc => pc.config === undefined), false);
    assert.equal(f.pcs.length, 1);
    assert.equal(audio.closes, 0);
    assert.equal(f.localAudio.stops, 0);
    assert.equal(f.ports.snapshot().iceWaiting, true);
    assert.deepEqual(f.captures(), { video: 0, microphone: 1 });
  } finally {
    f.life.dispose(); f.ports.close(); Date.now = realNow;
  }
});


// Only the call-ICE long timer is controlled. Native RTC/socket/RPC remain the
// same fixture ports; no Node mock-timer API or real elapsed network wait.
function controlledAutomaticIceTimers() {
  const realNow = Date.now, realSet = globalThis.setTimeout, realClear = globalThis.clearTimeout;
  let at = realNow(); const pending = new Map(), owned = new WeakSet();
  Date.now = () => at;
  globalThis.setTimeout = (callback, delay, ...args) => {
    if (typeof callback === 'function' && Number(delay) >= 30000) {
      const token = Object.freeze({}); owned.add(token);
      pending.set(token, { token, delay: Number(delay), at: at + Number(delay), callback: () => callback(...args) });
      return token;
    }
    return realSet(callback, delay, ...args);
  };
  globalThis.clearTimeout = token => {
    if (token && typeof token === 'object' && owned.has(token)) { pending.delete(token); return; }
    return realClear(token);
  };
  const peek = () => [...pending.values()].sort((a, b) => a.at - b.at)[0];
  return {
    count: () => pending.size, peek,
    elapse(ms) { const next = peek(); assert.ok(!next || at + ms < next.at); at += ms; },
    async fire(duplicate = false) {
      const next = peek(); assert.ok(next, 'automatic ICE timer must be armed');
      pending.delete(next.token); at = Math.max(at, next.at); next.callback();
      if (duplicate) next.callback(); // Simulate a duplicate queued native callback: singleflight must win.
      await tick(); return next;
    },
    restore() { pending.clear(); Date.now = realNow; globalThis.setTimeout = realSet; globalThis.clearTimeout = realClear; },
  };
}
const iceOperations = f => f.operations.filter(value => value.op === 'call.ice.get');

test('review: automatic call ICE retry resumes a waiting peer after transient failure without recapture', async t => {
  const timers = controlledAutomaticIceTimers(); let refreshes = 0;
  const f = fixture(t, { failIce: args => args.expectedRevision > 0 && ++refreshes === 1 });
  try {
    await f.joinCall(); await f.enableMicrophone();
    const enabling = f.ports.enableCamera();
    await until(() => f.sockets[0].sent.some(value => value.type === 'call.video.publish'));
    f.ackVideo(); assert.equal((await enabling).ok, true);
    const audio = f.pcs.find(pc => pc.kind === 'audio'), video = f.pcs.find(pc => pc.kind === 'video');
    assert.equal(timers.count(), 1); assert.equal(timers.peek().delay, 105000);
    const failedTimer = await timers.fire(true);
    assert.equal(iceOperations(f).length, 2, 'duplicate timer must not duplicate the in-flight RPC');
    assert.equal(f.ports.snapshot().iceWaiting, true);
    assert.equal(audio.closes, 0); assert.equal(video.closes, 0);
    assert.equal(f.localAudio.stops, 0); assert.equal(f.localVideo.stops, 0); assert.equal(f.localVideo.enabled, true);
    assert.equal(timers.count(), 1); assert.equal(timers.peek().delay, 30000);
    failedTimer.callback(); await tick();
    assert.equal(iceOperations(f).length, 2, 'old timer cannot replace the current retry or dispatch again');
    assert.equal(timers.count(), 1); assert.equal(timers.peek().delay, 30000);
    timers.elapse(15001); // Old ICE expired; retry is still 14,999ms away.
    f.sockets[0].receive({ type: 'call.peers', peers: [...f.roster(true, true), {
      accountId: 'a-auto-retry', deviceId: 'd-auto-retry', microphone: false,
      mediaProtocol: 2, mediaInstance: 'media-auto-retry', camera: false, cameraGeneration: 0,
    }] });
    await tick(); assert.equal(f.pcs.length, 2); assert.equal(f.pcs.some(pc => !pc.config), false);
    assert.equal(audio.closes, 0); assert.equal(video.closes, 0);
    await timers.fire();
    await until(() => f.pcs.filter(pc => pc.kind === 'audio').length === 2);
    assert.equal(iceOperations(f).length, 3);
    assert.deepEqual(iceOperations(f)[2].args, iceOperations(f)[1].args, 'retry must reuse the exact ice_N identity');
    assert.equal(f.ports.snapshot().iceWaiting, false);
    assert.equal(f.pcs.every(pc => pc.config?.iceServers?.[0]?.urls?.[0] === 'turn:relay.invalid:3478?transport=udp'), true);
    assert.equal(audio.closes, 0); assert.equal(f.localAudio.stops, 0); assert.equal(f.localVideo.stops, 0);
    assert.equal(f.localVideo.enabled, true); assert.equal(f.ports.cameraSnapshot().state, 'on');
    assert.deepEqual(f.captures(), { video: 1, microphone: 1 });
    assert.equal(timers.count(), 1); assert.equal(timers.peek().delay, 105000, 'success returns to the normal expiry schedule');
  } finally { f.life.dispose(); f.ports.close(); await tick(); timers.restore(); }
});

test('review: automatic call ICE retry backs off on a nonadvancing reply and keeps only one bounded timer', async t => {
  const timers = controlledAutomaticIceTimers(), options = { replayIce: true }, f = fixture(t, options);
  try {
    await f.joinCall(); await timers.fire();
    for (const delay of [30000, 60000, 120000, 120000]) {
      assert.equal(timers.count(), 1); assert.equal(timers.peek().delay, delay);
      assert.equal(f.ports.snapshot().iceWaiting, true);
      const before = iceOperations(f).length;
      timers.elapse(delay - 1); await tick(); assert.equal(iceOperations(f).length, before);
      await timers.fire(); assert.equal(iceOperations(f).length, before + 1);
    }
    options.replayIce = false;
    await timers.fire();
    assert.equal(f.ports.snapshot().iceWaiting, false);
    assert.equal(timers.count(), 1); assert.equal(timers.peek().delay, 105000);
    assert.ok(iceOperations(f).slice(1).every(value => value.args.requestId === 'ice_1' && value.args.expectedRevision === 1));
    assert.deepEqual(f.captures(), { video: 0, microphone: 0 });
  } finally { f.life.dispose(); f.ports.close(); await tick(); timers.restore(); }
});

for (const ending of ['leave', 'account-change']) test(`review: automatic call ICE retry cancels on ${ending} including a stale callback`, async t => {
  const timers = controlledAutomaticIceTimers(), options = { failIce: args => args.expectedRevision > 0 }, f = fixture(t, options);
  try {
    await f.joinCall(); await timers.fire();
    assert.equal(timers.count(), 1); assert.equal(timers.peek().delay, 30000);
    const queued = timers.peek(); let gate;
    if (ending === 'account-change') {
      options.failIce = () => false; gate = deferred(); options.iceGate = gate;
      await timers.fire(); // Retry RPC is now held in the controlled port.
      assert.equal(iceOperations(f).length, 3);
      f.life.setIdentity({ accountId: 'account-after-retry', deviceId: 'device-after-retry' });
    } else f.life.end();
    const before = iceOperations(f).length;
    gate?.resolve(); await tick();
    assert.equal(timers.count(), 0, 'close/identity change must cancel the owned retry');
    queued.callback(); await tick(); // A callback already queued before clearTimeout must still be fenced.
    assert.equal(iceOperations(f).length, before); assert.equal(timers.count(), 0);
    assert.equal(f.sockets.length, 1); assert.equal(f.sockets[0].readyState, 3);
    assert.deepEqual(f.captures(), { video: 0, microphone: 0 });
  } finally { f.life.dispose(); f.ports.close(); await tick(); timers.restore(); }
});
