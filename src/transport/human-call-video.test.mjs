import test from 'node:test';
import assert from 'node:assert/strict';
import { createHumanCallVideoTransport, HUMAN_VIDEO_LIMITS } from './human-call-video.mjs';

globalThis.MediaStream ??= class { constructor(tracks) { this.tracks = tracks; } getTracks() { return this.tracks; } };
const tick = () => new Promise(resolve => setImmediate(resolve));
async function until(check) { for (let n = 0; n < 80; n++) { if (check()) return; await tick(); } throw Error('controlled_video_wait'); }
const scope = Object.freeze({ accountId: 'a-self', deviceId: 'd-self', callId: 'call-one', epoch: 1, joinRef: 'join-self', generation: 1 });
const self = (camera = false, cameraGeneration = 0) => ({ accountId: scope.accountId, deviceId: scope.deviceId, mediaProtocol: 2, mediaInstance: 'media-self', camera, cameraGeneration });
const peer = (n = 0, camera = false, cameraGeneration = 0, mediaInstance = 'media-peer-' + n) => ({ accountId: 'a-peer-' + n, deviceId: 'd-peer-' + n, mediaProtocol: 2, mediaInstance, camera, cameraGeneration });
const ice = (revision = 1) => ({ schema: 'soty.human-call-ice.v1', accountId: scope.accountId, deviceId: scope.deviceId, callId: scope.callId, epoch: scope.epoch,
  joinRef: scope.joinRef, revision, expiresAt: Date.now() + 120000, iceServers: [{ urls: ['stun:ice.invalid:3478'] }] });
const track = () => ({ kind: 'video', readyState: 'live', enabled: false, stops: 0,
  stop() { this.stops++; this.readyState = 'ended'; }, getSettings() { return { width: 960, height: 540, frameRate: 20 }; } });
class RTC {
  connectionState = 'connected'; iceConnectionState = 'connected'; signalingState = 'stable'; localDescription = null; remoteDescription = null;
  receivers = []; candidates = []; parameterWrites = 0; attaches = 0; closes = 0;
  constructor(config, options = {}) { this.config = config; this.options = options; }
  addTransceiver(kind, options) {
    assert.equal(kind, 'video'); assert.equal(options.direction, 'recvonly');
    const pc = this, sender = { track: null, params: { encodings: structuredClone(options.sendEncodings) },
      getParameters() { const value = structuredClone(this.params); if (pc.options.ignoreCaps && pc.parameterWrites) value.encodings[0].maxBitrate = 9000000; return value; },
      async setParameters(value) { pc.parameterWrites++; this.params = structuredClone(value); if (pc.options.holdCaps) await pc.options.holdCaps; },
      async replaceTrack(value) { if (value) pc.attaches++; this.track = value; if (!pc.options.blockDirection) pc.transceiver.currentDirection = value ? 'sendrecv' : 'recvonly'; }, };
    this.transceiver = { sender, direction: 'recvonly', currentDirection: 'recvonly' }; return this.transceiver;
  }
  async setLocalDescription() {
    if (this.connectionState === 'closed') throw Error('controlled_closed');
    const type = this.signalingState === 'have-remote-offer' ? 'answer' : 'offer';
    this.localDescription = { type, sdp: 'v=0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\n' }; this.signalingState = type === 'offer' ? 'have-local-offer' : 'stable';
  }
  async setRemoteDescription(value) { if (this.connectionState === 'closed') throw Error('controlled_closed');
    if (value.type === 'offer' && this.signalingState === 'have-local-offer') { this.rollbacks = (this.rollbacks ?? 0) + 1; this.signalingState = 'stable'; }
    assert.ok(value.type === 'offer' ? this.signalingState === 'stable' : this.signalingState === 'have-local-offer');
    this.remoteDescription = value; this.signalingState = value.type === 'offer' ? 'have-remote-offer' : 'stable'; }
  async addIceCandidate(value) { this.candidates.push(value); }
  getReceivers() { return this.receivers; }
  close() { this.closes++; if (this.options.failClose) throw Error('controlled_cleanup_unknown'); this.connectionState = 'closed'; }
  remote(value = track()) { const receiver = { track: value }; this.receivers.push(receiver); this.ontrack?.({ track: value, receiver }); return value; }
}
function fixture(t, options = {}) {
  const sent = [], pcs = [], outputs = []; let faults = 0;
  const video = createHumanCallVideoTransport({ scope, mediaInstance: 'media-self', send(type, values) { sent.push({ type, callId: scope.callId, epoch: scope.epoch, ...values }); },
    rtcFactory(config) { const pc = new RTC(config, options); pcs.push(pc); return pc; },
    renderRemote(value) { const output = { value, closes: 0, close() { this.closes++; } }; outputs.push(output); return output; }, onFault() { faults++; } });
  video.setIce(ice()); video.setRoster([self(), peer()]);
  t.after(() => { try { video.close(); } catch {} });
  function admission(cameraGeneration = 1) { return video.noteAdmission({ schema: 'soty.human-camera-admission.v1', callId: scope.callId, epoch: scope.epoch,
    accountId: scope.accountId, deviceId: scope.deviceId, grantRef: 'camera-' + cameraGeneration, cameraGeneration, expiresAt: Date.now() + 30000 }); }
  function prepare(value = admission(), valueTrack = track()) {
    const abort = new AbortController(); const stage = video.prepare({ scope, admission: value, track: valueTrack, limits: HUMAN_VIDEO_LIMITS, signal: abort.signal });
    return { stage, track: valueTrack, abort, admission: value };
  }
  function acknowledge(value) { video.receive({ type: 'call.video.published', callId: scope.callId, epoch: scope.epoch, grantRef: value.grantRef, cameraGeneration: value.cameraGeneration }); }
  function message(type, values = {}) { return { type, callId: scope.callId, epoch: scope.epoch, signal: { id: 'signal-one', deviceId: 'd-peer-0', targetDeviceId: scope.deviceId,
    fromMediaInstance: 'media-peer-0', toMediaInstance: 'media-self', fromCameraGeneration: 1, toCameraGeneration: 0, ...values } }; }
  return { video, sent, pcs, outputs, faults: () => faults, admission, prepare, acknowledge, message };
}

test('camera OFF has no video PCs; only negotiated cameras allocate at most three and OFF removes all', t => {
  const f = fixture(t); assert.equal(f.pcs.length, 0);
  f.video.setRoster([self(), { accountId: 'a-old', deviceId: 'd-old', microphone: false }, peer(0), peer(1)]); assert.equal(f.pcs.length, 0);
  f.video.setRoster([self(), peer(0, true, 1), peer(1, true, 1), peer(2, true, 1)]);
  assert.equal(f.video.snapshot().activePeers, 3); assert.equal(f.pcs.length, 3);
  f.video.setRoster([self(), peer(0, false, 1), peer(1, false, 1), peer(2, false, 1)]);
  assert.equal(f.video.snapshot().activePeers, 0); assert.equal(f.video.snapshot().retainedPeers, 0); assert.ok(f.pcs.every(pc => pc.closes === 1));
  assert.throws(() => f.video.setRoster([self(), peer(0), peer(1), peer(2), peer(3)]));
});
test('wrong or open ACK cannot commit; exact ACK uses real sender caps and leaves track enabling to its owner', async t => {
  const f = fixture(t), p = f.prepare(); let settled = false; const pending = p.stage.commit().then(value => { settled = true; return value; });
  f.video.receive({ type: 'call.video.published', callId: scope.callId, epoch: scope.epoch, grantRef: p.admission.grantRef, cameraGeneration: 99 });
  f.video.receive({ type: 'call.video.published', callId: scope.callId, epoch: scope.epoch, grantRef: p.admission.grantRef, cameraGeneration: 1, extra: true });
  await tick(); assert.equal(settled, false); assert.equal(f.pcs.length, 0); assert.equal(p.track.enabled, false);
  f.acknowledge(p.admission); const ack = await pending;
  assert.equal(ack.cameraGeneration, 1); assert.equal(f.pcs[0].parameterWrites, 1); assert.equal(f.pcs[0].attaches, 1);
  assert.deepEqual(f.pcs[0].transceiver.sender.getParameters().encodings, [{ maxBitrate: 1200000, maxFramerate: 24, scaleResolutionDownBy: 1 }]);
  assert.equal(p.track.enabled, false); p.stage.dispose(); assert.equal(p.track.stops, 0);
});
test('native sender ignoring requested caps rejects publication before attaching capture', async t => {
  const f = fixture(t, { ignoreCaps: true }), p = f.prepare(); const pending = p.stage.commit(); f.acknowledge(p.admission);
  await assert.rejects(pending); assert.equal(f.pcs[0].parameterWrites, 1); assert.equal(f.pcs[0].attaches, 0); assert.equal(p.track.enabled, false);
  assert.equal(f.faults(), 1); p.stage.dispose();
});
test('cancel tombstones pending grant; a late ACK cannot allocate or attach anything', async t => {
  const f = fixture(t), p = f.prepare(); const pending = p.stage.commit(); p.stage.dispose(); f.acknowledge(p.admission);
  await assert.rejects(pending); await tick(); assert.equal(f.pcs.length, 0); assert.equal(p.track.stops, 0);
  assert.equal(f.sent.filter(value => value.type === 'call.video.withdraw').length, 1);
  const next = f.prepare(f.admission(2)); const ready = next.stage.commit(); f.acknowledge(next.admission); await ready; next.stage.dispose();
});
test('only receiver tracks owned by current video PC are rendered or stopped', async t => {
  const f = fixture(t); f.video.setRoster([self(), peer(0, true, 1)]); const pc = f.pcs[0], ownTrack = pc.remote();
  assert.equal(f.outputs.length, 1); assert.equal(ownTrack.enabled, true);
  const foreign = track(); pc.ontrack({ track: foreign, receiver: { track: foreign } }); assert.equal(foreign.enabled, false); assert.equal(foreign.stops, 0);
  f.video.setRoster([self(), peer(0, false, 1)]); assert.equal(f.outputs[0].closes, 1); assert.equal(ownTrack.stops, 1); assert.equal(foreign.stops, 0);
});
test('same-device rejoin has new mediaInstance; old-generation and old-instance frames are ignored', async t => {
  const f = fixture(t); f.video.setRoster([self(), peer(0, true, 1)]); const old = f.pcs[0];
  f.video.setRoster([self(), peer(0, true, 1, 'media-peer-rejoined')]); const current = f.pcs[1];
  assert.equal(old.closes, 1); assert.equal(current.closes, 0);
  f.video.receive(f.message('video.offer', { kind: 'offer', sdp: 'v=0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\n' })); await tick(); assert.equal(current.remoteDescription, null);
  f.video.receive(f.message('video.offer', { fromMediaInstance: 'media-peer-rejoined', kind: 'offer', sdp: 'v=0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\n' })); await tick();
  assert.equal(current.remoteDescription?.type, 'offer');
});
test('withdrawn generation cannot reopen through a delayed ON roster; new generation may', t => {
  const f = fixture(t); f.video.setRoster([self(), peer(0, true, 1)]); f.video.setRoster([self(), peer(0, false, 1)]);
  f.video.setRoster([self(), peer(0, true, 1)]); assert.equal(f.video.snapshot().activePeers, 0);
  f.video.setRoster([self(), peer(0, true, 2)]); assert.equal(f.video.snapshot().activePeers, 1);
});
test('ICE revision never rolls back a newer PC and owned RTC configuration is a copy', t => {
  const f = fixture(t); f.video.setRoster([self(), peer(0, true, 1)]); const old = f.pcs[0];
  assert.equal(f.video.setIce(ice(2)), true); const current = f.pcs[1]; assert.equal(old.closes, 1);
  assert.equal(f.video.setIce(ice(1)), false); assert.equal(current.closes, 0); assert.equal(f.pcs.length, 2);
  current.config.iceServers[0].urls[0] = 'stun:mutated.invalid'; assert.equal(f.video.setIce(ice(3)), true);
  assert.equal(f.pcs[2].config.iceServers[0].urls[0], 'stun:ice.invalid:3478');
});
test('pending candidate queue is capped and faults close only owned video PCs', async t => {
  const f = fixture(t); f.video.setRoster([self(), peer(0, true, 1)]); await tick();
  for (let n = 0; n < 33; n++) f.video.receive(f.message('video.candidate', { id: 'candidate_' + n, candidate: 'candidate:synthetic' }));
  await until(() => f.faults() === 1); assert.equal(f.pcs[0].closes, 1); assert.equal(f.video.snapshot().unavailable, true);
});
test('unknown PC teardown retains its slot and blocks recreation', t => {
  const f = fixture(t, { failClose: true }); f.video.setRoster([self(), peer(0, true, 1)]); f.video.setRoster([self(), peer(0, false, 1)]);
  assert.equal(f.video.snapshot().cleanupFailed, true); assert.equal(f.video.snapshot().retainedPeers, 1);
  assert.throws(() => f.video.retry()); assert.equal(f.pcs.length, 1);
});
test('late sender-parameter resolution after close never attaches a capture', async t => {
  let release; const held = new Promise(resolve => { release = resolve; }); const f = fixture(t, { holdCaps: held }), p = f.prepare();
  const pending = p.stage.commit(); f.acknowledge(p.admission); await until(() => f.pcs[0]?.parameterWrites === 1);
  p.stage.dispose(); release(); await assert.rejects(pending); await tick(); assert.equal(f.pcs[0].attaches, 0); assert.equal(p.track.stops, 0);
});

test('polite glare rolls back through native operation and drains bounded candidates before answering', async t => {
  const f = fixture(t); f.video.setRoster([self(), peer(0, true, 1)]); await until(() => f.pcs[0].signalingState === 'have-local-offer');
  f.video.receive(f.message('video.candidate', { candidate: 'candidate:synthetic' }));
  f.video.receive(f.message('video.offer', { kind: 'offer', sdp: 'v=0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\n' })); await until(() => f.sent.some(value => value.type === 'video.answer'));
  assert.equal(f.pcs[0].rollbacks, 1); assert.equal(f.pcs[0].candidates.length, 1); assert.equal(f.pcs[0].signalingState, 'stable');
});
test('impolite glare ignores colliding offer and its candidates, then accepts current answer', async t => {
  const f = fixture(t); f.video.setRoster([self(), { ...peer(0, true, 1), deviceId: 'z-peer' }]); await until(() => f.pcs[0].signalingState === 'have-local-offer');
  const incoming = (type, extra) => f.message(type, { deviceId: 'z-peer', ...extra });
  f.video.receive(incoming('video.offer', { kind: 'offer', sdp: 'v=0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\n' })); f.video.receive(incoming('video.candidate', { candidate: 'candidate:collided' })); await tick();
  assert.equal(f.pcs[0].remoteDescription, null); assert.equal(f.pcs[0].candidates.length, 0);
  f.video.receive(incoming('video.answer', { kind: 'answer', sdp: 'v=0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\n' })); await until(() => f.pcs[0].remoteDescription?.type === 'answer');
  assert.equal(f.pcs[0].signalingState, 'stable'); assert.equal(f.faults(), 0);
});

test('publication waits for actual transceiver send direction and rejects audio SDP in video frames', async t => {
  const f = fixture(t, { blockDirection: true }), p = f.prepare(); let settled = false;
  const pending = p.stage.commit().then(value => { settled = true; return value; }); f.acknowledge(p.admission);
  await until(() => f.pcs[0]?.attaches === 1); await tick(); assert.equal(settled, false); assert.equal(p.track.enabled, false);
  f.pcs[0].transceiver.currentDirection = 'sendrecv'; await pending;
  assert.throws(() => f.video.receive(f.message('video.offer', { fromCameraGeneration: 0, toCameraGeneration: 1,
    kind: 'offer', sdp: 'v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n' })));
  p.stage.dispose();
});
