import test from 'node:test';
import assert from 'node:assert/strict';
import { createVoiceOrbClient } from '../../src/platform/voice-orb-client.mjs';

const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
const flush = () => new Promise(resolve => setImmediate(resolve));
const viewStatus = { schema: 'soty.voice-orb-status.v1', configured: true, voiceModel: 'gpt-realtime-2.1', brainModel: 'zai-org/GLM-5.3-Flash' };
class Events {
  listeners = new Map();
  addEventListener(type, listener) { const set = this.listeners.get(type) ?? new Set(); set.add(listener); this.listeners.set(type, set); }
  removeEventListener(type, listener) { this.listeners.get(type)?.delete(listener); }
  emit(type, value = {}) { for (const listener of [...this.listeners.get(type) ?? []]) listener(value); }
}
class Peer extends Events {
  iceGatheringState = 'complete'; connectionState = 'new'; closed = 0; channel = new Events();
  constructor() { super(); this.channel.readyState = 'open'; this.channel.close = () => { this.channel.readyState = 'closed'; this.channel.emit('close'); }; this.channel.send = () => {}; }
  addTrack() {}
  createDataChannel(label) { assert.equal(label, 'oai-events'); return this.channel; }
  async createOffer() { return { type: 'offer', sdp: 'synthetic_offer' }; }
  async setLocalDescription(offer) { this.localDescription = offer; }
  async setRemoteDescription() { this.remoteCalls = (this.remoteCalls ?? 0) + 1; await this.beforeRemote?.(); this.connectionState = 'connected'; this.message({ type: 'session.created', session: { id: this.providerId, model: 'gpt-realtime-2.1' } }); }
  message(value) { this.channel.emit('message', { data: JSON.stringify(value) }); }
  close() { this.closed++; this.connectionState = 'closed'; }
}
function fixture(t, options = {}) {
  let mediaCalls = 0, leaseCount = 0, releases = 0, peerCount = 0, sequence = 0;
  const calls = [], tracks = [], peers = [], views = [], local = { accountId: 'alice', deviceId: 'device_a', localRevision: 1 };
  const audio = { muted: false, srcObject: null, pauseCount: 0, playCount: 0, pause() { this.pauseCount++; }, async play() { this.playCount++; } };
  const client = { async getLocalState() { await options.readLocal?.(); return { ...local }; },
    async extension(op, args, context) {
      calls.push({ op, args: structuredClone(args), context });
      const override = await options.rpc?.(op, args); if (override !== undefined) return override;
      if (op === 'voice.orb.status') return args.requestId ? Promise.reject(new Error('voice_orb_unavailable')) : args.sessionId
        ? { ...viewStatus, sessionId: args.sessionId, task: null } : { ...viewStatus, configured: options.configured !== false };
      if (op === 'voice.orb.start') { const n = ++sequence; peers.at(-1).providerId = 'provider_' + n;
        return { schema: 'soty.voice-orb-started.v1', sessionId: 'owned_' + n, providerSessionId: 'provider_' + n,
          voiceModel: 'gpt-realtime-2.1', brainModel: 'zai-org/GLM-5.3-Flash', expiresAt: Date.now() + 15000, remainingDurationMs: 15000, sdp: 'synthetic_answer' }; }
      if (op === 'voice.orb.turn') return { schema: 'soty.voice-orb-turn.v1', sessionId: args.sessionId, callId: args.callId, answer: 'Подтверждённый результат.' };
      if (op === 'voice.orb.end') return { hangupAccepted: true, terminationVerified: false, accountingFinalized: false };
      if (op === 'voice.orb.cancel') return { cancelRequested: true, remoteTerminationVerified: false };
      throw new Error('unexpected_fixture_operation');
    } };
  const voice = createVoiceOrbClient({ client, accountId: 'alice', audio, events: new Events(), onChange: value => views.push(value),
    acquireCaptureLease() { if (leaseCount) return null; leaseCount++; return () => { releases++; leaseCount--; }; },
    async getUserMedia() { mediaCalls++; if (options.media) return options.media();
      const track = { enabled: true, readyState: 'live', stops: 0, stop() { this.stops++; this.readyState = 'ended'; } }; tracks.push(track);
      return { getTracks: () => [track], getAudioTracks: () => [track] }; },
    makePeer() { peerCount++; const peer = new Peer(); peer.beforeRemote = options.remote; peers.push(peer); return peer; }, makeStream: value => value,
    idFactory: () => 'request_' + (sequence + 1),
  });
  t.after(() => voice.dispose());
  return { voice, audio, local, calls, tracks, peers, views,
    counts: () => ({ mediaCalls, leaseCount, releases, peerCount }),
    async start() { await voice.refresh(); await voice.start(); assert.equal(voice.view().phase, 'listening'); },
  };
}

test('default disabled state and construction never capture audio or create a provider session', async t => {
  const f = fixture(t, { configured: false }); assert.equal(f.calls.length, 0);
  await f.voice.refresh(); await f.voice.start();
  assert.equal(f.voice.view().phase, 'disabled'); assert.equal(f.counts().mediaCalls, 0);
  assert.deepEqual(f.calls.map(row => row.op), ['voice.orb.status']);
});

test('End while initial local state is pending prevents even a later microphone request', async t => {
  const gate = deferred(), entered = deferred(); let reads = 0;
  const f = fixture(t, { async readLocal() { if (++reads === 3) { entered.resolve(); await gate.promise; } } });
  await f.voice.refresh(); const pending = f.voice.start(); await entered.promise;
  await f.voice.end(); gate.resolve(); await pending;
  assert.equal(f.counts().mediaCalls, 0); assert.equal(f.counts().releases, 1);
});

test('late getUserMedia result after End stops real owned tracks before releasing its capture lease', async t => {
  const gate = deferred(), entered = deferred();
  const track = { enabled: true, readyState: 'live', stops: 0, stop() { this.stops++; this.readyState = 'ended'; } };
  const f = fixture(t, { media() { entered.resolve(); return gate.promise; } });
  await f.voice.refresh(); const pending = f.voice.start(); await entered.promise; await f.voice.end();
  assert.equal(f.counts().releases, 0); assert.equal(f.counts().leaseCount, 1);
  gate.resolve({ getTracks: () => [track], getAudioTracks: () => [track] }); await pending;
  assert.equal(track.stops, 1); assert.equal(track.enabled, false); assert.equal(f.counts().releases, 1);
  assert.equal(f.calls.some(row => row.op === 'voice.orb.start'), false);
});

test('a start admitted AFTER early End status-unavailable still receives exactly one owned close', async t => {
  const gate = deferred(), entered = deferred();
  const f = fixture(t, { async rpc(op) { if (op === 'voice.orb.start') { entered.resolve(); await gate.promise;
    return { schema: 'soty.voice-orb-started.v1', sessionId: 'late_owned', providerSessionId: 'late_provider', expiresAt: Date.now() + 15000, remainingDurationMs: 15000, sdp: 'late_sdp', voiceModel: 'gpt-realtime-2.1', brainModel: 'zai-org/GLM-5.3-Flash' }; } } });
  await f.voice.refresh(); const pending = f.voice.start(); await entered.promise;
  await f.voice.end(); assert.equal(f.voice.view().phase, 'unknown');
  gate.resolve(); await pending;
  const ends = f.calls.filter(row => row.op === 'voice.orb.end'); assert.equal(ends.length, 1);
  assert.deepEqual(ends[0].args, { sessionId: 'late_owned' }); assert.equal(f.counts().releases, 1);
  assert.equal(f.voice.view().phase, 'idle'); assert.equal(f.voice.view().hangupAccepted, true);
});

test('held status for A cannot overwrite a new conversation B after End A and Start B', async t => {
  const gate = deferred(), entered = deferred(); let hold = true;
  const f = fixture(t, { async rpc(op, args) { if (op === 'voice.orb.status' && args.sessionId === 'owned_1' && hold) {
    hold = false; entered.resolve(); await gate.promise; return { ...viewStatus, task: { state: 'completed', result: { answer: 'Stale A secret' } } };
  } } });
  await f.start(); const pending = f.voice.check(); await entered.promise; await f.voice.end(); await f.voice.start();
  gate.resolve(); await pending; assert.equal(f.voice.view().answer, ''); assert.equal(f.voice.view().phase, 'listening');
});

test('whole-session A→B→A between RPCs cannot delegate a new task with a fresh revision', async t => {
  const f = fixture(t); await f.start();
  f.local.accountId = 'bob'; f.local.deviceId = 'device_b'; f.local.localRevision++;
  f.local.accountId = 'alice'; f.local.deviceId = 'device_a'; f.local.localRevision++;
  f.peers[0].message({ type: 'input_audio_buffer.committed', item_id: 'fixture_user' });
  f.peers[0].message({ type: 'response.done', response: { status: 'completed', metadata: { soty_input: 'fixture_user' }, output: [{ type: 'function_call', status: 'completed', name: 'delegate_to_agent', call_id: 'new_task', arguments: '{"task":"Сделай записку"}' }] } }); await flush(); await flush();
  assert.equal(f.calls.filter(row => row.op === 'voice.orb.turn').length, 0);
});

test('interruption pauses immediately, requests backend cancellation and stays muted until explicit resume', async t => {
  const f = fixture(t); await f.start(); await f.voice.interrupt();
  assert.equal(f.audio.muted, true); assert.equal(f.calls.filter(row => row.op === 'voice.orb.cancel').length, 1);
  f.peers[0].message({ type: 'output_audio_buffer.cleared', event_id: 'synthetic_ack' }); await flush(); assert.equal(f.audio.muted, true);
  await f.voice.resumePlayback(); assert.equal(f.audio.muted, false); assert.ok(f.audio.playCount > 0);
});

test('duplicate delegation metadata and unknown response never automatically dispatch a second turn', async t => {
  const f = fixture(t, { async rpc(op) { if (op === 'voice.orb.turn') throw new Error('synthetic_network_unknown'); } });
  await f.start(); const metadata = { type: 'response.done', response: { status: 'completed', metadata: { soty_input: 'fixture_user' }, output: [{ type: 'function_call', status: 'completed', name: 'delegate_to_agent', call_id: 'once_task', arguments: '{"task":"Сделай записку"}' }] } };
  f.peers[0].message({ type: 'input_audio_buffer.committed', item_id: 'fixture_user' });
  f.peers[0].message(metadata); await flush(); await flush(); f.peers[0].message(metadata); await flush();
  assert.equal(f.calls.filter(row => row.op === 'voice.orb.turn').length, 1); assert.equal(f.voice.view().phase, 'unknown');
  await f.voice.check(); assert.equal(f.calls.filter(row => row.op === 'voice.orb.turn').length, 1);
});


test('late refresh after dispose emits no obsolete state into a new voice panel', async t => {
  const gate = deferred(), entered = deferred();
  const f = fixture(t, { async rpc(op, args) { if (op === 'voice.orb.status' && !args.sessionId && !args.requestId) { entered.resolve(); await gate.promise; return viewStatus; } } });
  const pending = f.voice.refresh(); await entered.promise; f.voice.dispose();
  const eventsAfterDispose = f.views.length; gate.resolve(); await pending;
  assert.equal(f.views.length, eventsAfterDispose);
});


test('End physically stops capture before a slow root hangup answer arrives', async t => {
  const gate = deferred(), entered = deferred();
  const f = fixture(t, { async rpc(op) { if (op === 'voice.orb.end') { entered.resolve(); await gate.promise; return { hangupAccepted: true, terminationVerified: false, accountingFinalized: false }; } } });
  await f.start(); const pending = f.voice.end(); await entered.promise;
  assert.equal(f.tracks[0].readyState, 'ended'); assert.equal(f.tracks[0].enabled, false); assert.equal(f.peers[0].closed, 1);
  assert.equal(f.counts().releases, 1); gate.resolve(); await pending;
});


test('End during post-bootstrap local fence prevents later peer setup and all new start timers', async t => {
  const gate = deferred(), entered = deferred(); let reads = 0, intervalCalls = 0;
  const original = globalThis.setInterval, intervals = [];
  globalThis.setInterval = (...args) => { intervalCalls++; const timer = original(...args); intervals.push(timer); return timer; };
  const f = fixture(t, { async readLocal() { if (++reads === 7) { entered.resolve(); await gate.promise; } } });
  try {
    await f.voice.refresh(); const pending = f.voice.start(); await entered.promise; await f.voice.end(); gate.resolve(); await pending;
    assert.equal(f.peers[0].remoteCalls ?? 0, 0); assert.equal(intervalCalls, 0); assert.equal(f.tracks[0].readyState, 'ended');
  } finally { gate.resolve(); for (const timer of intervals) clearInterval(timer); globalThis.setInterval = original; }
});

test('End during setRemoteDescription cannot install a late identity polling interval', async t => {
  const gate = deferred(), entered = deferred(); let intervalCalls = 0;
  const original = globalThis.setInterval, intervals = [];
  globalThis.setInterval = (...args) => { intervalCalls++; const timer = original(...args); intervals.push(timer); return timer; };
  const f = fixture(t, { async remote() { entered.resolve(); await gate.promise; } });
  try {
    await f.voice.refresh(); const pending = f.voice.start(); await entered.promise; await f.voice.end(); gate.resolve(); await pending;
    assert.equal(f.peers[0].remoteCalls, 1); assert.equal(intervalCalls, 0); assert.equal(f.peers[0].closed, 1);
  } finally { gate.resolve(); for (const timer of intervals) clearInterval(timer); globalThis.setInterval = original; }
});
