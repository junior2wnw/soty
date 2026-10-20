import assert from 'node:assert/strict';
import {createVoiceOrbClient} from '../../../src/platform/voice-orb-client.mjs';
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
    openProjectEditor: options.openProjectEditor, readSelection: options.readSelection ?? (()=>null),
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


export {deferred,flush,viewStatus,Events,Peer,fixture};
