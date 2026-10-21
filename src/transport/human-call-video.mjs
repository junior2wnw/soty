import { createCameraLifecycle, CAMERA_LIMITS } from '../../modules/personal-agent/call/camera-lifecycle.mjs';
import { closedIceRecord, validateCallIceReply } from './human-call-ice.mjs';

export const HUMAN_VIDEO_LIMITS = Object.freeze({ ...CAMERA_LIMITS, peers: 3, candidates: 32, messages: 128, timeoutMs: 10_000 });
const id = value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{1,160}$/u.test(value);
const videoSdp = value => typeof value === 'string' && value.length > 0 && value.length <= 40_000
  && value.split(/\r?\n/u).filter(line => line.startsWith('m=')).length === 1
  && value.split(/\r?\n/u).filter(line => line.startsWith('m=')).every(line => line.startsWith('m=video '));
const gen = value => Number.isSafeInteger(value) && value >= 0;
const fail = (code = 'human_call_video_invalid') => { throw Object.assign(new Error(code), { code }); };
const demand = (value, code) => { if (!value) fail(code); };
const exact = (value, required, optional = []) => closedIceRecord(value, required, optional);
const admissionKeys = ['schema', 'callId', 'epoch', 'accountId', 'deviceId', 'grantRef', 'cameraGeneration', 'expiresAt'];
const scopeKeys = ['accountId', 'deviceId', 'callId', 'epoch', 'joinRef', 'generation'];
const ackKeys = ['type', 'callId', 'epoch', 'grantRef', 'cameraGeneration'];
export const createHumanCallCameraOwner = options => createCameraLifecycle(options);

/** Separate camera-only PCs. No capture, microphone or legacy room code. The
 * private host supplies signed admissions and current call ICE; native IO only
 * is injectable. The camera lifecycle alone owns/stops local capture tracks. */
export function createHumanCallVideoTransport({ scope, mediaInstance, send, rtcFactory = config => new RTCPeerConnection(config),
  renderRemote = () => ({ close() {} }), onFault = () => {}, clock = Date.now } = {}) {
  demand(exact(scope, scopeKeys) && ['accountId', 'deviceId', 'callId', 'joinRef'].every(key => id(scope[key]))
    && Number.isSafeInteger(scope.epoch) && scope.epoch > 0 && gen(scope.generation) && scope.generation > 0);
  demand(id(mediaInstance));
  demand([send, rtcFactory, renderRemote, onFault, clock].every(fn => typeof fn === 'function'));
  scope = Object.freeze({ ...scope });
  const iceScope = Object.freeze(Object.fromEntries(['accountId', 'deviceId', 'callId', 'epoch', 'joinRef'].map(key => [key, scope[key]])));
  const links = new Map(), retained = new Set(), roster = new Map(), tombstones = new Map();
  let closed = false, unavailable = false, ice = null, local = null, allowed = null, pending = null, cleanupFailed = false, nonce = 0;
  const now = () => { const value = clock(); demand(Number.isSafeInteger(value) && value >= 0); return value; };
  const live = link => !closed && links.get(link.id) === link && !link.closed && ice?.expiresAt > now();
  const own = () => roster.get(scope.deviceId) ?? { deviceId: scope.deviceId, accountId: scope.accountId, mediaProtocol: 2, mediaInstance, camera: false, cameraGeneration: 0 };
  const view = () => Object.freeze({ configured: Boolean(ice), activePeers: links.size, retainedPeers: retained.size,
    generation: own().cameraGeneration, cleanupFailed, unavailable, qualified: false });
  const fault = () => { if (unavailable || closed) return; unavailable = true; try { dropAll(); } catch { cleanupFailed = true; } try { onFault('human_call_video_unavailable'); } catch {} };
  function envelope(type, values) { demand(!closed); send(type, values); }
  function signalOut(link, type, values) {
    if (!live(link)) return;
    demand(nonce < Number.MAX_SAFE_INTEGER, 'human_call_video_capacity');
    envelope(type, { signal: { id: 'video_' + (++nonce), targetDeviceId: link.id,
      fromMediaInstance: mediaInstance, toMediaInstance: link.peerInstance, fromCameraGeneration: link.selfGeneration, toCameraGeneration: link.peerGeneration, ...values } });
  }
  function forgetOutput(link, track) {
    const output = link.receivers.get(track); let unknown = false;
    try { track.enabled = false; } catch { unknown = true; }
    try { output?.close(); } catch { unknown = true; }
    try { track.stop(); } catch { unknown = true; }
    try { if (track.readyState !== 'ended') unknown = true; } catch { unknown = true; }
    if (unknown) link.cleanupFailed = true; else link.receivers.delete(track);
  }
  function gateReceivers(link) {
    const peer = roster.get(link.id);
    for (const [track, output] of [...link.receivers]) {
      if (!live(link) || !peer?.camera || peer.cameraGeneration !== link.peerGeneration) { forgetOutput(link, track); continue; }
      if (output) continue;
      try {
        const rendered = renderRemote(Object.freeze({ deviceId: link.id, accountId: peer.accountId, cameraGeneration: link.peerGeneration,
          track, stream: new MediaStream([track]) }));
        demand(rendered && typeof rendered.close === 'function');
        link.receivers.set(track, rendered); track.enabled = true;
        if (!live(link)) forgetOutput(link, track);
      } catch { forgetOutput(link, track); fault(); }
    }
  }
  function closeLink(link) {
    if (link.closed) { if (link.cleanupFailed) fail('human_call_video_cleanup_unknown'); return; }
    link.closed = true; if (links.get(link.id) === link) links.delete(link.id);
    link.pc.ontrack = null; link.pc.onicecandidate = null; link.pc.onnegotiationneeded = null;
    link.pc.onconnectionstatechange = null; link.pc.oniceconnectionstatechange = null;
    for (const track of [...link.receivers.keys()]) forgetOutput(link, track);
    try { link.pc.close(); if (link.pc.connectionState !== 'closed') link.cleanupFailed = true; } catch { link.cleanupFailed = true; }
    link.candidates.length = 0;
    if (link.cleanupFailed) { cleanupFailed = true; fail('human_call_video_cleanup_unknown'); }
    retained.delete(link);
  }
  function dropAll() { let unknown = false; for (const link of [...retained]) { try { closeLink(link); } catch { unknown = true; } } if (unknown) fail('human_call_video_cleanup_unknown'); }
  function chain(link, action) {
    if (++link.queued > HUMAN_VIDEO_LIMITS.messages) { fault(); return Promise.resolve(); }
    const result = link.tail.then(async () => { if (live(link)) await action(); });
    link.tail = result.catch(() => { if (live(link)) fault(); }).finally(() => { link.queued--; });
    return link.tail;
  }
  function offer(link) {
    return chain(link, async () => {
      if (link.makingOffer || link.pc.signalingState !== 'stable') return;
      link.makingOffer = true;
      try {
        await link.pc.setLocalDescription(); if (!live(link)) return;
        const value = link.pc.localDescription; demand(value?.type === 'offer' && videoSdp(value.sdp));
        signalOut(link, 'video.offer', { kind: 'offer', sdp: value.sdp || '' });
      } finally { link.makingOffer = false; }
    });
  }
  async function attachLocal(link, stage) {
    if (!stage || !live(link) || local !== stage || stage.retired) return;
    const settings = stage.track.getSettings();
    demand(stage.track.kind === 'video' && stage.track.readyState === 'live' && settings.width > 0 && settings.height > 0 && settings.frameRate > 0
      && settings.width <= HUMAN_VIDEO_LIMITS.width && settings.height <= HUMAN_VIDEO_LIMITS.height && settings.frameRate <= HUMAN_VIDEO_LIMITS.frameRate,
    'human_call_video_constraints_unverified');
    // The transceiver starts with a null track and capped one-layer encodings.
    // No enabled capture is attached until native setParameters + readback pass.
    const sender = link.transceiver.sender;
    const parameters = sender.getParameters();
    demand(parameters && Array.isArray(parameters.encodings) && parameters.encodings.length === 1, 'human_call_video_caps_unverified');
    parameters.encodings = [{ ...parameters.encodings[0], maxBitrate: HUMAN_VIDEO_LIMITS.bitrate,
      maxFramerate: HUMAN_VIDEO_LIMITS.frameRate, scaleResolutionDownBy: 1 }];
    await sender.setParameters(parameters); if (!stage || !live(link) || local !== stage || stage.retired) return;
    const readback = sender.getParameters(), encoding = readback?.encodings?.[0];
    demand(Array.isArray(readback?.encodings) && readback.encodings.length === 1 && encoding
      && Number.isFinite(encoding.maxBitrate) && encoding.maxBitrate > 0 && encoding.maxBitrate <= HUMAN_VIDEO_LIMITS.bitrate
      && Number.isFinite(encoding.maxFramerate) && encoding.maxFramerate > 0 && encoding.maxFramerate <= HUMAN_VIDEO_LIMITS.frameRate
      && Number.isFinite(encoding.scaleResolutionDownBy) && encoding.scaleResolutionDownBy >= 1, 'human_call_video_caps_unverified');
    if (!stage || !live(link) || local !== stage || stage.retired) return;
    link.sender = sender; link.transceiver.direction = 'sendrecv';
    await sender.replaceTrack(stage.track);
    if (!live(link) || local !== stage || stage.retired) { try { await sender.replaceTrack(null); } catch { cleanupFailed = true; } return; }
    link.attached = stage; void offer(link);
  }
  function ensurePeer(peer) {
    const self = own(), old = links.get(peer.deviceId);
    if (old && old.selfGeneration === self.cameraGeneration && old.peerGeneration === peer.cameraGeneration && old.peerInstance === peer.mediaInstance) {
      if (local && self.camera && old.attached !== local && !old.attaching) {
        old.attaching = chain(old, () => attachLocal(old, local)).finally(() => { old.attaching = null; });
      }
      gateReceivers(old); return old;
    }
    if (old) closeLink(old);
    demand(retained.size < HUMAN_VIDEO_LIMITS.peers, 'human_call_video_capacity');
    demand(ice?.expiresAt > now(), 'human_call_video_ice_expired');
    const config = { iceServers: ice.iceServers.map(server => ({ ...server, urls: [...server.urls] })) };
    const pc = rtcFactory(config);
    const link = { id: peer.deviceId, pc, peerInstance: peer.mediaInstance, selfGeneration: self.cameraGeneration, peerGeneration: peer.cameraGeneration,
      transceiver: null, sender: null, attached: null, attaching: null, receivers: new Map(), candidates: [],
      tail: Promise.resolve(), queued: 0, makingOffer: false, ignoreOffer: false, settingAnswer: false, closed: false, cleanupFailed: false };
    links.set(link.id, link); retained.add(link);
    pc.onicecandidate = event => {
      if (!event.candidate || !live(link)) return;
      const value = event.candidate.toJSON(); signalOut(link, 'video.candidate', { candidate: value.candidate,
        ...(value.sdpMid !== undefined ? { sdpMid: value.sdpMid } : {}), ...(value.sdpMLineIndex !== undefined ? { sdpMLineIndex: value.sdpMLineIndex } : {}) });
    };
    pc.onnegotiationneeded = () => { void offer(link); };
    pc.onconnectionstatechange = () => { if (live(link) && ['failed', 'disconnected'].includes(pc.connectionState)) fault(); };
    pc.oniceconnectionstatechange = () => { if (live(link) && pc.iceConnectionState === 'failed') fault(); };
    pc.ontrack = event => {
      // A forged/stale callback cannot make us touch another owner's track.
      let owned = false; try { owned = pc.getReceivers().includes(event.receiver) && event.receiver.track === event.track; } catch {}
      if (!owned) return;
      const track = event.track;
      if (link.receivers.has(track)) return;
      link.receivers.set(track, null); track.enabled = false;
      if (!live(link) || track.kind !== 'video' || link.receivers.size > 1 || !roster.get(link.id)?.camera) { forgetOutput(link, track); return; }
      gateReceivers(link);
    };
    try {
      link.transceiver = pc.addTransceiver('video', { direction: 'recvonly', sendEncodings: [{ maxBitrate: HUMAN_VIDEO_LIMITS.bitrate,
        maxFramerate: HUMAN_VIDEO_LIMITS.frameRate, scaleResolutionDownBy: 1 }] });
      if (local && self.camera) link.attaching = chain(link, () => attachLocal(link, local)).finally(() => { link.attaching = null; });
      void offer(link); return link;
    } catch (error) { try { closeLink(link); } catch {} throw error; }
  }
  function reconcile() {
    if (closed || unavailable) return;
    const self = own();
    for (const link of [...links.values()]) {
      const peer = roster.get(link.id);
      if (!ice || !peer || peer.mediaProtocol !== 2 || !(self.camera || peer.camera)
        || link.selfGeneration !== self.cameraGeneration || link.peerGeneration !== peer.cameraGeneration || link.peerInstance !== peer.mediaInstance) closeLink(link);
    }
    if (!ice || ice.expiresAt <= now()) return;
    for (const peer of roster.values()) if (peer.deviceId !== scope.deviceId && peer.mediaProtocol === 2 && (self.camera || peer.camera)) ensurePeer(peer);
  }
  function setRoster(values) {
    demand(Array.isArray(values) && values.length <= 4);
    const next = new Map(), accounts = new Set();
    for (const peer of values) {
      demand(peer && id(peer.deviceId) && id(peer.accountId) && !next.has(peer.deviceId) && !accounts.has(peer.accountId));
      if (peer.mediaProtocol === 2) demand(id(peer.mediaInstance) && typeof peer.camera === 'boolean' && gen(peer.cameraGeneration) && (!peer.camera || peer.cameraGeneration > 0));
      else demand(peer.mediaProtocol === undefined);
      const previous = roster.get(peer.deviceId), sameInstance = previous?.mediaInstance === peer.mediaInstance, dead = sameInstance ? tombstones.get(peer.deviceId) : undefined;
      if (!sameInstance) tombstones.delete(peer.deviceId);
      demand(!previous || previous.accountId === peer.accountId);
      if (dead !== undefined && (peer.cameraGeneration < dead || peer.camera && peer.cameraGeneration === dead)) return view();
      if (sameInstance && previous?.mediaProtocol === 2 && peer.mediaProtocol === 2 && peer.cameraGeneration < previous.cameraGeneration) return view();
      if (sameInstance && previous?.camera && !peer.camera && previous.cameraGeneration === peer.cameraGeneration) tombstones.set(peer.deviceId, peer.cameraGeneration);
      next.set(peer.deviceId, { accountId: peer.accountId, deviceId: peer.deviceId, mediaProtocol: peer.mediaProtocol, mediaInstance: peer.mediaInstance,
        camera: peer.camera === true, cameraGeneration: peer.cameraGeneration ?? 0 }); accounts.add(peer.accountId);
    }
    const self = next.get(scope.deviceId); demand(self && self.accountId === scope.accountId && self.mediaProtocol === 2 && self.mediaInstance === mediaInstance);
    if (local && !local.retired && self.cameraGeneration === local.admission.cameraGeneration && !self.camera) fault();
    for (const key of tombstones.keys()) if (!next.has(key)) tombstones.delete(key);
    roster.clear(); for (const [key, value] of next) roster.set(key, value);
    try { reconcile(); } catch { fault(); } return view();
  }
  function noteAdmission(value) {
    demand(exact(value, admissionKeys) && value.schema === 'soty.human-camera-admission.v1'
      && ['accountId', 'deviceId', 'callId', 'epoch'].every(key => value[key] === scope[key]) && id(value.grantRef)
      && gen(value.cameraGeneration) && value.cameraGeneration >= own().cameraGeneration && !own().camera && value.expiresAt > now() && value.expiresAt <= now() + 60_000);
    demand(!local && !pending);
    allowed = Object.freeze({ ...value });
    const self = { ...own(), camera: false, cameraGeneration: value.cameraGeneration }; roster.set(scope.deviceId, self);
    try { reconcile(); } catch { fault(); } return allowed;
  }
  function receive(message) {
    if (closed || message?.callId !== scope.callId || message.epoch !== scope.epoch) return;
    if (message.type === 'call.video.published') {
      if (pending && exact(message, ackKeys) && message.grantRef === pending.admission.grantRef
        && message.cameraGeneration === pending.admission.cameraGeneration) { const waiter = pending; pending = null; clearTimeout(waiter.timer); waiter.resolve(Object.freeze({ ...message })); }
      return;
    }
    if (!['video.offer', 'video.answer', 'video.candidate'].includes(message.type)) return;
    const value = message.signal, candidate = message.type === 'video.candidate', kind = message.type === 'video.offer' ? 'offer' : 'answer';
    demand(exact(value, ['id', 'deviceId', 'targetDeviceId', 'fromMediaInstance', 'toMediaInstance', 'fromCameraGeneration', 'toCameraGeneration', ...(candidate ? ['candidate'] : ['kind', 'sdp'])], candidate ? ['sdpMid', 'sdpMLineIndex'] : []));
    const link = links.get(value.deviceId);
    if (!link || value.targetDeviceId !== scope.deviceId || value.fromMediaInstance !== link.peerInstance || value.toMediaInstance !== mediaInstance || value.fromCameraGeneration !== link.peerGeneration || value.toCameraGeneration !== link.selfGeneration) return;
    demand(id(value.id));
    if (candidate) {
      demand(typeof value.candidate === 'string' && value.candidate.length > 0 && value.candidate.length <= 8_000);
      demand(value.sdpMid === undefined || value.sdpMid === null || typeof value.sdpMid === 'string' && value.sdpMid.length <= 120);
      demand(value.sdpMLineIndex === undefined || value.sdpMLineIndex === null || gen(value.sdpMLineIndex) && value.sdpMLineIndex <= 128);
    } else demand(value.kind === kind && videoSdp(value.sdp));
    const selected = candidate ? { candidate: value.candidate, ...(value.sdpMid !== undefined ? { sdpMid: value.sdpMid } : {}),
      ...(value.sdpMLineIndex !== undefined ? { sdpMLineIndex: value.sdpMLineIndex } : {}) } : { type: kind, sdp: value.sdp };
    void chain(link, async () => {
      if (candidate) {
        if (link.ignoreOffer) return;
        if (!link.pc.remoteDescription) { demand(link.candidates.length < HUMAN_VIDEO_LIMITS.candidates, 'human_call_video_candidate_capacity'); link.candidates.push(selected); return; }
        await link.pc.addIceCandidate(selected); return;
      }
      const collision = kind === 'offer' && (link.makingOffer || link.pc.signalingState !== 'stable' && !link.settingAnswer);
      link.ignoreOffer = scope.deviceId < link.id && collision; if (link.ignoreOffer) return;
      link.settingAnswer = kind === 'answer';
      try { await link.pc.setRemoteDescription(selected); } finally { link.settingAnswer = false; }
      if (!live(link)) return;
      for (const item of link.candidates.splice(0)) { if (!live(link)) return; await link.pc.addIceCandidate(item); }
      if (kind === 'offer') {
        await link.pc.setLocalDescription(); if (!live(link)) return;
        const answer = link.pc.localDescription; demand(answer?.type === 'answer' && videoSdp(answer.sdp)); signalOut(link, 'video.answer', { kind: 'answer', sdp: answer.sdp || '' });
      }
    });
  }
  function prepare({ scope: offeredScope, admission, track, limits, signal }) {
    demand(!closed && exact(offeredScope, scopeKeys) && scopeKeys.every(key => offeredScope[key] === scope[key])
      && allowed && exact(admission, admissionKeys) && admissionKeys.every(key => admission[key] === allowed[key])
      && signal && !signal.aborted && !local && !pending && limits && Object.keys(CAMERA_LIMITS).every(key => limits[key] === CAMERA_LIMITS[key]));
    const stage = { admission: allowed, track, retired: false,
      dispose() {
        if (stage.retired) return; stage.retired = true;
        if (pending?.stage === stage) { const waiter = pending; pending = null; clearTimeout(waiter.timer); waiter.reject(Error('human_call_video_cancelled')); }
        if (local === stage) local = null; if (allowed === stage.admission) allowed = null;
        const self = { ...own() }; if (self.cameraGeneration === stage.admission.cameraGeneration) { self.camera = false; roster.set(scope.deviceId, self); tombstones.set(scope.deviceId, self.cameraGeneration); }
        // Closing owned PCs detaches local senders; never stop capture here.
        let unknown = false; try { dropAll(); } catch { unknown = true; }
        if (!closed) { try { envelope('call.video.withdraw', { cameraGeneration: stage.admission.cameraGeneration }); } catch {} }
        if (unknown) fail('human_call_video_cleanup_unknown');
      },
      async commit() {
        demand(!closed && !stage.retired && !signal.aborted && allowed === stage.admission && !pending && !local);
        const ack = await new Promise((resolve, reject) => {
          const timer = setTimeout(() => { if (pending?.stage === stage) pending = null; reject(Error('human_call_video_publish_unknown')); }, HUMAN_VIDEO_LIMITS.timeoutMs);
          pending = { stage, admission: stage.admission, timer, resolve, reject };
          try { envelope('call.video.publish', { grantRef: stage.admission.grantRef, cameraGeneration: stage.admission.cameraGeneration }); }
          catch (error) { clearTimeout(timer); pending = null; reject(error); }
        });
        demand(!closed && !stage.retired && !signal.aborted && allowed === stage.admission && stage.admission.expiresAt > now());
        local = stage; roster.set(scope.deviceId, { ...own(), camera: true, cameraGeneration: stage.admission.cameraGeneration });
        reconcile();
        const started = performance.now();
        for (;;) {
          demand(!closed && !unavailable && !stage.retired && !signal.aborted && local === stage && performance.now() - started < HUMAN_VIDEO_LIMITS.timeoutMs, 'human_call_video_network_unavailable');
          const peers = [...links.values()];
          if (peers.length && peers.every(link => live(link) && link.attached === stage && link.sender.track === stage.track && link.pc.connectionState === 'connected'
            && ['sendrecv', 'sendonly'].includes(link.transceiver.currentDirection))) break;
          await new Promise(resolve => setTimeout(resolve, 20));
        }
        return ack;
      } };
    return stage;
  }
  return Object.freeze({ setRoster, noteAdmission, receive, prepare, snapshot: view,
    retry() { demand(!closed && !cleanupFailed); unavailable = false; reconcile(); },
    setIce(value) {
      const next = validateCallIceReply(value, iceScope, { now: now() });
      if (ice && next.revision <= ice.revision) return false;
      dropAll(); ice = next; unavailable = false; reconcile(); return true;
    },
    close() {
      if (closed) { if (cleanupFailed) fail('human_call_video_cleanup_unknown'); return; }
      try { if (local) local.dispose(); else pending?.stage.dispose(); } finally { closed = true; allowed = null;
        if (pending) { clearTimeout(pending.timer); pending.reject(Error('human_call_video_closed')); pending = null; }
        roster.clear(); tombstones.clear(); dropAll(); }
    },
  });
}
