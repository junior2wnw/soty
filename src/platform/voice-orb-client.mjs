const id = value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{1,160}$/u.test(value);
const failure = code => Object.assign(new Error(code), { code });
const safeSelection = value => value && /^app-[a-f0-9]{32}$/u.test(value.appId) && /^[A-Za-z0-9_-]{43}$/u.test(value.handle)
  ? { appId: value.appId, handle: value.handle } : undefined;
const sameSelection = (a, b) => a?.appId === b?.appId && a?.handle === b?.handle;

/** One explicitly started Realtime conversation. No media, model or network call
 * happens on construction. Server sideband owns completed function/task/usage authority;
 * browser metadata is a trigger, never task text or a permission grant. */
export function createVoiceOrbClient({ client, accountId, audio, acquireCaptureLease, readSelection = () => null,
  takeNotePermission = () => false, isCurrent = () => true, onChange = () => {},
  getUserMedia = value => navigator.mediaDevices.getUserMedia(value),
  makePeer = () => new RTCPeerConnection(), makeStream = tracks => new MediaStream(tracks),
  idFactory = () => crypto.randomUUID(), events = globalThis.window } = {}) {
  if (!client || !audio || typeof acquireCaptureLease !== 'function') throw TypeError('voice_orb_ports_required');
  let phase = 'checking', configured = false, missingBinding = null, disposed = false, generation = 0;
  let frame = null, input = '', output = '', answer = '', code = null, playbackBlocked = false, refreshVersion = 0;
  const views = new Set();
  const view = () => ({ phase, configured, missingBinding, microphone: Boolean(frame?.tracks.some(track => track.enabled && track.readyState !== 'ended')),
    muted: frame?.muted ?? false, outputMuted: audio.muted === true, playbackBlocked, input, output, answer, code,
    active: Boolean(frame && !frame.cleaned), hangupAccepted: frame?.hangupAccepted ?? false,
    remoteTerminationVerified: frame?.terminationVerified ?? false, accountingFinalized: false, playbackVerified: false });
  const notify = () => {
    if (disposed) return;
    try { onChange(view()); } catch {}
    for (const listener of [...views]) { try { listener(view()); } catch {} }
  };
  const localCurrent = () => !disposed && isCurrent() === true;
  const current = async (owned = frame) => {
    if (!localCurrent()) throw failure('voice_orb_denied');
    const state = await client.getLocalState();
    if (!localCurrent() || state.accountId !== accountId || owned && (state.deviceId !== owned.deviceId || state.localRevision !== owned.localRevision))
      throw failure('voice_orb_denied');
  };
  const rpc = async (op, args, owned = frame) => {
    if (owned && (!belongs(owned) || owned.stopping)) throw failure('voice_orb_denied');
    await current(owned);
    if (owned && (!belongs(owned) || owned.stopping)) throw failure('voice_orb_denied');
    const result = await client.extension(op, args, { expectedAccountId: accountId });
    await current(owned); return result;
  };
  const cleanupRpc = async (op, args, owned) => {
    // Unmount retires presentation, not this owner's right to stop its session.
    // This path can only read/close the exact old request; it cannot start work.
    if (!['voice.orb.status', 'voice.orb.end'].includes(op)) throw failure('voice_orb_denied');
    const state = await client.getLocalState();
    if (state.accountId !== accountId || state.deviceId !== owned.deviceId) throw failure('voice_orb_denied');
    return client.extension(op, args, { expectedAccountId: accountId });
  };
  const ownsFrame = owned => frame === owned && owned.generation === generation && !owned.cleaned;
  const belongs = owned => ownsFrame(owned) && localCurrent();
  function release(owned) {
    if (!owned.cleaned || owned.capturePending || owned.cleanupFailed) return;
    owned.release?.(); owned.release = null;
  }
  function cleanup(owned) {
    if (owned.cleaned) return;
    owned.cleaned = true; clearTimeout(owned.readyTimer); clearTimeout(owned.durationTimer); clearTimeout(owned.closeTimer); clearInterval(owned.identityTimer);
    for (const track of owned.tracks) {
      try { track.enabled = false; track.stop(); if (track.readyState !== 'ended') owned.cleanupFailed = true; }
      catch { owned.cleanupFailed = true; }
    }
    try { owned.channel?.close(); } catch {} try { owned.peer?.close(); } catch {}
    if (frame === owned) { audio.pause(); audio.srcObject = null; }
    release(owned);
  }
  function closeKnownSession(owned, sessionId) {
    owned.rootClosures ??= new Map();
    if (owned.rootClosures.has(sessionId)) return owned.rootClosures.get(sessionId);
    // A late bootstrap may reveal its exact session AFTER early End's status
    // lookup said unavailable. This independent once-only close adopts it.
    const pending = owned.sessionId === sessionId && typeof owned.cleanupRoot === 'function'
      ? owned.cleanupRoot() : cleanupRpc('voice.orb.end', { sessionId }, owned);
    owned.rootClosures.set(sessionId, pending); return pending;
  }
  async function adoptLateSession(owned, sessionId) {
    try {
      const result = await closeKnownSession(owned, sessionId);
      owned.hangupAccepted = result?.hangupAccepted === true; owned.terminationVerified = result?.terminationVerified === true;
    } catch { owned.hangupAccepted = false; owned.terminationVerified = false; }
    cleanup(owned);
    if (frame === owned && !disposed) { phase = owned.hangupAccepted ? 'idle' : 'unknown'; code = owned.hangupAccepted ? 'voice_orb_accounting_pending' : 'voice_orb_unknown'; notify(); }
  }
  async function endOwned(owned, reason = 'owner_requested') {
    if (owned.ending) return owned.ending;
    owned.ending = (async () => {
      owned.stopping = true;
      for (const track of owned.tracks) { try { track.enabled = false; } catch {} }
      audio.pause(); audio.muted = true;
      clearInterval(owned.identityTimer);
      if (frame === owned) { phase = 'ending'; code = null; }
      // Stop physical capture and this peer immediately. Realtime hangup ACK
      // is not a Live final-usage drain; root cleanup still runs below.
      cleanup(owned); if (frame === owned) notify();
      let result;
      try {
        let sessionId = owned.sessionId;
        if (!sessionId && owned.bootstrapAttempted) sessionId = (await cleanupRpc('voice.orb.status', { requestId: owned.requestId }, owned)).sessionId;
        if (sessionId) result = await closeKnownSession(owned, sessionId);
      } catch { code = reason === 'identity_changed' ? 'voice_orb_denied' : 'voice_orb_unknown'; }
      owned.hangupAccepted = result?.hangupAccepted === true;
      owned.terminationVerified = result?.terminationVerified === true;
      cleanup(owned);
      if (frame === owned && !disposed) { phase = owned.hangupAccepted || !owned.bootstrapAttempted ? 'idle' : 'unknown';
        if (owned.hangupAccepted) code = 'voice_orb_accounting_pending'; notify(); }
      return result;
    })();
    return owned.ending;
  }
  async function observeIdentity(owned) {
    try { await current(owned); }
    catch { if (ownsFrame(owned)) { code = 'voice_orb_denied'; void endOwned(owned, 'identity_changed'); } }
  }
  function send(owned, value) {
    if (!belongs(owned) || !owned.ready || owned.stopping || owned.channel?.readyState !== 'open') throw failure('voice_orb_unknown');
    owned.channel.send(JSON.stringify(value));
  }
  async function delegate(owned, callId) {
    if (!belongs(owned) || owned.stopping || owned.calls.has(callId)) return;
    const selected = safeSelection(readSelection());
    const noteCreation = takeNotePermission() === true;
    const intent = { sessionId: owned.sessionId, callId, ...(selected ? { selection: selected } : {}), ...(noteCreation ? { noteCreation: true } : {}) };
    owned.calls.set(callId, intent); owned.latestCallId = callId; phase = 'working'; code = null; notify();
    try {
      const result = await rpc('voice.orb.turn', intent, owned);
      if (!belongs(owned) || owned.stopping || owned.latestCallId !== callId || !sameSelection(selected, safeSelection(readSelection()))) return;
      if (result.schema !== 'soty.voice-orb-turn.v1' || result.sessionId !== owned.sessionId || result.callId !== callId
        || typeof result.answer !== 'string' || result.answer.length > 8000) throw failure('voice_orb_unknown');
      answer = result.answer; phase = 'listening'; notify();
    } catch {
      // Read-only status is the only recovery path. No second turn/model POST.
      if (belongs(owned) && !owned.stopping && owned.latestCallId === callId) { phase = 'unknown'; code = 'voice_orb_unknown'; notify(); }
    }
  }
  function receive(owned, data) {
    if (!belongs(owned) || typeof data !== 'string' || data.length > 262144) return;
    let event;
    try { event = JSON.parse(data); } catch { code = 'voice_orb_unknown'; notify(); return; }
    if (event.type === 'session.created' || event.type === 'session.updated') {
      if (event.session?.id !== owned.providerSessionId || event.session.model !== 'gpt-realtime-2.1') {
        void endOwned(owned, 'session_changed'); return;
      }
      owned.ready = true; clearTimeout(owned.readyTimer); phase = 'listening'; notify();
    } else if (event.type === 'input_audio_buffer.speech_started') {
      owned.speaking = true; owned.latestCallId = null;
    } else if (event.type === 'input_audio_buffer.committed') {
      if (id(event.item_id)) { owned.latestUserItem = event.item_id; owned.speaking = false; }
    } else if (event.type === 'response.output_audio_transcript.delta') {
      if (typeof event.delta === 'string' && event.delta.length <= 8000) { output = (output + event.delta).slice(-8000); notify(); }
    } else if (event.type === 'response.done' && owned.ready && !owned.speaking && event.response?.status === 'completed'
      && event.response.metadata?.soty_input === owned.latestUserItem && id(owned.latestUserItem) && Array.isArray(event.response.output)) {
      const functions = event.response.output.filter(item => item.type === 'function_call');
      if (functions.length !== 1) return;
      const call = functions[0];
      if (call.name !== 'delegate_to_agent' || call.status !== 'completed' || !id(call.call_id)) return;
      // This is display/trigger data only. The signed turn sends no task text;
      // server sideband independently validates the completed structured call.
      try { const args = JSON.parse(call.arguments); if (typeof args.task === 'string' && args.task.length <= 8000) input = args.task; } catch {}
      void delegate(owned, call.call_id);
    } else if (event.type === 'error') { code = 'voice_orb_unknown'; notify(); }
  }
  const onPageHide = () => { if (frame && !frame.cleaned) { void endOwned(frame, 'page_hidden'); cleanup(frame); } };
  events?.addEventListener?.('pagehide', onPageHide);
  return Object.freeze({ view,
    subscribe(listener) { views.add(listener); listener(view()); return () => views.delete(listener); },
    async refresh() {
      const version = ++refreshVersion, expectedGeneration = generation;
      const fresh = () => localCurrent() && version === refreshVersion && generation === expectedGeneration;
      try {
        const result = await rpc('voice.orb.status', {}, null);
        if (!fresh()) return view();
        if (result.schema !== 'soty.voice-orb-status.v1' || result.voiceModel !== 'gpt-realtime-2.1' || result.brainModel !== 'zai-org/GLM-5.3-Flash') throw failure('voice_orb_unknown');
        configured = result.configured === true && result.canStart !== false; missingBinding = result.canStart === false ? 'voice_quota_unavailable' : result.missingBinding ?? null;
        if (!frame || frame.cleaned) phase = configured ? 'idle' : 'disabled'; code = null;
      } catch { if (!fresh()) return view(); configured = false; phase = 'disabled'; code = 'voice_orb_unavailable'; }
      notify(); return view();
    },
    async start() {
      if (!localCurrent() || !configured || phase === 'ending' || frame && !frame.cleaned) return;
      const permission = acquireCaptureLease();
      if (typeof permission !== 'function') { code = 'voice_orb_busy'; notify(); return; }
      const owned = { generation: ++generation, requestId: idFactory(), release: permission, tracks: [], capturePending: false,
        cleaned: false, ready: false, stopping: false, calls: new Map(), hangupAccepted: false, terminationVerified: false };
      frame = owned; phase = 'connecting'; code = null; input = output = answer = ''; playbackBlocked = false; audio.muted = false; notify();
      try {
        const state = await client.getLocalState();
        if (!localCurrent() || state.accountId !== accountId || !id(state.deviceId) || !Number.isSafeInteger(state.localRevision)) throw failure('voice_orb_denied');
        owned.deviceId = state.deviceId; owned.localRevision = state.localRevision; await current(owned);
        if (!belongs(owned) || owned.stopping) return;
        owned.peer = makePeer();
        owned.peer.addEventListener('track', event => {
          if (!belongs(owned) || owned.stopping) return;
          audio.srcObject = makeStream([event.track]);
          void audio.play().catch(() => { playbackBlocked = true; notify(); });
        });
        owned.peer.addEventListener('connectionstatechange', () => {
          if (belongs(owned) && !owned.stopping && ['closed', 'failed', 'disconnected'].includes(owned.peer.connectionState)) void endOwned(owned, 'connection_lost');
        });
        owned.capturePending = true;
        let stream;
        try { stream = await getUserMedia({ audio: true, video: false }); }
        finally { owned.capturePending = false; }
        const tracks = stream.getTracks(); owned.tracks = tracks;
        if (!belongs(owned) || owned.stopping) {
          for (const track of tracks) { try { track.enabled = false; track.stop(); if (track.readyState !== 'ended') owned.cleanupFailed = true; } catch { owned.cleanupFailed = true; } }
          release(owned); return;
        }
        await current(owned);
        if (!belongs(owned) || owned.stopping) { await endOwned(owned, 'identity_changed'); return; }
        for (const track of stream.getAudioTracks()) owned.peer.addTrack(track, stream);
        owned.channel = owned.peer.createDataChannel('oai-events');
        owned.channel.addEventListener('message', event => receive(owned, event.data));
        owned.channel.addEventListener('close', () => { if (belongs(owned) && !owned.stopping) void endOwned(owned, 'connection_lost'); });
        const offer = await owned.peer.createOffer(); await owned.peer.setLocalDescription(offer);
        if (owned.peer.iceGatheringState !== 'complete') await new Promise((resolve, reject) => {
          const timeout = setTimeout(() => done(false), 10000);
          const changed = () => { if (owned.peer.iceGatheringState === 'complete') done(true); };
          const done = success => { clearTimeout(timeout); owned.peer.removeEventListener('icegatheringstatechange', changed);
            if (success && belongs(owned) && !owned.stopping) resolve(); else reject(failure('voice_orb_unknown')); };
          owned.peer.addEventListener('icegatheringstatechange', changed); changed();
        });
        if (!belongs(owned) || owned.stopping) return;
        const sdp = owned.peer.localDescription?.sdp;
        if (typeof sdp !== 'string' || !sdp.trim() || sdp.length > 65536) throw failure('voice_orb_invalid');
        await current(owned);
        if (!belongs(owned) || owned.stopping) return;
        owned.bootstrapAttempted = true;
        // Adopt a returned owned handle before presentation checks; an unmount
        // or early End cannot make a late paid session disappear.
        const result = await client.extension('voice.orb.start', { requestId: owned.requestId, sdp }, { expectedAccountId: accountId });
        if (result.schema !== 'soty.voice-orb-started.v1' || !id(result.sessionId) || !id(result.providerSessionId)
          || result.voiceModel !== 'gpt-realtime-2.1' || result.brainModel !== 'zai-org/GLM-5.3-Flash' || typeof result.sdp !== 'string') throw failure('voice_orb_unknown');
        owned.sessionId = result.sessionId; owned.providerSessionId = result.providerSessionId;
        owned.cleanupRoot = client.voiceSessionCleanup?.(result) ?? null;
        if (!belongs(owned) || owned.stopping) { await adoptLateSession(owned, result.sessionId); return; }
        await current(owned);
        if (!belongs(owned) || owned.stopping) { await adoptLateSession(owned, result.sessionId); return; }
        if (!Number.isSafeInteger(result.remainingDurationMs) || result.remainingDurationMs <= 0 || result.remainingDurationMs > 900000)
          throw failure('voice_orb_unknown');
        const remaining = Math.min(result.remainingDurationMs, Math.max(0, Number(result.expiresAt) - Date.now()));
        if (!Number.isFinite(remaining) || remaining <= 0) throw failure('voice_orb_expired');
        owned.durationTimer = setTimeout(() => { void endOwned(owned, 'duration_limit'); }, remaining);
        owned.readyTimer = setTimeout(() => { if (!owned.ready) void endOwned(owned, 'readiness_timeout'); }, 15000);
        await owned.peer.setRemoteDescription({ type: 'answer', sdp: result.sdp });
        if (!belongs(owned) || owned.stopping) return;
        // Unified HTTP created the call. Browser never sends session.start or tool output.
        owned.identityTimer = setInterval(() => { void observeIdentity(owned); }, 1000);
      } catch { if (ownsFrame(owned)) { code = 'voice_orb_unknown'; await endOwned(owned, 'startup_unknown'); } }
      finally { release(owned); }
    },
    mute() {
      if (!frame || frame.cleaned || frame.stopping) return;
      frame.muted = !frame.muted;
      for (const track of frame.tracks) track.enabled = !frame.muted;
      notify();
    },
    async interrupt() {
      const owned = frame; if (!owned || owned.cleaned) return;
      owned.latestCallId = null; audio.pause(); audio.muted = true; notify();
      try { if (owned.sessionId) await rpc('voice.orb.cancel', { sessionId: owned.sessionId }, owned); }
      catch { code = 'voice_orb_unknown'; notify(); }
      // A cancel or buffer-clear ACK is not permission to resume stale queued audio.
      // Playback stays muted until the person explicitly selects play/resume.
    },
    async resumePlayback() {
      if (!frame || frame.cleaned || frame.stopping) return;
      audio.muted = false;
      try { await audio.play(); playbackBlocked = false; } catch { playbackBlocked = true; }
      notify();
    },
    async check() {
      const owned = frame; if (!owned?.sessionId || owned.cleaned) return;
      try {
        const result = await rpc('voice.orb.status', { sessionId: owned.sessionId }, owned);
        if (!belongs(owned) || owned.stopping) return;
        if (result.task?.state === 'completed' && typeof result.task.result?.answer === 'string') { answer = result.task.result.answer; phase = 'listening'; }
        code = result.task?.state === 'unknown' ? 'voice_orb_unknown' : null;
      } catch { if (!belongs(owned) || owned.stopping) return; code = 'voice_orb_unknown'; }
      notify();
    },
    async end() { if (frame && !frame.cleaned) return endOwned(frame); },
    dispose() {
      if (disposed) return;
      const owned = frame;
      if (owned && !owned.cleaned) { void endOwned(owned, 'disposed'); cleanup(owned); }
      phase = 'idle'; notify(); disposed = true; generation++; refreshVersion++; events?.removeEventListener?.('pagehide', onPageHide); views.clear();
    },
  });
}
