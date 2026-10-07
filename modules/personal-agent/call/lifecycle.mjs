// Browser-neutral local resource owner. Trusted adapters supply all authority/IO.
const MODES = new Set(['one-to-one', 'group']);
const EXTRAS = new Set(['agent-invite', 'recording', 'transcript']);
const validId = value => typeof value === 'string' && value.length > 0 && value.length <= 256;
const ok = () => Object.freeze({ ok: true });
const no = code => Object.freeze({ ok: false, code });
const opaque = value => value !== null && (typeof value === 'object' || typeof value === 'function');
function interruptible(work, signal) {
  return new Promise(resolve => {
    const abort = () => resolve(no(['join_failed', 'microphone_failed'].includes(signal.reason) ? signal.reason : 'stale'));
    signal.addEventListener('abort', abort, { once: true });
    if (signal.aborted) abort();
    work.then(resolve, () => resolve(no('operation_failed'))).finally(() => signal.removeEventListener('abort', abort));
  });
}

export function createCallLifecycle({ host, media, transport, onState = () => {} }) {
  if (typeof host?.authorize !== 'function' || typeof media?.acquire !== 'function'
    || typeof transport?.join !== 'function' || typeof onState !== 'function') throw new TypeError('invalid_ports');
  let identity = null, generation = 0, active = null, last = null, closed = false, notificationQueued = false, teardown = 0;
  // Slots own actual work/resources, not just the current UI generation.
  const capacity = 16, leases = new Set();
  const reserve = () => {
    if (leases.size >= capacity) return null;
    const lease = { pendingWork: true, retired: false, pendingDisposals: 0, cleanupFailed: false };
    leases.add(lease); return lease;
  };
  const release = lease => {
    if (lease.retired && !lease.pendingWork && !lease.pendingDisposals && !lease.cleanupFailed) leases.delete(lease);
  };
  const current = c => active === c && !c.abort.signal.aborted && !closed;
  const micCurrent = (c, m) => current(c) && c.mic === m && !m.abort.signal.aborted;
  const evidence = c => {
    return Object.freeze({ ...c.trackStats,
      pendingCaptureResults: c.pendingCapture, pendingJoinResults: c.pendingJoin,
      pendingPublishResults: c.pendingPublish, pendingDisposals: c.pendingDisposals,
      disposalFailures: c.disposalFailures, remoteTerminationVerified: false });
  };
  const snapshot = () => {
    const c = active || last;
    return Object.freeze({ state: active?.state || (closed ? 'disposed' : c?.state || 'idle'),
      microphone: active?.micState || 'off', scope: c?.scope || null, code: c?.code || null,
      extras: Object.freeze({ agentInvite: 'not_integrated', recording: 'not_integrated', transcript: 'not_integrated' }),
      capacity: Object.freeze({ limit: capacity, occupied: leases.size }),
      localEvidence: c ? evidence(c) : null });
  };
  const notify = () => {
    if (notificationQueued) return;
    notificationQueued = true;
    queueMicrotask(() => { notificationQueued = false; try { onState(snapshot()); } catch {} });
  };
  // Disposal cannot delay stopping local capture; errors never expose port payloads.
  function dispose(c, resource, method = 'dispose', lease = c.lease) {
    if (resource === null || resource === undefined) return;
    if (!opaque(resource)) { c.disposalFailures++; return; }
    if (c.disposed.has(resource)) return;
    c.disposed.add(resource);
    c.pendingDisposals++; lease.pendingDisposals++;
    teardown++;
    try {
      const result = resource[method]();
      Promise.resolve(result).catch(() => { c.disposalFailures++; lease.cleanupFailed = true; }).finally(() => {
        c.pendingDisposals--; lease.pendingDisposals--; release(lease); notify();
      });
    } catch { c.disposalFailures++; lease.cleanupFailed = true; c.pendingDisposals--; lease.pendingDisposals--; }
    finally { teardown--; }
  }
  function observeTrack(c, track) {
    let record = c.tracks.get(track);
    if (!record) { record = {}; c.tracks.set(track, record); c.trackStats.observedLocalTracks++; }
    return record;
  }
  function stop(c, tracks, lease) {
    teardown++;
    try {
    for (const track of tracks) {
      if (!opaque(track)) { lease.cleanupFailed = true; continue; }
      const record = observeTrack(c, track);
      const mark = (key, counter) => { if (!record[key]) { record[key] = true; c.trackStats[counter]++; } };
      try { track.enabled = false; } catch { mark('disableFailed', 'disableFailures'); }
      try { track.stop(); mark('stopReturned', 'stopCallsReturned'); } catch { mark('stopFailed', 'stopCallFailures'); }
      let ended = false;
      try { ended = track.readyState === 'ended'; if (ended) mark('ended', 'endedLocallyObserved'); } catch {}
      if (!ended) lease.cleanupFailed = true;
    }
    notify();
    } finally { teardown--; }
  }
  function retireMic(c, reason = 'stale') {
    const m = c.mic;
    c.mic = null; c.micState = 'off';
    if (!m) return;
    teardown++;
    try {
    m.abort.abort(reason);
    for (const remove of m.removers) { try { remove(); } catch {} } m.removers = [];
    stop(c, m.tracks, m.lease); m.tracks = [];
    dispose(c, m.stage, 'dispose', m.lease);
    m.lease.retired = true; release(m.lease);
    } finally { teardown--; }
  }
  function finish(reason) {
    const c = active;
    if (!c) return snapshot();
    teardown++;
    try {
    active = null; last = c; c.state = 'ended'; c.code = reason;
    c.abort.abort(reason === 'join_failed' ? 'join_failed' : 'stale'); retireMic(c); dispose(c, c.session, 'close');
    c.lease.retired = true; release(c.lease);
    notify(); return snapshot();
    } finally { teardown--; }
  }
  function setIdentity(value) {
    if (closed) return no('disposed');
    if (teardown) return no('call_transition');
    teardown++;
    try {
      let accountId, deviceId;
      try { accountId = value?.accountId; deviceId = value?.deviceId; } catch {}
      if (closed) return no('disposed');
      if (value !== null && (!validId(accountId) || !validId(deviceId))) {
        identity = null; finish('identity_invalid'); return no('invalid_identity');
      }
      const next = value === null ? null : Object.freeze({ accountId, deviceId });
      const changed = identity?.accountId !== next?.accountId || identity?.deviceId !== next?.deviceId;
      identity = next;
      if (changed) finish('identity_changed');
      return ok();
    } finally { teardown--; }
  }
  function join({ roomId, mode } = {}) {
    if (closed) return Promise.resolve(no('disposed'));
    if (teardown) return Promise.resolve(no('call_transition'));
    if (!identity) return Promise.resolve(no('identity_required'));
    if (!validId(roomId) || !MODES.has(mode)) return Promise.resolve(no('invalid_room'));
    if (active) return active.scope.roomId === roomId && active.scope.mode === mode
      ? active.joinPromise : Promise.resolve(no('call_active'));
    const lease = reserve();
    if (!lease) return Promise.resolve(no('call_capacity'));
    const c = { scope: Object.freeze({ ...identity, roomId, mode, generation: ++generation }),
      abort: new AbortController(), state: 'joining', micState: 'off', code: null,
      mic: null, session: null, lease, tracks: new WeakMap(), disposed: new WeakSet(),
      trackStats: { observedLocalTracks: 0, stopCallsReturned: 0, endedLocallyObserved: 0, stopCallFailures: 0, disableFailures: 0 },
      pendingCapture: 0, pendingJoin: 0, pendingPublish: 0, pendingDisposals: 0, disposalFailures: 0 };
    active = c;
    const work = Promise.resolve().then(async () => {
      try {
        if (!current(c)) return no('stale');
        const authorize = host.authorize;
        if (!current(c)) return no('stale');
        const admission = await Reflect.apply(authorize, host, [c.scope, 'join', { signal: c.abort.signal }]);
        if (!current(c)) return no('stale');
        if (!opaque(admission)) throw new Error(); // Shape is NOT proof of signed authority.
        let session;
        const joinPort = transport.join;
        if (!current(c)) return no('stale');
        c.pendingJoin++;
        try { session = await Reflect.apply(joinPort, transport, [{ scope: c.scope, admission, signal: c.abort.signal }]); }
        finally { c.pendingJoin--; }
        if (!current(c)) { dispose(c, session, 'close'); return no('stale'); }
        c.session = session;
        const close = session?.close;
        if (!current(c)) { dispose(c, session, 'close'); return no('stale'); }
        const prepare = session?.preparePublication;
        if (!current(c)) { dispose(c, session, 'close'); return no('stale'); }
        if (typeof close !== 'function' || typeof prepare !== 'function') throw new Error();
        c.prepare = prepare;
        c.state = 'listening'; notify(); return ok();
      } catch {
        if (!current(c)) return no('stale');
        finish('join_failed'); return no('join_failed');
      } finally { lease.pendingWork = false; release(lease); notify(); }
    });
    c.joinPromise = interruptible(work, c.abort.signal);
    notify(); return c.joinPromise;
  }
  function enableMicrophone() {
    if (teardown) return Promise.resolve(no('call_transition'));
    const c = active;
    if (!c || c.state !== 'listening') return Promise.resolve(no('not_listening'));
    if (c.mic) return c.mic.promise;
    const lease = reserve();
    if (!lease) return Promise.resolve(no('call_capacity'));
    const m = { abort: new AbortController(), tracks: [], stage: null, promise: null, removers: [], lease };
    c.mic = m; c.micState = 'permission'; c.code = null;
    const work = Promise.resolve().then(async () => {
      try {
        if (!micCurrent(c, m)) return no('stale');
        const authorize = host.authorize;
        if (!micCurrent(c, m)) return no('stale');
        const admission = await Reflect.apply(authorize, host, [c.scope, 'microphone', { signal: m.abort.signal }]);
        if (!micCurrent(c, m)) return no('stale');
        if (!opaque(admission)) throw new Error();
        let stream;
        const acquire = media.acquire;
        if (!micCurrent(c, m)) return no('stale');
        c.pendingCapture++;
        try { stream = await Reflect.apply(acquire, media, [{ audio: true, video: false, scope: c.scope, signal: m.abort.signal }]); }
        finally { c.pendingCapture--; }
        // Native getUserMedia cannot reliably be cancelled: adopt/stop late results.
        let tracks;
        try { tracks = stream.getTracks(); }
        catch { lease.cleanupFailed = true; throw new Error(); }
        // Inspect own indexed data; never invoke a returned Array's iterator or
        // index accessor. Retain all safely found tracks before rejecting shape.
        let keys, length, malformed = false, unknown = false;
        try {
          keys = Reflect.ownKeys(tracks);
        } catch { lease.cleanupFailed = true; throw new Error(); }
        try { length = Object.getOwnPropertyDescriptor(tracks, 'length')?.value; } catch { unknown = true; }
        try { malformed = !Array.isArray(tracks) || Object.getPrototypeOf(tracks) !== Array.prototype; }
        catch { malformed = true; unknown = true; }
        if (!Number.isSafeInteger(length) || length < 0) unknown = true;
        const seen = new Set(); let indices = 0;
        for (const key of keys) {
          if (key === 'length') continue;
          if (typeof key !== 'string' || !/^(0|[1-9][0-9]*)$/.test(key) || Number(key) >= 4294967295) { malformed = true; unknown = true; continue; }
          if (Number(key) >= length) unknown = true;
          indices++;
          let descriptor;
          try { descriptor = Object.getOwnPropertyDescriptor(tracks, key); } catch { unknown = true; continue; }
          if (!descriptor) { unknown = true; continue; }
          if (!Object.hasOwn(descriptor, 'value')) { unknown = true; continue; }
          if (!seen.has(descriptor.value)) { seen.add(descriptor.value); m.tracks.push(descriptor.value); }
        }
        if (indices !== length) unknown = true;
        if (unknown) lease.cleanupFailed = true;
        if (malformed || unknown) throw new Error();
        for (const track of m.tracks) { observeTrack(c, track); track.enabled = false; }
        if (!micCurrent(c, m)) { stop(c, m.tracks, lease); m.tracks = []; return no('stale'); }
        if (!m.tracks.length || m.tracks.length > 8 || m.tracks.some(t => t.kind !== 'audio' || t.readyState !== 'live' || typeof t.stop !== 'function')) throw new Error();
        if (!micCurrent(c, m)) { stop(c, m.tracks, lease); m.tracks = []; return no('stale'); }
        for (const track of m.tracks) {
          const ended = () => { if (micCurrent(c, m)) { retireMic(c); c.code = 'microphone_ended'; notify(); } };
          if (typeof track.addEventListener === 'function' && typeof track.removeEventListener === 'function') {
            track.addEventListener('ended', ended);
            m.removers.push(() => track.removeEventListener('ended', ended));
          }
        }
        if (!micCurrent(c, m)) { stop(c, m.tracks, lease); m.tracks = []; return no('stale'); }
        c.micState = 'publishing'; notify();
        c.pendingPublish++;
        let stage;
        try { stage = await Reflect.apply(c.prepare, c.session, [{ scope: c.scope, tracks: Object.freeze([...m.tracks]), admission, signal: m.abort.signal }]); }
        finally { c.pendingPublish--; }
        m.stage = stage;
        if (!micCurrent(c, m)) { dispose(c, stage, 'dispose', lease); return no('stale'); }
        const disposeStage = stage?.dispose;
        if (!micCurrent(c, m)) { dispose(c, stage, 'dispose', lease); return no('stale'); }
        const commit = stage?.commit;
        if (!micCurrent(c, m)) { dispose(c, stage, 'dispose', lease); return no('stale'); }
        if (typeof disposeStage !== 'function' || typeof commit !== 'function') throw new Error();
        c.pendingPublish++;
        try { await Reflect.apply(commit, stage, [{ signal: m.abort.signal }]); }
        finally { c.pendingPublish--; }
        if (!micCurrent(c, m)) { dispose(c, stage, 'dispose', lease); return no('stale'); }
        if (m.tracks.some(track => track.readyState !== 'live')) throw new Error();
        if (!micCurrent(c, m)) { stop(c, m.tracks, lease); return no('stale'); }
        for (const track of m.tracks) {
          if (!micCurrent(c, m)) { stop(c, m.tracks, lease); return no('stale'); }
          track.enabled = true;
        }
        if (!micCurrent(c, m)) { stop(c, m.tracks, lease); return no('stale'); }
        c.micState = 'on'; notify(); return ok();
      } catch {
        for (const remove of m.removers) { try { remove(); } catch {} } m.removers = [];
        stop(c, m.tracks, lease); m.tracks = []; dispose(c, m.stage, 'dispose', lease);
        if (!micCurrent(c, m)) return no('stale');
        retireMic(c, 'microphone_failed'); c.code = 'microphone_failed'; notify(); return no('microphone_failed');
      } finally { lease.pendingWork = false; release(lease); notify(); }
    });
    m.promise = interruptible(work, m.abort.signal);
    notify(); return m.promise;
  }
  return Object.freeze({ snapshot, setIdentity, join, enableMicrophone,
    mute() { if (active?.mic) { retireMic(active); notify(); } return snapshot(); },
    end() { return finish('ended'); }, cancel() { return finish('cancelled'); },
    revoke() { return finish('revoked'); },
    // Route changes are not account transitions and never tear down a call.
    navigate() { return snapshot(); },
    requestExtra(name) { return no(EXTRAS.has(name) ? 'feature_not_integrated' : 'invalid_feature'); },
    dispose() { closed = true; finish('disposed'); notify(); return snapshot(); },
  });
}
