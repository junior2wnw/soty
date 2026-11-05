const SCHEMA = 'soty.relay-window-identity.v1';

/** The shell sends a grant only to its current iframe at its exact app origin. */
export function createRelayIdentityBridge({ view, isCurrent, getFrame, getEntry, getBinding, getRuntimeScope, getHostSession, request, close }) {
  let disposed = false, paused = false, cached = null, flight = null, generation = 0;
  const issued = new Map();
  const now = () => view.performance.now();
  const bounded = promise => new Promise((resolve, reject) => {
    const timer = view.setTimeout(() => reject(Object.assign(Error('soty_identity_window_unavailable'), { code: 'soty_identity_close_timeout' })), 5000);
    Promise.resolve(promise).then(resolve, reject).finally(() => view.clearTimeout(timer));
  });
  function peer() {
    const frame = getFrame(), entry = getEntry(), binding = getBinding();
    if (disposed || paused || !isCurrent() || !frame?.isConnected || !frame.contentWindow || !entry || !binding) return null;
    const scope = getRuntimeScope?.() ?? null;
    if (scope) {
      if (Object.keys(scope).sort().join(',') !== 'instanceId,runtimeOrigin'
        || scope.instanceId!=='main'&&!/^widget-[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/u.test(scope.instanceId ?? '')) return null;
      try { const runtime=new URL(scope.runtimeOrigin),canonical=new URL(entry.origin),label=runtime.hostname.split('.')[0];
        const prefix=scope.instanceId==='main'?'app-m'+entry.appId.slice(4)+'-':'app-w'+scope.instanceId.slice(7).replaceAll('-','')+'-';
        if(runtime.origin!==scope.runtimeOrigin||runtime.origin===entry.origin||runtime.protocol!==canonical.protocol||runtime.port!==canonical.port
          ||!label.startsWith(prefix)||!/^[a-f0-9]{24}$/u.test(label.slice(prefix.length))
          ||runtime.hostname.slice(label.length)!==canonical.hostname.slice(canonical.hostname.indexOf('.')))return null; }
      catch { return null; }
    }
    const proof = getHostSession?.() ?? null;
    if (proof && (typeof proof !== 'object' || Array.isArray(proof)
      || Object.keys(proof).sort().join(',') !== 'frameId,lease,revision'
      || !/^[A-Za-z0-9_-]{43}$/u.test(proof.frameId ?? '') || !/^[A-Za-z0-9_-]{43}$/u.test(proof.lease ?? '')
      || !Number.isSafeInteger(proof.revision) || proof.revision < 1)) return null;
    if (scope?.instanceId === 'main' && !proof) return null;
    const hostSession = proof ? { frameId: proof.frameId, lease: proof.lease, revision: proof.revision } : null;
    const runtimeScope = scope ? {instanceId:scope.instanceId,runtimeOrigin:scope.runtimeOrigin} : null;
    return { frame, window: frame.contentWindow, entry, binding, runtimeScope, hostSession, origin:runtimeScope?.runtimeOrigin ?? entry.origin,
      key: JSON.stringify([entry.appId, entry.domainId, entry.origin, binding, runtimeScope, hostSession?.frameId ?? null, generation]) };
  }
  const still = expected => { const next = peer(); return next && next.frame === expected.frame && next.window === expected.window && next.key === expected.key; };
  async function grant(expected) {
    if (cached && cached.frame === expected.frame && cached.key === expected.key && cached.until > now()) return cached.value;
    if (flight && flight.frame === expected.frame && flight.key === expected.key) return flight.promise;
    const started = now();
    const promise = (async () => {
      const value = await request({ appId: expected.entry.appId, domainId: expected.entry.domainId, launchBinding: expected.binding,
        ...(expected.runtimeScope ?? {}), ...(expected.hostSession ? {hostSession:expected.hostSession} : {}) });
      if (value?.schema !== 'soty.relay-window-grant.v1' || typeof value.grant !== 'string'
        || !/^[A-Za-z0-9_-]{43}$/u.test(value.grant) || !Number.isSafeInteger(value.expiresAt)) throw Error('soty_identity_window_unavailable');
      for (const [token, item] of issued) if (item.until <= now()) issued.delete(token);
      // The server issued the grant no later than this response. Keep it until
      // it must have expired, including time spent in the request itself.
      if (!still(expected)) {
        void bounded(close({ appId: expected.entry.appId, grant: value.grant })).catch(() => {});
        throw Error('soty_identity_window_unavailable');
      }
      issued.set(value.grant, { appId: expected.entry.appId, until: now() + 30000 });
      // The server grants for 30 s. Elapsed time bounds the remaining lifetime
      // without trusting the browser's wall clock or retrying app requests.
      const lifetime = Math.min(20000, 30000 - (now() - started) - 2000);
      if (lifetime <= 0) throw Error('soty_identity_window_unavailable');
      cached = { frame: expected.frame, key: expected.key, value: value.grant, until: now() + lifetime };
      return cached.value;
    })();
    flight = { frame: expected.frame, key: expected.key, promise };
    try { return await promise; } finally { if (flight?.promise === promise) flight = null; }
  }
  const receive = event => {
    const data = event.data, expected = peer();
    if (!expected || event.source !== expected.window || event.origin !== expected.origin || !data || data.schema !== SCHEMA) return;
    if (data.type === 'invalidate' && Object.keys(data).sort().join(',') === 'grant,schema,type'
      && typeof data.grant === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(data.grant)) {
      if (cached?.frame === expected.frame && cached.key === expected.key && cached.value === data.grant) cached = null;
      return;
    }
    if (Object.keys(data).sort().join(',') !== 'requestId,schema,type' || data.type !== 'request'
      || typeof data.requestId !== 'string' || !/^[a-f0-9-]{36}$/u.test(data.requestId)) return;
    void grant(expected).then(value => {
      if (still(expected)) expected.window.postMessage({ schema: SCHEMA, type: 'grant', requestId: data.requestId, grant: value }, expected.origin);
    }).catch(() => {
      if (still(expected)) expected.window.postMessage({ schema: SCHEMA, type: 'unavailable', requestId: data.requestId }, expected.origin);
    });
  };
  view.addEventListener('message', receive);
  async function clear() {
    paused = true;
    if (flight) await bounded(flight.promise).catch(error => { if (error?.code === 'soty_identity_close_timeout') throw error; });
    cached = null;
    for (const [value, item] of issued) {
      if (item.until <= now()) { issued.delete(value); continue; }
      const result = await bounded(close({ appId: item.appId, grant: value }));
      if (!result || result.closed !== true) throw Error('soty_identity_window_unavailable');
      issued.delete(value);
    }
  }
  return Object.freeze({
    clear,
    // A Root boot reset invalidates volatile grants, not the Source document.
    // Old RAM may already be gone: close is idempotent/best effort here. Normal
    // host transport renewal does not call this or extend a grant's lifetime.
    reset() {
      const previous = [...issued]; generation++; cached = null; flight = null; issued.clear();
      for (const [grant, item] of previous) if (item.until > now()) {
        void bounded(close({ appId: item.appId, grant })).catch(() => {});
      }
    },
    hasActive() { return Boolean(flight || issued.size); },
    resume() { if (!disposed) paused = false; },
    dispose() { if (disposed) return; disposed = true; view.removeEventListener('message', receive); void clear().catch(() => {}); },
  });
}
