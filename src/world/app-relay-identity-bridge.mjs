const SCHEMA = 'soty.relay-window-identity.v1';

/** The shell sends a grant only to its current iframe at its exact app origin. */
export function createRelayIdentityBridge({ view, isCurrent, getFrame, getEntry, getBinding, request, close }) {
  let disposed = false, paused = false, cached = null, flight = null;
  const issued = new Map();
  const now = () => view.performance.now();
  const bounded = promise => new Promise((resolve, reject) => {
    const timer = view.setTimeout(() => reject(Object.assign(Error('soty_identity_window_unavailable'), { code: 'soty_identity_close_timeout' })), 5000);
    Promise.resolve(promise).then(resolve, reject).finally(() => view.clearTimeout(timer));
  });
  function peer() {
    const frame = getFrame(), entry = getEntry(), binding = getBinding();
    if (disposed || paused || !isCurrent() || !frame?.isConnected || !frame.contentWindow || !entry || !binding) return null;
    return { frame, window: frame.contentWindow, entry, binding, key: JSON.stringify([entry.appId, entry.domainId, entry.origin, binding]) };
  }
  const still = expected => { const next = peer(); return next && next.frame === expected.frame && next.window === expected.window && next.key === expected.key; };
  async function grant(expected) {
    if (cached && cached.frame === expected.frame && cached.key === expected.key && cached.until > now()) return cached.value;
    if (flight && flight.frame === expected.frame && flight.key === expected.key) return flight.promise;
    const started = now();
    const promise = (async () => {
      const value = await request({ appId: expected.entry.appId, domainId: expected.entry.domainId, launchBinding: expected.binding });
      if (value?.schema !== 'soty.relay-window-grant.v1' || typeof value.grant !== 'string'
        || !/^[A-Za-z0-9_-]{43}$/u.test(value.grant) || !Number.isSafeInteger(value.expiresAt)) throw Error('soty_identity_window_unavailable');
      for (const [token, item] of issued) if (item.until <= now()) issued.delete(token);
      // The server issued the grant no later than this response. Keep it until
      // it must have expired, including time spent in the request itself.
      issued.set(value.grant, { appId: expected.entry.appId, until: now() + 30000 });
      if (!still(expected)) throw Error('soty_identity_window_unavailable');
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
    if (!expected || event.source !== expected.window || event.origin !== expected.entry.origin || !data || data.schema !== SCHEMA) return;
    if (data.type === 'invalidate' && Object.keys(data).sort().join(',') === 'grant,schema,type'
      && typeof data.grant === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(data.grant)) {
      if (cached?.frame === expected.frame && cached.key === expected.key && cached.value === data.grant) cached = null;
      return;
    }
    if (Object.keys(data).sort().join(',') !== 'requestId,schema,type' || data.type !== 'request'
      || typeof data.requestId !== 'string' || !/^[a-f0-9-]{36}$/u.test(data.requestId)) return;
    void grant(expected).then(value => {
      if (still(expected)) expected.window.postMessage({ schema: SCHEMA, type: 'grant', requestId: data.requestId, grant: value }, expected.entry.origin);
    }).catch(() => {
      if (still(expected)) expected.window.postMessage({ schema: SCHEMA, type: 'unavailable', requestId: data.requestId }, expected.entry.origin);
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
    hasActive() { return Boolean(flight || issued.size); },
    resume() { if (!disposed) paused = false; },
    dispose() { if (disposed) return; disposed = true; view.removeEventListener('message', receive); void clear().catch(() => {}); },
  });
}
