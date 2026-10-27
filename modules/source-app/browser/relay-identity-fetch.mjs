const SCHEMA = 'soty.relay-window-identity.v1';

/** Ask the actual Soty parent for this window's short-lived grant. */
export function createRelayIdentityFetch({ fetch: fetcher = globalThis.fetch, origin = globalThis.location?.origin,
  view = globalThis.window, parentOrigin, timeoutMs = 5000 } = {}) {
  let base;
  try { base = new URL(origin); } catch { throw new TypeError('invalid_soty_origin'); }
  if (typeof fetcher !== 'function' || !['http:', 'https:'].includes(base.protocol) || base.origin !== origin) throw new TypeError('invalid_soty_origin');
  const canAsk = view?.parent && view.parent !== view && typeof view.addEventListener === 'function'
    && typeof view.crypto?.randomUUID === 'function' && Number.isSafeInteger(timeoutMs) && timeoutMs >= 100 && timeoutMs <= 30000;
  const unavailable = () => Object.assign(Error('soty_identity_window_unavailable'), { code: 'soty_identity_window_unavailable' });
  function trustedOrigins(values) {
    if (!Array.isArray(values) || values.length < 1 || values.length > 64) throw unavailable();
    return [...new Set(values.map(value => {
      let parent; try { parent = new URL(value); } catch { throw unavailable(); }
      if (typeof value !== 'string' || !['http:', 'https:'].includes(parent.protocol) || parent.origin !== value || value === origin) throw unavailable();
      return value;
    }))];
  }
  let configuration;
  function configuredOrigins() {
    if (parentOrigin !== undefined) return Promise.resolve().then(() => trustedOrigins([parentOrigin]));
    if (configuration) return configuration;
    const task = (async () => {
      const controller = new AbortController(); let timer;
      try {
        const timeout = new Promise((_, reject) => { timer = view.setTimeout(() => { controller.abort(); reject(unavailable()); }, timeoutMs); });
        const response = await Promise.race([fetcher('/_soty/relay-identity-config', { credentials: 'omit', cache: 'no-store', redirect: 'error', signal: controller.signal }), timeout]);
        if (!response?.ok) throw unavailable();
        const raw = await Promise.race([response.text(), timeout]);
        if (raw.length > 8192) throw unavailable();
        let value; try { value = JSON.parse(raw); } catch { throw unavailable(); }
        if (value?.schema !== 'soty.relay-identity-config.v1') throw unavailable();
        return trustedOrigins(value.shellOrigins);
      } finally { view.clearTimeout(timer); }
    })();
    configuration = task; void task.catch(() => { if (configuration === task) configuration = null; });
    return task;
  }
  const windowGrant = signal => new Promise((resolve, reject) => {
    if (!canAsk) { reject(Object.assign(Error('soty_identity_window_unavailable'), { code: 'soty_identity_window_unavailable' })); return; }
    if (signal?.aborted) { reject(signal.reason); return; }
    let done = false;
    const requestId = view.crypto.randomUUID(), finish = (value, error) => {
      if (done) return; done = true;
      view.clearTimeout(timer); view.removeEventListener('message', receive); signal?.removeEventListener('abort', abort);
      if (error) reject(error); else resolve(value);
    };
    const abort = () => finish(null, signal.reason);
    let origins = [];
    const receive = event => {
      const data = event.data;
      if (event.source !== view.parent || !origins.includes(event.origin) || !data || data.schema !== SCHEMA || data.requestId !== requestId) return;
      if (data.type !== 'grant' || typeof data.grant !== 'string' || !/^[A-Za-z0-9_-]{43}$/u.test(data.grant)) { finish(null, unavailable()); return; }
      finish({ grant: data.grant, parent: event.origin });
    };
    const timer = view.setTimeout(() => finish(null, unavailable()), timeoutMs);
    view.addEventListener('message', receive); signal?.addEventListener('abort', abort, { once: true });
    void configuredOrigins().then(values => {
      if (done) return; origins = values;
      try { for (const target of origins) view.parent.postMessage({ schema: SCHEMA, type: 'request', requestId }, target); }
      catch { finish(null, unavailable()); }
    }).catch(() => finish(null, unavailable()));
  });
  return (path, options = {}) => {
    if (typeof path !== 'string' || !options || typeof options !== 'object') throw new TypeError('invalid_soty_request');
    let url;
    try { url = new URL(path, base); } catch { throw new TypeError('invalid_soty_request'); }
    if (url.origin !== origin || url.hash || options.credentials !== undefined && options.credentials !== 'same-origin'
      || options.mode === 'no-cors' || options.redirect !== undefined && options.redirect !== 'error') throw new TypeError('invalid_soty_request');
    const headers = new Headers(options.headers); headers.set('x-soty-identity-request', '1');
    // Caller-supplied values never select the actor; only the exact parent reply
    // is forwarded. No request is sent when the window cannot be established.
    headers.delete('x-soty-window-grant');
    return windowGrant(options.signal).then(async first => {
      if (options.signal?.aborted) throw options.signal.reason;
      const send = async window => {
        headers.set('x-soty-window-grant', window.grant);
        const response = await fetcher(url.pathname + url.search, { ...options, headers, credentials: 'same-origin', redirect: 'error' });
        const refusedBeforeDispatch = response?.status === 503 && response.headers?.get('x-soty-identity-window') === 'not-dispatched';
        if (refusedBeforeDispatch) {
          try { view.parent.postMessage({ schema: SCHEMA, type: 'invalidate', grant: window.grant }, window.parent); } catch { /* A departed parent stays unavailable. */ }
        }
        return { response, refusedBeforeDispatch };
      };
      const firstResult = await send(first);
      const method = typeof options.method === 'string' ? options.method.toUpperCase() : options.method === undefined ? 'GET' : '';
      if (!firstResult.refusedBeforeDispatch || !['GET', 'HEAD'].includes(method) || options.body != null) return firstResult.response;
      // Root has explicitly refused before any upstream dispatch. Retry only a
      // safe read once; writes always return the original result to the caller.
      let refreshed;
      try { refreshed = await windowGrant(options.signal); }
      catch (error) { if (options.signal?.aborted) throw options.signal.reason; return firstResult.response; }
      return (await send(refreshed)).response;
    });
  };
}
