// Public host metadata only. A descriptor never grants recording or Source access.
const exact = (value, keys) => value !== null && typeof value === 'object' && !Array.isArray(value)
  && [Object.prototype, null].includes(Object.getPrototypeOf(value))
  && Reflect.ownKeys(value).length === keys.length
  && keys.every(key => Object.getOwnPropertyDescriptor(value, key)?.enumerable
    && Object.hasOwn(Object.getOwnPropertyDescriptor(value, key), 'value'));
const origin = value => {
  try { const url = new URL(value); return typeof value === 'string' && url.protocol === 'https:' && url.origin === value; }
  catch { return false; }
};
const unavailable = () => Object.assign(new Error('native_voice_descriptor_unavailable'), { code: 'native_voice_descriptor_unavailable' });

export function validateNativeVoiceDescriptor(value) {
  if (value === null) return null;
  if (!exact(value, ['scope', 'appId', 'frameOrigin', 'nativeOrigin', 'parentOrigin', 'source'])
    || value.scope !== 'kvartalufa.voice' || typeof value.appId !== 'string' || !/^app-[a-f0-9]{32}$/u.test(value.appId)
    || !['frameOrigin', 'nativeOrigin', 'parentOrigin'].every(key => origin(value[key]))
    || value.nativeOrigin !== value.frameOrigin || !exact(value.source, ['id', 'version', 'digest'])
    || typeof value.source.id !== 'string' || !/^[A-Za-z0-9_.:-]{1,160}$/u.test(value.source.id)
    || !Number.isSafeInteger(value.source.version) || value.source.version <= 0
    || typeof value.source.digest !== 'string' || !/^[a-f0-9]{64}$/u.test(value.source.digest)) throw unavailable();
  return Object.freeze({ ...value, source: Object.freeze({ ...value.source }) });
}

/** One page owns one bounded lookup; retries are explicit and share inflight IO. */
export function createNativeVoiceDescriptor({ fetch: fetchMetadata = globalThis.fetch, signal } = {}) {
  let state = 'pending', result = null, inflight = null, closed = false;
  const lifetime = new AbortController();
  const close = () => { if (closed) return; closed = true; state = 'closed'; lifetime.abort(); signal?.removeEventListener('abort', close); };
  if (signal?.aborted) close(); else signal?.addEventListener('abort', close, { once: true });
  const begin = () => {
    if (closed || inflight) return inflight;
    state = 'pending';
    const task = (async () => {
      try {
        const response = await fetchMetadata('/api/apps/capabilities', {
          cache: 'no-store', signal: AbortSignal.any([lifetime.signal, AbortSignal.timeout(8000)]),
        });
        if (!response?.ok) throw unavailable();
        const value = await response.json();
        if (closed) return;
        if (!value || typeof value !== 'object' || !Object.hasOwn(value, 'nativeVoice')) throw unavailable();
        result = validateNativeVoiceDescriptor(value.nativeVoice); state = 'ready';
      } catch { if (!closed) { result = null; state = 'unavailable'; } }
    })();
    inflight = task;
    void task.finally(() => { if (inflight === task) inflight = null; });
    return task;
  };
  if (!closed) begin();
  return Object.freeze({
    snapshot: () => Object.freeze({ state }),
    async read({ retry = false } = {}) {
      if (closed) throw unavailable();
      if (state === 'unavailable' && retry) begin();
      if (inflight) await inflight;
      if (closed || state !== 'ready') throw unavailable();
      return result;
    },
    close,
  });
}
