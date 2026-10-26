const SCHEMA = 'soty.app-work.v1';

/** Optional save handshake. The application still owns its data and storage. */
export function createSotyAppWork({ parentOrigin, isDirty, save, view = globalThis.window } = {}) {
  let origin;
  try { origin = new URL(parentOrigin); } catch { throw new TypeError('soty_work_parent_origin_required'); }
  if (!['http:', 'https:'].includes(origin.protocol) || origin.origin !== parentOrigin ||
      typeof isDirty !== 'function' || typeof save !== 'function' || !view?.parent || view.parent === view)
    throw new TypeError('soty_work_options_invalid');
  let session = null, revision = 0, disposed = false, saving = null;
  const read = () => { const value = isDirty(); if (typeof value !== 'boolean') throw Error('soty_work_dirty_invalid'); return value; };
  const send = data => { if (!disposed) view.parent.postMessage({ schema: SCHEMA, session, ...data }, parentOrigin); };
  const receive = event => {
    const data = event.data;
    if (disposed || event.source !== view.parent || event.origin !== parentOrigin || !data || data.schema !== SCHEMA ||
        typeof data.session !== 'string' || !/^[a-f0-9-]{36}$/u.test(data.session)) return;
    if (data.type === 'probe') {
      session = data.session;
      try { send({ type: 'state', dirty: read(), revision }); }
      catch { send({ type: 'state', error: true, revision }); }
      return;
    }
    if (data.session !== session || !['check', 'save'].includes(data.type) ||
        typeof data.requestId !== 'string' || !/^[a-f0-9-]{36}$/u.test(data.requestId)) return;
    const captured = session;
    void (async () => {
      try {
        if (data.type === 'save' && read()) {
          if (!saving) saving = Promise.resolve().then(save).finally(() => { saving = null; });
          await saving;
        }
        const dirty = read();
        if (!disposed && session === captured) send({ type: 'reply', requestId: data.requestId, dirty, revision });
      } catch {
        if (!disposed && session === captured) send({ type: 'reply', requestId: data.requestId, error: true, revision });
      }
    })();
  };
  view.addEventListener('message', receive);
  // An app may initialise after the iframe load event. Ask the already mounted
  // shell for its current challenge without supplying identity or authority.
  view.parent.postMessage({ schema: SCHEMA, type: 'hello' }, parentOrigin);
  return Object.freeze({
    changed() { if (disposed) return; revision++; if (!session) return;
      try { send({ type: 'changed', dirty: read(), revision }); }
      catch { send({ type: 'changed', error: true, revision }); } },
    dispose() { if (disposed) return; disposed = true; view.removeEventListener('message', receive); },
  });
}
