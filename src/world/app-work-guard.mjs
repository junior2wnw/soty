const SCHEMA = 'soty.app-work.v1';

/** Works only with the selected iframe and current account; never inspects its DOM. */
export function createAppWorkGuard({ view, getFrame, isCurrent, onStatus = () => {}, timeoutMs = 2500 }) {
  let active = null, pending = null, flight = null, supported = false, dirty = false, failed = false, revision = -1;
  const live = () => !!active && isCurrent() && getFrame() === active.frame && active.frame.isConnected;
  const status = () => onStatus({ supported, dirty: dirty || failed });
  const cancel = () => { if (pending) { const old = pending; pending = null; view.clearTimeout(old.timer); old.reject(Error('soty_work_frame_changed')); } };
  const send = data => active.frame.contentWindow.postMessage({ schema: SCHEMA, session: active.session, ...data }, active.origin);
  const probe = () => {
    cancel(); active.session = view.crypto.randomUUID(); supported = false; dirty = false; failed = false; revision = -1; status();
    if (live()) send({ type: 'probe' });
  };
  const receive = event => {
    const data = event.data;
    if (!live() || event.source !== active.frame.contentWindow || event.origin !== active.origin || !data ||
        data.schema !== SCHEMA) return;
    if (data.type === 'hello') {
      if (!active.session) active.session = view.crypto.randomUUID();
      try { send({ type: 'probe' }); } catch { /* No authority or state is inferred from a failed probe. */ }
      return;
    }
    if (data.session !== active.session || !['state', 'changed', 'reply'].includes(data.type) ||
        !Number.isSafeInteger(data.revision) || data.revision < 0 || data.revision < revision ||
        (data.error !== true && typeof data.dirty !== 'boolean')) return;
    if (data.type === 'reply' && (!pending || data.requestId !== pending.id)) return;
    supported = true; revision = data.revision;
    if (data.error === true) failed = true; else { dirty = data.dirty; if (data.type === 'reply') failed = false; }
    status();
    if (data.type === 'reply') {
      const result = pending; pending = null; view.clearTimeout(result.timer);
      if (data.error === true) result.reject(Error('soty_work_save_failed')); else result.resolve();
    }
  };
  view.addEventListener('message', receive);
  const request = type => new Promise((resolve, reject) => {
    if (!live() || pending) { reject(Error('soty_work_unavailable')); return; }
    const id = view.crypto.randomUUID();
    const timer = view.setTimeout(() => { if (pending?.id !== id) return; pending = null; failed = true; status(); reject(Error('soty_work_timeout')); }, timeoutMs);
    pending = { id, timer, resolve, reject };
    try { send({ type, requestId: id }); } catch { cancel(); failed = true; status(); }
  });
  return Object.freeze({
    attach(frame, origin) {
      if (active) active.frame.removeEventListener('load', active.load);
      cancel();
      const load = () => { if (live()) probe(); };
      active = { frame, origin, load, session: '' }; supported = false; dirty = false; failed = false; revision = -1; status();
      frame.addEventListener('load', load);
    },
    hasUnsavedChanges: () => dirty || failed,
    requiresCheck: () => supported,
    hasUnconfirmedWork: () => live() && !supported,
    async flush() {
      if (!supported) return;
      if (flight) return flight;
      const frame = active?.frame, session = active?.session;
      flight = (async () => {
        try {
          await request('check');
          if (dirty) await request('save');
          if (!live() || active.frame !== frame || active.session !== session || dirty || failed) throw Error('soty_work_not_saved');
        } catch (error) { if (active?.frame === frame && active?.session === session) { failed = true; status(); } throw error; }
      })().finally(() => { flight = null; });
      return flight;
    },
    dispose() { if (active) active.frame.removeEventListener('load', active.load); cancel(); active = null; view.removeEventListener('message', receive); },
  });
}
