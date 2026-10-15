import { ASSIST_SCHEMA, exact, nonce, validBinding, validCommand, sameBinding } from '../../modules/source-app/browser/assist-wire.mjs';

/** Root-owned current-frame locator; no URL navigation or iframe replacement. */
export function createBrowserAssistHost({ view, readPeer, approve = async () => false }) {
  let consent = null, disposed = false, observed = null; const pending = new Set();
  const current = () => {
    const peer = readPeer();
    if (disposed || !consent || !peer || peer.window !== consent.window || !sameBinding(peer.binding, consent.binding)) throw Error('assist_permission_closed');
    return peer;
  };
  const revoke = () => {
    if (consent) consent.window.postMessage({ schema: ASSIST_SCHEMA, revoke: consent.binding.channel }, consent.binding.origin);
    consent = null; observed = null; for (const stop of [...pending]) stop();
  };
  return {
    async probe() {
      const peer = readPeer(); if (disposed || !peer?.window || !validBinding(peer.binding)) return false;
      const id = [...view.crypto.getRandomValues(new Uint8Array(16))].map(x => x.toString(16).padStart(2, '0')).join('');
      return new Promise(resolve => {
        const finish = value => { view.clearTimeout(timer); view.removeEventListener('message', receive); resolve(value); };
        const receive = event => { const v = event.data;
          if (event.source === peer.window && event.origin === peer.binding.origin && !event.ports?.length
            && exact(v, ['schema', 'probe', 'enabled']) && v.schema === ASSIST_SCHEMA && v.probe === id) {
            const fresh = readPeer(); finish(!disposed && v.enabled === true && fresh?.window === peer.window && sameBinding(fresh.binding, peer.binding));
          }
        };
        const timer = view.setTimeout(() => finish(false), 1000); view.addEventListener('message', receive);
        peer.window.postMessage({ schema: ASSIST_SCHEMA, probe: id, appId: peer.binding.appId }, peer.binding.origin);
      });
    },
    // Must be called by the host's explicit per-app user control, never by a model reply.
    enable() { const peer = readPeer(); if (disposed || !peer?.window || !validBinding(peer.binding)) throw Error('assist_binding'); consent = { window: peer.window, binding: structuredClone(peer.binding) }; return structuredClone(consent.binding); },
    binding() { try { current(); return structuredClone(consent.binding); } catch { return null; } },
    async dispatch(command, signal, guard) {
      if (!validCommand(command)) throw Error('assist_command'); current();
      if (command.kind !== 'observe') {
        const item = observed?.snapshot === command.snapshot && observed.items.find(item => item.id === command.target && item.kind === command.kind);
        if (!item) throw Error('assist_stale_snapshot');
        let timer;
        try { if (await Promise.race([approve({ ...structuredClone(command), label: item.label }), new Promise(resolve => { timer = view.setTimeout(() => resolve(false), 6000); })]) !== true) throw Error('assist_approval_denied'); }
        finally { view.clearTimeout(timer); }
        current(); observed = null;
      }
      if (guard) await guard();
      if (signal?.aborted) throw Error('assist_cancelled');
      const peer = current(), requestId = [...view.crypto.getRandomValues(new Uint8Array(16))].map(x => x.toString(16).padStart(2, '0')).join('');
      return new Promise((resolve, reject) => {
        const stop = () => finish(Error('assist_cancelled'));
        const finish = (error, result) => { view.clearTimeout(timer); pending.delete(stop); view.removeEventListener('message', receive); signal?.removeEventListener('abort', stop); error ? reject(error) : resolve(result); };
        const receive = event => {
          const v = event.data;
          if (event.source !== peer.window || event.origin !== peer.binding.origin || event.ports?.length
            || !exact(v, ['schema', 'channel', 'requestId', 'result']) || v.schema !== ASSIST_SCHEMA || v.channel !== peer.binding.channel || v.requestId !== requestId) return;
          try {
            current(); if (JSON.stringify(v.result).length > 20000) throw Error();
            if (command.kind === 'observe' && v.result?.ok === true) {
              const d = v.result.data;
              if (!d || !nonce(d.snapshot) || !Array.isArray(d.items) || d.items.length > 64 || d.items.some(x => !exact(x, ['id', 'kind', 'label'])
                || !/^n[0-9]{1,3}$/u.test(x.id) || !['click', 'fill', 'text'].includes(x.kind) || typeof x.label !== 'string' || x.label.length > 160)) throw Error();
              observed = structuredClone(d);
            }
            finish(null, v.result);
          } catch { finish(Error('assist_permission_closed')); }
        };
        const timer = view.setTimeout(() => finish(Error('assist_outcome_unknown')), 5000);
        pending.add(stop); signal?.addEventListener('abort', stop, { once: true }); view.addEventListener('message', receive);
        peer.window.postMessage({ schema: ASSIST_SCHEMA, binding: peer.binding, requestId, command }, peer.binding.origin);
      });
    },
    revoke, close() { revoke(); disposed = true; },
  };
}

/** Runs beside the existing signed agent.turn, returning replies through Connect proof. */
export async function pumpBrowserAssist({ client, accountId, requestId, binding, host, signal, assertCurrent }) {
  const replies = new Map();
  while (!signal.aborted) {
    if (!sameBinding(host.binding(), binding)) throw Error('assist_permission_closed');
    const args = { requestId, channel: binding.channel };
    const result = await client.extension('agent.browser.poll', args, { expectedAccountId: accountId });
    if (signal.aborted) return;
    if (result.command) {
      if (!nonce(result.commandId) || !validCommand(result.command)) throw Error('assist_invalid_request');
      let reply = replies.get(result.commandId);
      if (!reply) {
        if (replies.size >= 12) throw Error('assist_capacity');
        try {
          const guard = async () => {
            await assertCurrent();
            if (signal.aborted || !sameBinding(host.binding(), binding)) throw Error('assist_permission_closed');
            const fresh = await client.extension('agent.browser.poll', args, { expectedAccountId: accountId });
            if (fresh.commandId !== result.commandId || JSON.stringify(fresh.command) !== JSON.stringify(result.command)) throw Error('assist_stale_command');
            await assertCurrent();
          };
          await assertCurrent(); reply = await host.dispatch(result.command, signal, guard);
        }
        catch { reply = { ok: false, error: 'assist_denied_or_unknown' }; }
        replies.set(result.commandId, reply); // Never repeat a click after a lost reply.
      }
      if (signal.aborted) return;
      await client.extension('agent.browser.reply', { ...args, commandId: result.commandId, result: reply }, { expectedAccountId: accountId });
    }
    await new Promise(resolve => { const done = () => { clearTimeout(timer); signal.removeEventListener('abort', done); resolve(); }; const timer = setTimeout(done, 250); signal.addEventListener('abort', done, { once: true }); });
  }
}
