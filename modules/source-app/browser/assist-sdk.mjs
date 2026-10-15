import { ASSIST_SCHEMA, exact, nonce, validBinding, validCommand } from './assist-wire.mjs';

/** Author opt-in only. Only explicitly labelled elements participate. No auto-install. */
export function createSourceBrowserAssist({ appId, parentOrigin, view = window, enabled = false }) {
  if (new URL(parentOrigin).origin !== parentOrigin || !/^app-[a-f0-9]{32}$/u.test(appId)) throw Error('assist_configuration');
  const doc = view.document, replies = new Map(), retired = new Set(); let active = enabled === true, closed = false, channel = null, snapshot = null, nodes = new Map();
  const random = () => [...view.crypto.getRandomValues(new Uint8Array(16))].map(x => x.toString(16).padStart(2, '0')).join('');
  const invalidate = () => { snapshot = null; nodes.clear(); };
  const observer = new view.MutationObserver(invalidate);
  observer.observe(doc.documentElement, { subtree: true, childList: true, attributes: true, characterData: true });
  const kind = node => {
    if (!node.isConnected || node.closest('[hidden],[inert],[data-soty-assist-private]') || node.disabled || node.readOnly
      || !node.getClientRects().length || view.getComputedStyle(node).visibility !== 'visible') return null;
    if (node.tagName === 'BUTTON' && node.type === 'button' && !node.closest('form')) return 'click';
    if (node.tagName === 'INPUT' && ['text', 'search'].includes(node.type) && !node.closest('form')
      && !/password|username|cc-|one-time|email|tel|address|token|secret|credential/iu.test([node.autocomplete, node.name, node.id].join(' '))) return 'fill';
    return node.tagName === 'SPAN' || node.tagName === 'P' || node.tagName === 'LI' ? 'text' : null;
  };
  const label = node => (node.getAttribute('data-soty-assist') || '').trim().slice(0, 160);
  function execute(command) {
    if (observer.takeRecords().length) invalidate();
    if (command.kind === 'observe') {
      invalidate(); snapshot = random(); const items = [];
      for (const node of doc.querySelectorAll('[data-soty-assist]')) {
        const action = kind(node), text = label(node); if (!action || !text) continue;
        if (items.length === 64) break;
        const id = 'n' + items.length; nodes.set(id, { node, action, text }); items.push({ id, kind: action, label: text });
      }
      // Author-provided labels only; never scrape input values, whole DOM or hidden text.
      return { snapshot, items, untrusted: true };
    }
    const item = nodes.get(command.target);
    if (!snapshot || snapshot !== command.snapshot || !item || kind(item.node) !== command.kind || label(item.node) !== item.text)
      throw Error('assist_stale_snapshot');
    invalidate();
    if (command.kind === 'click') item.node.click();
    else { item.node.value = command.text; item.node.dispatchEvent(new view.Event('input', { bubbles: true })); }
    return { performed: true, untrusted: true };
  }
  function receive(event) {
    const v = event.data;
    if (!closed && event.source === view.parent && event.origin === parentOrigin && !event.ports?.length
      && exact(v, ['schema', 'probe', 'appId']) && v.schema === ASSIST_SCHEMA && nonce(v.probe) && v.appId === appId) {
      view.parent.postMessage({ schema: ASSIST_SCHEMA, probe: v.probe, enabled: active }, parentOrigin); return;
    }

    if (!closed && event.source === view.parent && event.origin === parentOrigin && !event.ports?.length
      && exact(v, ['schema', 'revoke']) && v.schema === ASSIST_SCHEMA && nonce(v.revoke)) {
      retired.add(v.revoke); if (channel === v.revoke) { active = false; invalidate(); } return;
    }
    if (closed || !active || view.parent === view || event.source !== view.parent || event.origin !== parentOrigin || event.ports?.length
      || !exact(v, ['schema', 'binding', 'requestId', 'command']) || v.schema !== ASSIST_SCHEMA || !validBinding(v.binding)
      || v.binding.appId !== appId || v.binding.origin !== view.location.origin || !nonce(v.requestId) || !validCommand(v.command)) return;
    if (retired.has(v.binding.channel) || retired.size > 64) return;
    if (channel && channel !== v.binding.channel) return; // A new root consent needs an author-side reset/re-enable.
    channel = v.binding.channel;
    const digest = JSON.stringify(v), prior = replies.get(v.requestId);
    if (prior && prior.digest !== digest || !prior && replies.size >= 64) return;
    let result = prior?.result;
    if (!result) {
      try { result = { ok: true, data: execute(v.command) }; } catch { result = { ok: false, error: 'assist_stale_or_denied' }; }
      replies.set(v.requestId, { digest, result });
    }
    view.parent.postMessage({ schema: ASSIST_SCHEMA, channel, requestId: v.requestId, result }, parentOrigin);
  }
  view.addEventListener('message', receive);
  return { setEnabled(value) { if (channel) retired.add(channel); active = value === true; invalidate(); channel = null; replies.clear(); },
    close() { closed = true; active = false; invalidate(); replies.clear(); observer.disconnect(); view.removeEventListener('message', receive); } };
}
