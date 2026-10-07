import { validateAppLaunchBinding } from './app-launch.mjs';

const WATCH = 'soty.app-boot-watch.v1';
const FAILURE = 'soty.app-boot-failure.v1';
const appId = /^app-[a-f0-9]{32}$/u;
const noncePattern = /^[A-Za-z0-9_-]{43}$/u;

/** An app-controlled retry hint is not authentication or boot attestation. */
export function isAppBootRetryHint(data, expected) {
  return !!data && typeof data === 'object' && !Array.isArray(data)
    && Object.keys(data).sort().join(',') === 'appId,error,nonce,schema'
    && data.schema === FAILURE && data.error === 'app_session_check_failed'
    && data.appId === expected.appId && data.nonce === expected.nonce;
}

/** One budget for the original screen intent; rebinding never resets it. */
export function createAppBootRecovery({ view, appId: expectedApp, getFrame, isCurrent, canRecover = () => true, recover, onFailure }) {
  if (!appId.test(expectedApp)) throw Error('invalid_app_id');
  let disposed = false, attempted = false, sequence = 0, record = null;
  function stop() {
    const previous = record; record = null; sequence++;
    if (!previous) return;
    view.clearTimeout(previous.timer);
    previous.frame.removeEventListener('load', previous.load);
    view.removeEventListener('message', previous.receive);
  }
  const active = current => !disposed && record === current && current.sequence === sequence
    && isCurrent() && getFrame() === current.frame;
  function bind(frame, { origin, binding }) {
    stop();
    // Older launch responses retain manual recovery; no guessed provenance.
    if (disposed || !binding) return;
    try { validateAppLaunchBinding(binding); } catch { return; }
    let address;
    try { address = new URL(origin); } catch { return; }
    if (address.origin !== origin || !['http:', 'https:'].includes(address.protocol)) return;
    let nonce;
    try {
      const bytes = view.crypto.getRandomValues(new Uint8Array(32));
      nonce = view.btoa(String.fromCharCode(...bytes)).replaceAll('+', '-').replaceAll('/', '_').replaceAll('=', '');
    } catch { return; }
    if (!noncePattern.test(nonce)) return;
    const current = { frame, origin, nonce, sequence, loads: 0, timer: null, load: null, receive: null };
    record = current;
    current.receive = event => {
      if (!active(current) || event.origin !== origin || event.source !== frame.contentWindow
        || event.ports?.length || !isAppBootRetryHint(event.data, { appId: expectedApp, nonce })) return;
      if (attempted) { stop(); if (isCurrent()) onFailure?.(); return; }
      if (!canRecover()) return;
      attempted = true; stop(); // Claim before any await or duplicate/reentry.
      const claimed = sequence;
      const currentClaim = () => !disposed && sequence === claimed && isCurrent() && getFrame() === frame;
      void Promise.resolve().then(() => {
        if (!currentClaim()) return;
        return recover(currentClaim);
      }).catch(() => { if (currentClaim()) onFailure?.(); });
    };
    view.addEventListener('message', current.receive);
    current.load = () => {
      if (!active(current) || ++current.loads !== 1) { if (record === current) stop(); return; }
      try {
        frame.contentWindow.postMessage({ schema: WATCH, appId: expectedApp, nonce }, origin);
      } catch { if (record === current) stop(); }
    };
    frame.addEventListener('load', current.load);
    // Shorter than the server boot-check lease; never extends any server TTL.
    current.timer = view.setTimeout(() => { if (record === current) stop(); }, 25_000);
  }
  return { bind, cancel: stop, dispose() { disposed = true; stop(); }, attempted: () => attempted };
}

