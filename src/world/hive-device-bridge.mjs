const APP_ORIGIN = 'https://hive.4-2.xn--p1ai';
const SHELL_ORIGIN = 'https://4-2.xn--p1ai';
const SCHEMA = 'hive.soty-device-link.v1';

export function hiveDeviceLinkMessage({ challenge, oldId, id }) {
  if (!/^[A-Za-z0-9_-]{43}$/u.test(challenge || '')
    || ![oldId, id].every(value => /^dev_[A-Za-z0-9_-]{32}$/u.test(value || '')) || oldId === id) throw Error('device_link_invalid');
  return `hive.device.link.v1\n${challenge}\n${oldId}\n${id}\n${APP_ORIGIN}`;
}

async function readPreviousIdentity(view) {
  if (view.localStorage.getItem('hive.device-login.suppressed') === '1') return null;
  if (view.indexedDB.databases && !(await view.indexedDB.databases()).some(db => db.name === 'hive-account')) return null;
  const db = await new Promise((resolve, reject) => {
    const request = view.indexedDB.open('hive-account', 1);
    request.onupgradeneeded = () => request.transaction.abort();
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(Error('device_not_present'));
  });
  try {
    if (!db.objectStoreNames.contains('identity')) return null;
    return await new Promise((resolve, reject) => {
      const request = db.transaction('identity', 'readonly').objectStore('identity').get('current-device');
      request.onsuccess = () => resolve(request.result || null); request.onerror = () => reject(Error('device_not_present'));
    });
  } finally { db.close(); }
}

/** The only allowed recipient is the active, pinned HIVE frame. No key or cookie is sent. */
export function mountHiveDeviceBridge({ view, getFrame, isCurrent, readIdentity = () => readPreviousIdentity(view) }) {
  let disposed = false, signatures = 0, windowStarted = 0;
  const matches = event => {
    const frame = getFrame();
    if (disposed || !isCurrent() || view.location.origin !== SHELL_ORIGIN || !frame
      || event.source !== frame.contentWindow || event.origin !== APP_ORIGIN) return false;
    try { return new URL(frame.src).origin === APP_ORIGIN; } catch { return false; }
  };
  const receive = async event => {
    if (!matches(event)) return;
    const input = event.data, port = event.ports?.length === 1 ? event.ports[0] : null;
    if (!port || !input || input.schema !== SCHEMA || !/^[a-f0-9-]{36}$/u.test(input.requestId || '')
      || !['info', 'sign'].includes(input.method)) return;
    const allowed = input.method === 'info' ? ['schema','requestId','method'] : ['schema','requestId','method','challenge','oldId','id'];
    if (Object.keys(input).some(key => !allowed.includes(key))) return;
    let value = null;
    try {
      const identity = await readIdentity();
      if (!matches(event) || !identity?.publicJwk || identity.privateKey?.type !== 'private'
        || identity.privateKey.extractable !== false || !/^dev_[A-Za-z0-9_-]{32}$/u.test(identity.id)) return;
      if (input.method === 'info') value = { id: identity.id };
      else {
        if (input.oldId !== identity.id) throw Error('device_changed');
        const now = Date.now(); if (now - windowStarted > 60_000) { windowStarted = now; signatures = 0; }
        if (++signatures > 10) throw Error('device_link_busy');
        const message = hiveDeviceLinkMessage(input);
        const signature = new Uint8Array(await view.crypto.subtle.sign({name:'ECDSA',hash:'SHA-256'},identity.privateKey,new TextEncoder().encode(message)));
        value = { signature: btoa(String.fromCharCode(...signature)).replaceAll('+','-').replaceAll('/','_').replaceAll('=','') };
      }
    } catch { /* Missing or unavailable old storage never creates an old-origin identity. */ }
    finally {
      if (matches(event)) port.postMessage({schema:SCHEMA,requestId:input.requestId,ok:!!value,value});
      port.close();
    }
  };
  view.addEventListener('message', receive);
  return () => { disposed = true; view.removeEventListener('message', receive); };
}
