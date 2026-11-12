/** Opt-in local observations only. Never authority, readiness or a retry trigger. */
const BOOT = new Set(['boot_admission_pending','boot_cookie_pending','boot_continuation_pending','boot_navigation','boot_failed','boot_inactive']);
const ROOT = new Set(['root_launch','root_launch_failed','root_frame','root_attached','root_load','root_error','root_timeout','root_boot_unobserved','boot_document_ack']);
const CSP = new Set(['page_csp_frame_enforce','page_csp_frame_report','page_csp_script_enforce','page_csp_script_report','page_csp_other_enforce','page_csp_other_report']);
const LIMIT = 24, TTL = 120_000;
const ARM_KEY = 'soty.runtime-phase-arm.v1';
const REQUEST_ERRORS = new Set(['none','http','TypeError','SyntaxError','AbortError','other']);

function sessionRequest(data,watchId) {
  try {
    const keys = ['schema','watchId','method','status','elapsed100ms','headersReceived','jsonParsed','errorName','signalAborted'];
    if (!watchId || !data || typeof data !== 'object' || Array.isArray(data) || Reflect.ownKeys(data).length !== keys.length) return null;
    const result = {};
    for (const key of keys) { const property = Object.getOwnPropertyDescriptor(data,key); if (!property || !Object.hasOwn(property,'value')) return null; result[key] = property.value; }
    if (result.schema !== 'soty.runtime-session-request.v1' || result.watchId !== watchId || !['POST','GET'].includes(result.method)
      || result.status !== null && (!Number.isInteger(result.status) || result.status < 100 || result.status > 599)
      || !Number.isInteger(result.elapsed100ms) || result.elapsed100ms < 0 || result.elapsed100ms > 1200
      || !['headersReceived','jsonParsed','signalAborted'].every(key=>typeof result[key] === 'boolean') || !REQUEST_ERRORS.has(result.errorName)
      || !result.headersReceived && (result.status !== null || result.jsonParsed)) return null;
    const {schema,watchId:correlation,...safe} = result; return safe;
  } catch { return null; }
}

function bootPhase(data, watchId) {
  try {
    if (!data || typeof data !== 'object' || Array.isArray(data)) return null;
    const keys = Reflect.ownKeys(data);
    if (!watchId || keys.length !== 3 || !keys.includes('schema') || !keys.includes('phase') || !keys.includes('watchId')) return null;
    const schema = Object.getOwnPropertyDescriptor(data, 'schema'), phase = Object.getOwnPropertyDescriptor(data, 'phase'), watch = Object.getOwnPropertyDescriptor(data,'watchId');
    return schema && phase && watch && Object.hasOwn(schema, 'value') && Object.hasOwn(phase, 'value') && Object.hasOwn(watch,'value')
      && schema.value === 'soty.runtime-phase.v2' && watch.value === watchId && BOOT.has(phase.value) ? phase.value : null;
  } catch { return null; }
}

/** Root DOM only: no child document, attributes, text, URLs or exported geometry. */
export function sampleRuntimeFrame(frame, view) {
  const result = { connected: false, hasArea: false, ancestorsVisible: false, hit: 'unknown' };
  try {
    result.connected = frame.isConnected === true;
    if (!result.connected) return result;
    const rect = frame.getBoundingClientRect();
    result.hasArea = [rect.left, rect.top, rect.width, rect.height].every(Number.isFinite) && rect.width > 0 && rect.height > 0;
    result.ancestorsVisible = true;
    for (let node = frame; node; node = node.parentElement) {
      const style = view.getComputedStyle(node);
      if (style.display === 'none' || ['hidden','collapse'].includes(style.visibility) || Number(style.opacity || 1) <= 0) {
        result.ancestorsVisible = false; break;
      }
    }
    if (result.hasArea && typeof view.document.elementsFromPoint === 'function') {
      const hits = [[.5,.5],[.2,.2],[.8,.2],[.2,.8],[.8,.8]].filter(([x,y]) =>
        view.document.elementsFromPoint(rect.left + rect.width*x, rect.top + rect.height*y)[0] === frame).length;
      result.hit = hits === 5 ? 'all' : hits === 0 ? 'none' : 'some';
    }
  } catch { /* An unavailable DOM measurement stays partial. */ }
  return result;
}

export function createAppRuntimePhaseTrace({view,appId,isCurrent,getFrame,getEpoch,canSample=()=>true,onChange=()=>{}}) {
  let armed = false, disposed = false, capture = null, armTimer, armDeadline = 0, expiry;
  let restoredArm = false;
  const publicApp = typeof appId === 'string' && /^app-[a-f0-9]{32}$/.test(appId) ? appId : '';
  const forgetArm = () => { try { if (publicApp && JSON.parse(view.sessionStorage.getItem(ARM_KEY))?.appId === publicApp) view.sessionStorage.removeItem(ARM_KEY); } catch { /* RAM-only fallback. */ } };
  let peer = null, expectedOrigin = '', epoch, frameTimer, ackTimer, watchId = '';
  let releases = [], probes = [], loadProbeSent = false, acknowledged = false;
  const clock = () => view.performance.now();
  const notify = () => { try { onChange(); } catch { /* Diagnostics cannot break the app. */ } };
  const current = () => !disposed && isCurrent();
  const expired = () => {
    if (armed && clock() >= armDeadline || capture && clock()-capture.started >= TTL) { clear(); return true; }
    return false;
  };
  const live = () => current() && !expired() && capture && !capture.truncated;
  const frameLive = () => live() && peer?.isConnected === true && getFrame() === peer && getEpoch() === epoch;
  const stopListeners = () => {
    for (const release of releases) release(); releases = [];
    for (const timer of probes) view.clearTimeout(timer); probes = [];
    if (frameTimer !== undefined) view.clearTimeout(frameTimer); frameTimer = undefined;
    if (ackTimer !== undefined) view.clearTimeout(ackTimer); ackTimer = undefined;
    peer = null; expectedOrigin = ''; watchId = '';
  };
  const discard = () => {
    stopListeners(); if (expiry !== undefined) view.clearTimeout(expiry); expiry = undefined; capture = null;
  };
  const listen = (target,type,handler) => {
    target.addEventListener(type,handler); releases.push(()=>target.removeEventListener(type,handler));
  };
  const record = phase => {
    if (!live() || !ROOT.has(phase) && !BOOT.has(phase) && !CSP.has(phase)) return;
    if (capture.events.at(-1)?.phase === phase) return;
    capture.events.push({t100ms: Math.min(1200,Math.max(0,Math.floor((clock()-capture.started)/100))),phase});
    if (capture.events.length+capture.requests.length >= LIMIT) { capture.truncated = true; stopListeners(); }
    notify();
  };
  const sample = () => { if (frameLive() && canSample()) { capture.view = sampleRuntimeFrame(peer,view); notify(); } };
  const probe = () => {
    if (!frameLive() || !peer.contentWindow || !watchId) return;
    try { peer.contentWindow.postMessage({schema:'soty.runtime-phase-watch.v2',watchId},expectedOrigin); } catch { /* No retry or payload. */ }
  };
  const clear = (keepArm=false) => {
    if (!keepArm) forgetArm(); restoredArm = false;
    armed = false; armDeadline = 0; if (armTimer !== undefined) view.clearTimeout(armTimer); armTimer = undefined; discard(); notify();
  };
  const snapshot = () => {
    if (!current() || expired() || !capture) return null;
    return {schema:'soty.runtime-phase-trace.v1',surface:'main',observation:'partial',truncated:capture.truncated,
      events:capture.events.map(({t100ms,phase})=>({t100ms,phase})),
      requests:capture.requests.map(value=>({...value})),
      view:capture.view ? {...capture.view} : null};
  };
  const api = Object.freeze({
    arm() {
      if (!current()) return false;
      clear(); armed = true; armDeadline = clock()+TTL; armTimer = view.setTimeout(clear,TTL);
      try { if (publicApp) view.sessionStorage.setItem(ARM_KEY,JSON.stringify({schema:ARM_KEY,appId:publicApp,expiresAt:Date.now()+TTL})); } catch { /* No report or credentials are persisted. */ }
      notify(); return true;
    },
    reset() { discard(); notify(); }, // A non-manual launch must not consume the arm.
    beginManual() {
      if (!current() || expired() || !armed) return false;
      forgetArm(); restoredArm = false;
      if (armTimer !== undefined) view.clearTimeout(armTimer); armTimer = undefined; armed = false; discard();
      capture = {started:clock(),events:[],requests:[],truncated:false,view:null}; expiry = view.setTimeout(clear,TTL);
      listen(view.document,'securitypolicyviolation',event=>{
        if (!live()) return;
        try {
          const disposition = event.disposition;
          if (!['enforce','report'].includes(disposition)) return;
          const directive = event.effectiveDirective;
          const group = ['frame-src','child-src'].includes(directive) ? 'frame'
            : ['script-src','script-src-elem','script-src-attr'].includes(directive) ? 'script' : 'other';
          record(`page_csp_${group}_${disposition}`);
        } catch { /* Never inspect the error or other CSP fields. */ }
      });
      record('root_launch'); return true;
    },
    beginReload() { return restoredArm ? api.beginManual() : false; },
    attach(frame,origin) {
      if (!live()) return;
      try { const parsed = new URL(origin); if (!['https:','http:'].includes(parsed.protocol) || parsed.origin !== origin) return; } catch { return; }
      peer = frame; expectedOrigin = origin; epoch = getEpoch(); loadProbeSent = false; acknowledged = false;
      // Correlation only, never authority. The ephemeral ID is not exported.
      try { const value = view.crypto.randomUUID().replaceAll('-',''); watchId = /^[a-f0-9]{32}$/.test(value) ? value : ''; }
      catch { watchId = ''; }
      listen(frame,'load',()=>{
        if (!frameLive()) return;
        if (frameTimer !== undefined) view.clearTimeout(frameTimer); frameTimer = undefined;
        record('root_load');
        if (!loadProbeSent) { loadProbeSent = true; probe(); }
      });
      listen(frame,'error',()=>{ if (frameLive()) {
        if (frameTimer !== undefined) view.clearTimeout(frameTimer); frameTimer = undefined;
        if (ackTimer !== undefined) view.clearTimeout(ackTimer); ackTimer = undefined;
        record('root_error');
      } });
      listen(view,'message',event=>{
        if (!frameLive() || event.source !== peer.contentWindow || event.origin !== expectedOrigin || event.ports?.length) return;
        const phase = bootPhase(event.data,watchId);
        const request = phase ? null : sessionRequest(event.data,watchId);
        if (phase || request) {
          if (!acknowledged) {
            acknowledged = true;
            if (ackTimer !== undefined) view.clearTimeout(ackTimer); ackTimer = undefined;
            record('boot_document_ack');
          }
          if (phase) record(phase);
          else if (live() && capture.requests.length < 2 && !capture.requests.some(value=>value.method === request.method)) {
            capture.requests.push({t100ms:Math.min(1200,Math.max(0,Math.floor((clock()-capture.started)/100))),...request});
            if (capture.events.length+capture.requests.length >= LIMIT) { capture.truncated = true; stopListeners(); }
            notify();
          }
        }
      });
      frameTimer = view.setTimeout(()=>{ if (frameLive()) { sample(); record('root_timeout'); } },12_000);
      // A load event (including a blank/error document) is not a Boot response.
      // No response also cannot prove failure: a fast Boot may already have left.
      ackTimer = view.setTimeout(()=>{ if (frameLive() && !acknowledged) record('root_boot_unobserved'); },12_000);
      record('root_frame');
    },
    inserted() {
      if (!frameLive()) return;
      sample(); record('root_attached');
      probes = [0,250,1000,3000].map(ms=>view.setTimeout(probe,ms));
    },
    launchFailed() { record('root_launch_failed'); },
    sample,
    snapshot,
    state() { return !current() || expired() ? 'empty' : armed ? 'armed' : capture ? capture.truncated ? 'limited' : 'recording' : 'empty'; },
    async copy() {
      const value = snapshot(); if (!value) return false;
      try { await view.navigator.clipboard.writeText(JSON.stringify(value,null,2)); return current(); } catch { return false; }
    },
    clear,
    dispose() { clear(true); disposed = true; },
  });
  try {
    const raw = publicApp && view.sessionStorage.getItem(ARM_KEY), value = raw && raw.length <= 256 ? JSON.parse(raw) : null, remaining = value?.expiresAt-Date.now();
    if (value?.schema === ARM_KEY && Object.keys(value).sort().join(',') === 'appId,expiresAt,schema' && value.appId === publicApp
      && Number.isSafeInteger(value.expiresAt) && remaining > 0 && remaining <= TTL) {
      restoredArm = armed = true; armDeadline = clock()+remaining; armTimer = view.setTimeout(clear,remaining);
    } else if (value?.appId === publicApp) forgetArm();
  } catch { /* A denied or malformed storage entry cannot change launch behavior. */ }
  return api;
}
