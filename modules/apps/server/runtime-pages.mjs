import { readFileSync } from 'node:fs';
import { roundedHexPath, hexCluster } from '../../ui/hex.mjs';
import { createBootDisplayClient } from './runtime-display-client.mjs';
import { checkedScopedEntryPath } from './launch-path.mjs';

const html = value => String(value).replace(/[&<>"']/gu, character => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[character]);
const scriptData = value => JSON.stringify(value).replace(/[<>&\u2028\u2029]/gu,
  character => `\\u${character.charCodeAt(0).toString(16).padStart(4, '0')}`);

function shellAddress(value, required = false) {
  if (value === undefined && !required) return null;
  if (typeof value !== 'string' || /[\\\u0000-\u0020\u007f]/u.test(value)) throw new TypeError('invalid_shell_url');
  let url;
  try { url = new URL(value); } catch { throw new TypeError('invalid_shell_url'); }
  if (!['https:', 'http:'].includes(url.protocol) || url.username || url.password) throw new TypeError('invalid_shell_url');
  // The caller additionally pins this absolute URL to its configured shell.
  return url.href;
}

// The same function validates server renderer input and the eventual browser
// response path. It returns the original path; no query or fragment is rebuilt.
function checkedLocalPath(value) {
  if (typeof value !== 'string' || value.length > 8192 || !value.startsWith('/') || value.startsWith('//') || /[\\\u0000-\u0020\u007f]/u.test(value)) return null;
  let url, decoded, rawDecoded, normalized, normalizedRaw;
  try {
    url = new URL(value, 'https://runtime.invalid');
    decoded = decodeURIComponent(url.pathname);
    rawDecoded = decodeURIComponent(value.split('?', 1)[0]);
    normalized = new URL(decoded, 'https://runtime.invalid');
    normalizedRaw = new URL(rawDecoded, 'https://runtime.invalid');
  } catch { return null; }
  if ([url, normalized, normalizedRaw].some(item => item.origin !== 'https://runtime.invalid')
    || [decoded, rawDecoded].some(path => path.startsWith('//') || /[\\\u0000-\u001f\u007f]/u.test(path))
    || [normalized.pathname, normalizedRaw.pathname].some(path => path.startsWith('//') || path === '/_soty' || path.startsWith('/_soty/'))) return null;
  return value;
}

function resetPath(value) {
  if (value === undefined) return null;
  const result = checkedLocalPath(value);
  if (!result) throw new TypeError('invalid_public_reset_path');
  return result;
}

function nonceAttribute(value) {
  if (value === undefined) return '';
  if (typeof value !== 'string' || !/^[A-Za-z0-9+/]{22}==$/u.test(value)
    || Buffer.from(value, 'base64').toString('base64') !== value) throw new TypeError('invalid_page_nonce');
  return ` nonce="${html(value)}"`;
}

const copy = Object.freeze({
  loading: ['Открываем приложение', ''],
  checking: ['Открываем приложение', ''],
  opening: ['Открываем приложение', ''],
  denied: ['Доступ закрыт', 'Откройте приложение через Соты, чтобы проверить доступ.'],
  changed: ['Доступ изменился', 'Откройте приложение заново.'],
  expired: ['Нужно открыть заново', 'Ссылка для входа больше не действует.'],
  cookie: ['Вход не подтвердился', 'Проверка входа не прошла. Проверьте профиль Сот и обновите приложение в этом окне.'],
  offline: ['Устройство не в сети', 'Приложение появится, когда устройство подключится.'],
  stopped: ['Приложение остановлено', 'Владелец сможет запустить его снова.'],
  busy: ['Сейчас много подключений', 'Попробуйте открыть приложение чуть позже.'],
  address: ['Адрес недоступен', 'Вернитесь в Соты и выберите приложение.'],
  inactive: ['Адрес ещё не открыт', 'Попробуйте открыть приложение через Соты.'],
  network: ['Не удалось подключиться', 'Проверьте соединение и откройте приложение снова.'],
  connectorUpdate: ['Обновите коннектор', 'Устройство приложения пока не поддерживает ключ операции. Проверьте результат перед повторной отправкой.'],
  idempotencyUnsupported: ['Ключ операции недоступен', 'Этот режим приложения не поддерживает передачу ключа операции.'],
  identityWindowUnavailable: ['Не удалось подтвердить вход', 'Проверьте профиль Сот и обновите приложение в этом окне.'],
  resetting: ['Открываем без входа', 'Переходим к публичному приложению.'],
  unavailable: ['Приложение недоступно', 'Попробуйте открыть его немного позже.'],
});

const errorStates = Object.freeze({
  apps_access_denied: 'denied', app_access_revoked: 'denied', app_access_changed: 'denied', apps_authentication_required: 'denied',
  app_session_required: 'expired', app_ticket_invalid: 'expired', app_access_expired: 'expired',
  app_session_check_failed: 'cookie', app_cookie_required: 'cookie',
  app_idempotency_key_unavailable: 'connectorUpdate', app_idempotency_key_unsupported: 'idempotencyUnsupported',
  app_identity_window_unavailable: 'identityWindowUnavailable',
  app_offline: 'offline', app_stopped: 'stopped', app_device_busy: 'busy', apps_sessions_busy: 'busy', apps_launch_busy: 'busy',
  app_not_found: 'address', app_address_retired: 'address', app_named_runtime_unavailable: 'inactive',
  429: 'busy', 503: 'unavailable',
});

const statesFor = publicResetPath => publicResetPath ? { ...errorStates, app_access_changed: 'changed' } : errorStates;

// Managed pages remain self-contained under their existing inline-only CSP.
// Read the packaged canonical material once, never an application-selected URL.
const glassStyles = readFileSync(new URL('../../../vendor/optical-glass/src/ui-kit.css', import.meta.url), 'utf8');
const cluster = hexCluster([[0, 0], [1, 0], [0, 1]], 20, 3, 2);
const emblem = `<svg class="emblem" viewBox="0 0 ${cluster.width} ${cluster.height}" aria-hidden="true" focusable="false">${cluster.cells.map((cell, index) =>
  `<path class="cell cell-${index}" d="${roundedHexPath(20, undefined, cell)}"/>`).join('')}</svg>`;

const styles = `
${glassStyles}
:root{color-scheme:light dark;font-synthesis:none}
*{box-sizing:border-box}body{margin:0;min-width:0;min-height:100vh;min-height:100svh;display:grid;grid-template-rows:auto 1fr}
body[data-framed="true"]{grid-template-rows:1fr}body[data-framed="true"] .brand{display:none}
.brand{display:flex;align-items:center;gap:9px;padding:22px 24px;font-size:15px;font-weight:650;letter-spacing:.01em;color:var(--glass-ui-muted)}
.brand svg{width:22px;height:20px;fill:var(--glass-ui-accent)}.entry{align-self:center;justify-self:center;width:min(460px,calc(100% - 32px));margin:12px 0 64px;padding:clamp(24px,5vw,40px)}
.visual{width:88px;max-width:100%;margin-bottom:28px}.emblem{display:block;width:100%;height:auto;overflow:visible}.cell{fill:var(--glass-ui-surface);stroke:var(--glass-ui-edge);stroke-width:1}.cell-0{fill:color-mix(in srgb,var(--glass-ui-accent) 16%,var(--glass-ui-surface));stroke:var(--glass-ui-edge)}
[data-state="loading"] .cell{animation:breathe 2s ease-in-out infinite}.cell-1{animation-delay:.18s!important}.cell-2{animation-delay:.36s!important}
.content{min-width:0}h1{font-size:clamp(24px,6vw,32px);font-weight:620;line-height:1.18;letter-spacing:-.035em;margin:0;text-wrap:balance;overflow-wrap:anywhere}
.entry[data-state="loading"] .visual{display:none}.entry[data-state="loading"] h1{font-size:18px;line-height:1.4;font-weight:550}.entry[data-state="loading"] h1::after{content:"…"}.entry[data-state="loading"] .actions{justify-content:center}
.detail{margin:14px 0 0;max-width:36ch;font-size:15px;line-height:1.55;color:var(--glass-ui-muted);overflow-wrap:anywhere}.detail:empty{display:none}.actions{display:flex;flex-wrap:wrap;gap:10px;margin-top:26px}.action{appearance:none;min-height:46px;max-width:100%;display:inline-flex;align-items:center;justify-content:center;gap:8px;padding:12px 16px;font-weight:600;font-size:14px;line-height:1.35;font-family:inherit;text-decoration:none;text-align:center;cursor:pointer;overflow-wrap:anywhere}
.glass-ui :is(a,button).action[data-glass-role]{min-inline-size:44px}.action:disabled{cursor:wait;opacity:.65}.frame-hint{margin:20px 0 0;font-size:14px;line-height:1.5;color:var(--glass-ui-muted);max-width:34ch}.noscript{margin-top:20px;font-size:14px;line-height:1.5;color:var(--glass-ui-muted)}[hidden]{display:none!important}
@keyframes breathe{0%,100%{opacity:.48}50%{opacity:1}}
@media(max-width:359px){.brand{padding:18px 20px}.entry{padding:24px 20px}.actions{flex-direction:column}.action{width:100%}.visual{width:76px;margin-bottom:22px}}
@media(max-height:440px){.brand{padding:12px 20px}.entry{width:min(660px,calc(100% - 24px));padding:12px;margin:8px 0}.entry[data-state="error"]{display:grid;grid-template-columns:36px minmax(0,1fr);gap:12px}.visual{width:36px;margin:2px 0 0}h1{font-size:20px}.detail{margin-top:6px;font-size:14px;line-height:1.4}.actions{margin-top:10px;gap:8px}.frame-hint{margin-top:8px;font-size:13px;line-height:1.4}}
@media(max-height:280px){body[data-framed="true"] .entry{padding:10px;margin:6px 0}.action{min-height:44px;padding:10px 14px}}
@media(prefers-reduced-motion:reduce){*,*::before,*::after{animation:none!important;transition:none!important}}
`;

function pageClient(config, allowPath, createDisplay, allowScopedPath) {
  // Clear the one-time secret synchronously before DOM work or any request.
  let ticket = '', cleanupFailed = false;
  if (config.mode === 'boot') {
    ticket = location.hash.slice(1);
    try { history.replaceState(null, '', location.pathname + location.search); }
    catch { cleanupFailed = true; }
  }
  const entry = document.getElementById('entry'), title = document.getElementById('status-title'), detail = document.getElementById('status-detail');
  const actions = document.getElementById('actions'), shell = document.getElementById('shell-action'), reset = document.getElementById('reset-action'), frameHint = document.getElementById('frame-hint');
  const nativeContinue = config.mode === 'boot' && config.scoped === true && !config.renewal ? document.getElementById('native-action') : null;
  const topLevel = window.top === window.self, controllers = new Set();
  const bootDisplay = createDisplay(config);
  document.body.dataset.framed = String(!topLevel);
  let busy = false, navigated = false, offPage = false, generation = 0;
  let nativeGeneration = -1, cancelNavigationLoad = null;
  function clearNativeContinue() {
    if (!nativeContinue) return;
    nativeContinue.hidden = true; nativeContinue.removeAttribute('href'); nativeGeneration = -1;
    if (shell.hidden && reset.hidden) actions.hidden = true;
  }
  function waitForDocumentLoad() {
    if (document.readyState === 'complete') return Promise.resolve(true);
    return new Promise(resolve => {
      let done = false;
      const finish = loaded => {
        if (done) return; done = true;
        window.removeEventListener('load', loadedDocument); cancelNavigationLoad = null; resolve(loaded);
      };
      const loadedDocument = () => finish(true);
      cancelNavigationLoad = () => finish(false);
      window.addEventListener('load', loadedDocument, { once: true });
    });
  }
  nativeContinue?.addEventListener('click', event => {
    if (offPage || nativeGeneration !== generation) { event.preventDefault(); clearNativeContinue(); return; }
    // Keep ordinary link activation; suppress a still-waiting automatic attempt.
    if (!event.defaultPrevented && event.button === 0 && !event.metaKey && !event.ctrlKey && !event.altKey && !event.shiftKey) navigated = true;
  });
  // Optional local phase hints. Never readiness, recovery or authorization.
  // This starts only after the one-time fragment has already been cleared.
  const diagnostic = (() => {
    const phases = ['boot_admission_pending','boot_cookie_pending','boot_continuation_pending','boot_navigation','boot_failed','boot_inactive'];
    const requests = []; let requestCount = 0;
    let phase = null, parentOrigin = '', protocol = '', watchId = '', sent = 0, deadline = 0, timeout,
      active = config.mode === 'boot' && !config.renewal && !config.hostRenewal && !topLevel;
    const emit = data => {
      if (!active || !parentOrigin || sent >= 24) return;
      if (performance.now() >= deadline) { stop(); return; }
      sent++;
      try { window.parent.postMessage(data,parentOrigin); } catch { /* No payload or action. */ }
      if (sent >= 24) stop();
    };
    const send = () => { if (phase) emit(protocol === 'soty.runtime-phase-watch.v2'
      ? {schema:'soty.runtime-phase.v2',watchId,phase} : {schema:'soty.runtime-phase.v1',phase}); };
    const flushRequests = () => { while (active && parentOrigin && protocol === 'soty.runtime-phase-watch.v2' && requests.length)
      emit({schema:'soty.runtime-session-request.v1',watchId,...requests.shift()}); };
    const stop = () => { active = false; requests.length = 0; phase = null; parentOrigin = ''; protocol = ''; watchId = ''; clearTimeout(timeout); window.removeEventListener('message',receive); };
    const receive = event => {
      if (!active || offPage || navigated || event.source !== window.parent || !config.parentOrigins.includes(event.origin) || event.ports?.length) return;
      if (deadline && performance.now() >= deadline) { stop(); return; }
      try {
        const data = event.data;
        if (!data || typeof data !== 'object' || Array.isArray(data)) return;
        const keys = Reflect.ownKeys(data), schema = Object.getOwnPropertyDescriptor(data,'schema');
        if (!schema || !Object.hasOwn(schema,'value') || parentOrigin && parentOrigin !== event.origin) return;
        const legacy = config.scoped === true && schema.value === 'soty.runtime-phase-watch.v1' && keys.length === 1 && keys[0] === 'schema';
        const watch = Object.getOwnPropertyDescriptor(data,'watchId');
        const correlated = schema.value === 'soty.runtime-phase-watch.v2' && keys.length === 2 && keys.includes('schema') && keys.includes('watchId')
          && watch && Object.hasOwn(watch,'value') && typeof watch.value === 'string' && /^[a-f0-9]{32}$/.test(watch.value);
        if (!legacy && !correlated || protocol && (protocol !== schema.value || correlated && watchId !== watch.value)) return;
        if (!parentOrigin) { parentOrigin = event.origin; protocol = schema.value; watchId = correlated ? watch.value : ''; deadline = performance.now()+120_000; timeout = setTimeout(stop,120_000); }
        send(); flushRequests(); if (sent >= 24) stop();
      } catch { /* Ignore malformed input without reading or serializing it. */ }
    };
    if (active) window.addEventListener('message',receive);
    return { phase(next) { bootDisplay.phase(next); if (active && phases.includes(next)) { phase = next; send(); } },
      request(value) { if (active && requestCount < 2) { requestCount++; requests.push(value); flushRequests(); } },
      close() { bootDisplay.close(); if (active) { phase = 'boot_inactive'; send(); stop(); } } };
  })();
  let recoveryParentOrigin = '', recoveryNonce = '', recoveryApp = '', recoveryFailure = false, recoverySent = false;
  function notifyRecovery() {
    if (!recoveryParentOrigin || !recoveryFailure || recoverySent || offPage || navigated) return;
    recoverySent = true;
    try { window.parent.postMessage({ schema: 'soty.app-boot-failure.v1', appId: recoveryApp,
      nonce: recoveryNonce, error: 'app_session_check_failed' }, recoveryParentOrigin); } catch { /* Manual recovery stays visible. */ }
    recoveryParentOrigin = '';
  }
  window.addEventListener('message', event => {
    const data = event.data;
    if (config.mode !== 'boot' || topLevel || offPage || navigated || recoverySent || recoveryParentOrigin
      || event.source !== window.parent || !config.parentOrigins.includes(event.origin) || event.ports?.length
      || !data || typeof data !== 'object' || Array.isArray(data) || Object.keys(data).sort().join(',') !== 'appId,nonce,schema'
      || data.schema !== 'soty.app-boot-watch.v1' || !/^app-[a-f0-9]{32}$/.test(data.appId || '')
      || !/^[A-Za-z0-9_-]{43}$/.test(data.nonce || '')) return;
    recoveryParentOrigin = event.origin; recoveryNonce = data.nonce; recoveryApp = data.appId;
    notifyRecovery();
  });
  function show(state, pending = false, errorCode = '') {
    clearNativeContinue();
    const text = config.copy[state] || config.copy.unavailable;
    title.textContent = text[0]; detail.textContent = text[1];
    document.title = `${text[0]} · Соты`;
    entry.dataset.state = pending ? 'loading' : 'error'; entry.setAttribute('aria-busy', String(pending));
    entry.dataset.errorCode = typeof errorCode === 'string' && Object.hasOwn(config.errorStates, errorCode) ? errorCode : '';
    shell.hidden = pending || !topLevel || !config.shellUrl;
    if (!shell.hidden) shell.href = config.shellUrl;
    else shell.removeAttribute('href');
    reset.hidden = !config.publicResetPath || (pending && state !== 'resetting'); reset.disabled = pending;
    actions.hidden = shell.hidden && reset.hidden;
    frameHint.hidden = pending || topLevel;
  }
  function failure(error, fallback = 'network') {
    const state = typeof error?.code === 'string' && Object.hasOwn(config.errorStates, error.code) ? config.errorStates[error.code] : null;
    const responseState = error?.responseFailed && fallback === 'network'
      ? (Object.hasOwn(config.errorStates, error.status) ? config.errorStates[error.status] : 'unavailable') : fallback;
    const code = state ? error.code : error?.code === 'cookie_check' || responseState === 'cookie' ? 'app_session_check_failed' : '';
    show(state || (error?.code === 'cookie_check' ? 'cookie' : responseState), false, code);
  }
  async function requestJson(method, body, sessionCheck, frame) {
    const controller = new AbortController(); controllers.add(controller);
    const timeout = setTimeout(() => controller.abort(), 10_000);
    const started = performance.now(), observation = {method,status:null,elapsed100ms:0,headersReceived:false,jsonParsed:false,errorName:'none',signalAborted:false};
    try {
      const headers = { Accept: 'application/json' };
      if (body !== undefined) headers['Content-Type'] = 'application/json';
      if (sessionCheck !== undefined) headers['X-Soty-Boot-Check'] = sessionCheck;
      if (frame !== undefined) headers['X-Soty-Frame'] = frame;
      const response = await fetch('/_soty/session', { method, credentials: 'same-origin', cache: 'no-store', redirect: 'error',
        headers, ...(body === undefined ? {} : { body: JSON.stringify(body) }), signal: controller.signal });
      observation.headersReceived = true; observation.status = Number.isInteger(response.status) && response.status >= 100 && response.status <= 599 ? response.status : null;
      const value = await response.json();
      observation.jsonParsed = true;
      if (!response.ok || !value || typeof value !== 'object' || value.ok !== true) { observation.errorName = 'http'; throw { code: value?.error, responseFailed: true, status: response.status }; }
      return value;
    } catch (error) {
      if (observation.errorName === 'none') { try { observation.errorName = ['TypeError','SyntaxError','AbortError'].includes(error?.name) ? error.name : 'other'; } catch { observation.errorName = 'other'; } }
      throw error;
    } finally {
      clearTimeout(timeout); controllers.delete(controller);
      observation.elapsed100ms = Math.min(1200,Math.max(0,Math.round((performance.now()-started)/100)));
      observation.signalAborted = controller.signal.aborted;
      if (method === 'POST' || method === 'GET') diagnostic.request(observation);
    }
  }
  function navigate(path, currentGeneration) {
    if (offPage || generation !== currentGeneration) { clearNativeContinue(); return; }
    if (navigated) return;
    navigated = true; location.replace(path);
  }
  async function boot() {
    const currentGeneration = generation;
    if (cleanupFailed || !/^[A-Za-z0-9_-]{43}$/.test(ticket)) { ticket = ''; diagnostic.phase('boot_failed'); show('expired'); return; }
    busy = true; let checkingCookie = false;
    try {
      const body = { ticket }; ticket = '';
      diagnostic.phase('boot_admission_pending');
      const issued = await requestJson('POST', body);
      if (offPage || generation !== currentGeneration) return;
      const path = config.scoped ? allowScopedPath(issued.entryPath) : allowPath(issued.entryPath);
      if (!path || typeof issued.sessionCheck !== 'string' || !/^[A-Za-z0-9_-]{43}$/.test(issued.sessionCheck)) throw { code: 'cookie_check' };
      if(issued.frame!==undefined&&(!config.scoped||typeof issued.frame!=='string'||!/^[A-Za-z0-9_-]{43}$/.test(issued.frame)))throw{code:'cookie_check'};
      show('checking', true);
      checkingCookie = true;
      diagnostic.phase('boot_cookie_pending');
      const checked = await requestJson('GET', undefined, issued.sessionCheck, issued.frame);
      if (offPage || generation !== currentGeneration) return;
      if (checked.sessionCheck !== issued.sessionCheck) throw { code: 'cookie_check' };
      if(config.hostRenewal){
        if(issued.hostSessionRenewal!==true||typeof issued.hostSessionRequestId!=='string'
          ||!/^[A-Za-z0-9_-]{16,128}$/.test(issued.hostSessionRequestId)||!config.parentOrigins.includes(issued.hostSessionParentOrigin))throw{code:'app_host_session_changed'};
        // Transport wakeup only. Root must freshly confirm its own current
        // durable lease/CAS; this neither navigates nor calls the Source.
        window.parent.postMessage({schema:'soty.app-host-session-wakeup.v1',requestId:issued.hostSessionRequestId},issued.hostSessionParentOrigin);return;
      }
      if(config.scoped){
        if(typeof issued.scopedRequestId!=='string'||!/^[A-Za-z0-9_-]{43}$/.test(issued.scopedRequestId))throw{code:'app_scoped_context_closed'};
        let ready=false;
        diagnostic.phase('boot_continuation_pending');
        try{const response=await fetch('/api/embed/session-continue',{method:'POST',credentials:'same-origin',cache:'no-store',redirect:'error',
          headers:{'content-type':'application/json',...(issued.frame?{'x-soty-frame':issued.frame}:{}),...(config.renewal?{'x-soty-boot-check':issued.sessionCheck}:{})},body:JSON.stringify({requestId:issued.scopedRequestId}),signal:AbortSignal.timeout(10000)});
          const bytes=await response.arrayBuffer();if(bytes.byteLength>1024)throw{code:'app_scoped_continue_invalid'};
          const value=JSON.parse(new TextDecoder().decode(bytes));
          if(config.renewal&&(!response.ok||value.schema!=='soty.source-session-continuation.v1'||typeof value.ready!=='boolean'))throw{code:'app_scoped_continue_unknown'};
          ready=response.ok&&value.schema==='soty.source-session-continuation.v1'&&value.ready===true;
        }catch{if(config.renewal)throw{code:'app_scoped_continue_unknown'};}
        if(offPage||generation!==currentGeneration)return;
        if(config.renewal){if(issued.renewal!==true||typeof issued.renewalRequestId!=='string')throw{code:'app_scoped_context_closed'};
          window.parent.postMessage({schema:'soty.app-slot-ready.v1',requestId:issued.renewalRequestId,ready},config.parentOrigin);return;}
      }
      const at = path.indexOf('#'), requestPath = at < 0 ? path : path.slice(0, at), fragment = at < 0 ? '' : path.slice(at);
      const destination = issued.frame ? requestPath+'?sotyFrame='+issued.frame+fragment : path;
      show('opening', true);
      if (nativeContinue) {
        nativeGeneration = currentGeneration; nativeContinue.href = destination;
        nativeContinue.hidden = false; actions.hidden = false;
        if (!await waitForDocumentLoad() || offPage || generation !== currentGeneration) { clearNativeContinue(); return; }
        if (navigated) return;
      }
      diagnostic.phase('boot_navigation');
      navigate(destination, currentGeneration);
    } catch (error) { if (!offPage && generation === currentGeneration) {
      diagnostic.phase('boot_failed');
      if(config.renewal)window.parent.postMessage({schema:'soty.app-slot-ready.v1',ready:false,unknown:true},config.parentOrigin);
      failure(error,checkingCookie?'cookie':'network');
      recoveryFailure=!config.scoped&&checkingCookie&&error?.responseFailed===true&&error?.code==='app_session_check_failed';notifyRecovery();
    } }
    finally { if (generation === currentGeneration) busy = false; }
  }
  reset.addEventListener('click', async () => {
    if (busy || offPage || navigated || !config.publicResetPath) return;
    const currentGeneration = generation; busy = true; show('resetting', true);
    try { await requestJson('DELETE'); navigate(config.publicResetPath, currentGeneration); }
    catch (error) { if (!offPage && generation === currentGeneration) failure(error); }
    finally { if (generation === currentGeneration) busy = false; }
  });
  window.addEventListener('pagehide', () => { diagnostic.close(); offPage = true; generation++; clearNativeContinue(); cancelNavigationLoad?.(); recoverySent = true; recoveryFailure = false; recoveryParentOrigin = '';
    for (const controller of controllers) controller.abort(); });
  window.addEventListener('pageshow', event => {
    if (!event.persisted) return;
    clearNativeContinue(); cancelNavigationLoad?.();
    offPage = false; busy = false; navigated = false;
    show(config.mode === 'boot' ? 'expired' : config.initialState, false, config.mode === 'boot' ? 'app_ticket_invalid' : config.errorCode);
  });
  show(config.initialState, config.mode === 'boot', config.errorCode);
  if (config.mode === 'boot') void boot();
}

function initializeMaterialTheme() {
  if (typeof window.matchMedia !== 'function') return;
  const preference = window.matchMedia('(prefers-color-scheme: dark)'), root = document.body;
  const update = () => { root.dataset.glassTone = preference.matches ? 'dark' : 'light'; };
  update(); preference.addEventListener?.('change', update);
  window.addEventListener('pagehide', () => preference.removeEventListener?.('change', update));
  window.addEventListener('pageshow', event => { if(event.persisted){update();preference.addEventListener?.('change', update);} });
}

function render({ mode, state, shellUrl, publicResetPath, nonce,parentOrigins=[],scoped=false,renewal=false,hostRenewal=false,parentOrigin,errorCode='' }) {
  const config = { mode, initialState: state, shellUrl, publicResetPath,parentOrigins, copy, errorCode, errorStates: statesFor(publicResetPath),
    ...(scoped?{scoped:true,renewal,parentOrigin}:{}),...(hostRenewal?{hostRenewal:true}:{}) };
  const text = copy[state];
  const nativeAction = mode === 'boot' && scoped && !renewal ? '<a class="action action-primary" data-glass-role="accent" id="native-action" hidden>Продолжить открытие</a>' : '';
  const brand = roundedHexPath(10, undefined, { x: 11, y: 10 });
  return `<!doctype html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="referrer" content="no-referrer"><meta name="color-scheme" content="light dark"><title>${html(text[0])} · Соты</title><style>${styles}</style></head>
<body class="glass-ui" data-glass-material="working" data-glass-render-mode="static" data-glass-tone="light" data-glass-role="panel"><header class="brand"><svg viewBox="0 0 22 20" aria-hidden="true" focusable="false"><path d="${brand}"/></svg><span>Соты</span></header>
<main class="entry" id="entry" data-state="${mode === 'boot' ? 'loading' : 'error'}" aria-labelledby="status-title" aria-busy="${mode === 'boot'}"><div class="visual">${emblem}</div><div class="content"><div role="status" aria-live="polite" aria-atomic="true"><h1 id="status-title">${html(text[0])}</h1><p class="detail" id="status-detail">${html(text[1])}</p></div><div class="actions" id="actions" hidden><a class="action action-primary" data-glass-role="accent" id="shell-action" hidden>Открыть через Соты <span aria-hidden="true">↗</span></a><button class="action" data-glass-role="control" id="reset-action" type="button" hidden>Продолжить без входа</button>${nativeAction}</div><p class="frame-hint" id="frame-hint" hidden>В меню ⋯ над окном выберите «Обновить приложение».</p><noscript><p class="noscript">Для входа нужен JavaScript. Откройте приложение из Сот.</p></noscript></div></main>
<script${nonceAttribute(nonce)}>(${pageClient.toString()})(${scriptData(config)},${checkedLocalPath.toString()},${createBootDisplayClient.toString()},${checkedScopedEntryPath.toString()});(${initializeMaterialTheme.toString()})();</script></body></html>`;
}

export function renderBootPage({ shellUrl, publicResetPath, nonce, parentOrigins,scoped=false,renewal=false,hostRenewal=false,parentOrigin } = {}) {
  const shell = shellAddress(shellUrl, true), origins = parentOrigins ?? [new URL(shell).origin];
  if (!Array.isArray(origins) || !origins.length
    || origins.some(origin => typeof origin !== 'string' || new URL(shellAddress(origin, true)).origin !== origin)) throw new TypeError('invalid_parent_origins');
  if(typeof hostRenewal!=='boolean'||hostRenewal&&scoped)throw new TypeError('invalid_host_renewal');
  return render({ mode: 'boot', state: 'loading', shellUrl: shell, publicResetPath: resetPath(publicResetPath), nonce, parentOrigins: origins,scoped,renewal,hostRenewal,parentOrigin });
}

export function renderStatusPage({ error, shellUrl, publicResetPath, nonce } = {}) {
  const path = resetPath(publicResetPath), states = statesFor(path);
  const state = (typeof error === 'string' || typeof error === 'number') && Object.hasOwn(states, error) ? states[error] : 'unavailable';
  const errorCode = typeof error === 'string' && Object.hasOwn(states, error) ? error : '';
  return render({ mode: 'status', state, shellUrl: shellAddress(shellUrl), publicResetPath: path, nonce, errorCode });
}
