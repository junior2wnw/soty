import { roundedHexPath, hexCluster } from '../../ui/hex.mjs';
import { createPalette } from '../../ui/palette.mjs';

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
  loading: ['Открываем приложение', 'Проверяем ваш доступ.'],
  checking: ['Проверяем вход', 'Ещё мгновение — и можно продолжить.'],
  opening: ['Открываем приложение', 'Доступ подтверждён.'],
  denied: ['Доступ закрыт', 'Откройте приложение через Соты, чтобы проверить доступ.'],
  changed: ['Доступ изменился', 'Откройте приложение заново.'],
  expired: ['Нужно открыть заново', 'Ссылка для входа больше не действует.'],
  cookie: ['Вход не подтвердился', 'Откройте приложение отдельно и попробуйте снова.'],
  offline: ['Устройство не в сети', 'Приложение появится, когда устройство подключится.'],
  stopped: ['Приложение остановлено', 'Владелец сможет запустить его снова.'],
  busy: ['Сейчас много подключений', 'Попробуйте открыть приложение чуть позже.'],
  address: ['Адрес недоступен', 'Вернитесь в Соты и выберите приложение.'],
  inactive: ['Адрес ещё не открыт', 'Попробуйте открыть приложение через Соты.'],
  network: ['Не удалось подключиться', 'Проверьте соединение и откройте приложение снова.'],
  resetting: ['Открываем без входа', 'Переходим к публичному приложению.'],
  unavailable: ['Приложение недоступно', 'Попробуйте открыть его немного позже.'],
});

const errorStates = Object.freeze({
  apps_access_denied: 'denied', app_access_revoked: 'denied', app_access_changed: 'denied', apps_authentication_required: 'denied',
  app_session_required: 'expired', app_ticket_invalid: 'expired', app_access_expired: 'expired',
  app_session_check_failed: 'cookie', app_cookie_required: 'cookie',
  app_offline: 'offline', app_stopped: 'stopped', app_device_busy: 'busy', apps_sessions_busy: 'busy', apps_launch_busy: 'busy',
  app_not_found: 'address', app_address_retired: 'address', app_named_runtime_unavailable: 'inactive',
  429: 'busy', 503: 'unavailable',
});

const statesFor = publicResetPath => publicResetPath ? { ...errorStates, app_access_changed: 'changed' } : errorStates;

const palette = Object.entries(createPalette('dark', 50)).map(([name, value]) => `${name}:${value}`).join(';');
const cluster = hexCluster([[0, 0], [1, 0], [0, 1]], 20, 3, 2);
const emblem = `<svg class="emblem" viewBox="0 0 ${cluster.width} ${cluster.height}" aria-hidden="true" focusable="false">${cluster.cells.map((cell, index) =>
  `<path class="cell cell-${index}" d="${roundedHexPath(20, undefined, cell)}"/>`).join('')}</svg>`;

const styles = `
:root{${palette};color-scheme:var(--sw-color-scheme);font-family:Manrope,"Segoe UI",system-ui,sans-serif;font-synthesis:none}
*{box-sizing:border-box}body{margin:0;min-width:0;min-height:100vh;min-height:100svh;background:var(--sw-bg);color:var(--sw-ink);display:grid;grid-template-rows:auto 1fr}
body[data-framed="true"]{grid-template-rows:1fr}body[data-framed="true"] .brand{display:none}
.brand{display:flex;align-items:center;gap:9px;padding:22px 24px;font-size:15px;font-weight:650;letter-spacing:.01em;color:var(--sw-muted)}
.brand svg{width:22px;height:20px;fill:var(--sw-honey)}.entry{align-self:center;justify-self:center;width:min(460px,calc(100% - 32px));margin:12px 0 64px;padding:clamp(24px,5vw,40px);border:1px solid var(--sw-line);border-radius:28px;background:var(--sw-surface);box-shadow:var(--sw-shadow-small)}
.visual{width:88px;max-width:100%;margin-bottom:28px}.emblem{display:block;width:100%;height:auto;overflow:visible}.cell{fill:var(--sw-surface-raised);stroke:var(--sw-line);stroke-width:1}.cell-0{fill:var(--sw-honey-light);stroke:var(--sw-honey-border)}
[data-state="loading"] .cell{animation:breathe 2s ease-in-out infinite}.cell-1{animation-delay:.18s!important}.cell-2{animation-delay:.36s!important}
.content{min-width:0}.eyebrow{margin:0 0 8px;color:var(--sw-muted);font-size:12px;font-weight:600;letter-spacing:.06em}h1{font-size:clamp(24px,6vw,32px);font-weight:620;line-height:1.18;letter-spacing:-.035em;margin:0;text-wrap:balance;overflow-wrap:anywhere}
.detail{margin:14px 0 0;max-width:36ch;font-size:15px;line-height:1.55;color:var(--sw-muted);overflow-wrap:anywhere}.actions{display:flex;flex-wrap:wrap;gap:10px;margin-top:26px}.action{appearance:none;min-height:46px;max-width:100%;display:inline-flex;align-items:center;justify-content:center;gap:8px;padding:12px 16px;border:1px solid var(--sw-control-border);border-radius:14px;background:transparent;color:var(--sw-ink);font-weight:600;font-size:14px;line-height:1.35;font-family:inherit;text-decoration:none;text-align:center;cursor:pointer;overflow-wrap:anywhere;transition:background .15s,border-color .15s}
.action-primary{background:var(--sw-action);color:var(--sw-action-ink);border-color:var(--sw-action)}.action:hover{background:var(--sw-surface-hover)}.action-primary:hover{background:var(--sw-action-hover);border-color:var(--sw-action-hover)}.action:focus-visible{outline:3px solid var(--sw-focus);outline-offset:4px}.action:disabled{cursor:wait;opacity:.65}.frame-hint{margin:20px 0 0;font-size:14px;line-height:1.5;color:var(--sw-muted);max-width:34ch}.noscript{margin-top:20px;font-size:14px;line-height:1.5;color:var(--sw-muted)}[hidden]{display:none!important}
@keyframes breathe{0%,100%{opacity:.48}50%{opacity:1}}
@media(max-width:359px){.brand{padding:18px 20px}.entry{padding:24px 20px;border-radius:22px}.actions{flex-direction:column}.action{width:100%}.visual{width:76px;margin-bottom:22px}}
@media(max-height:440px) and (min-width:480px){.brand{padding:12px 20px}.entry{width:min(660px,calc(100% - 32px));display:grid;grid-template-columns:72px minmax(0,1fr);gap:24px;padding:22px 26px;margin:6px 0 24px}.visual{width:72px;margin:5px 0 0}h1{font-size:26px}.detail{margin-top:9px}.actions{margin-top:18px}.frame-hint{margin-top:14px}}
@media(max-height:280px) and (min-width:480px){body[data-framed="true"] .entry{width:min(660px,calc(100% - 24px));grid-template-columns:48px minmax(0,1fr);gap:16px;padding:12px 16px;margin:8px 0;border-radius:18px}body[data-framed="true"] .visual{width:48px;margin:2px 0 0}body[data-framed="true"] .eyebrow{display:none}body[data-framed="true"] h1{font-size:22px}body[data-framed="true"] .detail{margin-top:6px;font-size:14px;line-height:1.4;max-width:none}body[data-framed="true"] .actions{margin-top:10px;gap:8px}body[data-framed="true"] .action{min-height:44px;padding:10px 14px}body[data-framed="true"] .frame-hint{margin-top:8px;font-size:13px;line-height:1.4;max-width:none}}
@media(prefers-reduced-motion:reduce){*,*::before,*::after{animation:none!important;transition:none!important}}
`;

function pageClient(config, allowPath) {
  // Clear the one-time secret synchronously before DOM work or any request.
  let ticket = '', cleanupFailed = false;
  if (config.mode === 'boot') {
    ticket = location.hash.slice(1);
    try { history.replaceState(null, '', location.pathname + location.search); }
    catch { cleanupFailed = true; }
  }
  const entry = document.getElementById('entry'), title = document.getElementById('status-title'), detail = document.getElementById('status-detail');
  const actions = document.getElementById('actions'), shell = document.getElementById('shell-action'), reset = document.getElementById('reset-action'), frameHint = document.getElementById('frame-hint');
  const topLevel = window.top === window.self, controllers = new Set();
  document.body.dataset.framed = String(!topLevel);
  let busy = false, navigated = false, offPage = false, generation = 0;
  function show(state, pending = false) {
    const text = config.copy[state] || config.copy.unavailable;
    title.textContent = text[0]; detail.textContent = text[1];
    document.title = `${text[0]} · Соты`;
    entry.dataset.state = pending ? 'loading' : 'error'; entry.setAttribute('aria-busy', String(pending));
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
    show(state || (error?.code === 'cookie_check' ? 'cookie' : responseState));
  }
  async function requestJson(method, body, sessionCheck) {
    const controller = new AbortController(); controllers.add(controller);
    const timeout = setTimeout(() => controller.abort(), 10_000);
    try {
      const headers = { Accept: 'application/json' };
      if (body !== undefined) headers['Content-Type'] = 'application/json';
      if (sessionCheck !== undefined) headers['X-Soty-Boot-Check'] = sessionCheck;
      const response = await fetch('/_soty/session', { method, credentials: 'same-origin', cache: 'no-store', redirect: 'error',
        headers, ...(body === undefined ? {} : { body: JSON.stringify(body) }), signal: controller.signal });
      const value = await response.json();
      if (!response.ok || !value || typeof value !== 'object' || value.ok !== true) throw { code: value?.error, responseFailed: true, status: response.status };
      return value;
    } finally { clearTimeout(timeout); controllers.delete(controller); }
  }
  function navigate(path, currentGeneration) {
    if (navigated || offPage || generation !== currentGeneration) return;
    navigated = true; location.replace(path);
  }
  async function boot() {
    const currentGeneration = generation;
    if (cleanupFailed || !/^[A-Za-z0-9_-]{43}$/.test(ticket)) { ticket = ''; show('expired'); return; }
    busy = true; let checkingCookie = false;
    try {
      const body = { ticket }; ticket = '';
      const issued = await requestJson('POST', body);
      if (offPage || generation !== currentGeneration) return;
      const path = allowPath(issued.entryPath);
      if (!path || typeof issued.sessionCheck !== 'string' || !/^[A-Za-z0-9_-]{43}$/.test(issued.sessionCheck)) throw { code: 'cookie_check' };
      show('checking', true);
      checkingCookie = true;
      const checked = await requestJson('GET', undefined, issued.sessionCheck);
      if (offPage || generation !== currentGeneration) return;
      if (checked.sessionCheck !== issued.sessionCheck) throw { code: 'cookie_check' };
      show('opening', true); navigate(path, currentGeneration);
    } catch (error) { if (!offPage && generation === currentGeneration) failure(error, checkingCookie ? 'cookie' : 'network'); }
    finally { if (generation === currentGeneration) busy = false; }
  }
  reset.addEventListener('click', async () => {
    if (busy || offPage || navigated || !config.publicResetPath) return;
    const currentGeneration = generation; busy = true; show('resetting', true);
    try { await requestJson('DELETE'); navigate(config.publicResetPath, currentGeneration); }
    catch (error) { if (!offPage && generation === currentGeneration) failure(error); }
    finally { if (generation === currentGeneration) busy = false; }
  });
  window.addEventListener('pagehide', () => { offPage = true; generation++; for (const controller of controllers) controller.abort(); });
  window.addEventListener('pageshow', event => {
    if (!event.persisted) return;
    offPage = false; busy = false; navigated = false;
    show(config.mode === 'boot' ? 'expired' : config.initialState);
  });
  show(config.initialState, config.mode === 'boot');
  if (config.mode === 'boot') void boot();
}

function render({ mode, state, shellUrl, publicResetPath, nonce }) {
  const config = { mode, initialState: state, shellUrl, publicResetPath, copy, errorStates: statesFor(publicResetPath) };
  const text = copy[state];
  const brand = roundedHexPath(10, undefined, { x: 11, y: 10 });
  return `<!doctype html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="referrer" content="no-referrer"><meta name="color-scheme" content="dark"><title>${html(text[0])} · Соты</title><style>${styles}</style></head>
<body><header class="brand"><svg viewBox="0 0 22 20" aria-hidden="true" focusable="false"><path d="${brand}"/></svg><span>Соты</span></header>
<main class="entry" id="entry" data-state="${mode === 'boot' ? 'loading' : 'error'}" aria-labelledby="status-title" aria-busy="${mode === 'boot'}"><div class="visual">${emblem}</div><div class="content"><p class="eyebrow">ПРИЛОЖЕНИЕ</p><div role="status" aria-live="polite" aria-atomic="true"><h1 id="status-title">${html(text[0])}</h1><p class="detail" id="status-detail">${html(text[1])}</p></div><div class="actions" id="actions" hidden><a class="action action-primary" id="shell-action" hidden>Открыть через Соты <span aria-hidden="true">↗</span></a><button class="action" id="reset-action" type="button" hidden>Продолжить без входа</button></div><p class="frame-hint" id="frame-hint" hidden>Откройте отдельно кнопкой над приложением.</p><noscript><p class="noscript">Для входа нужен JavaScript. Откройте приложение из Сот.</p></noscript></div></main>
<script${nonceAttribute(nonce)}>(${pageClient.toString()})(${scriptData(config)},${checkedLocalPath.toString()});</script></body></html>`;
}

export function renderBootPage({ shellUrl, publicResetPath, nonce } = {}) {
  return render({ mode: 'boot', state: 'loading', shellUrl: shellAddress(shellUrl, true), publicResetPath: resetPath(publicResetPath), nonce });
}

export function renderStatusPage({ error, shellUrl, publicResetPath, nonce } = {}) {
  const path = resetPath(publicResetPath), states = statesFor(path);
  const state = (typeof error === 'string' || typeof error === 'number') && Object.hasOwn(states, error) ? states[error] : 'unavailable';
  return render({ mode: 'status', state, shellUrl: shellAddress(shellUrl), publicResetPath: path, nonce });
}
