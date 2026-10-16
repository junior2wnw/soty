import { roundedHexPath, hexCluster } from '../../ui/hex.mjs';
import { createPalette } from '../../ui/palette.mjs';
import { AccessError, assert, canonicalJson, exact, identifier, integer, text } from './validation.mjs';
import { DISCOVERY_LIMITS } from './discovery.mjs';
import { assertPublicStrings, CAPABILITY_VALIDATION_PROFILE } from './documentation.mjs';

export const DISCOVERY_HTML_LIMITS = Object.freeze({ index: 256 * 1024, detail: 512 * 1024, connect: 128 * 1024 });
const INVALID = 'discovery_render_invalid';
const html = value => String(value).replace(/[&<>"']/gu, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char]);
const digest = value => assert(typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value), INVALID);
const label = (value, max = 16384) => { text(value, { min: 0, max, code: INVALID }); assert(value.isWellFormed(), INVALID); return value; };
function boundedJson(value, maxBytes) {
  try { canonicalJson(value, { maxBytes }); }
  catch (error) { throw new AccessError(error?.code === 'payload_too_large' ? 'projection_too_large' : INVALID); }
  assertPublicStrings(value, INVALID);
}
function originValue(value) {
  if (value === '') return value;
  label(value, 2048);
  assert(!/[\\\u0000-\u0020\u007f]/u.test(value), INVALID);
  let url; try { url = new URL(value); } catch { throw new AccessError(INVALID); }
  assert(url.origin === value && !url.username && !url.password
    && (url.protocol === 'https:' || (url.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname))), INVALID);
  return value;
}
function checkedLinks(links, capabilityId, version) {
  identifier(capabilityId, INVALID); integer(version, 1, 1000000, INVALID);
  exact(links, ['html', 'detail', 'contract', 'inputSchema', 'outputSchema'], INVALID);
  const segment = encodeURIComponent(capabilityId), detail = `/api/capabilities/v1/catalog/${segment}/versions/${version}`;
  const expected = { html: `/agents/capabilities/${segment}/versions/${version}`, detail, contract: `${detail}/contract.json`,
    inputSchema: `${detail}/schemas/input`, outputSchema: `${detail}/schemas/output` };
  for (const key of Object.keys(expected)) assert(links[key] === expected[key], INVALID);
  return links;
}
function pageHref(query, limit, cursor) {
  const parameters = new URLSearchParams({ query, limit: String(limit) });
  if (cursor) parameters.set('cursor', cursor);
  return `/agents?${parameters}`;
}

const tokens = Object.entries(createPalette('dark', 50)).map(([key, value]) => `${key}:${value}`).join(';');
const cluster = hexCluster([[0, 0], [1, 0], [0, 1]], 6, 1, 1);
const mark = `<svg viewBox="0 0 ${cluster.width} ${cluster.height}" width="34" height="32" aria-hidden="true" focusable="false">${cluster.cells.map(cell => `<path d="${roundedHexPath(6, undefined, { x: cell.x, y: cell.y })}"/>`).join('')}</svg>`;
const css = `
:root{${tokens};color-scheme:var(--sw-color-scheme);font-family:Manrope,"Segoe UI",system-ui,sans-serif;font-synthesis:none}
*{box-sizing:border-box}body{margin:0;background:var(--sw-bg);color:var(--sw-ink);font-size:16px;line-height:1.55}a{color:var(--sw-action);text-underline-offset:.2em}a:hover{color:var(--sw-action-hover)}a,button,input,summary{touch-action:manipulation}:focus-visible{outline:3px solid var(--sw-focus);outline-offset:3px}h1,h2,h3,p{margin:0}h1{font-size:clamp(28px,4.3vw,46px);font-weight:650;line-height:1.15;letter-spacing:-.035em}h2{font-size:22px;line-height:1.3;letter-spacing:-.02em}h3{font-size:16px;line-height:1.4}button,input{font:inherit}svg path{fill:var(--sw-honey-light);stroke:var(--sw-honey);stroke-width:1}code,pre{font-family:ui-monospace,Consolas,monospace;font-size:13px}code{overflow-wrap:anywhere}pre{margin:8px 0 0;padding:14px;background:var(--sw-bg);border:1px solid var(--sw-line);border-radius:12px;white-space:pre-wrap;overflow-wrap:anywhere;tab-size:2;max-width:100%}ul{margin:10px 0 0;padding-left:22px}li+li{margin-top:7px}
.wrap{width:min(1120px,100%);margin-inline:auto;padding-inline:28px;min-width:0}.top{border-bottom:1px solid var(--sw-line)}.top-inner{min-height:76px;display:flex;align-items:center;justify-content:space-between;gap:14px;flex-wrap:wrap;padding-block:10px}.brand{display:inline-flex;align-items:center;gap:9px;min-height:44px;color:var(--sw-ink);font-weight:700;font-size:19px;text-decoration:none}.top nav{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.text-link{display:inline-flex;align-items:center;min-height:44px;padding:8px 10px;overflow-wrap:anywhere}.skip{position:absolute;left:12px;top:-100px;z-index:2;background:var(--sw-surface);padding:12px 18px}.skip:focus{top:12px}main{padding-block:42px 52px}.hero{max-width:780px}.eyebrow{color:var(--sw-muted);font-size:13px;letter-spacing:.06em;margin-bottom:12px}.intro{color:var(--sw-muted);margin-top:16px;max-width:66ch}.search{margin-top:28px;max-width:820px}.search label{display:block;font-weight:600;margin-bottom:9px}.search-row{display:flex;gap:10px;align-items:stretch}.search input[type=search]{min-width:0;width:100%;min-height:48px;padding:11px 14px;background:var(--sw-surface);border:1px solid var(--sw-control-border);border-radius:12px;color:var(--sw-ink)}.button{display:inline-flex;align-items:center;justify-content:center;min-height:44px;max-width:100%;padding:10px 16px;border:1px solid var(--sw-control-border);border-radius:12px;background:var(--sw-surface);color:var(--sw-ink);font-weight:600;text-decoration:none;text-align:center;cursor:pointer;overflow-wrap:anywhere}.button:hover{background:var(--sw-surface-hover);color:var(--sw-ink)}.primary{background:var(--sw-action);border-color:var(--sw-action);color:var(--sw-action-ink)}.primary:hover{background:var(--sw-action-hover);color:var(--sw-action-ink)}.hint{font-size:13px;color:var(--sw-muted);margin-top:8px}.result-heading{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;margin:32px 0 16px}.result-heading h2{font-size:17px}.result-heading p{font-size:14px;color:var(--sw-muted)}.cards{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px}.card,.panel{padding:22px;border:1px solid var(--sw-line);border-radius:18px;background:var(--sw-surface);min-width:0}.card{display:flex;flex-direction:column;gap:11px}.card h2 a{display:flex;align-items:center;min-height:44px;color:var(--sw-ink);text-decoration:none}.card h2 a:hover{text-decoration:underline}.card .summary{color:var(--sw-muted)}.identity{display:flex;align-items:baseline;gap:8px;flex-wrap:wrap;color:var(--sw-muted);font-size:13px}.badge{display:inline-flex;align-items:center;min-height:28px;padding:3px 9px;border-radius:8px;background:var(--sw-surface-raised);color:var(--sw-muted);font-size:12px;line-height:1.4}.card .badge{align-self:flex-start;margin-top:auto}.copy{overflow-wrap:anywhere;unicode-bidi:plaintext}.empty{padding:24px 0}.empty p{color:var(--sw-muted);margin:10px 0 18px}.pagination{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-top:22px}.readiness{padding:16px 18px;margin-top:24px;border-left:3px solid var(--sw-honey);border-radius:0 12px 12px 0;background:var(--sw-surface)}.readiness p{color:var(--sw-muted);font-size:14px;margin-top:4px}.back{margin-bottom:20px;padding-left:0}.detail-grid{display:grid;grid-template-columns:minmax(0,1.45fr) minmax(280px,1fr);gap:22px;margin-top:28px;align-items:start}.stack{display:grid;gap:20px;min-width:0}.panel h2+.copy,.panel h3+.copy{margin-top:10px}.scope-list{display:grid;gap:20px}.examples{margin-top:22px}.example{margin-top:16px}.example h3{margin-bottom:7px}.example-label{margin-top:12px;font-size:13px;color:var(--sw-muted)}details.panel{padding:0}summary{min-height:48px;padding:14px 20px;cursor:pointer;font-weight:600;border-radius:18px}details[open]>summary{border-bottom:1px solid var(--sw-line);border-radius:18px 18px 0 0}.details-body{padding:20px;min-width:0}.details-body h2{margin-bottom:12px}.machine-links{display:grid;gap:8px;margin-top:14px}.machine-links a{justify-content:flex-start}.metadata{margin:16px 0 0;display:grid;gap:13px}.metadata dt{color:var(--sw-muted);font-size:13px}.metadata dd{margin:3px 0 0;overflow-wrap:anywhere}.profile-copy{font-size:14px;color:var(--sw-muted)}.profile-copy+p{margin-top:10px}.footer{padding-block:22px;border-top:1px solid var(--sw-line);display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap;color:var(--sw-muted);font-size:13px}.footer nav{display:flex;flex-wrap:wrap;gap:6px}
.search-row>.button{flex:0 0 auto}
.scope-list>section>h2{font-size:16px;line-height:1.4;letter-spacing:normal}
.connect-entry{display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap;margin-top:24px;padding:16px 18px;border:1px solid var(--sw-line);border-radius:14px;background:var(--sw-surface)}.connect-entry p{color:var(--sw-muted);font-size:14px}.connect-sections{display:grid;gap:20px;margin-top:28px}.connect-sections .panel>p{margin-top:12px}.connect-sections ol{margin:14px 0 0;padding-left:24px}.connect-sections li>p{margin-top:4px;color:var(--sw-muted)}.connect-protocols{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:20px}.connect-table{width:100%;border-collapse:collapse;table-layout:fixed;margin-top:16px;font-size:14px}.connect-table th,.connect-table td{padding:12px 8px;vertical-align:top;text-align:left;border-bottom:1px solid var(--sw-line);overflow-wrap:anywhere}.connect-table th{font-weight:600;color:var(--sw-muted)}.connect-links{display:flex;flex-wrap:wrap;gap:8px;margin-top:16px}.connect-sections .metadata{margin-top:16px}.connect-sections code{white-space:normal}
@media(max-width:760px){.connect-protocols{grid-template-columns:minmax(0,1fr)}.connect-table th,.connect-table td{padding-inline:4px}}
@media(max-width:760px){.wrap{padding-inline:18px}main{padding-block:28px 36px}.detail-grid,.cards{grid-template-columns:minmax(0,1fr)}.detail-grid{gap:16px}.card,.panel{padding:18px}.top-inner{gap:6px}.top nav{gap:0}.hero{max-width:none}.intro{margin-top:12px}.result-heading{margin-top:26px}}
@media(max-width:380px){.wrap{padding-inline:14px}.top-inner{min-height:64px}.top nav .text-link{padding-inline:8px}.search-row{flex-wrap:wrap}.search-row input[type=search]{flex-basis:100%}.search-row .button{width:100%}.card,.panel{padding:16px}.details-body{padding:16px}h2{font-size:20px}.identity{font-size:12px}.footer{gap:6px}}
@media(max-height:420px) and (min-width:480px){.top-inner{min-height:60px;padding-block:6px}main{padding-top:22px}.eyebrow{margin-bottom:8px}.search{margin-top:20px}.back{margin-bottom:12px}}
@media(prefers-reduced-motion:reduce){*,*::before,*::after{animation:none!important;transition:none!important;scroll-behavior:auto!important}}
`;

function documentPage({ title, description, body, path, origin, noindex = false, maxBytes }) {
  const canonical = origin ? `<link rel="canonical" href="${html(origin + path)}">` : '';
  const result = `<!doctype html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${html(title)} — Соты</title><meta name="description" content="${html(description)}"><meta name="robots" content="${noindex ? 'noindex,follow' : 'index,follow'}">${canonical}<style>${css}</style><script defer src="/capability-docs-update.js"></script></head><body><a class="skip" href="#main">К содержанию</a><header class="top"><div class="wrap top-inner"><a class="brand" href="/" aria-label="Соты — главная">${mark}<span>Соты</span></a><nav aria-label="Документация"><a class="text-link" href="/agents">Возможности</a><a class="text-link" href="/agents/connect">Подключить агента</a><a class="text-link" href="/api/capabilities/v1/openapi.json">OpenAPI</a></nav></div></header><main id="main" class="wrap">${body}</main><footer class="wrap footer"><p>Публичные описания возможностей</p><nav aria-label="Данные каталога"><a class="text-link" href="/api/capabilities/v1/catalog">Каталог JSON</a><a class="text-link" href="/api/capabilities/v1/status">Состояние API</a></nav></footer></body></html>`;
  assert(Buffer.byteLength(result, 'utf8') <= maxBytes, 'projection_too_large');
  return result;
}

export function renderDiscoveryIndex(options) {
  exact(options, ['result', 'query', 'limit', 'origin', 'noindex'], INVALID);
  const { result, query = '', limit = 10, origin = '', noindex = false } = options;
  const base = originValue(origin);
  label(query, DISCOVERY_LIMITS.queryCodeUnits); integer(limit, 1, DISCOVERY_LIMITS.maxPage, INVALID); assert(typeof noindex === 'boolean', INVALID);
  exact(result, ['scope', 'revision', 'items', 'total', 'cursor'], INVALID);
  boundedJson(result, DISCOVERY_LIMITS.pageBytes);
  assert(result.scope === 'public' && Array.isArray(result.items) && result.items.length <= DISCOVERY_LIMITS.maxPage, INVALID);
  digest(result.revision); integer(result.total, result.items.length, DISCOVERY_LIMITS.versions, INVALID);
  assert(result.cursor === null || (typeof result.cursor === 'string' && /^[A-Za-z0-9_-]{1,512}$/u.test(result.cursor)), INVALID);
  const cards = result.items.map(item => {
    exact(item, ['capabilityId', 'version', 'appId', 'title', 'summary', 'digest', 'executionEnabled', 'match', 'links'], INVALID);
    const links = checkedLinks(item.links, item.capabilityId, item.version);
    identifier(item.appId, INVALID); assert(['exact-id', 'id-prefix', 'text', 'browse'].includes(item.match), INVALID);
    label(item.title); label(item.summary); digest(item.digest); assert(typeof item.executionEnabled === 'boolean', INVALID);
    return `<article class="card"><div class="identity"><code>${html(item.capabilityId)}</code><span>Версия ${item.version}</span></div><h2><a class="copy" href="${html(links.html)}">${html(item.title)}</a></h2><p class="summary copy">${html(item.summary)}</p><span class="badge">${item.executionEnabled ? 'Допуск к вызову проверяется отдельно' : 'Выполнение пока недоступно'}</span></article>`;
  }).join('');
  const empty = `<section class="empty"><h2>${result.total ? 'На этой странице нет записей' : 'Ничего не найдено'}</h2><p>${result.total ? 'Можно вернуться к началу этого поиска.' : 'Попробуйте другую задачу на русском или английском.'}</p><a class="button" href="${html(result.total ? pageHref(query, limit) : '/agents')}">${result.total ? 'К началу поиска' : 'Все возможности'}</a></section>`;
  const next = result.cursor ? `<nav class="pagination" aria-label="Страницы каталога"><a class="button" rel="next" href="${html(pageHref(query, limit, result.cursor))}">Следующие возможности</a></nav>` : '';
  const body = `<section class="hero" aria-labelledby="page-title"><p class="eyebrow">Для разработчиков и ИИ</p><h1 id="page-title">Возможности для ИИ</h1><p class="intro">Найдите действие, изучите ограничения и получите точную схему. Описание само по себе не даёт права вызова.</p></section><aside class="connect-entry" aria-label="Подключение агента"><div><strong>Нужен доступ к вашим приложениям?</strong><p>Протокол подключения, разрешение владельца и проверка результата.</p></div><a class="button" href="/agents/connect">Подключить агента</a></aside><form class="search" method="get" action="/agents" role="search"><label for="capability-query">Что хотите сделать?</label><div class="search-row"><input type="search" id="capability-query" name="query" maxlength="200" value="${html(query)}" placeholder="Например, сохранить заметку" aria-describedby="search-help"><input type="hidden" name="limit" value="${limit}"><button type="submit" class="button primary">Найти</button></div><p id="search-help" class="hint">Поиск на русском и английском. Только публичные описания.</p></form><section aria-labelledby="results-heading"><div class="result-heading"><h2 id="results-heading">Найдено: ${result.total}</h2><p>На странице: ${result.items.length}</p></div>${cards ? `<div class="cards">${cards}</div>` : empty}${next}</section>`;
  return documentPage({ title: 'Возможности для ИИ', description: 'Публичные возможности Сот: назначение, ограничения и точные схемы для независимых клиентов.',
    body, path: '/agents', origin: base, noindex: noindex || query.trim().length > 0, maxBytes: DISCOVERY_HTML_LIMITS.index });
}

/** Public protocol presence only. Never enumerate installed private app entries
 * or copy arbitrary OpenAPI text, schemas, extensions or tool metadata to HTML. */
export function createAgentConnectionProfile(openApi) {
  const post = (path, operationId) => openApi?.paths?.[path]?.post?.operationId === operationId;
  const mcp = openApi?.['x-soty-mcp'], oauth = openApi?.['x-soty-oauth-discovery'];
  return Object.freeze({
    mcp: mcp?.endpoint === '/mcp' && mcp.transport === 'streamable-http' && mcp.stateless === true
      && Array.isArray(mcp.methods) && mcp.methods.length === 1 && mcp.methods[0] === 'POST',
    notes: post('/api/capabilities/v1/notes/drafts', 'createNoteDraft'),
    applications: post('/api/capabilities/v1/app-actions/catalog', 'apps_catalog_search')
      && post('/api/capabilities/v1/app-actions/contract', 'apps_catalog_get'),
    invoke: post('/api/capabilities/v1/app-actions/invoke', 'apps_invoke')
      && post('/api/capabilities/v1/app-actions/get', 'apps_invocation_get')
      && post('/api/capabilities/v1/app-actions/cancel', 'apps_invocation_cancel'),
    query: post('/api/capabilities/v1/app-actions/query', 'apps_query'),
    guidance: post('/api/capabilities/v1/app-actions/guidance-list', 'apps_guidance_list')
      && post('/api/capabilities/v1/app-actions/guidance-get', 'apps_guidance_get'),
    oauth: oauth?.authorizationServer === '/.well-known/oauth-authorization-server/oauth'
      && oauth.httpResource === '/.well-known/oauth-protected-resource',
  });
}

export function renderAgentConnectionPage(options) {
  exact(options, ['profile', 'status', 'origin'], INVALID);
  const { profile, status, origin = '' } = options, base = originValue(origin);
  const flags = ['mcp', 'notes', 'applications', 'invoke', 'query', 'guidance', 'oauth'];
  exact(profile, flags, INVALID); for (const flag of flags) assert(typeof profile[flag] === 'boolean', INVALID);
  exact(status, ['notesCreateEnabled', 'audience'], INVALID); assert(typeof status.notesCreateEnabled === 'boolean', INVALID);
  assert(status.audience === null || typeof status.audience === 'string' && status.audience.length > 0, INVALID);
  const audience = status.audience === null ? null : originValue(status.audience);
  const audiences = audience
    ? `<dl class="metadata"><div><dt>HTTP audience</dt><dd><code>${html(audience)}</code></dd></div>${profile.mcp ? `<div><dt>MCP audience</dt><dd><code>${html(audience + '/mcp')}</code></dd></div>` : ''}</dl>`
    : '<p>Канонический audience для доступа не объявлен. Подключение нельзя завершить, подставив домен из адресной строки.</p>';
  const mcp = profile.mcp
    ? '<strong>MCP: транспорт настроен</strong><p><code>POST /mcp</code> · Streamable HTTP, без сохраняемой сессии. Для initialization и списка tools нужен действующий Bearer с отдельным MCP audience.</p>'
    : '<strong>MCP: не настроен</strong><p>Этот сервер не объявляет MCP transport. Используйте только операции, присутствующие в его OpenAPI.</p>';
  const oauth = profile.oauth
    ? '<strong>OAuth: профиль объявлен</strong><p>Проверьте метаданные перед подключением: наличие профиля не подтверждает доступность выдачи. Текущий OAuth-профиль относится к созданию записок, а не ко всем действиям приложений.</p><nav class="machine-links" aria-label="Метаданные OAuth"><a class="text-link" href="/.well-known/oauth-authorization-server/oauth">Сервер авторизации</a><a class="text-link" href="/.well-known/oauth-protected-resource">HTTP resource</a>'
      + (profile.mcp ? '<a class="text-link" href="/.well-known/oauth-protected-resource/mcp">MCP resource</a>' : '') + '</nav>'
    : '<strong>OAuth-подключение пока недоступно</strong><p>На этом сервере OAuth-профиль не объявлен. Для клиента нужен уже выданный владельцем сервисный ключ с точным audience. Эта страница не выдаёт и не расширяет доступ.</p>';
  const rows = [];
  const row = (name, path, purpose) => rows.push(`<tr><th scope="row"><code>${name}</code></th><td><code>POST /api/capabilities/v1/app-actions/${path}</code></td><td>${purpose}</td></tr>`);
  if (profile.applications) {
    row('apps_catalog_search', 'catalog', 'Найти действия, разрешённые вашему текущему доступу. Доступность приложения ещё не проверена.');
    row('apps_catalog_get', 'contract', 'Прочитать точный reference, версию, digest и схемы данных.');
    if (profile.guidance) {
      row('apps_guidance_list', 'guidance-list', 'Выбрать закреплённую инструкцию для этой возможности.');
      row('apps_guidance_get', 'guidance-get', 'Прочитать одну инструкцию по её точной ссылке и digest.');
    }
    if (profile.query) row('apps_query', 'query', 'Прочитать разрешённые данные приложения. Повтор с тем же ключом возвращает метаданные, а не сохранённый текст результата.');
    if (profile.invoke) {
      row('apps_invoke', 'invoke', 'Запросить одно разрешённое действие с постоянным idempotencyKey.');
      row('apps_invocation_get', 'get', 'Прочитать квитанцию своего действия без повторного выполнения.');
      row('apps_invocation_cancel', 'cancel', 'Запросить отмену. Уже совершённое действие этим не откатывается.');
    }
  }
  const applications = rows.length
    ? `<p>Это авторизованный каталог. Публичный поиск не раскрывает частные приложения, их ресурсы или инструкции. Одного наличия tool недостаточно: проверяются текущее разрешение владельца, доступ к приложению и права внутри самого приложения.</p><table class="connect-table"><thead><tr><th scope="col">MCP tool / operationId</th><th scope="col">HTTP</th><th scope="col">Задача</th></tr></thead><tbody>${rows.join('')}</tbody></table>`
    : '<p>Авторизованный каталог действий приложений этим сервером не объявлен. Пока можно изучить публичные описания и точные схемы из OpenAPI.</p>';
  const notes = !profile.notes ? 'Операция создания записок через HTTP этим сервером не объявлена.'
    : status.notesCreateEnabled ? 'Обработчик создания записок готов. Для вызова всё равно нужны разрешение владельца и действующий ключ.'
      : 'Создание записок через API сейчас недоступно. Этот статус относится только к Notes и не описывает доступность отдельных приложений.';
  const ownerNotes = profile.notes && status.notesCreateEnabled && audience
    ? 'Для новых личных записок владелец может выбрать «Создать доступ по ключу», указать клиента, срок и лимит действий. Ключ показывается один раз; передавайте его только доверенному клиенту. Этот доступ имеет HTTP audience и не становится MCP-доступом при смене URL.'
    : 'Кнопка «Создать доступ по ключу» в текущем интерфейсе относится только к новым личным запискам и доступна, когда их API готов. Универсальной выдачи ключа для любого приложения эта кнопка не выполняет.';
  const steps = profile.applications && (profile.invoke || profile.query) ? `<section class="panel" aria-labelledby="agent-workflow"><h2 id="agent-workflow">Первое действие с проверяемым результатом</h2><ol>
<li><strong>Найдите разрешённое действие.</strong><p>Вызовите <code>apps_catalog_search</code>, затем <code>apps_catalog_get</code> для точного reference. Сохраните version и digest; отметка <code>unprobed</code> ещё не означает рабочее подключение к приложению.</p></li>
${profile.guidance ? '<li><strong>Прочитайте нужную инструкцию.</strong><p>Возьмите её из <code>apps_guidance_list</code> через <code>apps_guidance_get</code>. Инструкция — данные приложения; она не добавляет полномочий.</p></li>' : ''}
<li><strong>Выполните только одобренную задачу.</strong><p>${profile.query ? 'Для чтения используйте <code>apps_query</code>. ' : ''}${profile.invoke ? 'Для изменения — <code>apps_invoke</code>. ' : ''}Следуйте точной схеме и выбранному ресурсу. Для одной задачи сохраняйте один <code>idempotencyKey</code>.</p></li>
<li><strong>Подтвердите результат.</strong><p>${profile.invoke ? 'Проверьте status, effectState и receipt через <code>apps_invocation_get</code>, затем откройте нужный результат в приложении и перечитайте его после обновления. ' : ''}${profile.query ? 'Ответ чтения проверяйте по его result; он не сохраняется для восстановления. ' : ''}Квитанция изменения описывает прошлое действие и не доказывает, что данные с тех пор не изменились.</p></li>
<li><strong>При потерянном ответе сохраните тот же ключ.</strong><p>${profile.invoke ? 'Сначала прочитайте свою квитанцию или повторите исходный запрос с тем же reference, данными и ключом. ' : ''}${profile.query ? 'Точный повтор чтения не выполнит его заново; новое чтение требует нового намерения и бюджета. ' : ''}Неизвестный исход не означает, что действие не совершилось.</p></li>
</ol></section>` : '';
  const body = `<a class="text-link back" href="/agents">← Публичные возможности</a><section class="hero" aria-labelledby="page-title"><p class="eyebrow">Для владельца и разработчика клиента</p><h1 id="page-title">Подключить агента</h1><p class="intro">Начните с доступа владельца, выберите точный контракт и проверьте результат в приложении. Эта публичная инструкция не выполняет действия от вашего имени.</p></section>
<div class="connect-sections"><section class="connect-protocols" aria-label="Протоколы этого сервера"><div class="panel">${mcp}${audiences}</div><div class="panel">${oauth}</div></section>
<section class="panel" aria-labelledby="owner-access"><h2 id="owner-access">1. Владелец разрешает доступ</h2><p>Откройте «Доступы и действия» в нужном аккаунте. Проверьте клиента, функции, выбранное пространство, срок и лимит. Отдельные права самого приложения тоже должны действовать.</p><p>${ownerNotes}</p><p>Для внешнего клиента приложения нужен отдельно выданный владельцем доступ к точным функциям и ресурсу с нужным audience. Разрешение встроенному личному помощнику не выдаёт его внутренний ключ стороннему клиенту. Если такого доступа ещё нет, подключение приложения пока не завершено.</p><div class="connect-links"><a class="button" href="/#access">Открыть доступы и действия</a></div></section>
<section class="panel" aria-labelledby="client-contract"><h2 id="client-contract">2. Клиент подключается по контракту</h2><p>Передавайте ключ только в заголовке Authorization: Bearer. Не помещайте его в URL, параметры страницы или текст запроса. HTTP и MCP audience различаются; cookie или открытый экран приложения не заменяют разрешение.</p><div class="connect-links"><a class="button" href="/api/capabilities/v1/openapi.json">Открыть точный OpenAPI</a><a class="text-link" href="/api/capabilities/v1/status">Статус Notes и audience</a></div><p>${html(notes)}</p></section>
<section class="panel" aria-labelledby="authorized-apps"><h2 id="authorized-apps">3. Действия ваших приложений</h2>${applications}</section>${steps}</div>`;
  return documentPage({ title: 'Подключить агента', description: 'MCP и HTTP Сот: доступ владельца, точные контракты приложений и проверка результата без раскрытия частных данных.',
    body, path: '/agents/connect', origin: base, maxBytes: DISCOVERY_HTML_LIMITS.connect });
}

function localeContent(locale, language) {
  exact(locale, ['title', 'summary', 'useWhen', 'notFor', 'examples'], INVALID);
  label(locale.title); label(locale.summary);
  const en = language === 'en';
  const headingTag = en ? 'h3' : 'h2';
  const section = (heading, values) => {
    assert(Array.isArray(values), INVALID); values.forEach(value => label(value));
    return values.length ? `<section><${headingTag}>${heading}</${headingTag}><ul>${values.map(value => `<li class="copy">${html(value)}</li>`).join('')}</ul></section>` : '';
  };
  assert(Array.isArray(locale.examples) && locale.examples.length <= 128, INVALID);
  const examples = locale.examples.map((example, index) => {
    exact(example, ['input', 'output'], INVALID);
    return `<article class="example"><h3>${en ? 'Example' : 'Пример'} ${index + 1}</h3><p class="example-label">${en ? 'Input' : 'Входные данные'}</p><pre><code>${html(JSON.stringify(example.input, null, 2))}</code></pre><p class="example-label">${en ? 'Illustrative result' : 'Иллюстрация результата'}</p><pre><code>${html(JSON.stringify(example.output, null, 2))}</code></pre></article>`;
  }).join('');
  return `<div class="scope-list">${section(en ? 'Use for' : 'Когда подходит', locale.useWhen)}${section(en ? 'Not for' : 'Для чего не подходит', locale.notFor)}</div>${examples ? `<section class="examples"><h2>${en ? 'Input and result examples' : 'Примеры данных'}</h2><p class="hint">${en ? 'Illustrations, not results of an executed call.' : 'Это иллюстрации, а не результаты выполненного вызова.'}</p>${examples}</section>` : ''}`;
}

export function renderCapabilityPage(options) {
  exact(options, ['detail', 'origin'], INVALID);
  const { detail, origin = '' } = options, base = originValue(origin);
  exact(detail, ['scope', 'capability', 'documentation', 'links'], INVALID); boundedJson(detail, DISCOVERY_LIMITS.detailBytes);
  const { capability, documentation } = detail;
  assert(detail.scope === 'public' && capability?.visibility === 'public' && typeof capability.executionEnabled === 'boolean', INVALID);
  const links = checkedLinks(detail.links, capability.capabilityId, capability.version);
  exact(documentation, ['revision', 'contractDigest', 'locales', 'validation'], INVALID);
  digest(capability.digest); digest(documentation?.revision); assert(documentation.contractDigest === capability.digest, INVALID);
  assert(documentation.validation && typeof documentation.validation === 'object', INVALID);
  assert(canonicalJson(documentation.validation) === canonicalJson(CAPABILITY_VALIDATION_PROFILE), INVALID);
  exact(documentation.locales, ['ru', 'en'], INVALID);
  const ru = documentation.locales.ru, en = documentation.locales.en;
  const ruContent = localeContent(ru, 'ru'), enContent = localeContent(en, 'en');
  const metadata = [['Приложение', capability.appId], ['Действия', capability.effects], ['Ресурсы', capability.resources], ['Получатели', capability.recipients]]
    .map(([name, value]) => { const values = Array.isArray(value) ? value : [value]; values.forEach(item => identifier(item, INVALID));
      return `<div><dt>${name}</dt><dd><code>${values.length ? html(values.join(', ')) : '—'}</code></dd></div>`; }).join('');
  const body = `<a class="text-link back" href="/agents">← Все возможности</a><section class="hero" aria-labelledby="page-title"><p class="eyebrow"><bdi>${html(capability.capabilityId)}</bdi> · Версия ${capability.version}</p><h1 id="page-title" class="copy">${html(ru.title)}</h1><p class="intro copy">${html(ru.summary)}</p><div class="readiness"><strong>${capability.executionEnabled ? 'Право вызова проверяется отдельно' : 'Описание опубликовано. Выполнение пока недоступно.'}</strong><p>${capability.executionEnabled ? 'Доступность обработчика не заменяет разрешение владельца.' : 'Можно изучить описание, примеры и схемы. Вызов этой возможности пока выключен.'}</p></div></section><div class="detail-grid"><div class="stack"><section class="panel" aria-label="Назначение и примеры">${ruContent}</section><details class="panel"><summary lang="en">English description and examples</summary><div class="details-body" lang="en"><h2 class="copy">${html(en.title)}</h2><p class="intro copy">${html(en.summary)}</p><div class="examples">${enContent}</div></div></details></div><aside class="stack" aria-label="Точный контракт"><section class="panel"><h2>Схемы и контракт</h2><p class="hint">Обычные JSON-документы для вашего клиента.</p><nav class="machine-links" aria-label="Документы возможности"><a class="button" href="${html(links.inputSchema)}">Схема входных данных</a><a class="button" href="${html(links.outputSchema)}">Схема результата</a><a class="button" href="${html(links.contract)}">Контракт для проверки SHA-256</a><a class="text-link" href="${html(links.detail)}">Полное описание JSON</a></nav><dl class="metadata">${metadata}<div><dt>SHA-256 контракта</dt><dd><code>${html(capability.digest)}</code></dd></div></dl><p class="hint">Сверяйте хеш байтов контракта, а не всего описания или отдельной схемы.</p></section><details class="panel"><summary>Ограничения текущего профиля</summary><div class="details-body"><p class="profile-copy">JSON Schema считает длину строки в символах Unicode. Текущий runtime дополнительно считает единицы UTF-16: например, 😀 занимает две. Также ограничены размер JSON и управляющие символы. Текст записки не нормализуется.</p><p class="profile-copy">Старый validator допускает одиночные surrogate escapes. Внешняя запись такого текста требует отдельного согласованного правила.</p><pre><code>${html(JSON.stringify(documentation.validation, null, 2))}</code></pre></div></details></aside></div>`;
  return documentPage({ title: ru.title, description: ru.summary, body, path: links.html, origin: base, maxBytes: DISCOVERY_HTML_LIMITS.detail });
}
