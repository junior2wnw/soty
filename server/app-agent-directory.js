import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { singleHeader } from './capabilities-ingress.js';
import { createAppModuleCatalog } from './app-agent-directory-modules.js';

const source = path => fileURLToPath(new URL(path, import.meta.url));
const files = new Map([
  ['/agents/app-platform.md', ['../public/agents/app-platform.md', 'text/markdown; charset=utf-8']],
  ['/agents/glass-ui.md', ['../public/agents/glass-ui.md', 'text/markdown; charset=utf-8']],
  ['/agents/monitoring.md', ['../modules/monitoring/README.md', 'text/markdown; charset=utf-8']],
  ['/agents/sdk/monitoring/sdk.mjs', ['../modules/monitoring/sdk.mjs', 'text/javascript; charset=utf-8']],
  ['/agents/sdk/monitoring/sdk.d.mts', ['../modules/monitoring/sdk.d.mts', 'text/plain; charset=utf-8']],
  ['/agents/sdk/monitoring/manifest.json', ['../modules/monitoring/manifest.json', 'application/json; charset=utf-8']],
  ['/agents/sdk/monitoring/examples/instrumentation.mjs', ['../modules/monitoring/examples/instrumentation.mjs', 'text/javascript; charset=utf-8']],
  ...['definitions.mjs','catalog.mjs','descriptor.mjs','server/index.mjs','server/schema.mjs','server/admin.mjs','server/validation.mjs'].map(name=>['/agents/sdk/monitoring/'+name,['../modules/monitoring/'+name,'text/javascript; charset=utf-8']]),
  ['/agents/examples/notes-descriptor.mjs', ['../public/agents/examples/notes-descriptor.mjs', 'text/javascript; charset=utf-8']],
  ['/agents/examples/gallery-descriptor.mjs', ['../public/agents/examples/gallery-descriptor.mjs', 'text/javascript; charset=utf-8']],
  ['/agents/sdk/app-contract/sdk.mjs', ['../modules/app-contract/sdk.mjs', 'text/javascript; charset=utf-8']],
  ['/agents/sdk/app-contract/index.mjs', ['../modules/app-contract/index.mjs', 'text/javascript; charset=utf-8']],
  ['/agents/sdk/app-contract/json.mjs', ['../modules/app-contract/json.mjs', 'text/javascript; charset=utf-8']],
  ['/agents/sdk/capabilities/server/validation.mjs', ['../modules/capabilities/server/validation.mjs', 'text/javascript; charset=utf-8']],
  ...['ui-kit.js','site.js','liquid-glass.js','widget-context.js'].map(name=>['/agents/sdk/glass-ui/src/'+name,['../vendor/optical-glass/src/'+name,'text/javascript; charset=utf-8']]),
  ['/agents/sdk/glass-ui/src/widget-context.d.ts',['../vendor/optical-glass/src/widget-context.d.ts','text/plain; charset=utf-8']],
  ...['ui-kit.css','design-system.css'].map(name=>['/agents/sdk/glass-ui/src/'+name,['../vendor/optical-glass/src/'+name,'text/css; charset=utf-8']]),
]);
const escape = value => String(value).replace(/[&<>"']/gu, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char]);
const namespace = path => path === '/.well-known/soty-app-platform' || path === '/agents/apps' || files.has(path) || path.startsWith('/agents/sdk/glass-ui/')
  || path === '/api/app-agent-directory' || path.startsWith('/api/app-agent-directory/');

/** Install before attachCapabilitiesDiscovery, which deliberately owns the rest of /agents. */
export function attachAppAgentDirectory(app, { directory, origin = '', moduleCatalog = createAppModuleCatalog(), authenticate } = {}) {
  const links = { schema: 'soty.app-platform-discovery.v1', documentation: '/agents/app-platform.md', apps: '/api/app-agent-directory/apps',
    modules: '/api/app-agent-directory/modules', ui: '/agents/apps', rootMcp: '/mcp',
    oauth: '/.well-known/oauth-protected-resource/mcp', sourceSdk: '/sdk/source-app/index.json',
    descriptorSdk: { entry: '/agents/sdk/app-contract/sdk.mjs', files: [...files.keys()].filter(path => path.startsWith('/agents/sdk/app-contract/') || path === '/agents/sdk/capabilities/server/validation.mjs') },
    glassUi:{documentation:'/agents/glass-ui.md',entry:'/agents/sdk/glass-ui/src/ui-kit.js',styles:'/agents/sdk/glass-ui/src/ui-kit.css',widgetContext:'/agents/sdk/glass-ui/src/widget-context.js',
      files:[...files.keys()].filter(path=>path.startsWith('/agents/sdk/glass-ui/'))},
    monitoring:{documentation:'/agents/monitoring.md',manifest:'/agents/sdk/monitoring/manifest.json',entry:'/agents/sdk/monitoring/sdk.mjs',server:'/agents/sdk/monitoring/server/index.mjs',
      files:[...files.keys()].filter(path=>path.startsWith('/agents/sdk/monitoring/'))},
    examples: ['/agents/examples/notes-descriptor.mjs', '/agents/examples/gallery-descriptor.mjs'],
    authority: 'Discovery metadata is not a grant. App execution needs a separate current credential and host-owned binding.' };
  app.use((req, res, next) => {
    const target = req.originalUrl || req.url || '', question = target.indexOf('?'), path = question < 0 ? target : target.slice(0, question);
    if (path === '/' && ['GET', 'HEAD'].includes(req.method))
      res.append('Link', '</.well-known/soty-app-platform>; rel="service-desc"; type="application/json", </agents/apps>; rel="alternate"; type="text/html"');
    if (!namespace(path)) { next(); return; }
    const send = (value, type = 'application/json; charset=utf-8', status = 200) => {
      const body = Buffer.isBuffer(value) ? value : Buffer.from(type.startsWith('application/json') ? JSON.stringify(value) : value);
      if (body.length > 2 * 1024 * 1024) throw Object.assign(new Error('directory_response_limit'), { code: 'directory_response_limit', status: 503 });
      res.status(status).set({ 'Content-Type': type, 'Content-Length': String(body.length) }).end(req.method === 'HEAD' ? undefined : body);
    };
    res.set({ 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'no-referrer',
      'Access-Control-Allow-Origin': '*', 'Vary': 'Authorization',
      'Content-Security-Policy': "default-src 'none'; base-uri 'none'; frame-ancestors 'none'" });
    try {
      if (!['GET', 'HEAD'].includes(req.method)) { res.set('Allow', 'GET, HEAD'); send({ error: { code: 'method_not_allowed' } }, undefined, 405); return; }
      if (path.includes('%') || path.includes('\\')) throw Object.assign(new Error('directory_invalid_input'), { code: 'directory_invalid_input' });
      const params = new URLSearchParams(question < 0 ? '' : target.slice(question + 1));
      const args = {};
      for (const [key, value] of params) {
        if (!['query', 'limit', 'cursor'].includes(key) || Object.hasOwn(args, key)) throw Object.assign(new Error('directory_invalid_input'), { code: 'directory_invalid_input' });
        args[key] = value;
      }
      if (args.limit !== undefined) {
        if (!/^[1-9]\d?$/u.test(args.limit)) throw Object.assign(new Error('directory_invalid_input'), { code: 'directory_invalid_input' });
        args.limit = Number(args.limit);
      }
      if (!['/api/app-agent-directory/apps', '/agents/apps'].includes(path) && Object.keys(args).length)
        throw Object.assign(new Error('directory_invalid_input'), { code: 'directory_invalid_input' });
      let actor;
      if (singleHeader(req, 'authorization') !== undefined) {
        if (typeof authenticate !== 'function') throw Object.assign(new Error('directory_authentication_required'), { code: 'directory_authentication_required', status: 401 });
        actor = authenticate(req);
        if (!actor || typeof actor.then === 'function') throw Object.assign(new Error('directory_authentication_required'), { code: 'directory_authentication_required', status: 401 });
      }
      if (path === '/.well-known/soty-app-platform' || path === '/api/app-agent-directory') { send({ ...links, ...(origin ? { origin } : {}) }); return; }
      if (path === '/api/app-agent-directory/modules') { send(moduleCatalog.list()); return; }
      if (files.has(path)) { const [file, type] = files.get(path); send(readFileSync(source(file)), type); return; }
      if (path === '/api/app-agent-directory/apps') { send(directory.list({ ...args, actor })); return; }
      const match = /^\/api\/app-agent-directory\/apps\/([A-Za-z0-9][A-Za-z0-9._:-]{0,127})$/u.exec(path);
      if (match) { send(directory.get({ appId: match[1], actor })); return; }
      if (path !== '/agents/apps') { send({ error: { code: 'directory_not_found' } }, undefined, 404); return; }
      const catalog = directory.list({ ...args, actor });
      const items = catalog.items.map(item => `<li><a href="/api/app-agent-directory/apps/${escape(item.appId)}">${escape(item.title)}</a><ul>${item.capabilities.map(capability =>
        `<li>${escape(capability.title || capability.id)} · ${capability.executionReady ? 'отдельное разрешение проверено' : 'описание; запуск отдельно'}</li>`).join('')}</ul></li>`).join('');
      send(`<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Соты · приложения для агентов</title><main><h1>Приложения Сот</h1><p><a href="/agents/app-platform.md">SDK и подключение</a> · <a href="/api/app-agent-directory/modules">Модули</a> · <a href="/api/app-agent-directory/apps">JSON каталог</a> · <a href="/agents">Root capabilities</a></p><p>Описание возможностей не выдаёт права на выполнение. Источник и доступ проверяются при каждом обращении.</p><ul>${items || '<li>Пока нет опубликованных владельцами описаний приложений.</li>'}</ul></main></html>`, 'text/html; charset=utf-8');
    } catch (error) {
      const status = [401, 403, 404, 409, 429, 503].includes(error?.status) ? error.status : 400;
      const code = typeof error?.code === 'string' && /^[a-z_]{1,96}$/u.test(error.code) ? error.code : 'directory_unavailable';
      if (status === 401) res.set('WWW-Authenticate', 'Bearer realm="soty"');
      send({ error: { code } }, undefined, status);
    }
  });
  return Object.freeze({ links, moduleCatalog });
}
