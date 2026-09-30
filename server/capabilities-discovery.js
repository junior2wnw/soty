import { createHash } from 'node:crypto';
import { AccessError, assert } from '../modules/capabilities/server/validation.mjs';
import { DISCOVERY_LIMITS } from '../modules/capabilities/server/discovery.mjs';
import { buildDiscoveryOpenApi } from '../modules/capabilities/server/openapi.mjs';
import { renderDiscoveryIndex, renderCapabilityPage } from '../modules/capabilities/server/discovery-pages.mjs';

const API = '/api/capabilities';
const BASE = `${API}/v1`;
const TARGET_BYTES = 8192;
const JSON_TYPE = 'application/json; charset=utf-8';
const HTML_TYPE = 'text/html; charset=utf-8';
const inNamespace = (pathname, prefix) => pathname === prefix || pathname.startsWith(`${prefix}/`);
const xml = value => value.replace(/[&<>"']/gu, character => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&apos;' })[character]);

/** Only explicit deployment configuration can name the public canonical host. */
export function validateDiscoveryOrigin({ discoveryOrigin = '', shellOrigins = [] } = {}) {
  if (discoveryOrigin === '') return '';
  const parse = value => {
    assert(typeof value === 'string' && value.length <= 512 && !/[\s\\%]/u.test(value)
      && /^https?:\/\/[^/?#]+\/?$/iu.test(value), 'discovery_origin_invalid');
    let url;
    try { url = new URL(value); } catch { throw new AccessError('discovery_origin_invalid'); }
    assert(['https:', 'http:'].includes(url.protocol) && !url.username && !url.password && url.pathname === '/'
      && !url.search && !url.hash && !url.hostname.endsWith('.'), 'discovery_origin_invalid');
    return url;
  };
  const url = parse(discoveryOrigin);
  // Match the existing Connect origin contract and the documentation renderer.
  const allowedTransport = value => value.protocol === 'https:'
    || value.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(value.hostname);
  assert(allowedTransport(url), 'discovery_origin_https_required');
  assert(Array.isArray(shellOrigins), 'discovery_origin_shell_required');
  const configured = shellOrigins.map(value => {
    const candidate = parse(value);
    assert(candidate.origin === value && allowedTransport(candidate), 'discovery_origin_shell_required');
    return candidate.origin;
  });
  assert(configured.includes(url.origin), 'discovery_origin_shell_required');
  return url.origin;
}

function decode(value, code) {
  try { return decodeURIComponent(value); } catch { throw new AccessError(code); }
}

function searchArgs(raw, allowed) {
  const args = Object.create(null);
  if (raw === '') return args;
  for (const pair of raw.split('&')) {
    const at = pair.indexOf('='), name = decode((at < 0 ? pair : pair.slice(0, at)).replaceAll('+', ' '), 'query_invalid');
    const value = decode((at < 0 ? '' : pair.slice(at + 1)).replaceAll('+', ' '), 'query_invalid');
    assert(allowed.includes(name) && !Object.hasOwn(args, name), 'query_invalid');
    args[name] = value;
  }
  if (Object.hasOwn(args, 'limit')) {
    assert(/^(?:[1-9]|1[0-9]|20)$/u.test(args.limit), 'invalid_input');
    args.limit = Number(args.limit);
  }
  return args;
}

function identify(pathname, segments) {
  if (pathname === `${BASE}/catalog`) return { kind: 'search' };
  if (pathname === `${BASE}/openapi.json`) return { kind: 'openapi' };
  if (pathname === `${BASE}/status`) return { kind: 'status' };
  if (pathname === '/agents') return { kind: 'index' };
  if (pathname === '/agents/sitemap.xml') return { kind: 'sitemap' };
  const api = /^\/api\/capabilities\/v1\/catalog\/([^/]+)\/versions\/([^/]+)(?:\/(contract\.json|schemas\/(input|output)))?$/u.exec(pathname);
  const page = /^\/agents\/capabilities\/([^/]+)\/versions\/([^/]+)$/u.exec(pathname);
  const match = api || page;
  if (!match) return null;
  assert(/^[1-9]\d{0,6}$/u.test(match[2]) && Number(match[2]) <= 1000000, 'invalid_input');
  return { kind: page ? 'detail-page' : api[4] ? 'schema' : api[3] ? 'contract' : 'detail',
    selection: { capabilityId: segments[api ? 5 : 3], version: Number(match[2]) }, schemaKind: api?.[4] };
}

// Parse opaque entity tags without splitting commas that occur inside a tag.
// Discovery emits a strong SHA-256 tag; GET/HEAD validators use weak comparison.
function matchesEtag(header, expected) {
  if (typeof header !== 'string') return false;
  if (header.trim() === '*') return true;
  const tag = /(?:W\/)?"[\x21\x23-\x7e\x80-\xff]*"/y;
  let at = 0, matches = false;
  while (at < header.length) {
    while (at < header.length && /[\t ,]/u.test(header[at])) at++;
    if (at === header.length) break;
    tag.lastIndex = at;
    const next = tag.exec(header);
    if (!next) return false;
    matches ||= next[0].replace(/^W\//u, '') === expected;
    at = tag.lastIndex;
    while (at < header.length && /[\t ]/u.test(header[at])) at++;
    if (at < header.length && header[at++] !== ',') return false;
  }
  return matches;
}

function send(req, res, body, type, { status = 200, limit = 512 * 1024, cache = true } = {}) {
  const bytes = Buffer.from(body, 'utf8');
  assert(bytes.length <= limit, 'projection_too_large');
  res.status(status).set({ 'Content-Type': type, 'Cache-Control': cache ? 'public,no-cache' : 'no-store',
    'Content-Length': String(bytes.length), 'X-Content-Type-Options': 'nosniff' });
  if (cache) {
    const etag = `"${createHash('sha256').update(bytes).digest('hex')}"`;
    res.set('ETag', etag);
    if (matchesEtag(req.headers['if-none-match'], etag)) { res.status(304).end(); return; }
  }
  res.end(req.method === 'HEAD' ? undefined : bytes);
}

function sitemap(catalog, origin) {
  assert(origin, 'not_found');
  const urls = [`${origin}/agents`];
  let cursor;
  do {
    const page = catalog.search({ limit: DISCOVERY_LIMITS.maxPage, ...(cursor ? { cursor } : {}) });
    for (const item of page.items) urls.push(`${origin}${item.links.html}`);
    assert(urls.length <= DISCOVERY_LIMITS.versions + 1, 'projection_too_large');
    assert(!page.cursor || page.items.length > 0, 'projection_too_large');
    cursor = page.cursor;
  } while (cursor);
  return `<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">${urls.map(url => `<url><loc>${xml(url)}</loc></url>`).join('')}</urlset>`;
}

/** Read-only public transport. It neither authenticates nor executes an action. */
export function attachCapabilitiesDiscovery(app, { catalog, origin = '', status = () => ({ notesCreateEnabled: false, audience: null }),
  openApi = buildDiscoveryOpenApi() }) {
  const openapi = JSON.stringify(openApi);
  app.use((req, res, next) => {
    const target = req.originalUrl || req.url;
    const split = target.indexOf('?'), pathname = split < 0 ? target : target.slice(0, split);
    const api = inNamespace(pathname, API), html = inNamespace(pathname, '/agents');
    if (!api && !html) { next(); return; }
    if (api) res.set('Access-Control-Allow-Origin', '*');
    if (html) res.set('Content-Security-Policy', "default-src 'none'; base-uri 'none'; object-src 'none'; script-src 'self'; style-src 'unsafe-inline'; img-src 'self' data:; frame-ancestors 'none'; form-action 'self'");
    try {
      assert(Buffer.byteLength(target, 'utf8') <= TARGET_BYTES, 'uri_too_long');
      assert(!/[^\u0021-\u007e]|[#\\]/u.test(target), 'invalid_input');
      // Reject malformed encoding even in an unknown path. Only the ID is
      // subsequently interpreted, once; encoded separators stay one segment.
      const segments = pathname.split('/').map(segment => decode(segment, 'invalid_input'));
      const route = identify(pathname, segments);
      assert(route, 'not_found');
      if (!['GET', 'HEAD'].includes(req.method)) {
        res.set('Allow', 'GET, HEAD');
        send(req, res, '{"error":{"code":"method_not_allowed"}}', JSON_TYPE, { status: 405, cache: false }); return;
      }
      const args = searchArgs(split < 0 ? '' : target.slice(split + 1), ['search', 'index'].includes(route.kind) ? ['query', 'limit', 'cursor'] : []);
      const json = (value, limit) => send(req, res, JSON.stringify(value), JSON_TYPE, { limit });
      switch (route.kind) {
        case 'search': json(catalog.search(args), DISCOVERY_LIMITS.pageBytes); break;
        case 'detail': json(catalog.get(route.selection), DISCOVERY_LIMITS.detailBytes); break;
        case 'contract': send(req, res, catalog.contract(route.selection).canonicalJson, JSON_TYPE, { limit: 262144 }); break;
        case 'schema': json(catalog.schema({ ...route.selection, kind: route.schemaKind }), 262144); break;
        case 'openapi': send(req, res, openapi, JSON_TYPE, { limit: 256 * 1024 }); break;
        case 'status': send(req, res, JSON.stringify(status()), JSON_TYPE, { limit: 4096, cache: false }); break;
        case 'index': {
          const result = catalog.search(args), noindex = split >= 0;
          if (noindex) res.set('X-Robots-Tag', 'noindex, follow');
          send(req, res, renderDiscoveryIndex({ result, query: args.query || '', limit: args.limit || DISCOVERY_LIMITS.defaultPage, origin, noindex }), HTML_TYPE, { limit: 256 * 1024 }); break;
        }
        case 'detail-page': send(req, res, renderCapabilityPage({ detail: catalog.get(route.selection), origin }), HTML_TYPE); break;
        case 'sitemap': send(req, res, sitemap(catalog, origin), 'application/xml; charset=utf-8', { limit: 128 * 1024 }); break;
      }
    } catch (error) {
      const codes = { invalid_input: 400, query_invalid: 400, cursor_invalid: 400, not_found: 404, uri_too_long: 414, projection_too_large: 500 };
      const code = error instanceof AccessError && Object.hasOwn(codes, error.code) ? error.code : 'internal_error';
      send(req, res, JSON.stringify({ error: { code } }), JSON_TYPE, { status: codes[code] || 500, cache: false });
    }
  });
}
