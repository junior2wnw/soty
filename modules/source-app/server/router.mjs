import { STANDARD_SOURCE_CONTRACT } from './standard-profile.mjs';
import { check } from './wire.mjs';

/** Source closes its own wire before proof/operation dispatch. Root's compiled
 * adapter remains a second boundary, not a substitute for this router. */
export function sourceEmbedRoute(req, url) {
  check(typeof req.url === 'string' && req.url.startsWith('/') && !req.url.startsWith('//')
    && req.url === url.pathname + url.search && !url.hash && !url.pathname.includes('%') && !url.pathname.includes('..'), 'source_app_route_unavailable', 404);
  if (url.pathname === '/api/embed/transport-ready') {
    check(req.method === 'HEAD' && !url.search, 'source_app_route_unavailable', 404);
    return Object.freeze({ requestBytes: 0, responseBytes: 128 });
  }
  const route = STANDARD_SOURCE_CONTRACT.rules.find(row => row.path === url.pathname && row.methods.includes(req.method));
  check(route, 'source_app_route_unavailable', 404);
  check([...url.searchParams].every(([key, value]) => route.queries.includes(key) && url.searchParams.getAll(key).length === 1
    && value.length <= 4096 && value.isWellFormed() && !/[\u0000-\u001f\u007f-\u009f]/u.test(value)), 'source_app_input_invalid', 400);
  return route;
}
export function sourceBodyHeaders(req, limit) {
  const length = req.headers['content-length'];
  check(length === undefined || typeof length === 'string' && /^(?:0|[1-9][0-9]{0,7})$/u.test(length)
    && Number(length) <= limit, 'source_app_payload_limit', 413);
  check(req.headers['content-encoding'] === undefined || req.headers['content-encoding'] === 'identity', 'source_app_content_type_invalid', 415);
  if (['GET', 'HEAD'].includes(req.method)) {
    check((length === undefined || length === '0') && req.headers['transfer-encoding'] === undefined, 'source_app_body_invalid', 400); return;
  }
  check(typeof req.headers['content-type'] === 'string' && /^application\/json(?:\s*;\s*charset=utf-8)?$/iu.test(req.headers['content-type']),
    'source_app_content_type_invalid', 415);
}
