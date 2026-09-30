import { performance } from 'node:perf_hooks';

export class OAuthIngressError extends Error {
  constructor(code) { super(code); this.name = 'OAuthIngressError'; this.code = code; }
}
const check = (condition, code = 'invalid_request') => { if (!condition) throw new OAuthIngressError(code); };
export const OAUTH_HTTP_LIMITS = Object.freeze({
  bodyBytes: 16384, bodyTimeoutMs: 10000, requests: 16, requestsPerPeer: 4,
  attempts: 120, windowMs: 60000, peers: 2048, urlBytes: 8192,
});

export function oauthSingleHeader(req, name) {
  let value;
  for (let index = 0; index < req.rawHeaders.length; index += 2) {
    if (req.rawHeaders[index].toLowerCase() !== name) continue;
    check(value === undefined); value = req.rawHeaders[index + 1];
  }
  return value;
}

/** Strict form decoding at the HTTP boundary; OAuth semantics remain with the
 * maintained provider. In particular a duplicate resource is never collapsed. */
export function parseOAuthForm(text) {
  check(typeof text === 'string' && Buffer.byteLength(text) <= OAUTH_HTTP_LIMITS.bodyBytes);
  const result = Object.create(null);
  if (!text) return result;
  const fields = text.split('&'); check(fields.length <= 32);
  for (const field of fields) {
    const at = field.indexOf('='); check(at > 0);
    let key, value;
    try {
      key = decodeURIComponent(field.slice(0, at).replaceAll('+', ' '));
      value = decodeURIComponent(field.slice(at + 1).replaceAll('+', ' '));
    } catch { throw new OAuthIngressError('invalid_request'); }
    check(/^[A-Za-z][A-Za-z0-9_]{0,63}$/u.test(key) && !Object.hasOwn(result, key));
    check(!/[\u0000-\u001f\u007f]/u.test(value));
    result[key] = value;
  }
  return result;
}

function formHeaders(req, maximum) {
  const type = oauthSingleHeader(req, 'content-type');
  check(typeof type === 'string' && /^application\/x-www-form-urlencoded(?:\s*;\s*charset\s*=\s*(?:utf-8|"utf-8"))?\s*$/iu.test(type), 'unsupported_media_type');
  const encoding = oauthSingleHeader(req, 'content-encoding');
  check(encoding === undefined || encoding.toLowerCase() === 'identity', 'unsupported_encoding');
  const length = oauthSingleHeader(req, 'content-length'), transfer = oauthSingleHeader(req, 'transfer-encoding');
  check(transfer === undefined || transfer.toLowerCase() === 'chunked');
  check(transfer === undefined || length === undefined);
  if (length !== undefined) {
    check(/^(?:0|[1-9]\d*)$/u.test(length) && Number.isSafeInteger(Number(length)));
    check(Number(length) <= maximum, 'payload_too_large');
  }
}

/** One bounded form buffer and a lease lasting until downstream work settles.
 * The owner MUST release in finally after awaiting its entire handler (for the
 * Provider, public provider.use + await next()). Socket close is not completion
 * of async adapter work. The peer is never inferred from forwarding headers. */
export function createOAuthIngress({ limits = {} } = {}) {
  check(limits && typeof limits === 'object' && !Array.isArray(limits), 'oauth_configuration_invalid');
  check(Object.keys(limits).every(key => Object.hasOwn(OAUTH_HTTP_LIMITS, key)), 'oauth_configuration_invalid');
  const bounds = { ...OAUTH_HTTP_LIMITS, ...limits };
  for (const [key, value] of Object.entries(bounds)) {
    check(Number.isSafeInteger(value) && value > 0 && value <= OAUTH_HTTP_LIMITS[key], 'oauth_configuration_invalid');
  }
  const peers = new Map(); let active = 0;
  return Object.freeze({
    enter(req, res) {
      const target = req.originalUrl || req.url;
      check(typeof target === 'string' && Buffer.byteLength(target) <= bounds.urlBytes);
      const now = performance.now(), peer = req.socket?.remoteAddress;
      check(typeof peer === 'string' && peer.length > 0 && peer.length <= 128);
      for (const [key, item] of peers) if (item.active === 0 && now - item.since >= bounds.windowMs) peers.delete(key);
      let item = peers.get(peer);
      if (!item) {
        check(peers.size < bounds.peers, 'temporarily_unavailable');
        item = { since: now, attempts: 0, active: 0 }; peers.set(peer, item);
      } else if (now - item.since >= bounds.windowMs) { item.since = now; item.attempts = 0; }
      check(item.attempts < bounds.attempts, 'rate_limit'); item.attempts++;
      check(active < bounds.requests && item.active < bounds.requestsPerPeer, 'temporarily_unavailable');
      active++; item.active++;
      let released = false, reading = false;
      const release = () => {
        if (released) return;
        released = true; active--; item.active--;
      };
      return Object.freeze({
        release,
        readForm() {
          check(!released && !reading); reading = true;
          formHeaders(req, bounds.bodyBytes);
          return new Promise((resolve, reject) => {
            let settled = false, size = 0, buffer = Buffer.allocUnsafe(bounds.bodyBytes);
            const cleanup = () => {
              clearTimeout(timer); req.off('data', data); req.off('end', end);
              req.off('aborted', aborted); req.off('error', aborted); req.off('close', closed);
              res.off('close', aborted);
            };
            const finish = (error, value) => {
              if (settled) return;
              settled = true; cleanup(); buffer = null;
              if (error) {
                req.pause();
                const ignore = () => {};
                req.once('error', ignore); req.once('close', () => req.off('error', ignore));
                reject(error);
              } else resolve(value);
            };
            const aborted = () => finish(new OAuthIngressError('request_aborted'));
            const closed = () => { if (!req.complete) aborted(); };
            const data = chunk => {
              if (!Buffer.isBuffer(chunk)) { finish(new OAuthIngressError('invalid_request')); return; }
              if (chunk.length > bounds.bodyBytes - size) { finish(new OAuthIngressError('payload_too_large')); return; }
              chunk.copy(buffer, size); size += chunk.length;
            };
            const end = () => {
              try {
                const text = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(buffer.subarray(0, size));
                finish(null, parseOAuthForm(text));
              } catch (error) { finish(error instanceof OAuthIngressError ? error : new OAuthIngressError('invalid_request')); }
            };
            const timer = setTimeout(() => finish(new OAuthIngressError('request_timeout')), bounds.bodyTimeoutMs);
            req.on('data', data); req.once('end', end); req.once('aborted', aborted);
            req.once('error', aborted); req.once('close', closed); res.once('close', aborted);
            if (req.destroyed || req.aborted || res.destroyed) aborted();
          });
        },
      });
    },
  });
}
