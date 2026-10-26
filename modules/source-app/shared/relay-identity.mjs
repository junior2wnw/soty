import { createHash } from 'node:crypto';

// Root and the receiving server bind a proof to the same normalized request.
export function relayRequestDigest(method, path) {
  if (typeof method !== 'string' || !/^(?:GET|HEAD|POST|PUT|PATCH|DELETE|OPTIONS)$/u.test(method)
    || typeof path !== 'string' || path.length > 8192 || !path.startsWith('/') || path.startsWith('//')
    || /[\\\u0000-\u0020\u007f#]/u.test(path)) throw new TypeError('invalid_relay_identity_request');
  const url = new URL(path, 'https://soty-request.invalid');
  if (url.origin !== 'https://soty-request.invalid') throw new TypeError('invalid_relay_identity_request');
  return createHash('sha256').update(JSON.stringify([method, url.pathname + url.search])).digest('hex');
}
