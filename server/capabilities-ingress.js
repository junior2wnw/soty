import { performance } from 'node:perf_hooks';
import { markHttpSocketClosing } from './http-closing-socket.mjs';

export class CapabilityHttpError extends Error {
  constructor(code) { super(code); this.name = 'CapabilityHttpError'; this.code = code; }
}
const requireValue = (value, code) => { if (!value) throw new CapabilityHttpError(code); };
export const NATIVE_HTTP_LIMITS = Object.freeze({
  bodyBytes: 2 * 1024 * 1024, bodyTimeoutMs: 15000,
  readers: 8, readersPerPeer: 2, attempts: 60, windowMs: 60000, peers: 2048,
});

/** Fixed flat scalar objects only. The scan rejects decoded duplicate keys
 * before JSON.parse could discard them; no nested client structure is read. */
function parseFlatScalarJson(text, fields) {
  requireValue(typeof text === 'string', 'invalid_input');
  let at = 0;
  const whitespace = () => { while (at < text.length && ' \t\n\r'.includes(text[at])) at++; };
  const string = () => {
    requireValue(text[at] === '"', 'invalid_input');
    const start = at++;
    while (at < text.length) {
      const char = text[at++];
      if (char === '\\') { at++; continue; }
      if (char === '"') {
        try { return JSON.parse(text.slice(start, at)); }
        catch { throw new CapabilityHttpError('invalid_input'); }
      }
    }
    throw new CapabilityHttpError('invalid_input');
  };
  whitespace(); requireValue(text[at++] === '{', 'invalid_input'); whitespace();
  const result = Object.create(null);
  const fieldCount = Object.keys(fields).length;
  for (let count = 0; count < fieldCount; count++) {
    const key = string();
    requireValue(Object.hasOwn(fields, key) && !Object.hasOwn(result, key), 'invalid_input');
    whitespace(); requireValue(text[at++] === ':', 'invalid_input'); whitespace();
    if (fields[key] === 'string') result[key] = string();
    else {
      const start = at;
      while (at < text.length && '0123456789eE+-.'.includes(text[at])) at++;
      try { result[key] = JSON.parse(text.slice(start, at)); }
      catch { throw new CapabilityHttpError('invalid_input'); }
      requireValue(Number.isSafeInteger(result[key]) && result[key] >= 0, 'invalid_input');
    }
    whitespace();
    if (text[at] === '}') break;
    requireValue(text[at++] === ',' && count < fieldCount - 1, 'invalid_input'); whitespace();
  }
  requireValue(text[at++] === '}' && Object.keys(result).length === fieldCount, 'invalid_input');
  whitespace(); requireValue(at === text.length, 'invalid_input');
  return result;
}

const DRAFT_FIELDS = Object.freeze({ title: 'string', body: 'string', idempotencyKey: 'string' });
const DELEGATION_FIELDS = Object.freeze({ label: 'string', expiresAt: 'integer' });
export const parseNativeDraftJson = text => parseFlatScalarJson(text, DRAFT_FIELDS);
export const parseDelegationJson = text => parseFlatScalarJson(text, DELEGATION_FIELDS);

export function singleHeader(req, name) {
  let value;
  for (let index = 0; index < req.rawHeaders.length; index += 2) {
    if (req.rawHeaders[index].toLowerCase() !== name) continue;
    requireValue(value === undefined, name === 'authorization' ? 'authorization_required' : 'invalid_input');
    value = req.rawHeaders[index + 1];
  }
  return value;
}

export function nativeBodyHeaders(req, maximum) {
  const contentType = singleHeader(req, 'content-type');
  requireValue(typeof contentType === 'string' && /^application\/json(?:\s*;\s*charset\s*=\s*(?:utf-8|"utf-8"))?\s*$/iu.test(contentType), 'unsupported_media_type');
  const encoding = singleHeader(req, 'content-encoding');
  requireValue(encoding === undefined || encoding.toLowerCase() === 'identity', 'unsupported_encoding');
  const length = singleHeader(req, 'content-length');
  const transfer = singleHeader(req, 'transfer-encoding');
  requireValue(transfer === undefined || transfer.toLowerCase() === 'chunked', 'invalid_input');
  requireValue(transfer === undefined || length === undefined, 'invalid_input');
  if (length !== undefined) {
    requireValue(/^(?:0|[1-9]\d*)$/u.test(length) && Number.isSafeInteger(Number(length)), 'invalid_input');
    requireValue(Number(length) <= maximum, 'payload_too_large');
  }
}

/** Bounded process-local ingress. Socket peers are never inferred from proxy
 * headers. The domain separately owns durable account/principal admission. */
export function createNativeIngress({ limits = {} } = {}) {
  requireValue(limits && typeof limits === 'object' && !Array.isArray(limits), 'ingress_configuration_invalid');
  requireValue(Object.keys(limits).every(key => Object.hasOwn(NATIVE_HTTP_LIMITS, key)), 'ingress_configuration_invalid');
  const bounds = { ...NATIVE_HTTP_LIMITS, ...limits };
  for (const [key, value] of Object.entries(bounds)) {
    requireValue(Number.isSafeInteger(value) && value > 0 && value <= NATIVE_HTTP_LIMITS[key], 'ingress_configuration_invalid');
  }
  const peers = new Map();
  let readers = 0;
  function enter(req) {
    const timestamp = performance.now(), peer = req.socket?.remoteAddress;
    requireValue(typeof peer === 'string' && peer.length > 0 && peer.length <= 128, 'invalid_input');
    for (const [key, value] of peers) {
      if (value.active === 0 && timestamp - value.since >= bounds.windowMs) peers.delete(key);
    }
    let entry = peers.get(peer);
    if (!entry) {
      requireValue(peers.size < bounds.peers, 'ingress_capacity');
      entry = { since: timestamp, attempts: 0, active: 0 }; peers.set(peer, entry);
    } else if (timestamp - entry.since >= bounds.windowMs) {
      entry.since = timestamp; entry.attempts = 0;
    }
    requireValue(entry.attempts < bounds.attempts, 'ingress_rate_limit');
    entry.attempts++;
    requireValue(readers < bounds.readers && entry.active < bounds.readersPerPeer, 'ingress_capacity');
    readers++; entry.active++;
    let released = false, reading = false;
    const release = () => {
      if (released) return;
      released = true; readers--; entry.active--;
    };
    return Object.freeze({
      release,
      async read(req, { parseJson = parseNativeDraftJson, maximumBytes = bounds.bodyBytes } = {}) {
        requireValue(!released && !reading, 'invalid_input'); reading = true;
        let bodyBytes;
        try {
          requireValue(typeof parseJson === 'function' && Number.isSafeInteger(maximumBytes)
            && maximumBytes > 0 && maximumBytes <= NATIVE_HTTP_LIMITS.bodyBytes, 'ingress_configuration_invalid');
          bodyBytes = Math.min(maximumBytes, bounds.bodyBytes);
          nativeBodyHeaders(req, bodyBytes);
        }
        catch (error) {
          // Synchronous fence precedes Promise rejection and another parser
          // request/upgrade from the same buffered TCP input.
          if (error instanceof CapabilityHttpError && error.code === 'payload_too_large') markHttpSocketClosing(req.socket);
          release(); throw error;
        }
        return new Promise((resolve, reject) => {
          let settled = false, bytes = 0;
          // One byte-bounded allocation also bounds object overhead when a
          // sender splits the request into millions of tiny HTTP chunks.
          let buffer = Buffer.allocUnsafe(bodyBytes);
          const cleanup = () => {
            clearTimeout(timer);
            req.off('data', data); req.off('end', end); req.off('aborted', aborted);
            req.off('error', aborted); req.off('close', closed);
          };
          const finish = (error, value) => {
            if (settled) return;
            settled = true; cleanup(); release();
            if (error) {
              if (error instanceof CapabilityHttpError && error.code === 'payload_too_large') markHttpSocketClosing(req.socket);
              buffer = null; req.pause();
              // An aborted IncomingMessage can emit its terminal error after
              // the aborted event; consume that event without processing data.
              const ignore = () => {};
              req.once('error', ignore); req.once('close', () => req.off('error', ignore));
              reject(error);
            }
            else resolve(value);
          };
          const aborted = () => finish(new CapabilityHttpError('request_aborted'));
          const closed = () => { if (!req.complete) aborted(); };
          const data = chunk => {
            if (bytes + chunk.length > bodyBytes) { finish(new CapabilityHttpError('payload_too_large')); return; }
            chunk.copy(buffer, bytes); bytes += chunk.length;
          };
          const end = () => {
            try {
              // Preserve BOM as a character, so the strict JSON reader rejects
              // it instead of silently rewriting the submitted document.
              const text = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(buffer.subarray(0, bytes));
              buffer = null;
              finish(null, parseJson(text));
            } catch (error) {
              finish(error instanceof CapabilityHttpError ? error : new CapabilityHttpError('invalid_input'));
            }
          };
          const timer = setTimeout(() => finish(new CapabilityHttpError('request_timeout')), bounds.bodyTimeoutMs);
          req.on('data', data); req.once('end', end); req.once('aborted', aborted);
          req.once('error', aborted); req.once('close', closed);
          if (req.destroyed || req.aborted) aborted();
        });
      },
    });
  }
  return Object.freeze({ enter });
}
