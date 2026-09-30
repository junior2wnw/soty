import { performance } from 'node:perf_hooks';
import { CapabilityHttpError, singleHeader, nativeBodyHeaders } from './capabilities-ingress.js';

export const MCP_LIMITS = Object.freeze({ bodyBytes: 2097152, outputBytes: 2097152, bodyTimeoutMs: 15000,
  responseTimeoutMs: 15000, readers: 8, readersPerPeer: 2, attempts: 60, windowMs: 60000, peers: 2048,
  depth: 20, nodes: 10000, metadataBytes: 16384 });

export class McpIngressError extends CapabilityHttpError {
  constructor(code, protocolCode = null) { super(code); this.protocolCode = protocolCode; }
}
const check = (condition, code = 'invalid_input', protocolCode = -32600) => {
  if (!condition) throw new McpIngressError(code, protocolCode);
};

/** Grammar/complexity guard, not an MCP codec. Decode keys before checking
 * duplicates, then let JSON.parse produce the one admitted plain value. */
export function parseMcpJson(text, limits = MCP_LIMITS) {
  let at = 0, nodes = 0;
  const space = () => { while (at < text.length && ' \t\n\r'.includes(text[at])) at++; };
  function string() {
    check(text[at] === '"', 'invalid_input', -32700);
    const start = at++;
    while (at < text.length) {
      const char = text[at++];
      if (char === '\\') { at++; continue; }
      if (char === '"') return [start, at];
    }
    throw new McpIngressError('invalid_input', -32700);
  }
  function value(depth) {
    check(depth <= limits.depth && ++nodes <= limits.nodes, 'payload_too_large');
    space();
    if (text[at] === '{') {
      at++; space(); const keys = new Set();
      if (text[at] === '}') { at++; return; }
      while (at < text.length) {
        const [start, end] = string(); let key;
        try { key = JSON.parse(text.slice(start, end)); } catch { throw new McpIngressError('invalid_input', -32700); }
        check(!keys.has(key), 'invalid_input', -32700); keys.add(key);
        check(++nodes <= limits.nodes, 'payload_too_large');
        space(); check(text[at++] === ':', 'invalid_input', -32700);
        const begin = at; value(depth + 1);
        if (key === '_meta') check(Buffer.byteLength(text.slice(begin, at)) <= limits.metadataBytes, 'payload_too_large');
        space(); if (text[at] === '}') { at++; return; }
        check(text[at++] === ',', 'invalid_input', -32700); space();
      }
    } else if (text[at] === '[') {
      at++; space(); if (text[at] === ']') { at++; return; }
      while (at < text.length) {
        value(depth + 1); space(); if (text[at] === ']') { at++; return; }
        check(text[at++] === ',', 'invalid_input', -32700); space();
      }
    } else if (text[at] === '"') { string(); return; }
    else {
      const start = at;
      while (at < text.length && !' \t\n\r,]}'.includes(text[at])) at++;
      check(at > start, 'invalid_input', -32700); return;
    }
    throw new McpIngressError('invalid_input', -32700);
  }
  value(0); space(); check(at === text.length, 'invalid_input', -32700);
  let result;
  try { result = JSON.parse(text); } catch { throw new McpIngressError('invalid_input', -32700); }
  check(result && typeof result === 'object' && !Array.isArray(result)
    && result.jsonrpc === '2.0' && typeof result.method === 'string');
  if (Object.hasOwn(result, 'id')) check(Number.isSafeInteger(result.id)
    || typeof result.id === 'string' && result.id.length <= 160);
  return result;
}

export function normalizeMcpLimits(options = {}) {
  check(options && typeof options === 'object' && !Array.isArray(options), 'ingress_configuration_invalid');
  check(Object.keys(options).every(key => Object.hasOwn(MCP_LIMITS, key)), 'ingress_configuration_invalid');
  const limits = { ...MCP_LIMITS, ...options };
  for (const [key, value] of Object.entries(limits)) check(Number.isSafeInteger(value) && value > 0
    && value <= MCP_LIMITS[key], 'ingress_configuration_invalid');
  return Object.freeze(limits);
}

/** One lease spans body, SDK response collection, authority recheck and wire
 * completion. Only socket peers count; forwarded headers never supply a peer. */
export function createMcpIngress(options = {}) {
  const limits = normalizeMcpLimits(options), peers = new Map(), active = new Set();
  let closed = false;
  function enter(req, res) {
    check(!closed, 'service_closed');
    const now = performance.now(), peer = req.socket?.remoteAddress;
    check(typeof peer === 'string' && peer.length > 0 && peer.length <= 128);
    for (const [id, entry] of peers) if (entry.active === 0 && now - entry.since >= limits.windowMs) peers.delete(id);
    let entry = peers.get(peer);
    if (!entry) {
      check(peers.size < limits.peers, 'ingress_capacity');
      entry = { since: now, attempts: 0, active: 0 }; peers.set(peer, entry);
    } else if (now - entry.since >= limits.windowMs) { entry.since = now; entry.attempts = 0; }
    check(entry.attempts < limits.attempts, 'ingress_rate_limit'); entry.attempts++;
    check(active.size < limits.readers && entry.active < limits.readersPerPeer, 'ingress_capacity');
    const controller = new AbortController();
    let released = false, readStarted = false, responseTimer;
    const stop = (code = 'request_aborted') => {
      if (!controller.signal.aborted) controller.abort(new McpIngressError(code));
    };
    const aborted = () => stop(), requestClosed = () => { if (!req.complete) stop(); };
    const responseClosed = () => { if (!res.writableFinished) stop(); };
    function release() {
      if (released) return;
      released = true; clearTimeout(responseTimer); active.delete(stop); entry.active--;
      req.off('aborted', aborted); req.off('error', aborted); req.off('close', requestClosed);
      res.off('close', responseClosed);
    }
    entry.active++; active.add(stop);
    req.once('aborted', aborted); req.once('error', aborted); req.once('close', requestClosed);
    res.once('close', responseClosed);
    const current = () => {
      if (controller.signal.aborted) throw controller.signal.reason;
      check(!released && !res.destroyed, 'request_aborted');
    };
    return Object.freeze({ signal: controller.signal, stop, release, current,
      async read() {
        current(); check(!readStarted); readStarted = true;
        nativeBodyHeaders(req, limits.bodyBytes);
        return new Promise((resolve, reject) => {
          let buffer = Buffer.allocUnsafe(limits.bodyBytes), used = 0, settled = false;
          const cleanup = () => {
            clearTimeout(timer); req.off('data', data); req.off('end', end);
            controller.signal.removeEventListener('abort', onAbort);
          };
          const finish = (error, result) => {
            if (settled) return; settled = true; cleanup(); buffer = null;
            if (error) { req.pause(); reject(error); } else resolve(result);
          };
          const onAbort = () => finish(controller.signal.reason);
          const data = chunk => {
            if (used + chunk.length > limits.bodyBytes) { finish(new McpIngressError('payload_too_large')); return; }
            chunk.copy(buffer, used); used += chunk.length;
          };
          const end = () => {
            try {
              const text = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(buffer.subarray(0, used));
              const body = parseMcpJson(text, limits);
              responseTimer = setTimeout(() => stop('request_timeout'), limits.responseTimeoutMs);
              finish(null, { text, body });
            } catch (error) { finish(error instanceof CapabilityHttpError ? error : new McpIngressError('invalid_input', -32700)); }
          };
          const timer = setTimeout(() => finish(new McpIngressError('request_timeout')), limits.bodyTimeoutMs);
          controller.signal.addEventListener('abort', onAbort, { once: true });
          req.on('data', data); req.once('end', end);
          if (controller.signal.aborted) onAbort(); else if (req.destroyed || req.aborted) stop();
        });
      },
      async collect(response) {
        current();
        if (!response.body) return Buffer.alloc(0);
        const reader = response.body.getReader(), buffer = Buffer.allocUnsafe(limits.outputBytes);
        let used = 0, complete = false;
        const cancel = () => { void reader.cancel().catch(() => {}); };
        controller.signal.addEventListener('abort', cancel, { once: true });
        try {
          while (true) {
            current(); const { done, value } = await reader.read(); current();
            if (done) { complete = true; return buffer.subarray(0, used); }
            check(value instanceof Uint8Array && used + value.byteLength <= limits.outputBytes, 'payload_too_large');
            buffer.set(value, used); used += value.byteLength;
          }
        } finally {
          controller.signal.removeEventListener('abort', cancel);
          if (!complete) { stop(); await reader.cancel().catch(() => {}); }
          reader.releaseLock();
        }
      },
      // Register completion before the synchronous final authority check and
      // res.end. No await may be introduced between those two operations.
      completion() {
        current();
        return new Promise(resolve => {
          const done = () => {
            res.off('finish', done); res.off('close', done); controller.signal.removeEventListener('abort', abort);
            resolve();
          };
          const abort = () => { if (!res.destroyed) res.destroy(); done(); };
          res.once('finish', done); res.once('close', done); controller.signal.addEventListener('abort', abort, { once: true });
          if (res.writableFinished || res.destroyed) done();
        });
      },
    });
  }
  return Object.freeze({ limits, enter, close() { closed = true; for (const stop of active) stop('service_closed'); peers.clear(); } });
}

export { singleHeader };
