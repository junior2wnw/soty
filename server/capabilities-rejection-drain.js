// Discard only; no JSON/identity/domain work and no caller-selected limits.
import { performance } from 'node:perf_hooks';
import { NATIVE_HTTP_LIMITS } from './capabilities-ingress.js';
export const REJECTION_DRAIN_LIMITS = Object.freeze({ bytes: NATIVE_HTTP_LIMITS.bodyBytes + 65536, wallMs: 2000,
  slots: NATIVE_HTTP_LIMITS.readers, slotsPerPeer: NATIVE_HTTP_LIMITS.readersPerPeer });
let active = 0;
const peers = new Map();
export const rejectionDrainSnapshot = () => Object.freeze({ active, peers: peers.size });
function terminalError(req) {
  if (req.closed) return;
  const ignore = () => {}, cleanup = () => req.off('error', ignore);
  req.once('error', ignore); req.once('close', cleanup);
}
export function discardRejectedRequest(req) {
  if (req.complete && req.readableEnded) return Promise.resolve({ complete: true, bytes: 0 });
  if (req.aborted || req.destroyed) return Promise.resolve({ complete: false, reason: 'aborted', bytes: 0 });
  const length = req.headers['content-length'];
  if (length !== undefined && (!/^(?:0|[1-9]\d*)$/u.test(length)
    || !Number.isSafeInteger(Number(length)) || Number(length) > REJECTION_DRAIN_LIMITS.bytes)) {
    terminalError(req); return Promise.resolve({ complete: false, reason: 'declared_size', bytes: 0 });
  }
  const peer = req.socket.remoteAddress;
  if (typeof peer !== 'string' || peer.length > 128 || active >= REJECTION_DRAIN_LIMITS.slots
    || (peers.get(peer) || 0) >= REJECTION_DRAIN_LIMITS.slotsPerPeer) {
    terminalError(req); return Promise.resolve({ complete: false, reason: 'capacity', bytes: 0 });
  }
  active++; peers.set(peer, (peers.get(peer) || 0) + 1);
  const started = performance.now();
  return new Promise(resolve => {
    let bytes = 0, settled = false;
    const cleanup = () => {
      clearTimeout(timer); req.off('data', data); req.off('end', end);
      req.off('aborted', aborted); req.off('error', aborted); req.off('close', close);
    };
    const finish = (complete, reason) => {
      if (settled) return; settled = true; cleanup();
      active--; const remaining = peers.get(peer) - 1;
      if (remaining) peers.set(peer, remaining); else peers.delete(peer);
      if (!complete) { req.pause(); terminalError(req); }
      resolve({ complete, ...(reason ? { reason } : {}), bytes });
    };
    const data = chunk => {
      if (performance.now() - started >= REJECTION_DRAIN_LIMITS.wallMs) { finish(false, 'deadline'); return; }
      if (!Buffer.isBuffer(chunk) || chunk.length > REJECTION_DRAIN_LIMITS.bytes - bytes) { finish(false, 'size'); return; }
      bytes += chunk.length;
    };
    const end = () => {
      if (performance.now() - started >= REJECTION_DRAIN_LIMITS.wallMs) finish(false, 'deadline');
      else finish(req.complete === true, req.complete ? undefined : 'incomplete');
    };
    const aborted = () => finish(false, 'aborted');
    const close = () => { if (!req.complete || !req.readableEnded) aborted(); else end(); };
    const timer = setTimeout(() => finish(false, 'deadline'), REJECTION_DRAIN_LIMITS.wallMs);
    req.on('data', data); req.once('end', end); req.once('aborted', aborted);
    req.once('error', aborted); req.once('close', close); req.resume();
    if (req.complete && req.readableEnded) end(); else if (req.aborted || req.destroyed) aborted();
  });
}
