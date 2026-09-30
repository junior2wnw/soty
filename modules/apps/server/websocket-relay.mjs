import { randomBytes } from 'node:crypto';
import { performance } from 'node:perf_hooks';
import { AppsError, assertApps, CHUNK_BYTES, createWebSocketFrameParser } from './protocol.mjs';

const DEFAULT_TIMING = Object.freeze({ quietMs: 30_000, insertMs: 30_000, responseMs: 30_000, frameMs: 30_000 });
const TIMING_KEYS = Object.keys(DEFAULT_TIMING);

export function normalizeWebSocketLivenessTiming(value) {
  if (value === undefined) return DEFAULT_TIMING;
  assertApps(value !== null && typeof value === 'object' && !Array.isArray(value) && [Object.prototype, null].includes(Object.getPrototypeOf(value)), 'invalid_websocket_liveness');
  assertApps(Reflect.ownKeys(value).every(key => TIMING_KEYS.includes(key)), 'invalid_websocket_liveness');
  const result = { ...DEFAULT_TIMING };
  for (const key of Object.keys(value)) {
    assertApps(Number.isSafeInteger(value[key]) && value[key] > 0 && value[key] <= DEFAULT_TIMING[key], 'invalid_websocket_liveness');
    result[key] = value[key];
  }
  return Object.freeze(result);
}

const deferred = () => {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
};

function pingFrame(tag, masked) {
  const header = masked ? 6 : 2, bytes = Buffer.alloc(header + tag.length);
  bytes[0] = 0x89; bytes[1] = tag.length | (masked ? 0x80 : 0);
  if (masked) randomBytes(4).copy(bytes, 2);
  for (let i = 0; i < tag.length; i++) bytes[header + i] = tag[i] ^ (masked ? bytes[2 + (i % 4)] : 0);
  return bytes;
}

function isOwnPong(frame, tag, masked) {
  if (!frame.control || !frame.complete || frame.opcode !== 10 || !frame.bytes || frame.bytes.length - frame.headerBytes !== tag.length) return false;
  for (let i = 0; i < tag.length; i++) {
    if ((frame.bytes[frame.headerBytes + i] ^ (masked ? frame.bytes[frame.headerBytes - 4 + (i % 4)] : 0)) !== tag[i]) return false;
  }
  return true;
}

// This is a bounded raw-byte relay, not a WebSocket endpoint or message codec.
// Each pump has one input, one 48KiB output buffer, and one pending control.
export function createWebSocketRelay({ toClient, toSource, assertActive, onFailure, timing, clock = {} } = {}) {
  const limits = normalizeWebSocketLivenessTiming(timing);
  assertApps([toClient, toSource, assertActive, onFailure].every(value => typeof value === 'function'), 'invalid_websocket_relay');
  const now = clock.now ?? (() => performance.now()), setTimer = clock.setTimeout ?? setTimeout, clearTimer = clock.clearTimeout ?? clearTimeout;
  assertApps([now, setTimer, clearTimer].every(value => typeof value === 'function'), 'invalid_websocket_clock');
  let started = false, closed = false, failure = null, timer = null, timerAt = null, closingAt = null, lastNow = -Infinity;
  const time = () => {
    const value = now();
    assertApps(Number.isFinite(value) && value >= 0 && value >= lastNow, 'invalid_websocket_clock');
    lastNow = value; return value;
  };
  const endpoint = name => ({ name, tag: randomBytes(32), lastFrameAt: 0, serial: 0, probe: null });
  const client = endpoint('client'), source = endpoint('source');
  const pump = (ingress, target, writer, masked) => ({
    ingress, target, writer, masked, parser: createWebSocketFrameParser({ masked }),
    staging: Buffer.alloc(CHUNK_BYTES), size: 0, input: null, running: false,
    frameAt: null, writeAt: null, writing: false, probeInBatch: null, activeWrite: null,
  });
  const clientPump = pump(client, source, toSource, true), sourcePump = pump(source, client, toClient, false);
  const pumps = [clientPump, sourcePump], endpoints = [client, source];

  function shutdown(error, notify) {
    if (closed) return;
    closed = true; failure = error;
    if (timer !== null) clearTimer(timer);
    timer = null; timerAt = null;
    for (const item of pumps) {
      const write = item.activeWrite; item.activeWrite = null; write?.reject(error);
      item.input?.reject(error); item.input = null; item.probeInBatch = null;
      item.size = 0; item.frameAt = null; item.writeAt = null; item.writing = false;
      item.staging = null; item.parser = null;
    }
    for (const peer of endpoints) peer.probe = null;
    if (notify) { try { onFailure(error); } catch { /* teardown must still finish */ } }
  }
  function fail(error) { shutdown(error instanceof Error ? error : new AppsError('app_websocket_transport_failed', 502), true); }
  function ensure() {
    if (closed) throw failure;
    assertApps(started, 'app_websocket_not_started');
    assertActive();
    const at = time(); checkDeadlines(at); return at;
  }
  function checkDeadlines(at) {
    if (closingAt !== null && at >= closingAt) throw new AppsError('app_websocket_close_timeout', 504);
    for (const item of pumps) {
      if (item.frameAt !== null && at >= item.frameAt) throw new AppsError('app_websocket_frame_timeout', 504);
      if (item.writeAt !== null && at >= item.writeAt) throw new AppsError('app_websocket_write_timeout', 504);
    }
    for (const peer of endpoints) {
      const probe = peer.probe;
      if (probe && at >= (probe.state === 'waiting' ? probe.responseAt : probe.insertAt)) throw new AppsError('app_websocket_liveness_timeout', 504);
    }
  }
  function arm() {
    if (!started || closed) return;
    const dates = [];
    if (closingAt !== null) dates.push(closingAt);
    else for (const peer of endpoints) dates.push(peer.probe ? (peer.probe.state === 'waiting' ? peer.probe.responseAt : peer.probe.insertAt) : peer.lastFrameAt + limits.quietMs);
    for (const item of pumps) { if (item.frameAt !== null) dates.push(item.frameAt); if (item.writeAt !== null) dates.push(item.writeAt); }
    const next = Math.min(...dates);
    if (timer !== null && timerAt === next) return;
    if (timer !== null) clearTimer(timer);
    timerAt = next;
    timer = setTimer(tick, Math.max(0, next - time())); timer?.unref?.();
  }
  function tick() {
    timer = null; timerAt = null;
    if (closed) return;
    try {
      const at = ensure();
      if (closingAt === null) for (const item of pumps) {
        const peer = item.target;
        if (!peer.probe && at >= peer.lastFrameAt + limits.quietMs) {
          peer.probe = { state: 'queued', serial: peer.serial, insertAt: at + limits.insertMs, responseAt: null };
          kick(item);
        }
      }
      arm();
    } catch (error) { fail(error); }
  }
  function complete(peer, at) {
    peer.lastFrameAt = at; peer.serial++;
    if (peer.probe && peer.probe.state !== 'writing') peer.probe = null;
  }
  function beginClosing(at) {
    if (closingAt !== null) return;
    closingAt = at + limits.responseMs;
    for (const item of pumps) {
      // An already offered write cannot be unsent. An unoffered control is
      // removed without disturbing application bytes following it.
      const pending = item.probeInBatch;
      if (pending && !item.writing) {
        item.staging.copyWithin(pending.offset, pending.offset + pending.length, item.size);
        item.size -= pending.length; item.probeInBatch = null;
      }
      item.target.probe = null;
    }
  }
  async function flush(item) {
    if (!item.size) return;
    const at = ensure(), size = item.size, pending = item.probeInBatch;
    item.writing = true; item.writeAt = at + limits.insertMs; arm();
    const completion = deferred(); item.activeWrite = completion;
    try {
      // Cancellation owns only this write. Racing every completed write
      // against a lifetime stop Promise would retain one reaction per write.
      try { Promise.resolve(item.writer(item.staging.subarray(0, size))).then(completion.resolve, completion.reject); }
      catch (error) { completion.reject(error); }
      await completion.promise;
      ensure();
      item.size = 0; item.probeInBatch = null; item.writeAt = null; item.writing = false;
      if (pending && closingAt === null && item.target.probe === pending.probe) {
        if (item.target.serial !== pending.probe.serial) item.target.probe = null;
        else { pending.probe.state = 'waiting'; pending.probe.responseAt = time() + limits.responseMs; }
      }
      arm();
    } catch (error) { fail(error); throw error; }
    finally { if (item.activeWrite === completion) item.activeWrite = null; }
  }
  async function inject(item) {
    let probe = item.target.probe;
    if (closingAt !== null || !item.parser.atBoundary || !probe || probe.state !== 'queued') return;
    const needed = item.masked ? 38 : 34;
    if (item.staging.length - item.size < needed) await flush(item);
    ensure(); probe = item.target.probe;
    if (closingAt !== null || !probe || probe.state !== 'queued') return;
    const frame = pingFrame(item.target.tag, item.masked), offset = item.size;
    frame.copy(item.staging, offset); item.size += frame.length;
    probe.state = 'writing';
    item.probeInBatch = { probe, offset, length: frame.length };
  }
  async function run(item) {
    while (!closed) {
      let at = ensure();
      if (item.parser.atBoundary && item.target.probe?.state === 'queued' && closingAt === null) { await inject(item); at = ensure(); }
      const input = item.input;
      if (!input) { if (item.size) await flush(item); arm(); return; }
      // One guard/time read for a bounded synchronous burst. Tiny frames must
      // not amplify into per-frame DB reads, timers, promises, or writes.
      while (input.offset < input.bytes.length) {
        if (item.staging.length - item.size < 139) break;
        const frame = item.parser.read(input.bytes, input.offset, item.staging.length - item.size);
        input.offset += frame.consumed;
        if (frame.started) item.frameAt = at + limits.frameMs;
        if (frame.bytes && !isOwnPong(frame, item.ingress.tag, item.masked)) {
          frame.bytes.copy(item.staging, item.size); item.size += frame.bytes.length;
        }
        if (frame.complete) {
          item.frameAt = null; complete(item.ingress, at);
          if (frame.opcode === 8) beginClosing(at);
        }
        if (item.parser.atBoundary && item.target.probe?.state === 'queued' && closingAt === null) break;
      }
      if (input.offset === input.bytes.length) {
        if (item.size) await flush(item);
        ensure();
        if (item.input === input) item.input = null;
        input.resolve(); arm();
      } else if (item.staging.length - item.size < 139) await flush(item);
      // A frame boundary with a pending probe returns to inject before the
      // next frame, even when both frames arrived in the same outer chunk.
    }
  }
  function kick(item) {
    if (closed || item.running) return;
    item.running = true;
    run(item).catch(fail).finally(() => {
      item.running = false;
      if (!closed && (item.input || (item.parser.atBoundary && item.target.probe?.state === 'queued'))) kick(item);
    });
  }
  function push(item, bytes) {
    try {
      ensure();
      assertApps(Buffer.isBuffer(bytes) && bytes.length <= CHUNK_BYTES, 'app_websocket_chunk_invalid');
      assertApps(!item.input, 'app_websocket_concurrent_input');
      if (!bytes.length) return Promise.resolve();
      const input = deferred();
      // Own a bounded allocation, including when the caller passed a small
      // view over a much larger backing store. Nothing survives settlement.
      item.input = { ...input, bytes: Buffer.from(bytes), offset: 0 };
      kick(item); return input.promise;
    } catch (error) { fail(error); return Promise.reject(error); }
  }
  return {
    start() {
      if (closed) throw failure;
      if (started) return;
      try {
        assertActive(); const at = time(); started = true;
        for (const peer of endpoints) peer.lastFrameAt = at;
        arm();
      } catch (error) { fail(error); throw error; }
    },
    clientBytes: bytes => push(clientPump, bytes),
    sourceBytes: bytes => push(sourcePump, bytes),
    close() { shutdown(new AppsError('app_websocket_closed'), false); },
  };
}
