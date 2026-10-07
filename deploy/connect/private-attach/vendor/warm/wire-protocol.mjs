// SOURCE PROPOSAL. These records bind bytes; they never create host/restore authority.
import { Readable, Writable } from 'node:stream';
import { performance } from 'node:perf_hooks';
import { TextDecoder, types } from 'node:util';

export const PROFILE = 'soty.restore-warm-control.v1';
export const MAX_FRAME_BYTES = 8192;
export const PRODUCTION_CLOCK = Object.freeze({ wallMs: 120000, idleMs: 15000 });
export const WARM_LEASE_MS = 120000;
export const SOURCE_KEYS = Object.freeze(['receiver', 'restore', 'format', 'sink', 'inventory', 'protocol', 'warmProbe', 'driver', 'entry']);
const HEX32 = /^[a-f0-9]{32}$/u, HEX64 = /^[a-f0-9]{64}$/u, IMAGE = /^sha256:[a-f0-9]{64}$/u;
const known = new WeakMap();
export function failure(code) {
  const error = new Error(code); known.set(error, code);
  Object.assign(error, { code, stack: `Error: ${code}` }); return error;
}
export const failureCode = error => known.get(error) || 'warm_receiver_failed';
export const fail = code => { throw failure(code); };
export function dataRecord(value, keys) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || types.isProxy(value)) fail('warm_record_invalid');
  let descriptors, prototype;
  try { descriptors = Object.getOwnPropertyDescriptors(value); prototype = Object.getPrototypeOf(value); }
  catch { fail('warm_record_invalid'); }
  if (prototype !== Object.prototype && prototype !== null || Reflect.ownKeys(descriptors).length !== keys.length
    || keys.some(key => !Object.hasOwn(descriptors, key) || !Object.hasOwn(descriptors[key], 'value'))) fail('warm_record_invalid');
  return Object.fromEntries(keys.map(key => [key, descriptors[key].value]));
}
export function captureSpec(value) {
  const captured = dataRecord(value, ['transaction', 'nonce', 'targetId', 'image', 'profile', 'sourcePins']);
  if (typeof captured.transaction !== 'string' || !HEX32.test(captured.transaction)
    || typeof captured.nonce !== 'string' || !HEX32.test(captured.nonce)
    || captured.targetId !== captured.transaction || captured.profile !== PROFILE
    || typeof captured.image !== 'string' || !IMAGE.test(captured.image)) fail('warm_spec_invalid');
  const sourcePins = dataRecord(captured.sourcePins, SOURCE_KEYS);
  if (Object.values(sourcePins).some(value => typeof value !== 'string' || !HEX64.test(value))) fail('warm_spec_invalid');
  return Object.freeze({ ...captured, sourcePins: Object.freeze(sourcePins) });
}
function witness(value, transaction) {
  const captured = dataRecord(value, ['generationId', 'checkpointSha256', 'inventorySha256']);
  if (captured.generationId !== transaction || typeof captured.checkpointSha256 !== 'string' || !HEX64.test(captured.checkpointSha256)
    || typeof captured.inventorySha256 !== 'string' || !HEX64.test(captured.inventorySha256)) fail('warm_binding_invalid');
  return Object.freeze(captured);
}
export function captureBinding(value, spec) {
  const captured = dataRecord(value, ['schema', 'transaction', 'nonce', 'targetId', 'image', 'profile', 'sourcePins',
    'expectedSha256', 'expectedManifestSha256', 'sourceWitness']);
  const echoed = captureSpec(Object.fromEntries(['transaction', 'nonce', 'targetId', 'image', 'profile', 'sourcePins'].map(key => [key, captured[key]])));
  if (captured.schema !== 'soty.receiver-bind.v1'
    || ['transaction', 'nonce', 'targetId', 'image', 'profile'].some(key => echoed[key] !== spec[key])
    || SOURCE_KEYS.some(key => echoed.sourcePins[key] !== spec.sourcePins[key])
    || typeof captured.expectedSha256 !== 'string' || !HEX64.test(captured.expectedSha256)
    || typeof captured.expectedManifestSha256 !== 'string' || !HEX64.test(captured.expectedManifestSha256)) fail('warm_binding_invalid');
  return Object.freeze({ ...captured, sourcePins: echoed.sourcePins, sourceWitness: witness(captured.sourceWitness, spec.transaction) });
}
export function bindingFrame(specValue, receiptValue) {
  const spec = captureSpec(specValue), receipt = dataRecord(receiptValue, ['expectedSha256', 'expectedManifestSha256', 'sourceWitness']);
  return captureBinding({ schema: 'soty.receiver-bind.v1', ...spec, ...receipt }, spec);
}
export function warmFrame(spec) { return Object.freeze({ schema: 'soty.receiver-warm.v1', ...spec, inputBytes: 0 }); }
export function boundFrame(binding) { return Object.freeze({ ...binding, schema: 'soty.receiver-bound.v1', inputBytes: 0 }); }
export function beginFrame(spec) { return Object.freeze({ schema: 'soty.receiver-control-begin.v1', ...spec }); }
export function beginAckFrame(spec) { return Object.freeze({ schema: 'soty.receiver-control-begin-ack.v1', ...spec, inputBytes: 0 }); }
export function captureBegin(value, spec) {
  const record = dataRecord(value, ['schema', 'transaction', 'nonce', 'targetId', 'image', 'profile', 'sourcePins']);
  if (record.schema !== 'soty.receiver-control-begin.v1') fail('warm_begin_invalid');
  const captured = captureSpec(Object.fromEntries(Object.keys(record).filter(key => key !== 'schema').map(key => [key, record[key]])));
  if (['transaction', 'nonce', 'targetId', 'image', 'profile'].some(key => captured[key] !== spec[key])
    || SOURCE_KEYS.some(key => captured.sourcePins[key] !== spec.sourcePins[key])) fail('warm_begin_invalid');
  return beginFrame(captured);
}
function sorted(value, depth = 0) {
  if (depth > 4) fail('warm_frame_invalid');
  if (Array.isArray(value)) return value.map(child => sorted(child, depth + 1));
  if (value && typeof value === 'object') {
    if (types.isProxy(value)) fail('warm_record_invalid');
    const keys = Reflect.ownKeys(value);
    if (keys.some(key => typeof key !== 'string')) fail('warm_record_invalid');
    const record = dataRecord(value, keys);
    return Object.fromEntries(keys.sort().map(key => [key, sorted(record[key], depth + 1)]));
  }
  return value;
}
export const canonicalJson = value => JSON.stringify(sorted(value));
export function encodeFrame(value) {
  const bytes = Buffer.from(canonicalJson(value) + '\n');
  if (bytes.length > MAX_FRAME_BYTES) fail('warm_frame_limit'); return bytes;
}
export function decodeCanonicalFrame(bytes) {
  if (!Buffer.isBuffer(bytes) || !bytes.length || bytes.length > MAX_FRAME_BYTES || bytes[bytes.length - 1] !== 10
    || bytes.subarray(0, -1).includes(10)) fail('warm_frame_invalid');
  let text, parsed;
  try { text = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(bytes.subarray(0, -1)); parsed = JSON.parse(text); }
  catch { fail('warm_frame_invalid'); }
  if (canonicalJson(parsed) !== text) fail('warm_frame_noncanonical');
  return parsed;
}
const signalAborted = Object.getOwnPropertyDescriptor(AbortSignal.prototype, 'aborted').get;
export function captureSignal(signal) { if (signal !== undefined) signalAborted.call(signal); return signal; }
export const signalIsAborted = signal => signal !== undefined && signalAborted.call(signal);
export function unreadPayload(input) {
  if (!(input instanceof Readable) || input.readableObjectMode || input.readableEncoding !== null || input.destroyed || input.closed
    || input.readableEnded || input.readableFlowing !== null || Readable.isDisturbed(input) || input.readableLength !== 0
    || !Number.isSafeInteger(input.readableHighWaterMark) || input.readableHighWaterMark < 1 || input.readableHighWaterMark > 65536)
    fail('warm_payload_not_read0');
}

// Public stream APIs only. One read/write operation at a time, one clock; ready is not progress.
export class WireOwner {
  constructor({ signal, limits = PRODUCTION_CLOCK, check = () => {}, absoluteLeaseDeadline, leaseOnly = false } = {}) {
    this.signal = captureSignal(signal); this.limits = dataRecord(limits, ['wallMs', 'idleMs']);
    if (Object.entries(this.limits).some(([key, value]) => !Number.isSafeInteger(value) || value < 1 || value > PRODUCTION_CLOCK[key])
      || typeof check !== 'function') fail('warm_clock_invalid');
    this.outerCheck = check; this.started = performance.now(); this.progress = this.started;
    if (absoluteLeaseDeadline !== undefined && (!Number.isFinite(absoluteLeaseDeadline) || absoluteLeaseDeadline > this.started + WARM_LEASE_MS))
      fail('warm_lease_invalid');
    this.absoluteLeaseDeadline = absoluteLeaseDeadline; this.leaseOnly = leaseOnly;
    this.failure = null; this.waiter = null; this.streams = new Set(); this.cleanups = []; this.timer = null;
    this.abort = () => this.stop('warm_cancelled');
    if (signal !== undefined) EventTarget.prototype.addEventListener.call(signal, 'abort', this.abort, { once: true });
    this.arm();
    try { this.check(); } catch (error) { this.release(); throw error; }
  }
  wake() { const waiter = this.waiter; this.waiter = null; waiter?.(); }
  stop(code) {
    this.failure ??= failure(code); clearTimeout(this.timer);
    for (const stream of this.streams) { try { stream.destroy(); } catch {} }
    this.wake();
  }
  check(progress = false) {
    if (this.failure) throw this.failure;
    try { this.outerCheck(); } catch { this.stop('warm_fence_closed'); throw this.failure; }
    if (this.signal !== undefined && signalAborted.call(this.signal)) this.stop('warm_cancelled');
    const now = performance.now();
    if (this.absoluteLeaseDeadline !== undefined && now > this.absoluteLeaseDeadline) this.stop('warm_lease_expired');
    if (!this.bodyHandoff && !this.leaseOnly && (now - this.started > this.limits.wallMs || now - this.progress > this.limits.idleMs)) this.stop('warm_timeout');
    if (this.failure) throw this.failure;
    if (progress) { this.progress = now; this.arm(); }
  }
  arm() {
    clearTimeout(this.timer);
    if (this.bodyHandoff && this.absoluteLeaseDeadline === undefined) return;
    const now = performance.now(), remaining = Math.min(this.absoluteLeaseDeadline === undefined ? Infinity : this.absoluteLeaseDeadline - now,
      this.leaseOnly || this.bodyHandoff ? Infinity : this.started + this.limits.wallMs - now,
      this.leaseOnly || this.bodyHandoff ? Infinity : this.progress + this.limits.idleMs - now);
    this.timer = setTimeout(() => { try { this.check(); this.arm(); } catch {} }, Math.max(1, Math.ceil(remaining + 1)));
  }
  own(stream) {
    if (this.streams.has(stream)) return;
    this.streams.add(stream);
    const error = () => this.stop('warm_wire_io_failed');
    stream.on('error', error); this.cleanups.push(() => stream.removeListener('error', error));
  }
  async readFrame(input, { requireEOF = true, allowPreviouslyOwnedRead = false } = {}) {
    if (!(input instanceof Readable) || input.readableObjectMode || input.readableEncoding !== null || input.destroyed || input.closed
      || !Number.isSafeInteger(input.readableHighWaterMark) || input.readableHighWaterMark < 1 || input.readableHighWaterMark > 65536
      || !allowPreviouslyOwnedRead && (Readable.isDisturbed(input) || input.readableFlowing !== null)
      || allowPreviouslyOwnedRead && input.readableFlowing === true) fail('warm_control_stream_invalid');
    this.own(input); const parts = []; let bytes = 0;
    const wake = () => this.wake();
    input.on('readable', wake); input.on('end', wake);
    const close = () => { if (!input.readableEnded) this.stop('warm_control_eof_missing'); else this.wake(); };
    input.on('close', close);
    try {
      for (;;) {
        this.check();
        if (input.readableLength > MAX_FRAME_BYTES - bytes) fail('warm_frame_limit');
        const chunk = input.read(Math.min(MAX_FRAME_BYTES + 1 - bytes, input.readableLength || 1));
        if (chunk !== null) {
          if (!Buffer.isBuffer(chunk) || chunk.length > MAX_FRAME_BYTES - bytes) fail('warm_frame_limit');
          parts.push(chunk); bytes += chunk.length; this.check(chunk.length > 0);
          if (!requireEOF && chunk.includes(10)) {
            if (input.readableLength !== 0) fail('warm_unsolicited_frame');
            return decodeCanonicalFrame(Buffer.concat(parts, bytes));
          }
          continue;
        }
        if (input.readableEnded) break;
        if (input.destroyed || input.closed) fail('warm_control_eof_missing');
        await new Promise(resolve => { this.waiter = resolve; });
      }
      this.check(); return decodeCanonicalFrame(Buffer.concat(parts, bytes));
    } finally {
      input.removeListener('readable', wake); input.removeListener('end', wake); input.removeListener('close', close);
    }
  }
  async readOne(input) { return this.readFrame(input); }
  async writeFrame(output, value) {
    if (!(output instanceof Writable) || output.writableObjectMode || output.destroyed || output.closed || output.writableEnded) fail('warm_output_invalid');
    this.own(output); this.check(); const bytes = encodeFrame(value);
    let callbackDone = false, needDrain = false, drained = false;
    const drain = () => { drained = true; this.wake(); };
    output.on('drain', drain);
    try {
      try { needDrain = !output.write(bytes, error => { callbackDone = true; if (error) this.stop('warm_wire_io_failed'); this.wake(); }); }
      catch { callbackDone = true; this.stop('warm_wire_io_failed'); }
      // A destroy/clock cannot prove a borrowed _write callback settled. A future
      // outer watchdog owns truly hung native IO; these public control bytes are not wiped early.
      while (!callbackDone || !this.failure && needDrain && !drained) await new Promise(resolve => { this.waiter = resolve; });
      this.check();
    } finally { output.removeListener('drain', drain); }
  }
  async end(output) {
    this.check(); this.own(output); let callbackDone = false;
    try { output.end(error => { callbackDone = true; if (error) this.stop('warm_wire_io_failed'); this.wake(); }); }
    catch { callbackDone = true; this.stop('warm_wire_io_failed'); }
    while (!callbackDone) await new Promise(resolve => { this.waiter = resolve; });
    this.check();
  }
  release() {
    clearTimeout(this.timer); this.wake();
    for (const cleanup of this.cleanups.splice(0)) cleanup();
    if (this.signal !== undefined) EventTarget.prototype.removeEventListener.call(this.signal, 'abort', this.abort);
  }
  handoffBody() {
    // The unchanged receiver owns its existing body clocks. The external sender
    // still owns its full GCM+bind+body clocks; no one resets that clock here.
    this.check(); this.bodyHandoff = true; this.arm();
  }
}
