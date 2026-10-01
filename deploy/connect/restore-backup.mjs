// Private dry admission and Linux receiver. No sender, CLI or plaintext result.
import { performance } from 'node:perf_hooks';
import { Readable } from 'node:stream';
import { createHash } from 'node:crypto';
import { readEncryptedBackup, createRestoreParser, formatFailure, formatFailureCode } from './backup-format.mjs';
import { captureRestoreTarget, openRestoreSink } from './restore-sink.mjs';

const HEX = /^[a-f0-9]{64}$/;
const LIMIT_KEYS = ['archiveBytes', 'plaintextBytes', 'fileBytes', 'extractedBytes', 'entries', 'headers',
  'pathBytes', 'pathDepth', 'externalFiles', 'externalBytes', 'wallMs', 'idleMs'];
const CODES = new Set(['restore_authentication_failed', 'restore_incomplete', 'restore_archive_invalid',
  'restore_limit_exceeded', 'restore_timeout', 'restore_io_failed']);
const aborted = Object.getOwnPropertyDescriptor(AbortSignal.prototype, 'aborted').get;
function exact(value, required, optional = []) {
  if (!value || typeof value !== 'object' || Array.isArray(value)
      || required.some(key => !Object.hasOwn(value, key))
      || Reflect.ownKeys(value).some(key => !required.includes(key) && !optional.includes(key)))
    throw formatFailure('restore_archive_invalid');
}
function capture(input) {
  exact(input, ['file', 'privateKeyPem', 'expectedSha256', 'expectedManifestSha256', 'sourceWitness', 'limits'], ['signal']);
  const { file, privateKeyPem, expectedSha256, expectedManifestSha256, sourceWitness, limits } = input;
  const signal = Object.hasOwn(input, 'signal') ? input.signal : undefined;
  if (typeof file !== 'string' || !file || file.length > 4096 || !file.isWellFormed() || file.includes('\0') || Buffer.byteLength(file) > 4096
      || typeof privateKeyPem !== 'string' || !privateKeyPem || privateKeyPem.length > 64 * 1024 || Buffer.byteLength(privateKeyPem) > 64 * 1024
      || typeof expectedSha256 !== 'string' || !HEX.test(expectedSha256)
      || typeof expectedManifestSha256 !== 'string' || !HEX.test(expectedManifestSha256)) throw formatFailure();
  exact(sourceWitness, ['generationId', 'checkpointSha256', 'inventorySha256']);
  const witness = { generationId: sourceWitness.generationId, checkpointSha256: sourceWitness.checkpointSha256,
    inventorySha256: sourceWitness.inventorySha256 };
  if (typeof witness.generationId !== 'string' || !/^[a-f0-9]{32}$/.test(witness.generationId)
      || typeof witness.checkpointSha256 !== 'string' || !HEX.test(witness.checkpointSha256)
      || typeof witness.inventorySha256 !== 'string' || !HEX.test(witness.inventorySha256)) throw formatFailure();
  exact(limits, LIMIT_KEYS);
  const capturedLimits = {};
  for (const key of LIMIT_KEYS) {
    const value = limits[key];
    if (!Number.isSafeInteger(value) || value <= 0) throw formatFailure('restore_limit_exceeded');
    capturedLimits[key] = value;
  }
  // Invoke the native brand-checking getter, never a caller-defined aborted or
  // reason property. Abort reasons are not part of this port's diagnostics.
  if (signal !== undefined) aborted.call(signal);
  return { file, privateKeyPem, expectedSha256, expectedManifestSha256,
    sourceWitness: Object.freeze(witness), limits: Object.freeze(capturedLimits), signal };
}

export async function inspectRestorableBackup(input) {
  try {
    const started = performance.now(), options = capture(input);
    let lastProgress = started;
    const check = (progress = false) => {
      const now = performance.now();
      if (options.signal !== undefined && aborted.call(options.signal)) throw formatFailure('restore_io_failed');
      if (now - started > options.limits.wallMs || now - lastProgress > options.limits.idleMs)
        throw formatFailure('restore_timeout');
      if (progress) lastProgress = now;
    };
    check();
    const result = await readEncryptedBackup({ file: options.file, privateKeyPem: options.privateKeyPem,
      restore: { expectedSha256: options.expectedSha256, expectedManifestSha256: options.expectedManifestSha256,
        sourceWitness: options.sourceWitness, limits: options.limits, check } });
    check();
    return Object.freeze({ ...result, strictProfile: 'soty.restore-manifest.v1', inventoryMatched: true });
  } catch (error) {
    const observed = formatFailureCode(error), code = CODES.has(observed) ? observed : 'restore_io_failed';
    throw Object.assign(new Error(code), { code, stack: `Error: ${code}` });
  }
}

const CHUNK = 64 * 1024;
const EXTRACT_LIMIT_KEYS = [...LIMIT_KEYS.filter(key => key !== 'archiveBytes'), 'freeSpaceReserveBytes'];
const EXTRACT_CODES = new Set(['restore_platform_unavailable', 'restore_target_invalid', 'restore_incomplete',
  'restore_archive_invalid', 'restore_limit_exceeded', 'restore_timeout', 'restore_io_failed', 'restore_cleanup_pending']);
function captureExtraction(value) {
  exact(value, ['input', 'target', 'expectedManifestSha256', 'sourceWitness', 'limits'], ['signal']);
  const input = value.input, target = captureRestoreTarget(value.target), expectedManifestSha256 = value.expectedManifestSha256;
  const signal = Object.hasOwn(value, 'signal') ? value.signal : undefined;
  // Private Node24.15/24.21 profile: the constructor's effective emitClose
  // option has no public getter. Require its pinned native state flag without
  // changing it; closed=true alone cannot wake a suppressed close event.
  if (!(input instanceof Readable) || input._readableState?.emitClose !== true
      || input.readableObjectMode || input.readableEncoding !== null
      || !Number.isSafeInteger(input.readableHighWaterMark) || input.readableHighWaterMark <= 0 || input.readableHighWaterMark > CHUNK
      || input.destroyed || input.closed || input.readableEnded || input.readableFlowing !== null
      || Readable.isDisturbed(input) || input.readableLength > CHUNK
      || typeof expectedManifestSha256 !== 'string' || !HEX.test(expectedManifestSha256)) throw formatFailure();
  const source = value.sourceWitness;
  exact(source, ['generationId', 'checkpointSha256', 'inventorySha256']);
  const sourceWitness = { generationId: source.generationId, checkpointSha256: source.checkpointSha256, inventorySha256: source.inventorySha256 };
  if (typeof sourceWitness.generationId !== 'string' || !/^[a-f0-9]{32}$/.test(sourceWitness.generationId)
      || typeof sourceWitness.checkpointSha256 !== 'string' || !HEX.test(sourceWitness.checkpointSha256)
      || typeof sourceWitness.inventorySha256 !== 'string' || !HEX.test(sourceWitness.inventorySha256)) throw formatFailure();
  const limits = value.limits; exact(limits, EXTRACT_LIMIT_KEYS);
  const captured = {};
  for (const key of EXTRACT_LIMIT_KEYS) {
    const amount = limits[key];
    if (!Number.isSafeInteger(amount) || amount <= 0) throw formatFailure('restore_limit_exceeded');
    captured[key] = amount;
  }
  if (signal !== undefined) aborted.call(signal);
  return { input, target, expectedManifestSha256, sourceWitness: Object.freeze(sourceWitness), limits: Object.freeze(captured), signal };
}

// One current read waiter, one deadline timer and one close completion. No
// per-chunk race against a lifetime promise retaining settled continuations.
class OwnedInput {
  waiter = null; failure = null; timer = null; didClose = false; listening = false;
  constructor(input, limits, signal) {
    this.input = input; this.limits = limits; this.signal = signal;
    this.started = performance.now(); this.lastProgress = this.started;
    this.closed = new Promise(resolve => { this.resolveClosed = resolve; });
    this.readable = () => this.wake();
    this.ended = () => this.wake();
    this.errored = () => this.fail('restore_io_failed');
    this.closeEvent = () => {
      this.didClose = true;
      if (!input.readableEnded && !this.failure) this.failure = formatFailure('restore_io_failed');
      this.resolveClosed(); this.wake();
    };
    this.abortEvent = () => this.fail('restore_io_failed');
    // Adding a readable listener can itself trigger _read. Defer that until
    // the pinned empty target has passed its read-only preflight.
    input.on('end', this.ended);
    input.on('error', this.errored); input.on('close', this.closeEvent);
    if (signal !== undefined) EventTarget.prototype.addEventListener.call(signal, 'abort', this.abortEvent, { once: true });
    this.arm();
  }
  wake() { const waiter = this.waiter; this.waiter = null; waiter?.(); }
  fail(code) {
    this.failure ??= formatFailure(code);
    // Never forward input errors or AbortSignal.reason into destroy/diagnostics.
    try { this.input.destroy(); } catch { this.failure = formatFailure('restore_cleanup_pending'); }
    this.wake();
  }
  arm() {
    clearTimeout(this.timer);
    const now = performance.now();
    const remaining = Math.min(this.started + this.limits.wallMs - now, this.lastProgress + this.limits.idleMs - now);
    this.timer = setTimeout(() => {
      try { this.check(); this.arm(); }
      catch { this.fail(this.failure ? formatFailureCode(this.failure) : 'restore_timeout'); }
    }, Math.max(1, Math.min(2_147_483_647, Math.ceil(remaining + 1))));
  }
  check(progress = false) {
    if (this.failure) throw this.failure;
    const now = performance.now();
    if (this.signal !== undefined && aborted.call(this.signal)) { this.failure = formatFailure('restore_io_failed'); throw this.failure; }
    if (now - this.started > this.limits.wallMs || now - this.lastProgress > this.limits.idleMs) {
      this.failure = formatFailure('restore_timeout'); throw this.failure;
    }
    if (progress) { this.lastProgress = now; this.arm(); }
  }
  async read() {
    if (!this.listening) { this.listening = true; this.input.on('readable', this.readable); }
    for (;;) {
      this.check();
      const chunk = this.input.read(CHUNK);
      if (chunk !== null) {
        if (!Buffer.isBuffer(chunk) || chunk.length > CHUNK) throw formatFailure('restore_limit_exceeded');
        this.check(chunk.length > 0); return chunk;
      }
      if (this.input.readableEnded) return null;
      if (this.didClose) throw formatFailure('restore_io_failed');
      // No await separates null read and registration; synchronous readable
      // state changes are re-observed on the next loop after the event wakeup.
      await new Promise(resolve => { this.waiter = resolve; });
    }
  }
  async close() {
    let destroyFailed = false;
    try { if (!this.didClose) this.input.destroy(); } catch { destroyFailed = true; }
    if (!destroyFailed) await this.closed;
    clearTimeout(this.timer); this.wake();
    this.input.removeListener('readable', this.readable); this.input.removeListener('end', this.ended);
    this.input.removeListener('error', this.errored); this.input.removeListener('close', this.closeEvent);
    if (this.signal !== undefined) EventTarget.prototype.removeEventListener.call(this.signal, 'abort', this.abortEvent);
    if (destroyFailed || !this.didClose) throw formatFailure('restore_cleanup_pending');
  }
}

export async function extractOwnedBackup(value) {
  let input, sink, parser, failure, receipt, failed = false;
  try {
    // The unsupported platform branch does not even inspect caller properties.
    if (process.platform !== 'linux') throw formatFailure('restore_platform_unavailable');
    const options = captureExtraction(value);
    input = new OwnedInput(options.input, options.limits, options.signal);
    const check = progress => input.check(progress);
    check(); sink = await openRestoreSink(options.target, options.limits, check);
    parser = createRestoreParser({ ...options, check }, sink);
    const digest = createHash('sha256'); let plaintextBytes = 0;
    for (;;) {
      const chunk = await input.read();
      if (chunk === null) break;
      if (chunk.length > options.limits.plaintextBytes - plaintextBytes) throw formatFailure('restore_limit_exceeded');
      plaintextBytes += chunk.length; digest.update(chunk); await parser.feed(chunk); check();
    }
    const counts = await parser.finish(); check();
    receipt = { extracted: true, targetId: options.target.targetId, manifestSha256: options.expectedManifestSha256,
      plaintextSha256: digest.digest('hex'), plaintextBytes, entries: counts.archiveEntries,
      fileBytes: counts.verifiedFileBytes, readbackVerified: true };
  } catch (error) { failed = true; failure = error; }
  finally {
    parser?.clear();
    // Cleanup always waits for owned I/O. A timer cannot prove that a syscall
    // or custom Readable._destroy completed; the parent hard watchdog owns it.
    try { await sink?.close(); } catch { failed = true; failure = formatFailure('restore_cleanup_pending'); }
    try { await input?.close(); } catch { failed = true; failure = formatFailure('restore_cleanup_pending'); }
  }
  if (!failed) { try { input.check(); } catch (error) { failed = true; failure = error; } }
  if (failed) {
    const observed = formatFailureCode(failure), code = EXTRACT_CODES.has(observed) ? observed : 'restore_io_failed';
    throw Object.assign(new Error(code), { code, stack: `Error: ${code}` });
  }
  return Object.freeze(receipt);
}
