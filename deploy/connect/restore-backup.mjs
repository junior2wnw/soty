// Private dry admission, Linux receiver and authenticated sender. No CLI or
// plaintext result; each port has its own closed options and ownership profile.
import { performance } from 'node:perf_hooks';
import { Readable, Writable, Duplex } from 'node:stream';
import { createHash } from 'node:crypto';
import { constants } from 'node:fs';
import { lstat, open } from 'node:fs/promises';
import path from 'node:path';
import { readEncryptedBackup, readEncryptedPass, createRestoreParser, formatFailure, formatFailureCode } from './backup-format.mjs';
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

const SEND_CODES = new Set([...CODES, 'restore_cleanup_pending']);
function captureSender(value) {
  exact(value, ['file', 'privateKeyPem', 'expectedSha256', 'expectedManifestSha256', 'sourceWitness', 'output', 'limits'], ['signal']);
  const options = capture({ file: value.file, privateKeyPem: value.privateKeyPem,
    expectedSha256: value.expectedSha256, expectedManifestSha256: value.expectedManifestSha256,
    sourceWitness: value.sourceWitness, limits: value.limits,
    ...(Object.hasOwn(value, 'signal') ? { signal: value.signal } : {}) });
  const output = value.output, state = output?._writableState;
  // Pinned native Node24.21 profile, without changing flags or wrapping _write /
  // _final / _destroy. A transport adapter, not the sender, owns its real pipe.
  if (!path.isAbsolute(options.file) || !(output instanceof Writable) || output instanceof Duplex
      || output === process.stdout || output === process.stderr
      || state?.emitClose !== true || state.autoDestroy !== true || state.constructed !== true
      || state.writing || state.pendingcb !== 0 || state.finalCalled || state.ending
      || output.writableObjectMode || !output.writable || output.destroyed || output.closed || output.errored
      || output.writableEnded || output.writableFinished || output.writableLength !== 0 || output.writableCorked !== 0
      || output.writableNeedDrain || !Number.isSafeInteger(output.writableHighWaterMark)
      || output.writableHighWaterMark < 1 || output.writableHighWaterMark > CHUNK) throw formatFailure();
  return { ...options, output };
}

class OwnedOutput {
  waiter = null; operation = null; failure = null; timer = null;
  didClose = false; didFinish = false; endIssued = false; destroyFailed = false;
  constructor(output, limits, signal) {
    this.output = output; this.limits = limits; this.signal = signal;
    this.started = performance.now(); this.lastProgress = this.started;
    this.closed = new Promise(resolve => { this.resolveClosed = resolve; });
    this.drainEvent = () => { if (this.operation) this.operation.drained = true; this.wake(); };
    this.errorEvent = () => this.fail('restore_io_failed');
    this.finishEvent = () => {
      if (!this.endIssued || output.writableFinished !== true) {
        this.fail('restore_io_failed'); return;
      }
      this.didFinish = true;
      this.wake();
    };
    this.closeEvent = () => {
      if (!output.closed) { this.fail('restore_io_failed'); return; }
      this.didClose = true; this.resolveClosed();
      if (!this.didFinish) this.fail('restore_io_failed');
      this.wake();
    };
    this.abortEvent = () => this.fail('restore_io_failed');
  }
  start() {
    this.output.on('drain', this.drainEvent); this.output.on('error', this.errorEvent);
    this.output.on('finish', this.finishEvent); this.output.on('close', this.closeEvent);
    if (this.signal !== undefined) EventTarget.prototype.addEventListener.call(this.signal, 'abort', this.abortEvent, { once: true });
    this.arm(); this.check();
  }
  wake() { const waiter = this.waiter; this.waiter = null; waiter?.(); }
  fail(code) {
    this.failure ??= formatFailure(code);
    clearTimeout(this.timer);
    try { if (!this.didClose && !this.output.destroyed) this.output.destroy(); }
    catch { this.destroyFailed = true; this.failure = formatFailure('restore_cleanup_pending'); }
    this.wake();
  }
  arm() {
    clearTimeout(this.timer);
    const now = performance.now();
    const remaining = Math.min(this.limits.wallMs - (now - this.started), this.limits.idleMs - (now - this.lastProgress));
    this.timer = setTimeout(() => {
      try { this.check(); this.arm(); }
      catch { this.fail(this.failure ? formatFailureCode(this.failure) : 'restore_timeout'); }
    }, Math.max(1, Math.min(2_147_483_647, Math.ceil(remaining + 1))));
  }
  check(progress = false) {
    if (!this.failure) {
      const now = performance.now();
      if (this.signal !== undefined && aborted.call(this.signal)) this.fail('restore_io_failed');
      else if (now - this.started > this.limits.wallMs || now - this.lastProgress > this.limits.idleMs) this.fail('restore_timeout');
      else if (progress) { this.lastProgress = now; this.arm(); }
    }
    if (this.failure) throw this.failure;
  }
  async write(chunk) {
    this.check();
    if (!Buffer.isBuffer(chunk) || chunk.length > CHUNK || this.operation) throw formatFailure('restore_limit_exceeded');
    const operation = { callbackDone: false, drained: false, needDrain: false };
    this.operation = operation;
    try {
      // The permanent drain listener and this operation exist BEFORE write.
      // Native afterWrite can emit drain before invoking the supplied callback.
      try {
        operation.needDrain = !this.output.write(chunk, error => {
          operation.callbackDone = true;
          if (error) this.fail('restore_io_failed');
          this.wake();
        });
      } catch { this.fail('restore_io_failed'); }
      // close/error/timeout cannot settle this borrowed buffer. A native close
      // may precede a held _write callback. If that callback never arrives, this
      // remains unresolved for the parent watchdog; no wipe or next file read.
      while (!operation.callbackDone || (!this.failure && operation.needDrain && !operation.drained))
        await new Promise(resolve => { this.waiter = resolve; });
      // Failure suppresses drain in native Writable; the callback still counts.
      this.check(chunk.length > 0);
    } finally { this.operation = null; }
  }
  async finish() {
    this.check(); this.endIssued = true;
    try { this.output.end(); } catch { this.fail('restore_io_failed'); }
    while (!this.didClose && !this.failure) await new Promise(resolve => { this.waiter = resolve; });
    this.check();
    if (!this.didFinish || !this.didClose) throw formatFailure('restore_io_failed');
  }
  async close() {
    try {
      try { if (!this.didClose) this.output.destroy(); } catch { this.destroyFailed = true; }
      if (!this.destroyFailed) await this.closed;
      // An end callback/error/close does not prove a held native _final callback
      // completed. With all writes settled, pendingcb can still belong to it.
      if (this.destroyFailed || !this.didClose
          || this.endIssued && !this.didFinish && this.output._writableState.pendingcb !== 0)
        throw formatFailure('restore_cleanup_pending');
    } finally {
      clearTimeout(this.timer); this.wake();
      this.output.removeListener('drain', this.drainEvent); this.output.removeListener('error', this.errorEvent);
      this.output.removeListener('finish', this.finishEvent); this.output.removeListener('close', this.closeEvent);
      if (this.signal !== undefined) EventTarget.prototype.removeEventListener.call(this.signal, 'abort', this.abortEvent);
    }
  }
}

function sameSource(actual, expected) {
  return actual.isFile() && actual.nlink === 1n && !actual.isSymbolicLink()
    && ['dev', 'ino', 'size', 'mtimeNs', 'ctimeNs'].every(key => actual[key] === expected[key]);
}
async function unchangedSource(handle, expected, check) {
  check();
  const stat = await handle.stat({ bigint: true }); check();
  if (!sameSource(stat, expected)) throw formatFailure('restore_authentication_failed');
  const extra = Buffer.alloc(1);
  try {
    const result = await handle.read(extra, 0, 1, Number(expected.size)); check();
    if (result.bytesRead !== 0) throw formatFailure('restore_authentication_failed');
  } finally { extra.fill(0); }
}

export async function sendAuthenticatedBackup(value) {
  let owner, handle, receipt, failure, failed = false;
  const closeFile = async () => {
    if (!handle) return;
    const owned = handle; handle = null;
    try { await owned.close(); } catch { throw formatFailure('restore_cleanup_pending'); }
  };
  try {
    const options = captureSender(value);
    owner = new OwnedOutput(options.output, options.limits, options.signal); owner.start();
    const check = progress => owner.check(progress);
    check(); const before = await lstat(options.file, { bigint: true }); check();
    if (!before.isFile() || before.isSymbolicLink() || before.nlink !== 1n || before.size < 32n) throw formatFailure();
    if (before.size > BigInt(options.limits.archiveBytes) || before.size > BigInt(Number.MAX_SAFE_INTEGER))
      throw formatFailure('restore_limit_exceeded');
    handle = await open(options.file, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
    await unchangedSource(handle, before, check);
    const restore = { expectedSha256: options.expectedSha256, expectedManifestSha256: options.expectedManifestSha256,
      sourceWitness: options.sourceWitness, limits: options.limits, check };
    const passOptions = { handle, privateKeyPem: options.privateKeyPem, restore };
    const first = await readEncryptedPass(passOptions);
    await unchangedSource(handle, before, check);
    const second = await readEncryptedPass(passOptions, owner);
    await unchangedSource(handle, before, check);
    if (second.receipt.sha256 !== first.receipt.sha256 || second.plaintextSha256 !== first.plaintextSha256
        || second.plaintextBytes !== first.plaintextBytes) throw formatFailure('restore_authentication_failed');
    await closeFile(); check();
    await owner.finish();
    receipt = { authenticated: true, archiveSha256: first.receipt.sha256, plaintextSha256: first.plaintextSha256,
      manifestSha256: options.expectedManifestSha256, plaintextBytes: first.plaintextBytes };
  } catch (error) { failed = true; failure = error; }
  finally {
    // Neither an expired deadline nor an abort can skip actual cleanup. The
    // caller owns the immutable copy/ACL guard and any hard process watchdog.
    try { await closeFile(); } catch { failed = true; failure = formatFailure('restore_cleanup_pending'); }
    try { await owner?.close(); } catch { failed = true; failure = formatFailure('restore_cleanup_pending'); }
  }
  if (!failed) { try { owner.check(); } catch (error) { failed = true; failure = error; } }
  if (failed) {
    const code = SEND_CODES.has(formatFailureCode(failure)) ? formatFailureCode(failure) : 'restore_io_failed';
    throw Object.assign(new Error(code), { code, stack: `Error: ${code}` });
  }
  return Object.freeze(receipt);
}
