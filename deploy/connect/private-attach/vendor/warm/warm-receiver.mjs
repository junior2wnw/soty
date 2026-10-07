// Programmatic SOURCE prototype. No CLI, Docker, host lease, auth grant or deployment.
import { Readable, Writable } from 'node:stream';
import { performance } from 'node:perf_hooks';
import { prepareLinuxWarm } from './linux-warm-preflight.mjs';
import { WireOwner, captureSpec, captureBinding, captureSignal, dataRecord, warmFrame, boundFrame,
  bindingFrame, unreadPayload, failure, failureCode, fail, PRODUCTION_CLOCK, SOURCE_KEYS,
  WARM_LEASE_MS, beginFrame, beginAckFrame, captureBegin, signalIsAborted } from './wire-protocol.mjs';

const expectedReceiptKeys = ['extracted', 'targetId', 'manifestSha256', 'plaintextSha256', 'plaintextBytes', 'entries', 'fileBytes', 'readbackVerified'];
const safeReceipt = (value, binding) => {
  const receipt = dataRecord(value, expectedReceiptKeys);
  if (receipt.extracted !== true || receipt.readbackVerified !== true || receipt.targetId !== binding.targetId
    || receipt.manifestSha256 !== binding.expectedManifestSha256 || typeof receipt.plaintextSha256 !== 'string'
    || !/^[a-f0-9]{64}$/u.test(receipt.plaintextSha256)
    || !Number.isSafeInteger(receipt.plaintextBytes) || receipt.plaintextBytes <= 0 || receipt.plaintextBytes > 256 * 1024 * 1024
    || !Number.isSafeInteger(receipt.entries) || receipt.entries <= 0 || receipt.entries > 10000
    || !Number.isSafeInteger(receipt.fileBytes) || receipt.fileBytes < 0 || receipt.fileBytes > 240 * 1024 * 1024) fail('warm_physical_receipt_invalid');
  return Object.freeze(receipt);
};
const nativeReceiver = async (control, payload) => {
  const { receiveStrictBackup } = await import('../../strict-receiver.mjs');
  return receiveStrictBackup(control, payload);
};
function createDriver({ preflight, receive, limits, leaseMs = WARM_LEASE_MS }) {
  if (!Number.isSafeInteger(leaseMs) || leaseMs < 1 || leaseMs > WARM_LEASE_MS) fail('warm_lease_invalid');
  return async options => {
    let wire, warmLease, bodyStarted = false, bindAdmitted = false, completed = false, failureValue;
    let payloadInput, controlInput, announcementOutput;
    const started = performance.now(); let emit = () => {};
    const report = (phase, code) => {
      try { emit(Object.freeze({ schema: 'soty.warm-receiver-phase.v1', phase, code,
        elapsedMs: Math.max(0, Math.floor(performance.now() - started)), bodyStarted })); } catch {}
    };
    try {
      const captured = dataRecord(options, ['spec', 'controlInput', 'payloadInput', 'announcementOutput', 'signal', 'emit']);
      const spec = captureSpec(captured.spec); emit = captured.emit;
      if (typeof emit !== 'function') fail('warm_port_invalid');
      payloadInput = captured.payloadInput; controlInput = captured.controlInput; announcementOutput = captured.announcementOutput;
      if (payloadInput === controlInput || controlInput === announcementOutput || payloadInput === announcementOutput
        || !(controlInput instanceof Readable) || !(announcementOutput instanceof Writable)) fail('warm_channels_invalid');
      unreadPayload(payloadInput);
      const signal = captureSignal(captured.signal), leaseDeadline = started + leaseMs;
      wire = new WireOwner({ signal, limits, absoluteLeaseDeadline: leaseDeadline, leaseOnly: true });
      // Own without a readable/data listener: no pre-body _read or pipe from payload.
      wire.own(payloadInput); wire.own(controlInput); wire.own(announcementOutput);
      const checkRead0 = () => { wire.check(); if (!bindAdmitted) unreadPayload(payloadInput); };
      report('receiver_start', 'pending');
      warmLease = await preflight(spec, checkRead0); checkRead0();
      await warmLease.recheck(); checkRead0();
      report('warm_read0', 'ok');
      await wire.writeFrame(announcementOutput, warmFrame(spec)); checkRead0();
      // Warm lease is absolute and independent of the sender/body clocks.
      const begin = await wire.readFrame(controlInput, { requireEOF: false }); checkRead0();
      captureBegin(begin, spec); await warmLease.recheck(); checkRead0();
      wire.release();
      wire = new WireOwner({ signal, limits, absoluteLeaseDeadline: leaseDeadline });
      wire.own(payloadInput); wire.own(controlInput); wire.own(announcementOutput);
      report('control_begin_read0', 'ok');
      await wire.writeFrame(announcementOutput, beginAckFrame(spec)); checkRead0();
      // Bind needs exact frame AND terminal EOF, distinct from BEGIN_CONTROL.
      const raw = await wire.readFrame(controlInput, { allowPreviouslyOwnedRead: true }); checkRead0();
      const binding = captureBinding(raw, spec);
      await warmLease.recheck(); checkRead0();
      report('control_bound_read0', 'ok');
      // This is the receiver's actual local bind point. Body may be queued by a
      // legitimate peer once it sees bound+EOF; the driver still does not read
      // it until acknowledgement and the final identity recheck settle.
      bindAdmitted = true;
      await wire.writeFrame(announcementOutput, boundFrame(binding)); checkRead0();
      await wire.end(announcementOutput); checkRead0();
      // The root/source identity window includes both acknowledgement awaits.
      await warmLease.recheck(); checkRead0();
      const control = Object.freeze({ targetId: binding.targetId, expectedManifestSha256: binding.expectedManifestSha256,
        sourceWitness: binding.sourceWitness });
      wire.handoffBody(); bodyStarted = true; report('body_receiver_start', 'pending');
      // Telemetry may synchronously cancel or outlive the lease. No port is
      // invoked until the existing owner fence has been checked again.
      wire.check();
      // The default handler is the unchanged physical receiver: its own native
      // PassThrough/parser/sink, 120000/15000 clocks, whole-manifest/readback.
      const receipt = safeReceipt(await receive(control, payloadInput), binding);
      wire.check();
      completed = true; report('physical_receipt', 'ok'); return receipt;
    } catch (error) {
      failureValue = wire?.failure || failure(failureCode(error));
      wire?.stop(failureValue.code);
      report('receiver_failed', failureValue.code);
      throw failureValue;
    } finally {
      try { await warmLease?.close(); }
      catch { completed = false; failureValue = failure('warm_cleanup_pending'); }
      if (completed) {
        try { wire.check(); }
        catch (error) { completed = false; failureValue = wire.failure || failure(failureCode(error)); }
      }
      // No claim that a destroyed stream is an absent Docker helper or that
      // arbitrary in-flight host/syscall work stopped; an outer watchdog is required.
      if (!completed) {
        for (const stream of [payloadInput, controlInput, announcementOutput]) { try { stream?.destroy(); } catch {} }
      }
      wire?.release();
      if (failureValue) throw failureValue;
    }
  };
}
export const runWarmReceiver = createDriver({ preflight: prepareLinuxWarm, receive: nativeReceiver, limits: PRODUCTION_CLOCK });

// CLOSED test seam, deliberately not called by runWarmReceiver.
export function createWarmReceiverForSyntheticFixture({ preflight, receive, limits = PRODUCTION_CLOCK, leaseMs = WARM_LEASE_MS }) {
  if (typeof preflight !== 'function' || typeof receive !== 'function') fail('warm_port_invalid');
  return createDriver({ preflight, receive, limits, leaseMs });
}

function validateWarmAck(value, spec, schema = 'soty.receiver-warm.v1') {
  const ack = dataRecord(value, ['schema', 'transaction', 'nonce', 'targetId', 'image', 'profile', 'sourcePins', 'inputBytes']);
  const captured = captureSpec(Object.fromEntries(['transaction', 'nonce', 'targetId', 'image', 'profile', 'sourcePins'].map(key => [key, ack[key]])));
  if (ack.schema !== schema || ack.inputBytes !== 0
    || ['transaction', 'nonce', 'targetId', 'image', 'profile'].some(key => captured[key] !== spec[key])
    || SOURCE_KEYS.some(key => captured.sourcePins[key] !== spec.sourcePins[key])) fail('warm_ack_invalid');
  return schema === 'soty.receiver-warm.v1' ? warmFrame(spec) : beginAckFrame(spec);
}
function validateBoundAck(value, expected) {
  const ack = dataRecord(value, ['schema', 'transaction', 'nonce', 'targetId', 'image', 'profile', 'sourcePins',
    'expectedSha256', 'expectedManifestSha256', 'sourceWitness', 'inputBytes']);
  if (ack.schema !== 'soty.receiver-bound.v1' || ack.inputBytes !== 0) fail('warm_ack_invalid');
  const binding = captureBinding(Object.fromEntries(Object.keys(ack).filter(key => key !== 'inputBytes')
    .map(key => [key, key === 'schema' ? 'soty.receiver-bind.v1' : ack[key]])), expected);
  if (binding.expectedSha256 !== expected.expectedSha256 || binding.expectedManifestSha256 !== expected.expectedManifestSha256
    || ['generationId', 'checkpointSha256', 'inventorySha256'].some(key => binding.sourceWitness[key] !== expected.sourceWitness[key]))
    fail('warm_ack_invalid');
  return boundFrame(binding);
}

// This is a typed *wire peer*, not a Docker/host ownership lease. Only the actual
// sender may call bindInsideAuthenticatedHook after its full first GCM pass.
function createClient({ spec: inputSpec, announcementInput, controlOutput, signal, limits, leaseMs = WARM_LEASE_MS }) {
  if (!Number.isSafeInteger(leaseMs) || leaseMs < 1 || leaseMs > WARM_LEASE_MS) fail('warm_lease_invalid');
  const spec = captureSpec(inputSpec); captureSignal(signal);
  if (signalIsAborted(signal)) fail('warm_cancelled');
  if (!(announcementInput instanceof Readable) || !(controlOutput instanceof Writable) || announcementInput === controlOutput)
    fail('warm_channels_invalid');
  const leaseDeadline = performance.now() + leaseMs;
  const localAbort = new AbortController();
  let phase = 'created', wire, leaseTimer;
  const parentAbort = () => cancel();
  const cancel = (code = 'warm_cancelled') => {
    phase = 'cancelled'; clearTimeout(leaseTimer); wire?.stop(code);
    localAbort.abort();
    if (signal !== undefined) EventTarget.prototype.removeEventListener.call(signal, 'abort', parentAbort);
    if (!wire) { try { announcementInput.destroy(); controlOutput.destroy(); } catch {} }
  };
  if (signal !== undefined) EventTarget.prototype.addEventListener.call(signal, 'abort', parentAbort, { once: true });
  leaseTimer = setTimeout(() => cancel('warm_lease_expired'), leaseMs + 1);
  return Object.freeze({
    // Compose this native signal with the actual sender/host operation. It is
    // cancellation only, never restore/host authority or cleanup proof.
    signal: localAbort.signal,
    async warmBeforeServingStop() {
      if (phase !== 'created') fail('warm_stage_invalid'); phase = 'warming';
      try {
        wire = new WireOwner({ signal: localAbort.signal, limits, absoluteLeaseDeadline: leaseDeadline, leaseOnly: true }); wire.own(controlOutput);
        const raw = await wire.readFrame(announcementInput, { requireEOF: false });
        const ack = validateWarmAck(raw, spec); wire.check();
        if (phase !== 'warming') fail('warm_stage_invalid'); phase = 'warm'; return ack;
      } catch (error) { cancel(); throw failure(failureCode(error)); }
      finally { wire?.release(); wire = null; }
    },
    async bindInsideAuthenticatedHook(receipt, inputFence) {
      if (phase !== 'warm') fail('warm_stage_invalid');
      const expected = bindingFrame(spec, receipt), fence = dataRecord(inputFence, ['check', 'authenticated']);
      const check = () => {
        if (typeof fence.check !== 'function') fail('warm_fence_invalid'); fence.check();
        const validated = bindingFrame(spec, dataRecord(fence.authenticated, ['expectedSha256', 'expectedManifestSha256', 'sourceWitness']));
        if (validated.expectedSha256 !== expected.expectedSha256 || validated.expectedManifestSha256 !== expected.expectedManifestSha256
          || ['generationId', 'checkpointSha256', 'inventorySha256'].some(key => validated.sourceWitness[key] !== expected.sourceWitness[key]))
          fail('warm_authenticated_source_mismatch');
        if (phase !== 'binding' && phase !== 'warm') fail('warm_stage_invalid');
      };
      try {
        check();
        if (announcementInput.readableLength !== 0 || announcementInput.readableEnded || announcementInput.destroyed)
          fail('warm_unsolicited_frame');
        phase = 'binding'; wire = new WireOwner({ signal: localAbort.signal, limits, check, absoluteLeaseDeadline: leaseDeadline }); wire.own(announcementInput);
        await wire.writeFrame(controlOutput, beginFrame(spec)); check();
        const beginAck = await wire.readFrame(announcementInput, { requireEOF: false, allowPreviouslyOwnedRead: true });
        validateWarmAck(beginAck, spec, 'soty.receiver-control-begin-ack.v1'); check();
        await wire.writeFrame(controlOutput, expected); check();
        await wire.end(controlOutput); check();
        const raw = await wire.readFrame(announcementInput, { allowPreviouslyOwnedRead: true });
        const ack = validateBoundAck(raw, expected); check(); phase = 'bound'; return ack;
      } catch (error) { cancel(); throw failure(failureCode(error)); }
      finally { wire?.release(); wire = null; }
    },
    cancel: () => cancel(),
  });
}
export function createWarmControlClient(options) {
  const captured = dataRecord(options, ['spec', 'announcementInput', 'controlOutput', 'signal']);
  return createClient({ ...captured, limits: PRODUCTION_CLOCK });
}
export function createWarmControlClientForSyntheticFixture(options) {
  const keys = ['spec', 'announcementInput', 'controlOutput', 'signal', 'limits'];
  if (Object.hasOwn(options, 'leaseMs')) keys.push('leaseMs');
  return createClient(dataRecord(options, keys));
}
