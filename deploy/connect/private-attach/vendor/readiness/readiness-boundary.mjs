// SOURCE PROPOSAL: trusted host ports; single transaction per owned boundary.
import { types } from 'node:util';
const HEX32 = /^[a-f0-9]{32}$/, HEX64 = /^[a-f0-9]{64}$/;
const IMAGE = /^sha256:[a-f0-9]{64}$/;
const CANCELLED = Symbol('readiness_cancelled');
const NativePromise = Promise, nativeThen = Promise.prototype.then;
const nativeRace = Promise.race.bind(Promise), promisePrototype = Promise.prototype;
const isProxy = types.isProxy, isPromise = types.isPromise;
const ownDescriptors = Object.getOwnPropertyDescriptors, ownKeys = Reflect.ownKeys;
const nativeAborted = Object.getOwnPropertyDescriptor(AbortSignal.prototype, 'aborted').get;
const fail = code => { throw Object.assign(new Error(code), { code }); };
const ownRecord = (value, required, optional = [], code = 'readiness_record_invalid') => {
  if (!value || typeof value !== 'object' || isProxy(value) || Array.isArray(value)) fail(code);
  const descriptors = ownDescriptors(value), keys = ownKeys(descriptors);
  if (keys.some(key => typeof key !== 'string') || keys.length > required.length + optional.length
    || required.some(key => !Object.hasOwn(descriptors, key))
    || keys.some(key => !required.includes(key) && !optional.includes(key))) fail(code);
  const out = Object.create(null);
  for (const key of keys) {
    const descriptor = descriptors[key];
    if (!Object.hasOwn(descriptor, 'value') || descriptor.enumerable !== true) fail(code);
    out[key] = descriptor.value;
  }
  return Object.freeze(out);
};
const stringMatches = (value, pattern) => typeof value === 'string' && pattern.test(value);
const envelope = (ok, value, code) => Object.freeze(Object.assign(Object.create(null), { ok, value, code }));
const witnessRecord = (value, code) => {
  const record = ownRecord(value, ['generationId','checkpointSha256','inventorySha256'], [], code);
  if (!stringMatches(record.generationId, HEX32) || !stringMatches(record.checkpointSha256, HEX64)
    || !stringMatches(record.inventorySha256, HEX64)) fail(code);
  return record;
};
const backupRecord = (value, code) => {
  const record = ownRecord(value, ['expectedSha256','expectedManifestSha256','sourceWitness'], [], code);
  if (!stringMatches(record.expectedSha256, HEX64) || !stringMatches(record.expectedManifestSha256, HEX64)) fail(code);
  return Object.freeze(Object.assign(Object.create(null), record, { sourceWitness: witnessRecord(record.sourceWitness, code) }));
};
export function createReadinessBoundary(input) {
  const ports = ownRecord(input, ['prepareBeforeStop','bindAfterAuthentication'], ['emit'], 'readiness_port_invalid');
  const prepareBeforeStop = ports.prepareBeforeStop, bindAfterAuthentication = ports.bindAfterAuthentication;
  const emit = Object.hasOwn(ports, 'emit') ? ports.emit : () => {};
  if ([prepareBeforeStop, bindAfterAuthentication, emit].some(fn => typeof fn !== 'function' || isProxy(fn))) fail('readiness_port_invalid');
  const leases = new WeakMap();
  // Reserving is synchronous and precedes emit/port calls. Once prepare is
  // actually invoked, its slot is PERMANENTLY consumed, including all failures.
  let reserved = false, consumed = false;
  const report = (phase, code) => { try { emit(Object.freeze({ schema: 'soty.restore-readiness-phase.v1', phase, code })); } catch {} };
  const get = lease => { const state = leases.get(lease); if (!state) fail('readiness_unowned_lease'); return state; };
  const phaseFence = (state, expected) => {
    if (state.externalSignal !== undefined && nativeAborted.call(state.externalSignal)) cancel(state);
    if (state.cancelled) fail('readiness_cancelled');
    if (state.phase !== expected) fail('readiness_stage_invalid');
  };
  const requestPortAbort = state => {
    if (!nativeAborted.call(state.controller.signal)) AbortController.prototype.abort.call(state.controller);
  };
  const cancel = state => {
    if (state.cancelled) return;
    state.cancelled = true; state.phase = 'cancelled';
    state.detachSignal?.(); state.resolveCancellation(CANCELLED);
    requestPortAbort(state); report('cancelled', 'admission_closed');
  };
  const createState = (spec, signal) => {
    let resolveCancellation;
    const cancellation = new NativePromise(resolve => { resolveCancellation = resolve; });
    const state = { spec, phase: 'warming', cancelled: false, receipt: null, externalSignal: signal,
      controller: new AbortController(), cancellation, resolveCancellation, detachSignal: null };
    if (signal !== undefined) {
      const listener = () => cancel(state);
      state.detachSignal = () => {
        EventTarget.prototype.removeEventListener.call(signal, 'abort', listener);
        state.detachSignal = null;
      };
      EventTarget.prototype.addEventListener.call(signal, 'abort', listener, { once: true });
      if (nativeAborted.call(signal)) cancel(state);
    }
    return state;
  };
  const waitPort = async (state, expectedPhase, invoke, capture, shapeCode) => {
    phaseFence(state, expectedPhase);
    const pending = new NativePromise(resolve => {
      const complete = value => {
        if (state.cancelled) { resolve(envelope(false, undefined, 'readiness_cancelled')); return; }
        try {
          const record = capture(value);
          phaseFence(state, expectedPhase);
          resolve(envelope(true, record));
        } catch { resolve(envelope(false, undefined, shapeCode)); }
      };
      let returned;
      try { phaseFence(state, expectedPhase); returned = invoke(); }
      catch { resolve(envelope(false, undefined, 'readiness_port_failed')); return; }
      // Never hand raw callback records to Promise resolution/then assimilation.
      // Ports may return an inert record or an ordinary same-realm native Promise.
      if (isProxy(returned)) { resolve(envelope(false, undefined, shapeCode)); return; }
      if (isPromise(returned)) {
        const descriptors = ownDescriptors(returned);
        const constructor = Object.hasOwn(descriptors, 'constructor') ? descriptors.constructor : undefined;
        if (Object.getPrototypeOf(returned) !== promisePrototype
          || (constructor && (!Object.hasOwn(constructor, 'value') || constructor.value !== NativePromise))) {
          resolve(envelope(false, undefined, 'readiness_port_failed')); return;
        }
        try { nativeThen.call(returned, complete, () => resolve(envelope(false, undefined, 'readiness_port_failed'))); }
        catch { resolve(envelope(false, undefined, 'readiness_port_failed')); }
      } else complete(returned);
    });
    const outcome = await nativeRace([pending, state.cancellation]);
    phaseFence(state, expectedPhase);
    if (outcome === CANCELLED) fail('readiness_cancelled');
    if (!outcome.ok) fail(outcome.code);
    // Return the owned null-prototype envelope, NOT a raw record/thenable.
    return outcome;
  };
  return Object.freeze({
    async warmBeforeServingStop(spec, control = undefined) {
      if (reserved || consumed) fail('readiness_capacity_exhausted');
      const captured = ownRecord(spec, ['transaction','nonce','image','receiverSourceSha256'], [], 'readiness_spec_invalid');
      if (!stringMatches(captured.transaction, HEX32) || !stringMatches(captured.nonce, HEX32)
        || !stringMatches(captured.image, IMAGE) || !stringMatches(captured.receiverSourceSha256, HEX64)) fail('readiness_spec_invalid');
      let signal;
      if (control !== undefined) {
        signal = ownRecord(control, ['signal'], [], 'readiness_signal_invalid').signal;
        try { if (isProxy(signal)) fail('readiness_signal_invalid'); nativeAborted.call(signal); }
        catch { fail('readiness_signal_invalid'); }
      }
      reserved = true;
      const state = createState(captured, signal); let warmed = false;
      try {
        report('warming_before_stop', 'pending'); phaseFence(state, 'warming');
        await waitPort(state, 'warming', () => {
          phaseFence(state, 'warming'); consumed = true;
          return prepareBeforeStop(captured, Object.freeze({ signal: state.controller.signal }));
        }, value => {
          const warm = ownRecord(value, ['transaction','nonce','image','receiverSourceSha256','inputBytes','verifiedBeforeStop'], [], 'readiness_warm_invalid');
          if (warm.transaction !== captured.transaction || warm.nonce !== captured.nonce || warm.image !== captured.image
            || warm.receiverSourceSha256 !== captured.receiverSourceSha256 || !Object.is(warm.inputBytes, 0)
            || warm.verifiedBeforeStop !== true) fail('readiness_warm_invalid');
          return warm;
        }, 'readiness_warm_invalid');
        phaseFence(state, 'warming');
        report('warm_read0_before_stop', 'ok'); phaseFence(state, 'warming');
        const lease = Object.freeze(Object.create(null));
        // No callback/property read occurs after the final fence and before publication.
        state.phase = 'warm'; leases.set(lease, state); warmed = true; return lease;
      } finally {
        reserved = false; // Consumed never resets, including late port settlement.
        if (!warmed) {
          state.detachSignal?.(); if (!state.cancelled) state.phase = 'failed'; requestPortAbort(state);
        }
      }
    },
    noteServingStopped(lease) {
      const state = get(lease);
      if (state.cancelled || state.phase !== 'warm') fail('readiness_stage_invalid');
      state.phase = 'stopped'; report('serving_stopped', 'ok'); phaseFence(state, 'stopped');
    },
    captureLaterBackup(lease, inputReceipt) {
      const state = get(lease);
      if (state.cancelled || state.phase !== 'stopped') fail('readiness_backup_invalid');
      const receipt = backupRecord(inputReceipt, 'readiness_backup_invalid');
      if (receipt.sourceWitness.generationId !== state.spec.transaction) fail('readiness_backup_invalid');
      phaseFence(state, 'stopped'); state.receipt = receipt; state.phase = 'captured';
      report('backup_captured', 'ok'); phaseFence(state, 'captured');
    },
    beforeBody(lease) {
      const state = get(lease);
      if (state.cancelled || state.phase !== 'captured') fail('readiness_stage_invalid');
      return async inputFence => {
        if (state.cancelled || state.phase !== 'captured') fail('readiness_stage_invalid');
        const fence = ownRecord(inputFence, ['check','authenticated'], [], 'readiness_stage_invalid');
        if (typeof fence.check !== 'function' || isProxy(fence.check)) fail('readiness_stage_invalid');
        fence.check(); phaseFence(state, 'captured');
        const authenticated = backupRecord(fence.authenticated, 'readiness_authenticated_source_mismatch');
        if (authenticated.expectedSha256 !== state.receipt.expectedSha256
          || authenticated.expectedManifestSha256 !== state.receipt.expectedManifestSha256
          || ['generationId','checkpointSha256','inventorySha256'].some(key => authenticated.sourceWitness[key] !== state.receipt.sourceWitness[key])) fail('readiness_authenticated_source_mismatch');
        phaseFence(state, 'captured'); state.phase = 'binding';
        report('binding_after_authentication', 'pending'); phaseFence(state, 'binding');
        const expected = Object.freeze({ ...state.spec, ...state.receipt });
        const portFence = Object.freeze({ authenticated, signal: state.controller.signal,
          check: () => { phaseFence(state, 'binding'); fence.check(); phaseFence(state, 'binding'); } });
        try {
          const result = await waitPort(state, 'binding', () => bindAfterAuthentication(expected, portFence), value => {
            const ready = ownRecord(value, ['transaction','nonce','expectedSha256','expectedManifestSha256','generationId','checkpointSha256','inventorySha256','inputBytes'], [], 'readiness_bound_invalid');
            if (ready.transaction !== expected.transaction || ready.nonce !== expected.nonce || !Object.is(ready.inputBytes, 0)
              || ready.expectedSha256 !== expected.expectedSha256 || ready.expectedManifestSha256 !== expected.expectedManifestSha256
              || ready.generationId !== expected.sourceWitness.generationId || ready.checkpointSha256 !== expected.sourceWitness.checkpointSha256
              || ready.inventorySha256 !== expected.sourceWitness.inventorySha256) fail('readiness_bound_invalid');
            return ready;
          }, 'readiness_bound_invalid');
          if (!result.ok) fail('readiness_bound_invalid');
          fence.check(); phaseFence(state, 'binding');
          report('bound_read0_inside_sender_budget', 'ok');
          fence.check(); phaseFence(state, 'binding');
          state.detachSignal?.();
          fence.check(); phaseFence(state, 'binding');
          // Atomic local commit: no callback/property validation follows this fence.
          state.phase = 'body_admitted';
        } finally {
          if (state.phase !== 'body_admitted') {
            state.detachSignal?.();
            if (!state.cancelled) state.phase = 'failed'; requestPortAbort(state);
          }
        }
      };
    },
    cancel(lease) { cancel(get(lease)); },
  });
}
