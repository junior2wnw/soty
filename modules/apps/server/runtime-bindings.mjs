import { randomBytes } from 'node:crypto';
import { AppsError, assertApps, appId, appPort, runtimePath, textId, connectorKey, FRAME_BYTES } from './protocol.mjs';
import { RUNTIME_PROFILE, SCOPED_RUNTIME_PROFILE, supportedRuntimeProfile, runtimeTargetDigest } from './schema.mjs';

export const RUNTIME_BINDING_LIMITS = Object.freeze({ apps: 100, pending: 4, ackMs: 5_000, preparationMs: 30_000, sendBytes: 4 * 1024 * 1024 });
const controlTypes = new Set(['binding-ack', 'binding-rejected', 'bound-observation', 'target-prepared', 'target-rejected']);
const bindingErrors = new Set(['invalid_app_port', 'invalid_app_path', 'unsupported_profile']);
const preparationErrors = new Set([...bindingErrors, 'app_prepare_busy']);
const nonce = () => randomBytes(32).toString('base64url');
const opaque = value => typeof value === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(value);
const digest = value => typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value);
const positive = value => Number.isSafeInteger(value) && value >= 1;
const clockValid = value => Number.isSafeInteger(value) && value >= 0 && value <= Number.MAX_SAFE_INTEGER - RUNTIME_BINDING_LIMITS.preparationMs;
const shape = (value, keys) => value && typeof value === 'object' && !Array.isArray(value) && Object.keys(value).every(key => keys.includes(key));
const pins = target => ({ appId: target.appId, revision: target.revision, digest: target.digest, profile: target.profile });
const pinsMatch = (value, target) => value.appId === target.appId && value.revision === target.revision && value.digest === target.digest && value.profile === target.profile;
const fingerprint = target => JSON.stringify([target.appId, target.revision, target.ownerAccountId, target.connectorKey,
  target.port, target.entryPath, target.profile, target.digest]);

/** Socket-owned configuration admission. Database authority remains with the
 * caller; an ACK is configuration evidence, never HTTP health or permission. */
export function createRuntimeBindings({ channels, send, now = Date.now, blockedPorts = [], onBindingInvalidated = () => {}, scopedTarget,
  timers = { setTimeout, clearTimeout } }) {
  assertApps(channels instanceof Map && typeof send === 'function' && typeof now === 'function'
    && typeof onBindingInvalidated === 'function' && typeof timers?.setTimeout === 'function'
    && typeof timers?.clearTimeout === 'function', 'app_binding_dependencies_required', 500);
  const states = new WeakMap(), activeStates = new Set(), references = new WeakMap(), proofs = new WeakMap();
  let closed = false;
  const timestamp = () => { const value = now(); assertApps(clockValid(value), 'app_binding_clock_invalid', 500); return value; };
  const later = (callback, ms) => { const timer = timers.setTimeout(callback, ms); timer?.unref?.(); return timer; };
  const clear = timer => { if (timer !== undefined) timers.clearTimeout(timer); };
  function current(state) {
    return !closed && !state.dropped && channels.get(state.key) === state.channel && state.channel.ws === state.ws
      && state.ws.readyState === 1 && state.channel.bindingVersion === 2 && state.channel.channelId === state.channelId
      && state.channel.identity?.linkId === state.identity.linkId && state.channel.identity?.hostDeviceId === state.identity.hostDeviceId
      && state.channel.identity?.connectorId === state.identity.connectorId && state.channel.key === state.key;
  }
  function context(channel) {
    assertApps(!closed, 'apps_closed', 503);
    assertApps(channel && channels.get(channel.key) === channel && channel.ws?.readyState === 1, 'app_offline', 503);
    assertApps(channel.bindingVersion === 2, 'app_source_protocol_required', 503);
    assertApps(opaque(channel.channelId) && connectorKey(channel.identity) === channel.key
      && channel.observations instanceof Map, 'app_bad_binding_channel', 500);
    let state = states.get(channel);
    if (!state) {
      state = { channel, ws: channel.ws, key: channel.key, channelId: channel.channelId,
        identity: Object.freeze({ ...channel.identity }), bindings: new Map(), preparing: new Map(), dropped: false };
      states.set(channel, state); activeStates.add(state);
    }
    assertApps(current(state), 'app_offline', 503); return state;
  }
  function normalizeTarget(state, value, { candidate = false } = {}) {
    assertApps(value && typeof value === 'object', 'app_invalid_binding_target', 500);
    const id = appId(value.appId), ownerAccountId = textId(value.ownerAccountId);
    assertApps(positive(value.revision) && digest(value.digest) && value.connectorKey === state.key
      && typeof value.profile === 'string' && value.profile.length <= 80 && typeof value.entryPath === 'string'
      && value.entryPath.length <= 8192 && Number.isSafeInteger(value.port), 'app_invalid_binding_target', 500);
    const target = Object.freeze({ appId: id, revision: value.revision, ownerAccountId, connectorKey: state.key,
      port: value.port, entryPath: value.entryPath, profile: value.profile, digest: value.digest });
    assertApps(target.digest === runtimeTargetDigest(target), 'app_invalid_binding_digest', 500);
    let error = null;
    if (target.profile !== RUNTIME_PROFILE) {
      try {
        if (target.profile !== SCOPED_RUNTIME_PROFILE || !state.channel.runtimeProfiles?.includes(target.profile)
          || typeof scopedTarget !== 'function' || scopedTarget(target, { candidate }) !== true) error = 'unsupported_profile';
      } catch { error = 'unsupported_profile'; }
    }
    if (!error) {
      try { appPort(target.port, blockedPorts); } catch { error = 'invalid_app_port'; }
      if (!error) try { runtimePath(target.entryPath); } catch { error = 'invalid_app_path'; }
    }
    const { connectorKey: _internal, ...wireTarget } = target;
    return { target, wireTarget: Object.freeze(wireTarget), fingerprint: fingerprint(target), error };
  }
  function invalidate(state, entry) {
    clear(entry.timer); entry.timer = undefined;
    state.channel.observations.delete(entry.target.appId);
    // Clear admission before calling user code. A throwing notifier can never
    // leave the old reference live, even when unrelated applications survive.
    entry.status = 'unavailable';
    onBindingInvalidated(state.channel, entry.target.appId);
  }
  function expireBinding(state, entry) {
    if (state.bindings.get(entry.target.appId) !== entry || entry.status !== 'pending') return;
    entry.status = 'unavailable'; entry.reason = 'app_binding_timeout'; clear(entry.timer); entry.timer = undefined;
    state.channel.observations.delete(entry.target.appId);
  }
  function bindingLive(state, entry) {
    if (entry?.status === 'pending') {
      const time = timestamp(); if (time < entry.createdAt || time >= entry.deadline) expireBinding(state, entry);
    }
    return current(state) && state.bindings.get(entry?.target.appId) === entry && entry?.status === 'bound';
  }
  function finishPreparation(state, entry, error, value) {
    if (state.preparing.get(entry.nonce) !== entry) return;
    state.preparing.delete(entry.nonce); clear(entry.timer); entry.signal?.removeEventListener('abort', entry.abort);
    if (error) entry.reject(error); else entry.resolve(value);
  }
  function drop(channel) {
    const state = states.get(channel); if (!state || state.dropped) return;
    state.dropped = true; activeStates.delete(state);
    let firstError;
    for (const entry of state.bindings.values()) {
      try { invalidate(state, entry); } catch (error) { firstError ??= error; }
    }
    state.bindings.clear();
    for (const entry of [...state.preparing.values()]) finishPreparation(state, entry, new AppsError('app_offline', 503));
    // Cleanup is complete even if the host notification was faulty.
    if (firstError) throw firstError;
  }
  function failedSend(state, code) {
    try { drop(state.channel); } finally { state.ws.terminate?.(); }
    throw new AppsError(code, 503);
  }
  function sendControl(state, frame, budget) {
    if (!current(state)) return failedSend(state, 'app_offline');
    const bytes = Buffer.byteLength(JSON.stringify(frame), 'utf8'), buffered = state.ws.bufferedAmount;
    assertApps(bytes <= FRAME_BYTES, 'app_binding_frame_too_large', 500);
    if (!Number.isFinite(buffered) || buffered < 0 || buffered + bytes > RUNTIME_BINDING_LIMITS.sendBytes
      || (budget && budget.bytes + bytes > RUNTIME_BINDING_LIMITS.sendBytes)) return failedSend(state, 'app_binding_backpressure');
    if (budget) budget.bytes += bytes;
    let accepted;
    try { accepted = send(state.channel, frame); } catch { return failedSend(state, 'app_offline'); }
    if (accepted !== true || !current(state)) return failedSend(state, 'app_offline');
  }
  function sync(channel, targets) {
    const state = context(channel);
    assertApps(Array.isArray(targets) && targets.length <= RUNTIME_BINDING_LIMITS.apps, 'app_binding_capacity', 429);
    const desired = new Map();
    for (const value of targets) {
      const normalized = normalizeTarget(state, value);
      assertApps(!desired.has(normalized.target.appId), 'app_duplicate_binding', 500); desired.set(normalized.target.appId, normalized);
    }
    const removed = [], budget = { bytes: 0 };
    for (const [id, entry] of state.bindings) {
      bindingLive(state, entry);
      const next = desired.get(id);
      if (!next || next.fingerprint !== entry.fingerprint || entry.status === 'unavailable') {
        state.bindings.delete(id); removed.push({ appId: id, syncId: entry.syncId }); invalidate(state, entry);
      }
    }
    if (removed.length) sendControl(state, { type: 'binding-remove', channelId: state.channelId, bindings: removed }, budget);
    for (const [id, value] of desired) {
      let entry = state.bindings.get(id);
      if (entry?.status === 'bound' || entry?.status === 'rejected') continue;
      if (!entry) {
        const createdAt = timestamp();
        entry = { ...value, syncId: nonce(), createdAt, deadline: createdAt + RUNTIME_BINDING_LIMITS.ackMs,
          status: value.error ? 'rejected' : 'pending', reason: value.error || null, timer: undefined };
        const reference = Object.freeze({ channelId: state.channelId, syncId: entry.syncId, ...pins(entry.target), connectorKey: state.key });
        entry.reference = reference; references.set(reference, { state, entry }); state.bindings.set(id, entry);
        if (entry.status === 'pending') entry.timer = later(() => expireBinding(state, entry), RUNTIME_BINDING_LIMITS.ackMs);
      }
      if (entry.status === 'pending') sendControl(state, { type: 'binding-set', channelId: state.channelId,
        syncId: entry.syncId, target: entry.wireTarget }, budget);
    }
  }
  function validPins(frame) {
    assertApps(typeof frame.appId === 'string' && /^app-[a-f0-9]{32}$/u.test(frame.appId) && positive(frame.revision)
      && digest(frame.digest) && supportedRuntimeProfile(frame.profile), 'app_bad_binding_frame');
  }
  function validObservation(frame) {
    assertApps((frame.state === 'responding' && Number.isSafeInteger(frame.httpStatus) && frame.httpStatus >= 200 && frame.httpStatus <= 499)
      || (frame.state === 'unreachable' && (frame.httpStatus === null || (Number.isSafeInteger(frame.httpStatus)
        && frame.httpStatus >= 500 && frame.httpStatus <= 599))), 'app_bad_binding_observation');
  }
  function handleFrame(channel, frame) {
    if (!frame || !controlTypes.has(frame.type)) return false;
    // A late handler from a replaced socket may run before its close event.
    // It can never create state or modify the replacement's observations.
    if (closed || !channel || channels.get(channel.key) !== channel || channel.ws?.readyState !== 1) return true;
    const state = context(channel), candidate = frame.type.startsWith('target-');
    const fields = ['type', 'channelId', candidate ? 'nonce' : 'syncId', 'appId', 'revision', 'digest', 'profile'];
    if (frame.type.endsWith('rejected')) fields.push('error');
    if (frame.type === 'bound-observation' || frame.type === 'target-prepared') fields.push('state', 'httpStatus');
    assertApps(shape(frame, fields) && frame.channelId === state.channelId && opaque(candidate ? frame.nonce : frame.syncId), 'app_bad_binding_frame');
    validPins(frame);
    if (frame.type.endsWith('rejected')) assertApps((candidate ? preparationErrors : bindingErrors).has(frame.error), 'app_bad_binding_frame');
    if (frame.type === 'bound-observation' || frame.type === 'target-prepared') validObservation(frame);
    if (candidate) {
      const entry = state.preparing.get(frame.nonce); if (!entry) return true;
      assertApps(pinsMatch(frame, entry.target), 'app_bad_binding_frame');
      const time = timestamp();
      if (time < entry.createdAt || time >= entry.deadline || entry.signal?.aborted) {
        finishPreparation(state, entry, new AppsError('apps_source_probe_timeout', 504)); return true;
      }
      if (frame.type === 'target-rejected') {
        finishPreparation(state, entry, new AppsError(frame.error, frame.error === 'app_prepare_busy' ? 429 : 400)); return true;
      }
      if (frame.state !== 'responding') { finishPreparation(state, entry, new AppsError('app_source_unreachable', 503)); return true; }
      const evidence = Object.freeze({});
      proofs.set(evidence, { state, target: entry.target, fingerprint: entry.fingerprint, actor: entry.actor,
        preparationId: entry.preparationId, createdAt: entry.createdAt, expiresAt: entry.createdAt + RUNTIME_BINDING_LIMITS.preparationMs, signal: entry.signal });
      finishPreparation(state, entry, null, evidence); return true;
    }
    const entry = state.bindings.get(frame.appId);
    if (!entry || entry.syncId !== frame.syncId) return true;
    assertApps(pinsMatch(frame, entry.target), 'app_bad_binding_frame');
    bindingLive(state, entry);
    if (frame.type === 'binding-ack') {
      if (entry.status === 'pending') { entry.status = 'bound'; entry.reason = null; clear(entry.timer); entry.timer = undefined; }
      return true;
    }
    if (frame.type === 'binding-rejected') {
      if (entry.status !== 'pending') return true;
      entry.status = 'rejected'; entry.reason = frame.error; clear(entry.timer); entry.timer = undefined;
      state.channel.observations.delete(frame.appId); return true;
    }
    if (!bindingLive(state, entry)) return true;
    state.channel.observations.set(frame.appId, { state: frame.state === 'responding' ? 'ready' : 'stopped', at: timestamp(),
      targetRevision: entry.target.revision, targetDigest: entry.target.digest, evidence: 'connector-v2-observation', httpStatus: frame.httpStatus });
    return true;
  }
  function requireBinding(channel, decision) {
    const state = context(channel);
    assertApps(decision && [1, 2].includes(decision.requiredBindingVersion) && decision.route?.connectorKey === state.key,
      'app_binding_changed', 503);
    const entry = state.bindings.get(decision.appId);
    assertApps(entry && entry.target.revision === decision.targetRevision && entry.target.digest === decision.targetDigest
      && entry.target.profile === decision.profile && bindingLive(state, entry), 'app_binding_pending', 503);
    return entry.reference;
  }
  function assertBindingCurrent(channel, reference) {
    const value = references.get(reference);
    assertApps(value && value.state.channel === channel && bindingLive(value.state, value.entry), 'app_binding_changed', 503);
    return reference;
  }
  function openPins(reference) {
    const value = references.get(reference);
    assertApps(value && bindingLive(value.state, value.entry), 'app_binding_changed', 503);
    return { channelId: reference.channelId, syncId: reference.syncId, ...pins(reference) };
  }
  function getState(channel, id, expectedTarget) {
    const state = states.get(channel), entry = state?.bindings.get(id);
    if (!state || !current(state)) return { state: 'unavailable', reason: channel?.bindingVersion === 2 ? 'app_offline' : 'app_source_protocol_required' };
    if (!entry) return { state: 'unavailable', reason: 'app_binding_pending' };
    // An owner read can observe a newer database target before the next channel
    // reconciliation. Never project the previous target's ACK or rejection onto it.
    if (expectedTarget && (expectedTarget.appId !== id || expectedTarget.connectorKey !== state.key
      || entry.target.revision !== expectedTarget.revision || entry.target.digest !== expectedTarget.digest
      || entry.target.profile !== expectedTarget.profile)) return { state: 'pending', reason: 'app_binding_changed' };
    bindingLive(state, entry);
    return { state: entry.status, ...(entry.reason ? { reason: entry.reason } : {}) };
  }
  function prepareTarget({ preparationId, actor, target, requiredBindingVersion, signal }) {
    assertApps(requiredBindingVersion === 2 && opaque(preparationId), 'apps_source_preparation_mismatch', 409);
    const capturedActor = Object.freeze({ accountId: textId(actor?.accountId), deviceId: textId(actor?.deviceId) });
    const state = context(channels.get(target?.connectorKey)), normalized = normalizeTarget(state, target, { candidate: true });
    assertApps(capturedActor.accountId === normalized.target.ownerAccountId, 'apps_source_preparation_mismatch', 409);
    assertApps(!normalized.error, normalized.error || 'app_invalid_binding_target');
    assertApps(!signal?.aborted, 'apps_source_preparation_stale', 409);
    assertApps(state.preparing.size < RUNTIME_BINDING_LIMITS.pending, 'app_prepare_busy', 429);
    const createdAt = timestamp();
    return new Promise((resolve, reject) => {
      const entry = { ...normalized, actor: capturedActor, preparationId, nonce: nonce(), createdAt,
        deadline: createdAt + RUNTIME_BINDING_LIMITS.ackMs, signal, resolve, reject, timer: undefined };
      entry.abort = () => finishPreparation(state, entry, new AppsError('apps_source_preparation_stale', 409));
      state.preparing.set(entry.nonce, entry); signal?.addEventListener('abort', entry.abort, { once: true });
      entry.timer = later(() => finishPreparation(state, entry, new AppsError('apps_source_probe_timeout', 504)), RUNTIME_BINDING_LIMITS.ackMs);
      try { sendControl(state, { type: 'target-prepare', channelId: state.channelId, nonce: entry.nonce, target: entry.wireTarget }); }
      catch (error) { finishPreparation(state, entry, error); }
    });
  }
  function verifyPreparedTarget({ evidence, preparationId, actor, target, requiredBindingVersion }) {
    const proof = proofs.get(evidence), time = now();
    return Boolean(proof && requiredBindingVersion === 2 && current(proof.state) && !proof.signal?.aborted && clockValid(time)
      && time >= proof.createdAt && time < proof.expiresAt && preparationId === proof.preparationId
      && actor?.accountId === proof.actor.accountId && actor?.deviceId === proof.actor.deviceId
      && target && fingerprint(target) === proof.fingerprint);
  }
  function close() {
    if (closed) return; closed = true; let firstError;
    for (const state of [...activeStates]) try { drop(state.channel); } catch (error) { firstError ??= error; }
    if (firstError) throw firstError;
  }
  return Object.freeze({ sync, handleFrame, requireBinding, assertBindingCurrent, openPins, getState,
    prepareTarget, verifyPreparedTarget, drop, close });
}
