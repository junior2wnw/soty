import { createSourceFrameFetch } from './frame-fetch.mjs';
import { parseSourceJson } from '../shared/strict-json.mjs';

const opaque = value => typeof value === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(value);
const requestId = value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{16,128}$/u.test(value);
const digest = value => typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value);
const integer = value => Number.isSafeInteger(value) && value >= 1;
const metadata = value => typeof value?.title === 'string' && value.title.isWellFormed() && value.title.trim() && value.title.length <= 240
  && !/[\u0000-\u001f\u007f]/u.test(value.title) && ['owner', 'editor', 'viewer', 'reader'].includes(value.role) && integer(value.revision);
const canonical = value => Array.isArray(value) ? '[' + value.map(canonical).join(',') + ']'
  : value && typeof value === 'object' ? '{' + Object.keys(value).sort().map(key => JSON.stringify(key) + ':' + canonical(value[key])).join(',') + '}' : JSON.stringify(value);
const freeze = value => { if (value && typeof value === 'object') { Object.values(value).forEach(freeze); Object.freeze(value); } return value; };

export class SourceWorkspaceClientError extends Error {
  constructor(code, status = 503, effectUnknown = false) {
    super(code); this.name = 'SourceWorkspaceClientError'; this.code = code; this.status = status; this.effectUnknown = effectUnknown;
  }
}
const fail = (code = 'source_client_input_invalid', status = 400) => { throw new SourceWorkspaceClientError(code, status); };

// Match the maintained wire grammar before hashing; no getters/toJSON, dropped
// undefined values or changed key order can silently change a write intent.
function copy(input) {
  let nodes = 0;
  function visit(value, level) {
    if (++nodes > 4096 || level > 18) fail('source_app_payload_limit', 413);
    if (value === null || typeof value === 'boolean') return value;
    if (typeof value === 'string') { if (!value.isWellFormed() || /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/u.test(value)) fail(); return value; }
    if (typeof value === 'number') { if (!Number.isFinite(value)) fail(); return value; }
    if (!value || typeof value !== 'object') fail();
    const descriptors = Object.getOwnPropertyDescriptors(value), array = Array.isArray(value), prototype = Object.getPrototypeOf(value);
    if (array ? prototype !== Array.prototype : ![Object.prototype, null].includes(prototype)) fail();
    if (array) {
      if (value.length > 128 || Reflect.ownKeys(descriptors).length !== value.length + 1) fail();
      return Array.from({ length: value.length }, (_, i) => { if (!descriptors[i] || !Object.hasOwn(descriptors[i], 'value')) fail(); return visit(descriptors[i].value, level + 1); });
    }
    if (Reflect.ownKeys(descriptors).some(key => typeof key !== 'string' || !descriptors[key].enumerable || !Object.hasOwn(descriptors[key], 'value'))) fail();
    return Object.fromEntries(Object.entries(descriptors).map(([key, field]) => [key, visit(field.value, level + 1)]));
  }
  const value = visit(input, 0);
  if (new TextEncoder().encode(JSON.stringify(value)).byteLength > 65536) fail('source_app_payload_limit', 413);
  return value;
}
function fields(value, required, optional = []) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || !required.every(key => Object.hasOwn(value, key))
    || Object.keys(value).some(key => !required.includes(key) && !optional.includes(key))) fail();
  return value;
}
const hash = async value => [...new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(canonical(value))))]
  .map(byte => byte.toString(16).padStart(2, '0')).join('');

/** One current Source workspace, fixed same-origin routes and explicit write
 * intents. Cookies/actor/Native rights remain exclusively server-owned. */
export function createSourceWorkspaceClient({ fetch: fetcher = globalThis.fetch, href = globalThis.location?.href,
  view = globalThis.window, parentOrigin, isCurrent = () => true } = {}) {
  if (typeof isCurrent !== 'function') fail();
  const sourceFetch = createSourceFrameFetch({ fetch: fetcher, href, view, ...(parentOrigin === undefined ? {} : { parentOrigin }), shortSessionHandoff: true });
  const intents = new WeakMap(), pending = new Map(), preparing = new Map();
  let epoch = 0, contextSequence = 0, connectSequence = 0, context = null, closed = false, refreshFlight = null;
  const current = captured => !closed && isCurrent() && captured === epoch;
  const check = captured => { if (!current(captured)) fail('source_client_context_changed', 409); };
  const capture = () => { check(epoch); if (!context) fail('source_client_context_required', 409); return { epoch, context }; };
  const invalidate = () => { epoch++; contextSequence++; connectSequence++; context = null; pending.clear(); preparing.clear(); refreshFlight = null; };

  async function request(path, args, captured, signal) {
    check(captured); signal?.throwIfAborted();
    const body = JSON.stringify(args);
    if (new TextEncoder().encode(body).byteLength > (path === 'receipt' ? 16384 : 65536)) fail('source_app_payload_limit', 413);
    const response = await sourceFetch('/api/embed/' + path, { method: 'POST', credentials: 'same-origin', redirect: 'error',
      referrerPolicy: 'no-referrer', signal, headers: { 'content-type': 'application/json', accept: 'application/json' }, body });
    check(captured);
    let value;
    try {
      if (response.redirected || !/^application\/json(?:;|$)/iu.test(response.headers.get('content-type') || '')) throw Error();
      const reader = response.body.getReader(), chunks = []; let count = 0;
      try { while (true) { const { done, value: chunk } = await reader.read(); if (done) break;
        count += chunk.byteLength; if (count > 65536) throw Error(); chunks.push(chunk); } }
      finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
      const bytes = new Uint8Array(count); let offset = 0;
      for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
      value = parseSourceJson(new TextDecoder('utf-8', { fatal: true }).decode(bytes), { bytes: 65536 });
    } catch { check(captured); fail('source_client_response_invalid', 502); }
    check(captured); signal?.throwIfAborted();
    if (!response.ok) throw new SourceWorkspaceClientError(/^[a-z0-9_]{1,128}$/u.test(value?.error?.code ?? '') ? value.error.code : 'source_app_unknown', response.status);
    if (path === 'session-continue') {
      if (value?.schema !== 'soty.source-session-continuation.v1' || value.ready !== true || typeof value.renewable !== 'boolean'
        || !integer(value.sessionExpiresAt) || !integer(value.accessExpiresAt) || !digest(value.receiptDigest)) fail('source_client_response_invalid', 502);
      return freeze(value);
    }
    if (!value || value.ok !== true || Object.keys(value).sort().join(',') !== 'data,ok' || !value.data || typeof value.data !== 'object' || Array.isArray(value.data)) fail('source_client_response_invalid', 502);
    return freeze(value.data);
  }
  async function acquire(args, selection, signal) {
    const captured = epoch, sequence = ++contextSequence;
    const input = selection ? { operation: 'soty.context.select', nativeId: args.nativeId, generation: args.generation } : { operation: 'soty.current_context' };
    if (!requestId(args.requestId) || selection && (typeof args.nativeId !== 'string' || !/^[A-Za-z0-9_.:-]{1,128}$/u.test(args.nativeId) || !integer(args.generation))) fail();
    const value = await request('query', { requestId: args.requestId, input }, captured, signal);
    if (sequence !== contextSequence) fail('source_client_context_changed', 409);
    if (value.schema !== 'soty.current-context.v2' || !opaque(value.context?.ref) || !integer(value.context.generation)
      || !integer(value.context.expiresAt) || !metadata(value)
      || value.selection?.kind !== 'soty.workspace.v1' || typeof value.selection.nativeId !== 'string'
      || typeof value.selection.incarnationId !== 'string'
      || value.continuityDigest !== undefined && !digest(value.continuityDigest)) fail('source_client_response_invalid', 502);
    if (context && (context.context.ref !== value.context.ref || context.context.generation !== value.context.generation)) {
      epoch++;
      // Keep sent request IDs pinned until their original receipt is resolved.
      // A renewed lease must not turn an unknown write into a new preparation.
      for (const [id, entry] of pending) if (!entry.sent) pending.delete(id);
    }
    context = value; return value;
  }
  function input(args) {
    const selected = copy(args); fields(selected, ['requestId', 'input']);
    if (!requestId(selected.requestId) || !selected.input || Array.isArray(selected.input) || typeof selected.input !== 'object'
      || typeof selected.input.operation !== 'string' || !selected.input.operation || selected.input.operation.length > 160
      || ['soty.current_context', 'soty.context.select'].includes(selected.input.operation)) fail();
    return selected;
  }
  function proof(value, entry) {
    if (value.requestId !== entry.requestId || value.inputDigest !== entry.inputDigest) fail('source_client_response_invalid', 502);
    if (['not_applied', 'unknown'].includes(value.outcome)) fail('source_receipt_unknown', 409);
    if (value.outcome !== 'committed') fail('source_client_response_invalid', 502);
    if (value.outcome === 'committed' && (value.replayed !== undefined && typeof value.replayed !== 'boolean' || value.contextAfter?.schema !== 'soty.context-after.v1'
      || value.contextAfter.mutationRef !== entry.mutationRef || value.contextAfter.generation !== entry.generation
      || !metadata(value.contextAfter) || value.workspaceReceipt?.schema !== 'soty.workspace-receipt.v1'
      || !Array.isArray(value.workspaceReceipt.effects) || !value.workspaceReceipt.effects.length || value.workspaceReceipt.effects.length > 32
      || value.workspaceReceipt.receipt?.verificationMethod !== 'domain_read' || !Array.isArray(value.workspaceReceipt.receipt.artifacts)
      || !value.workspaceReceipt.receipt.artifacts.length || value.workspaceReceipt.receipt.artifacts.length > 32)) fail('source_client_response_invalid', 502);
    return value;
  }
  async function receipt(reference, signal) {
    const known = intents.get(reference);
    if (!known) { reference = copy(reference); fields(reference, ['requestId', 'contextRef', 'inputDigest']); }
    const selected = capture();
    if (!reference || !requestId(reference.requestId) || !opaque(reference.contextRef) || !digest(reference.inputDigest)
      || reference.contextRef !== selected.context.context.ref || known && known.generation !== selected.context.context.generation) fail('source_client_context_changed', 409);
    const entry = known ?? { ...reference, generation: selected.context.context.generation,
      mutationRef: await hash({ schema: 'soty.workspace-mutation.v1', requestId: reference.requestId, inputDigest: reference.inputDigest, contextRef: reference.contextRef }) };
    check(selected.epoch);
    const response = await request('receipt', { requestId: entry.requestId, contextRef: entry.contextRef,
      input: { inputDigest: entry.inputDigest } }, selected.epoch, signal);
    const value = response.recovery === undefined ? proof(response, entry) : await recoveredProof(response, reference, known, selected, signal);
    pending.delete(entry.requestId); return value;
  }
  async function recoveredProof(value, reference, known, selected, signal) {
    // A missing/old backend or a foreign actor may report no row. That is not
    // proof that this different context's original write was never applied.
    if (value.outcome !== 'committed' || !value.recovery) fail('source_receipt_recovery_unavailable', 409);
    const { recovery, ...original } = value;
    if (Object.keys(recovery).sort().join(',') !== 'contextAfter,contextRef,receiptDigest,schema'
      || recovery.schema !== 'soty.workspace-receipt-recovery.v1' || recovery.contextRef !== reference.contextRef
      || !digest(recovery.receiptDigest) || recovery.receiptDigest !== await hash(original)) fail('source_client_response_invalid', 502);
    const originalMutation = await hash({ schema: 'soty.workspace-mutation.v1', requestId: reference.requestId, inputDigest: reference.inputDigest, contextRef: reference.contextRef });
    proof(original, { ...reference, mutationRef: originalMutation, generation: known?.generation ?? original.contextAfter?.generation });
    const freshMutation = await hash({ schema: 'soty.workspace-mutation.v1', requestId: reference.requestId, inputDigest: reference.inputDigest, contextRef: selected.context.context.ref });
    if (recovery.contextAfter?.schema !== 'soty.context-after.v1' || !metadata(recovery.contextAfter)
      || recovery.contextAfter.mutationRef !== freshMutation || recovery.contextAfter.generation !== selected.context.context.generation) fail('source_client_response_invalid', 502);
    check(selected.epoch); signal?.throwIfAborted(); pending.delete(reference.requestId);
    return value;
  }
  async function recoverReceipt(reference, signal) {
    const known = intents.get(reference);
    if (!known) {
      reference = copy(reference); fields(reference, ['requestId', 'contextRef', 'inputDigest'], ['input']);
      if (reference.input !== undefined && await hash(reference.input) !== reference.inputDigest) fail('request_conflict', 409);
    }
    const selected = capture();
    if (!reference || !requestId(reference.requestId) || !opaque(reference.contextRef) || !digest(reference.inputDigest)) fail();
    if (reference.contextRef === selected.context.context.ref) return receipt(known ? reference : {
      requestId: reference.requestId, contextRef: reference.contextRef, inputDigest: reference.inputDigest }, signal);
    const value = await request('receipt', { requestId: reference.requestId, contextRef: selected.context.context.ref,
      input: { inputDigest: reference.inputDigest } }, selected.epoch, signal);
    return recoveredProof(value, reference, known, selected, signal);
  }
  function continueSession(args, signal) {
    const selected = fields(copy(args), ['requestId']); if (!requestId(selected.requestId)) fail();
    return request('session-continue', selected, epoch, signal);
  }
  async function refreshForOperation(requestId, signal) {
    const before = capture();
    if (!refreshFlight) {
      const flight = (async () => {
        // Parent-frame renewal does not renew the Source cookie or its private
        // Native context reference. Both reads must finish before a NEW write.
        await continueSession({ requestId }, signal); check(before.epoch);
        const value = await acquire({ requestId }, false, signal), after = capture();
        if (context !== value || value.context.generation !== before.context.context.generation
          || value.role !== before.context.role || canonical(value.selection) !== canonical(before.context.selection)
          || (before.context.continuityDigest !== undefined && value.continuityDigest !== before.context.continuityDigest)
          || (value.context.ref !== before.context.context.ref && !digest(before.context.continuityDigest))) {
          invalidate(); fail('source_client_context_changed', 409);
        }
        return after;
      })();
      refreshFlight = flight;
      void flight.finally(() => { if (refreshFlight === flight) refreshFlight = null; }).catch(() => {});
    }
    const selected = await refreshFlight; check(selected.epoch); signal?.throwIfAborted(); return selected;
  }
  return Object.freeze({
    async connect(args, signal) {
      const selected = fields(copy(args), ['requestId']), captured = epoch, sequence = ++connectSequence;
      try {
        await continueSession(selected, signal); check(captured);
        return await acquire(selected, false, signal);
      } catch (error) { if (sequence === connectSequence && current(captured)) invalidate(); throw error; }
    },
    continueSession,
    currentContext(args, signal) { fields(copy(args), ['requestId']); return acquire(args, false, signal); },
    selectContext(args, signal) { fields(copy(args), ['requestId', 'nativeId', 'generation']); return acquire(args, true, signal); },
    context: () => current(epoch) ? context : null,
    async query(args, signal) {
      const selected = input(args); let captured = capture(), value;
      try { value = await request('query', { ...selected, contextRef: captured.context.context.ref }, captured.epoch, signal); }
      catch (error) {
        // Only a read is retried, at most once. Native/Root revocation must
        // still pass fresh continuation and exact continuity before any data.
        if (!['authentication_required', 'source_workspace_context_denied'].includes(error?.code)) throw error;
        check(captured.epoch); captured = await refreshForOperation(selected.requestId, signal);
        value = await request('query', { ...selected, contextRef: captured.context.context.ref }, captured.epoch, signal);
      }
      if (value.requestId !== selected.requestId) fail('source_client_response_invalid', 502); return value;
    },
    async prepareInvoke(args, signal) {
      const selected = input(args), captured = capture(), serialized = canonical(selected.input);
      const prior = pending.get(selected.requestId) ?? preparing.get(selected.requestId);
      if (prior) { if (prior.serialized !== serialized || prior.contextRef !== undefined && prior.contextRef !== captured.context.context.ref) fail('request_conflict', 409); return prior.preparing; }
      if (pending.size + preparing.size >= 128) fail('source_client_pending_limit', 409);
      const entry = { ...selected, serialized, sent: false, flight: null };
      preparing.set(entry.requestId, entry);
      entry.preparing = (async () => {
        const fresh = await refreshForOperation(entry.requestId, signal);
        if (preparing.get(entry.requestId) !== entry) fail('source_client_context_changed', 409);
        entry.epoch = fresh.epoch; entry.contextRef = fresh.context.context.ref; entry.generation = fresh.context.context.generation;
        entry.inputDigest = await hash(selected.input);
        entry.mutationRef = await hash({ schema: 'soty.workspace-mutation.v1', requestId: entry.requestId, inputDigest: entry.inputDigest, contextRef: entry.contextRef });
        check(entry.epoch); signal?.throwIfAborted();
        const intent = freeze({ requestId: entry.requestId, input: selected.input, contextRef: entry.contextRef, inputDigest: entry.inputDigest });
        preparing.delete(entry.requestId); pending.set(entry.requestId, entry);
        intents.set(intent, entry); return intent;
      })().catch(error => { if (preparing.get(entry.requestId) === entry) preparing.delete(entry.requestId); throw error; });
      return entry.preparing;
    },
    async invoke(intent, signal) {
      const entry = intents.get(intent); if (!entry) fail();
      if (!current(entry.epoch) || context?.context.ref !== entry.contextRef) throw new SourceWorkspaceClientError('source_client_context_changed', 409, entry.sent);
      if (entry.flight) return entry.flight;
      // A sent intent is never automatically applied again. Retry reads its
      // original receipt, including after a network-unknown invoke outcome.
      if (entry.sent) return receipt(intent, signal).catch(error => { throw new SourceWorkspaceClientError(error?.code ?? 'source_receipt_unknown', error?.status ?? 503, true); });
      signal?.throwIfAborted(); entry.sent = true;
      entry.flight = (async () => {
        try {
          const response = await request('invoke', { requestId: entry.requestId, input: entry.input, contextRef: entry.contextRef }, entry.epoch, signal);
          if (response.recovery !== undefined) fail('source_client_response_invalid', 502);
          const value = proof(response, entry);
          if (value.outcome !== 'committed') fail('source_app_effect_unknown', 503);
          pending.delete(entry.requestId); return value;
        } catch (error) { throw new SourceWorkspaceClientError(error?.code ?? 'source_app_effect_unknown', error?.status ?? 503, true); }
      })().finally(() => { entry.flight = null; });
      return entry.flight;
    },
    receipt(reference, signal) {
      return receipt(reference, signal).catch(error => { throw new SourceWorkspaceClientError(error?.code ?? 'source_receipt_unknown', error?.status ?? 503, true); });
    },
    recoverReceipt(reference, signal) {
      return recoverReceipt(reference, signal).catch(error => { throw new SourceWorkspaceClientError(error?.code ?? 'source_receipt_recovery_unavailable', error?.status ?? 503, true); });
    },
    invalidate,
    dispose() { invalidate(); closed = true; },
  });
}
