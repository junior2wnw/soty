import { check, fields, syncResult, deepFreeze, jsonCopy, SourceAppError } from './wire.mjs';

const approved = new WeakSet();
const approvedCommits = new WeakSet();
/** Constructor code supplies Native account/resource/ACL hooks. No HTTP/body
 * projection, Root owner flag, descriptor or OIDC subject creates these rights. */
export function createSourceNativeAuthorityPort(options) {
  const value = fields(options, ['capture', 'withCurrent'], ['linkVerifiedIdentity', 'createEmptyGuest', 'read', 'execute', 'readProof', 'feedback']);
  check(typeof value.capture === 'function' && typeof value.withCurrent === 'function' && value.withCurrent.constructor?.name !== 'AsyncFunction');
  for (const key of ['linkVerifiedIdentity', 'createEmptyGuest', 'read', 'execute', 'readProof'])
    check(value[key] === undefined || typeof value[key] === 'function');
  if (value.feedback !== undefined) {
    const feedback = fields(value.feedback, ['context', 'list', 'get', 'submit'], ['reply', 'status', 'accept']);
    Object.values(feedback).forEach(method => check(typeof method === 'function')); value.feedback = Object.freeze(feedback);
  }
  const port = Object.freeze(value); approved.add(port); return port;
}
export function isSourceNativeAuthorityPort(value) { return value && approved.has(value); }
/** Host-only operation capability. A Native mutator calls commit(action) inside
 * its own transaction, after checking its operation-specific role/resource.
 * It is neither a JSON permission nor a transaction supplied by the SDK. */
export function isSourceNativeCommitPort(value) { return !!value && approvedCommits.has(value); }

/** Private proof wrappers are instance-local, not serializable authority.
 * Exactly one synchronous final Native fence also runs inside Source commits. */
export function createNativeAuthorityRuntime(port) {
  check(isSourceNativeAuthorityPort(port), 'source_app_native_authority_required', 503);
  const proofs = new WeakMap(); let closed = false, fencing = false;
  function withCurrent(proof, action) {
    const captured = proofs.get(proof);
    check(!closed && captured && !fencing && typeof action === 'function', 'source_app_authority_invalid', 503);
    let live = true, entered = false, poisoned = false, result;
    fencing = true;
    const callback = () => {
      if (!live || entered || closed) { poisoned = true; throw new Error('source_app_authority_invalid'); }
      entered = true;
      try { result = syncResult(action()); return result; } catch (error) { poisoned = true; throw error; }
    };
    try {
      const returned = syncResult(port.withCurrent(captured.native, captured.binding, callback));
      check(entered && !poisoned && returned === result, 'source_app_authority_invalid', 503);
      return result;
    } finally { live = false; fencing = false; }
  }
  function operationCommit(proof) {
    let open = true, used = false, entered = false, poisoned = false;
    const capability = Object.freeze({ commit(action) {
      if (!open || used || closed || typeof action !== 'function') {
        poisoned = true; throw new SourceAppError('source_app_commit_invalid', 503);
      }
      used = true;
      return withCurrent(proof, () => { entered = true; return syncResult(action()); });
    } });
    approvedCommits.add(capability);
    return { capability, seal() { open = false; }, assertUsed() {
      check(used && entered && !poisoned, 'source_app_commit_required', 503);
    }, mayHaveCommitted() { return entered; } };
  }
  async function invoke(proof, method, input, { mutation = false, bytes = 65536 } = {}) {
    const captured = proofs.get(proof); withCurrent(proof, () => true);
    const payload = deepFreeze(jsonCopy(input, { bytes })), final = mutation ? operationCommit(proof) : null;
    try {
      const result = await method(captured.native, captured.binding, payload, final?.capability);
      final?.seal(); final?.assertUsed();
      withCurrent(proof, () => true);
      return deepFreeze(jsonCopy(result, { bytes }));
    } catch (error) {
      // Once a Native action has run, a later denial, invalid response or lost
      // ACK does not prove rollback. Recovery must read the Source receipt;
      // this runtime never repeats an apply on its own.
      if (final?.mayHaveCommitted()) throw new SourceAppError('source_app_effect_unknown', 503);
      throw error;
    } finally { final?.seal(); }
  }
  return Object.freeze({
    async capture(binding, nativeRequest) {
      check(!closed, 'source_app_closed', 503);
      const selected = deepFreeze(jsonCopy(binding));
      const native = await port.capture(selected, nativeRequest);
      check(!closed && native && typeof native === 'object', 'source_app_native_access_denied', 403);
      const proof = Object.freeze(Object.create(null)); proofs.set(proof, { native, binding: selected });
      withCurrent(proof, () => true); return proof;
    },
    withCurrent,
    commitIdentity(proof, identity, { createEmptyGuest = false } = {}) {
      const captured = proofs.get(proof);
      const operation = createEmptyGuest ? 'createEmptyGuest' : 'linkVerifiedIdentity';
      check(typeof port[operation] === 'function', 'source_app_native_link_required', 403);
      // Source owns the actual association, empty-resource policy and transaction.
      return withCurrent(proof, () => syncResult(port[operation](captured.native, captured.binding, deepFreeze(jsonCopy(identity)))));
    },
    async call(proof, operation, input) {
      check(['read', 'execute', 'readProof'].includes(operation) && typeof port[operation] === 'function', 'source_app_capability_disabled', 503);
      return invoke(proof, port[operation], input, { mutation: operation === 'execute' });
    },
    async feedback(proof, operation, input = {}) {
      check(['context', 'list', 'get', 'submit', 'reply', 'status', 'accept'].includes(operation)
        && typeof port.feedback?.[operation] === 'function', 'source_app_feedback_unavailable', 503);
      return invoke(proof, port.feedback[operation], input, { mutation: ['submit', 'reply', 'status', 'accept'].includes(operation), bytes: 1500000 });
    },
    close() { closed = true; },
  });
}
