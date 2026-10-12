import { check, fields, syncResult, deepFreeze, jsonCopy, SourceAppError } from './wire.mjs';
import { standardSourceContract } from './standard-profile.mjs';
import { createWorkspaceContextAuthority, workspaceMutationRef, contextAfter, workspaceReceipt } from './workspace-context.mjs';
import { digest } from './wire.mjs';
import { isVerifiedRootContext } from '../../apps/scoped-embed/source-authority-client.mjs';

const approved = new WeakSet();
const approvedCommits = new WeakSet();
/** Constructor code supplies Native account/resource/ACL hooks. No HTTP/body
 * projection, Root owner flag, descriptor or OIDC subject creates these rights. */
export function createSourceNativeAuthorityPort(options) {
  const value = fields(options, ['capture', 'withCurrent'], ['assertCurrent', 'captureRoot', 'commitRootSession', 'rememberLogin', 'recoverLogin', 'linkVerifiedIdentity', 'createEmptyGuest', 'read', 'execute', 'readProof', 'feedback', 'currentContext', 'selectContext']);
  check(typeof value.capture === 'function' && typeof value.withCurrent === 'function' && value.withCurrent.constructor?.name !== 'AsyncFunction');
  for (const key of ['assertCurrent', 'captureRoot', 'commitRootSession', 'rememberLogin', 'recoverLogin', 'linkVerifiedIdentity', 'createEmptyGuest', 'read', 'execute', 'readProof'])
    check(value[key] === undefined || typeof value[key] === 'function');
  for (const key of ['currentContext', 'selectContext']) check(value[key] === undefined || typeof value[key] === 'function' && value[key].constructor?.name !== 'AsyncFunction');
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
export function createNativeAuthorityRuntime(port, options = {}) {
  check(isSourceNativeAuthorityPort(port), 'source_app_native_authority_required', 503);
  const selected = fields(options, [], ['sourceProfile', 'clock', 'workspaceCapacity']);
  const contract = standardSourceContract(selected.sourceProfile);
  check(selected.workspaceCapacity === undefined || contract.kind === 'soty.workspace.v1', 'source_workspace_capacity_invalid', 503);
  const capacity = selected.workspaceCapacity === undefined ? {} : fields(selected.workspaceCapacity, [], ['maxSlots']);
  const workspace = contract.kind === 'soty.workspace.v1' ? createWorkspaceContextAuthority({ clock: selected.clock, ...capacity }) : null;
  check(!workspace || typeof port.currentContext === 'function', 'source_workspace_context_required', 503);
  const feedbackGetBytes = contract.rules.find(row => row.path === '/api/embed/feedback/ticket').responseBytes;
  const proofs = new WeakMap(); let closed = false, fencing = false;
  function withCurrent(proof, action) {
    const captured = proofs.get(proof);
    check(!closed && captured && !fencing && typeof action === 'function', 'source_app_authority_invalid', 503);
    if (captured.verifiedRoot) check(isVerifiedRootContext(captured.verifiedRoot), 'source_root_witness_invalid', 403);
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
      if (captured.verifiedRoot) check(isVerifiedRootContext(captured.verifiedRoot), 'source_root_witness_invalid', 403);
      return result;
    } finally { live = false; fencing = false; }
  }
  /** Fresh host-only Source check OUTSIDE a Native transaction. A PG hook
   * queries current SQL authority; it cannot replace the synchronous final
   * checkpoint, create permission, return rows or use a JSON proof wrapper. */
  async function assertCurrent(proof) {
    const captured = proofs.get(proof);
    check(!closed && captured && !fencing, 'source_app_authority_invalid', 503);
    if (captured.verifiedRoot) check(isVerifiedRootContext(captured.verifiedRoot), 'source_root_witness_invalid', 403);
    if (port.assertCurrent === undefined) { withCurrent(proof, () => true); return; }
    const result = await port.assertCurrent(captured.native, captured.binding);
    check(!closed && !fencing && proofs.get(proof) === captured && result === undefined, 'source_app_authority_invalid', 503);
    if (captured.verifiedRoot) check(isVerifiedRootContext(captured.verifiedRoot), 'source_root_witness_invalid', 403);
  }
  function operationCommit(proof, context) {
    let open = true, used = false, entered = false, poisoned = false, inAction = false;
    const capability = Object.freeze({ ...(context ? { contextAfter() {
      check(open && inAction && !closed, 'source_app_commit_invalid', 503);
      const projection = context.check();
      return deepFreeze({ schema: 'soty.context-after.v1', mutationRef: context.mutationRef, ...projection });
    } } : {}), commit(action) {
      if (!open || used || closed || typeof action !== 'function') {
        poisoned = true; throw new SourceAppError('source_app_commit_invalid', 503);
      }
      used = true;
      return withCurrent(proof, () => { context?.check(); entered = true; inAction = true;
        try { const result = syncResult(action()); context?.check(); return result; } finally { inAction = false; } });
    } });
    approvedCommits.add(capability);
    return { capability, seal() { open = false; }, assertUsed() {
      check(used && entered && !poisoned, 'source_app_commit_required', 503);
    }, mayHaveCommitted() { return entered; } };
  }
  async function invoke(proof, method, input, { mutation = false, bytes = 65536, responseBytes = bytes, context = null } = {}) {
    const captured = proofs.get(proof); await assertCurrent(proof);
    if (context) withCurrent(proof, context.check);
    const payload = deepFreeze(jsonCopy(input, { bytes })), final = mutation ? operationCommit(proof, context) : null;
    try {
      const result = await method(captured.native, captured.binding, payload, final?.capability);
      final?.seal(); final?.assertUsed();
      await assertCurrent(proof);
      if (context) withCurrent(proof, context.check);
      return deepFreeze(jsonCopy(result, { bytes: responseBytes }));
    } catch (error) {
      // Once a Native action has run, a later denial, invalid response or lost
      // ACK does not prove rollback. Recovery must read the Source receipt;
      // this runtime never repeats an apply on its own.
      if (final?.mayHaveCommitted()) throw new SourceAppError('source_app_effect_unknown', 503);
      throw error;
    } finally { final?.seal(); }
  }
  return Object.freeze({
    async captureRoot(binding, verifiedRootContext) {
      check(!closed && contract.auth.proof === 'root-signed-current-context.v1' && isVerifiedRootContext(verifiedRootContext)
        && typeof port.captureRoot === 'function', 'source_root_witness_invalid', 403);
      const selected = deepFreeze(jsonCopy(binding)), context = verifiedRootContext;
      check(digest(selected.rootPrincipal) === digest(context.rootPrincipal) && digest(selected.resource) === digest(context.resource)
        && selected.identity.issuer === context.humanPrincipal.issuer && selected.identity.subject === context.humanPrincipal.subject
        && selected.rootReferenceDigest === digest(context.reference) && selected.instanceId === context.instanceId
        && selected.rootExpiresAt === context.expiresAt, 'source_root_witness_invalid', 403);
      const native = await port.captureRoot(selected, context);
      check(!closed && native && typeof native === 'object' && isVerifiedRootContext(context), 'source_root_witness_invalid', 403);
      const proof = Object.freeze(Object.create(null)); proofs.set(proof, { native, binding: selected, verifiedRoot: context });
      await assertCurrent(proof); return proof;
    },
    commitRootSession(proof) {
      const captured = proofs.get(proof);
      check(captured?.verifiedRoot && contract.auth.proof === 'root-signed-current-context.v1' && typeof port.commitRootSession === 'function', 'source_app_native_link_required', 403);
      return withCurrent(proof, () => syncResult(port.commitRootSession(captured.native, captured.binding)));
    },
    async capture(binding, nativeRequest) {
      check(!closed, 'source_app_closed', 503);
      const selected = deepFreeze(jsonCopy(binding));
      const native = await port.capture(selected, nativeRequest);
      check(!closed && native && typeof native === 'object', 'source_app_native_access_denied', 403);
      const proof = Object.freeze(Object.create(null)); proofs.set(proof, { native, binding: selected });
      await assertCurrent(proof); return proof;
    },
    withCurrent,
    assertCurrent,
    rememberLogin(proof, intent) {
      const captured = proofs.get(proof), input = fields(intent, ['interactionIdHash', 'expiresAt']);
      check(/^[a-f0-9]{64}$/u.test(input.interactionIdHash) && Number.isSafeInteger(input.expiresAt) && input.expiresAt > 0
        && typeof port.rememberLogin === 'function', 'source_app_native_login_not_ready', 503);
      const marker = withCurrent(proof, () => syncResult(port.rememberLogin(captured.native, captured.binding, Object.freeze(input))));
      const result = fields(marker, ['idHash', 'version', 'bindingDigest']);
      check(result.version === 1 && /^[a-f0-9]{64}$/u.test(result.idHash) && /^[a-f0-9]{64}$/u.test(result.bindingDigest), 'source_app_authority_invalid', 503);
      return Object.freeze(result);
    },
    async recoverLogin(binding, marker, intent) {
      check(!closed && typeof port.recoverLogin === 'function', 'source_app_native_login_not_ready', 503);
      const selected = deepFreeze(jsonCopy(binding)), reference = fields(marker, ['idHash', 'version', 'bindingDigest']);
      check(reference.version === 1 && /^[a-f0-9]{64}$/u.test(reference.idHash) && /^[a-f0-9]{64}$/u.test(reference.bindingDigest), 'source_app_authority_invalid', 503);
      const selectedIntent = fields(intent, ['interactionIdHash', 'expiresAt']);
      check(/^[a-f0-9]{64}$/u.test(selectedIntent.interactionIdHash) && Number.isSafeInteger(selectedIntent.expiresAt) && selectedIntent.expiresAt > 0);
      const native = await port.recoverLogin(selected, Object.freeze(reference), Object.freeze(selectedIntent));
      check(!closed && native && typeof native === 'object', 'source_app_native_access_denied', 403);
      const proof = Object.freeze(Object.create(null)); proofs.set(proof, { native, binding: selected }); await assertCurrent(proof); return proof;
    },
    commitIdentity(proof, identity, { createEmptyGuest = false } = {}) {
      const captured = proofs.get(proof);
      const operation = createEmptyGuest ? 'createEmptyGuest' : 'linkVerifiedIdentity';
      check(typeof port[operation] === 'function', 'source_app_native_link_required', 403);
      // Source owns the actual association, empty-resource policy and transaction.
      return withCurrent(proof, () => syncResult(port[operation](captured.native, captured.binding, deepFreeze(jsonCopy(identity)))));
    },
    async call(proof, operation, input) {
      check(['read', 'execute', 'readProof'].includes(operation) && typeof port[operation] === 'function', 'source_app_capability_disabled', 503);
      if (!workspace) return invoke(proof, port[operation], input, { mutation: operation === 'execute' });
      const captured = proofs.get(proof); await assertCurrent(proof);
      const nativeProjection = () => syncResult(port.currentContext(captured.native, captured.binding));
      if (operation === 'read' && input.input?.operation === 'soty.context.select') {
        const locator = fields(input.input, ['operation', 'nativeId', 'generation']);
        check(input.contextRef === undefined && typeof port.selectContext === 'function'
          && typeof locator.nativeId === 'string' && /^[A-Za-z0-9_.:-]{1,128}$/u.test(locator.nativeId)
          && Number.isSafeInteger(locator.generation) && locator.generation >= 1, 'source_workspace_selection_denied', 403);
        return withCurrent(proof, () => {
          check(syncResult(port.selectContext(captured.native, captured.binding,
            Object.freeze({ nativeId: locator.nativeId, generation: locator.generation }))) === undefined);
          return workspace.read(captured.binding, nativeProjection());
        });
      }
      if (operation === 'read' && input.input?.operation === 'soty.current_context') {
        fields(input.input, ['operation']); check(input.contextRef === undefined);
        return withCurrent(proof, () => workspace.read(captured.binding, nativeProjection()));
      }
      const { contextRef, ...args } = fields(input, ['requestId', 'input', 'contextRef'], ['reference']);
      const context = { check: () => workspace.assert(captured.binding, nativeProjection(), contextRef),
        mutationRef: workspaceMutationRef(args.requestId, operation === 'readProof' ? args.input.inputDigest : digest(args.input), contextRef) };
      const result = await invoke(proof, port[operation], args, { mutation: operation === 'execute', context });
      if (operation === 'readProof' && result.outcome === 'committed') {
        workspaceReceipt(result.workspaceReceipt);
        const after = contextAfter(result.contextAfter), current = withCurrent(proof, context.check);
        check(after.mutationRef === context.mutationRef && after.generation === current.generation, 'source_workspace_receipt_denied', 403);
      }
      return result;
    },
    async feedback(proof, operation, input = {}, contextRef) {
      check(['context', 'list', 'get', 'submit', 'reply', 'status', 'accept'].includes(operation)
        && typeof port.feedback?.[operation] === 'function', 'source_app_feedback_unavailable', 503);
      const mutation = ['submit', 'reply', 'status', 'accept'].includes(operation);
      let context = null;
      if (contract.auth.proof === 'root-signed-current-context.v1') {
        const captured = proofs.get(proof);
        context = { check: () => workspace.assert(captured.binding,
          syncResult(port.currentContext(captured.native, captured.binding)), contextRef),
          ...(mutation ? { mutationRef: workspaceMutationRef(input.requestId, digest(input), contextRef) } : {}) };
      } else check(contextRef === undefined, 'source_workspace_context_denied', 403);
      return invoke(proof, port.feedback[operation], input, { mutation, context,
        bytes: 1500000, responseBytes: operation === 'get' ? feedbackGetBytes : 1500000 });
    },
    close() { closed = true; workspace?.close(); },
  });
}
