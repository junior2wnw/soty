import { fields, check, digest, nonce, opaque, deepFreeze } from './wire.mjs';

const roles = ['owner', 'editor', 'viewer', 'reader'];
const denied = value => check(value, 'source_workspace_context_denied', 403);
export function contextMetadata(input) {
  const value = fields(input, ['title', 'role', 'revision']);
  denied(typeof value.title === 'string' && value.title.isWellFormed() && value.title.trim() && value.title.length <= 240
    && !/[\u0000-\u001f\u007f]/u.test(value.title) && roles.includes(value.role)
    && Number.isSafeInteger(value.revision) && value.revision >= 1);
  return value;
}
export function workspaceContext(input) {
  const value = fields(input, ['ref', 'generation', 'expiresAt']);
  denied(opaque(value.ref) && Number.isSafeInteger(value.generation) && value.generation >= 1
    && Number.isSafeInteger(value.expiresAt) && value.expiresAt > 0); return value;
}
export function workspaceMutationRef(requestId, inputDigest, contextRef) {
  denied(typeof requestId === 'string' && /^[A-Za-z0-9_.:-]{16,128}$/u.test(requestId)
    && /^[a-f0-9]{64}$/u.test(inputDigest) && opaque(contextRef));
  return digest({ schema: 'soty.workspace-mutation.v1', requestId, inputDigest, contextRef });
}
export function contextAfter(input) {
  const value = fields(input, ['schema', 'mutationRef', 'generation', 'title', 'role', 'revision']);
  denied(value.schema === 'soty.context-after.v1' && /^[a-f0-9]{64}$/u.test(value.mutationRef)
    && Number.isSafeInteger(value.generation) && value.generation >= 1);
  contextMetadata({ title: value.title, role: value.role, revision: value.revision }); return value;
}
export function workspaceReceipt(input) {
  const value = fields(input, ['schema', 'effects', 'receipt']);
  denied(value.schema === 'soty.workspace-receipt.v1' && Array.isArray(value.effects) && value.effects.length > 0 && value.effects.length <= 32);
  const receipt = fields(value.receipt, ['verificationMethod', 'artifacts']);
  denied(receipt.verificationMethod === 'domain_read' && Array.isArray(receipt.artifacts) && receipt.artifacts.length > 0 && receipt.artifacts.length <= 32);
  // Core independently validates every declared effect/artifact and its correspondence.
  return { effects: value.effects, receipt };
}
export function workspaceReceiptBinding(input) {
  const value = fields(input, ['schema', 'contextRef', 'scopeDigest', 'receiptDigest', 'mac']);
  denied(value.schema === 'soty.workspace-receipt-binding.v1' && opaque(value.contextRef)
    && [value.scopeDigest, value.receiptDigest, value.mac].every(part => /^[a-f0-9]{64}$/u.test(part)));
  return value;
}
/** The old immutable receipt and the current read attestation are different
 * claims. This validator is shared by Source and the maintained Root adapter;
 * it does not create a Native capability or verify a persisted binding MAC. */
export function workspaceReadProofContext(input, { requestId, inputDigest, contextRef }) {
  const receiptDenied = condition => check(condition, 'source_workspace_receipt_denied', 403);
  const value = fields(input, ['requestId', 'inputDigest', 'outcome', 'contextAfter', 'workspaceReceipt'],
    ['replayed', 'receipt', 'result', 'data', 'recovery']);
  receiptDenied(value.requestId === requestId && value.inputDigest === inputDigest && value.outcome === 'committed');
  workspaceReceipt(value.workspaceReceipt);
  const originalAfter = contextAfter(value.contextAfter);
  if (value.recovery === undefined) {
    receiptDenied(originalAfter.mutationRef === workspaceMutationRef(requestId, inputDigest, contextRef)); return originalAfter;
  }
  const recovery = fields(value.recovery, ['schema', 'contextRef', 'receiptDigest', 'contextAfter']);
  const { recovery: _recovery, ...original } = value;
  receiptDenied(recovery.schema === 'soty.workspace-receipt-recovery.v1' && opaque(recovery.contextRef)
    && /^[a-f0-9]{64}$/u.test(recovery.receiptDigest) && recovery.receiptDigest === digest(original)
    && originalAfter.mutationRef === workspaceMutationRef(requestId, inputDigest, recovery.contextRef));
  const currentAfter = contextAfter(recovery.contextAfter);
  receiptDenied(currentAfter.mutationRef === workspaceMutationRef(requestId, inputDigest, contextRef)); return currentAfter;
}
// domain_read attestation, deliberately NOT a content/artifact_hash receipt.
export function workspaceWitness({ after, artifact, requestId, inputDigest, scope }) {
  contextAfter(after);
  const identity = { type: artifact.type, id: artifact.id, ...(artifact.revision === undefined ? {} : { revision: artifact.revision }) };
  return digest({ schema: 'soty.workspace-context-witness.v1', contextAfter: after, artifactIdentity: identity,
    requestId, inputDigest, sourceBinding: scope });
}

/** Bounded, private Native leases. The caller invokes all methods inside its
 * existing withCurrent fence; Native supplies current child ACL/generation.
 * References cannot create permission and are never recoverable after restart. */
export function createWorkspaceContextAuthority(options = {}) {
  const value = fields(options, [], ['clock', 'maxSlots']), clock = value.clock ?? Date.now, maxSlots = value.maxSlots === undefined ? 128 : value.maxSlots;
  check(typeof clock === 'function' && Number.isSafeInteger(maxSlots) && maxSlots >= 1 && maxSlots <= 65536,
    'source_workspace_capacity_invalid', 503);
  const slots = new Map();
  function current(binding, raw, create) {
    const now = clock(), projection = fields(raw, ['key', 'generation', 'expiresAt', 'title', 'role', 'revision']);
    const metadata = contextMetadata({ title: projection.title, role: projection.role, revision: projection.revision });
    denied(binding.resource.selection.kind === 'soty.workspace.v1' && /^[a-f0-9]{64}$/u.test(binding.sessionIdHash)
      && /^[a-f0-9]{64}$/u.test(binding.rootReferenceDigest) && Number.isSafeInteger(binding.rootExpiresAt) && binding.rootExpiresAt > now
      && typeof projection.key === 'string' && projection.key.length > 0 && projection.key.length <= 4096
      && Number.isSafeInteger(projection.generation) && projection.generation >= 1
      && Number.isSafeInteger(projection.expiresAt) && projection.expiresAt > now);
    const key = digest({ identity: binding.identity, rootPrincipal: binding.rootPrincipal, humanPrincipal: binding.humanPrincipal,
      resource: binding.resource, semanticDigest: binding.semanticDigest, sessionIdHash: binding.sessionIdHash, rootReferenceDigest: binding.rootReferenceDigest });
    for (const [id, entry] of slots) if (entry.expiresAt <= now) slots.delete(id);
    let entry = slots.get(key);
    if (entry && (entry.child !== projection.key || entry.generation !== projection.generation)) { slots.delete(key); entry = undefined; }
    if (!entry && create) {
      denied(slots.size < maxSlots);
      entry = { child: projection.key, ref: nonce(), generation: projection.generation,
        expiresAt: Math.min(now + 300000, binding.rootExpiresAt, projection.expiresAt) }; slots.set(key, entry);
    }
    denied(entry); entry.expiresAt = Math.min(entry.expiresAt, binding.rootExpiresAt, projection.expiresAt);
    return { entry, metadata };
  }
  return Object.freeze({
    read(binding, projection) {
      const { entry, metadata } = current(binding, projection, true);
      return deepFreeze({ schema: 'soty.current-context.v2', selection: { ...binding.resource.selection },
        context: { ref: entry.ref, generation: entry.generation, expiresAt: entry.expiresAt }, ...metadata });
    },
    assert(binding, projection, ref) {
      denied(opaque(ref)); const { entry, metadata } = current(binding, projection, false);
      denied(entry.ref === ref); return { generation: entry.generation, ...metadata };
    },
    receiptScope(binding, projection, ref) {
      const { entry } = current(binding, projection, false); denied(entry.ref === ref);
      // Deliberately excludes ephemeral Root refs, Source sessions and selection
      // generations. The same current actor may read a historical exact receipt
      // after restart; a different Native child/incarnation or actor may not.
      return digest({ schema: 'soty.workspace-receipt-scope.v1', identity: binding.identity,
        rootPrincipal: binding.rootPrincipal, humanPrincipal: binding.humanPrincipal,
        resource: binding.resource, semanticDigest: binding.semanticDigest, child: entry.child });
    },
    close() { slots.clear(); },
  });
}
