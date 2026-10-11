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
export function createWorkspaceContextAuthority({ clock = Date.now } = {}) {
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
      denied(slots.size < 128);
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
    close() { slots.clear(); },
  });
}
