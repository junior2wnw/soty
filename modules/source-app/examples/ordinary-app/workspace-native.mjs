import { createSourceNativeAuthorityPort, isSourceNativeCommitPort } from '../../server/native-authority.mjs';
import { check, fields, digest, nonce } from '../../server/wire.mjs';

/** Example adapter: the existing Native tables own application namespace AND
 * each person's child ACL. UI selection is a locator, never permission. */
export function createOrdinaryWorkspacePort({ base, store, children, initialChild,
  creation = { operation: 'items.create', titleField: 'title', artifactType: 'ordinary-item' } }) {
  check(Array.isArray(children) && children.length > 0 && children.length <= 32 && new Set(children).size === children.length
    && (children.includes(initialChild) || typeof initialChild === 'function'));
  const { db } = store, proofs = new WeakMap(), slots = new Map();
  fields(creation, ['operation', 'titleField', 'artifactType'], ['category']);
  check([creation.operation, creation.titleField, creation.artifactType].every(value => typeof value === 'string' && /^[a-z][a-z0-9_.-]{0,63}$/u.test(value)));
  const slotKey = binding => digest([binding.rootReferenceDigest, binding.sessionIdHash]);
  function member(binding, id) {
    check(children.includes(id), 'workspace_child_denied', 403);
    const row = db.prepare(`SELECT r.title,r.incarnation_id,m.role,m.active,s.principal_id,s.expires_at FROM native_resources r
      JOIN native_memberships m ON m.resource_id=r.id JOIN source_sessions s ON s.principal_id=m.principal_id
      WHERE r.id=? AND r.realm_id=? AND s.id_hash=? AND s.active=1`).get(id, store.realmId, binding.sessionIdHash);
    check(row?.active === 1 && row.expires_at > store.clock(), 'workspace_child_denied', 403); return row;
  }
  function inspect(proof, binding) {
    const held = proofs.get(proof); check(held, 'workspace_proof_denied', 403);
    if (!held.child) return held;
    const selected = slots.get(slotKey(binding));
    check(selected && selected.id === held.child.id && selected.generation === held.child.generation, 'workspace_child_changed', 403);
    const currentMember = member(binding, selected.id);
    check(currentMember.incarnation_id === held.child.incarnationId, 'workspace_child_changed', 403);
    return { ...held, member: currentMember };
  }
  function wrap(native, binding) {
    let child = null;
    if (binding.sessionIdHash) {
      for (const [id, entry] of slots) if (entry.expiresAt <= store.clock()) slots.delete(id);
      const key = slotKey(binding); let selected = slots.get(key);
      if (!selected) { check(slots.size < 128, 'workspace_capacity', 503);
        selected = { id: typeof initialChild === 'function' ? initialChild(binding) : initialChild, generation: 1 }; member(binding, selected.id); slots.set(key, selected); }
      const currentMember = member(binding, selected.id); selected.expiresAt = currentMember.expires_at;
      child = { ...selected, incarnationId: currentMember.incarnation_id };
    }
    const proof = Object.freeze({}); proofs.set(proof, { native, child }); return proof;
  }
  const methods = {
    async capture(binding, request) { return wrap(await base.capture(binding, request), binding); },
    withCurrent(proof, binding, action) {
      const held = proofs.get(proof); check(held, 'workspace_proof_denied', 403);
      return base.withCurrent(held.native, binding, () => { inspect(proof, binding); return action(); });
    },
    currentContext(proof, binding) {
      const held = inspect(proof, binding); check(held.child, 'workspace_context_unavailable', 403);
      const title = held.member.title.replace(/[\u0000-\u001f\u007f]/gu, ' ').trim().slice(0, 240).replace(/[\uD800-\uDBFF]$/u, '') || 'Мой проект';
      const revision = db.prepare('SELECT COUNT(*)+1 AS revision FROM native_items WHERE resource_id=?').get(held.child.id).revision;
      return { key: digest([held.child.id, held.child.incarnationId]), generation: held.child.generation, expiresAt: held.member.expires_at,
        title, role: held.member.role === 'owner' ? 'owner' : 'editor', revision };
    },
    selectContext(proof, binding, { nativeId, generation }) {
      const held = inspect(proof, binding), key = slotKey(binding), selected = slots.get(key);
      check(generation > (selected.uiGeneration ?? 0), 'workspace_selection_stale', 409);
      const currentMember = member(binding, nativeId);
      const next = { id: nativeId, generation: selected.generation + 1, uiGeneration: generation, expiresAt: currentMember.expires_at };
      slots.set(key, next);
      // Rebind only this checked selection request; concurrent old proofs stay old.
      proofs.set(proof, { ...held, child: { ...next, incarnationId: currentMember.incarnation_id } });
    },
    read(proof, binding, args) {
      const held = inspect(proof, binding), input = fields(args.input, ['operation']); check(input.operation === 'items.list');
      const items = db.prepare('SELECT id,title,revision,created_at AS createdAt FROM native_items WHERE resource_id=? ORDER BY created_at,id LIMIT 100').all(held.child.id);
      return { requestId: args.requestId, items: items.map(row => ({ ...row })) };
    },
    execute(proof, binding, args, final) {
      const input = fields(args.input, ['operation', creation.titleField, ...(creation.category ? ['category'] : [])]), title = input[creation.titleField];
      check(input.operation === creation.operation && typeof title === 'string' && title.trim() && title.length <= 500
        && (!creation.category || input.category === creation.category));
      check(isSourceNativeCommitPort(final) && typeof final.contextAfter === 'function');
      return store.tx(() => final.commit(() => {
        const held = inspect(proof, binding); check(['owner', 'editor', 'participant'].includes(held.member.role), 'workspace_write_denied', 403);
        const inputDigest = digest(input), prior = db.prepare('SELECT * FROM native_receipts WHERE resource_id=? AND principal_id=? AND request_id=?').get(held.child.id, held.member.principal_id, args.requestId);
        check(!prior || prior.input_digest === inputDigest, 'workspace_request_conflict', 409);
        if (prior) return { ...JSON.parse(prior.result_json), replayed: true };
        const id = 'item-' + nonce().slice(0, 20), createdAt = store.clock();
        db.prepare('INSERT INTO native_items VALUES(?,?,?,?,?)').run(id, held.child.id, title, 1, createdAt);
        const result = { requestId: args.requestId, inputDigest, outcome: 'committed', replayed: false,
          receipt: { id, revision: 1, createdAt }, contextAfter: final.contextAfter(),
          workspaceReceipt: { schema: 'soty.workspace-receipt.v1',
            effects: [{ kind: 'created', resourceType: creation.artifactType, resourceId: id, revision: 1 }],
            receipt: { verificationMethod: 'domain_read', artifacts: [{ type: creation.artifactType, id, revision: 1 }] } } };
        db.prepare('INSERT INTO native_receipts VALUES(?,?,?,?,?)').run(held.child.id, held.member.principal_id, args.requestId, inputDigest, JSON.stringify(result));
        return result;
      }));
    },
    readProof(proof, binding, args) {
      const held = inspect(proof, binding), input = fields(args.input, ['inputDigest']); check(/^[a-f0-9]{64}$/u.test(input.inputDigest));
      const row = db.prepare('SELECT * FROM native_receipts WHERE resource_id=? AND principal_id=? AND request_id=?').get(held.child.id, held.member.principal_id, args.requestId);
      check(!row || row.input_digest === input.inputDigest, 'workspace_request_conflict', 409);
      return row ? JSON.parse(row.result_json) : { requestId: args.requestId, inputDigest: input.inputDigest, outcome: 'unknown' };
    },
  };
  for (const name of ['rememberLogin', 'linkVerifiedIdentity', 'createEmptyGuest']) if (base[name]) methods[name] = (proof, binding, ...args) => base[name](proofs.get(proof).native, binding, ...args);
  if (base.recoverLogin) methods.recoverLogin = async (binding, ...args) => wrap(await base.recoverLogin(binding, ...args), binding);
  return Object.freeze({ port: createSourceNativeAuthorityPort(methods) });
}
