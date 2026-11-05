/** Async media decode never holds Connect's SQLite writer. Each authority check
 * re-enters the original signed actor's current installation fence. */
export function createMaterialsExtension({ materials, connect, onChange = () => {} }) {
  return Object.freeze({ operations: materials.operations,challengeStorage:materials.challengeStorage,
    async executeAsync({ op, args, actor, signal }) {
      const guard = () => {
        if (signal?.aborted) throw Object.assign(new Error('materials_cancelled'), { code: 'materials_cancelled' });
        return connect().withActorAuthorityFence(actor, () => true);
      };
      const result = await materials.execute({ op, args, actor, guard });
      guard();
      if (['materials.commit', 'materials.trash', 'materials.restore'].includes(op) && !result.replayed) {
        try { onChange({ accountIds: [actor.accountId],source:{providerId:'soty.materials',ownerId:actor.accountId,ref:result.material?.materialId??result.materialId} }); } catch { /* The durable write already succeeded. */ }
      }
      return result;
    },
  });
}
