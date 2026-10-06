import { createFieldDocument, fieldEntityKey } from '../../modules/field/contract.mjs';
import { nextFieldSlot } from './unified-field-state.mjs';

/** A query owns its placements. Appending/removing results never reassigns an old node. */
export function createFieldSearchPlacement({ id = () => crypto.randomUUID() } = {}) {
  const placements = new Map(), contexts = new Map(), contextEntities = new Map(); let activeScope = '';
  const titles = { app: 'Приложения', person: 'Люди', community: 'Сообщества' };
  function clear() { placements.clear(); contexts.clear(); contextEntities.clear(); }
  return {
    clear,
    contextEntity(contextId) { return contextEntities.get(contextId) ?? null; },
    update(scope, items, metadata) {
      if (scope !== activeScope) { activeScope = scope; clear(); }
      const visibleItems = [...new Map(items.slice(0, 256).map(item => [fieldEntityKey(item.entity), item])).values()];
      const byEntity = new Map(metadata.map(item => [fieldEntityKey(item.entity), item]));
      for (const item of visibleItems) byEntity.set(fieldEntityKey(item.entity), item);
      const contextFor = (key, title) => {
        const known = contexts.get(key); if (known) { known.title = title; return known.contextId; }
        const index = contexts.size, contextId = id();
        contexts.set(key, { contextId, title, x: index % 2 * 900, y: Math.floor(index / 2) * 850 }); return contextId;
      };
      for (const item of visibleItems) {
        const key = fieldEntityKey(item.entity), kind = item.entity.kind === 'builtin' ? 'app' : item.entity.kind;
        const count = [...placements.keys()].filter(placed => placed.startsWith(`${kind}:`) || kind === 'app' && placed.startsWith('builtin:')).length;
        const bucket = kind === 'community' ? `community:${item.entity.id}` : `${kind}:${Math.floor(count / 8)}`;
        if (placements.has(key)) {
          const context = [...contexts.values()].find(value => value.contextId === placements.get(key).contextId);
          if (kind === 'community' && context) { context.title = item.title; contextEntities.set(context.contextId, item); }
          continue;
        }
        const contextId = contextFor(bucket, kind === 'community' ? item.title : titles[kind] ?? 'Найденное');
        const document = createFieldDocument(); document.contexts = [...contexts.values()];
        // Retired slots stay reserved within this query. Reappearing entities
        // must not collide with a newer result that reused their old position.
        document.shortcuts = [...placements.values()].map(placement => ({ ...placement, entity: { ...placement.entity }, slot: [...placement.slot] }));
        placements.set(key, { shortcutId: id(), contextId, entity: { ...item.entity }, slot: nextFieldSlot(document, contextId, item.entity.kind) });
        if (kind === 'community') contextEntities.set(contextId, item);
      }
      const visible = new Set(visibleItems.map(item => fieldEntityKey(item.entity))), document = createFieldDocument();
      document.shortcuts = [...placements.entries()].flatMap(([key, placement]) => {
        const item = visible.has(key) ? byEntity.get(key) : null; return item ? [{ ...placement, slot: [...placement.slot], entity: { ...item.entity } }] : [];
      });
      const active = new Set(document.shortcuts.map(shortcut => shortcut.contextId));
      document.contexts = [...contexts.values()].filter(context => active.has(context.contextId)).map(context => ({ ...context }));
      for (const contextId of contextEntities.keys()) if (!active.has(contextId)) contextEntities.delete(contextId);
      return document;
    },
  };
}
