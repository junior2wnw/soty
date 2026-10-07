import type { Entity, EntityDraft } from './types.js';

const protectedFields = new Set([
  'id',
  'workspaceId',
  'baseline',
  'source',
  'createdAt',
  'updatedAt',
  'version',
  'forecastProvenance',
]);

/** Editor drafts include context; a PATCH contains only actual, editable changes. */
export function editableEntityPatch(entity: Entity, draft: EntityDraft): Partial<EntityDraft> {
  return Object.fromEntries(
    Object.entries(draft).filter(
      ([key, value]) =>
        !protectedFields.has(key) &&
        JSON.stringify(value) !== JSON.stringify(entity[key as keyof Entity]),
    ),
  );
}
