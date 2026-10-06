/** The durable field contains user layout and opaque identities, never directory payloads. */
export const FIELD_SCHEMA = 'soty.field.v1';
export const FIELD_ENTITY_KINDS = Object.freeze(['app', 'person', 'community', 'device', 'builtin']);
export const FIELD_LIMITS = Object.freeze({ contexts: 24, shortcuts: 256, bytes: 128 * 1024,
  idLength: 128, titleLength: 64, coordinate: 1_000_000, slotCoordinate: 10_000 });

export class FieldContractError extends TypeError {
  constructor(code = 'field_document_invalid') { super(code); this.name = 'FieldContractError'; this.code = code; }
}
const fail = (code) => { throw new FieldContractError(code); };
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value)
  && [Object.prototype, null].includes(Object.getPrototypeOf(value));
const exact = (value, keys) => object(value) && Object.keys(value).length === keys.length
  && keys.every(key => Object.hasOwn(value, key));
const printable = value => typeof value === 'string' && !/[\u0000-\u001f\u007f-\u009f]/u.test(value)
  && value.isWellFormed();
const identifier = value => printable(value) && value.length > 0 && value.length <= FIELD_LIMITS.idLength
  && value === value.trim();
const coordinate = value => typeof value === 'number' && Number.isFinite(value) && Math.abs(value) <= FIELD_LIMITS.coordinate;
const slotCoordinate = value => Number.isSafeInteger(value) && Math.abs(value) <= FIELD_LIMITS.slotCoordinate;
const positiveZero = value => value === 0 ? 0 : value;

export function createFieldDocument() { return { schema: FIELD_SCHEMA, contexts: [], shortcuts: [] }; }

export function validateFieldEntity(value) {
  if (!exact(value, ['kind', 'id']) || !FIELD_ENTITY_KINDS.includes(value.kind) || !identifier(value.id)) fail();
  return { kind: value.kind, id: value.id };
}

export function fieldEntityKey(entity) { return `${entity.kind}:${entity.id}`; }

/** Strict, bounded and detached clone. Additional keys cannot smuggle metadata into storage. */
export function validateFieldDocument(value) {
  if (!exact(value, ['schema', 'contexts', 'shortcuts']) || value.schema !== FIELD_SCHEMA
    || !Array.isArray(value.contexts) || !Array.isArray(value.shortcuts)) fail();
  if (value.contexts.length > FIELD_LIMITS.contexts || value.shortcuts.length > FIELD_LIMITS.shortcuts) fail('field_limit_exceeded');
  const contexts = [], shortcuts = [], contextIds = new Set(), shortcutIds = new Set(), slots = new Map();
  for (const context of value.contexts) {
    if (!exact(context, ['contextId', 'title', 'x', 'y']) || !identifier(context.contextId)
      || !printable(context.title) || !context.title.trim() || context.title.length > FIELD_LIMITS.titleLength
      || !coordinate(context.x) || !coordinate(context.y) || contextIds.has(context.contextId)) fail();
    contextIds.add(context.contextId); slots.set(context.contextId, new Set());
    contexts.push({ contextId: context.contextId, title: context.title.trim(), x: positiveZero(context.x), y: positiveZero(context.y) });
  }
  for (const shortcut of value.shortcuts) {
    if (!exact(shortcut, ['shortcutId', 'entity', 'contextId', 'slot']) || !identifier(shortcut.shortcutId)
      || !identifier(shortcut.contextId) || !contextIds.has(shortcut.contextId) || shortcutIds.has(shortcut.shortcutId)
      || !Array.isArray(shortcut.slot) || shortcut.slot.length !== 2 || !shortcut.slot.every(slotCoordinate)) fail();
    const entity = validateFieldEntity(shortcut.entity), occupied = slots.get(shortcut.contextId);
    const slot = shortcut.slot.map(positiveZero), key = slot.join(',');
    if (occupied.has(key)) fail();
    occupied.add(key); shortcutIds.add(shortcut.shortcutId);
    shortcuts.push({ shortcutId: shortcut.shortcutId, entity, contextId: shortcut.contextId, slot });
  }
  const document = { schema: FIELD_SCHEMA, contexts, shortcuts };
  if (new TextEncoder().encode(JSON.stringify(document)).byteLength > FIELD_LIMITS.bytes) fail('field_limit_exceeded');
  return document;
}
