import { captureQueryData } from '../modules/capabilities/server/readonly-queries.mjs';
import { AccessError, canonicalHash } from '../modules/capabilities/server/validation.mjs';

export const HIVE_WORKFLOW_LIMITS = Object.freeze({ cards: 12, links: 16 });
const need = value => { if (!value) throw new AccessError('selected_native_source_denied'); };
const identifier = value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{1,96}$/u.test(value);
const fields = (value, names) => need(value && !Array.isArray(value) && typeof value === 'object'
  && Object.keys(value).length === names.length && names.every(name => Object.hasOwn(value, name)));

/** An additive JSON workflow is compiled to the existing HIVE v2 atomic batch.
 * No import/replace, existing-cell edits, Native selector or authority fields. */
export function selectedHiveWorkflowOperations(raw) {
  const value = captureQueryData(raw);
  fields(value, ['operation', 'expectedRevision', 'viewId', 'cards', 'links']);
  need(value.operation === 'hive.workflow.create' && identifier(value.viewId)
    && Number.isSafeInteger(value.expectedRevision) && value.expectedRevision >= 1
    && value.expectedRevision < Number.MAX_SAFE_INTEGER
    && Array.isArray(value.cards) && value.cards.length >= 2 && value.cards.length <= HIVE_WORKFLOW_LIMITS.cards
    && Array.isArray(value.links) && value.links.length >= 1 && value.links.length <= HIVE_WORKFLOW_LIMITS.links);
  const ids = new Set(), cards = new Set(), operations = [];
  const unique = id => { need(identifier(id) && !ids.has(id)); ids.add(id); };
  for (const card of value.cards) {
    fields(card, ['id', 'title', 'x', 'y']); unique(card.id); cards.add(card.id);
    need(typeof card.title === 'string' && card.title.isWellFormed() && card.title.trim() && card.title.length <= 240
      && [card.x, card.y].every(number => Number.isFinite(number) && Math.abs(number) <= 1000000));
    operations.push({ op: 'object.add', object: { id: card.id, type: 'hive.process/action', title: card.title,
      summary: '', content: [], properties: {}, extensions: {} } },
    { op: 'occurrence.add', viewId: value.viewId, occurrence: { id: `${card.id}:first`,
      target: { kind: 'object', id: card.id }, x: card.x, y: card.y, width: 280, height: 240 } });
  }
  for (const link of value.links) {
    fields(link, ['id', 'sourceId', 'targetId']); unique(link.id);
    need(cards.has(link.sourceId) && cards.has(link.targetId));
    operations.push({ op: 'relation.add', relation: { id: link.id, type: 'hive.process/flow',
      endpoints: [{ role: 'source', target: { kind: 'object', id: link.sourceId } },
        { role: 'target', target: { kind: 'object', id: link.targetId } }], properties: {}, extensions: {} } });
  }
  return operations;
}

/** Admit exactly the Native representation produced by the SAME bounded
 * workflow compiler above. This is validation only: original input, digest,
 * requestId and Native receipt remain unchanged. Native's more permissive
 * standalone operation parser is not claimed to have changed. */
export function validateHiveWorkflowNativeInput(raw) {
  const value = captureQueryData(raw); fields(value, ['operation', 'expectedRevision', 'operations']);
  need(value.operation === 'hive.operations' && Array.isArray(value.operations));
  const cards = [], links = []; let viewId, offset = 0;
  while (value.operations[offset]?.op === 'object.add') {
    const add = value.operations[offset++], place = value.operations[offset++];
    fields(add, ['op', 'object']); fields(place, ['op', 'viewId', 'occurrence']);
    need(place.op === 'occurrence.add' && add.object && place.occurrence);
    viewId ??= place.viewId;
    cards.push({ id: add.object.id, title: add.object.title, x: place.occurrence.x, y: place.occurrence.y });
  }
  for (; offset < value.operations.length; offset++) {
    const add = value.operations[offset]; fields(add, ['op', 'relation']);
    need(add.op === 'relation.add' && Array.isArray(add.relation?.endpoints) && add.relation.endpoints.length === 2);
    links.push({ id: add.relation.id, sourceId: add.relation.endpoints[0]?.target?.id, targetId: add.relation.endpoints[1]?.target?.id });
  }
  const expected = selectedHiveWorkflowOperations({ operation: 'hive.workflow.create', expectedRevision: value.expectedRevision, viewId, cards, links });
  need(canonicalHash(value.operations) === canonicalHash(expected)); return true;
}
