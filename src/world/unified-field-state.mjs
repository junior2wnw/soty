import { createFieldDocument, validateAnyFieldDocument, validateAnyFieldEntity as validateFieldEntity, fieldShortcutPlacement, validateFieldPlacement,convertFieldDocumentToV2 } from '../../modules/field/contract.mjs';
import { hexSpiral } from '../geometry/hex.mjs';
import { fieldSlotToPoint, fieldFootprint, fieldPolygonsOverlap, fieldBounds, fieldBoundsOverlap,rectPoints, UNIFIED_FIELD_METRICS } from './unified-field-layout.mjs';

const fail = code => { throw Object.assign(new Error(code), { code }); };
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
const preferred = Object.freeze([[0, 0], [0, -21], [21, -21], [-21, 0], [21, 0], [-21, 21], [0, 21]].map(Object.freeze));
const candidates = hexSpiral(12).map(([q, r]) => [q * 21, r * 21]);

export function nextFieldSlot(document, contextId, kind = 'app', metrics = UNIFIED_FIELD_METRICS) {
  const context = document.contexts.find(context => context.contextId === contextId);
  if (!context) fail('field_context_missing');
  const existing = document.shortcuts.filter(shortcut => shortcut.contextId === contextId);
  const occupied = new Set(existing.map(fieldShortcutPlacement).filter(value=>value.kind==='hex').map(value=>value.slot.join(',')));
  const contexts = new Map(document.contexts.map(value => [value.contextId, value]));
  const worldPoint = (slot, origin) => { const point = fieldSlotToPoint(slot, metrics); return { x: origin.x + point.x, y: origin.y + point.y }; };
  const footprints = document.shortcuts.map(shortcut => {
    const origin = contexts.get(shortcut.contextId); if (!origin) fail('field_context_missing');
    const placement=fieldShortcutPlacement(shortcut),point=placement.kind==='free'?{x:origin.x+placement.x,y:origin.y+placement.y}:worldPoint(placement.slot,origin);
    const polygon = placement.kind==='free'?rectPoints({left:point.x-placement.width/2,right:point.x+placement.width/2,top:point.y-placement.height/2,bottom:point.y+placement.height/2}):fieldFootprint(shortcut.entity.kind, point, metrics); return { polygon, bounds: fieldBounds(polygon) };
  });
  const slot = [...preferred, ...candidates].find(value => {
    if (occupied.has(value.join(','))) return false;
    const polygon = fieldFootprint(kind, worldPoint(value, context), metrics), bounds = fieldBounds(polygon);
    return !footprints.some(other => fieldBoundsOverlap(bounds, other.bounds, 10) && fieldPolygonsOverlap(polygon, other.polygon, 10));
  });
  if (!slot) fail('field_context_full');
  return [...slot];
}

/** Detached preview only. The caller explicitly applies one existing restore
 * command after confirming this exact prior document. Never moves other spaces. */
export function compactFieldContext(value, contextId, metrics = UNIFIED_FIELD_METRICS) {
  const before = validateAnyFieldDocument(value), document = structuredClone(before);
  if (!document.contexts.some(context => context.contextId === contextId)) fail('field_context_missing');
  const own = document.shortcuts.filter(shortcut => shortcut.contextId === contextId&&fieldShortcutPlacement(shortcut).kind==='hex');
  document.shortcuts = document.shortcuts.filter(shortcut => !own.some(item=>item.shortcutId===shortcut.shortcutId));
  for (const shortcut of own){const slot=nextFieldSlot(document, contextId, shortcut.entity.kind, metrics);document.shortcuts.push(document.schema==='soty.field.v1'?{...shortcut,slot}:{...shortcut,placement:{kind:'hex',slot}});}
  const placed = new Map(document.shortcuts.map(shortcut => [shortcut.shortcutId, shortcut]));
  document.shortcuts = before.shortcuts.map(shortcut => placed.get(shortcut.shortcutId));
  return validateAnyFieldDocument(document);
}

/** A command has no authority-bearing operation: every result is only a detached field document. */
export function applyFieldCommand(value, command, metrics = UNIFIED_FIELD_METRICS) {
  const before = validateAnyFieldDocument(value);let document = structuredClone(before);
  if (!command || typeof command !== 'object') fail('field_command_invalid');
  if (command.expected && !same(command.expected, before)) fail('field_conflict');
  const context = id => document.contexts.find(item => item.contextId === id) ?? fail('field_context_missing');
  const shortcut = id => document.shortcuts.find(item => item.shortcutId === id) ?? fail('field_shortcut_missing');
  const affected = [];
  if (command.type === 'add-context') {
    document.contexts.push({ contextId: command.contextId, title: command.title, x: command.x, y: command.y });
  } else if (command.type === 'rename-context') {
    context(command.contextId).title = command.title;
  } else if (command.type === 'move-context') {
    Object.assign(context(command.contextId), { x: command.x, y: command.y });
  } else if (command.type === 'remove-context') {
    context(command.contextId);
    if (document.shortcuts.some(item => item.contextId === command.contextId) && command.removeShortcuts !== true) fail('field_context_not_empty');
    if (command.removeShortcuts === true) {
      affected.push(...document.shortcuts.filter(item => item.contextId === command.contextId).map(item => item.shortcutId));
      document.shortcuts = document.shortcuts.filter(item => item.contextId !== command.contextId);
    }
    document.contexts = document.contexts.filter(item => item.contextId !== command.contextId);
  } else if (command.type === 'add-shortcut') {
    context(command.contextId);
    if(command.placement||command.entity?.kind==='object'){const placement=validateFieldPlacement(command.placement??{kind:'hex',slot:command.slot??nextFieldSlot(document,command.contextId,command.entity.kind,metrics)});document=convertFieldDocumentToV2(document);
      document.shortcuts.push({shortcutId:command.shortcutId,entity:validateFieldEntity(command.entity),contextId:command.contextId,placement});}
    else {const slot=command.slot ? [...command.slot] : nextFieldSlot(document, command.contextId, command.entity.kind, metrics);
      document.shortcuts.push(document.schema==='soty.field.v1'?{shortcutId:command.shortcutId,entity:validateFieldEntity(command.entity),contextId:command.contextId,slot}
        :{shortcutId:command.shortcutId,entity:validateFieldEntity(command.entity),contextId:command.contextId,placement:{kind:'hex',slot}});}
    affected.push(command.shortcutId);
  } else if (command.type === 'remove-shortcut') {
    shortcut(command.shortcutId); affected.push(command.shortcutId);
    document.shortcuts = document.shortcuts.filter(item => item.shortcutId !== command.shortcutId);
  } else if (command.type === 'move-shortcut') {
    const source = shortcut(command.shortcutId); context(command.contextId);
    if (!Array.isArray(command.slot) || command.slot.length !== 2) fail('field_command_invalid');
    const sourcePlacement=fieldShortcutPlacement(source);
    const other = document.shortcuts.find(item => item.shortcutId !== source.shortcutId && item.contextId === command.contextId && fieldShortcutPlacement(item).kind==='hex' && same(fieldShortcutPlacement(item).slot, command.slot));
    if (other) {
      if (command.swap !== true) fail('field_slot_occupied');
      other.contextId = source.contextId;if(document.schema==='soty.field.v1')other.slot=[...source.slot];else other.placement=sourcePlacement;affected.push(other.shortcutId);
    }
    source.contextId = command.contextId;if(document.schema==='soty.field.v1')source.slot=[...command.slot];else source.placement={kind:'hex',slot:[...command.slot]};affected.push(source.shortcutId);
  } else if(command.type==='place-shortcut'){
    shortcut(command.shortcutId);context(command.contextId);const placement=validateFieldPlacement(command.placement);
    document=convertFieldDocumentToV2(document);const source=shortcut(command.shortcutId);source.contextId=command.contextId;source.placement=placement;affected.push(source.shortcutId);
  } else if (command.type === 'restore') {
    const restored = validateAnyFieldDocument(command.document);
    return { before, document: restored, changed: !same(before, restored), affected: restored.shortcuts.map(item => item.shortcutId), type: command.type };
  } else fail('field_command_invalid');
  const after = validateAnyFieldDocument(document);
  return { before, document: after, changed: !same(before, after), affected, type: command.type };
}

export function createFieldHistory(limit = 30) { return { limit, entries: [] }; }
export function recordFieldHistory(history, change) {
  if (!change.changed) return;
  history.entries.push({ before: structuredClone(change.before), after: structuredClone(change.document) });
  if (history.entries.length > history.limit) history.entries.splice(0, history.entries.length - history.limit);
}
export function undoFieldHistory(history, current) {
  const last = history.entries.at(-1); if (!last) return null;
  if (!same(last.after, validateAnyFieldDocument(current))) fail('field_undo_conflict');
  history.entries.pop();
  return applyFieldCommand(current, { type: 'restore', document: last.before, expected: last.after });
}
export { createFieldDocument };
