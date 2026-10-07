import { createFieldDocument, validateFieldDocument, validateFieldEntity } from '../../modules/field/contract.mjs';
import { hexSpiral } from '../geometry/hex.mjs';
import { fieldSlotToPoint, fieldFootprint, fieldPolygonsOverlap, fieldBounds, fieldBoundsOverlap } from './unified-field-layout.mjs';

const fail = code => { throw Object.assign(new Error(code), { code }); };
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
const preferred = Object.freeze({ app: [[0, 0], [-25, 19], [6, 19]], builtin: [[0, 0], [-25, 19], [6, 19]],
  person: [[-24, -3], [29, 5], [-34, 19], [40, 19]], device: [[-17, 34], [-41, 34], [7, 34]], community: [[0, 0], [-25, 19], [6, 19]] });
const candidates = hexSpiral(12).map(([q, r]) => [q * 24, r * 24]);

export function nextFieldSlot(document, contextId, kind = 'app') {
  const context = document.contexts.find(context => context.contextId === contextId);
  if (!context) fail('field_context_missing');
  const existing = document.shortcuts.filter(shortcut => shortcut.contextId === contextId);
  const occupied = new Set(existing.map(shortcut => shortcut.slot.join(',')));
  const contexts = new Map(document.contexts.map(value => [value.contextId, value]));
  const worldPoint = (slot, origin) => { const point = fieldSlotToPoint(slot); return { x: origin.x + point.x, y: origin.y + point.y }; };
  const footprints = document.shortcuts.map(shortcut => {
    const origin = contexts.get(shortcut.contextId); if (!origin) fail('field_context_missing');
    const polygon = fieldFootprint(shortcut.entity.kind, worldPoint(shortcut.slot, origin)); return { polygon, bounds: fieldBounds(polygon) };
  });
  const slot = [...(preferred[kind] ?? preferred.app), ...candidates].find(value => {
    if (occupied.has(value.join(','))) return false;
    const polygon = fieldFootprint(kind, worldPoint(value, context)), bounds = fieldBounds(polygon);
    return !footprints.some(other => fieldBoundsOverlap(bounds, other.bounds, 10) && fieldPolygonsOverlap(polygon, other.polygon, 10));
  });
  if (!slot) fail('field_context_full');
  return [...slot];
}

/** A command has no authority-bearing operation: every result is only a detached field document. */
export function applyFieldCommand(value, command) {
  const before = validateFieldDocument(value), document = structuredClone(before);
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
    document.shortcuts.push({ shortcutId: command.shortcutId, entity: validateFieldEntity(command.entity), contextId: command.contextId,
      slot: command.slot ? [...command.slot] : nextFieldSlot(document, command.contextId, command.entity.kind) });
    affected.push(command.shortcutId);
  } else if (command.type === 'remove-shortcut') {
    shortcut(command.shortcutId); affected.push(command.shortcutId);
    document.shortcuts = document.shortcuts.filter(item => item.shortcutId !== command.shortcutId);
  } else if (command.type === 'move-shortcut') {
    const source = shortcut(command.shortcutId); context(command.contextId);
    if (!Array.isArray(command.slot) || command.slot.length !== 2) fail('field_command_invalid');
    const other = document.shortcuts.find(item => item.shortcutId !== source.shortcutId && item.contextId === command.contextId && same(item.slot, command.slot));
    if (other) {
      if (command.swap !== true) fail('field_slot_occupied');
      other.contextId = source.contextId; other.slot = [...source.slot]; affected.push(other.shortcutId);
    }
    source.contextId = command.contextId; source.slot = [...command.slot]; affected.push(source.shortcutId);
  } else if (command.type === 'restore') {
    const restored = validateFieldDocument(command.document);
    return { before, document: restored, changed: !same(before, restored), affected: restored.shortcuts.map(item => item.shortcutId), type: command.type };
  } else fail('field_command_invalid');
  const after = validateFieldDocument(document);
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
  if (!same(last.after, validateFieldDocument(current))) fail('field_undo_conflict');
  history.entries.pop();
  return applyFieldCommand(current, { type: 'restore', document: last.before, expected: last.after });
}
export { createFieldDocument };
