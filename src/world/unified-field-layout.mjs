import { axialToPixel, pixelToAxial, hexVertices, SQRT3 } from '../geometry/hex.mjs';
import { fieldEntityKey, FIELD_LIMITS } from '../../modules/field/contract.mjs';
import { softContourPath } from './field-map-layout.mjs';

export const UNIFIED_FIELD_METRICS = Object.freeze({ radius: 120, gap: 24, placementRadius: 6, placementGap: .5, person: 96, personLabel: 26,
  personLabelWidth: 120, deviceWidth: 208, deviceHeight: 44, contextTitleWidth: 280, contextTitleHeight: 76,
  contourPadding: 28, contextGap: 48 });

export function fieldSlotToPoint(slot, metrics = UNIFIED_FIELD_METRICS) {
  const point = axialToPixel([slot[1], slot[0]], metrics.placementRadius, metrics.placementGap);
  return { x: point.y, y: point.x };
}
export function fieldPointToSlot(point, metrics = UNIFIED_FIELD_METRICS) {
  const [r, q] = pixelToAxial({ x: point.y, y: point.x }, metrics.placementRadius, metrics.placementGap);
  return [q, r];
}
export const rectPoints = bounds => [{ x: bounds.left, y: bounds.top }, { x: bounds.right, y: bounds.top },
  { x: bounds.right, y: bounds.bottom }, { x: bounds.left, y: bounds.bottom }];
export function fieldBounds(points, padding = 0) {
  if (!points.length) return { left: -140, top: -160, right: 140, bottom: 160 };
  return { left: Math.min(...points.map(p => p.x)) - padding, top: Math.min(...points.map(p => p.y)) - padding,
    right: Math.max(...points.map(p => p.x)) + padding, bottom: Math.max(...points.map(p => p.y)) + padding };
}
export function fieldBoundsOverlap(a, b, gap = 0) {
  return a.left < b.right + gap && a.right + gap > b.left && a.top < b.bottom + gap && a.bottom + gap > b.top;
}
export function fieldPolygonsOverlap(a, b, gap = 0) {
  for (const polygon of [a, b]) for (let i = 0; i < polygon.length; i++) {
    const start = polygon[i], end = polygon[(i + 1) % polygon.length], dx = end.x - start.x, dy = end.y - start.y;
    const length = Math.hypot(dx, dy); if (!length) continue;
    const normal = { x: -dy / length, y: dx / length };
    const project = points => points.map(p => p.x * normal.x + p.y * normal.y);
    const first = project(a), second = project(b);
    if (Math.max(...first) + gap <= Math.min(...second) || Math.max(...second) + gap <= Math.min(...first)) return false;
  }
  return true;
}

export function fieldFootprint(kind, point, metrics = UNIFIED_FIELD_METRICS) {
  const { x: cx, y: cy } = point;
  if (kind === 'person') return rectPoints({ left: cx - metrics.personLabelWidth / 2, top: cy - metrics.person / 2,
    right: cx + metrics.personLabelWidth / 2, bottom: cy + metrics.person / 2 + metrics.personLabel });
  if (kind === 'device') return rectPoints({ left: cx - metrics.deviceWidth / 2, top: cy - metrics.deviceHeight / 2,
    right: cx + metrics.deviceWidth / 2, bottom: cy + metrics.deviceHeight / 2 });
  return hexVertices(metrics.radius).map(p => ({ x: cx - p.y, y: cy + p.x }));
}
function nodeGeometry(shortcut, context, entity, metrics) {
  const local = fieldSlotToPoint(shortcut.slot, metrics), cx = context.x + local.x, cy = context.y + local.y;
  const kind = entity.entity.kind;
  let width = metrics.radius * SQRT3, height = metrics.radius * 2;
  if (kind === 'person') {
    width = metrics.personLabelWidth; height = metrics.person + metrics.personLabel;
  } else if (kind === 'device') {
    width = metrics.deviceWidth; height = metrics.deviceHeight;
  }
  const footprint = fieldFootprint(kind, { x: cx, y: cy }, metrics);
  const top = kind === 'person' ? cy - metrics.person / 2 : cy - height / 2;
  return { shortcutId: shortcut.shortcutId, entity, contextId: context.contextId, slot: [...shortcut.slot], kind,
    cx, cy, x: cx - width / 2, y: top, width, height, footprint, bounds: fieldBounds(footprint) };
}

/** Geometry depends on persisted world coordinates, never on current viewport dimensions. */
export function layoutUnifiedField(scene, entities, metrics = UNIFIED_FIELD_METRICS) {
  const byEntity = new Map();
  for (const entity of entities) if (!byEntity.has(fieldEntityKey(entity.entity))) byEntity.set(fieldEntityKey(entity.entity), entity);
  const nodes = [], contexts = [], contextById = new Map(scene.contexts.map(context => [context.contextId, context]));
  for (const shortcut of scene.shortcuts) {
    const entity = byEntity.get(fieldEntityKey(shortcut.entity)), context = contextById.get(shortcut.contextId);
    if (entity && context) nodes.push(nodeGeometry(shortcut, context, entity, metrics));
  }
  for (const context of scene.contexts) {
    const children = nodes.filter(node => node.contextId === context.contextId);
    const points = children.length ? children.flatMap(node => node.footprint)
      : rectPoints({ left: context.x - 120, top: context.y - 110, right: context.x + 120, bottom: context.y + 110 });
    const body = fieldBounds(points, metrics.contourPadding), centreX = (body.left + body.right) / 2;
    const header = { x: centreX - metrics.contextTitleWidth / 2, y: body.top - metrics.contextTitleHeight * .55,
      width: metrics.contextTitleWidth, height: metrics.contextTitleHeight };
    const headerBounds = { left: header.x, top: header.y, right: header.x + header.width, bottom: header.y + header.height };
    const bounds = fieldBounds([...rectPoints(body), ...rectPoints(headerBounds)]);
    contexts.push({ ...context, children: children.map(node => node.shortcutId), bounds, body, header,
      contour: softContourPath(points, metrics.contourPadding) });
  }
  const bounds = contexts.length ? fieldBounds(contexts.flatMap(context => rectPoints(context.bounds))) : null;
  return { nodes, contexts, bounds };
}

/** Small spatial buckets keep viewport work local; oversized contexts remain in a separate list. */
export function createFieldSpatialIndex(nodes, bucketSize = 320) {
  const buckets = new Map(), large = [];
  for (const node of nodes) {
    const left = Math.floor(node.bounds.left / bucketSize), right = Math.floor(node.bounds.right / bucketSize);
    const top = Math.floor(node.bounds.top / bucketSize), bottom = Math.floor(node.bounds.bottom / bucketSize);
    if ((right - left + 1) * (bottom - top + 1) > 64) { large.push(node); continue; }
    for (let x = left; x <= right; x++) for (let y = top; y <= bottom; y++) {
      const key = `${x},${y}`; if (!buckets.has(key)) buckets.set(key, []); buckets.get(key).push(node);
    }
  }
  return { query(bounds, overscan = 100) {
    const area = { left: bounds.left - overscan, top: bounds.top - overscan, right: bounds.right + overscan, bottom: bounds.bottom + overscan };
    const selected = new Map(large.filter(node => fieldBoundsOverlap(node.bounds, area)).map(node => [node.shortcutId, node]));
    const left = Math.floor(area.left / bucketSize), right = Math.floor(area.right / bucketSize);
    const top = Math.floor(area.top / bucketSize), bottom = Math.floor(area.bottom / bucketSize);
    // An overview can span arbitrarily distant user coordinates. Do not iterate millions of empty buckets.
    const visit = values => { for (const node of values) if (fieldBoundsOverlap(node.bounds, area)) selected.set(node.shortcutId, node); };
    if ((right - left + 1) * (bottom - top + 1) > 4096) { for (const values of buckets.values()) visit(values); }
    else for (let x = left; x <= right; x++) for (let y = top; y <= bottom; y++) visit(buckets.get(`${x},${y}`) ?? []);
    return [...selected.values()];
  } };
}

export function nearestFieldNode(nodes, currentId, direction) {
  const current = nodes.find(node => node.shortcutId === currentId) ?? nodes[0]; if (!current) return null;
  const [dx, dy] = direction;
  const eligible = nodes.filter(node => node.shortcutId !== current.shortcutId).map(node => {
    const x = node.cx - current.cx, y = node.cy - current.cy;
    return { node, forward: x * dx + y * dy, score: Math.hypot(x, y) + Math.abs(x * dy - y * dx) * 2.4 };
  }).filter(item => item.forward > 1).sort((a, b) => a.score - b.score || a.node.shortcutId.localeCompare(b.node.shortcutId));
  return eligible[0]?.node ?? null;
}

export function fieldMovePreview(document, entities, shortcutId, target, metrics = UNIFIED_FIELD_METRICS) {
  if (!target || !Array.isArray(target.slot) || target.slot.length !== 2 || !target.slot.every(value => Number.isSafeInteger(value) && Math.abs(value) <= FIELD_LIMITS.slotCoordinate))
    return { valid: false, reason: 'missing', target };
  const source = document.shortcuts.find(item => item.shortcutId === shortcutId);
  if (!source || !document.contexts.some(item => item.contextId === target.contextId)) return { valid: false, reason: 'missing', target };
  const occupied = document.shortcuts.find(item => item.shortcutId !== shortcutId && item.contextId === target.contextId
    && item.slot[0] === target.slot[0] && item.slot[1] === target.slot[1]);
  const scene = structuredClone(document), moved = scene.shortcuts.find(item => item.shortcutId === shortcutId);
  if (occupied) { const other = scene.shortcuts.find(item => item.shortcutId === occupied.shortcutId); other.contextId = source.contextId; other.slot = [...source.slot]; }
  moved.contextId = target.contextId; moved.slot = [...target.slot];
  const layout = layoutUnifiedField(scene, entities, metrics), changed = new Set([shortcutId, occupied?.shortcutId].filter(Boolean));
  for (const node of layout.nodes.filter(item => changed.has(item.shortcutId))) {
    if (layout.nodes.some(other => other.shortcutId !== node.shortcutId && fieldPolygonsOverlap(node.footprint, other.footprint, 10)))
      return { valid: false, reason: 'collision', target, occupied: occupied?.shortcutId };
  }
  return { valid: true, reason: occupied ? 'swap' : 'free', target, occupied: occupied?.shortcutId, layout };
}
