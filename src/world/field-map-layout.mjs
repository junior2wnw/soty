import { axialToPixel, axialDistance, hexVertices, hexSpiral, SQRT3 } from '../geometry/hex.mjs';

export const FIELD_SCALE_MIN = .65;
export const FIELD_SCALE_MAX = 1.4;
const TITLE_HEIGHT = 58;
const SIDE_ROOM = 78;
const PERSON_WIDTH = 76;
const PERSON_HEIGHT = 94;

export function clampFieldScale(value) {
  return Number.isFinite(value) ? Math.max(FIELD_SCALE_MIN, Math.min(FIELD_SCALE_MAX, value)) : 1;
}

export function fieldEntityId(entity) {
  return entity.type === 'community' ? `community:${entity.value.communityId}` : `person:${entity.value.profileId}`;
}

function hull(points) {
  const sorted = [...points].sort((a, b) => a.x - b.x || a.y - b.y);
  const cross = (a, b, c) => (b.x - a.x) * (c.y - a.y) - (b.y - a.y) * (c.x - a.x);
  const half = values => {
    const result = [];
    for (const point of values) {
      while (result.length > 1 && cross(result.at(-2), result.at(-1), point) <= 1e-7) result.pop();
      result.push(point);
    }
    return result;
  };
  return [...half(sorted).slice(0, -1), ...half(sorted.reverse()).slice(0, -1)];
}

/** A smooth common contour, derived from the real tile vertices rather than a card rectangle. */
export function softContourPath(points, padding = 20) {
  if (points.length < 3) return '';
  const boundary = hull(points);
  const expanded = boundary.map((point, index) => {
    const before = boundary[(index + boundary.length - 1) % boundary.length], after = boundary[(index + 1) % boundary.length];
    const normal = (a, b) => {
      const dx = b.x - a.x, dy = b.y - a.y, length = Math.hypot(dx, dy);
      return { x: dy / length, y: -dx / length };
    };
    const first = normal(before, point), second = normal(point, after);
    const denominator = Math.max(.2, 1 + first.x * second.x + first.y * second.y);
    return { x: point.x + (first.x + second.x) * padding / denominator, y: point.y + (first.y + second.y) * padding / denominator };
  });
  const midpoint = (a, b) => ({ x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 });
  const format = point => `${Number(point.x.toFixed(3))} ${Number(point.y.toFixed(3))}`;
  const start = midpoint(expanded.at(-1), expanded[0]);
  return `M ${format(start)} ` + expanded.map((point, index) => `Q ${format(point)} ${format(midpoint(point, expanded[(index + 1) % expanded.length]))}`).join(' ') + ' Z';
}

/** Rotate the shared regular hex geometry into the point-up presentation used by the approved field. */
export function communityFieldGeometry(tileCount = 3, radius = 78) {
  const count = Number.isFinite(tileCount) ? Math.max(1, Math.min(7, Math.floor(tileCount))) : 3, gap = 14, padding = 28;
  radius = Number.isFinite(radius) && radius > 0 ? radius : 78;
  const coordinates = count === 1 ? [[0, 0]] : count === 2 ? [[0, 0], [0, -1]]
    : count === 3 ? [[0, 0], [1, 0], [1, -1]] : hexSpiral(1).slice(0, count);
  const rotate = point => ({ x: -point.y, y: point.x });
  const centres = coordinates.map(axial => rotate(axialToPixel(axial, radius, gap)));
  const vertices = centres.flatMap(centre => hexVertices(radius).map(point => {
    const rotated = rotate(point);
    return { x: centre.x + rotated.x, y: centre.y + rotated.y };
  }));
  const minX = Math.min(...vertices.map(point => point.x)), minY = Math.min(...vertices.map(point => point.y));
  const maxX = Math.max(...vertices.map(point => point.x)), maxY = Math.max(...vertices.map(point => point.y));
  const offset = { x: padding - minX, y: TITLE_HEIGHT + padding - minY };
  const tileWidth = SQRT3 * radius, tileHeight = radius * 2;
  const cells = centres.map((point, index) => ({ axial: coordinates[index], x: point.x + offset.x, y: point.y + offset.y,
    left: point.x + offset.x - tileWidth / 2, top: point.y + offset.y - radius }));
  const shifted = vertices.map(point => ({ x: point.x + offset.x, y: point.y + offset.y }));
  return { width: maxX - minX + padding * 2, height: maxY - minY + padding * 2 + TITLE_HEIGHT,
    radius, tileWidth, tileHeight, titleHeight: TITLE_HEIGHT, cells, contour: softContourPath(shifted, 22) };
}

function uniqueBy(values, key) {
  const seen = new Set();
  return values.filter(value => {
    const id = key(value);
    if (!id || seen.has(id)) return false;
    seen.add(id);
    return true;
  });
}

/**
 * Build only from caller-supplied visible records. A grant on an app does not invent a
 * community, and a preview member does not invent a selectable person on this page.
 */
export function layoutField(entities, allowedApps = [], viewportWidth = 900, viewportHeight = 640) {
  const width = Math.max(280, Number.isFinite(viewportWidth) ? viewportWidth : 900);
  const visible = uniqueBy(entities, fieldEntityId);
  const communities = visible.filter(entity => entity.type === 'community');
  const people = visible.filter(entity => entity.type === 'person');
  const communityIds = new Set(communities.map(entity => entity.value.communityId));
  const apps = uniqueBy(allowedApps, app => app.appId).filter(app => !app.communityId || communityIds.has(app.communityId));
  const groupedApps = new Map();
  for (const app of apps) if (app.communityId) {
    if (!groupedApps.has(app.communityId)) groupedApps.set(app.communityId, []);
    groupedApps.get(app.communityId).push(app);
  }
  const publicProfiles = new Set(people.map(person => person.value.profileId));
  const items = [], connections = [], ownedPeople = new Set();
  const compact = width < 720;
  const padding = compact ? 12 : 26, gutter = compact ? 26 : 30;
  // The column grid belongs to the viewport, never to the current result count.
  // Page two must not shrink and move the first visible community.
  const minimumBlock = communityFieldGeometry(7, 58).width + SIDE_ROOM * 2;
  const columns = !compact && width >= minimumBlock * 2 + padding * 2 + gutter ? 2 : 1;
  const columnWidth = (width - padding * 2 - gutter * (columns - 1)) / columns;
  const columnBottom = Array(columns).fill(compact ? 12 : 26);
  let requiredWidth = width;

  communities.forEach((entity, communityIndex) => {
    const group = entity.value, groupId = fieldEntityId(entity);
    const groupApps = groupedApps.get(group.communityId) ?? [];
    const visibleApps = groupApps.slice(0, groupApps.length > 7 ? 6 : 7);
    const overflowCount = groupApps.length > 7 ? groupApps.length - visibleApps.length : 0;
    const tileCount = groupApps.length ? visibleApps.length + (overflowCount ? 1 : 0) : 3;
    const memberIds = new Set((group.previewMembers ?? []).map(member => member.profileId));
    const members = people.filter(person => memberIds.has(person.value.profileId) && !ownedPeople.has(fieldEntityId(person))).slice(0, 3);
    const hasSides = !compact && members.length > 0;
    const sideRoom = hasSides ? SIDE_ROOM : 0;
    // At narrow widths a complete cluster stays legible; the map remains pannable if
    // a seven-cell community cannot fit. No individual hex is squeezed or distorted.
    const target = columnWidth - sideRoom * 2;
    const radius = Math.max(compact ? 55 : 58, Math.min(82, (target - 56 - 28) / (tileCount > 3 ? SQRT3 * 3 : tileCount === 1 ? SQRT3 : SQRT3 * 2)));
    const shape = communityFieldGeometry(tileCount, radius);
    let column = 0;
    if (columns > 1) column = columnBottom[0] <= columnBottom[1] ? 0 : 1;
    const blockWidth = Math.max(columnWidth, shape.width + sideRoom * 2);
    const x = padding + column * (columnWidth + gutter) + (blockWidth - shape.width) / 2;
    const y = columnBottom[column] + (columns > 1 && communityIndex === 1 ? 22 : 0);
    items.push({ id: groupId, kind: 'community', entity, x, y, width: shape.width, height: shape.height, shape, hasApps: groupApps.length > 0, appCount: groupApps.length });
    requiredWidth = Math.max(requiredWidth, x + shape.width + sideRoom + padding);
    visibleApps.forEach((app, index) => {
      const cell = shape.cells[index];
      items.push({ id: `app:${app.appId}`, kind: 'app', app, communityId: group.communityId, x: x + cell.left, y: y + cell.top,
        width: shape.tileWidth, height: shape.tileHeight, radius, axial: cell.axial });
    });
    if (overflowCount) {
      const cell = shape.cells[visibleApps.length];
      items.push({ id: `more:${group.communityId}`, kind: 'overflow', entity, count: overflowCount, x: x + cell.left, y: y + cell.top, width: shape.tileWidth, height: shape.tileHeight, radius });
    }
    if (groupApps.length > 1) {
      shape.cells.forEach((cell, index) => shape.cells.slice(index + 1).forEach(other => {
        if (axialDistance(cell.axial, other.axial) === 1) connections.push({ kind: 'app', communityId: group.communityId,
          from: { x: x + cell.x, y: y + cell.y }, to: { x: x + other.x, y: y + other.y } });
      }));
    }
    let blockBottom = y + shape.height;
    members.forEach((person, index) => {
      const id = fieldEntityId(person);
      ownedPeople.add(id);
      const personX = hasSides && index < 2 ? index === 0 ? x - SIDE_ROOM : x + shape.width + 2
        : x + shape.width / 2 - PERSON_WIDTH / 2 + (compact ? (index - (members.length - 1) / 2) * 84 : 0);
      const personY = hasSides && index < 2 ? y + shape.titleHeight + shape.height * (index === 0 ? .24 : .42)
        : y + shape.height + 12;
      items.push({ id, kind: 'person', entity: person, x: personX, y: personY, width: PERSON_WIDTH, height: PERSON_HEIGHT });
      connections.push({ kind: 'person', communityId: group.communityId,
        from: { x: personX + PERSON_WIDTH / 2, y: personY + 31 }, to: { x: x + shape.width / 2, y: y + shape.titleHeight + (shape.height - shape.titleHeight) / 2 } });
      blockBottom = Math.max(blockBottom, personY + PERSON_HEIGHT);
    });
    columnBottom[column] = blockBottom + gutter;
  });

  let top = communities.length ? Math.max(...columnBottom) + 12 : padding;
  const standalone = apps.filter(app => !app.communityId);
  if (standalone.length) {
    const radius = compact ? 60 : 78, tileWidth = SQRT3 * radius, tileHeight = radius * 2;
    const count = Math.max(1, Math.floor((width - padding * 2 + 24) / (tileWidth + 24)));
    standalone.forEach((app, index) => {
      const rowCount = Math.min(count, standalone.length - Math.floor(index / count) * count);
      const inset = (width - rowCount * tileWidth - (rowCount - 1) * 24) / 2;
      items.push({ id: `app:${app.appId}`, kind: 'app', app, x: inset + (index % count) * (tileWidth + 24), y: top + Math.floor(index / count) * (tileHeight + 32), width: tileWidth, height: tileHeight, radius });
    });
    top += Math.ceil(standalone.length / count) * (tileHeight + 32) + 16;
  }
  const remaining = people.filter(person => !ownedPeople.has(fieldEntityId(person)));
  const personColumns = Math.max(2, Math.floor((width - padding * 2) / 114));
  remaining.forEach((entity, index) => {
    const rowCount = Math.min(personColumns, remaining.length - Math.floor(index / personColumns) * personColumns);
    const pitch = Math.min(126, (width - padding * 2) / rowCount);
    items.push({ id: fieldEntityId(entity), kind: 'person', entity, x: (width - rowCount * pitch) / 2 + (index % personColumns) * pitch + (pitch - PERSON_WIDTH) / 2,
      y: top + Math.floor(index / personColumns) * (PERSON_HEIGHT + 28), width: PERSON_WIDTH, height: PERSON_HEIGHT });
  });
  // Shared members keep one circular node, with each relationship grounded in a
  // visible community's public preview. A connection never creates another record.
  const personItems = new Map(items.filter(item => item.kind === 'person').map(item => [item.entity.value.profileId, item]));
  const groupItems = new Map(items.filter(item => item.kind === 'community').map(item => [item.entity.value.communityId, item]));
  const connected = new Set(connections.filter(connection => connection.kind === 'person').map(connection => `${connection.communityId}:${connection.from.x}:${connection.from.y}`));
  for (const entity of communities) for (const member of entity.value.previewMembers ?? []) {
    const personItem = personItems.get(member.profileId), groupItem = groupItems.get(entity.value.communityId);
    if (!personItem || !groupItem) continue;
    const from = { x: personItem.x + PERSON_WIDTH / 2, y: personItem.y + 31 };
    const key = `${entity.value.communityId}:${from.x}:${from.y}`;
    if (connected.has(key)) continue;
    connected.add(key);
    connections.push({ kind: 'person', communityId: entity.value.communityId, from,
      to: { x: groupItem.x + groupItem.shape.width / 2, y: groupItem.y + groupItem.shape.titleHeight + (groupItem.shape.height - groupItem.shape.titleHeight) / 2 } });
  }
  const height = Math.max(Number.isFinite(viewportHeight) ? viewportHeight : 640, ...items.map(item => item.y + item.height + 90));
  return { width: requiredWidth, height, items, connections, compact };
}

export function fieldItemVisible(item, bounds, overscan = 180) {
  return item.x <= bounds.right + overscan && item.x + item.width >= bounds.left - overscan
    && item.y <= bounds.bottom + overscan && item.y + item.height >= bounds.top - overscan;
}

/** Arrow navigation uses centres and strongly penalises a sideways detour. */
export function fieldNeighbour(items, currentId, direction) {
  const current = items.find(item => item.id === currentId) ?? items[0];
  if (!current) return undefined;
  const origin = { x: current.x + current.width / 2, y: current.y + current.height / 2 };
  const [dx, dy] = direction;
  return items.filter(item => item !== current).map(item => {
    const x = item.x + item.width / 2 - origin.x, y = item.y + item.height / 2 - origin.y;
    return { item, forward: x * dx + y * dy, score: Math.hypot(x, y) + Math.abs(x * dy - y * dx) * 2.4 };
  }).filter(candidate => candidate.forward > 8).sort((a, b) => a.score - b.score)[0]?.item;
}
