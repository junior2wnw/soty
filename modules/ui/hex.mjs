/** Shared regular, flat-top hexagons. Radius means centre-to-vertex distance. */
export const SQRT3 = Math.sqrt(3);
export const HEX_HEIGHT_FACTOR = SQRT3 / 2;
export const HEX_ASPECT_RATIO = 2 / SQRT3;
export const HEX_SAFE_WIDTH = 0.66;
export const HEX_SAFE_HEIGHT = 0.6;
export const HEX_CORNER_RATIO = 0.14;
export const HEX_DIRECTIONS = Object.freeze([[1, 0], [1, -1], [0, -1], [-1, 0], [-1, 1], [0, 1]].map(Object.freeze));
export const HEX_FLOWER = Object.freeze([[0, 0], [0, -1], [-1, 0], [1, -1], [-1, 1], [0, 1], [1, 0]].map(Object.freeze));

function positive(value, name, allowZero = false) {
  if (!Number.isFinite(value) || (allowZero ? value < 0 : value <= 0)) throw new RangeError(`${name} must be ${allowZero ? 'non-negative' : 'positive'} and finite`);
  return value;
}

export function hexMetrics(radius, gap = 0) {
  positive(radius, 'radius'); positive(gap, 'gap', true);
  // Expanding the placement apothem by gap / 2 gives an equal perpendicular gap on all six sides.
  const layoutRadius = radius + gap / SQRT3;
  return { radius, gap, width: 2 * radius, height: SQRT3 * radius, apothem: SQRT3 * radius / 2,
    layoutRadius, stepX: 1.5 * layoutRadius, stepY: SQRT3 * layoutRadius };
}

export function hexVertices(radius, centre = { x: 0, y: 0 }) {
  positive(radius, 'radius');
  const half = radius / 2, apothem = SQRT3 * half;
  return [[radius, 0], [half, apothem], [-half, apothem], [-radius, 0], [-half, -apothem], [half, -apothem]]
    .map(([x, y]) => ({ x: centre.x + x, y: centre.y + y }));
}

export const HEX_POLYGON = `polygon(${hexVertices(1).map(({ x, y }) => `${(x + 1) * 50}% ${Math.round((y + SQRT3 / 2) / SQRT3 * 100)}%`).join(',')})`;

/** Circular fillets tangent to the six regular edges; layout and safe area stay unchanged. */
export function roundedHexPath(radius, cornerRadius = radius * HEX_CORNER_RATIO, centre = { x: 0, y: 0 }) {
  positive(radius, 'radius'); positive(cornerRadius, 'cornerRadius', true);
  if (cornerRadius > radius * SQRT3 / 2) throw new RangeError('cornerRadius exceeds the apothem');
  const vertices = hexVertices(radius, centre), tangent = cornerRadius / SQRT3;
  const format = value => Number(value.toFixed(8));
  const point = ({ x, y }) => `${format(x)} ${format(y)}`;
  if (!cornerRadius) return `M ${vertices.map(point).join(' L ')} Z`;
  return vertices.map((vertex, index) => {
    const previous = vertices[(index + 5) % 6], next = vertices[(index + 1) % 6];
    const toward = other => ({ x: vertex.x + (other.x - vertex.x) * tangent / radius, y: vertex.y + (other.y - vertex.y) * tangent / radius });
    return `${index ? 'L' : 'M'} ${point(toward(previous))} A ${format(cornerRadius)} ${format(cornerRadius)} 0 0 1 ${point(toward(next))}`;
  }).join(' ') + ' Z';
}

export function axialToPixel([q, r], radius, gap = 0) {
  const { stepX, stepY } = hexMetrics(radius, gap);
  return { x: q * stepX, y: (r + q / 2) * stepY };
}

export function pixelToAxial({ x, y }, radius, gap = 0) {
  const { stepX, stepY } = hexMetrics(radius, gap);
  const q = x / stepX, r = y / stepY - q / 2, s = -q - r;
  let rq = Math.round(q), rr = Math.round(r), rs = Math.round(s);
  const dq = Math.abs(rq - q), dr = Math.abs(rr - r), ds = Math.abs(rs - s);
  if (dq > dr && dq > ds) rq = -rr - rs;
  else if (dr > ds) rr = -rq - rs;
  // Normalise -0 so round trips can be compared without a signed-zero exception.
  return [rq || 0, rr || 0];
}

export function axialDistance([q, r], [otherQ, otherR] = [0, 0]) {
  const dq = q - otherQ, dr = r - otherR;
  return Math.max(Math.abs(dq), Math.abs(dr), Math.abs(dq + dr));
}

export function hexRing(distance) {
  if (!Number.isInteger(distance) || distance < 0) throw new RangeError('distance must be a non-negative integer');
  if (!distance) return [[0, 0]];
  const result = [];
  let q = -distance, r = distance;
  for (const [dq, dr] of HEX_DIRECTIONS) for (let step = 0; step < distance; step++) {
    result.push([q, r]); q += dq; r += dr;
  }
  return result;
}

export function hexSpiral(distance) {
  if (!Number.isInteger(distance) || distance < 0) throw new RangeError('distance must be a non-negative integer');
  const result = [[0, 0]];
  for (let ring = 1; ring <= distance; ring++) result.push(...hexRing(ring));
  return result;
}

/** Column-offset presentation converted to axial coordinates; adjacent columns interlock. */
export function hexGrid(count, columns) {
  if (!Number.isInteger(count) || count < 0 || !Number.isInteger(columns) || columns < 1) throw new RangeError('count and columns must be valid integers');
  return Array.from({ length: count }, (_, index) => {
    const q = index % columns;
    return [q, Math.floor(index / columns) - Math.floor(q / 2)];
  });
}

/** A perpendicular edge inset, not the same inset on both bounding-box axes. */
export function insetHex(radius, inset) {
  positive(radius, 'radius'); positive(inset, 'inset', true);
  const innerRadius = radius - 2 * inset / SQRT3;
  if (innerRadius <= 0) throw new RangeError('inset consumes the hexagon');
  return { radius: innerRadius, block: inset, inline: 2 * inset / SQRT3, ...hexMetrics(innerRadius) };
}

/** An interior rectangle with a little room before the sloping edges. */
export function hexSafeRect(radius, inset = 0) {
  const inner = insetHex(radius, inset);
  const width = inner.width * HEX_SAFE_WIDTH, height = inner.height * HEX_SAFE_HEIGHT;
  return { width, height, left: radius - width / 2, top: SQRT3 * radius / 2 - height / 2 };
}

export function pointInHex({ x, y }, radius, epsilon = 1e-9) {
  positive(radius, 'radius');
  return Math.abs(y) <= SQRT3 * radius / 2 + epsilon && SQRT3 * Math.abs(x) + Math.abs(y) <= SQRT3 * radius + epsilon;
}

const pointKey = ({ x, y }) => `${Math.round(x * 1e7)},${Math.round(y * 1e7)}`;
const pointsText = points => points.map(({ x, y }) => `${Number(x.toFixed(6))},${Number(y.toFixed(6))}`).join(' ');

/** Exterior loops of joined placement cells. Shared edges are removed, never approximated. */
export function hexBoundary(coordinates, radius) {
  const edges = new Map();
  for (const axial of coordinates) {
    const vertices = hexVertices(radius, axialToPixel(axial, radius));
    vertices.forEach((start, index) => {
      const end = vertices[(index + 1) % 6], startKey = pointKey(start), endKey = pointKey(end);
      const key = [startKey, endKey].sort().join('|');
      if (edges.has(key)) edges.delete(key);
      else edges.set(key, { start, end, startKey, endKey });
    });
  }
  const next = new Map([...edges.values()].map(edge => [edge.startKey, edge]));
  const loops = [];
  while (next.size) {
    const first = next.values().next().value;
    let edge = first;
    const loop = [];
    do {
      loop.push(edge.start); next.delete(edge.startKey);
      if (edge.endKey === first.startKey) break;
      edge = next.get(edge.endKey);
      if (!edge) throw new Error('Hex boundary is not a closed manifold');
    } while (next.size);
    loops.push(loop);
  }
  return loops;
}

/** Geometry for a cluster, including true cell bounds and a contour through the middle of gaps. */
export function hexCluster(coordinates, radius, gap = 0, padding = 0) {
  positive(padding, 'padding', true);
  if (!coordinates.length) throw new RangeError('A hex cluster needs at least one cell');
  const metrics = hexMetrics(radius, gap);
  const loops = hexBoundary(coordinates, metrics.layoutRadius);
  const vertices = loops.flat();
  const minX = Math.min(...vertices.map(point => point.x)), minY = Math.min(...vertices.map(point => point.y));
  const width = Math.max(...vertices.map(point => point.x)) - minX + padding * 2;
  const height = Math.max(...vertices.map(point => point.y)) - minY + padding * 2;
  const offset = { x: padding - minX, y: padding - minY };
  const cells = coordinates.map(axial => {
    const point = axialToPixel(axial, radius, gap);
    const x = point.x + offset.x, y = point.y + offset.y;
    return { axial, x, y, left: x - radius, top: y - metrics.height / 2 };
  });
  return { ...metrics, width, height, cells, loops: loops.map(loop => loop.map(point => ({ x: point.x + offset.x, y: point.y + offset.y }))) };
}

export function hexPolygonPoints(radius, centre) { return pointsText(hexVertices(radius, centre)); }
export function polygonPoints(points) { return pointsText(points); }
