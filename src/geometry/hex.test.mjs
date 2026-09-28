import test from 'node:test';
import assert from 'node:assert/strict';
import { SQRT3, HEX_ASPECT_RATIO, HEX_DIRECTIONS, HEX_FLOWER, HEX_POLYGON,
  axialDistance, axialToPixel, pixelToAxial, hexMetrics, hexVertices, hexRing, hexSpiral,
  insetHex, hexSafeRect, pointInHex, hexBoundary, hexCluster, hexGrid, roundedHexPath } from './hex.mjs';

const near = (actual, expected, epsilon = 1e-7) => assert.ok(Math.abs(actual - expected) <= epsilon, `${actual} != ${expected}`);
let seed = 0x50_74_79;
const random = () => { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed / 0x100000000; };
const distance = (a, b) => Math.hypot(a.x - b.x, a.y - b.y);

test('500 regular hexagons keep six equal sides, equal apothems and one aspect ratio', () => {
  for (let sample = 0; sample < 500; sample++) {
    const radius = 4 + random() * 400, metrics = hexMetrics(radius), corners = hexVertices(radius);
    near(metrics.width / metrics.height, HEX_ASPECT_RATIO);
    corners.forEach((corner, index) => {
      const next = corners[(index + 1) % 6];
      near(Math.hypot(corner.x, corner.y), radius);
      near(distance(corner, next), radius);
      near(Math.hypot((corner.x + next.x) / 2, (corner.y + next.y) / 2), metrics.apothem);
    });
  }
  assert.equal(HEX_POLYGON, 'polygon(100% 50%,75% 100%,25% 100%,0% 50%,25% 0%,75% 0%)');
});

test('all six axial neighbours have the requested edge gap, including mobile legacy sizes', () => {
  const fixtures = [[39, 6], [35, 6], [48, 6], [72, 8]];
  for (let sample = 0; sample < 250; sample++) fixtures.push([5 + random() * 300, random() * 24]);
  for (const [radius, gap] of fixtures) {
    for (const axial of HEX_DIRECTIONS) {
      const point = axialToPixel(axial, radius, gap);
      near(Math.hypot(point.x, point.y) - SQRT3 * radius, gap);
      assert.equal(axialDistance(axial), 1);
    }
  }
});

test('rounded corners are six equal circular arcs tangent to the original edges at every size', () => {
  for (const radius of [15, 17, 24, 48, 72, 144]) {
    for (const fraction of [.01, .14, .5, SQRT3 / 2]) {
      const cornerRadius = radius * fraction, vertices = hexVertices(radius);
      const arcs = [...roundedHexPath(radius, cornerRadius).matchAll(/[ML] ([\d.-]+) ([\d.-]+) A ([\d.-]+) ([\d.-]+) 0 0 1 ([\d.-]+) ([\d.-]+)/g)];
      assert.equal(arcs.length, 6);
      arcs.forEach((arc, index) => {
        const [, sx, sy, rx, ry, ex, ey] = arc.map(Number);
        near(rx, cornerRadius); near(ry, cornerRadius);
        const vertex = vertices[index], previous = vertices[(index + 5) % 6], next = vertices[(index + 1) % 6];
        const centreScale = 1 - 2 * cornerRadius / (SQRT3 * radius);
        const centre = { x: vertex.x * centreScale, y: vertex.y * centreScale };
        for (const [point, neighbour] of [[{ x: sx, y: sy }, previous], [{ x: ex, y: ey }, next]]) {
          near(distance(point, centre), cornerRadius);
          near(distance(point, vertex), cornerRadius / SQRT3);
          near(((point.x - centre.x) * (neighbour.x - vertex.x) + (point.y - centre.y) * (neighbour.y - vertex.y)) / radius, 0);
        }
        const startAngle = Math.atan2(sy - centre.y, sx - centre.x);
        for (let sample = 0; sample <= 12; sample++) {
          const angle = startAngle + Math.PI / 3 * sample / 12;
          assert.ok(pointInHex({ x: centre.x + cornerRadius * Math.cos(angle), y: centre.y + cornerRadius * Math.sin(angle) }, radius, 1e-7));
        }
      });
    }
    assert.ok(!roundedHexPath(radius, 0).includes(' A '));
  }
});

test('perpendicular insets preserve the six edge distances and the aspect ratio', () => {
  for (let sample = 0; sample < 400; sample++) {
    const radius = 10 + random() * 180, inset = random() * radius * 0.7;
    const outer = hexMetrics(radius), inner = insetHex(radius, inset);
    near(outer.apothem - inner.apothem, inset);
    near((outer.width - inner.width) / 2, inner.inline);
    near((outer.height - inner.height) / 2, inner.block);
    near(inner.width / inner.height, HEX_ASPECT_RATIO);
  }
});

test('every safe rectangle corner lies inside the inner hexagon at supported tile sizes', () => {
  for (const radius of [15, 17, 20.5, 24, 32.5, 35, 39, 48, 64, 72, 75]) {
    for (const inset of [0, 2, 3, 4]) {
      const rectangle = hexSafeRect(radius, inset), inner = insetHex(radius, inset);
      for (const x of [-rectangle.width / 2, rectangle.width / 2]) {
        for (const y of [-rectangle.height / 2, rectangle.height / 2]) assert.ok(pointInHex({ x, y }, inner.radius));
      }
    }
  }
});

test('axial/pixel conversion round trips 1000 centres and points inside their cells', () => {
  for (let sample = 0; sample < 1000; sample++) {
    const radius = 5 + random() * 500, gap = random() * 20;
    const axial = [Math.floor(random() * 201) - 100, Math.floor(random() * 201) - 100];
    const point = axialToPixel(axial, radius, gap);
    assert.deepEqual(pixelToAxial(point, radius, gap), axial);
    assert.deepEqual(pixelToAxial({ x: point.x + radius * .1, y: point.y - radius * .1 }, radius, gap), axial);
  }
});

test('rings/spirals are unique, continuous and have the correct count and distance', () => {
  for (let radius = 0; radius <= 12; radius++) {
    const ring = hexRing(radius), spiral = hexSpiral(radius);
    assert.equal(ring.length, radius ? 6 * radius : 1);
    assert.equal(spiral.length, 1 + 3 * radius * (radius + 1));
    assert.equal(new Set(spiral.map(cell => cell.join(','))).size, spiral.length);
    ring.forEach((cell, index) => {
      assert.equal(axialDistance(cell), radius);
      if (radius) assert.equal(axialDistance(cell, ring[(index + 1) % ring.length]), 1);
    });
  }
});

test('flower outline removes shared edges and keeps a closed 18-edge exterior', () => {
  for (const radius of [4.75, 32, 48, 65.456, 144]) {
    const loops = hexBoundary(HEX_FLOWER, radius);
    assert.equal(loops.length, 1); assert.equal(loops[0].length, 18);
    loops[0].forEach((point, index) => near(distance(point, loops[0][(index + 1) % loops[0].length]), radius));
  }
});

test('dense responsive grids retain all tiles inside bounds with the same gap and no intersections', () => {
  for (const width of [320, 390, 768, 1440]) {
    const radius = width < 600 ? 64 : 72, gap = 8, padding = 12;
    const metrics = hexMetrics(radius, gap);
    const columns = Math.max(1, Math.floor((width - padding * 2 - metrics.layoutRadius * 2) / metrics.stepX) + 1);
    for (const count of [1, 2, 3, 7, 11, 28]) {
      const coordinates = hexGrid(count, columns), cluster = hexCluster(coordinates, radius, gap, padding);
      assert.ok(cluster.width <= width + 1e-7);
      for (const cell of cluster.cells) {
        assert.ok(cell.left >= padding - 1e-7 && cell.top >= padding - 1e-7);
        assert.ok(cell.left + radius * 2 <= cluster.width - padding + 1e-7);
        assert.ok(cell.top + SQRT3 * radius <= cluster.height - padding + 1e-7);
        for (const other of cluster.cells) {
          if (cell === other) continue;
          const separation = distance(cell, other);
          assert.ok(separation >= SQRT3 * radius + gap - 1e-7);
          if (axialDistance(cell.axial, other.axial) === 1) near(separation - SQRT3 * radius, gap);
        }
      }
    }
  }
});

test('scaling the whole layout preserves regular geometry and scales the edge gap uniformly', () => {
  for (const scale of [.65, .8, 1, 1.25, 1.4, 2]) {
    const metrics = hexMetrics(48, 6), center = axialToPixel([1, 0], 48, 6);
    near((metrics.width * scale) / (metrics.height * scale), HEX_ASPECT_RATIO);
    near(Math.hypot(center.x * scale, center.y * scale) - SQRT3 * 48 * scale, 6 * scale);
  }
});

test('invalid sizes fail explicitly instead of emitting NaN CSS or negative polygons', () => {
  for (const radius of [0, -1, NaN, Infinity]) assert.throws(() => hexMetrics(radius), RangeError);
  assert.throws(() => hexMetrics(10, -1), RangeError);
  assert.throws(() => insetHex(10, 10), RangeError);
  assert.throws(() => hexCluster([], 10), RangeError);
  assert.throws(() => hexGrid(1, 0), RangeError);
  assert.throws(() => hexRing(-1), RangeError);
  for (const corner of [-1, NaN, Infinity, 10]) assert.throws(() => roundedHexPath(10, corner), RangeError);
});
