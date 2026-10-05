import test from 'node:test';
import assert from 'node:assert/strict';
import { SQRT3, axialDistance } from '../geometry/hex.mjs';
import { layoutField, communityFieldGeometry, clampFieldScale, fieldItemVisible, fieldNeighbour } from './field-map-layout.mjs';

const person = id => ({ type: 'person', value: { profileId: id, displayName: `Profile ${id}`, revision: 1 } });
const community = (id, members = []) => ({ type: 'community', value: { communityId: id, name: `Group ${id}`, previewMembers: members.map(member => member.value), revision: 1 } });
const app = (id, communityId) => ({ appId: id, name: `App ${id}`, status: 'ready', ...(communityId ? { communityId } : {}) });

test('field never invents a community, selectable preview member or an app from another group', () => {
  const publicPerson = person('public'), previewOnly = person('preview-only');
  const allowed = app('allowed', 'visible'), hidden = app('hidden', 'not-in-results');
  const result = layoutField([community('visible', [publicPerson, previewOnly]), publicPerson], [allowed, hidden]);
  assert.deepEqual(result.items.map(item => item.id).sort(), ['app:allowed', 'community:visible', 'person:public']);
  assert.equal(result.connections.filter(connection => connection.kind === 'person').length, 1);
  assert.ok(result.items.find(item => item.kind === 'app').app === allowed);
});

test('deduplication and independent apps retain exactly the supplied records', () => {
  const selected = community('visible'), independent = app('local');
  const result = layoutField([selected, selected, person('p'), person('p')], [independent, independent]);
  assert.equal(new Set(result.items.map(item => item.id)).size, result.items.length);
  assert.equal(result.items.filter(item => item.kind === 'app').length, 1);
  assert.ok(!result.connections.length);
});

test('one shared person retains both verified memberships without creating a duplicate node', () => {
  const member = person('shared');
  const result = layoutField([community('a', [member]), community('b', [member]), member], [], 1440);
  assert.equal(result.items.filter(item => item.kind === 'person').length, 1);
  assert.deepEqual(result.connections.filter(connection => connection.kind === 'person').map(connection => connection.communityId).sort(), ['a', 'b']);
});

test('point-up regular hexes preserve side spacing and all tiles stay inside their contour bounds', () => {
  for (const count of [1, 2, 3, 4, 5, 6, 7]) for (const radius of [55, 60, 78, 82]) {
    const shape = communityFieldGeometry(count, radius);
    assert.equal(shape.cells.length, count);
    assert.ok(Math.abs(shape.tileWidth / shape.tileHeight - SQRT3 / 2) < 1e-10);
    assert.doesNotMatch(shape.contour, /NaN|Infinity/);
    for (const cell of shape.cells) {
      assert.ok(cell.left >= 0 && cell.top >= shape.titleHeight);
      assert.ok(cell.left + shape.tileWidth <= shape.width + 1e-8);
      assert.ok(cell.top + shape.tileHeight <= shape.height + 1e-8);
      for (const other of shape.cells) {
        if (axialDistance(cell.axial, other.axial) === 1) assert.ok(Math.abs(Math.hypot(cell.x - other.x, cell.y - other.y) - SQRT3 * radius - 14) < 1e-8);
      }
    }
  }
});

test('responsive layout retains every visible node without clipping labels or geometry at 320..1440', () => {
  const people = Array.from({ length: 12 }, (_, index) => person(`p-${index}`));
  const groups = [community('a', people.slice(0, 3)), community('b', people.slice(3, 6)), community('c', people.slice(6, 9))];
  const apps = Array.from({ length: 3 }, (_, index) => app(`a-${index}`, 'a')).concat(app('b-1', 'b'), app('b-2', 'b'));
  for (const width of [280, 320, 390, 600, 768, 1024, 1440]) {
    const result = layoutField([...groups, ...people], apps, width, 680);
    assert.equal(result.items.length, 20);
    for (const item of result.items) {
      assert.ok(Number.isFinite(item.x) && Number.isFinite(item.y));
      assert.ok(item.x >= 0 && item.y >= 0, `${width}: ${item.id} outside origin`);
      assert.ok(item.x + item.width <= result.width + 1e-8, `${width}: ${item.id} beyond plane`);
      assert.ok(item.y + item.height <= result.height, `${width}: ${item.id} beyond plane`);
      if (item.kind === 'app') assert.ok(item.radius >= 55);
    }
    const circles = result.items.filter(item => item.kind === 'person');
    for (let index = 0; index < circles.length; index++) for (const other of circles.slice(index + 1)) {
      const item = circles[index];
      const overlap = item.x < other.x + other.width && item.x + item.width > other.x && item.y < other.y + other.height && item.y + item.height > other.y;
      assert.ok(!overlap, `${width}: circles ${item.id}/${other.id} overlap`);
    }
  }
});

test('large app sets produce a precise community overflow target instead of silent removal', () => {
  const result = layoutField([community('a')], Array.from({ length: 13 }, (_, index) => app(String(index), 'a')));
  assert.equal(result.items.filter(item => item.kind === 'app').length, 6);
  assert.equal(result.items.find(item => item.kind === 'overflow').count, 7);
  assert.equal(result.items.find(item => item.kind === 'community').appCount, 13);
});

test('virtual visibility includes touching edges and excludes distant rows, arrow keys use spatial neighbours', () => {
  const result = layoutField([person('left'), person('middle'), person('right')], [], 600);
  const middle = result.items[1];
  assert.equal(fieldNeighbour(result.items, middle.id, [1, 0]).id, result.items[2].id);
  assert.equal(fieldNeighbour(result.items, middle.id, [-1, 0]).id, result.items[0].id);
  assert.equal(fieldNeighbour(result.items, result.items[0].id, [-1, 0]), undefined);
  assert.equal(fieldItemVisible(middle, { left: middle.x + middle.width, right: middle.x + middle.width + 10, top: 0, bottom: 100 }, 0), true);
  assert.equal(fieldItemVisible(middle, { left: 10000, right: 11000, top: 10000, bottom: 11000 }), false);
  assert.equal(clampFieldScale(NaN), 1);
  assert.equal(clampFieldScale(100), 1.4);
  assert.equal(clampFieldScale(-1), .65);
});
