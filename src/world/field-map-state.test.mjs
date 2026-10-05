import test from 'node:test';
import assert from 'node:assert/strict';
import { createFieldLayoutState } from './field-layout.mjs';
import { layoutField } from './field-map-layout.mjs';
import { stabilizeFieldMap } from './field-map-state.mjs';
const person = id => ({ type: 'person', value: { profileId: id, displayName: id, revision: 1 } });
const community = (id, members = []) => ({ type: 'community', value: { communityId: id, name: id, previewMembers: members.map(member => member.value), revision: 1 } });
const app = (id, communityId) => ({ appId: id, name: id, ...(communityId ? { communityId } : {}) });
const coordinates = layout => new Map(layout.items.map(({ id, x, y }) => [id, [x, y]]));
test('pagination and changed ordering preserve identities including an app-only account', () => {
  for (const width of [320, 390, 768, 901, 1100, 1440]) {
    const state = createFieldLayoutState(), a = community('a'), b = community('b'), p = person('p');
    const first = stabilizeFieldMap(state, layoutField([a, p], [app('local')], width), width), before = coordinates(first);
    const next = stabilizeFieldMap(state, layoutField([b, p, a], [app('local'), app('new-local')], width), width), after = coordinates(next);
    for (const [id, value] of before) assert.deepEqual(after.get(id), value, `${width}:${id}`);
    const roots = next.items.filter(item => item.kind === 'community' || item.kind === 'app' && !item.communityId);
    for (const a of roots) for (const b of roots) if (a !== b) assert.ok(a.x + a.width <= b.x || b.x + b.width <= a.x || a.y + a.height <= b.y || b.y + b.height <= a.y);
  }
});
test('app children and verified person edges remain aligned after stable page append', () => {
  const state = createFieldLayoutState(), p = person('p'), a = community('a'), b = community('b', [p]);
  stabilizeFieldMap(state, layoutField([a], [app('local')], 1100), 1100);
  const map = stabilizeFieldMap(state, layoutField([a, b, p], [app('local'), app('group-app', 'b')], 1100), 1100);
  const group = map.items.find(item => item.id === 'community:b'), tile = map.items.find(item => item.id === 'app:group-app'), face = map.items.find(item => item.id === 'person:p');
  assert.equal(tile.x, group.x + group.shape.cells[0].left); assert.equal(tile.y, group.y + group.shape.cells[0].top);
  const edge = map.connections.find(edge => edge.kind === 'person');
  assert.deepEqual(edge.from, { x: face.x + face.width / 2, y: face.y + 31 });
  assert.equal(edge.to.x, group.x + group.width / 2);
  assert.ok(map.height >= face.y + face.height);
});
test('resize or changed group geometry reflows, without introducing invisible nodes', () => {
  const state = createFieldLayoutState(), a = community('a');
  stabilizeFieldMap(state, layoutField([a], [], 1440), 1440);
  const resized = stabilizeFieldMap(state, layoutField([a], [app('app', 'a'), app('hidden', 'missing')], 320), 320);
  assert.deepEqual(resized.items.map(item => item.id), ['community:a', 'app:app']);
  assert.ok(resized.width >=320); assert.equal(state.key, JSON.stringify([320, true]));
  const empty = stabilizeFieldMap(state, layoutField([], [], 320), 320); assert.equal(empty.items.length, 0); assert.equal(empty.connections.length, 0);
});
