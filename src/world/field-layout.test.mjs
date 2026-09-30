import test from 'node:test';
import assert from 'node:assert/strict';
import { createFieldLayoutState, stableFieldLayout } from './field-layout.mjs';

const metrics = { padding: 22, gap: 28,
  community: { left: 22, columns: 3, pitch: 280, width: 250, height: 280 },
  person: { left: 22, columns: 6, pitch: 140, width: 112, height: 118 } };
const group = id => ({ id, type: 'community' }), person = id => ({ id, type: 'person' });
const positions = values => Object.fromEntries(values.map(({ id, ...position }) => [id, position]));

test('filtering, reordering and adding another type never move an existing identity', () => {
  const state = createFieldLayoutState();
  const initial = positions(stableFieldLayout(state, [group('a'), group('b'), person('p')], metrics));
  assert.deepEqual(positions(stableFieldLayout(state, [person('p')], metrics)).p, initial.p);
  const next = positions(stableFieldLayout(state, [group('new'), person('p'), group('b'), group('a'), person('q')], metrics));
  for (const id of Object.keys(initial)) assert.deepEqual(next[id], initial[id]);
  assert.ok(next.new.y >= initial.p.y + initial.p.height + metrics.gap);
  assert.ok(next.q.y >= next.new.y + next.new.height + metrics.gap);
});

test('removed results retain only coordinates, no profile or membership payload', () => {
  const state = createFieldLayoutState();
  stableFieldLayout(state, [group('removed')], metrics);
  assert.deepEqual(stableFieldLayout(state, [], metrics), []);
  assert.deepEqual(Object.keys(state.placements.get('removed')).sort(), ['height', 'width', 'x', 'y']);
});

test('a deliberate viewport geometry change rebuilds a compact non-overlapping layout', () => {
  const state = createFieldLayoutState();
  stableFieldLayout(state, [group('a'), group('b')], metrics);
  const narrow = { ...metrics, community: { ...metrics.community, columns: 1, pitch: 280 } };
  const next = stableFieldLayout(state, [group('a'), group('b')], narrow);
  assert.equal(next[0].x, next[1].x);
  assert.ok(next[1].y >= next[0].y + next[0].height + metrics.gap);
});
