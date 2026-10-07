import test from 'node:test';
import assert from 'node:assert/strict';
import { DateTime } from 'luxon';
import {
  fitTimeline,
  MAX_SPAN,
  MIN_SPAN,
  normalizeViewport,
  packTimeline,
  panViewport,
  scaleToSpan,
  spanLabel,
  spanToScale,
  timelineTicks,
  TIMELINE_END,
  TIMELINE_START,
  viewportBounds,
  zoomViewport,
} from './timeline.js';
import type { Entity, TimeRange } from './types.js';

const DAY = 86_400_000;
const NOW = DateTime.fromISO('2026-10-02T06:00:00Z').toMillis();
const ms = (value: string) => DateTime.fromISO(value, { zone: 'UTC' }).toMillis();
const range = (start: string | null, end: string | null = null, timezone = 'UTC'): TimeRange => ({
  start,
  end,
  timezone,
  precision: start ? 'exact' : 'unknown',
});
function entity(
  start: string | null,
  end: string | null = null,
  extra: Partial<Entity> = {},
): Entity {
  const plan = range(start, end);
  return {
    id: 'sample',
    workspaceId: 'personal',
    typeId: 'event',
    kind: end ? 'period' : 'point',
    title: 'Sample',
    description: '',
    parentId: null,
    ownerId: null,
    participantIds: [],
    status: 'planned',
    plan,
    baseline: { ...plan },
    actual: null,
    forecast: null,
    dueAt: null,
    tags: [],
    fields: {},
    links: [],
    allocations: [],
    recurrence: null,
    source: {
      kind: 'manual',
      label: '',
      observedAt: '2026-10-02T06:00:00Z',
      receivedAt: '2026-10-02T06:00:00Z',
    },
    createdAt: '2026-10-02T06:00:00Z',
    updatedAt: '2026-10-02T06:00:00Z',
    version: 1,
    ...extra,
  };
}

test('continuous zoom preserves the exact instant under the cursor across arbitrary scales', () => {
  for (const span of [MIN_SPAN, 93_000, DAY, 73.25 * DAY, 5_000 * DAY]) {
    for (const fraction of [0, 0.13, 0.5, 0.91, 1]) {
      for (const factor of [0.31, 0.99, 1.02, 3.7, 200]) {
        const original = { center: NOW, span };
        const before = viewportBounds(original);
        const zoomed = zoomViewport(original, factor, fraction);
        const after = viewportBounds(zoomed);
        assert.ok(
          Math.abs(before.start + span * fraction - (after.start + zoomed.span * fraction)) < 0.005,
        );
        assert.ok(zoomed.span >= MIN_SPAN && zoomed.span <= MAX_SPAN);
      }
    }
  }
  assert.equal(zoomViewport({ center: NOW, span: DAY }, 2).span, DAY / 2);
});

test('panning and extreme zoom stop at calendar edges without invalid timestamps', () => {
  const all = normalizeViewport({ center: TIMELINE_END, span: Infinity });
  assert.deepEqual(viewportBounds(all), { start: TIMELINE_START, end: TIMELINE_END });
  for (const center of [TIMELINE_START - MAX_SPAN, TIMELINE_END + MAX_SPAN, NaN, Infinity]) {
    for (const span of [-1, NaN, Infinity, MIN_SPAN, MAX_SPAN, MAX_SPAN * 2]) {
      const normalized = normalizeViewport({ center, span });
      const bounds = viewportBounds(normalized);
      assert.ok(Number.isFinite(normalized.center));
      assert.ok(normalized.span >= MIN_SPAN && normalized.span <= MAX_SPAN);
      assert.ok(bounds.start >= TIMELINE_START && bounds.end <= TIMELINE_END);
    }
  }
  const view = { center: NOW, span: DAY };
  assert.equal(panViewport(view, 100, 1_000).center, NOW + DAY / 10);
  assert.deepEqual(panViewport(view, 100, 0), view);
  assert.deepEqual(panViewport(view, NaN, 100), view);
  assert.deepEqual(zoomViewport(view, 0), view);
  const edge = zoomViewport(all, 100, 0);
  assert.equal(viewportBounds(edge).start, TIMELINE_START);
});

test('logarithmic slider spans the whole calendar with reversible intermediate values', () => {
  assert.equal(scaleToSpan(0), MAX_SPAN);
  assert.equal(scaleToSpan(100), MIN_SPAN);
  let previous = Infinity;
  for (let scale = 0; scale <= 100; scale += 0.5) {
    const span = scaleToSpan(scale);
    assert.ok(span <= previous);
    assert.ok(Math.abs(spanToScale(span) - scale) < 1e-10);
    previous = span;
  }
  assert.equal(spanLabel(MIN_SPAN), '1 минута');
  assert.equal(spanLabel(DAY * 2), '2 дня');
  assert.match(spanLabel(MAX_SPAN), /лет$/);
});

test('fit covers every dated layer, uncertainty, deadline, and Gregorian extremes', () => {
  const layered = entity('2026-10-02T10:00:00Z', '2026-10-02T12:00:00Z', {
    plan: {
      ...range('2026-10-02T10:00:00Z', '2026-10-02T12:00:00Z'),
      earliest: '2026-09-30',
      latest: '2026-10-05',
    },
    baseline: range('2026-09-28T10:00:00Z'),
    actual: range('2026-09-29T08:00:00Z', '2026-09-29T13:00:00Z'),
    forecast: range('2026-10-07T08:00:00Z', '2026-10-07T13:00:00Z'),
    dueAt: '2026-10-09T23:00:00Z',
  });
  const bounds = viewportBounds(fitTimeline([layered], NOW));
  assert.ok(bounds.start < ms('2026-09-28T10:00:00Z'));
  assert.ok(bounds.end > ms('2026-10-09T23:00:00Z'));
  const extreme = fitTimeline([entity('0001-01-01T00:00:00Z', '9999-12-31T23:59:59.999Z')], NOW);
  assert.deepEqual(viewportBounds(extreme), { start: TIMELINE_START, end: TIMELINE_END });
});

test('undated notes and invalid ranges do not invent dates or contaminate fit', () => {
  const note = entity(null, null, {
    kind: 'note',
    plan: range(null),
    baseline: range(null),
    createdAt: '1000-01-01T00:00:00Z',
  });
  const empty = fitTimeline([], NOW);
  assert.deepEqual(fitTimeline([note], NOW), empty);
  assert.equal(empty.center, NOW);
  const dated = entity('2026-10-04T10:00:00Z');
  assert.deepEqual(fitTimeline([dated, note], NOW), fitTimeline([dated], NOW));
  assert.deepEqual(fitTimeline([entity('invalid-date')], NOW), empty);
});

test('finite recurrence fit includes the last multi-weekday count and calendar duration over DST', () => {
  const weekly = entity('2027-03-01T09:00:00-05:00', '2027-03-01T10:00:00-05:00', {
    plan: range('2027-03-01T09:00:00-05:00', '2027-03-01T10:00:00-05:00', 'America/New_York'),
    recurrence: { frequency: 'week', interval: 1, weekdays: [1, 3], count: 6, exceptions: [] },
  });
  assert.ok(viewportBounds(fitTimeline([weekly], NOW)).end > ms('2027-03-17T10:00:00-04:00'));
  const until = {
    ...weekly,
    recurrence: { frequency: 'week' as const, interval: 1, until: '2027-04-01', exceptions: [] },
  };
  assert.ok(viewportBounds(fitTimeline([until], NOW)).end > ms('2027-04-02T00:59:59.999-04:00'));
  const monthly = entity('2027-01-31T09:00:00Z', null, {
    recurrence: {
      frequency: 'month',
      interval: 1,
      count: 3,
      exceptions: [],
      calendarPolicy: 'skip-invalid',
    },
  });
  assert.ok(viewportBounds(fitTimeline([monthly], NOW)).end > ms('2027-05-31T09:00:00Z'));
  const infinite = {
    ...weekly,
    recurrence: { frequency: 'week' as const, interval: 1, exceptions: [] },
  };
  assert.deepEqual(
    fitTimeline([infinite], NOW),
    fitTimeline([{ ...weekly, recurrence: null }], NOW),
  );
});

test('daily calendar ticks keep local midnight across the spring DST gap', () => {
  const start = ms('2027-03-12T00:00:00-05:00');
  const end = ms('2027-03-16T00:00:00-04:00');
  const ticks = timelineTicks(
    { center: (start + end) / 2, span: end - start },
    600,
    'America/New_York',
  );
  const locals = ticks.map((tick) => DateTime.fromMillis(tick.at, { zone: 'America/New_York' }));
  assert.ok(locals.length >= 3);
  assert.ok(locals.every((local) => local.hour === 0 && local.minute === 0));
  assert.ok(
    ticks.some((tick, index) => index > 0 && tick.at - ticks[index - 1]!.at === 23 * 3_600_000),
  );
});

test('repeated fall hour remains two different instants with explicit offsets', () => {
  const start = ms('2026-11-01T00:00:00-04:00');
  const end = ms('2026-11-01T04:00:00-05:00');
  const ticks = timelineTicks(
    { center: (start + end) / 2, span: end - start },
    700,
    'America/New_York',
  );
  const repeated = ticks.filter((tick) => tick.label === '01:00');
  assert.equal(repeated.length, 2);
  assert.equal(repeated[1]!.at - repeated[0]!.at, 3_600_000);
  assert.match(repeated[0]!.context!, /UTC-04:00/);
  assert.match(repeated[1]!.context!, /UTC-05:00/);
});

test('coarse hour marks retain their wall-clock grid through DST', () => {
  const start = ms('2027-03-13T12:00:00-05:00');
  const end = ms('2027-03-15T12:00:00-04:00');
  const ticks = timelineTicks(
    { center: (start + end) / 2, span: end - start },
    1_000,
    'America/New_York',
  );
  const locals = ticks.map((tick) => DateTime.fromMillis(tick.at, { zone: 'America/New_York' }));
  assert.ok(locals.length >= 6);
  assert.ok(locals.every((local) => local.hour % 6 === 0 && local.minute === 0));
  assert.ok(
    ticks.some((tick, index) => index > 0 && tick.at - ticks[index - 1]!.at === 5 * 3_600_000),
  );
});

test('ticks remain sorted, finite, visible and bounded from a minute to ten millennia', () => {
  for (const width of [1, 320, 390, 1_440, 10_000, 100_000]) {
    for (const span of [MIN_SPAN, 3.7 * DAY, 400 * DAY, 50_000 * DAY, MAX_SPAN]) {
      const view = normalizeViewport({ center: NOW, span });
      const bounds = viewportBounds(view);
      const ticks = timelineTicks(view, width, 'UTC');
      assert.ok(ticks.length > 0 && ticks.length <= 200);
      for (const [index, tick] of ticks.entries()) {
        assert.ok(Number.isFinite(tick.at) && tick.at >= bounds.start && tick.at <= bounds.end);
        assert.ok(tick.label.length > 0);
        if (index > 0) assert.ok(tick.at > ticks[index - 1]!.at);
      }
    }
  }
  assert.deepEqual(timelineTicks({ center: NOW, span: DAY }, 0, 'UTC'), []);
  assert.deepEqual(
    timelineTicks({ center: NOW, span: DAY }, 320, 'Invalid/Zone'),
    timelineTicks({ center: NOW, span: DAY }, 320, 'UTC'),
  );
});

test('packing is deterministic, optimal for overlap, and preserves all footprints', () => {
  const items = [
    { key: 'c', left: 120, right: 160 },
    { key: 'a', left: 0, right: 100 },
    { key: 'b', left: 30, right: 90 },
    { key: 'd', left: 102, right: 117 },
  ];
  const packed = packTimeline(items, 2);
  assert.deepEqual(packed, packTimeline([...items].reverse(), 2));
  assert.equal(new Set(packed.map((item) => item.lane)).size, 2);
  assert.deepEqual(
    items.map((item) => item.key),
    ['c', 'a', 'b', 'd'],
  );
  for (const a of packed)
    for (const b of packed) {
      if (a.key !== b.key && a.lane === b.lane)
        assert.ok(a.right + 2 <= b.left || b.right + 2 <= a.left);
    }
  assert.equal(packed.find((item) => item.key === 'd')!.lane, 0);
  assert.throws(() => packTimeline([{ key: 'bad', left: NaN, right: 2 }]), RangeError);
});

test('packing thousands of simultaneous items does not cap or discard them', () => {
  const crowded = Array.from({ length: 5_000 }, (_, index) => ({
    key: String(index).padStart(5, '0'),
    left: 0,
    right: 100,
    entityId: `entity-${index}`,
  }));
  const packed = packTimeline(crowded);
  assert.equal(packed.length, crowded.length);
  assert.equal(new Set(packed.map((item) => item.lane)).size, crowded.length);
  assert.equal(new Set(packed.map((item) => item.entityId)).size, crowded.length);
  const sparse = packTimeline(
    Array.from({ length: 10_000 }, (_, index) => ({
      key: String(index),
      left: index * 20,
      right: index * 20 + 10,
    })),
    5,
  );
  assert.equal(sparse.length, 10_000);
  assert.ok(sparse.every((item) => item.lane === 0));
});

test('continuous packing keeps existing rows when widths change without a collision', () => {
  const items = [
    { key: 'a', left: 0, right: 80 },
    { key: 'b', left: 0, right: 90 },
    { key: 'c', left: 110, right: 180 },
  ];
  const initial = packTimeline(items, 12);
  const lanes = new Map(initial.map((item) => [item.key, item.lane]));
  const next = packTimeline(
    items.map((item) => ({ ...item, right: item.key === 'a' ? 100 : item.right })),
    12,
    lanes,
  );
  assert.equal(next.find((item) => item.key === 'a')!.lane, lanes.get('a'));
  assert.equal(next.find((item) => item.key === 'b')!.lane, lanes.get('b'));
});

test('continuous packing resolves new overlaps and removes empty old rows', () => {
  const items = Array.from({ length: 250 }, (_, i) => ({
    key: String(i),
    left: (i * 97) % 1000,
    right: ((i * 97) % 1000) + 110,
  }));
  const prior = new Map(items.map((item, i) => [item.key, (i % 17) + 50]));
  const packed = packTimeline(items, 12, prior);
  assert.equal(packed.length, items.length);
  for (const a of packed)
    for (const b of packed)
      if (a.key !== b.key && a.lane === b.lane)
        assert.ok(a.right + 12 <= b.left || b.right + 12 <= a.left);
  assert.ok(Math.min(...packed.map((item) => item.lane)) === 0);
  const single = packTimeline([items[0]!], 12, prior);
  assert.equal(single[0]!.lane, 0);
});

test('new occurrences cannot steal the rows of existing later events', () => {
  const items = [
    { key: 'new', left: 0, right: 80 },
    { key: 'existing', left: 20, right: 100 },
    { key: 'later', left: 140, right: 190 },
  ];
  const previous = new Map([
    ['existing', 0],
    ['later', 0],
  ]);
  const packed = packTimeline(items, 12, previous);
  assert.equal(packed.find((item) => item.key === 'existing')!.lane, 0);
  assert.equal(packed.find((item) => item.key === 'later')!.lane, 0);
  assert.equal(packed.find((item) => item.key === 'new')!.lane, 1);
});
