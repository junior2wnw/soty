import test from 'node:test';
import assert from 'node:assert/strict';
import { createSeed } from './seed.js';
import { rangeFromNs } from './precise-time.js';
import {
  DAY,
  MS,
  NS,
  YEAR,
  MAX_SPAN,
  fitTimeline,
  focusRange,
  formatOffset,
  instantLabel,
  periodLabel,
  timeReference,
  panViewport,
  parseDuration,
  project,
  scaleToSpan,
  spanToScale,
  timeAt,
  timelineTicks,
  viewportBounds,
  zoomViewport,
} from './universal-timeline.js';

test('point under cursor stays exact across calendar, deep time and subsecond zoom', () => {
  for (const span of [MAX_SPAN, 2_000_000n * YEAR, 7n * DAY, 20n * MS, 19n * NS]) {
    const view = { center: -1_000_000n * YEAR + 123456789n, span };
    for (const fraction of [0, 0.17, 0.5, 0.93, 1]) {
      const anchor = timeAt(view, fraction);
      const next = zoomViewport(view, 2.7, fraction);
      assert.equal(timeAt(next, fraction), anchor);
    }
  }
});
test('nanosecond projection at a million-year distance subtracts before Number conversion', () => {
  const at = -1_000_000n * YEAR + 9007199254740999n;
  const view = { center: at, span: 20n };
  assert.equal(project(at + 1n, view, 1000) - project(at, view, 1000), 50);
});
test('the entire logarithmic rail reaches its real limits and roundtrips', () => {
  assert.equal(scaleToSpan(0), MAX_SPAN);
  assert.equal(scaleToSpan(100), 1n);
  for (const span of [NS, MS, 10n * DAY, YEAR, 2_000_000n * YEAR, MAX_SPAN]) {
    const recovered = scaleToSpan(spanToScale(span));
    assert.ok(Math.abs(Number(recovered - span) / Number(span)) < 1e-12);
  }
});
test('exact duration expressions accept Russian units and reject fractions of ns', () => {
  assert.equal(parseDuration('2 млн лет'), 2_000_000n * YEAR);
  assert.equal(parseDuration('0,1 мкс'), 100n);
  assert.equal(parseDuration('-20.000001 мс'), -20_000_001n);
  assert.equal(parseDuration('0.1 нс'), null);
  assert.equal(parseDuration('Infinity с'), null);
  assert.equal(parseDuration('1 лет + 2 дн + 0.000000001 с'), YEAR + 2n * DAY + 1n);
});
test('editor offset never rounds away a stored ns', () => {
  for (const at of [
    0n,
    1n,
    -1n,
    12345n,
    -20_000_001n,
    YEAR * 1_000_000n,
    YEAR * 1_000_000n + 1n,
    -(YEAR * 719_331n + 166n * DAY + 359993284n),
  ])
    assert.equal(parseDuration(formatOffset(at)), at);
});
test('microscopic creation and labels retain their reference in distant negative epochs', () => {
  const at = -640_334n * YEAR - 1n;
  const reference = timeReference(at, 'UTC', true);
  const anchor = BigInt(reference.anchorNs);
  assert.ok(anchor <= at && at - anchor < 1_000_000_000n);
  assert.notEqual(anchor, 0n);
  assert.equal(timeReference(at, 'UTC', false).anchorNs, '0');
  assert.equal(periodLabel({ center: at, span: 20n * MS }, 'UTC'), instantLabel(at, 'UTC', true));
  assert.notEqual(instantLabel(at, 'UTC', true), instantLabel(at + 1n, 'UTC', true));
  const shifted = -1_850_000n * YEAR + 20n * MS + 1n;
  assert.equal(formatOffset(shifted), '-1 850 000 лет +20.000001 мс');
  assert.equal(parseDuration(formatOffset(shifted)), shifted);
});
test('pan keeps scale and minimum window is nonempty and half open', () => {
  const view = { center: 123456789n, span: 20n * MS };
  assert.equal(panViewport(view, 100, 1000).center, view.center + 2n * MS);
  assert.equal(panViewport(view, 100, 1000).span, view.span);
  const tiny = viewportBounds({ center: 100n, span: 1n });
  assert.equal(tiny.end - tiny.start, 1n);
});
test('ruler remains bounded, ordered and valid at every width and scale', () => {
  for (const span of [NS, 20n * MS, DAY, 2_000_000n * YEAR, MAX_SPAN])
    for (const width of [320, 1440]) {
      const view = { center: -1_000_000n * YEAR, span },
        bounds = viewportBounds(view);
      const ticks = timelineTicks(view, width, 'Asia/Yekaterinburg');
      assert.ok(ticks.length > 0 && ticks.length <= 48);
      assert.ok(ticks.every((tick) => tick.at >= bounds.start && tick.at <= bounds.end));
      assert.ok(ticks.slice(1).every((tick, index) => tick.at > ticks[index]!.at));
    }
});
test('fit sees every precise layer and search focus reveals microscopic interval', () => {
  const seed = createSeed();
  const original = seed.entities[0]!;
  const entity = {
    ...original,
    recurrence: null,
    plan: rangeFromNs(-1_000_000n * YEAR, -999_999n * YEAR),
    actual: rangeFromNs(123n, 125n),
  };
  const fitted = fitTimeline([entity], Date.now()),
    bounds = viewportBounds(fitted);
  assert.ok(bounds.start <= -1_000_000n * YEAR && bounds.end >= 125n);
  const microscopic = {
    ...rangeFromNs(123n, 125n),
    precise: { ...rangeFromNs(123n, 125n).precise!, resolutionNs: '1' },
  };
  const focused = focusRange(microscopic, { center: 0n, span: MAX_SPAN });
  assert.equal(focused.span, 20n);
  assert.equal(focused.center, 124n);
  const ordinary = {
    start: '2026-10-03T07:00:00.000000001Z',
    end: '2026-10-03T07:00:00.020000001Z',
    timezone: 'UTC',
    precision: 'exact' as const,
  };
  const calendarFocused = focusRange(ordinary, { center: 0n, span: MAX_SPAN });
  assert.equal(calendarFocused.span, 26_666_666n);
  assert.equal(
    focusRange({ ...ordinary, end: '2026-10-03T07:00:00.000000002Z' }, calendarFocused).span,
    20n,
  );
});
test('fit handles precise facts without plan and a large collection without argument overflow', () => {
  const seed = createSeed();
  const entity = {
    ...seed.entities[0]!,
    recurrence: null,
    baseline: { start: null, end: null, timezone: 'UTC', precision: 'unknown' as const },
    plan: { start: null, end: null, timezone: 'UTC', precision: 'unknown' as const },
    dueAt: null,
    forecast: null,
    actual: rangeFromNs(-1_000_000n * YEAR, -1_000_000n * YEAR + 1n),
  };
  assert.equal(fitTimeline([entity], 0).span, 20n);
  const many = Array.from({ length: 70_000 }, (_, index) => ({ ...entity, id: `fit-${index}` }));
  assert.equal(fitTimeline(many, 0).span, 20n);
});
