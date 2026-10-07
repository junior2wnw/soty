import test from 'node:test';
import assert from 'node:assert/strict';
import {
  advanceWheelZoom,
  retargetWheelZoom,
  wheelZoomDelta,
  type WheelZoomMotion,
} from './smooth-zoom.js';
import { DAY, MAX_SPAN, MS, YEAR, timeAt, type TimelineViewport } from './universal-timeline.js';

function settle(
  view: TimelineViewport,
  motion: WheelZoomMotion | null,
  start = 0,
  interval = 1000 / 60,
) {
  const frames: TimelineViewport[] = [view];
  for (let elapsed = interval; motion && elapsed < 2000; elapsed += interval) {
    const next = advanceWheelZoom(view, motion, start + elapsed);
    view = next.view;
    motion = next.motion;
    frames.push(view);
  }
  assert.equal(motion, null, 'zoom must settle without an endless animation');
  return frames;
}

test('a mouse notch is gentle and equivalent line/pixel events have equal sensitivity', () => {
  const ratio = Math.exp(wheelZoomDelta(-120, 0, 900));
  assert.ok(ratio > 1.1 && ratio < 1.2);
  assert.equal(wheelZoomDelta(-3, 1, 900), wheelZoomDelta(-48, 0, 900));
  assert.equal(wheelZoomDelta(3, 0, 900), -wheelZoomDelta(-3, 0, 900));
  assert.ok(Math.abs(wheelZoomDelta(-2, 0, 900)) < 0.003);
});

test('large wheel/page events are bounded and invalid input cannot break the viewport', () => {
  for (const mode of [0, 1, 2])
    for (const delta of [-1000000, 1000000])
      assert.ok(Math.abs(wheelZoomDelta(delta, mode, 900)) <= 0.24);
  assert.equal(wheelZoomDelta(Infinity, 0, 900), 0);
  assert.equal(wheelZoomDelta(NaN, 0, 900), 0);
  assert.equal(wheelZoomDelta(1, 2, NaN), 0);
});

test('one notch moves over multiple frames and reaches exactly its requested scale', () => {
  const view = { center: 123456789n, span: 7n * DAY };
  const motion = retargetWheelZoom(view, null, wheelZoomDelta(-120, 0, 900), 0.37, 0)!;
  const frames = settle(view, motion);
  assert.ok(frames.length >= 8);
  assert.equal(frames.at(-1)!.span, motion.targetSpan);
  for (let i = 1; i < frames.length; i++) {
    assert.ok(frames[i]!.span <= frames[i - 1]!.span);
    assert.ok(Math.abs(Math.log(Number(frames[i]!.span) / Number(frames[i - 1]!.span))) <= 0.061);
  }
});

test('the exact cursor instant stays fixed in every frame at all time scales', () => {
  for (const span of [MAX_SPAN, 2_000_000n * YEAR, 7n * DAY, 20n * MS, 19n]) {
    const view = { center: -1_000_000n * YEAR + 123456789n, span };
    for (const fraction of [0, 0.17, 0.5, 0.93, 1]) {
      const motion = retargetWheelZoom(view, null, wheelZoomDelta(-120, 0, 900), fraction, 0);
      const anchor = timeAt(view, fraction);
      for (const frame of settle(view, motion)) assert.equal(timeAt(frame, fraction), anchor);
    }
  }
});

test('refresh rate changes do not change perceived speed or the final target', () => {
  const view = { center: 123456789n, span: 7n * DAY };
  const motion = retargetWheelZoom(view, null, 0.144, 0.37, 0)!;
  const spans: bigint[] = [];
  for (const interval of [1000 / 120, 1000 / 60, 25]) {
    let current = view;
    let ongoing: WheelZoomMotion | null = motion;
    for (let at = interval; at <= 100.00001 && ongoing; at += interval) {
      const next = advanceWheelZoom(current, ongoing, at);
      current = next.view;
      ongoing = next.motion;
    }
    spans.push(current.span);
    assert.equal(settle(current, ongoing, 100, interval).at(-1)!.span, motion.targetSpan);
  }
  for (const span of spans) assert.ok(Math.abs(Number(span - spans[0]!) / Number(span)) < 1e-10);
});

test('fast and slow wheel bursts request the same cumulative scale without acceleration', () => {
  const origin = { center: 0n, span: 7n * DAY };
  const targets: bigint[] = [];
  for (const interval of [10, 500]) {
    let view = origin;
    let motion: WheelZoomMotion | null = null;
    for (let i = 0; i < 3; i++) {
      if (interval === 500 && motion) {
        view = settle(view, motion, (i - 1) * interval).at(-1)!;
        motion = null;
      }
      motion = retargetWheelZoom(view, motion, 0.144, 0.37, i * interval);
    }
    targets.push(motion!.targetSpan);
  }
  assert.equal(targets[0], targets[1]);
  assert.ok(Number(origin.span) / Number(targets[0]) < 1.6);
});

test('reversal responds immediately instead of continuing toward the old target', () => {
  const origin = { center: 0n, span: 7n * DAY };
  const first = retargetWheelZoom(origin, null, 0.144, 0.37, 0)!;
  const middle = advanceWheelZoom(origin, first, 16);
  const reversed = retargetWheelZoom(middle.view, middle.motion, -0.144, 0.37, 20)!;
  assert.ok(reversed.targetSpan > middle.view.span);
  const next = advanceWheelZoom(middle.view, reversed, 36);
  assert.ok(next.view.span > middle.view.span);
  assert.equal(timeAt(next.view, 0.37), timeAt(middle.view, 0.37));
});

test('moving the cursor while scrolling adopts the new exact anchor', () => {
  const origin = { center: -1_000_000n * YEAR + 1n, span: 20n * MS };
  const first = retargetWheelZoom(origin, null, 0.144, 0.1, 0)!;
  const middle = advanceWheelZoom(origin, first, 16);
  const next = retargetWheelZoom(middle.view, middle.motion, 0.144, 0.9, 20)!;
  for (const frame of settle(middle.view, next, 20))
    assert.equal(timeAt(frame, 0.9), timeAt(middle.view, 0.9));
});

test('even after a stalled frame a long burst resumes with a bounded visual step', () => {
  const origin = { center: 0n, span: 7n * DAY };
  let motion: WheelZoomMotion | null = null;
  for (let i = 0; i < 20; i++) motion = retargetWheelZoom(origin, motion, 0.144, 0.37, i);
  const next = advanceWheelZoom(origin, motion!, 2000);
  assert.ok(Math.abs(Math.log(Number(next.view.span) / Number(origin.span))) <= 0.061);
  assert.ok(next.motion);
});

test('nanosecond and billion-year limits remain usable without stalling', () => {
  const minimum = { center: -1_000_000n * YEAR + 1n, span: 1n };
  assert.equal(retargetWheelZoom(minimum, null, 0.144, 0.37, 0), null);
  const outward = retargetWheelZoom(minimum, null, -0.144, 0.37, 0)!;
  assert.equal(settle(minimum, outward).at(-1)!.span, 2n);
  assert.equal(timeAt(settle(minimum, outward).at(-1)!, 0.37), timeAt(minimum, 0.37));
  const maximum = { center: 0n, span: MAX_SPAN };
  assert.equal(retargetWheelZoom(maximum, null, -0.144, 0.37, 0), null);
  assert.ok(
    settle(maximum, retargetWheelZoom(maximum, null, 0.144, 0.37, 0)).at(-1)!.span < MAX_SPAN,
  );
});
