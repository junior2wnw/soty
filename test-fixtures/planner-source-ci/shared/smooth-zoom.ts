import {
  clampNs,
  MAX_SPAN,
  MIN_SPAN,
  zoomToSpan,
  zoomViewport,
  type TimelineViewport,
} from './universal-timeline.js';

export type WheelZoomMotion = {
  origin: TimelineViewport;
  targetSpan: bigint;
  fraction: number;
  direction: number;
  lastTime: number;
};

/** Device units become a bounded relative step, with no speed-dependent acceleration. */
export function wheelZoomDelta(delta: number, mode: number, pageHeight: number): number {
  if (!Number.isFinite(delta)) return 0;
  const unit = mode === 1 ? 16 : mode === 2 ? Math.max(1, pageHeight) : 1;
  const change = -delta * unit * 0.0012;
  return Number.isFinite(change) ? Math.max(-0.24, Math.min(0.24, change)) : 0;
}

export function retargetWheelZoom(
  current: TimelineViewport,
  previous: WheelZoomMotion | null,
  delta: number,
  fraction: number,
  now: number,
): WheelZoomMotion | null {
  if (!delta) return previous;
  const direction = Math.sign(delta);
  // Reversing the wheel cancels outstanding travel in the old direction.
  const continuing = previous?.direction === direction;
  const base = continuing ? previous.targetSpan : current.span;
  let targetSpan = zoomViewport({ center: 0n, span: base }, Math.exp(delta)).span;
  // At nanosecond resolution a gesture must still be able to leave the minimum.
  if (targetSpan === base)
    targetSpan = clampNs(base + (direction < 0 ? 1n : -1n), MIN_SPAN, MAX_SPAN);
  if (targetSpan === current.span) return null;
  return {
    origin: current,
    targetSpan,
    fraction: Math.max(0, Math.min(1, fraction)),
    direction,
    lastTime: continuing ? previous.lastTime : now,
  };
}

/** Damping operates on relative scale; the cursor anchor always stays in exact BigInt time. */
export function advanceWheelZoom(
  current: TimelineViewport,
  motion: WheelZoomMotion,
  now: number,
): { view: TimelineViewport; motion: WheelZoomMotion | null } {
  const difference = motion.targetSpan - current.span;
  const relative = Math.log(Number(motion.targetSpan) / Number(current.span));
  if ((difference >= -1n && difference <= 1n) || Math.abs(relative) < 0.0005)
    return { view: zoomToSpan(motion.origin, motion.targetSpan, motion.fraction), motion: null };

  // A stalled frame never releases a large accumulated jump on the next frame.
  const elapsed = Math.max(1, Math.min(32, now - motion.lastTime));
  const step =
    Math.sign(relative) * Math.min(0.06, Math.abs(relative) * (1 - Math.exp(-elapsed / 50)));
  let span = BigInt(Math.round(Number(current.span) * Math.exp(step)));
  if (span === current.span) span += difference > 0n ? 1n : -1n;
  span =
    difference > 0n
      ? clampNs(span, current.span, motion.targetSpan)
      : clampNs(span, motion.targetSpan, current.span);
  return {
    view: zoomToSpan(motion.origin, span, motion.fraction),
    motion: span === motion.targetSpan ? null : { ...motion, lastTime: now },
  };
}
