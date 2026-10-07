import type { TimeRange } from './types.js';
import { canonicalRange, withRangeEdgeNs } from './precise-time.js';

/** A recorded start or completion is an anchor, not a fabricated open duration. */
export function factualRangeForDrawing(range: TimeRange): TimeRange {
  if (range.precise) {
    const { start, end } = canonicalRange(range);
    if ((start === null) === (end === null)) return range;
    const recorded = start ?? end;
    return withRangeEdgeNs(withRangeEdgeNs(range, 'start', recorded), 'end', recorded);
  }
  if (!!range.start === !!range.end) return range;
  const recorded = range.start ?? range.end;
  return { ...range, start: recorded, end: recorded };
}
