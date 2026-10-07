import { DateTime } from 'luxon';
import type { Entity, TimeRange } from './types.js';

export interface TimelineViewport {
  center: number;
  span: number;
}

export interface TimelineTick {
  at: number;
  label: string;
  context?: string;
  major?: boolean;
}

const SECOND = 1_000;
const MINUTE = 60 * SECOND;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;
const YEAR = 365.2425 * DAY;
const DEFAULT_SPAN = 14 * DAY;

export const MIN_SPAN = MINUTE;
export const TIMELINE_START = -62_135_596_800_000; // 0001-01-01T00:00:00.000Z
export const TIMELINE_END = 253_402_300_799_999; // 9999-12-31T23:59:59.999Z
export const MAX_SPAN = TIMELINE_END - TIMELINE_START;

const clamp = (value: number, minimum: number, maximum: number) =>
  Math.min(maximum, Math.max(minimum, value));

/** Keep both edges within the supported calendar, including at the widest zoom. */
export function normalizeViewport(view: TimelineViewport): TimelineViewport {
  const requestedSpan = Number.isFinite(view.span)
    ? view.span
    : view.span === Infinity
      ? MAX_SPAN
      : DEFAULT_SPAN;
  const span = clamp(requestedSpan, MIN_SPAN, MAX_SPAN);
  const center = clamp(
    Number.isFinite(view.center) ? view.center : 0,
    TIMELINE_START + span / 2,
    TIMELINE_END - span / 2,
  );
  return { center, span };
}

export function viewportBounds(view: TimelineViewport): { start: number; end: number } {
  const normalized = normalizeViewport(view);
  return {
    start: normalized.center - normalized.span / 2,
    end: normalized.center + normalized.span / 2,
  };
}

/** factor > 1 zooms in; the instant under fraction of the viewport stays fixed. */
export function zoomViewport(
  view: TimelineViewport,
  factor: number,
  fraction = 0.5,
): TimelineViewport {
  const normalized = normalizeViewport(view);
  if (!(factor > 0)) return normalized;
  const position = Number.isFinite(fraction) ? clamp(fraction, 0, 1) : 0.5;
  const anchor = normalized.center + (position - 0.5) * normalized.span;
  const span = clamp(normalized.span / factor, MIN_SPAN, MAX_SPAN);
  return normalizeViewport({ center: anchor + (0.5 - position) * span, span });
}

/** Positive pixels move the viewport forward in time. Grab gestures pass -dx. */
export function panViewport(
  view: TimelineViewport,
  deltaPixels: number,
  width: number,
): TimelineViewport {
  const normalized = normalizeViewport(view);
  if (!Number.isFinite(deltaPixels) || !Number.isFinite(width) || width <= 0) return normalized;
  return normalizeViewport({
    center: normalized.center + (deltaPixels / width) * normalized.span,
    span: normalized.span,
  });
}

const SCALE_LOG_RANGE = Math.log(MAX_SPAN / MIN_SPAN);

/** A logarithmic slider: 0 = the whole calendar, 100 = one minute. */
export function spanToScale(span: number): number {
  const normalized = normalizeViewport({ center: 0, span }).span;
  return clamp((Math.log(MAX_SPAN / normalized) / SCALE_LOG_RANGE) * 100, 0, 100);
}

export function scaleToSpan(scale: number): number {
  const normalized = Number.isFinite(scale) ? clamp(scale, 0, 100) : 0;
  if (normalized === 0) return MAX_SPAN;
  if (normalized === 100) return MIN_SPAN;
  return MAX_SPAN * Math.exp((-normalized / 100) * SCALE_LOG_RANGE);
}

function validZone(zone: string): string {
  return DateTime.fromMillis(0, { zone }).isValid ? zone : 'UTC';
}

function parseDate(value: string | null | undefined, zone: string): DateTime | null {
  if (!value) return null;
  const parsed = DateTime.fromISO(value, { zone });
  return parsed.isValid ? parsed : null;
}

/** Calendar duration preserves the end's local clock through a DST transition. */
function recurrenceEnd(start: DateTime, entity: Entity): DateTime {
  const base = parseDate(entity.plan.start, start.zoneName ?? 'UTC');
  const end = parseDate(entity.plan.end, start.zoneName ?? 'UTC');
  if (!base || !end || end.toMillis() <= base.toMillis()) return start;
  if (entity.recurrence?.durationPolicy === 'elapsed')
    return start.plus({ milliseconds: end.toMillis() - base.toMillis() });
  const duration = end.diff(base, ['days', 'hours', 'minutes', 'seconds', 'milliseconds']);
  return start.plus(duration);
}

const RECURRENCE_FIT_STEPS = 2_048;

/** Resolve a finite count without expanding an unbounded series into the UI. */
function recurrenceLast(entity: Entity, base: DateTime): DateTime | null {
  const rule = entity.recurrence;
  if (
    !rule ||
    !Number.isInteger(rule.count) ||
    !rule.count ||
    rule.count < 1 ||
    !Number.isInteger(rule.interval) ||
    rule.interval < 1 ||
    rule.interval > 365
  )
    return null;
  const weekdays = [...new Set(rule.weekdays?.length ? rule.weekdays : [base.weekday])]
    .filter((day) => Number.isInteger(day) && day >= 1 && day <= 7)
    .sort((a, b) => a - b);
  const firstWeekdays = weekdays.filter((day) => day >= base.weekday);
  const candidate = (index: number, anchor = base): DateTime | null => {
    if (rule.frequency === 'week') {
      if (!weekdays.length) return null;
      const remainder = index - firstWeekdays.length;
      const week = remainder < 0 ? 0 : 1 + Math.floor(remainder / weekdays.length);
      const weekday =
        remainder < 0 ? firstWeekdays[index]! : weekdays[remainder % weekdays.length]!;
      return anchor
        .startOf('week')
        .plus({ weeks: week * rule.interval, days: weekday - 1 })
        .set({
          hour: anchor.hour,
          minute: anchor.minute,
          second: anchor.second,
          millisecond: anchor.millisecond,
        });
    }
    if (rule.frequency === 'day') return anchor.plus({ days: index * rule.interval });
    if (rule.frequency === 'month') return anchor.plus({ months: index * rule.interval });
    return null;
  };
  const exceptions = new Set(rule.exceptions);
  const exceptionInstants = new Set(
    rule.exceptions
      .filter((value) => value.length !== 10)
      .map((value) => parseDate(value, base.zoneName ?? 'UTC')?.toMillis()),
  );
  const isExcepted = (date: DateTime) =>
    exceptions.has(date.toISODate()!) || exceptionInstants.has(date.toMillis());
  if (rule.calendarPolicy !== 'skip-invalid') {
    for (let back = 0; back < Math.min(rule.count, 64); back++) {
      const date = candidate(rule.count - 1 - back);
      if (!date) return null;
      // An out-of-range positive count still needs the widest supported calendar.
      if (!date.isValid) return DateTime.fromMillis(TIMELINE_END, { zone: 'UTC' });
      if (!isExcepted(date)) return date;
    }
    return null;
  }
  // For skip-invalid, invalid local dates do not consume COUNT. Bound the work
  // rather than pretending nominal occurrences have actually happened.
  let emitted = 0;
  let last: DateTime | null = null;
  const calendarBase = DateTime.utc(
    base.year,
    base.month,
    base.day,
    base.hour,
    base.minute,
    base.second,
    base.millisecond,
  );
  for (let index = 0; index < RECURRENCE_FIT_STEPS; index++) {
    let date = candidate(index);
    if (!date?.isValid) break;
    if (
      date.hour !== base.hour ||
      date.minute !== base.minute ||
      date.toISODate() !== candidate(index, calendarBase)?.toISODate() ||
      (rule.frequency === 'month' && date.day !== base.day)
    )
      continue;
    date = date.getPossibleOffsets().sort((a, b) => a.toMillis() - b.toMillis())[0] ?? date;
    emitted++;
    if (!isExcepted(date)) last = date;
    if (emitted >= rule.count) return last;
  }
  return null;
}

/** Fit every dated layer; an undated note never acquires an invented timestamp. */
export function fitTimeline(entities: Entity[], fallbackNow: number): TimelineViewport {
  let first = Infinity;
  let last = -Infinity;
  const include = (at: number | undefined) => {
    if (at === undefined || !Number.isFinite(at)) return;
    const bounded = clamp(at, TIMELINE_START, TIMELINE_END);
    first = Math.min(first, bounded);
    last = Math.max(last, bounded);
  };
  const includeRange = (range: TimeRange | null) => {
    if (!range) return;
    const zone = validZone(range.timezone);
    for (const value of [range.start, range.end, range.earliest])
      include(parseDate(value, zone)?.toMillis());
    const latest = parseDate(range.latest, zone);
    include(range.latest?.length === 10 ? latest?.endOf('day').toMillis() : latest?.toMillis());
  };
  for (const entity of entities) {
    for (const range of [entity.plan, entity.baseline, entity.actual, entity.forecast])
      includeRange(range);
    const zone = validZone(entity.plan.timezone);
    const due = parseDate(entity.dueAt, zone);
    include(entity.dueAt?.length === 10 ? due?.endOf('day').toMillis() : due?.toMillis());
    const base = parseDate(entity.plan.start, zone);
    if (!base || !entity.recurrence) continue;
    const until = parseDate(entity.recurrence.until, zone);
    const untilAt = entity.recurrence.until?.length === 10 ? until?.endOf('day') : until;
    const counted = recurrenceLast(entity, base);
    const horizon =
      counted && untilAt
        ? counted.toMillis() <= untilAt.toMillis()
          ? counted
          : untilAt
        : (counted ?? untilAt);
    if (horizon && horizon.toMillis() >= base.toMillis())
      include(recurrenceEnd(horizon, entity).toMillis());
  }
  if (!Number.isFinite(first))
    return normalizeViewport({ center: fallbackNow, span: DEFAULT_SPAN });
  const extent = last - first;
  const padding = Math.max(extent * 0.08, MIN_SPAN / 2);
  return normalizeViewport({
    center: first + extent / 2,
    span: extent === 0 ? DAY : extent + padding * 2,
  });
}

type TickUnit = 'second' | 'minute' | 'hour' | 'day' | 'week' | 'month' | 'year';
interface TickInterval {
  unit: TickUnit;
  step: number;
  approximate: number;
}

const TICK_INTERVALS: TickInterval[] = [
  ...[1, 2, 5, 10, 15, 30].map((step) => ({
    unit: 'second' as const,
    step,
    approximate: step * SECOND,
  })),
  ...[1, 2, 5, 10, 15, 30].map((step) => ({
    unit: 'minute' as const,
    step,
    approximate: step * MINUTE,
  })),
  ...[1, 2, 3, 6, 12].map((step) => ({ unit: 'hour' as const, step, approximate: step * HOUR })),
  ...[1, 2].map((step) => ({ unit: 'day' as const, step, approximate: step * DAY })),
  ...[1, 2].map((step) => ({ unit: 'week' as const, step, approximate: step * 7 * DAY })),
  ...[1, 2, 3, 6].map((step) => ({
    unit: 'month' as const,
    step,
    approximate: (step * YEAR) / 12,
  })),
  ...[1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000].map((step) => ({
    unit: 'year' as const,
    step,
    approximate: step * YEAR,
  })),
].sort((a, b) => a.approximate - b.approximate);

function alignedTick(start: DateTime, interval: TickInterval): DateTime {
  const { unit, step } = interval;
  if (unit === 'second')
    return start.startOf('second').set({ second: Math.floor(start.second / step) * step });
  if (unit === 'minute')
    return start.startOf('minute').set({ minute: Math.floor(start.minute / step) * step });
  if (unit === 'hour') {
    for (let hour = Math.floor(start.hour / step) * step; hour >= 0; hour -= step) {
      const candidate = start.startOf('hour').set({ hour });
      if (candidate.hour === hour) return candidate;
    }
    return start.startOf('day');
  }
  if (unit === 'day')
    return start.startOf('month').plus({ days: Math.floor((start.day - 1) / step) * step });
  if (unit === 'week') {
    const ordinal = DateTime.utc(start.year, start.month, start.day).toMillis() / DAY;
    const week = Math.floor((ordinal - 4) / 7);
    const remainder = ((week % step) + step) % step;
    return start.startOf('week').minus({ weeks: remainder });
  }
  if (unit === 'month')
    return start.startOf('year').plus({ months: Math.floor((start.month - 1) / step) * step });
  return start.set({ year: Math.floor(start.year / step) * step }).startOf('year');
}

function tickLabel(date: DateTime, unit: TickUnit): string {
  if (unit === 'second') return date.toFormat('HH:mm:ss');
  if (unit === 'minute' || unit === 'hour') return date.toFormat('HH:mm');
  if (unit === 'day' || unit === 'week') return date.toFormat('d LLL');
  if (unit === 'month') return date.toFormat('LLL');
  return String(date.year);
}

function nextTick(date: DateTime, interval: TickInterval): DateTime {
  if (interval.unit !== 'hour' || interval.step === 1)
    return date.plus({ [`${interval.unit}s`]: interval.step });
  // Six-hour marks remain 00, 06, 12, 18 in local time, even on a 23/25-hour
  // day. Missing wall-clock slots are skipped, not shifted into fake marks.
  let cursor = date;
  for (let attempt = 0; attempt < 4; attempt++) {
    const hour = (Math.floor(cursor.hour / interval.step) + 1) * interval.step;
    const day = cursor.startOf('day').plus({ days: hour >= 24 ? 1 : 0 });
    const candidate = day.set({ hour: hour % 24 });
    if (candidate.hour === hour % 24 && candidate.toMillis() > date.toMillis()) return candidate;
    cursor = candidate;
  }
  return date.plus({ hours: interval.step });
}

/** Calendar-aware marks change density with scale, never with a calendar mode. */
export function timelineTicks(view: TimelineViewport, width: number, zone: string): TimelineTick[] {
  if (!Number.isFinite(width) || width <= 0) return [];
  const normalized = normalizeViewport(view);
  const { start, end } = viewportBounds(normalized);
  const targetCount = clamp(Math.floor(width / 92), 2, 100);
  const targetDuration = normalized.span / targetCount;
  const interval =
    TICK_INTERVALS.find((entry) => entry.approximate >= targetDuration) ?? TICK_INTERVALS.at(-1)!;
  let cursor = alignedTick(
    DateTime.fromMillis(start, { zone: validZone(zone), locale: 'ru' }),
    interval,
  );
  const ticks: TimelineTick[] = [];
  let previous: DateTime | null = null;
  for (let tries = 0; tries < 202 && cursor.isValid && cursor.toMillis() <= end; tries++) {
    const at = cursor.toMillis();
    if (at >= start && at >= TIMELINE_START && at <= TIMELINE_END) {
      const subday = ['second', 'minute', 'hour'].includes(interval.unit);
      let context: string | undefined;
      if (subday) {
        if (!previous || cursor.toISODate() !== previous.toISODate())
          context = cursor.toFormat('d LLL yyyy');
        if (cursor.getPossibleOffsets().length > 1)
          context = [context, `UTC${cursor.toFormat('ZZ')}`].filter(Boolean).join(' · ');
      } else if (interval.unit !== 'year' && (!previous || cursor.year !== previous.year)) {
        context = String(cursor.year);
      }
      const major =
        interval.unit === 'year' ||
        (interval.unit === 'month' && cursor.month === 1) ||
        (!subday && cursor.day === 1) ||
        (subday && cursor.hour === 0 && cursor.minute === 0 && cursor.second === 0);
      ticks.push({
        at,
        label: tickLabel(cursor, interval.unit),
        ...(context ? { context } : {}),
        major,
      });
      previous = cursor;
      if (ticks.length === 200) break;
    }
    const next = nextTick(cursor, interval);
    if (!next.isValid || next.toMillis() <= at) break;
    cursor = next;
  }
  // A short edge fragment or an entire millennial view can fall between nice
  // calendar marks. Keep one readable date in that case rather than an empty ruler.
  if (!ticks.length) {
    const date = DateTime.fromMillis(normalized.center, { zone: validZone(zone), locale: 'ru' });
    ticks.push({ at: normalized.center, label: tickLabel(date, interval.unit), major: true });
  }
  return ticks;
}

export function spanLabel(span: number): string {
  const value = normalizeViewport({ center: 0, span }).span;
  const units = [
    { duration: YEAR, forms: ['год', 'года', 'лет'] },
    { duration: YEAR / 12, forms: ['месяц', 'месяца', 'месяцев'] },
    { duration: 7 * DAY, forms: ['неделя', 'недели', 'недель'] },
    { duration: DAY, forms: ['день', 'дня', 'дней'] },
    { duration: HOUR, forms: ['час', 'часа', 'часов'] },
    { duration: MINUTE, forms: ['минута', 'минуты', 'минут'] },
  ];
  const unit = units.find((entry) => value >= entry.duration) ?? units.at(-1)!;
  const amount = Math.max(1, Math.round(value / unit.duration));
  const mod100 = amount % 100;
  const mod10 = amount % 10;
  const form =
    mod100 >= 11 && mod100 <= 14 ? 2 : mod10 === 1 ? 0 : mod10 >= 2 && mod10 <= 4 ? 1 : 2;
  return `${amount.toLocaleString('ru-RU')} ${unit.forms[form]}`;
}

class MinHeap<T> {
  private values: T[] = [];
  constructor(private readonly compare: (a: T, b: T) => number) {}
  get size() {
    return this.values.length;
  }
  peek() {
    return this.values[0];
  }
  push(value: T) {
    const values = this.values;
    values.push(value);
    let index = values.length - 1;
    while (index > 0) {
      const parent = Math.floor((index - 1) / 2);
      if (this.compare(values[parent]!, value) <= 0) break;
      values[index] = values[parent]!;
      index = parent;
    }
    values[index] = value;
  }
  pop(): T | undefined {
    const values = this.values;
    const result = values[0];
    const last = values.pop();
    if (!values.length || last === undefined) return result;
    let index = 0;
    while (index * 2 + 1 < values.length) {
      const left = index * 2 + 1;
      const right = left + 1;
      const child =
        right < values.length && this.compare(values[right]!, values[left]!) < 0 ? right : left;
      if (this.compare(last, values[child]!) <= 0) break;
      values[index] = values[child]!;
      index = child;
    }
    values[index] = last;
    return result;
  }
}

/** Interval packing; optional previous lanes preserve visual continuity during navigation. */
export function packTimeline<T extends { key: string; left: number; right: number }>(
  items: T[],
  gap = 12,
  previous?: ReadonlyMap<string, number>,
): Array<T & { lane: number }> {
  const spacing = Number.isFinite(gap) ? Math.max(0, gap) : 12;
  // Compact empty rows, but keep the relative order of rows still on screen.
  const occupied = previous
    ? [
        ...new Set(
          items.flatMap((item) => {
            const lane = previous.get(item.key);
            return lane != null && Number.isInteger(lane) && lane >= 0 ? [lane] : [];
          }),
        ),
      ].sort((a, b) => a - b)
    : [];
  const ranks = new Map(occupied.map((lane, index) => [lane, index]));
  const preference = (item: T) => ranks.get(previous?.get(item.key) ?? -1);
  const sorted = [...items].sort(
    (a, b) =>
      a.left - b.left ||
      (previous ? (preference(a) ?? Infinity) - (preference(b) ?? Infinity) : 0) ||
      a.right - b.right ||
      (a.key < b.key ? -1 : a.key > b.key ? 1 : 0),
  );
  // New groups/occurrences must not take a row that an existing item will need later.
  const reservations = new Map<number, { items: T[]; next: number }>();
  for (const item of sorted) {
    const lane = preference(item);
    if (lane == null) continue;
    let queue = reservations.get(lane);
    if (!queue) reservations.set(lane, (queue = { items: [], next: 0 }));
    queue.items.push(item);
  }
  const active = new MinHeap<{ right: number; lane: number }>(
    (a, b) => a.right - b.right || a.lane - b.lane,
  );
  const free = new MinHeap<number>((a, b) => a - b);
  const available = new Set<number>();
  let lanes = occupied.length;
  for (let lane = 0; lane < lanes; lane++) {
    free.push(lane);
    available.add(lane);
  }
  return sorted.map((item) => {
    if (!Number.isFinite(item.left) || !Number.isFinite(item.right) || item.right < item.left)
      throw new RangeError(`Invalid timeline footprint: ${item.key}`);
    while (active.size && active.peek()!.right + spacing <= item.left) {
      const lane = active.pop()!.lane;
      free.push(lane);
      available.add(lane);
    }
    const preferred = preference(item);
    if (preferred != null) reservations.get(preferred)!.next++;
    let lane: number;
    if (preferred != null && available.has(preferred)) lane = preferred;
    else {
      let found: number | undefined;
      const reserved: number[] = [];
      while (free.size) {
        const candidate = free.pop()!;
        if (!available.has(candidate)) continue;
        const queue = reservations.get(candidate);
        const next = queue?.items[queue.next];
        if (next && next.left < item.right + spacing) reserved.push(candidate);
        else {
          found = candidate;
          break;
        }
      }
      for (const candidate of reserved) free.push(candidate);
      lane = found ?? lanes++;
    }
    available.delete(lane);
    active.push({ right: item.right, lane });
    return { ...item, lane };
  });
}
