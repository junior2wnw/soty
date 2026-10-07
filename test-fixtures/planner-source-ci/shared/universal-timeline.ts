import { DateTime } from 'luxon';
import type { Entity, TimeRange } from './types.js';
import {
  canonicalRange,
  hasTime,
  hasSubMillisecondTime,
  isoToNs,
  nsToISO,
} from './precise-time.js';
import {
  fitTimeline as calendarFit,
  timelineTicks as calendarTicks,
  TIMELINE_START,
  TIMELINE_END,
} from './timeline.js';

export const NS = 1n;
export const US = 1_000n;
export const MS = 1_000_000n;
export const SECOND = 1_000n * MS;
export const MINUTE = 60n * SECOND;
export const HOUR = 60n * MINUTE;
export const DAY = 24n * HOUR;
/** A relative duration for the wide axis, never a calendar recurrence interval. */
export const YEAR = 31_556_952n * SECOND;
export const MIN_SPAN = NS;
export const MAX_SPAN = 1_000_000_000n * YEAR;
const FRACTION = 1_000_000_000n;
const LOG_RANGE = Math.log(Number(MAX_SPAN));
export type TimelineViewport = { center: bigint; span: bigint };
export type TimelineTick = { at: bigint; label: string; context?: string; major?: boolean };
const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));
export const minNs = (...values: bigint[]) => values.reduce((a, b) => (a < b ? a : b));
export const maxNs = (...values: bigint[]) => values.reduce((a, b) => (a > b ? a : b));
export const clampNs = (value: bigint, min: bigint, max: bigint) => minNs(max, maxNs(min, value));
export const fromMillis = (ms: number) => BigInt(Math.trunc(ms)) * MS;
export function normalizeViewport(view: TimelineViewport): TimelineViewport {
  return { center: view.center, span: clampNs(view.span, MIN_SPAN, MAX_SPAN) };
}
export function viewportBounds(view: TimelineViewport) {
  const normalized = normalizeViewport(view);
  const start = normalized.center - normalized.span / 2n;
  return { start, end: start + normalized.span };
}
function fraction(value: number) {
  return BigInt(Math.round((Number.isFinite(value) ? value : 0) * Number(FRACTION)));
}
export function timeAt(view: TimelineViewport, position: number): bigint {
  return viewportBounds(view).start + (view.span * fraction(clamp(position, 0, 1))) / FRACTION;
}
export function project(at: bigint, view: TimelineViewport, width = 100): number {
  // Subtract before conversion. Adjacent ns at a million-year offset stay distinct.
  return (Number(at - viewportBounds(view).start) / Number(view.span)) * width;
}
export function zoomViewport(
  view: TimelineViewport,
  factor: number,
  position = 0.5,
): TimelineViewport {
  const current = normalizeViewport(view);
  if (!(factor > 0) || !Number.isFinite(factor)) return current;
  const p = clamp(position, 0, 1);
  const anchor = timeAt(current, p);
  const divisor = maxNs(1n, fraction(factor));
  const span = clampNs((current.span * FRACTION) / divisor, MIN_SPAN, MAX_SPAN);
  const start = anchor - (span * fraction(p)) / FRACTION;
  return { center: start + span / 2n, span };
}
export function zoomToSpan(view: TimelineViewport, span: bigint, position = 0.5): TimelineViewport {
  const target = clampNs(span, MIN_SPAN, MAX_SPAN);
  const anchor = timeAt(view, position);
  return { center: anchor - (target * fraction(position)) / FRACTION + target / 2n, span: target };
}
export function panViewport(
  view: TimelineViewport,
  deltaPixels: number,
  width: number,
): TimelineViewport {
  if (!Number.isFinite(deltaPixels) || !Number.isFinite(width) || width <= 0) return view;
  return { ...view, center: view.center + (view.span * fraction(deltaPixels / width)) / FRACTION };
}
export function spanToScale(span: bigint): number {
  const bounded = clampNs(span, MIN_SPAN, MAX_SPAN);
  return clamp(((LOG_RANGE - Math.log(Number(bounded))) / LOG_RANGE) * 100, 0, 100);
}
export function scaleToSpan(value: number): bigint {
  const scale = clamp(Number.isFinite(value) ? value : 0, 0, 100);
  if (scale === 0) return MAX_SPAN;
  if (scale === 100) return MIN_SPAN;
  return clampNs(BigInt(Math.round(Math.exp(LOG_RANGE * (1 - scale / 100)))), MIN_SPAN, MAX_SPAN);
}
const UNITS = [
  { size: YEAR, name: 'лет', aliases: ['y', 'yr', 'year', 'years', 'г', 'год', 'года', 'лет'] },
  { size: DAY, name: 'дн', aliases: ['d', 'day', 'days', 'д', 'дн', 'день', 'дня', 'дней'] },
  { size: HOUR, name: 'ч', aliases: ['h', 'hour', 'hours', 'ч', 'час', 'часа', 'часов'] },
  {
    size: MINUTE,
    name: 'мин',
    aliases: ['m', 'min', 'minute', 'minutes', 'мин', 'минута', 'минут'],
  },
  {
    size: SECOND,
    name: 'с',
    aliases: ['s', 'sec', 'second', 'seconds', 'с', 'сек', 'секунда', 'секунд'],
  },
  { size: MS, name: 'мс', aliases: ['ms', 'мс', 'миллисекунда', 'миллисекунд'] },
  { size: US, name: 'мкс', aliases: ['us', 'µs', 'μs', 'мкс', 'микросекунда', 'микросекунд'] },
  { size: NS, name: 'нс', aliases: ['ns', 'нс', 'наносекунда', 'наносекунд'] },
];
/** Exact decimal parser: numbers never pass through binary floating point. */
function parseSingleDuration(text: string): bigint | null {
  const match = text
    .trim()
    .toLowerCase()
    .replaceAll('−', '-')
    .replaceAll(',', '.')
    .match(/^([+-]?\d+(?:\.\d+)?)\s*(тыс\.?|млн\.?|млрд\.?|k|million|billion)?\s*([a-zа-яµμ]+)$/u);
  if (!match) return null;
  const unit = UNITS.find((value) => value.aliases.includes(match[3]!));
  if (!unit) return null;
  const amount = match[1]!,
    negative = amount.startsWith('-');
  const [whole, decimals = ''] = amount.replace(/^[+-]/, '').split('.');
  if (decimals.length > 18 || whole!.length > 40) return null;
  const denominator = 10n ** BigInt(decimals.length);
  const multiplier = match[2]?.replace('.', '');
  const power = ['млрд', 'billion'].includes(multiplier ?? '')
    ? 1_000_000_000n
    : ['млн', 'million'].includes(multiplier ?? '')
      ? 1_000_000n
      : multiplier
        ? 1_000n
        : 1n;
  const numerator = BigInt(whole! + decimals) * unit.size * power;
  if (numerator % denominator !== 0n) return null;
  return (negative ? -1n : 1n) * (numerator / denominator);
}
export function parseDuration(text: string): bigint | null {
  const normalized = text
    .trim()
    .toLowerCase()
    .replaceAll('−', '-')
    .replaceAll(',', '.')
    .replace(/\s+/g, '');
  if (!normalized || normalized.length > 500) return null;
  const tokens = /[+-]?\d+(?:\.\d+)?(?:тыс\.?|млн\.?|млрд\.?|k|million|billion)?[a-zа-яµμ]+/guy;
  let total = 0n,
    position = 0;
  while (position < normalized.length) {
    tokens.lastIndex = position;
    const token = tokens.exec(normalized);
    if (!token) return null;
    const amount = parseSingleDuration(token[0]);
    if (amount == null) return null;
    total += amount;
    position = tokens.lastIndex;
  }
  return total;
}
export function formatDuration(ns: bigint): string {
  const abs = ns < 0n ? -ns : ns;
  const unit = UNITS.find((value) => abs >= value.size) ?? UNITS.at(-1)!;
  const scaled = Number(abs) / Number(unit.size);
  const suffix =
    unit.size === YEAR && scaled >= 1e9
      ? ' млрд лет'
      : unit.size === YEAR && scaled >= 1e6
        ? ' млн лет'
        : unit.size === YEAR && scaled === 1
          ? ' год'
          : ` ${unit.name}`;
  const amount = suffix.includes('млрд')
    ? scaled / 1e9
    : suffix.includes('млн')
      ? scaled / 1e6
      : scaled;
  return `${ns < 0n ? '−' : ''}${amount.toLocaleString('ru-RU', { maximumFractionDigits: 3 })}${suffix}`;
}
/** Lossless editor value; unlike the rounded labels, this always parses back exactly. */
export function formatOffset(ns: bigint): string {
  const abs = ns < 0n ? -ns : ns;
  if (abs >= DAY) {
    let remaining = ns;
    const parts: string[] = [];
    const append = (amount: bigint, name: string) => {
      if (!amount) return;
      const magnitude = amount < 0n ? -amount : amount;
      const sign = amount < 0n ? '-' : parts.length ? '+' : '';
      parts.push(`${sign}${magnitude.toLocaleString('ru-RU')} ${name}`);
    };
    // Nearest-year reference keeps a tiny shift across a negative year boundary readable.
    if (abs >= YEAR) {
      const years = ((abs + YEAR / 2n) / YEAR) * (ns < 0n ? -1n : 1n);
      append(years, years === 1n || years === -1n ? 'год' : 'лет');
      remaining -= years * YEAR;
    }
    for (const unit of [
      { size: DAY, name: 'дн' },
      { size: HOUR, name: 'ч' },
      { size: MINUTE, name: 'мин' },
    ]) {
      const amount = remaining / unit.size;
      remaining %= unit.size;
      append(amount, unit.name);
    }
    if (remaining)
      parts.push(`${remaining > 0n && parts.length ? '+' : ''}${formatOffset(remaining)}`);
    return parts.join(' ');
  }
  const unit =
    abs >= SECOND
      ? { size: SECOND, digits: 9, name: 'с' }
      : abs >= MS
        ? { size: MS, digits: 6, name: 'мс' }
        : abs >= US
          ? { size: US, digits: 3, name: 'мкс' }
          : { size: NS, digits: 0, name: 'нс' };
  const whole = abs / unit.size;
  const remainder = abs % unit.size;
  const decimals = remainder
    ? '.' + remainder.toString().padStart(unit.digits, '0').replace(/0+$/, '')
    : '';
  return `${ns < 0n ? '-' : ''}${whole}${decimals} ${unit.name}`;
}
export const spanLabel = formatDuration;
export function calendarAt(ns: bigint, zone: string): DateTime | null {
  const iso = nsToISO(ns, zone);
  if (!iso) return null;
  const value = DateTime.fromISO(iso, { setZone: true }).setZone(zone);
  return value.isValid ? value : null;
}
export function instantLabel(ns: bigint, zone: string, detailed = false): string {
  const date = calendarAt(ns, zone);
  if (!date) return `${detailed ? formatOffset(ns) : formatDuration(ns)} от 1970`;
  const sub = ((ns % SECOND) + SECOND) % SECOND;
  return (
    date.setLocale('ru').toFormat(detailed ? 'd LLL yyyy, HH:mm:ss' : 'd LLL yyyy') +
    (detailed && sub ? `.${sub.toString().padStart(9, '0').replace(/0+$/, '')}` : '')
  );
}
/** Short offsets use the visible second, including epochs outside the calendar. */
export function timeReference(at: bigint, zone: string, nearby = false) {
  const local = calendarAt(at, zone);
  const reference = local || nearby ? at - (((at % SECOND) + SECOND) % SECOND) : 0n;
  return {
    anchorNs: reference.toString(),
    label: local || nearby ? instantLabel(reference, zone, true) : '1970 · точка отсчёта',
  };
}
export function periodLabel(view: TimelineViewport, zone: string): string {
  if (view.span < MINUTE) return instantLabel(view.center, zone, true);
  const bounds = viewportBounds(view);
  const first = calendarAt(bounds.start, zone),
    last = calendarAt(bounds.end, zone);
  if (!first || !last)
    return `${formatDuration(bounds.start)} — ${formatDuration(bounds.end)} · 1970`;
  if (view.span < 2n * DAY) return first.setLocale('ru').toFormat('d LLLL yyyy');
  if (view.span < 400n * DAY)
    return (
      first.setLocale('ru').toFormat('d LLL') + ' — ' + last.setLocale('ru').toFormat('d LLL yyyy')
    );
  return `${first.year} — ${last.year}`;
}
export function fitTimeline(entities: Entity[], fallbackNow: number): TimelineViewport {
  let first: bigint | null = null,
    last: bigint | null = null;
  let onlyPrecise = true;
  let preciseResolution = NS;
  const include = (at: bigint | null) => {
    if (at == null) return;
    first = first == null || at < first ? at : first;
    last = last == null || at > last ? at : last;
  };
  const calendar = entities.filter((entity) => !entity.plan.precise && entity.recurrence);
  if (calendar.length) {
    const fitted = calendarFit(calendar, fallbackNow),
      bounds = { start: fitted.center - fitted.span / 2, end: fitted.center + fitted.span / 2 };
    include(fromMillis(bounds.start));
    include(fromMillis(bounds.end));
  }
  for (const entity of entities) {
    for (const range of [entity.plan, entity.baseline, entity.actual, entity.forecast]) {
      if (!range) continue;
      const coordinates = canonicalRange(range);
      if (hasTime(range) && !range.precise) onlyPrecise = false;
      if (hasTime(range) && range.precise?.resolutionNs)
        preciseResolution = maxNs(preciseResolution, BigInt(range.precise.resolutionNs));
      for (const at of [
        coordinates.start,
        coordinates.end,
        coordinates.earliest,
        coordinates.latest,
      ])
        include(at);
    }
    const due = isoToNs(entity.dueAt, entity.plan.timezone);
    if (due != null) {
      include(due);
      onlyPrecise = false;
    }
  }
  const start = first as bigint | null,
    end = last as bigint | null;
  if (start == null || end == null) return { center: fromMillis(fallbackNow), span: 14n * DAY };
  const extent = end - start;
  const minimum = onlyPrecise ? preciseResolution * 20n : DAY;
  return normalizeViewport({
    center: start + extent / 2n,
    span: maxNs(minimum, extent + extent / 6n),
  });
}
export function focusRange(range: TimeRange, previous: TimelineViewport): TimelineViewport {
  const value = canonicalRange(range),
    start = value.start ?? value.end;
  if (start == null) return previous;
  const end = value.end ?? start;
  const resolution = range.precise?.resolutionNs
    ? BigInt(range.precise.resolutionNs)
    : range.precise
      ? NS
      : range.precision === 'day'
        ? DAY
        : range.precision === 'month'
          ? 31n * DAY
          : hasSubMillisecondTime(range)
            ? NS
            : [value.start, value.end].some((at) => at != null && at % SECOND !== 0n)
              ? MS
              : SECOND;
  const extent = end - start;
  return normalizeViewport({
    center: start + extent / 2n,
    span: maxNs(extent + extent / 3n, resolution * 20n),
  });
}
const STEPS = [
  ...[NS, US, MS].flatMap((size) =>
    [1n, 2n, 5n, 10n, 20n, 50n, 100n, 200n, 500n].map((step) => size * step),
  ),
  ...[SECOND, MINUTE].flatMap((size) => [1n, 2n, 5n, 10n, 15n, 30n].map((step) => size * step)),
  ...[HOUR, DAY].flatMap((size) => [1n, 2n, 5n, 10n].map((step) => size * step)),
  ...Array.from({ length: 10 }, (_, index) =>
    [1n, 2n, 5n].map((step) => YEAR * 10n ** BigInt(index) * step),
  ).flat(),
].sort((a, b) => (a < b ? -1 : 1));
function floorDivision(value: bigint, divisor: bigint) {
  const result = value / divisor;
  return value < 0n && value % divisor ? result - 1n : result;
}
export function timelineTicks(view: TimelineViewport, width: number, zone: string): TimelineTick[] {
  if (!(width > 0)) return [];
  const bounds = viewportBounds(view);
  if (
    view.span >= MINUTE &&
    bounds.start >= fromMillis(TIMELINE_START) &&
    bounds.end <= fromMillis(TIMELINE_END)
  ) {
    return calendarTicks(
      { center: Number(view.center / MS), span: Number(view.span / MS) },
      width,
      zone,
    ).map((tick) => ({ ...tick, at: fromMillis(tick.at) }));
  }
  const count = BigInt(Math.max(2, Math.min(24, Math.floor(width / 100))));
  const step = STEPS.find((value) => value >= view.span / count) ?? STEPS.at(-1)!;
  const base = view.span < MINUTE ? floorDivision(view.center, SECOND) * SECOND : 0n;
  const context = view.span < MINUTE ? instantLabel(base, zone, true) : 'Отсчёт от 1970';
  let at = floorDivision(bounds.start, step) * step;
  const ticks: TimelineTick[] = [];
  for (let i = 0; i < 48 && at <= bounds.end; i++, at += step) {
    if (at < bounds.start) continue;
    const label = view.span < MINUTE ? formatDuration(at - base) : formatDuration(at);
    ticks.push({ at, label, major: at === 0n, ...(ticks.length === 0 ? { context } : {}) });
  }
  return ticks;
}
