import { DateTime } from 'luxon';
import type { PreciseTimeRange, TimeRange } from './types.js';

export const NS_PER_MILLISECOND = 1_000_000n;
export const NS_PER_SECOND = 1_000_000_000n;
export const NS_PER_MINUTE = 60n * NS_PER_SECOND;
export const MAX_NS_DECIMAL_LENGTH = 40;
const DECIMAL = /^(?:0|[1-9]\d*|-[1-9]\d*)$/;
const EDGES = ['start', 'end', 'earliest', 'latest'] as const;
export type TimeEdge = (typeof EDGES)[number];
export interface CanonicalTimeRange {
  start: bigint | null;
  end: bigint | null;
  earliest: bigint | null;
  latest: bigint | null;
}

/** Refuses floats, exponent notation and oversized input before allocating a BigInt. */
export function parseNs(value: unknown): bigint | null {
  if (typeof value !== 'string' || value.length > MAX_NS_DECIMAL_LENGTH || !DECIMAL.test(value))
    return null;
  return BigInt(value);
}

function floorDiv(value: bigint, divisor: bigint): bigint {
  const quotient = value / divisor;
  return value % divisor < 0n ? quotient - 1n : quotient;
}

/** Calendar input uses IANA zones and preserves all nine fractional second digits. */
export function isoToNs(value: string | null | undefined, timezone = 'UTC'): bigint | null {
  if (!value || typeof value !== 'string' || value.length > 100) return null;
  const fractional = value.match(
    /T\d{2}:?\d{2}:?\d{2}[.,](\d+)(?:Z|[+-]\d{2}(?::?\d{2})?)?$/i,
  )?.[1];
  if (fractional && fractional.length > 9) return null;
  let date = DateTime.fromISO(value, { zone: timezone, setZone: true });
  if (!date.isValid || date.year < 1 || date.year > 9999) return null;
  const hasOffset = /(?:Z|[+-]\d{2}(?::?\d{2})?)$/i.test(value) && value.includes('T');
  if (!hasOffset && value.includes('T')) {
    const wall = value.match(/T(\d{2}):?(\d{2})(?::?(\d{2}))?/);
    // A nonexistent local clock time is unknown input, rather than the later normalized hour.
    if (
      wall &&
      Number(wall[1]) < 24 &&
      (date.hour !== Number(wall[1]) ||
        date.minute !== Number(wall[2]) ||
        date.second !== Number(wall[3] ?? 0))
    )
      return null;
    // A folded clock input uses the first occurrence consistently; explicit offsets select either.
    date = date.getPossibleOffsets().sort((a, b) => a.toMillis() - b.toMillis())[0] ?? date;
  }
  const millis = date.toMillis();
  if (!Number.isSafeInteger(millis)) return null;
  const remainingNs = fractional ? BigInt(fractional.padEnd(9, '0').slice(3)) : 0n;
  return BigInt(millis) * NS_PER_MILLISECOND + remainingNs;
}

const MIN_CALENDAR_NS = isoToNs('0001-01-01T00:00:00Z')!;
const MAX_CALENDAR_NS = isoToNs('9999-12-31T23:59:59.999999999Z')!;

/** Calendar mirrors are optional outside years 0001–9999; no date is fabricated. */
export function nsToISO(value: bigint, timezone = 'UTC'): string | null {
  if (typeof value !== 'bigint' || value < MIN_CALENDAR_NS || value > MAX_CALENDAR_NS) return null;
  const milliseconds = floorDiv(value, NS_PER_MILLISECOND);
  let date = DateTime.fromMillis(Number(milliseconds), { zone: timezone });
  if (!date.isValid) return null;
  // Historical IANA offsets may include seconds; ISO minute offsets cannot encode them exactly.
  if (date.year < 1 || date.year > 9999 || !Number.isInteger(date.offset)) date = date.toUTC();
  const iso = date.toISO();
  if (!iso) return null;
  const fractional = value - floorDiv(value, NS_PER_SECOND) * NS_PER_SECOND;
  return iso.replace(/\.\d{3}(?=Z|[+-]\d{2}:\d{2}$)/, `.${fractional.toString().padStart(9, '0')}`);
}

/** Read-only coordinate projection. Invalid values remain null and are validated separately. */
export function canonicalRange(range: TimeRange): CanonicalTimeRange {
  const coordinate = (edge: TimeEdge): bigint | null => {
    if (range.precise && (edge === 'start' || edge === 'end' || range.precise[edge] !== undefined))
      return parseNs(range.precise[edge]);
    return isoToNs(range[edge], range.timezone);
  };
  return {
    start: coordinate('start'),
    end: coordinate('end'),
    earliest: coordinate('earliest'),
    latest: coordinate('latest'),
  };
}

export function hasTime(range: TimeRange | null | undefined): boolean {
  if (!range) return false;
  const coordinates = canonicalRange(range);
  return coordinates.start !== null || coordinates.end !== null;
}

/** Includes declared ISO fractional precision even when the nanosecond residue is zero. */
export function hasSubMillisecondTime(range: TimeRange | null | undefined): boolean {
  return (
    !!range &&
    EDGES.some((edge) => {
      const value = range[edge];
      if (typeof value === 'string' && /T.*[.,]\d{4,}/i.test(value)) return true;
      const coordinate = range.precise ? parseNs(range.precise[edge]) : null;
      return coordinate !== null && coordinate % NS_PER_MILLISECOND !== 0n;
    })
  );
}

/** Validates exact coordinates, ordering and consistency with optional ISO mirrors. */
export function preciseRangeErrors(range: TimeRange): string[] {
  const errors: string[] = [];
  for (const edge of EDGES)
    if (range[edge] != null && isoToNs(range[edge], range.timezone) === null)
      errors.push(
        `Календарное значение ${edge}: нужна существующая дата 0001–9999 с точностью до наносекунд и корректным часовым поясом.`,
      );
  const precise = range.precise;
  if (precise) {
    if (precise.scale !== 'unix-nanoseconds') errors.push('Неизвестная система точного времени.');
    for (const edge of EDGES) {
      const raw = precise[edge];
      if ((edge === 'start' || edge === 'end') && raw === undefined)
        errors.push(`Точная координата ${edge} должна быть числом наносекунд или null.`);
      else if (raw != null && parseNs(raw) === null)
        errors.push(`Точная координата ${edge}: нужно целое десятичное число наносекунд.`);
      if (range[edge] != null && (edge === 'start' || edge === 'end' || raw !== undefined)) {
        const calendar = isoToNs(range[edge], range.timezone);
        const exact = parseNs(raw);
        if (calendar === null || exact === null || calendar !== exact)
          errors.push(`Календарное значение ${edge} не совпадает с точной координатой.`);
      }
    }
    if (precise.resolutionNs !== undefined) {
      const resolution = parseNs(precise.resolutionNs);
      if (resolution === null || resolution <= 0n)
        errors.push('Разрешение времени должно быть положительным целым числом наносекунд.');
    }
  }
  const coordinates = canonicalRange(range);
  if (coordinates.start !== null && coordinates.end !== null && coordinates.end < coordinates.start)
    errors.push('Окончание раньше начала.');
  if (
    coordinates.earliest !== null &&
    coordinates.latest !== null &&
    coordinates.earliest > coordinates.latest
  )
    errors.push('Обратный диапазон неопределённости.');
  return errors;
}

/** Creates exact JSON coordinates and lossless calendar mirrors where available. */
export function rangeFromNs(
  start: bigint | null,
  end: bigint | null,
  timezone = 'UTC',
  precision: TimeRange['precision'] = 'exact',
): TimeRange {
  return {
    start: start === null ? null : nsToISO(start, timezone),
    end: end === null ? null : nsToISO(end, timezone),
    timezone,
    precision: start === null && end === null ? 'unknown' : precision,
    precise: {
      scale: 'unix-nanoseconds',
      start: start?.toString() ?? null,
      end: end?.toString() ?? null,
    },
  };
}

/** Elapsed coordinate movement, explicitly distinct from calendar/DST day movement. */
export function shiftRangeNs(range: TimeRange, delta: bigint): TimeRange {
  const coordinates = canonicalRange(range);
  const moved = (edge: TimeEdge) =>
    coordinates[edge] === null ? null : coordinates[edge]! + delta;
  const next = rangeFromNs(moved('start'), moved('end'), range.timezone, range.precision);
  next.precision = range.precision;
  for (const edge of ['earliest', 'latest'] as const)
    if (range[edge] !== undefined || range.precise?.[edge] !== undefined) {
      const value = moved(edge);
      next[edge] = value === null ? null : nsToISO(value, range.timezone);
      next.precise![edge] = value?.toString() ?? null;
    }
  if (range.precise?.resolutionNs !== undefined)
    next.precise!.resolutionNs = range.precise.resolutionNs;
  return next;
}

export function formatNs(value: bigint, timezone = 'UTC'): string {
  return nsToISO(value, timezone) ?? `${value.toString()} нс от 1970-01-01T00:00:00Z`;
}

export function formatPreciseRange(range: TimeRange): string {
  const { start, end } = canonicalRange(range);
  if (start === null && end === null) return 'Без даты';
  if (start === null) return `Окончание: ${formatNs(end!, range.timezone)}`;
  if (end === null || end === start) return formatNs(start, range.timezone);
  return `${formatNs(start, range.timezone)} — ${formatNs(end, range.timezone)}`;
}

export function sameTimeRange(a: TimeRange, b: TimeRange): boolean {
  const first = canonicalRange(a),
    second = canonicalRange(b);
  return (
    a.timezone === b.timezone &&
    a.precision === b.precision &&
    a.precise?.resolutionNs === b.precise?.resolutionNs &&
    EDGES.every((edge) => first[edge] === second[edge])
  );
}

/** Rebuilds mirrors after a precise endpoint update without altering other endpoints. */
export function withRangeEdgeNs(range: TimeRange, edge: TimeEdge, value: bigint | null): TimeRange {
  const coords = canonicalRange(range);
  const precise: PreciseTimeRange = range.precise
    ? { ...range.precise }
    : {
        scale: 'unix-nanoseconds',
        start: coords.start?.toString() ?? null,
        end: coords.end?.toString() ?? null,
      };
  precise[edge] = value?.toString() ?? null;
  return { ...range, [edge]: value === null ? null : nsToISO(value, range.timezone), precise };
}
