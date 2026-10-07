import { DateTime } from 'luxon';
import { isoToNs, nsToISO, NS_PER_SECOND } from './precise-time.js';
import type { TimeRange } from './types.js';

/** Whole-day ends are stored as the exclusive local calendar boundary. */
export function displayRangeEnd(range: TimeRange): string | null {
  if (!range.end || range.precision !== 'day') return range.end;
  const end = DateTime.fromISO(range.end, { zone: range.timezone });
  const start = range.start ? DateTime.fromISO(range.start, { zone: range.timezone }) : null;
  return end.isValid &&
    (!start?.isValid || end.toMillis() > start.toMillis()) &&
    end.toMillis() === end.startOf('day').toMillis()
    ? end.minus({ days: 1 }).toISO()
    : range.end;
}

export function localRangeInput(range: TimeRange, edge: 'start' | 'end', allDay: boolean): string {
  const value = allDay && edge === 'end' ? displayRangeEnd(range) : range[edge];
  if (!value) return '';
  const date = DateTime.fromISO(value, { zone: range.timezone });
  if (!date.isValid) return '';
  if (allDay) return date.toFormat('yyyy-MM-dd');
  const coordinate = isoToNs(value, range.timezone);
  const fraction =
    coordinate === null ? 0n : ((coordinate % NS_PER_SECOND) + NS_PER_SECOND) % NS_PER_SECOND;
  const showSeconds = date.second !== 0 || fraction !== 0n;
  const base = date.toFormat(`yyyy-MM-dd'T'HH:mm${showSeconds ? ':ss' : ''}`);
  return fraction ? `${base}.${fraction.toString().padStart(9, '0').replace(/0+$/, '')}` : base;
}

export function withLocalRangeInput(
  range: TimeRange,
  edge: 'start' | 'end',
  value: string,
  allDay: boolean,
): TimeRange {
  let timestamp: string | null = null;
  if (value) {
    const date = DateTime.fromISO(value, { zone: range.timezone });
    const wallValue = value.split('.')[0]!;
    const wallFormat = allDay
      ? 'yyyy-MM-dd'
      : `yyyy-MM-dd'T'HH:mm${wallValue.length >= 19 ? ':ss' : ''}`;
    if (!date.isValid || date.toFormat(wallFormat) !== wallValue)
      throw new Error('Такого местного времени нет. Выберите другую дату или время.');
    if (allDay) {
      timestamp = (
        edge === 'end' ? date.startOf('day').plus({ days: 1 }) : date.startOf('day')
      ).toISO();
    } else {
      const coordinate = isoToNs(value, range.timezone);
      timestamp = coordinate === null ? null : nsToISO(coordinate, range.timezone);
      if (timestamp === null)
        throw new Error('Такого местного времени нет. Выберите другую дату или время.');
    }
  }
  const next = { ...range, [edge]: timestamp };
  return { ...next, precision: next.start || next.end ? (allDay ? 'day' : 'exact') : 'unknown' };
}

export function changeRangePrecision(range: TimeRange, allDay: boolean): TimeRange {
  if (!allDay) return { ...range, precision: range.start || range.end ? 'exact' : 'unknown' };
  const next = { ...range, start: null, end: null };
  return withLocalRangeInput(
    withLocalRangeInput(next, 'start', localRangeInput(range, 'start', true), true),
    'end',
    localRangeInput({ ...range, precision: 'day' }, 'end', true),
    true,
  );
}
