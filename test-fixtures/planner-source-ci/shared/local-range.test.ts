import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  changeRangePrecision,
  displayRangeEnd,
  localRangeInput,
  withLocalRangeInput,
} from './local-range.js';
import { isoToNs, preciseRangeErrors } from './precise-time.js';
import type { TimeRange } from './types.js';
import {
  calendarRangeRepresentable,
  withCalendarCoordinates,
  withPreciseCoordinates,
} from '../src/utils.js';
const unknown = (timezone = 'Asia/Yekaterinburg'): TimeRange => ({
  start: null,
  end: null,
  timezone,
  precision: 'unknown',
});

test('inclusive whole-day holiday round trips without changing its last calendar day', () => {
  const range = withLocalRangeInput(
    withLocalRangeInput(unknown(), 'start', '2027-06-05', true),
    'end',
    '2027-06-10',
    true,
  );
  assert.equal(range.precision, 'day');
  assert.equal(range.start, '2027-06-05T00:00:00.000+05:00');
  assert.equal(range.end, '2027-06-11T00:00:00.000+05:00');
  assert.equal(localRangeInput(range, 'end', true), '2027-06-10');
  assert.equal(displayRangeEnd(range), '2027-06-10T00:00:00.000+05:00');
  assert.equal(
    localRangeInput(changeRangePrecision(range, false), 'end', false),
    '2027-06-11T00:00',
  );
  assert.deepEqual(changeRangePrecision(changeRangePrecision(range, false), true), range);
});

test('whole-day ends use calendar arithmetic through DST rather than a 24-hour subtraction', () => {
  const range = withLocalRangeInput(
    withLocalRangeInput(unknown('America/New_York'), 'start', '2027-03-13', true),
    'end',
    '2027-03-14',
    true,
  );
  assert.equal(Date.parse(range.end!) - Date.parse(range.start!), 47 * 3600000);
  assert.equal(localRangeInput(range, 'end', true), '2027-03-14');
  assert.match(displayRangeEnd(range)!, /2027-03-14T00:00:00.000-05:00/);
});

test('unknown ranges stay unknown, open ends remain open, and date removal never creates time', () => {
  assert.deepEqual(changeRangePrecision(unknown(), true), unknown());
  const range = withLocalRangeInput(unknown(), 'start', '2027-06-05', true);
  assert.equal(range.end, null);
  assert.deepEqual(withLocalRangeInput(range, 'start', '', true), unknown());
});

test('missing local clocks and nonexistent local calendar dates are rejected before save', () => {
  assert.throws(
    () => withLocalRangeInput(unknown('America/New_York'), 'start', '2027-03-14T02:30', false),
    /местного времени/,
  );
  assert.throws(
    () => withLocalRangeInput(unknown('Pacific/Apia'), 'start', '2011-12-30', true),
    /местного времени/,
  );
  const valid = withLocalRangeInput(
    unknown('America/New_York'),
    'start',
    '2027-03-14T03:30',
    false,
  );
  assert.match(valid.start!, /-04:00$/);
});

test('midday day-precision ends retain their stored final calendar day', () => {
  const range: TimeRange = {
    start: '2027-06-05T00:00:00+05:00',
    end: '2027-06-10T12:00:00+05:00',
    timezone: 'Asia/Yekaterinburg',
    precision: 'day',
  };
  assert.equal(displayRangeEnd(range), range.end);
  assert.equal(localRangeInput(range, 'end', true), '2027-06-10');
});

test('exact local values show seconds and fractions only when needed and round trip nanoseconds', () => {
  let range = withLocalRangeInput(unknown(), 'start', '2027-06-05T10:11', false);
  assert.equal(localRangeInput(range, 'start', false), '2027-06-05T10:11');
  range = withLocalRangeInput(range, 'start', '2027-06-05T10:11:12', false);
  assert.equal(localRangeInput(range, 'start', false), '2027-06-05T10:11:12');
  range = withLocalRangeInput(range, 'start', '2027-06-05T10:11:12.020000001', false);
  assert.equal(localRangeInput(range, 'start', false), '2027-06-05T10:11:12.020000001');
  assert.equal(range.start, '2027-06-05T10:11:12.020000001+05:00');
});

test('an exact period end with milliseconds remains visible and unchanged on edit', () => {
  const range = withLocalRangeInput(
    withLocalRangeInput(unknown(), 'start', '2027-06-05T10:11:12', false),
    'end',
    '2027-06-05T10:11:12.020',
    false,
  );
  assert.equal(localRangeInput(range, 'end', false), '2027-06-05T10:11:12.02');
  assert.equal(range.end, '2027-06-05T10:11:12.020000000+05:00');
});

test('editing a period start preserves its end and reports reversed bounds without clearing them', () => {
  const initial = withLocalRangeInput(
    withLocalRangeInput(unknown(), 'start', '2027-06-05T10:00', false),
    'end',
    '2027-06-05T11:00',
    false,
  );
  const movedStart = withLocalRangeInput(initial, 'start', '2027-06-05T10:30', false);
  assert.equal(movedStart.end, initial.end);
  assert.equal(preciseRangeErrors(movedStart).length, 0);
  const reversed = withLocalRangeInput(movedStart, 'start', '2027-06-05T12:00', false);
  assert.equal(reversed.end, initial.end);
  assert.ok(
    preciseRangeErrors(reversed).some((error) => error.includes('Окончание раньше начала')),
  );
});

test('calendar ISO9 view preserves all four edges without storing precise coordinates', () => {
  const range: TimeRange = {
    start: '2026-10-03T07:00:00.000000001Z',
    end: '2026-10-03T07:00:00.020000001Z',
    earliest: '2026-10-03T06:59:59.999999999Z',
    latest: '2026-10-03T07:00:00.030000001Z',
    timezone: 'Asia/Yekaterinburg',
    precision: 'approximate',
  };
  const view = withPreciseCoordinates(range);
  assert.equal(view.precise?.start, isoToNs(range.start, range.timezone)?.toString());
  assert.equal(view.precise?.end, isoToNs(range.end, range.timezone)?.toString());
  assert.equal(view.precise?.earliest, isoToNs(range.earliest, range.timezone)?.toString());
  assert.equal(view.precise?.latest, isoToNs(range.latest, range.timezone)?.toString());

  const saved = withCalendarCoordinates(view);
  assert.equal(saved.precise, undefined);
  assert.equal(saved.precision, 'approximate');
  for (const edge of ['start', 'end', 'earliest', 'latest'] as const)
    assert.equal(isoToNs(saved[edge], saved.timezone), isoToNs(range[edge], range.timezone));
  assert.equal(calendarRangeRepresentable(view), true);
  assert.deepEqual(preciseRangeErrors(saved), []);
});

test('calendar-only precise view rejects coordinates beyond the ISO calendar range', () => {
  const range: TimeRange = {
    start: '9999-12-31T23:59:59.999999999Z',
    end: null,
    timezone: 'UTC',
    precision: 'exact',
  };
  const view = withPreciseCoordinates(range);
  view.precise!.start = (BigInt(view.precise!.start!) + 1_000_000_000n).toString();
  assert.equal(calendarRangeRepresentable(view), false);
  assert.throws(() => withCalendarCoordinates(view), /диапазоне лет 0001–9999/);
});
