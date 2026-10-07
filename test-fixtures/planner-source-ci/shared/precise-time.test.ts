import test from 'node:test';
import assert from 'node:assert/strict';
import {
  canonicalRange,
  formatPreciseRange,
  hasTime,
  isoToNs,
  nsToISO,
  parseNs,
  preciseRangeErrors,
  rangeFromNs,
  sameTimeRange,
  shiftRangeNs,
} from './precise-time.js';
import { range as rangeSchema } from '../server/validation.ts';

test('ISO conversion preserves nine digits, offsets and negative epoch coordinates', () => {
  assert.equal(isoToNs('1970-01-01T00:00:00.000000001Z'), 1n);
  assert.equal(isoToNs('1969-12-31T23:59:59.999999999Z'), -1n);
  assert.equal(isoToNs('1970-01-01T05:00:00.000000001+05:00'), 1n);
  assert.equal(nsToISO(-1n), '1969-12-31T23:59:59.999999999Z');
  assert.equal(nsToISO(1n, 'Asia/Yekaterinburg'), '1970-01-01T05:00:00.000000001+05:00');
  assert.equal(isoToNs('2026-10-03T12:00:00.1234567890Z'), null);
});

test('coordinate parsing refuses lossy JSON numbers, exponents and unbounded allocation', () => {
  for (const invalid of [1, '1e9', '1.0', '+1', '01', '-0', '', '1'.repeat(41)])
    assert.equal(parseNs(invalid), null);
  assert.equal(parseNs('-31556952000000000000000001'), -31556952000000000000000001n);
});

test('billion-year positions remain exact and never fabricate a calendar mirror', () => {
  const epoch = -31556952000000000000000000n;
  const range = rangeFromNs(epoch, epoch + 1n);
  assert.equal(range.start, null);
  assert.equal(range.end, null);
  assert.deepEqual(canonicalRange(range), {
    start: epoch,
    end: epoch + 1n,
    earliest: null,
    latest: null,
  });
  assert.equal(hasTime(range), true);
  assert.equal(nsToISO(epoch), null);
  assert.match(formatPreciseRange(range), /1970-01-01T00:00:00Z/);
  assert.deepEqual(rangeSchema.parse(range), range);
  assert.equal(JSON.parse(JSON.stringify(range)).precise.end, (epoch + 1n).toString());
});

test('schema rejects contradictory mirrors and nanosecond reversed ranges', () => {
  const range = rangeFromNs(1n, 2n);
  const contradictory = { ...range, start: '1970-01-01T00:00:00.000Z' };
  assert.equal(rangeSchema.safeParse(contradictory).success, false);
  assert.ok(preciseRangeErrors(contradictory).some((error) => error.includes('не совпадает')));
  assert.equal(rangeSchema.safeParse(rangeFromNs(2n, 1n)).success, false);
  assert.equal(
    rangeSchema.safeParse({ ...range, precise: { ...range.precise!, resolutionNs: '0' } }).success,
    false,
  );
});

test('elapsed shifts preserve nanosecond uncertainty bounds and measurement resolution', () => {
  const range = rangeFromNs(1001n, 2001n, 'UTC', 'approximate');
  range.earliest = nsToISO(900n);
  range.latest = nsToISO(2100n);
  Object.assign(range.precise!, { earliest: '900', latest: '2100', resolutionNs: '100' });
  const before = structuredClone(range);
  const moved = shiftRangeNs(range, 10000000000000000000000000n);
  assert.equal(canonicalRange(moved).earliest, 10000000000000000000000900n);
  assert.equal(canonicalRange(moved).latest, 10000000000000000000002100n);
  assert.equal(moved.precise?.resolutionNs, '100');
  assert.equal(moved.precision, 'approximate');
  assert.deepEqual(range, before);
  assert.deepEqual(preciseRangeErrors(moved), []);
  const boundsOnly = {
    ...range,
    start: null,
    end: null,
    precise: { ...range.precise!, start: null, end: null },
  };
  assert.equal(shiftRangeNs(boundsOnly, 1n).precision, 'approximate');
});

test('IANA mirrors retain timezone offsets while elapsed movement crosses DST honestly', () => {
  const before = isoToNs('2026-03-28T12:00:00', 'Europe/Berlin')!;
  const moved = shiftRangeNs(rangeFromNs(before, null, 'Europe/Berlin'), 86400000000000n);
  assert.equal(moved.start, '2026-03-29T13:00:00.000000000+02:00');
  assert.equal(isoToNs('2026-03-29T12:00:00', 'Europe/Berlin')! - before, 82800000000000n);
});

test('historical IANA second offsets use a lossless UTC mirror instead of a rounded timezone', () => {
  const coordinate = isoToNs('1800-01-01T12:00:00.000000001Z')!;
  const mirror = nsToISO(coordinate, 'Europe/Paris');
  assert.equal(mirror, '1800-01-01T12:00:00.000000001Z');
  assert.equal(isoToNs(mirror, 'Europe/Paris'), coordinate);
  assert.deepEqual(preciseRangeErrors(rangeFromNs(coordinate, null, 'Europe/Paris')), []);
});

test('local DST gaps are refused and folds choose a deterministic first occurrence', () => {
  assert.equal(isoToNs('2026-03-29T02:30:00', 'Europe/Berlin'), null);
  assert.equal(
    isoToNs('2026-10-25T02:30:00', 'Europe/Berlin'),
    isoToNs('2026-10-25T02:30:00+02:00'),
  );
  assert.notEqual(isoToNs('2026-10-25T02:30:00+01:00'), isoToNs('2026-10-25T02:30:00+02:00'));
});

test('same-time comparison checks exact coordinates and resolution, not mirror spelling', () => {
  const range = rangeFromNs(1000n, 2000n);
  assert.equal(sameTimeRange(range, { ...range, start: '1970-01-01T01:00:00.000001+01:00' }), true);
  assert.equal(sameTimeRange(range, rangeFromNs(1001n, 2000n)), false);
  assert.equal(
    sameTimeRange(range, { ...range, precise: { ...range.precise!, resolutionNs: '10' } }),
    false,
  );
});
