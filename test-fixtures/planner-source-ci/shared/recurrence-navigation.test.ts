import test from 'node:test';
import assert from 'node:assert/strict';
import { DateTime } from 'luxon';
import { recurrenceNavigation } from './recurrence-navigation.js';
import type { Entity, Recurrence, TimeRange } from './types.js';
import { isoToNs } from './precise-time.js';

function series(start: string, rule: Recurrence, end: string | null = null, zone = 'UTC'): Entity {
  const plan: TimeRange = { start, end, timezone: zone, precision: 'exact' };
  return {
    id: 'series',
    workspaceId: 'personal',
    typeId: 'period',
    kind: end ? 'period' : 'point',
    title: 'Series',
    description: '',
    parentId: null,
    ownerId: null,
    participantIds: [],
    status: 'planned',
    plan,
    baseline: { ...plan },
    actual: null,
    forecast: null,
    dueAt: null,
    tags: [],
    fields: {},
    links: [],
    allocations: [],
    recurrence: rule,
    source: { kind: 'manual', label: '', observedAt: start, receivedAt: start },
    createdAt: start,
    updatedAt: start,
    version: 1,
  };
}
const day = (count?: number): Recurrence => ({
  frequency: 'day',
  interval: 1,
  exceptions: [],
  count,
});
const month = (policy: 'adjust' | 'skip-invalid'): Recurrence => ({
  frequency: 'month',
  interval: 1,
  count: 3,
  exceptions: [],
  calendarPolicy: policy,
});
const date = (value: string | null | undefined) => value?.slice(0, 10);

test('adjacent ISO9 occurrences preserve both recorded edges of a 20 ms period', () => {
  const entity = series('2026-10-03T12:00:00.000000001Z', day(3), '2026-10-03T12:00:00.020000001Z');
  const before = structuredClone(entity),
    first = recurrenceNavigation(entity);
  const second = recurrenceNavigation(entity, first.next.occurrence!.start);
  assert.equal(second.current.occurrence?.start, '2026-10-04T12:00:00.000000001Z');
  assert.equal(second.current.occurrence?.end, '2026-10-04T12:00:00.020000001Z');
  assert.equal(second.previous.occurrence?.start, entity.plan.start);
  assert.equal(second.previous.occurrence?.end, entity.plan.end);
  assert.equal(second.next.occurrence?.start, '2026-10-05T12:00:00.000000001Z');
  assert.equal(second.next.occurrence?.end, '2026-10-05T12:00:00.020000001Z');
  assert.equal(
    isoToNs(second.current.occurrence!.end)! - isoToNs(second.current.occurrence!.start)!,
    20000000n,
  );
  assert.deepEqual(entity, before);
});

test('a one-nanosecond period stays nonzero when navigating and wrong same-ms starts are refused', () => {
  const entity = series('2026-10-03T12:00:00.000000001Z', day(3), '2026-10-03T12:00:00.000000002Z');
  const first = recurrenceNavigation(entity),
    second = recurrenceNavigation(entity, first.next.occurrence!.start);
  for (const result of [second.previous, second.current, second.next])
    assert.equal(isoToNs(result.occurrence!.end)! - isoToNs(result.occurrence!.start)!, 1n);
  assert.equal(second.current.occurrence?.end, '2026-10-04T12:00:00.000000002Z');
  assert.equal(
    recurrenceNavigation(entity, '2026-10-04T12:00:00.000000002Z').current.status,
    'invalid',
  );
});

test('ISO9 navigation retains calendar and elapsed duration policies through DST', () => {
  const entity = series(
    '2026-03-28T12:00:00.000000001+01:00',
    day(3),
    '2026-03-29T12:00:00.000000003+02:00',
    'Europe/Berlin',
  );
  const calendar = recurrenceNavigation(entity).next.occurrence!;
  const elapsed = recurrenceNavigation({
    ...entity,
    recurrence: { ...entity.recurrence!, durationPolicy: 'elapsed' },
  }).next.occurrence!;
  assert.equal(calendar.start, '2026-03-29T12:00:00.000000001+02:00');
  assert.equal(calendar.end, '2026-03-30T12:00:00.000000003+02:00');
  assert.equal(elapsed.start, calendar.start);
  assert.equal(elapsed.end, '2026-03-30T11:00:00.000000003+02:00');
  const selected = recurrenceNavigation(entity, calendar.start);
  assert.equal(selected.current.occurrence?.end, calendar.end);
  assert.equal(selected.previous.occurrence?.end, entity.plan.end);
});

test('adjacent dates stop at COUNT and preserve the original series', () => {
  const entity = series('2026-01-01T09:00:00Z', day(3), '2026-01-01T09:45:00Z');
  const before = structuredClone(entity),
    first = recurrenceNavigation(entity);
  assert.equal(first.current.occurrence?.index, 0);
  assert.equal(first.previous.status, 'end');
  assert.equal(date(first.next.occurrence?.start), '2026-01-02');
  const last = recurrenceNavigation(entity, '2026-01-03T09:00:00Z');
  assert.equal(date(last.previous.occurrence?.start), '2026-01-02');
  assert.equal(last.next.status, 'end');
  assert.equal(last.current.occurrence?.end, '2026-01-03T09:45:00.000Z');
  assert.deepEqual(entity, before);
});

test('date and instant exceptions consume COUNT without inventing a replacement', () => {
  const entity = series('2026-01-01T09:00:00Z', {
    ...day(4),
    exceptions: ['2026-01-02', '2026-01-03T10:00:00+01:00'],
  });
  const first = recurrenceNavigation(entity);
  assert.equal(date(first.next.occurrence?.start), '2026-01-04');
  assert.equal(first.next.occurrence?.index, 3);
  const last = recurrenceNavigation(entity, first.next.occurrence!.start);
  assert.equal(date(last.previous.occurrence?.start), '2026-01-01');
  assert.equal(last.next.status, 'end');
  assert.equal(recurrenceNavigation(entity, '2026-01-02T09:00:00Z').current.status, 'invalid');
});

test('excluded initial dates and weekly weekday rules locate the first real instance', () => {
  const entity = series(
    '2026-10-03T19:00:00+05:00',
    { frequency: 'week', interval: 1, weekdays: [2, 4], count: 4, exceptions: ['2026-10-06'] },
    null,
    'Asia/Yekaterinburg',
  );
  const first = recurrenceNavigation(entity);
  assert.equal(date(first.current.occurrence?.start), '2026-10-08');
  assert.equal(first.current.occurrence?.index, 1);
  assert.equal(first.previous.status, 'end');
  assert.equal(date(first.next.occurrence?.start), '2026-10-13');
  const last = recurrenceNavigation(entity, '2026-10-15T19:00:00+05:00');
  assert.equal(date(last.previous.occurrence?.start), '2026-10-13');
  assert.equal(last.next.status, 'end');
});

test('monthly adjustment retains day31 while skip-invalid does not consume invalid months', () => {
  const adjusted = series('2026-01-31T09:00:00Z', month('adjust'));
  const skipped = series('2026-01-31T09:00:00Z', month('skip-invalid'));
  assert.equal(date(recurrenceNavigation(adjusted).next.occurrence?.start), '2026-02-28');
  assert.equal(
    date(recurrenceNavigation(adjusted, '2026-02-28T09:00:00Z').next.occurrence?.start),
    '2026-03-31',
  );
  const first = recurrenceNavigation(skipped);
  assert.equal(date(first.next.occurrence?.start), '2026-03-31');
  assert.equal(first.next.occurrence?.index, 1);
  const third = recurrenceNavigation(skipped, '2026-05-31T09:00:00Z');
  assert.equal(date(third.previous.occurrence?.start), '2026-03-31');
  assert.equal(third.current.occurrence?.index, 2);
  assert.equal(third.next.status, 'end');
});

test('UNTIL is inclusive for exact instants and whole local dates', () => {
  for (const until of ['2026-01-03', '2026-01-03T09:00:00Z']) {
    const entity = series('2026-01-01T09:00:00Z', { ...day(), until });
    const last = recurrenceNavigation(entity, '2026-01-03T09:00:00Z');
    assert.equal(last.current.status, 'found');
    assert.equal(last.next.status, 'end');
  }
  const excluded = series('2026-01-01T09:00:00Z', {
    ...day(),
    until: '2026-01-03',
    exceptions: ['2026-01-03'],
  });
  assert.equal(recurrenceNavigation(excluded, '2026-01-02T09:00:00Z').next.status, 'end');
});

test('spring DST missing time follows each policy and preserves COUNT', () => {
  const entity = series(
    '2026-03-28T02:30:00+01:00',
    { ...day(3), calendarPolicy: 'skip-invalid' },
    '2026-03-28T03:15:00+01:00',
    'Europe/Berlin',
  );
  const next = recurrenceNavigation(entity).next.occurrence!;
  assert.equal(next.start, '2026-03-30T02:30:00.000+02:00');
  assert.equal(next.end, '2026-03-30T03:15:00.000+02:00');
  assert.equal(next.index, 1);
  assert.equal(
    date(recurrenceNavigation(entity, next.start).previous.occurrence?.start),
    '2026-03-28',
  );
  const adjusted = recurrenceNavigation({
    ...entity,
    recurrence: { ...entity.recurrence!, calendarPolicy: 'adjust' },
  }).next.occurrence!;
  assert.equal(adjusted.start, '2026-03-29T03:30:00.000+02:00');
});

test('fall DST chooses the first fold and reports the real interval offsets', () => {
  const entity = series(
    '2026-10-24T02:30:00+02:00',
    { ...day(3), calendarPolicy: 'skip-invalid', until: '2026-10-25T02:30:00+02:00' },
    '2026-10-24T03:15:00+02:00',
    'Europe/Berlin',
  );
  const next = recurrenceNavigation(entity).next.occurrence!;
  assert.equal(next.start, '2026-10-25T02:30:00.000+02:00');
  assert.equal(next.end, '2026-10-25T02:15:00.000+01:00');
  assert.equal(recurrenceNavigation(entity, next.start).next.status, 'end');
});

test('calendar and elapsed durations remain distinct across DST', () => {
  const entity = series(
    '2026-03-28T12:00:00+01:00',
    day(3),
    '2026-03-29T12:00:00+02:00',
    'Europe/Berlin',
  );
  const calendar = recurrenceNavigation(entity).next.occurrence!;
  const elapsed = recurrenceNavigation({
    ...entity,
    recurrence: { ...entity.recurrence!, durationPolicy: 'elapsed' },
  }).next.occurrence!;
  assert.equal(calendar.end, '2026-03-30T12:00:00.000+02:00');
  assert.equal(elapsed.end, '2026-03-30T11:00:00.000+02:00');
});

test('long overlapping periods do not hide the adjacent start behind the expansion cap', () => {
  const entity = series('2000-01-01T00:00:00Z', day(5000), '2010-01-01T00:00:00Z');
  const neighbors = recurrenceNavigation(entity, '2006-01-01T00:00:00Z');
  assert.equal(date(neighbors.previous.occurrence?.start), '2005-12-31');
  assert.equal(date(neighbors.next.occurrence?.start), '2006-01-02');
  assert.equal(date(neighbors.current.occurrence?.end), '2016-01-02');
});

test('native distant series jump directly while skip-invalid work limits stay explicit', () => {
  const entity = series('2000-01-01T09:00:00Z', day(100000));
  const distant = recurrenceNavigation(entity, '2200-01-01T09:00:00Z');
  assert.equal(date(distant.previous.occurrence?.start), '2199-12-31');
  assert.equal(date(distant.next.occurrence?.start), '2200-01-02');
  const skipped = series('2000-01-31T09:00:00Z', { ...month('skip-invalid'), count: 100000 });
  const limited = recurrenceNavigation(skipped, '2500-01-31T09:00:00Z');
  assert.equal(limited.current.status, 'limited');
  assert.equal(limited.current.occurrence, null);
});

test('a large exception gap yields a bounded-search notice rather than a fabricated date', () => {
  const base = DateTime.utc(2000, 1, 1, 9);
  const entity = series(base.toISO()!, {
    ...day(10000),
    exceptions: Array.from({ length: 5000 }, (_, i) => base.plus({ days: i }).toISODate()!),
  });
  const navigation = recurrenceNavigation(entity);
  assert.equal(navigation.current.status, 'limited');
  assert.equal(navigation.next.occurrence, null);
});

test('distant skip-invalid neighbors share one bounded engine window', () => {
  const entity = series('2000-01-31T09:00:00Z', { ...month('skip-invalid'), count: 100000 });
  const navigation = recurrenceNavigation(entity, '2200-01-31T09:00:00Z');
  assert.equal(navigation.current.status, 'found');
  assert.equal(date(navigation.previous.occurrence?.start), '2199-12-31');
  assert.equal(date(navigation.next.occurrence?.start), '2200-03-31');
});

test('all excluded finite instances report an empty series and malformed rules stay explicit', () => {
  const empty = series('2026-01-01T09:00:00Z', {
    ...day(2),
    exceptions: ['2026-01-01', '2026-01-02'],
  });
  assert.equal(recurrenceNavigation(empty).current.status, 'end');
  assert.equal(
    recurrenceNavigation({ ...empty, recurrence: { ...day(2), interval: 0 } }).current.status,
    'invalid',
  );
});
