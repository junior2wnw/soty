import test from 'node:test';
import assert from 'node:assert/strict';
import { DateTime } from 'luxon';
import {
  applyTemplate,
  computeResourceConflicts,
  deriveForecast,
  emptyRange,
  evaluateSignals,
  expandRecurrence,
  getEffectiveRange,
  MAX_RECURRENCE_OCCURRENCES,
  suggestFromText,
  validateDependency,
  validateEntity,
} from './engine.js';
import { createSeed, universalTemplates } from './seed.js';
import type { Dependency, Entity, PlannerSnapshot, TimeRange } from './types.js';

const NOW = '2026-10-02T06:00:00Z';
const r = (start: string | null, end: string | null = null, timezone = 'UTC'): TimeRange => ({
  start,
  end,
  timezone,
  precision: start ? 'exact' : 'unknown',
});
const ms = (value: string | null | undefined) =>
  value ? DateTime.fromISO(value).toMillis() : null;
function fixture(): PlannerSnapshot {
  const snapshot = createSeed(NOW);
  return {
    ...snapshot,
    entities: [],
    dependencies: [],
    resources: [],
    rules: [],
    signals: [],
    comments: [],
  };
}
function item(
  id: string,
  start: string | null,
  end: string | null = null,
  extra: Partial<Entity> = {},
): Entity {
  const plan = r(start, end);
  return {
    id,
    workspaceId: 'personal',
    typeId: end ? 'period' : 'event',
    kind: end ? 'period' : 'point',
    title: id,
    description: '',
    parentId: null,
    ownerId: 'local-owner',
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
    recurrence: null,
    source: { kind: 'manual', label: 'Ручной ввод', observedAt: NOW, receivedAt: NOW },
    createdAt: NOW,
    updatedAt: NOW,
    version: 1,
    ...extra,
  };
}
function dep(
  fromId: string,
  toId: string,
  kind: Dependency['kind'] = 'finish-start',
  lagMinutes = 0,
): Dependency {
  return {
    id: `${fromId}-${toId}-${kind}`,
    workspaceId: 'personal',
    fromId,
    toId,
    kind,
    lagMinutes,
  };
}
function crew(snapshot: PlannerSnapshot, capacity = 1, workingWeekdays = [1, 2, 3, 4, 5, 6, 7]) {
  snapshot.resources = [
    {
      id: 'crew',
      workspaceId: 'personal',
      name: 'Команда',
      kind: 'person',
      capacity,
      unit: 'команды',
      timezone: 'UTC',
      workingWeekdays,
    },
  ];
}

test('seed is genuinely mixed, valid and all sample data is labelled', () => {
  const snapshot = createSeed(NOW);
  assert.ok(
    snapshot.entities.some(
      (entity) => entity.workspaceId === 'personal' && entity.typeId === 'trip',
    ),
  );
  assert.ok(
    snapshot.entities.some(
      (entity) => entity.workspaceId === 'team' && entity.typeId === 'delivery',
    ),
  );
  for (const entity of snapshot.entities) {
    assert.equal(entity.source.kind, 'sample');
    assert.deepEqual(validateEntity(entity, snapshot), [], entity.title);
  }
  assert.ok(snapshot.signals.some((signal) => signal.kind === 'resource-conflict'));
  assert.ok(snapshot.signals.some((signal) => signal.kind === 'overdue'));
  assert.ok(snapshot.templates.some((template) => template.id === 'template-vacation'));
});

test('unknown and approximate times remain explicit, actual never falls back to plan', () => {
  const snapshot = fixture();
  const unknown = item('idea', null, null, { typeId: 'note', kind: 'note', status: 'draft' });
  const approximate = item('flexible', '2026-10-05T10:00:00Z', '2026-10-05T11:00:00Z', {
    plan: {
      ...r('2026-10-05T10:00:00Z', '2026-10-05T11:00:00Z'),
      precision: 'approximate',
      earliest: '2026-10-04',
      latest: '2026-10-06',
    },
  });
  snapshot.entities = [unknown, approximate];
  assert.deepEqual(validateEntity(unknown, snapshot), []);
  assert.deepEqual(validateEntity(approximate, snapshot), []);
  assert.deepEqual(getEffectiveRange(approximate, 'actual'), emptyRange('UTC'));
  assert.equal(unknown.plan.start, null);
  assert.ok(
    validateEntity({ ...approximate, plan: r('2026-10-06', '2026-10-05') }, snapshot).some(
      (error) => error.includes('раньше'),
    ),
  );
  assert.ok(
    validateEntity({ ...approximate, plan: r('not-a-date', null, 'Invalid/Zone') }, snapshot)
      .length >= 2,
  );
});

test('custom schema validates typed fields and membership without industry scheduling branches', () => {
  const snapshot = fixture();
  snapshot.types.push({
    id: 'observation',
    label: 'Наблюдение',
    kind: 'point',
    icon: 'circle',
    color: '#123456',
    fields: [
      { id: 'amount', label: 'Объём', type: 'number', required: true },
      { id: 'choice', label: 'Категория', type: 'select', options: ['A', 'B'] },
    ],
  });
  const entity = item('custom', '2026-10-05', null, {
    typeId: 'observation',
    fields: { amount: 4, choice: 'A', entirelyCustom: 'свободное поле' },
  });
  assert.deepEqual(validateEntity(entity, snapshot), []);
  assert.ok(
    validateEntity(
      { ...entity, fields: { amount: '4', choice: 'C' }, ownerId: 'outsider' },
      snapshot,
    ).length >= 3,
  );
  assert.ok(
    validateEntity(
      { ...entity, links: [{ id: 'bad', kind: 'url', label: 'bad', url: 'javascript:alert(1)' }] },
      snapshot,
    ).some((error) => error.includes('ссылка')),
  );
});

test('hierarchy cycles and cross-workspace parent/resources are rejected', () => {
  const snapshot = fixture();
  const a = item('a', null),
    b = item('b', null, null, { parentId: 'a' });
  snapshot.entities = [a, b, item('foreign', null, null, { workspaceId: 'team' })];
  assert.ok(
    validateEntity({ ...a, parentId: 'b' }, snapshot).some((error) => error.includes('цикл')),
  );
  assert.ok(
    validateEntity({ ...a, parentId: 'foreign' }, snapshot).some((error) =>
      error.includes('другом'),
    ),
  );
});

test('FS, SS, FF cascades use maximum constraints and preserve all baselines and facts', () => {
  const snapshot = fixture();
  snapshot.entities = [
    item('a', '2026-10-05T09:00:00Z', '2026-10-05T10:00:00Z'),
    item('b', '2026-10-05T10:00:00Z', '2026-10-05T11:00:00Z'),
    item('c', '2026-10-05T11:00:00Z', '2026-10-05T12:00:00Z'),
    item('ss', '2026-10-05T09:00:00Z', '2026-10-05T09:30:00Z'),
    item('ff', '2026-10-05T09:00:00Z', '2026-10-05T10:00:00Z'),
  ];
  snapshot.dependencies = [
    dep('a', 'b', 'finish-start', 30),
    dep('b', 'c'),
    dep('a', 'ss', 'start-start', 15),
    dep('a', 'ff', 'finish-finish', 30),
  ];
  const before = structuredClone(snapshot);
  const preview = deriveForecast(snapshot, [
    { entityId: 'a', plan: r('2026-10-05T12:00:00Z', '2026-10-05T13:00:00Z') },
  ]);
  const ranges = new Map(preview.changes.map((change) => [change.entityId, change.plan]));
  assert.equal(ms(ranges.get('b')?.start), ms('2026-10-05T13:30:00Z'));
  assert.equal(ms(ranges.get('c')?.end), ms('2026-10-05T15:30:00Z'));
  assert.equal(ms(ranges.get('ss')?.start), ms('2026-10-05T12:15:00Z'));
  assert.equal(ms(ranges.get('ff')?.end), ms('2026-10-05T13:30:00Z'));
  assert.deepEqual(preview.affectedIds.sort(), ['a', 'b', 'c', 'ff', 'ss']);
  assert.deepEqual(snapshot, before);
});

test('multiple predecessors constrain a target, related links do not constrain time', () => {
  const snapshot = fixture();
  snapshot.entities = [
    item('a', '2026-10-05T09:00Z', '2026-10-05T12:00Z'),
    item('b', '2026-10-05T09:00Z', '2026-10-05T15:00Z'),
    item('c', '2026-10-05T10:00Z', '2026-10-05T11:00Z'),
  ];
  snapshot.dependencies = [dep('a', 'c'), dep('b', 'c'), dep('c', 'a', 'related')];
  const preview = deriveForecast(snapshot, []);
  assert.equal(
    ms(preview.changes.find((change) => change.entityId === 'c')?.plan.start),
    ms('2026-10-05T15:00Z'),
  );
  assert.equal(preview.conflicts.filter((conflict) => conflict.kind === 'cycle').length, 0);
});

test('cycles report their exact members and still allow independent projections', () => {
  const snapshot = fixture();
  snapshot.entities = ['a', 'b', 'descendant', 'd', 'e'].map((id) =>
    item(id, '2026-10-05T09:00Z', '2026-10-05T10:00Z'),
  );
  snapshot.dependencies = [dep('a', 'b'), dep('b', 'a'), dep('b', 'descendant'), dep('d', 'e')];
  const preview = deriveForecast(snapshot, [
    { entityId: 'd', plan: r('2026-10-05T12:00Z', '2026-10-05T13:00Z') },
  ]);
  assert.deepEqual(preview.conflicts.find((conflict) => conflict.kind === 'cycle')?.entityIds, [
    'a',
    'b',
  ]);
  assert.equal(
    ms(preview.changes.find((change) => change.entityId === 'e')?.plan.start),
    ms('2026-10-05T13:00Z'),
  );
  assert.ok(validateDependency(dep('e', 'd'), snapshot).some((error) => error.includes('цикл')));
});

test('actual completion constrains forecasts but completed/started facts are never shifted', () => {
  const snapshot = fixture();
  const source = item('done', '2026-10-05T09:00Z', '2026-10-05T10:00Z', {
    status: 'done',
    actual: r('2026-10-05T09:00Z', '2026-10-05T14:00Z'),
  });
  const target = item('next', '2026-10-05T10:00Z', '2026-10-05T11:00Z');
  snapshot.entities = [source, target];
  snapshot.dependencies = [dep('done', 'next')];
  const before = structuredClone(snapshot);
  assert.equal(ms(deriveForecast(snapshot, []).changes[0]?.plan.start), ms('2026-10-05T14:00Z'));
  assert.deepEqual(snapshot, before);
  target.actual = r('2026-10-05T11:00Z');
  target.status = 'active';
  assert.ok(
    deriveForecast(snapshot, []).conflicts.some((conflict) => conflict.kind === 'dependency'),
  );
  assert.equal(deriveForecast(snapshot, []).changes.length, 0);
  assert.ok(
    deriveForecast(snapshot, [
      { entityId: 'done', plan: r('2026-10-06', '2026-10-07') },
    ]).conflicts.some((conflict) => conflict.message.includes('завершённую')),
  );
});

test('deadline conflicts are reviewable, invalid ranges do not propagate', () => {
  const snapshot = fixture();
  snapshot.entities = [
    item('a', '2026-10-05T09:00Z', '2026-10-05T10:00Z', { dueAt: '2026-10-05T11:00Z' }),
  ];
  assert.ok(
    deriveForecast(snapshot, [
      { entityId: 'a', plan: r('2026-10-05T12:00Z', '2026-10-05T13:00Z') },
    ]).conflicts.some((conflict) => conflict.kind === 'deadline'),
  );
  const invalid = deriveForecast(snapshot, [
    { entityId: 'a', plan: r('2026-10-05T12:00Z', '2026-10-05T11:00Z') },
  ]);
  assert.equal(invalid.changes.length, 0);
  assert.ok(invalid.conflicts.some((conflict) => conflict.kind === 'invalid-time'));
});

test('derived forecasts return toward the plan when a delay clears, while manual estimates survive', () => {
  const snapshot = fixture();
  const source = item('source', '2026-10-05T09:00Z', '2026-10-05T10:00Z', {
    forecast: r('2026-10-05T09:00Z', '2026-10-05T14:00Z'),
    forecastProvenance: 'manual',
  });
  const next = item('next', '2026-10-05T10:00Z', '2026-10-05T11:00Z', {
    forecast: r('2026-10-05T14:00Z', '2026-10-05T15:00Z'),
    forecastProvenance: 'derived',
  });
  const manual = item('manual', '2026-10-05T10:00Z', '2026-10-05T11:00Z', {
    forecast: r('2026-10-05T16:00Z', '2026-10-05T17:00Z'),
    forecastProvenance: 'manual',
  });
  snapshot.entities = [source, next, manual];
  snapshot.dependencies = [dep('source', 'next'), dep('source', 'manual')];
  assert.equal(
    ms(
      deriveForecast(snapshot, []).changes.find((change) => change.entityId === 'next')?.plan.start,
    ),
    ms('2026-10-05T14:00Z'),
  );
  source.forecast = null;
  const preview = deriveForecast(snapshot, []);
  assert.deepEqual(preview.changes.find((change) => change.entityId === 'next')?.plan, next.plan);
  assert.ok(!preview.changes.some((change) => change.entityId === 'manual'));
  assert.equal(next.forecast?.start, '2026-10-05T14:00Z', 'projection remains pure');
  source.forecast = r('2026-10-05T09:00Z', '2026-10-05T20:00Z');
  const blockedManual = deriveForecast(snapshot, []);
  assert.ok(
    blockedManual.conflicts.some(
      (conflict) => conflict.kind === 'dependency' && conflict.entityIds.includes('manual'),
    ),
  );
  assert.ok(!blockedManual.changes.some((change) => change.entityId === 'manual'));
});

test('resource capacity sums three fractional allocations, not just pairwise overlaps', () => {
  const snapshot = fixture();
  crew(snapshot);
  snapshot.entities = ['a', 'b', 'c'].map((id) =>
    item(id, '2026-10-05T09:00Z', '2026-10-05T10:00Z', {
      allocations: [{ resourceId: 'crew', amount: 0.4 }],
    }),
  );
  const conflicts = computeResourceConflicts(snapshot);
  assert.ok(
    conflicts.some(
      (conflict) => conflict.entityIds.length === 3 && conflict.message.includes('1.2'),
    ),
  );
  snapshot.entities[2]!.status = 'cancelled';
  assert.equal(computeResourceConflicts(snapshot).length, 0);
});

test('adjacent half-hour reservations do not conflict; a day interval overlaps a half-hour', () => {
  const snapshot = fixture();
  crew(snapshot);
  snapshot.entities = [
    item('a', '2026-10-05T09:00Z', '2026-10-05T09:30Z', {
      allocations: [{ resourceId: 'crew', amount: 1 }],
    }),
    item('b', '2026-10-05T09:30Z', '2026-10-05T10:00Z', {
      allocations: [{ resourceId: 'crew', amount: 1 }],
    }),
  ];
  assert.equal(computeResourceConflicts(snapshot).length, 0);
  snapshot.entities.push(
    item('day', '2026-10-05T00:00Z', '2026-10-06T00:00Z', {
      allocations: [{ resourceId: 'crew', amount: 1 }],
    }),
  );
  assert.ok(
    computeResourceConflicts(snapshot).some((conflict) => conflict.entityIds.includes('day')),
  );
});

test('simultaneous points consume instantaneous capacity without invented durations', () => {
  const snapshot = fixture();
  crew(snapshot);
  snapshot.entities = [
    item('a', '2026-10-05T10:00Z', null, { allocations: [{ resourceId: 'crew', amount: 0.6 }] }),
    item('b', '2026-10-05T10:00Z', null, { allocations: [{ resourceId: 'crew', amount: 0.6 }] }),
  ];
  assert.ok(computeResourceConflicts(snapshot).some((conflict) => conflict.entityIds.length === 2));
  snapshot.entities[1]!.plan.start = '2026-10-05T10:01Z';
  assert.equal(computeResourceConflicts(snapshot).length, 0);
  snapshot.entities.push(
    item('before', '2026-10-05T09:30Z', '2026-10-05T10:00Z', {
      allocations: [{ resourceId: 'crew', amount: 1 }],
    }),
  );
  assert.equal(
    computeResourceConflicts(snapshot).length,
    0,
    'a period ending exactly at a point does not own that instant',
  );
  snapshot.entities[0]!.plan.start = '2026-10-05T09:59Z';
  assert.ok(
    computeResourceConflicts(snapshot).some(
      (conflict) => conflict.entityIds.includes('a') && conflict.entityIds.includes('before'),
    ),
  );
});

test('resource calendars use their timezone and proposed ranges, without mutating accepted plans', () => {
  const snapshot = fixture();
  crew(snapshot, 1, [1, 2, 3, 4, 5]);
  const entity = item('a', '2026-10-05T09:00Z', '2026-10-05T10:00Z', {
    allocations: [{ resourceId: 'crew', amount: 1 }],
  });
  snapshot.entities = [entity];
  assert.equal(computeResourceConflicts(snapshot).length, 0);
  assert.ok(
    computeResourceConflicts(snapshot, [
      { entityId: 'a', plan: r('2026-10-03T09:00Z', '2026-10-03T10:00Z') },
    ]).some((conflict) => conflict.message.includes('календарю')),
  );
  assert.equal(entity.plan.start, '2026-10-05T09:00Z');
});

test('explicit resource windows clip capacity and availability checks to the requested interval', () => {
  const snapshot = fixture();
  crew(snapshot, 1, [1, 2, 3, 4, 5]);
  snapshot.entities = [
    item('long', '2026-10-02T09:00Z', '2026-10-05T12:00Z', {
      allocations: [{ resourceId: 'crew', amount: 1 }],
    }),
  ];
  const window = { from: '2026-10-05T09:00Z', to: '2026-10-05T10:00Z' };
  assert.equal(
    computeResourceConflicts(snapshot, [], window).length,
    0,
    'the weekend outside the requested window is excluded',
  );
  snapshot.entities.push(
    item('overlap', '2026-10-02T09:00Z', '2026-10-05T12:00Z', {
      allocations: [{ resourceId: 'crew', amount: 1 }],
    }),
  );
  const conflict = computeResourceConflicts(snapshot, [], window).find(
    (c) => c.kind === 'resource',
  )!;
  assert.ok(
    conflict.message.includes('2026-10-05T09:00') && conflict.message.includes('2026-10-05T10:00'),
  );
  assert.ok(!conflict.message.includes('2026-10-02'));
});

test('far-future bookings and proposals are checked against recurring allocations', () => {
  const snapshot = fixture();
  crew(snapshot);
  const recurring = item('daily', '2020-01-01T09:00Z', '2020-01-01T10:00Z', {
    allocations: [{ resourceId: 'crew', amount: 0.6 }],
    recurrence: { frequency: 'day', interval: 1, count: 100_000, exceptions: [] },
  });
  const booking = item('future', '2031-10-05T09:30Z', '2031-10-05T10:00Z', {
    allocations: [{ resourceId: 'crew', amount: 0.6 }],
  });
  snapshot.entities = [recurring, booking];
  assert.ok(
    computeResourceConflicts(snapshot).some(
      (conflict) => conflict.entityIds.includes('daily') && conflict.entityIds.includes('future'),
    ),
  );
  booking.plan = r('2026-10-05T12:00Z', '2026-10-05T13:00Z');
  assert.ok(
    computeResourceConflicts(snapshot, [
      { entityId: 'future', plan: r('2031-10-05T09:30Z', '2031-10-05T10:00Z') },
    ]).some((conflict) => conflict.entityIds.includes('future')),
  );
  assert.equal(
    computeResourceConflicts(snapshot, [], { from: '2031-10-05T12:00Z', to: '2031-10-05T13:00Z' })
      .length,
    0,
  );
});

test('daily recurrence preserves local clock through daylight-saving transition', () => {
  const entity = item('daily', '2026-03-07T09:00:00-05:00', '2026-03-07T10:00:00-05:00', {
    plan: r('2026-03-07T09:00:00-05:00', '2026-03-07T10:00:00-05:00', 'America/New_York'),
    recurrence: { frequency: 'day', interval: 1, count: 3, exceptions: [] },
  });
  const occurrences = expandRecurrence(entity, '2026-03-07', '2026-03-11');
  assert.equal(occurrences.length, 3);
  assert.deepEqual(
    occurrences.map((value) => DateTime.fromISO(value.start!, { zone: 'America/New_York' }).hour),
    [9, 9, 9],
  );
  assert.equal(DateTime.fromISO(occurrences[0]!.start!, { setZone: true }).offset, -300);
  assert.equal(DateTime.fromISO(occurrences[1]!.start!, { setZone: true }).offset, -240);
  assert.equal((ms(occurrences[1]!.start)! - ms(occurrences[0]!.start)!) / 3_600_000, 23);
});

test('all-day recurrence preserves local dates rather than a fixed 24-hour duration', () => {
  const entity = item('days', '2026-03-07T00:00:00-05:00', '2026-03-08T00:00:00-05:00', {
    plan: {
      ...r('2026-03-07T00:00:00-05:00', '2026-03-08T00:00:00-05:00', 'America/New_York'),
      precision: 'day',
    },
    recurrence: { frequency: 'day', interval: 1, count: 3, exceptions: [] },
  });
  const occurrences = expandRecurrence(entity, '2026-03-07', '2026-03-11');
  assert.equal((ms(occurrences[1]!.end)! - ms(occurrences[1]!.start)!) / 3_600_000, 23);
  assert.equal(DateTime.fromISO(occurrences[1]!.end!, { zone: 'America/New_York' }).hour, 0);
});

test('monthly recurrence does not drift after February and exceptions retain count/index', () => {
  const entity = item('month', '2026-01-31T09:00:00Z', null, {
    recurrence: { frequency: 'month', interval: 1, count: 3, exceptions: ['2026-02-28'] },
  });
  const occurrences = expandRecurrence(entity, '2026-01-01', '2026-05-01');
  assert.deepEqual(
    occurrences.map((value) => value.start!.slice(0, 10)),
    ['2026-01-31', '2026-03-31'],
  );
  assert.deepEqual(
    occurrences.map((value) => value.index),
    [0, 2],
  );
  assert.equal(expandRecurrence(entity, '2026-03-01', '2026-04-01').length, 1);
});

test('standard calendar policy skips impossible monthly dates and DST gap times without consuming count', () => {
  const monthly = item('standard-month', '2026-01-31T09:00Z', null, {
    recurrence: {
      frequency: 'month',
      interval: 1,
      count: 3,
      calendarPolicy: 'skip-invalid',
      exceptions: [],
    },
  });
  assert.deepEqual(
    expandRecurrence(monthly, '2026-01-01', '2026-07-01').map((value) => value.start!.slice(0, 10)),
    ['2026-01-31', '2026-03-31', '2026-05-31'],
  );
  const daily = item('standard-gap', '2026-03-07T02:30:00-05:00', null, {
    plan: r('2026-03-07T02:30:00-05:00', null, 'America/New_York'),
    recurrence: {
      frequency: 'day',
      interval: 1,
      count: 3,
      calendarPolicy: 'skip-invalid',
      exceptions: [],
    },
  });
  const occurrences = expandRecurrence(daily, '2026-03-07', '2026-03-12');
  assert.deepEqual(
    occurrences.map((value) => value.start!.slice(0, 10)),
    ['2026-03-07', '2026-03-09', '2026-03-10'],
  );
  assert.deepEqual(
    occurrences.map((value) => value.index),
    [0, 1, 2],
  );
});

test('standard calendar policy selects the first occurrence of a repeated DST clock time', () => {
  const daily = item('fold', '2026-10-31T01:30:00-04:00', null, {
    plan: r('2026-10-31T01:30:00-04:00', null, 'America/New_York'),
    recurrence: {
      frequency: 'day',
      interval: 1,
      count: 3,
      calendarPolicy: 'skip-invalid',
      exceptions: [],
    },
  });
  const occurrence = expandRecurrence(daily, '2026-10-31', '2026-11-04')[1]!;
  assert.equal(DateTime.fromISO(occurrence.start!).toUTC().toISO(), '2026-11-01T05:30:00.000Z');
  daily.plan = r('2026-01-01T01:30:00-05:00', null, 'America/New_York');
  daily.recurrence!.count = 400;
  assert.equal(
    expandRecurrence(daily, '2026-11-01T05:15Z', '2026-11-01T05:45Z')[0]?.start,
    '2026-11-01T01:30:00.000-04:00',
  );
});

test('weekly weekdays, interval, count, exceptions and inclusive date until work together', () => {
  const entity = item('week', '2026-10-07T09:00Z', null, {
    recurrence: {
      frequency: 'week',
      interval: 2,
      weekdays: [1, 3, 5],
      count: 5,
      until: '2026-10-23',
      exceptions: ['2026-10-09'],
    },
  });
  const occurrences = expandRecurrence(entity, '2026-10-01', '2026-11-01');
  assert.deepEqual(
    occurrences.map((value) => value.start!.slice(0, 10)),
    ['2026-10-07', '2026-10-19', '2026-10-21', '2026-10-23'],
  );
  assert.deepEqual(
    occurrences.map((value) => value.index),
    [0, 2, 3, 4],
  );
  assert.deepEqual(
    expandRecurrence(entity, '2026-10-20', '2026-11-01').map((value) => value.index),
    [3, 4],
  );
});

test('expansion is bounded and can efficiently jump to a distant visible window', () => {
  const entity = item('long', '2020-01-01T09:00Z', null, {
    recurrence: { frequency: 'day', interval: 1, count: 100_000, exceptions: [] },
  });
  assert.equal(
    expandRecurrence(entity, '2020-01-01', '2200-01-01').length,
    MAX_RECURRENCE_OCCURRENCES,
  );
  assert.equal(expandRecurrence(entity, '2040-01-01', '2040-01-05').length, 4);
  assert.deepEqual(
    expandRecurrence(
      { ...entity, recurrence: { ...entity.recurrence!, interval: 0 } },
      '2020-01-01',
      '2020-02-01',
    ),
    [],
  );
  assert.deepEqual(expandRecurrence(entity, 'bad', 'bad'), []);
});

test('signals are deterministic, deduplicated, preserve acknowledgement and respect inactive objects', () => {
  const snapshot = fixture();
  snapshot.entities = [
    item('late', '2026-10-01T10:00Z', '2026-10-01T11:00Z', { dueAt: '2026-10-01T11:00Z' }),
    item('risk', '2026-10-03T10:00Z', '2026-10-03T11:00Z', {
      dueAt: '2026-10-03T11:00Z',
      forecast: r('2026-10-03T10:00Z', '2026-10-03T13:00Z'),
    }),
    item('idea', null, null, { typeId: 'note', kind: 'note', status: 'draft' }),
    item('stale', '2026-10-05', null, {
      source: {
        kind: 'import',
        label: 'Пример CSV',
        observedAt: '2026-09-30T12:00Z',
        receivedAt: '2026-09-30T12:00Z',
        staleAfterMinutes: 60,
      },
    }),
    item('cancelled', null, null, { status: 'cancelled', dueAt: '2026-10-01' }),
  ];
  const first = evaluateSignals(snapshot, NOW),
    second = evaluateSignals(snapshot, '2026-10-02T06:01Z');
  assert.deepEqual(
    first.map((signal) => signal.id),
    second.map((signal) => signal.id),
  );
  assert.equal(new Set(first.map((signal) => signal.dedupeKey)).size, first.length);
  assert.deepEqual(
    new Set(first.map((signal) => signal.kind)),
    new Set(['overdue', 'forecast-risk', 'missing-date', 'stale-source']),
  );
  assert.ok(!first.some((signal) => signal.entityId === 'cancelled'));
  snapshot.signals = first.map((signal) => ({
    ...signal,
    state: 'acknowledged' as const,
    acknowledgedBy: 'local-owner',
  }));
  assert.ok(
    evaluateSignals(snapshot).every(
      (signal) =>
        signal.state === 'acknowledged' &&
        signal.createdAt === first.find((old) => old.id === signal.id)?.createdAt,
    ),
  );
  snapshot.entities[0]!.status = 'done';
  assert.ok(!evaluateSignals(snapshot).some((signal) => signal.entityId === 'late'));
});

test('reminders appear only inside the configured window and recurrence targets have independent IDs', () => {
  const snapshot = fixture();
  snapshot.entities = [
    item('meeting', '2026-10-02T07:00Z', null, {
      recurrence: { frequency: 'day', interval: 1, count: 2, exceptions: [] },
    }),
  ];
  snapshot.rules = [
    {
      id: 'remind',
      workspaceId: 'personal',
      name: 'За час',
      enabled: true,
      trigger: 'before-start',
      leadMinutes: 60,
      action: 'signal',
    },
  ];
  const today = evaluateSignals(snapshot, NOW).filter((signal) => signal.kind === 'reminder');
  const tomorrow = evaluateSignals(snapshot, '2026-10-03T06:00Z').filter(
    (signal) => signal.kind === 'reminder',
  );
  assert.equal(today.length, 1);
  assert.equal(tomorrow.length, 1);
  assert.notEqual(today[0]!.id, tomorrow[0]!.id);
  assert.equal(evaluateSignals(snapshot, '2026-10-02T05:59Z').length, 0);
  snapshot.rules[0]!.enabled = false;
  assert.equal(evaluateSignals(snapshot, NOW).length, 0);
});

test('template instances have independent identities, nesting and calendar-aware offsets', () => {
  const template = universalTemplates.find((value) => value.id === 'template-trip')!;
  const options = {
    workspaceId: 'personal',
    anchorDate: '2026-03-07T09:00:00-05:00',
    title: 'Моя поездка',
    ownerId: 'local-owner',
    timezone: 'America/New_York',
  };
  const first = applyTemplate(template, options),
    second = applyTemplate(template, options);
  assert.equal(first.entities[0]!.title, 'Моя поездка');
  assert.ok(first.entities.slice(1).every((entity) => entity.parentId === first.entities[0]!.id));
  assert.ok(
    first.entities.every((entity) => !second.entities.some((other) => other.id === entity.id)),
  );
  assert.ok(
    first.dependencies.every(
      (dependency) =>
        first.entities.some((entity) => entity.id === dependency.fromId) &&
        first.entities.some((entity) => entity.id === dependency.toId),
    ),
  );
  const stay = first.entities.find((entity) => entity.title === 'На месте')!;
  assert.equal(DateTime.fromISO(stay.plan.end!, { zone: 'America/New_York' }).hour, 9);
  assert.equal((ms(stay.plan.end)! - ms(stay.plan.start)!) / 3_600_000, 119);
  assert.deepEqual(stay.plan, stay.baseline);
  assert.notEqual(stay.plan, stay.baseline);
});

test('invalid template references, hierarchy/dependency cycles and bad anchors fail clearly', () => {
  const template = structuredClone(universalTemplates[0]!);
  assert.throws(
    () => applyTemplate(template, { workspaceId: 'personal', anchorDate: 'bad' }),
    /опорная/,
  );
  template.items[0]!.parentKey = 'missing';
  assert.throws(
    () => applyTemplate(template, { workspaceId: 'personal', anchorDate: NOW }),
    /Родитель/,
  );
  template.items[0]!.parentKey = 'work';
  assert.throws(
    () => applyTemplate(template, { workspaceId: 'personal', anchorDate: NOW }),
    /вложенности/,
  );
  template.items[0]!.parentKey = undefined;
  template.dependencies.push({
    fromKey: 'result',
    toKey: 'prepare',
    kind: 'finish-start',
    lagMinutes: 0,
  });
  assert.throws(
    () => applyTemplate(template, { workspaceId: 'personal', anchorDate: NOW }),
    /зависимостей/,
  );
});

test('local assistant parses Russian dates/time/durations and keeps unknown dates unknown', () => {
  const snapshot = fixture();
  const period = suggestFromText('Добавь поездку с 15 по 20 октября', snapshot, 'personal', NOW);
  assert.equal(period.provider, 'local');
  assert.equal(period.drafts.length, 1);
  assert.equal(period.drafts[0]!.typeId, 'trip');
  assert.equal(
    DateTime.fromISO(period.drafts[0]!.plan.start!).setZone('Asia/Yekaterinburg').day,
    15,
  );
  assert.equal(DateTime.fromISO(period.drafts[0]!.plan.end!).setZone('Asia/Yekaterinburg').day, 20);
  const halfHour = suggestFromText('Встреча завтра в 14:30 на 30 минут', snapshot, 'personal', NOW)
    .drafts[0]!;
  assert.equal((ms(halfHour.plan.end)! - ms(halfHour.plan.start)!) / 60000, 30);
  assert.equal(DateTime.fromISO(halfHour.plan.start!, { zone: 'Asia/Yekaterinburg' }).hour, 14);
  const unknown = suggestFromText('Идея для альбома', snapshot, 'personal', NOW);
  assert.equal(unknown.drafts[0]!.plan.start, null);
  assert.ok(unknown.assumptions.some((value) => value.includes('без даты')));
  assert.equal(snapshot.entities.length, 0);
});

test('assistant custom type fields are driven by schema, and risk explanations reference real scoped entities', () => {
  const snapshot = fixture();
  snapshot.types.push({
    id: 'fishing',
    label: 'Рыбалка',
    icon: 'circle',
    color: '#777777',
    kind: 'period',
    fields: [
      { id: 'place', label: 'Место', type: 'text' },
      { id: 'distance', label: 'Расстояние', type: 'number' },
    ],
  });
  const custom = suggestFromText(
    'Рыбалка завтра; Место: озеро; Расстояние: 20',
    snapshot,
    'personal',
    NOW,
  ).drafts[0]!;
  assert.equal(custom.typeId, 'fishing');
  assert.deepEqual(custom.fields, { place: 'озеро', distance: 20 });
  snapshot.entities = [
    item('my-late', '2026-10-01', null, { dueAt: '2026-10-01T12:00Z' }),
    item('their-late', '2026-10-01', null, { workspaceId: 'team', dueAt: '2026-10-01T12:00Z' }),
  ];
  const risks = suggestFromText('Покажи риски', snapshot, 'personal', NOW);
  assert.deepEqual(risks.evidenceIds, ['my-late']);
  assert.equal(risks.drafts.length, 0);
  assert.ok(!risks.summary.includes('their-late'));
});

test('assistant relocation is a reviewable dependency preview, never an automatic mutation', () => {
  const snapshot = fixture();
  snapshot.entities = [
    item('Поставка', '2026-10-05T09:00Z', '2026-10-05T10:00Z'),
    item('Монтаж', '2026-10-05T10:00Z', '2026-10-05T11:00Z'),
  ];
  snapshot.dependencies = [dep('Поставка', 'Монтаж')];
  const before = structuredClone(snapshot);
  const suggestion = suggestFromText('Перенеси Поставка на 3 дня', snapshot, 'personal');
  assert.equal(suggestion.preview?.changes.length, 2);
  assert.deepEqual(suggestion.evidenceIds.sort(), ['Монтаж', 'Поставка'].sort());
  assert.deepEqual(snapshot, before);
});
