import test from 'node:test';
import assert from 'node:assert/strict';
import {
  applyTemplate,
  computeResourceConflicts,
  deriveForecast,
  evaluateSignals,
  expandPreciseOccurrences,
  expandRecurrence,
  validateEntity,
} from './engine.js';
import { canonicalRange, isoToNs, rangeFromNs } from './precise-time.js';
import { createSeed } from './seed.js';
import type { Entity, PlannerSnapshot, Template, TimeRange } from './types.js';

const NOW = '2026-10-03T07:00:00Z';
const DEEP = -31556952000000000000000000n;
function fixture(): PlannerSnapshot {
  return {
    ...createSeed(NOW),
    entities: [],
    dependencies: [],
    resources: [],
    rules: [],
    signals: [],
    notifications: [],
  };
}
function item(id: string, plan: TimeRange, extra: Partial<Entity> = {}): Entity {
  const period = canonicalRange(plan).end !== null;
  return {
    id,
    workspaceId: 'personal',
    typeId: period ? 'period' : 'event',
    kind: period ? 'period' : 'point',
    title: id,
    description: '',
    parentId: null,
    ownerId: 'local-owner',
    participantIds: [],
    status: 'planned',
    plan,
    baseline: structuredClone(plan),
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
function resource(snapshot: PlannerSnapshot, weekdays = [1, 2, 3, 4, 5, 6, 7]) {
  snapshot.resources = [
    {
      id: 'shared',
      workspaceId: 'personal',
      name: 'Ресурс',
      kind: 'other',
      capacity: 1,
      unit: 'ед.',
      timezone: 'UTC',
      workingWeekdays: weekdays,
    },
  ];
}

test('deep-time plan, baseline, actual and forecast validate without invented dates', () => {
  const snapshot = fixture(),
    plan = rangeFromNs(DEEP, DEEP + 10n);
  const entity = item('deep', plan, {
    actual: rangeFromNs(DEEP + 1n, null),
    forecast: rangeFromNs(DEEP + 1n, DEEP + 11n),
  });
  snapshot.entities = [entity];
  assert.deepEqual(validateEntity(entity, snapshot), []);
  const signals = evaluateSignals(snapshot);
  assert.equal(
    signals.some((signal) => signal.kind === 'missing-date'),
    false,
  );
  assert.ok(
    signals.some(
      (signal) => signal.kind === 'forecast-risk' && signal.description.includes('1 нс'),
    ),
  );
});

test('precise half-open viewport keeps tiny events at huge coordinates', () => {
  const entity = item('tiny', rangeFromNs(DEEP + 5n, DEEP + 6n));
  assert.equal(expandPreciseOccurrences(entity, DEEP + 5n, DEEP + 6n).length, 1);
  assert.equal(expandPreciseOccurrences(entity, DEEP + 6n, DEEP + 7n).length, 0);
  assert.equal(expandPreciseOccurrences(entity, DEEP + 4n, DEEP + 5n).length, 0);
  const occurrence = expandRecurrence(entity, (DEEP + 5n).toString(), (DEEP + 6n).toString())[0]!;
  assert.equal(occurrence.start, null);
  assert.equal(occurrence.precise?.start, (DEEP + 5n).toString());
});

test('exact dependency forecast preserves duration and never changes baseline or actual', () => {
  const snapshot = fixture();
  snapshot.entities = [
    item('a', rangeFromNs(DEEP, DEEP + 10n)),
    item('b', rangeFromNs(DEEP + 5n, DEEP + 7n)),
  ];
  snapshot.dependencies = [
    {
      id: 'a-b',
      workspaceId: 'personal',
      fromId: 'a',
      toId: 'b',
      kind: 'finish-start',
      lagMinutes: 0,
    },
  ];
  const before = structuredClone(snapshot);
  const preview = deriveForecast(snapshot, [
    { entityId: 'a', plan: rangeFromNs(DEEP + 20n, DEEP + 30n) },
  ]);
  assert.equal(preview.conflicts.length, 0);
  const projected = canonicalRange(preview.changes.find((change) => change.entityId === 'b')!.plan);
  assert.equal(projected.start, DEEP + 30n);
  assert.equal(projected.end, DEEP + 32n);
  assert.deepEqual(snapshot, before);
});

test('started and finished facts remain protected even when every ISO mirror is null', () => {
  const snapshot = fixture();
  const entity = item('a', rangeFromNs(DEEP, DEEP + 10n), { actual: rangeFromNs(DEEP, null) });
  snapshot.entities = [entity];
  const moved = deriveForecast(snapshot, [
    { entityId: 'a', plan: rangeFromNs(DEEP + 1n, DEEP + 11n) },
  ]);
  assert.equal(moved.changes.length, 0);
  assert.ok(moved.conflicts.some((conflict) => conflict.message.includes('уже начата')));
  entity.actual = rangeFromNs(null, DEEP + 10n);
  assert.equal(
    deriveForecast(snapshot, [{ entityId: 'a', plan: rangeFromNs(DEEP, DEEP + 11n) }]).changes
      .length,
    0,
  );
});

test('resource conflicts distinguish adjacent one-nanosecond reservations', () => {
  const snapshot = fixture();
  resource(snapshot);
  snapshot.entities = [
    item('a', rangeFromNs(DEEP, DEEP + 1n), { allocations: [{ resourceId: 'shared', amount: 1 }] }),
    item('b', rangeFromNs(DEEP + 1n, DEEP + 2n), {
      allocations: [{ resourceId: 'shared', amount: 1 }],
    }),
  ];
  assert.deepEqual(computeResourceConflicts(snapshot), []);
  snapshot.entities[1]!.plan = rangeFromNs(DEEP, DEEP + 1n);
  const conflicts = computeResourceConflicts(snapshot);
  assert.ok(
    conflicts.some((conflict) => conflict.kind === 'resource' && conflict.entityIds.length === 2),
  );
});

test('exact reservations overlap ordinary calendar bookings without rounding', () => {
  const snapshot = fixture();
  resource(snapshot);
  const now = isoToNs(NOW)!;
  snapshot.entities = [
    item(
      'calendar',
      { start: NOW, end: '2026-10-03T08:00:00Z', timezone: 'UTC', precision: 'exact' },
      { allocations: [{ resourceId: 'shared', amount: 1 }] },
    ),
    item('exact', rangeFromNs(now + 1n, now + 2n), {
      allocations: [{ resourceId: 'shared', amount: 1 }],
    }),
  ];
  assert.ok(
    computeResourceConflicts(snapshot).some(
      (conflict) => conflict.entityIds.includes('exact') && conflict.entityIds.includes('calendar'),
    ),
  );
});

test('resource calendar limits are reported honestly outside its calendar range', () => {
  const snapshot = fixture();
  resource(snapshot, [1, 2, 3, 4, 5]);
  snapshot.entities = [
    item('deep', rangeFromNs(DEEP, DEEP + 1n), {
      allocations: [{ resourceId: 'shared', amount: 1 }],
    }),
  ];
  assert.ok(
    computeResourceConflicts(snapshot).some(
      (conflict) => conflict.kind === 'invalid-time' && conflict.message.includes('0001–9999'),
    ),
  );
});

test('exact template reanchoring preserves one-nanosecond offsets and resolution', () => {
  const plan = rangeFromNs(DEEP + 1n, DEEP + 3n, 'UTC', 'approximate');
  plan.precise!.resolutionNs = '10';
  const template: Template = {
    id: 'exact',
    name: 'Точный',
    description: '',
    anchorNs: DEEP.toString(),
    items: [
      {
        key: 'a',
        title: 'A',
        typeId: 'period',
        kind: 'period',
        offsetDays: 0,
        durationDays: 0,
        schedule: plan,
      },
    ],
    dependencies: [],
  };
  const before = structuredClone(template),
    target = -DEEP;
  const created = applyTemplate(template, { workspaceId: 'personal', anchorNs: target.toString() });
  assert.equal(canonicalRange(created.entities[0]!.plan).start, target + 1n);
  assert.equal(canonicalRange(created.entities[0]!.plan).end, target + 3n);
  assert.equal(created.entities[0]!.plan.precise?.resolutionNs, '10');
  assert.equal(created.entities[0]!.plan.precision, 'approximate');
  assert.deepEqual(created.entities[0]!.baseline, created.entities[0]!.plan);
  assert.deepEqual(template, before);
});

test('calendar recurrence and calendar template operations never masquerade as fixed nanoseconds', () => {
  const snapshot = fixture(),
    precise = item('series', rangeFromNs(isoToNs(NOW)!, null), {
      recurrence: { frequency: 'day', interval: 1, count: 2, exceptions: [] },
    });
  assert.ok(validateEntity(precise, snapshot).some((error) => error.includes('ISO + IANA')));
  const template: Template = {
    id: 'calendar',
    name: 'Календарный',
    description: '',
    items: [
      { key: 'a', title: 'A', typeId: 'period', kind: 'period', offsetDays: 0, durationDays: 1 },
    ],
    dependencies: [],
  };
  assert.throws(
    () => applyTemplate(template, { workspaceId: 'personal', anchorNs: DEEP.toString() }),
    /календарных годах/,
  );
});

test('reminders compare precise instants without treating deep-time objects as undated', () => {
  const snapshot = fixture(),
    now = isoToNs(NOW)!;
  snapshot.entities = [
    item('near', rangeFromNs(now + 1n, null)),
    item('far', rangeFromNs(-DEEP, null)),
  ];
  snapshot.rules = [
    {
      id: 'start',
      workspaceId: 'personal',
      name: 'Начало',
      enabled: true,
      trigger: 'before-start',
      leadMinutes: 1,
      action: 'signal',
    },
  ];
  const signals = evaluateSignals(snapshot);
  assert.ok(signals.some((signal) => signal.kind === 'reminder' && signal.entityId === 'near'));
  assert.equal(
    signals.some((signal) => signal.kind === 'reminder' && signal.entityId === 'far'),
    false,
  );
  assert.equal(
    signals.some((signal) => signal.kind === 'missing-date'),
    false,
  );
});
