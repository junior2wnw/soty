import test from 'node:test';
import assert from 'node:assert/strict';
import {
  applyTemplate,
  computeResourceConflicts,
  deriveForecast,
  evaluateSignals,
  expandRecurrence,
  validateEntity,
  validateDependency,
} from './engine.js';
import { canonicalRange, isoToNs, rangeFromNs } from './precise-time.js';
import { createSeed } from './seed.js';
import type { Entity, PlannerSnapshot, Template, TimeRange } from './types.js';
import { dependencySchema, ruleSchema } from '../server/validation.ts';
const NOW = '2026-10-03T07:00:00Z';
test('legacy numeric minute lags round 20 milliseconds to the nearest nanosecond', () => {
  const snapshot = fixture(),
    deep = -31556952000000000000000000n;
  snapshot.entities = [item('a', rangeFromNs(deep, null)), item('b', rangeFromNs(deep, null))];
  snapshot.dependencies = [
    {
      id: 'a-b',
      workspaceId: 'personal',
      fromId: 'a',
      toId: 'b',
      kind: 'start-start',
      lagMinutes: 20 / 60000,
    },
  ];
  const preview = deriveForecast(snapshot, []);
  assert.equal(preview.conflicts.length, 0);
  assert.equal(
    canonicalRange(preview.changes.find((change) => change.entityId === 'b')!.plan).start,
    deep + 20000000n,
  );
  assert.deepEqual(validateDependency(snapshot.dependencies[0]!, snapshot), []);
  assert.equal(dependencySchema.safeParse(snapshot.dependencies[0]).success, true);
});

test('nonzero numeric durations below one nanosecond are rejected explicitly', () => {
  const snapshot = fixture(),
    now = isoToNs(NOW)!;
  snapshot.entities = [item('a', rangeFromNs(now, null)), item('b', rangeFromNs(now, null))];
  const dependency = {
    id: 'a-b',
    workspaceId: 'personal',
    fromId: 'a',
    toId: 'b',
    kind: 'start-start' as const,
    lagMinutes: 0.5 / 60000000000,
  };
  assert.ok(
    validateDependency(dependency, snapshot).some((message) => message.includes('наносекунды')),
  );
  assert.equal(dependencySchema.safeParse(dependency).success, false);
  const rule = {
    id: 'start',
    workspaceId: 'personal',
    name: 'Начало',
    enabled: true,
    trigger: 'before-start' as const,
    action: 'signal' as const,
    leadMinutes: dependency.lagMinutes,
  };
  assert.equal(ruleSchema.safeParse(rule).success, false);
  assert.equal(ruleSchema.safeParse({ ...rule, leadMinutes: 20 / 60000 }).success, true);
  assert.equal(ruleSchema.safeParse({ ...rule, leadMinutes: 0 }).success, true);
});
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
function resource(snapshot: PlannerSnapshot) {
  snapshot.resources = [
    {
      id: 'shared',
      workspaceId: 'personal',
      name: 'Ресурс',
      kind: 'other',
      capacity: 1,
      unit: 'ед.',
      timezone: 'UTC',
      workingWeekdays: [1, 2, 3, 4, 5, 6, 7],
    },
  ];
}

test('ordinary ISO nanoseconds survive a one-nanosecond viewport without requiring precise fields', () => {
  const plan: TimeRange = {
    start: '2026-10-03T07:00:00.000000001Z',
    end: '2026-10-03T07:00:00.000000002Z',
    timezone: 'UTC',
    precision: 'exact',
  };
  const entity = item('iso-nano', plan),
    snapshot = fixture();
  assert.deepEqual(validateEntity(entity, snapshot), []);
  const occurrence = expandRecurrence(entity, plan.start!, plan.end!)[0]!;
  assert.equal(occurrence.start, plan.start);
  assert.equal(occurrence.end, plan.end);
  assert.equal(expandRecurrence(entity, plan.end!, '2026-10-03T07:00:00.000000003Z').length, 0);
});

test('calendar recurrence preserves ISO nanosecond offsets and duration through DST', () => {
  const plan: TimeRange = {
    start: '2026-03-07T09:00:00.000000001-05:00',
    end: '2026-03-07T09:00:00.000000003-05:00',
    timezone: 'America/New_York',
    precision: 'exact',
  };
  const entity = item('nano-calendar', plan, {
    recurrence: { frequency: 'day', interval: 1, count: 3, exceptions: [] },
  });
  const occurrences = expandRecurrence(entity, '2026-03-07', '2026-03-11');
  assert.equal(occurrences.length, 3);
  assert.equal(occurrences[1]!.start, '2026-03-08T09:00:00.000000001-04:00');
  assert.equal(occurrences[1]!.end, '2026-03-08T09:00:00.000000003-04:00');
  assert.equal(isoToNs(occurrences[1]!.start)! - isoToNs(occurrences[0]!.start)!, 82800000000000n);
  assert.equal(entity.plan.precise, undefined);
});

test('calendar EXDATE and inclusive UNTIL compare exact ISO nanoseconds', () => {
  const entity = item(
    'nano-exceptions',
    { start: '2026-10-03T07:00:00.000000001Z', end: null, timezone: 'UTC', precision: 'exact' },
    {
      recurrence: {
        frequency: 'day',
        interval: 1,
        count: 3,
        exceptions: ['2026-10-04T07:00:00.000000002Z'],
        until: '2026-10-05T07:00:00.000000000Z',
      },
    },
  );
  assert.equal(expandRecurrence(entity, '2026-10-03', '2026-10-07').length, 2);
  entity.recurrence!.exceptions = ['2026-10-04T07:00:00.000000001Z'];
  assert.equal(expandRecurrence(entity, '2026-10-03', '2026-10-07').length, 1);
  entity.plan.start = '2026-10-03T23:59:59.999999999Z';
  entity.recurrence = {
    frequency: 'day',
    interval: 1,
    count: 3,
    exceptions: [],
    until: '2026-10-04',
  };
  assert.equal(expandRecurrence(entity, '2026-10-03', '2026-10-07').length, 2);
});

test('calendar resource series distinguish adjacent nanosecond reservations and tiny audit windows', () => {
  const snapshot = fixture();
  resource(snapshot);
  const plan = (start: string, end: string): TimeRange => ({
    start,
    end,
    timezone: 'UTC',
    precision: 'exact',
  });
  const extra = {
    allocations: [{ resourceId: 'shared', amount: 1 }],
    recurrence: { frequency: 'day' as const, interval: 1, count: 2, exceptions: [] },
  };
  snapshot.entities = [
    item('a', plan('2026-10-03T07:00:00.000000001Z', '2026-10-03T07:00:00.000000002Z'), extra),
    item('b', plan('2026-10-03T07:00:00.000000002Z', '2026-10-03T07:00:00.000000003Z'), extra),
  ];
  assert.deepEqual(computeResourceConflicts(snapshot), []);
  snapshot.entities[1]!.plan = { ...snapshot.entities[0]!.plan };
  assert.ok(computeResourceConflicts(snapshot).some((conflict) => conflict.kind === 'resource'));
  assert.ok(
    computeResourceConflicts(snapshot, [], {
      from: '2026-10-03T07:00:00.000000001Z',
      to: '2026-10-03T07:00:00.000000002Z',
    }).some((conflict) => conflict.kind === 'resource'),
  );
});

test('ordinary ISO nanoseconds drive dependency forecasts while keeping calendar series calendar-based', () => {
  const snapshot = fixture();
  const plan = (start: string, end: string | null): TimeRange => ({
    start,
    end,
    timezone: 'UTC',
    precision: 'exact',
  });
  snapshot.entities = [
    item('a', plan('2026-10-03T07:00:00.000000001Z', '2026-10-03T07:00:00.000000003Z')),
    item('b', plan('2026-10-03T07:00:00.000000001Z', '2026-10-03T07:00:00.000000003Z'), {
      recurrence: { frequency: 'day', interval: 1, count: 2, exceptions: [] },
    }),
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
  const preview = deriveForecast(snapshot, []);
  const next = preview.changes.find((change) => change.entityId === 'b')!.plan;
  assert.equal(next.start, '2026-10-03T07:00:00.000000003Z');
  assert.equal(next.end, '2026-10-03T07:00:00.000000005Z');
  assert.equal(next.precise, undefined);
  assert.equal(preview.conflicts.length, 0);
});

test('overdue respects exact deadlines and recorded completion while a partial start remains overdue', () => {
  const snapshot = fixture();
  const now = isoToNs(NOW)!;
  const entity = item('deadline', rangeFromNs(now, now + 5n), {
    dueAt: '2026-10-03T07:00:00.000000001Z',
    actual: rangeFromNs(now, null),
  });
  snapshot.entities = [entity];
  assert.ok(
    evaluateSignals(snapshot, '2026-10-03T07:00:00.000000002Z').some(
      (signal) => signal.kind === 'overdue',
    ),
  );
  entity.actual = rangeFromNs(null, now + 1n);
  assert.equal(
    evaluateSignals(snapshot, '2026-10-03T07:00:00.000000002Z').some(
      (signal) => signal.kind === 'overdue',
    ),
    false,
  );
});

test('ordinary ISO template reanchoring preserves fractional anchor and schedule nanoseconds', () => {
  const schedule: TimeRange = {
    start: '2026-10-03T07:00:00.000000003Z',
    end: '2026-10-03T07:00:00.000000005Z',
    timezone: 'UTC',
    precision: 'exact',
  };
  const template: Template = {
    id: 'iso-template',
    name: 'ISO nanos',
    description: '',
    anchorDate: '2026-10-03T07:00:00.000000001Z',
    timezone: 'UTC',
    items: [
      {
        key: 'a',
        title: 'A',
        typeId: 'period',
        kind: 'period',
        offsetDays: 0,
        durationDays: 0,
        schedule,
      },
    ],
    dependencies: [],
  };
  const created = applyTemplate(template, {
    workspaceId: 'personal',
    anchorDate: '2026-10-04T07:00:00.000000002Z',
  }).entities[0]!;
  assert.equal(created.plan.start, '2026-10-04T07:00:00.000000004Z');
  assert.equal(created.plan.end, '2026-10-04T07:00:00.000000006Z');
  assert.equal(created.plan.precise, undefined);
});
