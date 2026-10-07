import test from 'node:test';
import assert from 'node:assert/strict';
import { DateTime } from 'luxon';
import { PlannerStore } from './store.ts';
import { ApiError } from './validation.ts';
import { exportContent, importContent, parseCsv, parseIcs } from './imports.ts';
import { createSeed } from '../shared/seed.ts';
import { expandRecurrence } from '../shared/engine.ts';
import type { Entity, PlannerSnapshot, TimeRange } from '../shared/types.ts';

const NOW = '2026-10-02T10:00:00Z';
const range = (
  start: string | null,
  end: string | null = null,
  timezone = 'UTC',
  precision: TimeRange['precision'] = start ? 'exact' : 'unknown',
): TimeRange => ({ start, end, timezone, precision });
function fixture() {
  const seed = createSeed(NOW);
  seed.entities = [];
  seed.dependencies = [];
  seed.resources = [];
  seed.rules = [];
  seed.signals = [];
  seed.notifications = [];
  seed.scenarios = [];
  seed.audit = [];
  seed.comments = [];
  return new PlannerStore(':memory:', seed);
}
function entity(id: string, extra: Partial<Entity> = {}): Entity {
  const plan = range('2026-10-05T09:00:00Z', '2026-10-05T10:00:00Z');
  return {
    id,
    workspaceId: 'personal',
    typeId: 'period',
    kind: 'period',
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
function richSnapshot(): PlannerSnapshot {
  const store = fixture();
  const snapshot = store.read();
  store.close();
  snapshot.types.push({
    id: 'personal-check',
    workspaceId: 'personal',
    label: 'Проверка',
    icon: 'circle',
    color: '#123456',
    kind: 'period',
    fields: [
      {
        id: 'result',
        label: 'Результат',
        type: 'select',
        required: true,
        options: ['Ожидается', 'Готово'],
      },
      { id: 'score', label: 'Баллы', type: 'number' },
      { id: 'passed', label: 'Пройдено', type: 'boolean' },
    ],
  });
  const parent = entity('source-parent', {
    typeId: 'process',
    kind: 'process',
    title: 'Процесс',
    plan: range('2026-10-05T08:00:00Z', '2026-10-05T09:00:00Z'),
  });
  const child = entity('source-child', {
    title: '=Проверка, "номер один"',
    description: 'Первая строка\nВторая строка, с запятой',
    typeId: 'personal-check',
    parentId: parent.id,
    participantIds: ['local-owner'],
    fields: { result: 'Ожидается', score: 7.5, passed: false, optional: null },
    tags: ['проект|этап', 'друзья, работа'],
    baseline: range('2026-10-04T08:00:00Z', '2026-10-04T09:00:00Z', 'UTC', 'approximate'),
    actual: range('2026-10-05T09:10:00Z'),
    forecast: range('2026-10-05T10:00:00Z', '2026-10-05T11:00:00Z'),
    forecastProvenance: 'derived',
    dueAt: '2026-10-05T12:00:00Z',
    allocations: [{ resourceId: 'source-resource', amount: 0.4 }],
    recurrence: {
      frequency: 'week',
      interval: 2,
      weekdays: [1, 3],
      count: 8,
      exceptions: ['2026-10-07'],
      calendarPolicy: 'skip-invalid',
      durationPolicy: 'elapsed',
    },
    links: [
      { id: 'web', label: 'Материалы', kind: 'url', url: 'https://example.test/material' },
      {
        id: 'file-link',
        label: 'Файл.pdf',
        kind: 'file',
        fileId: 'unavailable-file',
        url: '/api/files/unavailable-file',
      },
    ],
    source: {
      kind: 'webhook',
      label: 'Исходный источник',
      observedAt: '2026-10-01T08:00:00Z',
      receivedAt: NOW,
      staleAfterMinutes: 120,
    },
  });
  snapshot.entities = [parent, child];
  snapshot.resources = [
    {
      id: 'source-resource',
      workspaceId: 'personal',
      name: 'Общий ресурс',
      kind: 'equipment',
      capacity: 1,
      unit: 'шт.',
      timezone: 'UTC',
      workingWeekdays: [1, 2, 3, 4, 5],
    },
  ];
  snapshot.dependencies = [
    {
      id: 'source-dependency',
      workspaceId: 'personal',
      fromId: parent.id,
      toId: child.id,
      kind: 'finish-start',
      lagMinutes: 30,
    },
  ];
  return snapshot;
}
const invalid = (code?: string) => (error: unknown) =>
  error instanceof ApiError && error.status === 400 && (!code || error.code === code);
const calendar = (...lines: string[]) =>
  [
    'BEGIN:VCALENDAR',
    'VERSION:2.0',
    'BEGIN:VEVENT',
    ...lines,
    'END:VEVENT',
    'END:VCALENDAR',
    '',
  ].join('\r\n');
function imported(store: PlannerStore, content: string, timezone?: string) {
  if (timezone)
    store.change(store.localUser(), 'personal', 'workspace-test', '', (state) => {
      state.workspaces.find((w) => w.id === 'personal')!.timezone = timezone;
    });
  return importContent(store, store.localUser(), {
    workspaceId: 'personal',
    format: 'ics',
    content,
  }).entities.at(-1)!;
}
const instants = (entity: Entity, from: string, to: string) =>
  expandRecurrence(entity, from, to).map((item) => [
    DateTime.fromISO(item.start!).toMillis(),
    item.end ? DateTime.fromISO(item.end).toMillis() : null,
    item.index,
  ]);

for (const format of ['json', 'csv'])
  test(`${format.toUpperCase()} round trip preserves typed fields, facts, forecasts, baseline, recurrence and remaps context`, () => {
    const snapshot = richSnapshot(),
      exported = exportContent(snapshot, 'personal', format),
      store = fixture();
    try {
      const result = importContent(store, store.localUser(), {
        workspaceId: 'personal',
        format,
        content: exported.content,
      });
      const source = snapshot.entities[1]!,
        copy = result.entities.find((e) => e.title === source.title)!,
        parent = result.entities.find((e) => e.title === 'Процесс')!;
      assert.ok(copy);
      assert.notEqual(copy.id, source.id);
      assert.equal(copy.parentId, parent.id);
      for (const key of [
        'description',
        'kind',
        'baseline',
        'actual',
        'forecast',
        'forecastProvenance',
        'dueAt',
        'tags',
        'fields',
        'recurrence',
        'participantIds',
      ] as const)
        assert.deepEqual(copy[key], source[key], key);
      assert.deepEqual(copy.plan, source.plan);
      assert.notEqual(copy.typeId, source.typeId);
      assert.deepEqual(
        result.types.find((t) => t.id === copy.typeId)?.fields,
        snapshot.types.find((t) => t.id === source.typeId)?.fields,
      );
      assert.notEqual(copy.allocations[0]!.resourceId, 'source-resource');
      assert.equal(
        result.resources.find((r) => r.id === copy.allocations[0]!.resourceId)?.name,
        'Общий ресурс',
      );
      assert.ok(
        result.dependencies.some(
          (d) => d.fromId === parent.id && d.toId === copy.id && d.lagMinutes === 30,
        ),
      );
      assert.deepEqual(
        copy.links,
        source.links.filter((link) => link.kind === 'url'),
      );
      assert.ok(exported.warnings.some((warning) => warning.includes('вложения')));
      assert.ok(result.importWarnings.some((warning) => warning.includes('вложения')));
      assert.equal(copy.source.kind, 'import');
      assert.equal(copy.source.observedAt, source.source.observedAt);
      assert.equal(copy.source.staleAfterMinutes, 120);
      assert.equal(store.read().audit.at(-1)?.action, 'import');
    } finally {
      store.close();
    }
  });

test('CSV quoted multiline data and formula escaping are reversible without turning literals into formulas', () => {
  const snapshot = richSnapshot();
  snapshot.entities[0]!.title = "'=literal";
  const output = exportContent(snapshot, 'personal', 'csv'),
    rows = parseCsv(output.content);
  assert.equal(rows[1]!.title, '\'=Проверка, "номер один"');
  assert.equal(rows[1]!.escapedFields, 'title');
  assert.equal(rows[0]!.title, "'=literal");
  assert.equal(rows[0]!.escapedFields, '');
  assert.equal(rows[1]!.description, snapshot.entities[1]!.description);
  assert.ok(rows[0]!.workspaceData);
  assert.equal(rows[1]!.workspaceData, '');
  assert.throws(() => parseCsv('title,title\r\nx,y'), invalid());
  assert.throws(() => parseCsv('title,status\r\n"unfinished'), invalid());
});

test('plain CSV imports typed JSON fields and namespaced fields with explicit projection warnings', () => {
  const store = fixture();
  try {
    const result = importContent(store, store.localUser(), {
      workspaceId: 'personal',
      format: 'csv',
      content:
        'title,typeId,start,fields,field.location,unused\r\n"Встреча",event,2026-10-05,"{""count"":2,""confirmed"":true}",Уфа,omitted',
    });
    assert.deepEqual(result.entities[0]!.fields, { count: 2, confirmed: true, location: 'Уфа' });
    assert.ok(result.importWarnings.some((w) => w.includes('проекция')));
    assert.ok(result.importWarnings.some((w) => w.includes('unused')));
    const before = store.read();
    assert.throws(
      () =>
        importContent(store, store.localUser(), {
          workspaceId: 'personal',
          format: 'csv',
          content: 'title,fields\r\nBroken,"{oops"',
        }),
      invalid(),
    );
    assert.deepEqual(store.read(), before);
  } finally {
    store.close();
  }
});

test('JSON and full CSV preserve exact nanosecond decimal strings; ICS rejects the projection', () => {
  const start = '-31556889864403199999999999';
  const end = '-31556889864403199999999998';
  const precisePlan: TimeRange = {
    start: null,
    end: null,
    timezone: 'UTC',
    precision: 'exact',
    precise: {
      scale: 'unix-nanoseconds',
      start,
      end,
      resolutionNs: '1',
    },
  };
  const snapshot = richSnapshot();
  snapshot.dependencies = [];
  snapshot.entities = [
    entity('deep-time', {
      title: 'Exact deep-time interval',
      plan: precisePlan,
      baseline: structuredClone(precisePlan),
      recurrence: null,
    }),
    entity('negative-nanosecond', {
      title: 'One nanosecond before epoch',
      plan: {
        start: '1969-12-31T23:59:59.999999999Z',
        end: '1970-01-01T00:00:00.000000000Z',
        timezone: 'UTC',
        precision: 'exact',
        precise: { scale: 'unix-nanoseconds', start: '-1', end: '0', resolutionNs: '1' },
      },
      baseline: {
        start: '1969-12-31T23:59:59.999999999Z',
        end: '1970-01-01T00:00:00.000000000Z',
        timezone: 'UTC',
        precision: 'exact',
        precise: { scale: 'unix-nanoseconds', start: '-1', end: '0', resolutionNs: '1' },
      },
      recurrence: null,
    }),
  ];
  for (const format of ['json', 'csv'] as const) {
    const store = fixture();
    try {
      const exported = exportContent(snapshot, 'personal', format);
      const result = importContent(store, store.localUser(), {
        workspaceId: 'personal',
        format,
        content: exported.content,
      });
      const copy = result.entities[0]!;
      assert.equal(copy.plan.precise?.start, start);
      assert.equal(copy.plan.precise?.end, end);
      assert.equal(typeof copy.plan.precise?.start, 'string');
      assert.deepEqual(copy.baseline.precise, precisePlan.precise);
      const negative = result.entities.find(
        (candidate) => candidate.title === 'One nanosecond before epoch',
      )!;
      assert.equal(negative.plan.precise?.start, '-1');
      assert.equal(negative.plan.start, '1969-12-31T23:59:59.999999999Z');
      if (format === 'csv') {
        assert.ok(exported.warnings.some((warning) => warning.includes('decimal strings')));
        assert.equal(JSON.parse(parseCsv(exported.content)[0]!.precise).start, start);
      } else assert.equal(JSON.parse(exported.content).schemaVersion, 3);
    } finally {
      store.close();
    }
  }
  assert.throws(
    () => exportContent(snapshot, 'personal', 'ics'),
    invalid('ics_projection_unsupported'),
  );
});

test('ICS rejects non-zero subsecond plan, recurrence and typed field values without truncation', () => {
  const exactSecond = '2026-10-05T09:00:00Z';
  const subsecond = '2026-10-05T09:00:00.000000001Z';
  const cases: Array<(snapshot: PlannerSnapshot) => void> = [
    (snapshot) => {
      snapshot.entities = [entity('plan-subsecond', { plan: range(subsecond) })];
    },
    (snapshot) => {
      snapshot.entities = [
        entity('uncertainty-subsecond', {
          plan: { ...range(exactSecond), earliest: subsecond, latest: exactSecond },
        }),
      ];
    },
    (snapshot) => {
      snapshot.entities = [
        entity('until-subsecond', {
          recurrence: {
            frequency: 'day',
            interval: 1,
            until: subsecond,
            exceptions: [],
            calendarPolicy: 'skip-invalid',
          },
        }),
      ];
    },
    (snapshot) => {
      snapshot.entities = [
        entity('exception-subsecond', {
          recurrence: {
            frequency: 'day',
            interval: 1,
            exceptions: [subsecond],
            calendarPolicy: 'skip-invalid',
          },
        }),
      ];
    },
    (snapshot) => {
      snapshot.types.push({
        id: 'dated-type',
        workspaceId: 'personal',
        label: 'Dated',
        icon: 'circle',
        color: '#000000',
        kind: 'point',
        fields: [{ id: 'observed', label: 'Observed', type: 'date' }],
      });
      snapshot.entities = [
        entity('field-subsecond', {
          typeId: 'dated-type',
          kind: 'point',
          plan: range(exactSecond),
          fields: { observed: subsecond },
        }),
      ];
    },
  ];
  for (const arrange of cases) {
    const snapshot = richSnapshot();
    snapshot.dependencies = [];
    arrange(snapshot);
    assert.throws(
      () => exportContent(snapshot, 'personal', 'ics'),
      invalid('ics_projection_unsupported'),
    );
  }
  const wholeSecond = richSnapshot();
  wholeSecond.dependencies = [];
  wholeSecond.entities = [
    entity('whole-second', {
      plan: range('2026-10-05T09:00:00.000000000Z', '2026-10-05T09:00:01.000000000Z'),
    }),
  ];
  assert.match(exportContent(wholeSecond, 'personal', 'ics').content, /DTSTART:20261005T090000Z/);
});

test('ICS TZID and EXDATE preserve a local recurring clock across DST and count excludes exceptions', () => {
  const store = fixture();
  try {
    const event = imported(
      store,
      calendar(
        'UID:dst@example.test',
        'DTSTART;TZID="America/New_York":20260307T090000',
        'DTEND;TZID=America/New_York:20260307T100000',
        'RRULE:FREQ=DAILY;COUNT=4',
        'EXDATE;TZID=America/New_York:20260309T090000',
        'SUMMARY:Практика',
      ),
    );
    assert.equal(event.plan.timezone, 'America/New_York');
    assert.equal(event.recurrence?.calendarPolicy, 'skip-invalid');
    const occurrences = expandRecurrence(event, '2026-03-07', '2026-03-12');
    assert.deepEqual(
      occurrences.map((o) => o.start!.slice(0, 10)),
      ['2026-03-07', '2026-03-08', '2026-03-10'],
    );
    assert.deepEqual(
      occurrences.map((o) => DateTime.fromISO(o.start!, { zone: event.plan.timezone }).hour),
      [9, 9, 9],
    );
    assert.deepEqual(
      occurrences.map((o) => o.index),
      [0, 1, 3],
    );
    assert.equal(
      (DateTime.fromISO(occurrences[1]!.start!).toMillis() -
        DateTime.fromISO(occurrences[0]!.start!).toMillis()) /
        3600000,
      23,
    );
  } finally {
    store.close();
  }
});

test('ICS all-day DTSTART/DTEND, inclusive DATE UNTIL and DATE EXDATE retain local dates and DST duration', () => {
  const store = fixture();
  try {
    const event = imported(
      store,
      calendar(
        'DTSTART;VALUE=DATE:20260307',
        'DTEND;VALUE=DATE:20260308',
        'RRULE:FREQ=DAILY;UNTIL=20260310',
        'EXDATE;VALUE=DATE:20260309',
      ),
      'America/New_York',
    );
    assert.deepEqual(event.plan, range('2026-03-07', '2026-03-08', 'America/New_York', 'day'));
    assert.equal(event.recurrence?.until, '2026-03-10');
    assert.deepEqual(event.recurrence?.exceptions, ['2026-03-09']);
    const occurrences = expandRecurrence(event, '2026-03-07', '2026-03-12');
    assert.deepEqual(
      occurrences.map((o) => o.start!.slice(0, 10)),
      ['2026-03-07', '2026-03-08', '2026-03-10'],
    );
    assert.equal(
      (DateTime.fromISO(occurrences[1]!.end!).toMillis() -
        DateTime.fromISO(occurrences[1]!.start!).toMillis()) /
        3600000,
      23,
    );
    const oneDay = imported(store, calendar('DTSTART;VALUE=DATE:20260401'));
    assert.equal(oneDay.plan.end, '2026-04-02');
  } finally {
    store.close();
  }
});

test('ICS monthly COUNT ignores impossible dates and daily COUNT ignores a DST gap', () => {
  const store = fixture();
  try {
    const month = imported(
      store,
      calendar('DTSTART:20260131T090000Z', 'RRULE:FREQ=MONTHLY;COUNT=3', 'EXDATE:20260331T090000Z'),
    );
    assert.deepEqual(
      expandRecurrence(month, '2026-01-01', '2026-07-01').map((o) => [
        o.start!.slice(0, 10),
        o.index,
      ]),
      [
        ['2026-01-31', 0],
        ['2026-05-31', 2],
      ],
    );
    const daily = imported(
      store,
      calendar('DTSTART;TZID=America/New_York:20260307T023000', 'RRULE:FREQ=DAILY;COUNT=3'),
    );
    assert.deepEqual(
      expandRecurrence(daily, '2026-03-07', '2026-03-12').map((o) => [
        o.start!.slice(0, 10),
        o.index,
      ]),
      [
        ['2026-03-07', 0],
        ['2026-03-09', 1],
        ['2026-03-10', 2],
      ],
    );
  } finally {
    store.close();
  }
});

test('ICS weekly BYDAY INTERVAL and COUNT retain RFC ordering', () => {
  const store = fixture();
  try {
    const event = imported(
      store,
      calendar(
        'DTSTART:20261007T090000Z',
        'RRULE:FREQ=WEEKLY;INTERVAL=2;BYDAY=MO,WE,FR;COUNT=5;WKST=MO',
        'EXDATE:20261009T090000Z',
      ),
    );
    assert.deepEqual(
      expandRecurrence(event, '2026-10-01', '2026-11-01').map((o) => [
        o.start!.slice(0, 10),
        o.index,
      ]),
      [
        ['2026-10-07', 0],
        ['2026-10-19', 2],
        ['2026-10-21', 3],
        ['2026-10-23', 4],
      ],
    );
  } finally {
    store.close();
  }
});

test('ICS rejects unsupported rules and detached replacements atomically rather than dropping their meaning', () => {
  const store = fixture();
  try {
    const before = store.read();
    const rules = [
      'FREQ=YEARLY',
      'FREQ=MONTHLY;BYMONTHDAY=15,30',
      'FREQ=MONTHLY;BYDAY=1MO',
      'FREQ=DAILY;BYHOUR=9,10',
      'FREQ=WEEKLY;WKST=SU',
      'FREQ=DAILY;COUNT=0',
      'FREQ=DAILY;COUNT=2;UNTIL=20261010T090000Z',
      'FREQ=WEEKLY;BYDAY=TU',
    ];
    for (const rule of rules)
      assert.throws(
        () => imported(store, calendar('DTSTART:20261005T090000Z', `RRULE:${rule}`)),
        invalid(),
        rule,
      );
    for (const property of ['RDATE:20261006T090000Z', 'RECURRENCE-ID:20261006T090000Z'])
      assert.throws(
        () => imported(store, calendar('DTSTART:20261005T090000Z', property)),
        invalid('unsupported_recurrence'),
      );
    assert.throws(
      () =>
        imported(
          store,
          calendar('DTSTART:20261005T090000Z', 'RRULE:FREQ=DAILY;UNTIL=20261010T090000'),
        ),
      invalid(),
    );
    assert.deepEqual(store.read(), before);
  } finally {
    store.close();
  }
});

test('ICS rejects impossible DTSTART and mismatched values, resolves DST folds to their first instant', () => {
  for (const lines of [
    ['DTSTART:20260230T090000Z'],
    ['DTSTART;TZID=America/New_York:20260308T023000'],
    ['DTSTART;TZID=Unknown/Zone:20261005T090000'],
    ['DTSTART:20261005T090000Z', 'DTEND;VALUE=DATE:20261006'],
    ['DTSTART:20261005T090000Z', 'RRULE:FREQ=DAILY;COUNT=2', 'EXDATE;VALUE=DATE:20261006'],
    ['DTSTART:20261005T090000Z', 'DTSTART:20261006T090000Z'],
  ])
    assert.throws(() => parseIcs(calendar(...lines), 'personal', 'UTC'), invalid());
  const fold = parseIcs(
    calendar('DTSTART;TZID=America/New_York:20261101T013000'),
    'personal',
    'UTC',
  )[0]!.plan as TimeRange;
  assert.equal(DateTime.fromISO(fold.start!).toUTC().toISO(), '2026-11-01T05:30:00.000Z');
});

test('ICS export/reimport preserves supported TZID and all-day recurrence instants with explicit projection warnings', () => {
  const store = fixture(),
    importedStore = fixture();
  try {
    const timed = imported(
      store,
      calendar(
        'DTSTART;TZID=America/New_York:20260307T090000',
        'DTEND;TZID=America/New_York:20260307T100000',
        'RRULE:FREQ=DAILY;COUNT=4',
        'EXDATE;TZID=America/New_York:20260309T090000',
        'SUMMARY:Поездка\\, отдых',
        'DESCRIPTION:Строка 1\\nСтрока 2',
      ),
    );
    const output = exportContent(store.snapshot(store.localUser()), 'personal', 'ics');
    assert.match(output.content, /DTSTART;TZID=America\/New_York:20260307T090000/);
    assert.ok(output.warnings.some((w) => w.includes('базовый план')));
    const copied = imported(importedStore, output.content);
    assert.equal(copied.title, timed.title);
    assert.equal(copied.description, timed.description);
    assert.equal(copied.plan.timezone, timed.plan.timezone);
    assert.deepEqual(
      instants(copied, '2026-03-01', '2026-03-12'),
      instants(timed, '2026-03-01', '2026-03-12'),
    );
    for (const line of output.content.split('\r\n'))
      assert.ok(Buffer.byteLength(line, 'utf8') <= 75);
    const allDayStore = fixture();
    try {
      const allDay = imported(
        allDayStore,
        calendar(
          'DTSTART;VALUE=DATE:20260307',
          'DTEND;VALUE=DATE:20260308',
          'RRULE:FREQ=DAILY;UNTIL=20260310',
          'EXDATE;VALUE=DATE:20260309',
        ),
        'America/New_York',
      );
      const allDayExport = exportContent(
        allDayStore.snapshot(allDayStore.localUser()),
        'personal',
        'ics',
      );
      assert.match(allDayExport.content, /DTSTART;VALUE=DATE:20260307/);
      assert.match(allDayExport.content, /UNTIL=20260310/);
      const copy = imported(importedStore, allDayExport.content, 'America/New_York');
      assert.deepEqual(
        instants(copy, '2026-03-01', '2026-03-12'),
        instants(allDay, '2026-03-01', '2026-03-12'),
      );
    } finally {
      allDayStore.close();
    }
  } finally {
    store.close();
    importedStore.close();
  }
});

test('ICS refuses native adjust or uncertain schedules and warns about excluded undated objects', () => {
  const snapshot = richSnapshot();
  snapshot.entities = [
    entity('native', { recurrence: { frequency: 'month', interval: 1, count: 3, exceptions: [] } }),
  ];
  assert.throws(
    () => exportContent(snapshot, 'personal', 'ics'),
    invalid('ics_projection_unsupported'),
  );
  snapshot.entities = [
    entity('approximate', { plan: range('2026-10-05', '2026-10-06', 'UTC', 'approximate') }),
  ];
  assert.throws(
    () => exportContent(snapshot, 'personal', 'ics'),
    invalid('ics_projection_unsupported'),
  );
  snapshot.entities = [
    entity('dated'),
    entity('idea', { plan: range(null), typeId: 'note', kind: 'note' }),
  ];
  const output = exportContent(snapshot, 'personal', 'ics');
  assert.ok(output.warnings.some((w) => w.includes('1 объектов без даты')));
  assert.equal((output.content.match(/BEGIN:VEVENT/g) ?? []).length, 1);
});

test('floating ICS time and nested alarms produce warnings, while DURATION has calendar-day semantics', () => {
  const store = fixture();
  try {
    const result = importContent(store, store.localUser(), {
      workspaceId: 'personal',
      format: 'ics',
      content: calendar(
        'DTSTART:20260307T090000',
        'DURATION:PT30M',
        'BEGIN:VALARM',
        'TRIGGER:-PT15M',
        'ACTION:DISPLAY',
        'DESCRIPTION:Wake up',
        'END:VALARM',
      ),
    });
    const event = result.entities[0]!;
    assert.equal(
      (DateTime.fromISO(event.plan.end!).toMillis() -
        DateTime.fromISO(event.plan.start!).toMillis()) /
        60000,
      30,
    );
    assert.ok(result.importWarnings.some((w) => w.includes('без TZID')));
    assert.ok(result.importWarnings.some((w) => w.includes('VALARM')));
  } finally {
    store.close();
  }
});

test('ICS recurring DTEND preserves exact elapsed duration while DURATION preserves nominal calendar days across DST', () => {
  const store = fixture(),
    secondStore = fixture();
  try {
    const exact = imported(
      store,
      calendar(
        'DTSTART;TZID=America/New_York:20260306T090000',
        'DTEND;TZID=America/New_York:20260307T090000',
        'RRULE:FREQ=DAILY;COUNT=3',
      ),
    );
    const nominal = imported(
      store,
      calendar(
        'DTSTART;TZID=America/New_York:20260306T090000',
        'DURATION:P1D',
        'RRULE:FREQ=DAILY;COUNT=3',
      ),
    );
    assert.equal(exact.recurrence?.durationPolicy, 'elapsed');
    assert.equal(nominal.recurrence?.durationPolicy, 'calendar');
    const exactOccurrences = expandRecurrence(exact, '2026-03-06', '2026-03-10'),
      nominalOccurrences = expandRecurrence(nominal, '2026-03-06', '2026-03-10');
    assert.equal(
      DateTime.fromISO(exactOccurrences[1]!.end!, { zone: 'America/New_York' }).hour,
      10,
    );
    assert.equal(
      DateTime.fromISO(nominalOccurrences[1]!.end!, { zone: 'America/New_York' }).hour,
      9,
    );
    assert.equal(
      (DateTime.fromISO(exactOccurrences[1]!.end!).toMillis() -
        DateTime.fromISO(exactOccurrences[1]!.start!).toMillis()) /
        3600000,
      24,
    );
    assert.equal(
      (DateTime.fromISO(nominalOccurrences[1]!.end!).toMillis() -
        DateTime.fromISO(nominalOccurrences[1]!.start!).toMillis()) /
        3600000,
      23,
    );
    const output = exportContent(store.snapshot(store.localUser()), 'personal', 'ics');
    const copies = importContent(secondStore, secondStore.localUser(), {
      workspaceId: 'personal',
      format: 'ics',
      content: output.content,
    }).entities;
    assert.deepEqual(
      instants(copies[0]!, '2026-03-06', '2026-03-10'),
      instants(exact, '2026-03-06', '2026-03-10'),
    );
    assert.deepEqual(
      instants(copies[1]!, '2026-03-06', '2026-03-10'),
      instants(nominal, '2026-03-06', '2026-03-10'),
    );
  } finally {
    store.close();
    secondStore.close();
  }
});

test('ICS text escaping, categories ending in backslashes and Unicode folding survive export/reimport', () => {
  const store = fixture();
  try {
    const snapshot = store.read();
    const source = entity('escaped', {
      title: 'Поездка: ' + 'Я'.repeat(90),
      description: 'Буквальное \\n; запятая, и настоящий\nперенос',
      tags: ['папка\\', 'метка,с запятой', 'текст\\n'],
    });
    snapshot.entities = [source];
    const output = exportContent(snapshot, 'personal', 'ics');
    const copy = imported(store, output.content);
    assert.equal(copy.title, source.title);
    assert.equal(copy.description, source.description);
    assert.deepEqual(copy.tags, source.tags);
    for (const line of output.content.split('\r\n'))
      assert.ok(Buffer.byteLength(line, 'utf8') <= 75);
  } finally {
    store.close();
  }
});

test('ICS refuses ambiguous second DST-fold times and URL content-line injection', () => {
  const store = fixture();
  try {
    const snapshot = store.read();
    snapshot.entities = [
      entity('second-fold', {
        plan: range('2026-11-01T01:30:00-05:00', null, 'America/New_York'),
        kind: 'point',
        typeId: 'event',
      }),
    ];
    assert.throws(
      () => exportContent(snapshot, 'personal', 'ics'),
      invalid('ics_projection_unsupported'),
    );
    snapshot.entities = [
      entity('url', {
        links: [
          {
            id: 'bad-url',
            kind: 'url',
            label: 'Bad',
            url: 'https://example.test/\r\nBEGIN:VEVENT',
          },
        ],
      }),
    ];
    assert.throws(
      () => exportContent(snapshot, 'personal', 'ics'),
      invalid('ics_projection_unsupported'),
    );
  } finally {
    store.close();
  }
});
