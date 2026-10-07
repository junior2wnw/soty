import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  PlannerAgentService,
  normalizeAgentDate,
  plannerToolDefinitions,
} from './agent-service.ts';
import { PlannerStore } from './store.ts';
import { ApiError } from './validation.ts';
import { createSeed } from '../shared/seed.ts';
import type { PlannerSnapshot } from '../shared/types.ts';

function seed() {
  const s = createSeed('2026-10-02T10:00:00.000Z');
  for (const k of [
    'entities',
    'dependencies',
    'resources',
    'rules',
    'signals',
    'notifications',
    'scenarios',
    'audit',
    'comments',
  ] as const)
    s[k] = [];
  return s;
}
function fixture(s: PlannerSnapshot = seed()) {
  const store = new PlannerStore(':memory:', s);
  const user = store.localUser();
  const workspace = s.workspaces.find((w) =>
    s.memberships.some((m) => m.userId === user.id && m.workspaceId === w.id),
  )!;
  const service = new PlannerAgentService(store, user);
  const apply = (operations: unknown[], extra: Record<string, unknown> = {}) =>
    service.call('planner_apply', { requestId: randomUUID(), operations, ...extra }) as any;
  const create = (title = 'Task', data: Record<string, unknown> = {}) =>
    apply([{ op: 'create', workspace: workspace.id, key: 'new', data: { title, ...data } }]).refs
      .new as string;
  return { store, user, workspace, service, apply, create };
}
const code = (expected: string) => (error: unknown) =>
  error instanceof ApiError && error.code === expected;

test('MCP descriptors and help contain working compact examples; overview and note defaults', () => {
  const f = fixture();
  try {
    assert.equal(plannerToolDefinitions.length, 3);
    assert.ok(plannerToolDefinitions.every((t) => t.inputSchema.type === 'object'));
    assert.ok(
      !plannerToolDefinitions
        .find((t) => t.name === 'planner_read')!
        .inputSchema.required?.includes('limit'),
    );
    const help = f.service.call('planner_help') as any;
    assert.ok(help.examples.length);
    assert.match(help.rules.join(' '), /localPlan.*endInclusive.*exclusive boundary/);
    const id = f.create();
    const item = f.store.read().entities.find((e) => e.id === id)!;
    assert.equal(item.kind, 'note');
    assert.equal(item.plan.precision, 'unknown');
    const overview = f.service.call('planner_read', { collection: 'overview' }) as any;
    assert.equal(overview.workspaces[0].timezone, f.workspace.timezone);
    assert.ok(overview.types.some((t: any) => t.fields));
    assert.throws(() => f.service.call('__proto__'), code('unknown_tool'));
  } finally {
    f.store.close();
  }
});

test('compact MCP reads add local schedule labels without changing stored UTC ranges', () => {
  const f = fixture();
  try {
    const dayId = f.create('Local date period', {
      kind: 'period',
      start: '2027-06-05',
      end: '2027-06-11',
    });
    const timedId = f.create('Local timed period', {
      kind: 'period',
      timezone: 'America/New_York',
      precision: 'exact',
      start: '2027-11-01T09:00:00-04:00',
      end: '2027-11-01T10:30:00-04:00',
    });
    const middayId = f.create('Day precision with non-midnight end', {
      kind: 'period',
      plan: {
        start: '2027-06-05T09:00:00+05:00',
        end: '2027-06-05T11:00:00+05:00',
        timezone: f.workspace.timezone,
        precision: 'day',
      },
    });
    const undatedId = f.create('Still undated');
    const saved = f.store.read();
    const byId = new Map(saved.entities.map((item) => [item.id, item]));
    const read = (id: string) => (f.service.call('planner_read', { ids: [id] }) as any).items[0];

    const day = read(dayId);
    assert.deepEqual(day.plan, byId.get(dayId)!.plan);
    assert.deepEqual(day.localPlan, {
      start: '2027-06-05',
      endInclusive: '2027-06-10',
      timezone: f.workspace.timezone,
    });
    assert.equal(day.baseline, undefined);
    assert.equal(day.actual, undefined);
    const detail = (f.service.call('planner_read', { ids: [dayId], detail: true }) as any).items[0];
    const { localPlan, ...rawDetail } = detail;
    assert.deepEqual(localPlan, day.localPlan);
    assert.deepEqual(rawDetail, byId.get(dayId));
    assert.equal(detail.actual, null);

    const timed = read(timedId);
    assert.deepEqual(timed.plan, byId.get(timedId)!.plan);
    assert.deepEqual(timed.localPlan, {
      start: '2027-11-01T09:00:00-04:00',
      end: '2027-11-01T10:30:00-04:00',
      timezone: 'America/New_York',
    });

    const midday = read(middayId);
    assert.deepEqual(midday.plan, byId.get(middayId)!.plan);
    assert.deepEqual(midday.localPlan, {
      start: '2027-06-05T09:00:00+05:00',
      end: '2027-06-05T11:00:00+05:00',
      timezone: f.workspace.timezone,
    });

    assert.equal(read(undatedId).localPlan, undefined);
  } finally {
    f.store.close();
  }
});

test('MCP applies and reads exact nanosecond coordinates as decimal strings without Number coercion', () => {
  const f = fixture();
  try {
    const start = '1822694400123456789';
    const end = '1822694400123456790';
    const id = f.create('One nanosecond', {
      plan: {
        timezone: 'UTC',
        precision: 'exact',
        precise: {
          scale: 'unix-nanoseconds',
          start,
          end,
          resolutionNs: '1',
        },
      },
    });
    const read = (f.service.call('planner_read', { ids: [id], detail: true }) as any).items[0];
    assert.equal(read.plan.precise.start, start);
    assert.equal(read.plan.precise.end, end);
    assert.equal(typeof read.plan.precise.start, 'string');
    assert.equal(read.plan.start, '2027-10-05T00:00:00.123456789Z');
    assert.equal(read.plan.end, '2027-10-05T00:00:00.123456790Z');
    assert.equal(read.localPlan.start, read.plan.start);
    assert.equal(read.localPlan.end, read.plan.end);
    assert.deepEqual(read.baseline, read.plan);

    assert.throws(
      () =>
        f.create('Unsafe JSON number', {
          plan: {
            timezone: 'UTC',
            precision: 'exact',
            precise: { scale: 'unix-nanoseconds', start: 1822694400123456789, end: null },
          },
        }),
      code('invalid_precise_time'),
    );
  } finally {
    f.store.close();
  }
});

test('atomic batch creates friendly type, resource, object, dependency and comment refs', () => {
  const f = fixture();
  try {
    const response = f.apply([
      {
        op: 'create',
        collection: 'types',
        workspace: f.workspace.name,
        key: 'type',
        data: {
          label: 'Inspection',
          kind: 'point',
          fields: [
            {
              id: 'result',
              label: 'Result',
              type: 'select',
              required: true,
              options: ['pass', 'fail'],
            },
          ],
        },
      },
      {
        op: 'create',
        collection: 'resources',
        workspace: f.workspace.name,
        key: 'room',
        data: { name: 'Room', kind: 'place', capacity: 2 },
      },
      {
        op: 'create',
        key: 'first',
        workspace: f.workspace.name,
        data: {
          title: 'Inspection A',
          type: '$type',
          start: '2026-10-05',
          fields: { Result: 'pass' },
          resources: [{ resource: '$room', amount: 1 }],
          owner: f.user.displayName,
          participants: [f.user.id],
        },
      },
      {
        op: 'create',
        key: 'second',
        workspace: f.workspace.id,
        data: {
          title: 'Inspection B',
          type: '$type',
          start: '2026-10-06',
          fields: { Result: 'fail' },
          parent: '$first',
        },
      },
      {
        op: 'create',
        collection: 'dependencies',
        workspace: f.workspace.name,
        key: 'dep',
        data: { from: '$first', to: '$second' },
      },
      { op: 'comment', ref: '$first', data: { text: 'Ready' } },
    ]);
    const state = f.store.read();
    assert.equal(state.entities.length, 2);
    assert.equal(state.entities[0].fields.result, 'pass');
    assert.equal(state.entities[0].allocations[0].resourceId, response.refs.room);
    assert.equal(state.entities[1].parentId, response.refs.first);
    assert.equal(state.dependencies.length, 1);
    assert.equal(state.comments[0].entityId, response.refs.first);
    assert.equal(state.audit.length, 6);
    assert.equal(response.results[2].object.version, 1);
  } finally {
    f.store.close();
  }
});

test('invalid field and invalid select roll back all preceding operations and receipts', () => {
  const f = fixture();
  try {
    const before = f.store.read();
    const requestId = 'rollback';
    assert.throws(
      () =>
        f.apply(
          [
            {
              op: 'create',
              collection: 'resources',
              workspace: f.workspace.id,
              data: { name: 'Transient' },
            },
            {
              op: 'create',
              workspace: f.workspace.id,
              data: { title: 'Broken', type: 'metric', fields: { value: 'not a number' } },
            },
          ],
          { requestId },
        ),
      code('invalid_entity'),
    );
    assert.deepEqual(f.store.read(), before);
    assert.equal(f.store.db.prepare('SELECT COUNT(*) AS n FROM agent_requests').get()!.n, 0);
    assert.throws(
      () =>
        f.apply([
          {
            op: 'create',
            collection: 'types',
            workspace: f.workspace.id,
            key: 'type',
            data: {
              label: 'Choice',
              kind: 'note',
              fields: [
                { id: 'choice', label: 'Choice', type: 'select', required: true, options: ['yes'] },
              ],
            },
          },
          {
            op: 'create',
            workspace: f.workspace.id,
            data: { title: 'Broken', type: '$type', fields: { Choice: 'no' } },
          },
        ]),
      code('invalid_entity'),
    );
    assert.deepEqual(f.store.read(), before);
  } finally {
    f.store.close();
  }
});

test('durable idempotency replays without revision/event changes and rejects mismatch', () => {
  const dir = mkdtempSync(join(tmpdir(), 'planner-agent-test-'));
  const path = join(dir, 'planner.sqlite');
  let store = new PlannerStore(path, seed());
  try {
    const user = store.localUser();
    const ws = store.read().memberships.find((m) => m.userId === user.id)!.workspaceId;
    const args = {
      requestId: 'durable',
      operations: [{ op: 'create', workspace: ws, data: { title: 'Exactly once' } }],
    };
    const original = new PlannerAgentService(store, user).call('planner_apply', args);
    const revision = store.read().revision;
    store.close();
    store = new PlannerStore(path);
    let events = 0;
    store.on('change', () => events++);
    const service = new PlannerAgentService(store, user);
    assert.deepEqual(service.call('planner_apply', args), original);
    assert.equal(store.read().revision, revision);
    assert.equal(events, 0);
    assert.equal(store.read().entities.length, 1);
    assert.throws(
      () =>
        service.call('planner_apply', {
          ...args,
          operations: [{ op: 'create', workspace: ws, data: { title: 'Changed' } }],
        }),
      code('idempotency_mismatch'),
    );
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('ambiguous names produce choices; IDs are deterministic; local keys cannot overwrite', () => {
  const f = fixture();
  try {
    const a = f.create('Same');
    f.create('Same');
    assert.throws(
      () => f.service.call('planner_read', { workspace: f.workspace.id, ids: ['Same'] }),
      (e: any) => e.code === 'ambiguous_reference' && e.details.choices.length === 2,
    );
    const read = f.service.call('planner_read', { ids: [a] }) as any;
    assert.equal(read.items.length, 1);
    assert.equal(read.items[0].id, a);
    const revision = f.store.read().revision;
    assert.throws(
      () =>
        f.apply([
          { op: 'create', key: 'same', workspace: f.workspace.id, data: { title: 'First' } },
          { op: 'create', key: 'same', workspace: f.workspace.id, data: { title: 'Second' } },
        ]),
      code('invalid_key'),
    );
    assert.equal(f.store.read().revision, revision);
  } finally {
    f.store.close();
  }
});

test('object optimistic concurrency and immutable baseline preserve actual facts', () => {
  const f = fixture();
  try {
    const id = f.create('Work', {
      start: '2026-10-05',
      actual: { start: '2026-10-04' },
      forecast: { start: '2026-10-06' },
    });
    const original = f.store.read().entities[0];
    assert.throws(
      () => f.apply([{ op: 'update', ref: id, data: { title: 'Stale' } }]),
      code('version_conflict'),
    );
    const changed = f.apply([
      { op: 'update', ref: id, expectedVersion: 1, data: { start: '2026-10-07', title: 'Moved' } },
    ]).results[0].object;
    assert.equal(changed.version, 2);
    const updated = f.store.read().entities[0];
    assert.deepEqual(updated.baseline, original.baseline);
    assert.deepEqual(updated.actual, original.actual);
    assert.deepEqual(updated.forecast, original.forecast);
    assert.throws(
      () => f.apply([{ op: 'delete', ref: id, expectedVersion: 1 }]),
      code('version_conflict'),
    );
    assert.throws(
      () =>
        f.apply([
          {
            op: 'update',
            ref: id,
            expectedVersion: 2,
            data: { baseline: { start: '2020-01-01' } },
          },
        ]),
      code('unknown_fields'),
    );
    f.apply([{ op: 'update', ref: id, expectedVersion: 2, data: { status: 'done' } }]);
    assert.throws(
      () => f.apply([{ op: 'update', ref: id, expectedVersion: 3, data: { actual: null } }]),
      code('reason_required'),
    );
    f.apply([
      {
        op: 'update',
        ref: id,
        expectedVersion: 3,
        reason: 'Recorded in error',
        data: { actual: null },
      },
    ]);
    assert.equal(f.store.read().entities[0].actual, null);
    f.apply([{ op: 'delete', ref: id, expectedVersion: 4 }]);
    assert.equal(f.store.read().entities.length, 0);
  } finally {
    f.store.close();
  }
});

test('timezone date-only conversion and explicit-offset DST recurrence preview are real', () => {
  assert.equal(normalizeAgentDate('2026-03-07', 'America/New_York'), '2026-03-07T05:00:00.000Z');
  assert.equal(normalizeAgentDate('2026-03-09', 'America/New_York'), '2026-03-09T04:00:00.000Z');
  assert.throws(
    () => normalizeAgentDate('2026-03-08T02:30:00', 'America/New_York'),
    code('offset_required'),
  );
  assert.throws(() => normalizeAgentDate('2026-02-30', 'UTC'), code('invalid_date'));
  const s = seed();
  s.workspaces[0].timezone = 'America/New_York';
  const f = fixture(s);
  try {
    const id = f.create('Daily', {
      start: '2026-03-07T09:00:00-05:00',
      end: '2026-03-07T10:00:00-05:00',
      recurrence: { frequency: 'day', count: 3 },
    });
    const preview = f.service.call('planner_read', {
      collection: 'occurrences',
      workspace: f.workspace.id,
      ids: [id],
      from: '2026-03-07',
      to: '2026-03-10',
    }) as any;
    assert.equal(preview.items.length, 3);
    assert.equal(
      Date.parse(preview.items[1].start) - Date.parse(preview.items[0].start),
      23 * 3600000,
    );
    assert.equal(f.store.read().entities[0].kind, 'period');
    assert.throws(
      () => f.create('No offset', { start: '2026-11-01T01:30:00' }),
      code('offset_required'),
    );
  } finally {
    f.store.close();
  }
});

test('DAG cycles and cross-space references roll back the batch', () => {
  const f = fixture();
  try {
    const revision = f.store.read().revision;
    assert.throws(
      () =>
        f.apply([
          { op: 'create', workspace: f.workspace.id, key: 'a', data: { title: 'A' } },
          { op: 'create', workspace: f.workspace.id, key: 'b', data: { title: 'B' } },
          {
            op: 'create',
            collection: 'dependencies',
            workspace: f.workspace.id,
            data: { from: '$a', to: '$b' },
          },
          {
            op: 'create',
            collection: 'dependencies',
            workspace: f.workspace.id,
            data: { from: '$b', to: '$a' },
          },
        ]),
      code('invalid_dependency'),
    );
    assert.equal(f.store.read().revision, revision);
    assert.equal(f.store.read().entities.length, 0);
    const a = f.create('A');
    const b = f.create('B');
    f.apply([{ op: 'update', ref: a, expectedVersion: 1, data: { parent: b } }]);
    assert.throws(
      () => f.apply([{ op: 'update', ref: b, expectedVersion: 1, data: { parent: a } }]),
      code('invalid_entity'),
    );
    assert.throws(
      () => f.apply([{ op: 'delete', ref: b, expectedVersion: 1 }]),
      code('referenced_object'),
    );
  } finally {
    f.store.close();
  }
});

test('type updates revalidate required fields and revisions; references prevent deletion', () => {
  const f = fixture();
  try {
    const type = f.apply([
      {
        op: 'create',
        collection: 'types',
        workspace: f.workspace.id,
        data: { label: 'Custom', kind: 'note' },
      },
    ]).results[0].id;
    const id = f.create('Typed', { type });
    const revision = f.store.read().revision;
    assert.throws(
      () => f.apply([{ op: 'update', collection: 'types', ref: type, data: { label: 'Renamed' } }]),
      code('revision_required'),
    );
    assert.throws(
      () =>
        f.apply(
          [
            {
              op: 'update',
              collection: 'types',
              ref: type,
              data: {
                fields: [{ id: 'required', label: 'Required', type: 'text', required: true }],
              },
            },
          ],
          { expectedRevision: revision },
        ),
      code('invalid_entity'),
    );
    assert.equal(f.store.read().revision, revision);
    assert.throws(
      () =>
        f.apply([{ op: 'delete', collection: 'types', ref: type }], { expectedRevision: revision }),
      code('type_in_use'),
    );
    assert.throws(
      () =>
        f.apply([{ op: 'update', collection: 'types', ref: 'note', data: { label: 'Mutated' } }], {
          expectedRevision: revision,
        }),
      code('protected_definition'),
    );
    f.apply([{ op: 'update', collection: 'types', ref: type, data: { label: 'Renamed' } }], {
      expectedRevision: revision,
    });
    f.apply(
      [
        { op: 'delete', ref: id, expectedVersion: 1 },
        { op: 'delete', collection: 'types', ref: type },
      ],
      { expectedRevision: f.store.read().revision },
    );
    assert.ok(!f.store.read().types.some((t) => t.id === type));
  } finally {
    f.store.close();
  }
});

test('scope and ACL filter all collections and references without leaking hidden identities', () => {
  const s = seed();
  s.users.push({ id: 'hidden-person', displayName: 'Hidden Person' });
  s.workspaces.push({
    id: 'hidden-space',
    name: 'Secret',
    mode: 'team',
    timezone: 'UTC',
    createdAt: s.serverTime,
  });
  s.memberships.push({ workspaceId: 'hidden-space', userId: 'hidden-person', role: 'owner' });
  s.resources.push({
    id: 'hidden-resource',
    workspaceId: 'hidden-space',
    name: 'Secret room',
    kind: 'place',
    capacity: 1,
    unit: '',
    timezone: 'UTC',
    workingWeekdays: [1],
  });
  const f = fixture(s);
  try {
    const scoped = new PlannerAgentService(f.store, f.user, { workspaceIds: [f.workspace.id] });
    for (const collection of ['workspaces', 'resources', 'people'])
      assert.ok(!JSON.stringify(scoped.call('planner_read', { collection })).includes('hidden-'));
    assert.throws(() => scoped.resolveWorkspace('Secret'), code('not_found'));
    assert.throws(
      () =>
        scoped.call('planner_apply', {
          requestId: 'scope',
          operations: [
            {
              op: 'create',
              workspace: f.workspace.id,
              data: { title: 'Hidden ref', resources: ['hidden-resource'] },
            },
          ],
        }),
      code('not_found'),
    );
    const id = f.create();
    const readOnly = new PlannerAgentService(f.store, f.user, {
      workspaceIds: [f.workspace.id],
      readOnly: true,
    });
    assert.throws(
      () =>
        readOnly.call('planner_apply', {
          requestId: 'deny',
          operations: [{ op: 'delete', ref: id, expectedVersion: 1 }],
        }),
      code('read_only'),
    );
    const viewer = { id: 'viewer', displayName: 'Viewer' };
    f.store.transaction((state) => {
      state.users.push(viewer);
      state.memberships.push({ workspaceId: f.workspace.id, userId: viewer.id, role: 'viewer' });
    });
    const viewerService = new PlannerAgentService(f.store, viewer);
    assert.equal((viewerService.call('planner_read', { ids: [id] }) as any).items.length, 1);
    assert.throws(
      () =>
        viewerService.call('planner_apply', {
          requestId: 'deny',
          operations: [{ op: 'delete', ref: id, expectedVersion: 1 }],
        }),
      code('forbidden'),
    );
    const approver = { id: 'approver', displayName: 'Approver' };
    f.store.transaction((state) => {
      state.users.push(approver);
      state.memberships.push({
        workspaceId: f.workspace.id,
        userId: approver.id,
        role: 'approver',
      });
    });
    new PlannerAgentService(f.store, approver).assertWrite(f.workspace.id, ['owner', 'approver']);
  } finally {
    f.store.close();
  }
});

test('persistent idempotency never replays data after access is revoked', () => {
  const f = fixture();
  try {
    const args = {
      requestId: 'revoke',
      operations: [
        { op: 'create', workspace: f.workspace.id, data: { title: 'Visible before revoke' } },
      ],
    };
    f.service.call('planner_apply', args);
    f.store.transaction((s) => {
      s.memberships = s.memberships.filter(
        (m) => !(m.userId === f.user.id && m.workspaceId === f.workspace.id),
      );
    });
    assert.throws(() => f.service.call('planner_apply', args), code('not_found'));
  } finally {
    f.store.close();
  }
});

test('pagination rejects stale cursors; filters and versions stay compact', () => {
  const f = fixture();
  try {
    f.create('Alpha');
    f.create('Beta');
    const first = f.service.call('planner_read', { workspace: f.workspace.id, limit: 1 }) as any;
    assert.equal(first.items.length, 1);
    assert.ok(first.cursor);
    assert.equal(first.items[0].version, 1);
    assert.equal(first.items[0].baseline, undefined);
    const second = f.service.call('planner_read', {
      workspace: f.workspace.id,
      limit: 1,
      cursor: first.cursor,
    }) as any;
    assert.equal(second.items[0].title, 'Beta');
    assert.equal(second.cursor, null);
    f.create('Gamma');
    assert.throws(
      () =>
        f.service.call('planner_read', {
          workspace: f.workspace.id,
          limit: 1,
          cursor: first.cursor,
        }),
      code('stale_cursor'),
    );
    assert.equal((f.service.call('planner_read', { query: 'alp' }) as any).items[0].title, 'Alpha');
  } finally {
    f.store.close();
  }
});

test('notification settings and preceding object mutation roll back together', () => {
  const f = fixture();
  try {
    const prefs = f.store.preferences(f.user.id);
    const before = f.store.read();
    assert.throws(
      () =>
        f.apply(
          [
            { op: 'settings', data: { notifications: { repeatMinutes: 7 } } },
            { op: 'create', workspace: f.workspace.id, data: { title: '' } },
          ],
          { expectedRevision: before.revision },
        ),
      code('validation'),
    );
    assert.deepEqual(f.store.preferences(f.user.id), prefs);
    assert.deepEqual(f.store.read(), before);
    f.apply(
      [
        {
          op: 'settings',
          data: { notifications: { repeatMinutes: 7, timezone: 'America/New_York' } },
        },
      ],
      { expectedRevision: before.revision },
    );
    assert.equal(
      (f.service.call('planner_read', { collection: 'settings' }) as any).settings.notifications
        .repeatMinutes,
      7,
    );
    assert.throws(
      () =>
        f.apply([{ op: 'settings', data: { notifications: { repeatMinutes: 8 } } }], {
          expectedRevision: before.revision,
        }),
      code('revision_conflict'),
    );
  } finally {
    f.store.close();
  }
});

test('two database connections enforce revision and object-version concurrency', () => {
  const dir = mkdtempSync(join(tmpdir(), 'planner-agent-concurrency-'));
  const path = join(dir, 'planner.sqlite');
  const first = new PlannerStore(path, seed());
  const second = new PlannerStore(path);
  try {
    const user = first.localUser();
    const workspace = first.read().memberships.find((m) => m.userId === user.id)!.workspaceId;
    const a = new PlannerAgentService(first, user);
    const b = new PlannerAgentService(second, user);
    const readRevision = second.read().revision;
    const response = a.call('planner_apply', {
      requestId: 'writer-one',
      expectedRevision: readRevision,
      operations: [{ op: 'create', workspace, data: { title: 'Concurrent' } }],
    }) as any;
    assert.throws(
      () =>
        b.call('planner_apply', {
          requestId: 'writer-two',
          expectedRevision: readRevision,
          operations: [{ op: 'create', workspace, data: { title: 'Stale write' } }],
        }),
      code('revision_conflict'),
    );
    const id = response.results[0].id;
    a.call('planner_apply', {
      requestId: 'version-one',
      operations: [{ op: 'update', ref: id, expectedVersion: 1, data: { title: 'Updated' } }],
    });
    assert.throws(
      () =>
        b.call('planner_apply', {
          requestId: 'version-two',
          operations: [{ op: 'update', ref: id, expectedVersion: 1, data: { title: 'Stale' } }],
        }),
      code('version_conflict'),
    );
    assert.equal(second.read().entities[0].title, 'Updated');
    assert.equal(second.read().entities.length, 1);
  } finally {
    second.close();
    first.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('friendly metric inference and autogenerated type field IDs retain validation', () => {
  const f = fixture();
  try {
    const id = f.create('Temperature', { kind: 'metric', fields: { Значение: 17 } });
    assert.equal(f.store.read().entities.find((e) => e.id === id)!.typeId, 'metric');
    const result = f.apply([
      {
        op: 'create',
        collection: 'types',
        workspace: f.workspace.id,
        key: 'type',
        data: {
          label: 'Measurements',
          kind: 'metric',
          fields: [{ label: 'Amount', type: 'number', required: true }],
        },
      },
      {
        op: 'create',
        workspace: f.workspace.id,
        data: { title: 'Measured', type: '$type', fields: { Amount: 24 } },
      },
    ]);
    const type = f.store.read().types.find((t) => t.id === result.refs.type)!;
    assert.ok(type.fields[0].id);
    assert.equal(result.results[1].object.fields[type.fields[0].id], 24);
    assert.throws(
      () =>
        f.apply(
          Array.from({ length: 101 }, () => ({
            op: 'create',
            workspace: f.workspace.id,
            data: { title: 'Too much' },
          })),
        ),
      code('validation'),
    );
  } finally {
    f.store.close();
  }
});
