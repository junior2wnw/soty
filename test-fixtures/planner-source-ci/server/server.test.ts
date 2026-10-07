import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { request as httpRequest } from 'node:http';
import { mkdirSync, rmSync, readFileSync } from 'node:fs';
import { resolve, join, sep } from 'node:path';
import { PlannerStore } from './store.ts';
import { PlannerScheduler, quietUntil, publicAddress } from './scheduler.ts';
import { createPlannerServer } from './main.ts';
import { createSeed } from '../shared/seed.ts';
import { ApiError } from './validation.ts';
import { importContent } from './imports.ts';
import type { Entity, PlannerSnapshot, TimeRange, User } from '../shared/types.ts';
import { editableEntityPatch } from '../shared/entity-edit.ts';
import { draftOf } from '../src/utils.ts';

const at = '2026-10-02T10:00:00.000Z';
const time = (start: string, end: string | null = null): TimeRange => ({
  start,
  end,
  timezone: 'UTC',
  precision: 'exact',
});
function emptySeed() {
  const s = createSeed(at);
  s.entities = [];
  s.dependencies = [];
  s.resources = [];
  s.rules = [];
  s.signals = [];
  s.notifications = [];
  s.scenarios = [];
  s.audit = [];
  s.comments = [];
  return s;
}
function fixture() {
  const output = resolve('output', 'server-tests');
  const directory = join(output, randomUUID());
  mkdirSync(directory, { recursive: true });
  const path = join(directory, 'planner.sqlite');
  const store = new PlannerStore(path, emptySeed());
  return {
    store,
    path,
    directory,
    cleanup() {
      store.close();
      if (!resolve(directory).startsWith(output + sep)) throw new Error('Unsafe test cleanup path');
      rmSync(directory, { recursive: true, force: true });
    },
  };
}
function newEntity(
  store: PlannerStore,
  actor = store.localUser(),
  extra: Record<string, unknown> = {},
) {
  const snapshot = store.createEntity(actor, {
    workspaceId: 'personal',
    typeId: 'event',
    title: 'Test event',
    plan: time('2026-10-03T10:00:00Z'),
    ...extra,
  });
  return snapshot.entities.at(-1)!;
}
function error(status: number, code?: string) {
  return (e: unknown) => e instanceof ApiError && e.status === status && (!code || e.code === code);
}

test('a full editor draft can remove dates without patching protected context or factual layers', () => {
  const f = fixture();
  try {
    const actor = f.store.localUser();
    const entity = newEntity(f.store, actor, {
      actual: time('2026-10-03T10:05:00Z'),
      forecast: time('2026-10-03T11:00:00Z'),
    });
    const draft = draftOf(entity);
    draft.title = 'Undated idea';
    draft.plan = { start: null, end: null, timezone: 'UTC', precision: 'unknown' };
    const patch = editableEntityPatch(entity, draft);
    const next = f.store.patchEntity(actor, entity.id, { version: entity.version, patch });
    const updated = next.entities.find((e) => e.id === entity.id)!;
    assert.equal(updated.title, draft.title);
    assert.deepEqual(updated.plan, draft.plan);
    for (const key of [
      'workspaceId',
      'baseline',
      'source',
      'actual',
      'forecast',
      'forecastProvenance',
    ] as const)
      assert.deepEqual(updated[key], entity[key]);
    assert.equal(updated.version, entity.version + 1);
    assert.deepEqual(editableEntityPatch(updated, draftOf(updated)), {});
    assert.throws(
      () =>
        f.store.patchEntity(actor, entity.id, {
          version: updated.version,
          patch: { workspaceId: updated.workspaceId },
        }),
      error(400, 'protected_field'),
    );
  } finally {
    f.cleanup();
  }
});

test('SQLite restart preserves entities, audit, optimistic versions and session authentication', () => {
  const f = fixture();
  let reopened: PlannerStore | undefined;
  try {
    const entity = newEntity(f.store);
    f.store.patchEntity(f.store.localUser(), entity.id, {
      version: 1,
      patch: { title: 'Persisted title' },
    });
    const account = f.store.register({
      email: 'restart@example.test',
      password: 'test-password-123',
      displayName: 'Restart',
    });
    const token = account.token;
    f.store.close();
    reopened = new PlannerStore(f.path);
    assert.equal(
      reopened.read().entities.find((e) => e.id === entity.id)?.title,
      'Persisted title',
    );
    assert.equal(reopened.read().entities.find((e) => e.id === entity.id)?.version, 2);
    assert.equal(reopened.read().audit.filter((a) => a.entityId === entity.id).length, 2);
    assert.equal(reopened.session(token)?.id, account.user.id);
    assert.throws(
      () =>
        reopened!.patchEntity(reopened!.localUser(), entity.id, {
          version: 1,
          patch: { title: 'Lost edit' },
        }),
      error(409, 'version_conflict'),
    );
    reopened.close();
    reopened = undefined;
  } finally {
    if (reopened) reopened.close();
    const output = resolve('output', 'server-tests');
    assert.ok(f.directory.startsWith(output + sep));
    rmSync(f.directory, { recursive: true, force: true });
  }
});

test('workspace ACL isolates state, mutations, linked resources and history under CURRENT membership', () => {
  const f = fixture();
  try {
    const actor = f.store.localUser();
    const entity = newEntity(f.store);
    const foreign = f.store.register({
      email: 'foreign@example.test',
      password: 'test-password-123',
      displayName: 'Foreign',
    }).user;
    assert.equal(f.store.snapshot(foreign).entities.length, 0);
    assert.equal(f.store.snapshot(foreign).audit.length, 1);
    assert.throws(
      () => f.store.patchEntity(foreign, entity.id, { version: 1, patch: { title: 'Intrusion' } }),
      error(403),
    );
    assert.throws(
      () => f.store.createEntity(foreign, { workspaceId: 'personal', title: 'Intrusion' }),
      error(403),
    );
    f.store.membership(
      actor,
      { workspaceId: 'personal', userId: foreign.id, role: 'viewer' },
      'create',
    );
    assert.ok(f.store.snapshot(foreign).entities.some((e) => e.id === entity.id));
    assert.throws(
      () =>
        f.store.patchEntity(foreign, entity.id, { version: 1, patch: { title: 'Viewer write' } }),
      error(403),
    );
    const historyAt = new Date(Date.now() + 1000).toISOString();
    f.store.membership(actor, { workspaceId: 'personal', userId: foreign.id }, 'delete');
    assert.ok(!f.store.history(foreign, historyAt).entities.some((e) => e.id === entity.id));
    assert.ok(!f.store.snapshot(foreign).users.some((u) => u.id === actor.id));
    f.store.change(actor, 'team', 'resource-create', '', (state) => {
      state.resources.push({
        id: 'private-resource',
        workspaceId: 'team',
        name: 'Other team',
        kind: 'person',
        capacity: 1,
        unit: 'person',
        timezone: 'UTC',
        workingWeekdays: [1, 2, 3, 4, 5],
      });
    });
    assert.throws(
      () =>
        f.store.patchEntity(actor, entity.id, {
          version: 1,
          patch: { allocations: [{ resourceId: 'private-resource', amount: 1 }] },
        }),
      error(400),
    );
  } finally {
    f.cleanup();
  }
});

test('scenario approval preserves baseline and actual; stale entity edits and context edits cannot be approved', () => {
  const f = fixture();
  try {
    const actor = f.store.localUser();
    const entity = newEntity(
      f.store,
      { ...actor },
      {
        typeId: 'period',
        kind: 'period',
        plan: time('2026-10-03T10:00:00Z', '2026-10-03T11:00:00Z'),
      },
    );
    const baseline = structuredClone(entity.baseline);
    const state = f.store.createScenario(actor, {
      workspaceId: 'personal',
      name: 'Approved shift',
      changes: [
        { entityId: entity.id, plan: time('2026-10-04T10:00:00Z', '2026-10-04T11:00:00Z') },
      ],
    });
    const scenario = state.scenarios.at(-1)!;
    f.store.decideScenario(actor, scenario.id, 'submit');
    f.store.decideScenario(actor, scenario.id, 'approve');
    const updated = f.store.read().entities.find((e) => e.id === entity.id)!;
    assert.deepEqual(updated.baseline, baseline);
    assert.equal(updated.plan.start, '2026-10-04T10:00:00Z');
    assert.equal(updated.actual, null);
    const pending = f.store
      .createScenario(actor, {
        workspaceId: 'personal',
        changes: [
          { entityId: entity.id, plan: time('2026-10-05T10:00:00Z', '2026-10-05T11:00:00Z') },
        ],
      })
      .scenarios.at(-1)!;
    f.store.decideScenario(actor, pending.id, 'submit');
    f.store.patchEntity(actor, entity.id, {
      version: updated.version,
      patch: { description: 'Concurrent information' },
    });
    assert.equal(f.store.read().scenarios.find((s) => s.id === pending.id)?.state, 'stale');
    assert.throws(
      () => f.store.decideScenario(actor, pending.id, 'approve'),
      error(409, 'stale_scenario'),
    );
    const fresh = f.store
      .createScenario(actor, {
        workspaceId: 'personal',
        changes: [
          { entityId: entity.id, plan: time('2026-10-06T10:00:00Z', '2026-10-06T11:00:00Z') },
        ],
      })
      .scenarios.at(-1)!;
    f.store.decideScenario(actor, fresh.id, 'submit');
    f.store.change(actor, 'personal', 'resource-create', 'Calendar edit', (s) => {
      s.resources.push({
        id: 'new-capacity',
        workspaceId: 'personal',
        name: 'Capacity',
        kind: 'person',
        capacity: 1,
        unit: 'h',
        timezone: 'UTC',
        workingWeekdays: [1, 2, 3, 4, 5],
      });
    });
    assert.equal(f.store.read().scenarios.find((s) => s.id === fresh.id)?.state, 'stale');
    assert.throws(
      () => f.store.decideScenario(actor, fresh.id, 'approve'),
      error(409, 'stale_scenario'),
    );
    const newEntityContext = f.store
      .createScenario(actor, {
        workspaceId: 'personal',
        changes: [
          { entityId: entity.id, plan: time('2026-10-07T10:00:00Z', '2026-10-07T11:00:00Z') },
        ],
      })
      .scenarios.at(-1)!;
    newEntity(f.store);
    assert.equal(
      f.store.read().scenarios.find((s) => s.id === newEntityContext.id)?.state,
      'stale',
    );
    assert.throws(
      () =>
        f.store.patchEntity(actor, entity.id, {
          version: f.store.read().entities.find((e) => e.id === entity.id)!.version,
          patch: { baseline: time('2026-01-01') },
        }),
      error(400, 'protected_field'),
    );
    assert.throws(
      () =>
        f.store.createEntity(actor, {
          workspaceId: 'personal',
          typeId: 'event',
          title: 'Forged provider',
          source: { kind: 'webhook', label: 'Forged', observedAt: at, receivedAt: at },
        }),
      error(400, 'protected_source'),
    );
  } finally {
    f.cleanup();
  }
});

test('malformed JSON, invalid dependency cycles and invalid second item roll back complete import', () => {
  const f = fixture();
  try {
    const before = f.store.read();
    const actor = f.store.localUser();
    assert.throws(
      () =>
        importContent(f.store, actor, {
          workspaceId: 'personal',
          format: 'json',
          content: '{broken',
        }),
      error(400),
    );
    assert.throws(
      () =>
        importContent(f.store, actor, {
          workspaceId: 'personal',
          format: 'json',
          content: {
            entities: [
              { id: 'a', title: 'Valid', typeId: 'event', plan: time('2026-10-04') },
              {
                id: 'b',
                title: 'Invalid',
                typeId: 'period',
                kind: 'period',
                plan: time('2026-10-06', '2026-10-01'),
              },
            ],
          },
        }),
      error(400),
    );
    assert.deepEqual(f.store.read(), before);
    assert.throws(
      () =>
        importContent(f.store, actor, {
          workspaceId: 'personal',
          format: 'json',
          content: {
            entities: [
              { id: 'a', title: 'A', typeId: 'event', plan: time('2026-10-04') },
              { id: 'b', title: 'B', typeId: 'event', plan: time('2026-10-05') },
            ],
            dependencies: [
              { id: 'ab', fromId: 'a', toId: 'b', kind: 'finish-start', lagMinutes: 0 },
              { id: 'ba', fromId: 'b', toId: 'a', kind: 'finish-start', lagMinutes: 0 },
            ],
          },
        }),
      error(400),
    );
    assert.deepEqual(f.store.read(), before);
  } finally {
    f.cleanup();
  }
});

test('scheduler durable rule ledger prevents duplicate followups across repeated ticks and restart; acknowledgement is not resolution', () => {
  const f = fixture();
  let reopened: PlannerStore | undefined;
  try {
    const actor = f.store.localUser();
    const entity = newEntity(f.store, actor, { dueAt: '2026-10-02T09:00:00Z' });
    f.store.change(actor, 'personal', 'rule-create', '', (s) => {
      s.rules.push({
        id: 'followup',
        workspaceId: 'personal',
        name: 'After overdue',
        enabled: true,
        trigger: 'overdue',
        leadMinutes: 0,
        action: 'create-followup',
        followupTitle: 'Contact owner',
        ownerId: actor.id,
      });
    });
    const scheduler = new PlannerScheduler(f.store);
    scheduler.tick(at);
    const first = f.store.read();
    const followup = first.entities.find((e) => e.title === 'Contact owner')!;
    assert.ok(followup);
    const risk = first.signals.find((s) => s.entityId === entity.id && s.kind === 'overdue')!;
    assert.ok(risk);
    const beforeCount = first.notifications.length;
    scheduler.tick(at);
    assert.equal(f.store.read().entities.filter((e) => e.title === 'Contact owner').length, 1);
    assert.equal(f.store.read().notifications.length, beforeCount);
    f.store.change(actor, 'personal', 'signal-ack', '', (s) => {
      const signal = s.signals.find((x) => x.id === risk.id)!;
      signal.state = 'acknowledged';
      signal.updatedAt = at;
      signal.acknowledgedBy = actor.id;
    });
    scheduler.tick('2026-10-02T10:01:00.000Z');
    assert.equal(f.store.read().signals.find((s) => s.id === risk.id)?.state, 'acknowledged');
    assert.equal(f.store.read().notifications.length, beforeCount);
    f.store.close();
    reopened = new PlannerStore(f.path);
    new PlannerScheduler(reopened).tick('2026-10-02T10:02:00.000Z');
    assert.equal(reopened.read().entities.filter((e) => e.title === 'Contact owner').length, 1);
    const original = reopened.read().entities.find((e) => e.id === entity.id)!;
    reopened.patchEntity(reopened.localUser(), entity.id, {
      version: original.version,
      patch: { status: 'done', actual: time('2026-10-02T09:30:00Z', '2026-10-02T10:03:00Z') },
    });
    new PlannerScheduler(reopened).tick('2026-10-02T10:04:00.000Z');
    assert.equal(reopened.read().signals.find((s) => s.id === risk.id)?.state, 'resolved');
    reopened.close();
    reopened = undefined;
  } finally {
    if (reopened) reopened.close();
    assert.ok(f.directory.startsWith(resolve('output', 'server-tests') + sep));
    rmSync(f.directory, { recursive: true, force: true });
  }
});

test('valid undated notes remain visible without notification or escalation noise', () => {
  const f = fixture();
  try {
    newEntity(f.store, f.store.localUser(), {
      typeId: 'note',
      kind: 'note',
      status: 'draft',
      plan: { start: null, end: null, timezone: 'UTC', precision: 'unknown' },
    });
    const scheduler = new PlannerScheduler(f.store);
    scheduler.tick(at);
    scheduler.tick('2026-10-03T10:00:00.000Z');
    assert.ok(f.store.read().signals.some((s) => s.kind === 'missing-date'));
    assert.equal(f.store.read().notifications.length, 0);
  } finally {
    f.cleanup();
  }
});

test('invitations require one-use owner codes and keep pending identities isolated per workspace', () => {
  const f = fixture();
  try {
    const actor = f.store.localUser();
    const response = f.store.membership(
      actor,
      { workspaceId: 'personal', email: 'pending@example.test', role: 'editor' },
      'create',
    );
    assert.ok(response.invitation);
    const invitation = response.invitation!;
    assert.throws(
      () =>
        f.store.register({
          email: invitation.email,
          password: 'test-password-123',
          displayName: 'Invited',
        }),
      error(403, 'invitation_required'),
    );
    assert.throws(
      () =>
        f.store.register({
          email: invitation.email,
          password: 'test-password-123',
          displayName: 'Invited',
          invitationToken: 'wrong-code',
        }),
      error(403, 'invitation_required'),
    );
    const other = f.store.membership(
      actor,
      { workspaceId: 'team', email: invitation.email, role: 'approver' },
      'create',
    ).invitation!;
    assert.notEqual(other.userId, invitation.userId);
    assert.equal(other.workspaceId, 'team');
    assert.equal(f.store.user(other.userId)?.pending, true);
    assert.throws(() => f.store.invitation(actor, 'team', invitation.userId), error(404));
    assert.ok(!JSON.stringify(f.store.read()).includes(invitation.token));
    assert.ok(
      !JSON.stringify(f.store.db.prepare('SELECT token_hash FROM invitations').all()).includes(
        invitation.token,
      ),
    );
    const account = f.store.register({
      email: invitation.email,
      password: 'test-password-123',
      displayName: 'Invited',
      invitationToken: invitation.token,
    });
    assert.equal(account.user.id, invitation.userId);
    assert.equal(account.user.pending, undefined);
    assert.equal(f.store.role(account.user.id, 'personal'), 'editor');
    assert.equal(f.store.role(account.user.id, 'team'), undefined);
    assert.equal(
      f.store.db.prepare('SELECT user_id FROM invitations WHERE user_id=?').get(account.user.id),
      undefined,
    );
    assert.throws(
      () =>
        f.store.register({
          email: invitation.email,
          password: 'test-password-123',
          displayName: 'Replay',
          invitationToken: invitation.token,
        }),
      error(409, 'account_exists'),
    );
    const teamEntity = newEntity(f.store, actor, {
      workspaceId: 'team',
      ownerId: other.userId,
      participantIds: [other.userId],
    });
    assert.throws(
      () => f.store.acceptInvitation(account.user, { invitationToken: invitation.token }),
      error(403, 'invitation_required'),
    );
    const accepted = f.store.acceptInvitation(account.user, { invitationToken: other.token });
    assert.equal(f.store.role(account.user.id, 'team'), 'approver');
    assert.equal(f.store.user(other.userId), undefined);
    const assigned = accepted.entities.find((e) => e.id === teamEntity.id)!;
    assert.equal(assigned.ownerId, account.user.id);
    assert.deepEqual(assigned.participantIds, [account.user.id]);
    assert.equal(assigned.version, teamEntity.version + 1);
    assert.throws(
      () => f.store.acceptInvitation(account.user, { invitationToken: other.token }),
      error(403, 'invitation_required'),
    );
  } finally {
    f.cleanup();
  }
});

test('registered email invitations remain pending until code acceptance; rotation, expiry and revocation invalidate codes', () => {
  const f = fixture();
  try {
    const owner = f.store.localUser();
    const account = f.store.register({
      email: 'registered@example.test',
      password: 'test-password-123',
      displayName: 'Registered',
    }).user;
    const first = f.store.membership(
      owner,
      { workspaceId: 'personal', email: account.email, role: 'viewer' },
      'create',
    ).invitation!;
    assert.notEqual(first.userId, account.id);
    assert.equal(f.store.role(account.id, 'personal'), undefined);
    assert.equal(f.store.snapshot(account).entities.length, 0);
    const stranger = f.store.register({
      email: 'stranger@example.test',
      password: 'test-password-123',
      displayName: 'Stranger',
    }).user;
    assert.throws(
      () => f.store.acceptInvitation(stranger, { invitationToken: first.token }),
      error(403, 'invitation_required'),
    );
    assert.throws(
      () => f.store.acceptInvitation(owner, { invitationToken: first.token }),
      error(401),
    );
    const rotated = f.store.invitation(owner, 'personal', first.userId);
    assert.notEqual(rotated.token, first.token);
    assert.throws(
      () => f.store.acceptInvitation(account, { invitationToken: first.token }),
      error(403, 'invitation_required'),
    );
    f.store.db
      .prepare('UPDATE invitations SET expires_at=? WHERE user_id=?')
      .run('2020-01-01T00:00:00Z', first.userId);
    assert.throws(
      () => f.store.acceptInvitation(account, { invitationToken: rotated.token }),
      error(403, 'invitation_required'),
    );
    const live = f.store.invitation(owner, 'personal', first.userId);
    f.store.membership(owner, { workspaceId: 'personal', userId: first.userId }, 'delete');
    assert.throws(
      () => f.store.acceptInvitation(account, { invitationToken: live.token }),
      error(403, 'invitation_required'),
    );
    assert.equal(f.store.user(first.userId), undefined);
    assert.ok(!JSON.stringify(f.store.read()).includes(live.token));
    const pendingOwner = f.store.membership(
      owner,
      { workspaceId: 'personal', email: 'pending-owner@example.test', role: 'owner' },
      'create',
    ).invitation!;
    assert.throws(
      () => f.store.membership(owner, { workspaceId: 'personal', userId: owner.id }, 'delete'),
      error(409),
    );
    f.store.membership(owner, { workspaceId: 'personal', userId: pendingOwner.userId }, 'delete');
  } finally {
    f.cleanup();
  }
});

test('scheduler derives dependency forecasts, returns to plan when delay clears, and preserves manual estimates and facts', () => {
  const f = fixture();
  try {
    const owner = f.store.localUser();
    const upstream = newEntity(f.store, owner, {
      typeId: 'period',
      kind: 'period',
      plan: time('2026-10-03T10:00:00Z', '2026-10-03T11:00:00Z'),
      forecast: time('2026-10-05T10:00:00Z', '2026-10-05T11:00:00Z'),
    });
    const downstream = newEntity(f.store, owner, {
      typeId: 'period',
      kind: 'period',
      plan: time('2026-10-03T11:00:00Z', '2026-10-03T12:00:00Z'),
    });
    const manual = newEntity(f.store, owner, {
      typeId: 'period',
      kind: 'period',
      plan: time('2026-10-03T12:00:00Z', '2026-10-03T13:00:00Z'),
      forecast: time('2026-10-04T12:00:00Z', '2026-10-04T13:00:00Z'),
    });
    const done = newEntity(f.store, owner, {
      typeId: 'period',
      kind: 'period',
      plan: time('2026-10-03T13:00:00Z', '2026-10-03T14:00:00Z'),
      actual: time('2026-10-03T13:01:00Z', '2026-10-03T14:01:00Z'),
      status: 'done',
    });
    f.store.change(owner, 'personal', 'dependency-create', '', (s) => {
      s.dependencies.push(
        {
          id: 'derived-link',
          workspaceId: 'personal',
          fromId: upstream.id,
          toId: downstream.id,
          kind: 'finish-start',
          lagMinutes: 0,
        },
        {
          id: 'manual-link',
          workspaceId: 'personal',
          fromId: upstream.id,
          toId: manual.id,
          kind: 'finish-start',
          lagMinutes: 0,
        },
        {
          id: 'done-link',
          workspaceId: 'personal',
          fromId: upstream.id,
          toId: done.id,
          kind: 'finish-start',
          lagMinutes: 0,
        },
      );
    });
    const scheduler = new PlannerScheduler(f.store);
    scheduler.tick(at);
    const shifted = f.store.read().entities.find((e) => e.id === downstream.id)!;
    assert.equal(shifted.forecastProvenance, 'derived');
    assert.equal(shifted.forecast!.start, '2026-10-05T11:00:00.000Z');
    assert.deepEqual(shifted.plan, downstream.plan);
    assert.deepEqual(shifted.baseline, downstream.baseline);
    assert.deepEqual(
      f.store.read().entities.find((e) => e.id === manual.id)!.forecast,
      manual.forecast,
    );
    assert.deepEqual(f.store.read().entities.find((e) => e.id === done.id)!.actual, done.actual);
    f.store.patchEntity(owner, upstream.id, {
      version: upstream.version,
      patch: { forecast: null },
    });
    scheduler.tick(at);
    const cleared = f.store.read().entities.find((e) => e.id === downstream.id)!;
    assert.equal(cleared.forecast, null);
    assert.equal(cleared.forecastProvenance, undefined);
    const revision = f.store.read().revision;
    scheduler.tick(at);
    assert.equal(f.store.read().revision, revision);
  } finally {
    f.cleanup();
  }
});

test('quiet hours respect timezone and wrap midnight, and browser outbox is deferred while in-app stays visible', () => {
  const f = fixture();
  try {
    const actor = f.store.localUser();
    f.store.updateSettings(actor, {
      notifications: {
        quietStart: '22:00',
        quietEnd: '08:00',
        timezone: 'Asia/Yekaterinburg',
        browserEnabled: true,
        repeatMinutes: 60,
        escalationMinutes: 60,
      },
    });
    newEntity(f.store, actor, { dueAt: '2026-10-02T18:00:00Z' });
    new PlannerScheduler(f.store).tick('2026-10-02T19:00:00Z');
    const notifications = f.store.read().notifications;
    assert.ok(notifications.some((n) => n.channel === 'in-app' && n.state === 'delivered'));
    assert.ok(
      notifications.some(
        (n) => n.channel === 'browser' && n.scheduledAt === '2026-10-03T03:00:00.000Z',
      ),
    );
    const prefs = f.store.preferences(actor.id).notifications;
    assert.equal(quietUntil('2026-10-03T04:00:00Z', prefs), null);
    assert.equal(publicAddress('127.0.0.1'), false);
    assert.equal(publicAddress('169.254.169.254'), false);
    assert.equal(publicAddress('8.8.8.8'), true);
  } finally {
    f.cleanup();
  }
});

test('notification outbox persists across restart and failed webhook delivery keeps a bounded retry schedule', async () => {
  const f = fixture();
  let reopened: PlannerStore | undefined;
  try {
    const owner = f.store.localUser();
    f.store.updateSettings(owner, {
      notifications: {
        browserEnabled: true,
        quietStart: '00:00',
        quietEnd: '00:00',
        timezone: 'UTC',
        repeatMinutes: 60,
        escalationMinutes: 60,
      },
    });
    f.store.db
      .prepare(
        'INSERT INTO webhook_connections(workspace_id,endpoint,enabled,status,updated_at) VALUES(?,?,?,?,?)',
      )
      .run('personal', 'https://127.0.0.1', 1, 'configured', at);
    newEntity(f.store, owner, { dueAt: '2026-10-02T09:00:00Z' });
    new PlannerScheduler(f.store).tick(at);
    const notifications = f.store.read().notifications;
    const webhook = notifications.find((n) => n.channel === 'webhook')!;
    const browser = notifications.find((n) => n.channel === 'browser')!;
    assert.ok(webhook);
    assert.ok(browser);
    assert.equal(webhook.state, 'pending');
    f.store.close();
    reopened = new PlannerStore(f.path);
    const scheduler = new PlannerScheduler(reopened);
    assert.equal(reopened.read().notifications.find((n) => n.id === browser.id)?.state, 'pending');
    const retryAt = at;
    await scheduler.deliverWebhooks(retryAt);
    const failed = reopened.read().notifications.find((n) => n.id === webhook.id)!;
    assert.equal(failed.state, 'failed');
    assert.equal(failed.attempts, 1);
    assert.ok(Date.parse(failed.scheduledAt) > Date.parse(retryAt));
    await scheduler.deliverWebhooks(retryAt);
    assert.equal(reopened.read().notifications.find((n) => n.id === webhook.id)!.attempts, 1);
    reopened.close();
    reopened = new PlannerStore(f.path);
    assert.equal(
      reopened.read().notifications.find((n) => n.id === webhook.id)?.scheduledAt,
      failed.scheduledAt,
    );
    reopened.close();
    reopened = undefined;
  } finally {
    if (reopened) reopened.close();
    assert.ok(f.directory.startsWith(resolve('output', 'server-tests') + sep));
    rmSync(f.directory, { recursive: true, force: true });
  }
});

test('webhook address guard rejects mapped loopback, transition and malformed IPv6 addresses', () => {
  assert.equal(publicAddress('::ffff:7f00:1'), false);
  assert.equal(publicAddress('::ffff:127.0.0.1'), false);
  assert.equal(publicAddress('64:ff9b::a00:1'), false);
  assert.equal(publicAddress('2002:7f00:1::'), false);
  assert.equal(publicAddress('2001:db8::1'), false);
  assert.equal(publicAddress('invalid:address'), false);
  assert.equal(publicAddress('2001:4860:4860::8888'), true);
});

test('HTTP public binding requires auth, rejects CSRF, scopes viewer writes/history/files, and stores passwords as hashes', async () => {
  const output = resolve('output', 'server-tests'),
    directory = join(output, randomUUID());
  mkdirSync(directory, { recursive: true });
  const app = await createPlannerServer({
    dbPath: join(directory, 'planner.sqlite'),
    host: '0.0.0.0',
    port: 0,
    development: false,
    scheduler: false,
  });
  const port = await app.listen();
  const origin = `http://127.0.0.1:${port}`;
  const call = async (
    path: string,
    method = 'GET',
    value?: unknown,
    cookie?: string,
    originHeader = origin,
  ) => {
    const headers: Record<string, string> = { Origin: originHeader };
    if (cookie) headers.Cookie = cookie;
    if (value !== undefined) headers['Content-Type'] = 'application/json';
    return fetch(origin + path, {
      method,
      headers,
      body: value === undefined ? undefined : JSON.stringify(value),
    });
  };
  try {
    assert.equal((await call('/api/state')).status, 401);
    assert.equal((await call('/health')).status, 200);
    assert.equal(
      (
        await call(
          '/api/auth/register',
          'POST',
          { email: 'owner-http@example.test', displayName: 'Owner', password: 'test-password-123' },
          undefined,
          'https://hostile.example',
        )
      ).status,
      403,
    );
    const registration = await call('/api/auth/register', 'POST', {
      email: 'owner-http@example.test',
      displayName: 'Owner',
      password: 'test-password-123',
    });
    assert.equal(registration.status, 201);
    const ownerCookie = registration.headers.get('set-cookie')!.split(';')[0];
    const ownerState = (await registration.json()) as PlannerSnapshot;
    const workspaceId = ownerState.workspaces[0].id;
    assert.equal(ownerState.entities.length, 0);
    assert.equal(ownerState.user.local, undefined);
    const credentials = app.store.db
      .prepare('SELECT salt,password_hash FROM credentials')
      .get() as { salt: string; password_hash: string };
    assert.equal(credentials.password_hash.length, 128);
    assert.ok(!JSON.stringify(credentials).includes('test-password-123'));
    const entityResponse = await call(
      '/api/entities',
      'POST',
      {
        workspaceId,
        typeId: 'event',
        title: 'Private HTTP object',
        plan: time('2026-10-03T10:00:00Z'),
      },
      ownerCookie,
    );
    assert.equal(entityResponse.status, 201);
    const ownerEntity = ((await entityResponse.json()) as PlannerSnapshot).entities[0];
    const viewerRegistration = await call('/api/auth/register', 'POST', {
      email: 'viewer-http@example.test',
      displayName: 'Viewer',
      password: 'test-password-123',
    });
    const viewerCookie = viewerRegistration.headers.get('set-cookie')!.split(';')[0];
    const viewerState = (await viewerRegistration.json()) as PlannerSnapshot;
    assert.equal(
      (
        await call(
          `/api/entities/${ownerEntity.id}`,
          'PATCH',
          { version: 1, patch: { title: 'Intrusion' } },
          viewerCookie,
        )
      ).status,
      403,
    );
    assert.equal(
      (
        await call(
          '/api/memberships',
          'POST',
          { workspaceId, userId: viewerState.user.id, role: 'viewer' },
          ownerCookie,
        )
      ).status,
      200,
    );
    const afterInvite = (await (
      await call('/api/state', 'GET', undefined, viewerCookie)
    ).json()) as PlannerSnapshot;
    assert.ok(afterInvite.entities.some((e) => e.id === ownerEntity.id));
    assert.equal(
      (
        await call(
          `/api/entities/${ownerEntity.id}`,
          'PATCH',
          { version: 1, patch: { title: 'Viewer cannot edit' } },
          viewerCookie,
        )
      ).status,
      403,
    );
    const form = new FormData();
    form.append('workspaceId', workspaceId);
    form.append('entityId', ownerEntity.id);
    form.append('file', new Blob(['real attachment bytes'], { type: 'text/plain' }), 'notes.txt');
    const upload = await fetch(origin + '/api/files', {
      method: 'POST',
      headers: { Origin: origin, Cookie: ownerCookie },
      body: form,
    });
    assert.equal(upload.status, 201);
    const file = (await upload.json()) as { id: string; url: string };
    assert.equal(
      await (await call(file.url, 'GET', undefined, viewerCookie)).text(),
      'real attachment bytes',
    );
    assert.equal(
      (
        await call(
          '/api/memberships',
          'DELETE',
          { workspaceId, userId: viewerState.user.id },
          ownerCookie,
        )
      ).status,
      200,
    );
    assert.equal((await call(file.url, 'GET', undefined, viewerCookie)).status, 403);
    const old = (await (
      await call(
        `/api/history?at=${encodeURIComponent(new Date(Date.now() + 1000).toISOString())}`,
        'GET',
        undefined,
        viewerCookie,
      )
    ).json()) as PlannerSnapshot;
    assert.ok(!old.entities.some((e) => e.id === ownerEntity.id));
    const login = await call('/api/auth/login', 'POST', {
      email: 'viewer-http@example.test',
      password: 'test-password-123',
    });
    assert.equal(login.status, 200);
    const logged = (await login.json()) as PlannerSnapshot;
    assert.equal(logged.user.id, viewerState.user.id);
    assert.ok(!logged.entities.some((e) => e.id === ownerEntity.id));
    assert.equal(
      (
        await call('/api/auth/login', 'POST', {
          email: 'viewer-http@example.test',
          password: 'wrong-password',
        })
      ).status,
      401,
    );
    assert.equal(
      (
        await call(
          `/api/entities/${ownerEntity.id}`,
          'PATCH',
          { version: 0, patch: { title: 'Lost edit' } },
          ownerCookie,
        )
      ).status,
      409,
    );
    const invitationResponse = await call(
      '/api/memberships',
      'POST',
      { workspaceId, email: viewerState.user.email, role: 'approver' },
      ownerCookie,
    );
    assert.equal(invitationResponse.status, 200);
    const invited = (await invitationResponse.json()) as PlannerSnapshot & {
      invitation: { token: string; userId: string; workspaceId: string };
    };
    assert.equal(invited.users.find((u) => u.id === invited.invitation.userId)?.pending, true);
    assert.equal(
      (
        (await (await call('/api/state', 'GET', undefined, viewerCookie)).json()) as PlannerSnapshot
      ).workspaces.some((w) => w.id === workspaceId),
      false,
    );
    const acceptedResponse = await call(
      '/api/invitations/accept',
      'POST',
      { invitationToken: invited.invitation.token },
      viewerCookie,
    );
    assert.equal(acceptedResponse.status, 200);
    const accepted = (await acceptedResponse.json()) as PlannerSnapshot;
    assert.equal(
      accepted.memberships.find(
        (m) => m.workspaceId === workspaceId && m.userId === viewerState.user.id,
      )?.role,
      'approver',
    );
    assert.equal(
      (
        await call(
          '/api/invitations/accept',
          'POST',
          { invitationToken: invited.invitation.token },
          viewerCookie,
        )
      ).status,
      403,
    );
    assert.ok(!JSON.stringify(app.store.read()).includes(invited.invitation.token));
    const secureLogin = await call(
      '/api/auth/login',
      'POST',
      { email: 'owner-http@example.test', password: 'test-password-123' },
      undefined,
      origin.replace('http:', 'https:'),
    );
    assert.equal(secureLogin.status, 200);
    assert.ok(secureLogin.headers.get('set-cookie')?.includes('; Secure'));
    const exported = await call(
      `/api/export?workspaceId=${workspaceId}&format=csv`,
      'GET',
      undefined,
      ownerCookie,
    );
    assert.equal(exported.status, 200);
    const warnings = JSON.parse(
      decodeURIComponent(exported.headers.get('x-planner-warnings')!),
    ) as string[];
    assert.ok(warnings.length);
    const imported = await call(
      '/api/import',
      'POST',
      { workspaceId, format: 'csv', content: await exported.text() },
      ownerCookie,
    );
    assert.equal(imported.status, 200);
    assert.ok(
      ((await imported.json()) as PlannerSnapshot & { importWarnings: string[] }).importWarnings
        .length,
    );
  } finally {
    await app.close();
    assert.ok(directory.startsWith(output + sep));
    rmSync(directory, { recursive: true, force: true });
  }
});

test('loopback auto-owner mode rejects hostile Host and Origin; shared attachments survive owner entity deletion', async () => {
  const output = resolve('output', 'server-tests'),
    directory = join(output, randomUUID());
  mkdirSync(directory, { recursive: true });
  const app = await createPlannerServer({
    dbPath: join(directory, 'planner.sqlite'),
    host: '127.0.0.1',
    port: 0,
    development: false,
    scheduler: false,
  });
  const port = await app.listen();
  const origin = `http://127.0.0.1:${port}`;
  const call = async (path: string, method = 'GET', value?: unknown) =>
    fetch(origin + path, {
      method,
      headers: {
        Origin: origin,
        ...(value !== undefined ? { 'Content-Type': 'application/json' } : {}),
      },
      body: value === undefined ? undefined : JSON.stringify(value),
    });
  try {
    const auth = (await (await call('/api/auth/me')).json()) as { mode: string };
    assert.equal(auth.mode, 'local');
    const hostileHostStatus = await new Promise<number>((resolveStatus, reject) => {
      const req = httpRequest(
        {
          hostname: '127.0.0.1',
          port,
          path: '/api/state',
          headers: { Host: `hostile.example:${port}` },
        },
        (res) => {
          res.resume();
          resolveStatus(res.statusCode ?? 0);
        },
      );
      req.on('error', reject);
      req.end();
    });
    assert.equal(hostileHostStatus, 403);
    assert.equal(
      (
        await fetch(origin + '/api/entities', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ workspaceId: 'personal', title: 'Cross site' }),
        })
      ).status,
      403,
    );
    const owner = app.store.localUser();
    const a = newEntity(app.store, owner, { title: 'Attachment owner' });
    const b = newEntity(app.store, owner, { title: 'Attachment reference' });
    const form = new FormData();
    form.append('workspaceId', 'personal');
    form.append('entityId', a.id);
    form.append('file', new Blob(['shared bytes']), 'shared.txt');
    const file = (await (
      await fetch(origin + '/api/files', {
        method: 'POST',
        headers: { Origin: origin },
        body: form,
      })
    ).json()) as { id: string; url: string };
    app.store.patchEntity(owner, b.id, {
      version: b.version,
      patch: {
        links: [
          { id: 'link-shared', label: 'Shared', kind: 'file', fileId: file.id, url: file.url },
        ],
      },
    });
    const bVersion = app.store.read().entities.find((e) => e.id === b.id)!.version;
    app.store.deleteEntity(owner, a.id, a.version);
    assert.equal(app.store.read().entities.find((e) => e.id === b.id)!.links.length, 1);
    assert.equal(app.store.read().entities.find((e) => e.id === b.id)!.version, bVersion);
    assert.equal(await (await call(file.url)).text(), 'shared bytes');
    assert.equal(
      (
        app.store.db.prepare('SELECT entity_id FROM files WHERE id=?').get(file.id) as {
          entity_id: string | null;
        }
      ).entity_id,
      b.id,
    );
    assert.equal((await call(file.url, 'DELETE', {})).status, 200);
    assert.equal(app.store.read().entities.find((e) => e.id === b.id)!.links.length, 0);
    assert.equal((await call(file.url)).status, 404);
    const last = newEntity(app.store, owner, { title: 'Last attachment owner' });
    const lastForm = new FormData();
    lastForm.append('workspaceId', 'personal');
    lastForm.append('entityId', last.id);
    lastForm.append('file', new Blob(['exclusive bytes']), 'exclusive.txt');
    const exclusive = (await (
      await fetch(origin + '/api/files', {
        method: 'POST',
        headers: { Origin: origin },
        body: lastForm,
      })
    ).json()) as { url: string };
    app.store.deleteEntity(owner, last.id, last.version);
    assert.equal((await call(exclusive.url)).status, 404);
    const pending = app.store.membership(
      owner,
      { workspaceId: 'personal', email: 'deleted-workspace@example.test', role: 'editor' },
      'create',
    ).invitation!;
    assert.equal((await call('/api/workspaces/personal', 'DELETE', { confirm: true })).status, 200);
    assert.equal(app.store.user(pending.userId), undefined);
    assert.equal(
      app.store.db.prepare('SELECT user_id FROM invitations WHERE user_id=?').get(pending.userId),
      undefined,
    );
    assert.equal(
      (
        await call('/api/auth/register', 'POST', {
          email: pending.email,
          password: 'test-password-123',
          displayName: 'New account after revocation',
        })
      ).status,
      201,
    );
  } finally {
    await app.close();
    assert.ok(directory.startsWith(output + sep));
    rmSync(directory, { recursive: true, force: true });
  }
});
