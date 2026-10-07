import { createHash } from 'node:crypto';
import { DateTime } from 'luxon';
import { z } from 'zod';
import { plannerMutationDataSchema } from './agent-schema.ts';
import { migrateWorkItemProofs, workItemProof } from './agent-receipts.ts';
import { PlannerStore, uid } from './store.ts';
import {
  ApiError,
  object,
  parse,
  entitySchema,
  typeSchema,
  resourceSchema,
  ruleSchema,
  dependencySchema,
  preferencesSchema,
  timezone,
  text,
} from './validation.ts';
import {
  expandPreciseOccurrences,
  expandRecurrence,
  validateDependency,
} from '../shared/engine.ts';
import { isoToNs, nsToISO, parseNs, preciseRangeErrors } from '../shared/precise-time.ts';
import type { Entity, PlannerSnapshot, User, TimeRange, Role, Rule } from '../shared/types.ts';

export interface PlannerAgentScope {
  workspaceIds?: string[];
  readOnly?: boolean;
}
const collections = [
  'overview',
  'objects',
  'workspaces',
  'types',
  'resources',
  'rules',
  'dependencies',
  'people',
  'settings',
  'signals',
  'scenarios',
  'templates',
  'comments',
  'notifications',
  'occurrences',
] as const;
const ref = z.string().min(1).max(500);
const nanoseconds = z
  .string()
  .regex(/^(?:0|-?[1-9]\d*)$/)
  .max(40)
  .describe('Canonical signed Unix-epoch nanoseconds decimal string; never a JSON number.');
const operationSchema = z
  .object({
    op: z.enum(['create', 'update', 'delete', 'comment', 'settings']),
    collection: z
      .enum(['objects', 'workspaces', 'types', 'resources', 'rules', 'dependencies'])
      .optional(),
    ref: ref.optional(),
    workspace: ref.optional(),
    key: ref.optional(),
    expectedVersion: z.number().int().positive().optional(),
    reason: z.string().max(8000).optional(),
    confirm: z.boolean().optional(),
    data: z.record(z.string(), z.unknown()).optional(),
  })
  .strict();
export const plannerSchemas = {
  planner_help: z.object({}).strict(),
  planner_read: z
    .object({
      collection: z.enum(collections).default('objects'),
      workspace: ref.optional(),
      query: z.string().max(500).optional(),
      ids: z.array(ref).max(100).optional(),
      limit: z.number().int().min(1).max(200).default(50),
      cursor: z.string().max(2000).optional(),
      detail: z.boolean().default(false),
      from: z.string().optional(),
      to: z.string().optional(),
      fromNs: nanoseconds.optional(),
      toNs: nanoseconds.optional(),
    })
    .strict(),
  planner_apply: z
    .object({
      requestId: z.string().min(1).max(200),
      expectedRevision: z.number().int().nonnegative().optional(),
      operations: z.array(operationSchema).min(1).max(100),
    })
    .strict(),
};
export const plannerToolDefinitions = Object.entries(plannerSchemas).map(([name, schema]) => ({
  name,
  description: (
    {
      planner_help: 'Quick guide and ready-to-use examples for the planner.',
      planner_read:
        'Read compact objects or configuration. IDs or exact unique names; versions, timezone, exact decimal nanosecond coordinates and localPlan calendar labels included.',
      planner_apply:
        'Atomically create/update/delete objects or configuration using friendly names, dates and local $keys. Requires requestId; object changes require expectedVersion, configuration changes expectedRevision.',
    } as Record<string, string>
  )[name],
  inputSchema: z.toJSONSchema(schema, { io: 'input' }),
  annotations: {
    readOnlyHint: name !== 'planner_apply',
    destructiveHint: name === 'planner_apply',
    idempotentHint: true,
    openWorldHint: false,
  },
}));
// Discovery shows the actual shapes instead of asking a small model to guess opaque JSON.
const applyDefinition = plannerToolDefinitions.find((t) => t.name === 'planner_apply')!;
const operationDefinition = (
  applyDefinition.inputSchema.properties!.operations as {
    items: { properties: Record<string, unknown> };
  }
).items;
operationDefinition.properties.data = plannerMutationDataSchema;
const canonical = (value: unknown): string => {
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  if (value && typeof value === 'object')
    return (
      '{' +
      Object.keys(value)
        .sort()
        .map((k) => JSON.stringify(k) + ':' + canonical((value as Record<string, unknown>)[k]))
        .join(',') +
      '}'
    );
  return JSON.stringify(value) ?? 'null';
};
const hash = (value: unknown) => createHash('sha256').update(canonical(value)).digest('hex');
const fail = (code: string, message: string, details?: unknown, status = 400): never => {
  throw new ApiError(status, message, code, details);
};
const label = (v: Record<string, any>) => v.title ?? v.name ?? v.label ?? v.displayName ?? v.id;
const compact = (e: Entity) => ({
  id: e.id,
  title: e.title,
  workspaceId: e.workspaceId,
  typeId: e.typeId,
  kind: e.kind,
  status: e.status,
  version: e.version,
  plan: e.plan,
  due: e.dueAt,
  ownerId: e.ownerId,
  parentId: e.parentId,
  ...(e.actual ? { actual: e.actual } : {}),
  ...(e.forecast ? { forecast: e.forecast } : {}),
  ...(e.tags.length ? { tags: e.tags } : {}),
  ...(Object.keys(e.fields).length ? { fields: e.fields } : {}),
});
const compactRead = (e: Entity, detail = false) => {
  const asLocal = (value: string) =>
    DateTime.fromISO(value, { setZone: true }).setZone(e.plan.timezone);
  const atLocalMidnight = (value: DateTime) =>
    value.hour === 0 && value.minute === 0 && value.second === 0 && value.millisecond === 0;
  const displayLocal = (value: DateTime, dayOnly: boolean) =>
    dayOnly && atLocalMidnight(value)
      ? value.toFormat('yyyy-MM-dd')
      : value.toISO({ suppressMilliseconds: true });
  const preciseStart = parseNs(e.plan.precise?.start);
  const preciseEnd = parseNs(e.plan.precise?.end);
  const preciseLocal = e.plan.precise
    ? {
        start: preciseStart === null ? null : nsToISO(preciseStart, e.plan.timezone),
        ...(preciseEnd === null ? {} : { end: nsToISO(preciseEnd, e.plan.timezone) }),
        timezone: e.plan.timezone,
      }
    : undefined;
  return {
    ...(detail ? e : compact(e)),
    ...(e.plan.start || e.plan.end
      ? {
          localPlan:
            preciseLocal ??
            ({
              ...(e.plan.start
                ? { start: displayLocal(asLocal(e.plan.start), e.plan.precision === 'day') }
                : { start: null }),
              ...(e.plan.end
                ? e.plan.precision === 'day' &&
                  atLocalMidnight(asLocal(e.plan.end)) &&
                  (!e.plan.start || Date.parse(e.plan.end) > Date.parse(e.plan.start))
                  ? { endInclusive: asLocal(e.plan.end).minus({ days: 1 }).toFormat('yyyy-MM-dd') }
                  : { end: displayLocal(asLocal(e.plan.end), false) }
                : {}),
              timezone: e.plan.timezone,
            } as const),
        }
      : {}),
  };
};

export function normalizeAgentDate(value: string, zone: string): string {
  parse(timezone, zone);
  const day = /^\d{4}-\d{2}-\d{2}$/.test(value);
  if (!day && !/(?:Z|[+-]\d{2}:?\d{2})$/i.test(value))
    return fail(
      'offset_required',
      'Timed dates require Z or a UTC offset, including during DST. A date-only YYYY-MM-DD uses workspace midnight.',
    );
  const time = DateTime.fromISO(value, { zone, setZone: !day });
  if (!time.isValid || (day && time.toISODate() !== value))
    return fail('invalid_date', 'Invalid calendar date.');
  const fraction = value.match(/[.,](\d+)(?:Z|[+-]\d{2}:?\d{2})$/i)?.[1];
  if (fraction && fraction.length > 3) {
    const nanoseconds = isoToNs(value, zone);
    if (nanoseconds === null)
      return fail('invalid_date', 'Timestamp supports at most nine fractional second digits.');
    return nsToISO(nanoseconds, 'UTC')!;
  }
  return time.toUTC().toISO()!;
}

/** All mutations execute inside the store's single transaction, including durable request receipts. */
export class PlannerAgentService {
  constructor(
    readonly store: PlannerStore,
    readonly user: User,
    readonly scope: PlannerAgentScope = {},
  ) {
    store.db.exec(
      'CREATE TABLE IF NOT EXISTS agent_requests (user_id TEXT NOT NULL, request_id TEXT NOT NULL, fingerprint TEXT NOT NULL, workspace_ids TEXT NOT NULL, response TEXT NOT NULL, PRIMARY KEY(user_id,request_id))',
    );
    migrateWorkItemProofs(store);
  }
  snapshot(): PlannerSnapshot {
    return this.visible(this.store.read());
  }
  resolveWorkspace(value?: string): string {
    return this.workspace(this.store.read(), value).id;
  }
  assertWrite(workspaceId: string, roles: Role[] = ['owner', 'editor']): void {
    const state = this.store.read();
    if (this.scope.readOnly) fail('read_only', 'This MCP credential is read-only.', undefined, 403);
    this.workspace(state, workspaceId);
    this.store.requireRole(this.user, workspaceId, roles, state);
  }
  call(name: string, args: unknown = {}): Record<string, unknown> {
    if (!Object.hasOwn(plannerSchemas, name))
      return fail('unknown_tool', 'Unknown tool. Call planner_help.');
    if (name === 'planner_help') {
      parse(plannerSchemas.planner_help, args);
      return this.help();
    }
    if (name === 'planner_read') return this.read(parse(plannerSchemas.planner_read, args));
    return this.apply(parse(plannerSchemas.planner_apply, args));
  }
  private visible(state: PlannerSnapshot) {
    const allowed = new Set(
      state.memberships
        .filter(
          (m) =>
            m.userId === this.user.id &&
            (!this.scope.workspaceIds || this.scope.workspaceIds.includes(m.workspaceId)),
        )
        .map((m) => m.workspaceId),
    );
    // Narrow before snapshot: users and memberships must not retain other-space identities.
    const narrowed = {
      ...state,
      memberships: state.memberships.filter((m) => allowed.has(m.workspaceId)),
    };
    return this.store.snapshot(this.user, narrowed, narrowed);
  }
  private resolve(
    items: Record<string, any>[],
    value: unknown,
    what: string,
    locals = new Map<string, string>(),
  ): any {
    if (typeof value !== 'string')
      return fail('missing_reference', `Specify ${what} by ID or exact name.`);
    const wanted = value.startsWith('$')
      ? (locals.get(value.slice(1)) ??
        fail(
          'unknown_local_ref',
          `Unknown local reference ${value}; create its key earlier in this batch.`,
        ))
      : value;
    const exact = items.filter((v) => v.id === wanted);
    const found = exact.length ? exact : items.filter((v) => label(v) === wanted);
    if (found.length === 1) return found[0];
    if (!found.length)
      return fail(
        'not_found',
        `${what} is unavailable. Read ${what} and use an accessible ID.`,
        undefined,
        404,
      );
    return fail(
      'ambiguous_reference',
      `${what} name is ambiguous; use one of these IDs.`,
      {
        choices: found.map((v) => ({
          id: v.id,
          name: label(v),
          workspaceId: v.workspaceId,
          version: v.version,
        })),
      },
      409,
    );
  }
  private workspace(state: PlannerSnapshot, value?: unknown, locals?: Map<string, string>) {
    const ws = this.visible(state).workspaces;
    if (value === undefined && ws.length === 1) return ws[0];
    if (value === undefined)
      return fail('workspace_required', 'Specify workspace by ID or exact name.', {
        choices: ws.map((w) => ({ id: w.id, name: w.name, timezone: w.timezone })),
      });
    return this.resolve(ws, value, 'workspaces', locals);
  }
  private assertStateWrite(state: PlannerSnapshot, workspaceId: string, owner = false) {
    if (this.scope.readOnly) fail('read_only', 'This MCP credential is read-only.', undefined, 403);
    this.workspace(state, workspaceId);
    this.store.requireRole(this.user, workspaceId, owner ? ['owner'] : ['owner', 'editor'], state);
  }
  private date(value: unknown, zone: string): string | null {
    if (value === null) return null;
    if (typeof value !== 'string')
      return fail('invalid_date', 'Use YYYY-MM-DD or an ISO timestamp with explicit offset.');
    return normalizeAgentDate(value, zone);
  }
  private calendarDate(value: unknown, zone: string) {
    const normalized = this.date(value, zone);
    return typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value) ? value : normalized;
  }
  private timeRange(value: unknown, zone: string, base?: TimeRange): TimeRange {
    if (typeof value === 'string') value = { start: value };
    const raw = object(value);
    const allowed = ['start', 'end', 'timezone', 'precision', 'earliest', 'latest', 'precise'];
    this.keys(raw, allowed);
    const tz = parse(timezone, raw.timezone ?? base?.timezone ?? zone);
    const suppliedPrecise = raw.precise === undefined ? undefined : object(raw.precise);
    if (suppliedPrecise)
      this.keys(suppliedPrecise, ['scale', 'start', 'end', 'earliest', 'latest', 'resolutionNs']);
    const mergedPrecise =
      suppliedPrecise || base?.precise
        ? {
            scale: 'unix-nanoseconds' as const,
            start: null,
            end: null,
            ...(base?.precise ?? {}),
            ...(suppliedPrecise ?? {}),
          }
        : undefined;
    const result: Record<string, unknown> = {
      ...(base ?? { start: null, end: null }),
      ...raw,
      timezone: tz,
      ...(mergedPrecise ? { precise: mergedPrecise } : {}),
      precision:
        raw.precision ??
        base?.precision ??
        (typeof raw.start === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(raw.start) ? 'day' : 'exact'),
    };
    for (const k of ['start', 'end', 'earliest', 'latest'])
      if (k in raw) result[k] = this.date(raw[k], tz);
    if (mergedPrecise) {
      for (const key of ['start', 'end', 'earliest', 'latest'] as const) {
        if (key in raw && !(suppliedPrecise && key in suppliedPrecise)) {
          const mirror = result[key];
          mergedPrecise[key] =
            mirror === null ? null : (isoToNs(mirror as string, tz)?.toString() ?? null);
        } else if (suppliedPrecise && key in suppliedPrecise && !(key in raw)) {
          const coordinate = mergedPrecise[key];
          const parsed = coordinate === null ? null : parseNs(coordinate);
          result[key] = parsed === null ? null : nsToISO(parsed, tz);
        }
      }
    }
    const normalized = result as unknown as TimeRange;
    const errors = preciseRangeErrors(normalized);
    if (errors.length) fail('invalid_precise_time', 'Invalid exact nanosecond range.', { errors });
    return normalized;
  }
  private keys(data: Record<string, unknown>, allowed: string[]) {
    const unknown = Object.keys(data).filter((k) => !allowed.includes(k));
    if (unknown.length)
      fail('unknown_fields', 'Unsupported or protected fields.', { fields: unknown, allowed });
  }
  private entityData(
    data: Record<string, unknown>,
    state: PlannerSnapshot,
    ws: any,
    locals: Map<string, string>,
    before?: Entity,
  ) {
    this.keys(data, [
      'title',
      'description',
      'kind',
      'type',
      'typeId',
      'start',
      'end',
      'due',
      'dueAt',
      'timezone',
      'precision',
      'precise',
      'plan',
      'actual',
      'forecast',
      'owner',
      'ownerId',
      'participants',
      'participantIds',
      'resources',
      'allocations',
      'parent',
      'parentId',
      'fields',
      'tags',
      'links',
      'status',
      'recurrence',
    ]);
    const out: Record<string, unknown> = { ...data };
    const tz = parse(timezone, data.timezone ?? before?.plan.timezone ?? ws.timezone);
    const visible = this.visible(state);
    const members = new Set(
      visible.memberships.filter((m) => m.workspaceId === ws.id).map((m) => m.userId),
    );
    const people = visible.users.filter((u) => members.has(u.id));
    for (const [friendly, internal, items] of [
      ['owner', 'ownerId', people],
      ['parent', 'parentId', visible.entities.filter((e) => e.workspaceId === ws.id)],
    ] as const) {
      const value = data[friendly] ?? data[internal];
      if (friendly in data || internal in data)
        out[internal] =
          data[friendly] === null || data[internal] === null
            ? null
            : this.resolve(items as any[], value, friendly, locals).id;
      delete out[friendly];
    }
    if ('participants' in data || 'participantIds' in data) {
      const values = data.participants ?? data.participantIds;
      if (!Array.isArray(values))
        fail('invalid_participants', 'participants must be an array of IDs or names.');
      out.participantIds = (values as unknown[]).map(
        (v) => this.resolve(people, v, 'people', locals).id,
      );
      delete out.participants;
    }
    if ('resources' in data || 'allocations' in data) {
      const values = data.resources ?? data.allocations;
      if (!Array.isArray(values))
        fail('invalid_resources', 'resources must be an array of names or {resource, amount}.');
      out.allocations = (values as unknown[]).map((v) => {
        const a = typeof v === 'string' ? { resource: v, amount: 1 } : object(v);
        this.keys(a, ['resource', 'resourceId', 'amount']);
        return {
          resourceId: this.resolve(
            visible.resources.filter((r) => r.workspaceId === ws.id),
            a.resource ?? a.resourceId,
            'resources',
            locals,
          ).id,
          amount: a.amount ?? 1,
        };
      });
      delete out.resources;
    }
    if ('plan' in data && ('start' in data || 'end' in data || 'precise' in data))
      fail('conflicting_dates', 'Use plan or top-level start/end/precise, not both.');
    if ('plan' in data) out.plan = this.timeRange(data.plan, tz, before?.plan);
    else if (
      'start' in data ||
      'end' in data ||
      'precise' in data ||
      (before && ('timezone' in data || 'precision' in data))
    )
      out.plan = this.timeRange(
        {
          ...(data.start !== undefined ? { start: data.start } : {}),
          ...(data.end !== undefined ? { end: data.end } : {}),
          ...(data.precise !== undefined ? { precise: data.precise } : {}),
          timezone: tz,
          ...(data.precision ? { precision: data.precision } : {}),
        },
        tz,
        before?.plan,
      );
    for (const k of ['actual', 'forecast'])
      if (k in data) out[k] = data[k] === null ? null : this.timeRange(data[k], tz);
    if ('due' in data || 'dueAt' in data) out.dueAt = this.date(data.due ?? data.dueAt ?? null, tz);
    for (const k of ['start', 'end', 'due', 'timezone', 'precision', 'precise']) delete out[k];
    const types = visible.types.filter((t) => !t.workspaceId || t.workspaceId === ws.id);
    const typeRef = data.type ?? data.typeId;
    const autoKind =
      data.end ||
      (data.precise && object(data.precise).end) ||
      (data.plan &&
        typeof data.plan === 'object' &&
        (object(data.plan).end ||
          (object(data.plan).precise && object(object(data.plan).precise).end)))
        ? 'period'
        : data.start || data.precise || data.due || data.dueAt || data.plan
          ? 'point'
          : 'note';
    if (typeRef !== undefined) {
      const t = this.resolve(types, typeRef, 'types', locals);
      out.typeId = t.id;
      out.kind = data.kind ?? t.kind;
    } else if (!before) {
      out.kind = data.kind ?? autoKind;
      const builtin = types.find((t) => t.builtin && t.id === out.kind);
      if (builtin) out.typeId = builtin.id;
    }
    delete out.type;
    const type =
      types.find((t) => t.id === (out.typeId ?? before?.typeId)) ??
      types.find((t) => t.kind === out.kind && !t.fields.some((f) => f.required));
    if (data.fields) {
      const fields = object(data.fields);
      const result: Record<string, unknown> = { ...(before?.fields ?? {}) };
      for (const [k, v] of Object.entries(fields))
        result[this.resolve(type?.fields ?? [], k, 'type fields', locals).id] = v;
      out.fields = result;
    }
    if (data.links) {
      if (!Array.isArray(data.links)) fail('invalid_links', 'links must be an array.');
      out.links = (data.links as unknown[]).map((v) =>
        typeof v === 'string'
          ? { id: uid('link'), label: v, url: v, kind: 'url' }
          : { id: uid('link'), kind: 'url', ...object(v) },
      );
    }
    if (data.recurrence) {
      const r = object(data.recurrence);
      out.recurrence = {
        interval: 1,
        exceptions: [],
        ...r,
        ...(r.until
          ? {
              until: this.calendarDate(r.until, tz),
            }
          : {}),
      };
      if (Array.isArray(r.exceptions))
        out.recurrence = {
          ...(out.recurrence as object),
          exceptions: r.exceptions.map((value) => this.calendarDate(value, tz)),
        };
    }
    return out;
  }
  private read(args: z.infer<typeof plannerSchemas.planner_read>): Record<string, unknown> {
    const state = this.store.read();
    const snapshot = this.visible(state);
    const ws = args.workspace ? this.workspace(state, args.workspace) : undefined;
    if (args.collection === 'overview') {
      const spaces = snapshot.workspaces.filter((w) => !ws || w.id === ws.id);
      const ids = new Set(spaces.map((w) => w.id));
      const memberships = snapshot.memberships.filter((m) => ids.has(m.workspaceId));
      return {
        revision: state.revision,
        serverTime: snapshot.serverTime,
        readOnly: !!this.scope.readOnly,
        workspaces: spaces.map((w) => ({ ...w, role: this.store.role(this.user.id, w.id, state) })),
        types: snapshot.types.filter((t) => !t.workspaceId || ids.has(t.workspaceId)),
        resources: snapshot.resources.filter((r) => ids.has(r.workspaceId)),
        people: snapshot.users.filter((u) => memberships.some((m) => m.userId === u.id)),
        counts: Object.fromEntries(
          ['entities', 'rules', 'signals', 'scenarios', 'templates', 'dependencies'].map((k) => [
            k,
            (snapshot as any)[k].filter((v: any) => !v.workspaceId || ids.has(v.workspaceId))
              .length,
          ]),
        ),
      };
    }
    if (args.collection === 'settings')
      return {
        revision: state.revision,
        settings: snapshot.settings,
        serverTime: snapshot.serverTime,
      };
    const map: Record<string, string> = {
      objects: 'entities',
      people: 'users',
      occurrences: 'entities',
    };
    let items: Record<string, any>[] = (snapshot as any)[map[args.collection] ?? args.collection];
    if (ws)
      items = items.filter((v) =>
        args.collection === 'workspaces'
          ? v.id === ws.id
          : args.collection === 'people'
            ? snapshot.memberships.some((m) => m.workspaceId === ws.id && m.userId === v.id)
            : !v.workspaceId || v.workspaceId === ws.id,
      );
    if (args.query) {
      const query = args.query.toLocaleLowerCase();
      items = items.filter((v) => String(label(v)).toLocaleLowerCase().includes(query));
    }
    if (args.ids) {
      const ids = new Set(args.ids.map((id) => this.resolve(items, id, args.collection).id));
      items = items.filter((v) => ids.has(v.id));
    }
    if (args.collection === 'occurrences') {
      const exactWindow = args.fromNs !== undefined || args.toNs !== undefined;
      if (exactWindow && (args.from !== undefined || args.to !== undefined))
        fail('conflicting_range', 'Use from/to or decimal-string fromNs/toNs, not both.');
      if (exactWindow) {
        const from = parseNs(args.fromNs);
        const to = parseNs(args.toNs);
        if (from === null || to === null)
          fail(
            'invalid_range',
            'fromNs/toNs must be ordered canonical decimal-string nanoseconds.',
          );
        const exactFrom = from as bigint;
        const exactTo = to as bigint;
        if (exactTo <= exactFrom)
          fail(
            'invalid_range',
            'fromNs/toNs must be ordered canonical decimal-string nanoseconds.',
          );
        items = items.flatMap((e) =>
          expandPreciseOccurrences(e as Entity, exactFrom, exactTo).map((o) => ({
            ...o,
            timezone: e.plan.timezone,
          })),
        );
      } else {
        if (!args.from || !args.to)
          fail('range_required', 'occurrences requires from/to or fromNs/toNs.');
        const from = this.date(args.from, ws?.timezone ?? 'UTC')!;
        const to = this.date(args.to, ws?.timezone ?? 'UTC')!;
        if (Date.parse(to) < Date.parse(from) || Date.parse(to) - Date.parse(from) > 366 * 86400000)
          fail('invalid_range', 'Calendar preview range must be ordered and at most 366 days.');
        items = items.flatMap((e) =>
          expandRecurrence(e as Entity, from, to).map((o) => ({
            ...o,
            timezone: e.plan.timezone,
          })),
        );
      }
    }
    const fingerprint = hash({
      collection: args.collection,
      workspace: ws?.id,
      query: args.query,
      ids: args.ids,
      from: args.from,
      to: args.to,
      fromNs: args.fromNs,
      toNs: args.toNs,
      scope: this.scope,
      user: this.user.id,
    });
    let offset = 0;
    if (args.cursor) {
      let cursor: any;
      try {
        cursor = JSON.parse(Buffer.from(args.cursor, 'base64url').toString());
      } catch {
        fail('invalid_cursor', 'Invalid cursor.');
      }
      if (
        cursor.fingerprint !== fingerprint ||
        cursor.revision !== state.revision ||
        !Number.isInteger(cursor.offset) ||
        cursor.offset < 0
      )
        fail(
          'stale_cursor',
          'Restart this read without cursor; data or filters changed.',
          undefined,
          409,
        );
      offset = cursor.offset;
    }
    const page = items.slice(offset, offset + args.limit);
    return {
      revision: state.revision,
      serverTime: snapshot.serverTime,
      workspaces: snapshot.workspaces.map((w) => ({
        id: w.id,
        name: w.name,
        timezone: w.timezone,
        role: this.store.role(this.user.id, w.id, state),
      })),
      collection: args.collection,
      total: items.length,
      items: page.map((v) =>
        args.collection === 'objects' ? compactRead(v as Entity, args.detail) : v,
      ),
      cursor:
        offset + page.length < items.length
          ? Buffer.from(
              JSON.stringify({
                offset: offset + page.length,
                revision: state.revision,
                fingerprint,
              }),
            ).toString('base64url')
          : null,
    };
  }
  private help(): Record<string, unknown> {
    const s = this.visible(this.store.read());
    return {
      tools: plannerToolDefinitions.map((t) => ({ name: t.name, description: t.description })),
      collections,
      revision: s.revision,
      workspaces: s.workspaces.map((w) => ({ id: w.id, name: w.name, timezone: w.timezone })),
      rules: [
        'References accept ID, exact unique name, or $key of an earlier creation in the same batch.',
        'Each apply is atomic, max 100 operations, requestId is durable and unique for this user. Replaying identical input returns its original receipt.',
        'Object update/delete requires expectedVersion from read. Configuration update/delete requires expectedRevision.',
        'Dates: YYYY-MM-DD in workspace timezone; timed ISO must have Z or offset. baseline/source are protected. Correcting a done object needs reason.',
        'Exact axis coordinates use plan.precise with scale "unix-nanoseconds" and canonical signed decimal strings. Never send nanoseconds as JSON numbers. precise.start/end are authoritative; calendar start/end are exact mirrors only when representable.',
        'Object reads keep stored plan timestamps and add localPlan in the IANA timezone; day precision with a local-midnight plan.end shows endInclusive, otherwise end is local ISO with its offset. Raw plan.end remains the exclusive boundary. Use detail:true to include baseline and explicit null facts in the same read.',
        'Only supplied object fields change; actual and baseline survive plan edits.',
      ],
      createDataExamples: {
        objects: {
          title: 'Trip',
          start: '2027-06-01',
          end: '2027-06-08',
          type: 'optional type name',
          parent: 'optional parent ID/name',
          owner: 'optional person ID/name',
          participants: ['person ID/name'],
          resources: [{ resource: 'resource ID/name', amount: 1 }],
          fields: { 'Field label': 'value' },
          tags: ['travel'],
          links: ['https://example.com'],
          status: 'planned',
        },
        preciseObject: {
          title: 'Nanosecond interval',
          plan: {
            timezone: 'UTC',
            precision: 'exact',
            precise: {
              scale: 'unix-nanoseconds',
              start: '1798761600000000000',
              end: '1798761600000000001',
              resolutionNs: '1',
            },
          },
        },
        workspaces: { name: 'Trip planning', timezone: 'Asia/Yekaterinburg', mode: 'personal' },
        types: {
          label: 'Booking',
          kind: 'period',
          fields: [{ label: 'Budget', type: 'number', required: true }],
        },
        resources: { name: 'Meeting room', kind: 'place', capacity: 1, unit: 'room' },
        rules: {
          name: 'Remind 24h before',
          trigger: 'before-start',
          leadMinutes: 1440,
          type: 'optional type ID/name',
          action: 'signal',
        },
        dependencies: {
          from: 'predecessor ID/name',
          to: 'successor ID/name',
          kind: 'finish-start',
          lagMinutes: 0,
        },
      },
      vocabulary: {
        kinds: ['point', 'period', 'process', 'note', 'metric'],
        statuses: ['draft', 'planned', 'active', 'done', 'cancelled'],
        fieldTypes: ['text', 'number', 'boolean', 'date', 'url', 'select'],
        ruleTriggers: ['before-start', 'before-due', 'after-done', 'overdue', 'resource-conflict'],
      },
      examples: [
        { tool: 'planner_read', args: { collection: 'objects', limit: 20 } },
        {
          tool: 'planner_apply',
          args: {
            requestId: 'unique-request-id',
            operations: [
              {
                op: 'create',
                collection: 'objects',
                workspace: s.workspaces[0]?.id ?? 'workspace-id',
                key: 'meeting',
                data: {
                  title: 'Review',
                  start: '2026-10-05',
                  recurrence: { frequency: 'week', count: 4 },
                },
              },
              { op: 'comment', ref: '$meeting', data: { text: 'Agenda ready' } },
            ],
          },
        },
        {
          tool: 'planner_apply',
          args: {
            requestId: 'another-unique-id',
            operations: [
              {
                op: 'update',
                collection: 'objects',
                ref: 'object-id',
                expectedVersion: 1,
                data: { status: 'done', actual: { start: '2026-10-05T10:00:00+05:00' } },
              },
            ],
          },
        },
      ],
    };
  }
  private apply(args: z.infer<typeof plannerSchemas.planner_apply>): Record<string, unknown> {
    if (this.scope.readOnly) fail('read_only', 'This MCP credential is read-only.', undefined, 403);
    const fingerprint = hash({ args, scope: this.scope });
    let response: Record<string, unknown> = {};
    this.store.transaction((state) => {
      const previous = this.store.db
        .prepare(
          'SELECT fingerprint,workspace_ids,response FROM agent_requests WHERE user_id=? AND request_id=?',
        )
        .get(this.user.id, args.requestId) as
        { fingerprint: string; workspace_ids: string; response: string } | undefined;
      if (previous) {
        if (previous.fingerprint !== fingerprint)
          fail(
            'idempotency_mismatch',
            'requestId was used with different arguments; use a new requestId.',
            undefined,
            409,
          );
        for (const id of JSON.parse(previous.workspace_ids)) this.assertStateWrite(state, id);
        response = JSON.parse(previous.response);
        return false;
      }
      if (args.expectedRevision !== undefined && args.expectedRevision !== state.revision)
        fail(
          'revision_conflict',
          'Planner changed; read current revision and retry with a new requestId.',
          { revision: state.revision },
          409,
        );
      const locals = new Map<string, string>();
      const touched = new Set<string>();
      const results: Record<string, unknown>[] = [];
      for (const [index, op] of args.operations.entries()) {
        try {
          const data = op.data ?? {};
          const collection = op.collection ?? 'objects';
          if (op.key && (op.op !== 'create' || locals.has(op.key)))
            fail('invalid_key', 'key must be unique and is only valid on create.');
          if (op.op === 'settings') {
            if (this.scope.workspaceIds)
              fail(
                'scope_violation',
                'A workspace-scoped key cannot change account-wide notification settings. Use the local owner connection.',
                undefined,
                403,
              );
            if (args.expectedRevision === undefined)
              fail('revision_required', 'Settings update requires expectedRevision.');
            this.keys(data, ['notifications']);
            const current = this.store.preferences(this.user.id, state);
            current.notifications = parse(preferencesSchema, {
              ...current.notifications,
              ...object(data.notifications),
            });
            this.store.db
              .prepare(
                'INSERT INTO preferences(user_id,document) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET document=excluded.document',
              )
              .run(this.user.id, JSON.stringify(current));
            results.push({ index, op: 'settings', settings: current });
            continue;
          }
          const map = {
            objects: 'entities',
            workspaces: 'workspaces',
            types: 'types',
            resources: 'resources',
            rules: 'rules',
            dependencies: 'dependencies',
          } as const;
          const list = state[map[collection]] as any[];
          const visible = this.visible(state)[map[collection]] as any[];
          const target =
            op.op === 'create'
              ? undefined
              : this.resolve(
                  op.op === 'comment' ? this.visible(state).entities : visible,
                  op.ref,
                  collection,
                  locals,
                );
          if (op.op === 'comment') {
            this.assertStateWrite(state, target.workspaceId);
            this.keys(data, ['text']);
            const message = z.string().trim().min(1).max(100000).parse(data.text);
            const c = {
              id: uid('comment'),
              workspaceId: target.workspaceId,
              entityId: target.id,
              userId: this.user.id,
              text: message,
              createdAt: new Date().toISOString(),
            };
            state.comments.push(c);
            this.store.audit(
              state,
              this.user.id,
              target.workspaceId,
              'comment-create',
              op.reason ?? '',
              { entityId: target.id, after: c },
            );
            touched.add(target.workspaceId);
            results.push({ index, id: c.id, entityId: target.id });
            continue;
          }
          if (target && collection === 'objects' && op.expectedVersion !== target.version)
            fail(
              'version_conflict',
              'Object update/delete requires its current expectedVersion.',
              { id: target.id, version: target.version },
              409,
            );
          if (target && collection !== 'objects' && args.expectedRevision === undefined)
            fail('revision_required', 'Configuration update/delete requires expectedRevision.');
          if (target && !target.workspaceId && collection === 'types')
            fail('protected_definition', 'Global type definitions are read-only.', undefined, 403);
          if (collection === 'workspaces' && op.op === 'create') {
            if (this.scope.workspaceIds)
              fail(
                'scope_violation',
                'This credential cannot create workspaces outside its scope.',
                undefined,
                403,
              );
            this.keys(data, ['name', 'mode', 'timezone', 'description']);
            const w = z
              .object({
                id: z.string(),
                name: z.string().trim().min(1).max(300),
                mode: z.enum(['personal', 'team']),
                timezone,
                createdAt: z.string(),
                description: z.string().max(100000).optional(),
              })
              .strict()
              .parse({
                mode: 'personal',
                timezone: 'Asia/Yekaterinburg',
                ...data,
                id: uid('workspace'),
                createdAt: new Date().toISOString(),
              });
            state.workspaces.push(w);
            state.memberships.push({ workspaceId: w.id, userId: this.user.id, role: 'owner' });
            touched.add(w.id);
            if (op.key) locals.set(op.key, w.id);
            this.store.audit(state, this.user.id, w.id, 'workspace-create', op.reason ?? '', {
              after: w,
            });
            results.push({ index, id: w.id, name: w.name });
            continue;
          }
          const ws =
            collection === 'workspaces'
              ? this.workspace(state, target.id)
              : this.workspace(state, target?.workspaceId ?? op.workspace, locals);
          this.assertStateWrite(state, ws.id, collection === 'workspaces');
          touched.add(ws.id);
          const before = target ? structuredClone(target) : undefined;
          let after: any;
          if (op.op === 'delete') {
            if (collection === 'workspaces') {
              if (op.confirm !== true)
                fail('confirmation_required', 'Workspace deletion requires confirm:true.');
              if (
                [
                  'entities',
                  'resources',
                  'rules',
                  'dependencies',
                  'types',
                  'templates',
                  'scenarios',
                  'comments',
                  'signals',
                  'notifications',
                ].some((k) => (state as any)[k].some((v: any) => v.workspaceId === ws.id))
              )
                fail('workspace_not_empty', 'Delete workspace contents explicitly first.');
              state.memberships = state.memberships.filter((m) => m.workspaceId !== ws.id);
              for (const table of ['files', 'webhook_connections', 'invitations'])
                this.store.db.prepare(`DELETE FROM ${table} WHERE workspace_id=?`).run(ws.id);
            }
            if (collection === 'objects') {
              if (
                state.entities.some((e) => e.parentId === target.id) ||
                state.dependencies.some((d) => d.fromId === target.id || d.toId === target.id)
              )
                fail(
                  'referenced_object',
                  'Object has children or dependencies. Detach them explicitly first.',
                  {
                    children: state.entities
                      .filter((e) => e.parentId === target.id)
                      .map((e) => ({ id: e.id, version: e.version })),
                    dependencies: state.dependencies
                      .filter((d) => d.fromId === target.id || d.toId === target.id)
                      .map((d) => d.id),
                  },
                  409,
                );
              state.comments = state.comments.filter((c) => c.entityId !== target.id);
              const signals = new Set(
                state.signals.filter((s) => s.entityId === target.id).map((s) => s.id),
              );
              state.signals = state.signals.filter((s) => s.entityId !== target.id);
              state.notifications = state.notifications.filter((n) => !signals.has(n.signalId));
              const files = this.store.db
                .prepare('SELECT id FROM files WHERE entity_id=?')
                .all(target.id) as { id: string }[];
              for (const file of files) {
                const other = state.entities.find(
                  (e) => e.id !== target.id && e.links.some((l) => l.fileId === file.id),
                );
                if (other)
                  this.store.db
                    .prepare('UPDATE files SET entity_id=? WHERE id=?')
                    .run(other.id, file.id);
                else this.store.db.prepare('DELETE FROM files WHERE id=?').run(file.id);
              }
            }
            if (
              collection === 'types' &&
              (state.entities.some((e) => e.typeId === target.id) ||
                state.rules.some((r) => r.typeId === target.id) ||
                state.templates.some((t) => t.items.some((i) => i.typeId === target.id)))
            )
              fail('type_in_use', 'Type is used by objects, rules or templates.', undefined, 409);
            if (
              collection === 'resources' &&
              (state.entities.some((e) => e.allocations.some((a) => a.resourceId === target.id)) ||
                state.templates.some((t) =>
                  t.items.some((i) => i.allocations?.some((a) => a.resourceId === target.id)),
                ))
            )
              fail(
                'resource_in_use',
                'Remove allocations before deleting this resource.',
                undefined,
                409,
              );
            list.splice(
              list.findIndex((v) => v.id === target.id),
              1,
            );
          } else if (collection === 'objects') {
            const normalized = this.entityData(data, state, ws, locals, target);
            if (target) {
              if (target.status === 'done' && Object.keys(data).length && !op.reason?.trim())
                fail('reason_required', 'Correcting a completed object requires reason.');
              after = parse(entitySchema, {
                ...target,
                ...normalized,
                ...('forecast' in normalized
                  ? { forecastProvenance: normalized.forecast ? 'manual' : undefined }
                  : {}),
                updatedAt: new Date().toISOString(),
                version: target.version + 1,
              });
              this.store.validateEntityReferences(after, state);
            } else
              after = this.store.makeEntity(
                { ...normalized, workspaceId: ws.id },
                state,
                this.user,
              );
            if (target) list[list.findIndex((v) => v.id === target.id)] = after;
            else list.push(after);
          } else {
            const value: Record<string, any> = {
              ...(target ?? {}),
              ...data,
              id: target?.id ?? uid(collection.slice(0, -1)),
              ...(collection === 'workspaces' ? {} : { workspaceId: ws.id }),
            };
            if (['id', 'workspaceId', 'builtin', 'createdAt'].some((k) => k in data))
              fail('protected_field', 'ID, workspace, builtin and creation date are protected.');
            if (collection === 'workspaces') {
              this.keys(data, ['name', 'description', 'timezone', 'mode']);
              after = z
                .object({
                  id: z.string(),
                  name: z.string().trim().min(1).max(300),
                  mode: z.enum(['personal', 'team']),
                  timezone,
                  createdAt: z.string(),
                  description: z.string().max(100000).optional(),
                })
                .strict()
                .parse(value);
            }
            if (collection === 'types') {
              if (Array.isArray(value.fields))
                value.fields = value.fields.map((f: unknown) => ({
                  id: uid('field'),
                  ...object(f),
                }));
              after = parse(typeSchema, {
                icon: 'circle',
                color: '#4368f5',
                kind: 'note',
                fields: [],
                ...value,
                builtin: false,
              });
              if (new Set(after.fields.map((f: any) => f.id)).size !== after.fields.length)
                fail('duplicate_field', 'Type field IDs must be unique.');
            }
            if (collection === 'resources')
              after = parse(resourceSchema, {
                kind: 'other',
                capacity: 1,
                unit: '',
                timezone: ws.timezone,
                workingWeekdays: [],
                ...value,
              });
            if (collection === 'rules') {
              if (value.type) {
                value.typeId = this.resolve(
                  this.visible(state).types.filter(
                    (t) => !t.workspaceId || t.workspaceId === ws.id,
                  ),
                  value.type,
                  'types',
                  locals,
                ).id;
                delete value.type;
              }
              if (value.owner) {
                value.ownerId = this.resolve(
                  this.visible(state).users.filter((u) =>
                    state.memberships.some((m) => m.userId === u.id && m.workspaceId === ws.id),
                  ),
                  value.owner,
                  'people',
                  locals,
                ).id;
                delete value.owner;
              }
              after = parse(ruleSchema, {
                enabled: true,
                leadMinutes: 0,
                action: 'signal',
                ...value,
              });
            }
            if (collection === 'dependencies') {
              for (const k of ['from', 'to']) {
                value[k + 'Id'] = this.resolve(
                  this.visible(state).entities.filter((e) => e.workspaceId === ws.id),
                  value[k] ?? value[k + 'Id'],
                  'objects',
                  locals,
                ).id;
                delete value[k];
              }
              after = parse(dependencySchema, { kind: 'finish-start', lagMinutes: 0, ...value });
            }
            if (target) list[list.findIndex((v) => v.id === target.id)] = after;
            else list.push(after);
          }
          if (op.key) locals.set(op.key, after.id);
          this.store.audit(state, this.user.id, ws.id, `${collection}-${op.op}`, op.reason ?? '', {
            entityId: collection === 'objects' ? (after?.id ?? target.id) : undefined,
            before,
            after,
          });
          results.push({
            index,
            op: op.op,
            collection,
            id: after?.id ?? target.id,
            ...(after
              ? collection === 'objects'
                ? { object: compact(after) }
                : { value: after }
              : {}),
            ...(op.key ? { key: op.key } : {}),
          });
        } catch (error) {
          if (error instanceof ApiError)
            throw new ApiError(error.status, error.message, error.code, {
              operation: index,
              ...(typeof error.details === 'object' &&
              error.details !== null &&
              !Array.isArray(error.details)
                ? (error.details as object)
                : { cause: error.details }),
            });
          if (error instanceof z.ZodError)
            throw new ApiError(400, 'Invalid operation data.', 'validation', {
              operation: index,
              issues: error.issues,
            });
          throw error;
        }
      }
      for (const e of state.entities.filter((e) => touched.has(e.workspaceId)))
        this.store.validateEntityReferences(e, state);
      for (const d of state.dependencies.filter((d) => touched.has(d.workspaceId))) {
        const errors = validateDependency(d, {
          ...state,
          dependencies: state.dependencies.filter((v) => v.id !== d.id),
        });
        if (errors.length) fail('invalid_dependency', 'Invalid dependency or cycle.', errors);
      }
      for (const r of state.rules.filter((r) => touched.has(r.workspaceId)))
        this.validateRule(r, state);
      this.store.invalidateScenarios(state);
      response = {
        requestId: args.requestId,
        revision: state.revision + 1,
        results,
        refs: Object.fromEntries(locals),
      };
      const workspaceIds = [...touched].filter((id) => state.workspaces.some((w) => w.id === id));
      const proof = workItemProof(args, this.scope, this.user, response, workspaceIds);
      this.store.db
        .prepare(
          'INSERT INTO agent_requests(user_id,request_id,fingerprint,workspace_ids,response,proof_profile,proof_json) VALUES(?,?,?,?,?,?,?)',
        )
        .run(
          this.user.id,
          args.requestId,
          fingerprint,
          JSON.stringify(workspaceIds),
          JSON.stringify(response),
          proof?.protocol ?? null,
          proof ? JSON.stringify(proof) : null,
        );
    });
    return response;
  }
  private validateRule(r: Rule, state: PlannerSnapshot) {
    if (
      r.typeId &&
      !state.types.some(
        (t) => t.id === r.typeId && (!t.workspaceId || t.workspaceId === r.workspaceId),
      )
    )
      fail('invalid_rule', 'Rule type is unavailable.');
    if (
      r.ownerId &&
      !state.memberships.some((m) => m.workspaceId === r.workspaceId && m.userId === r.ownerId)
    )
      fail('invalid_rule', 'Rule owner is unavailable.');
  }
}
