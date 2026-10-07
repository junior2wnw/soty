import { createHash } from 'node:crypto';
import { DateTime } from 'luxon';
import { z } from 'zod';
import type { Tool } from '@modelcontextprotocol/sdk/types.js';
import {
  PlannerAgentService,
  plannerToolDefinitions,
  normalizeAgentDate,
} from './agent-service.ts';
import { ApiError, parse, entitySchema, text } from './validation.ts';
import { exportContent, importContent } from './imports.ts';
import {
  applyTemplate,
  validateDependency,
  evaluateSignals,
  computeResourceConflicts,
} from '../shared/engine.ts';
import type {
  Entity,
  PlannerSnapshot,
  Role,
  Scenario,
  Template,
  TimeRange,
  User,
} from '../shared/types.ts';
import { PlannerStore, uid } from './store.ts';
import {
  canonicalRange,
  isoToNs,
  nsToISO,
  parseNs,
  rangeFromNs,
  shiftRangeNs,
} from '../shared/precise-time.ts';
import type { AgentScope } from './agent-auth.ts';
import type { PlannerToolBackend } from './mcp.ts';

const ref = z.string().trim().min(1).max(500);
const nanoseconds = z
  .string()
  .regex(/^(?:0|-?[1-9]\d*)$/)
  .max(40);
const mutation = {
  requestId: z.string().min(1).max(200).optional(),
  expectedRevision: z.number().int().nonnegative().optional(),
};
const changes = z
  .object({
    object: ref,
    expectedVersion: z.number().int().positive().optional(),
    start: z.string().nullable().optional(),
    end: z.string().nullable().optional(),
    startNs: nanoseconds.nullable().optional(),
    endNs: nanoseconds.nullable().optional(),
    shiftNs: nanoseconds.optional(),
    shiftMinutes: z.number().finite().min(-52560000).max(52560000).optional(),
  })
  .strict();
const schemas = {
  planner_scenario: z
    .object({
      action: z.enum(['preview', 'create', 'submit', 'approve', 'reject']),
      workspace: ref.optional(),
      scenario: ref.optional(),
      name: z.string().min(1).max(300).optional(),
      reason: z.string().max(8000).optional(),
      changes: z.array(changes).min(1).max(100).optional(),
      confirm: z.boolean().optional(),
      ...mutation,
    })
    .strict(),
  planner_transfer: z
    .object({
      action: z.enum(['export', 'import']),
      workspace: ref.optional(),
      format: z.enum(['json', 'csv', 'ics']).default('json'),
      content: z
        .union([
          z.string().max(1500000),
          z.record(z.string(), z.unknown()),
          z.array(z.record(z.string(), z.unknown())),
        ])
        .optional(),
      ...mutation,
    })
    .strict(),
  planner_template: z
    .object({
      action: z.enum(['save', 'apply', 'rename', 'delete']),
      workspace: ref.optional(),
      template: ref.optional(),
      object: ref.optional(),
      name: z.string().trim().min(1).max(300).optional(),
      start: z.string().optional(),
      anchorNs: nanoseconds.optional(),
      title: z.string().max(500).optional(),
      confirm: z.boolean().optional(),
      ...mutation,
    })
    .strict(),
  planner_attention: z
    .object({
      action: z.enum(['ack', 'snooze', 'resolve', 'accept-risk']),
      workspace: ref.optional(),
      signal: ref,
      until: z.string().optional(),
      reason: z.string().max(8000).optional(),
      ...mutation,
    })
    .strict(),
  planner_file: z
    .object({
      action: z.enum(['attach', 'read', 'delete']),
      object: ref.optional(),
      file: ref.optional(),
      name: z.string().min(1).max(500).optional(),
      mime: z.string().max(200).optional(),
      text: z.string().max(1000000).optional(),
      base64: z.string().max(1400000).optional(),
      format: z.enum(['text', 'base64']).default('base64'),
      expectedVersion: z.number().int().positive().optional(),
      ...mutation,
    })
    .strict(),
};
const descriptions: Record<string, string> = {
  planner_file:
    'Attach a real file using text or base64, or read/delete an existing uploaded file by ID. Limit1MB through MCP; larger files use the website. Attach requires object, name, expectedVersion and requestId. Delete requires expectedRevision and requestId. No arbitrary server paths.',
  planner_scenario:
    'Preview or propose a schedule change with dependency cascades and conflicts. Exact events use decimal-string startNs/endNs/shiftNs and never JSON numbers. A preview never changes data. Create requires expectedVersion for each object. Submit then approve (confirm:true) using the returned scenario ID; preserve baseline and actual dates. Reuse requestId on retries.',
  planner_transfer:
    'Export/import JSON (full logical model with exact decimal-string nanoseconds), CSV or interoperable ICS. Import creates copies with new IDs and requires requestId. Files/accounts/history are excluded, with explicit warnings. CSV exposes precise as JSON; ICS rejects nanosecond semantics instead of silently losing them.',
  planner_template:
    'Save an object and its descendants as a reusable template, then apply it to a new start date or exact decimal-string anchorNs. Supports undated notes, repeats, uncertainty and exact nanosecond schedules. Optional expectedRevision protects an observed snapshot. A new instance gets fresh IDs and no historical facts.',
  planner_attention:
    'Handle an existing signal by ID: acknowledge, snooze, resolve or accept-risk. Accept-risk requires an owner/approver and a reason. Resolve refuses a still-active condition. Use planner_read signals to discover IDs.',
};
const extraTools = Object.entries(schemas).map(([name, schema]) => ({
  name,
  description: descriptions[name],
  inputSchema: z.toJSONSchema(schema, { io: 'input' }),
  annotations: {
    readOnlyHint: false,
    destructiveHint: false,
    idempotentHint: true,
    openWorldHint: false,
  },
})) as Tool[];
const canonical = (v: unknown): string =>
  Array.isArray(v)
    ? `[${v.map(canonical).join(',')}]`
    : v && typeof v === 'object'
      ? `{${Object.keys(v)
          .sort()
          .map((k) => `${JSON.stringify(k)}:${canonical((v as Record<string, unknown>)[k])}`)
          .join(',')}}`
      : (JSON.stringify(v) ?? 'null');
const label = (v: { id: string; title?: string; name?: string }) => v.title ?? v.name ?? v.id;
function resolveRef<T extends { id: string; title?: string; name?: string }>(
  items: T[],
  value: string | undefined,
  collection: string,
): T {
  const matches = items.filter((v) => v.id === value);
  const found = matches.length ? matches : items.filter((v) => label(v) === value);
  if (found.length === 1) return found[0];
  if (found.length > 1)
    throw new ApiError(
      409,
      `Имя неоднозначно: используйте ID из ${collection}`,
      'ambiguous_reference',
      { choices: found.map((v) => ({ id: v.id, name: label(v) })) },
    );
  throw new ApiError(
    404,
    `Объект ${collection} недоступен. Прочитайте ${collection} и укажите ID.`,
    'not_found',
  );
}
const compactScenario = (s: Scenario) => ({
  id: s.id,
  workspaceId: s.workspaceId,
  name: s.name,
  state: s.state,
  reason: s.reason,
  preview: s.preview,
});

/** Transport-independent extensions reuse the same domain/ACL and atomic receipt boundary. */
export class PlannerAgentBackend implements PlannerToolBackend {
  readonly service: PlannerAgentService;
  readonly tools = [...plannerToolDefinitions, ...extraTools] as Tool[];
  private readonly identity: string;
  constructor(
    private store: PlannerStore,
    private user: User,
    private scope: AgentScope = {},
  ) {
    this.service = new PlannerAgentService(store, user, scope);
    this.identity = `${user.id}:${scope.keyId ?? canonical(scope)}`;
    store.db.exec(
      'CREATE TABLE IF NOT EXISTS agent_extra_requests (identity TEXT NOT NULL, request_id TEXT NOT NULL, fingerprint TEXT NOT NULL, response TEXT NOT NULL, PRIMARY KEY(identity,request_id))',
    );
  }
  call(name: string, input: unknown): Record<string, unknown> {
    if (name in schemas) {
      if (name === 'planner_scenario') return this.scenario(parse(schemas.planner_scenario, input));
      if (name === 'planner_transfer') return this.transfer(parse(schemas.planner_transfer, input));
      if (name === 'planner_template') return this.template(parse(schemas.planner_template, input));
      if (name === 'planner_file') return this.file(parse(schemas.planner_file, input));
      return this.attention(parse(schemas.planner_attention, input));
    }
    const result = this.service.call(name, input);
    if (name === 'planner_help')
      return {
        ...result,
        extensions: Object.fromEntries(extraTools.map((t) => [t.name, t.description])),
        examplesForExtensions: {
          preview: {
            tool: 'planner_scenario',
            arguments: {
              action: 'preview',
              workspace: 'personal',
              changes: [{ object: 'event-id', shiftMinutes: 1440 }],
            },
          },
          precisePreview: {
            tool: 'planner_scenario',
            arguments: {
              action: 'preview',
              workspace: 'personal',
              changes: [
                {
                  object: 'exact-event-id',
                  startNs: '1798761600000000000',
                  endNs: '1798761600000000001',
                },
              ],
            },
          },
          propose: {
            tool: 'planner_scenario',
            arguments: {
              action: 'create',
              workspace: 'personal',
              name: 'Перенос на день',
              requestId: 'unique-proposal-1',
              changes: [{ object: 'event-id', expectedVersion: 1, shiftMinutes: 1440 }],
            },
          },
          approve:
            'Call planner_scenario submit with scenario ID and a new requestId, then approve with confirm:true and another requestId. Workspace is inferred from an accessible unique scenario ID. Read the preview before approving.',
          export: {
            tool: 'planner_transfer',
            arguments: { action: 'export', workspace: 'personal', format: 'json' },
          },
          template: {
            tool: 'planner_template',
            arguments: {
              action: 'apply',
              workspace: 'personal',
              template: 'template-id',
              start: '2027-06-01',
              requestId: 'unique-template-run-1',
            },
          },
          preciseTemplate: {
            tool: 'planner_template',
            arguments: {
              action: 'apply',
              workspace: 'personal',
              template: 'exact-template-id',
              anchorNs: '1798761600000000000',
              requestId: 'unique-exact-template-run-1',
            },
          },
        },
      };
    return result;
  }
  private revision(state: PlannerSnapshot, expected?: number) {
    if (expected !== undefined && expected !== state.revision)
      throw new ApiError(
        409,
        'Данные изменились. Прочитайте актуальную revision.',
        'version_conflict',
        { expectedRevision: expected, actualRevision: state.revision },
      );
  }
  private receipt(
    name: string,
    input: { requestId?: string },
    workspaceId: string,
    roles: Role[] = ['owner', 'editor'],
  ) {
    this.service.assertWrite(workspaceId, roles);
    if (!input.requestId)
      throw new ApiError(
        400,
        'Для изменения нужен уникальный requestId. Повторяйте тот же ID и аргументы после потери ответа.',
        'request_id_required',
      );
    const fingerprint = createHash('sha256').update(canonical({ name, input })).digest('hex');
    const old = this.store.db
      .prepare(
        'SELECT fingerprint,response FROM agent_extra_requests WHERE identity=? AND request_id=?',
      )
      .get(this.identity, input.requestId) as { fingerprint: string; response: string } | undefined;
    if (old && old.fingerprint !== fingerprint)
      throw new ApiError(
        409,
        'Этот requestId уже использован с другими аргументами.',
        'idempotency_conflict',
      );
    return {
      replay: old
        ? ({ ...JSON.parse(old.response), replayed: true } as Record<string, unknown>)
        : undefined,
      save: (response: Record<string, unknown>) => {
        const saved = { workspaceId, ...response };
        this.store.db
          .prepare(
            'INSERT INTO agent_extra_requests(identity,request_id,fingerprint,response) VALUES(?,?,?,?)',
          )
          .run(this.identity, input.requestId!, fingerprint, JSON.stringify(saved));
        return saved;
      },
    };
  }
  private scenario(input: z.infer<typeof schemas.planner_scenario>) {
    const snapshot = this.service.snapshot();
    const inferredWorkspace =
      input.workspace ??
      (input.scenario
        ? resolveRef(snapshot.scenarios, input.scenario, 'scenarios').workspaceId
        : input.changes?.length
          ? resolveRef(snapshot.entities, input.changes[0].object, 'objects').workspaceId
          : undefined);
    const workspaceId = this.service.resolveWorkspace(inferredWorkspace);
    const ws = snapshot.workspaces.find((w) => w.id === workspaceId)!;
    const normalize = () => {
      if (!input.changes?.length)
        throw new ApiError(
          400,
          'Нужны changes: [{object,start,end}] или [{object,shiftMinutes}].',
          'changes_required',
        );
      return input.changes.map((change) => {
        const entity = resolveRef(
          snapshot.entities.filter((e) => e.workspaceId === workspaceId),
          change.object,
          'objects',
        );
        if (input.action === 'create' && change.expectedVersion === undefined)
          throw new ApiError(
            400,
            'Создание сценария требует expectedVersion для каждого объекта.',
            'version_required',
            { object: entity.id, currentVersion: entity.version },
          );
        if (change.expectedVersion !== undefined && change.expectedVersion !== entity.version)
          throw new ApiError(409, 'Версия объекта изменилась.', 'version_conflict', {
            object: entity.id,
            currentVersion: entity.version,
          });
        const hasShift = change.shiftMinutes !== undefined || change.shiftNs !== undefined;
        const hasCoordinates =
          change.start !== undefined ||
          change.end !== undefined ||
          change.startNs !== undefined ||
          change.endNs !== undefined;
        if (change.shiftMinutes !== undefined && change.shiftNs !== undefined)
          throw new ApiError(400, 'Используйте shiftMinutes или shiftNs.', 'conflicting_dates');
        if (hasShift && hasCoordinates)
          throw new ApiError(
            400,
            'Используйте сдвиг или новые start/end координаты.',
            'conflicting_dates',
          );
        if (!hasShift && !hasCoordinates)
          throw new ApiError(
            400,
            'Укажите start/end, startNs/endNs, shiftMinutes или shiftNs.',
            'changes_required',
          );
        if (
          (change.startNs !== undefined || change.endNs !== undefined) &&
          (change.start !== undefined || change.end !== undefined)
        )
          throw new ApiError(
            400,
            'В одном изменении используйте календарные start/end или точные startNs/endNs.',
            'conflicting_dates',
          );
        let plan: TimeRange = structuredClone(entity.plan);
        const rebuildPrecise = (
          before: TimeRange,
          start: bigint | null,
          end: bigint | null,
        ): TimeRange => {
          const next = rangeFromNs(start, end, before.timezone, before.precision);
          next.precise = { ...before.precise, ...next.precise! };
          for (const edge of ['earliest', 'latest'] as const) {
            if (edge in before) next[edge] = before[edge];
            if (before.precise && edge in before.precise) next.precise[edge] = before.precise[edge];
          }
          return next;
        };
        if (hasShift) {
          if (!plan.start && !plan.end && !plan.precise?.start && !plan.precise?.end)
            throw new ApiError(
              400,
              'Объект без координат нельзя сдвинуть; задайте start или startNs.',
              'undated_object',
            );
          if (plan.precise) {
            if (change.shiftMinutes !== undefined && !Number.isSafeInteger(change.shiftMinutes))
              throw new ApiError(
                400,
                'Для точного события shiftMinutes должен быть безопасным целым числом; для иной точности используйте decimal-string shiftNs.',
                'inexact_shift',
              );
            const delta =
              change.shiftNs !== undefined
                ? BigInt(change.shiftNs)
                : BigInt(change.shiftMinutes!) * 60_000_000_000n;
            plan = shiftRangeNs(plan, delta);
          } else {
            if (change.shiftNs !== undefined)
              throw new ApiError(
                400,
                'shiftNs применяется к объекту с plan.precise; сначала задайте startNs/endNs.',
                'precise_range_required',
              );
            for (const key of ['start', 'end'] as const)
              if (plan[key])
                plan[key] = DateTime.fromISO(plan[key]!)
                  .plus({ minutes: change.shiftMinutes })
                  .toUTC()
                  .toISO();
            for (const key of ['earliest', 'latest'] as const)
              if (plan[key])
                plan[key] = DateTime.fromISO(plan[key]!)
                  .plus({ minutes: change.shiftMinutes })
                  .toUTC()
                  .toISO();
          }
        } else if (change.startNs !== undefined || change.endNs !== undefined) {
          if (entity.recurrence)
            throw new ApiError(
              400,
              'Точные наносекундные координаты пока не поддерживают календарные повторения.',
              'precise_recurrence_unsupported',
            );
          const start =
            change.startNs !== undefined
              ? change.startNs
              : (plan.precise?.start ??
                (plan.start ? isoToNs(plan.start, plan.timezone)?.toString() : null));
          const end =
            change.endNs !== undefined
              ? change.endNs
              : (plan.precise?.end ??
                (plan.end ? isoToNs(plan.end, plan.timezone)?.toString() : null));
          plan = rebuildPrecise(plan, parseNs(start), parseNs(end));
        } else {
          for (const key of ['start', 'end'] as const)
            if (change[key] !== undefined)
              plan[key] =
                change[key] === null ? null : normalizeAgentDate(change[key]!, plan.timezone);
          if (plan.precise) {
            const start =
              change.start !== undefined
                ? plan.start
                  ? (isoToNs(plan.start, plan.timezone)?.toString() ?? null)
                  : null
                : plan.precise.start;
            const end =
              change.end !== undefined
                ? plan.end
                  ? (isoToNs(plan.end, plan.timezone)?.toString() ?? null)
                  : null
                : plan.precise.end;
            plan = rebuildPrecise(plan, parseNs(start), parseNs(end));
          }
        }
        return { entityId: entity.id, plan };
      });
    };
    if (input.action === 'preview')
      return {
        revision: snapshot.revision,
        workspaceId,
        timezone: ws.timezone,
        preview: this.store.preview(this.user, { workspaceId, changes: normalize() }),
        semantics:
          'shiftMinutes shifts elapsed time; shiftNs and startNs/endNs are exact decimal-string nanoseconds. Date-only start/end use the object timezone. End is exclusive.',
      };
    const roles: Role[] = ['approve', 'reject'].includes(input.action)
      ? ['owner', 'approver']
      : ['owner', 'editor'];
    const receipt = this.receipt('planner_scenario', input, workspaceId, roles);
    if (receipt.replay) return receipt.replay;
    let response: Record<string, unknown> = {};
    const hooks = {
      before: (state: PlannerSnapshot) => this.revision(state, input.expectedRevision),
      after: (state: PlannerSnapshot, s: Scenario) => {
        response = receipt.save({ revision: state.revision + 1, scenario: compactScenario(s) });
      },
    };
    if (input.action === 'create')
      this.store.createScenario(
        this.user,
        { workspaceId, name: input.name, reason: input.reason, changes: normalize() },
        hooks,
      );
    else {
      const existing = resolveRef(
        snapshot.scenarios.filter((s) => s.workspaceId === workspaceId),
        input.scenario,
        'scenarios',
      );
      if (input.action === 'approve' && input.confirm !== true)
        throw new ApiError(
          400,
          'Проверьте preview и укажите confirm:true для утверждения.',
          'confirmation_required',
        );
      this.store.decideScenario(this.user, existing.id, input.action, input.reason, hooks);
    }
    return response;
  }
  private transfer(input: z.infer<typeof schemas.planner_transfer>) {
    const workspaceId = this.service.resolveWorkspace(input.workspace);
    if (input.action === 'export') {
      const snapshot = this.service.snapshot();
      const output = exportContent(snapshot, workspaceId, input.format);
      if (Buffer.byteLength(output.content) > 1500000)
        throw new ApiError(
          413,
          'Экспорт больше лимита MCP. Скачайте файл в Настройки → Данные.',
          'export_too_large',
        );
      return { revision: snapshot.revision, workspaceId, format: input.format, ...output };
    }
    const receipt = this.receipt('planner_transfer', input, workspaceId);
    if (receipt.replay) return receipt.replay;
    if (input.content === undefined)
      throw new ApiError(400, 'Импорт требует content.', 'content_required');
    let prior = new Set<string>();
    let response: Record<string, unknown> = {};
    importContent(
      this.store,
      this.user,
      { workspaceId, format: input.format, content: input.content },
      {
        before: (state) => {
          this.revision(state, input.expectedRevision);
          prior = new Set(state.entities.map((e) => e.id));
        },
        after: (state, warnings) => {
          response = receipt.save({
            revision: state.revision + 1,
            workspaceId,
            created: state.entities
              .filter((e) => e.workspaceId === workspaceId && !prior.has(e.id))
              .map((e) => ({ id: e.id, title: e.title, version: e.version })),
            warnings,
          });
        },
      },
    );
    return response;
  }
  private template(input: z.infer<typeof schemas.planner_template>) {
    const workspaceId = this.service.resolveWorkspace(input.workspace);
    const snapshot = this.service.snapshot();
    const ws = snapshot.workspaces.find((w) => w.id === workspaceId)!;
    const receipt = this.receipt('planner_template', input, workspaceId);
    if (receipt.replay) return receipt.replay;
    let response: Record<string, unknown> = {};
    this.store.change(
      this.user,
      workspaceId,
      `template-${input.action}`,
      'MCP: шаблон',
      (state) => {
        this.revision(state, input.expectedRevision);
        if (input.action === 'rename' || input.action === 'delete') {
          if (input.expectedRevision === undefined)
            throw new ApiError(
              400,
              'Изменение шаблона требует expectedRevision.',
              'revision_required',
            );
          const template = resolveRef(
            snapshot.templates.filter((t) => t.workspaceId === workspaceId),
            input.template,
            'templates',
          );
          const current = state.templates.find((t) => t.id === template.id)!;
          if (input.action === 'delete') {
            if (input.confirm !== true)
              throw new ApiError(
                400,
                'Удаление шаблона требует confirm:true.',
                'confirmation_required',
              );
            state.templates = state.templates.filter((t) => t.id !== current.id);
            response = receipt.save({ revision: state.revision + 1, deleted: current.id });
          } else {
            if (!input.name) throw new ApiError(400, 'Укажите новое name.', 'name_required');
            current.name = input.name;
            response = receipt.save({ revision: state.revision + 1, template: current });
          }
          return { after: response };
        }
        if (input.action === 'save') {
          if (!input.name) throw new ApiError(400, 'Укажите name шаблона.', 'name_required');
          const root = resolveRef(
            snapshot.entities.filter((e) => e.workspaceId === workspaceId),
            input.object,
            'objects',
          );
          const ids = new Set([root.id]);
          for (let previous = -1; previous !== ids.size;) {
            previous = ids.size;
            for (const e of snapshot.entities)
              if (e.workspaceId === workspaceId && e.parentId && ids.has(e.parentId)) ids.add(e.id);
          }
          const source = snapshot.entities.filter((e) => ids.has(e.id));
          const hasPreciseSchedule = source.some((e) => e.plan.precise);
          const coordinateStarts = source
            .map((e) => canonicalRange(e.plan).start)
            .filter((value): value is bigint => value !== null);
          const exactAnchor = hasPreciseSchedule
            ? coordinateStarts.reduce<bigint | undefined>(
                (lowest, value) => (lowest === undefined || value < lowest ? value : lowest),
                undefined,
              )
            : undefined;
          const anchorDate =
            exactAnchor !== undefined
              ? nsToISO(exactAnchor, ws.timezone)
              : (root.plan.start ??
                source.find((e) => e.plan.start)?.plan.start ??
                snapshot.serverTime);
          const anchor = DateTime.fromISO(anchorDate ?? snapshot.serverTime).setZone(ws.timezone);
          const template: Template = {
            id: uid('template'),
            workspaceId,
            name: input.name,
            description: `Из «${root.title}»`,
            ...(anchorDate ? { anchorDate } : {}),
            ...(exactAnchor !== undefined ? { anchorNs: exactAnchor.toString() } : {}),
            timezone: ws.timezone,
            items: source.map((e) => ({
              key: e.id,
              ...(e.parentId && ids.has(e.parentId) ? { parentKey: e.parentId } : {}),
              title: e.title,
              typeId: e.typeId,
              kind: e.kind,
              offsetDays: e.plan.start
                ? DateTime.fromISO(e.plan.start).setZone(ws.timezone).diff(anchor, 'days').days
                : 0,
              durationDays:
                e.plan.end && e.plan.start
                  ? DateTime.fromISO(e.plan.end)
                      .setZone(ws.timezone)
                      .diff(DateTime.fromISO(e.plan.start).setZone(ws.timezone), 'days').days
                  : 0,
              fields: e.fields,
              description: e.description,
              schedule: structuredClone(e.plan),
              allocations: e.allocations.map((a) => ({ ...a })),
              recurrence: e.recurrence ? structuredClone(e.recurrence) : null,
              dueAt: e.dueAt,
              tags: [...e.tags],
            })),
            dependencies: snapshot.dependencies
              .filter((d) => ids.has(d.fromId) && ids.has(d.toId))
              .map((d) => ({
                fromKey: d.fromId,
                toKey: d.toId,
                kind: d.kind,
                lagMinutes: d.lagMinutes,
              })),
          };
          state.templates.push(template);
          response = receipt.save({
            revision: state.revision + 1,
            template,
            warnings: [
              'Шаблон копирует структуру, даты, неопределённость, повторы, сроки, поля, теги, связи и назначения существующих ресурсов. Факты, владельцы, файлы и оповещения задаются отдельно для нового экземпляра.',
            ],
          });
        } else {
          const template = resolveRef(
            snapshot.templates.filter((t) => !t.workspaceId || t.workspaceId === workspaceId),
            input.template,
            'templates',
          );
          if (!input.start && input.anchorNs === undefined)
            throw new ApiError(
              400,
              'Укажите start или decimal-string anchorNs новой структуры.',
              'start_required',
            );
          const normalizedStart = input.start
            ? normalizeAgentDate(input.start, ws.timezone)
            : undefined;
          let created: ReturnType<typeof applyTemplate>;
          try {
            created = applyTemplate(template, {
              workspaceId,
              ...(input.start
                ? {
                    anchorDate: /^\d{4}-\d{2}-\d{2}$/.test(input.start)
                      ? input.start
                      : normalizedStart,
                  }
                : {}),
              ...(input.anchorNs !== undefined ? { anchorNs: input.anchorNs } : {}),
              timezone: ws.timezone,
              title: input.title,
              ownerId: this.user.id,
            });
          } catch (error) {
            throw new ApiError(400, (error as Error).message, 'invalid_template');
          }
          state.entities.push(...created.entities);
          for (const e of created.entities) {
            parse(entitySchema, e);
            this.store.validateEntityReferences(e, state);
          }
          for (const d of created.dependencies) {
            const errors = validateDependency(d, state);
            if (errors.length)
              throw new ApiError(400, 'Недопустимая связь шаблона', 'validation', errors);
            state.dependencies.push(d);
          }
          response = receipt.save({
            revision: state.revision + 1,
            workspaceId,
            created: created.entities.map((e) => ({
              id: e.id,
              title: e.title,
              version: e.version,
              plan: e.plan,
            })),
            dependencies: created.dependencies,
            conflicts: computeResourceConflicts({
              ...state,
              resources: state.resources.filter((r) => r.workspaceId === workspaceId),
              entities: state.entities.filter((e) => e.workspaceId === workspaceId),
            }),
          });
        }
        return { after: response };
      },
    );
    return response;
  }
  private file(input: z.infer<typeof schemas.planner_file>) {
    if (input.action !== 'read' && input.requestId) {
      const prior = this.store.db
        .prepare('SELECT response FROM agent_extra_requests WHERE identity=? AND request_id=?')
        .get(this.identity, input.requestId) as { response: string } | undefined;
      if (prior) {
        const saved = JSON.parse(prior.response);
        if (saved.workspaceId) {
          const receipt = this.receipt('planner_file', input, saved.workspaceId);
          if (receipt.replay) return receipt.replay;
        }
      }
    }
    const snapshot = this.service.snapshot();
    if (input.action === 'attach') {
      const entity = resolveRef(snapshot.entities, input.object, 'objects');
      const receipt = this.receipt('planner_file', input, entity.workspaceId);
      if (receipt.replay) return receipt.replay;
      if (!input.name || (input.text === undefined) === (input.base64 === undefined))
        throw new ApiError(
          400,
          'Укажите name и ровно одно из text/base64.',
          'file_content_required',
        );
      if (
        input.base64 !== undefined &&
        !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(input.base64)
      )
        throw new ApiError(400, 'Некорректный base64.', 'invalid_base64');
      const data =
        input.text !== undefined
          ? Buffer.from(input.text, 'utf8')
          : Buffer.from(input.base64!, 'base64');
      if (data.length > 1000000)
        throw new ApiError(
          413,
          'Лимит файла MCP1MB; загрузите больший файл через сайт.',
          'file_too_large',
        );
      const name = input.name.replace(/[\\/\r\n\x00]/g, '_');
      const mime =
        input.mime ??
        (input.text !== undefined ? 'text/plain; charset=utf-8' : 'application/octet-stream');
      let response: Record<string, unknown> = {};
      this.store.change(this.user, entity.workspaceId, 'file-upload', 'MCP: файл', (state) => {
        this.revision(state, input.expectedRevision);
        const current = state.entities.find((e) => e.id === entity.id)!;
        if (input.expectedVersion !== current.version)
          throw new ApiError(
            409,
            'Загрузка требует актуальный expectedVersion объекта.',
            'version_conflict',
            { object: current.id, version: current.version },
          );
        const before = structuredClone(current);
        const fileId = uid('file');
        const now = new Date().toISOString();
        this.store.db
          .prepare(
            'INSERT INTO files(id,workspace_id,entity_id,name,mime,size,data,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?)',
          )
          .run(
            fileId,
            current.workspaceId,
            current.id,
            name,
            mime,
            data.length,
            data,
            this.user.id,
            now,
          );
        current.links.push({
          id: uid('link'),
          label: name,
          kind: 'file',
          url: `/api/files/${fileId}`,
          fileId,
        });
        current.version++;
        current.updatedAt = now;
        this.store.validateEntityReferences(current, state);
        response = receipt.save({
          revision: state.revision + 1,
          file: { id: fileId, name, mime, size: data.length, url: `/api/files/${fileId}` },
          object: { id: current.id, version: current.version },
        });
        return { entityId: current.id, before, after: current };
      });
      return response;
    }
    if (!input.file) throw new ApiError(400, 'Укажите ID файла из links объекта.', 'file_required');
    const row = this.store.db
      .prepare('SELECT id,workspace_id,name,mime,size,data FROM files WHERE id=?')
      .get(input.file) as
      | {
          id: string;
          workspace_id: string;
          name: string;
          mime: string;
          size: number;
          data: Uint8Array;
        }
      | undefined;
    if (!row || !snapshot.workspaces.some((w) => w.id === row.workspace_id))
      throw new ApiError(404, 'Файл недоступен.', 'not_found');
    if (input.action === 'read') {
      if (row.size > 1000000)
        throw new ApiError(413, 'Файл больше1MB; скачайте его через сайт.', 'file_too_large');
      const data = Buffer.from(row.data);
      let content: string;
      try {
        content =
          input.format === 'text'
            ? new TextDecoder('utf-8', { fatal: true }).decode(data)
            : data.toString('base64');
      } catch {
        throw new ApiError(400, 'Файл не UTF-8. Используйте format:base64.', 'not_text');
      }
      return {
        revision: snapshot.revision,
        file: { id: row.id, name: row.name, mime: row.mime, size: row.size },
        format: input.format,
        content,
      };
    }
    const receipt = this.receipt('planner_file', input, row.workspace_id);
    if (receipt.replay) return receipt.replay;
    if (input.expectedRevision === undefined)
      throw new ApiError(
        400,
        'Удаление файла требует expectedRevision; удаляются все ссылки на файл в пространстве.',
        'revision_required',
      );
    let response: Record<string, unknown> = {};
    this.store.change(
      this.user,
      row.workspace_id,
      'file-delete',
      'MCP: удаление файла',
      (state) => {
        this.revision(state, input.expectedRevision);
        const affected = [];
        for (const e of state.entities.filter(
          (e) => e.workspaceId === row.workspace_id && e.links.some((l) => l.fileId === row.id),
        )) {
          e.links = e.links.filter((l) => l.fileId !== row.id);
          e.version++;
          e.updatedAt = new Date().toISOString();
          affected.push({ id: e.id, version: e.version });
        }
        this.store.db.prepare('DELETE FROM files WHERE id=?').run(row.id);
        response = receipt.save({ revision: state.revision + 1, deleted: row.id, affected });
        return { before: { id: row.id, name: row.name }, after: response };
      },
    );
    return response;
  }
  private attention(input: z.infer<typeof schemas.planner_attention>) {
    const snapshot = this.service.snapshot();
    const workspaceId = this.service.resolveWorkspace(input.workspace);
    const signal = resolveRef(
      snapshot.signals.filter((s) => s.workspaceId === workspaceId),
      input.signal,
      'signals',
    );
    const roles: Role[] =
      input.action === 'accept-risk' ? ['owner', 'approver'] : ['owner', 'editor', 'approver'];
    const receipt = this.receipt('planner_attention', input, workspaceId, roles);
    if (receipt.replay) return receipt.replay;
    let response: Record<string, unknown> = {};
    this.store.change(
      this.user,
      workspaceId,
      `signal-${input.action}`,
      text(input.reason),
      (state) => {
        this.revision(state, input.expectedRevision);
        const current = state.signals.find((s) => s.id === signal.id)!;
        const before = structuredClone(current);
        const now = new Date().toISOString();
        if (input.action === 'snooze') {
          const until = input.until
            ? normalizeAgentDate(
                input.until,
                snapshot.workspaces.find((w) => w.id === workspaceId)!.timezone,
              )
            : '';
          if (
            !until ||
            Date.parse(until) <= Date.now() ||
            Date.parse(until) > Date.now() + 30 * 86400000
          )
            throw new ApiError(
              400,
              'Отсрочка должна быть в будущем в пределах 30 дней.',
              'invalid_snooze',
            );
          this.store.db
            .prepare('INSERT OR REPLACE INTO signal_controls(signal_id,snoozed_until) VALUES(?,?)')
            .run(current.id, until);
          for (const n of state.notifications.filter(
            (n) => n.signalId === current.id && ['pending', 'failed'].includes(n.state),
          )) {
            n.state = 'snoozed';
            n.snoozedUntil = until;
          }
          response = { signal: { ...current, snoozedUntil: until } };
        } else {
          if (input.action === 'ack' && ['resolved', 'accepted-risk'].includes(current.state))
            throw new ApiError(409, 'Сигнал уже обработан.', 'signal_processed');
          if (input.action === 'resolve') {
            if (
              evaluateSignals(state, now).some((s) => s.dedupeKey === current.dedupeKey) ||
              state.scenarios.some(
                (s) => `approval:${s.id}` === current.dedupeKey && s.state === 'pending',
              )
            )
              throw new ApiError(
                409,
                'Условие сигнала сохраняется. Исправьте объект или примите риск с причиной.',
                'condition_active',
              );
            current.state = 'resolved';
            current.resolvedAt = now;
          }
          if (input.action === 'ack') {
            current.state = 'acknowledged';
            current.acknowledgedBy = this.user.id;
          }
          if (input.action === 'accept-risk') {
            if (!input.reason?.trim())
              throw new ApiError(400, 'Для принятия риска нужна причина.', 'reason_required');
            current.state = 'accepted-risk';
          }
          current.updatedAt = now;
          response = { signal: current };
        }
        response = receipt.save({ revision: state.revision + 1, ...response });
        return { entityId: current.entityId, before, after: response };
      },
      roles,
    );
    return response;
  }
}
