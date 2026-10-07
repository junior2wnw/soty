import { z } from 'zod';
import type { TimeRange } from '../shared/types.ts';
import { NS_PER_MINUTE, parseNs, preciseRangeErrors } from '../shared/precise-time.ts';

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
    public code = 'invalid_request',
    public details?: unknown,
  ) {
    super(message);
  }
}
export const id = z
  .string()
  .min(1)
  .max(160)
  .regex(/^[A-Za-z0-9_.:-]+$/);
export const iso = z
  .string()
  .max(64)
  .refine((v) => Number.isFinite(Date.parse(v)), 'Invalid date');
export const timezone = z
  .string()
  .max(100)
  .refine((v) => {
    try {
      new Intl.DateTimeFormat('en', { timeZone: v });
      return true;
    } catch {
      return false;
    }
  }, 'Unknown timezone');
export const nanoseconds = z
  .string()
  .max(40)
  .refine(
    (value) => parseNs(value) !== null,
    'Use a canonical decimal integer nanosecond coordinate',
  );
const minuteGrid = (value: number) => value === 0 || Math.abs(value) >= 1 / Number(NS_PER_MINUTE);
const minuteGridMessage =
  'Nonzero minute durations must be at least 1 ns; numeric minutes are rounded to the nearest ns';
export const preciseTimeRange = z
  .object({
    scale: z.literal('unix-nanoseconds'),
    start: nanoseconds.nullable(),
    end: nanoseconds.nullable(),
    earliest: nanoseconds.nullable().optional(),
    latest: nanoseconds.nullable().optional(),
    resolutionNs: nanoseconds
      .refine((value) => parseNs(value)! > 0n, 'Resolution must be positive')
      .optional(),
  })
  .strict();
export const range = z
  .object({
    start: iso.nullable(),
    end: iso.nullable(),
    timezone,
    precision: z.enum(['exact', 'day', 'month', 'approximate', 'unknown']),
    earliest: iso.nullable().optional(),
    latest: iso.nullable().optional(),
    precise: preciseTimeRange.optional(),
  })
  .strict()
  .superRefine((value, context) => {
    for (const message of preciseRangeErrors(value)) context.addIssue({ code: 'custom', message });
  });
export const role = z.enum(['owner', 'editor', 'approver', 'viewer']);
export const entitySchema = z
  .object({
    id,
    workspaceId: id,
    typeId: id,
    kind: z.enum(['point', 'period', 'process', 'note', 'metric']),
    title: z.string().trim().min(1).max(500),
    description: z.string().max(100_000),
    parentId: id.nullable(),
    ownerId: id.nullable(),
    participantIds: z.array(id).max(1000),
    status: z.enum(['draft', 'planned', 'active', 'done', 'cancelled']),
    plan: range,
    baseline: range,
    actual: range.nullable(),
    forecast: range.nullable(),
    forecastProvenance: z.enum(['manual', 'derived']).optional(),
    dueAt: iso.nullable(),
    tags: z.array(z.string().max(100)).max(100),
    fields: z.record(
      z.string().max(100),
      z.union([z.string().max(100_000), z.number().finite(), z.boolean(), z.null()]),
    ),
    links: z
      .array(
        z
          .object({
            id,
            label: z.string().max(500),
            url: z
              .string()
              .max(5000)
              .refine(
                (v) => /^https?:\/\//i.test(v) || /^\/api\/files\/[A-Za-z0-9_.:-]+$/.test(v),
                'Use an HTTP link or an uploaded file',
              ),
            kind: z.enum(['url', 'file']),
            fileId: id.optional(),
          })
          .strict(),
      )
      .max(200),
    allocations: z
      .array(z.object({ resourceId: id, amount: z.number().positive().finite() }).strict())
      .max(500),
    recurrence: z
      .object({
        frequency: z.enum(['day', 'week', 'month']),
        interval: z.number().int().min(1).max(365),
        weekdays: z.array(z.number().int().min(1).max(7)).max(7).optional(),
        count: z.number().int().min(1).max(100000).optional(),
        until: iso.optional(),
        exceptions: z.array(iso).max(10000),
        calendarPolicy: z.enum(['adjust', 'skip-invalid']).optional(),
        durationPolicy: z.enum(['calendar', 'elapsed']).optional(),
      })
      .strict()
      .nullable(),
    source: z
      .object({
        kind: z.enum(['manual', 'sample', 'import', 'webhook']),
        label: z.string().max(500),
        externalId: z.string().max(500).optional(),
        observedAt: iso,
        receivedAt: iso,
        staleAfterMinutes: z.number().positive().max(525600).optional(),
      })
      .strict(),
    createdAt: iso,
    updatedAt: iso,
    version: z.number().int().positive(),
  })
  .strict();
export const resourceSchema = z
  .object({
    id,
    workspaceId: id,
    name: z.string().trim().min(1).max(300),
    kind: z.enum(['person', 'equipment', 'place', 'budget', 'other']),
    capacity: z.number().positive().finite(),
    unit: z.string().max(100),
    timezone,
    workingWeekdays: z.array(z.number().int().min(1).max(7)).max(7),
  })
  .strict();
export const ruleSchema = z
  .object({
    id,
    workspaceId: id,
    name: z.string().trim().min(1).max(300),
    enabled: z.boolean(),
    trigger: z.enum(['before-start', 'before-due', 'after-done', 'overdue', 'resource-conflict']),
    typeId: id.optional(),
    leadMinutes: z.number().finite().min(0).max(525600).refine(minuteGrid, minuteGridMessage),
    action: z.enum(['signal', 'create-followup']),
    followupTitle: z.string().max(500).optional(),
    ownerId: id.optional(),
  })
  .strict();
export const dependencySchema = z
  .object({
    id,
    workspaceId: id,
    fromId: id,
    toId: id,
    kind: z.enum(['finish-start', 'start-start', 'finish-finish', 'related']),
    lagMinutes: z.number().finite().min(-525600).max(525600).refine(minuteGrid, minuteGridMessage),
  })
  .strict();
export const typeSchema = z
  .object({
    id,
    workspaceId: id.optional(),
    label: z.string().trim().min(1).max(100),
    icon: z.string().max(100),
    color: z.string().regex(/^#[0-9a-f]{3,8}$/i),
    kind: z.enum(['point', 'period', 'process', 'note', 'metric']),
    builtin: z.boolean().optional(),
    fields: z
      .array(
        z
          .object({
            id,
            label: z.string().trim().min(1).max(100),
            type: z.enum(['text', 'number', 'boolean', 'date', 'url', 'select']),
            required: z.boolean().optional(),
            options: z.array(z.string().max(200)).max(100).optional(),
          })
          .strict(),
      )
      .max(100),
  })
  .strict();
export const templateSchema = z
  .object({
    id,
    workspaceId: id.optional(),
    name: z.string().trim().min(1).max(300),
    description: z.string().max(100000),
    anchorDate: iso.optional(),
    anchorNs: nanoseconds.optional(),
    timezone: timezone.optional(),
    items: z
      .array(
        z
          .object({
            key: id,
            parentKey: id.optional(),
            title: z.string().trim().min(1).max(500),
            typeId: id,
            kind: z.enum(['point', 'period', 'process', 'note', 'metric']),
            offsetDays: z.number().finite(),
            durationDays: z.number().finite().nonnegative(),
            fields: z
              .record(z.string(), z.union([z.string(), z.number().finite(), z.boolean(), z.null()]))
              .optional(),
            description: z.string().max(100000).optional(),
            schedule: range.optional(),
            recurrence: entitySchema.shape.recurrence.optional(),
            allocations: entitySchema.shape.allocations.optional(),
            dueAt: iso.nullable().optional(),
            tags: z.array(z.string().max(100)).max(100).optional(),
          })
          .strict(),
      )
      .min(1)
      .max(1000),
    dependencies: z
      .array(
        z
          .object({
            fromKey: id,
            toKey: id,
            kind: z.enum(['finish-start', 'start-start', 'finish-finish', 'related']),
            lagMinutes: z
              .number()
              .finite()
              .min(-525600)
              .max(525600)
              .refine(minuteGrid, minuteGridMessage),
          })
          .strict(),
      )
      .max(5000),
  })
  .strict();
export const preferencesSchema = z
  .object({
    quietStart: z.string().regex(/^([01]\d|2[0-3]):[0-5]\d$/),
    quietEnd: z.string().regex(/^([01]\d|2[0-3]):[0-5]\d$/),
    timezone,
    browserEnabled: z.boolean(),
    repeatMinutes: z.number().int().min(1).max(525600),
    escalationMinutes: z.number().int().min(1).max(525600),
  })
  .strict();
export function parse<T>(schema: z.ZodType<T>, value: unknown): T {
  const result = schema.safeParse(value);
  if (!result.success)
    throw new ApiError(
      400,
      'Проверьте данные запроса',
      'validation',
      result.error.issues.map((v) => ({ path: v.path.join('.'), message: v.message })),
    );
  return result.data;
}
export function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new ApiError(400, 'Ожидается объект JSON');
  return value as Record<string, unknown>;
}
export function blankRange(tz = 'Asia/Yekaterinburg'): TimeRange {
  return { start: null, end: null, timezone: tz, precision: 'unknown' };
}
export function text(value: unknown, fallback = '') {
  return typeof value === 'string' ? value : fallback;
}
