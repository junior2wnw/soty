import { z } from 'zod';

const ref = z
  .string()
  .min(1)
  .max(500)
  .describe('An ID, exact unique name, or $key created earlier in this batch.');
const date = z
  .string()
  .describe(
    'YYYY-MM-DD or ISO timestamp with Z/UTC offset. Date-only uses the object/workspace timezone.',
  );
const nanoseconds = z
  .string()
  .regex(/^(?:0|-?[1-9]\d*)$/)
  .max(40)
  .describe(
    'Canonical signed integer decimal string of Unix-epoch nanoseconds. Never send this as a JSON number.',
  );
const precise = z
  .object({
    scale: z.literal('unix-nanoseconds').optional(),
    start: nanoseconds.nullable().optional(),
    end: nanoseconds.nullable().optional(),
    earliest: nanoseconds.nullable().optional(),
    latest: nanoseconds.nullable().optional(),
    resolutionNs: nanoseconds
      .refine((value) => !value.startsWith('-') && value !== '0', 'resolutionNs must be positive')
      .optional(),
  })
  .strict()
  .describe(
    'Exact coordinates for the single 1 ns to 1 billion year axis. start/end are authoritative; ISO calendar fields are optional exact mirrors only.',
  );
const kind = z.enum(['point', 'period', 'process', 'note', 'metric']);
const value = z.union([z.string(), z.number().finite(), z.boolean(), z.null()]);
const range = z
  .object({
    start: date.nullable().optional(),
    end: date.nullable().optional(),
    timezone: z.string().optional(),
    precision: z.enum(['exact', 'day', 'month', 'approximate', 'unknown']).optional(),
    earliest: date.nullable().optional(),
    latest: date.nullable().optional(),
    precise: precise.optional(),
  })
  .strict();
const recurrence = z
  .object({
    frequency: z.enum(['day', 'week', 'month']),
    interval: z.number().int().min(1).optional(),
    weekdays: z
      .array(z.number().int().min(1).max(7))
      .optional()
      .describe('ISO weekdays: Monday=1, Sunday=7.'),
    count: z.number().int().positive().optional(),
    until: date.optional(),
    exceptions: z.array(date).optional(),
    calendarPolicy: z.enum(['adjust', 'skip-invalid']).optional(),
    durationPolicy: z.enum(['calendar', 'elapsed']).optional(),
  })
  .strict();
const objects = z
  .object({
    title: z.string().optional(),
    description: z.string().optional(),
    kind: kind.optional(),
    type: ref.optional(),
    typeId: ref.optional(),
    start: date.nullable().optional(),
    end: date.nullable().optional().describe('End of a period; date-only end is exclusive.'),
    due: date.nullable().optional(),
    dueAt: date.nullable().optional(),
    timezone: z.string().optional(),
    precision: z.enum(['exact', 'day', 'month', 'approximate', 'unknown']).optional(),
    precise: precise.optional(),
    plan: range.optional(),
    actual: range.nullable().optional(),
    forecast: range.nullable().optional(),
    owner: ref.nullable().optional(),
    ownerId: ref.nullable().optional(),
    parent: ref.nullable().optional(),
    parentId: ref.nullable().optional(),
    participants: z.array(ref).optional(),
    participantIds: z.array(ref).optional(),
    resources: z
      .array(
        z.union([
          ref,
          z.object({ resource: ref, amount: z.number().positive().optional() }).strict(),
        ]),
      )
      .optional(),
    allocations: z
      .array(z.object({ resourceId: ref, amount: z.number().positive() }).strict())
      .optional(),
    fields: z
      .record(z.string(), value)
      .optional()
      .describe('Values keyed by exact field labels or IDs from the selected type.'),
    tags: z.array(z.string()).optional(),
    links: z
      .array(
        z.union([
          z.string(),
          z
            .object({
              url: z.string(),
              label: z.string().optional(),
              id: z.string().optional(),
              kind: z.enum(['url', 'file']).optional(),
              fileId: z.string().optional(),
            })
            .strict(),
        ]),
      )
      .optional(),
    status: z.enum(['draft', 'planned', 'active', 'done', 'cancelled']).optional(),
    recurrence: recurrence.nullable().optional(),
  })
  .strict()
  .describe(
    'For collection objects. Create requires title. Omitted dates stay unknown. Only supplied update fields change.',
  );
const workspaces = z
  .object({
    name: z.string().optional(),
    description: z.string().optional(),
    timezone: z.string().optional(),
    mode: z.enum(['personal', 'team']).optional(),
  })
  .strict()
  .describe('For workspaces. Create requires name; timezone defaults to Asia/Yekaterinburg.');
const types = z
  .object({
    label: z.string().optional(),
    kind: kind.optional(),
    color: z.string().optional(),
    icon: z.string().optional(),
    fields: z
      .array(
        z
          .object({
            id: z.string().optional(),
            label: z.string(),
            type: z.enum(['text', 'number', 'boolean', 'date', 'url', 'select']),
            required: z.boolean().optional(),
            options: z.array(z.string()).optional().describe('Required choices for select fields.'),
          })
          .strict(),
      )
      .optional(),
  })
  .strict()
  .describe(
    'For types. Field IDs are generated if omitted. Custom fields are enforced on every object.',
  );
const resources = z
  .object({
    name: z.string().optional(),
    kind: z.enum(['person', 'equipment', 'place', 'budget', 'other']).optional(),
    capacity: z.number().positive().optional(),
    unit: z.string().optional(),
    timezone: z.string().optional(),
    workingWeekdays: z.array(z.number().int().min(1).max(7)).optional(),
  })
  .strict()
  .describe(
    'For resources. Defaults: capacity1, unrestricted days. Set ISO weekdays explicitly if the resource has a working calendar.',
  );
const rules = z
  .object({
    name: z.string().optional(),
    enabled: z.boolean().optional(),
    trigger: z
      .enum(['before-start', 'before-due', 'after-done', 'overdue', 'resource-conflict'])
      .optional(),
    type: ref.optional(),
    typeId: ref.optional(),
    leadMinutes: z.number().int().nonnegative().optional(),
    action: z.enum(['signal', 'create-followup']).optional(),
    followupTitle: z.string().optional(),
    owner: ref.optional(),
    ownerId: ref.optional(),
  })
  .strict()
  .describe('For rules. Create requires name and trigger; leadMinutes=60 means one hour before.');
const dependencies = z
  .object({
    from: ref.optional(),
    to: ref.optional(),
    fromId: ref.optional(),
    toId: ref.optional(),
    kind: z.enum(['finish-start', 'start-start', 'finish-finish', 'related']).optional(),
    lagMinutes: z.number().optional(),
  })
  .strict()
  .describe('For dependencies. Create requires from/to; default finish-start, lag0.');
const comment = z
  .object({ text: z.string() })
  .strict()
  .describe('For op comment; ref identifies the object.');
const settings = z
  .object({
    notifications: z
      .object({
        quietStart: z.string().optional(),
        quietEnd: z.string().optional(),
        timezone: z.string().optional(),
        browserEnabled: z.boolean().optional(),
        repeatMinutes: z.number().optional(),
        escalationMinutes: z.number().optional(),
      })
      .strict(),
  })
  .strict()
  .describe(
    'For op settings. Account-wide preferences require an unscoped local owner connection and expectedRevision.',
  );

export const plannerMutationDataSchema = z.toJSONSchema(
  z.union([objects, workspaces, types, resources, rules, dependencies, comment, settings]),
  { io: 'input' },
);
