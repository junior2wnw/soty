import { DateTime, type DurationLikeObject } from 'luxon';
import {
  canonicalRange,
  formatNs,
  hasTime,
  hasSubMillisecondTime,
  isoToNs,
  nsToISO,
  NS_PER_MILLISECOND,
  NS_PER_MINUTE,
  parseNs,
  preciseRangeErrors,
  sameTimeRange,
  shiftRangeNs,
  withRangeEdgeNs,
} from './precise-time.js';
import type {
  AssistantSuggestion,
  Conflict,
  Dependency,
  Entity,
  EntityDraft,
  Occurrence,
  PlanChange,
  PlannerSnapshot,
  ScenarioPreview,
  Signal,
  Template,
  TimeRange,
} from './types.js';

/** Expansion is bounded; callers should request the visible window rather than all time. */
export const MAX_RECURRENCE_OCCURRENCES = 2_000;
const MAX_RECURRENCE_STEPS = 100_000;
const INACTIVE = new Set(['done', 'cancelled']);

/** Legacy numeric minutes use the nearest nanosecond; event coordinates never round. */
function minutesToNs(value: number): bigint {
  const match = value.toString().match(/^(-?)(\d+)(?:\.(\d+))?(?:e([+-]?\d+))?$/i);
  if (!match) return 0n;
  const decimals = match[3] ?? '';
  const magnitude = BigInt(match[2]! + decimals) * NS_PER_MINUTE;
  const power = Number(match[4] ?? 0) - decimals.length;
  const divisor = power < 0 ? 10n ** BigInt(-power) : 1n;
  const result =
    power >= 0 ? magnitude * 10n ** BigInt(power) : (magnitude + divisor / 2n) / divisor;
  return match[1] ? -result : result;
}

export function parseTime(value: string | null | undefined, timezone = 'UTC'): DateTime | null {
  if (!value || typeof value !== 'string') return null;
  const result = DateTime.fromISO(value, { zone: timezone, setZone: true }).setZone(timezone);
  return result.isValid ? result : null;
}

export function emptyRange(timezone = 'UTC'): TimeRange {
  return { start: null, end: null, timezone, precision: 'unknown' };
}

export function getEffectiveRange(entity: Entity, mode: 'plan' | 'actual' | 'forecast'): TimeRange {
  if (mode === 'actual') return entity.actual ?? emptyRange(entity.plan.timezone);
  return mode === 'forecast' ? (entity.forecast ?? entity.plan) : entity.plan;
}

function sameRange(a: TimeRange, b: TimeRange): boolean {
  return sameTimeRange(a, b);
}

function rangeErrors(range: TimeRange | null, label: string): string[] {
  if (!range) return [];
  const errors: string[] = [];
  if (!DateTime.now().setZone(range.timezone).isValid)
    errors.push(`${label}: неизвестный часовой пояс.`);
  if (!['exact', 'day', 'month', 'approximate', 'unknown'].includes(range.precision))
    errors.push(`${label}: неизвестная точность времени.`);
  for (const key of ['start', 'end', 'earliest', 'latest'] as const) {
    if (range[key] != null && !parseTime(range[key], range.timezone))
      errors.push(`${label}: некорректное значение ${key}.`);
  }
  errors.push(...preciseRangeErrors(range).map((error) => `${label}: ${error}`));
  return errors;
}

export function validateEntity(entity: Entity, snapshot: PlannerSnapshot): string[] {
  const errors: string[] = [];
  const workspace = snapshot.workspaces.find((item) => item.id === entity.workspaceId);
  const type = snapshot.types.find(
    (item) =>
      item.id === entity.typeId && (!item.workspaceId || item.workspaceId === entity.workspaceId),
  );
  if (!workspace) errors.push('Пространство не существует.');
  if (!entity.title?.trim()) errors.push('У объекта должно быть название.');
  if (entity.title?.length > 300) errors.push('Название длиннее 300 символов.');
  if (!type) errors.push('Тип объекта не существует в этом пространстве.');
  else if (type.kind !== entity.kind) errors.push('Форма объекта не соответствует его типу.');
  if (!['draft', 'planned', 'active', 'done', 'cancelled'].includes(entity.status))
    errors.push('Неизвестное состояние объекта.');
  for (const [label, range] of [
    ['План', entity.plan],
    ['Базовый план', entity.baseline],
    ['Факт', entity.actual],
    ['Прогноз', entity.forecast],
  ] as const)
    errors.push(...rangeErrors(range, label));
  if (entity.dueAt && !parseTime(entity.dueAt, entity.plan.timezone))
    errors.push('Некорректный срок обязательства.');

  const availableUsers = new Set(
    snapshot.memberships
      .filter((item) => item.workspaceId === entity.workspaceId)
      .map((item) => item.userId),
  );
  if (entity.ownerId && !availableUsers.has(entity.ownerId))
    errors.push('Ответственный не состоит в этом пространстве.');
  for (const id of entity.participantIds)
    if (!availableUsers.has(id)) errors.push('Участник не состоит в этом пространстве.');
  if (entity.parentId) {
    let parentId: string | null = entity.parentId;
    const visited = new Set([entity.id]);
    while (parentId) {
      if (visited.has(parentId)) {
        errors.push('Вложенность образует цикл.');
        break;
      }
      visited.add(parentId);
      const parent = snapshot.entities.find((item) => item.id === parentId);
      if (!parent) {
        errors.push('Родительский объект не существует.');
        break;
      }
      if (parent.workspaceId !== entity.workspaceId) {
        errors.push('Родитель находится в другом пространстве.');
        break;
      }
      parentId = parent.parentId;
    }
  }
  const resourceIds = new Set<string>();
  for (const allocation of entity.allocations) {
    const resource = snapshot.resources.find((item) => item.id === allocation.resourceId);
    if (!resource || resource.workspaceId !== entity.workspaceId)
      errors.push('Ресурс не существует в этом пространстве.');
    if (!Number.isFinite(allocation.amount) || allocation.amount <= 0)
      errors.push('Количество ресурса должно быть положительным.');
    if (resourceIds.has(allocation.resourceId))
      errors.push('Один ресурс указан в распределении дважды.');
    resourceIds.add(allocation.resourceId);
  }
  for (const field of type?.fields ?? []) {
    const value = entity.fields[field.id];
    if (field.required && (value == null || value === ''))
      errors.push(`Заполните поле «${field.label}».`);
    if (value == null || value === '') continue;
    if (field.type === 'number' && (typeof value !== 'number' || !Number.isFinite(value)))
      errors.push(`Поле «${field.label}» должно быть числом.`);
    if (field.type === 'boolean' && typeof value !== 'boolean')
      errors.push(`Поле «${field.label}» должно быть логическим значением.`);
    if (['text', 'date', 'url', 'select'].includes(field.type) && typeof value !== 'string')
      errors.push(`Поле «${field.label}» должно быть текстом.`);
    if (
      field.type === 'date' &&
      typeof value === 'string' &&
      !parseTime(value, entity.plan.timezone)
    )
      errors.push(`В поле «${field.label}» некорректная дата.`);
    if (field.type === 'select' && field.options && !field.options.includes(String(value)))
      errors.push(`Недопустимое значение поля «${field.label}».`);
    if (field.type === 'url' && typeof value === 'string' && !isSafeLink(value))
      errors.push(`В поле «${field.label}» требуется ссылка HTTP или HTTPS.`);
  }
  for (const value of Object.values(entity.fields))
    if (value !== null && !['string', 'number', 'boolean'].includes(typeof value))
      errors.push('Дополнительные поля должны содержать простые значения.');
  for (const link of entity.links)
    if (!isSafeLink(link.url, link.kind === 'file'))
      errors.push('Недопустимая ссылка в содержимом.');
  if (!parseTime(entity.source?.observedAt) || !parseTime(entity.source?.receivedAt))
    errors.push('У источника некорректное время наблюдения или получения.');
  if (
    entity.source?.staleAfterMinutes != null &&
    (!Number.isFinite(entity.source.staleAfterMinutes) || entity.source.staleAfterMinutes <= 0)
  )
    errors.push('Интервал свежести источника должен быть положительным.');
  if (entity.recurrence) {
    if (entity.plan.precise)
      errors.push(
        'Календарное повторение требует календарного плана ISO + IANA; точные наносекундные координаты нельзя повторять как календарные дни.',
      );
    const rule = entity.recurrence;
    if (!['day', 'week', 'month'].includes(rule.frequency))
      errors.push('Неизвестная частота повторения.');
    if (!Number.isInteger(rule.interval) || rule.interval < 1 || rule.interval > 365)
      errors.push('Интервал повторения должен быть целым числом от 1 до 365.');
    if (
      rule.count != null &&
      (!Number.isInteger(rule.count) || rule.count < 1 || rule.count > 100_000)
    )
      errors.push('Число повторений должно быть целым числом от 1 до 100000.');
    if (!entity.plan.start) errors.push('Для повторения нужна начальная дата.');
    if (rule.weekdays?.some((day) => !Number.isInteger(day) || day < 1 || day > 7))
      errors.push('Дни недели задаются числами от 1 до 7.');
    if (rule.weekdays?.length && rule.frequency !== 'week')
      errors.push('Выбор дней недели поддерживается только для недельного повторения.');
    if (rule.calendarPolicy && !['adjust', 'skip-invalid'].includes(rule.calendarPolicy))
      errors.push('Неизвестная политика календарного повторения.');
    if (rule.durationPolicy && !['calendar', 'elapsed'].includes(rule.durationPolicy))
      errors.push('Неизвестная политика длительности повторения.');
    if (rule.until && !parseTime(rule.until, entity.plan.timezone))
      errors.push('Некорректное окончание серии.');
    if (
      rule.until &&
      entity.plan.start &&
      (parseTime(rule.until, entity.plan.timezone)?.endOf('day').toMillis() ?? Infinity) <
        (parseTime(entity.plan.start, entity.plan.timezone)?.toMillis() ?? 0)
    )
      errors.push('Окончание серии раньше её начала.');
    if (rule.exceptions.some((date) => !parseTime(date, entity.plan.timezone)))
      errors.push('Некорректная дата исключения серии.');
  }
  return [...new Set(errors)];
}

function isSafeLink(value: string, localFile = false): boolean {
  if (localFile && value.startsWith('/') && !value.startsWith('//')) return true;
  try {
    return ['http:', 'https:'].includes(new URL(value).protocol);
  } catch {
    return false;
  }
}

function cycleGroups(entities: Entity[], dependencies: Dependency[]): string[][] {
  const ids = new Set(entities.map((entity) => entity.id));
  const outgoing = new Map<string, string[]>();
  for (const dep of dependencies)
    if (dep.kind !== 'related' && ids.has(dep.fromId) && ids.has(dep.toId)) {
      outgoing.set(dep.fromId, [...(outgoing.get(dep.fromId) ?? []), dep.toId]);
    }
  let sequence = 0;
  const index = new Map<string, number>(),
    low = new Map<string, number>(),
    stack: string[] = [],
    onStack = new Set<string>(),
    groups: string[][] = [];
  const visit = (id: string) => {
    index.set(id, sequence);
    low.set(id, sequence++);
    stack.push(id);
    onStack.add(id);
    for (const next of outgoing.get(id) ?? []) {
      if (!index.has(next)) {
        visit(next);
        low.set(id, Math.min(low.get(id)!, low.get(next)!));
      } else if (onStack.has(next)) low.set(id, Math.min(low.get(id)!, index.get(next)!));
    }
    if (low.get(id) === index.get(id)) {
      const members: string[] = [];
      let next: string;
      do {
        next = stack.pop()!;
        onStack.delete(next);
        members.push(next);
      } while (next !== id);
      if (members.length > 1 || (outgoing.get(id) ?? []).includes(id)) groups.push(members.sort());
    }
  };
  for (const entity of entities) if (!index.has(entity.id)) visit(entity.id);
  return groups;
}

export function validateDependency(dependency: Dependency, snapshot: PlannerSnapshot): string[] {
  const errors: string[] = [];
  const from = snapshot.entities.find((entity) => entity.id === dependency.fromId),
    to = snapshot.entities.find((entity) => entity.id === dependency.toId);
  if (!from || !to) errors.push('Для связи нужны два существующих объекта.');
  if (
    from &&
    to &&
    (from.workspaceId !== to.workspaceId || from.workspaceId !== dependency.workspaceId)
  )
    errors.push('Связь может соединять объекты одного пространства.');
  if (dependency.fromId === dependency.toId)
    errors.push('Объект нельзя связать зависимостью с самим собой.');
  if (!['finish-start', 'start-start', 'finish-finish', 'related'].includes(dependency.kind))
    errors.push('Неизвестный тип связи.');
  if (!Number.isFinite(dependency.lagMinutes) || Math.abs(dependency.lagMinutes) > 5_256_000)
    errors.push('Некорректный временной сдвиг связи.');
  if (dependency.lagMinutes !== 0 && Math.abs(dependency.lagMinutes) < 1 / Number(NS_PER_MINUTE))
    errors.push(
      'Ненулевой временной сдвиг связи должен быть не меньше одной наносекунды. Числовые минуты округляются до ближайшей наносекунды.',
    );
  if (dependency.kind !== 'related') {
    const dependencies = [
      ...snapshot.dependencies.filter((item) => item.id !== dependency.id),
      dependency,
    ];
    if (
      cycleGroups(snapshot.entities, dependencies).some(
        (group) => group.includes(dependency.fromId) && group.includes(dependency.toId),
      )
    )
      errors.push('Эта зависимость образует цикл.');
  }
  return [...new Set(errors)];
}

function durationOf(range: TimeRange): DurationLikeObject | null {
  const start = parseTime(range.start, range.timezone),
    end = parseTime(range.end, range.timezone);
  return start && end ? end.diff(start, ['days', 'milliseconds']).toObject() : null;
}

function shiftedRange(range: TimeRange, start: DateTime): TimeRange {
  if (range.precise) {
    const original = canonicalRange(range).start;
    const next = isoToNs(start.toISO(), range.timezone);
    return original !== null && next !== null ? shiftRangeNs(range, next - original) : { ...range };
  }
  const original = parseTime(range.start, range.timezone),
    end = parseTime(range.end, range.timezone);
  if (!original) return { ...range };
  const delta = start.toMillis() - original.toMillis();
  const preserveFraction = (value: string | null | undefined, shifted: DateTime): string | null => {
    if (
      !value ||
      (!hasSubMillisecondTime({
        ...range,
        start: value,
        end: null,
        earliest: null,
        latest: null,
      }) &&
        Number.isInteger(shifted.offset))
    )
      return shifted.toISO();
    const source = isoToNs(value, range.timezone)!;
    const residual = ((source % NS_PER_MILLISECOND) + NS_PER_MILLISECOND) % NS_PER_MILLISECOND;
    return nsToISO(BigInt(shifted.toMillis()) * NS_PER_MILLISECOND + residual, range.timezone);
  };
  const shiftBound = (value: string | null | undefined) =>
    value == null
      ? value
      : (preserveFraction(value, parseTime(value, range.timezone)!.plus({ milliseconds: delta })) ??
        value);
  return {
    ...range,
    start: preserveFraction(range.start, start.setZone(range.timezone)),
    end: end
      ? preserveFraction(range.end, start.plus(durationOf(range) ?? {}).setZone(range.timezone))
      : range.end,
    ...(range.earliest !== undefined ? { earliest: shiftBound(range.earliest) } : {}),
    ...(range.latest !== undefined ? { latest: shiftBound(range.latest) } : {}),
  };
}

/** Exact new start with calendar wall-clock duration; calendar series remain calendar series. */
function shiftCalendarRangeToNs(range: TimeRange, start: bigint): TimeRange {
  const mirror = nsToISO(start, range.timezone);
  const date = parseTime(mirror, range.timezone);
  if (!date)
    throw new Error('Точный перенос календарного плана вышел за календарные годы 0001–9999.');
  const moved = shiftedRange(range, date);
  const remainder = start - canonicalRange(moved).start!;
  if (remainder === 0n) return moved;
  const coords = canonicalRange(moved);
  for (const edge of ['start', 'end', 'earliest', 'latest'] as const)
    if (coords[edge] !== null) moved[edge] = nsToISO(coords[edge]! + remainder, range.timezone);
  return moved;
}

/** Pure projection: accepted plans, baselines and recorded facts are never written here. */
export function deriveForecast(
  snapshot: PlannerSnapshot,
  changes: PlanChange[],
  workspaceId?: string,
): ScenarioPreview {
  const entities = snapshot.entities.filter(
    (entity) => !workspaceId || entity.workspaceId === workspaceId,
  );
  const byId = new Map(entities.map((entity) => [entity.id, entity]));
  const candidates = new Map(
    entities.map((entity) => [
      entity.id,
      {
        ...(entity.forecastProvenance === 'derived'
          ? entity.plan
          : getEffectiveRange(entity, 'forecast')),
      },
    ]),
  );
  const explicit = new Set<string>(),
    changed = new Set<string>(),
    conflicts: Conflict[] = [],
    explanations: string[] = [];
  for (const entity of entities)
    if (
      entity.forecastProvenance === 'derived' &&
      entity.forecast &&
      !sameRange(entity.plan, entity.forecast)
    )
      changed.add(entity.id);
  for (const change of changes) {
    const entity = byId.get(change.entityId);
    if (!entity) {
      conflicts.push({
        kind: 'invalid-time',
        entityIds: [change.entityId],
        message: 'Объект не найден в выбранном пространстве.',
      });
      continue;
    }
    const errors = rangeErrors(change.plan, 'Предложенное время');
    if (errors.length) {
      conflicts.push({ kind: 'invalid-time', entityIds: [entity.id], message: errors.join(' ') });
      continue;
    }
    if (entity.status === 'done' || (entity.actual && canonicalRange(entity.actual).end !== null)) {
      conflicts.push({
        kind: 'dependency',
        entityIds: [entity.id],
        message: `«${entity.title}»: завершённую работу нельзя переносить сценарием.`,
      });
      continue;
    }
    if (
      entity.actual &&
      canonicalRange(entity.actual).start !== null &&
      canonicalRange(change.plan).start !== canonicalRange(entity.plan).start
    ) {
      conflicts.push({
        kind: 'dependency',
        entityIds: [entity.id],
        message: `«${entity.title}»: работа уже начата; можно уточнить окончание, сохранив начало.`,
      });
      continue;
    }
    candidates.set(entity.id, { ...change.plan });
    explicit.add(entity.id);
    changed.add(entity.id);
  }
  const dependencies = snapshot.dependencies.filter(
    (dep) => dep.kind !== 'related' && byId.has(dep.fromId) && byId.has(dep.toId),
  );
  const cycles = cycleGroups(entities, dependencies);
  for (const group of cycles)
    conflicts.push({
      kind: 'cycle',
      entityIds: group,
      message: `Цикл зависимостей: ${group.map((id) => byId.get(id)!.title).join(' → ')}.`,
    });
  const indegree = new Map(entities.map((entity) => [entity.id, 0]));
  const outgoing = new Map<string, Dependency[]>();
  for (const dep of dependencies) {
    if (byId.get(dep.fromId)!.workspaceId !== byId.get(dep.toId)!.workspaceId) {
      conflicts.push({
        kind: 'dependency',
        entityIds: [dep.fromId, dep.toId],
        message: 'Зависимость пересекает границу пространства.',
      });
      continue;
    }
    indegree.set(dep.toId, (indegree.get(dep.toId) ?? 0) + 1);
    outgoing.set(dep.fromId, [...(outgoing.get(dep.fromId) ?? []), dep]);
  }
  const queue = entities
    .filter((entity) => indegree.get(entity.id) === 0)
    .map((entity) => entity.id);
  for (let cursor = 0; cursor < queue.length; cursor++) {
    const fromId = queue[cursor]!,
      source = byId.get(fromId)!,
      sourceRange = candidates.get(fromId)!;
    for (const dep of outgoing.get(fromId) ?? []) {
      const target = byId.get(dep.toId)!,
        targetRange = candidates.get(dep.toId)!;
      if (
        sourceRange.precise ||
        source.actual?.precise ||
        targetRange.precise ||
        target.actual?.precise ||
        hasSubMillisecondTime(sourceRange) ||
        hasSubMillisecondTime(source.actual) ||
        hasSubMillisecondTime(targetRange) ||
        hasSubMillisecondTime(target.actual)
      ) {
        const sourceKey = dep.kind === 'start-start' ? 'start' : 'end';
        const targetKey = dep.kind === 'finish-finish' ? 'end' : 'start';
        const sourceCoords = canonicalRange(sourceRange),
          targetCoords = canonicalRange(targetRange);
        const sourceActual = source.actual ? canonicalRange(source.actual)[sourceKey] : null;
        const targetActual = target.actual ? canonicalRange(target.actual)[targetKey] : null;
        const sourceAt =
          sourceActual ??
          sourceCoords[sourceKey] ??
          (source.kind === 'point' ? sourceCoords.start : null);
        const targetAt =
          targetActual ??
          targetCoords[targetKey] ??
          (target.kind === 'point' ? targetCoords.start : null);
        if (!INACTIVE.has(source.status) || source.status === 'done') {
          if (sourceAt === null || targetAt === null) {
            if (!INACTIVE.has(target.status))
              conflicts.push({
                kind: 'dependency',
                entityIds: [source.id, target.id],
                message: `Для расчёта «${source.title} → ${target.title}» не хватает точного времени ${sourceAt === null ? 'исходного' : 'зависимого'} объекта.`,
              });
          } else {
            const required = sourceAt + minutesToNs(dep.lagMinutes);
            if (required > targetAt) {
              if (
                target.forecast &&
                target.forecastProvenance !== 'derived' &&
                !explicit.has(target.id) &&
                targetActual === null &&
                !INACTIVE.has(target.status)
              )
                conflicts.push({
                  kind: 'dependency',
                  entityIds: [source.id, target.id],
                  message: `Ручной прогноз «${target.title}» противоречит зависимости от «${source.title}». Уточните оценку; ручное значение не перезаписано.`,
                });
              else if (targetActual !== null || INACTIVE.has(target.status))
                conflicts.push({
                  kind: 'dependency',
                  entityIds: [source.id, target.id],
                  message: `«${target.title}» уже исполнено или начато: факт противоречит зависимости от «${source.title}».`,
                });
              else if (targetCoords.start !== null) {
                try {
                  const preservesStart =
                    targetKey === 'end' &&
                    target.actual &&
                    canonicalRange(target.actual).start !== null;
                  const next = preservesStart
                    ? targetRange.precise
                      ? withRangeEdgeNs(targetRange, 'end', required)
                      : { ...targetRange, end: nsToISO(required, targetRange.timezone) }
                    : targetRange.precise
                      ? shiftRangeNs(targetRange, required - targetAt)
                      : shiftCalendarRangeToNs(
                          targetRange,
                          targetCoords.start + required - targetAt,
                        );
                  if (preservesStart && next.end === null && !targetRange.precise)
                    throw new Error(
                      'Точное окончание календарного плана вышло за календарные годы 0001–9999.',
                    );
                  candidates.set(target.id, next);
                  changed.add(target.id);
                  explanations.push(
                    `«${target.title}» сдвигается вслед за «${source.title}» (${dep.kind}, ${dep.lagMinutes} мин).`,
                  );
                } catch (error) {
                  conflicts.push({
                    kind: 'invalid-time',
                    entityIds: [source.id, target.id],
                    message:
                      error instanceof Error
                        ? error.message
                        : 'Не удалось точно перенести календарный план.',
                  });
                }
              }
            }
          }
        }
        indegree.set(dep.toId, indegree.get(dep.toId)! - 1);
        if (indegree.get(dep.toId) === 0) queue.push(dep.toId);
        continue;
      }
      const sourceKey = dep.kind === 'start-start' ? 'start' : 'end';
      const actualSource = source.actual?.[sourceKey];
      const sourceDate = parseTime(
        actualSource ??
          sourceRange[sourceKey] ??
          (source.kind === 'point' ? sourceRange.start : null),
        sourceRange.timezone,
      );
      const targetKey = dep.kind === 'finish-finish' ? 'end' : 'start';
      const targetDate = parseTime(
        target.actual?.[targetKey] ??
          targetRange[targetKey] ??
          (target.kind === 'point' ? targetRange.start : null),
        targetRange.timezone,
      );
      if (!INACTIVE.has(source.status) || source.status === 'done') {
        if (!sourceDate || !targetDate) {
          if (!INACTIVE.has(target.status))
            conflicts.push({
              kind: 'dependency',
              entityIds: [source.id, target.id],
              message: `Для расчёта «${source.title} → ${target.title}» не хватает ${!sourceDate ? 'времени исходного объекта' : 'времени зависимого объекта'}.`,
            });
        } else {
          const required = sourceDate
            .plus({ minutes: dep.lagMinutes })
            .setZone(targetRange.timezone);
          if (required.toMillis() > targetDate.toMillis()) {
            if (
              target.forecast &&
              target.forecastProvenance !== 'derived' &&
              !explicit.has(target.id) &&
              !target.actual?.[targetKey] &&
              !INACTIVE.has(target.status)
            ) {
              conflicts.push({
                kind: 'dependency',
                entityIds: [source.id, target.id],
                message: `Ручной прогноз «${target.title}» противоречит зависимости от «${source.title}». Уточните оценку; ручное значение не перезаписано.`,
              });
            } else if (
              target.actual?.[targetKey] ||
              INACTIVE.has(target.status) ||
              (target.actual?.start && targetKey === 'start')
            ) {
              conflicts.push({
                kind: 'dependency',
                entityIds: [source.id, target.id],
                message: `«${target.title}» уже исполнено или начато: факт противоречит зависимости от «${source.title}».`,
              });
            } else {
              const start = parseTime(targetRange.start, targetRange.timezone);
              if (start) {
                let next: TimeRange;
                if (targetKey === 'end' && target.actual?.start)
                  next = { ...targetRange, end: required.toISO() };
                else if (targetKey === 'end') {
                  const originalEnd = parseTime(targetRange.end, targetRange.timezone) ?? start;
                  next = shiftedRange(
                    targetRange,
                    start.plus({ milliseconds: required.toMillis() - originalEnd.toMillis() }),
                  );
                } else next = shiftedRange(targetRange, required);
                candidates.set(target.id, next);
                changed.add(target.id);
                explanations.push(
                  `«${target.title}» сдвигается вслед за «${source.title}» (${dep.kind}, ${dep.lagMinutes} мин).`,
                );
              }
            }
          }
        }
      }
      indegree.set(dep.toId, indegree.get(dep.toId)! - 1);
      if (indegree.get(dep.toId) === 0) queue.push(dep.toId);
    }
  }
  const outputIds = new Set(changed);
  if (!changes.length)
    for (const entity of entities)
      if (
        entity.forecastProvenance === 'derived' &&
        !sameRange(candidates.get(entity.id)!, entity.plan)
      )
        outputIds.add(entity.id);
  const resultChanges: PlanChange[] = [...outputIds].sort().flatMap((id) => {
    const plan = candidates.get(id)!;
    return explicit.has(id) ||
      (!changes.length &&
        byId.get(id)!.forecastProvenance === 'derived' &&
        !sameRange(plan, byId.get(id)!.plan)) ||
      !sameRange(plan, getEffectiveRange(byId.get(id)!, 'forecast'))
      ? [{ entityId: id, plan }]
      : [];
  });
  for (const entity of entities) {
    const range = candidates.get(entity.id)!;
    if (range.precise || hasSubMillisecondTime(range)) {
      const coordinate = canonicalRange(range),
        end = coordinate.end ?? coordinate.start;
      const due = isoToNs(entity.dueAt, range.timezone);
      if (!INACTIVE.has(entity.status) && end !== null && due !== null && end > due)
        conflicts.push({
          kind: 'deadline',
          entityIds: [entity.id],
          message: `«${entity.title}» выходит за срок обязательства на ${(end - due).toString()} нс.`,
        });
      continue;
    }
    const end = parseTime(range.end ?? range.start, range.timezone),
      due = parseTime(entity.dueAt, range.timezone);
    if (!INACTIVE.has(entity.status) && end && due && end.toMillis() > due.toMillis())
      conflicts.push({
        kind: 'deadline',
        entityIds: [entity.id],
        message: `«${entity.title}» выходит за срок обязательства на ${Math.ceil(end.diff(due, 'minutes').minutes)} мин.`,
      });
  }
  conflicts.push(
    ...computeResourceConflicts(
      workspaceId
        ? {
            ...snapshot,
            entities,
            resources: snapshot.resources.filter(
              (resource) => resource.workspaceId === workspaceId,
            ),
          }
        : snapshot,
      resultChanges,
    ),
  );
  return {
    changes: resultChanges,
    conflicts: uniqueConflicts(conflicts),
    affectedIds: resultChanges.map((change) => change.entityId).sort(),
    explanations: [...new Set(explanations)],
  };
}

function uniqueConflicts(conflicts: Conflict[]): Conflict[] {
  const seen = new Set<string>();
  return conflicts.filter((conflict) => {
    const key = `${conflict.kind}|${conflict.resourceId ?? ''}|${[...conflict.entityIds].sort().join(',')}|${conflict.message}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

/** Half-open intervals: adjacent reservations do not conflict. Capacity sums every allocation. */
export function computeResourceConflicts(
  snapshot: PlannerSnapshot,
  changes: PlanChange[] = [],
  window?: { from: string; to: string },
): Conflict[] {
  const plans = new Map(changes.map((change) => [change.entityId, change.plan]));
  const conflicts: Conflict[] = [];
  const base = parseTime(snapshot.serverTime) ?? DateTime.now();
  const horizonStart = window ? parseTime(window.from) : base.minus({ days: 30 });
  const horizonEnd = window ? parseTime(window.to) : base.plus({ days: 366 });
  const horizonStartNs = isoToNs(window?.from ?? horizonStart?.toISO());
  const horizonEndNs = isoToNs(window?.to ?? horizonEnd?.toISO());
  if (
    !horizonStart ||
    !horizonEnd ||
    horizonStartNs === null ||
    horizonEndNs === null ||
    horizonEndNs <= horizonStartNs
  )
    return [
      { kind: 'invalid-time', entityIds: [], message: 'Некорректное окно проверки ресурсов.' },
    ];
  for (const resource of snapshot.resources) {
    const bookings: { entityId: string; start: bigint; end: bigint; amount: number }[] = [];
    const windows: { from: DateTime; to: DateTime }[] = [{ from: horizonStart, to: horizonEnd }];
    // Always inspect every known reservation, even when it lies years beyond the rolling window.
    // Future series anchors also get a window; an explicit window is available for viewport QA.
    if (!window)
      for (const entity of snapshot.entities) {
        if (
          entity.workspaceId !== resource.workspaceId ||
          INACTIVE.has(entity.status) ||
          !entity.allocations.some((allocation) => allocation.resourceId === resource.id)
        )
          continue;
        const range = plans.get(entity.id) ?? getEffectiveRange(entity, 'forecast');
        const start = parseTime(entity.actual?.start ?? range.start, range.timezone),
          end = parseTime(range.end ?? range.start, range.timezone);
        if (!start || !end) continue;
        if (!entity.recurrence)
          windows.push({
            from: start.minus({ milliseconds: 1 }),
            to: end.plus({ milliseconds: 1 }),
          });
        else if (start.toMillis() > horizonEnd.toMillis())
          windows.push({ from: start, to: start.plus({ days: 366 }) });
      }
    windows.sort((a, b) => a.from.toMillis() - b.from.toMillis());
    const mergedWindows: { from: DateTime; to: DateTime }[] = [];
    for (const candidate of windows) {
      const previous = mergedWindows.at(-1);
      if (previous && candidate.from.toMillis() <= previous.to.toMillis()) {
        if (candidate.to.toMillis() > previous.to.toMillis()) previous.to = candidate.to;
      } else mergedWindows.push({ ...candidate });
    }
    for (const entity of snapshot.entities) {
      if (entity.workspaceId !== resource.workspaceId || INACTIVE.has(entity.status)) continue;
      const amount = entity.allocations
        .filter((item) => item.resourceId === resource.id)
        .reduce((sum, item) => sum + item.amount, 0);
      if (!Number.isFinite(amount) || amount <= 0) continue;
      const range = plans.get(entity.id) ?? getEffectiveRange(entity, 'forecast');
      if (!entity.recurrence) {
        const coordinates = canonicalRange(range);
        const recordedStart = entity.actual ? canonicalRange(entity.actual).start : null;
        let first = recordedStart ?? coordinates.start;
        let last = coordinates.end ?? (entity.kind === 'point' ? coordinates.start : null);
        if (first === null || last === null || last < first) continue;
        if (window) {
          const from = isoToNs(window.from)!,
            to = isoToNs(window.to)!;
          if (first >= to || (last > first ? last <= from : first < from)) continue;
          if (first < from) first = from;
          if (last > to) last = to;
        }
        bookings.push({ entityId: entity.id, start: first, end: last, amount });
        const working = resource.workingWeekdays;
        if (working.length && new Set(working).size < 7) {
          const firstISO = nsToISO(first, resource.timezone),
            lastISO = nsToISO(last, resource.timezone);
          if (!firstISO || !lastISO) {
            conflicts.push({
              kind: 'invalid-time',
              resourceId: resource.id,
              entityIds: [entity.id],
              message: `Календарь «${resource.name}» нельзя проверить вне календарных лет 0001–9999 для «${entity.title}». Загрузка ресурса рассчитана по точным координатам.`,
            });
          } else {
            const start = parseTime(firstISO, resource.timezone)!,
              end = parseTime(lastISO, resource.timezone)!;
            let day = start.startOf('day'),
              checked = 0;
            while (
              ((isoToNs(day.toISO()) ?? last) < last || (first === last && checked === 0)) &&
              checked++ < 400
            ) {
              if (!working.includes(day.weekday)) {
                conflicts.push({
                  kind: 'resource',
                  resourceId: resource.id,
                  entityIds: [entity.id],
                  message: `«${resource.name}» недоступен по календарю ${day.toISODate()} для «${entity.title}».`,
                });
                break;
              }
              day = day.plus({ days: 1 });
            }
            if (checked > 400 && day.toMillis() < end.toMillis())
              conflicts.push({
                kind: 'invalid-time',
                resourceId: resource.id,
                entityIds: [entity.id],
                message: `Календарь «${resource.name}» проверен только на первых 400 днях «${entity.title}». Уточните период проверки ресурса.`,
              });
          }
        }
        continue;
      }
      const start = parseTime(entity.actual?.start ?? range.start, range.timezone);
      const end = parseTime(
        range.end ?? (entity.kind === 'point' ? range.start : null),
        range.timezone,
      );
      if (!start || !end || end.toMillis() < start.toMillis()) continue;
      const recordedStart = entity.actual?.precise ? canonicalRange(entity.actual).start : null;
      const projected = {
        ...entity,
        plan: {
          ...range,
          start:
            recordedStart !== null
              ? nsToISO(recordedStart, range.timezone)
              : (entity.actual?.start ?? range.start),
        },
      };
      const occurrenceMap = new Map<string, Occurrence>();
      if (entity.recurrence) {
        let steps = 0;
        for (const resourceWindow of mergedWindows) {
          let fromNs = window ? horizonStartNs : isoToNs(resourceWindow.from.toISO())!;
          const toNs = window ? horizonEndNs : isoToNs(resourceWindow.to.toISO())!;
          while (fromNs < toNs) {
            if (steps++ >= 400 || occurrenceMap.size >= 10_000) {
              conflicts.push({
                kind: 'invalid-time',
                resourceId: resource.id,
                entityIds: [entity.id],
                message: `Проверка «${entity.title}» ограничена 10000 повторениями / 400 окнами. Уточните период проверки ресурса.`,
              });
              break;
            }
            const from = parseTime(nsToISO(fromNs), resourceWindow.from.zoneName!)!;
            const calendarChunk = from.plus({ days: 366 });
            const residual =
              ((fromNs % NS_PER_MILLISECOND) + NS_PER_MILLISECOND) % NS_PER_MILLISECOND;
            const calendarChunkNs = calendarChunk.isValid
              ? BigInt(calendarChunk.toMillis()) * NS_PER_MILLISECOND + residual
              : toNs;
            const chunkEndNs = calendarChunkNs < toNs ? calendarChunkNs : toNs;
            const expanded = expandRecurrence(projected, nsToISO(fromNs)!, nsToISO(chunkEndNs)!);
            if (expanded.length >= MAX_RECURRENCE_OCCURRENCES)
              conflicts.push({
                kind: 'invalid-time',
                resourceId: resource.id,
                entityIds: [entity.id],
                message: `Окно «${entity.title}» достигло лимита ${MAX_RECURRENCE_OCCURRENCES} повторений. Сузьте период проверки ресурса; отсутствие других конфликтов не подтверждено.`,
              });
            for (const occurrence of expanded) occurrenceMap.set(occurrence.id, occurrence);
            fromNs = chunkEndNs;
          }
        }
      } else if (
        !window ||
        (start.toMillis() < horizonEnd.toMillis() &&
          (end.toMillis() > start.toMillis()
            ? end.toMillis() > horizonStart.toMillis()
            : start.toMillis() >= horizonStart.toMillis()))
      ) {
        occurrenceMap.set(entity.id, {
          id: entity.id,
          entityId: entity.id,
          start: start.toISO()!,
          end: end.toISO()!,
          index: 0,
        });
      }
      const occurrences = [...occurrenceMap.values()];
      for (const occurrence of occurrences) {
        let first = parseTime(occurrence.start, resource.timezone)!,
          last = parseTime(
            occurrence.end ?? (entity.kind === 'point' ? occurrence.start : null),
            resource.timezone,
          );
        if (!last || last.toMillis() < first.toMillis()) continue;
        if (window) {
          if (first.toMillis() < horizonStart.toMillis())
            first = horizonStart.setZone(resource.timezone);
          if (last.toMillis() > horizonEnd.toMillis()) last = horizonEnd.setZone(resource.timezone);
        }
        let firstNs = isoToNs(occurrence.start, resource.timezone)!,
          lastNs = isoToNs(
            occurrence.end ?? (entity.kind === 'point' ? occurrence.start : null),
            resource.timezone,
          )!;
        if (window) {
          if (firstNs < horizonStartNs) firstNs = horizonStartNs;
          if (lastNs > horizonEndNs) lastNs = horizonEndNs;
        }
        bookings.push({
          entityId: entity.id,
          start: firstNs,
          end: lastNs,
          amount,
        });
        const working = resource.workingWeekdays;
        if (working.length && new Set(working).size < 7) {
          let day = first.startOf('day'),
            checked = 0;
          while (
            (day.toMillis() < last.toMillis() ||
              (first.toMillis() === last.toMillis() && checked === 0)) &&
            checked++ < 400
          ) {
            if (!working.includes(day.weekday)) {
              conflicts.push({
                kind: 'resource',
                resourceId: resource.id,
                entityIds: [entity.id],
                message: `«${resource.name}» недоступен по календарю ${day.toISODate()} для «${entity.title}».`,
              });
              break;
            }
            day = day.plus({ days: 1 });
          }
          if (checked > 400 && day.toMillis() < last.toMillis())
            conflicts.push({
              kind: 'invalid-time',
              resourceId: resource.id,
              entityIds: [entity.id],
              message: `Календарь «${resource.name}» проверен только на первых 400 днях «${entity.title}». Уточните период проверки ресурса.`,
            });
        }
      }
    }
    const events = bookings
      .flatMap((booking, index) =>
        booking.end === booking.start
          ? [{ at: booking.start, change: 0, index }]
          : [
              { at: booking.start, change: 1, index },
              { at: booking.end, change: -1, index },
            ],
      )
      .sort((a, b) => (a.at < b.at ? -1 : a.at > b.at ? 1 : a.change - b.change));
    const active = new Set<number>();
    for (let cursor = 0; cursor < events.length;) {
      const at = events[cursor]!.at;
      const instant: number[] = [];
      while (cursor < events.length && events[cursor]!.at === at) {
        const event = events[cursor++]!;
        if (event.change < 0) active.delete(event.index);
        else if (event.change > 0) active.add(event.index);
        else instant.push(event.index);
      }
      const next = events[cursor]?.at;
      const amount = [...active].reduce((sum, index) => sum + bookings[index]!.amount, 0);
      const instantAmount =
        amount + instant.reduce((sum, index) => sum + bookings[index]!.amount, 0);
      if (instant.length && instantAmount > resource.capacity + 1e-9) {
        const entityIds = [
          ...new Set([...active, ...instant].map((index) => bookings[index]!.entityId)),
        ].sort();
        conflicts.push({
          kind: 'resource',
          resourceId: resource.id,
          entityIds,
          message: `«${resource.name}»: одновременно требуется ${Number(instantAmount.toFixed(3))} ${resource.unit} при ёмкости ${resource.capacity} в ${formatNs(at, resource.timezone)}.`,
        });
      }
      if (next != null && next > at && amount > resource.capacity + 1e-9) {
        const entityIds = [
          ...new Set([...active].map((index) => bookings[index]!.entityId)),
        ].sort();
        conflicts.push({
          kind: 'resource',
          resourceId: resource.id,
          entityIds,
          message: `«${resource.name}»: занято ${Number(amount.toFixed(3))} ${resource.unit} при ёмкости ${resource.capacity}; ${formatNs(at, resource.timezone)} — ${formatNs(next, resource.timezone)}.`,
        });
      }
    }
  }
  return uniqueConflicts(conflicts);
}

/** Exact half-open viewport query; relative positions never pass through Number or Luxon. */
export function expandPreciseOccurrences(entity: Entity, from: bigint, to: bigint): Occurrence[] {
  if (to <= from || entity.recurrence) return [];
  const range = entity.plan,
    coordinates = canonicalRange(range);
  const start = coordinates.start;
  const end = coordinates.end;
  if (start === null || start >= to || (end !== null && end > start ? end <= from : start < from))
    return [];
  return [
    {
      id: entity.id,
      entityId: entity.id,
      start: range.start,
      end: range.end,
      index: 0,
      ...(range.precise ? { precise: { ...range.precise } } : {}),
    },
  ];
}

export function expandRecurrence(entity: Entity, from: string, to: string): Occurrence[] {
  if (entity.plan.precise) {
    const first = isoToNs(from, entity.plan.timezone) ?? parseNs(from);
    const last = isoToNs(to, entity.plan.timezone) ?? parseNs(to);
    return first !== null && last !== null ? expandPreciseOccurrences(entity, first, last) : [];
  }
  const range = entity.plan,
    base = parseTime(range.start, range.timezone),
    windowStart = parseTime(from, range.timezone),
    windowEnd = parseTime(to, range.timezone);
  const windowStartNs = isoToNs(from, range.timezone),
    windowEndNs = isoToNs(to, range.timezone);
  const baseNs = isoToNs(range.start, range.timezone);
  if (
    !base ||
    !windowStart ||
    !windowEnd ||
    baseNs === null ||
    windowStartNs === null ||
    windowEndNs === null ||
    windowEndNs <= windowStartNs
  )
    return [];
  const baseEnd = parseTime(range.end, range.timezone);
  const baseEndNs = isoToNs(range.end, range.timezone);
  if (range.end !== null && baseEndNs === null) return [];
  const startSubMs = baseNs - BigInt(base.toMillis()) * NS_PER_MILLISECOND;
  const endSubMs =
    baseEnd && baseEndNs !== null
      ? baseEndNs - BigInt(baseEnd.toMillis()) * NS_PER_MILLISECOND
      : 0n;
  const coordinateOf = (date: DateTime, subMs = startSubMs) =>
    BigInt(date.toMillis()) * NS_PER_MILLISECOND + subMs;
  const duration =
    entity.recurrence?.durationPolicy === 'elapsed' && baseEnd
      ? { milliseconds: baseEnd.toMillis() - base.toMillis() }
      : durationOf(range);
  const results: Occurrence[] = [];
  const add = (start: DateTime, index: number) => {
    const end = baseEnd ? start.plus(duration ?? {}) : null;
    if (!start.isValid || (end && !end.isValid)) return;
    const startNs = coordinateOf(start),
      endNs = end ? coordinateOf(end, endSubMs) : null;
    if (
      startNs < windowEndNs &&
      (endNs !== null && endNs > startNs ? endNs > windowStartNs : startNs >= windowStartNs)
    ) {
      results.push({
        id: `${entity.id}@${hasSubMillisecondTime({ ...range, end: null, earliest: null, latest: null }) ? nsToISO(startNs) : start.toUTC().toISO()}`,
        entityId: entity.id,
        start:
          hasSubMillisecondTime({ ...range, end: null, earliest: null, latest: null }) ||
          !Number.isInteger(start.offset)
            ? nsToISO(startNs, range.timezone)
            : start.toISO()!,
        end:
          endNs === null
            ? null
            : hasSubMillisecondTime({
                  ...range,
                  start: range.end,
                  end: null,
                  earliest: null,
                  latest: null,
                }) || !Number.isInteger(end!.offset)
              ? nsToISO(endNs, range.timezone)
              : end!.toISO(),
        index,
      });
    }
  };
  const rule = entity.recurrence;
  if (!rule) {
    add(base, 0);
    return results;
  }
  if (!Number.isInteger(rule.interval) || rule.interval < 1 || rule.interval > 365) return [];
  if (!['day', 'week', 'month'].includes(rule.frequency)) return [];
  const untilParsed = rule.until ? parseTime(rule.until, range.timezone) : null;
  if (rule.until && !untilParsed) return [];
  const untilNs = !rule.until
    ? null
    : rule.until.length === 10
      ? BigInt(untilParsed!.endOf('day').toMillis()) * NS_PER_MILLISECOND + NS_PER_MILLISECOND - 1n
      : isoToNs(rule.until, range.timezone);
  if (rule.until && untilNs === null) return [];
  const count = rule.count ?? 100_000;
  const skipInvalid = rule.calendarPolicy === 'skip-invalid';
  const exceptionDates = new Set(rule.exceptions.filter((value) => value.length === 10));
  const exceptionInstants = new Set(
    rule.exceptions
      .filter((value) => value.length !== 10)
      .map((value) => isoToNs(value, range.timezone)),
  );
  const candidateStart = windowStart.minus(duration ?? {});
  const allowed = (date: DateTime, index: number) => {
    const atNs = coordinateOf(date);
    if (index >= count || (untilNs !== null && atNs > untilNs)) return false;
    if (!exceptionDates.has(date.toISODate()!) && !exceptionInstants.has(atNs)) add(date, index);
    return true;
  };
  if (rule.frequency === 'week') {
    const weekdays = [...new Set(rule.weekdays?.length ? rule.weekdays : [base.weekday])]
      .filter((day) => Number.isInteger(day) && day >= 1 && day <= 7)
      .sort((a, b) => a - b);
    if (!weekdays.length) return [];
    const weekBase = base.startOf('week');
    let group = skipInvalid
      ? 0
      : Math.max(0, Math.floor(candidateStart.diff(weekBase, 'weeks').weeks / rule.interval) - 1);
    const firstCount = weekdays.filter((day) => day >= base.weekday).length;
    let index = group === 0 ? 0 : firstCount + (group - 1) * weekdays.length;
    let steps = 0;
    while (
      steps++ < MAX_RECURRENCE_STEPS &&
      results.length < MAX_RECURRENCE_OCCURRENCES &&
      index < count
    ) {
      const week = weekBase.plus({ weeks: group * rule.interval });
      if (!week.isValid || BigInt(week.toMillis()) * NS_PER_MILLISECOND >= windowEndNs) break;
      for (const weekday of weekdays) {
        let day = week.plus({ days: weekday - 1 }).set({
          hour: base.hour,
          minute: base.minute,
          second: base.second,
          millisecond: base.millisecond,
        });
        if (day.toMillis() < base.toMillis()) continue;
        if (
          skipInvalid &&
          (day.hour !== base.hour || day.minute !== base.minute || day.weekday !== weekday)
        )
          continue;
        if (skipInvalid)
          day = day.getPossibleOffsets().sort((a, b) => a.toMillis() - b.toMillis())[0] ?? day;
        if (!allowed(day, index++)) return results;
        if (results.length >= MAX_RECURRENCE_OCCURRENCES) return results;
      }
      group++;
    }
  } else {
    const unit = rule.frequency === 'day' ? 'days' : 'months';
    let step = skipInvalid
      ? 0
      : Math.max(0, Math.floor(candidateStart.diff(base, unit).get(unit) / rule.interval) - 1);
    let index = step;
    const calendarBase = DateTime.fromObject(
      { year: base.year, month: base.month, day: base.day },
      { zone: 'UTC' },
    );
    for (
      let steps = 0;
      steps < MAX_RECURRENCE_STEPS && results.length < MAX_RECURRENCE_OCCURRENCES && index < count;
      steps++, step++
    ) {
      let date = base.plus({ [unit]: step * rule.interval });
      if (skipInvalid) {
        const expected = calendarBase.plus({ [unit]: step * rule.interval });
        const invalid =
          date.hour !== base.hour ||
          date.minute !== base.minute ||
          (rule.frequency === 'month'
            ? date.day !== base.day
            : date.toISODate() !== expected.toISODate());
        if (invalid) continue;
        date = date.getPossibleOffsets().sort((a, b) => a.toMillis() - b.toMillis())[0] ?? date;
      }
      if (!date.isValid || coordinateOf(date) >= windowEndNs) break;
      if (!allowed(date, index++)) break;
    }
  }
  return results;
}

function stableId(value: string): string {
  let hash = 2166136261;
  for (let i = 0; i < value.length; i++) hash = Math.imul(hash ^ value.charCodeAt(i), 16777619);
  return (hash >>> 0).toString(36);
}

export function evaluateSignals(snapshot: PlannerSnapshot, now = snapshot.serverTime): Signal[] {
  const clock = parseTime(now) ?? DateTime.now();
  const clockNs = isoToNs(now) ?? BigInt(Math.trunc(clock.toMillis())) * 1_000_000n;
  const at = clock.toUTC().toISO()!;
  const signals = new Map<string, Signal>();
  const push = (
    entity: Entity,
    kind: Signal['kind'],
    severity: Signal['severity'],
    title: string,
    description: string,
    suffix = '',
    affectedIds = [entity.id],
    dueAt = entity.dueAt,
    ownerId = entity.ownerId,
  ) => {
    const key = `${kind}|${entity.workspaceId}|${entity.id}|${suffix}`;
    const previous = snapshot.signals.find((signal) => signal.dedupeKey === key);
    signals.set(key, {
      id: previous?.id ?? `sig-${stableId(key)}`,
      workspaceId: entity.workspaceId,
      entityId: entity.id,
      kind,
      severity,
      title,
      description,
      affectedIds: [...new Set(affectedIds)].sort(),
      responsibleUserId: ownerId,
      dueAt,
      state:
        previous && ['acknowledged', 'accepted-risk'].includes(previous.state)
          ? previous.state
          : 'open',
      createdAt: previous?.createdAt ?? at,
      updatedAt: at,
      dedupeKey: key,
      ...(previous?.acknowledgedBy ? { acknowledgedBy: previous.acknowledgedBy } : {}),
    });
  };
  for (const entity of snapshot.entities) {
    if (INACTIVE.has(entity.status)) continue;
    const range = getEffectiveRange(entity, 'forecast');
    const due = parseTime(entity.dueAt, entity.plan.timezone),
      predicted = parseTime(range.end ?? range.start, range.timezone);
    const dueNs = isoToNs(entity.dueAt, entity.plan.timezone);
    const completed = entity.actual && canonicalRange(entity.actual).end !== null;
    if (!completed && due && dueNs !== null && dueNs < clockNs)
      push(
        entity,
        'overdue',
        'critical',
        'Срок прошёл',
        `«${entity.title}» не завершено к ${due.setLocale('ru').toFormat('d LLL, HH:mm')}.`,
        entity.dueAt!,
      );
    const limit = due ?? parseTime(entity.plan.end ?? entity.plan.start, entity.plan.timezone);
    if (
      entity.forecast &&
      (range.precise ||
        entity.plan.precise ||
        hasSubMillisecondTime(range) ||
        hasSubMillisecondTime(entity.plan))
    ) {
      const forecastCoords = canonicalRange(range),
        planCoords = canonicalRange(entity.plan);
      const predictionNs = forecastCoords.end ?? forecastCoords.start;
      const limitNs =
        isoToNs(entity.dueAt, entity.plan.timezone) ?? planCoords.end ?? planCoords.start;
      if (predictionNs !== null && limitNs !== null && predictionNs > limitNs)
        push(
          entity,
          'forecast-risk',
          'warning',
          'Прогноз выходит за срок',
          `«${entity.title}»: ожидаемое окончание позже ${entity.dueAt ? 'обязательства' : 'принятого плана'} на ${(predictionNs - limitNs).toString()} нс. Прогноз не изменяет план.`,
          `${limitNs}|${predictionNs}`,
        );
    } else if (entity.forecast && predicted && limit && predicted.toMillis() > limit.toMillis())
      push(
        entity,
        'forecast-risk',
        'warning',
        'Прогноз выходит за срок',
        `«${entity.title}»: ожидаемое окончание позже ${due ? 'обязательства' : 'принятого плана'} на ${Math.ceil(predicted.diff(limit, 'minutes').minutes)} мин. Прогноз не изменяет план.`,
        `${limit.toUTC().toISO()}|${predicted.toUTC().toISO()}`,
      );
    if (!hasTime(entity.plan) && !hasTime(entity.actual))
      push(
        entity,
        'missing-date',
        entity.status === 'draft' || entity.kind === 'note' ? 'info' : 'warning',
        'Время пока не задано',
        `«${entity.title}» находится среди объектов без даты. Укажите время или оставьте объект в этом контексте.`,
      );
    const freshness = entity.source.staleAfterMinutes;
    const received = parseTime(entity.source.receivedAt);
    if (freshness && received && clock.diff(received, 'minutes').minutes > freshness)
      push(
        entity,
        'stale-source',
        'warning',
        'Данные источника требуют проверки',
        `«${entity.source.label}»: последнее получение ${received.setZone(entity.plan.timezone).setLocale('ru').toFormat('d LLL, HH:mm')}. Автоматического подтверждения свежести нет.`,
        entity.source.externalId ?? entity.source.label,
      );
    for (const rule of snapshot.rules) {
      if (
        !rule.enabled ||
        rule.workspaceId !== entity.workspaceId ||
        (rule.typeId && rule.typeId !== entity.typeId) ||
        !['before-start', 'before-due'].includes(rule.trigger)
      )
        continue;
      const lead = Math.max(0, rule.leadMinutes);
      if (
        rule.trigger === 'before-start' &&
        (entity.plan.precise || hasSubMillisecondTime(entity.plan))
      ) {
        const targetNs = canonicalRange(entity.plan).start;
        if (targetNs !== null && clockNs >= targetNs - minutesToNs(lead) && clockNs <= targetNs)
          push(
            entity,
            'reminder',
            'info',
            rule.name,
            `«${entity.title}»: начало ${formatNs(targetNs, entity.plan.timezone)}.`,
            `${rule.id}|${targetNs}`,
            [entity.id],
            nsToISO(targetNs, entity.plan.timezone),
            rule.ownerId ?? entity.ownerId,
          );
        continue;
      }
      let targets: string[] = [];
      if (rule.trigger === 'before-start' && entity.recurrence)
        targets = expandRecurrence(
          entity,
          clock.toISO()!,
          clock.plus({ minutes: lead, milliseconds: 1 }).toISO()!,
        )
          .map((item) => item.start)
          .filter((value): value is string => value !== null);
      else
        targets = [rule.trigger === 'before-due' ? entity.dueAt : entity.plan.start].filter(
          (value): value is string => !!value,
        );
      for (const value of targets) {
        const target = parseTime(value, entity.plan.timezone);
        if (
          target &&
          clock.toMillis() >= target.minus({ minutes: lead }).toMillis() &&
          clock.toMillis() <= target.toMillis()
        )
          push(
            entity,
            'reminder',
            'info',
            rule.name,
            `«${entity.title}»: ${rule.trigger === 'before-due' ? 'срок' : 'начало'} ${target.setLocale('ru').toFormat('d LLL, HH:mm')}.`,
            `${rule.id}|${target.toUTC().toISO()}`,
            [entity.id],
            target.toISO(),
            rule.ownerId ?? entity.ownerId,
          );
      }
    }
  }
  for (const conflict of computeResourceConflicts(snapshot)) {
    const entity = snapshot.entities.find((item) => item.id === conflict.entityIds[0]);
    if (entity)
      push(
        entity,
        'resource-conflict',
        'warning',
        'Конфликт ресурса',
        conflict.message,
        `${conflict.resourceId}|${conflict.entityIds.join(',')}|${conflict.message}`,
        conflict.entityIds,
        null,
      );
  }
  const preview = deriveForecast(snapshot, []);
  for (const conflict of preview.conflicts.filter((item) =>
    ['cycle', 'dependency'].includes(item.kind),
  )) {
    const entity = snapshot.entities.find((item) => item.id === conflict.entityIds[0]);
    if (entity && !INACTIVE.has(entity.status))
      push(
        entity,
        'dependency',
        'warning',
        conflict.kind === 'cycle' ? 'Цикл зависимостей' : 'Зависимость требует внимания',
        conflict.message,
        `${conflict.kind}|${conflict.entityIds.join(',')}`,
        conflict.entityIds,
        null,
      );
  }
  const priority = { critical: 0, warning: 1, info: 2 };
  return [...signals.values()].sort(
    (a, b) =>
      priority[a.severity] - priority[b.severity] ||
      (a.dueAt ?? 'z').localeCompare(b.dueAt ?? 'z') ||
      a.id.localeCompare(b.id),
  );
}

function newId(prefix: string): string {
  return `${prefix}-${globalThis.crypto.randomUUID()}`;
}

export function applyTemplate(
  template: Template,
  options: {
    workspaceId: string;
    anchorDate?: string;
    anchorNs?: string;
    title?: string;
    ownerId?: string;
    timezone?: string;
  },
): { entities: Entity[]; dependencies: Dependency[] } {
  const timezone = options.timezone ?? 'UTC';
  if (!DateTime.now().setZone(timezone).isValid)
    throw new Error('У шаблона некорректный часовой пояс.');
  const calendarAnchorNs = isoToNs(options.anchorDate, timezone);
  const explicitAnchorNs = options.anchorNs === undefined ? null : parseNs(options.anchorNs);
  if (options.anchorNs !== undefined && explicitAnchorNs === null)
    throw new Error('У шаблона некорректная точная опорная координата.');
  if (options.anchorDate !== undefined && calendarAnchorNs === null)
    throw new Error('У шаблона некорректная опорная дата.');
  if (
    explicitAnchorNs !== null &&
    calendarAnchorNs !== null &&
    explicitAnchorNs !== calendarAnchorNs
  )
    throw new Error('Опорная дата шаблона не совпадает с точной координатой.');
  const anchorNs = explicitAnchorNs ?? calendarAnchorNs;
  if (anchorNs === null) throw new Error('У шаблона не задана опорная дата или точная координата.');
  const anchor = parseTime(options.anchorDate ?? nsToISO(anchorNs, timezone), timezone);
  let originalAnchor = template.anchorDate
    ? parseTime(template.anchorDate, template.timezone ?? timezone)
    : null;
  if (template.anchorDate && !originalAnchor)
    throw new Error('У шаблона некорректная исходная дата.');
  const originalCalendarNs = isoToNs(template.anchorDate, template.timezone ?? timezone);
  if (template.anchorDate && originalCalendarNs === null)
    throw new Error('Исходная календарная дата шаблона должна находиться в годах 0001–9999.');
  const originalExactNs = template.anchorNs === undefined ? null : parseNs(template.anchorNs);
  if (template.anchorNs !== undefined && originalExactNs === null)
    throw new Error('У шаблона некорректная исходная точная координата.');
  if (
    originalExactNs !== null &&
    originalCalendarNs !== null &&
    originalExactNs !== originalCalendarNs
  )
    throw new Error('Исходная дата шаблона не совпадает с точной координатой.');
  const originalAnchorNs = originalExactNs ?? originalCalendarNs;
  if (!originalAnchor && originalAnchorNs !== null)
    originalAnchor = parseTime(
      nsToISO(originalAnchorNs, template.timezone ?? timezone),
      template.timezone ?? timezone,
    );
  const wallMillis = (date: DateTime) =>
    ((date.hour * 60 + date.minute) * 60 + date.second) * 1000 + date.millisecond;
  const shiftScheduleDate = (
    value: string | null | undefined,
    sourceTimezone: string,
  ): string | null => {
    if (!value) return null;
    if (!anchor)
      throw new Error(
        'Календарные даты шаблона требуют опорную дату в календарных годах 0001–9999.',
      );
    if (!originalAnchor)
      throw new Error('Для точного расписания шаблона нужна исходная опорная дата.');
    const source = parseTime(value, sourceTimezone)?.setZone(originalAnchor.zoneName!);
    if (!source) throw new Error('В шаблоне некорректная дата.');
    const dayOffset = Math.round(
      source.startOf('day').diff(originalAnchor.startOf('day'), 'days').days,
    );
    const clockOffset = wallMillis(source) + wallMillis(anchor) - wallMillis(originalAnchor);
    const day = anchor
      .startOf('day')
      .plus({ days: dayOffset + Math.floor(clockOffset / 86400000) });
    const clock = ((clockOffset % 86400000) + 86400000) % 86400000;
    const parts = {
      year: day.year,
      month: day.month,
      day: day.day,
      hour: Math.floor(clock / 3600000),
      minute: Math.floor(clock / 60000) % 60,
      second: Math.floor(clock / 1000) % 60,
      millisecond: clock % 1000,
    };
    let shifted = DateTime.fromObject(parts, { zone: timezone });
    if (
      !shifted.isValid ||
      shifted.hour !== parts.hour ||
      shifted.minute !== parts.minute ||
      shifted.toISODate() !== day.toISODate()
    )
      throw new Error(
        'Дата шаблона попала в пропущенное местное время. Выберите другую опорную дату или время.',
      );
    shifted =
      shifted.getPossibleOffsets().sort((a, b) => a.toMillis() - b.toMillis())[0] ?? shifted;
    if (value.length === 10) return shifted.toISODate();
    const remainder = (ns: bigint) =>
      ((ns % NS_PER_MILLISECOND) + NS_PER_MILLISECOND) % NS_PER_MILLISECOND;
    const sourceNs = isoToNs(value, sourceTimezone)!;
    const correction = remainder(sourceNs) + remainder(anchorNs) - remainder(originalAnchorNs!);
    return correction !== 0n ||
      hasSubMillisecondTime({
        start: value,
        end: null,
        timezone: sourceTimezone,
        precision: 'exact',
      }) ||
      !Number.isInteger(shifted.offset)
      ? nsToISO(BigInt(shifted.toMillis()) * NS_PER_MILLISECOND + correction, timezone)
      : shifted.toISO();
  };
  const keys = new Set(template.items.map((item) => item.key));
  if (keys.size !== template.items.length)
    throw new Error('Ключи элементов шаблона должны быть уникальными.');
  for (const item of template.items) {
    if (item.schedule && rangeErrors(item.schedule, 'Шаблон').length)
      throw new Error('В шаблоне некорректное расписание.');
    if (item.schedule?.precise && item.recurrence)
      throw new Error(
        'Календарное повторение шаблона требует расписание ISO + IANA, без точных наносекундных координат.',
      );
    if (item.parentKey && !keys.has(item.parentKey))
      throw new Error('Родитель шаблона отсутствует.');
    if (
      !Number.isFinite(item.offsetDays) ||
      !Number.isFinite(item.durationDays) ||
      item.durationDays < 0
    )
      throw new Error('Некорректное смещение или длительность элемента шаблона.');
    const visited = new Set([item.key]);
    let parent = item.parentKey;
    while (parent) {
      if (visited.has(parent)) throw new Error('В шаблоне цикл вложенности.');
      visited.add(parent);
      parent = template.items.find((value) => value.key === parent)?.parentKey;
    }
  }
  for (const dep of template.dependencies)
    if (!keys.has(dep.fromKey) || !keys.has(dep.toKey))
      throw new Error('В шаблоне отсутствует объект зависимости.');
  const ids = new Map(template.items.map((item) => [item.key, newId('obj')])),
    at = DateTime.now().toUTC().toISO()!;
  const firstRoot = template.items.find((item) => !item.parentKey)?.key;
  const entities = template.items.map((item) => {
    let plan: TimeRange;
    if (item.schedule?.precise) {
      if (originalAnchorNs === null && hasTime(item.schedule))
        throw new Error('Для точного расписания шаблона нужна исходная опорная координата.');
      plan = shiftRangeNs(
        { ...item.schedule, timezone },
        originalAnchorNs === null ? 0n : anchorNs - originalAnchorNs,
      );
    } else {
      if (!anchor)
        throw new Error(
          'Календарный элемент шаблона требует опорную дату в календарных годах 0001–9999.',
        );
      const start = anchor.plus({ days: item.offsetDays });
      plan = {
        start: start.toISO(),
        end:
          item.kind === 'point' || item.kind === 'note' || item.kind === 'metric'
            ? null
            : start.plus({ days: item.durationDays }).toISO(),
        timezone,
        precision: options.anchorDate?.length === 10 ? 'day' : 'exact',
      };
    }
    if (item.schedule && !item.schedule.precise) {
      plan = { ...item.schedule, timezone };
      for (const key of ['start', 'end', 'earliest', 'latest'] as const)
        if (key in item.schedule)
          plan[key] = shiftScheduleDate(item.schedule[key], item.schedule.timezone);
    }
    const recurrence = item.recurrence ? structuredClone(item.recurrence) : null;
    if (recurrence && originalAnchor) {
      if (recurrence.until)
        recurrence.until = shiftScheduleDate(
          recurrence.until,
          item.schedule?.timezone ?? template.timezone ?? timezone,
        )!;
      recurrence.exceptions = recurrence.exceptions.map((date) =>
        shiftScheduleDate(date, item.schedule?.timezone ?? template.timezone ?? timezone)!,
      );
    }
    return {
      id: ids.get(item.key)!,
      workspaceId: options.workspaceId,
      typeId: item.typeId,
      kind: item.kind,
      title: item.key === firstRoot && options.title ? options.title : item.title,
      description: item.description ?? '',
      parentId: item.parentKey ? ids.get(item.parentKey)! : null,
      ownerId: options.ownerId ?? null,
      participantIds: [],
      status: 'draft' as const,
      plan,
      baseline: { ...plan },
      actual: null,
      forecast: null,
      dueAt: item.dueAt
        ? shiftScheduleDate(item.dueAt, item.schedule?.timezone ?? template.timezone ?? timezone)
        : null,
      tags: [...(item.tags ?? [])],
      fields: { ...item.fields },
      links: [],
      allocations: (item.allocations ?? []).map((allocation) => ({ ...allocation })),
      recurrence,
      source: {
        kind: 'manual' as const,
        label: `Шаблон «${template.name}»`,
        observedAt: at,
        receivedAt: at,
      },
      createdAt: at,
      updatedAt: at,
      version: 1,
    };
  });
  const dependencies: Dependency[] = template.dependencies.map((dep) => ({
    id: newId('dep'),
    workspaceId: options.workspaceId,
    fromId: ids.get(dep.fromKey)!,
    toId: ids.get(dep.toKey)!,
    kind: dep.kind,
    lagMinutes: dep.lagMinutes,
  }));
  if (cycleGroups(entities, dependencies).length) throw new Error('В шаблоне цикл зависимостей.');
  return { entities, dependencies };
}

const MONTHS = [
  'январ',
  'феврал',
  'март',
  'апрел',
  'ма[йя]',
  'июн',
  'июл',
  'август',
  'сентябр',
  'октябр',
  'ноябр',
  'декабр',
];
const WEEKDAYS = ['понедельник', 'вторник', 'сред', 'четверг', 'пятниц', 'суббот', 'воскресень'];

function datesFromText(text: string, now: DateTime, assumptions: string[]): DateTime[] {
  const iso = [
    ...text.matchAll(/\b(\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}(?::\d{2})?(?:Z|[+-]\d{2}:\d{2})?)?)\b/gi),
  ]
    .map((match) => parseTime(match[1]!, now.zoneName!))
    .filter((date): date is DateTime => !!date);
  if (iso.length) return iso;
  const numeric = [...text.matchAll(/\b(\d{1,2})\.(\d{1,2})(?:\.(\d{4}))?\b/g)]
    .map((match) =>
      DateTime.fromObject(
        { year: Number(match[3] ?? now.year), month: Number(match[2]), day: Number(match[1]) },
        { zone: now.zoneName! },
      ),
    )
    .filter((date) => date.isValid);
  if (numeric.length) {
    if (!/\d{1,2}\.\d{1,2}\.\d{4}/.test(text))
      assumptions.push(`Год не указан: использован ${now.year}.`);
    return numeric;
  }
  for (let month = 0; month < MONTHS.length; month++) {
    const pattern = new RegExp(
      `(?:с\\s+)?(\\d{1,2})\\s*(?:[-–—]|по|до)\\s*(\\d{1,2})\\s+${MONTHS[month]}[а-я]*(?:\\s+(\\d{4}))?`,
      'i',
    );
    const range = text.match(pattern);
    if (range) {
      if (!range[3]) assumptions.push(`Год не указан: использован ${now.year}.`);
      return [Number(range[1]), Number(range[2])]
        .map((day) =>
          DateTime.fromObject(
            { year: Number(range[3] ?? now.year), month: month + 1, day },
            { zone: now.zoneName! },
          ),
        )
        .filter((date) => date.isValid);
    }
  }
  const named: DateTime[] = [];
  for (let month = 0; month < MONTHS.length; month++) {
    const pattern = new RegExp(`(\\d{1,2})\\s+${MONTHS[month]}[а-я]*(?:\\s+(\\d{4}))?`, 'gi');
    for (const match of text.matchAll(pattern)) {
      if (!match[2]) assumptions.push(`Год не указан: использован ${now.year}.`);
      const date = DateTime.fromObject(
        { year: Number(match[2] ?? now.year), month: month + 1, day: Number(match[1]) },
        { zone: now.zoneName! },
      );
      if (date.isValid) named.push(date);
    }
  }
  if (named.length) return named;
  if (text.includes('послезавтра')) return [now.plus({ days: 2 }).startOf('day')];
  if (text.includes('завтра')) return [now.plus({ days: 1 }).startOf('day')];
  if (text.includes('сегодня')) return [now.startOf('day')];
  const after = text.match(/через\s+(\d+)\s*(д(?:ень|ня|ней)|недел[юьиь]|месяц[а-я]*)/i);
  if (after)
    return [
      now
        .plus({
          [after[2]!.startsWith('д') ? 'days' : after[2]!.startsWith('нед') ? 'weeks' : 'months']:
            Number(after[1]),
        })
        .startOf('day'),
    ];
  for (let index = 0; index < WEEKDAYS.length; index++)
    if (new RegExp(WEEKDAYS[index]!, 'i').test(text)) {
      const delta = (index + 1 - now.weekday + 7) % 7;
      return [now.plus({ days: delta || (text.includes('следующ') ? 7 : 0) }).startOf('day')];
    }
  return [];
}

/** Deterministic Russian parser and evidence lookup, deliberately not an external AI service. */
export function suggestFromText(
  text: string,
  snapshot: PlannerSnapshot,
  workspaceId: string,
  now = snapshot.serverTime,
): AssistantSuggestion {
  const workspace = snapshot.workspaces.find((item) => item.id === workspaceId);
  const base: AssistantSuggestion = {
    summary: '',
    drafts: [],
    evidenceIds: [],
    assumptions: [
      'Локальный разбор текста. Изменения сохраняются только после вашего подтверждения.',
    ],
    provider: 'local',
  };
  if (!workspace) return { ...base, summary: 'Пространство не найдено.' };
  const input = text.trim().toLocaleLowerCase('ru');
  const clock = parseTime(now, workspace.timezone) ?? DateTime.now().setZone(workspace.timezone);
  if (!input) return { ...base, summary: 'Напишите событие, период или вопрос о ваших объектах.' };
  if (
    /риск|почему|сорв|перегруз|что\s+(?:мешает|просрочено|требует)|покажи.*(?:проблем|срок)/.test(
      input,
    )
  ) {
    const active = evaluateSignals(snapshot, now).filter(
      (signal) => signal.workspaceId === workspaceId && signal.severity !== 'info',
    );
    base.evidenceIds = [...new Set(active.flatMap((signal) => signal.affectedIds))];
    base.summary = active.length
      ? `${active.length} условий требуют внимания. ${active
          .slice(0, 4)
          .map((signal) => `${signal.title}: ${signal.description}`)
          .join(' ')}`
      : 'По текущим датам, связям и распределениям явных рисков не найдено. Это не проверка внешних источников.';
    base.assumptions.push('Основания — только объекты, ресурсы и источники этого пространства.');
    return base;
  }
  if (/^(?:перенеси|сдвинь)/.test(input)) {
    const entity = snapshot.entities
      .filter((item) => item.workspaceId === workspaceId)
      .sort((a, b) => b.title.length - a.title.length)
      .find((item) => input.includes(item.title.toLocaleLowerCase('ru')));
    if (!entity || !entity.plan.start)
      return {
        ...base,
        summary: 'Для переноса укажите точное название существующего объекта с начальной датой.',
        assumptions: [...base.assumptions, 'Неоднозначный объект не выбирается автоматически.'],
      };
    const days = input.match(/на\s+(\d+)\s*д(?:ень|ня|ней)/);
    const dates = datesFromText(input, clock, base.assumptions);
    const original = parseTime(entity.plan.start, entity.plan.timezone)!;
    const start = days ? original.plus({ days: Number(days[1]) }) : dates[0];
    if (!start)
      return {
        ...base,
        summary: 'Объект найден; уточните новую дату или число дней сдвига.',
        evidenceIds: [entity.id],
      };
    base.preview = deriveForecast(
      snapshot,
      [{ entityId: entity.id, plan: shiftedRange(entity.plan, start) }],
      workspaceId,
    );
    base.evidenceIds = base.preview.affectedIds;
    base.summary = `Подготовлен вариант переноса «${entity.title}»: затронуто ${base.preview.affectedIds.length}, конфликтов ${base.preview.conflicts.length}. План пока не изменён.`;
    return base;
  }
  const dates = datesFromText(input, clock, base.assumptions);
  const time = input.match(/(?:в\s+|\b)([01]?\d|2[0-3]):([0-5]\d)\b/);
  let start = dates[0] ?? null;
  if (start && time) start = start.set({ hour: Number(time[1]), minute: Number(time[2]) });
  let end = dates[1] ?? null;
  const duration = input.match(
    /(?:на|длительностью)\s+(\d+)\s*(минут[а-я]*|час[а-я]*|д(?:ень|ня|ней)|недел[а-я]*)/,
  );
  if (start && duration)
    end = start.plus({
      [duration[2]!.startsWith('мин')
        ? 'minutes'
        : duration[2]!.startsWith('час')
          ? 'hours'
          : duration[2]!.startsWith('нед')
            ? 'weeks'
            : 'days']: Number(duration[1]),
    });
  if (start && /на\s+неделю/.test(input)) end = start.plus({ weeks: 1 });
  const candidates: [RegExp, string][] = [
    [/отпуск/, 'vacation'],
    [/поездк|путешеств|командировк/, 'trip'],
    [/обуч|курс|урок|занят/, 'learning'],
    [/встреч|созвон/, 'meeting'],
    [/заметк|идея|мысль/, 'note'],
    [/измер|пульс|температур|вес\s+\d/, 'metric'],
    [/проект|процесс|запуск/, 'process'],
  ];
  const wanted =
    candidates.find(([pattern]) => pattern.test(input))?.[1] ?? (end ? 'period' : 'event');
  const accessibleTypes = snapshot.types.filter(
    (item) => !item.workspaceId || item.workspaceId === workspaceId,
  );
  const namedCustomType = accessibleTypes
    .filter((item) => !item.builtin)
    .find((item) => input.includes(item.label.toLocaleLowerCase('ru')));
  const type =
    namedCustomType ??
    accessibleTypes.find((item) => item.id === wanted) ??
    accessibleTypes.find((item) => item.kind === (end ? 'period' : 'point'));
  if (!type) return { ...base, summary: 'В пространстве нет подходящего типа объекта.' };
  const fields: EntityDraft['fields'] = {};
  if (type.kind === 'metric') {
    const value = input.match(
      /(?:пульс|температур[а-я]*|вес|значение)\s*[:=]?\s*(-?\d+(?:[.,]\d+)?)/,
    );
    if (value) {
      const numericField = type.fields.find((field) => field.type === 'number');
      if (numericField) fields[numericField.id] = Number(value[1]!.replace(',', '.'));
    }
  }
  for (const field of type.fields) {
    const escaped = field.label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const match = text.match(new RegExp(`${escaped}\\s*[:=]\\s*([^;\\n]+)`, 'i'));
    if (!match) continue;
    const value = match[1]!.trim();
    if (field.type === 'number') {
      const numeric = Number(value.replace(',', '.'));
      if (Number.isFinite(numeric)) fields[field.id] = numeric;
    } else if (field.type === 'boolean') {
      if (/^(?:да|true|нет|false)$/i.test(value)) fields[field.id] = /^(?:да|true)$/i.test(value);
    } else fields[field.id] = value;
  }
  const recurring = /кажд|ежеднев|еженедел|ежемесяч/.test(input);
  const weekdays = WEEKDAYS.map((day, index) =>
    new RegExp(day).test(input) ? index + 1 : 0,
  ).filter(Boolean);
  const plan: TimeRange = {
    start: start?.toISO() ?? null,
    end: end?.toISO() ?? null,
    timezone: workspace.timezone,
    precision: start ? (time || /\d{4}-\d{2}-\d{2}T/.test(input) ? 'exact' : 'day') : 'unknown',
  };
  if (start && !end && ['period', 'process'].includes(type.kind))
    base.assumptions.push('Окончание периода не указано: оно остаётся неизвестным.');
  if (!start) base.assumptions.push('Дата не распознана: черновик будет без даты.');
  if (dates.length > 1 && dates[1]!.toMillis() < dates[0]!.toMillis())
    base.assumptions.push('Окончание раньше начала: исправьте даты перед сохранением.');
  const title = text
    .replace(/^(?:создай|добавь|запланируй|составь|поставь)\s+/i, '')
    .trim()
    .slice(0, 180);
  base.drafts = [
    {
      workspaceId,
      typeId: type.id,
      kind: type.kind,
      title,
      description: text,
      parentId: null,
      ownerId: snapshot.user.id,
      participantIds: [],
      status: 'draft',
      plan,
      actual: null,
      forecast: null,
      dueAt: null,
      tags: [],
      fields,
      links: [],
      allocations: [],
      recurrence:
        recurring && start
          ? {
              frequency: /ежемесяч|каждый\s+месяц/.test(input)
                ? 'month'
                : weekdays.length || /еженедел|каждую\s+недел/.test(input)
                  ? 'week'
                  : 'day',
              interval: 1,
              ...(weekdays.length ? { weekdays } : {}),
              exceptions: [],
            }
          : null,
    },
  ];
  base.summary = `Подготовлен черновик «${title}»${start ? ` на ${start.setLocale('ru').toFormat('d LLL yyyy')}` : ' без даты'}. Проверьте время и поля перед сохранением.`;
  return base;
}
