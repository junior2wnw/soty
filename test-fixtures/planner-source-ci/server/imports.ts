import { DateTime, Duration } from 'luxon';
import type { PlannerSnapshot, User, Entity, Recurrence, TimeRange } from '../shared/types.ts';
import { PlannerStore, uid } from './store.ts';
import {
  ApiError,
  object,
  text,
  parse,
  id,
  typeSchema,
  resourceSchema,
  dependencySchema,
} from './validation.ts';
import { parseTime, validateDependency } from '../shared/engine.ts';

const MAX_IMPORT_ENTITIES = 1000;
const CSV_VERSION = 'universal-planner-csv-v3';
const LEGACY_CSV_VERSIONS = new Set(['universal-planner-csv-v2', CSV_VERSION]);
const DAYS = ['MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'];
const ATTACHMENT_WARNING =
  'Двоичные вложения и ссылки на локальные файлы не перенесены. Полная резервная копия — каталог данных с SQLite и файлами, снятый при остановленном сервере.';
const ICS_PROJECTION_WARNING =
  'ICS переносит календарный план, название, описание, метки и первую веб-ссылку. Типы, дополнительные поля, базовый план, факт, прогноз, ресурсы и зависимости сохраняйте в JSON.';
const unique = (warnings: string[]) => [...new Set(warnings)];

export function parseCsv(content: string): Record<string, string>[] {
  content = content.replace(/^\uFEFF/, '');
  const firstLine = content.split(/\r?\n/, 1)[0] ?? '';
  const delimiter = firstLine.includes(';') && !firstLine.includes(',') ? ';' : ',';
  const rows: string[][] = [];
  let row: string[] = [],
    cell = '',
    quoted = false,
    afterQuote = false;
  for (let i = 0; i < content.length; i++) {
    const c = content[i];
    if (quoted) {
      if (c === '"') {
        if (content[i + 1] === '"') {
          cell += '"';
          i++;
        } else {
          quoted = false;
          afterQuote = true;
        }
      } else cell += c;
      continue;
    }
    if (c === '"') {
      if (cell || afterQuote) throw new ApiError(400, 'Некорректные кавычки CSV');
      quoted = true;
      continue;
    }
    if (afterQuote && ![delimiter, '\n', '\r'].includes(c))
      throw new ApiError(400, 'Лишние символы после кавычек CSV');
    if (c === delimiter) {
      row.push(cell);
      cell = '';
      afterQuote = false;
    } else if (c === '\n' || c === '\r') {
      if (c === '\r' && content[i + 1] === '\n') i++;
      row.push(cell);
      if (row.some((v) => v.length)) rows.push(row);
      row = [];
      cell = '';
      afterQuote = false;
    } else cell += c;
  }
  if (quoted) throw new ApiError(400, 'Незакрытая строка CSV');
  row.push(cell);
  if (row.some((v) => v.length)) rows.push(row);
  const headers = rows.shift()?.map((v) => v.trim());
  if (
    !headers?.includes('title') ||
    headers.some((v) => !v) ||
    new Set(headers).size !== headers.length
  )
    throw new ApiError(400, 'CSV должен содержать уникальные заголовки и колонку title');
  if (rows.length > MAX_IMPORT_ENTITIES)
    throw new ApiError(413, 'Слишком много объектов в импорте');
  return rows.map((values, index) => {
    if (values.length !== headers.length)
      throw new ApiError(400, `CSV: строка ${index + 2} имеет другое число колонок`);
    return Object.fromEntries(headers.map((h, i) => [h, values[i]]));
  });
}

function jsonValue(value: string, label: string): unknown {
  try {
    return JSON.parse(value);
  } catch {
    throw new ApiError(400, `Некорректный JSON в ${label}`);
  }
}

function csvDraft(
  source: Record<string, string>,
  workspaceId: string,
  timezone: string,
  warnings: string[],
): Record<string, unknown> {
  const row = { ...source };
  for (const key of (row.escapedFields ?? '').split('|'))
    if (row[key]?.startsWith("'") && /^[=+@-]/.test(row[key].slice(1)))
      row[key] = row[key].slice(1);
  if (row.entityData) {
    if (!LEGACY_CSV_VERSIONS.has(row.formatVersion))
      throw new ApiError(400, 'Неизвестная версия полного CSV. Используйте JSON или CSV v3.');
    const raw = object(jsonValue(row.entityData, 'entityData'));
    for (const key of ['title', 'typeId', 'kind', 'description', 'status'] as const)
      if (key in row) raw[key] = row[key];
    if ('dueAt' in row) raw.dueAt = row.dueAt || null;
    const plan = { ...object(raw.plan) };
    for (const key of ['start', 'end', 'timezone'] as const)
      if (key in row) plan[key] = row[key] || (key === 'timezone' ? timezone : null);
    if ('precise' in row)
      plan.precise = row.precise ? object(jsonValue(row.precise, 'precise')) : undefined;
    raw.plan = plan;
    if ('tags' in row && row.tags !== (Array.isArray(raw.tags) ? raw.tags.join('|') : ''))
      raw.tags = row.tags ? row.tags.split('|') : [];
    return { ...raw, workspaceId };
  }
  warnings.push(
    'Обычный CSV — проекция колонок. Отсутствующие поля модели не восстановлены; полный перенос сохраняет CSV v3 этого приложения или JSON.',
  );
  if (row.precise)
    warnings.push(
      'CSV: precise прочитан как JSON с decimal strings; табличные вычисления над этими значениями могут потерять точность.',
    );
  const fields: Record<string, unknown> = row.fields ? object(jsonValue(row.fields, 'fields')) : {};
  for (const [key, value] of Object.entries(row))
    if (key.startsWith('field.')) fields[key.slice(6)] = value;
  const known = new Set([
    'id',
    'title',
    'typeId',
    'kind',
    'description',
    'status',
    'start',
    'end',
    'timezone',
    'precision',
    'precise',
    'dueAt',
    'tags',
    'fields',
    'actual',
    'forecast',
    'forecastProvenance',
    'baseline',
    'recurrence',
    'allocations',
    'links',
    'parentId',
    'ownerId',
    'participantIds',
    'escapedFields',
    'formatVersion',
    'workspaceData',
    'exportWarnings',
  ]);
  const ignored = Object.keys(row).filter((key) => !known.has(key) && !key.startsWith('field.'));
  if (ignored.length)
    warnings.push(
      `CSV: нераспознанные колонки ${ignored.join(', ')} не перенесены. Дополнительные значения задаются колонкой fields с JSON или field.<id>.`,
    );
  const draft: Record<string, unknown> = {
    workspaceId,
    id: row.id || undefined,
    title: row.title,
    typeId: row.typeId || undefined,
    kind: row.kind || undefined,
    description: row.description || '',
    status: row.status || 'planned',
    plan: {
      start: row.start || null,
      end: row.end || null,
      timezone: row.timezone || timezone,
      precision: row.precision || (row.start ? 'exact' : 'unknown'),
      ...(row.precise ? { precise: object(jsonValue(row.precise, 'precise')) } : {}),
    },
    dueAt: row.dueAt || null,
    tags: row.tags ? row.tags.split('|') : [],
    fields,
    parentId: row.parentId || null,
    ownerId: row.ownerId || undefined,
    forecastProvenance: row.forecastProvenance || undefined,
  };
  for (const key of [
    'actual',
    'forecast',
    'baseline',
    'recurrence',
    'allocations',
    'links',
    'participantIds',
  ])
    if (row[key]) draft[key] = jsonValue(row[key], key);
  return draft;
}

const unescapeIcs = (value: string) =>
  value.replace(/\\([nN\\,;])/g, (_match, character: string) =>
    /[nN]/.test(character) ? '\n' : character,
  );
const escapeIcs = (value: string) =>
  value.replace(/\\/g, '\\\\').replace(/\r?\n/g, '\\n').replace(/,/g, '\\,').replace(/;/g, '\\;');
function icsTextList(value: string): string[] {
  const values: string[] = [];
  let item = '',
    escaped = false;
  for (const character of value) {
    if (escaped) {
      item += '\\' + character;
      escaped = false;
    } else if (character === '\\') escaped = true;
    else if (character === ',') {
      values.push(unescapeIcs(item));
      item = '';
    } else item += character;
  }
  if (escaped) item += '\\';
  values.push(unescapeIcs(item));
  return values;
}
interface IcsProperty {
  value: string;
  params: Record<string, string>;
}
type IcsEvent = Record<string, IcsProperty[]>;
interface IcsDate {
  value: string;
  date: DateTime;
  dateOnly: boolean;
  zone: string;
  floating: boolean;
}

function icsProperty(line: string): { name: string; property: IcsProperty } {
  let quoted = false,
    colon = -1;
  for (let i = 0; i < line.length; i++) {
    if (line[i] === '"') quoted = !quoted;
    else if (line[i] === ':' && !quoted) {
      colon = i;
      break;
    }
  }
  if (colon < 1 || quoted) throw new ApiError(400, 'Некорректное поле ICS');
  const parts = line.slice(0, colon).match(/(?:[^;"]|"[^"]*")+/g) ?? [];
  const name = (parts.shift() ?? '').toUpperCase(),
    params: Record<string, string> = {};
  if (!/^[A-Z0-9-]+$/.test(name)) throw new ApiError(400, 'Некорректное имя поля ICS');
  for (const part of parts) {
    const equal = part.indexOf('='),
      key = part.slice(0, equal).toUpperCase();
    if (equal < 1 || params[key] !== undefined)
      throw new ApiError(400, 'Некорректный или повторный параметр ICS');
    params[key] = part.slice(equal + 1).replace(/^"(.*)"$/, '$1');
  }
  return { name, property: { value: line.slice(colon + 1), params } };
}

function icsDate(property: IcsProperty, fallbackZone: string): IcsDate {
  const { value, params } = property;
  const dateOnly = params.VALUE === 'DATE' || (!params.VALUE && /^\d{8}$/.test(value));
  if (params.VALUE && !['DATE', 'DATE-TIME'].includes(params.VALUE))
    throw new ApiError(400, 'ICS: неподдерживаемый тип даты');
  if ((dateOnly && !/^\d{8}$/.test(value)) || (!dateOnly && !/^\d{8}T\d{6}Z?$/.test(value)))
    throw new ApiError(400, 'ICS содержит неподдерживаемый формат даты');
  const utc = value.endsWith('Z');
  if (params.TZID && (dateOnly || utc))
    throw new ApiError(400, 'TZID нельзя сочетать с VALUE=DATE или временем UTC');
  const zone = utc ? 'UTC' : (params.TZID ?? fallbackZone);
  const components = {
    year: Number(value.slice(0, 4)),
    month: Number(value.slice(4, 6)),
    day: Number(value.slice(6, 8)),
    hour: dateOnly ? 0 : Number(value.slice(9, 11)),
    minute: dateOnly ? 0 : Number(value.slice(11, 13)),
    second: dateOnly ? 0 : Number(value.slice(13, 15)),
  };
  let date = DateTime.fromObject(components, { zone });
  if (
    !date.isValid ||
    Object.entries(components).some(
      ([key, expected]) => date[key as keyof typeof components] !== expected,
    )
  )
    throw new ApiError(400, `ICS содержит невозможную дату или местное время в ${zone}`);
  date = date.getPossibleOffsets().sort((a, b) => a.toMillis() - b.toMillis())[0] ?? date;
  return {
    value: dateOnly ? date.toISODate()! : date.toISO()!,
    date,
    dateOnly,
    zone,
    floating: !dateOnly && !utc && !params.TZID,
  };
}

function single(event: IcsEvent, name: string): IcsProperty | undefined {
  if ((event[name]?.length ?? 0) > 1) throw new ApiError(400, `Повтор поля ${name} в ICS`);
  return event[name]?.[0];
}

function recurrenceOf(event: IcsEvent, start: IcsDate): Recurrence | null {
  const property = single(event, 'RRULE');
  if (!property) {
    if (event.EXDATE) throw new ApiError(400, 'ICS: EXDATE без RRULE не поддерживается');
    return null;
  }
  const rules: Record<string, string> = {};
  for (const part of property.value.toUpperCase().split(';')) {
    const pair = part.split('=');
    if (pair.length !== 2 || !pair[0] || !pair[1] || rules[pair[0]])
      throw new ApiError(400, 'ICS: некорректное или повторное поле RRULE');
    rules[pair[0]] = pair[1];
  }
  const supported = new Set(['FREQ', 'INTERVAL', 'COUNT', 'UNTIL', 'BYDAY', 'WKST']);
  const unsupported = Object.keys(rules).filter((key) => !supported.has(key));
  if (unsupported.length)
    throw new ApiError(
      400,
      `ICS: RRULE ${unsupported.join(', ')} не поддерживается. Правило не импортировано.`,
      'unsupported_recurrence',
    );
  const frequency = ({ DAILY: 'day', WEEKLY: 'week', MONTHLY: 'month' } as const)[
    rules.FREQ as 'DAILY' | 'WEEKLY' | 'MONTHLY'
  ];
  if (!frequency)
    throw new ApiError(
      400,
      'ICS: поддерживаются RRULE DAILY, WEEKLY и MONTHLY',
      'unsupported_recurrence',
    );
  if (rules.COUNT && rules.UNTIL)
    throw new ApiError(400, 'ICS: COUNT и UNTIL не могут одновременно ограничивать RRULE');
  if (rules.WKST && rules.WKST !== 'MO')
    throw new ApiError(400, 'ICS: поддерживается начало недели WKST=MO', 'unsupported_recurrence');
  const number = (value: string, maximum: number, label: string) => {
    if (!/^\d+$/.test(value) || Number(value) < 1 || Number(value) > maximum)
      throw new ApiError(400, `ICS: ${label} должен быть целым числом от 1 до ${maximum}`);
    return Number(value);
  };
  const rule: Recurrence = {
    frequency,
    interval: number(rules.INTERVAL || '1', 365, 'INTERVAL'),
    exceptions: [],
    calendarPolicy: 'skip-invalid',
  };
  if (rules.COUNT) rule.count = number(rules.COUNT, 100_000, 'COUNT');
  if (rules.UNTIL) {
    if (
      start.dateOnly !== /^\d{8}$/.test(rules.UNTIL) ||
      (!start.dateOnly && !start.floating && !rules.UNTIL.endsWith('Z')) ||
      (start.floating && rules.UNTIL.endsWith('Z'))
    )
      throw new ApiError(
        400,
        'ICS: тип и часовой пояс UNTIL должны соответствовать DTSTART (для TZID требуется UTC)',
      );
    rule.until = icsDate(
      { value: rules.UNTIL, params: start.dateOnly ? { VALUE: 'DATE' } : {} },
      start.zone,
    ).value;
    if ((parseTime(rule.until, start.zone)?.toMillis() ?? 0) < start.date.toMillis())
      throw new ApiError(400, 'ICS: UNTIL раньше DTSTART');
  }
  if (rules.BYDAY) {
    if (frequency !== 'week')
      throw new ApiError(
        400,
        'ICS: BYDAY поддерживается только для WEEKLY',
        'unsupported_recurrence',
      );
    const weekdays = rules.BYDAY.split(',');
    if (
      weekdays.some((value) => !DAYS.includes(value)) ||
      new Set(weekdays).size !== weekdays.length
    )
      throw new ApiError(
        400,
        'ICS: поддерживаются неповторяющиеся BYDAY MO–SU без порядковых номеров',
        'unsupported_recurrence',
      );
    rule.weekdays = weekdays.map((value) => DAYS.indexOf(value) + 1).sort((a, b) => a - b);
    if (!rule.weekdays.includes(start.date.weekday))
      throw new ApiError(
        400,
        'ICS: DTSTART должен входить в BYDAY; иначе набор повторений неоднозначен',
        'unsupported_recurrence',
      );
  }
  for (const property of event.EXDATE ?? [])
    for (const value of property.value.split(',')) {
      const exception = icsDate({ ...property, value }, start.zone);
      if (exception.dateOnly !== start.dateOnly)
        throw new ApiError(400, 'ICS: EXDATE и DTSTART должны иметь одинаковый тип даты');
      rule.exceptions.push(exception.value);
    }
  rule.exceptions = [...new Set(rule.exceptions)];
  return rule;
}

function icsDraft(
  event: IcsEvent,
  workspaceId: string,
  timezone: string,
  warnings: string[],
): Record<string, unknown> {
  for (const key of ['RDATE', 'RECURRENCE-ID'])
    if (event[key])
      throw new ApiError(
        400,
        `ICS: ${key} не поддерживается; календарный набор не импортирован`,
        'unsupported_recurrence',
      );
  const property = single(event, 'DTSTART');
  if (!property) throw new ApiError(400, 'У события ICS отсутствует DTSTART');
  const start = icsDate(property, timezone),
    endProperty = single(event, 'DTEND'),
    durationProperty = single(event, 'DURATION');
  if (endProperty && durationProperty)
    throw new ApiError(400, 'ICS: DTEND и DURATION взаимоисключающие');
  let end = endProperty ? icsDate(endProperty, start.zone) : null;
  if (end && (end.dateOnly !== start.dateOnly || end.date.toMillis() <= start.date.toMillis()))
    throw new ApiError(400, 'ICS: DTEND должен быть позже DTSTART и иметь тот же тип даты');
  if (durationProperty) {
    const value = durationProperty.value;
    const duration = Duration.fromISO(value);
    if (
      !/^P(?:\d+W|(?:\d+D)?(?:T(?:\d+H)?(?:\d+M)?(?:\d+S)?)?)$/.test(value) ||
      !duration.isValid ||
      duration.as('milliseconds') <= 0 ||
      (start.dateOnly && value.includes('T'))
    )
      throw new ApiError(
        400,
        'ICS: DURATION должен быть положительным периодом дней/недель/времени',
      );
    if (
      (duration.days || duration.weeks) &&
      Duration.fromObject({
        hours: duration.hours,
        minutes: duration.minutes,
        seconds: duration.seconds,
      }).as('hours') >= 24
    )
      throw new ApiError(
        400,
        'ICS: смешанный DURATION с временем больше суток не поддерживается',
        'unsupported_recurrence',
      );
    const date = start.date.plus(duration);
    end = { ...start, date, value: start.dateOnly ? date.toISODate()! : date.toISO()! };
  }
  if (start.dateOnly && !end) {
    const date = start.date.plus({ days: 1 });
    end = { ...start, date, value: date.toISODate()! };
  }
  if (start.floating)
    warnings.push(
      `ICS: местное время без TZID интерпретировано в часовом поясе пространства ${timezone}.`,
    );
  const known = new Set([
    'UID',
    'DTSTAMP',
    'DTSTART',
    'DTEND',
    'DURATION',
    'SUMMARY',
    'DESCRIPTION',
    'RRULE',
    'EXDATE',
    'STATUS',
    'CATEGORIES',
    'URL',
    'LOCATION',
    'CREATED',
    'LAST-MODIFIED',
    'SEQUENCE',
    'TRANSP',
    'X-PLANNER-TIMEZONE',
    'X-PLANNER-PROJECTION',
    'X-PLANNER-WARNING',
  ]);
  const ignored = Object.keys(event).filter((key) => !known.has(key));
  if (ignored.length) warnings.push(`ICS: поля ${ignored.join(', ')} не перенесены.`);
  const statusValue = single(event, 'STATUS')?.value.toUpperCase();
  if (statusValue && !['CONFIRMED', 'TENTATIVE', 'CANCELLED'].includes(statusValue))
    throw new ApiError(400, 'ICS: неизвестный STATUS события');
  const link = single(event, 'URL'),
    location = single(event, 'LOCATION');
  const uidValue = single(event, 'UID')?.value;
  const recurrence = recurrenceOf(event, start);
  if (recurrence && end && !start.dateOnly)
    recurrence.durationPolicy =
      endProperty ||
      (durationProperty &&
        !Duration.fromISO(durationProperty.value).days &&
        !Duration.fromISO(durationProperty.value).weeks)
        ? 'elapsed'
        : 'calendar';
  return {
    id: uidValue,
    workspaceId,
    title: unescapeIcs(single(event, 'SUMMARY')?.value || 'Событие'),
    description: unescapeIcs(single(event, 'DESCRIPTION')?.value || ''),
    typeId: end ? 'period' : 'event',
    kind: end ? 'period' : 'point',
    status:
      statusValue === 'CANCELLED' ? 'cancelled' : statusValue === 'TENTATIVE' ? 'draft' : 'planned',
    plan: {
      start: start.value,
      end: end?.value ?? null,
      timezone: start.zone,
      precision: start.dateOnly ? 'day' : 'exact',
    },
    tags: (event.CATEGORIES ?? []).flatMap((p) => icsTextList(p.value)),
    fields: location ? { location: unescapeIcs(location.value) } : {},
    links: link
      ? [{ id: uid('link'), label: 'Ссылка календаря', url: link.value, kind: 'url' }]
      : [],
    recurrence,
  };
}

export function parseIcs(
  content: string,
  workspaceId: string,
  timezone: string,
  warnings: string[] = [],
): Record<string, unknown>[] {
  const lines = content
    .replace(/^\uFEFF/, '')
    .replace(/\r\n/g, '\n')
    .replace(/\n[ \t]/g, '')
    .split('\n');
  const drafts: Record<string, unknown>[] = [],
    stack: string[] = [];
  let event: IcsEvent | null = null,
    ended = false,
    calendarCount = 0;
  for (const line of lines) {
    if (!line) continue;
    const { name, property } = icsProperty(line);
    if (name === 'BEGIN') {
      const component = property.value.toUpperCase(),
        parent = stack.at(-1);
      if (component === 'VCALENDAR') {
        if (stack.length || ended || ++calendarCount > 1)
          throw new ApiError(400, 'Ожидается один календарь ICS');
      } else if (!parent) throw new ApiError(400, 'Компонент вне VCALENDAR');
      else if (component === 'VEVENT') {
        if (parent !== 'VCALENDAR' || event) throw new ApiError(400, 'Вложенный VEVENT');
        event = {};
      } else if (component === 'VALARM' && parent === 'VEVENT')
        warnings.push(
          'Напоминания VALARM не перенесены; настройте правила уведомлений в приложении.',
        );
      else if (component === 'VTIMEZONE' && parent === 'VCALENDAR')
        warnings.push(
          'Часовые пояса ICS интерпретированы по установленным правилам IANA; собственные определения VTIMEZONE не импортированы.',
        );
      else if (parent !== 'VTIMEZONE' || !['STANDARD', 'DAYLIGHT'].includes(component))
        throw new ApiError(400, `ICS: компонент ${component} не поддерживается`);
      stack.push(component);
      continue;
    }
    if (name === 'END') {
      const component = property.value.toUpperCase();
      if (stack.pop() !== component) throw new ApiError(400, 'Несогласованные компоненты ICS');
      if (component === 'VEVENT') {
        drafts.push(icsDraft(event!, workspaceId, timezone, warnings));
        event = null;
      }
      if (component === 'VCALENDAR') ended = true;
      continue;
    }
    if (!stack.length || ended) throw new ApiError(400, 'Поле вне календаря ICS');
    if (stack.at(-1) === 'VEVENT' && event) (event[name] ??= []).push(property);
  }
  if (stack.length || event || !ended || calendarCount !== 1)
    throw new ApiError(400, 'ICS не завершён');
  if (!drafts.length || drafts.length > MAX_IMPORT_ENTITIES)
    throw new ApiError(400, 'ICS должен содержать от 1 до 1000 событий');
  return drafts;
}

export function importContent(
  store: PlannerStore,
  user: User,
  input: Record<string, unknown>,
  hooks?: {
    before?: (state: PlannerSnapshot) => void;
    after?: (state: PlannerSnapshot, warnings: string[]) => void;
  },
): PlannerSnapshot & { importWarnings: string[] } {
  const workspaceId = parse(id, input.workspaceId),
    format = text(input.format, 'json'),
    warnings: string[] = [];
  const result = store.change(user, workspaceId, 'import', `Импорт ${format}`, (state) => {
    hooks?.before?.(state);
    const ws = state.workspaces.find((w) => w.id === workspaceId)!;
    let payload: Record<string, unknown> = {},
      rawEntities: Record<string, unknown>[] = [];
    if (format === 'csv') {
      const rows = parseCsv(text(input.content));
      const metadata = rows.filter((row) => row.workspaceData);
      if (metadata.length > 1)
        throw new ApiError(400, 'CSV: workspaceData должен находиться только в первой строке');
      if (metadata.length) payload = object(jsonValue(metadata[0].workspaceData, 'workspaceData'));
      rawEntities = rows.map((row) => csvDraft(row, workspaceId, ws.timezone, warnings));
    } else if (format === 'ics') {
      rawEntities = parseIcs(text(input.content), workspaceId, ws.timezone, warnings);
      warnings.push(ICS_PROJECTION_WARNING);
    } else if (format === 'json') {
      try {
        const parsed =
          typeof input.content === 'string' ? JSON.parse(input.content) : input.content;
        if (Array.isArray(parsed)) rawEntities = parsed.map(object);
        else {
          payload = object(parsed);
          if (!Array.isArray(payload.entities))
            throw new ApiError(400, 'JSON должен содержать entities');
          rawEntities = payload.entities.map(object);
        }
      } catch (error) {
        if (error instanceof ApiError) throw error;
        throw new ApiError(400, 'Некорректный JSON');
      }
    } else throw new ApiError(400, 'Неизвестный формат');
    if (payload.schemaVersion !== undefined && ![1, 2, 3].includes(Number(payload.schemaVersion)))
      throw new ApiError(400, 'Неизвестная версия JSON/CSV модели');
    if (!rawEntities.length || rawEntities.length > MAX_IMPORT_ENTITIES)
      throw new ApiError(400, 'Нужно от 1 до 1000 объектов');
    const originalIds = rawEntities.filter((raw) => raw.id).map((raw) => text(raw.id));
    if (new Set(originalIds).size !== originalIds.length)
      throw new ApiError(400, 'Повторяющиеся ID объектов');
    const typeMap = new Map<string, string>(),
      resourceMap = new Map<string, string>();
    if (payload.types !== undefined) {
      if (!Array.isArray(payload.types) || payload.types.length > 100)
        throw new ApiError(400, 'Некорректные типы');
      for (const raw of payload.types) {
        const type = object(raw),
          original = text(type.id),
          builtin = state.types.find((t) => t.id === original && t.builtin);
        if (typeMap.has(original)) throw new ApiError(400, 'Повторяющиеся ID типов');
        if (builtin && type.builtin) {
          typeMap.set(original, original);
          continue;
        }
        const next = parse(typeSchema, { ...type, id: uid('type'), workspaceId, builtin: false });
        state.types.push(next);
        typeMap.set(original, next.id);
      }
    }
    if (payload.resources !== undefined) {
      if (!Array.isArray(payload.resources) || payload.resources.length > 1000)
        throw new ApiError(400, 'Некорректные ресурсы');
      for (const raw of payload.resources) {
        const resource = object(raw),
          original = text(resource.id);
        if (resourceMap.has(original)) throw new ApiError(400, 'Повторяющиеся ID ресурсов');
        const next = parse(resourceSchema, { ...resource, id: uid('resource'), workspaceId });
        state.resources.push(next);
        resourceMap.set(original, next.id);
      }
    }
    const idMap = new Map<string, string>(),
      created: Entity[] = [];
    const memberIds = new Set(
      state.memberships.filter((m) => m.workspaceId === workspaceId).map((m) => m.userId),
    );
    for (const raw of rawEntities) {
      const rawLinks = Array.isArray(raw.links) ? raw.links.map(object) : [],
        links = rawLinks.filter((link) => link.kind !== 'file');
      if (rawLinks.length !== links.length) warnings.push(ATTACHMENT_WARNING);
      const allocations = Array.isArray(raw.allocations)
        ? raw.allocations.map((value) => {
            const allocation = object(value);
            return {
              ...allocation,
              resourceId: resourceMap.get(text(allocation.resourceId)) ?? allocation.resourceId,
            };
          })
        : [];
      const ownerId =
        raw.ownerId === null ? null : memberIds.has(text(raw.ownerId)) ? raw.ownerId : user.id;
      const rawParticipants = Array.isArray(raw.participantIds) ? raw.participantIds : [],
        participantIds = rawParticipants.filter(
          (value) => typeof value === 'string' && memberIds.has(value),
        );
      if (
        (raw.ownerId && raw.ownerId !== ownerId) ||
        rawParticipants.length !== participantIds.length
      )
        warnings.push(
          'Участники вне текущего пространства не перенесены; недоступный ответственный заменён импортирующим пользователем.',
        );
      const originalSource = raw.source ? object(raw.source) : {};
      const now = new Date().toISOString();
      const entity = store.makeEntity(
        {
          ...raw,
          workspaceId,
          typeId: typeMap.get(text(raw.typeId)) ?? raw.typeId,
          parentId: null,
          ownerId,
          participantIds,
          allocations,
          links,
          source: {
            kind: 'import',
            label: `Импорт ${format.toUpperCase()}`,
            externalId: raw.id ? text(raw.id) : undefined,
            observedAt: originalSource.observedAt ?? now,
            receivedAt: now,
            ...(originalSource.staleAfterMinutes != null
              ? { staleAfterMinutes: originalSource.staleAfterMinutes }
              : {}),
          },
        },
        state,
        user,
        true,
      );
      if (raw.id) idMap.set(text(raw.id), entity.id);
      created.push(entity);
      state.entities.push(entity);
    }
    for (let i = 0; i < created.length; i++) {
      const parent = rawEntities[i].parentId;
      if (parent) {
        const mapped = idMap.get(text(parent));
        if (!mapped) throw new ApiError(400, 'Родитель импорта не найден');
        created[i].parentId = mapped;
      }
      store.validateEntityReferences(created[i], state);
    }
    if (payload.dependencies !== undefined) {
      if (!Array.isArray(payload.dependencies) || payload.dependencies.length > 5000)
        throw new ApiError(400, 'Некорректные связи');
      for (const raw of payload.dependencies) {
        const dep = object(raw),
          fromId = idMap.get(text(dep.fromId)),
          toId = idMap.get(text(dep.toId));
        if (!fromId || !toId) throw new ApiError(400, 'Связь с отсутствующим объектом');
        const dependency = parse(dependencySchema, {
          ...dep,
          id: uid('dependency'),
          workspaceId,
          fromId,
          toId,
        });
        const errors = validateDependency(dependency, state);
        if (errors.length)
          throw new ApiError(400, 'Некорректная связь импорта', 'validation', errors);
        state.dependencies.push(dependency);
      }
    }
    if (Array.isArray(payload.excludedAttachments) && payload.excludedAttachments.length)
      warnings.push(ATTACHMENT_WARNING);
    warnings.push(
      'Импорт создаёт копии с новыми ID и историей изменений; исходная база, учётные записи и аудит не восстановлены.',
    );
    hooks?.after?.(state, unique(warnings));
    return {
      after: {
        format,
        importedIds: created.map((entity) => entity.id),
        count: created.length,
        warnings: unique(warnings),
      },
    };
  });
  return { ...result, importWarnings: unique(warnings) };
}

function csvEscape(value: string): string {
  if (/^[=+@-]/.test(value)) value = `'${value}`;
  return `"${value.replace(/"/g, '""')}"`;
}
function metadata(
  snapshot: PlannerSnapshot,
  workspaceId: string,
  entities: Entity[],
  warnings: string[],
) {
  return {
    schemaVersion: 3,
    exportedAt: new Date().toISOString(),
    attachmentPolicy: ATTACHMENT_WARNING,
    warnings,
    excludedAttachments: entities.flatMap((entity) =>
      entity.links
        .filter((link) => link.kind === 'file')
        .map((link) => ({ entityId: entity.id, label: link.label })),
    ),
    workspace: snapshot.workspaces.find((workspace) => workspace.id === workspaceId),
    dependencies: snapshot.dependencies.filter((dep) => dep.workspaceId === workspaceId),
    types: snapshot.types.filter((type) => !type.workspaceId || type.workspaceId === workspaceId),
    resources: snapshot.resources.filter((resource) => resource.workspaceId === workspaceId),
  };
}
function portableEntity(entity: Entity): Entity {
  return { ...entity, links: entity.links.filter((link) => link.kind !== 'file') };
}
function foldIcs(line: string): string {
  const chunks: string[] = [];
  let chunk = '';
  for (const character of line) {
    if (Buffer.byteLength(chunk + character, 'utf8') > 75) {
      chunks.push(chunk);
      chunk = ' ' + character;
    } else chunk += character;
  }
  chunks.push(chunk);
  return chunks.join('\r\n');
}
function exportDate(value: string, timezone: string): DateTime {
  const fraction = value.match(/[.,](\d+)(?:Z|[+-]\d{2}:?\d{2})$/i)?.[1];
  const date = parseTime(value, timezone);
  if (!date || date.millisecond || (fraction && /[1-9]/.test(fraction)))
    throw new ApiError(
      400,
      'ICS не может точно сохранить эту дату или доли секунды. Используйте JSON.',
      'ics_projection_unsupported',
    );
  return date;
}

const hasNonZeroSubsecond = (value: unknown): value is string =>
  typeof value === 'string' &&
  /T\d{2}:?\d{2}(?::?\d{2})?[.,]\d*[1-9]\d*(?:Z|[+-]\d{2}:?\d{2})?$/i.test(value);

function assertIcsSecondPrecision(snapshot: PlannerSnapshot, entity: Entity): void {
  const scheduleValues = [
    entity.plan.start,
    entity.plan.end,
    entity.plan.earliest,
    entity.plan.latest,
    entity.recurrence?.until,
    ...(entity.recurrence?.exceptions ?? []),
  ];
  if (scheduleValues.some(hasNonZeroSubsecond))
    throw new ApiError(
      400,
      `ICS не может точно сохранить дробные секунды плана или повторения объекта «${entity.title}». Используйте JSON.`,
      'ics_projection_unsupported',
    );
  const type = snapshot.types.find((candidate) => candidate.id === entity.typeId);
  for (const field of type?.fields.filter((candidate) => candidate.type === 'date') ?? []) {
    const value = entity.fields[field.id];
    if (hasNonZeroSubsecond(value))
      throw new ApiError(
        400,
        `ICS не может точно сохранить наносекундное значение поля «${field.label}» объекта «${entity.title}». Используйте JSON.`,
        'ics_projection_unsupported',
      );
  }
}
function icsDateLine(name: string, value: string, range: TimeRange): string {
  const date = exportDate(value, range.timezone);
  if (range.precision === 'day') {
    if (date.hour || date.minute || date.second)
      throw new ApiError(
        400,
        'ICS: период с точностью до дня должен начинаться и заканчиваться в полночь. Используйте JSON.',
        'ics_projection_unsupported',
      );
    return `${name};VALUE=DATE:${date.toFormat('yyyyMMdd')}`;
  }
  if (date.offset === 0 && ['UTC', 'Etc/UTC', 'Etc/GMT', 'GMT'].includes(range.timezone))
    return `${name}:${date.toUTC().toFormat("yyyyMMdd'T'HHmmss'Z'")}`;
  const first = date.getPossibleOffsets().sort((a, b) => a.toMillis() - b.toMillis())[0];
  if (first && first.toMillis() !== date.toMillis())
    throw new ApiError(
      400,
      'ICS с TZID выбирает первое вхождение повторённого местного времени; это значение относится ко второму. Используйте JSON.',
      'ics_projection_unsupported',
    );
  return `${name};TZID=${range.timezone}:${date.toFormat("yyyyMMdd'T'HHmmss")}`;
}

export function exportContent(
  snapshot: PlannerSnapshot,
  workspaceId: string,
  format: string,
): { content: string; mime: string; extension: string; warnings: string[] } {
  if (!snapshot.workspaces.some((workspace) => workspace.id === workspaceId))
    throw new ApiError(403, 'Пространство недоступно');
  const entities = snapshot.entities.filter((entity) => entity.workspaceId === workspaceId),
    warnings: string[] = [];
  if (entities.some((entity) => entity.links.some((link) => link.kind === 'file')))
    warnings.push(ATTACHMENT_WARNING);
  if (format === 'json')
    return {
      content: JSON.stringify(
        {
          ...metadata(snapshot, workspaceId, entities, warnings),
          entities: entities.map(portableEntity),
        },
        null,
        2,
      ),
      mime: 'application/json; charset=utf-8',
      extension: 'json',
      warnings,
    };
  if (format === 'csv') {
    warnings.push(
      'Полная модель хранится в колонках entityData и workspaceData. Сохраните эти колонки для обратного импорта; читаемые колонки можно редактировать.',
    );
    if (entities.some((entity) => entity.plan.precise))
      warnings.push(
        'Точные координаты сохраняются как decimal strings в precise и entityData. Обычные start/end содержат только доступное календарное зеркало; не открывайте и не сохраняйте precise как число в табличном редакторе.',
      );
    const headers = [
      'title',
      'typeId',
      'kind',
      'start',
      'end',
      'dueAt',
      'status',
      'description',
      'tags',
      'timezone',
      'precise',
      'escapedFields',
      'formatVersion',
      'entityData',
      'workspaceData',
      'exportWarnings',
    ];
    const rows = entities.map((entity, index) => {
      const values = [
        entity.title,
        entity.typeId,
        entity.kind,
        entity.plan.start ?? '',
        entity.plan.end ?? '',
        entity.dueAt ?? '',
        entity.status,
        entity.description,
        entity.tags.join('|'),
        entity.plan.timezone,
        entity.plan.precise ? JSON.stringify(entity.plan.precise) : '',
      ];
      const escaped = values
        .flatMap((value, i) => (/^[=+@-]/.test(value) ? [headers[i]] : []))
        .join('|');
      return [
        ...values,
        escaped,
        CSV_VERSION,
        JSON.stringify(portableEntity(entity)),
        index === 0 ? JSON.stringify(metadata(snapshot, workspaceId, entities, warnings)) : '',
        index === 0 ? warnings.join('\n') : '',
      ]
        .map(csvEscape)
        .join(',');
    });
    return {
      content: '\uFEFF' + [headers.join(','), ...rows].join('\r\n'),
      mime: 'text/csv; charset=utf-8',
      extension: 'csv',
      warnings,
    };
  }
  if (format === 'ics') {
    warnings.push(ICS_PROJECTION_WARNING);
    for (const entity of entities) assertIcsSecondPrecision(snapshot, entity);
    const precise = entities.filter((entity) => entity.plan.precise);
    if (precise.length)
      throw new ApiError(
        400,
        `ICS не сохраняет точные наносекундные координаты (${precise.length} объектов). Используйте JSON или полный CSV v3.`,
        'ics_projection_unsupported',
      );
    const undated = entities.filter((entity) => !entity.plan.start);
    if (undated.length)
      warnings.push(`ICS: ${undated.length} объектов без даты исключены. Сохраните их в JSON.`);
    const dated = entities.filter((entity) => entity.plan.start);
    if (
      dated.some(
        (entity) =>
          entity.recurrence &&
          entity.plan.precision !== 'day' &&
          !['UTC', 'Etc/UTC', 'Etc/GMT', 'GMT'].includes(entity.plan.timezone),
      )
    )
      warnings.push(
        'Повторения с TZID используют правила IANA принимающего календаря. Встроенные определения VTIMEZONE не включены.',
      );
    const lines = [
      'BEGIN:VCALENDAR',
      'VERSION:2.0',
      'PRODID:-//Universal Planner//Timeline//RU',
      'CALSCALE:GREGORIAN',
      'X-PLANNER-PROJECTION:CALENDAR-PLAN',
    ];
    for (const warning of warnings) lines.push(`X-PLANNER-WARNING:${escapeIcs(warning)}`);
    for (const entity of dated) {
      const range = entity.plan;
      if (['unknown', 'month', 'approximate'].includes(range.precision))
        throw new ApiError(
          400,
          `ICS: «${entity.title}» имеет неопределённое время, которое нельзя точно передать календарём. Используйте JSON.`,
          'ics_projection_unsupported',
        );
      if (entity.recurrence && entity.recurrence.calendarPolicy !== 'skip-invalid')
        throw new ApiError(
          400,
          `ICS: повтор «${entity.title}» корректирует невозможные даты. RFC 5545 пропускает их; используйте JSON для сохранения смысла серии.`,
          'ics_projection_unsupported',
        );
      if (entity.recurrence?.count && entity.recurrence.until)
        throw new ApiError(
          400,
          'ICS не поддерживает одновременные COUNT и UNTIL. Используйте JSON.',
          'ics_projection_unsupported',
        );
      lines.push(
        'BEGIN:VEVENT',
        `UID:${entity.id}@universal-planner`,
        `DTSTAMP:${DateTime.utc().toFormat("yyyyMMdd'T'HHmmss'Z'")}`,
        icsDateLine('DTSTART', range.start!, range),
        `SUMMARY:${escapeIcs(entity.title)}`,
        `DESCRIPTION:${escapeIcs(entity.description)}`,
      );
      if (range.end) {
        if (
          (parseTime(range.end, range.timezone)?.toMillis() ?? 0) >
          (parseTime(range.start, range.timezone)?.toMillis() ?? 0)
        ) {
          if (
            entity.recurrence &&
            range.precision !== 'day' &&
            entity.recurrence.durationPolicy !== 'elapsed'
          ) {
            const start = exportDate(range.start!, range.timezone),
              end = exportDate(range.end, range.timezone);
            lines.push(
              `DURATION:${Duration.fromObject(end.diff(start, ['days', 'milliseconds']).toObject())
                .shiftTo('days', 'hours', 'minutes', 'seconds')
                .toISO()}`,
            );
          } else lines.push(icsDateLine('DTEND', range.end, range));
        } else if (range.precision === 'day')
          throw new ApiError(
            400,
            'ICS не может сохранить нулевой период с точностью до дня. Используйте JSON.',
            'ics_projection_unsupported',
          );
      } else if (range.precision === 'day')
        throw new ApiError(
          400,
          'ICS событие на весь день предполагает период минимум в один день. Укажите окончание или используйте JSON.',
          'ics_projection_unsupported',
        );
      if (entity.status === 'cancelled') lines.push('STATUS:CANCELLED');
      else if (entity.status === 'draft') lines.push('STATUS:TENTATIVE');
      else lines.push('STATUS:CONFIRMED');
      if (entity.tags.length) lines.push(`CATEGORIES:${entity.tags.map(escapeIcs).join(',')}`);
      const webLink = entity.links.find((link) => link.kind === 'url');
      if (webLink) {
        if (/[\r\n\0]/.test(webLink.url))
          throw new ApiError(
            400,
            'ICS: веб-ссылка содержит управляющие символы. Исправьте ссылку или используйте JSON.',
            'ics_projection_unsupported',
          );
        lines.push(`URL:${webLink.url}`);
      }
      if (entity.recurrence) {
        const rule = entity.recurrence;
        let rr = `FREQ=${({ day: 'DAILY', week: 'WEEKLY', month: 'MONTHLY' } as const)[rule.frequency]};INTERVAL=${rule.interval}`;
        if (rule.count) rr += `;COUNT=${rule.count}`;
        if (rule.until) {
          const until = parseTime(rule.until, range.timezone);
          if (!until) throw new ApiError(400, 'Некорректное окончание повторения');
          if (range.precision === 'day') {
            if (
              rule.until.length !== 10 &&
              (until.hour || until.minute || until.second || until.millisecond)
            )
              throw new ApiError(
                400,
                'ICS не может сохранить время UNTIL в серии на весь день. Используйте JSON.',
                'ics_projection_unsupported',
              );
            rr += `;UNTIL=${until.toFormat('yyyyMMdd')}`;
          } else
            rr += `;UNTIL=${(rule.until.length === 10 ? until.endOf('day') : exportDate(rule.until, range.timezone)).toUTC().toFormat("yyyyMMdd'T'HHmmss'Z'")}`;
        }
        if (rule.weekdays?.length) {
          const base = parseTime(range.start, range.timezone)!;
          if (rule.frequency !== 'week' || !rule.weekdays.includes(base.weekday))
            throw new ApiError(
              400,
              'ICS: DTSTART должен входить в недельный BYDAY. Используйте JSON.',
              'ics_projection_unsupported',
            );
          rr += `;BYDAY=${rule.weekdays.map((day) => DAYS[day - 1]).join(',')};WKST=MO`;
        }
        lines.push(`RRULE:${rr}`);
        for (const exception of rule.exceptions) {
          if (range.precision === 'day') {
            const local = parseTime(exception, range.timezone);
            if (!local || (exception.length !== 10 && (local.hour || local.minute || local.second)))
              throw new ApiError(
                400,
                'ICS: EXDATE не соответствует серии на весь день. Используйте JSON.',
                'ics_projection_unsupported',
              );
            lines.push(icsDateLine('EXDATE', local.toISODate()!, range));
          } else if (exception.length === 10) {
            const base = parseTime(range.start, range.timezone)!,
              day = parseTime(exception, range.timezone)!.set({
                hour: base.hour,
                minute: base.minute,
                second: base.second,
              });
            lines.push(icsDateLine('EXDATE', day.toISO()!, range));
          } else lines.push(icsDateLine('EXDATE', exception, range));
        }
      }
      lines.push('END:VEVENT');
    }
    lines.push('END:VCALENDAR');
    return {
      content: lines.map(foldIcs).join('\r\n') + '\r\n',
      mime: 'text/calendar; charset=utf-8',
      extension: 'ics',
      warnings: unique(warnings),
    };
  }
  throw new ApiError(400, 'Неизвестный формат экспорта');
}
