import { DateTime } from 'luxon';
import { displayRangeEnd } from '../shared/local-range';
import {
  canonicalRange,
  isoToNs,
  NS_PER_SECOND,
  nsToISO,
  rangeFromNs,
} from '../shared/precise-time';
import { instantLabel } from '../shared/universal-timeline';
import type {
  Entity,
  EntityDraft,
  EntityKind,
  PlannerSnapshot,
  Signal,
  TimeRange,
} from '../shared/types';

export const kindLabels: Record<EntityKind, string> = {
  point: 'Событие',
  period: 'Период',
  process: 'Процесс',
  note: 'Заметка',
  metric: 'Показатель',
};
export const statusLabels = {
  draft: 'Черновик',
  planned: 'Запланировано',
  active: 'В процессе',
  done: 'Готово',
  cancelled: 'Отменено',
};
export const roleLabels = {
  owner: 'Владелец',
  editor: 'Редактор',
  approver: 'Согласующий',
  viewer: 'Читатель',
};

export function requiresAttention(signal: Signal) {
  return (
    (signal.state === 'open' || signal.state === 'acknowledged') &&
    !(signal.kind === 'missing-date' && signal.severity === 'info')
  );
}

export function dateLabel(iso?: string | null, zone = 'Asia/Yekaterinburg', time = false) {
  if (!iso) return 'Время не задано';
  const d = DateTime.fromISO(iso, { zone }).setLocale('ru');
  return d.isValid ? d.toFormat(time ? 'd MMM, HH:mm' : 'd MMM yyyy') : 'Некорректная дата';
}
export function rangeLabel(range: TimeRange | null) {
  if (range?.precise) {
    const coordinates = canonicalRange(range);
    if (coordinates.start === null)
      return coordinates.end === null
        ? 'Время не задано'
        : `До ${instantLabel(coordinates.end, range.timezone, true)}`;
    const first = instantLabel(coordinates.start, range.timezone, true);
    return coordinates.end !== null && coordinates.end !== coordinates.start
      ? `${first} — ${instantLabel(coordinates.end, range.timezone, true)}`
      : first;
  }
  if (!range?.start)
    return range?.end ? `До ${dateLabel(range.end, range.timezone)}` : 'Время не задано';
  const first = dateLabel(range.start, range.timezone);
  return range.end && range.end !== range.start
    ? `${first} — ${dateLabel(displayRangeEnd(range), range.timezone)}`
    : first;
}
export function withPreciseCoordinates(range: TimeRange): TimeRange {
  const coordinates = canonicalRange(range);
  const exact = rangeFromNs(coordinates.start, coordinates.end, range.timezone, range.precision);
  if (range.earliest !== undefined || range.precise?.earliest !== undefined)
    exact.precise!.earliest = coordinates.earliest?.toString() ?? null;
  if (range.latest !== undefined || range.precise?.latest !== undefined)
    exact.precise!.latest = coordinates.latest?.toString() ?? null;
  if (range.precise?.resolutionNs !== undefined)
    exact.precise!.resolutionNs = range.precise.resolutionNs;
  return exact;
}
export function calendarRangeRepresentable(range: TimeRange): boolean {
  const coordinates = canonicalRange(range);
  return [coordinates.start, coordinates.end, coordinates.earliest, coordinates.latest].every(
    (coordinate) => coordinate === null || nsToISO(coordinate, range.timezone) !== null,
  );
}
export function withCalendarCoordinates(range: TimeRange): TimeRange {
  const coordinates = canonicalRange(range);
  if (!calendarRangeRepresentable(range))
    throw new Error('Это время нельзя сохранить как календарную дату в диапазоне лет 0001–9999.');
  const precise = range.precise;
  const { precise: _precise, ...calendar } = range;
  return {
    ...calendar,
    start: coordinates.start === null ? null : nsToISO(coordinates.start, range.timezone),
    end: coordinates.end === null ? null : nsToISO(coordinates.end, range.timezone),
    ...(precise?.earliest !== undefined
      ? {
          earliest:
            coordinates.earliest === null ? null : nsToISO(coordinates.earliest, range.timezone),
        }
      : {}),
    ...(precise?.latest !== undefined
      ? { latest: coordinates.latest === null ? null : nsToISO(coordinates.latest, range.timezone) }
      : {}),
    precision:
      range.precision === 'unknown' && (coordinates.start !== null || coordinates.end !== null)
        ? 'exact'
        : range.precision,
  };
}
export function inputDate(iso?: string | null, zone = 'Asia/Yekaterinburg') {
  if (!iso) return '';
  const date = DateTime.fromISO(iso, { zone });
  if (!date.isValid) return '';
  const coordinate = isoToNs(iso, zone);
  const fraction =
    coordinate === null ? 0n : ((coordinate % NS_PER_SECOND) + NS_PER_SECOND) % NS_PER_SECOND;
  const showSeconds = date.second !== 0 || fraction !== 0n;
  const base = date.toFormat(`yyyy-MM-dd'T'HH:mm${showSeconds ? ':ss' : ''}`);
  return fraction ? `${base}.${fraction.toString().padStart(9, '0').replace(/0+$/, '')}` : base;
}
export function fromInput(value: string, zone = 'Asia/Yekaterinburg') {
  if (!value) return null;
  const coordinate = isoToNs(value, zone);
  return coordinate === null ? null : nsToISO(coordinate, zone);
}
export function emptyRange(zone = 'Asia/Yekaterinburg'): TimeRange {
  return { start: null, end: null, timezone: zone, precision: 'unknown' };
}
export function draftOf(entity: Entity): EntityDraft {
  const {
    id: _id,
    version: _version,
    createdAt: _created,
    updatedAt: _updated,
    baseline: _baseline,
    source: _source,
    ...draft
  } = entity;
  return structuredClone(draft);
}
export function newDraft(
  snapshot: PlannerSnapshot,
  workspaceId: string,
  anchor: DateTime,
): EntityDraft {
  const workspace = snapshot.workspaces.find((w) => w.id === workspaceId) ?? snapshot.workspaces[0];
  const type =
    snapshot.types.find(
      (t) => t.kind === 'point' && (!t.workspaceId || t.workspaceId === workspace.id),
    ) ?? snapshot.types[0];
  const start = anchor.setZone(workspace.timezone);
  return {
    workspaceId: workspace.id,
    typeId: type.id,
    kind: type.kind,
    title: '',
    description: '',
    parentId: null,
    ownerId: snapshot.user.id,
    participantIds: [],
    status: 'planned',
    plan: { start: start.toISO(), end: null, timezone: workspace.timezone, precision: 'exact' },
    actual: null,
    forecast: null,
    dueAt: null,
    tags: [],
    fields: {},
    links: [],
    allocations: [],
    recurrence: null,
  };
}
export function canEdit(snapshot: PlannerSnapshot, workspaceId: string) {
  const role = snapshot.memberships.find(
    (m) => m.workspaceId === workspaceId && m.userId === snapshot.user.id,
  )?.role;
  return role === 'owner' || role === 'editor';
}
export function canApprove(snapshot: PlannerSnapshot, workspaceId: string) {
  const role = snapshot.memberships.find(
    (m) => m.workspaceId === workspaceId && m.userId === snapshot.user.id,
  )?.role;
  return role === 'owner' || role === 'approver';
}
export function uid() {
  return crypto.randomUUID();
}
