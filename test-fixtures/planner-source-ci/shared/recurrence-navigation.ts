import { DateTime } from 'luxon';
import { expandRecurrence, MAX_RECURRENCE_OCCURRENCES, parseTime } from './engine.js';
import type { Entity, Occurrence } from './types.js';
import { hasSubMillisecondTime, isoToNs, nsToISO, NS_PER_MILLISECOND } from './precise-time.js';

export type OccurrenceSearch = {
  occurrence: Occurrence | null;
  status: 'found' | 'end' | 'limited' | 'invalid';
};
export type RecurrenceNavigation = {
  current: OccurrenceSearch;
  previous: OccurrenceSearch;
  next: OccurrenceSearch;
};
export const MAX_NAVIGATION_STEPS = 4096;
const CHUNK_GROUPS = 32;
const MAX_CHUNKS = 128;
const CALENDAR_END = DateTime.utc(9999, 12, 31, 23, 59, 59, 999).toMillis();
const result = (status: OccurrenceSearch['status'], occurrence: Occurrence | null = null) => ({
  status,
  occurrence,
});

/** Adjacent actual instances, using the engine's COUNT, exception and calendar policies. */
export function recurrenceNavigation(
  entity: Entity,
  selectedStart?: string | null,
): RecurrenceNavigation {
  const rule = entity.recurrence,
    base = parseTime(entity.plan.start, entity.plan.timezone),
    baseEnd = parseTime(entity.plan.end, entity.plan.timezone);
  const baseNs = isoToNs(entity.plan.start, entity.plan.timezone),
    baseEndNs = isoToNs(entity.plan.end, entity.plan.timezone);
  const invalid = () => ({
    current: result('invalid'),
    previous: result('invalid'),
    next: result('invalid'),
  });
  if (
    !rule ||
    !base ||
    entity.plan.precise ||
    baseNs === null ||
    !Number.isInteger(rule.interval) ||
    rule.interval < 1 ||
    rule.interval > 365 ||
    !['day', 'week', 'month'].includes(rule.frequency) ||
    (rule.count != null && (!Number.isInteger(rule.count) || rule.count < 1)) ||
    (entity.plan.end != null && (!baseEnd || baseEndNs === null || baseEndNs < baseNs))
  )
    return invalid();
  const weekdays = [...new Set(rule.weekdays?.length ? rule.weekdays : [base.weekday])]
    .filter((day) => Number.isInteger(day) && day >= 1 && day <= 7)
    .sort((a, b) => a - b);
  if (rule.frequency === 'week' && !weekdays.length) return invalid();
  const unit = rule.frequency === 'day' ? 'days' : rule.frequency === 'week' ? 'weeks' : 'months';
  const weekBase = base.startOf('week');
  const calendarBase = rule.frequency === 'week' ? weekBase : base;
  const factor = rule.frequency === 'week' ? weekdays.length : 1;
  const skipInvalid = rule.calendarPolicy === 'skip-invalid';
  let hardEnd = CALENDAR_END;
  if (rule.until) {
    const until = parseTime(rule.until, entity.plan.timezone);
    if (!until) return invalid();
    hardEnd = Math.min(hardEnd, (rule.until.length === 10 ? until.endOf('day') : until).toMillis());
  }
  // Native COUNT is arithmetic. Skip-invalid COUNT consumes only valid local dates;
  // its end is deliberately not guessed from nominal calendar positions.
  if (rule.count && !skipInvalid) {
    let last: DateTime;
    if (rule.frequency === 'week') {
      const first = weekdays.filter((day) => day >= base.weekday);
      if (rule.count <= first.length)
        last = weekBase.plus({ days: first[rule.count - 1] - 1 }).set({
          hour: base.hour,
          minute: base.minute,
          second: base.second,
          millisecond: base.millisecond,
        });
      else {
        const offset = rule.count - first.length - 1;
        last = weekBase
          .plus({ weeks: (Math.floor(offset / weekdays.length) + 1) * rule.interval })
          .plus({ days: weekdays[offset % weekdays.length] - 1 })
          .set({
            hour: base.hour,
            minute: base.minute,
            second: base.second,
            millisecond: base.millisecond,
          });
      }
    } else last = base.plus({ [unit]: (rule.count - 1) * rule.interval });
    if (last.isValid) hardEnd = Math.min(hardEnd, last.toMillis());
  }
  // Search starts independently of overlapping durations, so a long period cannot
  // fill the engine's result cap with thousands of older overlapping instances.
  const startsOnly: Entity = { ...entity, plan: { ...entity.plan, end: null } };
  const duration = baseEnd
    ? rule.durationPolicy === 'elapsed'
      ? { milliseconds: baseEnd.toMillis() - base.toMillis() }
      : baseEnd.diff(base, ['days', 'milliseconds']).toObject()
    : null;
  const endSubMs =
    baseEnd && baseEndNs !== null
      ? baseEndNs - BigInt(baseEnd.toMillis()) * NS_PER_MILLISECOND
      : 0n;
  const preciseEnd = hasSubMillisecondTime({
    ...entity.plan,
    start: entity.plan.end,
    end: null,
    earliest: null,
    latest: null,
  });
  const hydrate = (occurrence: Occurrence): Occurrence => {
    if (!duration) return { ...occurrence, end: null };
    const end = parseTime(occurrence.start, entity.plan.timezone)!.plus(duration);
    // Luxon calculates calendar/DST duration; restore the recorded fraction below its ms grid.
    const endNs = BigInt(end.toMillis()) * NS_PER_MILLISECOND + endSubMs;
    return {
      ...occurrence,
      end:
        preciseEnd || !Number.isInteger(end.offset)
          ? nsToISO(endNs, entity.plan.timezone)
          : end.toISO(),
    };
  };
  let remaining = MAX_NAVIGATION_STEPS;
  let lastWindow: Occurrence[] = [];
  function expand(from: DateTime, to: DateTime): OccurrenceSearch | Occurrence[] {
    const groups = skipInvalid
      ? Math.max(0, to.diff(calendarBase, unit).get(unit) / rule!.interval)
      : Math.max(0, to.diff(from, unit).get(unit) / rule!.interval);
    const cost = (Math.ceil(groups) + 3) * factor;
    if (!Number.isFinite(cost) || cost > remaining) return result('limited');
    remaining -= cost;
    try {
      const values = expandRecurrence(startsOnly, from.toISO()!, to.toISO()!);
      if (values.length >= MAX_RECURRENCE_OCCURRENCES) return result('limited');
      lastWindow = values;
      return values;
    } catch {
      return result('invalid');
    }
  }
  function search(reference: DateTime, direction: -1 | 1, inclusive = false): OccurrenceSearch {
    let cursor = reference.toMillis() + (direction === 1 && !inclusive ? 1 : 0);
    for (let chunk = 0; chunk < MAX_CHUNKS; chunk++) {
      if ((direction === -1 && cursor <= base!.toMillis()) || (direction === 1 && cursor > hardEnd))
        return result('end');
      const at = DateTime.fromMillis(cursor, { zone: entity.plan.timezone });
      const next = at.plus({ [unit]: CHUNK_GROUPS * rule!.interval * direction });
      if (!next.isValid) return result('limited');
      const from = DateTime.fromMillis(
        Math.max(base!.toMillis(), direction === 1 ? cursor : next.toMillis()),
        { zone: entity.plan.timezone },
      );
      const to = DateTime.fromMillis(
        Math.min(hardEnd + 1, direction === 1 ? next.toMillis() : cursor),
        { zone: entity.plan.timezone },
      );
      if (to.toMillis() <= from.toMillis()) return result('end');
      const values = expand(from, to);
      if (!Array.isArray(values)) return values;
      const occurrence = direction === 1 ? values[0] : values.at(-1);
      if (occurrence) return result('found', hydrate(occurrence));
      cursor = direction === 1 ? to.toMillis() : from.toMillis();
    }
    return result('limited');
  }
  let current: OccurrenceSearch;
  if (selectedStart) {
    const at = parseTime(selectedStart, entity.plan.timezone);
    const selectedNs = isoToNs(selectedStart, entity.plan.timezone);
    if (!at || selectedNs === null || selectedNs < baseNs || at.toMillis() > hardEnd)
      return invalid();
    const from = DateTime.fromMillis(
      Math.max(base.toMillis(), at.minus({ [unit]: CHUNK_GROUPS * rule.interval }).toMillis()),
      { zone: entity.plan.timezone },
    );
    const to = DateTime.fromMillis(
      Math.min(hardEnd + 1, at.plus({ [unit]: CHUNK_GROUPS * rule.interval }).toMillis()),
      { zone: entity.plan.timezone },
    );
    const values = expand(from, to);
    if (!Array.isArray(values)) current = values;
    else {
      const selected = values.find(
        (value) => isoToNs(value.start, entity.plan.timezone) === selectedNs,
      );
      current = selected ? result('found', hydrate(selected)) : result('invalid');
    }
  } else current = search(base, 1, true);
  if (!current.occurrence)
    return { current, previous: result(current.status), next: result(current.status) };
  const at = parseTime(current.occurrence.start, entity.plan.timezone)!;
  const atNs = isoToNs(current.occurrence.start, entity.plan.timezone)!;
  const neighbors = lastWindow;
  const prior = neighbors
    .filter((value) => isoToNs(value.start, entity.plan.timezone)! < atNs)
    .at(-1);
  const following = neighbors.find((value) => isoToNs(value.start, entity.plan.timezone)! > atNs);
  const previous = prior ? result('found', hydrate(prior)) : search(at, -1);
  const next = following
    ? result('found', hydrate(following))
    : rule.count && current.occurrence.index >= rule.count - 1
      ? result('end')
      : search(at, 1);
  return { current, previous, next };
}
