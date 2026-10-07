import { useRef, useState, type FormEvent } from 'react';
import { Check, Clock3, LoaderCircle, SlidersHorizontal } from 'lucide-react';
import type { Entity, EntityDraft, FieldValue, PlannerSnapshot } from '../../shared/types';
import {
  canEdit,
  emptyRange,
  rangeLabel,
  withCalendarCoordinates,
  withPreciseCoordinates,
} from '../utils';
import {
  canonicalRange,
  hasSubMillisecondTime,
  nsToISO,
  preciseRangeErrors,
} from '../../shared/precise-time';
import {
  changeRangePrecision,
  displayRangeEnd,
  localRangeInput,
  withLocalRangeInput,
} from '../../shared/local-range';
import Modal from './Modal';
import PreciseRangeFields, { type TimeContext } from './PreciseRangeFields';

type Props = {
  snapshot: PlannerSnapshot;
  initial: EntityDraft;
  entity?: Entity;
  busy: boolean;
  onClose: () => void;
  onSave: (draft: EntityDraft, proposal: boolean, reason: string) => Promise<void>;
  onExpand: (draft: EntityDraft) => void;
  timeContext?: TimeContext;
  preferPrecise?: boolean;
};

export default function QuickEditor({
  snapshot,
  initial,
  entity,
  busy,
  onClose,
  onSave,
  onExpand,
  timeContext,
  preferPrecise = false,
}: Props) {
  const [draft, setDraft] = useState(initial);
  const [error, setError] = useState('');
  const [invalidPreciseInput, setInvalidPreciseInput] = useState(false);
  const [subMillisecondCalendar] = useState(
    () => !initial.plan.precise && hasSubMillisecondTime(initial.plan),
  );
  const pending = useRef(false);
  const [showTime, setShowTime] = useState(
    !!(initial.plan.start || initial.plan.end || initial.plan.precise),
  );
  const [allDay, setAllDay] = useState(
    (!initial.plan.precise && initial.plan.precision === 'day') ||
      (!initial.plan.start && !initial.plan.end && initial.kind === 'period'),
  );
  const [reason, setReason] = useState('');
  const planChanged = !!entity && JSON.stringify(draft.plan) !== JSON.stringify(entity.plan);
  const needsReason = planChanged && entity?.status === 'done';
  const proposal =
    planChanged && snapshot.workspaces.find((w) => w.id === draft.workspaceId)?.mode === 'team';
  const saveLabel = entity ? (proposal ? 'Проверить изменение' : 'Сохранить') : 'Добавить на шкалу';
  const workspaces = snapshot.workspaces.filter((w) => canEdit(snapshot, w.id));
  const types = snapshot.types.filter((t) => !t.workspaceId || t.workspaceId === draft.workspaceId);
  const type = types.find((t) => t.id === draft.typeId);
  const preciseCoordinates = canonicalRange(draft.plan);
  const unknown = preciseCoordinates.start === null && preciseCoordinates.end === null;
  const calendarRepresentable = [
    preciseCoordinates.start,
    preciseCoordinates.end,
    preciseCoordinates.earliest,
    preciseCoordinates.latest,
  ].every((coordinate) => coordinate === null || nsToISO(coordinate, draft.plan.timezone) !== null);
  const fields = type?.fields.filter((f) => f.required || draft.kind === 'metric') ?? [];
  const updateField = (id: string, value: FieldValue) =>
    setDraft((d) => ({ ...d, fields: { ...d.fields, [id]: value } }));
  function updateTime(key: 'start' | 'end', value: string) {
    try {
      setDraft({ ...draft, plan: withLocalRangeInput(draft.plan, key, value, allDay) });
      setError('');
    } catch (e) {
      setError((e as Error).message);
    }
  }
  function toggleDate() {
    if (showTime) {
      setDraft((d) => ({ ...d, plan: emptyRange(d.plan.timezone) }));
    } else if (preferPrecise && unknown) {
      setDraft((d) => ({ ...d, plan: withPreciseCoordinates(emptyRange(d.plan.timezone)) }));
    }
    setInvalidPreciseInput(false);
    setShowTime(!showTime);
  }
  function setPreciseMode(precise: boolean) {
    setDraft((current) => {
      const calendar = allDay ? changeRangePrecision(current.plan, false) : current.plan;
      return {
        ...current,
        plan: precise ? withPreciseCoordinates(calendar) : withCalendarCoordinates(calendar),
      };
    });
    setAllDay(false);
    setInvalidPreciseInput(false);
    setError('');
  }
  async function submit(event: FormEvent) {
    event.preventDefault();
    if (pending.current || busy || !draft.title.trim()) return;
    if (invalidPreciseInput) {
      setError('Исправьте формат точного времени перед сохранением.');
      return;
    }
    const timeErrors = preciseRangeErrors(draft.plan);
    if (timeErrors.length) {
      setError(timeErrors[0]!);
      return;
    }
    const missing = fields.find(
      (f) => f.required && (draft.fields[f.id] == null || draft.fields[f.id] === ''),
    );
    if (missing) {
      setError(`Заполните «${missing.label}».`);
      return;
    }
    pending.current = true;
    setError('');
    try {
      await onSave({ ...draft, title: draft.title.trim() }, proposal, reason);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      pending.current = false;
    }
  }
  return (
    <Modal
      title={
        entity ? (entity.recurrence ? 'Изменить серию' : 'Изменить объект') : 'Добавить на шкалу'
      }
      onClose={onClose}
      compact
    >
      <form className="quick-editor" onSubmit={submit}>
        <div className="quick-title-row">
          <input
            autoFocus
            aria-label="Название"
            placeholder={entity ? 'Название' : 'Что добавить?'}
            required
            maxLength={300}
            value={draft.title}
            onChange={(e) => setDraft((d) => ({ ...d, title: e.target.value }))}
          />
          <button
            type="submit"
            className="quick-save"
            aria-label={saveLabel}
            title={`${saveLabel} · Enter`}
            disabled={busy || !draft.title.trim() || invalidPreciseInput}
          >
            {busy ? <LoaderCircle size={20} className="spin" /> : <Check size={21} />}
          </button>
        </div>
        <div className="quick-meta-row">
          <select
            aria-label="Тип объекта"
            value={draft.typeId}
            onChange={(e) => {
              const next = types.find((t) => t.id === e.target.value);
              if (next) {
                setDraft((d) => ({ ...d, typeId: next.id, kind: next.kind }));
                if (unknown) setAllDay(next.kind === 'period');
              }
            }}
          >
            {types.map((t) => (
              <option value={t.id} key={t.id}>
                {t.label}
              </option>
            ))}
          </select>
          {!entity && workspaces.length > 1 && (
            <select
              aria-label="Пространство"
              value={draft.workspaceId}
              onChange={(e) => {
                const workspace = workspaces.find((w) => w.id === e.target.value)!;
                setDraft((d) => {
                  const currentType = snapshot.types.find((t) => t.id === d.typeId);
                  const nextType =
                    currentType &&
                    (!currentType.workspaceId || currentType.workspaceId === workspace.id)
                      ? currentType
                      : (snapshot.types.find(
                          (t) =>
                            (!t.workspaceId || t.workspaceId === workspace.id) && t.kind === d.kind,
                        ) ??
                        snapshot.types.find(
                          (t) => !t.workspaceId || t.workspaceId === workspace.id,
                        )!);
                  return {
                    ...d,
                    workspaceId: workspace.id,
                    typeId: nextType.id,
                    kind: nextType.kind,
                    plan: { ...d.plan, timezone: workspace.timezone },
                  };
                });
              }}
            >
              {workspaces.map((w) => (
                <option value={w.id} key={w.id}>
                  {w.name}
                </option>
              ))}
            </select>
          )}
          <button
            type="button"
            className={'quick-date-toggle ' + (unknown ? 'is-unknown' : '')}
            aria-label={showTime ? 'Оставить без даты' : 'Задать дату'}
            aria-expanded={showTime}
            title={showTime ? 'Без даты' : 'Задать дату'}
            onClick={toggleDate}
          >
            <Clock3 size={17} />
            <span>{showTime ? 'Без даты' : 'Дата'}</span>
          </button>
          <button
            type="button"
            className="quick-expand"
            aria-label="Все поля и повторения"
            title="Все поля и повторения"
            onClick={() => onExpand(draft)}
          >
            <SlidersHorizontal size={17} />
          </button>
        </div>
        {showTime && draft.plan.precise && (
          <div className="quick-precise-time">
            <p className="precise-current-value">Получится: {rangeLabel(draft.plan)}</p>
            <label className="quick-precision-switch">
              Точность
              <select
                aria-label="Точность времени"
                value="precise"
                onChange={(event) => setPreciseMode(event.target.value === 'precise')}
                disabled={!calendarRepresentable}
              >
                <option value="calendar">Календарная</option>
                <option value="precise">До наносекунд</option>
              </select>
            </label>
            <PreciseRangeFields
              value={draft.plan}
              onChange={(plan) => setDraft((current) => ({ ...current, plan }))}
              timeContext={timeContext}
              onInvalidChange={setInvalidPreciseInput}
            />
          </div>
        )}
        {showTime && subMillisecondCalendar && (
          <div className="quick-precise-time">
            <p className="precise-current-value">{rangeLabel(draft.plan)}</p>
            <PreciseRangeFields
              value={withPreciseCoordinates(draft.plan)}
              onChange={(plan) => setDraft((current) => ({ ...current, plan }))}
              timeContext={timeContext}
              onInvalidChange={setInvalidPreciseInput}
              calendarOnly
              showResolution={false}
            />
          </div>
        )}
        {showTime && !draft.plan.precise && !subMillisecondCalendar && (
          <div className="quick-time-row">
            <input
              type={allDay ? 'date' : 'datetime-local'}
              step={allDay ? undefined : 'any'}
              aria-label="Начало"
              title="Начало"
              value={localRangeInput(draft.plan, 'start', allDay)}
              onChange={(e) => updateTime('start', e.target.value)}
            />
            <span aria-hidden="true">→</span>
            <input
              type={allDay ? 'date' : 'datetime-local'}
              step={allDay ? undefined : 'any'}
              aria-label="Окончание"
              title={allDay ? 'Последний день включительно' : 'Окончание'}
              value={localRangeInput(draft.plan, 'end', allDay)}
              onChange={(e) => updateTime('end', e.target.value)}
            />
            <div className="quick-time-meta">
              <label className="quick-day-toggle">
                <input
                  type="checkbox"
                  checked={allDay}
                  onChange={(e) => {
                    try {
                      const next = e.target.checked;
                      setDraft({
                        ...draft,
                        plan:
                          !next && draft.plan.precision === 'day'
                            ? {
                                ...draft.plan,
                                end: displayRangeEnd(draft.plan),
                                precision: 'exact',
                              }
                            : changeRangePrecision(draft.plan, next),
                      });
                      setAllDay(next);
                      setError('');
                    } catch (error) {
                      setError((error as Error).message);
                    }
                  }}
                />
                Весь день
              </label>
              <label className="quick-precision-switch">
                Точность
                <select
                  aria-label="Точность времени"
                  value="calendar"
                  onChange={(event) => setPreciseMode(event.target.value === 'precise')}
                >
                  <option value="calendar">Календарная</option>
                  <option value="precise">До наносекунд</option>
                </select>
              </label>
              <small title="Часовой пояс">{draft.plan.timezone}</small>
            </div>
          </div>
        )}
        {needsReason && (
          <label className="quick-reason">
            Причина изменения
            <input value={reason} onChange={(e) => setReason(e.target.value)} required />
          </label>
        )}
        {!!fields.length && (
          <div className="quick-required-fields">
            {fields.map((field) => (
              <label key={field.id}>
                {field.label}
                {field.type === 'boolean' ? (
                  <select
                    aria-label={field.label}
                    value={String(draft.fields[field.id] ?? '')}
                    required={field.required}
                    onChange={(e) =>
                      updateField(
                        field.id,
                        e.target.value === '' ? null : e.target.value === 'true',
                      )
                    }
                  >
                    <option value="">—</option>
                    <option value="false">Нет</option>
                    <option value="true">Да</option>
                  </select>
                ) : field.type === 'select' ? (
                  <select
                    aria-label={field.label}
                    value={String(draft.fields[field.id] ?? '')}
                    required={field.required}
                    onChange={(e) => updateField(field.id, e.target.value)}
                  >
                    <option value="">—</option>
                    {field.options?.map((o) => (
                      <option key={o}>{o}</option>
                    ))}
                  </select>
                ) : (
                  <input
                    aria-label={field.label}
                    type={
                      field.type === 'number'
                        ? 'number'
                        : field.type === 'date'
                          ? 'date'
                          : field.type === 'url'
                            ? 'url'
                            : 'text'
                    }
                    step="any"
                    required={field.required}
                    value={String(draft.fields[field.id] ?? '')}
                    onChange={(e) =>
                      updateField(
                        field.id,
                        field.type === 'number' && e.target.value
                          ? Number(e.target.value)
                          : e.target.value,
                      )
                    }
                  />
                )}
              </label>
            ))}
          </div>
        )}
        {error && (
          <p className="quick-error" role="alert">
            {error}
          </p>
        )}
      </form>
    </Modal>
  );
}
