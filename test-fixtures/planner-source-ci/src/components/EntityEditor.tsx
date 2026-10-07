import { useRef, useState, type FormEvent } from 'react';
import {
  Plus,
  Paperclip,
  X,
  Clock3,
  Layers3,
  FileText,
  Users,
  Repeat2,
  ArrowRight,
  Download,
} from 'lucide-react';
import type {
  Entity,
  EntityDraft,
  EntityKind,
  FieldValue,
  PlannerSnapshot,
  TimeRange,
} from '../../shared/types';
import { hasSubMillisecondTime, preciseRangeErrors } from '../../shared/precise-time';
import {
  changeRangePrecision,
  displayRangeEnd,
  localRangeInput,
  withLocalRangeInput,
} from '../../shared/local-range';
import { request } from '../api';
import {
  emptyRange,
  fromInput,
  inputDate,
  kindLabels,
  rangeLabel,
  statusLabels,
  uid,
  withPreciseCoordinates,
} from '../utils';
import Modal from './Modal';
import PreciseRangeFields, { type TimeContext } from './PreciseRangeFields';

type Props = {
  snapshot: PlannerSnapshot;
  initial: EntityDraft;
  entity?: Entity;
  busy: boolean;
  onClose: () => void;
  onSave: (draft: EntityDraft, proposal: boolean, reason: string) => Promise<void>;
  timeContext?: TimeContext;
};
const tabs = [
  { id: 'main', label: 'Объект', icon: Layers3 },
  { id: 'time', label: 'Время', icon: Clock3 },
  { id: 'content', label: 'Содержание', icon: FileText },
  { id: 'people', label: 'Участники и ресурсы', icon: Users },
] as const;

export function TimeEditor({
  value,
  onChange,
  label,
  timeContext,
  onInvalidChange,
}: {
  value: TimeRange;
  onChange: (value: TimeRange) => void;
  label: string;
  timeContext?: TimeContext;
  onInvalidChange?: (invalid: boolean) => void;
}) {
  const [subMillisecondCalendar] = useState(() => !value.precise && hasSubMillisecondTime(value));
  const setPrecision = (precision: TimeRange['precision']) => {
    if (precision === 'unknown') {
      const { precise: _precise, ...range } = value;
      onChange({ ...range, start: null, end: null, earliest: null, latest: null, precision });
      onInvalidChange?.(false);
      return;
    }
    if (precision === 'day' && !value.precise) {
      onChange({ ...changeRangePrecision(value, true), precision });
      return;
    }
    if (value.precision === 'day' && precision === 'exact' && !value.precise) {
      onChange({ ...value, end: displayRangeEnd(value), precision });
      return;
    }
    onChange({ ...value, precision });
  };
  const setInput = (key: 'start' | 'end' | 'earliest' | 'latest', next: string) => {
    if (key === 'start' || key === 'end') {
      onChange(withLocalRangeInput(value, key, next, value.precision === 'day'));
      return;
    }
    onChange({ ...value, [key]: fromInput(next, value.timezone) });
  };
  return (
    <fieldset className="range-editor">
      <legend>{label}</legend>
      <div className="form-grid">
        <label>
          Точность
          <select
            value={value.precision}
            onChange={(e) => setPrecision(e.target.value as TimeRange['precision'])}
          >
            <option value="exact">Точное время</option>
            <option value="day">До дня</option>
            <option value="month">До месяца</option>
            <option value="approximate">Приблизительно</option>
            <option value="unknown">Пока неизвестно</option>
          </select>
        </label>
        <label>
          Часовой пояс
          <input
            value={value.timezone}
            list="timezones"
            onChange={(e) => onChange({ ...value, timezone: e.target.value })}
            required
          />
        </label>
        {!value.precise && !subMillisecondCalendar && (
          <>
            <label>
              Начало
              <input
                type={value.precision === 'day' ? 'date' : 'datetime-local'}
                step={value.precision === 'day' ? undefined : 'any'}
                value={
                  value.precision === 'day'
                    ? localRangeInput(value, 'start', true)
                    : inputDate(value.start, value.timezone)
                }
                onChange={(e) => setInput('start', e.target.value)}
              />
            </label>
            <label>
              Окончание <span className="field-optional">необязательно</span>
              <input
                type={value.precision === 'day' ? 'date' : 'datetime-local'}
                step={value.precision === 'day' ? undefined : 'any'}
                title={value.precision === 'day' ? 'Последний день включительно' : undefined}
                value={
                  value.precision === 'day'
                    ? localRangeInput(value, 'end', true)
                    : inputDate(value.end, value.timezone)
                }
                onChange={(e) => setInput('end', e.target.value)}
              />
            </label>
          </>
        )}
        {value.precision === 'approximate' && !subMillisecondCalendar && (
          <>
            <label>
              Не раньше
              <input
                type="datetime-local"
                step="any"
                value={inputDate(value.earliest, value.timezone)}
                onChange={(e) => setInput('earliest', e.target.value)}
              />
            </label>
            <label>
              Не позже
              <input
                type="datetime-local"
                step="any"
                value={inputDate(value.latest, value.timezone)}
                onChange={(e) => setInput('latest', e.target.value)}
              />
            </label>
          </>
        )}
      </div>
      {value.precise || subMillisecondCalendar ? (
        <div className="precise-editor-wrap">
          <p className="precise-current-value">{rangeLabel(value)}</p>
          <PreciseRangeFields
            value={subMillisecondCalendar ? withPreciseCoordinates(value) : value}
            onChange={onChange}
            timeContext={timeContext}
            onInvalidChange={onInvalidChange}
            calendarOnly={subMillisecondCalendar}
            showResolution={!subMillisecondCalendar}
          />
          {value.precise && value.start && value.end && value.precision === 'exact' && (
            <button
              type="button"
              className="text-button"
              onClick={() => {
                const { precise: _precise, ...calendarRange } = value;
                onChange(calendarRange);
                onInvalidChange?.(false);
              }}
            >
              Вернуться к обычному времени
            </button>
          )}
        </div>
      ) : (
        value.precision === 'exact' &&
        !!(value.start || value.end) && (
          <details className="precise-editor-disclosure">
            <summary>Точность до наносекунд</summary>
            <PreciseRangeFields
              value={withPreciseCoordinates(value)}
              onChange={onChange}
              timeContext={timeContext}
              onInvalidChange={onInvalidChange}
            />
          </details>
        )
      )}
      {!value.start && !value.end && !value.precise && (
        <p className="helper">
          Объект будет в разделе «Без даты». Неизвестное время не подменяется сегодняшним.
        </p>
      )}
    </fieldset>
  );
}

export default function EntityEditor({
  snapshot,
  initial,
  entity,
  busy,
  onClose,
  onSave,
  timeContext,
}: Props) {
  const [draft, setDraft] = useState<EntityDraft>(() => structuredClone(initial));
  const [tab, setTab] = useState<(typeof tabs)[number]['id']>('main');
  const [reason, setReason] = useState('');
  const [proposal, setProposal] = useState(
    snapshot.workspaces.find((w) => w.id === initial.workspaceId)?.mode === 'team',
  );
  const [error, setError] = useState<string | null>(null);
  const pending = useRef(false);
  const invalidPreciseFields = useRef(new Set<string>());
  const [uploading, setUploading] = useState(false);
  const [newKey, setNewKey] = useState('');
  const types = snapshot.types.filter((t) => !t.workspaceId || t.workspaceId === draft.workspaceId);
  const type = types.find((t) => t.id === draft.typeId);
  const workspace = snapshot.workspaces.find((w) => w.id === draft.workspaceId)!;
  const participants = snapshot.users.filter((u) =>
    snapshot.memberships.some((m) => m.workspaceId === draft.workspaceId && m.userId === u.id),
  );
  const resources = snapshot.resources.filter((r) => r.workspaceId === draft.workspaceId);
  const parents = snapshot.entities.filter(
    (e) => e.workspaceId === draft.workspaceId && e.id !== entity?.id,
  );
  const planChanged = entity && JSON.stringify(entity.plan) !== JSON.stringify(draft.plan);
  const needsReason =
    entity?.status === 'done' &&
    (planChanged || JSON.stringify(entity.actual) !== JSON.stringify(draft.actual));
  const update = <K extends keyof EntityDraft>(key: K, value: EntityDraft[K]) =>
    setDraft((d) => ({ ...d, [key]: value }));
  const setField = (key: string, value: FieldValue) =>
    setDraft((d) => ({ ...d, fields: { ...d.fields, [key]: value } }));
  async function submit(event: FormEvent) {
    event.preventDefault();
    if (pending.current || busy || uploading) return;
    setError(null);
    if (invalidPreciseFields.current.size > 0) {
      setTab('time');
      setError('Исправьте формат точного времени перед сохранением.');
      return;
    }
    if (!draft.title.trim()) {
      setTab('main');
      setError('Дайте объекту название.');
      return;
    }
    for (const range of [draft.plan, draft.actual, draft.forecast]) {
      if (range && preciseRangeErrors(range).length) {
        setTab('time');
        setError(preciseRangeErrors(range)[0]!);
        return;
      }
    }
    if (draft.recurrence && draft.plan.precise) {
      setTab('time');
      setError('Для точного относительного времени календарное повторение недоступно.');
      return;
    }
    for (const field of type?.fields ?? [])
      if (field.required && (draft.fields[field.id] == null || draft.fields[field.id] === '')) {
        setTab('content');
        setError(`Заполните поле «${field.label}».`);
        return;
      }
    try {
      pending.current = true;
      await onSave({ ...draft, title: draft.title.trim() }, !!planChanged && proposal, reason);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      pending.current = false;
    }
  }
  async function upload(file?: File) {
    if (!file) return;
    setUploading(true);
    setError(null);
    try {
      const form = new FormData();
      form.append('workspaceId', draft.workspaceId);
      form.append('file', file);
      if (entity) form.append('entityId', entity.id);
      const result = await request<{ id: string; url: string; label: string }>(
        '/api/files',
        'POST',
        form,
      );
      setDraft((d) => ({
        ...d,
        links: [
          ...d.links,
          { id: uid(), label: result.label, url: result.url, kind: 'file', fileId: result.id },
        ],
      }));
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setUploading(false);
    }
  }
  return (
    <Modal
      title={entity ? 'Редактировать объект' : 'Добавить на шкалу'}
      subtitle={
        entity
          ? 'Изменения сохраняются с историей и проверкой версии.'
          : 'Дата, период или целый процесс — одна понятная форма.'
      }
      onClose={onClose}
      wide
    >
      <datalist id="timezones">
        <option value="Asia/Yekaterinburg" />
        <option value="Europe/Moscow" />
        <option value="Europe/London" />
        <option value="Europe/Paris" />
        <option value="America/New_York" />
        <option value="Asia/Dubai" />
        <option value="Asia/Tokyo" />
        <option value="UTC" />
      </datalist>
      <form onSubmit={submit}>
        <div className="context-tabs" role="tablist" aria-label="Разделы объекта">
          {tabs.map((t) => (
            <button
              key={t.id}
              type="button"
              role="tab"
              aria-selected={tab === t.id}
              className={tab === t.id ? 'active' : ''}
              onClick={() => setTab(t.id)}
            >
              <t.icon size={16} />
              <span>{t.label}</span>
            </button>
          ))}
        </div>
        <div className="form-body">
          {tab === 'main' && (
            <div className="form-grid">
              <label className="full">
                Название
                <input
                  autoFocus
                  value={draft.title}
                  placeholder="Что происходит?"
                  onChange={(e) => update('title', e.target.value)}
                  required
                  maxLength={240}
                />
              </label>
              <label>
                Пространство
                <select
                  value={draft.workspaceId}
                  disabled={!!entity}
                  onChange={(e) => {
                    const w = snapshot.workspaces.find((w) => w.id === e.target.value)!;
                    const nextType = snapshot.types.find(
                      (t) => (!t.workspaceId || t.workspaceId === w.id) && t.kind === draft.kind,
                    );
                    setDraft((d) => ({
                      ...d,
                      workspaceId: w.id,
                      typeId: nextType?.id ?? d.typeId,
                      parentId: null,
                      plan: { ...d.plan, timezone: w.timezone },
                      allocations: [],
                    }));
                  }}
                >
                  {snapshot.workspaces
                    .filter((w) =>
                      snapshot.memberships.some(
                        (m) =>
                          m.workspaceId === w.id &&
                          m.userId === snapshot.user.id &&
                          ['owner', 'editor'].includes(m.role),
                      ),
                    )
                    .map((w) => (
                      <option key={w.id} value={w.id}>
                        {w.name}
                      </option>
                    ))}
                </select>
              </label>
              <label>
                Тип
                <select
                  value={draft.typeId}
                  onChange={(e) => {
                    const next = types.find((t) => t.id === e.target.value)!;
                    setDraft((d) => ({ ...d, typeId: next.id, kind: next.kind }));
                  }}
                >
                  {types.map((t) => (
                    <option key={t.id} value={t.id}>
                      {t.label} · {kindLabels[t.kind]}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                Форма на шкале
                <select value={draft.kind} disabled aria-describedby="kind-hint">
                  {Object.entries(kindLabels).map(([key, name]) => (
                    <option key={key} value={key}>
                      {name}
                    </option>
                  ))}
                </select>
                <small id="kind-hint">Определяется выбранным типом</small>
              </label>
              <label>
                Состояние
                <select
                  value={draft.status}
                  onChange={(e) => update('status', e.target.value as EntityDraft['status'])}
                >
                  {Object.entries(statusLabels).map(([key, name]) => (
                    <option key={key} value={key}>
                      {name}
                    </option>
                  ))}
                </select>
              </label>
              <label className="full">
                Внутри другого объекта
                <select
                  value={draft.parentId ?? ''}
                  onChange={(e) => update('parentId', e.target.value || null)}
                >
                  <option value="">Самостоятельный объект</option>
                  {parents.map((e) => (
                    <option key={e.id} value={e.id}>
                      {e.title}
                    </option>
                  ))}
                </select>
              </label>
              <label className="full">
                Описание
                <textarea
                  rows={4}
                  value={draft.description}
                  placeholder="Контекст, идея, план, заметки…"
                  onChange={(e) => update('description', e.target.value)}
                />
              </label>
              <label className="full">
                Теги
                <input
                  value={draft.tags.join(', ')}
                  onChange={(e) =>
                    update(
                      'tags',
                      e.target.value
                        .split(',')
                        .map((t) => t.trim())
                        .filter(Boolean),
                    )
                  }
                  placeholder="Поездка, личное, запуск"
                />
              </label>
              <button
                type="button"
                className="text-button full align-left"
                onClick={() => setTab('time')}
              >
                Уточнить время и повторение <ArrowRight size={15} />
              </button>
            </div>
          )}
          {tab === 'time' && (
            <>
              <TimeEditor
                value={draft.plan}
                onChange={(r) => update('plan', r)}
                label="План"
                timeContext={timeContext}
                onInvalidChange={(invalid) => {
                  if (invalid) invalidPreciseFields.current.add('plan');
                  else invalidPreciseFields.current.delete('plan');
                }}
              />
              <div className="form-grid">
                <label className="full">
                  Крайний срок
                  <input
                    type="datetime-local"
                    step="any"
                    value={inputDate(draft.dueAt, draft.plan.timezone)}
                    onChange={(e) =>
                      update('dueAt', fromInput(e.target.value, draft.plan.timezone))
                    }
                  />
                </label>
              </div>
              {(['actual', 'forecast'] as const).map((key) => (
                <div key={key} className="optional-range">
                  <label className="checkbox-label">
                    <input
                      type="checkbox"
                      checked={draft[key] !== null}
                      onChange={(e) => {
                        if (!e.target.checked) invalidPreciseFields.current.delete(key);
                        setDraft((d) => ({
                          ...d,
                          [key]: e.target.checked ? emptyRange(d.plan.timezone) : null,
                          ...(key === 'forecast' ? { forecastProvenance: 'manual' as const } : {}),
                        }));
                      }}
                    />
                    {key === 'actual'
                      ? 'Записать факт'
                      : draft.forecastProvenance === 'derived'
                        ? 'Расчётный прогноз'
                        : 'Задать прогноз'}
                    <span className="field-optional">отдельно от плана</span>
                  </label>
                  {key === 'forecast' && draft.forecastProvenance === 'derived' && (
                    <p className="helper">
                      Рассчитан по текущему плану и факту. Введённые вами даты станут ручным
                      прогнозом.
                    </p>
                  )}
                  {draft[key] && (
                    <TimeEditor
                      value={draft[key]!}
                      onChange={(r) =>
                        setDraft((d) => ({
                          ...d,
                          [key]: r,
                          ...(key === 'forecast' ? { forecastProvenance: 'manual' as const } : {}),
                        }))
                      }
                      label={key === 'actual' ? 'Фактическое время' : 'Прогноз'}
                      timeContext={timeContext}
                      onInvalidChange={(invalid) => {
                        if (invalid) invalidPreciseFields.current.add(key);
                        else invalidPreciseFields.current.delete(key);
                      }}
                    />
                  )}
                </div>
              ))}
              <fieldset className="range-editor">
                <legend>
                  <Repeat2 size={15} /> Повторение
                </legend>
                <label className="checkbox-label">
                  <input
                    type="checkbox"
                    checked={!!draft.recurrence}
                    disabled={!!draft.plan.precise}
                    onChange={(e) =>
                      update(
                        'recurrence',
                        e.target.checked
                          ? { frequency: 'week', interval: 1, exceptions: [] }
                          : null,
                      )
                    }
                  />
                  Повторять событие
                </label>
                {draft.plan.precise && (
                  <p className="helper">Относительное время нельзя повторять по календарю.</p>
                )}
                {draft.recurrence && (
                  <div className="form-grid">
                    <label>
                      Периодичность
                      <select
                        value={draft.recurrence.frequency}
                        onChange={(e) =>
                          update('recurrence', {
                            ...draft.recurrence!,
                            frequency: e.target.value as 'day' | 'week' | 'month',
                          })
                        }
                      >
                        <option value="day">Каждый день</option>
                        <option value="week">Каждую неделю</option>
                        <option value="month">Каждый месяц</option>
                      </select>
                    </label>
                    <label>
                      Каждые N периодов
                      <input
                        type="number"
                        min={1}
                        max={365}
                        value={draft.recurrence.interval}
                        onChange={(e) =>
                          update('recurrence', {
                            ...draft.recurrence!,
                            interval: Number(e.target.value),
                          })
                        }
                      />
                    </label>
                    <label>
                      Количество
                      <input
                        type="number"
                        min={1}
                        value={draft.recurrence.count ?? ''}
                        onChange={(e) =>
                          update('recurrence', {
                            ...draft.recurrence!,
                            count: e.target.value ? Number(e.target.value) : undefined,
                          })
                        }
                      />
                    </label>
                    <label>
                      Повторять до
                      <input
                        type="date"
                        value={draft.recurrence.until?.slice(0, 10) ?? ''}
                        onChange={(e) =>
                          update('recurrence', {
                            ...draft.recurrence!,
                            until: e.target.value || undefined,
                          })
                        }
                      />
                    </label>
                    <label className="full">
                      Если даты или времени нет
                      <select
                        value={draft.recurrence.calendarPolicy ?? 'adjust'}
                        onChange={(e) =>
                          update('recurrence', {
                            ...draft.recurrence!,
                            calendarPolicy: e.target.value as 'adjust' | 'skip-invalid',
                          })
                        }
                      >
                        <option value="adjust">Перенести на допустимую дату / время</option>
                        <option value="skip-invalid">Пропустить это повторение</option>
                      </select>
                      <small>
                        Например, 31-е число в коротком месяце или время перехода на летнее время.
                      </small>
                    </label>
                    {draft.recurrence.frequency === 'week' && (
                      <div className="full weekday-picker">
                        {['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс'].map((day, i) => (
                          <label key={day}>
                            <input
                              type="checkbox"
                              checked={draft.recurrence!.weekdays?.includes(i + 1) ?? false}
                              onChange={(e) =>
                                update('recurrence', {
                                  ...draft.recurrence!,
                                  weekdays: e.target.checked
                                    ? [...(draft.recurrence!.weekdays ?? []), i + 1]
                                    : (draft.recurrence!.weekdays ?? []).filter((n) => n !== i + 1),
                                })
                              }
                            />
                            {day}
                          </label>
                        ))}
                      </div>
                    )}
                    <label className="full">
                      Исключения (даты через запятую)
                      <input
                        placeholder="2026-10-12, 2026-10-19"
                        value={draft.recurrence.exceptions.join(', ')}
                        onChange={(e) =>
                          update('recurrence', {
                            ...draft.recurrence!,
                            exceptions: e.target.value
                              .split(',')
                              .map((s) => s.trim())
                              .filter(Boolean),
                          })
                        }
                      />
                    </label>
                  </div>
                )}
              </fieldset>
            </>
          )}
          {tab === 'content' && (
            <>
              <h3>Поля объекта</h3>
              <div className="form-grid">
                {(type?.fields ?? []).map((field) => (
                  <label key={field.id}>
                    {field.label}
                    {field.required && ' *'}
                    {field.type === 'boolean' ? (
                      <select
                        aria-label={field.label}
                        value={String(draft.fields[field.id] ?? '')}
                        onChange={(e) =>
                          setField(
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
                        onChange={(e) => setField(field.id, e.target.value)}
                      >
                        <option value="">Выбрать…</option>
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
                        value={String(draft.fields[field.id] ?? '')}
                        onChange={(e) =>
                          setField(
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
                {Object.entries(draft.fields)
                  .filter(([key]) => !(type?.fields ?? []).some((f) => f.id === key))
                  .map(([key, value]) => (
                    <label key={key}>
                      {key}
                      <div className="input-action">
                        <input
                          value={String(value ?? '')}
                          type={typeof value === 'number' ? 'number' : 'text'}
                          onChange={(e) =>
                            setField(
                              key,
                              typeof value === 'number' ? Number(e.target.value) : e.target.value,
                            )
                          }
                        />
                        <button
                          type="button"
                          className="icon-button"
                          aria-label={`Удалить поле ${key}`}
                          onClick={() =>
                            setDraft((d) => ({
                              ...d,
                              fields: Object.fromEntries(
                                Object.entries(d.fields).filter(([k]) => k !== key),
                              ),
                            }))
                          }
                        >
                          <X size={16} />
                        </button>
                      </div>
                    </label>
                  ))}
              </div>
              <div className="inline-form">
                <input
                  value={newKey}
                  onChange={(e) => setNewKey(e.target.value)}
                  placeholder="Название произвольного поля"
                />
                <button
                  type="button"
                  className="secondary-button"
                  disabled={!newKey.trim() || newKey in draft.fields}
                  onClick={() => {
                    setField(newKey.trim(), '');
                    setNewKey('');
                  }}
                >
                  <Plus size={15} />
                  Поле
                </button>
              </div>
              <h3>Ссылки и файлы</h3>
              <div className="link-editor">
                {draft.links.map((link) => (
                  <div className="link-row" key={link.id}>
                    <input
                      aria-label="Название ссылки"
                      value={link.label}
                      placeholder="Название"
                      onChange={(e) =>
                        update(
                          'links',
                          draft.links.map((l) =>
                            l.id === link.id ? { ...l, label: e.target.value } : l,
                          ),
                        )
                      }
                    />
                    {link.kind === 'file' ? (
                      <a
                        className="secondary-button attachment-download"
                        href={link.url}
                        download
                        aria-label={`Скачать ${link.label}`}
                      >
                        <Download size={15} />
                        Скачать файл
                      </a>
                    ) : (
                      <input
                        aria-label="Адрес ссылки"
                        type="url"
                        value={link.url}
                        placeholder="https://…"
                        onChange={(e) =>
                          update(
                            'links',
                            draft.links.map((l) =>
                              l.id === link.id ? { ...l, url: e.target.value } : l,
                            ),
                          )
                        }
                      />
                    )}
                    <button
                      type="button"
                      className="icon-button"
                      aria-label="Убрать вложение"
                      onClick={() =>
                        update(
                          'links',
                          draft.links.filter((l) => l.id !== link.id),
                        )
                      }
                    >
                      <X size={16} />
                    </button>
                  </div>
                ))}
              </div>
              <div className="button-row">
                <button
                  type="button"
                  className="secondary-button"
                  onClick={() =>
                    update('links', [
                      ...draft.links,
                      { id: uid(), label: '', url: '', kind: 'url' },
                    ])
                  }
                >
                  <Plus size={15} />
                  Ссылка
                </button>
                <label className="secondary-button upload-button">
                  <Paperclip size={15} />
                  {uploading ? 'Загрузка…' : 'Загрузить файл'}
                  <input
                    type="file"
                    disabled={uploading}
                    onChange={(e) => void upload(e.target.files?.[0])}
                  />
                </label>
              </div>
              <p className="helper">
                Файлы сохраняются на сервере и доступны только участникам этого пространства.
              </p>
            </>
          )}
          {tab === 'people' && (
            <>
              <div className="form-grid">
                <label className="full">
                  Ответственный
                  <select
                    value={draft.ownerId ?? ''}
                    onChange={(e) => update('ownerId', e.target.value || null)}
                  >
                    <option value="">Не назначен</option>
                    {participants.map((u) => (
                      <option key={u.id} value={u.id}>
                        {u.displayName}
                      </option>
                    ))}
                  </select>
                </label>
              </div>
              <h3>Участники</h3>
              <div className="check-list">
                {participants.map((u) => (
                  <label className="checkbox-label" key={u.id}>
                    <input
                      type="checkbox"
                      checked={draft.participantIds.includes(u.id)}
                      onChange={(e) =>
                        update(
                          'participantIds',
                          e.target.checked
                            ? [...draft.participantIds, u.id]
                            : draft.participantIds.filter((id) => id !== u.id),
                        )
                      }
                    />
                    {u.displayName}
                  </label>
                ))}
              </div>
              <h3>Ресурсы</h3>
              {resources.length ? (
                resources.map((resource) => {
                  const allocation = draft.allocations.find((a) => a.resourceId === resource.id);
                  return (
                    <div className="resource-form-row" key={resource.id}>
                      <label className="checkbox-label">
                        <input
                          type="checkbox"
                          checked={!!allocation}
                          onChange={(e) =>
                            update(
                              'allocations',
                              e.target.checked
                                ? [...draft.allocations, { resourceId: resource.id, amount: 1 }]
                                : draft.allocations.filter((a) => a.resourceId !== resource.id),
                            )
                          }
                        />
                        {resource.name}
                      </label>
                      <span className="muted">
                        из {resource.capacity} {resource.unit}
                      </span>
                      {allocation && (
                        <input
                          aria-label={`Количество ${resource.name}`}
                          type="number"
                          min={0.01}
                          step="any"
                          value={allocation.amount}
                          onChange={(e) =>
                            update(
                              'allocations',
                              draft.allocations.map((a) =>
                                a.resourceId === resource.id
                                  ? { ...a, amount: Number(e.target.value) }
                                  : a,
                              ),
                            )
                          }
                        />
                      )}
                    </div>
                  );
                })
              ) : (
                <p className="empty-inline">
                  Ресурсы можно добавить в настройках пространства: люди, техника, бюджет или места.
                </p>
              )}
            </>
          )}
          {(planChanged || needsReason) && (
            <div className="change-method">
              {planChanged && (
                <>
                  <h3>Вы меняете план</h3>
                  <label className="checkbox-label">
                    <input
                      type="checkbox"
                      checked={proposal}
                      onChange={(e) => setProposal(e.target.checked)}
                    />
                    Сначала посмотреть последствия и создать предложение
                  </label>
                  <p className="helper">
                    {proposal
                      ? 'После сохранения откроется проверка зависимостей, сроков и ресурсов.'
                      : 'Новые даты будут применены сразу после сохранения и записаны в историю.'}
                  </p>
                </>
              )}
              <label>
                Причина изменения
                <input
                  value={reason}
                  onChange={(e) => setReason(e.target.value)}
                  placeholder="Что изменилось?"
                  required={!!needsReason}
                />
              </label>
            </div>
          )}
          {error && (
            <div className="inline-error" role="alert">
              {error}
            </div>
          )}
        </div>
        <footer className="modal-footer">
          <span className="helper">
            {workspace?.name}
            {entity ? ` · версия ${entity.version}` : ''}
          </span>
          <div className="button-row">
            <button type="button" className="secondary-button" onClick={onClose}>
              Отмена
            </button>
            <button className="primary-button" disabled={busy || uploading}>
              {busy
                ? 'Сохраняем…'
                : planChanged && proposal
                  ? 'Проверить изменение'
                  : entity
                    ? 'Сохранить'
                    : 'Добавить на шкалу'}
            </button>
          </div>
        </footer>
      </form>
    </Modal>
  );
}
