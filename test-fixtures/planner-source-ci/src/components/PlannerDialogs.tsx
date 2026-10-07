import { useEffect, useState, type FormEvent } from 'react';
import { DateTime } from 'luxon';
import {
  ArrowRight,
  Check,
  FileText,
  GitBranch,
  Lightbulb,
  Plus,
  Search,
  Sparkles,
  X,
} from 'lucide-react';
import type {
  AssistantSuggestion,
  Entity,
  PlanChange,
  PlannerSnapshot,
  Scenario,
  ScenarioPreview,
} from '../../shared/types';
import { request, type Mutate } from '../api';
import {
  canApprove,
  canEdit,
  dateLabel,
  fromInput,
  inputDate,
  rangeLabel,
  requiresAttention,
  statusLabels,
} from '../utils';
import Modal from './Modal';
import { SignalActions } from './EntityDetail';

type Common = {
  snapshot: PlannerSnapshot;
  workspaceId: string;
  busy: boolean;
  mutate: Mutate;
  onClose: () => void;
};
export function AttentionDialog({
  snapshot,
  workspaceId,
  busy,
  mutate,
  onClose,
  onSelect,
}: Common & { onSelect: (e: Entity) => void }) {
  const [includeHistory, setIncludeHistory] = useState(false);
  const [error, setError] = useState('');
  const signals = snapshot.signals
    .filter(
      (s) =>
        (workspaceId === 'all' || s.workspaceId === workspaceId) &&
        (includeHistory || requiresAttention(s)),
    )
    .sort(
      (a, b) =>
        ({ critical: 0, warning: 1, info: 2 })[a.severity] -
        { critical: 0, warning: 1, info: 2 }[b.severity],
    );
  async function act(id: string, action: string, body?: unknown) {
    try {
      await mutate(`/api/signals/${id}/${action}`, 'POST', body);
      setError('');
    } catch (e) {
      setError((e as Error).message);
    }
  }
  return (
    <Modal
      title="Требует внимания"
      subtitle="Каждое оповещение связано с объектом, ответственным и дальнейшим действием."
      onClose={onClose}
      wide
    >
      <div className="form-body">
        <label className="checkbox-label">
          <input
            type="checkbox"
            checked={includeHistory}
            onChange={(e) => setIncludeHistory(e.target.checked)}
          />
          Показать информацию, решённое и принятые риски
        </label>
        {signals.length ? (
          <div className="attention-list">
            {signals.map((signal) => {
              const entity = snapshot.entities.find((e) => e.id === signal.entityId);
              const snoozed = snapshot.notifications.find(
                (n) => n.signalId === signal.id && n.state === 'snoozed',
              );
              return (
                <article key={signal.id} className={`signal-card ${signal.severity}`}>
                  <div>
                    <span className="eyebrow">
                      {signal.severity === 'critical'
                        ? 'Высокий приоритет'
                        : signal.severity === 'warning'
                          ? 'Проверьте'
                          : 'Информация'}
                    </span>
                    <h3>{signal.title}</h3>
                    <p>{signal.description}</p>
                    {signal.responsibleUserId && (
                      <small>
                        Ответственный:{' '}
                        {snapshot.users.find((u) => u.id === signal.responsibleUserId)
                          ?.displayName ?? 'Участник'}
                      </small>
                    )}
                    {signal.dueAt && (
                      <small>До {dateLabel(signal.dueAt, entity?.plan.timezone, true)}</small>
                    )}
                    {snoozed?.snoozedUntil && (
                      <small>
                        Оповещение отложено до{' '}
                        {dateLabel(snoozed.snoozedUntil, entity?.plan.timezone, true)}
                      </small>
                    )}
                    {entity && (
                      <button
                        className="text-button align-left"
                        onClick={() => {
                          onSelect(entity);
                          onClose();
                        }}
                      >
                        Открыть {entity.title}
                        <ArrowRight size={14} />
                      </button>
                    )}
                  </div>
                  <SignalActions
                    signal={signal}
                    busy={busy}
                    editable={canEdit(snapshot, signal.workspaceId)}
                    onAction={(action, body) => void act(signal.id, action, body)}
                  />
                </article>
              );
            })}
          </div>
        ) : (
          <div className="dialog-empty">
            <Check size={28} />
            <h3>Можно спокойно продолжать</h3>
            <p>В этом пространстве сейчас нет открытых предупреждений.</p>
          </div>
        )}
        {error && (
          <p className="inline-error" role="alert">
            {error}
          </p>
        )}
        <details className="delivery-details">
          <summary>Доставка оповещений</summary>
          {snapshot.notifications
            .filter((n) => workspaceId === 'all' || n.workspaceId === workspaceId)
            .slice(-30)
            .reverse()
            .map((n) => (
              <div key={n.id}>
                <span>{n.summary}</span>
                <small>
                  {n.channel === 'browser'
                    ? 'Браузер'
                    : n.channel === 'in-app'
                      ? 'На шкале'
                      : 'Webhook'}{' '}
                  ·{' '}
                  {
                    {
                      pending: 'ожидает доставки',
                      delivered: 'доставлено',
                      failed: 'ошибка доставки',
                      snoozed: 'отложено',
                    }[n.state]
                  }
                </small>
              </div>
            ))}
          <p className="helper">
            Доставка сообщения не означает, что объект принят в работу или проблема решена.
          </p>
        </details>
      </div>
    </Modal>
  );
}

export function SearchDialog({
  snapshot,
  workspaceId,
  onClose,
  onSelect,
}: {
  snapshot: PlannerSnapshot;
  workspaceId: string;
  onClose: () => void;
  onSelect: (e: Entity) => void;
}) {
  const [q, setQ] = useState('');
  const results = snapshot.entities.filter(
    (e) =>
      (workspaceId === 'all' || e.workspaceId === workspaceId) &&
      `${e.title} ${e.description} ${e.tags.join(' ')} ${Object.values(e.fields).join(' ')} ${e.links.map((l) => l.label).join(' ')}`
        .toLocaleLowerCase()
        .includes(q.toLocaleLowerCase()),
  );
  return (
    <Modal title="Найти во времени" onClose={onClose}>
      <div className="search-box">
        <Search size={19} />
        <input
          autoFocus
          placeholder="Название, тег, заметка или поле…"
          value={q}
          onChange={(e) => setQ(e.target.value)}
          aria-label="Поиск объектов"
        />
        <kbd>Esc</kbd>
      </div>
      <div className="search-results">
        <p className="helper">{results.length} объектов · в доступных вам пространствах</p>
        {results.map((e) => (
          <button
            key={e.id}
            onClick={() => {
              onSelect(e);
              onClose();
            }}
          >
            <span>
              <strong>{e.title}</strong>
              <small>
                {rangeLabel(e.plan)} · {statusLabels[e.status]}
              </small>
            </span>
            <ArrowRight size={16} />
          </button>
        ))}
        {!results.length && (
          <p className="empty-inline">Ничего не нашлось. Попробуйте другое название или тег.</p>
        )}
      </div>
    </Modal>
  );
}

export function TemplateDialog({
  snapshot,
  workspaceId,
  busy,
  mutate,
  onClose,
  anchor,
  onApplied,
}: Common & { anchor: DateTime; onApplied: (next: PlannerSnapshot) => void }) {
  const [space, setSpace] = useState(
    workspaceId === 'all'
      ? (snapshot.workspaces.find((w) => w.mode === 'personal')?.id ?? snapshot.workspaces[0]?.id)
      : workspaceId,
  );
  const [selected, setSelected] = useState(snapshot.templates[0]?.id ?? '');
  const [start, setStart] = useState(
    inputDate(
      anchor.set({ hour: 9, minute: 0 }).toISO(),
      snapshot.workspaces.find((w) => w.id === space)?.timezone,
    ),
  );
  const [error, setError] = useState('');
  const template = snapshot.templates.find((t) => t.id === selected);
  const timezone =
    snapshot.workspaces.find((w) => w.id === space)?.timezone ?? 'Asia/Yekaterinburg';
  async function apply(event: FormEvent) {
    event.preventDefault();
    try {
      const next = await mutate(`/api/templates/${selected}/apply`, 'POST', {
        workspaceId: space,
        start: fromInput(start, timezone),
      });
      onApplied(next);
      onClose();
    } catch (e) {
      setError((e as Error).message);
    }
  }
  return (
    <Modal
      title="Начать с готового процесса"
      subtitle="Поездка, отпуск, рабочий запуск или ваш собственный шаблон."
      onClose={onClose}
      wide
    >
      <form onSubmit={apply}>
        <div className="form-body">
          <div className="template-grid">
            {snapshot.templates
              .filter((t) => !t.workspaceId || t.workspaceId === space)
              .map((t) => (
                <button
                  type="button"
                  key={t.id}
                  className={selected === t.id ? 'template-card active' : 'template-card'}
                  onClick={() => setSelected(t.id)}
                >
                  <span className="template-icon">
                    <LayersSymbol name={t.name} />
                  </span>
                  <strong>{t.name}</strong>
                  <p>{t.description}</p>
                  <small>
                    {t.items.length} объектов · {t.dependencies.length} связей
                  </small>
                </button>
              ))}
          </div>
          <div className="form-grid">
            <label>
              Пространство
              <select value={space} onChange={(e) => setSpace(e.target.value)}>
                {snapshot.workspaces
                  .filter((w) => canEdit(snapshot, w.id))
                  .map((w) => (
                    <option key={w.id} value={w.id}>
                      {w.name}
                    </option>
                  ))}
              </select>
            </label>
            <label>
              Начало
              <input
                type="datetime-local"
                step="any"
                value={start}
                onChange={(e) => setStart(e.target.value)}
                required
              />
            </label>
          </div>
          {template && (
            <div className="template-preview">
              <h3>Что появится на шкале</h3>
              {template.items.map((item) => (
                <div key={item.key} className={item.parentKey ? 'child' : ''}>
                  <span>{item.title}</span>
                  <small>
                    {dateLabel(
                      DateTime.fromISO(start, { zone: timezone })
                        .plus({ days: item.offsetDays })
                        .toISO(),
                      timezone,
                    )}
                    {item.durationDays ? ` · ${item.durationDays} дн.` : ''}
                  </small>
                </div>
              ))}
            </div>
          )}
          {error && (
            <p className="inline-error" role="alert">
              {error}
            </p>
          )}
        </div>
        <footer className="modal-footer">
          <span className="helper">Даты и связи можно будет изменить</span>
          <button className="primary-button" disabled={busy || !template || !space}>
            <Plus size={16} />
            {busy ? 'Создаём…' : 'Добавить процесс'}
          </button>
        </footer>
      </form>
    </Modal>
  );
}
function LayersSymbol({ name }: { name: string }) {
  return <span>{/поезд|trip/i.test(name) ? '↗' : /отпуск|holiday/i.test(name) ? '☼' : '⌁'}</span>;
}

export function AssistantDialog({
  snapshot,
  workspaceId,
  busy,
  mutate,
  onClose,
  anchor,
  onApplied,
}: Common & { anchor: DateTime; onApplied: (next: PlannerSnapshot) => void }) {
  const [space, setSpace] = useState(
    workspaceId === 'all'
      ? (snapshot.workspaces.find((w) => w.mode === 'personal')?.id ?? snapshot.workspaces[0]?.id)
      : workspaceId,
  );
  const [text, setText] = useState('');
  const [suggestion, setSuggestion] = useState<AssistantSuggestion | null>(null);
  const [working, setWorking] = useState(false);
  const [error, setError] = useState('');
  async function parse(event: FormEvent) {
    event.preventDefault();
    setWorking(true);
    setError('');
    try {
      setSuggestion(
        await request<AssistantSuggestion>('/api/assistant', 'POST', {
          workspaceId: space,
          text,
          start: anchor.toISO(),
        }),
      );
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setWorking(false);
    }
  }
  async function apply() {
    if (!suggestion) return;
    try {
      const next = await mutate('/api/assistant/apply', 'POST', {
        workspaceId: space,
        drafts: suggestion.drafts,
      });
      onApplied(next);
      onClose();
    } catch (e) {
      setError((e as Error).message);
    }
  }
  return (
    <Modal
      title="Сформулируйте, что задумали"
      subtitle="Локальный разбор текста предлагает объекты и даты. Вы проверяете результат перед добавлением."
      onClose={onClose}
      wide
    >
      <div className="form-body">
        <form onSubmit={parse}>
          <label>
            Пространство
            <select
              value={space}
              onChange={(e) => {
                setSpace(e.target.value);
                setSuggestion(null);
              }}
            >
              {snapshot.workspaces
                .filter((w) => canEdit(snapshot, w.id))
                .map((w) => (
                  <option key={w.id} value={w.id}>
                    {w.name}
                  </option>
                ))}
            </select>
          </label>
          <label className="assistant-input">
            Ваш план
            <textarea
              autoFocus
              rows={4}
              placeholder="Поездка в Стамбул с 12 по 18 октября. За неделю забронировать отель…"
              value={text}
              onChange={(e) => {
                setText(e.target.value);
                setSuggestion(null);
              }}
              required
            />
          </label>
          <div className="assistant-examples">
            {[
              'Отпуск с 12 по 18 октября',
              'Встреча завтра в 15:00',
              'Поездка в Стамбул на 7 дней',
            ].map((example) => (
              <button
                type="button"
                key={example}
                onClick={() => {
                  setText(example);
                  setSuggestion(null);
                }}
              >
                {example}
              </button>
            ))}
          </div>
          <button className="secondary-button" disabled={working || !text.trim()}>
            <Sparkles size={15} />
            {working ? 'Разбираем…' : 'Предложить объекты'}
          </button>
        </form>
        {suggestion && (
          <div className="assistant-result">
            <div className="result-heading">
              <Lightbulb size={19} />
              <h3>{suggestion.summary}</h3>
            </div>
            {suggestion.assumptions.length > 0 && (
              <div className="assumptions">
                <strong>Проверьте допущения</strong>
                <ul>
                  {suggestion.assumptions.map((a, i) => (
                    <li key={i}>{a}</li>
                  ))}
                </ul>
              </div>
            )}
            {suggestion.drafts.map((draft, i) => (
              <div className="suggested-draft" key={i}>
                <label>
                  Название
                  <input
                    value={draft.title}
                    onChange={(e) =>
                      setSuggestion(
                        (s) =>
                          s && {
                            ...s,
                            drafts: s.drafts.map((d, index) =>
                              index === i ? { ...d, title: e.target.value } : d,
                            ),
                          },
                      )
                    }
                  />
                </label>
                <div className="form-grid">
                  <label>
                    Начало
                    <input
                      type="datetime-local"
                      step="any"
                      value={inputDate(draft.plan.start, draft.plan.timezone)}
                      onChange={(e) =>
                        setSuggestion(
                          (s) =>
                            s && {
                              ...s,
                              drafts: s.drafts.map((d, index) =>
                                index === i
                                  ? {
                                      ...d,
                                      plan: {
                                        ...d.plan,
                                        start: fromInput(e.target.value, d.plan.timezone),
                                      },
                                    }
                                  : d,
                              ),
                            },
                        )
                      }
                    />
                  </label>
                  <label>
                    Окончание
                    <input
                      type="datetime-local"
                      step="any"
                      value={inputDate(draft.plan.end, draft.plan.timezone)}
                      onChange={(e) =>
                        setSuggestion(
                          (s) =>
                            s && {
                              ...s,
                              drafts: s.drafts.map((d, index) =>
                                index === i
                                  ? {
                                      ...d,
                                      plan: {
                                        ...d.plan,
                                        end: fromInput(e.target.value, d.plan.timezone),
                                      },
                                    }
                                  : d,
                              ),
                            },
                        )
                      }
                    />
                  </label>
                </div>
                <p className="helper">{draft.description}</p>
              </div>
            ))}
            {suggestion.evidenceIds.length > 0 && (
              <p className="helper">
                Использованы доступные объекты:{' '}
                {suggestion.evidenceIds
                  .map((id) => snapshot.entities.find((e) => e.id === id)?.title ?? id)
                  .join(', ')}
              </p>
            )}
            <p className="helper">
              Разбор по правилам, без подключённой языковой модели. Предложения могут требовать
              уточнения.
            </p>
          </div>
        )}
        {error && (
          <p className="inline-error" role="alert">
            {error}
          </p>
        )}
      </div>
      <footer className="modal-footer">
        <span className="helper">Без автоматического изменения вашего плана</span>
        <button
          className="primary-button"
          disabled={
            busy || !suggestion?.drafts.length || suggestion.drafts.some((d) => !d.title.trim())
          }
          onClick={() => void apply()}
        >
          <Plus size={15} />
          Добавить {suggestion?.drafts.length ? `${suggestion.drafts.length} объектов` : 'на шкалу'}
        </button>
      </footer>
    </Modal>
  );
}

export function PreviewDialog({
  snapshot,
  workspaceId,
  busy,
  mutate,
  onClose,
  changes,
  onPreview,
  defaultName,
}: Common & {
  changes: PlanChange[];
  onPreview: (preview: ScenarioPreview | null) => void;
  defaultName: string;
}) {
  const [preview, setPreview] = useState<ScenarioPreview | null>(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);
  const [name, setName] = useState(defaultName);
  const [reason, setReason] = useState('');
  useEffect(() => {
    let live = true;
    void request<ScenarioPreview>('/api/scenarios/preview', 'POST', { workspaceId, changes })
      .then((result) => {
        if (live) {
          setPreview(result);
          onPreview(result);
        }
      })
      .catch((e) => {
        if (live) setError((e as Error).message);
      })
      .finally(() => {
        if (live) setLoading(false);
      });
    return () => {
      live = false;
    };
  }, []);
  async function save(action: 'draft' | 'submit' | 'approve') {
    if (!preview) return;
    setError('');
    try {
      const oldIds = new Set(snapshot.scenarios.map((s) => s.id));
      const next = await mutate('/api/scenarios', 'POST', { workspaceId, name, reason, changes });
      const scenario = next.scenarios.find((s) => !oldIds.has(s.id));
      if (!scenario)
        throw new Error('Не удалось найти сохранённый вариант. Обновите список предложений.');
      if (action !== 'draft') await mutate(`/api/scenarios/${scenario.id}/submit`);
      if (action === 'approve') await mutate(`/api/scenarios/${scenario.id}/approve`);
      onPreview(null);
      onClose();
    } catch (e) {
      setError((e as Error).message);
    }
  }
  const hardConflict = preview?.conflicts.some(
    (c) => c.kind === 'cycle' || c.kind === 'invalid-time',
  );
  return (
    <Modal
      title="Перед изменением — последствия"
      subtitle="Исходный план сохраняется. Предложение становится планом после согласования."
      onClose={onClose}
      wide
    >
      <div className="form-body">
        <div className="form-grid">
          <label>
            Название варианта
            <input value={name} onChange={(e) => setName(e.target.value)} required />
          </label>
          <label>
            Причина
            <input
              value={reason}
              onChange={(e) => setReason(e.target.value)}
              placeholder="Почему меняются сроки?"
            />
          </label>
        </div>
        {loading ? (
          <p className="helper">Проверяем зависимости и доступность ресурсов…</p>
        ) : (
          preview && (
            <>
              <div className="preview-summary">
                <div>
                  <strong>{preview.changes.length}</strong>
                  <span>изменений</span>
                </div>
                <div>
                  <strong>{preview.affectedIds.length}</strong>
                  <span>затронутых объектов</span>
                </div>
                <div>
                  <strong>{preview.conflicts.length}</strong>
                  <span>проверить</span>
                </div>
              </div>
              {preview.conflicts.map((c, i) => (
                <div className="inline-warning" key={i}>
                  <strong>{c.message}</strong>
                  <small>
                    {c.entityIds
                      .map((id) => snapshot.entities.find((e) => e.id === id)?.title ?? 'Объект')
                      .join(' · ')}
                  </small>
                </div>
              ))}
              <div className="plan-change-list">
                {preview.changes.map((change) => {
                  const entity = snapshot.entities.find((e) => e.id === change.entityId);
                  return (
                    <div key={change.entityId}>
                      <strong>{entity?.title}</strong>
                      <span>
                        {rangeLabel(entity?.plan ?? null)}
                        <ArrowRight size={13} />
                        {rangeLabel(change.plan)}
                      </span>
                    </div>
                  );
                })}
              </div>
              {preview.explanations.length > 0 && (
                <details open>
                  <summary>Как получился этот вариант</summary>
                  <ul className="explanation-list">
                    {preview.explanations.map((e, i) => (
                      <li key={i}>{e}</li>
                    ))}
                  </ul>
                </details>
              )}
            </>
          )
        )}
        {error && (
          <p className="inline-error" role="alert">
            {error}
          </p>
        )}
      </div>
      <footer className="modal-footer">
        <button
          className="secondary-button"
          onClick={() => void save('draft')}
          disabled={busy || !preview || !name.trim()}
        >
          Сохранить вариант
        </button>
        <div className="button-row">
          <button className="secondary-button" onClick={onClose}>
            Отмена
          </button>
          {canApprove(snapshot, workspaceId) ? (
            <button
              className="primary-button"
              onClick={() => void save('approve')}
              disabled={busy || !preview || hardConflict || !name.trim()}
            >
              <Check size={16} />
              Согласовать и применить
            </button>
          ) : (
            <button
              className="primary-button"
              onClick={() => void save('submit')}
              disabled={busy || !preview || hardConflict || !name.trim()}
            >
              <ArrowRight size={16} />
              Отправить на согласование
            </button>
          )}
        </div>
      </footer>
    </Modal>
  );
}

export function ScenariosDialog({
  snapshot,
  workspaceId,
  busy,
  mutate,
  onClose,
  onCompare,
}: Common & { onCompare: (scenario: Scenario) => void }) {
  const [error, setError] = useState('');
  const [active, setActive] = useState<string | null>(null);
  const scenarios = snapshot.scenarios
    .filter((s) => workspaceId === 'all' || s.workspaceId === workspaceId)
    .sort((a, b) => b.createdAt.localeCompare(a.createdAt));
  async function action(s: Scenario, next: string) {
    try {
      await mutate(
        `/api/scenarios/${s.id}/${next}`,
        'POST',
        next === 'reject' ? { reason: 'Отклонено при рассмотрении' } : undefined,
      );
      setError('');
    } catch (e) {
      setError((e as Error).message);
    }
  }
  return (
    <Modal
      title="Варианты и согласования"
      subtitle="Обсудите изменение, сравните с планом и примите решение."
      onClose={onClose}
      wide
    >
      <div className="form-body">
        {scenarios.length ? (
          scenarios.map((s) => (
            <article key={s.id} className="scenario-card">
              <header>
                <GitBranch size={18} />
                <div>
                  <h3>{s.name}</h3>
                  <p>{s.reason || 'Причина не указана'}</p>
                </div>
                <span className={`status-pill ${s.state}`}>
                  {
                    {
                      draft: 'Вариант',
                      pending: 'Ждёт решения',
                      approved: 'Применён',
                      rejected: 'Отклонён',
                      stale: 'План изменился',
                    }[s.state]
                  }
                </span>
              </header>
              <p className="helper">
                {s.preview.changes.length} изменений · {s.preview.conflicts.length} предупреждений ·{' '}
                {dateLabel(s.createdAt)}
              </p>
              <div className="button-row">
                <button
                  className="small-button"
                  onClick={() => setActive(active === s.id ? null : s.id)}
                >
                  Изменения
                </button>
                <button
                  className="small-button"
                  onClick={() => {
                    onCompare(s);
                    onClose();
                  }}
                >
                  Сравнить на шкале
                </button>
                {s.state === 'draft' && canEdit(snapshot, s.workspaceId) && (
                  <button
                    className="secondary-button"
                    disabled={busy}
                    onClick={() => void action(s, 'submit')}
                  >
                    Отправить на согласование
                  </button>
                )}
                {s.state === 'pending' && canApprove(snapshot, s.workspaceId) && (
                  <>
                    <button
                      className="primary-button"
                      disabled={busy}
                      onClick={() => void action(s, 'approve')}
                    >
                      <Check size={14} />
                      Согласовать
                    </button>
                    <button
                      className="secondary-button"
                      disabled={busy}
                      onClick={() => void action(s, 'reject')}
                    >
                      Отклонить
                    </button>
                  </>
                )}
              </div>
              {active === s.id && (
                <div className="plan-change-list">
                  {s.preview.changes.map((c) => (
                    <div key={c.entityId}>
                      <strong>
                        {snapshot.entities.find((e) => e.id === c.entityId)?.title ?? 'Объект'}
                      </strong>
                      <span>{rangeLabel(c.plan)}</span>
                    </div>
                  ))}
                  {s.preview.conflicts.map((c, i) => (
                    <p className="inline-warning" key={i}>
                      {c.message}
                    </p>
                  ))}
                </div>
              )}
            </article>
          ))
        ) : (
          <div className="dialog-empty">
            <GitBranch size={26} />
            <h3>Решения начинаются с вариантов</h3>
            <p>
              Перенесите объект на шкале или откройте «Перенести с проверкой» в его подробностях.
            </p>
          </div>
        )}
        {error && (
          <p className="inline-error" role="alert">
            {error}
          </p>
        )}
      </div>
    </Modal>
  );
}
