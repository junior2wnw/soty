import { useState, type FormEvent } from 'react';
import {
  ArrowRight,
  Check,
  ChevronRight,
  Clock3,
  Download,
  ExternalLink,
  FileText,
  History,
  Link2,
  MessageSquare,
  Pencil,
  Play,
  Plus,
  Send,
  Trash2,
  Users,
  X,
} from 'lucide-react';
import type { Entity, PlannerSnapshot, Signal } from '../../shared/types';
import type { Mutate } from '../api';
import { canEdit, dateLabel, kindLabels, rangeLabel, statusLabels } from '../utils';

type Props = {
  entity: Entity;
  snapshot: PlannerSnapshot;
  mutate: Mutate;
  busy: boolean;
  readOnly: boolean;
  onEdit: () => void;
  onClose: () => void;
  onSelect: (entity: Entity) => void;
  onDeleted: () => void;
  onMove: () => void;
};
const relationLabels = {
  'finish-start': 'Начать после завершения',
  'start-start': 'Начать вместе',
  'finish-finish': 'Завершить вместе',
  related: 'Связано',
};
const actionLabels: Record<string, string> = {
  'entity.create': 'Объект создан',
  'entity.update': 'Объект изменён',
  'entity.delete': 'Объект удалён',
  'scenario.approve': 'Изменение согласовано',
  'signal.ack': 'Предупреждение принято',
  'signal.resolve': 'Предупреждение закрыто',
};
export function SignalActions({
  signal,
  onAction,
  busy,
  editable = true,
}: {
  signal: Signal;
  onAction: (action: string, body?: unknown) => void;
  busy: boolean;
  editable?: boolean;
}) {
  return (
    <div className="signal-actions">
      <span className={`signal-state ${signal.state}`}>
        {signal.state === 'acknowledged'
          ? 'Принято в работу'
          : signal.state === 'resolved'
            ? 'Решено'
            : signal.state === 'accepted-risk'
              ? 'Риск принят'
              : 'Требует внимания'}
      </span>
      {editable && signal.state !== 'resolved' && signal.state !== 'accepted-risk' && (
        <>
          {signal.state === 'open' && (
            <button className="small-button" disabled={busy} onClick={() => onAction('ack')}>
              Принять в работу
            </button>
          )}
          <button className="small-button" disabled={busy} onClick={() => onAction('resolve')}>
            Решено
          </button>
          <button
            className="small-button"
            disabled={busy}
            onClick={() =>
              onAction('snooze', { until: new Date(Date.now() + 86400000).toISOString() })
            }
          >
            На завтра
          </button>
          <button className="text-button" disabled={busy} onClick={() => onAction('accept-risk')}>
            Принять риск
          </button>
        </>
      )}
    </div>
  );
}
function safeLink(url: string) {
  return /^(https?:\/\/|\/api\/files\/)/i.test(url) ? url : '#';
}

export default function EntityDetail({
  entity,
  snapshot,
  mutate,
  busy,
  readOnly,
  onEdit,
  onClose,
  onSelect,
  onDeleted,
  onMove,
}: Props) {
  const [tab, setTab] = useState('overview');
  const [comment, setComment] = useState('');
  const [relationTarget, setRelationTarget] = useState('');
  const [relationKind, setRelationKind] = useState<keyof typeof relationLabels>('finish-start');
  const [lag, setLag] = useState(0);
  const [error, setError] = useState('');
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [previewFile, setPreviewFile] = useState<string | null>(null);
  const editable = !readOnly && canEdit(snapshot, entity.workspaceId);
  const owner = snapshot.users.find((u) => u.id === entity.ownerId);
  const type = snapshot.types.find((t) => t.id === entity.typeId);
  const signals = snapshot.signals.filter(
    (s) => s.entityId === entity.id && s.state !== 'resolved' && s.state !== 'accepted-risk',
  );
  const comments = snapshot.comments.filter((c) => c.entityId === entity.id);
  const dependencies = snapshot.dependencies.filter(
    (d) => d.fromId === entity.id || d.toId === entity.id,
  );
  const children = snapshot.entities.filter((e) => e.parentId === entity.id);
  const history = snapshot.audit
    .filter((a) => a.entityId === entity.id)
    .sort((a, b) => b.createdAt.localeCompare(a.createdAt));
  async function act(path: string, method = 'POST', body?: unknown) {
    setError('');
    try {
      return await mutate(path, method, body);
    } catch (e) {
      setError((e as Error).message);
      return null;
    }
  }
  async function addComment(e: FormEvent) {
    e.preventDefault();
    if (!comment.trim()) return;
    const result = await act('/api/comments', 'POST', {
      workspaceId: entity.workspaceId,
      entityId: entity.id,
      text: comment.trim(),
    });
    if (result) setComment('');
  }
  async function addDependency(e: FormEvent) {
    e.preventDefault();
    const result = await act('/api/dependencies', 'POST', {
      workspaceId: entity.workspaceId,
      fromId: relationTarget,
      toId: entity.id,
      kind: relationKind,
      lagMinutes: lag,
    });
    if (result) setRelationTarget('');
  }
  async function changeStatus(status: Entity['status']) {
    const actual =
      status === 'active'
        ? {
            start: snapshot.serverTime,
            end: null,
            timezone: entity.plan.timezone,
            precision: 'exact',
          }
        : status === 'done'
          ? {
              start: entity.actual?.start ?? null,
              end: snapshot.serverTime,
              timezone: entity.plan.timezone,
              precision: 'exact',
            }
          : entity.actual;
    await act(`/api/entities/${entity.id}`, 'PATCH', {
      version: entity.version,
      patch: { status, actual },
      reason: status === 'done' ? 'Выполнение завершено' : 'Начало выполнения',
    });
  }
  return (
    <article className="inline-detail" aria-label={`Подробности ${entity.title}`}>
      <header className="detail-heading">
        <div>
          <div className="detail-kicker">
            <span>
              {type?.label ?? kindLabels[entity.kind]}
              {entity.recurrence ? ' · Серия' : ''}
              {entity.plan.precise ? ' · Точное относительное время' : ''}
            </span>
            <span className={`status-pill ${entity.status}`}>{statusLabels[entity.status]}</span>
            {readOnly && <span className="status-pill">Исторический снимок</span>}
          </div>
          <h2>{entity.title}</h2>
          <p className="muted">
            {entity.recurrence ? 'Первое повторение: ' : ''}
            {rangeLabel(entity.plan)} · {owner?.displayName ?? 'Ответственный не назначен'}
          </p>
        </div>
        <div className="button-row">
          {editable && (
            <button className="secondary-button" onClick={onEdit}>
              <Pencil size={14} />
              <span>{entity.recurrence ? 'Изменить серию' : 'Изменить'}</span>
            </button>
          )}
          <button className="icon-button" onClick={onClose} aria-label="Свернуть подробности">
            <X size={18} />
          </button>
        </div>
      </header>
      {signals.length > 0 && (
        <div className="detail-signals">
          {signals.map((s) => (
            <div key={s.id} className={`signal-card ${s.severity}`}>
              <div>
                <strong>{s.title}</strong>
                <p>{s.description}</p>
                {s.dueAt && (
                  <small>Действие до {dateLabel(s.dueAt, entity.plan.timezone, true)}</small>
                )}
              </div>
              <SignalActions
                signal={s}
                busy={busy}
                editable={editable}
                onAction={(action, body) =>
                  void act(`/api/signals/${s.id}/${action}`, 'POST', body)
                }
              />
            </div>
          ))}
        </div>
      )}
      <div className="context-tabs" role="tablist" aria-label="Подробности объекта">
        {[
          { id: 'overview', title: 'Обзор', icon: FileText },
          { id: 'time', title: 'Время', icon: Clock3 },
          {
            id: 'links',
            title: `Связи${dependencies.length ? ` · ${dependencies.length}` : ''}`,
            icon: Link2,
          },
          {
            id: 'discussion',
            title: `Обсуждение${comments.length ? ` · ${comments.length}` : ''}`,
            icon: MessageSquare,
          },
          { id: 'history', title: 'История', icon: History },
        ].map((t) => (
          <button
            key={t.id}
            role="tab"
            aria-selected={tab === t.id}
            className={tab === t.id ? 'active' : ''}
            onClick={() => setTab(t.id)}
          >
            <t.icon size={15} />
            <span>{t.title}</span>
          </button>
        ))}
      </div>
      <div className="detail-body">
        {tab === 'overview' && (
          <>
            {entity.description ? (
              <p className="description-text">{entity.description}</p>
            ) : (
              <p className="muted">
                Добавьте контекст, заметки или материалы через редактирование.
              </p>
            )}
            {!!entity.tags.length && (
              <div className="tag-row">
                {entity.tags.map((tag) => (
                  <span key={tag}>{tag}</span>
                ))}
              </div>
            )}
            {!!Object.keys(entity.fields).length && (
              <dl className="property-grid">
                {Object.entries(entity.fields).map(([key, value]) => (
                  <div key={key}>
                    <dt>{type?.fields.find((f) => f.id === key)?.label ?? key}</dt>
                    <dd>
                      {typeof value === 'boolean' ? (value ? 'Да' : 'Нет') : String(value ?? '—')}
                    </dd>
                  </div>
                ))}
              </dl>
            )}
            {entity.links.length > 0 && (
              <div className="attachment-list">
                {entity.links.map((link) => (
                  <div className="attachment" key={link.id}>
                    <FileText size={18} />
                    <div>
                      <strong>{link.label || 'Материал'}</strong>
                      <small>{link.kind === 'file' ? 'Файл в пространстве' : link.url}</small>
                    </div>
                    {link.kind === 'file' && (
                      <button
                        className="small-button"
                        onClick={() => setPreviewFile(previewFile === link.url ? null : link.url)}
                      >
                        Просмотр
                      </button>
                    )}
                    <a
                      className="icon-button"
                      href={safeLink(link.url)}
                      target="_blank"
                      rel="noreferrer"
                      aria-label={`Открыть ${link.label}`}
                    >
                      {link.kind === 'file' ? <Download size={17} /> : <ExternalLink size={17} />}
                    </a>
                  </div>
                ))}
              </div>
            )}
            {previewFile && (
              <div className="file-preview">
                <div>
                  <span>Просмотр материала</span>
                  <button
                    className="icon-button"
                    onClick={() => setPreviewFile(null)}
                    aria-label="Закрыть просмотр"
                  >
                    <X size={16} />
                  </button>
                </div>
                <iframe src={safeLink(previewFile)} title="Вложенный материал" />
              </div>
            )}
            {children.length > 0 && (
              <div className="children-list">
                <h3>Внутри этого процесса</h3>
                {children.map((child) => (
                  <button key={child.id} onClick={() => onSelect(child)}>
                    <span>{child.title}</span>
                    <small>{rangeLabel(child.plan)}</small>
                    <ChevronRight size={15} />
                  </button>
                ))}
              </div>
            )}
            {entity.allocations.length > 0 && (
              <>
                <h3>Ресурсы</h3>
                <div className="resource-chips">
                  {entity.allocations.map((a) => {
                    const r = snapshot.resources.find((r) => r.id === a.resourceId);
                    return (
                      <span key={a.resourceId}>
                        <Users size={14} />
                        {r?.name ?? 'Ресурс'} · {a.amount} / {r?.capacity} {r?.unit}
                      </span>
                    );
                  })}
                </div>
              </>
            )}
            {editable && entity.status !== 'done' && entity.status !== 'cancelled' && (
              <div className="button-row detail-execution">
                {entity.status !== 'active' && (
                  <button
                    className="secondary-button"
                    disabled={busy}
                    onClick={() => void changeStatus('active')}
                  >
                    <Play size={14} />
                    {entity.recurrence ? 'Начать всю серию' : 'Начать сейчас'}
                  </button>
                )}
                <button
                  className="secondary-button"
                  disabled={busy}
                  onClick={() => void changeStatus('done')}
                >
                  <Check size={15} />
                  {entity.recurrence ? 'Завершить всю серию' : 'Завершить сейчас'}
                </button>
              </div>
            )}
          </>
        )}
        {tab === 'time' && (
          <>
            {entity.recurrence && (
              <p className="helper">
                Даты первого повторения задают начало серии. Изменения в этих параметрах применяются
                ко всей серии.
              </p>
            )}
            <div className="time-comparison">
              {[
                { name: 'Исходный план', range: entity.baseline },
                { name: 'Текущий план', range: entity.plan },
                { name: 'Факт', range: entity.actual },
                { name: 'Прогноз', range: entity.forecast },
              ].map(({ name, range }) => (
                <div key={name}>
                  <span>{name}</span>
                  <strong>{range ? rangeLabel(range) : 'Не записан'}</strong>
                  <small>
                    {range?.timezone}
                    {range?.precise ? ' · наносекунды' : ''}
                    {range?.precision === 'approximate' ? ' · приблизительно' : ''}
                    {name === 'Прогноз' && range
                      ? entity.forecastProvenance === 'derived'
                        ? ' · рассчитан по плану и факту'
                        : ' · введён вручную'
                      : ''}
                  </small>
                </div>
              ))}
            </div>
            {entity.dueAt && (
              <p className="deadline-text">
                Крайний срок: {dateLabel(entity.dueAt, entity.plan.timezone, true)}
              </p>
            )}
            {entity.recurrence && (
              <p className="helper">
                Повторяется каждые {entity.recurrence.interval}{' '}
                {entity.recurrence.frequency === 'day'
                  ? 'дн.'
                  : entity.recurrence.frequency === 'week'
                    ? 'нед.'
                    : 'мес.'}
                {entity.recurrence.count ? ` · ${entity.recurrence.count} повторений` : ''}
                {entity.recurrence.until ? ` · до ${dateLabel(entity.recurrence.until)}` : ''}.
                Невозможная дата или время:{' '}
                {entity.recurrence.calendarPolicy === 'skip-invalid'
                  ? 'пропуск повторения'
                  : 'перенос на допустимую дату или время'}
                .
              </p>
            )}
            {editable && (
              <div className="button-row">
                <button className="secondary-button" onClick={onEdit}>
                  <Pencil size={15} />
                  {entity.recurrence ? 'Изменить даты серии' : 'Ввести даты'}
                </button>
                <button className="secondary-button" onClick={onMove}>
                  <ArrowRight size={15} />
                  {entity.recurrence ? 'Перенести серию с проверкой' : 'Перенести с проверкой'}
                </button>
              </div>
            )}
            <p className="helper">
              Факт записывается отдельно. Отсутствие факта означает отсутствие данных о выполнении.
            </p>
          </>
        )}
        {tab === 'links' && (
          <>
            {dependencies.length ? (
              <div className="relation-list">
                {dependencies.map((dep) => {
                  const before = snapshot.entities.find((e) => e.id === dep.fromId);
                  const after = snapshot.entities.find((e) => e.id === dep.toId);
                  const other = entity.id === dep.fromId ? after : before;
                  return (
                    <div key={dep.id}>
                      <Link2 size={16} />
                      <div>
                        <button
                          className="text-button align-left"
                          onClick={() => other && onSelect(other)}
                        >
                          {before?.title ?? 'Закрытый объект'} <ArrowRight size={12} />{' '}
                          {after?.title ?? 'Закрытый объект'}
                        </button>
                        <small>
                          {relationLabels[dep.kind]}
                          {dep.lagMinutes ? ` · ${dep.lagMinutes} мин. между объектами` : ''}
                        </small>
                      </div>
                      {editable && (
                        <button
                          className="icon-button danger"
                          disabled={busy}
                          onClick={() => void act(`/api/dependencies/${dep.id}`, 'DELETE')}
                          aria-label="Удалить связь"
                        >
                          <Trash2 size={15} />
                        </button>
                      )}
                    </div>
                  );
                })}
              </div>
            ) : (
              <p className="empty-inline">
                Свяжите объекты, чтобы видеть последовательность и последствия изменения.
              </p>
            )}
            {editable && (
              <form className="relation-form" onSubmit={addDependency}>
                <h3>Добавить связь</h3>
                <div className="form-grid">
                  <label>
                    Предшествующий / связанный объект
                    <select
                      value={relationTarget}
                      onChange={(e) => setRelationTarget(e.target.value)}
                      required
                    >
                      <option value="">Выберите объект</option>
                      {snapshot.entities
                        .filter((e) => e.workspaceId === entity.workspaceId && e.id !== entity.id)
                        .map((e) => (
                          <option key={e.id} value={e.id}>
                            {e.title}
                          </option>
                        ))}
                    </select>
                  </label>
                  <label>
                    Смысл связи
                    <select
                      value={relationKind}
                      onChange={(e) =>
                        setRelationKind(e.target.value as keyof typeof relationLabels)
                      }
                    >
                      {Object.entries(relationLabels).map(([key, label]) => (
                        <option key={key} value={key}>
                          {label}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label>
                    Интервал (минуты)
                    <input
                      type="number"
                      value={lag}
                      onChange={(e) => setLag(Number(e.target.value))}
                    />
                  </label>
                  <button
                    className="secondary-button form-bottom"
                    disabled={busy || !relationTarget}
                  >
                    <Plus size={15} />
                    Связать
                  </button>
                </div>
              </form>
            )}
          </>
        )}
        {tab === 'discussion' && (
          <>
            {comments.length ? (
              <div className="comments">
                {comments.map((c) => (
                  <article key={c.id}>
                    <div className="avatar small">
                      {snapshot.users.find((u) => u.id === c.userId)?.displayName.charAt(0)}
                    </div>
                    <div>
                      <header>
                        <strong>
                          {snapshot.users.find((u) => u.id === c.userId)?.displayName ?? 'Участник'}
                        </strong>
                        <time>{dateLabel(c.createdAt, entity.plan.timezone, true)}</time>
                      </header>
                      <p>{c.text}</p>
                    </div>
                  </article>
                ))}
              </div>
            ) : (
              <p className="empty-inline">
                Обсуждение сохраняется рядом с объектом и его изменениями.
              </p>
            )}
            {!readOnly && (
              <form onSubmit={addComment} className="comment-form">
                <label className="sr-only" htmlFor={`comment-${entity.id}`}>
                  Комментарий
                </label>
                <textarea
                  id={`comment-${entity.id}`}
                  value={comment}
                  onChange={(e) => setComment(e.target.value)}
                  rows={2}
                  placeholder="Комментарий к этому объекту…"
                  required
                />
                <button className="primary-button" disabled={busy || !comment.trim()}>
                  <Send size={15} />
                  Отправить
                </button>
              </form>
            )}
          </>
        )}
        {tab === 'history' && (
          <>
            {history.length ? (
              <ol className="audit-list">
                {history.map((entry) => (
                  <li key={entry.id}>
                    <span className="audit-dot" />
                    <div>
                      <strong>
                        {actionLabels[entry.action] ??
                          entry.action.replaceAll('-', ' ').replaceAll('.', ' ')}
                      </strong>
                      <p>{entry.reason || 'Изменение сохранено'}</p>
                      <small>
                        {snapshot.users.find((u) => u.id === entry.userId)?.displayName ??
                          'Участник'}{' '}
                        · {dateLabel(entry.createdAt, entity.plan.timezone, true)}
                      </small>
                      {!!entry.before && !!entry.after && (
                        <details>
                          <summary>Посмотреть изменения</summary>
                          <pre>{summarizeChanges(entry.before, entry.after)}</pre>
                        </details>
                      )}
                    </div>
                  </li>
                ))}
              </ol>
            ) : (
              <p className="empty-inline">История будет появляться после первых изменений.</p>
            )}
            <p className="source-info">
              Источник: {entity.source.label}. Наблюдение{' '}
              {dateLabel(entity.source.observedAt, entity.plan.timezone, true)}. Получено{' '}
              {dateLabel(entity.source.receivedAt, entity.plan.timezone, true)}.
            </p>
          </>
        )}
        {error && (
          <p className="inline-error" role="alert">
            {error}
          </p>
        )}
      </div>
      {editable && (
        <footer className="detail-footer">
          <span>Версия {entity.version} · изменения сохраняются на сервере</span>
          {confirmDelete ? (
            <div className="button-row">
              <span className="danger">Удалить объект и его связи?</span>
              <button
                className="small-button danger"
                disabled={busy}
                onClick={async () => {
                  const result = await act(`/api/entities/${entity.id}`, 'DELETE', {
                    version: entity.version,
                  });
                  if (result) onDeleted();
                }}
              >
                Удалить
              </button>
              <button className="small-button" onClick={() => setConfirmDelete(false)}>
                Отмена
              </button>
            </div>
          ) : (
            <button className="text-button danger" onClick={() => setConfirmDelete(true)}>
              <Trash2 size={13} />
              Удалить
            </button>
          )}
        </footer>
      )}
    </article>
  );
}

function summarizeChanges(before: unknown, after: unknown) {
  if (!before || !after || typeof before !== 'object' || typeof after !== 'object')
    return 'Изменение записано в журнал.';
  const old = before as Record<string, unknown>,
    next = after as Record<string, unknown>;
  return (
    [
      'title',
      'status',
      'plan',
      'actual',
      'forecast',
      'description',
      'parentId',
      'ownerId',
      'tags',
      'fields',
      'allocations',
    ]
      .filter((k) => JSON.stringify(old[k]) !== JSON.stringify(next[k]))
      .map(
        (k) =>
          `${({ title: 'Название', status: 'Состояние', plan: 'План', actual: 'Факт', forecast: 'Прогноз', description: 'Описание', parentId: 'Родитель', ownerId: 'Ответственный', tags: 'Теги', fields: 'Поля', allocations: 'Ресурсы' } as Record<string, string>)[k]}\nБыло: ${JSON.stringify(old[k] ?? null)}\nСтало: ${JSON.stringify(next[k] ?? null)}`,
      )
      .join('\n\n') || 'Метаданные обновлены.'
  );
}
