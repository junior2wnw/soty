import {
  lazy,
  Suspense,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type FormEvent,
} from 'react';
import { DateTime } from 'luxon';
import {
  Bell,
  Check,
  ChevronLeft,
  ChevronRight,
  Clock3,
  GitBranch,
  History,
  Keyboard,
  Layers3,
  LoaderCircle,
  Maximize2,
  Minus,
  MoreHorizontal,
  Plus,
  Search,
  Settings2,
  SlidersHorizontal,
  Sparkles,
  WifiOff,
  X,
} from 'lucide-react';
import type {
  Entity,
  EntityDraft,
  PlanChange,
  PlannerSnapshot,
  Scenario,
  ScenarioPreview,
} from '../shared/types';
import { request, usePlanner, isEmbedded } from './api';
import { startEmbedLogin } from '../shared/embed-login.mjs';
import {
  canEdit,
  dateLabel,
  draftOf,
  emptyRange,
  fromInput,
  inputDate,
  newDraft,
  requiresAttention,
  statusLabels,
  withPreciseCoordinates,
} from './utils';
import Timeline, { type GroupMode } from './components/Timeline';
import {
  fitTimeline,
  calendarAt,
  focusRange,
  fromMillis,
  instantLabel,
  timeReference,
  parseDuration,
  periodLabel,
  panViewport,
  zoomToSpan,
  SECOND,
  MINUTE,
  DAY,
  normalizeViewport,
  scaleToSpan,
  spanLabel,
  spanToScale,
  viewportBounds,
  zoomViewport,
  type TimelineViewport,
} from '../shared/universal-timeline';
import {
  canonicalRange,
  hasTime,
  hasSubMillisecondTime,
  rangeFromNs,
  nsToISO,
  preciseRangeErrors,
} from '../shared/precise-time';
import { localRangeInput, withLocalRangeInput } from '../shared/local-range';
import PreciseRangeFields from './components/PreciseRangeFields';
import EntityDetail from './components/EntityDetail';
const EntityEditor = lazy(() => import('./components/EntityEditor'));
import QuickEditor from './components/QuickEditor';
import { editableEntityPatch } from '../shared/entity-edit';
import Modal from './components/Modal';
const WorkspaceSettings = lazy(() => import('./components/WorkspaceSettings'));
import {
  AssistantDialog,
  AttentionDialog,
  PreviewDialog,
  ScenariosDialog,
  SearchDialog,
  TemplateDialog,
} from './components/PlannerDialogs';

type Filters = {
  statuses: string[];
  types: string[];
  owner: string;
  tags: string;
  attention: boolean;
  unknown: boolean;
};
const emptyFilters: Filters = {
  statuses: [],
  types: [],
  owner: '',
  tags: '',
  attention: false,
  unknown: false,
};
type SavedView = {
  id: string;
  name: string;
  workspaceId: string;
  groupBy: GroupMode;
  filters: Filters;
};
type Dialog =
  | 'search'
  | 'settings'
  | 'attention'
  | 'templates'
  | 'assistant'
  | 'scenarios'
  | 'filters'
  | 'history'
  | 'help'
  | 'date'
  | null;

export default function App() {
  const planner = usePlanner();
  const [embedLoginError, setEmbedLoginError] = useState<string | null>(null);
  const [embedLoginPending, setEmbedLoginPending] = useState(false);
  const [workspaceId, setWorkspaceId] = useState('all');
  const [viewport, setViewport] = useState<TimelineViewport>({
    center: fromMillis(Date.now()),
    span: 35n * DAY,
  });
  const zone = planner.snapshot?.settings.notifications.timezone ?? 'Asia/Yekaterinburg';
  const anchor = calendarAt(viewport.center, zone) ?? DateTime.now().setZone(zone);
  const setAnchor = useCallback((date: DateTime) => {
    setViewport((previous) =>
      normalizeViewport({ ...previous, center: fromMillis(date.toMillis()) }),
    );
  }, []);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [dialog, setDialog] = useState<Dialog>(null);
  const [toolsMenu, setToolsMenu] = useState(false);
  const [editor, setEditor] = useState<{
    draft: EntityDraft;
    entity?: Entity;
    advanced?: boolean;
    timeContext?: { anchorNs: string; label: string };
  } | null>(null);
  const [prospective, setProspective] = useState<{
    workspaceId: string;
    changes: PlanChange[];
    name: string;
  } | null>(null);
  const [preview, setPreview] = useState<ScenarioPreview | null>(null);
  const [comparison, setComparison] = useState<Scenario | null>(null);
  const [moveEntity, setMoveEntity] = useState<Entity | null>(null);
  const [filters, setFilters] = useState<Filters>(emptyFilters);
  const [savedViews, setSavedViews] = useState<SavedView[]>([]);
  const [viewsUserId, setViewsUserId] = useState<string | null>(null);
  const [historical, setHistorical] = useState<PlannerSnapshot | null>(null);
  const [historicalAt, setHistoricalAt] = useState('');
  const initialized = useRef<string | null>(null);
  const delivered = useRef(new Set<string>());
  const closeDialog = useCallback(() => setDialog(null), []);
  const closeEditor = useCallback(() => setEditor(null), []);
  const closePreview = useCallback(() => {
    setProspective(null);
    setPreview(null);
  }, []);
  const closeMove = useCallback(() => setMoveEntity(null), []);
  const snapshot =
    historical && initialized.current === planner.snapshot?.user.id ? historical : planner.snapshot;
  const readOnly = historical !== null && snapshot === historical;
  const overviewViewport = useMemo(
    () =>
      fitTimeline(
        snapshot?.entities ?? [],
        snapshot ? DateTime.fromISO(snapshot.serverTime).toMillis() : Date.now(),
      ),
    [snapshot?.entities],
  );

  useEffect(() => {
    const current = planner.snapshot;
    if (!current) {
      initialized.current = null;
      setViewsUserId(null);
      return;
    }
    if (initialized.current === current.user.id) return;
    initialized.current = current.user.id;
    setViewport(fitTimeline(current.entities, DateTime.fromISO(current.serverTime).toMillis()));
    setWorkspaceId('all');
    setSelectedId(null);
    setHistorical(null);
    setComparison(null);
    setDialog(null);
    setEditor(null);
    setProspective(null);
    setPreview(null);
    setMoveEntity(null);
    setFilters(emptyFilters);
    setToolsMenu(false);
    delivered.current.clear();
    let views: SavedView[] = [];
    try {
      const stored: unknown = JSON.parse(
        localStorage.getItem(`planner-saved-views-v1:${current.user.id}`) ?? '[]',
      );
      if (Array.isArray(stored))
        views = stored.filter((view): view is SavedView => {
          if (!view || typeof view !== 'object') return false;
          const f = view.filters;
          return (
            typeof view.id === 'string' &&
            typeof view.name === 'string' &&
            typeof view.workspaceId === 'string' &&
            (view.workspaceId === 'all' ||
              current.workspaces.some((w) => w.id === view.workspaceId)) &&
            ['process', 'workspace', 'type', 'owner'].includes(view.groupBy) &&
            f &&
            Array.isArray(f.statuses) &&
            f.statuses.every((item: unknown) => typeof item === 'string') &&
            Array.isArray(f.types) &&
            f.types.every((item: unknown) => typeof item === 'string') &&
            typeof f.owner === 'string' &&
            typeof f.tags === 'string' &&
            typeof f.attention === 'boolean' &&
            typeof f.unknown === 'boolean'
          );
        });
    } catch {
      /* A damaged browser preference must not prevent loading the timeline. */
    }
    setSavedViews(views);
    setViewsUserId(current.user.id);
  }, [planner.snapshot]);
  useEffect(() => {
    if (!viewsUserId || viewsUserId !== planner.snapshot?.user.id) return;
    try {
      localStorage.setItem(`planner-saved-views-v1:${viewsUserId}`, JSON.stringify(savedViews));
    } catch {
      /* Storage may be disabled or full. */
    }
  }, [savedViews, viewsUserId, planner.snapshot?.user.id]);
  useEffect(() => {
    if (
      planner.snapshot &&
      workspaceId !== 'all' &&
      !planner.snapshot.workspaces.some((w) => w.id === workspaceId)
    )
      setWorkspaceId('all');
  }, [planner.snapshot, workspaceId]);
  useEffect(() => {
    if (
      !planner.snapshot ||
      !planner.snapshot.settings.notifications.browserEnabled ||
      !('Notification' in window) ||
      Notification.permission !== 'granted'
    )
      return;
    for (const notification of planner.snapshot.notifications) {
      if (
        notification.channel !== 'browser' ||
        notification.userId !== planner.snapshot.user.id ||
        notification.state !== 'pending' ||
        delivered.current.has(notification.id) ||
        Date.parse(notification.scheduledAt) > Date.now()
      )
        continue;
      delivered.current.add(notification.id);
      const signal = planner.snapshot.signals.find((s) => s.id === notification.signalId);
      try {
        const alert = new Notification(signal?.title ?? 'Планировщик', {
          body: notification.summary,
          tag: notification.signalId,
        });
        alert.onclick = () => {
          window.focus();
          if (signal) setSelectedId(signal.entityId);
          setDialog('attention');
          alert.close();
        };
        void planner
          .mutate(`/api/notifications/${notification.id}/delivered`, 'POST', { state: 'delivered' })
          .catch(() => {});
      } catch {
        void planner
          .mutate(`/api/notifications/${notification.id}/delivered`, 'POST', {
            state: 'failed',
            error: 'Браузер не смог показать оповещение',
          })
          .catch(() => {});
      }
    }
  }, [planner.snapshot]);

  const writableWorkspace = snapshot?.workspaces.find(
    (w) => (workspaceId === 'all' || workspaceId === w.id) && canEdit(snapshot, w.id),
  );
  function editContext(entity: Entity) {
    const coordinates = canonicalRange(entity.plan);
    const at = coordinates.start ?? coordinates.end;
    if (at == null || !entity.plan.precise) return undefined;
    const extent =
      coordinates.start != null && coordinates.end != null
        ? coordinates.end - coordinates.start
        : 0n;
    return timeReference(at, entity.plan.timezone, extent < MINUTE || viewport.span < MINUTE);
  }
  const selectEntity = useCallback((entity: Entity) => {
    setSelectedId(entity.id);
    const range = [entity.plan, entity.actual, entity.forecast].find(
      (value) => value && hasTime(value),
    );
    if (range) setViewport((previous) => focusRange(range, previous));
  }, []);
  const createAt = useCallback(
    (at?: bigint) => {
      if (!snapshot || readOnly) return;
      if (!writableWorkspace) {
        setDialog('settings');
        return;
      }
      const local = at == null ? null : calendarAt(at, writableWorkspace.timezone);
      const draft = newDraft(snapshot, writableWorkspace.id, local ?? DateTime.now());
      const referenceAt = at ?? viewport.center;
      const timeContext = timeReference(referenceAt, draft.plan.timezone, viewport.span < MINUTE);
      if (at == null) draft.plan = emptyRange(draft.plan.timezone);
      else if (!local || viewport.span < MINUTE) {
        draft.plan = rangeFromNs(at, null, draft.plan.timezone, 'exact');
      }
      setEditor({ draft, timeContext });
    },
    [snapshot, writableWorkspace, readOnly, viewport.span, viewport.center],
  );
  useEffect(() => {
    const handle = (event: KeyboardEvent) => {
      if ((event.target as HTMLElement).closest('input,textarea,select,[contenteditable="true"]'))
        return;
      if (event.key === 'Escape') {
        setToolsMenu(false);
        if (!dialog && !editor && !prospective && !moveEntity) setSelectedId(null);
        return;
      }
      if (dialog || editor || prospective || moveEntity) return;
      if (
        ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'k') ||
        event.key === '/'
      ) {
        event.preventDefault();
        setDialog('search');
      } else if (
        !event.ctrlKey &&
        !event.metaKey &&
        !event.altKey &&
        event.key.toLowerCase() === 'n'
      ) {
        event.preventDefault();
        createAt();
      } else if (
        !event.ctrlKey &&
        !event.metaKey &&
        !event.altKey &&
        event.key.toLowerCase() === 't'
      )
        setAnchor(DateTime.now().setZone(anchor.zoneName!));
      else if (
        !event.ctrlKey &&
        !event.metaKey &&
        !event.altKey &&
        (event.key === '+' || event.key === '=')
      ) {
        event.preventDefault();
        setViewport((view) => zoomViewport(view, 1.7));
      } else if (!event.ctrlKey && !event.metaKey && !event.altKey && event.key === '-') {
        event.preventDefault();
        setViewport((view) => zoomViewport(view, 1 / 1.7));
      } else if (
        !event.ctrlKey &&
        !event.metaKey &&
        !event.altKey &&
        (event.key.toLowerCase() === 'f' || event.key === '0') &&
        planner.snapshot
      ) {
        event.preventDefault();
        setWorkspaceId('all');
        setFilters(emptyFilters);
        setViewport(
          fitTimeline(
            planner.snapshot.entities,
            DateTime.fromISO(planner.snapshot.serverTime).toMillis(),
          ),
        );
      } else if (event.altKey && event.key === 'ArrowLeft') {
        event.preventDefault();
        setViewport((view) => panViewport(view, -0.7, 1));
      } else if (event.altKey && event.key === 'ArrowRight') {
        event.preventDefault();
        setViewport((view) => panViewport(view, 0.7, 1));
      }
    };
    window.addEventListener('keydown', handle);
    return () => window.removeEventListener('keydown', handle);
  }, [anchor, dialog, editor, prospective, moveEntity, createAt, setAnchor, planner.snapshot]);

  if (planner.loading)
    return (
      <div className="boot-screen">
        <div className="brand">
          <span className="brand-symbol" />
          планировщик
        </div>
        <div className="boot-line" />
        <p>
          <LoaderCircle className="spin" size={18} />
          Собираем ваше время…
        </p>
      </div>
    );
  if (!snapshot)
    return isEmbedded() ? (
      <main className="boot-state">
        <h1>Подтвердите профиль Сот</h1>
        <p role="status">{embedLoginError ?? planner.error ?? 'Войдите, чтобы открыть выбранное пространство.'}</p>
        <button
          disabled={embedLoginPending}
          onClick={() => { setEmbedLoginPending(true); setEmbedLoginError(null); void startEmbedLogin().catch(error => setEmbedLoginError(error.message)).finally(() => setEmbedLoginPending(false)); }}
          style={{ display: 'inline-block', minHeight: 44, padding: '12px 16px' }}
        >
          Войти через Соты
        </button>
      </main>
    ) : (
      <AuthEntry
        mutate={planner.mutate}
        error={planner.error}
        refresh={planner.refresh}
        busy={planner.busy}
      />
    );

  const selected = snapshot.entities.find((e) => e.id === selectedId);
  const visible = snapshot.entities.filter((e) => {
    if (workspaceId !== 'all' && e.workspaceId !== workspaceId) return false;
    if (filters.statuses.length && !filters.statuses.includes(e.status)) return false;
    if (filters.types.length && !filters.types.includes(e.typeId)) return false;
    if (filters.owner && e.ownerId !== filters.owner) return false;
    if (
      filters.tags &&
      !filters.tags
        .split(',')
        .map((t) => t.trim().toLocaleLowerCase())
        .filter(Boolean)
        .every((t) => e.tags.some((tag) => tag.toLocaleLowerCase().includes(t)))
    )
      return false;
    if (
      filters.attention &&
      !snapshot.signals.some((s) => s.entityId === e.id && requiresAttention(s))
    )
      return false;
    if (filters.unknown && [e.plan, e.actual, e.forecast].some((range) => range && hasTime(range)))
      return false;
    return true;
  });
  const filterCount =
    filters.statuses.length +
    filters.types.length +
    (filters.owner ? 1 : 0) +
    (filters.tags ? 1 : 0) +
    (filters.attention ? 1 : 0) +
    (filters.unknown ? 1 : 0);
  const openSignals = snapshot.signals.filter(
    (s) => (workspaceId === 'all' || s.workspaceId === workspaceId) && requiresAttention(s),
  );
  const pendingScenarios = snapshot.scenarios.filter(
    (s) => (workspaceId === 'all' || s.workspaceId === workspaceId) && s.state === 'pending',
  );
  const periodTitle = periodLabel(viewport, zone);
  const filterCountTotal = filterCount + (workspaceId !== 'all' ? 1 : 0);
  const overviewScale = spanToScale(overviewViewport.span);
  function showEverything() {
    setWorkspaceId('all');
    setFilters(emptyFilters);
    setViewport({ ...overviewViewport });
  }

  async function saveEntity(draft: EntityDraft, proposal: boolean, reason: string) {
    if (editor?.entity) {
      const entity = editor.entity;
      const patch = editableEntityPatch(entity, draft);
      if (proposal) {
        const { plan: _plan, ...changed } = patch;
        if (Object.keys(changed).length)
          await planner.mutate(`/api/entities/${entity.id}`, 'PATCH', {
            version: entity.version,
            patch: changed,
            reason,
          });
        setEditor(null);
        setProspective({
          workspaceId: entity.workspaceId,
          changes: [{ entityId: entity.id, plan: draft.plan }],
          name: `Перенос: ${entity.title}`,
        });
        return;
      }
      if (Object.keys(patch).length)
        await planner.mutate(`/api/entities/${entity.id}`, 'PATCH', {
          version: entity.version,
          patch,
          reason,
        });
      setSelectedId(entity.id);
    } else {
      const beforeIds = new Set(planner.snapshot?.entities.map((e) => e.id));
      const next = await planner.mutate('/api/entities', 'POST', draft);
      const created = next.entities.find((e) => !beforeIds.has(e.id));
      if (created) {
        setSelectedId(created.id);
        if (hasTime(created.plan)) setViewport((previous) => focusRange(created.plan, previous));
      }
    }
    setEditor(null);
  }
  function propose(entity: Entity, plan: Entity['plan']) {
    if (!canEdit(snapshot!, entity.workspaceId) || readOnly) return;
    setSelectedId(entity.id);
    setProspective({
      workspaceId: entity.workspaceId,
      changes: [{ entityId: entity.id, plan }],
      name: `Перенос: ${entity.title}`,
    });
  }
  function applied(next: PlannerSnapshot) {
    const ids = new Set(snapshot!.entities.map((e) => e.id));
    const created = next.entities.find((e) => !ids.has(e.id));
    if (created) {
      if (created.plan.start)
        setAnchor(DateTime.fromISO(created.plan.start, { zone: created.plan.timezone }));
      setSelectedId(created.id);
    }
  }

  return (
    <main className="planner-app">
      <a href="#timeline" className="skip-link">
        Перейти к временной шкале
      </a>
      <header className="app-toolbar uni-toolbar minimal-toolbar">
        <button
          className="minimal-home"
          onClick={showEverything}
          aria-label="Показать всё на шкале"
          title="Вся шкала · F"
        >
          <Maximize2 size={18} />
        </button>
        <h1 className="minimal-period" title={periodTitle}>
          {periodTitle}
        </h1>
        <div className="toolbar-right">
          <button
            className="icon-button"
            aria-label="Найти на шкале"
            title="Найти · Ctrl K"
            onClick={() => setDialog('search')}
          >
            <Search size={19} />
          </button>
          <button
            className="icon-button minimal-add"
            aria-label="Добавить на шкалу"
            title="Добавить · N"
            onClick={() => createAt()}
            disabled={readOnly || !writableWorkspace}
          >
            <Plus size={22} />
          </button>
          <div className="uni-more-control">
            <button
              id="timeline-menu-trigger"
              className="icon-button"
              aria-label="Возможности и настройки"
              title="Возможности и настройки"
              aria-expanded={toolsMenu}
              aria-haspopup="menu"
              aria-controls={toolsMenu ? 'timeline-menu' : undefined}
              onClick={() => setToolsMenu(!toolsMenu)}
            >
              <MoreHorizontal size={21} />
              {openSignals.length > 0 && <i className="notification-dot" />}
              {filterCountTotal > 0 && <i className="uni-filter-count">{filterCountTotal}</i>}
            </button>
            {toolsMenu && (
              <>
                <button
                  className="popover-dismiss"
                  tabIndex={-1}
                  aria-label="Закрыть меню возможностей"
                  onClick={() => setToolsMenu(false)}
                />
                <div
                  id="timeline-menu"
                  className="minimal-menu"
                  role="menu"
                  aria-label="Возможности шкалы"
                  onKeyDown={(event) => {
                    const items = [
                      ...event.currentTarget.querySelectorAll<HTMLButtonElement>(
                        'button:not([disabled])',
                      ),
                    ];
                    const index = items.indexOf(document.activeElement as HTMLButtonElement);
                    if (['ArrowDown', 'ArrowUp', 'Home', 'End'].includes(event.key)) {
                      event.preventDefault();
                      const next =
                        event.key === 'Home'
                          ? 0
                          : event.key === 'End'
                            ? items.length - 1
                            : (index + (event.key === 'ArrowDown' ? 1 : -1) + items.length) %
                              items.length;
                      items[next]?.focus();
                    } else if (event.key === 'Escape') {
                      event.preventDefault();
                      setToolsMenu(false);
                      document.getElementById('timeline-menu-trigger')?.focus();
                    }
                  }}
                >
                  {(
                    [
                      ['date', Clock3, 'Дата и масштаб', 0],
                      ['filters', SlidersHorizontal, 'Фильтры', filterCountTotal],
                      ['attention', Bell, 'Оповещения', openSignals.length],
                      ['settings', Settings2, 'Настройки и MCP', 0],
                      ['templates', Layers3, 'Шаблоны', 0],
                      ['assistant', Sparkles, 'Из описания', 0],
                      ['scenarios', GitBranch, 'Варианты', pendingScenarios.length],
                      ['history', History, 'История', 0],
                      ['help', Keyboard, 'Жесты и клавиши', 0],
                    ] as const
                  )
                    .filter(([next]) => !isEmbedded() || next !== 'settings')
                    .map(([next, Icon, label, count], index) => (
                      <button
                        key={next}
                        role="menuitem"
                        autoFocus={index === 0}
                        disabled={
                          readOnly &&
                          ['attention', 'settings', 'templates', 'assistant', 'scenarios'].includes(
                            next,
                          )
                        }
                        onClick={() => {
                          setToolsMenu(false);
                          setDialog(next);
                        }}
                      >
                        <Icon size={17} />
                        <span>{label}</span>
                        {count > 0 && <small>{count}</small>}
                      </button>
                    ))}
                </div>
              </>
            )}
          </div>
        </div>
      </header>

      {filterCountTotal > 0 && (
        <div className="uni-active-filter">
          <span>
            На шкале {visible.length} из {snapshot.entities.length} объектов
          </span>
          <button className="text-button" onClick={showEverything}>
            <X size={14} />
            Показать всё
          </button>
        </div>
      )}
      {readOnly && (
        <div className="context-banner">
          <History size={16} />
          <span>
            Состояние на {dateLabel(historicalAt, anchor.zoneName!, true)} · только просмотр
          </span>
          <button className="small-button" onClick={() => setHistorical(null)}>
            Вернуться в настоящее
          </button>
        </div>
      )}
      {comparison && (
        <div className="context-banner">
          <GitBranch size={16} />
          <span>Сравнение: {comparison.name} · пунктиром предложенные даты</span>
          <button className="small-button" onClick={() => setComparison(null)}>
            Закрыть сравнение
          </button>
        </div>
      )}
      {!planner.connected && (
        <div className="offline-banner" role="status">
          <WifiOff size={16} />
          <span>Связь с сервером потеряна. Показаны последние полученные данные.</span>
          <button className="small-button" onClick={() => void planner.refresh()}>
            Повторить
          </button>
        </div>
      )}
      {planner.error && (
        <div className="global-error" role="alert">
          <span>{planner.error}</span>
          <button className="small-button" onClick={() => void planner.refresh()}>
            Обновить
          </button>
          <button
            className="icon-button"
            onClick={() => planner.setError(null)}
            aria-label="Закрыть сообщение"
          >
            <X size={16} />
          </button>
        </div>
      )}

      <div id="timeline" tabIndex={-1}>
        <Timeline
          snapshot={snapshot}
          entities={visible}
          viewport={viewport}
          selectedId={selectedId}
          onSelect={(entity) => setSelectedId(entity.id)}
          onCreate={createAt}
          onDeselect={() => setSelectedId(null)}
          onPropose={propose}
          onNavigate={setViewport}
          readOnly={readOnly || !writableWorkspace}
          proposed={preview?.changes ?? comparison?.preview.changes}
          detail={
            selected && (
              <EntityDetail
                key={selected.id}
                entity={selected}
                snapshot={snapshot}
                mutate={planner.mutate}
                busy={planner.busy}
                readOnly={readOnly}
                onEdit={() =>
                  setEditor({
                    entity: selected,
                    draft: draftOf(selected),
                    timeContext: editContext(selected),
                  })
                }
                onClose={() => setSelectedId(null)}
                onSelect={selectEntity}
                onDeleted={() => setSelectedId(null)}
                onMove={() => setMoveEntity(selected)}
              />
            )
          }
        />
      </div>
      <div className="universal-scale">
        <input
          type="range"
          min="0"
          max="100"
          step="0.01"
          aria-label="Масштаб временной шкалы"
          aria-valuetext={spanLabel(viewport.span)}
          value={spanToScale(viewport.span)}
          onChange={(event) =>
            setViewport((previous) => zoomToSpan(previous, scaleToSpan(Number(event.target.value))))
          }
        />
        <output aria-live="off">{spanLabel(viewport.span)}</output>
      </div>
      <footer className="timeline-footer minimal-footer">
        <span
          role="status"
          aria-label={
            planner.busy
              ? 'Сохраняем изменения'
              : readOnly
                ? 'Исторический снимок'
                : planner.connected
                  ? 'Изменения сохранены'
                  : 'Нет связи'
          }
          title={
            planner.busy
              ? 'Сохраняем изменения'
              : readOnly
                ? 'Исторический снимок'
                : planner.connected
                  ? 'Изменения сохранены'
                  : 'Нет связи'
          }
        >
          {planner.busy ? (
            <LoaderCircle size={13} className="spin" />
          ) : (
            <i className={planner.connected ? 'connection-dot' : 'connection-dot offline'} />
          )}
        </span>
        {visible.some((entity) => entity.source.kind === 'sample') && (
          <span className="footer-example-count">
            Примеры: {visible.filter((entity) => entity.source.kind === 'sample').length}
          </span>
        )}
      </footer>

      {editor && !editor.advanced && (
        <QuickEditor
          key={editor.entity?.id ?? 'new'}
          timeContext={editor.timeContext}
          preferPrecise={viewport.span < MINUTE || calendarAt(viewport.center, zone) == null}
          snapshot={snapshot}
          initial={editor.draft}
          entity={editor.entity}
          onSave={saveEntity}
          onClose={closeEditor}
          onExpand={(draft) => setEditor({ ...editor, draft, advanced: true })}
          busy={planner.busy}
        />
      )}
      {editor && editor.advanced && (
        <Suspense
          fallback={
            <Modal title="Параметры объекта" onClose={closeEditor}>
              <div className="deferred-panel" role="status">
                <LoaderCircle size={20} className="spin" />
              </div>
            </Modal>
          }
        >
          <EntityEditor
            key={editor.entity?.id ?? 'new'}
            timeContext={editor.timeContext}
            snapshot={snapshot}
            initial={editor.draft}
            entity={editor.entity}
            onSave={saveEntity}
            onClose={closeEditor}
            busy={planner.busy}
          />
        </Suspense>
      )}
      {prospective && (
        <PreviewDialog
          snapshot={snapshot}
          workspaceId={prospective.workspaceId}
          changes={prospective.changes}
          defaultName={prospective.name}
          onPreview={setPreview}
          busy={planner.busy}
          mutate={planner.mutate}
          onClose={closePreview}
        />
      )}
      {moveEntity && (
        <MoveDialog
          entity={moveEntity}
          onClose={closeMove}
          onMove={(range) => {
            propose(moveEntity, range);
            setMoveEntity(null);
          }}
        />
      )}
      {dialog === 'search' && (
        <SearchDialog
          snapshot={snapshot}
          workspaceId={workspaceId}
          onClose={closeDialog}
          onSelect={selectEntity}
        />
      )}
      {dialog === 'settings' && (
        <Suspense
          fallback={
            <Modal title="Настройки" onClose={closeDialog}>
              <div className="deferred-panel" role="status">
                <LoaderCircle size={20} className="spin" />
              </div>
            </Modal>
          }
        >
          <WorkspaceSettings
            snapshot={snapshot}
            workspaceId={workspaceId}
            mutate={planner.mutate}
            busy={planner.busy}
            onClose={closeDialog}
            onWorkspace={() => setWorkspaceId('all')}
            refresh={planner.refresh}
          />
        </Suspense>
      )}
      {dialog === 'attention' && (
        <AttentionDialog
          snapshot={snapshot}
          workspaceId={workspaceId}
          mutate={planner.mutate}
          busy={planner.busy}
          onClose={closeDialog}
          onSelect={selectEntity}
        />
      )}
      {dialog === 'templates' && (
        <TemplateDialog
          snapshot={snapshot}
          workspaceId={workspaceId}
          mutate={planner.mutate}
          busy={planner.busy}
          anchor={anchor}
          onApplied={applied}
          onClose={closeDialog}
        />
      )}
      {dialog === 'assistant' && (
        <AssistantDialog
          snapshot={snapshot}
          workspaceId={workspaceId}
          mutate={planner.mutate}
          busy={planner.busy}
          anchor={anchor}
          onApplied={applied}
          onClose={closeDialog}
        />
      )}
      {dialog === 'scenarios' && (
        <ScenariosDialog
          snapshot={snapshot}
          workspaceId={workspaceId}
          mutate={planner.mutate}
          busy={planner.busy}
          onClose={closeDialog}
          onCompare={(scenario) => {
            setComparison(scenario);
            const e = snapshot.entities.find((e) => e.id === scenario.changes[0]?.entityId);
            if (e) selectEntity(e);
          }}
        />
      )}
      {dialog === 'filters' && (
        <FiltersDialog
          snapshot={snapshot}
          filters={filters}
          savedViews={savedViews}
          workspaceId={workspaceId}
          onChange={setFilters}
          onWorkspace={setWorkspaceId}
          onSave={(view) => setSavedViews((v) => [...v, view])}
          onDelete={(id) => setSavedViews((v) => v.filter((view) => view.id !== id))}
          onSelect={(view) => {
            setWorkspaceId(view.workspaceId);
            setFilters(view.filters);
          }}
          onClose={closeDialog}
        />
      )}
      {dialog === 'history' && (
        <HistoryDialog
          zone={anchor.zoneName!}
          initial={historicalAt}
          onClose={closeDialog}
          onLoad={(next, at) => {
            setHistorical(next);
            setHistoricalAt(at);
            setSelectedId(null);
            setDialog(null);
          }}
        />
      )}
      {dialog === 'date' && (
        <DateJumpDialog
          anchor={anchor}
          viewport={viewport}
          overviewScale={overviewScale}
          onNavigate={setViewport}
          onFit={showEverything}
          onClose={closeDialog}
          onJump={(date) => {
            setAnchor(date);
            setSelectedId(null);
          }}
        />
      )}
      {dialog === 'help' && (
        <Modal title="Одна шкала. Много возможностей." onClose={closeDialog}>
          <div className="form-body help-body">
            <p>
              Добавьте объект кнопкой «Добавить». Нажмите на объект, чтобы увидеть содержание, даты,
              связи и историю. Подробности раскрываются прямо на шкале.
            </p>
            <p>
              Колесо в любой части шкалы плавно приближает время под курсором. Перетаскивайте фон,
              чтобы сдвигать время; Shift + колесо также сдвигает его. На телефоне разведите два
              пальца для приближения. Нижний регулятор охватывает весь диапазон: от наносекунды до
              миллиарда лет. Значок обзора собирает ваши даты в одном кадре.
            </p>
            <p>
              План, факт и прогноз видны вместе у одного объекта. Идеи без даты находятся здесь же,
              под шкалой. Для переноса выберите объект и откройте «Перенести», либо перетащите план
              с зажатым Shift: перед сохранением откроется проверка последствий.
            </p>
            <dl className="shortcut-list">
              {[
                ['N', 'Добавить объект'],
                ['Ctrl / ⌘ + K или /', 'Найти'],
                ['T', 'Сегодня'],
                ['F', 'Показать всё'],
                ['+ / −', 'Изменить масштаб'],
                ['Alt + ← / →', 'Соседний период'],
                ['Esc', 'Закрыть подробности или окно'],
              ].map(([key, label]) => (
                <div key={key}>
                  <dt>
                    <kbd>{key}</kbd>
                  </dt>
                  <dd>{label}</dd>
                </div>
              ))}
            </dl>
            <p className="helper">
              Все действия доступны видимыми кнопками. Tab перемещает фокус, Enter нажимает
              выбранную кнопку.
            </p>
          </div>
        </Modal>
      )}
    </main>
  );
}

function DateJumpDialog({
  anchor,
  viewport,
  overviewScale,
  onNavigate,
  onFit,
  onClose,
  onJump,
}: {
  anchor: DateTime;
  viewport: TimelineViewport;
  overviewScale: number;
  onNavigate: (viewport: TimelineViewport) => void;
  onFit: () => void;
  onClose: () => void;
  onJump: (date: DateTime) => void;
}) {
  const [value, setValue] = useState(anchor.toISODate() ?? '');
  const [scaleText, setScaleText] = useState(spanLabel(viewport.span));
  const [error, setError] = useState('');
  const zone = anchor.zoneName ?? 'Asia/Yekaterinburg';
  const scaleRange = Math.max(0, 100 - overviewScale);
  function jump(value: string) {
    setValue(value);
    const date = DateTime.fromISO(value, { zone });
    if (!date.isValid) return false;
    onJump(date.startOf('day'));
    setError('');
    return true;
  }
  function submit(event: FormEvent) {
    event.preventDefault();
    if (jump(value)) onClose();
    else setError('Выберите дату.');
  }
  return (
    <Modal title="Дата и масштаб" onClose={onClose} compact>
      <form
        className="scale-explicit"
        onSubmit={(event) => {
          event.preventDefault();
          const span = parseDuration(scaleText);
          if (span == null || span <= 0n) {
            setError('Укажите масштаб: 2 млн лет, 20 мс или 1 нс.');
            return;
          }
          onNavigate(zoomToSpan(viewport, span));
          onClose();
        }}
      >
        <label htmlFor="scale-expression">Видимый интервал</label>
        <div className="date-navigator-date">
          <input
            id="scale-expression"
            aria-label="Точный масштаб"
            autoFocus
            value={scaleText}
            onChange={(event) => setScaleText(event.target.value)}
            placeholder="2 млн лет · 20 мс · 1 нс"
          />
          <button className="quick-save" aria-label="Применить масштаб" title="Применить · Enter">
            <Check size={20} />
          </button>
        </div>
      </form>
      <form className="date-navigator" onSubmit={submit}>
        <div className="date-navigator-date">
          <input
            type="date"
            aria-label="Дата на шкале"
            value={value}
            min="0001-01-01"
            max="9999-12-31"
            onChange={(event) => jump(event.target.value)}
            required
          />
          <button
            type="submit"
            className="quick-save"
            aria-label="Перейти к дате"
            title="Перейти · Enter"
          >
            <Check size={20} />
          </button>
        </div>
        <div className="date-navigator-tools">
          <button
            type="button"
            className="icon-button"
            aria-label="Предыдущий период"
            title="Предыдущий период"
            onClick={() => {
              const next = panViewport(viewport, -0.8, 1);
              onNavigate(next);
              setValue(calendarAt(next.center, zone)?.toISODate() ?? '');
            }}
          >
            <ChevronLeft size={20} />
          </button>
          <button
            type="button"
            className="icon-button"
            aria-label="Сегодня"
            title="Сегодня · T"
            onClick={() => jump(DateTime.now().setZone(zone).toISODate()!)}
          >
            <Clock3 size={18} />
          </button>
          <button
            type="button"
            className="icon-button"
            aria-label="Следующий период"
            title="Следующий период"
            onClick={() => {
              const next = panViewport(viewport, 0.8, 1);
              onNavigate(next);
              setValue(calendarAt(next.center, zone)?.toISODate() ?? '');
            }}
          >
            <ChevronRight size={20} />
          </button>
          <button
            type="button"
            className="icon-button date-fit"
            aria-label="Показать всё на шкале"
            title="Вся шкала · F"
            onClick={onFit}
          >
            <Maximize2 size={18} />
          </button>
        </div>
        <div className="date-navigator-scale">
          <button
            type="button"
            className="icon-button"
            aria-label="Отдалить временную шкалу"
            title="Отдалить"
            onClick={() => onNavigate(zoomViewport(viewport, 1 / 1.7))}
          >
            <Minus size={18} />
          </button>
          <label>
            <span>{spanLabel(viewport.span)}</span>
            <input
              type="range"
              min="0"
              max="100"
              aria-label="Масштаб временной шкалы"
              value={spanToScale(viewport.span)}
              onChange={(event) =>
                onNavigate(
                  normalizeViewport({
                    ...viewport,
                    span: scaleToSpan(Number(event.target.value)),
                  }),
                )
              }
            />
          </label>
          <button
            type="button"
            className="icon-button"
            aria-label="Приблизить временную шкалу"
            title="Приблизить"
            onClick={() => onNavigate(zoomViewport(viewport, 1.7))}
          >
            <Plus size={18} />
          </button>
        </div>
        <small className="date-navigator-zone">{zone}</small>
        {error && (
          <p className="quick-error" role="alert">
            {error}
          </p>
        )}
      </form>
    </Modal>
  );
}

function MoveDialog({
  entity,
  onClose,
  onMove,
}: {
  entity: Entity;
  onClose: () => void;
  onMove: (range: Entity['plan']) => void;
}) {
  const [range, setRange] = useState(() => structuredClone(entity.plan));
  const [invalid, setInvalid] = useState(false);
  const [relativeInput] = useState(
    () => !!entity.plan.precise || hasSubMillisecondTime(entity.plan),
  );
  const [timeContext] = useState(() => {
    const coordinates = canonicalRange(entity.plan);
    const at = coordinates.start ?? coordinates.end;
    if (at == null) return undefined;
    const extent =
      coordinates.start != null && coordinates.end != null
        ? coordinates.end - coordinates.start
        : 0n;
    return timeReference(at, entity.plan.timezone, extent < MINUTE);
  });
  const [error, setError] = useState('');
  const day = range.precision === 'day';
  return (
    <Modal
      title={`Перенести «${entity.title}»`}
      subtitle="Введите новый интервал. Затем проверим последствия и покажем вариант."
      onClose={onClose}
    >
      <form
        onSubmit={(e) => {
          e.preventDefault();
          const errors = preciseRangeErrors(range);
          if (invalid || errors.length) {
            setError(errors[0] ?? 'Проверьте значения времени.');
            return;
          }
          onMove(range);
        }}
      >
        <div className="form-body form-grid">
          {relativeInput ? (
            <PreciseRangeFields
              value={range.precise ? range : withPreciseCoordinates(range)}
              timeContext={timeContext}
              calendarOnly={!entity.plan.precise}
              showResolution={!!entity.plan.precise}
              onChange={setRange}
              onInvalidChange={setInvalid}
            />
          ) : (
            <>
              <label>
                Новое начало
                <input
                  type={day ? 'date' : 'datetime-local'}
                  step="any"
                  value={localRangeInput(range, 'start', day)}
                  onChange={(e) =>
                    setRange((current) =>
                      withLocalRangeInput(current, 'start', e.target.value, day),
                    )
                  }
                  required
                />
              </label>
              <label>
                {day ? 'Последний день' : 'Новое окончание'}
                <input
                  type={day ? 'date' : 'datetime-local'}
                  step="any"
                  value={localRangeInput(range, 'end', day)}
                  onChange={(e) =>
                    setRange((current) => withLocalRangeInput(current, 'end', e.target.value, day))
                  }
                />
              </label>
            </>
          )}
          {error && (
            <p className="quick-error" role="alert">
              {error}
            </p>
          )}
        </div>
        <footer className="modal-footer">
          <span className="helper">{entity.plan.timezone}</span>
          <button className="primary-button" disabled={invalid}>
            Проверить последствия
            <ArrowRightIcon />
          </button>
        </footer>
      </form>
    </Modal>
  );
}
function ArrowRightIcon() {
  return <ChevronRight size={16} />;
}

function FiltersDialog({
  snapshot,
  filters,
  savedViews,
  workspaceId,
  onChange,
  onWorkspace,
  onSave,
  onSelect,
  onDelete,
  onClose,
}: {
  snapshot: PlannerSnapshot;
  filters: Filters;
  savedViews: SavedView[];
  workspaceId: string;
  onChange: (f: Filters) => void;
  onWorkspace: (id: string) => void;
  onSave: (v: SavedView) => void;
  onSelect: (v: SavedView) => void;
  onDelete: (id: string) => void;
  onClose: () => void;
}) {
  const [name, setName] = useState('');
  return (
    <Modal title="Смотреть то, что важно" onClose={onClose}>
      <div className="form-body">
        <label>
          На общей шкале
          <select
            aria-label="Фильтр пространства"
            value={workspaceId}
            onChange={(event) => onWorkspace(event.target.value)}
          >
            <option value="all">Все пространства вместе</option>
            {snapshot.workspaces.map((space) => (
              <option key={space.id} value={space.id}>
                {space.name}
              </option>
            ))}
          </select>
        </label>
        <h3>Состояние</h3>
        <div className="check-list horizontal">
          {Object.entries(statusLabels).map(([key, label]) => (
            <label className="checkbox-label" key={key}>
              <input
                type="checkbox"
                checked={filters.statuses.includes(key)}
                onChange={(e) =>
                  onChange({
                    ...filters,
                    statuses: e.target.checked
                      ? [...filters.statuses, key]
                      : filters.statuses.filter((k) => k !== key),
                  })
                }
              />
              {label}
            </label>
          ))}
        </div>
        <h3>Типы</h3>
        <div className="check-list horizontal">
          {snapshot.types
            .filter((t) => !t.workspaceId || workspaceId === 'all' || t.workspaceId === workspaceId)
            .map((t) => (
              <label className="checkbox-label" key={t.id}>
                <input
                  type="checkbox"
                  checked={filters.types.includes(t.id)}
                  onChange={(e) =>
                    onChange({
                      ...filters,
                      types: e.target.checked
                        ? [...filters.types, t.id]
                        : filters.types.filter((id) => id !== t.id),
                    })
                  }
                />
                {t.label}
              </label>
            ))}
        </div>
        <div className="form-grid">
          <label>
            Ответственный
            <select
              value={filters.owner}
              onChange={(e) => onChange({ ...filters, owner: e.target.value })}
            >
              <option value="">Все</option>
              {snapshot.users.map((u) => (
                <option key={u.id} value={u.id}>
                  {u.displayName}
                </option>
              ))}
            </select>
          </label>
          <label>
            Теги через запятую
            <input
              value={filters.tags}
              onChange={(e) => onChange({ ...filters, tags: e.target.value })}
              placeholder="Поездка, работа…"
            />
          </label>
        </div>
        <div className="check-list">
          <label className="checkbox-label">
            <input
              type="checkbox"
              checked={filters.attention}
              onChange={(e) => onChange({ ...filters, attention: e.target.checked })}
            />
            Только требует внимания
          </label>
          <label className="checkbox-label">
            <input
              type="checkbox"
              checked={filters.unknown}
              onChange={(e) => onChange({ ...filters, unknown: e.target.checked })}
            />
            Только без даты
          </label>
        </div>
        <div className="settings-section">
          <h3>Сохранить представление</h3>
          <div className="inline-form">
            <input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="Мои поездки, решения, команда…"
            />
            <button
              className="secondary-button"
              disabled={!name.trim()}
              onClick={() => {
                onSave({
                  id: crypto.randomUUID(),
                  name: name.trim(),
                  workspaceId,
                  filters: structuredClone(filters),
                  groupBy: 'process',
                });
                setName('');
              }}
            >
              Сохранить
            </button>
          </div>
          {savedViews.map((v) => (
            <div className="saved-view-row" key={v.id}>
              <button
                className="text-button"
                onClick={() => {
                  onSelect(v);
                  onClose();
                }}
              >
                {v.name}
              </button>
              <button
                className="icon-button"
                aria-label={`Удалить представление ${v.name}`}
                onClick={() => onDelete(v.id)}
              >
                <X size={14} />
              </button>
            </div>
          ))}
          <p className="helper">
            Представления сохраняются в этом браузере; объекты сохраняются на сервере.
          </p>
        </div>
      </div>
      <footer className="modal-footer">
        <button
          className="secondary-button"
          onClick={() => {
            onChange(emptyFilters);
            onWorkspace('all');
          }}
        >
          Сбросить фильтры
        </button>
        <button className="primary-button" onClick={onClose}>
          Показать на шкале
        </button>
      </footer>
    </Modal>
  );
}

function HistoryDialog({
  zone,
  initial,
  onClose,
  onLoad,
}: {
  zone: string;
  initial: string;
  onClose: () => void;
  onLoad: (snapshot: PlannerSnapshot, at: string) => void;
}) {
  const [value, setValue] = useState(
    inputDate(initial || DateTime.now().minus({ hours: 1 }).toISO(), zone),
  );
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  async function load(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    try {
      const at = fromInput(value, zone)!;
      const next = await request<PlannerSnapshot>(`/api/history?at=${encodeURIComponent(at)}`);
      onLoad(next, at);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setBusy(false);
    }
  }
  return (
    <Modal
      title="Вернуться к прошлому состоянию"
      subtitle="Посмотрите, каким был план на выбранный момент. Ваши текущие права продолжают действовать."
      onClose={onClose}
    >
      <form onSubmit={load}>
        <div className="form-body">
          <label>
            Момент времени
            <input
              type="datetime-local"
              step="any"
              value={value}
              onChange={(e) => setValue(e.target.value)}
              required
            />
          </label>
          <p className="helper">{zone} · исторический режим доступен только для просмотра.</p>
          {error && (
            <p className="inline-error" role="alert">
              {error}
            </p>
          )}
        </div>
        <footer className="modal-footer">
          <button className="secondary-button" type="button" onClick={onClose}>
            Отмена
          </button>
          <button className="primary-button" disabled={busy}>
            <History size={16} />
            {busy ? 'Загрузка…' : 'Открыть снимок'}
          </button>
        </footer>
      </form>
    </Modal>
  );
}

function AuthEntry({
  mutate,
  error,
  refresh,
  busy,
}: {
  mutate: ReturnType<typeof usePlanner>['mutate'];
  error: string | null;
  refresh: () => Promise<void>;
  busy: boolean;
}) {
  const [mode, setMode] = useState('login');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [name, setName] = useState('');
  const [formError, setFormError] = useState('');
  const [invitationToken, setInvitationToken] = useState('');
  async function submit(event: FormEvent) {
    event.preventDefault();
    setFormError('');
    try {
      await mutate(`/api/auth/${mode}`, 'POST', {
        email,
        password,
        ...(mode === 'register'
          ? {
              displayName: name,
              ...(invitationToken.trim() ? { invitationToken: invitationToken.trim() } : {}),
            }
          : {}),
      });
    } catch (e) {
      setFormError((e as Error).message);
    }
  }
  return (
    <main className="auth-screen">
      <div className="brand">
        <span className="brand-symbol" />
        планировщик
      </div>
      <div className="auth-card">
        <p className="eyebrow">Всё начинается со времени</p>
        <h1>
          Ваши планы.
          <br />
          Ваша история.
        </h1>
        <p className="muted">События, поездки и работа на одной шкале.</p>
        <div className="segmented-control">
          <button
            className={mode === 'login' ? 'active' : ''}
            aria-pressed={mode === 'login'}
            onClick={() => {
              setMode('login');
              setFormError('');
            }}
          >
            Войти
          </button>
          <button
            className={mode === 'register' ? 'active' : ''}
            aria-pressed={mode === 'register'}
            onClick={() => {
              setMode('register');
              setFormError('');
            }}
          >
            Создать аккаунт
          </button>
        </div>
        <form onSubmit={submit}>
          {mode === 'register' && (
            <label>
              Имя
              <input value={name} onChange={(e) => setName(e.target.value)} required />
            </label>
          )}
          <label>
            Email
            <input
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              required
              autoComplete="email"
            />
          </label>
          <label>
            Пароль
            <input
              type="password"
              minLength={10}
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
              autoComplete={mode === 'register' ? 'new-password' : 'current-password'}
            />
            <small>Минимум 10 символов</small>
          </label>
          {mode === 'register' && (
            <label>
              Код приглашения <span className="field-optional">если вас пригласили</span>
              <input
                type="password"
                value={invitationToken}
                onChange={(e) => setInvitationToken(e.target.value)}
                autoComplete="off"
                spellCheck={false}
              />
            </label>
          )}
          {(formError || error) && (
            <p className="inline-error" role="alert">
              {formError || error}
            </p>
          )}
          <button className="primary-button" disabled={busy}>
            {mode === 'login' ? 'Войти' : 'Создать аккаунт'}
          </button>
        </form>
        <button className="text-button" onClick={() => void refresh()}>
          Повторить подключение
        </button>
      </div>
      <div className="auth-time-line" />
    </main>
  );
}
