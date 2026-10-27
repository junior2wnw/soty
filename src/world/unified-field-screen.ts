import './unified-field.css';
import { createFieldDocument, fieldEntityKey, type FieldDocument, type FieldEntityRef } from '../../modules/field/contract.mjs';
import { createUnifiedField, type FieldDirectoryEntity, type UnifiedField, type UnifiedFieldMode, type UnifiedFieldSummary, type UnifiedFieldViewState } from './unified-field';
import { compactFieldContext, nextFieldSlot } from './unified-field-state.mjs';
import { createPersonalFieldDocument, ensureFieldEntityPlacement } from './app-field-placement.mjs';
import { layoutUnifiedField, fieldBounds, rectPoints } from './unified-field-layout.mjs';
import { fitFieldCamera } from './unified-field-camera.mjs';
import { createFieldSearchPlacement } from './unified-field-search.mjs';
import { unplacedOwnApps, type OwnAppOverview } from './own-apps-overview.mjs';
import { createFieldDirectory, type DirectoryApi, type DirectoryApp, type DirectoryEntity, type DirectoryRecord } from './field-directory';
import { createFieldPersistence, type FieldPersistenceState } from './field-persistence';
import { resolveFieldAppArt } from './field-art.mjs';
import { avatar, button, el, iconButton, labeledField, nounCount, textInput } from './dom';
import { createDialog, errorText, isDialogFocusTarget, type WorldDialog } from './dialogs';
import { icon } from './icons';
import type { WorldCommunity } from './types';
import type { RecentAction } from './product';

export type FieldFilter = 'all' | 'app' | 'person' | 'community' | 'device';
export interface UnifiedFieldScreenOptions {
  api: DirectoryApi; accountId: string; isCurrent(): boolean;
  mode?: UnifiedFieldMode; query?: string; filter?: FieldFilter;
  filtersByMode?: { mine: FieldFilter; search: FieldFilter };
  viewState?: UnifiedFieldViewState; pinnedApps?: readonly string[];
  onViewStateChange?: (view: UnifiedFieldViewState) => void;
  onRoute(mode: UnifiedFieldMode, query: string, filter: FieldFilter): void;
  onOpen(entity: DirectoryEntity, record: DirectoryRecord | null, source?: { presentation: 'list'; listQuery: string }): void;
  directMessagesEnabled?: boolean;
  onCreate(kind: 'app' | 'community' | 'device' | 'person' | 'assistant'): void;
  onAppSettings?: (app: DirectoryApp) => void;
  onOpenSaved?: () => void;
  onCreateMenu?: () => void;
  onOpenMore?: () => void;
  onSearchVisibilityChange?(): void;
  getRecentWork?: () => readonly RecentAction[];
  onOpenRecent?: (route: string) => void;
  onKnownAppEntries?(entries: DirectoryApp['entry'][]): void;
  onInspectOwn?: (entity: DirectoryEntity, record: DirectoryRecord | null, openPlacement?: () => void) => boolean;
  onMessage(message: string, error?: boolean): void;
  onContextChange?(contextId: string): void;
  resolveArt?: (entity: FieldDirectoryEntity) => import('./app-art.mjs').AppArt | null;
}
export interface UnifiedFieldScreen {
  element: HTMLElement; ready: Promise<void>;
  setMode(mode: UnifiedFieldMode, query?: string, filter?: FieldFilter): void;
  attachHeaderSearch(header: HTMLElement): void;
  addEntity(ref: FieldEntityRef): Promise<void>;
  focusSearch(): void; closeSearch(): void; focusContext(contextId: string): void; openAdd(): void; refresh(options?: { preserveView?: boolean }): Promise<void>; refreshOwnApps(): Promise<void>;
  openView(): void; openList(query?: string): void; arrange(): void;
  hasUnsavedChanges(): boolean; flush(): Promise<void>; reconnect(): void; dispose(): void;
}

const BUILTINS: DirectoryEntity[] = [
  { entity: { kind: 'builtin', id: 'notes' }, title: 'Записки', symbol: 'note', coverKey: 'notes', source: 'builtin' },
  { entity: { kind: 'builtin', id: 'chess' }, title: 'Шахматы', symbol: 'chess', coverKey: 'chess', source: 'builtin' },
  { entity: { kind: 'builtin', id: 'assistant' }, title: 'Помощник', symbol: 'assistant', coverKey: 'assistant', source: 'builtin' },
];
const labels: Record<FieldFilter, string> = { all: 'Всё', app: 'Приложения', person: 'Люди', community: 'Комнаты', device: 'Устройства' };
const kindLabel = (kind: FieldEntityRef['kind']): string => ({ app: 'Приложение', builtin: 'Возможность Сот', person: 'Человек', community: 'Комната', device: 'Моё устройство' })[kind];
const matches = (item: DirectoryEntity, query: string, filter: FieldFilter): boolean =>
  (filter === 'all' || item.entity.kind === filter || filter === 'app' && item.entity.kind === 'builtin') &&
  `${item.title} ${item.description ?? ''}`.toLocaleLowerCase('ru').includes(query.trim().toLocaleLowerCase('ru'));

/** Production orchestration: only refs/positions are durable; fresh metadata keeps its authority boundary. */
export function createUnifiedFieldScreen(options: UnifiedFieldScreenOptions): UnifiedFieldScreen {
  let disposed = false, mode = options.mode ?? 'mine', filter = options.filter ?? 'all';
  const filtersByMode = options.filtersByMode ?? { mine: mode === 'mine' ? filter : 'all', search: mode === 'search' ? filter : 'all' };
  const queries: Record<UnifiedFieldMode, string> = { mine: mode === 'mine' ? options.query ?? '' : '', search: mode === 'search' ? options.query ?? '' : '' };
  const current = (): boolean => !disposed && options.isCurrent();
  const abort = new AbortController(), signal = abort.signal;
  const directory = createFieldDirectory({ api: options.api, accountId: options.accountId, isCurrent: current });
  const persistence = createFieldPersistence({ api: options.api, accountId: options.accountId, isCurrent: current, localFirst: true });
  const element = el('section', 'uf-screen'); element.setAttribute('aria-label', 'Мои соты');
  element.dataset.ready = 'loading';
  element.dataset.startupScreenAt = String(Date.now());
  const head = el('div', 'uf-screen-head'), title = el('h1', 'uf-screen-title', 'Мои соты');
  const sides = el('div', 'uf-screen-switch'); sides.setAttribute('role', 'group'); sides.setAttribute('aria-label', 'Сторона поля');
  const mineButton = button('Моё', undefined, '', () => changeMode('mine'));
  const searchButton = button('Люди и каталог', undefined, '', () => changeMode('search'));
  sides.append(mineButton, searchButton);
  const arrange = button('Расставить', 'sliders', 'uf-screen-arrange', () => engine?.setArrange(!summary?.arranging));
  head.append(title, arrange);
  const search = el('label', 'uf-global-search'); const searchInput = textInput('', 'Найти в моих сотах', 100);
  search.setAttribute('data-floating-obstacle', '');
  searchInput.type = 'search'; searchInput.setAttribute('aria-label', 'Найти в моих сотах'); searchInput.autocomplete = 'off';
  const clear = iconButton('Очистить поиск', 'close', () => { searchInput.value = ''; queries[mode] = ''; inputChanged(); searchInput.focus(); });
  clear.classList.add('uf-search-clear'); search.append(icon('search'), searchInput, clear);
  const closeSearch = iconButton('Закрыть поиск', 'close', () => {
    if (searchInput.value || queries[mode]) { searchInput.value = ''; inputChanged(true); }
    element.dataset.search = 'closed';
    renderMineSearchResults(); renderOwnApps();
    options.onSearchVisibilityChange?.();
    const opener = searchOpener; searchOpener = null;
    if (isDialogFocusTarget(opener)) opener.focus({ preventScroll: true }); else focusFieldControl();
  });
  closeSearch.classList.add('uf-search-close'); search.append(closeSearch);
  const listButton = button('Список', 'list', 'uf-list-button', () => openList());
  listButton.setAttribute('aria-haspopup', 'dialog'); listButton.setAttribute('aria-label', 'Соты списком');
  const localSearch = el('div', 'uf-local-search');
  const mineSearchResults = el('div', 'uf-search-results'); mineSearchResults.hidden = true;
  mineSearchResults.setAttribute('role', 'group'); mineSearchResults.setAttribute('aria-label', 'Результаты на моём поле');
  mineSearchResults.setAttribute('data-floating-obstacle', '');
  const mineSearchStatus = el('p', 'uf-search-result-status'); mineSearchStatus.setAttribute('role', 'status');
  const mineSearchList = el('div', 'uf-search-result-list');
  sides.classList.add('uf-search-scope'); sides.setAttribute('aria-label', 'Где искать');
  sides.setAttribute('data-floating-obstacle', '');
  mineSearchResults.append(mineSearchStatus, mineSearchList); localSearch.append(sides, search, listButton, mineSearchResults);
  const workspaceTools = el('nav', 'uf-workspace-tools'); workspaceTools.setAttribute('aria-label', 'Моё: вид и сохранённое');
  const create = button('Создать', 'plus', 'uf-workspace-create', () => options.onCreateMenu?.()); create.hidden = !options.onCreateMenu;
  const moreTools = button('Ещё', 'more', 'uf-workspace-more', () => options.onOpenMore?.()); moreTools.hidden = !options.onOpenMore;
  const listView = button('Список', 'list', '', () => openList()); listView.setAttribute('aria-haspopup', 'dialog');
  const savedEntries = button('Сохранённое', 'folder', '', () => options.onOpenSaved?.()); savedEntries.hidden = !options.onOpenSaved;
  workspaceTools.append(create, listView, moreTools, savedEntries, button('Пространства', 'layers', '', () => manageContexts()));
  const filters = el('div', 'uf-filters'); filters.setAttribute('role', 'group'); filters.setAttribute('aria-label', 'Какие объекты показать');
  const compactFilter = el('select', 'uf-compact-filter'); compactFilter.setAttribute('aria-label', 'Какие объекты показать'); filters.append(compactFilter);
  compactFilter.addEventListener('change', () => { filter = compactFilter.value as FieldFilter; filtersByMode[mode] = filter; updateChrome(); inputChanged(true); });
  const filterButtons = new Map<FieldFilter, HTMLButtonElement>();
  const host = el('div', 'uf-scene-host');
  const ownAppsOverview = el('section', 'uf-own-apps'); ownAppsOverview.setAttribute('aria-label', 'Мои приложения вне поля'); ownAppsOverview.hidden = true;
  const loading = el('div', 'uf-loading', 'Открываем поле…'); loading.setAttribute('role', 'status');
  const empty = el('div', 'uf-empty'); empty.hidden = true; empty.setAttribute('role', 'status');
  const feedback = el('div', 'uf-storage-status'); feedback.hidden = true; feedback.setAttribute('role', 'status');
  const toolbar = el('div', 'uf-context-toolbar');
  const zoom = el('div', 'uf-zoom-controls'); const percent = el('output', 'uf-zoom-value', '100%'); percent.setAttribute('aria-label', 'Масштаб поля');
  const setZoom = (factor: number): void => { if (!engine) return; const camera = engine.getCamera(); engine.setCamera({ ...camera, scale: camera.scale * factor }); };
  zoom.append(iconButton('Уменьшить поле', 'minus', () => setZoom(1 / 1.18)), percent, iconButton('Увеличить поле', 'plus', () => setZoom(1.18)));
  const contextSelector = el('select', 'uf-context-selector'); contextSelector.setAttribute('aria-label', 'Пространство поля');
  contextSelector.addEventListener('change', () => { closePreview(false); contextSelector.value ? engine?.focusContext(contextSelector.value) : engine?.fitOverview(); }, { signal });
  const overview = button('Показать всё', 'expand', 'uf-overview-button', () => { closePreview(false); engine?.fitOverview(); });
  const spaces = iconButton('Управлять пространствами', 'settings', () => manageContexts());
  const viewTools = iconButton('Вид поля', 'sliders', () => openViewTools()); viewTools.classList.add('uf-view-tools');
  toolbar.append(zoom, contextSelector, overview, spaces, viewTools);
  const panControls = el('div', 'uf-pan-controls'); panControls.hidden = true;
  panControls.setAttribute('role', 'group'); panControls.setAttribute('aria-label', 'Перемещение поля');
  const panField = (x: number, y: number): void => {
    if (!engine) return;
    const camera = engine.getCamera(), viewport = engine.element;
    const stepX = Math.max(48, Math.min(240, viewport.clientWidth * .4)), stepY = Math.max(48, Math.min(240, viewport.clientHeight * .4));
    engine.setCamera({ ...camera, x: camera.x + x * stepX / camera.scale, y: camera.y + y * stepY / camera.scale });
  };
  const hidePanControls = (): void => { panControls.hidden = true; viewTools.focus({ preventScroll: true }); };
  for (const [label, symbol, x, y] of [['Посмотреть левее', 'back', -1, 0], ['Посмотреть выше', 'up', 0, -1], ['Посмотреть ниже', 'down', 0, 1], ['Посмотреть правее', 'next', 1, 0]] as const) {
    panControls.append(iconButton(label, symbol, () => panField(x, y)));
  }
  panControls.append(iconButton('Скрыть кнопки перемещения', 'close', hidePanControls));
  panControls.addEventListener('keydown', event => { if (event.key === 'Escape') { event.preventDefault(); event.stopPropagation(); hidePanControls(); } }, { signal });
  const arrangeHint = el('p', 'uf-arrange-hint', 'Перетащите соту или нажмите на неё'); arrangeHint.hidden = true;
  const more = button('Показать ещё', 'plus', 'uf-more', () => run(searchDirectory(true))); more.hidden = true;
  const searchLimit = el('div', 'uf-search-limit'); searchLimit.hidden = true;
  const searchLimitCopy = el('span', '', 'Показаны первые 256 результатов. Уточните поиск.'); searchLimitCopy.setAttribute('role', 'status');
  searchLimit.append(searchLimitCopy, button('Уточнить поиск', 'search', 'sw-button-quiet', () => searchInput.focus()));
  const undo = button('Отменить', 'back', 'uf-undo', () => run(engine?.undo())); undo.hidden = true;
  host.append(loading, empty, feedback, toolbar, panControls, arrangeHint, more, undo); element.append(head, workspaceTools, localSearch, filters, searchLimit, ownAppsOverview, host);
  const dialogs = new Set<WorldDialog>();
  const metadata = new Map<string, DirectoryEntity>(BUILTINS.map(item => [fieldEntityKey(item.entity), item]));
  let engine: UnifiedField | null = null, summary: UnifiedFieldSummary | null = null, state: FieldPersistenceState | null = null;
  let searchItems: DirectoryEntity[] = [], available: DirectoryEntity[] = [], searchCursor: string | null = null;
  let ownedApps: unknown[] = [], ownedAppsState: 'loading' | 'ready' | 'offline' = 'loading', ownAppsSignature = '';
  let availableCursor: string | null = null;
  const searchPlacement = createFieldSearchPlacement();
  let activeSearchScene = createFieldDocument();
  let searchResponseScope = '';
  let searchGeneration = 0, mineGeneration = 0, searchTimer: ReturnType<typeof setTimeout> | null = null;
  let mineResolvedGeneration = -1;
  let mineConfirmedGeneration = -1;
  let mineKnownRefs = new Set(BUILTINS.map(item => fieldEntityKey(item.entity)));
  let mineFilterAdmitted = false;
  let activeSearches = 0, queuedSearch: ((admitted: boolean) => void) | null = null;
  let mineBusy = false, queuedMine: ((admitted: boolean) => void) | null = null;
  let initialMineTask: Promise<void> | null = null, initialMineRefs = '';
  let preview: HTMLElement | null = null, selected: { item: DirectoryEntity; shortcutId?: string } | null = null;
  let previewOpener: HTMLElement | null = null;
  let searchOpener: HTMLElement | null = null;
  let listDialog: WorldDialog | null = null, renderOpenList: (() => void) | null = null;
  let mineSearchSignature = '';
  let header: HTMLElement | null = null, contextKey = '', statusKey = '', composing = false;
  let emptyDocument: FieldDocument | null = null, emptyScope = '', metadataRevision = 0, emptyMetadataRevision = -1;
  let firstResolve = !options.viewState?.mine;
  let knownAppSignature = '';
  const wide = matchMedia('(min-width: 901px)');
  let viewPercent: HTMLOutputElement | null = null;
  const report = (error: unknown): void => { if (current()) options.onMessage(errorText(error), true); };
  const run = (promise: Promise<unknown> | undefined): void => { if (promise) void promise.catch(report); };
  function dialog(name: string, onClose?: () => void): WorldDialog {
    const opener = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const instance = createDialog(name, () => { dialogs.delete(instance); onClose?.(); }, {
      isCurrent: current,
      resolve: () => isDialogFocusTarget(opener) ? opener : fieldControl(),
    }); dialogs.add(instance); return instance;
  }
  function fieldControl(): HTMLElement | null {
    return [header?.querySelector<HTMLElement>('[data-field-search]'), element.closest('.sw-app')?.querySelector<HTMLElement>('.sw-mobile-nav .of-menu-button'), element.querySelector<HTMLElement>('.uf-node.is-selected')]
      .find(node => isDialogFocusTarget(node ?? null)) ?? null;
  }
  function focusFieldControl(): void { fieldControl()?.focus({ preventScroll: true }); }
  function openCompactPreview(): void {
    if (!engine || mode !== 'mine') return;
    if (!state || state.state !== 'saved' || state.pendingCount || !state.localDurable || persistence.hasUnsavedChanges() || engine.hasUnsavedChanges()) {
      options.onMessage('Сначала дождитесь сохранения поля или проверьте его состояние.', true); return;
    }
    const owner = engine, expected = owner.snapshot();
    if (!expected.contexts.length) return;
    let previewField: UnifiedField | null = null;
    const disposePreview = (): void => { previewField?.destroy(); previewField = null; };
    const instance = dialog('Собрать рядом', disposePreview), context = el('select', 'sw-select');
    context.setAttribute('aria-label', 'Какое пространство собрать');
    for (const space of expected.contexts) { const option = el('option', '', space.title); option.value = space.contextId; context.append(option); }
    context.value = summary?.focusContextId || expected.contexts[0]!.contextId;
    const previewHost = el('div', 'of-compact-preview'), error = el('p', 'sw-error'); error.setAttribute('role', 'alert');
    previewHost.inert = true; previewHost.setAttribute('aria-hidden', 'true');
    const apply = button('Собрать рядом', 'check', 'sw-button-primary');
    const render = (): void => {
      previewField?.destroy(); previewField = null; error.textContent = '';
      try {
        const candidate = compactFieldContext(expected, context.value);
        previewField = createUnifiedField({ accountId: options.accountId, document: candidate, entities: allMetadata(),
          viewState: { mineContext: context.value, mineFit: 'context' },
          resolveArt: item => options.resolveArt ? options.resolveArt(item) : resolveFieldAppArt({ ...(item.entity.kind === 'app' ? { appId: item.entity.id } : {}), ...(item.coverKey ? { coverKey: item.coverKey } : {}) }),
        });
        previewHost.replaceChildren(previewField.element); apply.disabled = false;
      } catch (reason) { previewHost.replaceChildren(); error.textContent = errorText(reason); apply.disabled = true; }
    };
    context.addEventListener('change', render, { signal });
    apply.addEventListener('click', () => {
      if (!current() || engine !== owner || mode !== 'mine' || apply.disabled) return;
      apply.disabled = true; context.disabled = true; error.textContent = '';
      void owner.compactContext(context.value, expected).then(result => {
        if (!current() || engine !== owner || !instance.element.open) return;
        if (result?.status === 'conflict') { error.textContent = 'Поле изменилось. Закройте предпросмотр и проверьте версии.'; return; }
        instance.close(); options.onMessage(!result ? 'Соты уже собраны рядом.' : result.status === 'saved' ? 'Соты собраны. Действие можно отменить.' : 'Новая расстановка сохранена на устройстве и ждёт подтверждения.');
      }).catch(reason => { if (current() && instance.element.open) error.textContent = errorText(reason); });
    }, { signal });
    instance.body.append(el('p', 'sw-muted', 'Так будут расположены соты в выбранном пространстве. Остальные пространства сохранят свои места. После применения можно нажать «Отменить».'),
      labeledField('Пространство', context), previewHost, error, apply); render();
  }
  function openViewTools(): void {
    const instance = dialog('Вид поля'); instance.element.classList.add('uf-view-dialog');
    const types = el('select', 'sw-select'); types.setAttribute('aria-label', 'Какие объекты показать');
    for (const kind of ['all', 'app', 'community', 'person', 'device'] as FieldFilter[]) {
      if (mode === 'search' && kind === 'device') continue;
      const option = el('option', '', labels[kind]); option.value = kind; types.append(option);
    }
    types.value = filter;
    types.addEventListener('change', () => { filter = types.value as FieldFilter; filtersByMode[mode] = filter; updateChrome(); inputChanged(true); instance.close(); });
    const row = el('div', 'uf-view-zoom'), value = el('output', 'uf-zoom-value', percent.value || '100%');
    value.setAttribute('aria-label', 'Масштаб поля'); viewPercent = value;
    row.append(iconButton('Уменьшить поле', 'minus', () => setZoom(1 / 1.18)), value, iconButton('Увеличить поле', 'plus', () => setZoom(1.18)));
    const contexts = el('select', 'sw-select'); contexts.setAttribute('aria-label', 'Показать пространство');
    contexts.append(...[...contextSelector.options].map(option => { const copy = el('option', '', option.textContent ?? ''); copy.value = option.value; return copy; }));
    contexts.value = contextSelector.value;
    contexts.addEventListener('change', () => { closePreview(false); contexts.value ? engine?.focusContext(contexts.value) : engine?.fitOverview(); instance.close(); });
    instance.body.append(labeledField('Показать', types), labeledField('Перейти к', contexts), row,
      button('Показать всё', 'expand', 'sw-button-wide', () => { closePreview(false); engine?.fitOverview(); instance.close(); }));
    const panToggle = button(panControls.hidden ? 'Кнопки перемещения' : 'Скрыть кнопки перемещения', 'arrow', 'sw-button-wide', () => {
      const show = panControls.hidden; instance.close({ restoreFocus: false }); panControls.hidden = !show;
      (show ? panControls.querySelector<HTMLButtonElement>('button') : viewTools)?.focus({ preventScroll: true });
    });
    panToggle.setAttribute('aria-expanded', String(!panControls.hidden));
    instance.body.append(panToggle, el('p', 'uf-input-help', 'Тяните поле или прокручивайте. Масштаб — два пальца, Ctrl/⌘ + колесо или −/+. В фокусе поля +/− меняют масштаб, 0 показывает всё. Двойной щелчок по пустому полю приближает.'));
    instance.body.append(button('Соты списком', 'list', 'sw-button-wide', () => { instance.close({ restoreFocus: false }); openList(); }));
    if (mode === 'mine') instance.body.append(button(summary?.arranging ? 'Закончить расстановку' : 'Расставить соты', 'move', 'sw-button-wide', () => { instance.close(); engine?.setArrange(!summary?.arranging); }));
    if (mode === 'mine') instance.body.append(button('Собрать рядом', 'cells', 'sw-button-wide', () => { instance.close({ restoreFocus: false }); openCompactPreview(); }));
    if (mode === 'mine') instance.body.append(button('Мои пространства', 'layers', 'sw-button-wide', () => { instance.close(); manageContexts(); }));
  }
  function merge(items: readonly DirectoryEntity[], publicSearch = false): void {
    for (const item of items) {
      const key = fieldEntityKey(item.entity);
      if (publicSearch && metadata.get(key)?.personAction === 'chat') continue;
      metadata.set(key, item);
    }
    metadataRevision++;
  }
  function pruneMetadata(): void {
    const keep = new Set([...allMetadata(), ...available].map(item => fieldEntityKey(item.entity)));
    for (const shortcut of (engine?.snapshot() ?? state?.document)?.shortcuts ?? []) keep.add(fieldEntityKey(shortcut.entity));
    for (const key of metadata.keys()) if (!keep.has(key)) metadata.delete(key);
  }
  function allMetadata(): DirectoryEntity[] {
    if (mode === 'search') return [...new Map([...BUILTINS, ...searchItems].map(item => [fieldEntityKey(item.entity), item])).values()];
    const needed = new Set([...BUILTINS, ...searchItems].map(item => fieldEntityKey(item.entity)));
    for (const shortcut of (engine?.snapshot() ?? state?.document)?.shortcuts ?? []) needed.add(fieldEntityKey(shortcut.entity));
    return [...metadata.values()].filter(item => needed.has(fieldEntityKey(item.entity)));
  }
  function overviewApps(): OwnAppOverview[] {
    return ownedAppsState === 'ready' && engine ? unplacedOwnApps(ownedApps, options.accountId, engine.snapshot().shortcuts) : [];
  }
  function overviewEntity(app: OwnAppOverview): DirectoryEntity {
    return { entity: { kind: 'app', id: app.id }, title: app.name, source: 'owner' };
  }
  function currentEntity(ref: FieldEntityRef): DirectoryEntity | undefined {
    const key = fieldEntityKey(ref);
    if (mode === 'search') return searchItems.find(item => fieldEntityKey(item.entity) === key);
    const own = ref.kind === 'app' ? overviewApps().find(app => app.id === ref.id) : undefined;
    return own ? overviewEntity(own) : metadata.get(key);
  }
  function renderOwnApps(): void {
    const visible = current() && !!engine && mode === 'mine' && element.dataset.search !== 'open' && !summary?.arranging && !queries.mine.trim() && ['all', 'app'].includes(filter);
    const apps = visible ? overviewApps() : [];
    const signature = JSON.stringify([visible, ownedAppsState, apps]);
    if (signature === ownAppsSignature) return;
    ownAppsSignature = signature; ownAppsOverview.replaceChildren();
    ownAppsOverview.hidden = !visible || ownedAppsState === 'loading' || ownedAppsState === 'ready' && apps.length === 0;
    if (ownAppsOverview.hidden) return;
    if (ownedAppsState === 'offline') {
      const status = el('p', 'uf-own-apps-status', 'Не удалось обновить ваши приложения. Проверьте подключение.'); status.setAttribute('role', 'status');
      ownAppsOverview.append(status); return;
    }
    const heading = el('h2', '', 'Ваши приложения'), list = el('div', 'uf-own-apps-list');
    for (const app of apps) {
      const label = app.state === 'offline' ? `${app.name} · нет связи` : app.state === 'stopped' ? `${app.name} · остановлено` : app.name;
      list.append(button(label, 'grid', 'uf-own-app', () => {
        if (!current() || mode !== 'mine') return;
        const fresh = overviewApps().find(value => value.id === app.id);
        if (fresh) options.onOpen(overviewEntity(fresh), null);
      }));
    }
    ownAppsOverview.append(heading, list);
  }
  async function loadOwnedApps(generation: number): Promise<void> {
    ownedApps = []; ownedAppsState = 'loading'; renderOwnApps();
    try {
      const result = await options.api.request<{ apps: unknown[] }>('apps.list', { expectedAccountId: options.accountId });
      if (!current() || generation !== mineGeneration) return;
      if (!Array.isArray(result.apps)) throw Error('invalid_apps_overview');
      ownedApps = result.apps; ownedAppsState = 'ready';
    } catch {
      if (!current() || generation !== mineGeneration) return;
      ownedApps = []; ownedAppsState = 'offline';
    }
    renderOwnApps(); renderMineSearchResults(); renderOpenList?.();
    if (engine && mode === 'mine') { emptyDocument = null; updateMineEmpty(engine.snapshot()); }
  }
  function placeSearch(): void {
    searchInput.placeholder = mode === 'mine' ? 'Найти на моём поле' : 'Приложения, люди, комнаты';
    if (search.parentElement !== localSearch) localSearch.prepend(search);
    if (listButton.parentElement !== localSearch) localSearch.append(listButton);
  }
  function updateChrome(): void {
    const focused = document.activeElement;
    element.dataset.mode = mode; element.dataset.filter = filter; title.textContent = mode === 'mine' ? 'Мои соты' : 'Найти своё';
    workspaceTools.hidden = mode !== 'mine';
    if (mode !== 'search') searchLimit.hidden = true;
    element.dataset.query = queries[mode] ? 'active' : 'empty';
    if (queries[mode]) element.dataset.search = 'open';
    element.setAttribute('aria-label', mode === 'mine' ? 'Мои соты' : 'Поиск в Сотах');
    mineButton.setAttribute('aria-pressed', String(mode === 'mine')); searchButton.setAttribute('aria-pressed', String(mode === 'search'));
    searchInput.value = queries[mode]; searchInput.placeholder = mode === 'mine' ? 'Найти в моих сотах' : 'Приложения, люди, комнаты';
    searchInput.setAttribute('aria-label', searchInput.placeholder); clear.hidden = !searchInput.value;
    arrange.hidden = mode !== 'mine'; spaces.hidden = mode !== 'mine';
    contextSelector.setAttribute('aria-label', mode === 'mine' ? 'Мои пространства' : 'Раздел поиска');
    contextSelector.title = mode === 'mine' ? 'Перейти к пространству на поле' : 'Перейти к разделу результатов';
    for (const kind of ['all', 'app', 'community', 'person', 'device'] as FieldFilter[]) {
      let target = filterButtons.get(kind);
      if (!target) { target = button(labels[kind], undefined, '', () => { filter = kind; filtersByMode[mode] = kind; updateChrome(); inputChanged(true); }); filterButtons.set(kind, target); filters.append(target); }
      target.setAttribute('aria-pressed', String(filter === kind)); target.hidden = mode === 'search' && kind === 'device';
    }
    compactFilter.replaceChildren(...(['all', 'app', 'community', 'person', 'device'] as FieldFilter[]).filter(kind => mode !== 'search' || kind !== 'device').map(kind => { const option = el('option', '', labels[kind]); option.value = kind; return option; }));
    compactFilter.value = filter;
    if (focused instanceof HTMLElement && filters.contains(focused) && !focused.getClientRects().length) searchInput.focus({ preventScroll: true });
    contextKey = ''; if (summary) updateSummary(summary); placeSearch(); renderMineSearchResults(); renderOwnApps();
  }
  function route(): void { options.onRoute(mode, queries[mode], filter); }
  function changeMode(next: UnifiedFieldMode): void { if (next === mode) return; closeList(false); closePreview(false); emptyDocument = null; filtersByMode[mode] = filter; mode = next; filter = filtersByMode[mode]; if (filter === 'device' && mode === 'search') filter = 'all'; engine?.cancel(); engine?.update({ mode, entities: allMetadata() }); updateChrome(); route(); if (mode === 'search') run(searchDirectory()); else { updateMineFilter(); if (engine) revealReadyMine(engine); } }
  function inputChanged(immediate = false): void {
    closeList(false);
    queries[mode] = searchInput.value; clear.hidden = !searchInput.value; route();
    element.dataset.query = queries[mode] ? 'active' : 'empty';
    renderOwnApps();
    if (searchTimer) clearTimeout(searchTimer);
    if (mode === 'mine') { updateMineFilter(); return; }
    searchGeneration++; searchCursor = null; more.hidden = true; searchLimit.hidden = true;
    closePreview(false);
    if (!composing) { if (immediate) run(searchDirectory()); else searchTimer = setTimeout(() => run(searchDirectory()), 160); }
  }
  searchInput.addEventListener('input', () => inputChanged(), { signal });
  searchInput.addEventListener('compositionstart', () => { composing = true; if (searchTimer) clearTimeout(searchTimer); }, { signal });
  searchInput.addEventListener('compositionend', () => { composing = false; inputChanged(true); }, { signal });
  searchInput.addEventListener('keydown', event => {
    if (event.isComposing || composing) return;
    if ((event.key === 'ArrowDown' || event.key === 'Enter') && mode === 'mine' && !mineSearchResults.hidden) {
      const first = mineSearchList.querySelector<HTMLButtonElement>('button:not([disabled])');
      if (first) { event.preventDefault(); if (event.key === 'Enter') first.click(); else first.focus(); }
    }
    if (event.key === 'Escape') {
      event.stopPropagation();
      if (searchInput.value) { searchInput.value = ''; inputChanged(true); }
      else closeSearch.click();
    }
  }, { signal });
  wide.addEventListener('change', placeSearch, { signal });
  function updateMineFilter(): void {
    if (!engine || mode !== 'mine') return;
    const doc = summary?.document ?? engine.snapshot();
    let initialEntities: FieldDirectoryEntity[] | undefined;
    if (!mineFilterAdmitted) {
      const query = queries.mine.trim().toLocaleLowerCase('ru');
      const contextMatch = !!query && doc.contexts.some(context => context.title.toLocaleLowerCase('ru').includes(query));
      const knownMatch = doc.shortcuts.some(shortcut => {
        const key = fieldEntityKey(shortcut.entity), item = metadata.get(key);
        return mineKnownRefs.has(key) && item && matches(item, queries.mine, filter);
      });
      if (mineConfirmedGeneration !== mineGeneration || query && !contextMatch && !knownMatch && doc.shortcuts.some(shortcut => !mineKnownRefs.has(fieldEntityKey(shortcut.entity)))) {
        updateMineEmpty(doc); return;
      }
      initialEntities = allMetadata();
      mineFilterAdmitted = true;
    }
    engine.update({ ...(initialEntities ? { entities: initialEntities } : {}), filter: queries.mine, visibleKinds: filter === 'all' ? ['app', 'builtin', 'person', 'community', 'device'] : filter === 'app' ? ['app', 'builtin'] : [filter] });
    updateMineEmpty(summary?.document ?? engine.snapshot());
  }
  function updateMineEmpty(doc: FieldDocument): void {
    if (mode !== 'mine') return;
    renderMineSearchResults();
    if (mineResolvedGeneration !== mineGeneration && element.dataset.mineState !== 'error') {
      empty.hidden = true; emptyDocument = null; return;
    }
    const scope = `${queries.mine}:${filter}`;
    if (doc === emptyDocument && scope === emptyScope && metadataRevision === emptyMetadataRevision) return;
    emptyDocument = doc; emptyScope = scope; emptyMetadataRevision = metadataRevision;
    const query = queries.mine.trim().toLocaleLowerCase('ru');
    const matchingContexts = new Set(doc.contexts.filter(context => context.title.toLocaleLowerCase('ru').includes(query)).map(context => context.contextId));
    const count = doc.shortcuts.filter(shortcut => {
      const item = metadata.get(fieldEntityKey(shortcut.entity));
      return item && matches(item, matchingContexts.has(shortcut.contextId) ? '' : queries.mine, filter);
    }).length;
    const availableOwnApps = overviewApps().some(app => matches(overviewEntity(app), queries.mine, filter));
    empty.hidden = count > 0 || availableOwnApps || !!query && matchingContexts.size > 0;
    if (!empty.hidden && (ownedAppsState !== 'ready' || element.dataset.mineState === 'error' || doc.shortcuts.some(shortcut => !mineKnownRefs.has(fieldEntityKey(shortcut.entity))))) {
      empty.replaceChildren(el('h2', '', 'Доступ пока не подтверждён'), button('Повторить', 'refresh', 'sw-button-quiet', () => run(loadMine()))); return;
    }
    if (!empty.hidden) empty.replaceChildren(el('h2', '', queries.mine ? 'Ничего не найдено' : filter !== 'all' ? 'В этом разделе пока пусто' : 'Здесь будет ваше пространство'), el('p', '', queries.mine ? 'Попробуйте другое название.' : 'Добавьте приложение, человека или комнату.'), button(queries.mine ? 'Очистить поиск' : filter !== 'all' ? 'Показать всё' : 'Добавить на поле', queries.mine ? 'close' : filter !== 'all' ? 'layers' : 'plus', 'sw-button-primary', () => {
      if (queries.mine) { searchInput.value = ''; inputChanged(); searchInput.focus(); }
      else if (filter !== 'all') { filter = 'all'; filtersByMode.mine = 'all'; updateChrome(); inputChanged(true); }
      else openAdd();
    }));
  }
  function updateSummary(next: UnifiedFieldSummary): void {
    const documentChanged = next.document !== summary?.document;
    if (summary && (next.document !== summary.document || next.focusContextId !== summary.focusContextId || next.arranging !== summary.arranging)) closeList(false);
    if (next.mode === 'mine' && next.focusContextId !== summary?.focusContextId && current()) options.onContextChange?.(next.focusContextId);
    if (mode === 'mine' && selected?.shortcutId) {
      const inspected = next.document.shortcuts.find(shortcut => shortcut.shortcutId === selected?.shortcutId);
      if (!inspected || next.focusContextId && inspected.contextId !== next.focusContextId) closePreview(false);
    }
    summary = next; arrange.setAttribute('aria-pressed', String(next.arranging)); arrange.querySelector('span')!.textContent = next.arranging ? 'Готово' : 'Расставить';
    if (documentChanged) publishKnownApps(next.document);
    element.dataset.arranging = String(next.arranging);
    element.dataset.focusContext = next.focusContextId;
    arrangeHint.hidden = !next.arranging;
    const zoomValue = `${Math.round(next.camera.scale * 100)}%`; if (percent.value !== zoomValue) percent.value = zoomValue;
    if (viewPercent?.isConnected && viewPercent.value !== zoomValue) viewPercent.value = zoomValue;
    undo.hidden = !next.canUndo || mode !== 'mine';
    const contexts = mode === 'mine' ? next.document.contexts : searchScene().contexts;
    const key = JSON.stringify(contexts.map(context => [context.contextId, context.title]));
    if (contextKey !== key) { contextKey = key; contextSelector.replaceChildren(el('option', '', mode === 'mine' ? 'Все пространства' : 'Все результаты'), ...contexts.map(context => { const option = el('option', '', context.title); option.value = context.contextId; return option; })); contextSelector.firstElementChild!.setAttribute('value', ''); }
    contextSelector.value = next.focusContextId || ''; updateMineEmpty(next.document); updateStatus(); renderOwnApps();
  }
  function publishKnownApps(document = engine?.snapshot()): void {
    if (!current() || !document || !options.onKnownAppEntries) return;
    const entries = new Map<string, DirectoryApp['entry']>();
    for (const shortcut of document.shortcuts) {
      if (shortcut.entity.kind !== 'app' || !mineKnownRefs.has(fieldEntityKey(shortcut.entity))) continue;
      const record = directory.getRecord(shortcut.entity);
      if (record && 'entry' in record) entries.set(record.entry.appId, record.entry);
    }
    const values = [...entries.values()], signature = JSON.stringify(values);
    if (signature !== knownAppSignature) { knownAppSignature = signature; options.onKnownAppEntries(values); }
  }
  function updateStatus(): void {
    if (!state) return;
    feedback.dataset.state = state.state;
    const key = `${state.state}:${state.localDurable}:${state.pendingCount}`;
    if (statusKey === key) return; statusKey = key; feedback.replaceChildren();
    feedback.hidden = state.state === 'saved' || state.state === 'loading';
    if (feedback.hidden) return;
    const copy = state.state === 'saving' ? 'Сохраняем…' : state.state === 'conflict' ? 'Поле изменилось в другом окне' : state.state === 'storage-error' ? 'Не удалось сохранить на этом устройстве' : state.localDurable ? 'Сохранено на устройстве · ждёт сети' : 'Изменения ещё не сохранены';
    feedback.append(el('span', '', copy));
    if (state.state === 'conflict') feedback.append(button('Выбрать версию', 'layers', '', () => conflictDialog()));
    else if (state.state !== 'saving') {
      feedback.append(button('Повторить', 'refresh', '', () => run(persistence.retry().then(applyPersistence))), button('Скачать раскладку', 'download', '', exportLayout));
      if (!state.localDurable && persistence.hasUnsavedChanges()) feedback.append(button('Отменить несохранённое', 'back', '', () => {
        const confirm = dialog('Отменить несохранённую расстановку?'); confirm.body.append(el('p', '', 'Перед отменой можно скачать свою раскладку. На поле вернётся последняя сохранённая версия.'), button('Скачать раскладку', 'download', 'sw-button-wide', exportLayout), button('Отменить изменения', 'back', 'sw-button-wide', () => run(persistence.discardVolatile().then(next => { if (!current()) return; engine?.destroy(); engine = null; mountEngine(next); confirm.close(); }))));
      }));
    }
  }
  function applyPersistence(next: FieldPersistenceState): void {
    if (!current()) return; closeList(false); state = next; updateStatus();
    if (!engine && next.state !== 'loading' && next.document.contexts.length) mountEngine(next);
    if (engine && next.state === 'saved' && !engine.hasUnsavedChanges()) {
      const previousRefs = new Set(engine.snapshot().shortcuts.map(shortcut => fieldEntityKey(shortcut.entity)));
      const newRefs = next.document.shortcuts.some(shortcut => !previousRefs.has(fieldEntityKey(shortcut.entity)));
      if (newRefs) { mineResolvedGeneration = -1; empty.hidden = true; }
      engine.update({ document: next.document, revision: next.projectedRevision, persistence: 'saved' }); updateMineFilter();
      // A layout received from another window/device contains references, not
      // names or access. Resolve new identities through the existing directory.
      if (newRefs) run(element.dataset.ready === 'loading' ? loadInitialMine() : loadMine());
    }
  }
  function exportLayout(): void {
    const url = URL.createObjectURL(new Blob([JSON.stringify(persistence.exportPending(), null, 2)], { type: 'application/json' }));
    const link = el('a'); link.href = url; link.download = 'soty-field.json'; link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
  }
  function conflictDialog(): void {
    const instance = dialog('Две версии поля');
    instance.body.append(el('p', '', 'В другом окне изменили расстановку. Скачайте свою версию или выберите, какую оставить.'), button('Скачать мою версию', 'download', 'sw-button-wide', exportLayout));
    const choose = (choice: 'local' | 'remote'): void => { for (const target of instance.body.querySelectorAll<HTMLButtonElement>('button')) target.disabled = true;
      run(persistence.resolveConflict(choice).then(next => { if (!current()) return; engine?.destroy(); engine = null; mountEngine(next); instance.close(); options.onMessage(choice === 'local' ? 'Сохранена ваша расстановка' : 'Открыта другая версия'); }).finally(() => { if (instance.element.open) for (const target of instance.body.querySelectorAll<HTMLButtonElement>('button')) target.disabled = false; })); };
    instance.body.append(button('Использовать другую версию', 'refresh', 'sw-button-wide', () => choose('remote')), button('Сохранить мою вместо другой', 'check', 'sw-button-wide', () => choose('local')));
    if (state && !state.localDurable && persistence.hasUnsavedChanges()) instance.body.append(button('Отменить несохранённое', 'back', 'sw-button-wide', () => {
      const confirm = dialog('Отменить несохранённую расстановку?');
      confirm.body.append(el('p', '', 'Можно сначала скачать свою раскладку. После отмены откроется последняя версия, сохранённая на этом устройстве.'), button('Скачать раскладку', 'download', 'sw-button-wide', exportLayout), button('Отменить изменения', 'back', 'sw-button-wide', () => run(persistence.discardVolatile().then(next => {
        if (!current()) return; engine?.destroy(); engine = null; mountEngine(next); confirm.close(); instance.close();
      }))));
    }));
  }
  function searchScene(): FieldDocument {
    return activeSearchScene;
  }
  function searchSlot(): Promise<boolean> {
    if (activeSearches < 2) { activeSearches++; return Promise.resolve(true); }
    queuedSearch?.(false);
    return new Promise(resolve => { queuedSearch = resolve; });
  }
  function releaseSearchSlot(): void {
    activeSearches--;
    if (queuedSearch) { const waiting = queuedSearch; queuedSearch = null; activeSearches++; waiting(true); }
  }
  async function searchDirectory(append = false, preserveView = false): Promise<void> {
    if (mode !== 'search') return;
    closeList(false);
    const generation = ++searchGeneration, query = queries.search, selectedFilter = filter;
    const scope = `${query}:${selectedFilter}`;
    const freshScope = searchResponseScope !== scope || searchItems.length === 0;
    const restoreView = !searchResponseScope && options.viewState?.search && options.viewState.searchScope === scope;
    const builtins = selectedFilter === 'all' || selectedFilter === 'app' ? BUILTINS.filter(item => matches(item, query, 'app')) : [];
    const arriving = new Map((append || !freshScope ? searchItems : builtins).map(item => [fieldEntityKey(item.entity), item]));
    let fitted = false, resultSize = 0;
    const live = (): boolean => current() && generation === searchGeneration && mode === 'search' && query === queries.search && selectedFilter === filter;
    const display = (items: DirectoryEntity[], progress = false): void => {
      if (!live()) return;
      const combined = progress ? [...arriving.values()] : append ? [...new Map([...searchItems, ...items].map(item => [fieldEntityKey(item.entity), item])).values()] : [...builtins, ...items];
      if (progress && !combined.length) return;
      resultSize = combined.length;
      const next = combined.slice(0, 256), changed = searchResponseScope !== scope || JSON.stringify(next) !== JSON.stringify(searchItems);
      if (selected && (searchResponseScope !== scope || !progress && !append && !combined.some(item => fieldEntityKey(item.entity) === fieldEntityKey(selected!.item.entity)))) closePreview(false);
      searchItems = next; merge(items, true); searchResponseScope = scope; pruneMetadata();
      if (changed) {
        const scene = searchPlacement.update(scope, searchItems, allMetadata()); activeSearchScene = scene;
        engine?.update({ entities: allMetadata(), searchDocument: scene, scope });
        if (!fitted && !preserveView && !restoreView && freshScope && engine && scene.contexts.length) {
          fitted = true;
          if (!wide.matches) { engine.focusContext(scene.contexts[0]!.contextId); if (progress) engine.setCamera(engine.getCamera()); }
          else {
            const layout = layoutUnifiedField(scene, allMetadata()), visibleContexts = layout.contexts.slice(0, 2);
            const bounds = fieldBounds(visibleContexts.flatMap(context => rectPoints(context.bounds)));
            const viewport = engine.element;
            engine.setCamera(fitFieldCamera(bounds, { width: viewport.clientWidth, height: viewport.clientHeight }, { padding: 30, maxScale: 1 }));
          }
        }
      }
      if (progress) { element.dataset.searchState = 'partial'; empty.hidden = true; }
    };
    more.disabled = true; element.dataset.searchState = 'loading'; element.dataset.searchPending = 'true'; empty.hidden = true; searchLimit.hidden = true;
    if (!await searchSlot()) return;
    try {
      if (!live()) return;
      const page = await directory.search({ query, kinds: selectedFilter === 'all' ? ['app', 'person', 'community'] : [selectedFilter], ...(append && searchCursor ? { cursor: searchCursor } : {}), limit: 36,
        onProgress: progress => { if (!live()) return; for (const item of progress.items) arriving.set(fieldEntityKey(item.entity), item); display(progress.items, true); } });
      if (!live()) return;
      display(page.items);
      searchLimit.hidden = !(resultSize > 256 || Boolean(page.cursor) && searchItems.length >= 256);
      searchCursor = page.cursor; more.hidden = !searchCursor || searchItems.length >= 256;
      element.dataset.searchState = page.status;
      empty.hidden = searchItems.length > 0;
      if (!searchItems.length) {
        const title = filter === 'community' ? 'Комнаты не найдены' : filter === 'person' ? 'Люди не найдены' : filter === 'app' ? 'Приложения не найдены' : 'Пока ничего не найдено';
        const hint = query ? 'Попробуйте другое название.' : filter === 'community' ? 'Здесь появятся доступные вам комнаты.' : filter === 'person' ? 'Здесь появятся люди, открывшие свой профиль для поиска.' : 'Здесь появятся доступные приложения, люди и комнаты.';
        empty.replaceChildren(el('h2', '', page.status === 'offline' ? 'Поиск ждёт подключения' : title), el('p', '', page.status === 'offline' ? 'Ваше поле остаётся доступным.' : hint), button(page.status === 'offline' ? 'Повторить' : 'На моё поле', page.status === 'offline' ? 'refresh' : 'back', 'sw-button-quiet', () => page.status === 'offline' ? run(searchDirectory()) : changeMode('mine')));
      }
      else if (page.status === 'partial') options.onMessage('Часть результатов временно недоступна. Можно повторить поиск.');
      if (summary) { contextKey = ''; updateSummary(summary); }
    } catch (error) {
      if (!live()) return;
      element.dataset.searchState = 'offline'; empty.hidden = false;
      empty.replaceChildren(el('h2', '', 'Не удалось обновить поиск'), el('p', '', 'Попробуйте ещё раз. Ваша расстановка сохранена отдельно.'), button('Повторить', 'refresh', 'sw-button-quiet', () => run(searchDirectory()))); throw error;
    } finally { releaseSearchSlot(); if (live()) { more.disabled = false; element.dataset.searchPending = 'false'; } }
  }
  function loadInitialMine(): Promise<void> {
    const refs = JSON.stringify([...new Set(engine?.snapshot().shortcuts.map(shortcut => fieldEntityKey(shortcut.entity)) ?? [])].sort());
    if (!initialMineTask || refs !== initialMineRefs) {
      initialMineRefs = refs; initialMineTask = loadMine();
    }
    return initialMineTask;
  }
  async function loadMine(): Promise<void> {
    if (!engine || !current()) return; closeList(false); const generation = ++mineGeneration;
    mineConfirmedGeneration = -1; empty.hidden = true; element.dataset.mineState = 'loading'; renderMineSearchResults();
    if (mineBusy) {
      queuedMine?.(false);
      if (!await new Promise<boolean>(resolve => { queuedMine = resolve; })) return;
    } else mineBusy = true;
    try { if (current() && generation === mineGeneration && engine) await loadMinePass(generation); }
    finally {
      if (queuedMine) { const next = queuedMine; queuedMine = null; next(true); }
      else mineBusy = false;
    }
  }
  async function loadMinePass(generation: number): Promise<void> {
    if (!engine) return;
    const resolvedUnavailableRefs = new Set<string>();
    let accepting = true;
    const live = (): boolean => accepting && current() && generation === mineGeneration;
    const view = (): string => JSON.stringify({ camera: engine!.getCamera(), context: summary?.focusContextId, selection: summary?.selectedShortcutId, fit: options.viewState?.mineFit });
    const firstView = firstResolve ? view() : null;
    mineConfirmedGeneration = -1;
    empty.hidden = true; element.dataset.mineState = 'loading';
    mineKnownRefs = new Set(BUILTINS.map(item => fieldEntityKey(item.entity)));
    const refs = [...new Map(engine.snapshot().shortcuts.map(shortcut => [fieldEntityKey(shortcut.entity), shortcut.entity])).values()];
    const resolved = { items: [] as DirectoryEntity[], unavailable: [] as FieldEntityRef[], errors: [] as { source: string; code: string }[] };
    const accept = (batch: typeof resolved): void => {
      if (!live()) return;
      for (const ref of batch.unavailable) {
        const key = fieldEntityKey(ref); resolvedUnavailableRefs.add(key);
        metadata.set(key, { entity: ref, title: batch.errors.length ? 'Ждёт подключения' : 'Недоступно', symbol: 'lock', source: 'owner' });
        if (!batch.errors.length) mineKnownRefs.add(key);
      }
      merge(batch.items); for (const item of batch.items) mineKnownRefs.add(fieldEntityKey(item.entity));
      resolved.items.push(...batch.items); resolved.unavailable.push(...batch.unavailable); resolved.errors.push(...batch.errors);
      mineConfirmedGeneration = generation;
      engine!.update({ entities: allMetadata() }); updateMineFilter(); loading.hidden = true;
      publishKnownApps();
      if (batch.items.some(item => item.entity.kind === 'app')) element.dataset.startupAppsAt ??= String(Date.now());
      if (selected && batch.unavailable.some(ref => fieldEntityKey(ref) === fieldEntityKey(selected!.item.entity))) closePreview(false);
    };
    try {
      // Independent sources publish confirmed batches without moving the user's saved shortcuts.
      let failed = false, firstError: unknown;
      await Promise.allSettled([
        refs.filter(ref => ref.kind === 'person' || ref.kind === 'community'),
        refs.filter(ref => ref.kind === 'app'),
        refs.filter(ref => ref.kind === 'device'),
      ].map(async sourceRefs => {
        try {
          for (let offset = 0; offset < sourceRefs.length; offset += 60) {
            if (!live()) return;
            const batch = await directory.resolve(sourceRefs.slice(offset, offset + 60));
            if (!live()) return;
            accept(batch);
          }
        } catch (error) { accepting = false; if (!failed) { failed = true; firstError = error; } throw error; }
      }));
      if (failed) throw firstError;
      if (!live()) return;
      mineConfirmedGeneration = generation;
      const first = await directory.loadMine({ limit: 60, onProgress: progress => {
        if (!live()) return;
        available = [...new Map([...available, ...progress.items].map(item => [fieldEntityKey(item.entity), item])).values()];
        merge(progress.items); engine!.update({ entities: allMetadata() }); updateMineFilter();
      } });
      if (!live()) return;
      available = first.items; availableCursor = first.cursor; merge(first.items); pruneMetadata();
      mineResolvedGeneration = generation;
      element.dataset.mineState = engine.snapshot().shortcuts.some(shortcut => !mineKnownRefs.has(fieldEntityKey(shortcut.entity))) ? 'unknown' : 'ready';
      engine.update({ entities: allMetadata() }); updateMineFilter();
      if (selected && mode === 'mine') {
        const latest = currentEntity(selected.item.entity);
        if (!latest || resolved.unavailable.some(ref => fieldEntityKey(ref) === fieldEntityKey(selected!.item.entity))) closePreview(false);
        else if (JSON.stringify(latest) !== JSON.stringify(selected.item)) openPreview(latest, selected.shortcutId);
      }
      if (firstResolve && mode === 'mine') {
        firstResolve = false;
        if (!selected && options.viewState?.mineFit !== 'manual' && firstView === view()) {
          if (wide.matches) engine.fitOverview(); else engine.focusContext(engine.snapshot().contexts[0]?.contextId ?? '');
        }
      }
    } catch (error) {
      if (current() && generation === mineGeneration) {
        for (const shortcut of engine.snapshot().shortcuts) if (!mineKnownRefs.has(fieldEntityKey(shortcut.entity))) metadata.set(fieldEntityKey(shortcut.entity), { entity: shortcut.entity, title: 'Ждёт подключения', symbol: 'lock', source: 'owner' });
        metadataRevision++; element.dataset.mineState = 'error'; emptyDocument = null;
        engine.update({ entities: allMetadata() }); updateMineFilter();
        if (selected && (!mineKnownRefs.has(fieldEntityKey(selected.item.entity)) || resolvedUnavailableRefs.has(fieldEntityKey(selected.item.entity)))) closePreview(false);
      }
      throw error;
    } finally {
      accepting = false;
      if (current() && generation === mineGeneration) {
        loading.hidden = true; if (mineResolvedGeneration !== generation) element.dataset.mineState = 'error';
        await loadOwnedApps(generation);
      }
    }
  }
  function closePreview(restore = true): void {
    preview?.remove(); preview = null;
    delete element.dataset.preview;
    if (restore) {
      const target = selected?.shortcutId ? element.querySelector<HTMLButtonElement>(`[data-shortcut-id="${CSS.escape(selected.shortcutId)}"]`) : previewOpener;
      if (target?.isConnected && target.getClientRects().length && getComputedStyle(target).visibility !== 'hidden') target.focus({ preventScroll: true });
      if (!target || document.activeElement !== target) focusFieldControl();
    }
    previewOpener = null;
    selected = null;
  }
  function openPreview(item: DirectoryEntity, shortcutId?: string): void {
    const active = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const opener = active && preview?.contains(active) ? previewOpener : active && (element.contains(active) || active === searchInput) ? active : previewOpener;
    closePreview(false); previewOpener = opener; selected = { item, ...(shortcutId ? { shortcutId } : {}) };
    const panel = el('aside', 'uf-preview'); panel.setAttribute('aria-label', `Об объекте: ${item.title}`);
    panel.tabIndex = -1; const close = iconButton('Закрыть карточку', 'close', () => closePreview()); close.classList.add('uf-preview-close');
    const copy = el('div', 'uf-preview-content'); copy.append(el('small', 'uf-eyebrow', kindLabel(item.entity.kind)), el('h2', '', item.title));
    if (mode === 'mine' && shortcutId) {
      const doc = engine?.snapshot(), placement = doc?.shortcuts.find(shortcut => shortcut.shortcutId === shortcutId);
      const context = doc?.contexts.find(context => context.contextId === placement?.contextId);
      if (context) copy.append(el('p', 'uf-preview-placement', `В пространстве «${context.title}»`));
    }
    if (item.entity.kind === 'person') copy.prepend(avatar(item.title, item.avatarUrl, item.color, item.entity.id, item.avatarRevision));
    if (item.description) copy.append(el('p', '', item.description));
    const record = directory.getRecord(item.entity, item.personAction), actions = el('div', 'uf-preview-actions');
    if (record && 'kind' in record && record.kind === 'contact') copy.append(el('p', 'uf-preview-meta', 'Ваш контакт'));
    if (item.entity.kind === 'community' && record && 'communityId' in record) {
      const group = record as WorldCommunity;
      const emblem = el('div', 'uf-preview-emblem'); emblem.setAttribute('aria-hidden', 'true'); emblem.append(icon(group.symbol || 'people')); copy.prepend(emblem);
      copy.append(el('span', 'uf-preview-badge', group.membership?.state === 'active' ? 'Вы участник' : group.joinPolicy === 'open' ? 'Открытая группа' : group.joinPolicy === 'request' ? 'По заявке' : 'По приглашению'));
      const members = el('div', 'uf-preview-members');
      for (const person of group.previewMembers.slice(0, 3)) { const face = avatar(person.displayName, person.avatarUrl, person.avatarColor, person.profileId, person.avatarRevision); face.title = person.displayName; members.append(face); }
      if (group.previewMembers.length) copy.append(members);
      copy.append(el('p', 'uf-preview-meta', nounCount(group.memberCount, 'участник', 'участника', 'участников')));
      if (group.showcase) { const showcase = el('section', 'uf-preview-showcase'); showcase.append(el('h3', '', 'О комнате'), el('p', '', group.showcase)); copy.append(showcase); }
      if (group.membership?.state === 'active') actions.append(button('Открыть чат', 'chat', 'sw-button-primary', () => options.onOpen(item, directory.getRecord(item.entity))));
      else {
        const join = button(group.membership?.state === 'invited' ? 'Принять приглашение' : group.membership?.state === 'requested' ? 'Заявка отправлена' : group.joinPolicy === 'open' ? 'Вступить' : group.joinPolicy === 'request' ? 'Подать заявку' : 'По приглашению', 'people', 'sw-button-primary');
        join.disabled = ['requested', 'banned'].includes(group.membership?.state ?? '') || group.joinPolicy === 'invite' && group.membership?.state !== 'invited';
        join.addEventListener('click', () => {
          const selection = selected, selectedMode = mode; join.disabled = true;
          run(options.api.request<{ community: WorldCommunity }>('world.membership.join', { communityId: group.communityId }).then(async result => {
            if (!current()) return; await loadMine(); if (!current()) return;
            const refreshed = currentEntity(item.entity);
            if (refreshed && selected === selection && mode === selectedMode) openPreview(refreshed, shortcutId);
            options.onMessage(result.community.membership?.state === 'active' ? 'Вы в комнате' : 'Заявка отправлена');
          }).finally(() => { if (join.isConnected) join.disabled = false; }));
        });
        actions.append(join, button('О комнате', 'info', 'sw-button-quiet', () => options.onOpen(item, directory.getRecord(item.entity))));
      }
    } else {
      const ownProfile = item.entity.kind === 'person' && item.entity.id === options.accountId;
      const contact = item.entity.kind === 'person' && record && 'kind' in record && record.kind === 'contact';
      const openLabel = ownProfile ? 'Редактировать профиль' : contact ? options.directMessagesEnabled ? 'Написать' : 'Контакт' : item.entity.kind === 'person' ? 'Профиль' : item.entity.kind === 'device' ? 'Устройства' : 'Открыть';
      const open = button(openLabel, contact && options.directMessagesEnabled ? 'chat' : item.entity.kind === 'person' ? 'person' : 'external', 'sw-button-primary', () => options.onOpen(item, directory.getRecord(item.entity, item.personAction)));
      if (item.entity.kind === 'person') open.setAttribute('aria-label', contact ? openLabel : ownProfile ? 'Редактировать профиль' : 'Профиль и контакт');
      else if (item.entity.kind === 'device') open.setAttribute('aria-label', 'Управление устройствами');
      open.disabled = !ownProfile && !record && ['person', 'device', 'community'].includes(item.entity.kind); actions.append(open);
    }
    if (item.entity.kind === 'app' && record && 'canManage' in record && record.canManage && options.onAppSettings) actions.append(button('Настройки приложения', 'settings', 'sw-button-quiet', () => options.onAppSettings!(record as DirectoryApp)));
    const alreadyPlaced = engine?.snapshot().shortcuts.some(shortcut => fieldEntityKey(shortcut.entity) === fieldEntityKey(item.entity));
    actions.append(button(alreadyPlaced ? 'Добавить в другое пространство' : 'На моё поле', 'plus', 'sw-button-quiet', () => addToContext(item)));
    if (mode === 'mine' && shortcutId) actions.append(button('Переместить', 'sliders', 'sw-button-quiet', () => { closePreview(); engine?.setArrange(true); engine?.beginMove(shortcutId); }), button('Убрать с поля', 'close', 'sw-button-quiet', () => { closePreview(); run(engine?.removeShortcut(shortcutId).then(() => updateMineFilter())); }));
    copy.append(actions); panel.append(close, copy); element.append(panel); element.dataset.preview = 'open'; preview = panel;
    panel.addEventListener('keydown', event => { if (event.key === 'Escape') { event.stopPropagation(); closePreview(); } }, { signal });
    panel.focus({ preventScroll: true });
  }
  function inspect(entity: FieldDirectoryEntity, shortcutId: string): void {
    if (!current()) return;
    const item = currentEntity(entity.entity); if (!item) return;
    closePreview(false);
    const capturedEngine = engine, capturedMode = mode, entityKey = fieldEntityKey(item.entity);
    const placement = capturedMode === 'mine' ? (): void => {
      if (!current() || !capturedEngine || engine !== capturedEngine || mode !== capturedMode
        || !capturedEngine.snapshot().shortcuts.some(value => value.shortcutId === shortcutId && fieldEntityKey(value.entity) === entityKey)) return;
      const fresh = currentEntity(item.entity); if (fresh) openPreview(fresh, shortcutId);
    } : undefined;
    if (!options.onInspectOwn?.(item, directory.getRecord(item.entity, item.personAction), placement)) openPreview(item, shortcutId);
  }
  function closeList(restoreFocus = true): void {
    const instance = listDialog; listDialog = null; renderOpenList = null; instance?.close({ restoreFocus });
  }
  function listEntries(localQuery = ''): { key: string; title: string; detail: string; entity?: FieldEntityRef; shortcutId?: string; contextId?: string }[] {
    if (!current() || !engine) return [];
    if (mode === 'search' && (searchResponseScope !== `${queries.search}:${filter}` || element.dataset.searchState === 'loading' || element.dataset.searchState === 'offline')) return [];
    const privateReady = mode === 'search' || mineConfirmedGeneration === mineGeneration;
    const doc = mode === 'mine' ? engine.snapshot() : searchScene(), query = queries[mode].trim().toLocaleLowerCase('ru');
    const matchingContexts = new Set(doc.contexts.filter(context => context.title.toLocaleLowerCase('ru').includes(query)).map(context => context.contextId));
    const known = mode === 'mine' ? mineKnownRefs : new Set(searchItems.map(item => fieldEntityKey(item.entity)));
    const entries = new Map<string, { key: string; title: string; detail: string; entity: FieldEntityRef; shortcutId: string; contexts: Set<string> }>();
    const includedContexts = new Set<string>();
    for (const shortcut of doc.shortcuts) {
      const key = fieldEntityKey(shortcut.entity), item = currentEntity(shortcut.entity);
      if (!known.has(key) || !item || (item.entity.kind === 'builtin' ? !BUILTINS.some(builtin => fieldEntityKey(builtin.entity) === key) : (!privateReady || !directory.getRecord(item.entity, item.personAction)))) continue;
      if (mode === 'mine' && !matches(item, matchingContexts.has(shortcut.contextId) ? '' : queries.mine, filter)) continue;
      const context = doc.contexts.find(value => value.contextId === shortcut.contextId); if (!context) continue;
      includedContexts.add(context.contextId);
      let entry = entries.get(key);
      if (!entry) { entry = { key, title: item.title, detail: item.entity.kind === 'person' ? item.personAction === 'chat' ? 'Личный чат' : 'Профиль' : kindLabel(item.entity.kind), entity: item.entity, shortcutId: shortcut.shortcutId, contexts: new Set() }; entries.set(key, entry); }
      entry.contexts.add(context.title);
    }
    const rows: { key: string; title: string; detail: string; entity?: FieldEntityRef; shortcutId?: string; contextId?: string }[] = [...entries.values()].map(({ contexts, ...entry }) => ({ ...entry, detail: `${entry.detail} · ${[...contexts].join(', ')}` }));
    if (mode === 'mine') for (const app of overviewApps()) {
      const item = overviewEntity(app);
      if (!entries.has(fieldEntityKey(item.entity)) && matches(item, queries.mine, filter)) rows.push({ key: fieldEntityKey(item.entity), title: item.title, detail: 'Моё приложение · вне поля', entity: item.entity });
    }
    for (const context of doc.contexts) if (!query || matchingContexts.has(context.contextId) || includedContexts.has(context.contextId)) rows.push({ key: `context:${context.contextId}`, title: context.title, detail: 'Пространство', contextId: context.contextId });
    const find = localQuery.trim().toLocaleLowerCase('ru');
    return rows.filter(row => `${row.title} ${row.detail}`.toLocaleLowerCase('ru').includes(find));
  }
  function renderMineSearchResults(): void {
    const query = queries.mine, needle = query.trim().toLocaleLowerCase('ru');
    const recent = current() && engine && mode === 'mine' && !needle && !composing
      && element.dataset.search === 'open' && filter === 'all' && options.onOpenRecent
      ? options.getRecentWork?.().slice(0, 6) ?? [] : [];
    if (recent.length) {
      const signature = JSON.stringify(['recent', recent]);
      if (signature === mineSearchSignature) return;
      mineSearchSignature = signature; mineSearchResults.hidden = false;
      mineSearchStatus.textContent = 'Недавняя работа · в этом профиле'; mineSearchList.replaceChildren();
      for (const item of recent) {
        const action = button('', item.symbol, 'uf-list-entry uf-search-result', () => {
          if (!current() || mode !== 'mine' || queries.mine.trim() || composing || filter !== 'all'
            || element.dataset.search !== 'open' || mineSearchResults.hidden || !action.isConnected
            || !options.getRecentWork?.().some(value => value.route === item.route)) return;
          options.onOpenRecent?.(item.route);
        });
        const copy = el('span', 'uf-list-copy'); copy.append(el('strong', '', item.title), el('small', '', 'Продолжить'));
        action.replaceChildren(icon(item.symbol), copy); mineSearchList.append(action);
      }
      return;
    }
    if (!current() || !engine || mode !== 'mine' || !needle || composing) {
      mineSearchResults.hidden = true; mineSearchSignature = '';
      mineSearchStatus.textContent = ''; mineSearchList.replaceChildren(); return;
    }
    const rows = listEntries().filter(row => row.entity || row.title.toLocaleLowerCase('ru').includes(needle));
    const pending = mineConfirmedGeneration !== mineGeneration || element.dataset.mineState !== 'ready' || ownedAppsState !== 'ready';
    const signature = JSON.stringify([query, filter, pending, rows]);
    if (signature === mineSearchSignature) return;
    mineSearchSignature = signature; mineSearchResults.hidden = false;
    mineSearchStatus.textContent = rows.length ? 'Найдено: ' + rows.length + (pending ? ' · часть сот ещё проверяется' : '')
      : pending ? 'Доступ к сотам ещё не подтверждён.' : 'Ничего не найдено.';
    mineSearchList.replaceChildren();
    const capturedFilter = filter;
    const live = (): boolean => current() && mode === 'mine' && queries.mine === query && filter === capturedFilter
      && !composing && !mineSearchResults.hidden && mineSearchResults.isConnected;
    for (const row of rows.slice(0, 6)) {
      const action = button('', row.contextId ? 'layers' : 'next', 'uf-list-entry uf-search-result', () => {
        if (!live() || !action.isConnected) return;
        const fresh = listEntries().find(value => value.key === row.key);
        if (!fresh) { mineSearchSignature = ''; renderMineSearchResults(); return; }
        if (fresh.contextId) { closePreview(false); engine?.focusContext(fresh.contextId); }
        else if (fresh.entity) { const item = metadata.get(fieldEntityKey(fresh.entity)); if (item) activate(item, fresh.shortcutId); }
      });
      const copy = el('span', 'uf-list-copy'); copy.append(el('strong', '', row.title), el('small', '', row.detail));
      action.replaceChildren(icon(row.contextId ? 'layers' : 'next'), copy); mineSearchList.append(action);
    }
    if (rows.length > 6) mineSearchList.append(button('Открыть списком', 'list', 'uf-list-entry', () => {
      if (live()) openList();
    }));
  }
  function openList(initialQuery = ''): void {
    if (!current() || !engine || document.querySelector('dialog[open]')) return;
    closePreview(false);
    const capturedMode = mode, capturedQuery = queries[mode], capturedFilter = filter;
    const instance = dialog(mode === 'mine' ? 'Мои соты списком' : 'Результаты списком'); listDialog = instance;
    instance.element.classList.add('uf-list-dialog');
    instance.element.addEventListener('close', () => { if (listDialog === instance) { listDialog = null; renderOpenList = null; } }, { once: true });
    const find = textInput('', 'Найти в этом списке', 100); find.type = 'search'; find.autocomplete = 'off'; find.setAttribute('aria-label', 'Найти в этом списке');
    find.value = initialQuery.slice(0, 100);
    const status = el('p', 'uf-list-status'); status.setAttribute('role', 'status');
    const list = el('div', 'uf-list-results'); list.setAttribute('role', 'group'); list.setAttribute('aria-label', 'Соты и пространства');
    const live = (): boolean => current() && listDialog === instance && instance.element.open && mode === capturedMode && queries[mode] === capturedQuery && filter === capturedFilter && !Array.from(document.querySelectorAll('dialog[open]')).some(other => other !== instance.element);
    const render = (): void => {
      if (!live()) return;
      list.replaceChildren(); const rows = listEntries(find.value);
      for (const row of rows) {
        const action = button('', row.contextId ? 'layers' : 'next', 'uf-list-entry', () => {
          if (!live()) return;
          const fresh = listEntries(find.value).find(value => value.key === row.key);
          if (!fresh) { render(); status.textContent = 'Список изменился. Доступ к соте пока не подтверждён.'; find.focus({ preventScroll: true }); return; }
          closeList();
          if (!current()) return;
          if (fresh.contextId) { closePreview(false); engine?.focusContext(fresh.contextId); }
          else if (fresh.entity) { const item = currentEntity(fresh.entity); if (item) activate(item, fresh.shortcutId, { presentation: 'list', listQuery: find.value }); }
        });
        const copy = el('span', 'uf-list-copy'); copy.append(el('strong', '', row.title), el('small', '', row.detail)); action.replaceChildren(icon(row.contextId ? 'layers' : 'next'), copy); list.append(action);
      }
      const pending = mode === 'mine' ? mineConfirmedGeneration !== mineGeneration || element.dataset.mineState !== 'ready' || ownedAppsState !== 'ready' : element.dataset.searchPending === 'true' || element.dataset.searchState === 'loading' || element.dataset.searchState === 'offline' || searchResponseScope !== `${queries.search}:${filter}`;
      status.textContent = rows.length ? `${rows.length} в списке${pending ? ' · доступ к части сот ещё не подтверждён' : ''}` : pending ? 'Доступ к сотам пока не подтверждён. Закройте список и обновите поле.' : find.value.trim() ? 'В этом списке ничего не найдено.' : 'В текущем разделе нет сот.';
    };
    renderOpenList = render;
    find.addEventListener('input', render, { signal });
    instance.body.append(find, status, list);
    if (mode === 'search' && searchCursor) instance.body.append(el('p', 'uf-list-page-note', 'Здесь только загруженные результаты. «Показать ещё» на поле добавит следующую страницу.'));
    render();
    const focus = matchMedia('(pointer: coarse)').matches ? list.querySelector<HTMLButtonElement>('button') ?? instance.element.querySelector<HTMLButtonElement>('button') : find;
    focus?.focus({ preventScroll: true });
  }
  function activate(entity: FieldDirectoryEntity, shortcutId?: string, source?: { presentation: 'list'; listQuery: string }): void {
    if (!current()) return;
    const item = currentEntity(entity.entity); if (!item) return;
    const record = directory.getRecord(item.entity, item.personAction);
    const confirmed = mineConfirmedGeneration === mineGeneration && mineKnownRefs.has(fieldEntityKey(item.entity));
    const contact = confirmed && options.directMessagesEnabled === true && item.entity.kind === 'person' && item.entity.id !== options.accountId
      && record && 'kind' in record && record.kind === 'contact' && record.peerAccountId === item.entity.id;
    const joinedRoom = confirmed && item.entity.kind === 'community' && record && 'communityId' in record
      && record.communityId === item.entity.id && record.membership?.state === 'active';
    if (!summary?.arranging && (item.entity.kind === 'app' || mode === 'mine' && (item.entity.kind === 'builtin' || contact || joinedRoom))) { options.onOpen(item, record, source); return; }
    openPreview(item, shortcutId);
  }
  function addToContext(item: DirectoryEntity): void {
    const doc = engine?.snapshot(); if (!doc) return;
    const instance = dialog('Добавить на моё поле'), select = el('select', 'sw-select');
    for (const context of doc.contexts) { const option = el('option', '', context.title); option.value = context.contextId; select.append(option); }
    if (summary?.focusContextId) select.value = summary.focusContextId;
    const submit = button('Добавить', 'plus', 'sw-button-primary');
    submit.disabled = !doc.contexts.length;
    submit.addEventListener('click', () => { submit.disabled = true; const contextId = select.value;
      const latest = engine?.snapshot(); if (!latest) { submit.disabled = false; return; }
      let placement: ReturnType<typeof ensureFieldEntityPlacement>;
      try { placement = ensureFieldEntityPlacement(latest, { entity: item.entity, contextId, shortcutId: crypto.randomUUID() }); }
      catch (error) { submit.disabled = false; report(error); return; }
      if (!placement.changed) {
        const state = persistence.getState();
        if (state.state === 'conflict') { submit.disabled = false; options.onMessage('Поле изменилось. Проверьте расстановку.', true); return; }
        if (persistence.hasUnsavedChanges() || !state.localDurable) { submit.disabled = false; options.onMessage('Расстановка ещё не сохранена', true); return; }
        instance.close(); changeMode('mine'); engine?.focusContext(contextId);
        options.onMessage(state.pendingCount ? 'Уже добавлено на устройстве · ждём подключения' : 'Уже на вашем поле'); return;
      }
      changeMode('mine'); merge([item]); engine?.update({ entities: [...allMetadata(), item] });
      run(engine?.addShortcut(item.entity, contextId).then(receipt => {
        if (!current()) return; updateMineFilter();
        if (receipt?.status === 'conflict') { options.onMessage('Поле изменилось. Проверьте расстановку.', true); return; }
        if (receipt?.status === 'volatile' && !receipt.localDurable) { options.onMessage('Расстановка ещё не сохранена', true); return; }
        instance.close(); engine?.focusContext(contextId);
        options.onMessage(receipt?.status === 'volatile' ? 'Добавлено на устройстве · ждём подключения' : 'Добавлено на поле');
      }).finally(() => { submit.disabled = false; })); });
    instance.body.append(el('p', '', item.title), labeledField('Пространство', select), submit);
    if (!doc.contexts.length) instance.body.append(button('Создать пространство', 'plus', '', () => { instance.close(); newContext(); }));
  }
  function newContext(): void {
    const instance = dialog('Новое пространство'), input = textInput('', 'Например, Работа или Близкие', 64), form = el('form'), error = el('p', 'sw-error');
    input.required = true; const submit = button('Создать', 'plus', 'sw-button-primary'); submit.type = 'submit';
    form.append(labeledField('Название', input), error, submit);
    form.addEventListener('submit', event => { event.preventDefault(); if (!form.reportValidity() || !input.value.trim()) return; submit.disabled = true;
      const existing = new Set(engine?.snapshot().contexts.map(context => context.contextId));
      void engine?.addContext(input.value.trim()).then(() => {
        if (!current() || !instance.element.open) return; instance.close();
        const created = engine?.snapshot().contexts.find(context => !existing.has(context.contextId));
        if (created) { closePreview(false); engine?.focusContext(created.contextId); }
      }).catch(errorValue => { error.textContent = errorText(errorValue); }).finally(() => { submit.disabled = false; }); });
    instance.body.append(form); input.focus();
  }
  function manageContexts(): void {
    const instance = dialog('Пространства');
    for (const context of engine?.snapshot().contexts ?? []) {
      const row = el('div', 'uf-context-row'); row.append(button(context.title, 'cells', '', () => { instance.close(); engine?.focusContext(context.contextId); }), iconButton(`Переместить пространство ${context.title}`, 'sliders', () => {
        instance.close(); const mover = dialog('Переместить пространство'), select = el('select', 'sw-select');
        for (const other of engine?.snapshot().contexts ?? []) { if (other.contextId === context.contextId) continue; const option = el('option', '', other.title); option.value = other.contextId; select.append(option); }
        mover.body.append(labeledField('Разместить рядом с', select));
        const controls = el('div', 'uf-context-move-actions');
        for (const [side, label, symbol] of [['left', 'Слева', 'back'], ['right', 'Справа', 'forward'], ['above', 'Сверху', 'up'], ['below', 'Снизу', 'down']] as const) {
          const move = button(label, symbol, '', () => {
            const document = engine?.snapshot(); if (!engine || !document) return; const layout = layoutUnifiedField(document, allMetadata());
            const source = layout.contexts.find(item => item.contextId === context.contextId), target = layout.contexts.find(item => item.contextId === select.value); if (!source) return;
            const point = { x: source.x, y: source.y };
            if (target) {
              if (side === 'left') { point.x += target.bounds.left - source.bounds.right - 64; point.y += target.y - source.y; }
              if (side === 'right') { point.x += target.bounds.right - source.bounds.left + 64; point.y += target.y - source.y; }
              if (side === 'above') { point.y += target.bounds.top - source.bounds.bottom - 64; point.x += target.x - source.x; }
              if (side === 'below') { point.y += target.bounds.bottom - source.bounds.top + 64; point.x += target.x - source.x; }
            } else { point.x += side === 'left' ? -64 : side === 'right' ? 64 : 0; point.y += side === 'above' ? -64 : side === 'below' ? 64 : 0; }
            move.disabled = true; run(engine.moveContext(context.contextId, point).then(() => { if (current()) { engine?.focusContext(context.contextId); mover.close(); } }).finally(() => { move.disabled = false; }));
          }); controls.append(move);
        }
        if (!select.options.length) { select.hidden = true; mover.body.append(el('p', '', 'Выберите направление сдвига.')); }
        mover.body.append(controls);
      }), iconButton(`Переименовать ${context.title}`, 'edit', () => {
        instance.close(); const editor = dialog('Название пространства'), name = textInput(context.title, '', 64), save = button('Сохранить', 'check', 'sw-button-primary');
        save.addEventListener('click', () => { if (!name.value.trim()) { name.focus(); return; } save.disabled = true; run(engine?.renameContext(context.contextId, name.value.trim()).then(() => editor.close()).finally(() => { save.disabled = false; })); });
        editor.body.append(labeledField('Название', name), save); name.focus();
      }), iconButton(`Убрать пространство ${context.title}`, 'close', () => {
        instance.close(); const confirm = dialog('Убрать пространство?'); confirm.body.append(el('p', '', 'С поля исчезнут пространство и его ярлыки. Приложения, контакты и участие в комнатах сохранятся.'), button('Убрать с поля', 'close', 'sw-button-primary', () => run(engine?.removeContext(context.contextId, { removeShortcuts: true }).then(() => { confirm.close(); updateMineFilter(); }))));
      })); instance.body.append(row);
    }
    instance.body.append(button('Новое пространство', 'plus', 'sw-button-wide', () => { instance.close(); newContext(); }));
  }
  function openAdd(): void {
    const instance = dialog('Добавить на поле'); instance.element.classList.add('uf-add-dialog');
    const find = textInput('', 'Найти среди доступного', 100), list = el('div', 'uf-add-list');
    find.setAttribute('aria-label', 'Найти среди доступного');
    const render = (): void => {
      list.replaceChildren(); const items = [...BUILTINS, ...available].filter(item => matches(item, find.value, 'all'));
      for (const item of items) list.append(button(item.title, item.symbol ?? (item.entity.kind === 'person' ? 'person' : item.entity.kind === 'community' ? 'people' : item.entity.kind === 'device' ? 'laptop' : 'grid'), 'sw-button-wide', () => { instance.close(); addToContext(item); }));
      if (!items.length) list.append(el('p', 'sw-muted', 'Здесь пока пусто. Можно найти новое или создать своё.'));
      if (availableCursor) {
        const moreAvailable = button('Показать ещё доступное', 'plus', 'sw-button-wide', () => {
          moreAvailable.disabled = true; const generation = mineGeneration, cursor = availableCursor;
          run(directory.loadMine({ ...(cursor ? { cursor } : {}), limit: 60 }).then(page => {
            if (!current() || !instance.element.open || generation !== mineGeneration) return;
            available = [...new Map([...available, ...page.items].map(item => [fieldEntityKey(item.entity), item])).values()]; availableCursor = page.cursor; merge(page.items); engine?.update({ entities: allMetadata() }); render();
          }).finally(() => { moreAvailable.disabled = false; }));
        }); list.append(moreAvailable);
      }
    };
    find.addEventListener('input', render); render();
    const create = el('div', 'uf-add-create');
    create.setAttribute('aria-label', 'Создать или подключить');
    for (const [kind, label, symbol] of [['assistant', 'Создать приложение с ИИ', 'sparkle'], ['app', 'Подключить приложение', 'grid'], ['person', 'Добавить контакт', 'person'], ['device', 'Подключить устройство', 'laptop']] as const) create.append(button(label, symbol, `sw-button-wide${kind === 'assistant' ? ' sw-button-primary' : ''}`, () => { instance.close(); options.onCreate(kind); }));
    create.append(button('Найти в Сотах', 'search', 'sw-button-wide', () => {
      instance.close(); queries.search = ''; filtersByMode.search = 'all';
      if (mode === 'search') { filter = 'all'; updateChrome(); route(); run(searchDirectory()); } else changeMode('search');
      searchInput.focus();
    }), button('Новое пространство', 'layers', 'sw-button-wide', () => { instance.close(); newContext(); }), button('Создать комнату', 'people', 'sw-button-wide', () => { instance.close(); options.onCreate('community'); }));
    const availableHeading = el('h3', '', 'Уже доступно');
    instance.body.append(create, availableHeading, find, list);
    create.querySelector<HTMLButtonElement>('button')?.focus({ preventScroll: true });
    // Refresh the available directory on each opening so newly connected apps/devices are reachable.
    run(loadMine().then(() => { if (current() && instance.element.open) render(); }));
  }
  function revealReadyMine(field: UnifiedField): void {
    void field.ready.then(() => {
      if (!current() || engine !== field || mode !== 'mine' || field.element.clientWidth <= 0 || field.element.clientHeight <= 0) return;
      // Placed builtins are usable without a directory response. Cached refs
      // keep their loading/unknown metadata and gain no display authority here.
      if (field.snapshot().shortcuts.some(shortcut => BUILTINS.some(item => fieldEntityKey(item.entity) === fieldEntityKey(shortcut.entity)))) loading.hidden = true;
    }).catch(error => { if (current() && engine === field) report(error); });
  }
  function mountEngine(loaded: FieldPersistenceState): void {
    state = loaded;
    mineFilterAdmitted = false;
    if (loaded.document.contexts.length) firstResolve = false;
    for (const shortcut of loaded.document.shortcuts) if (!metadata.has(fieldEntityKey(shortcut.entity))) metadata.set(fieldEntityKey(shortcut.entity), { entity: shortcut.entity, title: 'Загрузка…', symbol: 'lock', source: 'owner' });
    const field = createUnifiedField({ accountId: options.accountId, document: loaded.document, revision: loaded.projectedRevision,
      mode, entities: allMetadata(), readableMobileFocus: true, dimNonMatches: true, ...(options.viewState ? { viewState: options.viewState } : {}), commit: (doc, context) => persistence.commit(doc, context),
      onViewStateChange: view => { if (current()) options.onViewStateChange?.(view); },
      onActivate: activate, onInspect: inspect, onContextActivate: contextId => { engine?.focusContext(contextId); const entity = mode === 'search' ? searchPlacement.contextEntity(contextId) : null; const item = entity ? currentEntity(entity.entity) : null; if (item) openPreview(item); }, onStateChange: updateSummary,
      resolveArt: item => options.resolveArt ? options.resolveArt(item) : resolveFieldAppArt({ ...(item.entity.kind === 'app' ? { appId: item.entity.id } : {}), ...(item.coverKey ? { coverKey: item.coverKey } : {}) }, { screenWidth: window.innerWidth }),
    }); engine = field; host.prepend(field.element); if (mode === 'search') field.update({ searchDocument: searchScene() }); updateChrome(); updateStatus();
    element.dataset.startupLayoutAt ??= String(Date.now());
    revealReadyMine(field);
    // Cached references can start fresh signed metadata reads while the remote layout is loading.
    if (loaded.document.shortcuts.length && element.dataset.ready === 'loading') run(loadInitialMine());
  }
  persistence.subscribe(applyPersistence);
  const ready = (async (): Promise<void> => {
    const loaded = await persistence.load(); if (!current()) return;
    // The async local-store subscription may already have mounted the cache.
    const cachedField = engine as UnifiedField | null;
    if (!cachedField) mountEngine(loaded); else cachedField.update({ document: loaded.document, revision: loaded.projectedRevision });
    await engine!.ready; if (!current()) return;
    if (loaded.revision === 0 && loaded.document.contexts.length === 0 && loaded.localDurable && loaded.state !== 'conflict') {
      const initial = createPersonalFieldDocument();
      // Only this account's accepted pinned refs are eligible for the one-time migration.
      const refs = [...new Set(options.pinnedApps ?? [])].slice(0, 100).filter(id => /^[A-Za-z0-9_-]{3,160}$/u.test(id)).map(id => ({ kind: 'app' as const, id }));
      if (refs.length) { const resolved = await directory.resolve(refs); if (!current()) return; merge(resolved.items); for (const item of resolved.items) initial.shortcuts.push({ shortcutId: crypto.randomUUID(), entity: item.entity, contextId: 'personal', slot: nextFieldSlot(initial, 'personal', item.entity.kind) }); }
      const receipt = await persistence.commit(initial, { expectedRevision: 0, requestId: crypto.randomUUID() }); if (!current()) return;
      engine!.update({ document: receipt.document, revision: receipt.revision, entities: allMetadata() });
    }
    if (mode === 'mine') revealReadyMine(engine!);
    const mineTask = loadInitialMine();
    if (mode === 'search') await Promise.all([mineTask, searchDirectory()]); else await mineTask;
    if (!current()) return;
    if (current()) { element.dataset.ready = 'ready'; loading.hidden = true; }
  })().catch(error => { if (current()) { element.dataset.ready = 'error'; loading.hidden = true; } report(error); });
  updateChrome();
  return {
    element, ready,
    setMode(next, query, nextFilter) { if (!current()) return; closeList(false); if (query !== undefined) queries[next] = query; if (nextFilter) filtersByMode[next] = nextFilter; if (mode !== next) changeMode(next); else { filter = filtersByMode[mode]; updateChrome(); if (mode === 'search') run(searchDirectory()); else updateMineFilter(); } },
    attachHeaderSearch(next) { header = next; placeSearch(); },
    focusSearch() {
      const active = document.activeElement;
      if (active instanceof HTMLElement && !search.contains(active)) searchOpener = active;
      element.dataset.search = 'open'; searchInput.focus();
      renderMineSearchResults(); renderOwnApps();
      options.onSearchVisibilityChange?.();
    },
    closeSearch: () => closeSearch.click(),
    openView: openViewTools, openList, arrange() { engine?.setArrange(!summary?.arranging); },
    async addEntity(ref) {
      await ready; if (!current()) return;
      // A caller supplies an identity, never display metadata or authority.
      const result = await directory.resolve([ref]); if (!current()) return;
      const item = result.items.find(value => fieldEntityKey(value.entity) === fieldEntityKey(ref));
      if (!item) { options.onMessage(result.errors.length ? 'Не удалось проверить доступ. Повторите добавление.' : 'Сота недоступна для добавления.', true); return; }
      merge([item]); engine?.update({ entities: allMetadata() }); addToContext(item);
    },
    focusContext(contextId) { if (current()) engine?.focusContext(contextId); }, openAdd,
    async refresh(refreshOptions) {
      await ready; if (!current()) return;
      const mineTask = loadMine();
      if (mode === 'search') await Promise.all([mineTask, searchDirectory(false, refreshOptions?.preserveView === true)]); else await mineTask;
    },
    async refreshOwnApps() {
      await ready; if (!current() || mode !== 'mine') return;
      const generation = mineGeneration;
      try {
        const result = await options.api.request<{ apps: unknown[] }>('apps.list', { expectedAccountId: options.accountId });
        if (!current() || generation !== mineGeneration || mode !== 'mine' || !Array.isArray(result.apps)
          || ownAppsOverview.contains(document.activeElement)) return;
        ownedApps = result.apps; ownedAppsState = 'ready'; renderOwnApps();
      } catch { /* Preserve the existing overview during a transient failure. */ }
    },
    hasUnsavedChanges: () => !!engine?.hasUnsavedChanges() || persistence.hasUnsavedChanges(),
    async flush() { await engine?.flush(); await persistence.flush(); },
    reconnect() { run(persistence.retry().then(applyPersistence)); if (mode === 'search') run(searchDirectory()); else run(loadMine()); },
    dispose() { disposed = true; searchGeneration++; mineGeneration++; if (searchTimer) clearTimeout(searchTimer); queuedSearch?.(false); queuedSearch = null; queuedMine?.(false); queuedMine = null; abort.abort(); for (const instance of [...dialogs]) instance.close({ restoreFocus: false }); closePreview(false); engine?.destroy(); engine = null; persistence.dispose(); directory.dispose(); listDialog = null; search.remove(); listButton.remove(); element.remove(); },
  };
}
