import './unified-field.css';
import { createFieldDocument, fieldEntityKey, type FieldDocument, type FieldEntityRef } from '../../modules/field/contract.mjs';
import { createUnifiedField, type FieldDirectoryEntity, type UnifiedField, type UnifiedFieldMode, type UnifiedFieldSummary, type UnifiedFieldViewState } from './unified-field';
import { nextFieldSlot } from './unified-field-state.mjs';
import { layoutUnifiedField, fieldBounds, rectPoints } from './unified-field-layout.mjs';
import { fitFieldCamera } from './unified-field-camera.mjs';
import { createFieldSearchPlacement } from './unified-field-search.mjs';
import { createFieldDirectory, type DirectoryApi, type DirectoryApp, type DirectoryEntity, type DirectoryRecord } from './field-directory';
import { createFieldPersistence, type FieldPersistenceState } from './field-persistence';
import { resolveFieldAppArt } from './field-art.mjs';
import { avatar, button, el, iconButton, labeledField, nounCount, textInput } from './dom';
import { createDialog, errorText, type WorldDialog } from './dialogs';
import { icon } from './icons';
import type { WorldCommunity } from './types';

export type FieldFilter = 'all' | 'app' | 'person' | 'community' | 'device';
export interface UnifiedFieldScreenOptions {
  api: DirectoryApi; accountId: string; isCurrent(): boolean;
  mode?: UnifiedFieldMode; query?: string; filter?: FieldFilter;
  filtersByMode?: { mine: FieldFilter; search: FieldFilter };
  viewState?: UnifiedFieldViewState; pinnedApps?: readonly string[];
  onRoute(mode: UnifiedFieldMode, query: string, filter: FieldFilter): void;
  onOpen(entity: DirectoryEntity, record: DirectoryRecord | null): void;
  onCreate(kind: 'app' | 'community' | 'device' | 'person' | 'assistant'): void;
  onAppSettings?: (app: DirectoryApp) => void;
  onMessage(message: string, error?: boolean): void;
  resolveArt?: (entity: FieldDirectoryEntity) => import('./app-art.mjs').AppArt | null;
}
export interface UnifiedFieldScreen {
  element: HTMLElement; ready: Promise<void>;
  setMode(mode: UnifiedFieldMode, query?: string, filter?: FieldFilter): void;
  attachHeaderSearch(header: HTMLElement): void;
  focusSearch(): void; openAdd(): void; refresh(): Promise<void>;
  hasUnsavedChanges(): boolean; flush(): Promise<void>; reconnect(): void; dispose(): void;
}

const BUILTINS: DirectoryEntity[] = [
  { entity: { kind: 'builtin', id: 'notes' }, title: 'Записки', symbol: 'note', coverKey: 'notes', source: 'builtin' },
  { entity: { kind: 'builtin', id: 'chess' }, title: 'Шахматы', symbol: 'chess', coverKey: 'chess', source: 'builtin' },
];
const labels: Record<FieldFilter, string> = { all: 'Всё', app: 'Приложения', person: 'Люди', community: 'Сообщества', device: 'Устройства' };
const kindLabel = (kind: FieldEntityRef['kind']): string => ({ app: 'Приложение', builtin: 'Возможность Сот', person: 'Человек', community: 'Сообщество', device: 'Моё устройство' })[kind];
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
  const head = el('div', 'uf-screen-head'), title = el('h1', 'uf-screen-title', 'Мои соты');
  const sides = el('div', 'uf-screen-switch'); sides.setAttribute('role', 'group'); sides.setAttribute('aria-label', 'Сторона поля');
  const mineButton = button('Моё', undefined, '', () => changeMode('mine'));
  const searchButton = button('Поиск', undefined, '', () => changeMode('search'));
  sides.append(mineButton, searchButton);
  const arrange = button('Расставить', 'sliders', 'uf-screen-arrange', () => engine?.setArrange(!summary?.arranging));
  head.append(title, sides, arrange);
  const search = el('label', 'uf-global-search'); const searchInput = textInput('', 'Найти в моих сотах', 100);
  searchInput.type = 'search'; searchInput.setAttribute('aria-label', 'Найти в моих сотах'); searchInput.autocomplete = 'off';
  const clear = iconButton('Очистить поиск', 'close', () => { searchInput.value = ''; queries[mode] = ''; inputChanged(); searchInput.focus(); });
  clear.classList.add('uf-search-clear'); search.append(icon('search'), searchInput, clear);
  const localSearch = el('div', 'uf-local-search'); localSearch.append(search);
  const filters = el('div', 'uf-filters'); filters.setAttribute('role', 'group'); filters.setAttribute('aria-label', 'Какие объекты показать');
  const filterButtons = new Map<FieldFilter, HTMLButtonElement>();
  const host = el('div', 'uf-scene-host');
  const loading = el('div', 'uf-loading', 'Открываем поле…'); loading.setAttribute('role', 'status');
  const empty = el('div', 'uf-empty'); empty.hidden = true; empty.setAttribute('role', 'status');
  const feedback = el('div', 'uf-storage-status'); feedback.hidden = true; feedback.setAttribute('role', 'status');
  const toolbar = el('div', 'uf-context-toolbar');
  const zoom = el('div', 'uf-zoom-controls'); const percent = el('output', 'uf-zoom-value', '100%'); percent.setAttribute('aria-label', 'Масштаб поля');
  const setZoom = (factor: number): void => { if (!engine) return; const camera = engine.getCamera(); engine.setCamera({ ...camera, scale: camera.scale * factor }); };
  zoom.append(iconButton('Уменьшить поле', 'minus', () => setZoom(1 / 1.18)), percent, iconButton('Увеличить поле', 'plus', () => setZoom(1.18)));
  const contextSelector = el('select', 'uf-context-selector'); contextSelector.setAttribute('aria-label', 'Пространство поля');
  contextSelector.addEventListener('change', () => contextSelector.value ? engine?.focusContext(contextSelector.value) : engine?.fitOverview(), { signal });
  const overview = button('Обзор', 'refresh', 'uf-overview-button', () => engine?.fitOverview());
  const spaces = iconButton('Управлять пространствами', 'settings', () => manageContexts());
  toolbar.append(zoom, contextSelector, overview, spaces);
  const more = button('Показать ещё', 'plus', 'uf-more', () => run(searchDirectory(true))); more.hidden = true;
  const undo = button('Отменить', 'back', 'uf-undo', () => run(engine?.undo())); undo.hidden = true;
  host.append(loading, empty, feedback, toolbar, more, undo); element.append(head, localSearch, filters, host);
  const dialogs = new Set<WorldDialog>();
  const metadata = new Map<string, DirectoryEntity>(BUILTINS.map(item => [fieldEntityKey(item.entity), item]));
  let engine: UnifiedField | null = null, summary: UnifiedFieldSummary | null = null, state: FieldPersistenceState | null = null;
  let searchItems: DirectoryEntity[] = [], available: DirectoryEntity[] = [], searchCursor: string | null = null;
  let availableCursor: string | null = null;
  const searchPlacement = createFieldSearchPlacement();
  let activeSearchScene = createFieldDocument();
  let searchResponseScope = '';
  let searchGeneration = 0, mineGeneration = 0, searchTimer: ReturnType<typeof setTimeout> | null = null;
  let activeSearches = 0, queuedSearch: ((admitted: boolean) => void) | null = null;
  let preview: HTMLElement | null = null, selected: { item: DirectoryEntity; shortcutId?: string } | null = null;
  let previewOpener: HTMLElement | null = null;
  let header: HTMLElement | null = null, contextKey = '', statusKey = '', composing = false;
  let emptyDocument: FieldDocument | null = null, emptyScope = '', metadataRevision = 0, emptyMetadataRevision = -1;
  let firstResolve = true;
  const wide = matchMedia('(min-width: 769px)');
  const report = (error: unknown): void => { if (current()) options.onMessage(errorText(error), true); };
  const run = (promise: Promise<unknown> | undefined): void => { if (promise) void promise.catch(report); };
  function dialog(name: string): WorldDialog {
    const instance = createDialog(name, () => dialogs.delete(instance)); dialogs.add(instance); return instance;
  }
  function merge(items: readonly DirectoryEntity[]): void { for (const item of items) metadata.set(fieldEntityKey(item.entity), item); metadataRevision++; }
  function pruneMetadata(): void {
    const keep = new Set([...allMetadata(), ...available].map(item => fieldEntityKey(item.entity)));
    for (const key of metadata.keys()) if (!keep.has(key)) metadata.delete(key);
  }
  function allMetadata(): DirectoryEntity[] {
    const needed = new Set([...BUILTINS, ...searchItems].map(item => fieldEntityKey(item.entity)));
    for (const shortcut of (engine?.snapshot() ?? state?.document)?.shortcuts ?? []) needed.add(fieldEntityKey(shortcut.entity));
    return [...metadata.values()].filter(item => needed.has(fieldEntityKey(item.entity)));
  }
  function placeSearch(): void {
    const placeholder = header?.querySelector('.sx-global-search');
    if (wide.matches && header) { placeholder?.replaceWith(search); if (search.parentElement !== header) header.insertBefore(search, header.querySelector('.sx-mobile-profile')); }
    else { if (search.parentElement !== localSearch) localSearch.append(search); placeholder?.remove(); }
  }
  function updateChrome(): void {
    const focused = document.activeElement;
    element.dataset.mode = mode; element.dataset.filter = filter; title.textContent = mode === 'mine' ? 'Мои соты' : 'Найти своё';
    element.dataset.query = queries[mode] ? 'active' : 'empty';
    element.setAttribute('aria-label', mode === 'mine' ? 'Мои соты' : 'Поиск в Сотах');
    mineButton.setAttribute('aria-pressed', String(mode === 'mine')); searchButton.setAttribute('aria-pressed', String(mode === 'search'));
    searchInput.value = queries[mode]; searchInput.placeholder = mode === 'mine' ? 'Найти в моих сотах' : 'Приложения, люди, сообщества';
    searchInput.setAttribute('aria-label', searchInput.placeholder); clear.hidden = !searchInput.value;
    arrange.hidden = mode !== 'mine'; spaces.hidden = mode !== 'mine';
    for (const kind of ['all', 'app', 'community', 'person', 'device'] as FieldFilter[]) {
      let target = filterButtons.get(kind);
      if (!target) { target = button(labels[kind], undefined, '', () => { filter = kind; filtersByMode[mode] = kind; updateChrome(); inputChanged(true); }); filterButtons.set(kind, target); filters.append(target); }
      target.setAttribute('aria-pressed', String(filter === kind)); target.hidden = mode === 'search' && kind === 'device';
    }
    if (focused instanceof HTMLElement && filters.contains(focused) && !focused.getClientRects().length) searchInput.focus({ preventScroll: true });
    contextKey = ''; if (summary) updateSummary(summary); placeSearch();
  }
  function route(): void { options.onRoute(mode, queries[mode], filter); }
  function changeMode(next: UnifiedFieldMode): void { if (next === mode) return; closePreview(false); emptyDocument = null; filtersByMode[mode] = filter; mode = next; filter = filtersByMode[mode]; if (filter === 'device' && mode === 'search') filter = 'all'; engine?.cancel(); engine?.update({ mode }); updateChrome(); route(); if (mode === 'search') run(searchDirectory()); else updateMineFilter(); }
  function inputChanged(immediate = false): void {
    queries[mode] = searchInput.value; clear.hidden = !searchInput.value; route();
    element.dataset.query = queries[mode] ? 'active' : 'empty';
    if (searchTimer) clearTimeout(searchTimer);
    if (mode === 'mine') { updateMineFilter(); return; }
    searchGeneration++; searchCursor = null; more.hidden = true;
    closePreview(false);
    if (!composing) { if (immediate) run(searchDirectory()); else searchTimer = setTimeout(() => run(searchDirectory()), 160); }
  }
  searchInput.addEventListener('input', () => inputChanged(), { signal });
  searchInput.addEventListener('compositionstart', () => { composing = true; if (searchTimer) clearTimeout(searchTimer); }, { signal });
  searchInput.addEventListener('compositionend', () => { composing = false; inputChanged(true); }, { signal });
  searchInput.addEventListener('keydown', event => { if (event.key === 'Escape' && searchInput.value) { event.stopPropagation(); searchInput.value = ''; inputChanged(true); } }, { signal });
  wide.addEventListener('change', placeSearch, { signal });
  function updateMineFilter(): void {
    if (!engine || mode !== 'mine') return;
    engine.update({ filter: queries.mine, visibleKinds: filter === 'all' ? ['app', 'builtin', 'person', 'community', 'device'] : filter === 'app' ? ['app', 'builtin'] : [filter] });
    updateMineEmpty(summary?.document ?? engine.snapshot());
  }
  function updateMineEmpty(doc: FieldDocument): void {
    if (mode !== 'mine') return;
    const scope = `${queries.mine}:${filter}`;
    if (doc === emptyDocument && scope === emptyScope && metadataRevision === emptyMetadataRevision) return;
    emptyDocument = doc; emptyScope = scope; emptyMetadataRevision = metadataRevision;
    const query = queries.mine.trim().toLocaleLowerCase('ru');
    const matchingContexts = new Set(doc.contexts.filter(context => context.title.toLocaleLowerCase('ru').includes(query)).map(context => context.contextId));
    const count = doc.shortcuts.filter(shortcut => {
      const item = metadata.get(fieldEntityKey(shortcut.entity));
      return item && matches(item, matchingContexts.has(shortcut.contextId) ? '' : queries.mine, filter);
    }).length;
    empty.hidden = count > 0 || !!query && matchingContexts.size > 0;
    if (!empty.hidden) empty.replaceChildren(el('h2', '', queries.mine ? 'Ничего не найдено' : filter !== 'all' ? 'В этом разделе пока пусто' : 'Здесь будет ваше пространство'), el('p', '', queries.mine ? 'Попробуйте другое название.' : 'Добавьте приложение, человека или сообщество.'), button(queries.mine ? 'Очистить поиск' : filter !== 'all' ? 'Показать всё' : 'Добавить на поле', queries.mine ? 'close' : filter !== 'all' ? 'layers' : 'plus', 'sw-button-primary', () => {
      if (queries.mine) { searchInput.value = ''; inputChanged(); searchInput.focus(); }
      else if (filter !== 'all') { filter = 'all'; filtersByMode.mine = 'all'; updateChrome(); inputChanged(true); }
      else openAdd();
    }));
  }
  function updateSummary(next: UnifiedFieldSummary): void {
    summary = next; arrange.setAttribute('aria-pressed', String(next.arranging)); arrange.querySelector('span')!.textContent = next.arranging ? 'Готово' : 'Расставить';
    element.dataset.focusContext = next.focusContextId;
    const zoomValue = `${Math.round(next.camera.scale * 100)}%`; if (percent.value !== zoomValue) percent.value = zoomValue;
    undo.hidden = !next.canUndo || mode !== 'mine';
    const contexts = mode === 'mine' ? next.document.contexts : searchScene().contexts;
    const key = JSON.stringify(contexts.map(context => [context.contextId, context.title]));
    if (contextKey !== key) { contextKey = key; contextSelector.replaceChildren(el('option', '', 'Все пространства'), ...contexts.map(context => { const option = el('option', '', context.title); option.value = context.contextId; return option; })); contextSelector.firstElementChild!.setAttribute('value', ''); }
    contextSelector.value = next.focusContextId || ''; updateMineEmpty(next.document); updateStatus();
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
    if (!current()) return; state = next; updateStatus();
    if (!engine && next.state !== 'loading' && next.document.contexts.length) { mountEngine(next); loading.hidden = true; }
    if (engine && next.state === 'saved' && !engine.hasUnsavedChanges()) { engine.update({ document: next.document, revision: next.projectedRevision, persistence: 'saved' }); updateMineFilter(); }
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
  async function searchDirectory(append = false): Promise<void> {
    if (mode !== 'search') return;
    const generation = ++searchGeneration, query = queries.search, selectedFilter = filter;
    const freshScope = searchResponseScope !== `${query}:${selectedFilter}` || searchItems.length === 0;
    more.disabled = true; element.dataset.searchState = 'loading'; empty.hidden = true;
    if (!await searchSlot()) return;
    try {
      if (!current() || generation !== searchGeneration || mode !== 'search' || query !== queries.search || selectedFilter !== filter) return;
      const page = await directory.search({ query, kinds: selectedFilter === 'all' ? ['app', 'person', 'community'] : [selectedFilter], ...(append && searchCursor ? { cursor: searchCursor } : {}), limit: 36 });
      if (!current() || generation !== searchGeneration || mode !== 'search' || query !== queries.search || selectedFilter !== filter) return;
      const builtins = selectedFilter === 'all' || selectedFilter === 'app' ? BUILTINS.filter(item => matches(item, query, 'app')) : [];
      searchItems = append ? [...new Map([...searchItems, ...page.items].map(item => [fieldEntityKey(item.entity), item])).values()].slice(0, 256) : [...builtins, ...page.items];
      if (!append && selected) closePreview(false);
      merge(page.items); searchCursor = page.cursor; more.hidden = !searchCursor || searchItems.length >= 256;
      searchResponseScope = `${query}:${selectedFilter}`; pruneMetadata();
      const scene = searchPlacement.update(`${query}:${selectedFilter}`, searchItems, allMetadata()); activeSearchScene = scene;
      engine?.update({ entities: allMetadata(), searchDocument: scene, scope: `${query}:${selectedFilter}` });
      if (freshScope && engine && scene.contexts.length) {
        if (!wide.matches) engine.focusContext(scene.contexts[0]!.contextId);
        else {
          const layout = layoutUnifiedField(scene, allMetadata()), visibleContexts = layout.contexts.slice(0, 2);
          const bounds = fieldBounds(visibleContexts.flatMap(context => rectPoints(context.bounds)));
          const viewport = engine.element;
          engine.setCamera(fitFieldCamera(bounds, { width: viewport.clientWidth, height: viewport.clientHeight }, { padding: 30, maxScale: 1 }));
        }
      }
      element.dataset.searchState = page.status;
      empty.hidden = searchItems.length > 0;
      if (!searchItems.length) { empty.replaceChildren(el('h2', '', page.status === 'offline' ? 'Поиск ждёт подключения' : 'Пока ничего не найдено'), el('p', '', page.status === 'offline' ? 'Ваше поле остаётся доступным.' : query ? 'Попробуйте другое название.' : 'Здесь появятся доступные приложения, люди и сообщества.'), button(page.status === 'offline' ? 'Повторить' : 'На моё поле', page.status === 'offline' ? 'refresh' : 'back', 'sw-button-quiet', () => page.status === 'offline' ? run(searchDirectory()) : changeMode('mine'))); }
      else if (page.status === 'partial') options.onMessage('Часть результатов временно недоступна. Можно повторить поиск.');
      if (summary) { contextKey = ''; updateSummary(summary); }
    } catch (error) {
      if (!current() || generation !== searchGeneration || query !== queries.search || selectedFilter !== filter) return;
      element.dataset.searchState = 'offline'; empty.hidden = false;
      empty.replaceChildren(el('h2', '', 'Не удалось обновить поиск'), el('p', '', 'Попробуйте ещё раз. Ваша расстановка сохранена отдельно.'), button('Повторить', 'refresh', 'sw-button-quiet', () => run(searchDirectory()))); throw error;
    } finally { releaseSearchSlot(); if (current() && generation === searchGeneration && query === queries.search && selectedFilter === filter) more.disabled = false; }
  }
  async function loadMine(): Promise<void> {
    if (!engine) return; const generation = ++mineGeneration;
    const resolved = await directory.resolve(engine.snapshot().shortcuts.map(shortcut => shortcut.entity));
    if (!current() || generation !== mineGeneration) return;
    // Removing authority drops display metadata, while retaining the user's own shortcut for recovery/removal.
    for (const ref of resolved.unavailable) metadata.set(fieldEntityKey(ref), { entity: ref, title: resolved.errors.length ? 'Ждёт подключения' : 'Недоступно', symbol: 'lock', source: 'owner' }); merge(resolved.items);
    const first = await directory.loadMine({ limit: 60 });
    if (!current() || generation !== mineGeneration) return;
    available = first.items; availableCursor = first.cursor; merge(first.items); pruneMetadata(); engine.update({ entities: allMetadata() }); updateMineFilter();
    if (selected) {
      const latest = metadata.get(fieldEntityKey(selected.item.entity));
      if (!latest || resolved.unavailable.some(ref => fieldEntityKey(ref) === fieldEntityKey(selected!.item.entity))) closePreview(false);
      else if (JSON.stringify(latest) !== JSON.stringify(selected.item)) openPreview(latest, selected.shortcutId);
    }
    if (firstResolve && mode === 'mine') { firstResolve = false; if (wide.matches) engine.fitOverview(); else engine.focusContext(engine.snapshot().contexts[0]?.contextId ?? ''); }
  }
  function closePreview(restore = true): void {
    preview?.remove(); preview = null;
    delete element.dataset.preview;
    if (restore) {
      const target = selected?.shortcutId ? element.querySelector<HTMLButtonElement>(`[data-shortcut-id="${CSS.escape(selected.shortcutId)}"]`) : previewOpener;
      if (target?.isConnected && target.getClientRects().length && getComputedStyle(target).visibility !== 'hidden') target.focus({ preventScroll: true });
      if (!target || document.activeElement !== target) searchInput.focus({ preventScroll: true });
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
    if (item.entity.kind === 'person') copy.prepend(avatar(item.title, item.avatarUrl, item.color, item.entity.id, item.avatarRevision));
    if (item.description) copy.append(el('p', '', item.description));
    const record = directory.getRecord(item.entity), actions = el('div', 'uf-preview-actions');
    if (record && 'kind' in record && record.kind === 'contact') copy.append(el('p', 'uf-preview-meta', 'Ваш контакт'));
    if (item.entity.kind === 'community' && record && 'communityId' in record) {
      const group = record as WorldCommunity;
      const emblem = el('div', 'uf-preview-emblem'); emblem.setAttribute('aria-hidden', 'true'); emblem.append(icon(group.symbol || 'people')); copy.prepend(emblem);
      copy.append(el('span', 'uf-preview-badge', group.membership?.state === 'active' ? 'Вы участник' : group.joinPolicy === 'open' ? 'Открытая группа' : group.joinPolicy === 'request' ? 'По заявке' : 'По приглашению'));
      const members = el('div', 'uf-preview-members');
      for (const person of group.previewMembers.slice(0, 3)) { const face = avatar(person.displayName, person.avatarUrl, person.avatarColor, person.profileId, person.avatarRevision); face.title = person.displayName; members.append(face); }
      if (group.previewMembers.length) copy.append(members);
      copy.append(el('p', 'uf-preview-meta', nounCount(group.memberCount, 'участник', 'участника', 'участников')));
      if (group.showcase) { const showcase = el('section', 'uf-preview-showcase'); showcase.append(el('h3', '', 'О сообществе'), el('p', '', group.showcase)); copy.append(showcase); }
      if (group.membership?.state === 'active') actions.append(button('Открыть чат', 'chat', 'sw-button-primary', () => options.onOpen(item, directory.getRecord(item.entity))));
      else {
        const join = button(group.membership?.state === 'invited' ? 'Принять приглашение' : group.membership?.state === 'requested' ? 'Заявка отправлена' : group.joinPolicy === 'open' ? 'Вступить' : group.joinPolicy === 'request' ? 'Подать заявку' : 'По приглашению', 'people', 'sw-button-primary');
        join.disabled = ['requested', 'banned'].includes(group.membership?.state ?? '') || group.joinPolicy === 'invite' && group.membership?.state !== 'invited';
        join.addEventListener('click', () => { join.disabled = true; run(options.api.request<{ community: WorldCommunity }>('world.membership.join', { communityId: group.communityId }).then(async result => { if (!current()) return; await loadMine(); if (!current()) return; const refreshed = metadata.get(fieldEntityKey(item.entity)); if (refreshed) openPreview(refreshed, shortcutId); options.onMessage(result.community.membership?.state === 'active' ? 'Вы в сообществе' : 'Заявка отправлена'); }).finally(() => { if (join.isConnected) join.disabled = false; })); });
        actions.append(join, button('О сообществе', 'info', 'sw-button-quiet', () => options.onOpen(item, directory.getRecord(item.entity))));
      }
    } else { const open = button(item.entity.kind === 'person' ? 'Профиль и контакт' : item.entity.kind === 'device' ? 'Управление устройствами' : 'Открыть', item.entity.kind === 'person' ? 'person' : 'external', 'sw-button-primary', () => options.onOpen(item, directory.getRecord(item.entity))); open.disabled = !record && ['person', 'device', 'community'].includes(item.entity.kind); actions.append(open); }
    if (item.entity.kind === 'app' && record && 'canManage' in record && record.canManage && options.onAppSettings) actions.append(button('Настройки приложения', 'settings', 'sw-button-quiet', () => options.onAppSettings!(record as DirectoryApp)));
    const alreadyPlaced = engine?.snapshot().shortcuts.some(shortcut => fieldEntityKey(shortcut.entity) === fieldEntityKey(item.entity));
    actions.append(button(alreadyPlaced ? 'Добавить в другое пространство' : 'На моё поле', 'plus', 'sw-button-quiet', () => addToContext(item)));
    if (mode === 'mine' && shortcutId) actions.append(button('Переместить', 'sliders', 'sw-button-quiet', () => { closePreview(); engine?.setArrange(true); engine?.beginMove(shortcutId); }), button('Убрать с поля', 'close', 'sw-button-quiet', () => { closePreview(); run(engine?.removeShortcut(shortcutId).then(() => updateMineFilter())); }));
    copy.append(actions); panel.append(close, copy); element.append(panel); element.dataset.preview = 'open'; preview = panel;
    panel.addEventListener('keydown', event => { if (event.key === 'Escape') { event.stopPropagation(); closePreview(); } }, { signal });
    panel.focus({ preventScroll: true });
  }
  function activate(entity: FieldDirectoryEntity, shortcutId?: string): void {
    if (!current()) return;
    const item = metadata.get(fieldEntityKey(entity.entity)); if (!item) return;
    if (mode === 'mine' && !summary?.arranging && ['app', 'builtin'].includes(item.entity.kind)) { options.onOpen(item, directory.getRecord(item.entity)); return; }
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
      changeMode('mine'); merge([item]); engine?.update({ entities: [...allMetadata(), item] }); run(engine?.addShortcut(item.entity, contextId).then(() => { if (current()) { instance.close(); engine?.focusContext(contextId); updateMineFilter(); options.onMessage('Добавлено на поле'); } }).finally(() => { submit.disabled = false; })); });
    instance.body.append(el('p', '', item.title), labeledField('Пространство', select), submit);
    if (!doc.contexts.length) instance.body.append(button('Создать пространство', 'plus', '', () => { instance.close(); newContext(); }));
  }
  function newContext(): void {
    const instance = dialog('Новое пространство'), input = textInput('', 'Например, Работа или Близкие', 64), form = el('form'), error = el('p', 'sw-error');
    input.required = true; const submit = button('Создать', 'plus', 'sw-button-primary'); submit.type = 'submit';
    form.append(labeledField('Название', input), error, submit);
    form.addEventListener('submit', event => { event.preventDefault(); if (!form.reportValidity() || !input.value.trim()) return; submit.disabled = true;
      void engine?.addContext(input.value.trim()).then(() => { if (current()) instance.close(); }).catch(errorValue => { error.textContent = errorText(errorValue); }).finally(() => { submit.disabled = false; }); });
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
        instance.close(); const confirm = dialog('Убрать пространство?'); confirm.body.append(el('p', '', 'С поля исчезнут пространство и его ярлыки. Приложения, контакты и участие в сообществах сохранятся.'), button('Убрать с поля', 'close', 'sw-button-primary', () => run(engine?.removeContext(context.contextId, { removeShortcuts: true }).then(() => { confirm.close(); updateMineFilter(); }))));
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
    create.append(button('Найти новое', 'search', 'sw-button-wide', () => { instance.close(); changeMode('search'); searchInput.focus(); }), button('Новое пространство', 'cells', 'sw-button-wide', () => { instance.close(); newContext(); }));
    for (const [kind, label, symbol] of [['app', 'Подключить приложение', 'grid'], ['community', 'Создать сообщество', 'people'], ['device', 'Подключить устройство', 'laptop'], ['person', 'Добавить контакт', 'person'], ['assistant', 'Создать с ИИ', 'sparkle']] as const) create.append(button(label, symbol, 'sw-button-wide', () => { instance.close(); options.onCreate(kind); }));
    instance.body.append(find, list, create); find.focus();
    // Refresh the available directory on each opening so newly connected apps/devices are reachable.
    run(loadMine().then(() => { if (current() && instance.element.open) render(); }));
  }
  function mountEngine(loaded: FieldPersistenceState): void {
    state = loaded;
    if (loaded.document.contexts.length) firstResolve = false;
    for (const shortcut of loaded.document.shortcuts) if (!metadata.has(fieldEntityKey(shortcut.entity))) metadata.set(fieldEntityKey(shortcut.entity), { entity: shortcut.entity, title: 'Проверяем доступ', symbol: 'lock', source: 'owner' });
    const field = createUnifiedField({ accountId: options.accountId, document: loaded.document, revision: loaded.projectedRevision,
      mode, entities: allMetadata(), ...(options.viewState ? { viewState: options.viewState } : {}), commit: (doc, context) => persistence.commit(doc, context),
      onActivate: activate, onInspect: (entity, shortcutId) => { const item = metadata.get(fieldEntityKey(entity.entity)); if (item) openPreview(item, shortcutId); }, onContextActivate: contextId => { engine?.focusContext(contextId); const entity = mode === 'search' ? searchPlacement.contextEntity(contextId) : null; const item = entity ? metadata.get(fieldEntityKey(entity.entity)) : null; if (item) openPreview(item); }, onStateChange: updateSummary,
      resolveArt: item => options.resolveArt ? options.resolveArt(item) : resolveFieldAppArt({ ...(item.entity.kind === 'app' ? { appId: item.entity.id } : {}), ...(item.coverKey ? { coverKey: item.coverKey } : {}) }, { screenWidth: window.innerWidth }),
    }); engine = field; host.prepend(field.element); if (mode === 'search') field.update({ searchDocument: searchScene() }); updateChrome(); updateStatus();
  }
  persistence.subscribe(applyPersistence);
  const ready = (async (): Promise<void> => {
    const loaded = await persistence.load(); if (!current()) return;
    // The async local-store subscription may already have mounted the cache.
    const cachedField = engine as UnifiedField | null;
    if (!cachedField) mountEngine(loaded); else cachedField.update({ document: loaded.document, revision: loaded.projectedRevision });
    await engine!.ready; if (!current()) return;
    if (loaded.revision === 0 && loaded.document.contexts.length === 0 && loaded.localDurable && loaded.state !== 'conflict') {
      const initial = createFieldDocument(); initial.contexts.push({ contextId: 'personal', title: 'Личное', x: 0, y: 0 });
      for (const item of BUILTINS) initial.shortcuts.push({ shortcutId: `builtin-${item.entity.id}`, entity: item.entity, contextId: 'personal', slot: nextFieldSlot(initial, 'personal', item.entity.kind) });
      // Only this account's accepted pinned refs are eligible for the one-time migration.
      const refs = [...new Set(options.pinnedApps ?? [])].slice(0, 100).filter(id => /^[A-Za-z0-9_-]{3,160}$/u.test(id)).map(id => ({ kind: 'app' as const, id }));
      if (refs.length) { const resolved = await directory.resolve(refs); if (!current()) return; merge(resolved.items); for (const item of resolved.items) initial.shortcuts.push({ shortcutId: crypto.randomUUID(), entity: item.entity, contextId: 'personal', slot: nextFieldSlot(initial, 'personal', item.entity.kind) }); }
      const receipt = await persistence.commit(initial, { expectedRevision: 0, requestId: crypto.randomUUID() }); if (!current()) return;
      engine!.update({ document: receipt.document, revision: receipt.revision, entities: allMetadata() });
    }
    await loadMine(); if (!current()) return; if (mode === 'search') await searchDirectory();
    if (current()) { element.dataset.ready = 'ready'; loading.hidden = true; }
  })().catch(error => { if (current()) { element.dataset.ready = 'error'; loading.hidden = true; } report(error); });
  updateChrome();
  return {
    element, ready,
    setMode(next, query, nextFilter) { if (!current()) return; if (query !== undefined) queries[next] = query; if (nextFilter) filtersByMode[next] = nextFilter; if (mode !== next) changeMode(next); else { filter = filtersByMode[mode]; updateChrome(); if (mode === 'search') run(searchDirectory()); else updateMineFilter(); } },
    attachHeaderSearch(next) { header = next; placeSearch(); }, focusSearch() { searchInput.focus(); }, openAdd,
    async refresh() { await ready; if (!current()) return; await loadMine(); if (mode === 'search') await searchDirectory(); },
    hasUnsavedChanges: () => !!engine?.hasUnsavedChanges() || persistence.hasUnsavedChanges(),
    async flush() { await engine?.flush(); await persistence.flush(); },
    reconnect() { run(persistence.retry().then(applyPersistence)); if (mode === 'search') run(searchDirectory()); },
    dispose() { disposed = true; searchGeneration++; mineGeneration++; if (searchTimer) clearTimeout(searchTimer); queuedSearch?.(false); queuedSearch = null; abort.abort(); for (const instance of [...dialogs]) instance.close({ restoreFocus: false }); closePreview(false); engine?.destroy(); engine = null; persistence.dispose(); directory.dispose(); search.remove(); element.remove(); },
  };
}
