import './unified-field.css';
import { createFieldDocument, fieldEntityKey, type AnyFieldDocument as FieldDocument, type AnyFieldEntityRef as FieldEntityRef } from '../../modules/field/contract.mjs';
import { createUnifiedField, type FieldDirectoryEntity, type UnifiedField, type UnifiedFieldMode, type UnifiedFieldSummary, type UnifiedFieldViewState } from './unified-field';
import { compactFieldContext, nextFieldSlot } from './unified-field-state.mjs';
import { createPersonalFieldDocument, ensureFieldEntityPlacement } from './app-field-placement.mjs';
import { layoutUnifiedField, fieldBounds, rectPoints } from './unified-field-layout.mjs';
import { fitFieldCamera } from './unified-field-camera.mjs';
import { createFieldSearchPlacement } from './unified-field-search.mjs';
import { createLiveAppScene } from './live-app-scene.mjs';
import { deriveCanonicalAppCoverage } from './canonical-app-coverage.mjs';
import { mountFieldGlass,type FieldGlassHandle } from './field-glass';
import {createCanvasFieldContent,type CanvasProjection,type CanvasAppMount} from './canvas-field-content';
import { unplacedOwnApps, type OwnAppOverview } from './own-apps-overview.mjs';
import { createFieldDirectory, type DirectoryApi, type DirectoryApp, type DirectoryEntity, type DirectoryRecord, type DirectoryObjectMetadata } from './field-directory';
import { createFieldPersistence, type FieldPersistenceState } from './field-persistence';
import { resolveFieldAppArt } from './field-art.mjs';
import { avatar, button, el, iconButton, labeledField, nounCount, textInput } from './dom';
import { createDialog, errorText, isDialogFocusTarget, type WorldDialog } from './dialogs';
import { icon } from './icons';
import type { WorldCommunity } from './types';
import type { RecentAction } from './product';
import {mountCanvasObjectSettings,mountCanvasHiddenObjects} from './canvas-object-settings';
import {createCanvasObjectSecurityActions} from './canvas-object-security-actions.mjs';
import type {FieldNode} from './unified-field-layout.mjs';
import type {CommunityFieldObjectView} from './community-field-object';

export type FieldFilter = 'all' | 'app' | 'person' | 'community' | 'device';
export interface UnifiedFieldScreenOptions {
  api: DirectoryApi; accountId: string; isCurrent(): boolean;
  mountCanvasApp?:CanvasAppMount;
  mountCommunity?:(node:FieldNode,read:()=>WorldCommunity|null,isCurrent:()=>boolean)=>CommunityFieldObjectView;
  onCommunityFocus?:(communityId:string|null)=>void;
  persistence?: ReturnType<typeof createFieldPersistence>;
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
  onConnectDevice?(): void;
  onOpenMore?: () => void;
  readIdentity?: () => {displayName:string;avatarUrl?:string;avatarColor?:string;avatarRevision?:number|null}|null;
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
  addObject(objectId:string,size?:{width:number;height:number}):Promise<{shortcutId:string;status:'saved'|'pending';localDurable:boolean}>;
  watchEntities(): FieldEntityRef[];
  renewHostSessions(reason:'reset'):Promise<void>;
  focusSearch(): void; closeSearch(): void; focusContext(contextId: string): void; openAdd(): void; refresh(options?: { preserveView?: boolean }): Promise<void>; refreshOwnApps(): Promise<void>;
  openView(): void; openList(query?: string): void; arrange(): void;
  openHiddenObjects():void;
  openObjectSettings(objectId:string):Promise<void>;
  openCommunity(communityId:string,pane?:'voice'|'chat'):Promise<WorldCommunity|null>;
  closeCommunity():void;
  setInboxSummary(value:{messages:number;requests:number;invitations?:number;incomplete:boolean}):void;
  hasUnsavedChanges(): boolean; hasUnconfirmedWork():boolean; readRuntimeEpoch():number; flush(): Promise<void>; preparePublication():Promise<void>; reconnect(): void; dispose(): void;
}

const BUILTINS: DirectoryEntity[] = [
  { entity: { kind: 'builtin', id: 'notes' }, title: 'Записки', symbol: 'note', coverKey: 'notes', source: 'builtin' },
  { entity: { kind: 'builtin', id: 'chess' }, title: 'Шахматы', symbol: 'chess', coverKey: 'chess', source: 'builtin' },
  { entity: { kind: 'builtin', id: 'internet' }, title: 'Транзит · Интернет', description: 'TRANSIT: личный профиль sing-box', symbol: 'world', source: 'builtin' },
  { entity: { kind: 'builtin', id: 'assistant' }, title: 'Помощник', symbol: 'assistant', coverKey: 'assistant', source: 'builtin' },
];
const labels: Record<FieldFilter, string> = { all: 'Всё', app: 'Приложения', person: 'Люди', community: 'Комнаты', device: 'Устройства' };
const kindLabel = (kind: FieldEntityRef['kind']): string => ({ app: 'Приложение', builtin: 'Возможность Сот', person: 'Человек', community: 'Комната', device: 'Моё устройство',object:'Объект' })[kind];
const matches = (item: DirectoryEntity, query: string, filter: FieldFilter): boolean =>
  item.hidden!==true&&
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
  const canvasContent=createCanvasFieldContent({api:options.api,accountId:options.accountId,isCurrent:current,...(options.mountCanvasApp?{mountApp:options.mountCanvasApp}:{}),onObjectChanged:async()=>{if(current())await loadMine();},projection:id=>{const record=directory.getRecord({kind:'object',id});return record&&'objectId'in record?record as CanvasProjection:null;}});
  function readCommunity(id:string):WorldCommunity|null{if(!current())return null;const record=directory.getRecord({kind:'community',id});return record&&'communityId'in record?record as WorldCommunity:null;}
  const objectKey=(node:FieldNode):string|null=>mode==='mine'&&options.mountCommunity&&node.entity.entity.kind==='community'&&readCommunity(node.entity.entity.id)?`community:${node.entity.entity.id}`:canvasContent.objectKey(node);
  const createObject=(node:FieldNode)=>mode==='mine'&&options.mountCommunity&&node.entity.entity.kind==='community'&&readCommunity(node.entity.entity.id)
    ?options.mountCommunity(node,()=>readCommunity(node.entity.entity.id),current):canvasContent.createObject(node);
  const persistence = options.persistence ?? createFieldPersistence({ api: options.api, accountId: options.accountId, isCurrent: current, localFirst: true });
  const element = el('section', 'uf-screen'); element.setAttribute('aria-label', 'Мои соты');
  element.dataset.shell='canvas';
  element.dataset.ready = 'loading';
  element.dataset.startupScreenAt = String(Date.now());
  const canvasDock=el('nav','uf-canvas-dock');canvasDock.setAttribute('aria-label','Ваше пространство');
  const profileControl=iconButton('Меню Сот','person',()=>options.onOpenMore?.());profileControl.classList.add('uf-canvas-profile');profileControl.setAttribute('aria-haspopup','dialog');
  let canvasIdentityKey='';
  const searchControl=button('Найти в Сотах','search','uf-canvas-search',()=>showSearch());searchControl.dataset.fieldSearch='';
  const addControl=iconButton('Добавить на полотно','plus',()=>options.onCreateMenu?.());addControl.classList.add('uf-canvas-add');addControl.setAttribute('aria-haspopup','dialog');
  canvasDock.append(profileControl,searchControl,addControl);
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
  ownAppsOverview.addEventListener('focusin', event => {
    const target = event.target;
    if (target instanceof HTMLButtonElement && target.classList.contains('uf-own-app')) target.scrollIntoView({ block: 'nearest', inline: 'nearest' });
  }, { signal });
  const loading = el('div', 'uf-loading', 'Открываем полотно…'); loading.setAttribute('role', 'status');
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
  const scaleControl=button('100%','expand','uf-canvas-scale',()=>openViewTools());scaleControl.setAttribute('aria-haspopup','dialog');scaleControl.setAttribute('aria-label','Масштаб и вид полотна: 100%');
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
  const fieldChrome = el('div', 'uf-field-chrome'); fieldChrome.append(workspaceTools); localSearch.append(ownAppsOverview);
  host.append(loading, empty, feedback, toolbar, panControls, arrangeHint, more, undo); element.append(head, canvasDock, scaleControl,fieldChrome, localSearch, filters, searchLimit, host);
  const dialogs = new Set<WorldDialog>();
  const metadata = new Map<string, DirectoryEntity>(BUILTINS.map(item => [fieldEntityKey(item.entity), item]));
  let engine: UnifiedField | null = null, summary: UnifiedFieldSummary | null = null, state: FieldPersistenceState | null = null;
  let fieldGlass:FieldGlassHandle|null=null;
  let searchItems: DirectoryEntity[] = [], available: DirectoryEntity[] = [], searchCursor: string | null = null;
  let ownedApps: unknown[] = [], ownedAppsState: 'loading' | 'ready' | 'offline' = 'loading', ownAppsSignature = '';
  let ownAppsReadSequence = 0, ownAppsAppliedRead = 0;
  let availableCursor: string | null = null;
  const liveProjection=createLiveAppScene();
  let appProjectionGeneration=0,appCoverageSceneSignature=JSON.stringify({contexts:[],shortcuts:[]});
  let liveApps:DirectoryEntity[]=[],liveCursor:string|null=null,liveRead=0,livePages=1,liveAppsFramed=false;
  let liveCatalogApps: DirectoryEntity[] = [], liveOwnerApps: DirectoryEntity[] = [];
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
  let previewCommunity:CommunityFieldObjectView|null=null,communityOpenSequence=0,focusedCommunityId:string|null=null;
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
    return [searchControl,header?.querySelector<HTMLElement>('[data-field-search]'), element.closest('.sw-app')?.querySelector<HTMLElement>('.sw-mobile-nav .of-menu-button'), element.querySelector<HTMLElement>('.uf-node.is-selected')]
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
    if(mode==='mine'&&liveCursor&&livePages<4)instance.body.append(button('Ещё приложения на полотне','plus','sw-button-wide',()=>{run(refreshLiveApps(true));instance.close();}));
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
    if (mode === 'search') {const active=previewCommunity&&selected?metadata.get(fieldEntityKey(selected.item.entity)):null;return [...new Map([...BUILTINS, ...searchItems,...(active?[active]:[])].map(item => [fieldEntityKey(item.entity), item])).values()];}
    const needed = new Set([...BUILTINS, ...searchItems].map(item => fieldEntityKey(item.entity)));
    if(previewCommunity&&selected?.item.entity.kind==='community')needed.add(fieldEntityKey(selected.item.entity));
    for (const shortcut of (engine?.snapshot() ?? state?.document)?.shortcuts ?? []) needed.add(fieldEntityKey(shortcut.entity));
    for(const item of liveApps)needed.add(fieldEntityKey(item.entity));
    return [...metadata.values()].filter(item => needed.has(fieldEntityKey(item.entity)));
  }
  function readAppCoverage(document:FieldDocument){
    const objects=document.shortcuts.filter(shortcut=>shortcut.entity.kind==='object');
    const coverage=deriveCanonicalAppCoverage({document,representations:new Map(objects.map(shortcut=>[shortcut.entity.id,metadata.get(fieldEntityKey(shortcut.entity))?.ownerRepresents]))});
    const pending=appProjectionGeneration?new Set(objects.map(shortcut=>shortcut.entity.id)).size:0;
    element.dataset.appCoverage=pending?'loading':coverage.status;element.dataset.appCoveragePending=String(pending);
    element.dataset.appCoverageUnknown=String(coverage.unknownObjectIds.length);
    return coverage;
  }
  function liveAppScene(document:FieldDocument){
    const coverage=readAppCoverage(document);
    return liveProjection.update(document,liveApps,{representedAppIds: coverage.appIds});
  }
  function refreshAppCoverage(metadataChanged=false):void{
    if(!engine||!current())return;
    const scene=liveAppScene(engine.snapshot()),signature=JSON.stringify(scene);
    if(metadataChanged||signature!==appCoverageSceneSignature){appCoverageSceneSignature=signature;engine.update({...(metadataChanged?{entities:allMetadata()}:{}),liveScene:scene});}
  }
  function applyLiveApps(): void {
    if (!liveApps.length && !liveOwnerApps.length && !liveCatalogApps.length && element.dataset.liveAppsState === 'loading') return;
    liveApps = [...new Map([...liveOwnerApps, ...liveCatalogApps].map(item => [fieldEntityKey(item.entity), item])).values()];
    livePages = 1; liveCursor = null;
    element.dataset.liveApps = String(liveApps.length); element.dataset.liveMore = 'false';
    merge(liveApps);
    if (engine) { refreshAppCoverage(true); revealReadyMine(engine); }
    renderMineSearchResults(); renderOpenList?.();
  }
  async function refreshLiveApps(_append = false): Promise<void> {
    if (!current()) return;
    const read = ++liveRead, active = () => current() && read === liveRead;
    const unavailable = () => Object.assign(new Error('Не удалось обновить доступные приложения.'), { code: 'live_directory_unavailable' });
    const requested = new Map<string, Promise<DirectoryEntity[]>>();
    element.dataset.liveAppsState = 'loading';
    async function readSource(source: 'list' | 'catalog'): Promise<void> {
      const { apps } = await options.api.request<{ apps: { id: string }[] }>(`apps.${source}`, { expectedAccountId: options.accountId });
      if (!active()) return;
      if (!Array.isArray(apps)) throw unavailable();
      const ids = new Set((source === 'catalog' ? apps : unplacedOwnApps(apps, options.accountId, [])).map(app => app.id));
      const missing = [...ids].filter(id => !requested.has(id));
      // A catalogue entry is a candidate. Only the signed directory can admit it to the field.
      for (let offset = 0; offset < missing.length; offset += 60) {
        const batch = missing.slice(offset, offset + 60);
        const request = directory.resolve(batch.map(id => ({ kind: 'app', id }))).then(page => {
          if (page.errors.length) throw unavailable();
          return page.items;
        });
        for (const id of batch) requested.set(id, request);
      }
      const pages = await Promise.all(new Set([...ids].map(id => requested.get(id)!)));
      if (!active()) return;
      const items = pages.flat().filter(item => item.entity.kind === 'app' && ids.has(item.entity.id));
      if (source === 'catalog') liveCatalogApps = items; else liveOwnerApps = items;
      applyLiveApps();
    }
    const [catalog, registry] = await Promise.allSettled([readSource('catalog'), readSource('list')]);
    if (!active()) return;
    if (catalog.status === 'rejected' && registry.status === 'rejected') {
      element.dataset.liveAppsState = 'offline';
      if (engine) { emptyDocument = null; updateMineEmpty(engine.snapshot()); }
      throw unavailable();
    }
    if (catalog.status === 'rejected') liveCatalogApps = [];
    if (registry.status === 'rejected') liveOwnerApps = [];
    element.dataset.liveAppsState = catalog.status === 'fulfilled' && registry.status === 'fulfilled' ? 'ready' : 'partial';
    applyLiveApps();
    if (catalog.status === 'rejected') options.onMessage('Общий каталог пока недоступен. Ваши приложения доступны.', true);
    else if (registry.status === 'rejected') options.onMessage('Не удалось обновить ваши приложения. Общий каталог доступен.', true);
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
  function ownAppLabel(app: OwnAppOverview): string {
    return app.state === 'offline' ? `${app.name} · нет связи` : app.state === 'stopped' ? `${app.name} · остановлено` : app.name;
  }
  function ownAppButton(app: OwnAppOverview): HTMLButtonElement {
    const target = button(ownAppLabel(app), 'grid', 'uf-own-app', () => {
      if (!current() || mode !== 'mine') return;
      const fresh = overviewApps().find(value => value.id === app.id);
      if (fresh) options.onOpen(overviewEntity(fresh), null);
    });
    target.dataset.appId = app.id; return target;
  }
  function renderOwnApps(): void {
    const visible = current() && !!engine && mode === 'mine' && element.dataset.search === 'open' && !summary?.arranging && !queries.mine.trim() && ['all', 'app'].includes(filter);
    const apps = visible ? overviewApps() : [];
    const signature = JSON.stringify([visible, ownedAppsState, apps]);
    if (signature === ownAppsSignature) return;
    ownAppsSignature = signature;
    const oldRetry = ownAppsOverview.querySelector<HTMLButtonElement>('.uf-own-apps-retry');
    if (visible && ownedAppsState === 'loading' && oldRetry) {
      const status = ownAppsOverview.querySelector<HTMLElement>('.uf-own-apps-status');
      if (status) status.textContent = 'Обновляем ваши приложения…';
      oldRetry.setAttribute('aria-disabled', 'true');
      return;
    }
    const retryFocused = !!oldRetry && document.activeElement === oldRetry;
    const previous = ownAppsOverview.querySelector<HTMLDivElement>('.uf-own-apps-list');
    if (visible && ownedAppsState === 'ready' && apps.length && previous && ownAppsOverview.contains(document.activeElement)) {
      const wanted = new Map(apps.map(app => [app.id, app]));
      const existing = new Map(Array.from(previous.querySelectorAll<HTMLButtonElement>('.uf-own-app'), item => [item.dataset.appId, item]));
      for (const [id, item] of existing) {
        const app = id && wanted.get(id);
        if (!app) item.remove();
        else { const label = item.querySelector('span'); if (label) label.textContent = ownAppLabel(app); }
      }
      let next: HTMLButtonElement | null = null;
      for (const app of [...apps].reverse()) {
        let item = existing.get(app.id);
        if (!item) { item = ownAppButton(app); previous.insertBefore(item, next); }
        next = item;
      }
      return;
    }
    ownAppsOverview.replaceChildren();
    ownAppsOverview.hidden = !visible || ownedAppsState === 'loading' || ownedAppsState === 'ready' && apps.length === 0;
    if (!ownAppsOverview.hidden && ownedAppsState === 'offline') {
      const status = el('p', 'uf-own-apps-status', 'Не удалось обновить ваши приложения. Проверьте подключение.'); status.setAttribute('role', 'status');
      const retry = button('Повторить', 'refresh', 'sw-button-quiet uf-own-apps-retry', () => {
        if (current() && retry.getAttribute('aria-disabled') !== 'true') run(loadOwnedApps(mineGeneration));
      });
      ownAppsOverview.append(status, retry);
    } else if (!ownAppsOverview.hidden) {
      const heading = el('h2', '', 'Ваши приложения'), list = el('div', 'uf-own-apps-list');
      for (const app of apps) list.append(ownAppButton(app));
      ownAppsOverview.append(heading, list);
    }
    if (retryFocused && visible) {
      const target = ownAppsOverview.querySelector<HTMLButtonElement>('.uf-own-apps-retry, .uf-own-app') ?? listView;
      if (isDialogFocusTarget(target)) target.focus({ preventScroll: true }); else focusFieldControl();
    }
  }
  async function loadOwnedApps(generation: number): Promise<void> {
    const read = ++ownAppsReadSequence;
    ownedApps = []; ownedAppsState = 'loading'; renderOwnApps();
    try {
      const result = await options.api.request<{ apps: unknown[] }>('apps.list', { expectedAccountId: options.accountId });
      if (!current() || generation !== mineGeneration || read < ownAppsAppliedRead) return;
      if (!Array.isArray(result.apps)) throw Error('invalid_apps_overview');
      ownAppsAppliedRead = read;
      ownedApps = result.apps; ownedAppsState = 'ready';
    } catch {
      if (!current() || generation !== mineGeneration || read < ownAppsAppliedRead) return;
      ownAppsAppliedRead = read;
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
  let inboxSummary={messages:0,requests:0,invitations:0,incomplete:false};
  function updateIdentity():void{
    const identity=options.readIdentity?.();if(!identity)return;
    const key=JSON.stringify([identity.displayName,identity.avatarUrl,identity.avatarColor,identity.avatarRevision,inboxSummary]);if(canvasIdentityKey===key)return;
    canvasIdentityKey=key;const face=avatar(identity.displayName,identity.avatarUrl,identity.avatarColor,options.accountId,identity.avatarRevision);
    if(/^#[a-f0-9]{6}$/iu.test(identity.avatarColor??''))face.style.backgroundColor=identity.avatarColor!;
    profileControl.replaceChildren(face);profileControl.title=identity.displayName;
    const total=inboxSummary.messages+inboxSummary.requests+inboxSummary.invitations;
    profileControl.setAttribute('aria-label',`Меню, ${identity.displayName}${total?`. Новых сообщений: ${inboxSummary.messages}; запросов в контакты: ${inboxSummary.requests}${inboxSummary.invitations?`; приглашений в комнаты: ${inboxSummary.invitations}`:''}`:''}`);
    if(total){const badge=el('span','uf-profile-unread',total>99?'99+':String(total)+(inboxSummary.incomplete?'+':''));badge.setAttribute('aria-hidden','true');profileControl.append(badge);}
  }
  function updateChrome(): void {
    const focused = document.activeElement;
    updateIdentity();
    element.dataset.mode = mode; element.dataset.filter = filter; title.textContent = mode === 'mine' ? 'Мои соты' : 'Найти своё';
    workspaceTools.hidden = mode !== 'mine';
    if (mode !== 'search') searchLimit.hidden = true;
    element.dataset.query = queries[mode] ? 'active' : 'empty';
    if (queries[mode]) element.dataset.search = 'open';
    element.setAttribute('aria-label', mode === 'mine' ? 'Мои соты' : 'Поиск в Сотах');
    mineButton.setAttribute('aria-pressed', String(mode === 'mine')); searchButton.setAttribute('aria-pressed', String(mode === 'search'));
    searchInput.value = queries[mode]; searchInput.placeholder = mode === 'mine' ? 'Найти в моих сотах' : 'Приложения, люди, комнаты';
    searchInput.setAttribute('aria-label', searchInput.placeholder); clear.hidden = !searchInput.value;
    arrange.hidden = true; spaces.hidden = mode !== 'mine';
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
    if (mode !== 'mine') return;
    if (!engine) { renderMineSearchResults(); return; }
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
    engine.update({ ...(initialEntities ? { entities: initialEntities } : {}), filter: queries.mine, visibleKinds: filter === 'all' ? ['app', 'builtin', 'person', 'community', 'device','object'] : filter === 'app' ? ['app', 'builtin'] : [filter] });
    updateMineEmpty(summary?.document ?? engine.snapshot());
  }
  function hasConfirmedPersonalLayout(): boolean {
    return current() && !!engine && state?.serverConfirmed === true && state.state === 'saved'
      && state.localDurable && state.pendingCount === 0 && state.document.contexts.length > 0
      && !persistence.hasUnsavedChanges() && !engine.hasUnsavedChanges()
      && !canvasContent.hasUnsavedChanges() && !canvasContent.hasUnconfirmedWork()
      && JSON.stringify(engine.snapshot()) === JSON.stringify(state.document);
  }
  function updateMineEmpty(doc: FieldDocument): void {
    if (!current()) { empty.hidden = true; emptyDocument = null; return; }
    if (mode !== 'mine') {
      if (empty.className?.split(/\s+/u).includes('uf-personal-start')) { empty.className = 'uf-empty'; empty.hidden = true; emptyDocument = null; }
      return;
    }
    const query = queries.mine.trim().toLocaleLowerCase('ru');
    const canStart = (document: FieldDocument): boolean => current() && mode === 'mine' && !queries.mine.trim() && filter === 'all'
      && typeof options.onCreateMenu === 'function'
      && document.contexts.length > 0 && document.shortcuts.length === 0 && hasConfirmedPersonalLayout()
      && JSON.stringify(document) === JSON.stringify(state?.document)
      && element.dataset.search !== 'open' && !summary?.arranging && !selected;
    const personalStart = canStart(doc);
    if (!personalStart && (renderMineSearchResults() || mineResolvedGeneration !== mineGeneration && element.dataset.mineState !== 'error')) {
      empty.hidden = true; emptyDocument = null; return;
    }
    const scope = `${queries.mine}:${filter}:${personalStart}`;
    if (doc === emptyDocument && scope === emptyScope && metadataRevision === emptyMetadataRevision) return;
    emptyDocument = doc; emptyScope = scope; emptyMetadataRevision = metadataRevision;
    empty.className = 'uf-empty';
    if (personalStart) {
      const note = button('Записка', 'note', 'uf-first-note', () => {
        if (engine && canStart(engine.snapshot())) options.onCreateMenu?.();
      });
      note.setAttribute('aria-label', 'Создать первую записку'); note.setAttribute('aria-haspopup', 'dialog');
      note.setAttribute('data-glass-role', 'control'); note.setAttribute('data-glass-safe-area', 'off');
      empty.className = 'uf-empty uf-personal-start'; empty.hidden = false; empty.replaceChildren(note); return;
    }
    const matchingContexts = new Set(doc.contexts.filter(context => context.title.toLocaleLowerCase('ru').includes(query)).map(context => context.contextId));
    const count = doc.shortcuts.filter(shortcut => {
      const item = metadata.get(fieldEntityKey(shortcut.entity));
      return item && matches(item, matchingContexts.has(shortcut.contextId) ? '' : queries.mine, filter);
    }).length;
    const availableOwnApps = overviewApps().some(app => matches(overviewEntity(app), queries.mine, filter));
    empty.hidden = count > 0 || availableOwnApps || liveApps.some(item => matches(item, queries.mine, filter)) || !!query && matchingContexts.size > 0;
    const catalogVisible = filter === 'all' || filter === 'app';
    if (!empty.hidden && catalogVisible && element.dataset.liveAppsState === 'loading') {
      empty.replaceChildren(el('p', '', 'Загружаем доступные приложения…')); return;
    }
    if (!empty.hidden && (ownedAppsState !== 'ready' || element.dataset.mineState === 'error' || catalogVisible && ['partial', 'offline'].includes(element.dataset.liveAppsState ?? '') || doc.shortcuts.some(shortcut => !mineKnownRefs.has(fieldEntityKey(shortcut.entity))))) {
      empty.replaceChildren(el('h2', '', 'Доступ пока не подтверждён'), button('Повторить', 'refresh', 'sw-button-quiet', () => run(Promise.all([loadMine(), refreshLiveApps()])))); return;
    }
    if (!empty.hidden) empty.replaceChildren(el('h2', '', queries.mine ? 'Ничего не найдено' : filter !== 'all' ? 'Здесь пока пусто' : 'Здесь начинается ваше пространство'), el('p', '', queries.mine ? 'Попробуйте другое название.' : 'Оставьте мысль, добавьте фото или приложение.'), button(queries.mine ? 'Очистить поиск' : filter !== 'all' ? 'Показать всё' : 'Добавить', queries.mine ? 'close' : filter !== 'all' ? 'layers' : 'plus', 'sw-button-primary', () => {
      if (queries.mine) { searchInput.value = ''; inputChanged(); searchInput.focus(); }
      else if (filter !== 'all') { filter = 'all'; filtersByMode.mine = 'all'; updateChrome(); inputChanged(true); }
      else if(options.onCreateMenu)options.onCreateMenu();else openAdd();
    }));
  }
  function updateSummary(next: UnifiedFieldSummary): void {
    const documentChanged = next.document !== summary?.document;
    if (summary && (next.document !== summary.document || next.focusContextId !== summary.focusContextId || next.arranging !== summary.arranging)) closeList(false);
    if (next.mode === 'mine' && next.focusContextId !== summary?.focusContextId && current()&&(!next.focusContextId||next.document.contexts.some(context=>context.contextId===next.focusContextId))) options.onContextChange?.(next.focusContextId);
    if (mode === 'mine' && selected?.shortcutId) {
      const inspected = next.document.shortcuts.find(shortcut => shortcut.shortcutId === selected?.shortcutId);
      if (!inspected || next.focusContextId && inspected.contextId !== next.focusContextId) closePreview(false);
    }
    summary = next; arrange.setAttribute('aria-pressed', String(next.arranging)); arrange.querySelector('span')!.textContent = next.arranging ? 'Готово' : 'Расставить';
    if (documentChanged) { publishKnownApps(next.document); refreshAppCoverage(); }
    element.dataset.arranging = String(next.arranging);
    element.dataset.focusContext = next.focusContextId;
    arrangeHint.hidden = !next.arranging;
    const zoomValue = `${Math.round(next.camera.scale * 100)}%`; if (percent.value !== zoomValue) percent.value = zoomValue;
    const scaleText=scaleControl.querySelector('span');if(scaleText)scaleText.textContent=zoomValue;scaleControl.setAttribute('aria-label',`Масштаб и вид полотна: ${zoomValue}`);
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
      engine.update({ document: next.document, revision: next.projectedRevision, persistence: 'saved',liveScene:liveAppScene(next.document) }); updateMineFilter();
      // A layout received from another window/device contains references, not
      // names or access. Resolve new identities through the existing directory.
      if (newRefs) run(element.dataset.ready === 'loading' ? loadInitialMine() : loadMine());
    }
    if (engine) updateMineEmpty(engine.snapshot());
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
      if (selected && (searchResponseScope !== scope || !previewCommunity && !progress && !append && !combined.some(item => fieldEntityKey(item.entity) === fieldEntityKey(selected!.item.entity)))) closePreview(false);
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
    appProjectionGeneration=engine.snapshot().shortcuts.some(shortcut=>shortcut.entity.kind==='object')?generation:0;
    element.dataset.appCoverageErrors='0';
    readAppCoverage(engine.snapshot());
    mineConfirmedGeneration = -1; element.dataset.mineState = 'loading'; emptyDocument = null; updateMineEmpty(engine.snapshot()); renderMineSearchResults();
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
    element.dataset.mineState = 'loading'; emptyDocument = null; updateMineEmpty(engine.snapshot());
    mineKnownRefs = new Set(BUILTINS.map(item => fieldEntityKey(item.entity)));
    const refs = [...new Map([...engine.snapshot().shortcuts.map(shortcut=>shortcut.entity),...(previewCommunity&&selected?.item.entity.kind==='community'?[selected.item.entity]:[])].map(ref=>[fieldEntityKey(ref),ref])).values()];
    const resolved = { items: [] as DirectoryEntity[], unavailable: [] as FieldEntityRef[], errors: [] as { source: string; code: string }[] };
    const accept = (batch: typeof resolved&{objectMetadata?:DirectoryObjectMetadata[]}): void => {
      if (!live()) return;
      const objectMetadata=new Map((batch.objectMetadata??[]).map(value=>[value.objectId,value.ownerRepresents]));
      if(batch.errors.some(error=>error.source==='objects'))element.dataset.appCoverageErrors=String(Number(element.dataset.appCoverageErrors??0)+batch.errors.filter(error=>error.source==='objects').length);
      for (const ref of batch.unavailable) {
        const key = fieldEntityKey(ref); resolvedUnavailableRefs.add(key);
        const represents=ref.kind==='object'?objectMetadata.get(ref.id):undefined;
        metadata.set(key, { entity: ref, title: batch.errors.length ? 'Ждёт подключения' : 'Недоступно', symbol: 'lock', source: 'owner',...(represents===undefined?{}:{ownerRepresents:represents}) });
        if (!batch.errors.length) mineKnownRefs.add(key);
      }
      merge(batch.items); for (const item of batch.items) mineKnownRefs.add(fieldEntityKey(item.entity));
      resolved.items.push(...batch.items); resolved.unavailable.push(...batch.unavailable); resolved.errors.push(...batch.errors);
      mineConfirmedGeneration = generation;
      engine!.update({ entities: allMetadata() }); updateMineFilter(); loading.hidden = true;
      refreshAppCoverage();
      publishKnownApps();
      if (batch.items.some(item => item.entity.kind === 'app')) element.dataset.startupAppsAt ??= String(Date.now());
      if (selected && batch.unavailable.some(ref => fieldEntityKey(ref) === fieldEntityKey(selected!.item.entity))) closePreview(false);
      else if(previewCommunity&&selected){const latest=currentEntity(selected.item.entity);if(latest)openPreview(latest,selected.shortcutId);}
    };
    try {
      // Independent sources publish confirmed batches without moving the user's saved shortcuts.
      let failed = false, firstError: unknown;
      await Promise.allSettled([
        refs.filter(ref => ref.kind === 'person' || ref.kind === 'community'),
        refs.filter(ref => ref.kind === 'app'),
        refs.filter(ref => ref.kind === 'device'),
        refs.filter(ref => ref.kind === 'object'),
      ].map(async sourceRefs => {
        try {
          for (let offset = 0; offset < sourceRefs.length; offset += 60) {
            if (!live()) return;
            const batch = await directory.resolve(sourceRefs.slice(offset, offset + 60));
            if (!live()) return;
            accept(batch);
          }
        } catch (error) { accepting = false; if (!failed) { failed = true; firstError = error; } throw error; }
        finally { if(sourceRefs.some(ref=>ref.kind==='object')&&appProjectionGeneration===generation){appProjectionGeneration=0;if(live())refreshAppCoverage();} }
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
        if(appProjectionGeneration===generation)appProjectionGeneration=0;
        refreshAppCoverage();
        await loadOwnedApps(generation);
      }
    }
  }
  function closePreview(restore = true,notify = true): void {
    const hadCommunity=!!previewCommunity;previewCommunity?.dispose();previewCommunity=null;
    preview?.remove(); preview = null;
    delete element.dataset.preview;
    if (restore) {
      const target = selected?.shortcutId ? element.querySelector<HTMLButtonElement>(`[data-shortcut-id="${CSS.escape(selected.shortcutId)}"]`) : previewOpener;
      if (target?.isConnected && target.getClientRects().length && getComputedStyle(target).visibility !== 'hidden') target.focus({ preventScroll: true });
      if (!target || document.activeElement !== target) focusFieldControl();
    }
    previewOpener = null;
    selected = null;
    if(hadCommunity&&notify)options.onCommunityFocus?.(null);
  }
  function communityPreviewNode(item:DirectoryEntity):FieldNode{
    // Local sizing only: this transient view never enters persistence or history.
    return layoutUnifiedField({contexts:[{contextId:'preview',title:'Просмотр',x:0,y:0}],shortcuts:[{shortcutId:'community-preview',entity:item.entity,contextId:'preview',placement:{kind:'free',x:0,y:0,width:480,height:420}}]},[item]).nodes[0]!;
  }
  function openPreview(item: DirectoryEntity, shortcutId?: string): void {
    if(item.entity.kind==='community'&&previewCommunity&&selected?.item.entity.id===item.entity.id){selected={item,...(shortcutId?{shortcutId}:{})};previewCommunity.update(communityPreviewNode(item));return;}
    const active = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const opener = active && preview?.contains(active) ? previewOpener : active && (element.contains(active) || active === searchInput) ? active : previewOpener;
    closePreview(false,false); previewOpener = opener; selected = { item, ...(shortcutId ? { shortcutId } : {}) };
    const panel = el('aside', 'uf-preview'); panel.setAttribute('aria-label', `Об объекте: ${item.title}`);
    panel.tabIndex = -1; const close = iconButton('Закрыть карточку', 'close', () => closePreview()); close.classList.add('uf-preview-close');
    if(item.entity.kind==='community'&&options.mountCommunity&&readCommunity(item.entity.id)){
      const node=communityPreviewNode(item),view=options.mountCommunity(node,()=>readCommunity(item.entity.id),()=>current()&&preview===panel);
      const placed=engine?.snapshot().shortcuts.find(shortcut=>fieldEntityKey(shortcut.entity)===fieldEntityKey(item.entity));
      const pin=iconButton(placed?'Действия с сотой':'На моё поле',placed?'sliders':'plus',()=>placed?openPlacementActions(item,placed.shortcutId):addToContext(item));pin.classList.add('uf-preview-pin');
      const noticeHost=el('div','uf-preview-notice');noticeHost.dataset.noticeHost='';noticeHost.dataset.noticeOwner=item.entity.id;noticeHost.dataset.glassIgnore='';
      panel.dataset.community='true';panel.append(noticeHost,close,pin,view.element);element.append(panel);preview=panel;previewCommunity=view;element.dataset.preview='open';
      view.update(node);view.setVisible(true);view.setPresentation?.('interactive');options.onCommunityFocus?.(item.entity.id);
      panel.addEventListener('keydown',event=>{if(event.key==='Escape'&&!event.defaultPrevented){event.stopPropagation();closePreview();}},{signal});panel.focus({preventScroll:true});return;
    }
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
    for(const action of placementCommands(item,shortcutId))actions.append(button(action.label,action.symbol,'sw-button-quiet',action.run));
    copy.append(actions); panel.append(close, copy); element.append(panel); element.dataset.preview = 'open'; preview = panel;
    panel.addEventListener('keydown', event => { if (event.key === 'Escape') { event.stopPropagation(); closePreview(); } }, { signal });
    panel.focus({ preventScroll: true });
  }
  function placementCommands(item:DirectoryEntity,shortcutId?:string):{label:string;symbol:string;run:()=>void}[]{
    const alreadyPlaced=engine?.snapshot().shortcuts.some(shortcut=>fieldEntityKey(shortcut.entity)===fieldEntityKey(item.entity));
    const commands=[{label:alreadyPlaced?'Добавить в другое пространство':'На моё поле',symbol:'plus',run:()=>{if(current())addToContext(item);}}];
    const placementCurrent=()=>current()&&mode==='mine'&&!!shortcutId&&engine?.snapshot().shortcuts.some(shortcut=>shortcut.shortcutId===shortcutId&&fieldEntityKey(shortcut.entity)===fieldEntityKey(item.entity));
    if(placementCurrent())commands.push({label:'Переместить',symbol:'sliders',run:()=>{if(placementCurrent()){closePreview();engine?.closeObject();engine?.beginMove(shortcutId!);}}},{label:'Убрать с поля',symbol:'close',run:()=>{if(placementCurrent()){closePreview();run(engine?.removeShortcut(shortcutId!).then(()=>updateMineFilter()));}}});
    return commands;
  }
  function openPlacementActions(item:DirectoryEntity,shortcutId:string):WorldDialog{
    const instance=dialog(item.title),actions=el('div','sw-stack');
    for(const action of placementCommands(item,shortcutId))actions.append(button(action.label,action.symbol,'sw-button-wide',()=>{instance.close();action.run();}));
    instance.body.append(actions);return instance;
  }
  function inspect(entity: FieldDirectoryEntity, shortcutId: string): WorldDialog|void {
    if (!current()) return;
    const item = currentEntity(entity.entity); if (!item) return;
    closePreview(false);
    if(item.entity.kind==='object'){
      const personal=mode==='mine'&&engine?.snapshot().shortcuts.some(value=>value.shortcutId===shortcutId);
      let handle:ReturnType<typeof mountCanvasObjectSettings>|undefined;const instance=dialog(item.title,()=>handle?.dispose());
      handle=mountCanvasObjectSettings(instance,{api:options.api,accountId:options.accountId,objectId:item.entity.id,title:item.title,projection:directory.getRecord(item.entity) as CanvasProjection|null,isCurrent:current,
        onChanged:()=>loadMine(),onOpen:()=>{const target=element.querySelector<HTMLElement>(`[data-shortcut-id="${CSS.escape(shortcutId)}"] textarea`);if(target)target.focus();else options.onOpen(item,directory.getRecord(item.entity));},
        security:createCanvasObjectSecurityActions({api:options.api,accountId:options.accountId,objectId:item.entity.id,isCurrent:current,vault:canvasContent.vault,nativeTransition:canvasContent.nativeTransition}),
        vault:canvasContent.vault,
        beforeChange:async()=>{await engine?.flush();await canvasContent.flush();if(engine?.hasUnsavedChanges()||canvasContent.hasUnsavedChanges())throw Object.assign(new Error('canvas_unsaved_work'),{code:'canvas_unsaved_work'});},
        ...(personal?{onMove:()=>engine?.beginMove(shortcutId),onRemovePlacement:()=>run(engine?.removeShortcut(shortcutId).then(()=>updateMineFilter()))}:{}),
      });return instance;
    }
    if(item.entity.kind==='community'&&options.mountCommunity)return openPlacementActions(item,shortcutId);
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
    if(mode==='mine')for(const item of liveApps){
      const key=fieldEntityKey(item.entity);
      if(!rows.some(row=>row.key===key)&&matches(item,queries.mine,filter)&&directory.getRecord(item.entity))rows.push({key,title:item.title,detail:item.source==='owner'?'Моё приложение':'Доступное приложение',entity:item.entity});
    }
    for (const context of doc.contexts) if (!query || matchingContexts.has(context.contextId) || includedContexts.has(context.contextId)) rows.push({ key: `context:${context.contextId}`, title: context.title, detail: 'Пространство', contextId: context.contextId });
    const find = localQuery.trim().toLocaleLowerCase('ru');
    return rows.filter(row => `${row.title} ${row.detail}`.toLocaleLowerCase('ru').includes(find));
  }
  function renderMineSearchResults(): true | void {
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
    const canSearch = current() && mode === 'mine' && !!needle && !composing;
    const capturedFilter = filter;
    const live = (): boolean => current() && mode === 'mine' && queries.mine === query && filter === capturedFilter
      && !composing && !mineSearchResults.hidden && mineSearchResults.isConnected;
    if (canSearch && options.onConnectDevice && element.dataset.search === 'open' && (filter === 'all' || filter === 'device')
      && /^(?:подключить|добавить)\s+(?:ещ[её]\s+одно\s+)?устройство[.!?]?$/u.test(needle)) {
      const signature = JSON.stringify(['connect-device', query, filter]);
      if (signature !== mineSearchSignature) {
        mineSearchSignature = signature; mineSearchResults.hidden = false;
        mineSearchStatus.textContent = 'Найдено: 1';
        const action = button('Подключить ещё одно устройство', 'laptop', 'uf-list-entry uf-search-result', () => {
          if (live() && element.dataset.search === 'open' && action.isConnected) options.onConnectDevice?.();
        });
        mineSearchList.replaceChildren(action);
      }
      return true;
    }
    if (!canSearch || !engine) {
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
    for (const row of rows.slice(0, 6)) {
      const action = button('', row.contextId ? 'layers' : 'next', 'uf-list-entry uf-search-result', () => {
        if (!live() || !action.isConnected) return;
        const fresh = listEntries().find(value => value.key === row.key);
        if (!fresh) {
          const focused = document.activeElement === action;
          mineSearchSignature = ''; renderMineSearchResults();
          if (focused && (document.activeElement === document.body || document.activeElement === null) && live()) searchInput.focus({ preventScroll: true });
          return;
        }
        if (fresh.contextId) { closePreview(false); engine?.focusContext(fresh.contextId); }
        else if (fresh.entity) { const item = currentEntity(fresh.entity); if (item) activate(item, fresh.shortcutId); }
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
    const focusedContext=summary?.focusContextId;
    if (focusedContext&&doc.contexts.some(context=>context.contextId===focusedContext)) select.value = focusedContext;
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
        instance.close();closePreview(false);changeMode('mine');const placed=engine?.snapshot().shortcuts.find(shortcut=>shortcut.contextId===contextId&&fieldEntityKey(shortcut.entity)===fieldEntityKey(item.entity));if(placed)engine?.revealShortcut(placed.shortcutId);
        options.onMessage(state.pendingCount ? 'Уже добавлено на устройстве · ждём подключения' : 'Уже на вашем поле'); return;
      }
      changeMode('mine'); merge([item]); engine?.update({ entities: [...allMetadata(), item] });
      run(engine?.addShortcut(item.entity, contextId).then(receipt => {
        if (!current()) return; updateMineFilter();
        if (receipt?.status === 'conflict') { options.onMessage('Поле изменилось. Проверьте расстановку.', true); return; }
        if (receipt?.status === 'volatile' && !receipt.localDurable) { options.onMessage('Расстановка ещё не сохранена', true); return; }
        instance.close();closePreview(false);const placed=engine?.snapshot().shortcuts.find(shortcut=>shortcut.contextId===contextId&&fieldEntityKey(shortcut.entity)===fieldEntityKey(item.entity));if(placed)engine?.revealShortcut(placed.shortcutId);
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
      const document = field.snapshot(), liveContext = liveAppScene(document).contexts[0];
      const showsLiveApps = liveContext && (filter === 'all' || filter === 'app') && (!queries.mine.trim() || liveApps.some(item => matches(item, queries.mine, filter)));
      // Placed builtins are usable without a directory response. Cached refs
      // keep their loading/unknown metadata and gain no display authority here.
      if (showsLiveApps || document.shortcuts.some(shortcut => BUILTINS.some(item => fieldEntityKey(item.entity) === fieldEntityKey(shortcut.entity)))) loading.hidden = true;
      if (liveContext && showsLiveApps) {
        element.dataset.startupAppsAt ??= String(Date.now());
        if (!liveAppsFramed) {
          liveAppsFramed = true;
          if (document.shortcuts.every(shortcut => shortcut.entity.kind === 'builtin') && options.viewState?.mineFit !== 'manual' && !selected && !summary?.arranging && !queries.mine.trim() && ['all', 'app'].includes(filter)) field.focusContext(liveContext.contextId, 'overview');
        }
      }
    }).catch(error => { if (current() && engine === field) report(error); });
  }
  function mountEngine(loaded: FieldPersistenceState): void {
    fieldGlass?.dispose();fieldGlass=null;
    state = loaded;
    mineFilterAdmitted = false;
    if (loaded.document.contexts.length) firstResolve = false;
    for (const shortcut of loaded.document.shortcuts) if (!metadata.has(fieldEntityKey(shortcut.entity))) metadata.set(fieldEntityKey(shortcut.entity), { entity: shortcut.entity, title: 'Загрузка…', symbol: 'lock', source: 'owner' });
    const field = createUnifiedField({ accountId: options.accountId, document: loaded.document, revision: loaded.projectedRevision,
      createObject,objectKey,canContinueObjectFocus:(node,previousKey,nextKey)=>canvasContent.canContinueObjectFocus(node,previousKey,nextKey),
      mode, entities: allMetadata(), readableMobileFocus: true, readableObjectFocus:true, dimNonMatches: true, ...(options.viewState ? { viewState: options.viewState } : {}), commit: (doc, context) => persistence.commit(doc, context),
      onViewStateChange: view => { if (current()){options.viewState=view;options.onViewStateChange?.(view);} },
      onObjectFocusChange:entity=>{focusedCommunityId=entity?.entity.kind==='community'?entity.entity.id:null;if(current())options.onCommunityFocus?.(focusedCommunityId);},
      onActivate: activate, onInspect: inspect, onContextActivate: contextId => { engine?.focusContext(contextId); const entity = mode === 'search' ? searchPlacement.contextEntity(contextId) : null; const item = entity ? currentEntity(entity.entity) : null; if (item) openPreview(item); }, onStateChange: updateSummary,
      resolveArt: item => options.resolveArt ? options.resolveArt(item) : resolveFieldAppArt({ ...(item.entity.kind === 'app' ? { appId: item.entity.id } : {}), ...(item.coverKey ? { coverKey: item.coverKey } : {}) }, { screenWidth: window.innerWidth }),
    }); engine = field; host.prepend(field.element);appCoverageSceneSignature=JSON.stringify({contexts:[],shortcuts:[]});refreshAppCoverage();
    fieldGlass=mountFieldGlass(field.element,{onStatus:status=>{if(current()&&engine===field){element.dataset.glassMode=status.mode;element.dataset.glassSurfaces=String(status.surfaces);}}});
    if (mode === 'search') field.update({ searchDocument: searchScene() }); updateChrome(); updateStatus();
    element.dataset.startupLayoutAt ??= String(Date.now());
    revealReadyMine(field);
    // Cached references can start fresh signed metadata reads while the remote layout is loading.
    if (loaded.document.shortcuts.length && element.dataset.ready === 'loading') run(loadInitialMine());
  }
  const unobservePersistence = persistence.subscribe(applyPersistence);
  if (options.persistence) applyPersistence(persistence.getState());
  const ready = (async (): Promise<void> => {
    const liveTask = refreshLiveApps().catch(error => { if (current()) options.onMessage(errorText(error), true); });
    const loaded = await persistence.load(); if (!current()) return;
    // The async local-store subscription may already have mounted the cache.
    const cachedField = engine as UnifiedField | null;
    if (!cachedField) mountEngine(loaded); else cachedField.update({ document: loaded.document, revision: loaded.projectedRevision });
    await engine!.ready; if (!current()) return;
    if (!loading.hidden && mode === 'mine') loading.textContent = 'Загружаем доступные приложения…';
    if (loaded.revision === 0 && loaded.document.contexts.length === 0 && loaded.localDurable && loaded.state !== 'conflict') {
      const initial = createPersonalFieldDocument({includeBuiltins:false});
      // Only this account's accepted pinned refs are eligible for the one-time migration.
      const refs = [...new Set(options.pinnedApps ?? [])].slice(0, 100).filter(id => /^[A-Za-z0-9_-]{3,160}$/u.test(id)).map(id => ({ kind: 'app' as const, id }));
      if (refs.length) { const resolved = await directory.resolve(refs); if (!current()) return; merge(resolved.items); for (const item of resolved.items)if(item.entity.kind==='app')initial.shortcuts.push({ shortcutId: crypto.randomUUID(), entity: {kind:'app',id:item.entity.id}, contextId: 'personal', slot: nextFieldSlot(initial, 'personal', item.entity.kind) }); }
      const receipt = await persistence.commit(initial, { expectedRevision: 0, requestId: crypto.randomUUID() }); if (!current()) return;
      engine!.update({ document: receipt.document, revision: receipt.revision, entities: allMetadata() });
    }
    if (mode === 'mine') revealReadyMine(engine!);
    const mineTask = Promise.all([loadInitialMine(), liveTask]);
    if (mode === 'search') await Promise.all([mineTask, searchDirectory()]); else await mineTask;
    if (!current()) return;
    if (current()) { element.dataset.ready = 'ready'; loading.hidden = true; }
  })().catch(error => { if (current()) { element.dataset.ready = 'error'; loading.hidden = true; } report(error); });
  updateChrome();
  function showSearch():void {
    const active=document.activeElement;
    if(active instanceof HTMLElement&&!search.contains(active))searchOpener=active;
    element.dataset.search='open';searchInput.focus();renderMineSearchResults();renderOwnApps();options.onSearchVisibilityChange?.();
  }
  return {
    element, ready,
    async openCommunity(communityId,pane='voice'){
      const ticket=++communityOpenSequence;await ready;if(!current()||ticket!==communityOpenSequence)return null;
      const ref={kind:'community' as const,id:communityId},resolved=await directory.resolve([ref]);if(!current()||ticket!==communityOpenSequence)return null;
      merge(resolved.items);const item=resolved.items.find(value=>fieldEntityKey(value.entity)===fieldEntityKey(ref));
      if(!item||!readCommunity(communityId))throw Object.assign(new Error('community_unavailable'),{code:'community_unavailable'});
      engine?.update({entities:allMetadata()});const shortcut=engine?.snapshot().shortcuts.find(value=>value.entity.kind==='community'&&value.entity.id===communityId);
      const focused=mode==='mine'&&shortcut?engine?.focusObject(shortcut.shortcutId):null;
      if(focused&&'showChat'in focused){closePreview(false,false);const room=focused as CommunityFieldObjectView;if(pane==='chat')room.showChat();else room.showVoice();}
      else{openPreview(item,shortcut?.shortcutId);if(pane==='chat')previewCommunity?.showChat();else previewCommunity?.showVoice();}
      return readCommunity(communityId);
    },
    closeCommunity(){communityOpenSequence++;if(previewCommunity)closePreview();if(focusedCommunityId)engine?.closeObject();},
    watchEntities(){return [...new Map([...(engine?.snapshot().shortcuts.map(shortcut=>shortcut.entity)||[]),...liveApps.map(item=>item.entity),...(previewCommunity&&selected?.item.entity.kind==='community'?[selected.item.entity]:[])].map(ref=>[fieldEntityKey(ref),ref])).values()];},
    setMode(next, query, nextFilter) { if (!current()) return; closeList(false); if (query !== undefined) queries[next] = query; if (nextFilter) filtersByMode[next] = nextFilter; if (mode !== next) changeMode(next); else { filter = filtersByMode[mode]; updateChrome(); if (mode === 'search') run(searchDirectory()); else updateMineFilter(); } },
    attachHeaderSearch(next) { header = next; placeSearch(); },
    focusSearch:showSearch,
    closeSearch: () => closeSearch.click(),
    openView: openViewTools, openList, arrange() { engine?.setArrange(!summary?.arranging); },
    setInboxSummary(value){if(!current())return;inboxSummary={messages:Number.isSafeInteger(value.messages)?Math.max(0,value.messages):0,requests:Number.isSafeInteger(value.requests)?Math.max(0,value.requests):0,invitations:Number.isSafeInteger(value.invitations)?Math.max(0,value.invitations!):0,incomplete:value.incomplete===true};updateIdentity();},
    openHiddenObjects(){let handle:ReturnType<typeof mountCanvasHiddenObjects>|undefined;const instance=dialog('Скрытые объекты',()=>handle?.dispose());handle=mountCanvasHiddenObjects(instance,{api:options.api,accountId:options.accountId,isCurrent:current,onChanged:()=>loadMine()});},
    async openObjectSettings(objectId){
      await ready;if(!current())return;
      const shortcut=engine?.snapshot().shortcuts.find(item=>item.entity.kind==='object'&&item.entity.id===objectId),item=currentEntity({kind:'object',id:objectId});
      if(!shortcut||!item)throw Object.assign(new Error('canvas_access_denied'),{code:'canvas_access_denied'});
      const settings=inspect(item,shortcut.shortcutId);if(!settings)return;
      await new Promise<void>(resolve=>settings.element.addEventListener('close',()=>resolve(),{once:true}));
    },
    async addEntity(ref) {
      await ready; if (!current()) return;
      // A caller supplies an identity, never display metadata or authority.
      const result = await directory.resolve([ref]); if (!current()) return;
      const item = result.items.find(value => fieldEntityKey(value.entity) === fieldEntityKey(ref));
      if (!item) { options.onMessage(result.errors.length ? 'Не удалось проверить доступ. Повторите добавление.' : 'Сота недоступна для добавления.', true); return; }
      merge([item]); engine?.update({ entities: allMetadata() }); addToContext(item);
    },
    async addObject(objectId,size={width:320,height:240}){
      // Fresh admission of this object needs the confirmed layout, not unrelated directory pages.
      if(!hasConfirmedPersonalLayout())await ready;
      if(!current()||!engine)throw Object.assign(new Error('field_account_changed'),{code:'field_account_changed'});
      const field=engine,ref:FieldEntityRef={kind:'object',id:objectId};
      const result=await directory.resolve([ref]);if(!current()||engine!==field)throw Object.assign(new Error('field_account_changed'),{code:'field_account_changed'});
      const item=result.items.find(value=>fieldEntityKey(value.entity)===fieldEntityKey(ref));if(!item)throw Object.assign(new Error('canvas_object_unavailable'),{code:'canvas_object_unavailable'});
      merge([item]);mineKnownRefs.add(fieldEntityKey(ref));queries.mine='';filtersByMode.mine='all';if(mode!=='mine')changeMode('mine');filter='all';searchInput.value='';updateChrome();updateMineFilter();options.onRoute('mine','','all');field.update({entities:allMetadata()});
      let doc=field.snapshot(),existing=doc.shortcuts.find(value=>fieldEntityKey(value.entity)===fieldEntityKey(ref));
      if(!existing){
        if(!doc.contexts.length){const context=await field.addContext('Личное',{x:0,y:0});if(context?.status==='conflict')throw Object.assign(new Error('field_revision_conflict'),{code:'field_revision_conflict'});doc=field.snapshot();}
        const context=doc.contexts.find(value=>value.contextId===summary?.focusContextId)??doc.contexts[0];if(!context)throw Object.assign(new Error('field_context_missing'),{code:'field_context_missing'});
        const width=Math.max(96,Math.min(1600,size.width)),height=Math.max(96,Math.min(1600,size.height)),camera=field.getCamera();
        const nodes=layoutUnifiedField(doc,allMetadata()).nodes;
        let point={x:camera.x-context.x,y:camera.y-context.y};
        for(let index=0;index<49;index++){
          const ring=Math.ceil((Math.sqrt(index+1)-1)/2),side=Math.max(1,2*ring),offset=index-(2*ring-1)**2;
          const dx=ring===0?0:offset<side?-ring+offset:offset<side*2?ring:offset<side*3?ring-(offset-side*2):-ring;
          const dy=ring===0?0:offset<side?-ring:offset<side*2?-ring+(offset-side):offset<side*3?ring:ring-(offset-side*3);
          const candidate={x:Math.max(-999000,Math.min(999000,camera.x-context.x+dx*(width+28))),y:Math.max(-999000,Math.min(999000,camera.y-context.y+dy*(height+28)))};
          point=candidate;const x=context.x+point.x,y=context.y+point.y;
          if(nodes.every(node=>x+width/2+16<=node.bounds.left||x-width/2-16>=node.bounds.right||y+height/2+16<=node.bounds.top||y-height/2-16>=node.bounds.bottom))break;
        }
        const receipt=await field.addFreeShortcut(ref,context.contextId,{kind:'free',...point,width,height});
        if(!current()||engine!==field)throw Object.assign(new Error('field_account_changed'),{code:'field_account_changed'});
        if(receipt?.status==='conflict')throw Object.assign(new Error('field_revision_conflict'),{code:'field_revision_conflict'});
        doc=field.snapshot();existing=doc.shortcuts.find(value=>fieldEntityKey(value.entity)===fieldEntityKey(ref));
      }
      if(!existing)throw Object.assign(new Error('canvas_placement_unconfirmed'),{code:'canvas_placement_unconfirmed'});
      field.update({entities:allMetadata(),selectedShortcutId:existing.shortcutId});
      refreshAppCoverage();
      if(!current()||engine!==field)throw Object.assign(new Error('field_account_changed'),{code:'field_account_changed'});
      field.revealShortcut(existing.shortcutId);
      const saved=persistence.getState();return{shortcutId:existing.shortcutId,status:saved.state==='saved'?'saved':'pending',localDurable:saved.localDurable};
    },
    focusContext(contextId) { if (current()) engine?.focusContext(contextId); }, openAdd,
    async refresh(refreshOptions) {
      await ready; if (!current()) return;
      const mineTask = Promise.all([loadMine(),refreshLiveApps()]);
      if (mode === 'search') await Promise.all([mineTask, searchDirectory(false, refreshOptions?.preserveView === true)]); else await mineTask;
      if(current())updateIdentity();
    },
    async refreshOwnApps() {
      await ready; if (!current()) return;
      const generation = mineGeneration, read = ++ownAppsReadSequence;
      try {
        const result = await options.api.request<{ apps: unknown[] }>('apps.list', { expectedAccountId: options.accountId });
        if (!current() || generation !== mineGeneration || read < ownAppsAppliedRead) return;
        if (!Array.isArray(result.apps)) throw Error('invalid_apps_overview');
        ownAppsAppliedRead = read;
        ownedApps = result.apps; ownedAppsState = 'ready'; renderOwnApps();
        if (!mineSearchResults.contains(document.activeElement)) renderMineSearchResults();
        if (!listDialog?.element.querySelector('.uf-list-results')?.contains(document.activeElement)) renderOpenList?.();
      } catch {
        if (!current() || generation !== mineGeneration || read < ownAppsAppliedRead) return;
        const active = document.activeElement, ownFocused = ownAppsOverview.contains(active), searchFocused = mineSearchResults.contains(active);
        const list = listDialog?.element, listFocused = list?.querySelector('.uf-list-results')?.contains(active) === true;
        ownAppsAppliedRead = read; ownedApps = []; ownedAppsState = 'offline';
        renderOwnApps(); renderMineSearchResults(); renderOpenList?.();
        if (engine && mode === 'mine') { emptyDocument = null; updateMineEmpty(engine.snapshot()); }
        if (searchFocused) searchInput.focus({ preventScroll: true });
        else if (listFocused) list?.querySelector<HTMLInputElement>('input[aria-label="Найти в этом списке"]')?.focus({ preventScroll: true });
        else if (ownFocused) {
          const retry = ownAppsOverview.querySelector<HTMLButtonElement>('.uf-own-apps-retry');
          if (retry) retry.focus({ preventScroll: true }); else focusFieldControl();
        }
      }
    },
    async renewHostSessions(reason){if(current())await canvasContent.renewHostSessions(reason);},
    hasUnsavedChanges: () => !!engine?.hasUnsavedChanges() || persistence.hasUnsavedChanges() || canvasContent.hasUnsavedChanges(),
    hasUnconfirmedWork:()=>canvasContent.hasUnconfirmedWork(),readRuntimeEpoch:()=>canvasContent.readRuntimeEpoch(),
    async flush() { await engine?.flush(); await canvasContent.flush();await persistence.flush(); },
    async preparePublication(){await engine?.flush();await canvasContent.flush();await persistence.flush();const saved=persistence.getState();if(!current()||!saved||saved.state!=='saved'||saved.pendingCount||!saved.localDurable||persistence.hasUnsavedChanges()||engine?.hasUnsavedChanges()||canvasContent.hasUnsavedChanges())throw Object.assign(new Error('canvas_publication_not_ready'),{code:'canvas_publication_not_ready'});},
    reconnect() { run(persistence.retry().then(applyPersistence)); run(this.refresh({ preserveView: true })); },
    dispose() { disposed = true; searchGeneration++; mineGeneration++; if (searchTimer) clearTimeout(searchTimer); queuedSearch?.(false); queuedSearch = null; queuedMine?.(false); queuedMine = null; abort.abort(); for (const instance of [...dialogs]) instance.close({ restoreFocus: false }); closePreview(false); fieldGlass?.dispose();fieldGlass=null;engine?.destroy(); engine = null;canvasContent.dispose(); unobservePersistence(); if (!options.persistence) persistence.dispose(); directory.dispose(); listDialog = null; search.remove(); listButton.remove(); element.remove(); },
  };
}
