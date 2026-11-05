import { createFieldDocument, fieldEntityKey, validateAnyFieldDocument as validateFieldDocument, validateAnyFieldEntity as validateFieldEntity, type FieldContext, type AnyFieldDocument as FieldDocument, type AnyFieldEntityRef as FieldEntityRef, type AnyFieldEntityKind as FieldEntityKind, type FieldPlacement } from '../../modules/field/contract.mjs';
import { el, initials, nounCount, safeImageUrl } from './dom';
import { icon } from './icons';
import { roundedHexPath, SQRT3 } from '../geometry/hex.mjs';
import { applyFieldCommand, compactFieldContext, createFieldHistory, nextFieldSlot, recordFieldHistory, undoFieldHistory, type FieldCommand } from './unified-field-state.mjs';
import { fieldLevelOfDetail, fieldScreenToWorld, fitFieldCamera, normalizeFieldCamera, zoomFieldCamera, type FieldCamera, type FieldPoint } from './unified-field-camera.mjs';
import { createFieldSpatialIndex, fieldBounds, fieldBoundsOverlap, fieldMovePreview, fieldPointToSlot, fieldSlotToPoint, layoutUnifiedField,
  nearestFieldNode, UNIFIED_FIELD_METRICS, type FieldDirectoryEntity, type FieldMovePreview, type FieldMoveTarget,
  type FieldNode, type FieldScene, type UnifiedFieldLayout, type UnifiedFieldMetrics } from './unified-field-layout.mjs';
import type { AppArt } from './app-art.mjs';
import { resolveAppArt } from './app-art.mjs';
import type { FieldAppArt } from './field-art.mjs';
import { errorText } from './dialogs';
import { combineFieldLayout, combineFieldScene, validateLiveFieldScene } from './unified-field-live.mjs';
import {validateFieldPresentation,fieldPresentationClip,fieldPointInPolygon,fieldResizeAnchor,fieldResizeDimensions,type FieldPresentation} from './unified-field-presentation.mjs';
import type {FieldObjectView} from './native-field-objects';
import {fieldObjectFocusFrame,fieldObjectNeedsFocus} from './unified-field-object-focus.mjs';
import './unified-field.css';

export type { FieldDirectoryEntity, FieldScene, FieldMoveTarget, UnifiedFieldMetrics } from './unified-field-layout.mjs';
export type { FieldCamera } from './unified-field-camera.mjs';
export type { AnyFieldDocument as FieldDocument, FieldContext, AnyFieldEntityRef as FieldEntityRef } from '../../modules/field/contract.mjs';
export type UnifiedFieldMode = 'mine' | 'search';
type FieldVisualArt = AppArt & { icon?: FieldAppArt['icon'] };

export interface FieldWheelInput {
  deltaX: number; deltaY: number; deltaMode: number;
  ctrlKey: boolean; metaKey: boolean; shiftKey: boolean;
}

/** Scroll follows the canvas; only an explicit zoom gesture changes its scale. */
export function fieldWheelCamera(camera: FieldCamera, input: FieldWheelInput, anchor: FieldPoint, view: { width: number; height: number }): FieldCamera | null {
  if (![input.deltaX, input.deltaY].every(Number.isFinite) || ![0, 1, 2].includes(input.deltaMode)) return null;
  const factor = input.deltaMode === 1 ? 16 : input.deltaMode === 2 ? view.height : 1;
  const dx = input.deltaX * factor, dy = input.deltaY * factor;
  if (input.ctrlKey || input.metaKey) {
    if (!dy) return null;
    return zoomFieldCamera(camera, camera.scale * Math.exp(Math.max(-20, Math.min(20, -dy * .002))), anchor, view);
  }
  if (!dx && !dy) return null;
  const horizontal = input.shiftKey && !dx;
  return normalizeFieldCamera({ ...camera, x: camera.x + (horizontal ? dy : dx) / camera.scale, y: camera.y + (horizontal ? 0 : dy) / camera.scale });
}

/** A page or nested control must never lose its own scrolling to the canvas. */
export function shouldYieldFieldWheel(target: Element, viewport: HTMLElement, input: FieldWheelInput): boolean {
  if (target.closest('input,textarea,select,[contenteditable],.uf-move-tools,.uf-inspect,.uf-pan-controls')) return true;
  const styleOf = (node: Element): CSSStyleDeclaration | undefined => viewport.ownerDocument.defaultView?.getComputedStyle(node);
  const scrolls = (node: Element, axis: 'X' | 'Y', root = false): boolean => {
    const style = styleOf(node), overflow = style?.[axis === 'X' ? 'overflowX' : 'overflowY'] ?? '';
    const allowed = /auto|scroll|overlay/.test(overflow) || root && !/hidden|clip/.test(overflow);
    return allowed && (axis === 'X' ? node.scrollWidth - node.clientWidth : node.scrollHeight - node.clientHeight) > 1;
  };
  for (let node: Element | null = target; node && node !== viewport; node = node.parentElement) {
    if (scrolls(node, 'X') || scrolls(node, 'Y')) return true;
  }
  if (!input.ctrlKey && !input.metaKey && input.deltaY) {
    for (let node = viewport.parentElement; node; node = node.parentElement) {
      if (scrolls(node, 'Y', node === viewport.ownerDocument.scrollingElement)) return true;
    }
  }
  return false;
}
type CameraFit = 'manual' | 'overview' | 'context';
export interface FieldCommitResult { status: 'saved' | 'volatile' | 'conflict'; revision: number; document: FieldDocument; errorCode?: string; localDurable?: boolean }
export interface UnifiedFieldViewState { mine?: FieldCamera; search?: FieldCamera; mineContext?: string; searchContext?: string; mineSelection?: string; searchSelection?: string; mineFit?: CameraFit; searchFit?: CameraFit; searchScope?: string }
export interface UnifiedFieldSummary {
  mode: UnifiedFieldMode; camera: FieldCamera; focusContextId: string; selectedShortcutId: string; arranging: boolean;
  persistence: 'saved' | 'saving' | 'pending' | 'error' | 'conflict'; localDurable: boolean; revision: number;
  document: FieldDocument; canUndo: boolean; message: string;
}
export interface UnifiedFieldOptions {
  accountId: string;
  document?: FieldDocument;
  revision?: number;
  entities?: readonly FieldDirectoryEntity[];
  mode?: UnifiedFieldMode;
  viewState?: UnifiedFieldViewState;
  metrics?: Partial<UnifiedFieldMetrics>;
  readableMobileFocus?: boolean;
  /** Use the same provider element at readable screen size without changing its placement. */
  readableObjectFocus?: boolean;
  dimNonMatches?: boolean;
  commit?: (document: FieldDocument, context: { expectedRevision: number; requestId: string }) => Promise<FieldCommitResult>;
  onActivate?: (entity: FieldDirectoryEntity, shortcutId?: string) => void;
  onInspect?: (entity: FieldDirectoryEntity, shortcutId: string) => void;
  onObjectFocusChange?: (entity:FieldDirectoryEntity|null)=>void;
  onContextActivate?: (contextId: string) => void;
  onStateChange?: (summary: UnifiedFieldSummary) => void;
  onViewStateChange?: (view: UnifiedFieldViewState) => void;
  resolveArt?: (entity: FieldDirectoryEntity) => FieldVisualArt | null;
  createObject?: (node:FieldNode)=>FieldObjectView|null;
  /** Current authorized source identity. A null/replaced key destroys plaintext/media immediately. */
  objectKey?: (node:FieldNode)=>string|null;
  /** Only a trusted provider may keep UI focus across an explicit unlock. Its old view is still replaced. */
  canContinueObjectFocus?: (node:FieldNode,previousKey:string,nextKey:string)=>boolean;
}
export interface UnifiedFieldUpdate {
  document?: FieldDocument; revision?: number; entities?: readonly FieldDirectoryEntity[]; mode?: UnifiedFieldMode;
  /** ACL-aware, read-only availability from the directory; never included in snapshot/commit/history. */
  liveScene?: FieldScene;
  scope?: string; searchDocument?: FieldScene; selectedShortcutId?: string;
  filter?: string; visibleKinds?: readonly FieldEntityKind[];
  persistence?: 'saved' | 'pending' | 'conflict'; localDurable?: boolean; acceptRemote?: boolean;
}
export interface UnifiedField {
  element: HTMLElement;
  ready: Promise<void>;
  update(update: UnifiedFieldUpdate): void;
  getCamera(): FieldCamera;
  setCamera(camera: FieldCamera): void;
  focusContext(contextId: string): void;
  revealShortcut(shortcutId:string):void;
  focusObject(shortcutId:string):FieldObjectView|null;
  closeObject():void;
  fitOverview(): void;
  setArrange(enabled: boolean): void;
  addContext(title: string, point?: FieldPoint): Promise<FieldCommitResult | null>;
  renameContext(contextId: string, title: string): Promise<FieldCommitResult | null>;
  removeContext(contextId: string, options?: { removeShortcuts?: boolean }): Promise<FieldCommitResult | null>;
  addShortcut(entity: FieldEntityRef, contextId: string): Promise<FieldCommitResult | null>;
  addFreeShortcut(entity:FieldEntityRef,contextId:string,placement:Extract<FieldPlacement,{kind:'free'}>):Promise<FieldCommitResult|null>;
  setShortcutPlacement(shortcutId:string,placement:FieldPlacement,contextId?:string):Promise<FieldCommitResult|null>;
  resizeShortcut(shortcutId:string,width:number,height:number):Promise<FieldCommitResult|null>;
  removeShortcut(shortcutId: string): Promise<FieldCommitResult | null>;
  compactContext(contextId: string, expected: FieldDocument): Promise<FieldCommitResult | null>;
  moveShortcut(shortcutId: string, target: FieldMoveTarget): Promise<FieldCommitResult | null>;
  moveContext(contextId: string, point: FieldPoint): Promise<FieldCommitResult | null>;
  beginMove(shortcutId: string): void;
  previewMove(target: FieldMoveTarget): void;
  commitMove(): Promise<FieldCommitResult | null>;
  undo(): Promise<FieldCommitResult | null>;
  cancel(): void;
  hasUnsavedChanges(): boolean;
  flush(): Promise<void>;
  snapshot(): FieldDocument;
  destroy(): void;
}

const same = (a: unknown, b: unknown): boolean => JSON.stringify(a) === JSON.stringify(b);
const localImage = (value: string | null | undefined): string | null => {
  if (!value) return null;
  try { const url = new URL(value, document.baseURI); return /^https?:$/.test(url.protocol) && url.origin === location.origin ? url.href : null; } catch { return null; }
};
const SVG_NS = 'http://www.w3.org/2000/svg';
function installHexClip(): void {
  if (document.getElementById('uf-hex-defs')) return;
  const svg = document.createElementNS(SVG_NS, 'svg'); svg.id = 'uf-hex-defs'; svg.setAttribute('aria-hidden', 'true');
  svg.setAttribute('width', '0'); svg.setAttribute('height', '0'); svg.style.position = 'absolute'; svg.style.pointerEvents = 'none';
  const definitions = document.createElementNS(SVG_NS, 'defs'), clip = document.createElementNS(SVG_NS, 'clipPath');
  clip.id = 'uf-hex-clip'; clip.setAttribute('clipPathUnits', 'objectBoundingBox');
  const path = document.createElementNS(SVG_NS, 'path'); path.setAttribute('d', roundedHexPath(1, .12));
  path.setAttribute('transform', `matrix(0 .5 ${-1 / SQRT3} 0 .5 .5)`); clip.append(path); definitions.append(clip); svg.append(definitions); document.body.append(svg);
}
/** A structural DTO may have extra properties. Do not put source records into DOM signatures. */
function thinEntities(values: readonly FieldDirectoryEntity[]): FieldDirectoryEntity[] {
  return values.flatMap(value => {
    try {
      const entity = validateFieldEntity(value.entity);
      if (typeof value.title !== 'string' || !value.title.trim()) return [];
      const result: FieldDirectoryEntity = { entity, title: value.title.slice(0, 240) };
      for (const key of ['description', 'symbol', 'color', 'avatarUrl', 'coverKey'] as const) {
        const text = value[key]; if (typeof text === 'string') result[key] = text.slice(0, key === 'avatarUrl' ? 150_000 : key === 'description' ? 1600 : 128);
      }
      if (value.source && ['public', 'member', 'owner', 'builtin'].includes(value.source)) result.source = value.source;
      if (typeof value.hidden === 'boolean') result.hidden = value.hidden;
      if (Number.isFinite(value.updatedAt)) result.updatedAt = value.updatedAt!;
      if (entity.kind === 'person' && typeof value.avatarRevision === 'number' && Number.isSafeInteger(value.avatarRevision) && value.avatarRevision > 0) result.avatarRevision = value.avatarRevision;
      if (entity.kind === 'person' && (value.personAction === 'chat' || value.personAction === 'profile')) result.personAction = value.personAction;
      if (Number.isSafeInteger(value.unreadCount) && value.unreadCount! > 0) result.unreadCount = value.unreadCount!;
      if (entity.kind === 'device' && typeof value.online === 'boolean') result.online = value.online;
      const presentation=validateFieldPresentation(value.presentation);if(presentation)result.presentation=presentation;
      return [result];
    } catch { return []; }
  });
}
function hexFrame(radius: number): SVGSVGElement {
  const frame = document.createElementNS(SVG_NS, 'svg'); frame.classList.add('uf-hex-frame');
  frame.setAttribute('viewBox', `0 0 ${SQRT3 * radius} ${radius * 2}`); frame.setAttribute('aria-hidden', 'true');
  const path = document.createElementNS(SVG_NS, 'path'); path.setAttribute('d', roundedHexPath(radius - 1.5, radius * .12));
  path.setAttribute('transform', `translate(${SQRT3 * radius / 2} ${radius}) rotate(90)`); frame.append(path); return frame;
}

/** Only labels move in screen space; the user's world positions and camera stay intact. */
function compactOverviewTitles(contexts: UnifiedFieldLayout['contexts'], nodes: readonly FieldNode[], camera: FieldCamera,
  view: { width: number; height: number }, contourPadding: number): Map<string, { x: number; y: number; width: number }> {
  const height = 44, gap = 8;
  const projectX = (x: number): number => (x - camera.x) * camera.scale + view.width / 2;
  const projectY = (y: number): number => (y - camera.y) * camera.scale + view.height / 2;
  const clamp = (value: number, size: number, extent: number): number => extent < size + 16 ? extent / 2
    : Math.max(size / 2 + 8, Math.min(extent - size / 2 - 8, value));
  type Rect = { left: number; right: number; top: number; bottom: number };
  const occupied: Rect[] = nodes.map(node => {
    const w = Math.max(44, node.width * camera.scale);
    const h = Math.max(44, node.height * camera.scale);
    const x = projectX(node.cx), y = projectY(node.y + node.height / 2);
    return { left: x - w / 2 - gap, right: x + w / 2 + gap, top: y - h / 2 - gap, bottom: y + h / 2 + gap };
  });
  const placed: Rect[] = [], result = new Map<string, { x: number; y: number; width: number }>();
  const bodies = contexts.map(context => ({ contextId: context.contextId, left: projectX(context.body.left), right: projectX(context.body.right), top: projectY(context.body.top), bottom: projectY(context.body.bottom) }));
  const distance = (point: { x: number; y: number }, box: Rect): number => Math.hypot(Math.max(box.left - point.x, 0, point.x - box.right), Math.max(box.top - point.y, 0, point.y - box.bottom));
  const overlap = (a: Rect, b: Rect): number => Math.max(0, Math.min(a.right, b.right) - Math.max(a.left, b.left))
    * Math.max(0, Math.min(a.bottom, b.bottom) - Math.max(a.top, b.top));
  for (const context of contexts) {
    const width = Math.min(view.width - 16, Math.min(160, Math.max(112, context.title.length * 7.5 + 56)));
    const x = clamp(projectX(context.header.x + context.header.width / 2), width, view.width);
    const above = projectY(context.body.top + contourPadding) - height / 2 - 12;
    const below = projectY(context.body.bottom - contourPadding) + height / 2 + 12;
    let best = { x, y: clamp(above, height, view.height), width }, bestScore = Infinity;
    const ownBody = bodies.find(body => body.contextId === context.contextId)!;
    // Thirty local candidates, never a new grid. A label must prefer its own contour.
    // Coincident spaces can remain ambiguous; the existing selector is the fallback.
    for (let step = 0; step < 3; step++) for (const candidate of [above - step * (height + gap), below + step * (height + gap)]) for (const offset of [0, 24, -24, 48, -48]) {
      const cx = clamp(x + offset, width, view.width), y = clamp(candidate, height, view.height), point = { x: cx, y };
      const rect = { left: cx - width / 2, right: cx + width / 2, top: y - height / 2, bottom: y + height / 2 };
      const ownDistance = distance(point, ownBody), otherDistance = Math.min(...bodies.filter(body => body !== ownBody).map(body => distance(point, body)));
      const association = Math.max(0, ownDistance - otherDistance);
      const area = occupied.reduce((sum, other) => sum + overlap(rect, other), 0)
        + placed.reduce((sum, other) => sum + overlap(rect, other) * 2, 0);
      const score = (association > .01 ? 1e9 + association * 10000 : 0) + area + Math.hypot(cx - x, y - above) * .001;
      if (score < bestScore) { best = { x: cx, y, width }; bestScore = score; }
    }
    placed.push({ left: best.x - width / 2 - gap, right: best.x + width / 2 + gap, top: best.y - height / 2 - gap, bottom: best.y + height / 2 + gap });
    result.set(context.contextId, best);
  }
  return result;
}

/** One camera over immutable world coordinates. No server or capability mutation belongs here. */
export function createUnifiedField(options: UnifiedFieldOptions): UnifiedField {
  const accountId = options.accountId, metrics = { ...UNIFIED_FIELD_METRICS, ...options.metrics };
  installHexClip();
  const viewport = el('div', 'uf-viewport'); viewport.setAttribute('role', 'region'); viewport.setAttribute('aria-label', 'Поле Сот');
  viewport.style.setProperty('--uf-hex-clip', 'url(#uf-hex-clip)');
  const plane = el('div', 'uf-plane'); plane.setAttribute('role', 'group');
  plane.setAttribute('aria-label', 'Стрелки выбирают объект. Enter открывает. M перемещает, Escape отменяет. Space и перетаскивание двигают поле.');
  const live = el('div', 'uf-live'); live.setAttribute('aria-live', 'polite'); live.setAttribute('role', 'status');
  const tools = el('div', 'uf-move-tools'); tools.hidden = true;
  tools.setAttribute('role', 'group'); tools.setAttribute('aria-label', 'Перенос ярлыка');
  const moveHint = el('p', 'uf-move-hint', 'Выберите место, затем перенесите'); moveHint.setAttribute('aria-hidden', 'true');
  const contextSelect = el('select', 'uf-move-context'); contextSelect.setAttribute('aria-label', 'Переместить в пространство');
  const confirm = el('button', 'uf-move-confirm'); confirm.type = 'button'; confirm.setAttribute('aria-label', 'Переместить сюда');
  const confirmFull = el('span', 'uf-full-label', 'Переместить сюда'), confirmShort = el('span', 'uf-compact-label', 'Перенести');
  confirm.append(confirmFull, confirmShort);
  const cancelButton = el('button', 'uf-move-cancel'); cancelButton.type = 'button'; cancelButton.setAttribute('aria-label', 'Отменить перенос');
  cancelButton.append(icon('close'), el('span', 'uf-full-label', 'Отменить перенос'));
  tools.append(moveHint, contextSelect, confirm, cancelButton); viewport.append(plane, tools, live);
  const controller = new AbortController(), signal = controller.signal;
  const focusBar=el('div','uf-object-focus-bar'),focusBack=el('button','uf-object-focus-back'),focusTitle=el('strong'),focusControls=el('div','uf-object-focus-controls'),focusSettings=el('button','uf-object-focus-settings');
  focusBar.hidden=true;focusBack.type='button';focusBack.dataset.fieldControl='';focusBack.setAttribute('aria-label','Вернуться к полотну');
  focusSettings.type='button';focusSettings.dataset.fieldControl='';focusSettings.setAttribute('aria-label','Настроить объект на полотне');focusSettings.title='Настроить на полотне';focusSettings.append(icon('sliders'));focusSettings.hidden=!options.onInspect;
  focusBack.append(icon('back'),el('span','','Полотно'));focusBar.append(focusBack,focusTitle,focusControls,focusSettings);
  let activeFocusControls:{element:HTMLElement;parent:Node;next:ChildNode|null}|null=null;
  const releaseFocusControls=()=>{
    const previous=activeFocusControls;activeFocusControls=null;if(!previous)return;
    if(previous.parent.isConnected)previous.parent.insertBefore(previous.element,previous.next?.parentNode===previous.parent?previous.next:null);
    else previous.element.remove();
  };
  const useFocusControls=(element?:HTMLElement)=>{
    if(activeFocusControls?.element===element)return;releaseFocusControls();
    if(element?.parentNode){activeFocusControls={element,parent:element.parentNode,next:element.nextSibling};focusControls.append(element);}
  };
  const focusBackdrop=el('button','uf-object-focus-backdrop');focusBackdrop.type='button';focusBackdrop.dataset.fieldControl='';focusBackdrop.tabIndex=-1;focusBackdrop.hidden=true;focusBackdrop.setAttribute('aria-hidden','true');
  // Keep the provider in its original connected subtree: moving an iframe in DOM can reload it.
  if(options.readableObjectFocus){viewport.append(focusBar);plane.append(focusBackdrop);}
  let resolveReady!: () => void, readySettled = false;
  const ready = new Promise<void>(resolve => { resolveReady = resolve; });
  const viewState = options.viewState ?? {}, cameras: Record<UnifiedFieldMode, FieldCamera> = {
    mine: normalizeFieldCamera(viewState.mine), search: normalizeFieldCamera(viewState.search) };
  const contextFocus: Record<UnifiedFieldMode, string> = { mine: viewState.mineContext ?? '', search: viewState.searchContext ?? '' };
  const initialized: Record<UnifiedFieldMode, boolean> = { mine: !!viewState.mine, search: !!viewState.search };
  const cameraFit: Record<UnifiedFieldMode, CameraFit> = {
    mine: viewState.mineFit ?? (viewState.mineContext ? 'context' : viewState.mine ? 'manual' : 'overview'),
    search: viewState.searchFit ?? (viewState.searchContext ? 'context' : viewState.search ? 'manual' : 'overview'),
  };
  let fieldDocument = validateFieldDocument(options.document ?? createFieldDocument()), revision = options.revision ?? 0;
  let liveScene: FieldScene = {contexts:[],shortcuts:[]}, liveLayout = layoutUnifiedField(liveScene,[],metrics);
  let entities = thinEntities(options.entities ?? []), mode = options.mode ?? 'mine', scope = viewState.searchScope ?? '', searchDocument: FieldScene = { contexts: [], shortcuts: [] };
  let layout = layoutUnifiedField(fieldDocument, entities, metrics), index = createFieldSpatialIndex(layout.nodes);
  const selections: Record<UnifiedFieldMode, string> = { mine: viewState.mineSelection ?? '', search: viewState.searchSelection ?? '' };
  let selectedId = selections[mode], arranging = false, destroyed = false, raf = 0, suppressClickUntil = 0, spacePanning = false;
  let filter = '', visibleKinds: readonly FieldEntityKind[] | undefined;
  let persistence: UnifiedFieldSummary['persistence'] = 'saved', localDurable = true, message = '', generation = 0;
  let tail: Promise<unknown> = Promise.resolve(), lastIntent: { document: FieldDocument; expectedRevision: number; requestId: string } | null = null;
  type FieldTarget=HTMLElement&{disabled:boolean};
  const history = createFieldHistory(30), nodeElements = new Map<string, FieldTarget>(), contextElements = new Map<string, HTMLElement>(), inspectElements = new Map<string, HTMLButtonElement>();
  const objectViews=new Map<string,{key:string;view:FieldObjectView}>(),resizeElements=new Map<string,HTMLButtonElement>();
  const objectFocusButtons=new Map<string,HTMLButtonElement>();
  let objectFocus:{shortcutId:string;key:string;selection:string}|null=null;
  let exposedDocument: FieldDocument = structuredClone(fieldDocument), exposedSource = fieldDocument;
  let moving: { shortcutId: string; preview: FieldMovePreview; start: FieldDocument } | null = null;
  type Gesture = { pointerId: number; pointerType: string; forcePan: boolean; x: number; y: number; camera: FieldCamera; type: 'press' | 'pan' | 'move' | 'resize' | 'context' | 'inspect'; shortcutId: string; contextId: string; moved: boolean; point: FieldPoint; nodePoint?: FieldPoint; nodeSize?:{width:number;height:number};nodePresentation?:FieldPresentation;dragPoint?: FieldPoint; timer?: ReturnType<typeof setTimeout> };
  let gesture: Gesture | null = null, contextPreview: { document: FieldDocument; layout: UnifiedFieldLayout; valid: boolean; point: FieldPoint; contextId: string } | null = null;
  const pointers = new Map<number, FieldPoint>();
  type Pinch = { distance: number; world: FieldPoint; camera: FieldCamera };
  let pinch: Pinch | null = null;
  let lastInspection: { shortcutId: string; until: number } | null = null;
  let pendingPointer: { gesture: Gesture; point: FieldPoint } | { pinch: Pinch } | null = null, inPointerFrame = false;
  const viewportSize = () => ({ width: viewport.clientWidth || 900, height: viewport.clientHeight || 650 });
  const camera = () => cameras[mode];
  const scene = (): FieldScene => mode === 'mine' ? combineFieldScene(fieldDocument,liveScene) : searchDocument;
  const activeLayout = (): UnifiedFieldLayout => {
    const preview=contextPreview?.layout??moving?.preview.layout;
    return preview&&mode==='mine'?combineFieldLayout(preview,liveLayout):preview??layout;
  };

  function summary(): UnifiedFieldSummary {
    if (exposedSource !== fieldDocument) { exposedSource = fieldDocument; exposedDocument = structuredClone(fieldDocument); }
    return { mode, camera: { ...camera() }, focusContextId: contextFocus[mode], selectedShortcutId: selectedId, arranging,
      persistence, localDurable, revision, document: exposedDocument, canUndo: !!history.entries.length, message };
  }
  function emit(): void { if (!destroyed) options.onStateChange?.(summary()); }
  function announce(value: string): void { message = value; live.textContent = value; emit(); }
  function remember(): void {
    viewState[mode] = { ...camera() };
    selections[mode] = selectedId;
    if (mode === 'mine') { viewState.mineContext = contextFocus.mine; viewState.mineSelection = selectedId; viewState.mineFit = cameraFit.mine; }
    else { viewState.searchContext = contextFocus.search; viewState.searchSelection = selectedId; viewState.searchFit = cameraFit.search; viewState.searchScope = scope; }
    options.onViewStateChange?.(viewState);
  }
  function setCamera(next: FieldCamera, fit: CameraFit = 'manual'): void { if (destroyed) return; cameraFit[mode] = fit; cameras[mode] = normalizeFieldCamera(next); initialized[mode] = true; remember(); schedule(); emit(); }
  function fitOverview(): void {
    const view = viewportSize();
    contextFocus[mode] = ''; setCamera(fitFieldCamera(layout.bounds, view, { padding: 60, maxScale: view.width >= 720 ? 1.15 : 1 }), 'overview');
  }
  function focusContext(contextId: string): void {
    const context = layout.contexts.find(item => item.contextId === contextId); if (!context) return;
    const view = viewportSize(), compact = view.width < 720;
    const members = layout.nodes.filter(node => node.contextId === contextId);
    const bounds = compact && members.length ? fieldBounds(members.flatMap(node => node.footprint), 16) : context.bounds;
    const readableWidth = Math.min(144, view.width * .37, Math.max(1, view.height - 48) * Math.sqrt(3) / 2);
    const minScale = compact ? options.readableMobileFocus && members.length ? Math.max(.2, readableWidth / (Math.sqrt(3) * metrics.radius)) : .2 : .45;
    const fitted = fitFieldCamera(bounds, view, { padding: compact ? 24 : 44, maxScale: compact ? 1 : 1.15, minScale });
    if (compact && options.readableMobileFocus && members.length && !members.some(node =>
      Math.abs((node.cx - fitted.x) * fitted.scale) + node.width * fitted.scale / 2 <= view.width / 2 - 24
      && Math.abs((node.cy - fitted.y) * fitted.scale) + node.height * fitted.scale / 2 <= view.height / 2 - 24)) {
      const anchor = members.find(node => node.shortcutId === selectedId) ?? members.reduce((nearest, node) =>
        (node.cx - fitted.x) ** 2 + (node.cy - fitted.y) ** 2 < (nearest.cx - fitted.x) ** 2 + (nearest.cy - fitted.y) ** 2 ? node : nearest);
      fitted.x = anchor.cx; fitted.y = anchor.cy;
    }
    contextFocus[mode] = contextId; setCamera(fitted, 'context');
  }
  function revealShortcut(shortcutId:string):void {
    const node=activeNode(shortcutId);if(!node||destroyed)return;
    selectedId=shortcutId;contextFocus[mode]=node.contextId;
    const view=viewportSize(),scale=Math.max(.08,Math.min(camera().scale,Math.max(80,view.width-64)/node.width,Math.max(80,view.height-160)/node.height));
    setCamera({x:node.cx,y:node.cy,scale});
  }
  function rebuild(): void {
    const priorFocus = document.activeElement instanceof HTMLElement && plane.contains(document.activeElement) ? document.activeElement : null;
    layout = layoutUnifiedField(scene(), entities, metrics);
    liveLayout=layoutUnifiedField(liveScene,entities,metrics);
    if (!options.dimNonMatches && mode === 'mine' && (filter || visibleKinds?.length)) {
      const query = filter.trim().toLocaleLowerCase('ru'), matchingContexts = new Set(layout.contexts.filter(context => context.title.toLocaleLowerCase('ru').includes(query)).map(context => context.contextId));
      layout.nodes = layout.nodes.filter(node => (!visibleKinds?.length || visibleKinds.includes(node.kind) || node.kind === 'builtin' && visibleKinds.includes('app'))
        && (!query || matchingContexts.has(node.contextId) || `${node.entity.title} ${node.entity.description ?? ''}`.toLocaleLowerCase('ru').includes(query)));
      const kept = new Set(layout.nodes.map(node => node.shortcutId));
      layout.contexts = layout.contexts.filter(context => !query || context.children.some(id => kept.has(id)) || matchingContexts.has(context.contextId))
        .map(context => ({ ...context, children: context.children.filter(id => kept.has(id)) }));
    }
    index = createFieldSpatialIndex(layout.nodes);
    if (!layout.nodes.some(node => node.shortcutId === selectedId && (!options.dimNonMatches || matchesMineFilter(node)))) selectedId = '';
    if (contextFocus[mode] && !layout.contexts.some(context => context.contextId === contextFocus[mode])) {
      contextFocus[mode] = '';
      remember();
    }
    if (!initialized[mode] && viewport.clientWidth && viewport.clientHeight) {
      if (contextFocus[mode]) focusContext(contextFocus[mode]);
      else if (viewport.clientWidth < 720 && layout.contexts[0]) focusContext(layout.contexts[0].contextId);
      else fitOverview();
    }
    render();
    if (priorFocus && !priorFocus.isConnected) (plane.querySelector<HTMLButtonElement>('.uf-node:not(:disabled)[tabindex="0"]') ?? plane.querySelector<HTMLButtonElement>('.uf-context-title'))?.focus({ preventScroll: true });
  }
  function schedule(): void { if (!raf && !destroyed && !inPointerFrame) raf = requestAnimationFrame(() => { raf = 0; flushPointerSample(); render(); }); }
  function matchesMineFilter(node: FieldNode): boolean {
    if (mode !== 'mine') return true;
    if (visibleKinds?.length && !visibleKinds.includes(node.kind) && !(node.kind === 'builtin' && visibleKinds.includes('app'))) return false;
    const query = filter.trim().toLocaleLowerCase('ru');
    return !query || `${node.entity.title} ${node.entity.description ?? ''}`.toLocaleLowerCase('ru').includes(query)
      || Boolean(layout.contexts.find(context => context.contextId === node.contextId)?.title.toLocaleLowerCase('ru').includes(query));
  }
  function available(shortcutId: string): FieldNode | undefined { return layout.nodes.find(node => node.shortcutId === shortcutId && (!options.dimNonMatches || matchesMineFilter(node))); }
  function activeNode(shortcutId: string): FieldNode | undefined { return activeLayout().nodes.find(node => node.shortcutId === shortcutId && (!options.dimNonMatches || matchesMineFilter(node))); }

  function leaveObjectFocus(restoreFocus=true):void {
    if(!objectFocus)return;
    const previous=objectFocus;objectFocus=null;
    selectedId=available(previous.selection)?previous.selection:'';
    remember();render();emit();
    options.onObjectFocusChange?.(null);
    if(restoreFocus)objectFocusButtons.get(previous.shortcutId)?.focus({preventScroll:true});
  }
  function enterObjectFocus(shortcutId:string):void {
    if(!options.readableObjectFocus||destroyed||arranging||moving||gesture?.moved)return;
    const entry=objectViews.get(shortcutId),node=available(shortcutId);
    if(!entry||!node||options.objectKey&&options.objectKey(node)!==entry.key)return;
    if(objectFocus?.shortcutId===shortcutId)return;
    resetPointers();objectFocus={shortcutId,key:entry.key,selection:selectedId};selectedId=shortcutId;
    render();emit();focusBack.focus({preventScroll:true});
    options.onObjectFocusChange?.(node.entity);
  }
  focusBack.addEventListener('click',()=>leaveObjectFocus(),{signal});
  focusSettings.addEventListener('click',()=>{if(objectFocus)inspectShortcut(objectFocus.shortcutId);},{signal});
  focusBackdrop.addEventListener('click',()=>leaveObjectFocus(),{signal});
  if(options.readableObjectFocus)viewport.addEventListener('keydown',event=>{
    if(event.key==='Escape'&&!event.isComposing&&objectFocus&&!event.defaultPrevented){event.preventDefault();event.stopPropagation();leaveObjectFocus();}
  },{capture:true,signal});

  function renderNode(target: HTMLElement, node: FieldNode): void {
    const entity = node.entity, kind = entity.entity.kind;
    target.dataset.entityId = fieldEntityKey(entity.entity);
    target.dataset.fieldGlassNode='hex';
    target.className = `uf-node uf-node-${kind}`; target.replaceChildren();
    if (kind === 'person') {
      const face = el('span', 'uf-avatar'), src = safeImageUrl(entity.avatarUrl) ?? localImage(entity.avatarUrl);
      if (src) { const image = el('img'); image.src = src; image.alt = ''; image.decoding = 'async'; image.loading = 'lazy'; image.referrerPolicy = 'no-referrer';
        image.addEventListener('error', () => face.replaceChildren(el('span', '', initials(entity.title))), { once: true }); face.append(image); }
      else face.append(el('span', '', initials(entity.title)));
      if (!src && entity.avatarRevision && entity.avatarRevision > 0) {
        face.classList.add('sw-avatar'); face.dataset.profileId = entity.entity.id; face.dataset.avatarRevision = String(entity.avatarRevision); face.dataset.avatarLabel = initials(entity.title);
      }
      const label = el('span', 'uf-label uf-person-label');
      label.append(el('span', 'uf-label-text', entity.title), el('span', 'uf-person-action', entity.personAction === 'chat' ? 'Личный чат' : 'Профиль'));
      target.append(face, label, hexFrame(metrics.radius));
    } else if (kind === 'device') {
      const inner = el('span', 'uf-device-inner'), label = el('span', 'uf-label'); label.append(el('span', 'uf-label-text', entity.title)); inner.append(icon(entity.symbol || 'laptop', 'uf-device-icon'), label);
      // An online signal without a recent observation is not a live presence claim.
      if (typeof entity.updatedAt === 'number' && Date.now() - entity.updatedAt >= 0 && Date.now() - entity.updatedAt <= 45_000 && typeof entity.online === 'boolean') {
        const status = el('span', `uf-status ${entity.online ? 'is-online' : 'is-offline'}`); status.setAttribute('aria-label', entity.online ? 'Устройство в сети' : 'Устройство не в сети'); inner.append(status);
      }
      target.append(inner, hexFrame(metrics.radius));
      const hit = el('span', 'uf-hit-shape'); hit.setAttribute('aria-hidden', 'true'); target.append(hit);
    } else {
      let art: FieldVisualArt | null = null; try { art = options.resolveArt?.(entity) ?? null; } catch { /* Icon fallback remains usable. */ }
      const fallback = resolveAppArt({ appId: entity.entity.id });
      for (const name of ['base', 'accent', 'ink'] as const) {
        const value = art?.palette?.[name];
        target.style.setProperty(`--uf-art-${name}`, /^#(?:[a-f0-9]{3}|[a-f0-9]{6})$/i.test(value ?? '') ? value! : fallback.palette[name]);
      }
      target.dataset.artState = 'fallback';
      const symbol = art?.icon || entity.symbol || (kind === 'community' ? 'people' : 'cells');
      if (kind !== 'community') target.append(icon(symbol, 'uf-fallback-mark'));
      const src = localImage(art?.src);
      if (src) {
        const image = el('img', 'uf-art'); image.alt = ''; image.decoding = 'async'; image.loading = 'lazy'; image.referrerPolicy = 'no-referrer';
        const loaded = (): void => { if (image.parentElement !== target) return; target.classList.add('has-art'); target.dataset.artState = 'ready'; };
        const failed = (): void => { if (image.parentElement !== target) return; image.remove(); target.classList.remove('has-art'); target.dataset.artState = 'fallback'; };
        image.addEventListener('load', loaded, { once: true }); image.addEventListener('error', failed, { once: true });
        const srcset = art!.srcset.split(',').map(part => part.trim()).filter(Boolean);
        if (srcset.length && srcset.every(part => { const [path, width] = part.split(/\s+/u); return !!localImage(path) && /^\d+w$/.test(width ?? ''); })) image.srcset = art!.srcset;
        image.sizes = art!.sizes;
        const focal = art!.focalPoint; image.style.objectPosition = `${Math.max(0, Math.min(1, focal.x)) * 100}% ${Math.max(0, Math.min(1, focal.y)) * 100}%`;
        target.style.setProperty('--uf-focal-position', image.style.objectPosition);
        target.append(image); image.src = src;
        if (image.complete) queueMicrotask(() => image.naturalWidth > 0 ? loaded() : failed());
      }
      target.append(hexFrame(metrics.radius));
      const caption = el('span', 'uf-app-caption uf-label'); caption.append(icon(symbol, 'uf-node-icon'), el('span', 'uf-label-text', entity.title));
      target.append(caption);
      const hit = el('span', 'uf-hit-shape'); hit.setAttribute('aria-hidden', 'true'); target.append(hit);
    }
    if (entity.unreadCount && entity.unreadCount > 0) target.append(el('span', 'uf-unread', entity.unreadCount > 99 ? '99+' : String(entity.unreadCount)));
  }

  function render(): void {
    if (destroyed || inPointerFrame) return;
    const current = activeLayout(), view = viewportSize(), cam = camera();
    if(options.readableObjectFocus&&objectFocus){
      const node=activeNode(objectFocus.shortcutId),key=node?(options.objectKey?options.objectKey(node):fieldEntityKey(node.entity.entity)):null;
      if(!node||key!==objectFocus.key){
        if(node&&key&&options.canContinueObjectFocus?.(node,objectFocus.key,key)===true)objectFocus.key=key;
        else{objectFocus=null;options.onObjectFocusChange?.(null);}
      }
    }
    if(options.readableObjectFocus){
      viewport.dataset.objectFocus=String(!!objectFocus);focusBar.hidden=focusBackdrop.hidden=!objectFocus;
      if(objectFocus){
        focusTitle.textContent=activeNode(objectFocus.shortcutId)?.entity.title??'';
        focusBackdrop.style.left=`${cam.x-view.width/2/cam.scale}px`;focusBackdrop.style.top=`${cam.y-view.height/2/cam.scale}px`;
        focusBackdrop.style.width=`${view.width}px`;focusBackdrop.style.height=`${view.height}px`;focusBackdrop.style.transform=`scale(${1/cam.scale})`;
      }else {focusTitle.textContent='';releaseFocusControls();}
    }
    const naturalLevel = fieldLevelOfDetail(cam.scale, viewport.dataset.lod as 'overview' | 'context' | 'detail' | undefined);
    const level = arranging ? 'detail' : contextFocus[mode] && (view.width < 720 || naturalLevel === 'overview') ? 'context' : naturalLevel;
    viewport.dataset.mode = contextFocus[mode] ? 'focus' : 'overview'; viewport.dataset.side = mode; viewport.dataset.lod = level;
    viewport.dataset.persistence = persistence; viewport.classList.toggle('is-arranging', arranging); viewport.classList.toggle('is-moving', !!moving || !!contextPreview);
    viewport.dataset.panning=String(spacePanning||gesture?.type==='pan');
    viewport.dataset.dragging=String(gesture?.type==='move'||gesture?.type==='resize'||gesture?.type==='context');
    viewport.style.setProperty('--uf-scale', String(cam.scale)); viewport.style.setProperty('--uf-label-scale', String(1 / cam.scale));
    plane.style.transform = `translate(${view.width / 2 - cam.x * cam.scale}px,${view.height / 2 - cam.y * cam.scale}px) scale(${cam.scale})`;
    plane.style.transformOrigin = '0 0';
    const corners = [fieldScreenToWorld({ x: 0, y: 0 }, cam, view), fieldScreenToWorld({ x: view.width, y: view.height }, cam, view)];
    const bounds = { left: corners[0]!.x, top: corners[0]!.y, right: corners[1]!.x, bottom: corners[1]!.y };
    const mobileContext = view.width < 720 ? contextFocus[mode] : '';
    const visibleContexts = current.contexts.filter(context => (!mobileContext || context.contextId === mobileContext) && fieldBoundsOverlap(context.bounds, bounds, 120));
    const visible = (moving || contextPreview ? createFieldSpatialIndex(current.nodes) : index).query(bounds, 100);
    const retained = new Set(visible.map(node => node.shortcutId));
    if (selectedId) retained.add(selectedId); if (moving) retained.add(moving.shortcutId);
    if(options.readableObjectFocus&&objectFocus)retained.add(objectFocus.shortcutId);
    const protectedObjects=new Set<string>();
    if(options.createObject)for(const [id,entry]of objectViews)if(entry.view.element.contains(document.activeElement)||entry.view.hasUnsavedChanges()||entry.view.hasUnconfirmedWork?.()||entry.view.retainWhileHidden?.()){retained.add(id);protectedObjects.add(id);}
    const overviewIds = new Set(current.contexts.flatMap(context => options.dimNonMatches
      ? [...context.children.slice(0, 3), ...context.children.filter(id => available(id)).slice(0, 3)] : context.children.slice(0, 3)));
    const nodes = current.nodes.filter(node => (!mobileContext || node.contextId === mobileContext||protectedObjects.has(node.shortcutId)) && retained.has(node.shortcutId) && (level !== 'overview' || overviewIds.has(node.shortcutId) || node.shortcutId === selectedId||protectedObjects.has(node.shortcutId)||options.createObject&&(node.kind==='object'||objectViews.has(node.shortcutId)||options.objectKey?.(node)!=null)));
    const compactOverview = view.width < 720 && level === 'overview';
    viewport.dataset.compactOverview = String(compactOverview);
    const titlePlacements = compactOverview ? compactOverviewTitles(visibleContexts, nodes, cam, view, metrics.contourPadding) : null;
    const contextKeep = new Set(visibleContexts.map(context => context.contextId));
    for (const context of visibleContexts) {
      let wrapper = contextElements.get(context.contextId);
      if (!wrapper) {
        wrapper = el('div', 'uf-context'); wrapper.dataset.contextId = context.contextId;
        const svg = document.createElementNS(SVG_NS, 'svg'); svg.classList.add('uf-contour'); svg.setAttribute('aria-hidden', 'true'); svg.append(document.createElementNS(SVG_NS, 'path'));
        const title = el('button', 'uf-context-title'); title.type = 'button'; title.dataset.floatingSoft = ''; title.dataset.contextId = context.contextId;
        title.addEventListener('click', () => { if (mode === 'search') options.onContextActivate?.(context.contextId); else if (!arranging) focusContext(context.contextId); }, { signal });
        wrapper.append(svg, title); plane.append(wrapper); contextElements.set(context.contextId, wrapper);
      }
      wrapper.style.left = `${context.body.left}px`; wrapper.style.top = `${context.body.top}px`; wrapper.style.width = `${context.body.right - context.body.left}px`; wrapper.style.height = `${context.body.bottom - context.body.top}px`;
      const svg = wrapper.querySelector('svg')!; svg.setAttribute('viewBox', `${context.body.left} ${context.body.top} ${context.body.right - context.body.left} ${context.body.bottom - context.body.top}`);
      svg.querySelector('path')!.setAttribute('d', context.contour);
      const title = wrapper.querySelector<HTMLButtonElement>('.uf-context-title')!;
      if(options.readableObjectFocus)title.inert=!!objectFocus;
      const headerHeight = 44;
      const placement = titlePlacements?.get(context.contextId);
      const headerY = placement ? cam.y + (placement.y - view.height / 2) / cam.scale : context.body.top + metrics.contourPadding - (headerHeight / 2 + 12) / cam.scale;
      const headerX = placement ? cam.x + (placement.x - view.width / 2) / cam.scale : context.header.x + context.header.width / 2;
      title.style.left = `${headerX - context.body.left}px`; title.style.top = `${headerY - context.body.top}px`; title.style.width = `${context.header.width}px`; title.style.height = `${headerHeight}px`;
      const centre = { x: headerX, y: headerY };
      const nearest = visibleContexts.filter(other => {
        const otherHeaderY = other.body.top + metrics.contourPadding - (headerHeight / 2 + 12) / cam.scale;
        return other.contextId !== context.contextId && Math.abs((otherHeaderY - centre.y) * cam.scale) < 72;
      })
        .map(other => Math.abs((other.header.x + other.header.width / 2 - centre.x) * cam.scale));
      const screenX = (centre.x - cam.x) * cam.scale + view.width / 2;
      const edgeRoom = cameraFit[mode] === 'overview' ? 2 * Math.max(0, Math.min(screenX - 8, view.width - screenX - 8)) : view.width - 24;
      const titleWidth = placement?.width ?? Math.max(44, Math.min(view.width - 24, edgeRoom, metrics.contextTitleWidth, ...nearest.map(distance => distance - 14)));
      title.style.setProperty('--uf-title-screen-width', `${titleWidth}px`); title.classList.toggle('is-icon-only', !compactOverview && titleWidth < 112);
      const titleSignature = JSON.stringify([mode, context.title, context.children.length]);
      if (title.dataset.signature !== titleSignature) { const copy = el('span', 'uf-context-copy'); copy.append(el('strong', 'uf-context-name', context.title)); title.replaceChildren(icon(mode === 'mine' ? 'layers' : context.title === 'Приложения' ? 'grid' : 'people'), copy); title.dataset.signature = titleSignature; }
      title.setAttribute('aria-label', `${context.title}. ${nounCount(context.children.length, 'объект', 'объекта', 'объектов')}`);
      title.tabIndex = level === 'overview' ? 0 : -1;
    }
    for (const [id, element] of contextElements) if (!contextKeep.has(id)) { element.remove(); contextElements.delete(id); }
    const keep = new Set(nodes.map(node => node.shortcutId)), activeId = selectedId && keep.has(selectedId) ? selectedId : nodes.find(node => !options.dimNonMatches || matchesMineFilter(node))?.shortcutId;
    for (const node of nodes) {
      let target = nodeElements.get(node.shortcutId);
      const key=options.createObject?(options.objectKey?options.objectKey(node):fieldEntityKey(node.entity.entity)):null;
      const expectedObjectKey=typeof key==='string'&&key.length>0&&key.length<=1024?key:null;
      const previousObject=options.createObject?objectViews.get(node.shortcutId):undefined;
      const blockedObject=Boolean(options.objectKey&&expectedObjectKey===null&&(node.kind==='object'||previousObject||target?.dataset.nativeUnavailable==='true'));
      if(previousObject&&previousObject.key!==expectedObjectKey){
        objectViews.get(node.shortcutId)!.view.dispose();objectViews.delete(node.shortcutId);target?.remove();nodeElements.delete(node.shortcutId);target=undefined;
        if(options.readableObjectFocus)objectFocusButtons.delete(node.shortcutId);
      }
      let promoted:FieldObjectView|null=null;
      if(target&&options.createObject&&!objectViews.has(node.shortcutId)&&expectedObjectKey!==null){
        promoted=options.createObject(node);if(promoted){target.remove();nodeElements.delete(node.shortcutId);target=undefined;}
      }
      if (!target) {
        const view=promoted??(expectedObjectKey===null?null:options.createObject?.(node)??null);
        if(view)objectViews.set(node.shortcutId,{key:expectedObjectKey!,view});
        target = el(view?'article':'button', 'uf-node') as FieldTarget;if(!view)(target as HTMLButtonElement).type='button';
        target.dataset.floatingSoft = ''; target.dataset.shortcutId = node.shortcutId;
        if(view){target.classList.add('uf-native');const mask=el('div','uf-object-mask');mask.append(view.element);target.append(mask);if(view.overview){view.overview.classList.add('uf-object-overview');target.append(view.overview);}target.addEventListener('focusin',()=>{selectedId=node.shortcutId;remember();schedule();emit();},{signal});}
        target.addEventListener('focus', () => { selectedId = node.shortcutId; remember(); schedule(); emit(); }, { signal });
        target.addEventListener('click', event => {
          if((event.target as HTMLElement).closest('input,textarea,select,audio,video,iframe,[contenteditable],[data-field-control]'))return;
          const item = available(node.shortcutId); if (!item) return;
          if((event.currentTarget as HTMLElement).dataset.nativeUnavailable==='true'){options.onInspect?.({entity:item.entity.entity,title:'Объект недоступен'},item.shortcutId);return;}
          if ((event.currentTarget as HTMLButtonElement).dataset.preview === 'true') { focusPreview(item); return; }
          selectedId = item.shortcutId; remember();
          if (moving) previewMove(item.placement?.kind==='free'?{contextId:item.contextId,placement:{...item.placement}}:{ contextId: item.contextId, slot: [...item.slot], swap: true });
          else if (arranging) beginMove(item.shortcutId);
          else if(options.createObject&&objectViews.has(item.shortcutId))objectViews.get(item.shortcutId)!.view.activate?.();
          else options.onActivate?.(item.entity, item.shortcutId);
        }, { signal }); nodeElements.set(node.shortcutId, target); plane.append(target);
        target.addEventListener('contextmenu', event => { const current = available(node.shortcutId); if (!current) return;
          if (pinch || gesture && !['press', 'inspect'].includes(gesture.type) || performance.now() < suppressClickUntil) { event.preventDefault(); return; }
          if (lastInspection?.shortcutId === current.shortcutId && performance.now() < lastInspection.until) { event.preventDefault(); return; }
          if (!inspectShortcut(current.shortcutId)) return;
          event.preventDefault();
          if (gesture?.shortcutId === current.shortcutId && gesture.type === 'press') {
            if (gesture.timer) clearTimeout(gesture.timer); delete gesture.timer; gesture.moved = true; gesture.type = 'inspect';
            if (lastInspection) lastInspection.until = Infinity;
          } }, { signal });
      }
      target.dataset.nativeUnavailable=String(blockedObject);
      const displayEntity:FieldDirectoryEntity=blockedObject?{entity:node.entity.entity,title:'Объект недоступен',symbol:'lock'}:node.entity;
      const signature = JSON.stringify(displayEntity);
      const object=options.createObject?objectViews.get(node.shortcutId)?.view:undefined;
      const focused=options.readableObjectFocus===true&&objectFocus?.shortcutId===node.shortcutId;
      const mask=object?target.querySelector<HTMLElement>('.uf-object-mask'):null;
      let focusFrame:ReturnType<typeof fieldObjectFocusFrame>|null=null;
      if(focused&&mask){
        const rect=viewport.getBoundingClientRect(),visual=window.visualViewport;
        focusFrame=fieldObjectFocusFrame(view,visual?{left:Math.max(0,visual.offsetLeft-rect.left),top:Math.max(0,visual.offsetTop-rect.top),width:visual.width,height:visual.height}:undefined);
      }
      const presentation=node.presentation??{shape:'hex'};
      target.dataset.shape=presentation.shape;
      if(!blockedObject){target.dataset.fieldGlassNode=focusFrame?'rect':presentation.shape;if(presentation.shape==='polygon')target.dataset.fieldGlassPoints=JSON.stringify(presentation.points);else delete target.dataset.fieldGlassPoints;}
      else {delete target.dataset.fieldGlassNode;delete target.dataset.fieldGlassPoints;}
      const clip=presentation.shape==='hex'?'var(--uf-hex-clip)':fieldPresentationClip(presentation);
      if(mask)mask.style.clipPath=focusFrame?'none':clip;
      else if(presentation.shape!=='hex')target.style.setProperty('--uf-hex-clip',clip);
      else target.style.removeProperty?.('--uf-hex-clip');
      // Provider callbacks may synchronously force iframe layout. Publish only
      // the final reading geometry, never a temporary canonical placement.
      if(focusFrame){
        target.style.left=`${cam.x+(focusFrame.left-view.width/2)/cam.scale}px`;target.style.top=`${cam.y+(focusFrame.top-view.height/2)/cam.scale}px`;
        target.style.width=`${focusFrame.width}px`;target.style.height=`${focusFrame.height}px`;target.style.transform=`scale(${1/cam.scale})`;
      }else{
        const visual=gesture?.type==='move'&&gesture.shortcutId===node.shortcutId?gesture.dragPoint:undefined;
        target.style.left = `${visual?.x??node.x}px`; target.style.top = `${visual?.y??node.y}px`; target.style.width = `${node.width}px`; target.style.height = `${node.height}px`;
        if(options.readableObjectFocus)target.style.transform='';
      }
      if(options.readableObjectFocus)target.dataset.objectFocus=String(focused);
      if(object){if(!focused)object.update(node);}
      else if (target.dataset.signature !== signature) { renderNode(target,blockedObject?{...node,entity:displayEntity}:node); target.dataset.signature = signature; }
      target.dataset.layout=mode==='mine'&&liveScene.shortcuts.some(item=>item.shortcutId===node.shortcutId)?'live':'personal';
      const screenWidth = node.width * cam.scale, screenHeight = node.height * cam.scale;
      if(object?.setPresentation){
        // Camera LOD alone misses physically small windows and mobile context overrides.
        const glance=object.glanceSize?.()??{width:480,height:320};
        const detail=fieldLevelOfDetail(Math.min(cam.scale,screenWidth/glance.width,screenHeight/glance.height),target.dataset.objectLod as 'overview'|'context'|'detail'|undefined);
        target.dataset.objectLod=detail;
        const presentation=options.readableObjectFocus&&objectFocus?.shortcutId===node.shortcutId?'interactive':options.readableObjectFocus&&!arranging&&detail==='overview'?'overview':'glance';
        target.dataset.objectPresentation=presentation;object.setPresentation(presentation);
      }else delete target.dataset.objectPresentation;
      object?.setVisible(visible.some(value=>value.shortcutId===node.shortcutId)||options.readableObjectFocus===true&&objectFocus?.shortcutId===node.shortcutId);
      // A saved or manually zoomed-out context can be just as small as the
      // overview. Its thumbnails must not grow full-size captions/actions.
      const preview = !arranging && Math.min(screenWidth, screenHeight) < 44;
      target.dataset.preview = String(preview);
      target.style.setProperty('--uf-node-screen-width', `${screenWidth}px`); target.style.setProperty('--uf-node-screen-height', `${screenHeight}px`);
      const centreX = (node.cx - cam.x) * cam.scale + view.width / 2;
      const halfCaption = screenWidth * (node.kind === 'device' ? .4 : .32);
      const readableCaption = options.readableMobileFocus && view.width < 720;
      const captionLeft = Math.max(8, centreX - halfCaption), captionRight = Math.min(view.width - 8, centreX + halfCaption);
      target.style.setProperty('--uf-caption-width', `${readableCaption ? Math.max(0, captionRight - captionLeft) : screenWidth * .82}px`);
      target.style.setProperty('--uf-caption-offset', `${readableCaption ? ((captionLeft + captionRight) / 2 - centreX) / cam.scale : 0}px`);
      const caption = screenWidth < 135 ? 'compact' : 'full'; if (target.dataset.caption !== caption) target.dataset.caption = caption;
      // Search and overview remain navigable. Read-only describes layout changes, never inspection/opening.
      target.disabled = Boolean(options.dimNonMatches && !matchesMineFilter(node));
      target.dataset.muted = String(target.disabled); target.setAttribute('aria-hidden', String(target.disabled));
      target.dataset.interactive = String(!target.disabled);
      if(object)target.inert=target.disabled;
      if (options.onInspect && mode === 'mine' && !target.disabled && !preview) {
        let action = inspectElements.get(node.shortcutId);
        if (!action) { action = el('button', 'uf-inspect'); action.type = 'button'; action.dataset.fieldControl = ''; action.dataset.floatingSoft = ''; action.dataset.shortcutId = node.shortcutId; action.append(icon('more'));
          action.addEventListener('click', () => { inspectShortcut(node.shortcutId); }, { signal }); target.after(action); inspectElements.set(node.shortcutId, action); }
        const left = node.x + node.width;
        const top = node.y;
        if (options.readableMobileFocus && view.width < 720) {
          // Native content owns its whole interior: the old hex attachment
          // landed on note text and application controls. Keep its fixed-size
          // action just outside the upper corner, bounded by the viewport.
          const point = fieldScreenToWorld({ x: Math.max(30, Math.min(view.width - 30, centreX + screenWidth * (object ? .5 : .25))),
            y: object ? Math.max(30, Math.min(view.height - 30, (node.y - cam.y) * cam.scale + view.height / 2 - 8))
              : (node.cy - cam.y) * cam.scale + view.height / 2 - screenHeight * .18 }, cam, view);
          action.style.left = `${point.x - 22}px`; action.style.top = `${point.y - 22}px`;
        } else { action.style.left = `${left - 22 / cam.scale}px`; action.style.top = `${top + Math.min(node.height * .2, 32) - 22 / cam.scale}px`; }
        action.style.setProperty('--uf-node-screen-width', `${screenWidth}px`); action.style.setProperty('--uf-label-scale', String(1 / cam.scale));
        action.setAttribute('aria-label', `Действия: ${displayEntity.title}`); action.tabIndex = node.shortcutId === activeId ? 0 : -1; action.classList.toggle('is-selected', node.shortcutId === selectedId);
      } else { inspectElements.get(node.shortcutId)?.remove(); inspectElements.delete(node.shortcutId); }
      target.classList.toggle('is-selected', node.shortcutId === selectedId); target.classList.toggle('is-moving', node.shortcutId === moving?.shortcutId);
      target.setAttribute('aria-label', preview ? contextFocus[mode] === node.contextId ? `Приблизить: ${displayEntity.title}`
        : `Открыть пространство ${current.contexts.find(context => context.contextId === node.contextId)?.title ?? ''}` : `${displayEntity.title}${node.kind === 'person' ? node.entity.personAction === 'chat' ? '. Личный чат' : '. Профиль' : ''}${displayEntity.description ? `. ${displayEntity.description}` : ''}`);
      target.tabIndex = target.disabled || level === 'overview' && !arranging ? -1 : node.shortcutId === activeId ? 0 : -1;
      if(options.readableObjectFocus){
        if(!object?.setPresentation)object?.setFocused?.(focused);
        target.inert=target.disabled||!!objectFocus&&!focused;
        if(object&&mask){
          const unreadable=!focused&&!arranging&&(target.dataset.objectPresentation==='overview'||object.interaction==='focus'||fieldObjectNeedsFocus(cam.scale,target.dataset.needsFocus==='true'));
          target.dataset.focusInteraction=String(object.interaction==='focus');
          if(object.interaction==='focus'){inspectElements.get(node.shortcutId)?.remove();inspectElements.delete(node.shortcutId);}
          target.dataset.needsFocus=String(unreadable);mask.inert=unreadable;
          let expand=objectFocusButtons.get(node.shortcutId);
          if(!expand){
            expand=el('button','uf-object-expand');expand.type='button';expand.append(icon('expand'));
            expand.addEventListener('click',event=>{event.stopPropagation();if(event.detail>0&&performance.now()<suppressClickUntil||pinch||gesture?.moved)return;const item=available(node.shortcutId);if(!item)return;const target=nodeElements.get(node.shortcutId);if(target?.dataset.preview==='true'&&!target.dataset.objectPresentation)focusPreview(item);else enterObjectFocus(node.shortcutId);},{signal});
            target.append(expand);objectFocusButtons.set(node.shortcutId,expand);
          }
          expand.setAttribute('aria-label',`Развернуть: ${displayEntity.title}`);expand.hidden=focused||arranging||!!objectFocus;
          expand.tabIndex=preview&&!object.setPresentation||node.shortcutId!==activeId?-1:0;
          expand.style.setProperty('--uf-label-scale',String(1/cam.scale));
          if(focusFrame){
            useFocusControls(object.focusControls);
            object.update({...node,width:focusFrame.width,height:focusFrame.height,presentation:{shape:'rect'}});object.setVisible(true);
            focusBar.style.left=`${focusFrame.left}px`;focusBar.style.top=`${focusFrame.barTop}px`;focusBar.style.width=`${focusFrame.width}px`;
          }
        }else {objectFocusButtons.get(node.shortcutId)?.remove();objectFocusButtons.delete(node.shortcutId);delete target.dataset.needsFocus;}
        if(objectFocus){inspectElements.get(node.shortcutId)?.remove();inspectElements.delete(node.shortcutId);}
      }
      if(options.createObject&&mode==='mine'&&options.commit&&!target.disabled&&!blockedObject&&(!options.readableObjectFocus||!objectFocus)&&node.shortcutId===selectedId&&fieldDocument.shortcuts.some(item=>item.shortcutId===node.shortcutId)&&!preview){
        let handle=resizeElements.get(node.shortcutId);
        if(!handle){handle=el('button','uf-resize-handle');handle.type='button';handle.dataset.fieldControl='';handle.dataset.shortcutId=node.shortcutId;
          handle.setAttribute('aria-label',`Изменить размер: ${node.entity.title}`);handle.setAttribute('aria-description','Потяните метку. Стрелки меняют размер, Shift — по одному пикселю.');handle.append(icon('diagonal'));
          handle.addEventListener('pointerdown',event=>beginResize(event,node.shortcutId),{signal});
          handle.addEventListener('keydown',event=>{const item=activeNode(node.shortcutId);if(!item||!['ArrowLeft','ArrowRight','ArrowUp','ArrowDown'].includes(event.key))return;event.preventDefault();event.stopPropagation();const step=event.shiftKey?1:8;perform(resizeShortcut(item.shortcutId,Math.max(16,Math.min(8192,item.width+(event.key==='ArrowLeft'?-step:event.key==='ArrowRight'?step:0))),Math.max(16,Math.min(8192,item.height+(event.key==='ArrowUp'?-step:event.key==='ArrowDown'?step:0)))));},{signal});
          plane.append(handle);resizeElements.set(node.shortcutId,handle);}
        const anchor=fieldResizeAnchor(presentation,node.width,node.height);
        handle.style.left=`${node.x+anchor.x-22}px`;handle.style.top=`${node.y+anchor.y-22}px`;handle.style.setProperty('--uf-label-scale',String(1/cam.scale));
      }else if(options.createObject){resizeElements.get(node.shortcutId)?.remove();resizeElements.delete(node.shortcutId);}
    }
    for (const [id, target] of nodeElements) if (!keep.has(id)) { if(options.createObject){objectViews.get(id)?.view.setVisible(false);objectViews.get(id)?.view.dispose();objectViews.delete(id);resizeElements.get(id)?.remove();resizeElements.delete(id);}if(options.readableObjectFocus)objectFocusButtons.delete(id);target.remove(); nodeElements.delete(id); inspectElements.get(id)?.remove(); inspectElements.delete(id); }
    for (const [id, action] of inspectElements) if (mode !== 'mine') { action.remove(); inspectElements.delete(id); }
    plane.querySelector('.uf-drop-slot')?.remove();
    if (moving) {
      const context = current.contexts.find(item => item.contextId === moving!.preview.target.contextId);
      if (context) {
        const placement=moving.preview.target.placement,point = placement??fieldSlotToPoint(moving.preview.target.slot!, metrics), ghost = el('div', `uf-drop-slot ${moving.preview.valid ? 'is-valid' : 'is-invalid'}${moving.preview.occupied ? ' is-swap' : ''}`);
        const width=placement?.width??metrics.radius*SQRT3,height=placement?.height??metrics.radius*2;
        ghost.style.left = `${context.x + point.x - width/2}px`; ghost.style.top = `${context.y + point.y - height/2}px`;
        ghost.style.width = `${width}px`; ghost.style.height = `${height}px`;if(placement){ghost.classList.add('is-free');}else ghost.append(hexFrame(metrics.radius), icon(moving.preview.occupied ? 'refresh' : 'plus'));
        ghost.setAttribute('aria-hidden', 'true'); plane.append(ghost);
      }
    }
    tools.hidden = !moving || gesture?.type==='move'||gesture?.type==='resize';
    const moveState = String(!!moving); if (viewport.dataset.moving !== moveState) viewport.dataset.moving = moveState;
    if (moving) {
      const ids = fieldDocument.contexts.map(context => context.contextId).join('|');
      if (contextSelect.dataset.contexts !== ids) { contextSelect.replaceChildren(...fieldDocument.contexts.map(context => { const option = el('option', '', context.title); option.value = context.contextId; return option; })); contextSelect.dataset.contexts = ids; }
      contextSelect.value = moving.preview.target.contextId; confirm.disabled = !moving.preview.valid;
      const action = moving.preview.occupied ? 'Обменять местами' : 'Переместить сюда';
      if (confirm.getAttribute('aria-label') !== action) { confirm.setAttribute('aria-label', action); confirmFull.textContent = action; confirmShort.textContent = moving.preview.occupied ? 'Обменять' : 'Перенести'; }
      const hint = !moving.preview.valid ? 'Здесь соты будут перекрываться' : moving.preview.occupied ? 'Эти соты поменяются местами' : 'Выберите место, затем перенесите';
      if (moveHint.textContent !== hint) moveHint.textContent = hint;
    }
    if (!readySettled && initialized[mode] && viewport.clientWidth > 0 && viewport.clientHeight > 0) {
      readySettled = true; viewport.dataset.ready = 'true'; resolveReady();
    }
  }

  async function save(after: FieldDocument, intent?: { document: FieldDocument; expectedRevision: number; requestId: string }): Promise<FieldCommitResult> {
    if (destroyed) throw Object.assign(new Error('field_session_closed'), { code: 'field_session_closed' });
    if (!options.commit) throw Object.assign(new Error('field_read_only'), { code: 'field_read_only' });
    const capturedGeneration = generation;
    const transaction = intent ?? { document: structuredClone(after), expectedRevision: revision, requestId: crypto.randomUUID() };
    lastIntent = transaction; persistence = 'saving'; localDurable = false; emit();
    try {
      const result = await options.commit(structuredClone(transaction.document), { expectedRevision: transaction.expectedRevision, requestId: transaction.requestId });
      if (destroyed || generation !== capturedGeneration || options.accountId !== accountId) return result;
      const acknowledged = validateFieldDocument(result.document);
      if (!['saved', 'volatile', 'conflict'].includes(result.status) || !Number.isSafeInteger(result.revision) || result.revision < 0
        || result.status === 'saved' && result.revision <= transaction.expectedRevision) throw Object.assign(new Error('field_receipt_invalid'), { code: 'field_receipt_invalid' });
      revision = result.revision;
      if (result.status === 'conflict') { persistence = 'conflict'; localDurable = false; announce('Расстановка изменилась в другом окне. Изменения сохранены здесь для повторного выбора.'); }
      else if (result.status === 'saved') {
        if (!same(acknowledged, transaction.document)) {
          persistence = 'conflict'; localDurable = false; lastIntent = null;
          announce('Перестановка была сохранена, но поле уже изменилось в другом окне. Выберите актуальную версию.');
          schedule(); return result;
        }
        const latest = same(fieldDocument, after); persistence = latest ? 'saved' : 'saving'; localDurable = latest;
        if (latest) lastIntent = null; emit();
      } else { persistence = 'pending'; localDurable = result.localDurable === true && same(fieldDocument, after); emit(); }
      schedule(); return result;
    } catch (error) {
      if (!destroyed && generation === capturedGeneration) { persistence = 'error'; localDurable = false; announce(errorText(error)); schedule(); }
      throw error;
    }
  }
  function persist(after: FieldDocument): Promise<FieldCommitResult> {
    const task = tail.catch(() => undefined).then(() => save(after)); tail = task; return task;
  }
  async function execute(command: FieldCommand, record = true): Promise<FieldCommitResult | null> {
    if (destroyed || mode !== 'mine' || !options.commit) throw Object.assign(new Error('field_read_only'), { code: 'field_read_only' });
    const change = applyFieldCommand(fieldDocument, command, metrics); if (!change.changed) return null;
    cancel(); // A changed document invalidates every preview, including while its ACK is offline.
    if (record) recordFieldHistory(history, change);
    fieldDocument = change.document; persistence = 'saving'; localDurable = false; rebuild(); emit();
    return persist(structuredClone(fieldDocument));
  }
  function beginMove(shortcutId: string): void {
    if (destroyed || mode !== 'mine' || !options.commit || !available(shortcutId)) return;
    if(!fieldDocument.shortcuts.some(item=>item.shortcutId===shortcutId)){inspectShortcut(shortcutId);return;}
    const item = available(shortcutId)!; selectedId = shortcutId;
    moving = { shortcutId, start: structuredClone(fieldDocument), preview: fieldMovePreview(fieldDocument, entities, shortcutId,item.placement?.kind==='free'?{contextId:item.contextId,placement:{...item.placement}}:{ contextId: item.contextId, slot: [...item.slot] }, metrics) };
    render();nodeElements.get(shortcutId)?.focus({preventScroll:true});announce('Выберите свободное место или объект для обмена. Enter переносит, Escape отменяет.');
  }
  function previewMove(target: FieldMoveTarget): void {
    if (!moving || !same(moving.start, fieldDocument)) { cancel(); return; }
    moving.preview = fieldMovePreview(fieldDocument, entities, moving.shortcutId, target, metrics); render();
    live.textContent = moving.preview.valid ? moving.preview.occupied ? 'Занятое место. Можно обменять два ярлыка.' : 'Свободное место.' : 'Здесь объекты будут перекрываться.';
  }
  async function commitMove(): Promise<FieldCommitResult | null> {
    const move = moving; if (!move || !move.preview.valid || !same(move.start, fieldDocument)) return null;
    moving = null; tools.hidden = true;
    const result = await execute(move.preview.target.placement?{type:'place-shortcut',shortcutId:move.shortcutId,contextId:move.preview.target.contextId,placement:move.preview.target.placement}
      :{ type: 'move-shortcut', shortcutId: move.shortcutId, contextId: move.preview.target.contextId, slot: [...move.preview.target.slot!], swap: !!move.preview.occupied });
    if (!destroyed) { announce(result ? 'Ярлык перемещён.' : 'Расстановка не изменилась.'); focusNode(move.shortcutId); } return result;
  }
  function cancel(): void {
    pendingPointer = null; moving = null; contextPreview = null;
    // A subsequent pointerup cannot resurrect a cancelled whole-context preview.
    if (gesture && (gesture.type === 'move' || gesture.type==='resize' || gesture.type === 'context')) gesture.type = 'press';
    render();
  }
  function setArrange(enabled: boolean): void { arranging = enabled && mode === 'mine' && !!options.commit; cancel(); emit(); }
  function focusPreview(node: FieldNode): void {
    // First tap targets the actual thumbnail, rather than the centre of a large context.
    if(contextFocus[mode]!==node.contextId)focusContext(node.contextId);
    focusNode(node.shortcutId);
  }
  function focusNode(shortcutId: string): void {
    const node = activeNode(shortcutId); if (!node) return;
    selectedId = shortcutId; const cam = camera(), view = viewportSize(), margin = 40;
    const point = { x: (node.cx - cam.x) * cam.scale + view.width / 2, y: (node.cy - cam.y) * cam.scale + view.height / 2 };
    let x = cam.x, y = cam.y;
    if (point.x - node.width * cam.scale / 2 < margin || point.x + node.width * cam.scale / 2 > view.width - margin) x += (point.x - view.width / 2) / cam.scale;
    if (point.y - node.height * cam.scale / 2 < margin || point.y + node.height * cam.scale / 2 > view.height - margin) y += (point.y - view.height / 2) / cam.scale;
    const minScale = Math.max(.45, 44 / Math.max(1, Math.min(node.width, node.height)));
    if (cam.scale < minScale) { x = node.cx; y = node.cy; contextFocus[mode] = node.contextId; }
    if (view.width < 720 && contextFocus[mode] !== node.contextId) { contextFocus[mode] = node.contextId; x = node.cx; y = node.cy; }
    const next = normalizeFieldCamera({ ...cam, x, y, scale: Math.max(minScale, cam.scale) });
    if (next.x !== cam.x || next.y !== cam.y || next.scale !== cam.scale) cameraFit[mode] = 'manual';
    cameras[mode] = next; remember(); render(); emit(); nodeElements.get(shortcutId)?.focus({ preventScroll: true });
  }
  function perform(task: Promise<unknown>): void { void task.catch(error => { if (!destroyed && persistence !== 'error') announce(errorText(error)); }); }
  confirm.addEventListener('click', () => perform(commitMove()), { signal }); cancelButton.addEventListener('click', () => { cancel(); announce('Перенос отменён.'); focusNode(selectedId); }, { signal });
  contextSelect.addEventListener('change', () => { if (moving) { previewMove({ contextId: contextSelect.value, slot: nextFieldSlot(fieldDocument, contextSelect.value, available(moving.shortcutId)?.kind), swap: false }); if (viewport.clientWidth < 720) focusContext(contextSelect.value); } }, { signal });

  function inspectShortcut(shortcutId: string): boolean {
    const current = available(shortcutId); if (!current) return false;
    const preview = nodeElements.get(current.shortcutId)?.dataset.preview === 'true'&&objectFocus?.shortcutId!==current.shortcutId;
    if (!preview && !options.onInspect) return false;
    lastInspection = { shortcutId: current.shortcutId, until: performance.now() + 300 }; suppressClickUntil = performance.now() + 300;
    if (preview) focusPreview(current);
    else { selectedId = current.shortcutId; remember();options.onInspect?.(nodeElements.get(shortcutId)?.dataset.nativeUnavailable==='true'?{entity:current.entity.entity,title:'Объект недоступен'}:current.entity,current.shortcutId); }
    return true;
  }

  plane.addEventListener('keydown', event => {
    if(options.readableObjectFocus&&objectFocus)return;
    if (event.defaultPrevented || event.isComposing || (event.target as HTMLElement).closest('input,textarea,select,audio,video,iframe,[contenteditable],[data-field-control],.uf-object-expand')) return;
    if (event.altKey) return; // Alt+Left/Right belongs to browser history.
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'z' && !event.shiftKey && mode === 'mine') { event.preventDefault(); perform(undo()); return; }
    if (event.ctrlKey || event.metaKey) return;
    if ((event.key === 'ContextMenu' || event.shiftKey && event.key === 'F10') && options.onInspect) { if (inspectShortcut(selectedId)) event.preventDefault(); return; }
    if (event.key === 'Escape') { if (moving || contextPreview) { event.preventDefault(); cancel(); announce('Перенос отменён.'); } else if (arranging) setArrange(false); else fitOverview(); return; }
    if (event.key === ' ' && arranging) { event.preventDefault(); if (!moving) beginMove(selectedId); return; }
    if (event.key === ' ' && !moving) { event.preventDefault(); spacePanning=true; schedule(); return; }
    if (event.key.toLowerCase() === 'm' && mode==='mine' && options.commit) { event.preventDefault(); if(!moving)beginMove(selectedId); return; }
    if (event.key === 'Enter' && moving) { event.preventDefault(); perform(commitMove()); return; }
    if(event.key==='Enter'&&options.createObject&&objectViews.has(selectedId)){event.preventDefault();if(options.readableObjectFocus&&nodeElements.get(selectedId)?.dataset.needsFocus==='true')enterObjectFocus(selectedId);else objectViews.get(selectedId)!.view.activate?.();return;}
    if (!event.ctrlKey && !event.metaKey && ['+', '=', '-', '0'].includes(event.key)) { event.preventDefault(); if (event.key === '0') fitOverview(); else setCamera(zoomFieldCamera(camera(), camera().scale * (event.key === '-' ? .85 : 1.18), { x: viewportSize().width / 2, y: viewportSize().height / 2 }, viewportSize())); return; }
    const directions: Record<string, [number, number]> = { ArrowLeft: [-1, 0], ArrowRight: [1, 0], ArrowUp: [0, -1], ArrowDown: [0, 1] };
    if (!directions[event.key] && !['Home', 'End'].includes(event.key)) return;
    event.preventDefault();
    if (moving && directions[event.key]) {
      const free=moving.preview.target.placement;
      if(free){const direction=directions[event.key]!,step=event.shiftKey?1:10;previewMove({contextId:moving.preview.target.contextId,placement:{...free,x:free.x+direction[0]*step,y:free.y+direction[1]*step}});return;}
      if (event.shiftKey) {
        const neighbour = nearestFieldNode(activeLayout().nodes, moving.shortcutId, directions[event.key]!);
        if (neighbour) { previewMove({ contextId: neighbour.contextId, slot: [...neighbour.slot], swap: true }); announce(`Обмен с ${neighbour.entity.title}. Enter подтверждает, Escape отменяет.`); return; }
      }
      const delta: Record<string, [number, number]> = { ArrowLeft: [-1, 0], ArrowRight: [1, 0], ArrowUp: [0, -1], ArrowDown: [0, 1] };
      const shift = delta[event.key]!, target = moving.preview.target;
      previewMove({ contextId: target.contextId, slot: [target.slot![0] + shift[0], target.slot![1] + shift[1]], swap: true }); return;
    }
    const navigable = options.dimNonMatches ? layout.nodes.filter(matchesMineFilter) : layout.nodes;
    const next = event.key === 'Home' ? navigable[0] : event.key === 'End' ? navigable.at(-1) : nearestFieldNode(navigable, selectedId, directions[event.key]!);
    if (next) focusNode(next.shortcutId);
  }, { signal });
  window.addEventListener('keydown',event=>{
    if(options.readableObjectFocus&&objectFocus)return;
    if(event.defaultPrevented||event.isComposing||event.altKey||event.ctrlKey||event.metaKey||!['Space',' '].includes(event.code||event.key))return;
    const target=event.target as HTMLElement;
    if(target?.closest?.('input,textarea,select,[contenteditable],[data-field-control],.uf-object-expand'))return;
    if(!viewport.contains(target)&&!(document.activeElement===document.body&&viewport.matches(':hover')))return;
    event.preventDefault();spacePanning=true;schedule();
  },{signal});
  viewport.addEventListener('wheel', event => {
    if(options.readableObjectFocus&&objectFocus)return;
    if (event.defaultPrevented || !event.cancelable || gesture || pinch) return;
    const target = event.target instanceof Element ? event.target : viewport;
    if (shouldYieldFieldWheel(target, viewport, event)) return;
    const rect = viewport.getBoundingClientRect();
    const next = fieldWheelCamera(camera(), event, { x: event.clientX - rect.left, y: event.clientY - rect.top }, viewportSize());
    if (!next) return;
    event.preventDefault(); setCamera(next);
  }, { passive: false, signal });
  function localPoint(event: PointerEvent): FieldPoint { const rect = viewport.getBoundingClientRect(); return { x: event.clientX - rect.left, y: event.clientY - rect.top }; }
  function beginResize(event:PointerEvent,shortcutId:string):void {
    if(event.button!==0||destroyed||mode!=='mine'||!options.commit||!fieldDocument.shortcuts.some(item=>item.shortcutId===shortcutId))return;
    const node=activeNode(shortcutId);if(!node)return;event.preventDefault();event.stopPropagation();resetPointers();beginMove(shortcutId);
    const point=localPoint(event);pointers.set(event.pointerId,point);gesture={pointerId:event.pointerId,pointerType:event.pointerType,forcePan:false,x:point.x,y:point.y,camera:{...camera()},type:'resize',shortcutId,contextId:node.contextId,moved:true,point:fieldScreenToWorld(point,camera(),viewportSize()),nodePoint:{x:node.x,y:node.y},nodeSize:{width:node.width,height:node.height},nodePresentation:node.presentation??{shape:'rect'}};
    capturePointer(event.pointerId);
  }
  function flushPointerSample(): void {
    const sample = pendingPointer; pendingPointer = null; if (!sample || destroyed) return;
    inPointerFrame = true;
    try {
      if ('pinch' in sample) {
        if (pinch !== sample.pinch || pointers.size < 2) return;
        const current = sample.pinch, [a, b] = [...pointers.values()], scale = normalizeFieldCamera({ scale: current.camera.scale * Math.hypot(a!.x - b!.x, a!.y - b!.y) / current.distance }).scale;
        const midpoint = { x: (a!.x + b!.x) / 2, y: (a!.y + b!.y) / 2 }, view = viewportSize();
        setCamera({ scale, x: current.world.x - (midpoint.x - view.width / 2) / scale, y: current.world.y - (midpoint.y - view.height / 2) / scale }); return;
      }
      const { gesture: current, point } = sample;
      if (gesture !== current) return;
      if (current.type === 'pan') setCamera({ ...current.camera, x: current.camera.x - (point.x - current.x) / current.camera.scale, y: current.camera.y - (point.y - current.y) / current.camera.scale });
      else if(current.type==='resize'&&moving&&current.nodePoint&&current.nodeSize){
        const context=fieldDocument.contexts.find(item=>item.contextId===current.contextId);if(!context)return;
        const {width,height}=fieldResizeDimensions(current.nodePresentation??{shape:'rect'},current.nodeSize.width,current.nodeSize.height,{x:(point.x-current.x)/current.camera.scale,y:(point.y-current.y)/current.camera.scale});
        previewMove({contextId:context.contextId,placement:{kind:'free',x:current.nodePoint.x+width/2-context.x,y:current.nodePoint.y+height/2-context.y,width,height}});
      }
      else if (current.type === 'move' && moving) {
        if (point.x < 0 || point.y < 0 || point.x > viewportSize().width || point.y > viewportSize().height) { moving.preview = { valid: false, reason: 'missing', target: moving.preview.target }; return; }
        if(current.nodePoint)current.dragPoint={x:current.nodePoint.x+(point.x-current.x)/current.camera.scale,y:current.nodePoint.y+(point.y-current.y)/current.camera.scale};
        const world = current.dragPoint?{x:current.dragPoint.x+(available(current.shortcutId)?.width??0)/2,y:current.dragPoint.y+(available(current.shortcutId)?.height??0)/2}:fieldScreenToWorld(point, camera(), viewportSize());
        const personalContexts=layout.contexts.filter(context=>fieldDocument.contexts.some(item=>item.contextId===context.contextId));
        const destination = personalContexts.find(context => world.x >= context.body.left && world.x <= context.body.right && world.y >= context.body.top && world.y <= context.body.bottom)
          ?? personalContexts.find(context => context.contextId === available(current.shortcutId)?.contextId);
        const source=available(current.shortcutId);
        if (destination) previewMove(source?.placement?.kind==='free'?{contextId:destination.contextId,placement:{...source.placement,x:world.x-destination.x,y:world.y-destination.y}}
          :{ contextId: destination.contextId, slot: fieldPointToSlot({ x: world.x - destination.x, y: world.y - destination.y }, metrics), swap: true });
      } else if (current.type === 'context') {
        const context = fieldDocument.contexts.find(item => item.contextId === current.contextId); if (!context) return;
        const next = { x: context.x + (point.x - current.x) / current.camera.scale, y: context.y + (point.y - current.y) / current.camera.scale };
        try {
          const changed = applyFieldCommand(fieldDocument, { type: 'move-context', contextId: current.contextId, ...next }).document;
          const projected = layoutUnifiedField(changed, entities, metrics), movedContext = projected.contexts.find(item => item.contextId === current.contextId)!;
          contextPreview = { document: changed, layout: projected, point: next, contextId: current.contextId,
            valid: !projected.contexts.some(item => item.contextId !== current.contextId && fieldBoundsOverlap(movedContext.bounds, item.bounds, metrics.contextGap)) };
        } catch { contextPreview = null; } // Out-of-range movement remains a cancelled preview.
      }
    } finally { inPointerFrame = false; }
  }
  function finishGesture(cancelled: boolean, releaseCapture = true): void {
    if (cancelled) pendingPointer = null; else flushPointerSample();
    const current = gesture; gesture = null; if (current?.timer) clearTimeout(current.timer);
    if (current?.moved) suppressClickUntil = performance.now() + 300;
    if (current?.type === 'inspect' && lastInspection?.shortcutId === current.shortcutId) lastInspection.until = performance.now() + 300;
    if (releaseCapture && current && viewport.hasPointerCapture(current.pointerId)) viewport.releasePointerCapture(current.pointerId);
    if (cancelled) { cancel(); return; }
    if (current?.type === 'move'||current?.type==='resize') { if(moving?.preview.valid)perform(commitMove());else{cancel();announce('Здесь предметы будут перекрываться. Выберите другое место.');} }
    else if (current?.type === 'context' && contextPreview) {
      const change = contextPreview; contextPreview = null; if (change.valid) perform(execute({ type: 'move-context', contextId: change.contextId, x: change.point.x, y: change.point.y })); else render();
    }
    render();
  }
  function resetPointers(): void {
    const captured = [...pointers.keys()];
    pointers.clear();
    if (pinch) suppressClickUntil = performance.now() + 300;
    pinch = null;
    // Clear ownership before releasing capture: its queued lost event belongs
    // to this finished sequence and must not cancel a subsequent press.
    finishGesture(true);
    for (const id of captured) if (viewport.hasPointerCapture(id)) viewport.releasePointerCapture(id);
  }
  function capturePointer(id: number): boolean {
    try { viewport.setPointerCapture(id); return true; }
    catch { resetPointers(); return false; }
  }
  viewport.addEventListener('pointerdown', event => {
    suppressClickUntil = 0; // A new deliberate press is not the previous drag's trailing click.
    if(options.readableObjectFocus&&objectFocus)return;
    lastInspection = null; // A fresh right click or press is a new explicit action.
    if (event.button !== 0 && event.button !== 1) return;
    if ((event.target as HTMLElement).closest('.uf-move-tools,.uf-inspect,input,textarea,select,[contenteditable],[data-field-control]')) return;
    if(event.button===1)event.preventDefault();
    const point = localPoint(event); pointers.set(event.pointerId, point);
    if (pointers.size > 1) {
      finishGesture(true, false); const [a, b] = [...pointers.values()]; pinch = { distance: Math.max(1, Math.hypot(a!.x - b!.x, a!.y - b!.y)),
        world: fieldScreenToWorld({ x: (a!.x + b!.x) / 2, y: (a!.y + b!.y) / 2 }, camera(), viewportSize()), camera: { ...camera() } };
      for (const id of pointers.keys()) if (!capturePointer(id)) return;
      return;
    }
    let target = (event.target as HTMLElement).closest<HTMLElement>('[data-shortcut-id]');const header = (event.target as HTMLElement).closest<HTMLElement>('.uf-context-title');
    const node=target?activeNode(target.dataset.shortcutId??''):undefined;
    if(node?.presentation&&node.presentation.shape!=='rect'&&node.presentation.shape!=='hex'
      &&!fieldPointInPolygon(fieldScreenToWorld(point,camera(),viewportSize()),node.footprint))target=null;
    gesture = { pointerId: event.pointerId, pointerType:event.pointerType,forcePan:event.button===1||spacePanning,x: point.x, y: point.y, camera: { ...camera() }, type: 'press', shortcutId: target?.dataset.shortcutId ?? '', contextId: header?.dataset.contextId ?? '', moved: false,
      ...(node?{nodePoint:{x:node.x,y:node.y}}:{}),
      point: fieldScreenToWorld(point, camera(), viewportSize()) };
    if (gesture.pointerType==='touch'&&!gesture.forcePan&&gesture.shortcutId && options.onInspect && !arranging&&!moving && target?.dataset.preview !== 'true') {
      const captured = gesture; captured.timer = setTimeout(() => { if (gesture !== captured || captured.moved || destroyed) return;
        delete captured.timer; if (!capturePointer(captured.pointerId)) return;
        captured.moved = true; captured.type = 'inspect';
        if (inspectShortcut(captured.shortcutId) && lastInspection) lastInspection.until = Infinity; }, 450);
    }
  }, { signal });
  viewport.addEventListener('pointermove', event => {
    if (!pointers.has(event.pointerId)) return;
    if (event.pointerType !== 'touch' && event.buttons === 0) { resetPointers(); return; }
    const point = localPoint(event); pointers.set(event.pointerId, point);
    if (pinch && pointers.size >= 2) {
      pendingPointer = { pinch }; schedule(); return;
    }
    const current = gesture; if (!current || current.pointerId !== event.pointerId || current.type === 'inspect') return;
    const distance = Math.hypot(point.x - current.x, point.y - current.y);
    if (!current.moved && distance > (current.pointerType==='touch'?7:3)) {
      current.moved = true; if (current.timer) clearTimeout(current.timer); if (!capturePointer(event.pointerId)) return;
      const direct=!current.forcePan&&(arranging||current.pointerType!=='touch'||moving?.shortcutId===current.shortcutId);
      if (direct&&mode==='mine'&&options.commit&&current.shortcutId) {
        if(fieldDocument.shortcuts.some(item=>item.shortcutId===current.shortcutId)){beginMove(current.shortcutId);current.type='move';}
        else{current.type='inspect';inspectShortcut(current.shortcutId);}
      }
      else if (direct&&mode==='mine'&&options.commit&&current.contextId&&fieldDocument.contexts.some(item=>item.contextId===current.contextId)) current.type='context';
      else current.type='pan';
    }
    if (current.type !== 'press') { pendingPointer = { gesture: current, point }; schedule(); }
  }, { signal });
  viewport.addEventListener('pointerup', event => {
    if (!pointers.has(event.pointerId)) return;
    if (pinch) { flushPointerSample(); resetPointers(); return; }
    pointers.delete(event.pointerId);
    const point = localPoint(event), view = viewportSize();
    if (gesture?.pointerId === event.pointerId && gesture.type !== 'press') pendingPointer = { gesture, point };
    finishGesture(point.x < 0 || point.y < 0 || point.x > view.width || point.y > view.height);
  }, { signal });
  viewport.addEventListener('pointercancel', event => { if (pointers.has(event.pointerId)) resetPointers(); }, { signal });
  viewport.addEventListener('lostpointercapture', event => { if (pointers.has(event.pointerId)) resetPointers(); }, { signal });
  window.addEventListener('pointerup', event => {
    if (viewport.contains(event.target as Node) || !pointers.has(event.pointerId)) return;
    resetPointers();
  }, { signal });
  window.addEventListener('keyup',event=>{if(event.code==='Space'||event.key===' '){if(spacePanning)event.preventDefault();spacePanning=false;schedule();}},{signal});
  window.addEventListener('blur',()=>{spacePanning=false;resetPointers();},{signal});
  viewport.addEventListener('click', event => {
    if (event.detail > 0 && performance.now() < suppressClickUntil) { event.preventDefault(); event.stopPropagation(); return; }
    if (moving && !(event.target as HTMLElement).closest('.uf-node,.uf-context-title,.uf-move-tools')) {
      const rect = viewport.getBoundingClientRect(), world = fieldScreenToWorld({ x: event.clientX - rect.left, y: event.clientY - rect.top }, camera(), viewportSize());
      const context = layout.contexts.find(item => fieldBoundsOverlap(item.body, { left: world.x, right: world.x + 1, top: world.y, bottom: world.y + 1 }));
      if (context) previewMove({ contextId: context.contextId, slot: fieldPointToSlot({ x: world.x - context.x, y: world.y - context.y }, metrics), swap: true });
    }
  }, { capture: true, signal });
  viewport.addEventListener('dblclick', event => {
    if (event.defaultPrevented || event.button !== 0 || arranging || gesture || pinch || performance.now() < suppressClickUntil) return;
    const target = event.target instanceof Element ? event.target : viewport;
    if (target.closest('button,a,input,textarea,select,[contenteditable],.uf-node,.uf-context-title,.uf-move-tools,.uf-inspect,.uf-pan-controls')) return;
    if (shouldYieldFieldWheel(target, viewport, { deltaX: 0, deltaY: 0, deltaMode: 0, ctrlKey: false, metaKey: false, shiftKey: false })) return;
    const rect = viewport.getBoundingClientRect();
    event.preventDefault(); setCamera(zoomFieldCamera(camera(), camera().scale * 1.6, { x: event.clientX - rect.left, y: event.clientY - rect.top }, viewportSize()));
  }, { signal });
  let lastWidth = 0, lastHeight = 0;
  const resize = new ResizeObserver(() => {
    const width = viewport.clientWidth, height = viewport.clientHeight;
    const resized = lastWidth > 0 && lastHeight > 0 && (width !== lastWidth || height !== lastHeight);
    const changed = Math.abs(width - lastWidth) > 40 || Math.abs(height - lastHeight) > 40;
    lastWidth = width; lastHeight = height;
    // Pointer samples capture the previous viewport's coordinates. A reflow
    // must not let their late release persist a different slot or context.
    // Identical notifications and the first observation keep the owned gesture.
    if (resized && (gesture || pinch || moving || contextPreview || pointers.size)) resetPointers();
    if(options.readableObjectFocus&&objectFocus){render();return;}
    if (!initialized[mode]) rebuild();
    else if (changed && !gesture && !moving) {
      if (cameraFit[mode] === 'context' && contextFocus[mode]) focusContext(contextFocus[mode]);
      else if (cameraFit[mode] === 'overview') fitOverview();
      else render();
    }
    else render();
  }); resize.observe(viewport);
  if(options.readableObjectFocus){window.visualViewport?.addEventListener('resize',schedule,{signal});window.visualViewport?.addEventListener('scroll',schedule,{signal});}
  // TTL expiry removes device signals even when no new directory page arrives.
  const ttl = setInterval(() => { for (const target of nodeElements.values()) if (target.querySelector('.uf-status')) { target.dataset.signature = ''; } schedule(); }, 5_000);

  async function undo(): Promise<FieldCommitResult | null> {
    if (mode !== 'mine' || destroyed) return null;
    const change = undoFieldHistory(history, fieldDocument); if (!change) return null;
    cancel(); const result = await execute({ type: 'restore', document: change.document, expected: change.before }, false); if (!destroyed) announce('Последняя перестановка отменена.'); return result;
  }
  async function moveShortcut(shortcutId: string, target: FieldMoveTarget): Promise<FieldCommitResult | null> {
    const preview = fieldMovePreview(fieldDocument, entities, shortcutId, target, metrics);
    if (!preview.valid) throw Object.assign(new Error('field_collision'), { code: 'field_collision' });
    return execute(target.placement?{type:'place-shortcut',shortcutId,contextId:target.contextId,placement:target.placement}
      :{ type: 'move-shortcut', shortcutId, contextId: target.contextId, slot: [...target.slot!], swap: target.swap === true });
  }
  async function setShortcutPlacement(shortcutId:string,placement:FieldPlacement,contextId?:string):Promise<FieldCommitResult|null>{
    const source=fieldDocument.shortcuts.find(item=>item.shortcutId===shortcutId);if(!source)throw Object.assign(new Error('field_shortcut_missing'),{code:'field_shortcut_missing'});
    return execute({type:'place-shortcut',shortcutId,contextId:contextId??source.contextId,placement});
  }
  async function resizeShortcut(shortcutId:string,width:number,height:number):Promise<FieldCommitResult|null>{
    const node=activeNode(shortcutId),context=node?fieldDocument.contexts.find(item=>item.contextId===node.contextId):undefined;
    if(!node||!context)throw Object.assign(new Error('field_shortcut_missing'),{code:'field_shortcut_missing'});
    return setShortcutPlacement(shortcutId,{kind:'free',x:node.cx-context.x,y:node.cy-context.y,width,height},context.contextId);
  }
  async function moveContext(contextId: string, point: FieldPoint): Promise<FieldCommitResult | null> {
    const change = applyFieldCommand(fieldDocument, { type: 'move-context', contextId, ...point }), projected = layoutUnifiedField(change.document, entities, metrics);
    const context = projected.contexts.find(item => item.contextId === contextId)!;
    if (projected.contexts.some(other => other.contextId !== contextId && fieldBoundsOverlap(context.bounds, other.bounds, metrics.contextGap))) throw Object.assign(new Error('field_collision'), { code: 'field_collision' });
    return execute({ type: 'move-context', contextId, ...point });
  }
  rebuild();
  return {
    element: viewport, ready,
    update(update) {
      if (destroyed) return;
      const nextLive=update.liveScene?validateLiveFieldScene(update.liveScene,fieldDocument):liveScene;
      const changedFilter=update.filter!==undefined&&update.filter!==filter||update.visibleKinds!==undefined&&!same(update.visibleKinds,visibleKinds);
      const priorContext=contextFocus[mode];
      const followFilter=changedFilter&&mode==='mine'&&cameraFit[mode]==='context'&&!!priorContext&&viewport.clientWidth<720&&!gesture&&!moving;
      if (update.entities) entities = thinEntities(update.entities);
      if (update.filter !== undefined) filter = update.filter;
      if (update.visibleKinds !== undefined) visibleKinds = [...update.visibleKinds];
      if (update.document && (update.acceptRemote === true || persistence === 'saved' || same(update.document, fieldDocument))) {
        const next = validateFieldDocument(update.document), differs = !same(next, fieldDocument);
        validateLiveFieldScene(nextLive,next);
        if (update.acceptRemote && differs) { generation++; resetPointers(); history.entries.length = 0; selectedId = ''; }
        fieldDocument = next; if (update.revision !== undefined) revision = update.revision;
        if (update.persistence === 'saved' || update.acceptRemote === true) { persistence = 'saved'; localDurable = true; lastIntent = null; }
        else if (update.persistence === 'pending') { persistence = 'pending'; localDurable = update.localDurable === true; }
        else if (update.persistence === 'conflict') { persistence = 'conflict'; localDurable = false; }
      }
      liveScene=nextLive;
      if (update.searchDocument) searchDocument = structuredClone(update.searchDocument);
      if (update.mode && update.mode !== mode) { resetPointers(); remember(); mode = update.mode; arranging = false; selectedId = selections[mode]; }
      if (update.scope !== undefined && update.scope !== scope) { resetPointers(); scope = update.scope; if (mode === 'search') { initialized.search = false; contextFocus.search = ''; selectedId = ''; selections.search = ''; } }
      if (moving) { const source = fieldDocument.shortcuts.find(item => item.shortcutId === moving!.shortcutId); if (!source || !same(moving.start, fieldDocument) || !entities.some(entity => fieldEntityKey(entity.entity) === fieldEntityKey(source.entity))) cancel(); }
      if (update.selectedShortcutId !== undefined) selectedId = update.selectedShortcutId;
      rebuild(); emit();
      // A newly admitted search is user navigation; a repeated filter during metadata refresh is not.
      if(followFilter){
        const matching=options.dimNonMatches?layout.nodes.filter(matchesMineFilter):layout.nodes;
        if(matching.length&&!matching.some(node=>node.contextId===priorContext))focusContext(matching[0]!.contextId);
      }
    },
    getCamera: () => ({ ...camera() }), setCamera, focusContext, revealShortcut, fitOverview, setArrange,
    focusObject(shortcutId){enterObjectFocus(shortcutId);return objectFocus?.shortcutId===shortcutId?objectViews.get(shortcutId)?.view??null:null;},
    closeObject:leaveObjectFocus,
    addContext: (title, point) => {
      const personalBounds=layoutUnifiedField(fieldDocument,entities,metrics).bounds;
      const destination = point ?? { x: personalBounds ? personalBounds.right + 400 : 0, y: personalBounds ? (personalBounds.top + personalBounds.bottom) / 2 : 0 };
      return execute({ type: 'add-context', contextId: crypto.randomUUID(), title, ...destination });
    },
    renameContext: (contextId, title) => execute({ type: 'rename-context', contextId, title }),
    removeContext: (contextId, remove = {}) => execute({ type: 'remove-context', contextId, removeShortcuts: remove.removeShortcuts === true }),
    addShortcut: (entity, contextId) => execute({ type: 'add-shortcut', shortcutId: crypto.randomUUID(), entity, contextId }),
    addFreeShortcut:(entity,contextId,placement)=>execute({type:'add-shortcut',shortcutId:crypto.randomUUID(),entity,contextId,placement}),
    setShortcutPlacement,resizeShortcut,
    removeShortcut: shortcutId => execute({ type: 'remove-shortcut', shortcutId }), moveShortcut, moveContext, beginMove, previewMove, commitMove, undo, cancel,
    async compactContext(contextId, expected) {
      return execute({ type: 'restore', document: compactFieldContext(expected, contextId, metrics), expected });
    },
    hasUnsavedChanges: () => persistence !== 'saved' && !localDurable||[...objectViews.values()].some(entry=>entry.view.hasUnsavedChanges()),
    async flush() {
      await Promise.all([...objectViews.values()].map(entry=>entry.view.flush()));
      await tail.catch(() => undefined);
      if (persistence === 'saved' || localDurable) return;
      if (persistence === 'conflict') throw Object.assign(new Error('field_conflict'), { code: 'field_conflict' });
      const intent = lastIntent && same(lastIntent.document, fieldDocument) ? lastIntent : undefined;
      const result = await save(structuredClone(fieldDocument), intent);
      if (result.status !== 'saved' && !localDurable) throw Object.assign(new Error('field_not_saved'), { code: 'field_not_saved' });
    },
    snapshot: () => structuredClone(fieldDocument),
    destroy() { if (destroyed) return; remember(); resetPointers(); destroyed = true; generation++; controller.abort(); resize.disconnect(); clearInterval(ttl); if (raf) cancelAnimationFrame(raf);releaseFocusControls();for(const entry of objectViews.values())entry.view.dispose();objectViews.clear();resizeElements.clear();nodeElements.clear(); contextElements.clear(); inspectElements.clear();objectFocusButtons.clear();objectFocus=null; entities = []; viewport.remove(); if (!readySettled) { readySettled = true; resolveReady(); } },
  };
}
