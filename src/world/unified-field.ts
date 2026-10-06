import { createFieldDocument, fieldEntityKey, validateFieldDocument, validateFieldEntity, type FieldContext, type FieldDocument, type FieldEntityRef, type FieldEntityKind } from '../../modules/field/contract.mjs';
import { el, initials, nounCount, safeImageUrl } from './dom';
import { icon } from './icons';
import { roundedHexPath, SQRT3 } from '../geometry/hex.mjs';
import { applyFieldCommand, createFieldHistory, nextFieldSlot, recordFieldHistory, undoFieldHistory, type FieldCommand } from './unified-field-state.mjs';
import { fieldLevelOfDetail, fieldScreenToWorld, fitFieldCamera, normalizeFieldCamera, zoomFieldCamera, type FieldCamera, type FieldPoint } from './unified-field-camera.mjs';
import { createFieldSpatialIndex, fieldBounds, fieldBoundsOverlap, fieldMovePreview, fieldPointToSlot, fieldSlotToPoint, layoutUnifiedField,
  nearestFieldNode, UNIFIED_FIELD_METRICS, type FieldDirectoryEntity, type FieldMovePreview, type FieldMoveTarget,
  type FieldNode, type FieldScene, type UnifiedFieldLayout, type UnifiedFieldMetrics } from './unified-field-layout.mjs';
import type { AppArt } from './app-art.mjs';
import type { FieldAppArt } from './field-art.mjs';
import { errorText } from './dialogs';
import './unified-field.css';

export type { FieldDirectoryEntity, FieldScene, FieldMoveTarget, UnifiedFieldMetrics } from './unified-field-layout.mjs';
export type { FieldCamera } from './unified-field-camera.mjs';
export type { FieldDocument, FieldContext, FieldEntityRef } from '../../modules/field/contract.mjs';
export type UnifiedFieldMode = 'mine' | 'search';
type FieldVisualArt = AppArt & { icon?: FieldAppArt['icon'] };
export interface FieldCommitResult { status: 'saved' | 'volatile' | 'conflict'; revision: number; document: FieldDocument; errorCode?: string; localDurable?: boolean }
export interface UnifiedFieldViewState { mine?: FieldCamera; search?: FieldCamera; mineContext?: string; searchContext?: string; mineSelection?: string; searchSelection?: string }
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
  commit?: (document: FieldDocument, context: { expectedRevision: number; requestId: string }) => Promise<FieldCommitResult>;
  onActivate?: (entity: FieldDirectoryEntity, shortcutId?: string) => void;
  onInspect?: (entity: FieldDirectoryEntity, shortcutId: string) => void;
  onContextActivate?: (contextId: string) => void;
  onStateChange?: (summary: UnifiedFieldSummary) => void;
  resolveArt?: (entity: FieldDirectoryEntity) => FieldVisualArt | null;
}
export interface UnifiedFieldUpdate {
  document?: FieldDocument; revision?: number; entities?: readonly FieldDirectoryEntity[]; mode?: UnifiedFieldMode;
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
  fitOverview(): void;
  setArrange(enabled: boolean): void;
  addContext(title: string, point?: FieldPoint): Promise<FieldCommitResult | null>;
  renameContext(contextId: string, title: string): Promise<FieldCommitResult | null>;
  removeContext(contextId: string, options?: { removeShortcuts?: boolean }): Promise<FieldCommitResult | null>;
  addShortcut(entity: FieldEntityRef, contextId: string): Promise<FieldCommitResult | null>;
  removeShortcut(shortcutId: string): Promise<FieldCommitResult | null>;
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
      if (Number.isFinite(value.updatedAt)) result.updatedAt = value.updatedAt!;
      if (entity.kind === 'person' && typeof value.avatarRevision === 'number' && Number.isSafeInteger(value.avatarRevision) && value.avatarRevision > 0) result.avatarRevision = value.avatarRevision;
      if (Number.isSafeInteger(value.unreadCount) && value.unreadCount! > 0) result.unreadCount = value.unreadCount!;
      if (entity.kind === 'device' && typeof value.online === 'boolean') result.online = value.online;
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

/** One camera over immutable world coordinates. No server or capability mutation belongs here. */
export function createUnifiedField(options: UnifiedFieldOptions): UnifiedField {
  const accountId = options.accountId, metrics = { ...UNIFIED_FIELD_METRICS, ...options.metrics };
  installHexClip();
  const viewport = el('div', 'uf-viewport'); viewport.setAttribute('role', 'region'); viewport.setAttribute('aria-label', 'Поле Сот');
  viewport.style.setProperty('--uf-hex-clip', 'url(#uf-hex-clip)');
  const plane = el('div', 'uf-plane'); plane.setAttribute('role', 'group');
  plane.setAttribute('aria-label', 'Стрелки выбирают объект. Enter открывает. В расстановке Space переносит, Escape отменяет.');
  const live = el('div', 'uf-live'); live.setAttribute('aria-live', 'polite'); live.setAttribute('role', 'status');
  const tools = el('div', 'uf-move-tools'); tools.hidden = true;
  const contextSelect = el('select', 'uf-move-context'); contextSelect.setAttribute('aria-label', 'Переместить в пространство');
  const confirm = el('button', 'uf-move-confirm', 'Переместить сюда'); confirm.type = 'button';
  const cancelButton = el('button', 'uf-move-cancel', 'Отменить перенос'); cancelButton.type = 'button';
  tools.append(contextSelect, confirm, cancelButton); viewport.append(plane, tools, live);
  const controller = new AbortController(), signal = controller.signal;
  let resolveReady!: () => void, readySettled = false;
  const ready = new Promise<void>(resolve => { resolveReady = resolve; });
  const viewState = options.viewState ?? {}, cameras: Record<UnifiedFieldMode, FieldCamera> = {
    mine: normalizeFieldCamera(viewState.mine), search: normalizeFieldCamera(viewState.search) };
  const contextFocus: Record<UnifiedFieldMode, string> = { mine: viewState.mineContext ?? '', search: viewState.searchContext ?? '' };
  const initialized: Record<UnifiedFieldMode, boolean> = { mine: !!viewState.mine, search: !!viewState.search };
  let fieldDocument = validateFieldDocument(options.document ?? createFieldDocument()), revision = options.revision ?? 0;
  let entities = thinEntities(options.entities ?? []), mode = options.mode ?? 'mine', scope = '', searchDocument: FieldScene = { contexts: [], shortcuts: [] };
  let layout = layoutUnifiedField(fieldDocument, entities, metrics), index = createFieldSpatialIndex(layout.nodes);
  const selections: Record<UnifiedFieldMode, string> = { mine: viewState.mineSelection ?? '', search: viewState.searchSelection ?? '' };
  let selectedId = selections[mode], arranging = false, destroyed = false, raf = 0, suppressClickUntil = 0;
  let filter = '', visibleKinds: readonly FieldEntityKind[] | undefined;
  let persistence: UnifiedFieldSummary['persistence'] = 'saved', localDurable = true, message = '', generation = 0;
  let tail: Promise<unknown> = Promise.resolve(), lastIntent: { document: FieldDocument; expectedRevision: number; requestId: string } | null = null;
  const history = createFieldHistory(30), nodeElements = new Map<string, HTMLButtonElement>(), contextElements = new Map<string, HTMLElement>(), inspectElements = new Map<string, HTMLButtonElement>();
  let exposedDocument: FieldDocument = structuredClone(fieldDocument), exposedSource = fieldDocument;
  let moving: { shortcutId: string; preview: FieldMovePreview; start: FieldDocument } | null = null;
  type Gesture = { pointerId: number; x: number; y: number; camera: FieldCamera; type: 'press' | 'pan' | 'move' | 'context'; shortcutId: string; contextId: string; moved: boolean; point: FieldPoint; timer?: ReturnType<typeof setTimeout> };
  let gesture: Gesture | null = null, contextPreview: { document: FieldDocument; layout: UnifiedFieldLayout; valid: boolean; point: FieldPoint; contextId: string } | null = null;
  const pointers = new Map<number, FieldPoint>();
  type Pinch = { distance: number; world: FieldPoint; camera: FieldCamera };
  let pinch: Pinch | null = null;
  let pendingPointer: { gesture: Gesture; point: FieldPoint } | { pinch: Pinch } | null = null, inPointerFrame = false;
  const viewportSize = () => ({ width: viewport.clientWidth || 900, height: viewport.clientHeight || 650 });
  const camera = () => cameras[mode];
  const scene = (): FieldScene => mode === 'mine' ? fieldDocument : searchDocument;
  const activeLayout = (): UnifiedFieldLayout => contextPreview?.layout ?? moving?.preview.layout ?? layout;

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
    if (mode === 'mine') { viewState.mineContext = contextFocus.mine; viewState.mineSelection = selectedId; }
    else { viewState.searchContext = contextFocus.search; viewState.searchSelection = selectedId; }
  }
  function setCamera(next: FieldCamera): void { if (destroyed) return; cameras[mode] = normalizeFieldCamera(next); initialized[mode] = true; remember(); schedule(); emit(); }
  function fitOverview(): void {
    contextFocus[mode] = ''; setCamera(fitFieldCamera(layout.bounds, viewportSize(), { padding: viewport.clientWidth < 720 ? 16 : 30, maxScale: 1 }));
  }
  function focusContext(contextId: string): void {
    const context = layout.contexts.find(item => item.contextId === contextId); if (!context) return;
    const view = viewportSize(), compact = view.width < 720;
    const core = layout.nodes.filter(node => node.contextId === contextId && !['person', 'device'].includes(node.kind));
    const bounds = compact && core.length ? fieldBounds(core.flatMap(node => node.footprint), 16) : context.bounds;
    const minimum = compact ? Math.max(.55, Math.min(.76, .65 + (view.width - 320) / 70 * .11)) : .45;
    contextFocus[mode] = contextId; setCamera(fitFieldCamera(bounds, view, { padding: compact ? 12 : 30, maxScale: 1, minScale: minimum }));
  }
  function rebuild(): void {
    const priorFocus = document.activeElement instanceof HTMLElement && plane.contains(document.activeElement) ? document.activeElement : null;
    layout = layoutUnifiedField(scene(), entities, metrics);
    if (mode === 'mine' && (filter || visibleKinds?.length)) {
      const query = filter.trim().toLocaleLowerCase('ru'), matchingContexts = new Set(layout.contexts.filter(context => context.title.toLocaleLowerCase('ru').includes(query)).map(context => context.contextId));
      layout.nodes = layout.nodes.filter(node => (!visibleKinds?.length || visibleKinds.includes(node.kind) || node.kind === 'builtin' && visibleKinds.includes('app'))
        && (!query || matchingContexts.has(node.contextId) || `${node.entity.title} ${node.entity.description ?? ''}`.toLocaleLowerCase('ru').includes(query)));
      const kept = new Set(layout.nodes.map(node => node.shortcutId));
      layout.contexts = layout.contexts.filter(context => context.children.some(id => kept.has(id)) || !!query && matchingContexts.has(context.contextId))
        .map(context => ({ ...context, children: context.children.filter(id => kept.has(id)) }));
    }
    index = createFieldSpatialIndex(layout.nodes);
    if (!layout.nodes.some(node => node.shortcutId === selectedId)) selectedId = '';
    if (!layout.contexts.some(context => context.contextId === contextFocus[mode])) contextFocus[mode] = '';
    if (!initialized[mode] && viewport.clientWidth && viewport.clientHeight) {
      if (viewport.clientWidth < 720 && layout.contexts[0]) focusContext(contextFocus[mode] || layout.contexts[0].contextId); else fitOverview();
    }
    render();
    if (priorFocus && !priorFocus.isConnected) (plane.querySelector<HTMLButtonElement>('.uf-node:not(:disabled)[tabindex="0"]') ?? plane.querySelector<HTMLButtonElement>('.uf-context-title'))?.focus({ preventScroll: true });
  }
  function schedule(): void { if (!raf && !destroyed && !inPointerFrame) raf = requestAnimationFrame(() => { raf = 0; flushPointerSample(); render(); }); }
  function available(shortcutId: string): FieldNode | undefined { return layout.nodes.find(node => node.shortcutId === shortcutId); }
  function activeNode(shortcutId: string): FieldNode | undefined { return activeLayout().nodes.find(node => node.shortcutId === shortcutId); }

  function renderNode(target: HTMLButtonElement, node: FieldNode): void {
    const entity = node.entity, kind = entity.entity.kind;
    target.className = `uf-node uf-node-${kind}`; target.replaceChildren();
    if (kind === 'person') {
      const face = el('span', 'uf-avatar'), src = safeImageUrl(entity.avatarUrl) ?? localImage(entity.avatarUrl);
      if (src) { const image = el('img'); image.src = src; image.alt = ''; image.decoding = 'async'; image.loading = 'lazy'; image.referrerPolicy = 'no-referrer';
        image.addEventListener('error', () => face.replaceChildren(el('span', '', initials(entity.title))), { once: true }); face.append(image); }
      else face.append(el('span', '', initials(entity.title)));
      if (!src && entity.avatarRevision && entity.avatarRevision > 0) {
        face.classList.add('sw-avatar'); face.dataset.profileId = entity.entity.id; face.dataset.avatarRevision = String(entity.avatarRevision); face.dataset.avatarLabel = initials(entity.title);
      }
      const label = el('span', 'uf-label uf-person-label'); label.append(el('span', 'uf-label-text', entity.title)); target.append(face, label);
    } else if (kind === 'device') {
      const inner = el('span', 'uf-device-inner'), label = el('span', 'uf-label'); label.append(el('span', 'uf-label-text', entity.title)); inner.append(icon(entity.symbol || 'laptop', 'uf-device-icon'), label);
      // An online signal without a recent observation is not a live presence claim.
      if (typeof entity.updatedAt === 'number' && Date.now() - entity.updatedAt >= 0 && Date.now() - entity.updatedAt <= 45_000 && typeof entity.online === 'boolean') {
        const status = el('span', `uf-status ${entity.online ? 'is-online' : 'is-offline'}`); status.setAttribute('aria-label', entity.online ? 'Устройство в сети' : 'Устройство не в сети'); inner.append(status);
      }
      target.append(inner);
    } else {
      let art: FieldVisualArt | null = null; try { art = options.resolveArt?.(entity) ?? null; } catch { /* Icon fallback remains usable. */ }
      const src = localImage(art?.src);
      if (src) {
        const image = el('img', 'uf-art'); image.src = src; image.alt = ''; image.decoding = 'async'; image.loading = 'lazy'; image.referrerPolicy = 'no-referrer';
        const srcset = art!.srcset.split(',').map(part => part.trim()).filter(Boolean);
        if (srcset.length && srcset.every(part => { const [path, width] = part.split(/\s+/u); return !!localImage(path) && /^\d+w$/.test(width ?? ''); })) image.srcset = art!.srcset;
        image.sizes = art!.sizes;
        const focal = art!.focalPoint; image.style.objectPosition = `${Math.max(0, Math.min(1, focal.x)) * 100}% ${Math.max(0, Math.min(1, focal.y)) * 100}%`;
        target.style.setProperty('--uf-focal-position', image.style.objectPosition);
        for (const name of ['base', 'accent', 'ink'] as const) if (/^#[a-f0-9]{3,8}$/i.test(art!.palette[name])) target.style.setProperty(`--uf-art-${name}`, art!.palette[name]);
        image.addEventListener('error', () => { image.remove(); target.classList.remove('has-art'); }, { once: true }); target.append(image); target.classList.add('has-art');
      }
      target.append(hexFrame(metrics.radius));
      const caption = el('span', 'uf-app-caption uf-label'); caption.append(icon(art?.icon || entity.symbol || (kind === 'community' ? 'people' : 'cells'), 'uf-node-icon'), el('span', 'uf-label-text', entity.title));
      target.append(caption);
    }
    if (entity.unreadCount && entity.unreadCount > 0) target.append(el('span', 'uf-unread', entity.unreadCount > 99 ? '99+' : String(entity.unreadCount)));
  }

  function render(): void {
    if (destroyed || inPointerFrame) return;
    const current = activeLayout(), view = viewportSize(), cam = camera(), level = arranging ? 'detail' : fieldLevelOfDetail(cam.scale, viewport.dataset.lod as 'overview' | 'context' | 'detail' | undefined);
    viewport.dataset.mode = contextFocus[mode] ? 'focus' : 'overview'; viewport.dataset.side = mode; viewport.dataset.lod = level;
    viewport.dataset.persistence = persistence; viewport.classList.toggle('is-arranging', arranging); viewport.classList.toggle('is-moving', !!moving || !!contextPreview);
    viewport.style.setProperty('--uf-scale', String(cam.scale)); viewport.style.setProperty('--uf-label-scale', String(1 / cam.scale));
    plane.style.transform = `translate(${view.width / 2 - cam.x * cam.scale}px,${view.height / 2 - cam.y * cam.scale}px) scale(${cam.scale})`;
    plane.style.transformOrigin = '0 0';
    const corners = [fieldScreenToWorld({ x: 0, y: 0 }, cam, view), fieldScreenToWorld({ x: view.width, y: view.height }, cam, view)];
    const bounds = { left: corners[0]!.x, top: corners[0]!.y, right: corners[1]!.x, bottom: corners[1]!.y };
    const mobileContext = view.width < 720 ? contextFocus[mode] : '';
    const visibleContexts = current.contexts.filter(context => (!mobileContext || context.contextId === mobileContext) && fieldBoundsOverlap(context.bounds, bounds, 120));
    const contextKeep = new Set(visibleContexts.map(context => context.contextId));
    for (const context of visibleContexts) {
      let wrapper = contextElements.get(context.contextId);
      if (!wrapper) {
        wrapper = el('div', 'uf-context'); wrapper.dataset.contextId = context.contextId;
        const svg = document.createElementNS(SVG_NS, 'svg'); svg.classList.add('uf-contour'); svg.setAttribute('aria-hidden', 'true'); svg.append(document.createElementNS(SVG_NS, 'path'));
        const title = el('button', 'uf-context-title'); title.type = 'button'; title.dataset.contextId = context.contextId;
        title.addEventListener('click', () => { if (mode === 'search') options.onContextActivate?.(context.contextId); else if (!arranging) focusContext(context.contextId); }, { signal });
        wrapper.append(svg, title); plane.append(wrapper); contextElements.set(context.contextId, wrapper);
      }
      wrapper.style.left = `${context.body.left}px`; wrapper.style.top = `${context.body.top}px`; wrapper.style.width = `${context.body.right - context.body.left}px`; wrapper.style.height = `${context.body.bottom - context.body.top}px`;
      const svg = wrapper.querySelector('svg')!; svg.setAttribute('viewBox', `${context.body.left} ${context.body.top} ${context.body.right - context.body.left} ${context.body.bottom - context.body.top}`);
      svg.querySelector('path')!.setAttribute('d', context.contour);
      const title = wrapper.querySelector<HTMLButtonElement>('.uf-context-title')!;
      const headerHeight = window.innerWidth <= 720 || level === 'overview' ? 44 : metrics.contextTitleHeight;
      const headerY = context.body.top + metrics.contourPadding - (headerHeight / 2 + 12) / cam.scale;
      title.style.left = `${context.header.x + context.header.width / 2 - context.body.left}px`; title.style.top = `${headerY - context.body.top}px`; title.style.width = `${context.header.width}px`; title.style.height = `${headerHeight}px`;
      const centre = { x: context.header.x + context.header.width / 2, y: headerY };
      const nearest = visibleContexts.filter(other => other.contextId !== context.contextId && Math.abs((other.header.y + other.header.height / 2 - centre.y) * cam.scale) < 72)
        .map(other => Math.abs((other.header.x + other.header.width / 2 - centre.x) * cam.scale));
      const titleWidth = Math.max(44, Math.min(view.width - 24, metrics.contextTitleWidth, ...nearest.map(distance => distance - 14)));
      title.style.setProperty('--uf-title-screen-width', `${titleWidth}px`); title.classList.toggle('is-icon-only', titleWidth < 112);
      const titleSignature = JSON.stringify([mode, context.title, context.children.length]);
      if (title.dataset.signature !== titleSignature) { const copy = el('span', 'uf-context-copy'); copy.append(el('strong', 'uf-context-name', context.title)); title.replaceChildren(icon(mode === 'mine' ? 'cells' : 'people'), copy); title.dataset.signature = titleSignature; }
      title.setAttribute('aria-label', `${context.title}. ${nounCount(context.children.length, 'объект', 'объекта', 'объектов')}`);
      title.tabIndex = level === 'overview' ? 0 : -1;
    }
    for (const [id, element] of contextElements) if (!contextKeep.has(id)) { element.remove(); contextElements.delete(id); }
    const visible = (moving || contextPreview ? createFieldSpatialIndex(current.nodes) : index).query(bounds, 100);
    const retained = new Set(visible.map(node => node.shortcutId));
    if (selectedId) retained.add(selectedId); if (moving) retained.add(moving.shortcutId);
    const overviewIds = new Set(current.contexts.flatMap(context => context.children.slice(0, 3)));
    const nodes = current.nodes.filter(node => (!mobileContext || node.contextId === mobileContext) && retained.has(node.shortcutId) && (level !== 'overview' || overviewIds.has(node.shortcutId) || node.shortcutId === selectedId));
    const keep = new Set(nodes.map(node => node.shortcutId)), activeId = selectedId && keep.has(selectedId) ? selectedId : nodes[0]?.shortcutId;
    for (const node of nodes) {
      let target = nodeElements.get(node.shortcutId);
      if (!target) {
        target = el('button', 'uf-node'); target.type = 'button'; target.dataset.shortcutId = node.shortcutId;
        target.addEventListener('focus', () => { selectedId = node.shortcutId; remember(); schedule(); emit(); }, { signal });
        target.addEventListener('click', () => {
          const item = available(node.shortcutId); if (!item) return;
          selectedId = item.shortcutId; remember();
          if (arranging) { if (moving) previewMove({ contextId: item.contextId, slot: [...item.slot], swap: true }); else beginMove(item.shortcutId); }
          else options.onActivate?.(item.entity, item.shortcutId);
        }, { signal }); nodeElements.set(node.shortcutId, target); plane.append(target);
        target.addEventListener('contextmenu', event => { const current = available(node.shortcutId); if (current && options.onInspect) { event.preventDefault(); selectedId = current.shortcutId; remember(); options.onInspect(current.entity, current.shortcutId); } }, { signal });
      }
      const signature = JSON.stringify(node.entity);
      if (target.dataset.signature !== signature) { renderNode(target, node); target.dataset.signature = signature; }
      target.style.left = `${node.x}px`; target.style.top = `${node.y}px`; target.style.width = `${node.width}px`; target.style.height = `${node.height}px`;
      if (node.kind === 'device') {
        target.style.left = `${node.cx - node.width / cam.scale / 2}px`; target.style.top = `${node.cy - node.height / cam.scale / 2}px`;
        target.style.width = `${node.width / cam.scale}px`; target.style.height = `${node.height / cam.scale}px`;
      } else if (node.kind === 'person') target.style.height = `${Math.max(node.height, 44 / cam.scale)}px`;
      const screenWidth = node.kind === 'device' ? node.width : node.width * cam.scale, screenHeight = node.kind === 'device' ? node.height : node.height * cam.scale;
      target.style.setProperty('--uf-node-screen-width', `${screenWidth}px`); target.style.setProperty('--uf-node-screen-height', `${screenHeight}px`);
      // Search and overview remain navigable. Read-only describes layout changes, never inspection/opening.
      target.disabled = false;
      target.dataset.interactive = String(!target.disabled);
      if (options.onInspect && mode === 'mine' && !target.disabled) {
        let action = inspectElements.get(node.shortcutId);
        if (!action) { action = el('button', 'uf-inspect'); action.type = 'button'; action.dataset.shortcutId = node.shortcutId; action.append(icon('more'));
          action.addEventListener('click', () => { const current = available(node.shortcutId); if (current) { selectedId = current.shortcutId; remember(); options.onInspect?.(current.entity, current.shortcutId); } }, { signal }); plane.append(action); inspectElements.set(node.shortcutId, action); }
        const left = node.kind === 'device' ? node.cx + node.width / cam.scale / 2 : node.x + node.width;
        const top = node.kind === 'device' ? node.cy - node.height / cam.scale / 2 : node.y;
        action.style.left = `${left - 22 / cam.scale}px`; action.style.top = `${top + Math.min(node.height * .2, 32) - 22 / cam.scale}px`;
        action.style.setProperty('--uf-node-screen-width', `${screenWidth}px`); action.style.setProperty('--uf-label-scale', String(1 / cam.scale));
        action.setAttribute('aria-label', `Действия: ${node.entity.title}`); action.tabIndex = node.shortcutId === activeId ? 0 : -1; action.classList.toggle('is-selected', node.shortcutId === selectedId);
      } else { inspectElements.get(node.shortcutId)?.remove(); inspectElements.delete(node.shortcutId); }
      target.classList.toggle('is-selected', node.shortcutId === selectedId); target.classList.toggle('is-moving', node.shortcutId === moving?.shortcutId);
      target.setAttribute('aria-label', `${node.entity.title}${node.entity.description ? `. ${node.entity.description}` : ''}`);
      target.tabIndex = level === 'overview' && !arranging ? -1 : node.shortcutId === activeId ? 0 : -1;
    }
    for (const [id, target] of nodeElements) if (!keep.has(id)) { target.remove(); nodeElements.delete(id); inspectElements.get(id)?.remove(); inspectElements.delete(id); }
    for (const [id, action] of inspectElements) if (mode !== 'mine') { action.remove(); inspectElements.delete(id); }
    plane.querySelector('.uf-drop-slot')?.remove();
    if (moving) {
      const context = current.contexts.find(item => item.contextId === moving!.preview.target.contextId);
      if (context) {
        const point = fieldSlotToPoint(moving.preview.target.slot, metrics), ghost = el('div', `uf-drop-slot ${moving.preview.valid ? 'is-valid' : 'is-invalid'}${moving.preview.occupied ? ' is-swap' : ''}`);
        ghost.style.left = `${context.x + point.x - metrics.radius * SQRT3 / 2}px`; ghost.style.top = `${context.y + point.y - metrics.radius}px`;
        ghost.style.width = `${metrics.radius * SQRT3}px`; ghost.style.height = `${metrics.radius * 2}px`; ghost.append(hexFrame(metrics.radius), icon(moving.preview.occupied ? 'refresh' : 'plus'));
        ghost.setAttribute('aria-hidden', 'true'); plane.append(ghost);
      }
    }
    tools.hidden = !moving;
    if (moving) {
      const ids = fieldDocument.contexts.map(context => context.contextId).join('|');
      if (contextSelect.dataset.contexts !== ids) { contextSelect.replaceChildren(...fieldDocument.contexts.map(context => { const option = el('option', '', context.title); option.value = context.contextId; return option; })); contextSelect.dataset.contexts = ids; }
      contextSelect.value = moving.preview.target.contextId; confirm.disabled = !moving.preview.valid;
      confirm.textContent = moving.preview.occupied ? 'Обменять местами' : 'Переместить сюда';
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
    const change = applyFieldCommand(fieldDocument, command); if (!change.changed) return null;
    cancel(); // A changed document invalidates every preview, including while its ACK is offline.
    if (record) recordFieldHistory(history, change);
    fieldDocument = change.document; persistence = 'saving'; localDurable = false; rebuild(); emit();
    return persist(structuredClone(fieldDocument));
  }
  function beginMove(shortcutId: string): void {
    if (destroyed || mode !== 'mine' || !arranging || !available(shortcutId)) return;
    const item = available(shortcutId)!; selectedId = shortcutId;
    moving = { shortcutId, start: structuredClone(fieldDocument), preview: fieldMovePreview(fieldDocument, entities, shortcutId, { contextId: item.contextId, slot: [...item.slot] }, metrics) };
    render(); announce('Выберите свободное место или объект для обмена. Enter переносит, Escape отменяет.');
  }
  function previewMove(target: FieldMoveTarget): void {
    if (!moving || !same(moving.start, fieldDocument)) { cancel(); return; }
    moving.preview = fieldMovePreview(fieldDocument, entities, moving.shortcutId, target, metrics); render();
    live.textContent = moving.preview.valid ? moving.preview.occupied ? 'Занятое место. Можно обменять два ярлыка.' : 'Свободное место.' : 'Здесь объекты будут перекрываться.';
  }
  async function commitMove(): Promise<FieldCommitResult | null> {
    const move = moving; if (!move || !move.preview.valid || !same(move.start, fieldDocument)) return null;
    moving = null; tools.hidden = true;
    const result = await execute({ type: 'move-shortcut', shortcutId: move.shortcutId, contextId: move.preview.target.contextId, slot: [...move.preview.target.slot], swap: !!move.preview.occupied });
    if (!destroyed) { announce(result ? 'Ярлык перемещён.' : 'Расстановка не изменилась.'); focusNode(move.shortcutId); } return result;
  }
  function cancel(): void {
    pendingPointer = null; moving = null; contextPreview = null;
    // A subsequent pointerup cannot resurrect a cancelled whole-context preview.
    if (gesture && (gesture.type === 'move' || gesture.type === 'context')) gesture.type = 'press';
    render();
  }
  function setArrange(enabled: boolean): void { arranging = enabled && mode === 'mine' && !!options.commit; cancel(); emit(); }
  function focusNode(shortcutId: string): void {
    const node = activeNode(shortcutId); if (!node) return;
    selectedId = shortcutId; const cam = camera(), view = viewportSize(), margin = 40;
    const point = { x: (node.cx - cam.x) * cam.scale + view.width / 2, y: (node.cy - cam.y) * cam.scale + view.height / 2 };
    let x = cam.x, y = cam.y;
    if (point.x - node.width * cam.scale / 2 < margin || point.x + node.width * cam.scale / 2 > view.width - margin) x += (point.x - view.width / 2) / cam.scale;
    if (point.y - node.height * cam.scale / 2 < margin || point.y + node.height * cam.scale / 2 > view.height - margin) y += (point.y - view.height / 2) / cam.scale;
    if (cam.scale < .45) { x = node.cx; y = node.cy; contextFocus[mode] = node.contextId; }
    if (view.width < 720 && contextFocus[mode] !== node.contextId) { contextFocus[mode] = node.contextId; x = node.cx; y = node.cy; }
    cameras[mode] = normalizeFieldCamera({ ...cam, x, y, scale: Math.max(.45, cam.scale) }); remember(); render(); nodeElements.get(shortcutId)?.focus({ preventScroll: true });
  }
  function perform(task: Promise<unknown>): void { void task.catch(error => { if (!destroyed && persistence !== 'error') announce(errorText(error)); }); }
  confirm.addEventListener('click', () => perform(commitMove()), { signal }); cancelButton.addEventListener('click', () => { cancel(); announce('Перенос отменён.'); focusNode(selectedId); }, { signal });
  contextSelect.addEventListener('change', () => { if (moving) { previewMove({ contextId: contextSelect.value, slot: nextFieldSlot(fieldDocument, contextSelect.value, available(moving.shortcutId)?.kind), swap: false }); if (viewport.clientWidth < 720) focusContext(contextSelect.value); } }, { signal });

  plane.addEventListener('keydown', event => {
    if ((event.target as HTMLElement).closest('input,textarea,select,[contenteditable="true"]')) return;
    if (event.shiftKey && event.key === 'F10' && options.onInspect) { const current = available(selectedId); if (current) { event.preventDefault(); options.onInspect(current.entity, current.shortcutId); } return; }
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'z' && !event.shiftKey && mode === 'mine') { event.preventDefault(); perform(undo()); return; }
    if (event.key === 'Escape') { if (moving || contextPreview) { event.preventDefault(); cancel(); announce('Перенос отменён.'); } else if (arranging) setArrange(false); else fitOverview(); return; }
    if (event.key === ' ' && arranging) { event.preventDefault(); if (!moving) beginMove(selectedId); return; }
    if (event.key === 'Enter' && moving) { event.preventDefault(); perform(commitMove()); return; }
    if (!event.ctrlKey && !event.metaKey && ['+', '=', '-', '0'].includes(event.key)) { event.preventDefault(); if (event.key === '0') fitOverview(); else setCamera(zoomFieldCamera(camera(), camera().scale * (event.key === '-' ? .85 : 1.18), { x: viewportSize().width / 2, y: viewportSize().height / 2 }, viewportSize())); return; }
    const directions: Record<string, [number, number]> = { ArrowLeft: [-1, 0], ArrowRight: [1, 0], ArrowUp: [0, -1], ArrowDown: [0, 1] };
    if (!directions[event.key] && !['Home', 'End'].includes(event.key)) return;
    event.preventDefault();
    if (moving && directions[event.key]) {
      if (event.shiftKey) {
        const neighbour = nearestFieldNode(activeLayout().nodes, moving.shortcutId, directions[event.key]!);
        if (neighbour) { previewMove({ contextId: neighbour.contextId, slot: [...neighbour.slot], swap: true }); announce(`Обмен с ${neighbour.entity.title}. Enter подтверждает, Escape отменяет.`); return; }
      }
      const delta: Record<string, [number, number]> = { ArrowLeft: [-1, 0], ArrowRight: [1, 0], ArrowUp: [0, -1], ArrowDown: [0, 1] };
      const shift = delta[event.key]!, target = moving.preview.target;
      previewMove({ contextId: target.contextId, slot: [target.slot[0] + shift[0], target.slot[1] + shift[1]], swap: true }); return;
    }
    const next = event.key === 'Home' ? layout.nodes[0] : event.key === 'End' ? layout.nodes.at(-1) : nearestFieldNode(layout.nodes, selectedId, directions[event.key]!);
    if (next) focusNode(next.shortcutId);
  }, { signal });
  viewport.addEventListener('wheel', event => {
    if (!event.ctrlKey && !event.metaKey) return; event.preventDefault();
    const rect = viewport.getBoundingClientRect(), factor = event.deltaMode === 1 ? 16 : event.deltaMode === 2 ? viewportSize().height : 1;
    setCamera(zoomFieldCamera(camera(), camera().scale * Math.exp(-event.deltaY * factor * .002), { x: event.clientX - rect.left, y: event.clientY - rect.top }, viewportSize()));
  }, { passive: false, signal });
  function localPoint(event: PointerEvent): FieldPoint { const rect = viewport.getBoundingClientRect(); return { x: event.clientX - rect.left, y: event.clientY - rect.top }; }
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
      else if (current.type === 'move' && moving) {
        if (point.x < 0 || point.y < 0 || point.x > viewportSize().width || point.y > viewportSize().height) { moving.preview = { valid: false, reason: 'missing', target: moving.preview.target }; return; }
        const world = fieldScreenToWorld(point, camera(), viewportSize());
        const destination = layout.contexts.find(context => world.x >= context.body.left && world.x <= context.body.right && world.y >= context.body.top && world.y <= context.body.bottom)
          ?? layout.contexts.find(context => context.contextId === available(current.shortcutId)?.contextId);
        if (destination) previewMove({ contextId: destination.contextId, slot: fieldPointToSlot({ x: world.x - destination.x, y: world.y - destination.y }, metrics), swap: true });
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
  function finishGesture(cancelled: boolean): void {
    if (cancelled) pendingPointer = null; else flushPointerSample();
    const current = gesture; gesture = null; if (current?.timer) clearTimeout(current.timer);
    if (current?.moved) suppressClickUntil = performance.now() + 300;
    if (current && viewport.hasPointerCapture(current.pointerId)) viewport.releasePointerCapture(current.pointerId);
    if (cancelled) { cancel(); return; }
    if (current?.type === 'move') perform(commitMove());
    else if (current?.type === 'context' && contextPreview) {
      const change = contextPreview; contextPreview = null; if (change.valid) perform(execute({ type: 'move-context', contextId: change.contextId, x: change.point.x, y: change.point.y })); else render();
    }
    render();
  }
  viewport.addEventListener('pointerdown', event => {
    if (event.button !== 0) return;
    suppressClickUntil = 0; // A new deliberate press is not the previous drag's trailing click.
    if ((event.target as HTMLElement).closest('.uf-move-tools,.uf-inspect')) return;
    const point = localPoint(event); pointers.set(event.pointerId, point);
    if (pointers.size > 1) {
      finishGesture(true); const [a, b] = [...pointers.values()]; pinch = { distance: Math.max(1, Math.hypot(a!.x - b!.x, a!.y - b!.y)),
        world: fieldScreenToWorld({ x: (a!.x + b!.x) / 2, y: (a!.y + b!.y) / 2 }, camera(), viewportSize()), camera: { ...camera() } };
      for (const id of pointers.keys()) viewport.setPointerCapture(id); return;
    }
    const target = (event.target as HTMLElement).closest<HTMLElement>('[data-shortcut-id]'), header = (event.target as HTMLElement).closest<HTMLElement>('.uf-context-title');
    gesture = { pointerId: event.pointerId, x: point.x, y: point.y, camera: { ...camera() }, type: 'press', shortcutId: target?.dataset.shortcutId ?? '', contextId: header?.dataset.contextId ?? '', moved: false,
      point: fieldScreenToWorld(point, camera(), viewportSize()) };
    if (mode === 'mine' && gesture.shortcutId && options.commit && !arranging) {
      const captured = gesture; captured.timer = setTimeout(() => { if (gesture !== captured || captured.moved || destroyed) return; arranging = true; beginMove(captured.shortcutId); captured.type = 'move'; captured.moved = true; viewport.setPointerCapture(captured.pointerId); suppressClickUntil = performance.now() + 300; emit(); }, 450);
    }
  }, { signal });
  viewport.addEventListener('pointermove', event => {
    if (!pointers.has(event.pointerId)) return; const point = localPoint(event); pointers.set(event.pointerId, point);
    if (pinch && pointers.size >= 2) {
      pendingPointer = { pinch }; schedule(); return;
    }
    const current = gesture; if (!current || current.pointerId !== event.pointerId) return;
    const distance = Math.hypot(point.x - current.x, point.y - current.y);
    if (!current.moved && distance > 7) {
      current.moved = true; if (current.timer) clearTimeout(current.timer); viewport.setPointerCapture(event.pointerId);
      if (arranging && mode === 'mine' && current.shortcutId) { beginMove(current.shortcutId); current.type = 'move'; }
      else if (arranging && mode === 'mine' && current.contextId) current.type = 'context'; else current.type = 'pan';
    }
    if (current.type !== 'press') { pendingPointer = { gesture: current, point }; schedule(); }
  }, { signal });
  viewport.addEventListener('pointerup', event => {
    if (!pointers.has(event.pointerId)) return;
    if (pinch) { flushPointerSample(); pointers.delete(event.pointerId); pinch = null; suppressClickUntil = performance.now() + 300; finishGesture(true); return; }
    pointers.delete(event.pointerId);
    const point = localPoint(event), view = viewportSize();
    if (gesture?.pointerId === event.pointerId && gesture.type !== 'press') pendingPointer = { gesture, point };
    finishGesture(point.x < 0 || point.y < 0 || point.x > view.width || point.y > view.height);
  }, { signal });
  viewport.addEventListener('pointercancel', event => { pointers.delete(event.pointerId); pinch = null; finishGesture(true); }, { signal });
  viewport.addEventListener('lostpointercapture', () => { if (gesture) finishGesture(true); }, { signal });
  window.addEventListener('pointerup', event => {
    if (viewport.contains(event.target as Node) || !pointers.has(event.pointerId)) return;
    pointers.delete(event.pointerId); pinch = null; finishGesture(true);
  }, { signal });
  window.addEventListener('blur', () => { pointers.clear(); pinch = null; finishGesture(true); }, { signal });
  viewport.addEventListener('click', event => {
    if (event.detail > 0 && performance.now() < suppressClickUntil) { event.preventDefault(); event.stopPropagation(); return; }
    if (arranging && moving && !(event.target as HTMLElement).closest('.uf-node,.uf-context-title,.uf-move-tools')) {
      const rect = viewport.getBoundingClientRect(), world = fieldScreenToWorld({ x: event.clientX - rect.left, y: event.clientY - rect.top }, camera(), viewportSize());
      const context = layout.contexts.find(item => fieldBoundsOverlap(item.body, { left: world.x, right: world.x + 1, top: world.y, bottom: world.y + 1 }));
      if (context) previewMove({ contextId: context.contextId, slot: fieldPointToSlot({ x: world.x - context.x, y: world.y - context.y }, metrics), swap: true });
    }
  }, { capture: true, signal });
  let lastWidth = 0, lastHeight = 0;
  const resize = new ResizeObserver(() => {
    const width = viewport.clientWidth, height = viewport.clientHeight;
    const changed = Math.abs(width - lastWidth) > 40 || Math.abs(height - lastHeight) > 40;
    lastWidth = width; lastHeight = height;
    if (!initialized[mode]) rebuild();
    else if (changed && contextFocus[mode] && !gesture && !moving) focusContext(contextFocus[mode]);
    else render();
  }); resize.observe(viewport);
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
    return execute({ type: 'move-shortcut', shortcutId, contextId: target.contextId, slot: [...target.slot], swap: target.swap === true });
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
      if (update.entities) entities = thinEntities(update.entities);
      if (update.filter !== undefined) filter = update.filter;
      if (update.visibleKinds !== undefined) visibleKinds = [...update.visibleKinds];
      if (update.document && (update.acceptRemote === true || persistence === 'saved' || same(update.document, fieldDocument))) {
        const next = validateFieldDocument(update.document), differs = !same(next, fieldDocument);
        if (update.acceptRemote && differs) { generation++; cancel(); history.entries.length = 0; selectedId = ''; }
        fieldDocument = next; if (update.revision !== undefined) revision = update.revision;
        if (update.persistence === 'saved' || update.acceptRemote === true) { persistence = 'saved'; localDurable = true; lastIntent = null; }
        else if (update.persistence === 'pending') { persistence = 'pending'; localDurable = update.localDurable === true; }
        else if (update.persistence === 'conflict') { persistence = 'conflict'; localDurable = false; }
      }
      if (update.searchDocument) searchDocument = structuredClone(update.searchDocument);
      if (update.mode && update.mode !== mode) { finishGesture(true); remember(); mode = update.mode; arranging = false; selectedId = selections[mode]; }
      if (update.scope !== undefined && update.scope !== scope) { finishGesture(true); scope = update.scope; if (mode === 'search') { initialized.search = false; contextFocus.search = ''; selectedId = ''; selections.search = ''; } }
      if (moving) { const source = fieldDocument.shortcuts.find(item => item.shortcutId === moving!.shortcutId); if (!source || !same(moving.start, fieldDocument) || !entities.some(entity => fieldEntityKey(entity.entity) === fieldEntityKey(source.entity))) cancel(); }
      if (update.selectedShortcutId !== undefined) selectedId = update.selectedShortcutId;
      rebuild(); emit();
    },
    getCamera: () => ({ ...camera() }), setCamera, focusContext, fitOverview, setArrange,
    addContext: (title, point) => {
      const destination = point ?? { x: layout.bounds ? layout.bounds.right + 400 : 0, y: layout.bounds ? (layout.bounds.top + layout.bounds.bottom) / 2 : 0 };
      return execute({ type: 'add-context', contextId: crypto.randomUUID(), title, ...destination });
    },
    renameContext: (contextId, title) => execute({ type: 'rename-context', contextId, title }),
    removeContext: (contextId, remove = {}) => execute({ type: 'remove-context', contextId, removeShortcuts: remove.removeShortcuts === true }),
    addShortcut: (entity, contextId) => execute({ type: 'add-shortcut', shortcutId: crypto.randomUUID(), entity, contextId }),
    removeShortcut: shortcutId => execute({ type: 'remove-shortcut', shortcutId }), moveShortcut, moveContext, beginMove, previewMove, commitMove, undo, cancel,
    hasUnsavedChanges: () => persistence !== 'saved' && !localDurable,
    async flush() {
      await tail.catch(() => undefined);
      if (persistence === 'saved' || localDurable) return;
      if (persistence === 'conflict') throw Object.assign(new Error('field_conflict'), { code: 'field_conflict' });
      const intent = lastIntent && same(lastIntent.document, fieldDocument) ? lastIntent : undefined;
      const result = await save(structuredClone(fieldDocument), intent);
      if (result.status !== 'saved' && !localDurable) throw Object.assign(new Error('field_not_saved'), { code: 'field_not_saved' });
    },
    snapshot: () => structuredClone(fieldDocument),
    destroy() { if (destroyed) return; remember(); finishGesture(true); destroyed = true; generation++; controller.abort(); resize.disconnect(); clearInterval(ttl); if (raf) cancelAnimationFrame(raf); nodeElements.clear(); contextElements.clear(); inspectElements.clear(); entities = []; viewport.remove(); if (!readySettled) { readySettled = true; resolveReady(); } },
  };
}
