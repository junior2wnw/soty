import { avatar, el, nounCount } from './dom';
import { icon } from './icons';
import { entityId, worldColor, type WorldEntity, type WorldCommunity, type WorldAppRecord } from './types';
import { HEX_FLOWER, hexCluster, polygonPoints, roundedHexPath, SQRT3 } from '../geometry/hex.mjs';
import { placeHex } from '../geometry/dom';
import { clampFieldScale, fieldItemVisible, fieldNeighbour, layoutField, type FieldItem, type FieldLayout, type FieldShape } from './field-map-layout.mjs';
import { createFieldLayoutState, type FieldLayoutState } from './field-layout.mjs';
import { stabilizeFieldMap } from './field-map-state.mjs';
import './field.css';

const ISLAND = hexCluster(HEX_FLOWER, 44, 6, 14);
const COMPACT_ISLAND = hexCluster(HEX_FLOWER, 25, 4, 8);
const ISLAND_TITLE_HEIGHT = 62;

export function communityEmblem(community: WorldCommunity, size = ''): HTMLElement {
  const emblem = el('span', `sw-emblem sw-color-${worldColor(community.color)} ${size}`);
  emblem.append(icon(community.symbol || 'cells'));
  return emblem;
}

export function communityIsland(community: WorldCommunity, selected = false, compact = false): HTMLElement {
  const shape = compact ? COMPACT_ISLAND : ISLAND, titleHeight = compact ? 0 : ISLAND_TITLE_HEIGHT;
  const island = el('span', `sw-island sw-color-${worldColor(community.color)}${selected ? ' is-selected' : ''}${compact ? ' is-compact' : ''}`);
  island.style.setProperty('--island-shape-width', `${shape.width}px`);
  const outline = document.createElementNS('http://www.w3.org/2000/svg', 'svg'); outline.classList.add('sw-island-outline'); outline.setAttribute('viewBox', `0 0 ${shape.width} ${shape.height + titleHeight}`); outline.setAttribute('aria-hidden', 'true');
  for (const loop of shape.loops) {
    const boundary = document.createElementNS('http://www.w3.org/2000/svg', 'polygon');
    boundary.setAttribute('points', polygonPoints(loop.map(point => ({ x: point.x, y: point.y + titleHeight }))));
    outline.append(boundary);
  }
  island.append(outline);
  const title = el('span', 'sw-island-title');
  const relation = community.membership?.state === 'active' ? 'Вы в группе' : community.joinPolicy === 'open' ? 'Открыто' : community.joinPolicy === 'request' ? 'По заявке' : 'По приглашению';
  title.append(el('strong', '', community.name), el('span', '', `${nounCount(community.memberCount, 'участник', 'участника', 'участников')} · ${relation}`));
  island.append(title);
  const centre = communityEmblem(community, 'sw-island-centre');
  placeHex(centre, shape, 0, titleHeight);
  island.append(centre);
  const preview = community.previewMembers.slice(0, 5);
  preview.forEach((profile, index) => {
    const face = avatar(profile.displayName, profile.avatarUrl, worldColor(profile.avatarColor), profile.profileId, profile.avatarRevision);
    face.classList.add('sw-island-face', `sw-island-face-${index}`);
    placeHex(face, shape, index + 1, titleHeight);
    face.title = profile.displayName;
    island.append(face);
  });
  for (let index = preview.length; index < 5; index++) {
    const vacant = el('span', `sw-island-vacant sw-island-face sw-island-face-${index}`);
    placeHex(vacant, shape, index + 1, titleHeight);
    if (index === preview.length && community.memberCount === 1) vacant.append(icon('plus'));
    island.append(vacant);
  }
  const others = el('span', 'sw-island-others');
  placeHex(others, shape, 6, titleHeight);
  const otherCount = Math.max(0, community.memberCount - preview.length);
  others.append(el('span', '', otherCount > 99 ? '99+' : otherCount ? `+${otherCount}` : ''));
  if (otherCount) others.title = nounCount(otherCount, 'участник', 'участника', 'участников');
  island.append(others);
  if (community.unreadCount > 0) island.append(el('span', 'sw-unread', community.unreadCount > 99 ? '99+' : String(community.unreadCount)));
  return island;
}

export interface HexFieldOptions {
  state?: HexFieldState;
  onSelectApp?: (app: WorldAppRecord) => void;
  /** The approved asset resolver. Local images are optional; icon fallback is always honest. */
  resolveAppArt?: (app: WorldAppRecord) => string | null;
  onScaleChange?: (scale: number) => void;
}
export interface HexFieldState {
  layout: FieldLayoutState;
  scrollLeft: number;
  scrollTop: number;
  scale?: number;
}
export function createHexFieldState(): HexFieldState {
  return { layout: createFieldLayoutState(), scrollLeft: 0, scrollTop: 0 };
}
export interface HexField {
  element: HTMLElement;
  update(entities: WorldEntity[], selectedId?: string): void;
  setApps(apps: WorldAppRecord[]): void;
  setScale(scale: number): void;
  getScale(): number;
  resetView(): void;
  destroy(): void;
}

const SVG_NS = 'http://www.w3.org/2000/svg';

function installFieldClip(): void {
  if (document.getElementById('sw-field-hex-defs')) return;
  const svg = document.createElementNS(SVG_NS, 'svg'); svg.id = 'sw-field-hex-defs';
  svg.classList.add('soty-geometry-defs'); svg.setAttribute('aria-hidden', 'true'); svg.setAttribute('focusable', 'false');
  const defs = document.createElementNS(SVG_NS, 'defs');
  const clip = document.createElementNS(SVG_NS, 'clipPath'); clip.id = 'sw-field-hex-clip'; clip.setAttribute('clipPathUnits', 'objectBoundingBox');
  const path = document.createElementNS(SVG_NS, 'path'); path.setAttribute('d', roundedHexPath(1, .12));
  path.setAttribute('transform', `matrix(0 .5 ${-1 / SQRT3} 0 .5 .5)`);
  clip.append(path); defs.append(clip); svg.append(defs); (document.body ?? document.documentElement).append(svg);
}

function tileFrame(radius: number): SVGSVGElement {
  const frame = document.createElementNS(SVG_NS, 'svg'); frame.classList.add('sw-field-tile-frame');
  frame.setAttribute('viewBox', `0 0 ${SQRT3 * radius} ${radius * 2}`); frame.setAttribute('aria-hidden', 'true');
  const path = document.createElementNS(SVG_NS, 'path'); path.setAttribute('d', roundedHexPath(radius - 1.5, radius * .12));
  path.setAttribute('transform', `translate(${SQRT3 * radius / 2} ${radius}) rotate(90)`); frame.append(path);
  return frame;
}

function fieldCommunityShell(community: WorldCommunity, shape: FieldShape, hasApps: boolean, selected: boolean): HTMLElement {
  const shell = el('span', `sw-constellation sw-color-${worldColor(community.color)}${selected ? ' is-selected' : ''}`);
  const contour = document.createElementNS(SVG_NS, 'svg'); contour.classList.add('sw-constellation-contour');
  contour.setAttribute('viewBox', `0 0 ${shape.width} ${shape.height}`); contour.setAttribute('aria-hidden', 'true');
  const path = document.createElementNS(SVG_NS, 'path'); path.setAttribute('d', shape.contour); contour.append(path); shell.append(contour);
  const title = el('span', 'sw-constellation-label');
  title.append(icon('people'), el('span', 'sw-constellation-copy'));
  title.lastElementChild!.append(el('strong', '', community.name), el('small', '', community.description.trim() || nounCount(community.memberCount, 'участник', 'участника', 'участников')));
  if (community.membership?.state === 'active') { const joined = icon('check', 'sw-constellation-member'); title.append(joined); }
  if (community.unreadCount > 0) title.append(el('span', 'sw-constellation-unread', community.unreadCount > 99 ? '99+' : String(community.unreadCount)));
  shell.append(title);
  if (!hasApps) {
    shape.cells.forEach((cell, index) => {
      const tile = el('span', `sw-field-core${index ? ' is-empty' : ''}`);
      tile.style.left = `${cell.left}px`; tile.style.top = `${cell.top}px`; tile.style.width = `${shape.tileWidth}px`; tile.style.height = `${shape.tileHeight}px`;
      tile.append(tileFrame(shape.radius));
      if (!index) { const content = el('span', 'sw-field-core-content'); content.append(icon(community.symbol || 'cells')); tile.append(content); }
      tile.setAttribute('aria-hidden', 'true'); shell.append(tile);
    });
  }
  return shell;
}

/** Native touch scrolling, batched viewport rendering, and one GPU scale for the whole map. */
export function createHexField(onSelect: (entity: WorldEntity) => void, initialScale = 1, stateOrOptions: HexFieldState | HexFieldOptions = {}, extraOptions: HexFieldOptions = {}): HexField {
  const suppliedState = 'layout' in stateOrOptions;
  const options = suppliedState ? extraOptions : stateOrOptions;
  const state = suppliedState ? stateOrOptions : options.state ?? createHexFieldState();
  installFieldClip();
  const viewport = el('div', 'sw-world-viewport'); viewport.dataset.fieldMap = '';
  viewport.setAttribute('role', 'region'); viewport.setAttribute('aria-label', 'Поле приложений, людей и сообществ');
  const dimensions = el('div', 'sw-world-dimensions');
  const plane = el('div', 'sw-world-plane'); plane.setAttribute('role', 'grid');
  plane.setAttribute('aria-label', 'Соты. Стрелки выбирают, Enter открывает, плюс и минус меняют масштаб.'); plane.setAttribute('aria-colcount', '1');
  const links = document.createElementNS(SVG_NS, 'svg'); links.classList.add('sw-field-links'); links.setAttribute('aria-hidden', 'true');
  plane.append(links); dimensions.append(plane); viewport.append(dimensions);
  let entities: WorldEntity[] = [], apps: WorldAppRecord[] = [], selectedId = '', focusedId = '';
  let scale = clampFieldScale(state.scale ?? initialScale), offsetX = 0, raf = 0, destroyed = false, suppressClickUntil = 0;
  let map: FieldLayout = layoutField([], [], 900, 640);
  const slots = new Map<string, number>(), nodes = new Map<string, HTMLElement>();
  const controller = new AbortController(), signal = controller.signal;

  function artSource(app: WorldAppRecord): string | null {
    try {
      const source = options.resolveAppArt?.(app); if (!source) return null;
      const url = new URL(source, document.baseURI);
      return /^https?:$/.test(url.protocol) && url.origin === location.origin ? url.href : null;
    } catch { return null; }
  }

  function updateDimensions(): void {
    const viewportWidth = viewport.clientWidth || 900;
    offsetX = Math.max(0, (viewportWidth - map.width * scale) / 2);
    dimensions.style.width = `${Math.max(viewportWidth, map.width * scale)}px`;
    dimensions.style.height = `${Math.max(viewport.clientHeight, map.height * scale)}px`;
    plane.style.width = `${map.width}px`; plane.style.height = `${map.height}px`; plane.style.left = `${offsetX}px`;
    plane.style.transform = `scale(${scale})`; viewport.dataset.scale = String(Math.round(scale * 100));
  }

  function renderConnections(): void {
    links.setAttribute('viewBox', `0 0 ${map.width} ${map.height}`); links.setAttribute('width', String(map.width)); links.setAttribute('height', String(map.height));
    const fragment = document.createDocumentFragment();
    for (const connection of map.connections) {
      const { from, to } = connection;
      const distance = Math.hypot(to.x - from.x, to.y - from.y) || 1;
      const start = connection.kind === 'person' ? { x: from.x + (to.x - from.x) * 35 / distance, y: from.y + (to.y - from.y) * 35 / distance } : from;
      const path = document.createElementNS(SVG_NS, 'path'); path.classList.add(`sw-field-link-${connection.kind}`);
      path.setAttribute('d', `M ${start.x} ${start.y} Q ${(start.x + to.x) / 2} ${(start.y + to.y) / 2 - (connection.kind === 'person' ? 12 : 0)} ${to.x} ${to.y}`);
      path.dataset.selected = String(selectedId === `community:${connection.communityId}`); fragment.append(path);
      const dot = document.createElementNS(SVG_NS, 'circle'); dot.setAttribute('cx', String(start.x)); dot.setAttribute('cy', String(start.y)); dot.setAttribute('r', connection.kind === 'person' ? '2.5' : '2'); fragment.append(dot);
    }
    links.replaceChildren(fragment);
  }

  function layout(): void {
    if (destroyed) return;
    const stable = [...entities].sort((a, b) => (slots.get(entityId(a)) ?? 0) - (slots.get(entityId(b)) ?? 0));
    const width = viewport.clientWidth || 900;
    map = stabilizeFieldMap(state.layout, layoutField(stable, apps, width, viewport.clientHeight || 640), width);
    if (!map.items.some(item => item.id === focusedId)) focusedId = '';
    viewport.classList.toggle('is-compact', map.compact);
    plane.setAttribute('aria-rowcount', String(map.items.length));
    updateDimensions();
    viewport.scrollLeft = state.scrollLeft; viewport.scrollTop = state.scrollTop;
    renderConnections(); render();
  }

  function itemLabel(item: FieldItem): string {
    if (item.kind === 'app') return `${item.app.name}, приложение${item.app.description ? `. ${item.app.description}` : ''}`;
    if (item.kind === 'overflow') return `Ещё ${nounCount(item.count, 'приложение', 'приложения', 'приложений')}. Открыть ${item.entity.value.name}`;
    if (item.kind === 'person') return item.entity.value.displayName;
    return `${item.entity.value.name}, ${nounCount(item.entity.value.memberCount, 'участник', 'участника', 'участников')}${item.entity.value.membership?.state === 'active' ? '. Вы участник' : ''}`;
  }

  function renderItem(target: HTMLButtonElement, item: FieldItem, source: string | null): void {
    target.className = `sw-field-target sw-field-${item.kind}${item.kind === 'app' ? ` sw-color-${worldColor(item.app.color)}` : ''}`;
    if (item.kind === 'community') target.replaceChildren(fieldCommunityShell(item.entity.value, item.shape, item.hasApps, item.id === selectedId));
    else if (item.kind === 'person') {
      const profile = item.entity.value, face = avatar(profile.displayName, profile.avatarUrl, worldColor(profile.avatarColor), profile.profileId, profile.avatarRevision);
      face.classList.add('sw-field-face'); target.replaceChildren(face, el('span', 'sw-field-person-name', profile.displayName));
    } else {
      target.replaceChildren();
      if (item.kind === 'app' && source) {
        const cover = el('img', 'sw-field-app-art'); cover.src = source; cover.alt = ''; cover.loading = 'lazy'; cover.decoding = 'async'; cover.referrerPolicy = 'no-referrer';
        cover.addEventListener('error', () => { cover.remove(); target.classList.remove('has-art'); }, { once: true });
        target.append(cover); target.classList.add('has-art');
      }
      target.append(tileFrame(item.radius));
      const content = el('span', 'sw-field-app-content');
      content.append(icon(item.kind === 'app' ? item.app.symbol || 'cells' : 'more'));
      content.append(el('strong', '', item.kind === 'app' ? item.app.name : `+${item.count}`));
      const detail = item.kind === 'app' ? item.app.description || '' : nounCount(item.count, 'приложение', 'приложения', 'приложений');
      if (detail) content.append(el('small', '', detail));
      target.append(content);
    }
  }

  function itemSignature(item: FieldItem, source: string | null): string {
    if (item.kind === 'app') return JSON.stringify([item.app.name, item.app.description, item.app.symbol, item.app.color, item.app.status, item.radius, source]);
    if (item.kind === 'overflow') return JSON.stringify([item.count, item.radius]);
    if (item.kind === 'person') return JSON.stringify([item.entity.value.revision, item.entity.value.avatarRevision, item.entity.value.displayName, item.entity.value.avatarColor]);
    const group = item.entity.value;
    return JSON.stringify([group.revision, group.name, group.description, group.color, group.symbol, group.unreadCount, group.memberCount, group.membership?.state, item.shape.radius, item.appCount, item.id === selectedId]);
  }

  function render(): void {
    if (destroyed) return;
    const bounds = { left: (viewport.scrollLeft - offsetX) / scale, top: viewport.scrollTop / scale,
      right: (viewport.scrollLeft + viewport.clientWidth - offsetX) / scale, bottom: (viewport.scrollTop + viewport.clientHeight) / scale };
    const keep = new Set<string>();
    map.items.forEach((item, index) => {
      if (item.id !== focusedId && !fieldItemVisible(item, bounds)) return;
      keep.add(item.id);
      let node = nodes.get(item.id);
      if (!node) {
        node = el('div', `sw-field-row sw-field-${item.kind}-row`); node.setAttribute('role', 'row');
        const cell = el('div'); cell.setAttribute('role', 'gridcell');
        const target = el('button', 'sw-field-target'); target.type = 'button'; target.dataset.entityId = item.id;
        target.addEventListener('click', () => {
          const current = map.items.find(candidate => candidate.id === item.id); if (!current) return;
          if (current.kind === 'app') options.onSelectApp?.(current.app); else onSelect(current.entity);
        }, { signal });
        target.addEventListener('focus', () => { focusedId = item.id; updateTabStops(); }, { signal });
        cell.append(target); node.append(cell); nodes.set(item.id, node); plane.append(node);
      }
      const target = node.querySelector<HTMLButtonElement>('button')!;
      const source = item.kind === 'app' ? artSource(item.app) : null, signature = itemSignature(item, source);
      if (node.dataset.signature !== signature) { renderItem(target, item, source); node.dataset.signature = signature; }
      target.setAttribute('aria-label', itemLabel(item));
      if (item.kind === 'app' || item.kind === 'overflow') target.removeAttribute('aria-pressed'); else target.setAttribute('aria-pressed', String(item.id === selectedId));
      node.setAttribute('aria-rowindex', String(index + 1));
      node.style.left = `${item.x}px`; node.style.top = `${item.y}px`; node.style.width = `${item.width}px`; node.style.height = `${item.height}px`;
    });
    for (const [id, node] of nodes) if (!keep.has(id)) { node.remove(); nodes.delete(id); }
    updateTabStops();
  }

  function updateTabStops(): void {
    const active = focusedId && nodes.has(focusedId) ? focusedId : [...nodes].find(([id]) => id === selectedId)?.[0] ?? nodes.keys().next().value;
    for (const [id, node] of nodes) node.querySelector('button')!.tabIndex = id === active ? 0 : -1;
  }

  function schedule(): void { if (!raf && !destroyed) raf = requestAnimationFrame(() => { raf = 0; render(); }); }
  function changeScale(next: number, anchorX = viewport.clientWidth / 2, anchorY = viewport.clientHeight / 2): void {
    const zoom = clampFieldScale(next); if (zoom === scale || destroyed) return;
    const anchor = { x: (viewport.scrollLeft + anchorX - offsetX) / scale, y: (viewport.scrollTop + anchorY) / scale };
    scale = zoom; state.scale = scale; updateDimensions();
    viewport.scrollTo({ left: Math.max(0, anchor.x * scale + offsetX - anchorX), top: Math.max(0, anchor.y * scale - anchorY), behavior: 'instant' });
    state.scrollLeft = viewport.scrollLeft; state.scrollTop = viewport.scrollTop;
    render(); options.onScaleChange?.(scale);
  }
  function resetView(): void {
    changeScale(1, 0, 0); viewport.scrollTo({ left: 0, top: 0, behavior: 'instant' }); state.scrollLeft = 0; state.scrollTop = 0; state.scale = 1; render();
  }

  viewport.addEventListener('scroll', () => { state.scrollLeft = viewport.scrollLeft; state.scrollTop = viewport.scrollTop; schedule(); }, { passive: true, signal });
  viewport.addEventListener('wheel', event => {
    if (!event.ctrlKey && !event.metaKey) return;
    event.preventDefault();
    const rect = viewport.getBoundingClientRect(), delta = event.deltaY * (event.deltaMode === 1 ? 16 : event.deltaMode === 2 ? viewport.clientHeight : 1);
    changeScale(scale * Math.exp(-delta * .002), event.clientX - rect.left, event.clientY - rect.top);
  }, { passive: false, signal });
  plane.addEventListener('keydown', event => {
    if (!event.ctrlKey && !event.metaKey && ['+', '=', '-', '0'].includes(event.key)) {
      event.preventDefault(); if (event.key === '0') resetView(); else changeScale(scale + (event.key === '-' ? -.12 : .12)); return;
    }
    if (!['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown', 'Home', 'End'].includes(event.key)) return;
    event.preventDefault();
    const directions: Record<string, readonly [number, number]> = { ArrowLeft: [-1, 0], ArrowRight: [1, 0], ArrowUp: [0, -1], ArrowDown: [0, 1] };
    const next = event.key === 'Home' ? map.items[0] : event.key === 'End' ? map.items.at(-1) : fieldNeighbour(map.items, focusedId, directions[event.key]!);
    if (!next) return;
    focusedId = next.id; render(); nodes.get(focusedId)?.querySelector<HTMLButtonElement>('button')?.focus({ preventScroll: true });
    const margin = 30, left = next.x * scale + offsetX, top = next.y * scale;
    viewport.scrollTo({ left: Math.max(0, left < viewport.scrollLeft + margin ? left - margin : left + next.width * scale > viewport.scrollLeft + viewport.clientWidth - margin ? left + next.width * scale - viewport.clientWidth + margin : viewport.scrollLeft),
      top: Math.max(0, top < viewport.scrollTop + margin ? top - margin : top + next.height * scale > viewport.scrollTop + viewport.clientHeight - margin ? top + next.height * scale - viewport.clientHeight + margin : viewport.scrollTop), behavior: 'instant' });
  }, { signal });

  let drag: { x: number; y: number; left: number; top: number; pointerId: number; moved: boolean } | null = null;
  viewport.addEventListener('pointerdown', event => {
    if (event.pointerType === 'touch' || event.button !== 0) return;
    drag = { x: event.clientX, y: event.clientY, left: viewport.scrollLeft, top: viewport.scrollTop, pointerId: event.pointerId, moved: false };
  }, { signal });
  viewport.addEventListener('pointermove', event => {
    if (!drag || event.pointerId !== drag.pointerId) return;
    if (!event.buttons) { endDrag(); return; }
    if (!drag.moved && Math.hypot(event.clientX - drag.x, event.clientY - drag.y) > 6) {
      drag.moved = true; viewport.setPointerCapture(event.pointerId); viewport.classList.add('is-dragging');
    }
    if (drag.moved) { viewport.scrollLeft = drag.left - event.clientX + drag.x; viewport.scrollTop = drag.top - event.clientY + drag.y; }
  }, { signal });
  const endDrag = (): void => {
    if (drag?.moved) { suppressClickUntil = performance.now() + 300; if (viewport.hasPointerCapture(drag.pointerId)) viewport.releasePointerCapture(drag.pointerId); }
    drag = null; viewport.classList.remove('is-dragging');
  };
  viewport.addEventListener('pointerup', endDrag, { signal }); viewport.addEventListener('pointercancel', endDrag, { signal }); viewport.addEventListener('lostpointercapture', endDrag, { signal });
  viewport.addEventListener('click', event => { if (performance.now() < suppressClickUntil) { event.preventDefault(); event.stopPropagation(); } }, { capture: true, signal });
  const resize = new ResizeObserver(layout); resize.observe(viewport);

  return {
    element: viewport,
    update(next, selected = '') {
      const incoming = new Set(next.map(entityId));
      const retained = [...slots].filter(([id]) => incoming.has(id)).sort((a, b) => a[1] - b[1]);
      slots.clear(); retained.forEach(([id], index) => slots.set(id, index));
      next.forEach(entity => { if (!slots.has(entityId(entity))) slots.set(entityId(entity), slots.size); });
      entities = next; selectedId = selected; layout();
    },
    setApps(next) { apps = [...next]; layout(); },
    setScale: changeScale,
    getScale: () => scale,
    resetView,
    destroy() { state.scrollLeft = viewport.scrollLeft; state.scrollTop = viewport.scrollTop; state.scale = scale; destroyed = true; controller.abort(); resize.disconnect(); if (raf) cancelAnimationFrame(raf); nodes.clear(); slots.clear(); entities = []; apps = []; },
  };
}
