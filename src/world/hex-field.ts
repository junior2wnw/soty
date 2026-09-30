import { avatar, el, nounCount } from './dom';
import { icon } from './icons';
import { entityId, entityName, worldColor, type WorldEntity, type WorldCommunity } from './types';
import { HEX_FLOWER, hexCluster, polygonPoints } from '../geometry/hex.mjs';
import { placeHex } from '../geometry/dom';
import { createFieldLayoutState, stableFieldLayout, type FieldLayoutState } from './field-layout.mjs';

const ISLAND = hexCluster(HEX_FLOWER, 44, 6, 14);
const COMPACT_ISLAND = hexCluster(HEX_FLOWER, 25, 4, 8);
const ISLAND_TITLE_HEIGHT = 62;
const ISLAND_HEIGHT = ISLAND.height + ISLAND_TITLE_HEIGHT;
const FIELD_PADDING = 22;
const ISLAND_GAP = 28;

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

interface Position { entity: WorldEntity; x: number; y: number; width: number; height: number; }
export interface HexField { element: HTMLElement; update(entities: WorldEntity[], selectedId?: string): void; setScale(scale: number): void; destroy(): void; }
export interface HexFieldState { layout: FieldLayoutState; scrollLeft: number; scrollTop: number; }
export function createHexFieldState(): HexFieldState { return { layout: createFieldLayoutState(), scrollLeft: 0, scrollTop: 0 }; }

/** Native scrolling preserves touch panning and browser pinch zoom; rendering is viewport bounded. */
export function createHexField(onSelect: (entity: WorldEntity) => void, initialScale = 1, state = createHexFieldState()): HexField {
  const viewport = el('div', 'sw-world-viewport');
  viewport.setAttribute('aria-label', 'Поле людей и сообществ');
  const dimensions = el('div', 'sw-world-dimensions');
  const plane = el('div', 'sw-world-plane');
  plane.setAttribute('role', 'grid');
  plane.setAttribute('aria-label', 'Соты. Стрелки перемещают выбор, Enter открывает.');
  dimensions.append(plane); viewport.append(dimensions);
  let entities: WorldEntity[] = [], positions: Position[] = [], selectedId = '', focusedId = '';
  let zoom = initialScale, scale = initialScale, raf = 0, columns = 2, compact = false, destroyed = false;
  const nodes = new Map<string, HTMLElement>();
  const controller = new AbortController();
  const signal = controller.signal;

  function layout(): void {
    scale = zoom * Math.min(1, (viewport.clientWidth || 900) / (ISLAND.width + FIELD_PADDING * 2));
    const logicalWidth = (viewport.clientWidth || 900) / scale;
    compact = viewport.clientWidth < 600;
    const islandWidth = compact ? Math.max(COMPACT_ISLAND.width + 92, logicalWidth - FIELD_PADDING * 2) : ISLAND.width;
    const islandHeight = compact ? COMPACT_ISLAND.height : ISLAND_HEIGHT;
    columns = compact ? 1 : Math.max(1, Math.min(4, Math.floor((logicalWidth - FIELD_PADDING * 2 + ISLAND_GAP) / (islandWidth + ISLAND_GAP))));
    const width = Math.max(FIELD_PADDING * 2 + columns * islandWidth + (columns - 1) * ISLAND_GAP, logicalWidth);
    const pitch = (width - FIELD_PADDING * 2 + ISLAND_GAP) / columns;
    const firstColumnInset = FIELD_PADDING + (pitch - ISLAND_GAP - islandWidth) / 2;
    const peopleColumns = Math.max(2, Math.floor((width - FIELD_PADDING * 2) / 124));
    const peoplePitch = (width - FIELD_PADDING * 2) / peopleColumns;
    const byId = new Map(entities.map(entity => [entityId(entity), entity]));
    positions = stableFieldLayout(state.layout, entities.map(entity => ({ id: entityId(entity), type: entity.type })), {
      padding: FIELD_PADDING, gap: ISLAND_GAP,
      community: { left: firstColumnInset, columns, pitch, width: islandWidth, height: islandHeight },
      person: { left: FIELD_PADDING, columns: peopleColumns, pitch: peoplePitch, width: 112, height: 118 },
    }).map(position => ({ ...position, entity: byId.get(position.id)! }));
    const height = Math.max(viewport.clientHeight / scale, state.layout.bottom + FIELD_PADDING + 64 / scale);
    dimensions.style.width = `${width * scale}px`; dimensions.style.height = `${height * scale}px`;
    plane.style.width = `${width}px`; plane.style.height = `${height}px`;
    plane.style.transform = `scale(${scale})`;
    viewport.scrollLeft = state.scrollLeft; viewport.scrollTop = state.scrollTop;
    render();
  }

  function render(): void {
    if (destroyed) return;
    const left = viewport.scrollLeft / scale - 420, top = viewport.scrollTop / scale - 360;
    const right = left + viewport.clientWidth / scale + 840, bottom = top + viewport.clientHeight / scale + 720;
    const keep = new Set<string>();
    positions.forEach(position => {
      const id = entityId(position.entity);
      if (id !== focusedId && (position.x > right || position.x + position.width < left || position.y > bottom || position.y + position.height < top)) return;
      keep.add(id);
      let node = nodes.get(id);
      if (!node) {
        node = el('div', 'sw-field-row'); node.setAttribute('role', 'row');
        const cell = el('div'); cell.setAttribute('role', 'gridcell');
        const target = el('button', 'sw-field-target'); target.type = 'button';
        target.dataset.entityId = id;
        target.setAttribute('aria-label', position.entity.type === 'community' ? `${position.entity.value.name}, ${nounCount(position.entity.value.memberCount, 'участник', 'участника', 'участников')}` : position.entity.value.displayName);
        target.addEventListener('click', () => onSelect(positions.find(item => entityId(item.entity) === id)!.entity));
        target.addEventListener('focus', () => { focusedId = id; updateTabStops(); });
        cell.append(target); node.append(cell); nodes.set(id, node); plane.append(node);
      }
      const target = node.querySelector<HTMLButtonElement>('button')!;
      target.setAttribute('aria-label', position.entity.type === 'community' ? `${position.entity.value.name}, ${nounCount(position.entity.value.memberCount, 'участник', 'участника', 'участников')}` : position.entity.value.displayName);
      const signature = position.entity.type === 'community'
        ? JSON.stringify([position.entity.value.revision, id === selectedId, compact, position.entity.value.unreadCount, position.entity.value.memberCount, position.entity.value.previewMembers.map(member => [member.profileId, member.revision, member.avatarRevision])])
        : JSON.stringify([position.entity.value.revision, position.entity.value.avatarRevision, id === selectedId]);
      if (node.dataset.signature !== signature) {
        if (position.entity.type === 'community') target.replaceChildren(communityIsland(position.entity.value, id === selectedId, compact));
        else { const profile = position.entity.value; target.replaceChildren(avatar(profile.displayName, profile.avatarUrl, worldColor(profile.avatarColor), profile.profileId, profile.avatarRevision), el('span', 'sw-field-person-name', profile.displayName)); target.classList.add('sw-field-person'); }
        node.dataset.signature = signature;
      }
      target.setAttribute('aria-pressed', String(id === selectedId));
      node.style.left = `${position.x}px`; node.style.top = `${position.y}px`; node.style.width = `${position.width}px`; node.style.height = `${position.height}px`;
    });
    for (const [id, node] of nodes) if (!keep.has(id)) { node.remove(); nodes.delete(id); }
    updateTabStops();
  }

  function updateTabStops(): void {
    const active = focusedId && nodes.has(focusedId) ? focusedId : nodes.keys().next().value;
    for (const [id, node] of nodes) node.querySelector('button')!.tabIndex = id === active ? 0 : -1;
  }

  function schedule(): void { if (!raf) raf = requestAnimationFrame(() => { raf = 0; render(); }); }
  viewport.addEventListener('scroll', () => { state.scrollLeft = viewport.scrollLeft; state.scrollTop = viewport.scrollTop; schedule(); }, { passive: true, signal });
  plane.addEventListener('keydown', event => {
    if (!['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown', 'Home', 'End'].includes(event.key)) return;
    const current = positions.find(position => entityId(position.entity) === focusedId) ?? positions[0];
    if (!current) return;
    event.preventDefault();
    let next: Position | undefined;
    if (event.key === 'Home') next = positions[0];
    else if (event.key === 'End') next = positions.at(-1);
    else {
      const direction = event.key === 'ArrowRight' ? [1, 0] : event.key === 'ArrowLeft' ? [-1, 0] : event.key === 'ArrowDown' ? [0, 1] : [0, -1];
      next = positions.filter(position => (position.x - current.x) * direction[0]! + (position.y - current.y) * direction[1]! > 8).sort((a, b) => {
        const score = (position: Position) => Math.hypot(position.x - current.x, position.y - current.y) + Math.abs((position.x - current.x) * direction[1]! - (position.y - current.y) * direction[0]!) * 2;
        return score(a) - score(b);
      })[0];
    }
    if (!next) return;
    focusedId = entityId(next.entity); render();
    nodes.get(focusedId)?.querySelector<HTMLButtonElement>('button')?.focus({ preventScroll: true });
    viewport.scrollTo({ left: Math.max(0, next.x * scale - viewport.clientWidth / 2 + next.width * scale / 2), top: Math.max(0, next.y * scale - viewport.clientHeight / 2 + next.height * scale / 2), behavior: 'instant' });
  }, { signal });

  let drag: { x: number; y: number; left: number; top: number; pointerId: number } | null = null;
  viewport.addEventListener('pointerdown', event => {
    if (event.pointerType !== 'mouse' || event.button !== 0 || (event.target as Element).closest('button')) return;
    drag = { x: event.clientX, y: event.clientY, left: viewport.scrollLeft, top: viewport.scrollTop, pointerId: event.pointerId };
    viewport.setPointerCapture(event.pointerId); viewport.classList.add('is-dragging');
  }, { signal });
  viewport.addEventListener('pointermove', event => {
    if (!drag) return;
    viewport.scrollLeft = drag.left - event.clientX + drag.x; viewport.scrollTop = drag.top - event.clientY + drag.y;
  }, { signal });
  const endDrag = (): void => { drag = null; viewport.classList.remove('is-dragging'); };
  viewport.addEventListener('pointerup', endDrag, { signal }); viewport.addEventListener('pointercancel', endDrag, { signal });
  const resize = new ResizeObserver(layout); resize.observe(viewport);
  return {
    element: viewport,
    update(next, selected = '') {
      const incoming = new Set(next.map(entityId));
      if (!incoming.has(focusedId)) focusedId = '';
      entities = next; selectedId = selected; layout();
    },
    setScale(next) { zoom = Math.max(0.65, Math.min(1.4, next)); layout(); },
    destroy() { state.scrollLeft = viewport.scrollLeft; state.scrollTop = viewport.scrollTop; destroyed = true; controller.abort(); resize.disconnect(); if (raf) cancelAnimationFrame(raf); nodes.clear(); },
  };
}
