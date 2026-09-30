import { button, el, iconButton } from './dom';
import { icon } from './icons';
import { communityEmblem } from './hex-field';
import { createHexTile } from '../ui/hex-tile';
import { hexCluster, hexGrid } from '../geometry/hex.mjs';
import { placeHex } from '../geometry/dom';
import { worldColor, type WorldAppRecord, type WorldCommunity } from './types';
import { appTone, createApplicationCard } from './application-card';
export { appStatusLabel } from './application-card';

export interface AppHomeState {
  lens: 'all' | 'mine' | 'together'; communityId: string | null; presentation: 'cards' | 'field';
  scroll: number; fieldX: number; fieldY: number; slots: Map<string, number>; focusId: string | null;
  focusControl?: string | null;
  pinned: Set<string>;
}
export interface HomeNotePreview { noteId: string; title: string; preview: string; pinned: boolean; updatedAt: number; }
interface Options {
  apps: WorldAppRecord[]; communities: WorldCommunity[]; accountId: string; notes: HomeNotePreview[] | null;
  state: AppHomeState; appStatus: 'loading' | 'ready' | 'error'; notesStatus: 'loading' | 'ready' | 'error';
  onChange(): void; openApp(app: WorldAppRecord): void; openNotes(id?: string): void;
  openCommunity(group: WorldCommunity, chat?: boolean): void; inspectApp(app: WorldAppRecord): void;
  add(): void; explore(): void; library(): void; retry(): void;
  recent(): void; hasRecent: boolean; shortcuts: { title: string; symbol: string; action(): void }[];
}
interface Item { id: string; name: string; detail: string; symbol: string; color: string; app?: WorldAppRecord; }

export function createAppsHome(options: Options): { element: HTMLElement; destroy(): void } {
  const { state } = options;
  const groups = options.communities.filter(group => group.membership?.state === 'active');
  if (state.communityId && !groups.some(group => group.communityId === state.communityId)) { state.communityId = null; state.lens = 'all'; }
  const element = el('section', 'sx-home'); element.setAttribute('aria-label', 'Моё пространство');
  const heading = el('div', 'sx-page-heading'); const title = el('h1', '', 'Моё пространство');
  const presentation = el('div', 'sx-presentation'); presentation.setAttribute('role', 'group'); presentation.setAttribute('aria-label', 'Вид пространства');
  for (const [id, label, glyph] of [['cards', 'Карточки приложений', 'grid'], ['field', 'Поле приложений и сообществ', 'connections']] as const) {
    const control = iconButton(label, glyph, () => { state.presentation = id; state.focusId = null; state.focusControl = `view:${id}`; options.onChange(); });
    control.dataset.homeControl = `view:${id}`;
    control.setAttribute('aria-pressed', String(state.presentation === id)); presentation.append(control);
  }
  heading.append(title, presentation);
  const filters = el('div', 'sx-home-filters'); filters.setAttribute('role', 'group'); filters.setAttribute('aria-label', 'Приложения');
  for (const [id, label] of [['all', 'Все'], ['mine', 'Мои проекты'], ['together', 'Вместе']] as const) {
    const filter = button(label, undefined, 'sx-lens', () => { state.lens = id; state.communityId = null; state.focusId = null; state.focusControl = `lens:${id}`; options.onChange(); });
    filter.dataset.homeControl = `lens:${id}`;
    filter.setAttribute('aria-pressed', String(state.lens === id && !state.communityId)); filters.append(filter);
  }
  filters.append(button('Открытия', undefined, 'sx-lens', options.explore));
  const circles = el('div', 'sx-community-strip'); circles.setAttribute('aria-label', 'Сообщества');
  for (const group of groups) {
    const chip = button(group.name, undefined, 'sx-community-chip', () => { state.communityId = state.communityId === group.communityId ? null : group.communityId; state.focusId = null; state.focusControl = `community:${group.communityId}`; options.onChange(); });
    chip.dataset.homeControl = `community:${group.communityId}`;
    chip.prepend(communityEmblem(group)); chip.append(el('span', 'sx-chip-count', String(group.memberCount)));
    chip.setAttribute('aria-pressed', String(state.communityId === group.communityId));
    chip.setAttribute('aria-label', `${group.name}, показать приложения сообщества`); circles.append(chip);
  }
  if (state.communityId) {
    const chosen = groups.find(group => group.communityId === state.communityId);
    if (chosen) circles.append(button('О сообществе', 'arrow', 'sw-button-quiet', () => options.openCommunity(chosen)));
  }
  if (!groups.length) circles.append(button('Найти сообщество', 'people', 'sw-button-quiet', options.explore));
  const items: Item[] = options.apps.map(app => ({ id: `app:${app.appId}`, name: app.name,
    detail: app.description || app.deviceLabel || 'Приложение в Сотах', symbol: app.symbol || 'app', color: appTone(app), app }));
  items.splice(Math.min(3, items.length), 0, { id: 'native:notes', name: 'Записки', detail: 'Мысли, планы и списки', symbol: 'note', color: 'honey' });
  const matches = (item: Item): boolean => {
    if (state.communityId) return !!item.app?.grants?.communityIds.includes(state.communityId) || item.app?.communityId === state.communityId;
    if (state.lens === 'mine') return !!item.app && item.app.ownerAccountId === options.accountId;
    if (state.lens === 'together') return !!item.app && (!!item.app.grants?.communityIds.length || !!item.app.communityId || !!item.app.grants?.accountIds.length);
    return true;
  };
  const open = (item: Item): void => { state.focusId = item.id; item.app ? options.openApp(item.app) : options.openNotes(); };
  let viewport: HTMLElement | null = null;
  let restored = false;
  const body = el('div', state.presentation === 'field' ? 'sx-home-field' : 'sx-app-grid');
  if (state.presentation === 'cards') {
    for (const item of items.filter(matches).sort((a, b) => Number(state.pinned.has(b.app?.appId || '')) - Number(state.pinned.has(a.app?.appId || '')))) body.append(createAppCard(item, options, () => open(item)));
    if (!options.apps.length && options.appStatus === 'ready' && state.lens === 'all' && !state.communityId) {
      const start = el('button', 'sx-start-card'); start.type = 'button'; start.addEventListener('click', options.add);
      const mark = el('span', 'sx-start-mark'); mark.append(icon('plus'));
      start.append(mark, el('strong', '', 'Добавьте свой проект'), el('span', '', 'С вашего устройства — в ваше пространство')); body.append(start);
    }
    if (!body.childElementCount) {
      const empty = el('div', 'sx-home-empty'); empty.append(icon('app'), el('h2', '', state.lens === 'mine' ? 'Здесь будут ваши проекты' : 'Пока нет приложений'),
        button(state.communityId ? 'Добавить приложение' : 'Добавить проект', 'plus', 'sw-button-primary', options.add)); body.append(empty);
    }
  } else {
    viewport = el('div', 'sx-field-viewport'); const board = el('div', 'sx-field-board');
    const entities = [...items.map(item => ({ id: item.id, label: item.name, status: item.app ? 'Приложение' : 'Личное', symbol: item.symbol,
      color: item.color, selected: matches(item), action: () => open(item) })), ...groups.map(group => ({ id: `community:${group.communityId}`, label: group.name,
      status: 'Сообщество', symbol: 'people', color: worldColor(group.color), selected: !state.communityId || state.communityId === group.communityId, action: () => options.openCommunity(group) }))];
    for (const entry of entities) if (!state.slots.has(entry.id)) state.slots.set(entry.id, state.slots.size);
    // Keep the first row's bounds from the start: appending a cell must not recenter its neighbours.
    const count = Math.max(5, state.slots.size), cluster = hexCluster(hexGrid(count, 5), 85, 10, 24);
    board.style.width = `${cluster.width}px`; board.style.height = `${cluster.height}px`;
    for (const entry of entities) {
      const tile = createHexTile({ label: entry.label, status: entry.status, visual: icon(entry.symbol), radius: 85,
        className: `sx-field-cell sw-color-${entry.color}${entry.selected ? '' : ' is-muted'}`, onSelect: entry.action });
      tile.dataset.entityId = entry.id; placeHex(tile, cluster, state.slots.get(entry.id)!); board.append(tile);
    }
    viewport.append(board); body.append(viewport);
    viewport.addEventListener('scroll', () => { if (restored) { state.fieldX = viewport!.scrollLeft; state.fieldY = viewport!.scrollTop; } }, { passive: true });
  }
  element.append(heading, filters, circles);
  if (options.appStatus === 'loading' && !options.apps.length) {
    const loading = el('div', 'sx-inline-status', 'Загружаем приложения…'); loading.setAttribute('role', 'status'); element.append(loading);
  } else if (options.appStatus === 'error') {
    const error = el('div', 'sx-inline-status'); error.setAttribute('role', 'status');
    error.append(el('span', '', 'Не удалось обновить приложения'), button('Повторить', 'refresh', 'sw-button-quiet', options.retry)); element.append(error);
  }
  element.append(body);
  const footer = el('footer', 'sx-home-footer'); const shortcuts = el('div', 'sx-home-shortcuts');
  for (const shortcut of options.shortcuts) shortcuts.append(button(shortcut.title, shortcut.symbol, 'sw-button-quiet', shortcut.action));
  if (options.hasRecent) shortcuts.append(button('Недавнее', 'history', 'sw-button-quiet', options.recent));
  shortcuts.append(button('Все возможности', 'grid', 'sw-button-quiet', options.library));
  footer.append(shortcuts, button('Добавить проект', 'plus', 'sw-button-quiet', options.add)); element.append(footer);
  element.addEventListener('scroll', () => { if (restored) state.scroll = element.scrollTop; }, { passive: true });
  let frame = requestAnimationFrame(() => {
    element.scrollTop = state.scroll;
    if (viewport) { viewport.scrollLeft = state.fieldX; viewport.scrollTop = state.fieldY; }
    restored = true;
    if (state.focusControl) { element.querySelector<HTMLElement>(`[data-home-control="${CSS.escape(state.focusControl)}"]`)?.focus({ preventScroll: true }); state.focusControl = null; }
    else if (state.focusId) { element.querySelector<HTMLElement>(`[data-entity-id="${CSS.escape(state.focusId)}"]`)?.focus({ preventScroll: true }); state.focusId = null; }
  });
  return { element, destroy() {
    cancelAnimationFrame(frame); frame = 0;
    // Loading may replace this view before its first frame; never erase the saved viewport with a fresh node's zero scroll.
    if (restored) { state.scroll = element.scrollTop; if (viewport) { state.fieldX = viewport.scrollLeft; state.fieldY = viewport.scrollTop; } }
  } };
}

function createAppCard(item: Item, options: Options, open: () => void): HTMLElement {
  if (item.app) {
    const app = item.app;
    return createApplicationCard({ app, accountId: options.accountId, communities: options.communities, pinned: options.state.pinned.has(app.appId),
      ...(options.state.communityId ? { contextCommunityId: options.state.communityId } : {}),
      open, inspect: () => options.inspectApp(app), openCommunity: options.openCommunity, togglePin: () => {
        options.state.pinned.has(app.appId) ? options.state.pinned.delete(app.appId) : options.state.pinned.add(app.appId);
        options.state.focusId = null; options.state.focusControl = `pin:${app.appId}`; options.onChange(); return options.state.pinned.has(app.appId);
      } });
  }
  const card = el('article', `sx-app-card sw-color-${item.color} sx-notes-card`);
  const launch = el('button', 'sx-app-launch'); launch.type = 'button'; launch.dataset.entityId = item.id;
  launch.setAttribute('aria-label', `Открыть ${item.name}`); launch.addEventListener('click', open);
  const cover = el('div', 'sx-app-cover'); cover.setAttribute('aria-hidden', 'true');
  const paper = el('div', 'sx-note-paper'), note = options.notes?.[0];
  paper.append(el('strong', '', note?.title || (note ? 'Без названия' : options.notesStatus === 'loading' ? 'Открываем записки…' : options.notesStatus === 'error' ? 'Откройте записки' : 'Есть мысль?')),
    el('span', '', note?.preview || (note ? 'Открыть записку' : options.notesStatus === 'error' ? 'Не удалось обновить превью.' : options.notesStatus === 'loading' ? '' : 'Сохраните её здесь.'))); cover.append(paper);
  const identity = el('div', 'sx-card-identity'); const mark = el('span', 'sx-card-mark soty-hex'); mark.append(icon(item.symbol));
  const text = el('span', 'sx-card-copy'); text.append(el('strong', '', item.name), el('span', '', item.detail));
  identity.append(mark, text, icon('diagonal')); launch.append(cover, identity);
  const context = el('div', 'sx-card-context');
  const scope = el('span', 'sx-card-scope'); scope.append(icon('lock'), el('span', '', 'Личное')); context.append(scope);
  context.append(iconButton('Новая записка', 'plus', () => options.openNotes('new')));
  card.append(launch, context); return card;
}
