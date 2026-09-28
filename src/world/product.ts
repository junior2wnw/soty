import { button, el, iconButton, textInput } from './dom';
import { icon } from './icons';
import { type WorldDialog } from './dialogs';
import { HEX_FLOWER, hexCluster, hexGrid } from '../geometry/hex.mjs';
import { placeHex } from '../geometry/dom';
import { createHexTile } from '../ui/hex-tile';

export type CapabilityId = 'notes' | 'apps' | 'devices' | 'agent' | 'communities' | 'contacts' | 'files' | 'chess' | 'terminal' | 'internet' | 'legacy-notes' | 'appearance';
export interface Capability { id: CapabilityId; title: string; detail: string; symbol: string; color: string; category: 'Дела' | 'Общение' | 'Инструменты'; }
export const capabilities: readonly Capability[] = [
  { id: 'notes', title: 'Записки', detail: 'Идеи, планы и списки', symbol: 'list', color: 'honey', category: 'Дела' },
  { id: 'apps', title: 'Приложения', detail: 'Проекты с ваших устройств', symbol: 'grid', color: 'lilac', category: 'Дела' },
  { id: 'devices', title: 'Устройства', detail: 'Компьютеры рядом с вами', symbol: 'laptop', color: 'sage', category: 'Дела' },
  { id: 'agent', title: 'Создать с ИИ', detail: 'От идеи к приложению', symbol: 'sparkle', color: 'honey', category: 'Дела' },
  { id: 'communities', title: 'Сообщества', detail: 'Найдите своих в общем мире', symbol: 'people', color: 'coral', category: 'Общение' },
  { id: 'contacts', title: 'Контакты', detail: 'Люди и приглашения', symbol: 'person', color: 'blue', category: 'Общение' },
  { id: 'files', title: 'Файлы', detail: 'Вложения в ваших комнатах', symbol: 'folder', color: 'blue', category: 'Инструменты' },
  { id: 'chess', title: 'Шахматы', detail: 'С другом или компьютером', symbol: 'game', color: 'sage', category: 'Общение' },
  { id: 'terminal', title: 'Команды', detail: 'Управление подключённым устройством', symbol: 'tools', color: 'lilac', category: 'Инструменты' },
  { id: 'internet', title: 'Общий интернет', detail: 'Подключения и маршруты', symbol: 'world', color: 'blue', category: 'Инструменты' },
  { id: 'legacy-notes', title: 'Совместные тексты', detail: 'Общие заметки в комнате', symbol: 'chat', color: 'coral', category: 'Дела' },
  { id: 'appearance', title: 'Оформление', detail: 'Тема, яркость и движение', symbol: 'settings', color: 'sage', category: 'Инструменты' },
];
export interface RecentAction { route: string; title: string; symbol: string; }
export interface DeskPreferences { favorites: CapabilityId[]; recent: RecentAction[]; }
const defaults: CapabilityId[] = ['notes', 'apps', 'devices', 'agent'];
const deskKey = (accountId: string): string => `soty.desk.v1:${encodeURIComponent(accountId)}`;
export function loadDeskPreferences(accountId: string): DeskPreferences {
  try {
    const value = JSON.parse(localStorage.getItem(deskKey(accountId)) ?? '{}');
    return { favorites: Array.isArray(value.favorites) ? [...new Set<CapabilityId>(value.favorites.filter((id: unknown) => capabilities.some(item => item.id === id)))].slice(0, 8) : [...defaults],
      recent: Array.isArray(value.recent) ? value.recent.filter((item: RecentAction) => item && typeof item.route === 'string' && /^(?:notes|app|community)\/[A-Za-z0-9_-]{3,160}(?:\/[A-Za-z0-9_-]{3,160})?$/.test(item.route) && typeof item.title === 'string' && typeof item.symbol === 'string').slice(0, 6).map((item: RecentAction) => ({ route: item.route, title: item.title.slice(0, 100), symbol: item.symbol.slice(0, 20) })) : [] };
  } catch { return { favorites: [...defaults], recent: [] }; }
}
export function saveDeskPreferences(accountId: string, value: DeskPreferences): void {
  try { localStorage.setItem(deskKey(accountId), JSON.stringify(value)); } catch { /* Favorites are optional; core data is saved independently. */ }
}

export interface DeskCell { label: string; status: string; visual: Node; color: string; action(): void; }
export function createDeskCluster(cells: DeskCell[]): { element: HTMLElement; destroy(): void } {
  const element = el('div', 'sw-desk-cluster'); element.setAttribute('aria-label', 'Связанные с вами соты');
  const field = el('div', 'sw-desk-cluster-field'); element.append(field);
  const tiles = cells.slice(0, 7).map(cell => createHexTile({ label: cell.label, status: cell.status, visual: cell.visual, className: `sw-desk-hex sw-color-${cell.color}`, onSelect: cell.action }));
  field.append(...tiles);
  let previous = 0;
  const observer = new ResizeObserver(entries => {
    const width = Math.floor(entries[0]?.contentRect.width ?? 0); if (width < 1 || width === previous) return; previous = width;
    const wide = width >= 386, count = wide ? tiles.length : Math.min(4, tiles.length);
    const radius = wide ? Math.min(72, (width - 26) / 5) : Math.min(64, (width - 26) / 3.5);
    const cluster = hexCluster(wide ? HEX_FLOWER.slice(0, count) : hexGrid(count, 2), radius, 7, 6);
    field.style.width = `${cluster.width}px`; field.style.height = `${cluster.height}px`;
    tiles.forEach((tile, index) => { tile.hidden = index >= count; if (index < count) placeHex(tile, cluster, index); });
  });
  observer.observe(element);
  return { element, destroy() { observer.disconnect(); } };
}

export function createLibrary({ favorites, open, toggle }: { favorites: CapabilityId[]; open(id: CapabilityId): void; toggle(id: CapabilityId): boolean }): HTMLElement {
  const section = el('section', 'sw-library');
  const head = el('div', 'sw-section-head'); const copy = el('div'); copy.append(el('h1', '', 'Возможности'), el('p', 'sw-muted', 'Всё, что умеют Соты'));
  const search = el('label', 'sw-search'); const input = textInput('', 'Найти возможность', 80); input.type = 'search'; input.setAttribute('aria-label', 'Найти возможность'); search.append(icon('search'), input); head.append(copy, search);
  const categories = el('div', 'sw-library-filters'); categories.setAttribute('role', 'group'); categories.setAttribute('aria-label', 'Раздел возможностей');
  const grid = el('div', 'sw-capabilities'); const status = el('div', 'sw-sr-only'); status.setAttribute('role', 'status');
  let category = 'Все'; const pinned = new Set(favorites);
  const render = (): void => {
    grid.replaceChildren(); const query = input.value.trim().toLocaleLowerCase('ru');
    const entries = capabilities.filter(item => (category === 'Все' || category === item.category || category === 'Закреплено' && pinned.has(item.id)) && `${item.title} ${item.detail}`.toLocaleLowerCase('ru').includes(query));
    for (const item of entries) {
      const card = el('article', `sw-capability sw-color-${item.color}`);
      const action = el('button', 'sw-capability-open'); action.type = 'button'; action.addEventListener('click', () => open(item.id));
      const mark = el('span', 'sw-capability-mark soty-hex'); mark.append(icon(item.symbol));
      const copy = el('span', 'sw-capability-copy'); copy.append(el('strong', '', item.title), el('small', '', item.detail)); action.append(mark, copy, icon('next'));
      const pin = iconButton(`Закрепить: ${item.title}`, 'pin', () => { const next = toggle(item.id); next ? pinned.add(item.id) : pinned.delete(item.id); pin.setAttribute('aria-pressed', String(next)); status.textContent = next ? `${item.title}: закреплено на главной` : `${item.title}: откреплено`; if (category === 'Закреплено') render(); });
      pin.setAttribute('aria-pressed', String(pinned.has(item.id))); card.append(action, pin); grid.append(card);
    }
    if (!entries.length) grid.append(el('p', 'sw-muted', 'Ничего не найдено'));
  };
  for (const name of ['Все', 'Дела', 'Общение', 'Инструменты', 'Закреплено']) {
    const control = button(name, undefined, 'sw-filter-chip', () => { category = name; categories.querySelectorAll('button').forEach(item => item.setAttribute('aria-pressed', String(item === control))); render(); }); control.setAttribute('aria-pressed', String(name === category)); categories.append(control);
  }
  input.addEventListener('input', render); render(); section.append(head, categories, grid, status); return section;
}

export interface QuickCommand { title: string; detail?: string; symbol: string; action(): void; }
export function openCommandPalette(commands: QuickCommand[], dialog: WorldDialog): void {
  dialog.element.classList.add('sw-command-dialog');
  const input = textInput('', 'Название функции или проекта', 100); input.type = 'search'; input.setAttribute('aria-label', 'Поиск действий'); input.setAttribute('role', 'combobox'); input.setAttribute('aria-autocomplete', 'list'); input.setAttribute('aria-expanded', 'true');
  const list = el('div', 'sw-command-list'); list.id = `commands-${crypto.randomUUID()}`; list.setAttribute('role', 'listbox'); list.setAttribute('aria-label', 'Действия'); input.setAttribute('aria-controls', list.id);
  const count = el('div', 'sw-sr-only'); count.setAttribute('role', 'status');
  let visible: QuickCommand[] = [], selected = 0;
  const select = (index: number): void => { selected = index; Array.from(list.children).forEach((node, i) => node.setAttribute('aria-selected', String(i === selected))); const active = list.children[selected] as HTMLElement | undefined; if (active) { input.setAttribute('aria-activedescendant', active.id); active.scrollIntoView({ block: 'nearest' }); } else input.removeAttribute('aria-activedescendant'); };
  const run = (): void => { const command = visible[selected]; if (command) { dialog.close(); command.action(); } };
  const render = (): void => {
    const query = input.value.trim().toLocaleLowerCase('ru'); visible = commands.filter(item => `${item.title} ${item.detail ?? ''}`.toLocaleLowerCase('ru').includes(query)).slice(0, 12); list.replaceChildren();
    visible.forEach((item, index) => { const option = el('div', 'sw-command-option'); option.id = `${list.id}-${index}`; option.setAttribute('role', 'option'); const text = el('span'); text.append(el('strong', '', item.title)); if (item.detail) text.append(el('small', '', item.detail)); option.append(icon(item.symbol), text, icon('arrow')); option.addEventListener('pointerdown', event => event.preventDefault()); option.addEventListener('click', () => { selected = index; run(); }); list.append(option); });
    count.textContent = visible.length ? `${visible.length} действий` : 'Ничего не найдено'; select(0);
  };
  input.addEventListener('input', render); input.addEventListener('keydown', event => { if (event.isComposing) return; if (event.key === 'ArrowDown' || event.key === 'ArrowUp') { event.preventDefault(); if (visible.length) select((selected + (event.key === 'ArrowDown' ? 1 : visible.length - 1)) % visible.length); } if (event.key === 'Enter') { event.preventDefault(); run(); } });
  dialog.body.append(input, list, count); render(); input.focus();
}
