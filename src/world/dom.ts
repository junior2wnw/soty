import { icon } from './icons';
import { HEX_FLOWER, hexCluster } from '../geometry/hex.mjs';
import { placeHex } from '../geometry/dom';

export function el<K extends keyof HTMLElementTagNameMap>(tag: K, className = '', text?: string): HTMLElementTagNameMap[K] {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

export function button(label: string, iconName?: string, className = '', onClick?: () => void): HTMLButtonElement {
  const node = el('button', `sw-button ${className}`.trim());
  node.type = 'button';
  if (iconName) node.append(icon(iconName));
  node.append(el('span', '', label));
  if (onClick) node.addEventListener('click', onClick);
  return node;
}

export function iconButton(label: string, iconName: string, onClick?: () => void): HTMLButtonElement {
  const node = button(label, iconName, 'sw-icon-button', onClick);
  node.title = label;
  node.setAttribute('aria-label', label);
  return node;
}

export function heading(title: string, subtitle?: string): HTMLElement {
  const node = el('div', 'sw-heading');
  node.append(el('h1', '', title));
  if (subtitle) node.append(el('p', 'sw-muted', subtitle));
  return node;
}

export function initials(label: string): string {
  const name = label.split(/[·|]/, 1)[0] ?? label;
  const words = name.match(/[\p{L}\p{N}][\p{L}\p{M}\p{N}'’-]*/gu) ?? [];
  return words.slice(0, 2).map(part => Array.from(part)[0] ?? '').join('').toUpperCase() || 'С';
}

export function safeImageUrl(value: unknown): string | null {
  if (typeof value !== 'string' || !value.trim()) return null;
  if (/^data:image\/(?:png|jpeg|webp);base64,[A-Za-z0-9+/]+={0,2}$/.test(value) && value.length < 150_000) return value;
  return null;
}

export function avatar(label: string, imageUrl?: string | null, color = 'honey', profileId?: string, revision?: number | null): HTMLElement {
  const node = el('span', `sw-avatar sw-color-${color}`);
  if (profileId && revision) { node.dataset.profileId = profileId; node.dataset.avatarRevision = String(revision); node.dataset.avatarLabel = initials(label); }
  const url = safeImageUrl(imageUrl);
  if (url) {
    const image = el('img');
    image.src = url;
    image.alt = '';
    image.loading = 'lazy';
    image.decoding = 'async';
    image.referrerPolicy = 'no-referrer';
    image.addEventListener('error', () => { node.replaceChildren(el('span', '', initials(label))); }, { once: true });
    node.append(image);
  } else node.append(el('span', '', initials(label)));
  return node;
}

export function badge(label: string, iconName?: string, color = ''): HTMLElement {
  const node = el('span', `sw-badge ${color ? `sw-badge-${color}` : ''}`);
  if (iconName) node.append(icon(iconName));
  node.append(el('span', '', label));
  return node;
}

export function labeledField(label: string, input: HTMLElement, hint?: string): HTMLElement {
  const field = el('label', 'sw-field');
  field.append(el('span', 'sw-field-label', label), input);
  if (hint) field.append(el('small', 'sw-muted', hint));
  return field;
}

export function textInput(value = '', placeholder = '', maxLength = 160): HTMLInputElement {
  const input = el('input', 'sw-input');
  input.type = 'text';
  input.value = value;
  input.placeholder = placeholder;
  input.maxLength = maxLength;
  return input;
}

export function timeLabel(timestamp: number | string | undefined): string {
  if (!timestamp) return '';
  const date = new Date(timestamp);
  return Number.isNaN(date.getTime()) ? '' : date.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
}

export function nounCount(count: number, one: string, few: string, many: string): string {
  const abs = Math.abs(count) % 100;
  return `${count} ${abs > 10 && abs < 20 ? many : abs % 10 === 1 ? one : abs % 10 > 1 && abs % 10 < 5 ? few : many}`;
}

export function emptyState(title: string, description: string, action?: HTMLElement, iconName = 'cells'): HTMLElement {
  const node = el('div', 'sw-empty');
  const art = el('div', 'sw-empty-art');
  const cluster = hexCluster(HEX_FLOWER, 35, 5, 4);
  art.style.width = `${cluster.width}px`; art.style.height = `${cluster.height}px`;
  for (let index = 0; index < 7; index++) {
    const cell = el('span', `sw-empty-cell sw-empty-cell-${index}`);
    placeHex(cell, cluster, index);
    if (index === 0) cell.append(icon(iconName));
    art.append(cell);
  }
  node.append(art, el('h2', '', title), el('p', 'sw-muted', description));
  if (action) node.append(action);
  return node;
}
