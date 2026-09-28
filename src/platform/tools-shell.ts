import { icon } from '../world/icons';
import { loadPreferences } from '../world/preferences';
import './tools-shell.css';

const destinations = [
  ['mine', 'cells', 'Мои соты', 'Мои'], ['world', 'world', 'Общий мир', 'Мир'],
  ['messages', 'chat', 'Чаты', 'Чаты'], ['notes', 'list', 'Записки', 'Записки'],
  ['library', 'grid', 'Возможности', 'Ещё'],
] as const;

export function toolsNavigation(tool: string | null): HTMLElement {
  document.body.dataset.motion = loadPreferences().motion ? 'on' : 'off';
  const host = document.createElement('div'); host.className = 'tools-navigation';
  const header = document.createElement('header'); header.className = 'tools-header';
  const brand = document.createElement('a'); brand.href = '/#mine'; brand.className = 'tools-brand';
  brand.innerHTML = '<span class="soty-hex">S</span><strong>СОТЫ</strong>';
  const title = document.createElement('span'); title.className = 'tools-context';
  title.textContent = ({ chess: 'Шахматы', terminal: 'Команды', files: 'Файлы', notes: 'Совместные тексты', internet: 'Общий интернет' } as Record<string, string>)[tool ?? ''] ?? 'Комнаты';
  const nav = document.createElement('nav'); nav.setAttribute('aria-label', 'Главная навигация');
  for (const [view, symbol, name, short] of destinations) {
    const link = document.createElement('a'); link.href = `/#${view}`; link.setAttribute('aria-label', name);
    if (view === (tool ? 'library' : 'messages')) link.setAttribute('aria-current', 'page');
    const text = document.createElement('span'); text.textContent = name; text.dataset.short = short;
    link.append(icon(symbol), text); nav.append(link);
  }
  header.append(brand, title, nav); host.append(header); return host;
}

export interface ToolChoice { label: string; description: string; symbol: string; run(): void | Promise<void> }
export function chooseTool(title: string, description: string, choices: ToolChoice[]): void {
  const previous = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  const dialog = document.createElement('dialog'); dialog.className = 'tools-dialog';
  const heading = document.createElement('h2'); heading.id = `tool-${crypto.randomUUID()}`; heading.textContent = title;
  dialog.setAttribute('aria-labelledby', heading.id);
  const header = document.createElement('header');
  const close = document.createElement('button'); close.type = 'button'; close.className = 'tools-dialog-close'; close.setAttribute('aria-label', 'Закрыть'); close.append(icon('close'));
  close.addEventListener('click', () => dialog.close()); header.append(heading, close);
  const copy = document.createElement('p'); copy.textContent = description;
  const body = document.createElement('div'); body.className = 'tools-choices';
  for (const choice of choices) {
    const button = document.createElement('button'); button.type = 'button';
    const text = document.createElement('span'), name = document.createElement('strong'), detail = document.createElement('small');
    name.textContent = choice.label; detail.textContent = choice.description; text.append(name, detail); button.append(icon(choice.symbol), text, icon('next'));
    button.addEventListener('click', async () => {
      button.disabled = true;
      try { await choice.run(); dialog.close(); }
      catch { copy.textContent = 'Не удалось открыть. Попробуйте ещё раз.'; button.disabled = false; }
    }); body.append(button);
  }
  dialog.append(header, copy, body); document.body.append(dialog);
  dialog.addEventListener('close', () => { dialog.remove(); if (previous?.isConnected) previous.focus(); }, { once: true });
  dialog.showModal();
}
