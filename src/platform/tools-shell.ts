import { icon } from '../world/icons';
import { loadPreferences } from '../world/preferences';
import './tools-shell.css';

const destinations = [
  ['mine', 'app', 'Аппки'], ['messages', 'chat', 'Чаты'], ['assistant', 'sparkle', 'Помощник'],
] as const;

function link(href: string, name: string, symbol: string, className: string): HTMLAnchorElement {
  const node = document.createElement('a'); node.href = href; node.className = className;
  node.setAttribute('aria-label', name); node.title = name;
  const text = document.createElement('span'); text.textContent = name;
  node.append(icon(symbol), text); return node;
}

/** Shared destinations keep a room recognisable as an application inside Soty. */
export function toolsNavigation(tool: string | null): HTMLElement {
  document.body.dataset.motion = loadPreferences().motion ? 'on' : 'off';
  const host = document.createElement('div'); host.className = 'tools-navigation';
  const skip = document.createElement('a'); skip.className = 'tools-skip'; skip.href = '#tools-workspace'; skip.textContent = 'К рабочей области';
  const rail = document.createElement('aside'); rail.className = 'tools-rail'; rail.setAttribute('aria-label', 'Разделы Сот');
  const mark = document.createElement('a'); mark.href = '/#mine'; mark.className = 'tools-brand-mark'; mark.setAttribute('aria-label', 'Соты · приложения');
  const hex = document.createElement('span'); hex.className = 'soty-hex'; hex.textContent = 'S'; hex.setAttribute('aria-hidden', 'true'); mark.append(hex);
  const nav = document.createElement('nav'); nav.className = 'tools-primary'; nav.setAttribute('aria-label', 'Главная навигация');
  for (const [view, symbol, name] of destinations) {
    const item = link('/#' + view, name, symbol, 'tools-nav-item');
    if (view === (tool ? 'mine' : 'messages')) item.setAttribute('aria-current', 'page');
    nav.append(item);
  }
  const secondary = document.createElement('div'); secondary.className = 'tools-secondary';
  secondary.append(link('/#notes', 'Записки', 'list', 'tools-secondary-link'), link('/#access', 'Доступы и действия', 'lock', 'tools-secondary-link'));
  rail.append(mark, nav, secondary);
  const header = document.createElement('header'); header.className = 'tools-header';
  const brand = document.createElement('a'); brand.href = '/#mine'; brand.className = 'tools-wordmark'; brand.textContent = 'соты'; brand.setAttribute('aria-label', 'Соты · приложения');
  const context = document.createElement('span'); context.className = 'tools-context';
  context.textContent = ({ chess: 'Шахматы', terminal: 'Команды', files: 'Файлы комнаты', notes: 'Совместный текст', internet: 'Общий интернет' } as Record<string, string>)[tool ?? ''] ?? 'Комнаты';
  const actions = document.createElement('div'); actions.className = 'tools-header-actions';
  actions.append(link('/#notes', 'Записки', 'list', 'tools-header-link'), link('/#access', 'Доступы и действия', 'lock', 'tools-header-link'));
  // Keep the original hook: main.ts binds this very button after mounting navigation.
  const profile = document.querySelector<HTMLButtonElement>('.hive-panel .connect-open');
  if (profile) { profile.classList.add('tools-profile'); actions.append(profile); }
  header.append(brand, context, actions); host.append(skip, rail, header); return host;
}

export interface ToolChoice { label: string; description: string; symbol: string; run(): void | Promise<void> }
export function chooseTool(title: string, description: string, choices: ToolChoice[]): void {
  const previous = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  const dialog = document.createElement('dialog'); dialog.className = 'tools-dialog';
  const heading = document.createElement('h2'); heading.id = 'tool-' + crypto.randomUUID(); heading.textContent = title;
  dialog.setAttribute('aria-labelledby', heading.id);
  const header = document.createElement('header');
  const close = document.createElement('button'); close.type = 'button'; close.className = 'tools-dialog-close'; close.setAttribute('aria-label', 'Закрыть'); close.append(icon('close'));
  close.addEventListener('click', () => dialog.close()); header.append(heading, close);
  const copy = document.createElement('p'); copy.textContent = description;
  const message = document.createElement('p'); message.className = 'tools-dialog-error'; message.setAttribute('role', 'alert');
  const body = document.createElement('div'); body.className = 'tools-choices';
  let working = false;
  for (const choice of choices) {
    const button = document.createElement('button'); button.type = 'button';
    const text = document.createElement('span'), name = document.createElement('strong'), detail = document.createElement('small');
    name.textContent = choice.label; detail.textContent = choice.description; text.append(name, detail); button.append(icon(choice.symbol), text, icon('next'));
    button.addEventListener('click', async () => {
      if (working) return;
      working = true; message.textContent = ''; dialog.setAttribute('aria-busy', 'true');
      dialog.querySelectorAll<HTMLButtonElement>('button').forEach(control => { control.disabled = true; });
      try { await choice.run(); if (dialog.isConnected) dialog.close(); }
      catch {
        if (dialog.isConnected) { message.textContent = 'Не удалось открыть. Попробуйте ещё раз.'; dialog.querySelectorAll<HTMLButtonElement>('button').forEach(control => { control.disabled = false; }); button.focus(); }
      } finally { working = false; dialog.removeAttribute('aria-busy'); }
    }); body.append(button);
  }
  dialog.addEventListener('cancel', event => { if (working) event.preventDefault(); });
  dialog.append(header, copy, body, message); document.body.append(dialog);
  dialog.addEventListener('close', () => { dialog.remove(); if (previous?.isConnected) previous.focus(); }, { once: true });
  dialog.showModal(); body.querySelector<HTMLButtonElement>('button')?.focus();
}
