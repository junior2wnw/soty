import './app-engagement.css';
import { button, el } from './dom';
import { icon } from './icons';
import { createDialog, type WorldDialog } from './dialogs';
import { appStatusLabel } from './application-card';
import { engagementError, engagementStorage, entryLabel, type EngagementStorageOptions } from './app-saved';
import { createAppSavedLibraryState, createAppSavedState, dispatchAppSavedIntent } from './app-saved-state.mjs';
import type { AppEntry, SavedEntry, SavedIntent, SavedPending } from './app-saved-state.mjs';
import type { WorldApi } from './types';

export interface AppLibraryOptions extends EngagementStorageOptions {
  api: WorldApi; accountId: string; isCurrent(): boolean;
  openEntry(entry: AppEntry): void; discussEntry?(entry: AppEntry): void; onChanged?(): void;
}
export interface AppLibraryHandle { dispose(): void; refresh(): Promise<void>; focus(): void; }
interface Row { element: HTMLLIElement; value: SavedEntry; revision: number; title: HTMLElement; status: HTMLElement; url: HTMLElement; open: HTMLButtonElement; remove: HTMLButtonElement; discuss: HTMLButtonElement; }
const setText = (node: HTMLElement, value: string): void => { if (node.textContent !== value) node.textContent = value; };
const deny = (node: HTMLButtonElement, value: boolean): void => node.setAttribute('aria-disabled', String(value));
const entryOnly = (value: AppEntry): AppEntry => ({ appId: value.appId, domainId: value.domainId, origin: value.origin, path: value.path });

/** A server library lens. It never derives availability from local pins or
 * substitutes another domain when a saved entry is no longer accessible. */
export function mountAppLibrary(host: HTMLElement, options: AppLibraryOptions): AppLibraryHandle {
  const library = createAppSavedLibraryState({ accountId: options.accountId });
  const mutations = createAppSavedState({ accountId: options.accountId, ...engagementStorage(host, options) });
  const root = el('section', 'se-library'); root.setAttribute('aria-label', 'Сохранённые приложения');
  const header = el('div', 'se-library-header'), heading = el('div');
  heading.append(el('h2', '', 'Сохранённые'), el('p', 'se-muted', 'На устройствах вашего аккаунта'));
  const refreshButton = button('Обновить', 'refresh', 'sw-button-quiet', () => { void load(false); }); refreshButton.dataset.engagementKey = 'library-refresh';
  header.append(heading, refreshButton);
  const status = el('p', 'se-status'); status.setAttribute('role', 'status'); status.dataset.engagementKey = 'library-status';
  const pendingBox = el('div', 'se-pending'), pendingText = el('p'), pendingEntry = el('p', 'se-muted'), pendingActions = el('div', 'se-actions');
  const retry = button('Проверить запрос', 'refresh', 'sw-button-quiet', () => { const expected = lastPending; if (expected && !busy) void mutate({ expectedPending: expected }); });
  retry.dataset.engagementKey = 'library-retry';
  const abandon = button('Снять ожидание', undefined, 'sw-button-quiet', () => { const expected = lastPending; if (expected && !busy) showAbandon(expected); });
  pendingActions.append(retry, abandon); pendingBox.append(pendingText, pendingEntry, pendingActions);
  const list = el('ul', 'se-library-grid'); list.dataset.engagementKey = 'library-list';
  const empty = el('div', 'se-library-empty'); empty.append(icon('folder'), el('h3', '', 'Приложения под рукой'), el('p', '', 'Сохраняйте приложения, чтобы возвращаться к ним с других устройств.'));
  const pages = el('div', 'se-library-pages');
  const latest = button('К последним', 'refresh', 'sw-button-quiet', () => { void load(false); }); latest.dataset.engagementKey = 'library-latest';
  const more = button('Ещё сохранённые', 'down', 'sw-button-quiet', () => { void load(true); }); more.dataset.engagementKey = 'library-more';
  pages.append(latest, more); root.append(header, status, pendingBox, list, empty, pages); host.replaceChildren(root);
  let disposed = false, busy = false, request = 0, error = '', notice = '', lastPending: SavedPending | null = null;
  let dialog: WorldDialog | null = null;
  const rows = new Map<string, Row>();
  const current = (): boolean => !disposed && options.isCurrent();
  const closeDialog = (): void => { const previous = dialog; dialog = null; previous?.close(); };
  function showAbandon(expected: SavedPending): void {
    closeDialog(); const value = createDialog('Снять ожидание?', () => { if (dialog === value) dialog = null; }); dialog = value; value.element.classList.add('se-dialog');
    value.body.append(el('p', '', 'Запрос мог быть выполнен. Уберём только локальное ожидание и заново прочитаем сохранённые.'));
    const actions = el('div', 'se-actions'), no = button('Оставить', undefined, 'sw-button-quiet', closeDialog);
    actions.append(no, button('Снять ожидание', undefined, 'sw-button-quiet', () => {
      closeDialog(); void mutations.abandon(expected).then(async () => { if (current()) await load(false); }).catch(reason => { if (current()) { error = engagementError(reason); render(); } });
    })); value.body.append(actions); no.focus();
  }
  function createRow(value: SavedEntry, revision: number): Row {
    const element = el('li'); element.dataset.appId = value.appId;
    const card = el('article', 'se-library-card'), mark = el('span', 'se-library-mark'); mark.append(icon('app')); mark.setAttribute('aria-hidden', 'true');
    const copy = el('span', 'se-library-title'), title = el('strong'), state = el('span'); copy.append(title, state);
    const open = el('button', 'se-library-open'); open.type = 'button'; open.dataset.engagementKey = 'library-open'; open.append(mark, copy);
    const url = el('p', 'se-library-url'), tools = el('div', 'se-library-tools');
    const row: Row = { element, value, revision, title, status: state, url, open,
      remove: button('Убрать', 'trash', 'se-library-remove'), discuss: button('Обсудить', 'chat', 'sw-button-quiet') };
    row.remove.dataset.engagementKey = 'library-remove'; row.discuss.dataset.engagementKey = 'library-discuss';
    open.addEventListener('click', () => { if (current() && row.value.current) options.openEntry(entryOnly(row.value)); });
    row.discuss.addEventListener('click', () => { if (current() && row.value.current) options.discussEntry?.(entryOnly(row.value)); });
    row.remove.addEventListener('click', () => {
      if (!current() || busy || lastPending || library.read().loading) return;
      const captured = row.value; void mutate({ intent: { entry: entryOnly(captured), saved: false, expectedRevision: row.revision, currentEntry: captured } });
    });
    tools.append(row.discuss, row.remove); card.append(open, url, tools); element.append(card); return row;
  }
  function render(): void {
    if (!current()) { root.hidden = true; closeDialog(); return; }
    const state = library.read();
    try { lastPending = mutations.read().pending; } catch (reason) { error = engagementError(reason); lastPending = null; }
    pendingBox.hidden = !lastPending;
    if (lastPending) {
      setText(pendingText, lastPending.args.saved ? 'Сохранение ещё не подтверждено.' : 'Удаление из сохранённых ещё не подтверждено.');
      setText(pendingEntry, entryLabel(lastPending.entry));
    }
    deny(retry, busy); deny(abandon, busy); deny(refreshButton, state.loading || busy); deny(more, state.loading || busy); deny(latest, state.loading || busy);
    const message = error || (state.error ? engagementError(state.error) : '') || (state.resetRequired ? 'Список изменился. Откройте последние сохранённые.' : state.loading && !state.entries.length ? 'Загружаем сохранённые…' : state.stale ? 'Показана прежняя копия списка. Обновите её.' : notice);
    setText(status, message); status.dataset.tone = error || state.error ? 'error' : 'neutral';
    const keep = new Set(state.entries.map(entry => entry.appId));
    for (const [id, row] of rows) if (!keep.has(id)) { row.element.remove(); rows.delete(id); }
    let position: ChildNode | null = list.firstChild;
    for (const value of state.entries) {
      let row = rows.get(value.appId); if (!row) { row = createRow(value, state.revision ?? 0); rows.set(value.appId, row); }
      row.value = value; row.revision = state.revision ?? 0;
      setText(row.title, value.current?.name ?? value.label);
      setText(row.status, value.current ? appStatusLabel(value.current.status) : 'Недоступно · сохранённый вход');
      setText(row.url, entryLabel(value));
      row.open.setAttribute('aria-label', value.current ? `Открыть ${value.current.name}` : `${value.label}: вход недоступен`);
      deny(row.open, !value.current); row.discuss.hidden = !options.discussEntry || !value.current;
      deny(row.remove, busy || !!lastPending || state.loading || state.revision === null);
      row.remove.setAttribute('aria-label', `Убрать из сохранённых: ${value.current?.name ?? value.label}`);
      if (row.element !== position) list.insertBefore(row.element, position); position = row.element.nextSibling;
    }
    empty.hidden = state.loading || !!state.error || !!error || state.entries.length > 0;
    more.hidden = !state.nextCursor || state.resetRequired; latest.hidden = !state.resetRequired && !state.stale;
    pages.hidden = more.hidden && latest.hidden;
  }
  async function load(older: boolean): Promise<void> {
    if (!current() || busy || library.read().loading) return;
    const token = ++request; error = ''; render();
    try { await library.load({ api: options.api, isCurrent: () => current() && token === request, ...(older ? { older: true } : {}) }); }
    catch (reason) { if (current() && token === request) error = engagementError(reason); }
    finally { if (current() && token === request) render(); }
  }
  async function mutate(action: { intent: SavedIntent } | { expectedPending: SavedPending }): Promise<void> {
    if (!current() || busy) return;
    busy = true; request++; library.invalidate(); error = ''; notice = ''; render();
    try {
      const result = await dispatchAppSavedIntent({ state: mutations, api: options.api, isCurrent: current, ...action });
      if (!current()) return;
      if (result.status === 'accepted') { notice = 'Изменение сохранённых подтверждено.'; busy = false; await load(false); if (current()) options.onChanged?.(); }
      else if (result.status === 'superseded') error = 'Запрос изменился в другом окне. Обновите состояние.';
    } catch (reason) { if (current()) error = engagementError(reason); }
    finally { if (current()) { busy = false; render(); } }
  }
  const unLibrary = library.subscribe(render), unMutations = mutations.subscribe(render), view = host.ownerDocument.defaultView;
  const onStorage = (): void => { if (!current()) return; try { mutations.refreshLocal(); render(); } catch (reason) { error = engagementError(reason); render(); } };
  view?.addEventListener('storage', onStorage); render(); void load(false);
  return { refresh: () => load(false), focus: () => { if (current()) refreshButton.focus(); }, dispose() {
    if (disposed) return; disposed = true; request++; unLibrary(); unMutations(); library.dispose(); mutations.dispose(); view?.removeEventListener('storage', onStorage); closeDialog(); root.remove();
  } };
}
