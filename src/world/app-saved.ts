import './app-engagement.css';
import { button, el } from './dom';
import { createDialog, errorText, type WorldDialog } from './dialogs';
import { createAppSavedState, dispatchAppSavedIntent, normalizeAppEntry, normalizeAppSavedSnapshot } from './app-saved-state.mjs';
import type { AppEntry, SavedIntent, SavedPending, SavedSnapshot } from './app-saved-state.mjs';
import type { WorldApi } from './types';

export interface EngagementStorageOptions {
  storage?: Pick<Storage, 'getItem' | 'setItem'>;
  locks?: Pick<LockManager, 'request'> | null;
}
export interface AppSavedOptions extends EngagementStorageOptions {
  api: WorldApi; accountId: string; entry: AppEntry & { name?: string };
  isCurrent(): boolean; onChanged?(): void;
}
export interface AppSavedHandle { dispose(): void; refresh(): Promise<void>; }

export function engagementError(reason: unknown): string {
  const code = typeof reason === 'string' ? reason : reason && typeof reason === 'object' && 'code' in reason ? String(reason.code) : '';
  const messages: Record<string, string> = {
    apps_saved_revision_conflict: 'Сохранённые изменились в другом окне. Обновите список и сравните результат.',
    apps_saved_request_conflict: 'Прежний запрос не совпал с ответом. Сначала проверьте текущее сохранение.',
    apps_saved_capacity: 'Сохранённых приложений уже много. Уберите одно, чтобы добавить новое.',
    apps_saved_cursor_expired: 'Список изменился. Откройте последние сохранённые.',
    app_saved_pending_changed: 'Запрос изменился в другом окне. Ничего не отправлено; проверьте показанное ожидание.',
    app_saved_pending_unconfirmed: 'Сначала проверьте прежнее изменение сохранённых.',
    app_saved_storage_unavailable: 'Браузер не сохранил запрос. Действие не отправлено.',
    app_saved_lock_unavailable: 'Этот браузер не поддерживает безопасное сохранение запроса.',
    app_saved_invalid_receipt: 'Ответ не удалось подтвердить. Прежний запрос сохранён для проверки.',
    app_saved_replace_required: 'У приложения уже сохранён другой вход. Замена требует отдельного подтверждения.',
    app_discussion_storage_unavailable: 'Не удалось записать черновик на устройстве. Скопируйте текст перед закрытием.',
    app_discussion_local_capacity: 'Место для черновиков заполнено. Удалите ненужный черновик после проверки его текста.',
    app_discussion_lock_unavailable: 'Этот браузер не поддерживает безопасную отправку. Скопируйте текст.',
    app_discussion_pending_changed: 'Ожидающее сообщение изменилось в другом окне. Ничего не отправлено.',
    app_discussion_pending_unconfirmed: 'Сначала проверьте ранее отправленное сообщение.',
    app_discussion_draft_conflict: 'Черновик изменился в другом окне. Выберите, какой текст оставить.',
    app_discussion_draft_changed: 'Текст изменился. Проверьте его перед отправкой.',
    app_discussion_invalid_receipt: 'Ответ не удалось подтвердить. Прежнее сообщение сохранено для проверки.',
    apps_discussion_changed: 'Аудитория обсуждения изменилась. Прежний текст остался отдельным черновиком.',
    apps_discussion_request_conflict: 'Не удалось подтвердить прежнее сообщение. Повтор не меняет его текст или получателей.',
    apps_discussion_capacity: 'В обсуждении достигнут лимит сообщений. История остаётся доступной.',
    apps_discussion_rate_limited: 'Подождите немного перед следующим сообщением.',
    apps_discussion_busy: 'Обсуждение занято. Повторите действие через несколько секунд.',
    world_authority_busy: 'Не удалось проверить доступ. Повторите через несколько секунд.',
    app_unavailable: 'Этот вход сейчас недоступен.',
    authentication_required: 'Аккаунт изменился. Откройте приложение заново в нужном аккаунте.',
    ACTIVE_PROFILE_CHANGED: 'Аккаунт изменился. Откройте приложение заново в нужном аккаунте.',
  };
  return messages[code] ?? errorText(typeof reason === 'string' ? { code: reason } : reason);
}
export const sameAppEntry = (a: AppEntry, b: AppEntry): boolean => a.appId === b.appId && a.domainId === b.domainId && a.origin === b.origin && a.path === b.path;
export function entryLabel(entry: AppEntry): string { return `${entry.origin}${entry.path}`; }
export function engagementStorage(host: HTMLElement, options: EngagementStorageOptions): { storage: Pick<Storage, 'getItem' | 'setItem'>; locks?: Pick<LockManager, 'request'> } {
  const view = host.ownerDocument.defaultView;
  const storage = options.storage ?? { getItem: (key: string) => { if (!view) throw new Error('storage_unavailable'); return view.localStorage.getItem(key); },
    setItem: (key: string, value: string) => { if (!view) throw new Error('storage_unavailable'); view.localStorage.setItem(key, value); } };
  const locks = options.locks === null ? undefined : options.locks ?? view?.navigator.locks;
  return { storage, ...(locks ? { locks } : {}) };
}

/** A small toolbar control. Its pressed state is a current server snapshot,
 * never a historical receipt or an optimistic local bookmark. */
export function mountAppSaved(host: HTMLElement, options: AppSavedOptions): AppSavedHandle {
  const entry = normalizeAppEntry({ appId: options.entry.appId, domainId: options.entry.domainId, origin: options.entry.origin, path: options.entry.path });
  const state = createAppSavedState({ accountId: options.accountId, ...engagementStorage(host, options) });
  const root = el('div', 'se-saved'), control = button('Сохранить', 'folder', 'se-saved-control');
  const live = el('span', 'sw-sr-only'); live.setAttribute('role', 'status');
  control.dataset.engagementKey = 'save'; root.append(control, live); host.replaceChildren(root);
  let snapshot: SavedSnapshot | null = null, loading = false, busy = false, disposed = false, generation = 0, refreshAfterMutation = false;
  let error = '', notice = '', lastPending: SavedPending | null = null, dialog: WorldDialog | null = null;
  const current = (): boolean => !disposed && options.isCurrent();
  const disabled = (): boolean => busy || loading;
  function closeDialog(): void { const previous = dialog; dialog = null; previous?.close(); }
  function openDialog(title: string): WorldDialog {
    closeDialog(); const value = createDialog(title, () => { if (dialog === value) dialog = null; });
    value.element.classList.add('se-dialog'); dialog = value; return value;
  }
  function render(): void {
    if (!current()) { root.hidden = true; closeDialog(); return; }
    try { lastPending = state.read().pending; } catch (reason) { error = engagementError(reason); lastPending = null; }
    control.setAttribute('aria-pressed', String(!!snapshot?.entry));
    control.setAttribute('aria-disabled', String(disabled()));
    const different = !!snapshot?.entry && !sameAppEntry(snapshot.entry, entry);
    const label = busy ? 'Проверяем…' : lastPending || error ? 'Проверить' : snapshot?.entry ? 'Сохранено' : 'Сохранить';
    const span = control.querySelector('span')!; if (span.textContent !== label) span.textContent = label;
    control.title = lastPending ? 'Проверить прежнее изменение сохранённых' : error || (different ? 'Сохранён другой вход в это приложение' : snapshot?.entry ? 'Убрать из сохранённых' : 'Сохранить в аккаунте');
    control.setAttribute('aria-label', 'Сохранить приложение');
    control.dataset.state = loading ? 'loading' : lastPending ? 'pending' : error ? 'error' : snapshot?.entry ? different ? 'different-entry' : 'saved' : 'unsaved';
    const message = error || notice; if (live.textContent !== message) live.textContent = message;
  }
  async function refresh(): Promise<void> {
    if (!current()) return;
    if (busy) { refreshAfterMutation = true; return; }
    const token = ++generation; loading = true; render();
    try {
      const value = await options.api.request('apps.saved.get', { appId: entry.appId, expectedAccountId: options.accountId });
      if (!current() || token !== generation) return;
      snapshot = normalizeAppSavedSnapshot(value, entry.appId); error = '';
    } catch (reason) {
      if (!current() || token !== generation) return;
      snapshot = null; error = engagementError(reason);
    } finally { if (current() && token === generation) { loading = false; render(); } }
  }
  async function mutate(action: { intent: SavedIntent } | { expectedPending: SavedPending }): Promise<void> {
    if (!current() || busy) return;
    busy = true; generation++; loading = false; error = ''; notice = ''; render();
    try {
      const result = await dispatchAppSavedIntent({ state, api: options.api, isCurrent: current, ...action });
      if (!current()) return;
      if (result.status === 'accepted' && result.response) {
        if (result.response.receipt.appId === entry.appId) snapshot = result.response.current;
        else { snapshot = null; refreshAfterMutation = true; }
        notice = snapshot?.entry ? 'Сохранено в аккаунте.' : 'Состояние сохранения проверено.';
        options.onChanged?.();
      } else if (result.status === 'superseded') { error = 'Запрос изменился в другом окне. Обновите состояние.'; }
    } catch (reason) { if (current()) error = engagementError(reason); }
    finally { if (current()) { busy = false; render(); if (refreshAfterMutation) { refreshAfterMutation = false; void refresh(); } } }
  }
  function showPending(pending: SavedPending): void {
    const surface = openDialog('Результат не подтверждён');
    surface.body.append(el('p', '', pending.args.saved ? 'Проверим прежний запрос на сохранение.' : 'Проверим прежний запрос на удаление из сохранённых.'),
      el('p', 'se-muted', entryLabel(pending.entry)));
    const actions = el('div', 'se-actions');
    const dismiss = button('Закрыть', undefined, 'sw-button-quiet', closeDialog);
    const retry = button('Проверить запрос', 'refresh', 'sw-button-primary', () => { closeDialog(); void mutate({ expectedPending: pending }); });
    retry.dataset.engagementKey = 'save-retry';
    const abandon = button('Снять ожидание', undefined, 'sw-button-quiet', () => {
      const confirmation = openDialog('Снять ожидание?');
      confirmation.body.append(el('p', '', 'Запрос мог быть выполнен. Уберём только локальное ожидание и заново прочитаем сохранённые.'));
      const actions = el('div', 'se-actions'), no = button('Оставить', undefined, 'sw-button-quiet', closeDialog);
      actions.append(no, button('Снять ожидание', undefined, 'sw-button-quiet', () => {
        if (!current() || busy) return;
        closeDialog(); void state.abandon(pending).then(async () => { if (current()) await refresh(); }).catch(reason => { if (current()) { error = engagementError(reason); render(); } });
      })); confirmation.body.append(actions); no.focus();
    });
    actions.append(dismiss, retry); surface.body.append(actions, abandon); dismiss.focus();
  }
  control.addEventListener('click', () => {
    if (!current() || disabled()) return;
    const pending = lastPending;
    if (pending) { showPending(pending); return; }
    if (error || !snapshot) {
      const surface = openDialog('Сохранение приложения');
      surface.body.append(el('p', '', error || 'Состояние ещё не проверено.'), button('Обновить состояние', 'refresh', 'sw-button-primary', () => { closeDialog(); void refresh(); })); return;
    }
    const captured = snapshot;
    if (captured.entry && !sameAppEntry(captured.entry, entry)) {
      const surface = openDialog('Сохранён другой вход');
      const pair = el('dl', 'se-entry-pair');
      for (const [label, value] of [['Сохранено', entryLabel(captured.entry)], ['Сейчас открыто', entryLabel(entry)]]) {
        const row = el('div'); row.append(el('dt', '', label), el('dd', '', value)); pair.append(row);
      }
      const no = button('Оставить прежний', undefined, 'sw-button-quiet', closeDialog);
      const replace = button('Заменить вход', undefined, 'sw-button-primary', () => {
        closeDialog(); void mutate({ intent: { entry, saved: true, expectedRevision: captured.revision, currentEntry: captured.entry, replace: true } });
      }); replace.dataset.engagementKey = 'save-replace';
      const actions = el('div', 'se-actions'); actions.append(no, replace);
      surface.body.append(pair, actions, button('Убрать из сохранённых', 'trash', 'sw-button-quiet', () => {
        closeDialog(); void mutate({ intent: { entry, saved: false, expectedRevision: captured.revision, currentEntry: captured.entry } });
      })); no.focus(); return;
    }
    void mutate({ intent: { entry, saved: !captured.entry, expectedRevision: captured.revision, currentEntry: captured.entry } });
  });
  const unsubscribe = state.subscribe(render), view = host.ownerDocument.defaultView;
  const onStorage = (): void => { if (!current()) return; try { state.refreshLocal(); render(); } catch (reason) { error = engagementError(reason); render(); } };
  view?.addEventListener('storage', onStorage); render(); void refresh();
  return { refresh, dispose() { if (disposed) return; disposed = true; generation++; unsubscribe(); state.dispose(); view?.removeEventListener('storage', onStorage); closeDialog(); root.remove(); } };
}
