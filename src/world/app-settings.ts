import './app-settings.css';
import { button, el, iconButton, labeledField, textInput } from './dom';
import { errorText } from './dialogs';
import { appPublicationArgs, appSettingsObservationRemaining, appSettingsUpdateArgs, createAppSettingsDraftState, createAppSettingsState, dispatchAppSettingsIntent } from './app-settings-state.mjs';
import { createAppSourceState } from './app-source-state.mjs';
import { describeAppAudience, publicationFromInspection } from './app-audience.mjs';
import { deviceKey } from '../platform/device-key.mjs';
import type { AppInspection, AppSettingsOptions, SettingsPending } from './app-settings.types';

type Confirmation = { kind: 'retire'; domainId: string; origin: string; revision: number } | { kind: 'abandon'; pending: SettingsPending } | { kind: 'reset' } | { kind: 'revoke' }
  | { kind: 'source-reset' } | { kind: 'publication-reset' }
  | { kind: 'leave'; preview?: { domainId: string; path: string }; afterClose?: () => void };
type Completion = { kind: 'name' | 'grants' | 'publication'; args: Record<string, unknown> };
type SourceState = ReturnType<typeof createAppSourceState>;
type SourceIntent = ReturnType<SourceState['promoteIntent']>;
interface SourceDevice { hostDeviceId: string; connectorId: string; name: string; online: boolean; claimed: boolean; bindingVersion: 1 | 2 | null }
const pendingLabel = (pending: SettingsPending): string => pending.op === 'apps.domains.claim' ? 'Закрепление имени' : pending.op === 'apps.domains.retire' ? 'Закрытие адреса' : pending.op === 'apps.source.promote' ? 'Переключение источника' : 'Изменение доступа';
function settingsError(reason: unknown): string {
  const code = reason && typeof reason === 'object' && 'code' in reason ? String(reason.code) : '';
  const messages: Record<string, string> = {
    app_settings_storage_unavailable: 'Браузер не подтвердил локальную запись. Проверьте прежний запрос и текущее состояние приложения.',
    app_settings_lock_unavailable: 'Этот браузер не поддерживает безопасное сохранение запроса. Используйте актуальный браузер.',
    app_settings_pending_unconfirmed: 'Сначала проверьте ранее отправленный запрос.',
    app_settings_pending_changed: 'Запрос изменился в другом окне. Ничего не отправлено; проверьте показанные настройки.',
    app_settings_invalid_receipt: 'Ответ не удалось подтвердить. Сохранён прежний запрос для проверки.',
    app_revision_conflict: 'Название или доступ уже изменились. Ваш ввод сохранён. Обновите состояние для сравнения.',
    app_publication_revision_conflict: 'Публикация изменилась. Результат прежнего запроса не подтверждён; он сохранён без изменений.',
    app_publication_target_conflict: 'Источник приложения изменился. Прежнее согласие не применяется к новому источнику.',
    app_domains_revision_conflict: 'Адреса изменились. Прежний запрос сохранён; проверьте текущее состояние.',
    app_publication_request_conflict: 'Этот запрос не совпал с сохранённой историей. Нужна проверка текущего состояния.',
    app_domain_request_conflict: 'Этот запрос не совпал с историей адресов. Нужна проверка текущего состояния.',
    app_name_unavailable: 'Это имя уже закреплено. Сначала завершите проверку запроса, затем выберите другое.',
    app_name_reserved: 'Это имя зарезервировано платформой. Выберите другое после завершения проверки запроса.',
    invalid_app_slug: 'Имя: 3–48 латинских букв, цифр или дефисов; по краям — буква или цифра.',
    invalid_app_name: 'Введите название длиной от 1 до 64 символов.',
    app_exposure_ack_required: 'Выберите адрес и подтвердите открытие всего проекта.',
    apps_named_zone_disabled: 'Новые именные адреса пока не настроены на сервере.',
    apps_domain_limit_reached: 'Лимит закреплённых имён достигнут. Закрытие адреса не освобождает имя.',
    app_publication_domain_unavailable: 'Один из выбранных адресов уже недоступен. Ваш выбор сохранён для сравнения.',
    app_source_publication_dirty: 'Сначала сохраните или отмените правки доступа выше. Остальной ввод сохранён.',
    app_source_revision_conflict: 'Источник или доступ изменились. Обновите состояние и сравните его с вашим вводом.',
    app_source_invalid_preparation: 'Не удалось подтвердить ответ выбранного источника. Переключение не отправлено.',
    app_source_invalid_target: 'Данные источника не удалось подтвердить. Обновите состояние.',
    app_source_invalid_history: 'Не удалось подтвердить историю источников. Повторите её чтение.',
    app_source_unchanged: 'Это уже текущий источник. Измените устройство, порт или начальную страницу.',
    app_source_unavailable: 'Сейчас этот источник нельзя переключить. Обновите состояние приложения.',
    app_source_review_required: 'Проверьте источник ещё раз и подтвердите показанные настройки.',
    apps_source_preparation_expired: 'Проверка больше не действует. Ввод сохранён; источник можно проверить ещё раз.',
    apps_owner_required: 'Настройки доступны только владельцу приложения.',
    authentication_required: 'Аккаунт изменился. Откройте настройки заново в нужном аккаунте.',
  };
  return messages[code] ?? errorText(reason);
}
function safePermanentLink(value: string): string {
  const url = new URL(value);
  if (!['https:', 'http:'].includes(url.protocol) || url.username || url.password || url.pathname === '/_soty/boot') throw new TypeError('Invalid permanent link');
  return url.href;
}
const text = (node: HTMLElement, value: string): void => { if (node.textContent !== value) node.textContent = value; };
const inputValue = (node: HTMLInputElement, value: string): void => { if (node.value !== value) node.value = value; };
const keyed = <T extends HTMLElement>(node: T, key: string): T => { node.dataset.settingsKey = key; return node; };
// Keep a focused action in the keyboard sequence while it is pending. The host
// capture listener and each submit handler enforce aria-disabled functionally.
const disable = (node: HTMLButtonElement, value: boolean): void => { node.setAttribute('aria-disabled', String(value)); };
const disabled = (node: HTMLButtonElement): boolean => node.getAttribute('aria-disabled') === 'true';
function section(title: string, hint?: string): HTMLElement {
  const value = el('section', 'sw-app-settings-section'); value.append(el('h3', '', title));
  if (hint) value.append(el('p', 'sw-muted sw-app-settings-hint', hint)); return value;
}

/** Form nodes live for the lifetime of the window. Inspection and storage
 * notifications update them in place; no reparenting of editable controls. */
export function mountAppSettings(options: AppSettingsOptions): { dispose(): void; requestClose(trigger?: HTMLElement, afterClose?: () => void): void } {
  const { host, accountId, appId, api } = options;
  const store = createAppSettingsState({ accountId, appId, storage: { getItem: key => localStorage.getItem(key), setItem: (key, value) => localStorage.setItem(key, value) },
    ...(navigator.locks ? { locks: navigator.locks } : {}),
  });
  let model: ReturnType<typeof createAppSettingsDraftState> | null = null;
  let sourceModel: SourceState | null = null;
  let devices: SourceDevice[] = [], devicesLoaded = false, devicesLoading = false, devicesGeneration = 0;
  let deviceError = '', sourceError = '', sourceNotice = '', historyError = '', hadPreparation = false, sourceGeneration = 0, historyGeneration = 0;
  let sourcePrimaryAction: 'promote' | 'recheck' = 'promote', sourceRechecking = false;
  let sourceReadbackNotice: string | null = null;
  let lastDisplayedPending: SettingsPending | null = null;
  let sourceTimer: ReturnType<typeof setTimeout> | null = null;
  let disposed = false, busy = false, refreshing = false, staleInspection = false, notice = '', error = '';
  let confirmation: Confirmation | null = null, confirmTrigger: HTMLElement | null = null;
  let copyFallback: string | null = null, observationDeadline = 0, inspectionGeneration = 0, refreshGeneration = 0, copyGeneration = 0;
  let observationTimer: ReturnType<typeof setTimeout> | null = null;
  const current = (): boolean => !disposed && options.isCurrent();
  const sourceCurrent = (): boolean => current() && document.visibilityState === 'visible' && sourceDetails.open;
  const hasUnsavedDraft = (): boolean => { const state = model?.read(); return Boolean(sourceModel?.read().dirty || (state && (state.nameDirty || state.grantsDirty || state.publicationDirty || state.draft.slug.trim()))); };
  const call = <T>(op: string, args: Record<string, unknown>): Promise<T> => api.request<T>(op, { ...args, expectedAccountId: accountId });
  const getPending = (): { pending: SettingsPending | null; storageError: string } => {
    try { return { pending: store.read().pending, storageError: store.canDispatch() ? '' : settingsError({ code: 'app_settings_lock_unavailable' }) }; }
    catch (reason) { return { pending: null, storageError: settingsError(reason) }; }
  };
  function scheduleObservation(snapshot: AppInspection, elapsed: number): void {
    if (observationTimer) clearTimeout(observationTimer);
    const remaining = appSettingsObservationRemaining(snapshot, elapsed);
    observationDeadline = performance.now() + remaining;
    observationTimer = remaining > 0 ? setTimeout(() => { observationDeadline = 0; if (current()) render(); }, remaining) : null;
  }
  async function inspect(completed?: Completion, completedSource?: SettingsPending): Promise<boolean> {
    const generation = ++inspectionGeneration, started = performance.now();
    try {
      const snapshot = await call<AppInspection>('apps.inspect', { appId });
      if (!current() || generation !== inspectionGeneration) return false;
      if (snapshot.schema !== 'soty.app-inspection.v1' || snapshot.app.id !== appId) throw new TypeError('Invalid app inspection');
      if (model) model.observe(snapshot, completed); else model = createAppSettingsDraftState(snapshot);
      if (sourceModel) sourceModel.observe(snapshot, { publicationDirty: model.read().publicationDirty, ...(completedSource ? { completed: completedSource } : {}) });
      else sourceModel = createAppSourceState({ accountId, appId, snapshot });
      staleInspection = false; scheduleObservation(snapshot, Math.max(0, performance.now() - started)); options.onChanged(snapshot);
      if (sourceReadbackNotice && current()) { sourceNotice = sourceReadbackNotice; notice = sourceNotice; sourceReadbackNotice = null; }
      return true;
    } catch (reason) { if (!current() || generation !== inspectionGeneration) return false; staleInspection = true; throw reason; }
  }
  async function perform(action: () => Promise<void>): Promise<void> {
    if (!current() || busy) return;
    busy = true; refreshing = false; refreshGeneration++; inspectionGeneration++; copyGeneration++;
    error = ''; copyFallback = null; render();
    try { await action(); } catch (reason) { if (current()) error = settingsError(reason); }
    finally { if (current()) { busy = false; render(); } }
  }
  async function refresh(explicit = true): Promise<void> {
    if (!current() || busy || refreshing) return;
    const generation = ++refreshGeneration; refreshing = true; if (explicit) error = ''; render();
    try { if (await inspect() && explicit) notice = 'Состояние обновлено. Несохранённые правки сохранены.'; }
    catch (reason) { if (current() && generation === refreshGeneration) error = settingsError(reason); }
    finally { if (current() && generation === refreshGeneration) { refreshing = false; render(); } }
  }
  async function mutation(op?: SettingsPending['op'], args?: Record<string, unknown>, sourceIntent?: SourceIntent, expectedPending?: SettingsPending): Promise<void> {
    if (!op && !expectedPending) throw Object.assign(new Error('app_settings_pending_changed'), { code: 'app_settings_pending_changed' });
    const context = { state: store, api, isCurrent: current };
    const result = op ? await dispatchAppSettingsIntent({ ...context, op, args: args ?? {},
      ...(sourceIntent ? { expectedSource: sourceIntent.expectedSource, beforeCreate: () => sourceCurrent() && sourceIntent.beforeCreate() } : {}) })
      : await dispatchAppSettingsIntent({ ...context, expectedPending: expectedPending! });
    if (!current() || result.status === 'stale') return;
    staleInspection = true;
    if (result.status !== 'accepted' || !result.pending || !result.response) {
      sourceNotice = '';
      notice = 'Прежний запрос уже завершили в другом окне.'; await inspect(); return;
    }
    const accepted = result.pending;
    if (accepted.op === 'apps.domains.claim') {
      // A replay may precede later activation or retirement. Only inspection
      // describes the current address, not this historical receipt.
      notice = `Подтверждено закрепление имени «${String(accepted.args.slug)}».`;
      if (model && model.read().draft.slug.trim().toLowerCase() === accepted.args.slug) model.patch({ slug: '' });
    } else if (accepted.op === 'apps.domains.retire') notice = 'Закрытие адреса подтверждено. Имя остаётся закреплённым.';
    else if (accepted.op === 'apps.source.promote') {
      hadPreparation = false;
      sourceReadbackNotice = result.response.replayed ? 'Прежнее переключение подтверждено.' : 'Переключение подтверждено.';
      sourceNotice = result.response.replayed ? 'Прежнее переключение подтверждено. Читаем текущий источник.' : 'Переключение подтверждено. Читаем текущий источник.';
      notice = sourceNotice;
    }
    else notice = result.response.replayed ? 'Прежний запрос подтверждён. Текущее состояние могло измениться.' : 'Изменение подтверждено.';
    try {
      const inspected = await inspect(accepted.op === 'apps.publication.update' ? { kind: 'publication', args: accepted.args } : undefined,
        accepted.op === 'apps.source.promote' ? accepted : undefined);
      if (inspected && current() && accepted.op === 'apps.source.promote') {
        if (sourceHistory.open) void loadSourceHistory();
      }
    } catch (reason) {
      if (accepted.op !== 'apps.source.promote') throw reason;
      if (current()) {
        sourceNotice = 'Переключение подтверждено. Текущий источник не удалось проверить.';
        notice = sourceNotice; error = settingsError(reason);
      }
    }
  }
  function changedSource(): void { sourceGeneration++; sourceError = ''; sourceNotice = ''; hadPreparation = false; render(); }
  async function loadSourceDevices(): Promise<void> {
    if (!current() || devicesLoading) return;
    const generation = ++devicesGeneration; devicesLoading = true; deviceError = ''; render();
    try {
      const result = await call<{ devices: SourceDevice[] }>('apps.devices', {});
      if (!current() || generation !== devicesGeneration) return;
      if (!Array.isArray(result.devices) || result.devices.some(value => !value || typeof value.hostDeviceId !== 'string'
        || typeof value.connectorId !== 'string' || typeof value.name !== 'string' || typeof value.online !== 'boolean'
        || typeof value.claimed !== 'boolean' || ![1, 2, null].includes(value.bindingVersion))) throw new TypeError('Invalid source devices');
      devices = result.devices.filter(value => value.claimed); devicesLoaded = true;
    } catch (reason) { if (current() && generation === devicesGeneration) deviceError = settingsError(reason); }
    finally { if (current() && generation === devicesGeneration) { devicesLoading = false; render(); } }
  }
  async function checkSource(): Promise<void> {
    if (!sourceCurrent() || !sourceModel || disabled(sourceCheck) || !sourceForm.reportValidity()) return;
    const generation = ++sourceGeneration;
    sourceError = ''; sourceNotice = ''; hadPreparation = Boolean(sourceModel.read().preparation);
    await perform(async () => {
      try {
        const operation = sourceModel!.prepare({ api, isCurrent: sourceCurrent }); render();
        const result = await operation;
        if (current() && generation === sourceGeneration && result.status === 'ready') hadPreparation = true;
      } catch (reason) { if (current() && generation === sourceGeneration) sourceError = settingsError(reason); }
    });
  }
  async function promoteSource(): Promise<void> {
    if (!sourceCurrent() || !sourceModel || disabled(sourcePromote)) return;
    // Never change a timed-out plain click into a combined action whose label
    // the person has not seen. The next explicit click uses the new label.
    const action = sourcePrimaryAction, state = sourceModel.read();
    if (action === 'promote' ? !state.canPromote : !state.canRecheckPromote) { render(); return; }
    const generation = ++sourceGeneration; sourceError = ''; sourceNotice = 'Сохраняем переключение…';
    await perform(async () => {
      try {
        let intent: SourceIntent;
        if (action === 'recheck') {
          sourceRechecking = true; sourceNotice = 'Перепроверяем выбранный источник…';
          const operation = sourceModel!.recheckPromotion({ api, isCurrent: sourceCurrent }); render();
          const result = await operation;
          if (!sourceCurrent() || generation !== sourceGeneration) return;
          if (result.status === 'stale') { sourceNotice = ''; return; }
          hadPreparation = true;
          if (result.status !== 'ready') { sourceNotice = 'Проверка показала другие настройки. Просмотрите их и подтвердите заново.'; return; }
          intent = result.intent; sourceNotice = 'Сохраняем переключение…';
        } else intent = sourceModel!.promoteIntent();
        await mutation(intent.op, intent.args, intent);
      }
      catch (reason) { if (current() && generation === sourceGeneration) { sourceError = settingsError(reason); sourceNotice = ''; } }
      finally { sourceRechecking = false; }
    });
  }
  async function loadSourceHistory(older = false): Promise<void> {
    if (!current() || !sourceModel || sourceModel.read().history.loading) return;
    const generation = ++historyGeneration; historyError = '';
    const operation = sourceModel.loadHistory({ api, isCurrent: current, older }); render();
    try { await operation; }
    catch (reason) { if (current() && generation === historyGeneration) historyError = settingsError(reason); }
    finally { if (current() && generation === historyGeneration) render(); }
  }
  async function refreshSourceHistory(): Promise<void> {
    if (!current() || !sourceModel || sourceModel.read().history.loading) return;
    if (!sourceModel.read().history.stale) { await loadSourceHistory(); return; }
    if (busy || refreshing) return;
    await perform(async () => { if (await inspect() && current()) await loadSourceHistory(); });
  }
  async function copyLink(value: string): Promise<void> {
    if (!current() || busy) return;
    let link: string;
    try { link = safePermanentLink(value); } catch { error = 'Не удалось подтвердить постоянную ссылку. Обновите состояние.'; render(); return; }
    const generation = ++copyGeneration;
    try { await navigator.clipboard.writeText(link); if (current() && generation === copyGeneration) { notice = 'Постоянная ссылка скопирована.'; copyFallback = null; render(); } }
    catch { if (current() && generation === copyGeneration) { copyFallback = link; notice = 'Скопируйте постоянную ссылку из поля ниже.'; render(); } }
  }
  function preview(domainId?: string): void {
    if (!current() || busy || !model || staleInspection) return;
    const snapshot = model.read().snapshot;
    const selected = domainId ?? snapshot.addresses.canonical?.id ?? snapshot.addresses.aliases.find(value => value.active && value.state === 'bound')?.id;
    if (selected && snapshot.actions.canPreview) {
      const target = { domainId: selected, path: snapshot.source.entryPath };
      if (hasUnsavedDraft()) confirm({ kind: 'leave', preview: target }, document.activeElement instanceof HTMLElement ? document.activeElement : open);
      else options.onPreview(target);
    }
  }
  function confirm(value: Confirmation, trigger: HTMLElement): void { if (!current() || (busy && value.kind !== 'leave')) return; confirmation = value; confirmTrigger = trigger; render(); confirmNo.focus(); }
  function dismissConfirmation(): void {
    const trigger = confirmTrigger; confirmation = null; confirmTrigger = null; render();
    if (current() && trigger?.isConnected && !trigger.closest('[hidden]')) trigger.focus({ preventScroll: true });
  }
  async function acceptConfirmation(): Promise<void> {
    if (!confirmation || disabled(confirmYes)) return;
    const value = confirmation, trigger = confirmTrigger;
    if (value.kind === 'leave') {
      confirmation = null; confirmTrigger = null;
      if (current()) { if (value.preview) options.onPreview(value.preview); else { options.onClose(); value.afterClose?.(); } }
      return;
    }
    await perform(async () => {
      if (value.kind === 'reset') { model?.reset(); notice = 'Для дальнейших правок выбрана текущая версия.'; }
      else if (value.kind === 'publication-reset') { model?.resetPublication(); notice = 'Несохранённые правки доступа отменены.'; }
      else if (value.kind === 'source-reset') { sourceGeneration++; historyGeneration++; sourceModel?.reset(); sourceError = ''; sourceNotice = ''; hadPreparation = false; }
      else if (value.kind === 'abandon') {
        if (!await inspect() || !current()) return;
        const cleared = await store.abandon(value.pending);
        if (current()) notice = cleared ? 'Локальная проверка завершена. Это не отменяет возможное действие на сервере.' : 'Запрос уже изменился в другом окне; новая запись сохранена.';
      } else if (value.kind === 'retire') await mutation('apps.domains.retire', { appId, expectedAccountId: accountId, domainId: value.domainId, expectedDomainsRevision: value.revision });
      else { await call('apps.revoke', { appId }); if (!current()) return; staleInspection = true; notice = 'Приложение закрыто в Сотах.'; await inspect(); }
    });
    if (!current() || confirmation !== value) return;
    const shouldReturnFocus = confirmBox.contains(document.activeElement);
    confirmation = null; confirmTrigger = null; render();
    if (shouldReturnFocus && current()) (trigger?.isConnected && !trigger.closest('[hidden]') ? trigger : summaryName).focus({ preventScroll: true });
  }

  host.classList.add('sw-app-settings');
  const loading = el('p', 'sw-muted', 'Загружаем настройки…');
  const summary = el('div', 'sw-app-settings-summary'), summaryCopy = el('div', 'sw-grow');
  const summaryName = el('strong'), summaryState = el('p', 'sw-muted'); summaryName.tabIndex = -1;
  summaryCopy.append(summaryName, summaryState);
  const fresh = keyed(iconButton('Обновить текущее состояние', 'refresh', () => { void refresh(); }), 'refresh'); summary.append(summaryCopy, fresh);
  const topActions = el('div', 'sw-app-settings-actions');
  const open = keyed(button('Открыть', 'external', 'sw-button-primary', () => preview()), 'preview');
  const copyPrimary = keyed(button('Скопировать ссылку', 'link', 'sw-button-quiet', () => {
    const snapshot = model?.read().snapshot;
    const link = snapshot?.addresses.aliases.find(value => value.active && value.shareUrl)?.shareUrl ?? snapshot?.addresses.canonical?.shareUrl;
    if (link) void copyLink(link);
  }), 'copy-primary'); topActions.append(open, copyPrimary);
  const messages = el('div', 'sw-app-settings-messages');
  const note = el('p', 'sw-app-settings-notice'), failure = el('p', 'sw-error'), storageFailure = el('p', 'sw-error');
  note.setAttribute('role', 'status'); failure.setAttribute('role', 'alert'); storageFailure.setAttribute('role', 'alert');
  const copiedInput = keyed(textInput('', '', 8192), 'copy-fallback'); copiedInput.readOnly = true;
  copiedInput.addEventListener('focus', () => copiedInput.select());
  const copyField = labeledField('Постоянная ссылка — выделите и скопируйте', copiedInput);
  const pendingBox = section('Результат требует проверки', 'Запрос сохранён. Повтор отправит те же настройки с тем же номером. Он может подтвердить прежний результат или выполнить ещё не выполненное действие.');
  pendingBox.classList.add('sw-app-settings-pending'); const pendingTitle = pendingBox.querySelector('h3')!, pendingDetail = el('p');
  const retry = keyed(button('Повторить тот же запрос', 'refresh', 'sw-button-primary', () => {
    const pending = lastDisplayedPending;
    if (pending) void perform(() => mutation(undefined, undefined, undefined, pending));
  }), 'pending-retry');
  const finish = keyed(button('Завершить проверку', 'close', 'sw-button-quiet', () => { const pending = lastDisplayedPending; if (pending) confirm({ kind: 'abandon', pending }, finish); }), 'pending-abandon');
  const pendingActions = el('div', 'sw-app-settings-actions'); pendingActions.append(retry, finish); pendingBox.append(pendingDetail, pendingActions);
  const conflictBox = section('Настройки уже изменились', 'Ваш ввод сохранён. Обновление не заменяет версию, на основе которой вы редактировали.'); conflictBox.classList.add('sw-app-settings-pending');
  const conflictDetail = el('p'), reset = keyed(button('Взять текущие настройки', 'refresh', 'sw-button-quiet', () => confirm({ kind: 'reset' }, reset)), 'reset-current'); conflictBox.append(conflictDetail, reset);
  const unavailableDetail = el('p'), removeUnavailable = keyed(button('Снять недоступные адреса из выбора', 'close', 'sw-button-quiet', () => {
    if (!model) return; const state = model.read(), removed = new Set(state.unavailableDomainIds);
    model.patch({ activeDomainIds: state.draft.activeDomainIds.filter(id => !removed.has(id)), exposureConfirmed: false }); render();
  }), 'remove-unavailable'); conflictBox.append(unavailableDetail, removeUnavailable);
  const confirmBox = section('Подтвердите действие'); confirmBox.classList.add('sw-app-settings-confirm');
  const confirmTitle = confirmBox.querySelector('h3')!, confirmText = el('p');
  const confirmYes = keyed(button('Подтвердить', 'check', 'sw-button-primary', () => { void acceptConfirmation(); }), 'confirm-yes');
  const confirmNo = keyed(button('Оставить как есть', undefined, 'sw-button-quiet', dismissConfirmation), 'confirm-no');
  const confirmActions = el('div', 'sw-app-settings-actions'); confirmActions.append(confirmYes, confirmNo); confirmBox.append(confirmText, confirmActions);
  messages.append(note, failure, storageFailure, copyField, pendingBox, conflictBox, confirmBox);

  const content = el('div', 'sw-app-settings-content');
  const identity = section('Название'), nameForm = el('form', 'sw-app-settings-inline');
  const name = keyed(textInput('', 'Название приложения', 64), 'name'); name.required = true; name.setAttribute('aria-label', 'Название приложения');
  const saveName = keyed(button('Сохранить название', 'check', 'sw-button-quiet'), 'save-name'); saveName.type = 'submit'; nameForm.append(name, saveName); identity.append(nameForm);
  name.addEventListener('input', () => { model?.patch({ name: name.value }); render(); });
  nameForm.addEventListener('submit', event => { event.preventDefault(); if (!model || disabled(saveName) || !nameForm.reportValidity()) return;
    void perform(async () => { const args = appSettingsUpdateArgs(model!.base('name'), model!.read().draft, 'name', accountId); await api.request('apps.update', args); if (!current()) return; staleInspection = true; notice = 'Сохранение названия подтверждено.'; await inspect({ kind: 'name', args }); });
  });

  const addresses = section('Адреса', 'Отмеченные адреса включатся после сохранения доступа. Закрытое имя не освобождается.');
  const aliasList = el('div', 'sw-app-settings-aliases'), noAliases = el('p', 'sw-muted', 'Можно закрепить понятное имя и включить его ниже.');
  type AliasView = { row: HTMLElement; check: HTMLInputElement; title: HTMLElement; state: HTMLElement; controls: HTMLElement; copy: HTMLButtonElement; open: HTMLButtonElement; retire: HTMLButtonElement };
  const aliases = new Map<string, AliasView>();
  function createAlias(id: string): AliasView {
    const row = el('div', 'sw-app-settings-address'), label = el('label', 'sw-app-settings-address-main');
    const check = keyed(el('input'), `alias-${id}`); check.type = 'checkbox';
    check.addEventListener('change', () => {
      if (!model) return; const state = model.read();
      if (!state.snapshot.actions.canPublish || state.snapshot.addresses.aliases.find(value => value.id === id)?.state !== 'bound') return;
      const values = new Set(state.draft.activeDomainIds); check.checked ? values.add(id) : values.delete(id); model.patch({ activeDomainIds: [...values], exposureConfirmed: false }); render();
    });
    const copyText = el('span'), title = el('strong'), state = el('small', 'sw-muted'); copyText.append(title, state); label.append(check, copyText);
    const controls = el('div', 'sw-app-settings-address-actions');
    const copy = keyed(iconButton('Скопировать постоянную ссылку', 'link', () => { const value = model?.read().snapshot.addresses.aliases.find(alias => alias.id === id); if (value?.shareUrl) void copyLink(value.shareUrl); }), `copy-${id}`);
    const openAlias = keyed(iconButton('Проверить этот адрес', 'external', () => preview(id)), `preview-${id}`);
    const retire = keyed(iconButton('Закрыть адрес навсегда', 'close', () => { const snapshot = model?.read().snapshot, value = snapshot?.addresses.aliases.find(alias => alias.id === id); if (snapshot && value?.state === 'bound') confirm({ kind: 'retire', domainId: id, origin: value.origin, revision: snapshot.addresses.revision }, retire); }), `retire-${id}`);
    controls.append(copy, openAlias, retire); row.append(label, controls); aliasList.append(row); return { row, check, title, state, controls, copy, open: openAlias, retire };
  }
  const claimForm = el('form', 'sw-app-settings-claim');
  const slug = keyed(textInput('', 'my-app', 48), 'slug'); slug.minLength = 3; slug.pattern = '[A-Za-z0-9][A-Za-z0-9-]{1,46}[A-Za-z0-9]'; slug.required = true; slug.autocomplete = 'off'; slug.spellcheck = false;
  const claimPreview = el('p', 'sw-app-settings-url');
  slug.addEventListener('input', () => { model?.patch({ slug: slug.value }); render(); });
  const claim = keyed(button('Закрепить имя', 'plus', 'sw-button-quiet'), 'claim'); claim.type = 'submit';
  const nameCheck = keyed(button('Проверить имя', 'search', 'sw-button-quiet', () => {
    if (!claimForm.reportValidity()) return; const value = slug.value.trim();
    void perform(async () => { const result = await call<{ available: boolean; reason?: string }>('apps.names.check', { slug: value });
      if (current()) notice = result.available ? `Имя «${value}» сейчас доступно. Оно закрепится после нажатия «Закрепить имя».` : result.reason === 'disabled' ? 'Именные адреса пока не настроены.' : `Имя «${value}» недоступно. Выберите другое.`;
    });
  }), 'name-check');
  const claimActions = el('div', 'sw-app-settings-actions'); claimActions.append(nameCheck, claim); claimForm.append(labeledField('Новое имя адреса', slug), claimPreview, claimActions);
  claimForm.addEventListener('submit', event => { event.preventDefault(); if (!model || disabled(claim) || !claimForm.reportValidity()) return;
    const value = slug.value.trim(), revision = model.read().snapshot.addresses.revision;
    void perform(() => mutation('apps.domains.claim', { appId, expectedAccountId: accountId, slug: value, expectedDomainsRevision: revision }));
  });
  const claimCount = el('p', 'sw-muted sw-app-settings-hint');
  const claimDetails = keyed(el('details', 'sw-app-settings-details sw-app-settings-add-address'), 'add-address');
  const claimSummary = el('summary', '', 'Добавить адрес');
  let claimInitialised = false;
  claimDetails.append(claimSummary, claimForm, claimCount); addresses.append(aliasList, noAliases, claimDetails);

  const access = section('Доступ');
  const policy = keyed(el('select', 'sw-select'), 'policy'); policy.setAttribute('aria-label', 'Кому открыт доступ');
  for (const [value, label] of [['restricted', 'Владелец и выбранные люди'], ['anyone', 'Все по ссылке']] as const) { const option = el('option', '', label); option.value = value; policy.append(option); }
  policy.addEventListener('change', () => { model?.patch({ launchPolicy: policy.value === 'anyone' ? 'anyone' : 'restricted', exposureConfirmed: false }); render(); });
  const publicDetails = el('div', 'sw-app-settings-public'), ack = el('label', 'sw-app-settings-ack');
  const ackInput = keyed(el('input'), 'public-ack'), ackText = el('span'); ackInput.type = 'checkbox';
  ackInput.addEventListener('change', () => { model?.patch({ exposureConfirmed: ackInput.checked }); render(); }); ack.append(ackInput, ackText);
  const noPublicAliases = el('p', 'sw-muted', 'Сначала отметьте хотя бы один именной адрес.');
  publicDetails.append(el('p', 'sw-muted', 'Открыть сможет любой, кто узнает или угадает адрес.'), ack, noPublicAliases);
  const legacyListed = el('p', 'sw-muted sw-app-settings-hint');
  const publish = keyed(button('Сохранить доступ по адресам', 'check', 'sw-button-primary', () => {
    if (!model) return; void perform(async () => { const args = appPublicationArgs(model!.base('publication'), model!.read().draft, accountId); await mutation('apps.publication.update', args); });
  }), 'publish');
  const groups = keyed(el('details', 'sw-app-settings-details'), 'groups');
  const groupsSummary = el('summary'), contactsHint = el('p', 'sw-muted'), publicGroupsHint = el('p', 'sw-muted', 'Сейчас активные ссылки доступны всем. Список ниже не ограничивает публичный вход.');
  const choices = el('div', 'sw-app-settings-groups'), noGroups = el('p', 'sw-muted', 'Нет доступных групп для добавления.');
  const grants = new Map<string, { row: HTMLElement; input: HTMLInputElement; label: HTMLElement }>();
  const saveGrants = keyed(button('Сохранить выбранные группы', 'check', 'sw-button-quiet', () => {
    if (!model) return; void perform(async () => { const args = appSettingsUpdateArgs(model!.base('grants'), model!.read().draft, 'grants', accountId); await api.request('apps.update', args); if (!current()) return; staleInspection = true; notice = 'Изменение групп подтверждено. Контакты сохранены.'; await inspect({ kind: 'grants', args }); });
  }), 'save-grants');
  groups.append(groupsSummary, contactsHint, publicGroupsHint, choices, noGroups, saveGrants); access.append(policy, publicDetails, legacyListed, publish, groups);

  const source = section('Источник'), sourceState = el('p', 'sw-app-settings-observation'), sourceInfo = el('dl', 'sw-app-settings-source');
  const sourceDevice = el('dd'), sourcePort = el('dd'), sourcePath = el('dd', 'sw-app-settings-path'), sourceTime = el('p', 'sw-muted sw-app-settings-hint');
  sourceInfo.append(el('dt', '', 'Устройство'), sourceDevice, el('dt', '', 'Порт'), sourcePort, el('dt', '', 'Страница'), sourcePath);
  const sourceBinding = el('p', 'sw-muted sw-app-settings-hint');
  const sourceDetails = keyed(el('details', 'sw-app-settings-details sw-app-settings-source-editor'), 'source-editor');
  const sourceSummary = el('summary', '', 'Изменить источник');
  const sourceForm = el('form', 'sw-app-settings-source-form');
  const sourceSelect = keyed(el('select', 'sw-select'), 'source-device'); sourceSelect.required = true;
  const deviceOptions = new Map<string, HTMLOptionElement>();
  const reloadDevices = keyed(iconButton('Обновить список устройств', 'refresh', () => { void loadSourceDevices(); }), 'source-devices-refresh');
  const deviceRow = el('div', 'sw-app-settings-device-row'); deviceRow.append(labeledField('Устройство', sourceSelect), reloadDevices);
  const deviceHint = el('p', 'sw-muted sw-app-settings-hint'), deviceFailure = el('p', 'sw-error'); deviceFailure.setAttribute('role', 'alert');
  const sourcePortInput = keyed(el('input', 'sw-input'), 'source-port');
  sourcePortInput.type = 'number'; sourcePortInput.min = '1024'; sourcePortInput.max = '65535'; sourcePortInput.step = '1'; sourcePortInput.inputMode = 'numeric'; sourcePortInput.required = true;
  const sourcePathInput = keyed(textInput('/', '/', 8192), 'source-path');
  sourcePathInput.required = true; sourcePathInput.pattern = '/.*'; sourcePathInput.autocomplete = 'off'; sourcePathInput.spellcheck = false; sourcePathInput.setAttribute('autocapitalize', 'off');
  const sourceFields = el('div', 'sw-app-settings-source-fields');
  sourceFields.append(labeledField('Порт проекта', sourcePortInput), labeledField('Начальная страница', sourcePathInput));
  const sourceAudience = keyed(el('select', 'sw-select'), 'source-audience');
  for (const [value, label] of [['restricted', 'Владелец и выбранные люди'], ['anyone', 'Все по включённым ссылкам']] as const) {
    const option = el('option', '', label); option.value = value; sourceAudience.append(option);
  }
  sourceSelect.addEventListener('change', () => {
    const selected = devices.find(value => deviceKey(value) === sourceSelect.value);
    if (!sourceModel || !selected) return;
    sourceModel.patch({ hostDeviceId: selected.hostDeviceId, connectorId: selected.connectorId }); changedSource();
  });
  sourcePortInput.addEventListener('input', () => { sourceModel?.patch({ port: sourcePortInput.value }); changedSource(); });
  sourcePathInput.addEventListener('input', () => { sourceModel?.patch({ entryPath: sourcePathInput.value }); changedSource(); });
  sourceAudience.addEventListener('change', () => { sourceModel?.patch({ launchPolicy: sourceAudience.value === 'anyone' ? 'anyone' : 'restricted' }); changedSource(); });
  const sourceAccessBlock = el('div', 'sw-app-settings-source-block'), sourceAccessText = el('p', '', 'Сначала сохраните или отмените правки доступа выше.');
  const resetAccess = keyed(button('Отменить правки доступа', 'close', 'sw-button-quiet', () => confirm({ kind: 'publication-reset' }, resetAccess)), 'source-reset-access');
  sourceAccessBlock.append(sourceAccessText, resetAccess);
  const sourceConflict = el('div', 'sw-app-settings-source-block'), sourceConflictText = el('p');
  const sourceReset = keyed(button('Взять текущий источник', 'refresh', 'sw-button-quiet', () => confirm({ kind: 'source-reset' }, sourceReset)), 'source-reset');
  const sourceHistoryRefresh = keyed(button('Обновить историю', 'refresh', 'sw-button-quiet', () => { void refreshSourceHistory(); }), 'source-history-refresh');
  sourceConflict.append(sourceConflictText, sourceReset, sourceHistoryRefresh);
  const sourceCheck = keyed(button('Проверить', 'check', 'sw-button-quiet'), 'source-check'); sourceCheck.type = 'submit';
  const sourcePromote = keyed(button('Переключить', 'arrow', 'sw-button-primary', () => { void promoteSource(); }), 'source-promote');
  const sourceResult = el('div', 'sw-app-settings-source-result'), sourceResultHeader = el('div', 'sw-app-settings-source-result-header');
  const sourceResultState = el('strong'), sourceCountdown = el('span', 'sw-muted'); sourceResultState.setAttribute('role', 'status'); sourceCountdown.setAttribute('aria-live', 'off');
  sourceResultHeader.append(sourceResultState, sourceCountdown);
  const sourceCheckedTarget = el('p', 'sw-app-settings-source-target'), sourceResultHint = el('p', 'sw-muted sw-app-settings-hint', 'Ответ не подтверждает исправность всех страниц проекта.');
  const sourceAck = el('label', 'sw-app-settings-ack'), sourceAckInput = keyed(el('input'), 'source-public-ack'), sourceAckText = el('span'); sourceAckInput.type = 'checkbox';
  sourceAckInput.addEventListener('change', () => { sourceModel?.patch({ exposureConfirmed: sourceAckInput.checked }); render(); }); sourceAck.append(sourceAckInput, sourceAckText);
  sourceResult.append(sourceResultHeader, sourceCheckedTarget, sourceResultHint, sourceAck, sourcePromote);
  const sourceActions = el('div', 'sw-app-settings-actions'); sourceActions.append(sourceCheck);
  const sourceFailure = el('p', 'sw-error'), sourceFeedback = el('p', 'sw-app-settings-notice'); sourceFailure.setAttribute('role', 'alert'); sourceFeedback.setAttribute('role', 'status');
  const sourcePending = el('div', 'sw-app-settings-source-block');
  const sourcePendingRetry = keyed(button('Проверить результат переключения', 'refresh', 'sw-button-quiet', () => {
    const pending = lastDisplayedPending;
    if (pending?.op !== 'apps.source.promote') return;
    void perform(async () => { try { await mutation(undefined, undefined, undefined, pending); } catch (reason) { if (current()) sourceError = settingsError(reason); } });
  }), 'source-pending-retry');
  sourcePending.append(el('p', '', 'Результат пока не подтверждён. Прежний запрос сохранён.'), sourcePendingRetry);
  sourceForm.append(deviceRow, deviceHint, deviceFailure, sourceFields, labeledField('Доступ после переключения', sourceAudience),
    sourceAccessBlock, sourceConflict, sourceActions, sourceResult, sourceFailure, sourceFeedback, sourcePending);
  sourceForm.addEventListener('submit', event => { event.preventDefault(); void checkSource(); });
  sourceDetails.append(sourceSummary, sourceForm);
  sourceDetails.addEventListener('toggle', () => {
    if (!current()) return;
    if (sourceDetails.open) { if (!devicesLoaded && !devicesLoading) void loadSourceDevices(); }
    else { sourceGeneration++; sourceModel?.invalidatePreparation(); hadPreparation = false; sourceNotice = ''; if (sourceTimer) clearTimeout(sourceTimer); render(); }
  });
  const sourceHistory = keyed(el('details', 'sw-app-settings-details sw-app-settings-source-history'), 'source-history');
  const sourceHistorySummary = el('summary', '', 'Предыдущие источники');
  const sourceHistoryHint = el('p', 'sw-muted', 'Возврат меняет маршрут. Данные проекта не откатываются.');
  const sourceHistoryState = el('p', 'sw-muted sw-app-settings-hint'), sourceHistoryFailure = el('p', 'sw-error'); sourceHistoryFailure.setAttribute('role', 'alert');
  const historyList = el('ul', 'sw-app-settings-history-list');
  const historyRows = new Map<number, { row: HTMLLIElement; title: HTMLElement; path: HTMLElement; meta: HTMLElement; choose: HTMLButtonElement }>();
  const historyActions = el('div', 'sw-app-settings-actions');
  const historyFirst = keyed(button('К последним', 'refresh', 'sw-button-quiet', () => { void refreshSourceHistory(); }), 'source-history-first');
  const historyOlder = keyed(button('Ранее', 'arrow', 'sw-button-quiet', () => { void loadSourceHistory(true); }), 'source-history-older');
  historyActions.append(historyFirst, historyOlder); sourceHistory.append(sourceHistorySummary, sourceHistoryHint, sourceHistoryState, sourceHistoryFailure, historyList, historyActions);
  let historyOpened = false;
  sourceHistory.addEventListener('toggle', () => {
    if (sourceHistory.open && (!historyOpened || sourceModel?.read().history.stale)) { historyOpened = true; void refreshSourceHistory(); }
  });
  source.append(sourceState, sourceInfo, sourceBinding, sourceTime, sourceDetails, sourceHistory);
  const danger = keyed(el('details', 'sw-app-settings-details'), 'danger'); danger.append(el('summary', '', 'Закрыть приложение в Сотах'));
  const revoke = keyed(button('Закрыть все ссылки приложения', 'lock', 'sw-button-quiet sw-button-danger', () => confirm({ kind: 'revoke' }, revoke)), 'revoke');
  danger.append(el('p', 'sw-muted', 'Отдельное действие: закрывает весь доступ через Соты. Для смены аудитории используйте настройки выше.'), revoke);
  content.append(identity, addresses, access, source, danger); host.append(loading, summary, topActions, messages, content);

  function renderSource(blocked: boolean, pending: SettingsPending | null, storageError: string): void {
    if (!model || !sourceModel) return;
    const ordinary = model.read(), { snapshot } = ordinary;
    sourceModel.observe(snapshot, { publicationDirty: ordinary.publicationDirty });
    const state = sourceModel.read(), { draft } = state, editable = snapshot.actions.canEdit;
    if (!state.preparation && !state.preparing && (state.conflict || ordinary.publicationDirty || !editable)) hadPreparation = false;
    const observed = snapshot.source.observation, binding = snapshot.source.binding?.state;
    const observationState = ['responding', 'unreachable'].includes(observed.state) && performance.now() >= observationDeadline ? 'unknown' : observed.state;
    const routeState = binding && { offline: 'Устройство не в сети', legacy: '', 'update-required': 'Нужно обновить Соты Коннектор',
      pending: 'Подключаем источник', bound: '', rejected: 'Устройство не приняло источник', unavailable: 'Подключение источника не подтверждено' }[binding];
    text(sourceState, snapshot.app.state === 'revoked' ? 'Приложение закрыто в Сотах' : routeState || { offline: 'Устройство не в сети', unknown: 'Нет свежей проверки', responding: 'Источник отвечает', unreachable: 'Источник сейчас недоступен' }[observationState]);
    text(sourceBinding, binding === 'bound' ? 'Устройство подтвердило текущий маршрут.' : binding === 'legacy' ? 'На этом устройстве смена источника требует обновления Соты Коннектора.' : '');
    sourceBinding.hidden = !sourceBinding.textContent || snapshot.app.state === 'revoked';
    text(sourceDevice, snapshot.source.deviceName); text(sourcePort, String(snapshot.source.port)); text(sourcePath, snapshot.source.entryPath);
    text(sourceTime, observed.observedAt === null ? 'Пока нет наблюдения от устройства.' : `Последнее наблюдение: ${new Date(observed.observedAt).toLocaleString('ru-RU')}.`);
    const selectedKey = deviceKey(draft), names = new Map<string, number>();
    for (const value of devices) names.set(value.name, (names.get(value.name) ?? 0) + 1);
    const allowedKeys = new Set(devices.map(deviceKey)); allowedKeys.add(selectedKey);
    for (const [key, node] of deviceOptions) if (!allowedKeys.has(key)) { node.remove(); deviceOptions.delete(key); }
    for (const value of devices) {
      const key = deviceKey(value); let node = deviceOptions.get(key);
      if (!node) { node = el('option'); node.value = key; deviceOptions.set(key, node); sourceSelect.append(node); }
      text(node, `${value.name}${(names.get(value.name) ?? 0) > 1 ? ` · ${value.connectorId}` : ''}${value.online ? '' : ' · не в сети'}`);
    }
    const selectedDevice = devices.find(value => deviceKey(value) === selectedKey);
    if (!selectedDevice) {
      let node = deviceOptions.get(selectedKey);
      if (!node) { node = el('option'); node.value = selectedKey; deviceOptions.set(selectedKey, node); sourceSelect.append(node); }
      text(node, !devicesLoaded && selectedKey === deviceKey(snapshot.source) ? snapshot.source.deviceName : 'Выбранное устройство недоступно');
    }
    if (sourceSelect.value !== selectedKey) sourceSelect.value = selectedKey;
    sourceSelect.disabled = !editable; disable(reloadDevices, devicesLoading || !editable); reloadDevices.setAttribute('aria-busy', String(devicesLoading));
    text(deviceHint, devicesLoading ? 'Обновляем список…' : selectedDevice?.bindingVersion === 1 ? 'Для переключения обновите Соты Коннектор на этом устройстве.'
      : selectedDevice && !selectedDevice.online ? 'Устройство не в сети. Проверка уточнит подключение.' : !devicesLoaded ? 'Загрузите список устройств для проверки.'
        : !selectedDevice ? 'Обновите список или выберите другое устройство.' : '');
    deviceHint.hidden = !deviceHint.textContent; text(deviceFailure, deviceError); deviceFailure.hidden = !deviceError;
    inputValue(sourcePortInput, draft.port); inputValue(sourcePathInput, draft.entryPath);
    sourcePortInput.readOnly = sourcePathInput.readOnly = !editable;
    if (sourceAudience.value !== draft.launchPolicy) sourceAudience.value = draft.launchPolicy; sourceAudience.disabled = !editable;
    sourceAccessBlock.hidden = !ordinary.publicationDirty;
    disable(resetAccess, blocked || !ordinary.publicationDirty);
    sourceConflict.hidden = !state.conflict && !state.history.stale;
    text(sourceConflictText, state.conflict ? 'Источник или доступ изменились. Ваш ввод сохранён.' : 'История устарела. Обновите её, чтобы выбрать прежний источник.');
    sourceReset.hidden = !state.conflict; disable(sourceReset, blocked || !editable);
    sourceHistoryRefresh.hidden = !state.history.stale; disable(sourceHistoryRefresh, busy || refreshing || state.history.loading);
    const hasPreparation = Boolean(state.preparation && state.remainingMs > 0);
    sourceResult.hidden = !state.preparation && !hadPreparation;
    text(sourceResultState, hasPreparation ? 'Источник отвечает' : state.preparing ? 'Проверяем ответ…' : 'Нужна новая проверка');
    text(sourceCountdown, hasPreparation ? `${Math.max(0, Math.ceil(state.remainingMs / 1000))} с` : '');
    if (state.preparation) {
      const target = state.preparation.target;
      text(sourceCheckedTarget, `${target.deviceName} · порт ${target.port} · ${target.entryPath}`);
      text(sourceAckText, `Открыть всем весь проект на устройстве «${target.deviceName}», порт ${target.port}, включая его страницы и API.`);
    }
    sourceAck.hidden = draft.launchPolicy !== 'anyone'; sourceAckInput.checked = draft.exposureConfirmed;
    sourceAckInput.disabled = blocked || staleInspection || !state.preparation || !state.canPrepare || !editable;
    sourcePromote.hidden = sourceResult.hidden;
    sourcePrimaryAction = hasPreparation ? 'promote' : 'recheck';
    text(sourcePromote.querySelector('span')!, sourceRechecking ? 'Перепроверяем…' : sourcePrimaryAction === 'recheck' ? 'Перепроверить и переключить' : draft.mode === 'history' ? 'Вернуть этот источник' : 'Переключить');
    text(sourceCheck.querySelector('span')!, state.preparing ? 'Проверяем…' : hasPreparation || hadPreparation ? 'Проверить ещё раз' : 'Проверить');
    disable(sourceCheck, blocked || staleInspection || !state.canPrepare || !selectedDevice || !devicesLoaded || Boolean(deviceError));
    disable(sourcePromote, blocked || staleInspection || !(sourcePrimaryAction === 'recheck' ? state.canRecheckPromote : state.canPromote));
    sourcePromote.setAttribute('aria-busy', String(sourceRechecking));
    sourceCheck.setAttribute('aria-busy', String(state.preparing));
    const sourcePendingVisible = pending?.op === 'apps.source.promote'; sourcePending.hidden = !sourcePendingVisible;
    disable(sourcePendingRetry, busy || Boolean(storageError) || Boolean(confirmation));
    text(sourceFailure, sourceError); sourceFailure.hidden = !sourceError;
    const feedback = sourceNotice || (!sourcePendingVisible && !state.preparing && state.preparation && !hasPreparation ? 'Можно перечитать настройки без спешки. Переключение начнётся с новой проверки.' : '')
      || (draft.launchPolicy === 'anyone' && !snapshot.publication.activeDomainIds.length ? 'Для доступа всем сначала включите именной адрес выше.' : '');
    text(sourceFeedback, feedback); sourceFeedback.hidden = !feedback;
    if (sourceTimer) clearTimeout(sourceTimer);
    sourceTimer = hasPreparation ? setTimeout(() => { sourceTimer = null; if (current()) render(); }, Math.min(1000, state.remainingMs)) : null;
    text(sourceHistoryFailure, historyError); sourceHistoryFailure.hidden = !historyError;
    text(sourceHistoryState, state.history.loading ? 'Читаем предыдущие источники…' : !state.history.targets.length ? 'Здесь появятся сохранённые источники приложения.' : '');
    sourceHistoryState.hidden = !sourceHistoryState.textContent;
    const revisions = new Set(state.history.targets.map(value => value.revision));
    for (const [revision, view] of historyRows) if (!revisions.has(revision)) { view.row.remove(); historyRows.delete(revision); }
    for (const [index, value] of state.history.targets.entries()) {
      let view = historyRows.get(value.revision);
      if (!view) {
        const row = el('li', 'sw-app-settings-history-row'), copy = el('div', 'sw-grow'), title = el('strong'), path = el('p', 'sw-app-settings-path'), meta = el('small', 'sw-muted');
        const choose = keyed(button('Выбрать', undefined, 'sw-button-quiet', () => {
          const selected = sourceModel?.read().history.targets.find(item => item.revision === value.revision);
          if (!selected || disabled(choose) || !current()) return;
          sourceModel!.selectHistory(selected); sourceDetails.open = true; changedSource();
          sourceCheck.focus({ preventScroll: true }); sourceCheck.scrollIntoView({ block: 'nearest' });
        }), `source-history-${value.revision}`);
        copy.append(title, path, meta); row.append(copy, choose); historyList.append(row); view = { row, title, path, meta, choose }; historyRows.set(value.revision, view);
      }
      const at = historyList.children.item(index); if (at !== view.row) historyList.insertBefore(view.row, at);
      text(view.title, `${value.deviceName} · порт ${value.port}`); text(view.path, value.entryPath);
      const active = value.revision === snapshot.source.revision;
      text(view.meta, `${active ? 'Текущий · ' : ''}${new Date(value.createdAt).toLocaleDateString('ru-RU')} · версия ${value.revision}`);
      text(view.choose.querySelector('span')!, active ? 'Текущий' : 'Выбрать');
      disable(view.choose, blocked || staleInspection || !editable || active || state.history.stale);
    }
    text(historyFirst.querySelector('span')!, state.history.stale ? 'Обновить историю' : historyError ? 'Повторить чтение' : 'К последним');
    disable(historyFirst, busy || refreshing || state.history.loading); disable(historyOlder, state.history.loading || !state.history.nextCursor);
  }

  function render(): void {
    if (!current()) return;
    const activeBefore = document.activeElement instanceof HTMLElement && host.contains(document.activeElement) ? document.activeElement : null;
    const { pending, storageError } = getPending();
    lastDisplayedPending = pending;
    text(note, notice); note.hidden = !notice; text(failure, error); failure.hidden = !error; text(storageFailure, storageError); storageFailure.hidden = !storageError;
    copyField.hidden = !copyFallback; if (copyFallback) inputValue(copiedInput, copyFallback);
    loading.hidden = Boolean(model); text(loading, busy || refreshing ? 'Загружаем настройки…' : 'Настройки пока недоступны. Обновите состояние кнопкой справа.');
    summaryCopy.hidden = !model; topActions.hidden = !model; content.hidden = !model;
    disable(fresh, busy || refreshing); fresh.setAttribute('aria-busy', String(refreshing));
    pendingBox.hidden = !pending;
    if (pending) { text(pendingTitle, `${pendingLabel(pending)}: результат требует проверки`); text(pendingDetail, pending.op === 'apps.source.promote' && pending.expectedSource
      ? `${pending.expectedSource.deviceName} · порт ${pending.expectedSource.port} · ${pending.expectedSource.entryPath} · ${pending.args.launchPolicy === 'anyone' ? 'всем по включённым ссылкам' : 'владельцу и выбранным людям'}`
      : pending.op === 'apps.publication.update' ? `${pending.args.launchPolicy === 'anyone' ? 'Всем по ссылке' : 'Ограниченный доступ'} · адресов: ${(pending.args.activeDomainIds as string[]).length}` : pending.op === 'apps.domains.claim' ? `Имя: ${String(pending.args.slug)}` : 'Сохранён запрос на закрытие конкретного адреса.'); }
    disable(retry, busy || Boolean(storageError) || Boolean(confirmation)); disable(finish, busy || Boolean(storageError) || Boolean(confirmation));
    confirmBox.hidden = !confirmation;
    if (confirmation) {
      const value = confirmation;
      text(confirmTitle, value.kind === 'leave' ? 'Выйти без сохранения правок?' : value.kind === 'retire' ? 'Закрыть этот адрес навсегда?' : value.kind === 'revoke' ? 'Закрыть приложение в Сотах?' : value.kind === 'reset' ? 'Заменить ваши правки текущими настройками?' : value.kind === 'source-reset' ? 'Заменить ввод текущим источником?' : value.kind === 'publication-reset' ? 'Отменить правки доступа?' : 'Завершить проверку запроса?');
      text(confirmText, value.kind === 'leave' ? 'Несохранённый ввод будет потерян. Уже отправленное действие может завершиться после выхода. Сохранённые запросы останутся для проверки.' : value.kind === 'retire' ? `${value.origin}. Ссылка перестанет открываться. Имя останется закреплённым и не станет свободным.` : value.kind === 'revoke' ? 'Все ссылки и доступ через Соты закроются. Сам проект останется на вашем устройстве.' : value.kind === 'reset' ? 'Несохранённые название и настройки доступа будут заменены. Уже отправленный запрос это не отменяет.' : value.kind === 'source-reset' ? 'Ввод устройства, порта, страницы и доступа источника будет заменён. Остальные правки сохранятся.' : value.kind === 'publication-reset' ? 'Аудитория и выбранные адреса вернутся к текущему состоянию. Название, группы и новое имя адреса сохранятся.' : 'Это не отмена на сервере: действие могло уже выполниться или ещё завершиться. Сначала читаем текущее состояние, затем убираем только локальный запрос.');
      text(confirmYes.querySelector('span')!, value.kind === 'leave' ? (value.preview ? 'Открыть без этих правок' : 'Выйти без сохранения') : value.kind === 'reset' || value.kind === 'source-reset' ? 'Взять текущие' : value.kind === 'publication-reset' ? 'Отменить правки доступа' : value.kind === 'abandon' ? 'Проверить и завершить' : 'Закрыть доступ');
      text(confirmNo.querySelector('span')!, value.kind === 'leave' ? 'Продолжить редактирование' : 'Оставить как есть');
    }
    disable(confirmYes, busy && confirmation?.kind !== 'leave'); disable(confirmNo, busy && confirmation?.kind !== 'leave');
    if (!model) { conflictBox.hidden = true; messages.hidden = !notice && !error && !storageError && !copyFallback && !pending && !confirmation; return; }
    const state = model.read(), { snapshot, draft } = state, blocked = busy || Boolean(pending) || Boolean(storageError) || Boolean(confirmation);
    const savedAudience = describeAppAudience({ status: snapshot.app.state, ownerAccountId: accountId,
      grants: snapshot.app.grants, publication: publicationFromInspection(snapshot) }, accountId);
    text(summaryName, snapshot.app.name);
    text(summaryState, `${staleInspection ? 'Последнее полученное состояние: ' : ''}${savedAudience.details.join(' ')}`);
    disable(open, busy || staleInspection || !snapshot.actions.canPreview);
    disable(copyPrimary, busy || staleInspection || !(snapshot.addresses.aliases.some(value => value.active && value.shareUrl) || snapshot.addresses.canonical?.shareUrl));
    conflictBox.hidden = !(state.nameConflict || state.grantsConflict || state.publicationConflict);
    messages.hidden = !notice && !error && !storageError && !copyFallback && !pending && !confirmation && conflictBox.hidden;
    text(conflictDetail, `Сейчас: «${snapshot.app.name}». ${savedAudience.details.join(' ')} Контактов ${snapshot.app.grants.accountIds.length}, групп ${snapshot.app.grants.communityIds.length}.`);
    disable(reset, busy || Boolean(pending) || Boolean(confirmation));
    unavailableDetail.hidden = removeUnavailable.hidden = !state.unavailableDomainIds.length;
    text(unavailableDetail, `Недоступны выбранные адреса: ${state.unavailableDomainIds.map(id => snapshot.addresses.aliases.find(alias => alias.id === id)?.origin ?? 'ранее выбранный адрес').join(', ')}. Остальные правки сохранены.`);
    disable(removeUnavailable, busy || Boolean(confirmation));
    inputValue(name, draft.name); name.readOnly = !snapshot.actions.canEdit;
    disable(saveName, blocked || !snapshot.actions.canEdit || !state.nameDirty || !draft.name.trim() || state.nameConflict);
    const visibleAliases = new Set(snapshot.addresses.aliases.map(value => value.id));
    for (const [id, value] of aliases) if (!visibleAliases.has(id)) { value.row.remove(); aliases.delete(id); }
    for (const value of snapshot.addresses.aliases) {
      let view = aliases.get(value.id); if (!view) { view = createAlias(value.id); aliases.set(value.id, view); }
      text(view.title, value.origin); text(view.state, value.state === 'tombstone' ? 'Адрес закрыт навсегда' : value.active ? 'Сейчас включён' : 'Сейчас не включён');
      view.check.hidden = value.state !== 'bound'; view.check.checked = draft.activeDomainIds.includes(value.id); view.check.disabled = value.state !== 'bound' || !snapshot.actions.canPublish;
      view.controls.hidden = value.state !== 'bound';
      disable(view.copy, busy || staleInspection || !value.shareUrl); disable(view.open, busy || staleInspection || !snapshot.actions.canPreview || !value.active); disable(view.retire, blocked || !snapshot.actions.canEdit);
    }
    noAliases.hidden = snapshot.addresses.aliases.length > 0;
    // First-time publishing is immediately actionable. Subsequent reads keep
    // the owner's disclosure choice, focused input and unsaved name intact.
    if (!claimInitialised) { claimDetails.open = !snapshot.addresses.aliases.length; claimInitialised = true; }
    inputValue(slug, draft.slug); slug.readOnly = !snapshot.actions.canReserveName;
    let proposedOrigin = '';
    const slugValue = draft.slug.trim().toLowerCase();
    if (/^[a-z0-9][a-z0-9-]{1,46}[a-z0-9]$/u.test(slugValue) && snapshot.addresses.claimOrigin) {
      try { const base = new URL(snapshot.addresses.claimOrigin); if (['http:', 'https:'].includes(base.protocol) && !base.username && !base.password) proposedOrigin = new URL(`${base.protocol}//${slugValue}.${base.host}`).origin; } catch { /* Never invent a runtime zone. */ }
    }
    text(claimPreview, proposedOrigin || (snapshot.addresses.claimOrigin ? '3–48 латинских букв, цифр и дефисов' : 'Новые именные адреса пока недоступны.'));
    disable(claim, blocked || !snapshot.actions.canReserveName); disable(nameCheck, blocked || !snapshot.actions.canReserveName);
    text(claimCount, `Закреплено имён: ${snapshot.addresses.limits.usedByApp} из ${snapshot.addresses.limits.perApp}.`);
    if (policy.value !== draft.launchPolicy) policy.value = draft.launchPolicy; policy.disabled = !snapshot.actions.canPublish;
    publicDetails.hidden = draft.launchPolicy !== 'anyone'; ackInput.checked = draft.exposureConfirmed; ackInput.disabled = !snapshot.actions.canPublish;
    const base = model.base('publication');
    text(ackText, `Открыть всем весь проект на устройстве «${base.source.deviceName}», порт ${base.source.port}, включая его страницы и API.`);
    noPublicAliases.hidden = draft.activeDomainIds.length > 0;
    legacyListed.hidden = !snapshot.publication.listed; text(legacyListed, draft.launchPolicy === 'restricted' ? 'Прежняя отметка каталога будет снята вместе с ограничением доступа.' : 'Прежняя отметка каталога сохранится. Появление в каталоге здесь не обещается.');
    disable(publish, blocked || !snapshot.actions.canPublish || !state.publicationDirty || state.publicationConflict || (draft.launchPolicy === 'anyone' && (!draft.exposureConfirmed || !draft.activeDomainIds.length)));
    text(groupsSummary, `Выбранные люди и группы · ${snapshot.app.grants.accountIds.length + snapshot.app.grants.communityIds.length}`);
    text(contactsHint, snapshot.app.grants.accountIds.length ? `Контактов с доступом: ${snapshot.app.grants.accountIds.length}. Их доступ сохраняется.` : 'Доступ владельца сохраняется всегда.'); publicGroupsHint.hidden = snapshot.publication.launchPolicy !== 'anyone';
    const names = new Map(options.communities.filter(value => value.membership?.state === 'active' && value.permissions.canModerate).map(value => [value.communityId, value.name]));
    for (const id of [...model.base('grants').app.grants.communityIds, ...snapshot.app.grants.communityIds, ...draft.communityIds]) if (!names.has(id)) names.set(id, options.communities.find(value => value.communityId === id)?.name ?? 'Ранее выбранная группа');
    for (const [id, view] of grants) if (!names.has(id)) { view.row.remove(); grants.delete(id); }
    for (const [id, label] of names) {
      let view = grants.get(id);
      if (!view) {
        const row = el('label', 'sw-app-settings-choice'), check = keyed(el('input'), `grant-${id}`), labelNode = el('span'); check.type = 'checkbox';
        check.addEventListener('change', () => { if (!model) return; const values = new Set(model.read().draft.communityIds); check.checked ? values.add(id) : values.delete(id); model.patch({ communityIds: [...values] }); render(); });
        row.append(check, labelNode); choices.append(row); view = { row, input: check, label: labelNode }; grants.set(id, view);
      }
      text(view.label, label); view.input.checked = draft.communityIds.includes(id); view.input.disabled = !snapshot.actions.canEdit;
    }
    noGroups.hidden = names.size > 0; disable(saveGrants, blocked || !snapshot.actions.canEdit || !state.grantsDirty || state.grantsConflict);
    renderSource(blocked, pending, storageError);
    disable(revoke, busy || Boolean(confirmation) || !snapshot.actions.canEdit);
    // Only restore when this update actually hid the focused control. Never
    // reclaim focus from the dialog close button or another chosen control.
    if (activeBefore && (!activeBefore.isConnected || activeBefore.closest('[hidden]')) && (document.activeElement === activeBefore || document.activeElement === document.body)) summaryName.focus({ preventScroll: true });
  }
  const guardDisabled = (event: MouseEvent): void => { const node = event.target instanceof Element ? event.target.closest('button[aria-disabled="true"]') : null; if (node && host.contains(node)) { event.preventDefault(); event.stopImmediatePropagation(); } };
  const storageChanged = (event: StorageEvent): void => { if ((event.key === store.key || event.key === null) && current()) render(); };
  const visible = (): void => {
    if (!current()) return;
    if (document.visibilityState !== 'visible') {
      observationDeadline = 0; if (observationTimer) clearTimeout(observationTimer);
      sourceGeneration++; sourceModel?.invalidatePreparation(); hadPreparation = false; sourceNotice = '';
      if (sourceTimer) clearTimeout(sourceTimer); return;
    }
    // Background suspension must not extend a previous observation's life.
    render(); void refresh(false);
  };
  const beforeUnload = (event: BeforeUnloadEvent): void => { if (current() && (hasUnsavedDraft() || busy)) { event.preventDefault(); event.returnValue = ''; } };
  host.addEventListener('click', guardDisabled, true); window.addEventListener('storage', storageChanged); document.addEventListener('visibilitychange', visible);
  window.addEventListener('beforeunload', beforeUnload);
  render(); void refresh(false);
  return {
    requestClose(trigger, afterClose) {
      if (!current()) { options.onClose(); return; }
      if (confirmation && !afterClose) { if (!busy || confirmation.kind === 'leave') dismissConfirmation(); return; }
      if (hasUnsavedDraft()) confirm({ kind: 'leave', ...(afterClose ? { afterClose } : {}) }, trigger ?? (document.activeElement instanceof HTMLElement ? document.activeElement : summaryName));
      else { options.onClose(); afterClose?.(); }
    },
    dispose() { disposed = true; inspectionGeneration++; refreshGeneration++; copyGeneration++; devicesGeneration++; sourceGeneration++; historyGeneration++;
      lastDisplayedPending = null; sourceReadbackNotice = null; sourceModel?.dispose(); if (sourceTimer) clearTimeout(sourceTimer); if (observationTimer) clearTimeout(observationTimer);
      host.removeEventListener('click', guardDisabled, true); window.removeEventListener('storage', storageChanged); document.removeEventListener('visibilitychange', visible); window.removeEventListener('beforeunload', beforeUnload); },
  };
}
