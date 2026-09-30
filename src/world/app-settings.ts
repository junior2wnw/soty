import './app-settings.css';
import { button, el, iconButton, labeledField, textInput } from './dom';
import { errorText } from './dialogs';
import { appPublicationArgs, appSettingsObservationRemaining, appSettingsUpdateArgs, createAppSettingsDraftState, createAppSettingsState, dispatchAppSettingsIntent } from './app-settings-state.mjs';
import type { AppInspection, AppSettingsOptions, SettingsPending } from './app-settings.types';

type Confirmation = { kind: 'retire'; domainId: string; origin: string; revision: number } | { kind: 'abandon'; pending: SettingsPending } | { kind: 'reset' } | { kind: 'revoke' }
  | { kind: 'leave'; preview?: { domainId: string; path: string }; afterClose?: () => void };
type Completion = { kind: 'name' | 'grants' | 'publication'; args: Record<string, unknown> };
const pendingLabel = (pending: SettingsPending): string => pending.op === 'apps.domains.claim' ? 'Закрепление имени' : pending.op === 'apps.domains.retire' ? 'Закрытие адреса' : 'Изменение доступа';
function settingsError(reason: unknown): string {
  const code = reason && typeof reason === 'object' && 'code' in reason ? String(reason.code) : '';
  const messages: Record<string, string> = {
    app_settings_storage_unavailable: 'Браузер не подтвердил локальную запись. Проверьте прежний запрос и текущее состояние приложения.',
    app_settings_lock_unavailable: 'Этот браузер не поддерживает безопасное сохранение запроса. Используйте актуальный браузер.',
    app_settings_pending_unconfirmed: 'Сначала проверьте ранее отправленный запрос.',
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
  let disposed = false, busy = false, refreshing = false, staleInspection = false, notice = '', error = '';
  let confirmation: Confirmation | null = null, confirmTrigger: HTMLElement | null = null;
  let copyFallback: string | null = null, observationDeadline = 0, inspectionGeneration = 0, refreshGeneration = 0, copyGeneration = 0;
  let observationTimer: ReturnType<typeof setTimeout> | null = null;
  const current = (): boolean => !disposed && options.isCurrent();
  const hasUnsavedDraft = (): boolean => { const state = model?.read(); return Boolean(state && (state.nameDirty || state.grantsDirty || state.publicationDirty || state.draft.slug.trim())); };
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
  async function inspect(completed?: Completion): Promise<boolean> {
    const generation = ++inspectionGeneration, started = performance.now();
    try {
      const snapshot = await call<AppInspection>('apps.inspect', { appId });
      if (!current() || generation !== inspectionGeneration) return false;
      if (snapshot.schema !== 'soty.app-inspection.v1' || snapshot.app.id !== appId) throw new TypeError('Invalid app inspection');
      if (model) model.observe(snapshot, completed); else model = createAppSettingsDraftState(snapshot);
      staleInspection = false; scheduleObservation(snapshot, Math.max(0, performance.now() - started)); options.onChanged(snapshot); return true;
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
  async function mutation(op?: SettingsPending['op'], args?: Record<string, unknown>): Promise<void> {
    const result = await dispatchAppSettingsIntent({ state: store, api, isCurrent: current, ...(op ? { op } : {}), ...(args ? { args } : {}) });
    if (!current() || result.status === 'stale') return;
    staleInspection = true;
    if (result.status !== 'accepted' || !result.pending || !result.response) { notice = 'Прежний запрос уже завершили в другом окне. Читаем текущее состояние.'; await inspect(); return; }
    const accepted = result.pending;
    if (accepted.op === 'apps.domains.claim') {
      // A replay may precede later activation or retirement. Only inspection
      // describes the current address, not this historical receipt.
      notice = `Подтверждено закрепление имени «${String(accepted.args.slug)}». Читаем его текущее состояние.`;
      if (model && model.read().draft.slug.trim().toLowerCase() === accepted.args.slug) model.patch({ slug: '' });
    } else if (accepted.op === 'apps.domains.retire') notice = 'Закрытие адреса подтверждено. Имя остаётся закреплённым.';
    else notice = result.response.replayed ? 'Прежний запрос подтверждён. Текущее состояние могло измениться.' : 'Запрос доступа подтверждён. Читаем текущее состояние.';
    await inspect(accepted.op === 'apps.publication.update' ? { kind: 'publication', args: accepted.args } : undefined);
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
  const retry = keyed(button('Повторить тот же запрос', 'refresh', 'sw-button-primary', () => { void perform(() => mutation()); }), 'pending-retry');
  const finish = keyed(button('Завершить проверку', 'close', 'sw-button-quiet', () => { const { pending } = getPending(); if (pending) confirm({ kind: 'abandon', pending }, finish); }), 'pending-abandon');
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
  source.append(sourceState, sourceInfo, sourceTime, el('p', 'sw-muted sw-app-settings-hint', 'Ответ источника не подтверждает исправность программы или доступность домена. Источник здесь не изменяется.'));
  const danger = keyed(el('details', 'sw-app-settings-details'), 'danger'); danger.append(el('summary', '', 'Закрыть приложение в Сотах'));
  const revoke = keyed(button('Закрыть все ссылки приложения', 'lock', 'sw-button-quiet sw-button-danger', () => confirm({ kind: 'revoke' }, revoke)), 'revoke');
  danger.append(el('p', 'sw-muted', 'Отдельное действие: закрывает весь доступ через Соты. Для смены аудитории используйте настройки выше.'), revoke);
  content.append(identity, addresses, access, source, danger); host.append(loading, summary, topActions, messages, content);

  function render(): void {
    if (!current()) return;
    const activeBefore = document.activeElement instanceof HTMLElement && host.contains(document.activeElement) ? document.activeElement : null;
    const { pending, storageError } = getPending();
    text(note, notice); note.hidden = !notice; text(failure, error); failure.hidden = !error; text(storageFailure, storageError); storageFailure.hidden = !storageError;
    copyField.hidden = !copyFallback; if (copyFallback) inputValue(copiedInput, copyFallback);
    loading.hidden = Boolean(model); text(loading, busy || refreshing ? 'Загружаем настройки…' : 'Настройки пока недоступны. Обновите состояние кнопкой справа.');
    summaryCopy.hidden = !model; topActions.hidden = !model; content.hidden = !model;
    disable(fresh, busy || refreshing); fresh.setAttribute('aria-busy', String(refreshing));
    pendingBox.hidden = !pending;
    if (pending) { text(pendingTitle, `${pendingLabel(pending)}: результат требует проверки`); text(pendingDetail, pending.op === 'apps.publication.update' ? `${pending.args.launchPolicy === 'anyone' ? 'Всем по ссылке' : 'Ограниченный доступ'} · адресов: ${(pending.args.activeDomainIds as string[]).length}` : pending.op === 'apps.domains.claim' ? `Имя: ${String(pending.args.slug)}` : 'Сохранён запрос на закрытие конкретного адреса.'); }
    disable(retry, busy || Boolean(storageError) || Boolean(confirmation)); disable(finish, busy || Boolean(storageError) || Boolean(confirmation));
    confirmBox.hidden = !confirmation;
    if (confirmation) {
      const value = confirmation;
      text(confirmTitle, value.kind === 'leave' ? 'Выйти без сохранения правок?' : value.kind === 'retire' ? 'Закрыть этот адрес навсегда?' : value.kind === 'revoke' ? 'Закрыть приложение в Сотах?' : value.kind === 'reset' ? 'Заменить ваши правки текущими настройками?' : 'Завершить проверку запроса?');
      text(confirmText, value.kind === 'leave' ? 'Несохранённый ввод будет потерян. Уже отправленное действие может завершиться после выхода. Сохранённые запросы адресов и доступа останутся для проверки.' : value.kind === 'retire' ? `${value.origin}. Ссылка перестанет открываться. Имя останется закреплённым и не станет свободным.` : value.kind === 'revoke' ? 'Все ссылки и доступ через Соты закроются. Сам проект останется на вашем устройстве.' : value.kind === 'reset' ? 'Несохранённые название и настройки доступа будут заменены. Уже отправленный запрос это не отменяет.' : 'Это не отмена на сервере: действие могло уже выполниться или ещё завершиться. Сначала читаем текущее состояние, затем убираем только локальный запрос.');
      text(confirmYes.querySelector('span')!, value.kind === 'leave' ? (value.preview ? 'Открыть без этих правок' : 'Выйти без сохранения') : value.kind === 'reset' ? 'Взять текущие' : value.kind === 'abandon' ? 'Проверить и завершить' : 'Закрыть доступ');
      text(confirmNo.querySelector('span')!, value.kind === 'leave' ? 'Продолжить редактирование' : 'Оставить как есть');
    }
    disable(confirmYes, busy && confirmation?.kind !== 'leave'); disable(confirmNo, busy && confirmation?.kind !== 'leave');
    if (!model) { conflictBox.hidden = true; messages.hidden = !notice && !error && !storageError && !copyFallback && !pending && !confirmation; return; }
    const state = model.read(), { snapshot, draft } = state, blocked = busy || Boolean(pending) || Boolean(storageError) || Boolean(confirmation);
    text(summaryName, snapshot.app.name);
    text(summaryState, `${staleInspection ? 'Последнее полученное состояние: ' : ''}${snapshot.app.state === 'revoked' ? 'приложение закрыто в Сотах' : snapshot.publication.launchPolicy === 'anyone' ? 'доступ всем по активным ссылкам' : 'доступ владельцу и выбранным людям'}`);
    disable(open, busy || staleInspection || !snapshot.actions.canPreview);
    disable(copyPrimary, busy || staleInspection || !(snapshot.addresses.aliases.some(value => value.active && value.shareUrl) || snapshot.addresses.canonical?.shareUrl));
    conflictBox.hidden = !(state.nameConflict || state.grantsConflict || state.publicationConflict);
    messages.hidden = !notice && !error && !storageError && !copyFallback && !pending && !confirmation && conflictBox.hidden;
    text(conflictDetail, `Сейчас: «${snapshot.app.name}», ${snapshot.publication.launchPolicy === 'anyone' ? 'всем по активным ссылкам' : 'ограниченный доступ'}, контактов ${snapshot.app.grants.accountIds.length}, групп ${snapshot.app.grants.communityIds.length}.`);
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
    const observed = snapshot.source.observation;
    const observationState = ['responding', 'unreachable'].includes(observed.state) && performance.now() >= observationDeadline ? 'unknown' : observed.state;
    text(sourceState, { offline: 'Устройство не в сети', unknown: 'Нет свежей проверки', responding: 'Источник отвечает', unreachable: 'Источник сейчас недоступен' }[observationState]);
    text(sourceDevice, snapshot.source.deviceName); text(sourcePort, String(snapshot.source.port)); text(sourcePath, snapshot.source.entryPath);
    text(sourceTime, observed.observedAt === null ? 'Пока нет наблюдения от устройства.' : `Последнее наблюдение: ${new Date(observed.observedAt).toLocaleString('ru-RU')}.`);
    disable(revoke, busy || Boolean(confirmation) || !snapshot.actions.canEdit);
    // Only restore when this update actually hid the focused control. Never
    // reclaim focus from the dialog close button or another chosen control.
    if (activeBefore && (!activeBefore.isConnected || activeBefore.closest('[hidden]')) && (document.activeElement === activeBefore || document.activeElement === document.body)) summaryName.focus({ preventScroll: true });
  }
  const guardDisabled = (event: MouseEvent): void => { const node = event.target instanceof Element ? event.target.closest('button[aria-disabled="true"]') : null; if (node && host.contains(node)) { event.preventDefault(); event.stopImmediatePropagation(); } };
  const storageChanged = (event: StorageEvent): void => { if ((event.key === store.key || event.key === null) && current()) render(); };
  const visible = (): void => {
    if (!current()) return;
    if (document.visibilityState !== 'visible') { observationDeadline = 0; if (observationTimer) clearTimeout(observationTimer); return; }
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
    dispose() { disposed = true; inspectionGeneration++; refreshGeneration++; copyGeneration++; if (observationTimer) clearTimeout(observationTimer); host.removeEventListener('click', guardDisabled, true); window.removeEventListener('storage', storageChanged); document.removeEventListener('visibilitychange', visible); window.removeEventListener('beforeunload', beforeUnload); },
  };
}
