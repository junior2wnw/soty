import { button, el, iconButton } from './dom';

export interface DialogReturnTarget { isCurrent(): boolean; resolve(): HTMLElement | null; }
export interface DialogCloseContext { interrupted: boolean; }
export interface WorldDialog { element: HTMLDialogElement; body: HTMLElement; close(options?: { restoreFocus?: boolean }): void; }

/** Keep the primary action reachable while optional fields scroll independently. */
export function pinDialogSubmit(dialog: WorldDialog, form: HTMLFormElement, submit: HTMLButtonElement): void {
  form.id ||= `world-form-${crypto.randomUUID()}`;
  submit.type = 'submit'; submit.setAttribute('form', form.id);
  dialog.element.classList.add('sw-form-dialog');
  const actions = el('div', 'sw-dialog-form-actions'); actions.append(submit); dialog.element.append(actions);
}

/** Also accepts an explicit tabindex=-1 workflow heading/main, never a hidden control. */
export function isDialogFocusTarget(node: HTMLElement | null): node is HTMLElement {
  if (!node?.isConnected || node.matches(':disabled, [aria-disabled="true"]') || node.closest('[hidden], [inert]') ||
      (node.tabIndex < 0 && !node.hasAttribute('tabindex')) || !node.getClientRects().length) return false;
  const style = getComputedStyle(node);
  if (style.visibility === 'hidden' || style.visibility === 'collapse') return false;
  for (let parent = node.parentElement; parent; parent = parent.parentElement) {
    if (parent.tagName === 'DETAILS' && !parent.hasAttribute('open') && !parent.querySelector(':scope > summary')?.contains(node)) return false;
  }
  return true;
}

export function createDialog(title: string, onClose?: (context: DialogCloseContext) => void, returnTarget?: DialogReturnTarget): WorldDialog {
  const returnFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  const parentDialog = returnFocus?.closest('dialog[open]') ?? null;
  const dialog = el('dialog', 'sw-dialog');
  let finished = false, superseded = false;
  // Native close restores its original opener before queuing the close event.
  // An intervening action or another focus target cancels our fallback return.
  const interveningAction = (): void => { if (!dialog.open) superseded = true; };
  const interveningFocus = (event: FocusEvent): void => {
    if (!dialog.open && event.target !== returnFocus && event.target !== document.body) superseded = true;
  };
  const finish = (restoreFocus = true): void => {
    if (finished) return;
    finished = true;
    document.removeEventListener('pointerdown', interveningAction, true);
    document.removeEventListener('keydown', interveningAction, true);
    document.removeEventListener('click', interveningAction, true);
    document.removeEventListener('focusin', interveningFocus, true);
    const beforeCleanup = document.activeElement;
    const ownedFocus = beforeCleanup === document.body || beforeCleanup === returnFocus || dialog.contains(beforeCleanup);
    dialog.remove();
    // Dispose and any synchronous render must precede resolving a replacement
    // opener. Managed close cannot leave a later event to destroy that target.
    onClose?.({ interrupted: superseded || !ownedFocus });
    if (!restoreFocus || superseded || !ownedFocus || returnTarget && !returnTarget.isCurrent()) return;
    const afterCleanup = document.activeElement;
    if (afterCleanup !== document.body && afterCleanup !== beforeCleanup) return;
    if (Array.from(document.querySelectorAll('dialog[open]')).some(value => value !== parentDialog)) return;
    const target = returnTarget ? returnTarget.resolve() : returnFocus;
    if (isDialogFocusTarget(target) && (!parentDialog || !parentDialog.hasAttribute('open') || parentDialog.contains(target))) target.focus({ preventScroll: true });
  };
  const close = (options?: { restoreFocus?: boolean }): void => {
    if (finished) return;
    dialog.close();
    finish(options?.restoreFocus !== false);
  };
  const titleId = `world-dialog-${crypto.randomUUID()}`;
  dialog.setAttribute('aria-labelledby', titleId);
  const header = el('div', 'sw-dialog-header');
  const heading = el('h2', '', title); heading.id = titleId;
  header.append(heading, iconButton('Закрыть', 'close', () => close()));
  const body = el('div', 'sw-dialog-content');
  dialog.append(header, body); document.body.append(dialog);
  dialog.addEventListener('close', () => finish(), { once: true });
  dialog.addEventListener('keydown', event => {
    if (event.key !== 'Tab') return;
    const controls = Array.from(dialog.querySelectorAll<HTMLElement>('button, input, textarea, select, summary, a[href], [tabindex]')).filter(node => {
      if (node.tabIndex < 0 || node.matches(':disabled') || node.closest('[hidden]') || !node.getClientRects().length) return false;
      // Collapsed details can retain layout boxes for their hidden content.
      // Only the first direct summary remains in the native tab sequence.
      for (let parent = node.parentElement; parent && parent !== dialog; parent = parent.parentElement) {
        if (parent.tagName === 'DETAILS' && !parent.hasAttribute('open') && !parent.querySelector(':scope > summary')?.contains(node)) return false;
      }
      return true;
    });
    const first = controls[0], last = controls.at(-1);
    if (!first || !last) { event.preventDefault(); return; }
    if (event.shiftKey && (document.activeElement === first || !dialog.contains(document.activeElement))) { event.preventDefault(); last.focus(); }
    else if (!event.shiftKey && (document.activeElement === last || !dialog.contains(document.activeElement))) { event.preventDefault(); first.focus(); }
  });
  dialog.showModal();
  document.addEventListener('pointerdown', interveningAction, true);
  document.addEventListener('keydown', interveningAction, true);
  document.addEventListener('click', interveningAction, true);
  document.addEventListener('focusin', interveningFocus, true);
  return { element: dialog, body, close };
}

export function switchControl(label: string, checked: boolean, change: (next: boolean) => Promise<void>): HTMLButtonElement {
  const control = button(label, undefined, 'sw-switch');
  control.replaceChildren(); control.setAttribute('role', 'switch'); control.setAttribute('aria-label', label); control.setAttribute('aria-checked', String(checked));
  control.addEventListener('click', () => {
    const next = control.getAttribute('aria-checked') !== 'true';
    control.disabled = true;
    void change(next).then(() => control.setAttribute('aria-checked', String(next))).catch(() => { /* The caller presents the server error; the confirmed value stays unchanged. */ }).finally(() => { control.disabled = false; });
  });
  return control;
}

export function errorText(error: unknown): string {
  const code = typeof error === 'object' && error !== null && 'code' in error ? String(error.code) : '';
  const messages: Record<string, string> = {
    ACTIVE_PROFILE_CHANGED: 'Аккаунт изменился. Проверьте выбранный аккаунт и повторите действие.',
    world_revision_conflict: 'Данные изменились. Обновите и повторите действие.',
    revision_conflict: 'Данные изменились. Обновите и повторите действие.',
    field_revision_conflict: 'Расстановка изменилась в другом окне. Выберите, какую версию оставить.',
    field_local_revision_conflict: 'На этом устройстве изменили поле в другом окне. Выберите версию.',
    field_request_conflict: 'Этот перенос уже сохранён с другими изменениями. Выберите актуальную версию.',
    field_local_storage_unavailable: 'Не удалось записать раскладку на устройство. Повторите сохранение или скачайте её.',
    field_local_storage_corrupt: 'Сохранённая на устройстве раскладка не читается. Скачайте доступную версию и повторите.',
    field_local_capacity: 'На устройстве накопилось слишком много несинхронизированных изменений. Подключитесь к сети.',
    field_limit_exceeded: 'Поле заполнено. Уберите несколько ярлыков или лишнее пространство.',
    field_context_not_empty: 'В пространстве ещё есть ярлыки. Уберите их или подтвердите удаление всего пространства.',
    field_context_missing: 'Пространство изменилось. Выберите его заново.',
    field_move_collision: 'Здесь объекты перекроются. Выберите свободное место.',
    field_collision: 'Здесь объекты перекроются. Выберите свободное место.',
    field_slot_occupied: 'Место занято. Выберите другое или обменяйте ярлыки.',
    field_account_changed: 'Аккаунт изменился. Откройте поле заново.',
    field_network_unavailable: 'Расстановка сохранена на устройстве и отправится после подключения.',
    field_undo_conflict: 'После этого действия поле изменилось. Сначала проверьте текущую расстановку.',
    invalid_directory_query: 'Проверьте поисковую строку и попробуйте снова.',
    invalid_directory_cursor: 'Результаты поиска изменились. Начните поиск заново.',
    community_membership_required: 'Доступ к этому сообществу закрыт. Вернитесь в общий мир.',
    community_not_found: 'Сообщество недоступно или больше не существует.',
    profile_not_found: 'Этот профиль сейчас недоступен.',
    community_banned: 'Участие в этом сообществе ограничено организатором.',
    community_owner_cannot_leave: 'Сначала передайте сообщество другому участнику.',
    contact_request_not_allowed: 'Этот человек сейчас не принимает новые запросы.',
    world_contact_forbidden: 'Этот человек сейчас не принимает новые запросы.',
    rate_limited: 'Слишком много действий. Подождите немного и повторите.',
    app_offline: 'Устройство выключено или потеряло связь.',
    app_source_protocol_required: 'Обновите Соты на выбранном устройстве, чтобы переключать источник.',
    app_binding_pending: 'Устройство ещё подтверждает источник. Обновите состояние через несколько секунд.',
    app_binding_changed: 'Соединение или источник изменились. Обновите состояние приложения.',
    app_source_changed: 'Источник изменился. Откройте приложение заново.',
    app_revoked: 'Приложение отключено владельцем.',
    apps_source_probe_timeout: 'Источник не ответил вовремя. Проверьте запуск проекта, порт и путь.',
    app_source_unreachable: 'Источник не отвечает. Проверьте, запущен ли проект на выбранном устройстве.',
    app_prepare_busy: 'Устройство уже проверяет другие источники. Повторите через несколько секунд.',
    apps_source_preparation_capacity: 'Уже выполняется несколько проверок. Дождитесь их завершения и повторите.',
    apps_source_preparation_expired: 'Срок проверки истёк. Сначала проверьте исход прежнего запроса.',
    apps_source_preparation_stale: 'Устройство переподключилось или источник изменился. Нужна новая проверка.',
    apps_source_preparation_mismatch: 'Проверка не относится к выбранному источнику. Обновите состояние.',
    apps_source_target_unavailable: 'Этот прежний источник больше недоступен.',
    app_source_target_conflict: 'История источников изменилась в другом окне. Обновите её и проверьте выбор заново.',
    app_source_request_conflict: 'Сохранённый запрос не совпал с историей переключений. Проверьте текущее состояние.',
    apps_source_device_ambiguous: 'Уточните подключение выбранного устройства.',
    invalid_app_path: 'Укажите путь внутри проекта, начиная с /. Например, / или /dashboard.',
    app_stopped: 'Приложение остановлено на устройстве.',
    app_access_denied: 'Доступ к приложению закрыт.',
    app_not_found: 'Приложение недоступно.',
    app_connector_unclaimed: 'Сначала подключите это устройство к своему аккаунту.',
    origin_not_configured: 'Адрес приложений ещё не настроен на сервере.',
    local_profile_missing: 'Подключите аккаунт, чтобы продолжить.',
    account_required: 'Подключите аккаунт, чтобы продолжить.',
    invalid_avatar_mime: 'Выберите фотографию PNG, JPEG или WebP.',
    invalid_avatar_data: 'Фотография повреждена. Попробуйте другой файл.',
    avatar_too_large: 'Фотография слишком большая. Выберите файл до 20 МБ.',
    avatar_dimensions: 'Не получилось прочитать размер фотографии.',
    apps_access_denied: 'Доступ к приложению закрыт.',
    apps_device_not_owned: 'Сначала подключите устройство к своему аккаунту.',
    apps_community_admin_required: 'Открыть приложение группе может её владелец или модератор.',
    invalid_app_port: 'Этот порт недоступен. Укажите порт вашего веб-приложения.',
    app_port_already_registered: 'Приложение на этом порту уже добавлено.',
    community_permission_denied: 'Для этого действия нужны права организатора.',
  };
  if (messages[code]) return messages[code]!;
  if (error instanceof TypeError || (error instanceof Error && /fetch|network|failed to fetch/i.test(error.message))) return 'Нет связи с сервером. Проверьте подключение и повторите.';
  return 'Не получилось выполнить действие. Повторите попытку.';
}
