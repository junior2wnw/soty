import './ai-settings.css';
import { button, el, labeledField, textInput } from './dom';
import { aiErrorLabel, aiMoneyLabel, aiRublesInput, aiRublesToMicrorubles, createAiAccountController, type AiKey, type AiMutation } from './ai-account';
import type { WorldApi } from './types';

const states = {
  provisioning: ['Подключаем ИИ автоматически…', 'Настройка продолжится сама. Можно закрыть это окно и пользоваться Сотами.'],
  ready: ['ИИ готов', 'Помощник и приложения Сот используют ИИ автоматически.'],
  limited: ['ИИ временно ограничен', 'Возможность новых запросов зависит от доступного лимита и загрузки сервиса. Соты продолжат работать.'],
  suspended: ['ИИ приостановлен', 'Сейчас новые запросы ИИ недоступны. Остальные возможности Сот работают.'],
  unavailable: ['Сервис ИИ временно недоступен', 'Подключение восстановится автоматически. Можно продолжать пользоваться Сотами.'],
} as const;
const keyStates: Record<string, string> = { active: 'Действует', paused: 'Приостановлен', revoked: 'Отозван', pending_activation: 'Подключается', expired: 'Срок истёк' };
const dateLabel = (value: string | null): string => value ? new Date(value).toLocaleDateString('ru-RU') : 'Без срока';
function dateInput(value: string | null): string {
  if (!value) return ''; const instant = new Date(value);
  return `${instant.getFullYear()}-${String(instant.getMonth() + 1).padStart(2, '0')}-${String(instant.getDate()).padStart(2, '0')}`;
}
function expiry(value: string, original?: string | null): string | null {
  if (!value) return null;
  if (original && dateInput(original) === value) return original;
  const [year, month, day] = value.split('-').map(Number), instant = new Date(year!, month! - 1, day, 23, 59, 59, 999);
  if (!Number.isFinite(instant.valueOf()) || instant.valueOf() <= Date.now()) throw Object.assign(new Error('ai_invalid_request'), { code: 'ai_invalid_request' });
  return instant.toISOString();
}

export function mountAiSettings(options: { host: HTMLElement; api: WorldApi; accountId: string; isCurrent(): boolean }): { dispose(): void } {
  const { host } = options, root = el('div', 'sw-ai-settings');
  let disposed = false, listOpened = false, editorKey: AiKey | null = null, editing = false;
  let confirmation: { kind: 'rotate' | 'revoke'; key: AiKey; trigger: HTMLButtonElement } | null = null;
  let timer: ReturnType<typeof setTimeout> | null = null, polls = 0, wasBusy = false, lastSecret: string | null = null;
  const current = (): boolean => !disposed && options.isCurrent() && host.isConnected;
  const heading = el('h3', 'sw-ai-state', 'Проверяем ИИ…'), explanation = el('p', 'sw-muted', 'ИИ подключается к вашему аккаунту автоматически.');
  const status = el('section', 'sw-ai-status'); status.setAttribute('aria-live', 'polite'); status.append(heading, explanation);
  const balance = el('dl', 'sw-ai-balance'), metrics = new Map<string, HTMLElement>();
  for (const [field, label] of [['availableMicrorubles', 'Доступно'], ['spentMicrorubles', 'Использовано'], ['heldMicrorubles', 'В обработке'], ['pendingMicrorubles', 'Ожидает уточнения']]) {
    const item = el('div'), value = el('dd', '', '—'); item.append(el('dt', '', label), value); balance.append(item); metrics.set(field!, value);
  }
  const allowance = el('p', 'sw-muted'), usage = el('p', 'sw-muted'), updated = el('p', 'sw-muted sw-ai-updated');
  const refresh = button('Обновить состояние', 'refresh', 'sw-button-quiet', () => { if (current()) { polls = 0; void controller.loadAccount().then(schedule); if (listOpened) void controller.loadKeys(); } });
  const messages = el('div', 'sw-ai-messages'), notice = el('p', 'sw-ai-notice'), error = el('p', 'sw-ai-error');
  notice.setAttribute('role', 'status'); error.setAttribute('role', 'alert'); error.tabIndex = -1; messages.append(notice, error);
  const pending = el('section', 'sw-ai-pending');
  const recheck = button('Проверить результат', 'refresh', 'sw-button-quiet', () => { if (current()) void controller.reconcile(); });
  pending.append(el('h3', '', 'Проверяем предыдущее изменение'), el('p', 'sw-muted', 'Запрос сохранён. Повторная проверка использует тот же запрос и не создаёт второй ключ.'), recheck);
  const secret = el('section', 'sw-ai-secret'); secret.setAttribute('data-pwa-ignore', '');
  const secretValue = el('textarea', 'sw-input sw-ai-secret-value'); secretValue.readOnly = true; secretValue.rows = 3;
  secretValue.autocomplete = 'off'; secretValue.spellcheck = false; secretValue.setAttribute('autocapitalize', 'off'); secretValue.setAttribute('data-pwa-ignore', '');
  const secretTitle = el('h3', '', 'Сохраните ключ сейчас'); secretTitle.tabIndex = -1;
  const copy = button('Скопировать ключ', 'copy', '', () => {
    if (!current()) return;
    void controller.copySecret(value => navigator.clipboard?.writeText ? navigator.clipboard.writeText(value) : Promise.reject());
  });
  const done = button('Готово, скрыть ключ', 'check', 'sw-button-quiet', () => { if (current()) { controller.forgetSecret(); add.focus(); } });
  const secretActions = el('div', 'sw-ai-actions'); secretActions.append(copy, done);
  secret.append(secretTitle, el('p', 'sw-muted', 'Полное значение показывается один раз. Сохраните его в надёжном месте. При закрытии окна оно исчезнет.'), labeledField('Полное значение API-ключа', secretValue), secretActions);
  const advanced = el('details', 'sw-ai-advanced');
  advanced.append(el('summary', '', 'API для своих приложений'), el('p', 'sw-muted', 'Для помощника и приложений в Сотах ничего настраивать не нужно. Внешние API-ключи нужны для ваших собственных интеграций и используют общий лимит аккаунта.'));
  const endpoint = el('code', 'sw-ai-endpoint', 'https://inference.pochinit.online/v1');
  advanced.append(el('p', 'sw-muted', 'Адрес API, совместимого с OpenAI:'), endpoint);
  const add = button('Создать API-ключ', 'plus', '', () => openEditor(null));
  const list = el('div', 'sw-ai-keys'), listStatus = el('p', 'sw-muted');
  advanced.append(listStatus, list, add);
  const editor = el('form', 'sw-ai-editor'); editor.hidden = true; editor.autocomplete = 'off';
  const editorTitle = el('h3', '', 'Новый API-ключ'), name = textInput('', 'Например, «Мой проект»', 100);
  name.required = true; name.autocomplete = 'off';
  const daily = textInput('', 'В пределах общего лимита', 20); daily.inputMode = 'decimal';
  const until = el('input', 'sw-input'); until.type = 'date';
  const active = el('select', 'sw-select');
  for (const [value, label] of [['active', 'Действует'], ['paused', 'Приостановлен']]) { const option = el('option', '', label); option.value = value!; active.append(option); }
  const activeField = labeledField('Состояние ключа', active);
  const modelDetails = el('details', 'sw-ai-models'), allModels = el('input'); allModels.type = 'checkbox'; allModels.checked = true;
  const modelList = el('div', 'sw-ai-model-list'), modelChoices = new Map<string, HTMLInputElement>();
  const allLabel = el('label', 'sw-ai-choice'); allLabel.append(allModels, el('span', '', 'Все доступные модели'));
  modelDetails.append(el('summary', '', 'Доступ к моделям'), allLabel, modelList);
  allModels.addEventListener('change', () => { for (const control of modelChoices.values()) control.disabled = allModels.checked; });
  const fields = el('div', 'sw-ai-fields'); fields.append(labeledField('Лимит этого ключа в сутки, ₽', daily, 'Пустое поле — общий лимит аккаунта. Новый ключ не увеличивает доступный объём ИИ.'), labeledField('Действует до (включительно)', until, 'Пустое поле — без срока действия.'));
  const editorError = el('p', 'sw-ai-error'); editorError.setAttribute('role', 'alert');
  const save = button('Создать ключ', undefined); save.type = 'submit';
  const cancel = button('Отмена', undefined, 'sw-button-quiet', () => { editing = false; editor.hidden = true; add.focus({ preventScroll: true }); });
  const editorActions = el('div', 'sw-ai-actions'); editorActions.append(save, cancel);
  editor.append(editorTitle, labeledField('Название ключа', name), fields, activeField, modelDetails, editorError, editorActions); advanced.append(editor);
  const confirm = el('section', 'sw-ai-confirm'); confirm.hidden = true;
  const confirmTitle = el('h3'), confirmText = el('p', 'sw-muted'); confirmTitle.tabIndex = -1;
  const confirmAction = button('Подтвердить', undefined, '', () => {
    if (!current() || !confirmation || controller.read().busy || controller.read().pending) return;
    const { kind, key } = confirmation; confirmation = null; confirm.hidden = true;
    void controller.mutate(kind, { keyId: key.id });
  });
  const confirmCancel = button('Отмена', undefined, 'sw-button-quiet', () => { const trigger = confirmation?.trigger; confirmation = null; confirm.hidden = true; if (current() && trigger?.isConnected) trigger.focus({ preventScroll: true }); });
  const confirmActions = el('div', 'sw-ai-actions'); confirmActions.append(confirmAction, confirmCancel); confirm.append(confirmTitle, confirmText, confirmActions); advanced.append(confirm);
  root.append(status, balance, allowance, usage, updated, refresh, messages, pending, secret, advanced); host.append(root);

  const controller = createAiAccountController({ api: options.api, accountId: options.accountId, isCurrent: current, onChange: render,
    storage: { getItem: key => localStorage.getItem(key), setItem: (key, value) => localStorage.setItem(key, value), removeItem: key => localStorage.removeItem(key) },
    ...(navigator.locks ? { locks: navigator.locks } : {}),
  });
  function openEditor(key: AiKey | null): void {
    if (!current() || controller.read().busy || controller.read().pending || controller.read().secret || !controller.read().keys) return;
    confirmation = null; confirm.hidden = true; editorKey = key; editing = true; editor.hidden = false; editorError.textContent = '';
    editorTitle.textContent = key ? 'Настройки API-ключа' : 'Новый API-ключ'; save.querySelector('span')!.textContent = key ? 'Сохранить' : 'Создать ключ';
    name.value = key?.name ?? ''; daily.value = aiRublesInput(key?.limits.dailyMicrorubles); until.value = dateInput(key?.expiresAt ?? null);
    active.value = key?.status === 'paused' ? 'paused' : 'active'; activeField.hidden = !key || !['active', 'paused'].includes(key.status);
    allModels.checked = key?.allowedModels === null || !key; modelChoices.clear(); modelList.replaceChildren();
    const ids = [...new Set([...(controller.read().account?.models.map(model => model.id) ?? []), ...(key?.allowedModels ?? [])])];
    for (const id of ids) {
      const control = el('input'); control.type = 'checkbox'; control.checked = key?.allowedModels?.includes(id) ?? false; control.disabled = allModels.checked;
      const label = el('label', 'sw-ai-choice'); label.append(control, el('span', '', id)); modelChoices.set(id, control); modelList.append(label);
    }
    name.focus();
  }
  function ask(kind: 'rotate' | 'revoke', key: AiKey, trigger: HTMLButtonElement): void {
    if (!current() || controller.read().busy || controller.read().pending || controller.read().secret) return;
    editing = false; editor.hidden = true; confirmation = { kind, key, trigger }; confirm.hidden = false;
    confirmTitle.textContent = kind === 'rotate' ? `Заменить ключ «${key.name}»?` : `Отозвать ключ «${key.name}»?`;
    confirmText.textContent = kind === 'rotate'
      ? 'Появится новое значение этого ключа. Прежнее может действовать ещё до 24 часов. Для немедленного отключения отзовите ключ. Расход и лимиты сохранятся.'
      : 'Все значения этого ключа перестанут работать. Приложения, которые его используют, потеряют доступ к ИИ. Помощник Сот продолжит использовать встроенный доступ.';
    confirmAction.querySelector('span')!.textContent = kind === 'rotate' ? 'Заменить ключ' : 'Отозвать ключ'; confirmTitle.focus();
  }
  editor.addEventListener('submit', event => {
    event.preventDefault(); if (!current() || controller.read().busy || controller.read().pending || !editing || !editor.reportValidity()) return;
    try {
      const selected = [...modelChoices].filter(([, control]) => control.checked).map(([id]) => id);
      if (!allModels.checked && selected.length === 0) { editorError.textContent = 'Выберите хотя бы одну модель или разрешите все доступные.'; return; }
      const dailyLimit = aiRublesToMicrorubles(daily.value), expiresAt = expiry(until.value, editorKey?.expiresAt), allowedModels = allModels.checked ? null : selected;
      const args: Record<string, unknown> = {};
      if (!editorKey || name.value.trim() !== editorKey.name) args.name = name.value.trim();
      if (!editorKey || dailyLimit !== (editorKey.limits.dailyMicrorubles ?? null)) args.limits = { dailyMicrorubles: dailyLimit };
      if (!editorKey || expiresAt !== editorKey.expiresAt) args.expiresAt = expiresAt;
      const modelSet = (value: string[] | null): string => JSON.stringify(value ? [...value].sort() : null);
      if (!editorKey || modelSet(allowedModels) !== modelSet(editorKey.allowedModels)) args.allowedModels = allowedModels;
      if (editorKey) {
        args.keyId = editorKey.id;
        if (['active', 'paused'].includes(editorKey.status) && active.value !== editorKey.status) args.status = active.value;
        if (Object.keys(args).length === 1) { editing = false; editor.hidden = true; add.focus(); return; }
        args.expectedPolicyRevision = editorKey.policyRevision;
      }
      const kind: AiMutation = editorKey ? 'update' : 'create';
      void controller.mutate(kind, args);
    } catch (reason) { editorError.textContent = aiErrorLabel(reason); }
  });
  const rows = new Map<string, { row: HTMLElement; title: HTMLElement; meta: HTMLElement; edit: HTMLButtonElement; rotate: HTMLButtonElement; revoke: HTMLButtonElement; key: AiKey }>();
  function render(): void {
    if (!current()) return; const view = controller.read(), account = view.account;
    if (account) {
      [heading.textContent, explanation.textContent] = states[account.state]; heading.dataset.state = account.state;
      const preparingAllowance = account.state === 'provisioning' && account.allowance.grantedMicrorubles === '0';
      for (const [field, node] of metrics) node.textContent = preparingAllowance ? '—' : aiMoneyLabel(account.allowance[field as keyof typeof account.allowance] as string);
      const dailyLimit = account.allowance.limits.dailyMicrorubles;
      allowance.textContent = preparingAllowance ? 'Включённый лимит ИИ подготавливается автоматически.' : `Включено ${aiMoneyLabel(account.allowance.grantedMicrorubles)} на аккаунт один раз${typeof dailyLimit === 'string' ? `, до ${aiMoneyLabel(dailyLimit)} в сутки` : ''}. Это объём использования ИИ, включённый в сервис.`;
      usage.textContent = preparingAllowance ? '' : `${account.usage.requests.toLocaleString('ru-RU')} запросов · ${account.usage.tokens.toLocaleString('ru-RU')} токенов`;
      updated.textContent = `Обновлено в ${new Date(account.updatedAt).toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' })}`;
    } else if (!view.loading) { heading.textContent = 'Не удалось проверить ИИ'; explanation.textContent = 'Соты продолжат работать. Подключение ИИ проверится автоматически.'; }
    notice.textContent = view.notice; notice.hidden = !view.notice; error.textContent = view.error; error.hidden = !view.error;
    pending.hidden = !view.pending; recheck.disabled = view.busy; refresh.disabled = view.loading;
    secret.hidden = !view.secret; if (secretValue.value !== (view.secret ?? '')) secretValue.value = view.secret ?? '';
    if (view.secret && lastSecret !== view.secret) { secretTitle.textContent = `Сохраните ключ «${view.secretKey?.name ?? ''}»`; secretTitle.focus(); }
    lastSecret = view.secret;
    const blocked = view.busy || Boolean(view.pending) || Boolean(view.secret);
    add.disabled = blocked || !view.keys; save.disabled = blocked; cancel.disabled = view.busy;
    name.disabled = view.busy; daily.disabled = view.busy; until.disabled = view.busy; active.disabled = view.busy; allModels.disabled = view.busy;
    for (const control of modelChoices.values()) control.disabled = view.busy || allModels.checked;
    confirmAction.disabled = blocked;
    if (wasBusy && !view.busy && !view.pending && !view.error) { editing = false; editor.hidden = true; }
    if (wasBusy && !view.busy && view.error) error.focus();
    wasBusy = view.busy;
    listStatus.textContent = view.keysLoading && !view.keys ? 'Загружаем ключи…' : view.keys?.length === 0 ? 'Внешних API-ключей пока нет.' : '';
    const ids = new Set(view.keys?.map(key => key.id));
    for (const [id, value] of rows) if (!ids.has(id)) { value.row.remove(); rows.delete(id); }
    for (const key of view.keys ?? []) {
      let item = rows.get(key.id);
      if (!item) {
        const row = el('article', 'sw-ai-key'), title = el('h4'), meta = el('p', 'sw-muted');
        const edit = button('Изменить', 'edit', 'sw-button-quiet', () => openEditor(rows.get(key.id)!.key));
        const rotate = button('Заменить', 'refresh', 'sw-button-quiet', () => ask('rotate', rows.get(key.id)!.key, rotate));
        const revoke = button('Отозвать', 'close', 'sw-button-quiet', () => ask('revoke', rows.get(key.id)!.key, revoke));
        const actions = el('div', 'sw-ai-actions'); actions.append(edit, rotate, revoke); row.append(title, meta, actions); list.append(row);
        item = { row, title, meta, edit, rotate, revoke, key }; rows.set(key.id, item);
      }
      item.key = key; item.title.textContent = key.name;
      item.meta.textContent = `${key.prefix} · ${keyStates[key.status] ?? 'Недоступен'} · ${dateLabel(key.expiresAt)} · использовано ${aiMoneyLabel(key.spentMicrorubles)}${key.lastUsedAt ? ` · последний запрос ${dateLabel(key.lastUsedAt)}` : ''}`;
      for (const [action, label] of [[item.edit, 'Изменить'], [item.rotate, 'Заменить'], [item.revoke, 'Отозвать']] as const) { action.disabled = blocked; action.setAttribute('aria-label', `${label} ключ «${key.name}»`); }
      item.edit.hidden = key.status === 'revoked'; item.rotate.hidden = key.status !== 'active'; item.revoke.hidden = key.status === 'revoked';
    }
  }
  function schedule(): void {
    if (timer) clearTimeout(timer); timer = null;
    if (!current() || document.visibilityState !== 'visible' || polls >= 5) return;
    const state = controller.read(), needsPoll = !state.account || state.account.state !== 'ready' || state.account.allowance.heldMicrorubles !== '0' || state.account.allowance.pendingMicrorubles !== '0';
    if (!needsPoll) return;
    const delay = [2000, 5000, 10_000, 20_000, 30_000][polls++]!;
    timer = setTimeout(() => { timer = null; if (current() && document.visibilityState === 'visible') void controller.loadAccount().then(schedule); }, delay);
  }
  const visible = (): void => { if (timer) clearTimeout(timer); timer = null; if (current() && document.visibilityState === 'visible') { polls = 0; void controller.loadAccount().then(schedule); } };
  advanced.addEventListener('toggle', () => {
    if (!current()) return;
    if (advanced.open && !listOpened) { listOpened = true; void controller.loadKeys(); }
  });
  document.addEventListener('visibilitychange', visible);
  render(); void controller.start().then(schedule);
  return { dispose(): void {
    if (disposed) return; disposed = true; if (timer) clearTimeout(timer); document.removeEventListener('visibilitychange', visible);
    controller.dispose(); secretValue.value = ''; lastSecret = null; name.value = ''; daily.value = ''; until.value = ''; rows.clear(); modelChoices.clear(); root.remove();
  } };
}
