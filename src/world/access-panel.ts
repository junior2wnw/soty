import { button, el, iconButton, labeledField, textInput } from './dom';
import { createDialog, type WorldDialog } from './dialogs';
import { icon } from './icons';
import type { WorldApi } from './types';
import './access-panel.css';

export interface AccessAvailability { notesCreateEnabled: boolean; audience: string | null }
export interface AccessPanelOptions {
  api: WorldApi;
  accountId: string;
  accountLabel?: string;
  availability?: () => Promise<AccessAvailability>;
}
export interface AccessPanelHandle { dispose(): void; flush(): Promise<void>; hasUnsavedChanges(): boolean }
interface Principal { id: string; accountId: string; clientId: string; label: string; state: string; createdAt: number; revokedAt: number | null }
interface Budget { unit: string; limit: number; reserved: number; spent: number; remaining: number; uncertain: number }
interface Grant {
  id: string; principalId: string; clientId: string; parentGrantId: string | null; rootGrantId: string;
  capabilities: { capabilityId: string; version: number }[]; resources: string[]; effects: string[]; recipients: string[];
  expiresAt: number; createdAt: number; revokedAt: number | null; allowDelegation: boolean; budget?: Budget;
}
interface AccessEvent { id: string; kind: string; objectType: string; objectId: string; actorType: string; actorId: string; createdAt: number }
interface Effect { kind: string; resourceType: string; resourceId: string; revision?: number }
interface Artifact { type: string; id: string; revision?: number }
interface Invocation {
  invocationId: string; clientId: string; principalId: string; grantId: string; capabilityId: string; version: number;
  status: string; cancelRequested: boolean; effectState: string; effects: Effect[]; createdAt: number; updatedAt: number;
  receipt?: { verificationMethod: string; artifacts: Artifact[]; errorCode?: string };
}
type View = 'clients' | 'actions' | 'events';
interface Page<T> { items: T[]; cursor: string | null; next: string | null; previous: (string | null)[]; loaded: boolean; loading: boolean; error: string; request: number }
interface DialogState { dialog: WorldDialog; busy: boolean; dirty: boolean; clearSecret(): void }
const page = <T>(): Page<T> => ({ items: [], cursor: null, next: null, previous: [], loaded: false, loading: false, error: '', request: 0 });
const PAGE_SIZE = 20;
const notesCapability = { capabilityId: 'notes.createDraft', version: 1 } as const;
const effectNames: Record<string, string> = { read: 'Чтение', create: 'Создание', update: 'Изменение', delete: 'Удаление', publish: 'Публикация', send: 'Отправка', spend: 'Расходы' };
const auditNames: Record<string, string> = {
  'access.principals.create': 'Добавлен клиент', 'access.principals.revoke': 'Клиент отключён',
  'access.grants.issue': 'Выдан доступ', 'access.grants.derive': 'Передана часть доступа', 'access.grants.revoke': 'Доступ отозван',
  'access.credentials.issue': 'Создан ключ', 'access.credentials.revoke': 'Ключ отозван'
};
function codeOf(error: unknown): string {
  return typeof error === 'object' && error !== null && 'code' in error ? String(error.code) : '';
}
function errorLabel(error: unknown): string {
  const code = codeOf(error);
  if (['rate_limited', 'RATE_LIMITED'].includes(code)) return 'Слишком много запросов. Подождите немного.';
  if (code === 'cursor_invalid') return 'Список изменился. Обновите его.';
  if (code === 'quota_exceeded') return 'Достигнут лимит подключений. Проверьте действующие доступы.';
  if (code === 'capability_disabled') return 'Подключение записок пока недоступно.';
  if (['not_found', 'access_denied'].includes(code)) return 'Этот доступ уже изменился. Обновите список.';
  return 'Не удалось получить подтверждение сервера. Проверьте связь и повторите.';
}
function dateLabel(time: number): string {
  const date = new Date(time);
  if (!Number.isFinite(date.getTime())) return 'Дата не указана';
  return date.toLocaleString('ru-RU', { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit', ...(date.getFullYear() !== new Date().getFullYear() ? { year: 'numeric' as const } : {}) });
}
function grantName(grant: Grant): string {
  if (grant.capabilities.length === 1 && grant.capabilities[0]?.capabilityId === notesCapability.capabilityId) return 'Создание записок';
  return grant.capabilities.length === 1 ? 'Функция приложения' : `${grant.capabilities.length} функции приложений`;
}
function stateBadge(text: string, tone = ''): HTMLElement {
  const badge = el('span', `sa-state ${tone ? `sa-state-${tone}` : ''}`);
  badge.append(el('span', 'sa-state-dot'), el('span', '', text));
  return badge;
}
function fact(label: string, value: string): HTMLElement {
  const row = el('div', 'sa-fact'); row.append(el('dt', '', label), el('dd', '', value)); return row;
}
function validAvailability(value: AccessAvailability | undefined): AccessAvailability {
  if (!value || value.notesCreateEnabled !== true || typeof value.audience !== 'string') return { notesCreateEnabled: false, audience: null };
  try {
    const url = new URL(value.audience);
    if (url.username || url.password || url.search || url.hash || !['https:', 'http:', 'urn:', 'soty:'].includes(url.protocol)) return { notesCreateEnabled: false, audience: null };
    if (url.protocol === 'http:' && !['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname)) return { notesCreateEnabled: false, audience: null };
    return { notesCreateEnabled: true, audience: value.audience };
  } catch { return { notesCreateEnabled: false, audience: null }; }
}

export function mountAccessPanel(host: HTMLElement, options: AccessPanelOptions): AccessPanelHandle {
  const { api, accountId } = options;
  const workspace = el('section', 'sa-access');
  workspace.setAttribute('aria-label', 'Доступы и действия');
  const instanceId = `sa-${crypto.randomUUID()}`;
  const header = el('header', 'sa-header');
  const heading = el('div', 'sa-heading');
  heading.append(el('p', 'sa-eyebrow', 'Ваш контроль'), el('h1', '', 'Доступы и действия'));
  const account = el('span', 'sa-account'); account.append(icon('person'), el('span', '', options.accountLabel || 'Личный аккаунт'));
  account.title = `Аккаунт ${accountId}`;
  heading.append(account);
  const tools = el('div', 'sa-header-tools');
  const refresh = iconButton('Обновить доступы', 'refresh', () => { void refreshView(); });
  const add = button('Подключить клиента', 'plus', 'sw-button-primary', () => { void openCreate(); });
  add.disabled = true;
  tools.append(refresh, add); header.append(heading, tools);
  const availabilityHint = el('p', 'sa-availability', 'Проверяем доступность новых подключений…');
  availabilityHint.id = `${instanceId}-availability`; add.setAttribute('aria-describedby', availabilityHint.id);
  const tabs = el('div', 'sa-tabs'); tabs.setAttribute('role', 'tablist'); tabs.setAttribute('aria-label', 'Раздел доступа');
  const content = el('div', 'sa-content'); content.id = `${instanceId}-content`; content.setAttribute('role', 'tabpanel'); content.tabIndex = 0;
  const announcement = el('div', 'sa-announcement'); announcement.setAttribute('role', 'status'); announcement.setAttribute('aria-live', 'polite'); announcement.setAttribute('aria-atomic', 'true');
  workspace.append(header, availabilityHint, tabs, announcement, content); host.replaceChildren(workspace);

  let disposed = false;
  let invalid = false;
  let view: View = 'clients';
  let availability: AccessAvailability = { notesCreateEnabled: false, audience: null };
  let availabilityRequest = 0;
  let selectedPrincipal: string | null = null;
  let dialogState: DialogState | null = null;
  const mutations = new Set<Promise<void>>();
  const principals = page<Principal>(); const grants = page<Grant>(); const actions = page<Invocation>(); const events = page<AccessEvent>();
  const knownPrincipals = new Map<string, Principal>();
  const tabButtons = new Map<View, HTMLButtonElement>();
  const alive = (): boolean => !disposed && !invalid;
  const dialogAlive = (state: DialogState): boolean => alive() && dialogState === state && state.dialog.element.isConnected;
  function announce(message: string) { if (alive()) announcement.textContent = message; }

  function closeDialog(force = false) {
    const state = dialogState;
    if (!state || (state.busy && !force)) return;
    dialogState = null; state.clearSecret(); state.dirty = false;
    state.dialog.body.replaceChildren(); state.dialog.close(); state.dialog.element.remove();
  }
  function erase() {
    closeDialog(true);
    knownPrincipals.clear(); selectedPrincipal = null;
    for (const list of [principals, grants, actions, events]) { list.items = []; list.request++; list.cursor = null; list.next = null; list.previous = []; }
    availability = { notesCreateEnabled: false, audience: null }; availabilityRequest++;
    host.replaceChildren();
  }
  function accountClosed() {
    if (!alive()) return;
    invalid = true; erase();
    const message = el('section', 'sa-access sa-closed'); message.setAttribute('role', 'status');
    message.append(icon('lock'), el('h1', '', 'Доступ к аккаунту изменился'), el('p', '', 'Откройте этот раздел заново в нужном аккаунте.'));
    host.replaceChildren(message);
  }
  async function request<T>(method: string, args: Record<string, unknown> = {}): Promise<T> {
    if (!alive()) throw new Error('access_panel_closed');
    try {
      const result = await api.request<T>(method, { ...args, expectedAccountId: accountId });
      if (!alive()) throw new Error('access_panel_closed');
      return result;
    } catch (error) {
      if (['account_mismatch', 'device_revoked', 'authorization_required', 'authentication_required', 'local_profile_missing', 'account_required'].includes(codeOf(error))) accountClosed();
      throw error;
    }
  }
  function openDialog(title: string): DialogState {
    closeDialog(true);
    const dialog = createDialog(title, () => {
      state.clearSecret(); state.dirty = false;
      if (dialogState === state) dialogState = null;
    });
    const state: DialogState = { dialog, busy: false, dirty: false, clearSecret: () => {} };
    dialog.element.classList.add('sa-dialog');
    dialog.element.addEventListener('cancel', event => { if (state.busy) event.preventDefault(); });
    dialogState = state;
    return state;
  }
  function setBusy(state: DialogState, busy: boolean) {
    state.busy = busy;
    state.dialog.element.setAttribute('aria-busy', String(busy));
    const close = state.dialog.element.querySelector<HTMLButtonElement>('.sw-dialog-header button');
    if (close) close.disabled = busy;
  }
  function mutate(state: DialogState, work: () => Promise<void>) {
    if (state.busy || !dialogAlive(state)) return;
    setBusy(state, true);
    const promise = work().finally(() => {
      mutations.delete(promise);
      if (dialogAlive(state)) setBusy(state, false);
      updateAvailabilityHint();
    });
    mutations.add(promise); updateAvailabilityHint();
    void promise.catch(() => { /* Each operation presents its own safe error without logging the request. */ });
  }
  function markChanged() { actions.loaded = false; events.loaded = false; }

  function updateAvailabilityHint() {
    if (!alive()) return;
    add.disabled = !availability.notesCreateEnabled || !availability.audience || mutations.size > 0;
    availabilityHint.textContent = availability.notesCreateEnabled
      ? 'Подключённый клиент получает только выбранные вами действия.'
      : 'Новое подключение пока недоступно. Существующими доступами можно управлять.';
    availabilityHint.classList.toggle('is-ready', availability.notesCreateEnabled);
  }
  async function checkAvailability(): Promise<AccessAvailability> {
    const sequence = ++availabilityRequest;
    let result: AccessAvailability = { notesCreateEnabled: false, audience: null };
    try { if (options.availability) result = validAvailability(await options.availability()); } catch { /* Failure keeps issuance closed. */ }
    if (alive() && sequence === availabilityRequest) { availability = result; updateAvailabilityHint(); }
    return result;
  }
  async function load<T>(list: Page<T>, loader: (cursor: string | null) => Promise<{ items: T[]; next: string | null }>, cursor: string | null, direction: 'reset' | 'next' | 'previous' = 'reset'): Promise<void> {
    const sequence = ++list.request;
    list.loading = true; list.error = ''; render();
    try {
      const result = await loader(cursor);
      if (!alive() || sequence !== list.request) return;
      if (direction === 'next') list.previous.push(list.cursor);
      else if (direction === 'previous') list.previous.pop();
      else list.previous = [];
      list.items = result.items; list.cursor = cursor; list.next = result.next; list.loaded = true;
    } catch (error) {
      if (!alive() || sequence !== list.request) return;
      list.error = errorLabel(error);
    } finally {
      if (alive() && sequence === list.request) { list.loading = false; render(); }
    }
  }
  const listParams = (cursor: string | null): Record<string, unknown> => ({ limit: PAGE_SIZE, ...(cursor ? { cursor } : {}) });
  async function loadPrincipals(cursor: string | null = null, direction: 'reset' | 'next' | 'previous' = 'reset') {
    if (direction === 'reset') { selectedPrincipal = null; grants.request++; grants.items = []; grants.loaded = false; knownPrincipals.clear(); }
    await load(principals, async next => {
      const result = await request<{ principals: Principal[]; cursor: string | null }>('access.principals.list', listParams(next));
      for (const principal of result.principals) knownPrincipals.set(principal.id, principal);
      return { items: result.principals, next: result.cursor };
    }, cursor, direction);
  }
  async function loadGrants(principalId: string, cursor: string | null = null, direction: 'reset' | 'next' | 'previous' = 'reset') {
    await load(grants, async next => {
      const result = await request<{ grants: Grant[]; cursor: string | null }>('access.grants.list', { principalId, ...listParams(next) });
      return { items: result.grants, next: result.cursor };
    }, cursor, direction);
  }
  async function loadActions(cursor: string | null = null, direction: 'reset' | 'next' | 'previous' = 'reset') {
    await load(actions, async next => {
      const result = await request<{ invocations: Invocation[]; nextCursor: string | null }>('access.invocations.list', listParams(next));
      return { items: result.invocations, next: result.nextCursor };
    }, cursor, direction);
  }
  async function loadEvents(cursor: string | null = null, direction: 'reset' | 'next' | 'previous' = 'reset') {
    await load(events, async next => {
      const result = await request<{ events: AccessEvent[]; cursor: string | null }>('access.events.list', listParams(next));
      return { items: result.events, next: result.cursor };
    }, cursor, direction);
  }
  async function refreshView() {
    if (!alive()) return;
    refresh.disabled = true;
    await Promise.allSettled([checkAvailability(), view === 'clients' ? loadPrincipals() : view === 'actions' ? loadActions() : loadEvents()]);
    if (alive()) refresh.disabled = false;
  }
  function selectView(next: View) {
    view = next;
    for (const [name, tab] of tabButtons) { tab.setAttribute('aria-selected', String(name === view)); tab.tabIndex = name === view ? 0 : -1; }
    content.setAttribute('aria-labelledby', `${instanceId}-${view}`);
    render();
    if (next === 'clients' && !principals.loaded && !principals.loading) void loadPrincipals();
    if (next === 'actions' && !actions.loaded && !actions.loading) void loadActions();
    if (next === 'events' && !events.loaded && !events.loading) void loadEvents();
  }
  for (const [name, label, symbol] of [['clients', 'Клиенты', 'tools'], ['actions', 'Действия', 'activity'], ['events', 'История', 'lock']] as const) {
    const tab = button(label, symbol, 'sa-tab', () => selectView(name));
    tab.id = `${instanceId}-${name}`; tab.setAttribute('role', 'tab'); tab.setAttribute('aria-controls', content.id);
    if (name === 'events') tab.setAttribute('aria-label', 'История доступов');
    tabButtons.set(name, tab); tabs.append(tab);
    tab.addEventListener('keydown', event => {
      const names: View[] = ['clients', 'actions', 'events']; const index = names.indexOf(name);
      const next = event.key === 'ArrowRight' ? names[(index + 1) % 3] : event.key === 'ArrowLeft' ? names[(index + 2) % 3] : event.key === 'Home' ? names[0] : event.key === 'End' ? names[2] : undefined;
      if (next) { event.preventDefault(); selectView(next); tabButtons.get(next)?.focus(); }
    });
  }

  function pager<T>(list: Page<T>, navigate: (cursor: string | null, direction: 'next' | 'previous') => Promise<void>): HTMLElement {
    const row = el('nav', 'sa-pagination'); row.setAttribute('aria-label', 'Страницы списка');
    const back = button('Назад', 'back', '', () => { void navigate(list.previous.at(-1) ?? null, 'previous'); });
    const next = button('Дальше', 'next', '', () => { void navigate(list.next, 'next'); });
    const key = list === grants ? `grants-${selectedPrincipal}` : view;
    back.dataset.saFocus = `page-${key}-back`; next.dataset.saFocus = `page-${key}-next`;
    back.disabled = list.loading || !list.previous.length; next.disabled = list.loading || !list.next;
    row.append(back, el('span', 'sa-page-number', `Страница ${list.previous.length + 1}`), next);
    return row;
  }
  function listState<T>(parent: HTMLElement, list: Page<T>, title: string, description: string, retry: () => Promise<void>): boolean {
    if (list.error) {
      const warning = el('div', 'sa-load-error'); warning.setAttribute('role', 'alert');
      warning.append(el('span', '', list.error), button('Повторить', 'refresh', '', () => { void retry(); })); parent.append(warning);
    }
    if (list.loading && !list.loaded) {
      const loading = el('div', 'sa-empty'); loading.setAttribute('role', 'status'); loading.append(icon('refresh'), el('p', '', 'Загружаем…')); parent.append(loading); return true;
    }
    if (!list.items.length && !list.error) {
      const empty = el('div', 'sa-empty'); empty.append(icon(view === 'actions' ? 'activity' : view === 'events' ? 'lock' : 'tools'), el('h2', '', title), el('p', '', description)); parent.append(empty); return true;
    }
    return !list.items.length;
  }
  function render() {
    if (!alive()) return;
    const previousFocus = document.activeElement instanceof HTMLElement && content.contains(document.activeElement) ? document.activeElement : null;
    const focusKey = previousFocus?.dataset.saFocus;
    content.replaceChildren();
    const list = view === 'clients' ? principals : view === 'actions' ? actions : events;
    content.setAttribute('aria-busy', String(list.loading));
    if (view === 'clients') renderClients();
    else if (view === 'actions') renderActions();
    else renderEvents();
    if (previousFocus && !previousFocus.isConnected) {
      const replacement = focusKey ? [...content.querySelectorAll<HTMLElement>('[data-sa-focus]')].find(node => node.dataset.saFocus === focusKey && !node.matches(':disabled')) : null;
      (replacement || content).focus({ preventScroll: true });
    }
  }
  function renderClients() {
    if (listState(content, principals, 'Здесь будут ваши клиенты', 'Выданные разрешения и их отзыв всегда останутся под рукой.', () => loadPrincipals())) return;
    const list = el('div', 'sa-client-list');
    for (const principal of principals.items) {
      const card = el('article', 'sa-client'); card.classList.toggle('is-revoked', principal.state === 'revoked');
      const top = el('div', 'sa-client-header'); const mark = el('span', 'sa-client-mark'); mark.append(icon('tools'));
      const description = el('div', 'sa-client-title'); description.append(el('h2', '', principal.label), el('p', '', `Добавлен ${dateLabel(principal.createdAt)}`));
      const actions = el('div', 'sa-client-actions');
      if (principal.state === 'revoked') actions.append(stateBadge('Отключён'));
      const expand = button(selectedPrincipal === principal.id ? 'Свернуть' : 'Разрешения', 'down', 'sa-expand', () => {
        if (selectedPrincipal === principal.id) { selectedPrincipal = null; grants.request++; render(); }
        else { selectedPrincipal = principal.id; grants.items = []; grants.loaded = false; grants.previous = []; void loadGrants(principal.id); }
      });
      expand.dataset.saFocus = `principal-${principal.id}`;
      expand.setAttribute('aria-expanded', String(selectedPrincipal === principal.id));
      if (selectedPrincipal === principal.id) expand.setAttribute('aria-controls', `${instanceId}-grants-${principal.id}`);
      actions.append(expand); top.append(mark, description, actions); card.append(top);
      if (selectedPrincipal === principal.id) {
        const details = el('div', 'sa-client-details'); details.id = `${instanceId}-grants-${principal.id}`;
        if (!listState(details, grants, 'Разрешений пока нет', 'Клиент не получил действий через этот список.', () => loadGrants(principal.id))) {
          for (const grant of grants.items) details.append(renderGrant(grant, principal));
          if (grants.next || grants.previous.length) details.append(pager(grants, (cursor, direction) => loadGrants(principal.id, cursor, direction)));
        }
        if (principal.state !== 'revoked') {
          const revoke = button('Отключить клиента', 'lock', 'sa-revoke-all', () => { void inspectRevoke(principal); });
          revoke.dataset.saFocus = `revoke-principal-${principal.id}`;
          details.append(revoke);
        }
        card.append(details);
      }
      list.append(card);
    }
    content.append(list);
    if (principals.next || principals.previous.length) content.append(pager(principals, loadPrincipals));
  }
  function renderGrant(grant: Grant, principal: Principal): HTMLElement {
    const row = el('section', 'sa-grant'); const summary = el('div', 'sa-grant-heading');
    const ended = grant.revokedAt !== null || principal.state === 'revoked'; const expired = grant.expiresAt <= Date.now();
    summary.append(el('h3', '', grantName(grant)), stateBadge(ended ? 'Отозван' : expired ? 'Срок истёк' : 'Выдан', ended || expired ? '' : 'accent'));
    const facts = el('dl', 'sa-grant-facts');
    facts.append(fact('Срок', `До ${dateLabel(grant.expiresAt)}`), fact('Данные', grant.resources.every(value => value === 'notes:new') ? 'Только новые личные записки' : 'Выбранные ресурсы приложения'));
    if (grant.parentGrantId) facts.append(fact('Связь', 'Часть ранее выданного доступа'));
    if (grant.budget?.unit === 'invocations') {
      facts.append(fact('Лимит', `Осталось ${grant.budget.remaining} из ${grant.budget.limit} действий`));
      if (grant.budget.reserved > 0) facts.append(fact('Ожидают результата', String(grant.budget.reserved)));
      if (grant.budget.uncertain > 0) facts.append(fact('Не подтверждено', String(grant.budget.uncertain)));
    }
    row.append(summary, facts);
    if (grant.budget?.unit === 'invocations' && grant.budget.limit > 0) {
      const progress = el('progress', 'sa-budget'); progress.max = grant.budget.limit; progress.value = grant.budget.spent + grant.budget.reserved;
      progress.setAttribute('aria-label', `Использовано или зарезервировано ${progress.value} из ${grant.budget.limit} действий`); row.append(progress);
    }
    const detail = el('details', 'sa-permission-detail'); detail.append(el('summary', '', 'Что разрешено'));
    const precise = el('dl', 'sa-grant-facts');
    precise.append(fact('Действия', grant.effects.map(effect => effectNames[effect] || effect).join(', ') || 'Нет'), fact('Получатель', grant.recipients.map(recipient => recipient === 'soty:notes' ? 'Записки в Сотах' : recipient).join(', ') || 'Не указан'),
      fact('Передача доступа', grant.allowDelegation ? 'Разрешена в пределах этого доступа' : 'Не разрешена'));
    if (!grant.capabilities.every(capability => capability.capabilityId === notesCapability.capabilityId)) precise.append(fact('Функции', grant.capabilities.map(capability => `${capability.capabilityId} · v${capability.version}`).join(', ')), fact('Ресурсы', grant.resources.join(', ')));
    detail.append(precise); row.append(detail);
    if (!ended) {
      const revoke = button('Отозвать', 'lock', 'sa-revoke', () => { void inspectRevoke(principal, grant); });
      revoke.dataset.saFocus = `revoke-grant-${grant.id}`; row.append(revoke);
    }
    return row;
  }
  function actionState(item: Invocation): { label: string; tone: string } {
    const unknown = ['uncertain', 'execution_uncertain'].includes(item.status) || item.effectState === 'unknown';
    if (item.cancelRequested && !['succeeded', 'failed', 'cancelled'].includes(item.status)) return { label: unknown ? 'Остановка не подтверждена' : 'Запрошена остановка', tone: 'warning' };
    if (unknown) return { label: 'Результат не подтверждён', tone: 'warning' };
    return ({ accepted: { label: 'Принято', tone: 'accent' }, running: { label: 'Выполняется', tone: 'accent' }, succeeded: { label: 'Завершено', tone: 'success' }, failed: { label: 'Не удалось', tone: 'warning' }, cancelled: { label: 'Остановлено', tone: '' } } as Record<string, { label: string; tone: string }>)[item.status] || { label: 'Статус уточняется', tone: '' };
  }
  function renderActions() {
    if (listState(content, actions, 'Действий пока нет', 'Здесь появятся задания клиентов и их подтверждённые результаты.', () => loadActions())) return;
    const list = el('div', 'sa-history');
    for (const item of actions.items) {
      const row = el('article', 'sa-action'); const status = actionState(item);
      const top = el('div', 'sa-action-header'); const label = el('div');
      label.append(el('h2', '', item.capabilityId === notesCapability.capabilityId ? 'Создание записки' : 'Действие приложения'), el('p', 'sa-muted', `${knownPrincipals.get(item.principalId)?.label || 'Подключённый клиент'} · ${dateLabel(item.createdAt)}`));
      top.append(label, stateBadge(status.label, status.tone)); row.append(top);
      const effects = el('p', 'sa-effect');
      if (item.effectState === 'unknown') effects.textContent = 'Изменения ещё не подтверждены.';
      else if (item.effectState === 'partial') effects.textContent = 'Часть изменений уже выполнена.';
      else if (item.effects.length) effects.textContent = `Изменения сохранены: ${item.effects.length}.`;
      else if (['failed', 'cancelled'].includes(item.status)) effects.textContent = 'Изменений не зафиксировано.';
      if (effects.textContent) row.append(effects);
      const artifact = item.receipt?.artifacts.find(value => value.type === 'note' || value.type === 'notes');
      if (artifact) { const link = el('a', 'sa-result-link', 'Открыть записку'); link.href = `#notes/${encodeURIComponent(artifact.id)}`; link.append(icon('arrow')); row.append(link); }
      const detail = el('details', 'sa-permission-detail'); detail.append(el('summary', '', 'Подробности'));
      const facts = el('dl', 'sa-grant-facts'); facts.append(fact('Обновлено', dateLabel(item.updatedAt)), fact('Клиент', knownPrincipals.get(item.principalId)?.label || item.clientId), fact('Функция', `${item.capabilityId} · v${item.version}`));
      if (item.effectState !== 'none') facts.append(fact('Изменения', item.effects.map(effect => `${({ created: 'Создано', updated: 'Изменено', deleted: 'Удалено', published: 'Опубликовано', sent: 'Отправлено', charged: 'Потрачено' } as Record<string, string>)[effect.kind] || 'Действие'} · ${effect.resourceType === 'note' ? 'записка' : effect.resourceType}`).join('; ') || 'Пока не подтверждены'));
      detail.append(facts); row.append(detail); list.append(row);
    }
    content.append(list); if (actions.next || actions.previous.length) content.append(pager(actions, loadActions));
  }
  function renderEvents() {
    if (listState(content, events, 'История доступов пуста', 'Выдача ключей, разрешений и их отзыв появятся здесь.', () => loadEvents())) return;
    const list = el('ol', 'sa-event-list');
    for (const item of events.items) {
      const row = el('li', 'sa-event'); const marker = el('span', 'sa-event-marker'); marker.append(icon(item.kind.endsWith('.revoke') ? 'lock' : 'check'));
      const label = el('div'); label.append(el('strong', '', auditNames[item.kind] || 'Доступ изменён'), el('span', 'sa-muted', dateLabel(item.createdAt)));
      const detail = el('details', 'sa-event-detail'); detail.append(el('summary', '', 'Подробнее'));
      const facts = el('dl', 'sa-grant-facts'); facts.append(fact('Объект', knownPrincipals.get(item.objectId)?.label || item.objectId), fact('Изменено с устройства', item.actorId)); detail.append(facts);
      row.append(marker, label, detail); list.append(row);
    }
    content.append(list); if (events.next || events.previous.length) content.append(pager(events, loadEvents));
  }

  async function inspectRevoke(principal: Principal, target?: Grant): Promise<void> {
    const state = openDialog(target ? 'Отозвать разрешение?' : 'Отключить клиента?');
    const body = state.dialog.body; body.append(el('p', 'sa-dialog-intro', principal.label), el('p', 'sa-muted', 'Проверяем выданные разрешения…'));
    try {
      const result = await request<{ grants: Grant[]; cursor: string | null }>('access.grants.list', { principalId: principal.id, limit: 40, ...(target && grants.cursor ? { cursor: grants.cursor } : {}) });
      if (!dialogAlive(state)) return;
      const grant = target ? result.grants.find(value => value.id === target.id) || target : undefined;
      body.replaceChildren(el('p', 'sa-dialog-intro', principal.label));
      const impact = el('div', 'sa-consent-summary');
      impact.append(icon('lock'), el('p', '', target ? `Будет закрыт доступ «${grantName(grant!)}» и разрешения, переданные на его основе.` : 'Будут закрыты все разрешения этого клиента, в том числе переданные другим клиентам.'));
      const details = el('dl', 'sa-grant-facts');
      const visible = target ? [grant!] : result.grants.filter(value => value.revokedAt === null).slice(0, 3);
      for (const item of visible) details.append(fact(grantName(item), `До ${dateLabel(item.expiresAt)}`));
      if (!target && result.cursor) details.append(fact('Также', 'Остальные разрешения этого клиента'));
      body.append(impact, details, el('p', 'sa-muted', 'Уже выполненные изменения сохранятся. Начатые задания могут ещё выполняться.'));
      const message = el('p', 'sa-form-message'); message.setAttribute('role', 'alert');
      const controls = el('div', 'sa-dialog-actions'); const cancel = button('Оставить', undefined, '', () => closeDialog());
      const confirm = button(target ? 'Отозвать доступ' : 'Отключить клиента', 'lock', 'sa-danger', () => {
        mutate(state, async () => {
          confirm.disabled = true; cancel.disabled = true; message.textContent = '';
          try {
            if (target) {
              const response = await request<{ grant: Grant }>('access.grants.revoke', { grantId: target.id });
              if (!response.grant || response.grant.id !== target.id || response.grant.revokedAt === null) throw new Error('unconfirmed');
              if (!dialogAlive(state)) return;
              grants.items = grants.items.map(value => value.id === response.grant.id ? response.grant : value);
              announce('Разрешение отозвано.');
            } else {
              const response = await request<{ principal: Principal }>('access.principals.revoke', { principalId: principal.id });
              if (!response.principal || response.principal.id !== principal.id || response.principal.state !== 'revoked') throw new Error('unconfirmed');
              if (!dialogAlive(state)) return;
              principals.items = principals.items.map(value => value.id === response.principal.id ? response.principal : value); knownPrincipals.set(response.principal.id, response.principal);
              announce('Клиент отключён.');
            }
            markChanged(); state.busy = false; closeDialog(); render();
          } catch (error) {
            if (dialogAlive(state)) { message.textContent = 'Не удалось подтвердить отзыв. Обновите список и проверьте доступ.'; confirm.disabled = false; cancel.disabled = false; }
          }
        });
      });
      controls.append(cancel, confirm); body.append(message, controls); cancel.focus();
    } catch (error) {
      if (dialogAlive(state)) { body.replaceChildren(el('p', 'sa-form-message', errorLabel(error)), button('Закрыть', undefined, '', () => closeDialog())); }
    }
  }

  async function openCreate(): Promise<void> {
    const state = openDialog('Подключить клиента');
    state.dialog.body.append(el('p', 'sa-muted', 'Проверяем доступные действия…'));
    const current = await checkAvailability();
    if (!dialogAlive(state)) return;
    if (!current.notesCreateEnabled || !current.audience) {
      state.dialog.body.replaceChildren(el('p', '', 'Новое подключение пока недоступно.'), button('Закрыть', undefined, '', () => closeDialog())); return;
    }
    const form = el('form', 'sa-create-form');
    const name = textInput('', 'Например, мой помощник', 100); name.required = true; name.autocomplete = 'off';
    const expiry = el('select', 'sw-select');
    for (const [value, label] of [['1', 'На 1 час'], ['24', 'На 1 день'], ['168', 'На 7 дней']]) { const option = el('option', '', label); option.value = value!; option.selected = value === '24'; expiry.append(option); }
    const count = el('input', 'sw-input'); count.type = 'number'; count.min = '1'; count.max = '1000'; count.step = '1'; count.value = '10'; count.required = true; count.inputMode = 'numeric';
    const pair = el('div', 'sa-form-pair'); pair.append(labeledField('Срок', expiry), labeledField('Лимит действий', count));
    const consent = el('div', 'sa-consent-summary'); consent.append(icon('list'), el('div', '', 'Только создание новых личных записок'));
    const facts = el('dl', 'sa-consent-facts');
    const accountValue = options.accountLabel ? `${options.accountLabel} · ${accountId}` : accountId;
    facts.append(fact('Аккаунт', accountValue), fact('Получатель текста', 'Записки в Сотах'), fact('Доступ к прежним запискам', 'Не предоставляется'), fact('Передача доступа', 'Не разрешена'));
    const totals = el('p', 'sa-consent-total');
    const updateTotals = () => { totals.textContent = `До ${Math.max(0, Number(count.value) || 0)} действий · ${expiry.selectedOptions[0]?.textContent || ''} · без публикации`; };
    updateTotals();
    const message = el('p', 'sa-form-message'); message.setAttribute('role', 'alert');
    const submit = button('Создать ключ доступа', 'lock', 'sw-button-primary'); submit.type = 'submit';
    form.append(labeledField('Название клиента', name), pair, consent, facts, totals, message, submit);
    for (const input of [name, expiry, count]) input.addEventListener('input', () => { state.dirty = true; updateTotals(); });
    form.addEventListener('submit', event => {
      event.preventDefault(); if (!form.reportValidity() || state.busy) return;
      const label = name.value.trim(); const limit = Number(count.value); const hours = Number(expiry.value);
      if (!label || !Number.isSafeInteger(limit) || limit < 1 || limit > 1000 || ![1, 24, 168].includes(hours)) { message.textContent = 'Укажите название, срок и целый лимит от 1 до 1000.'; return; }
      mutate(state, async () => {
        submit.disabled = true; name.disabled = true; expiry.disabled = true; count.disabled = true; message.textContent = 'Подключаем…';
        let principalId: string | null = null;
        try {
          const ready = await checkAvailability();
          if (!dialogAlive(state)) return;
          if (!ready.notesCreateEnabled || !ready.audience || ready.audience !== current.audience) throw Object.assign(new Error('capability_disabled'), { code: 'capability_disabled' });
          const principal = await request<{ principal: Principal }>('access.principals.create', { label, clientLabel: label });
          if (!dialogAlive(state)) return;
          if (!principal.principal?.id || principal.principal.accountId !== accountId) throw new Error('unconfirmed');
          principalId = principal.principal.id;
          const expiresAt = Date.now() + hours * 60 * 60 * 1000;
          const grant = await request<{ grant: Grant }>('access.grants.issue', { principalId, capabilities: [notesCapability], resources: ['notes:new'], effects: ['create'], recipients: ['soty:notes'],
            expiresAt, allowDelegation: false, maxDepth: 0, budget: { unit: 'invocations', limit } });
          if (!dialogAlive(state)) return;
          if (!grant.grant?.id || grant.grant.principalId !== principalId) throw new Error('unconfirmed');
          const stillReady = await checkAvailability();
          if (!dialogAlive(state)) return;
          if (!stillReady.notesCreateEnabled || stillReady.audience !== ready.audience) throw Object.assign(new Error('capability_disabled'), { code: 'capability_disabled' });
          const issued = await request<{ token: string; credential: { id: string; grantId: string; audience: string; expiresAt: number } }>('access.credentials.issue', { grantId: grant.grant.id, audience: ready.audience, expiresAt });
          if (!dialogAlive(state)) return;
          if (typeof issued.token !== 'string' || !/^soty_cap_[A-Za-z0-9_-]{43}$/u.test(issued.token) || issued.credential?.audience !== ready.audience || issued.credential.grantId !== grant.grant.id || issued.credential.expiresAt !== expiresAt) throw new Error('unconfirmed');
          state.dirty = false; markChanged(); knownPrincipals.set(principalId, principal.principal);
          showSecret(state, issued.token, label, expiresAt, limit); announce('Ограниченный ключ создан. Скопируйте его до закрытия.');
          void loadPrincipals();
        } catch (error) {
          if (!dialogAlive(state)) return;
          message.textContent = principalId ? 'Настройка не завершена. Перед повтором закройте уже созданный доступ.' : codeOf(error) === 'capability_disabled' ? errorLabel(error) : 'Создание не подтверждено. Проверьте список клиентов перед новой попыткой.';
          submit.hidden = true;
          if (principalId) {
            const createdId = principalId;
            const cleanup = button('Закрыть созданный доступ', 'lock', 'sa-danger', () => {
              mutate(state, async () => {
                cleanup.disabled = true;
                try {
                  const result = await request<{ principal: Principal }>('access.principals.revoke', { principalId: createdId });
                  if (!dialogAlive(state)) return;
                  if (result.principal?.state !== 'revoked') throw new Error('unconfirmed');
                  announce('Незавершённое подключение закрыто.'); state.busy = false; state.dirty = false; closeDialog(); markChanged(); void loadPrincipals();
                } catch { if (dialogAlive(state)) { message.textContent = 'Отзыв не подтверждён. Обновите список клиентов и проверьте доступ.'; cleanup.disabled = false; } }
              });
            });
            form.append(cleanup);
          } else form.append(button('Проверить список', 'refresh', '', () => { state.dirty = false; closeDialog(); selectView('clients'); void loadPrincipals(); }));
        }
      });
    });
    state.dialog.body.replaceChildren(form); name.focus();
  }

  function showSecret(state: DialogState, value: string, label: string, expiresAt: number, limit: number) {
    let secret: string | null = value;
    state.dirty = true;
    const body = state.dialog.body;
    const title = state.dialog.element.querySelector('h2'); if (title) title.textContent = 'Ключ готов';
    const input = el('input', 'sa-secret'); input.type = 'password'; input.readOnly = true; input.autocomplete = 'off'; input.spellcheck = false; input.value = value; input.setAttribute('aria-label', 'Ключ доступа');
    state.clearSecret = () => { secret = null; input.value = ''; input.removeAttribute('value'); state.dirty = false; };
    const info = el('p', 'sa-muted', 'Скопируйте ключ в ваш клиент. После закрытия он исчезнет с этого экрана.');
    const token = el('div', 'sa-secret-row');
    const reveal = button('Показать', 'eye', '', () => { input.type = input.type === 'password' ? 'text' : 'password'; reveal.querySelector('span')!.textContent = input.type === 'password' ? 'Показать' : 'Скрыть'; reveal.setAttribute('aria-pressed', String(input.type === 'text')); });
    reveal.setAttribute('aria-pressed', 'false'); token.append(input, reveal);
    const status = el('p', 'sa-form-message'); status.setAttribute('role', 'status'); status.setAttribute('aria-live', 'polite');
    const copy = button('Скопировать ключ', undefined, 'sw-button-primary', () => {
      if (!secret || !dialogAlive(state)) return;
      copy.disabled = true;
      void Promise.resolve().then(() => {
        if (!secret || !dialogAlive(state)) return;
        if (!navigator.clipboard?.writeText) throw new Error('clipboard_unavailable');
        return navigator.clipboard.writeText(secret);
      }).then(() => { if (dialogAlive(state)) { status.classList.add('is-success'); status.textContent = 'Ключ скопирован.'; } }).catch(() => {
        if (dialogAlive(state)) { status.classList.remove('is-success'); input.type = 'text'; input.focus(); input.select(); reveal.setAttribute('aria-pressed', 'true'); reveal.querySelector('span')!.textContent = 'Скрыть'; status.textContent = 'Скопируйте выделенный ключ вручную.'; }
      }).finally(() => { if (dialogAlive(state)) copy.disabled = false; });
    });
    const summary = el('dl', 'sa-consent-facts'); summary.append(fact('Клиент', label), fact('Срок', `До ${dateLabel(expiresAt)}`), fact('Лимит', `${limit} действий`));
    body.replaceChildren(el('div', 'sa-key-mark'), info, summary, token, status, copy, button('Готово, закрыть', undefined, '', () => { state.dirty = false; closeDialog(); }));
    body.querySelector('.sa-key-mark')?.append(icon('check')); copy.focus();
  }

  const pageHidden = () => { closeDialog(true); };
  window.addEventListener('pagehide', pageHidden);
  selectView('clients'); void checkAvailability();
  return {
    dispose() { if (disposed) return; disposed = true; window.removeEventListener('pagehide', pageHidden); erase(); },
    async flush() { await Promise.allSettled([...mutations]); },
    hasUnsavedChanges() { return alive() && (mutations.size > 0 || Boolean(dialogState?.dirty)); }
  };
}
