import { button, el } from './dom';
import type { WorldApi } from './types';
import type { HumanSigninCapabilities, HumanSigninEntry, HumanSigninList, HumanSigninRevoke, HumanSigninScope, HumanSigninStatus } from '../../modules/human-identity/types.mjs';
import './human-signins.css';

type Intent = { expectedAccountId: string; scope: HumanSigninScope; entryId: string; expectedRevision: string; requestId: string };
type Pending = { schema: 'soty.human-signin-pending.v1'; accountId: string; deviceId: string; intent: Intent;
  clientLabel: string; targetDevice: string; kind: HumanSigninEntry['kind']; revokeScope: HumanSigninEntry['revokeScope'] };
type LocalStorage = Pick<Storage, 'getItem' | 'setItem' | 'removeItem'>;
export interface HumanSigninsOptions {
  host: HTMLElement; api: WorldApi; accountId: string; deviceId: string | null; accountLabel: string;
  isCurrent(): boolean;
  readCapability?: () => Promise<unknown>;
  storage?: LocalStorage | null;
}

const text = (value: unknown, max = 256): value is string => typeof value === 'string' && value.length > 0 && value.length <= max && !/[\uD800-\uDFFF\u0000]/u.test(value);
const opaque = (value: unknown): value is string => typeof value === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(value);
const digest = (value: unknown): value is string => typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value);
const timestamp = (value: unknown): value is number => Number.isSafeInteger(value) && (value as number) >= 0 && (value as number) <= 8640000000000000;
const object = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === 'object' && !Array.isArray(value);
function invalid(): never { throw Object.assign(new Error('human_signins_response_invalid'), { code: 'human_signins_response_invalid' }); }
function capability(value: unknown): HumanSigninCapabilities {
  if (!object(value) || value.schema !== 'soty.human-signins-capability.v1' || !['available', 'sessions', 'rememberedConsents', 'checkRevoke'].every(key => typeof value[key] === 'boolean')
    || value.sourceRootSessions !== false || !Array.isArray(value.scopes) || value.scopes.length > 2 || new Set(value.scopes).size !== value.scopes.length
    || value.scopes.some(scope => !['current-device', 'account'].includes(scope)) || !Number.isSafeInteger(value.maximumPageSize)
    || (value.available ? !((value.maximumPageSize as number) >= 1 && (value.maximumPageSize as number) <= 20 && value.scopes.includes('current-device'))
      : value.maximumPageSize !== 0 || value.scopes.length !== 0 || value.sessions || value.rememberedConsents || value.checkRevoke)) invalid();
  return { ...value, scopes: [...value.scopes] } as unknown as HumanSigninCapabilities;
}
function entry(value: unknown, scope: HumanSigninScope, deviceId: string | null, caps: HumanSigninCapabilities): HumanSigninEntry {
  if (!object(value) || !opaque(value.entryId) || !opaque(value.revokeRequestId) || !digest(value.revision)
    || !text(value.clientId) || !text(value.clientLabel, 512) || !text(value.deviceId) || typeof value.currentDevice !== 'boolean'
    || !timestamp(value.createdAt) || value.expiresAt !== null && !timestamp(value.expiresAt)
    || !Number.isSafeInteger(value.clientGeneration) || (value.clientGeneration as number) < 1
    || !['active', 'expired', 'device-revoked', 'client-changed'].includes(String(value.status))
    || typeof value.renewable !== 'boolean' || typeof value.remembered !== 'boolean'
    || (value.kind === 'oidc-session' ? !caps.sessions || value.revokeScope !== 'exact-session' || value.remembered
      : value.kind !== 'remembered-consent' || !caps.rememberedConsents || value.revokeScope !== 'app-device-version' || !value.remembered)
    || scope === 'current-device' && value.currentDevice !== true
    || deviceId !== null && value.currentDevice !== (value.deviceId === deviceId)) invalid();
  return { ...value } as unknown as HumanSigninEntry;
}
const code = (error: unknown): string => object(error) && typeof error.code === 'string' ? error.code : '';
function failure(error: unknown): string {
  if (['ACTIVE_PROFILE_CHANGED', 'DEVICE_REVOKED', 'human_identity_account_mismatch', 'identity_required', 'actor_revoked'].includes(code(error))) return 'Профиль или устройство изменились. Откройте настройки нужного профиля заново.';
  if (code(error) === 'human_identity_signins_changed') return 'Список изменился в другом окне. Обновите его перед новым выбором.';
  if (['human_identity_signins_unavailable', 'unsupported_operation'].includes(code(error))) return 'Управление этими входами сейчас недоступно на сервере.';
  if (code(error) === 'human_identity_decision_conflict') return 'Этот запрос не подтверждён для текущего входа. Его нельзя заменить новым запросом.';
  if (code(error) === 'human_signins_response_invalid') return 'Сервер не подтвердил поддерживаемый формат входов.';
  return 'Не удалось получить подтверждение сервера. Проверьте соединение и обновите состояние.';
}
const date = (value: number): string => new Date(value).toLocaleString('ru-RU', { day: 'numeric', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit' });
const deviceLabel = (value: string, current: string | null): string => value === current ? 'Это устройство' : `Другое моё устройство · ${value.slice(-8)}`;

/** One Human issuer editor. Public capability is discovery, never authority;
 * every list/check/revoke uses the current signed WorldApi and expected account. */
export function mountHumanSignins(options: HumanSigninsOptions): { dispose(): void; refresh(): Promise<void> } {
  const root = el('section', 'sw-human-signins'); root.setAttribute('aria-label', 'Входы в приложения'); options.host.append(root);
  let storage: LocalStorage | null = null;
  try { storage = options.storage === undefined ? globalThis.sessionStorage : options.storage; } catch { /* Read-only UI remains usable. */ }
  const storageKey = options.deviceId ? `soty:human-signin-pending:v1:${encodeURIComponent(options.accountId)}:${encodeURIComponent(options.deviceId)}` : null;
  let disposed = false, sequence = 0, scope: HumanSigninScope = 'current-device', caps: HumanSigninCapabilities | null = null;
  let entries: HumanSigninEntry[] = [], nextCursor: string | null = null, ready = false, busy: 'load' | 'revoke' | 'check' | null = null;
  let message = '', error = '', confirmation: HumanSigninEntry | null = null, pending: Pending | null = null, localUnreadable = false;
  let pendingStatus: 'unknown' | 'active' | 'unavailable' = 'unknown';
  const current = (): boolean => {
    const valid = !disposed && options.host.isConnected && options.isCurrent();
    if (!valid && !disposed) { disposed = true; sequence++; root.remove(); }
    return valid;
  };
  const live = (captured: number): boolean => current() && sequence === captured;
  const allowed = (row: HumanSigninEntry): boolean => Boolean(caps?.available && (row.kind === 'oidc-session' ? caps.sessions : caps.rememberedConsents));
  const clearPending = (): void => {
    pending = null; localUnreadable = false;
    try { if (storageKey) storage?.removeItem(storageKey); } catch { message += ' Локальная запись проверки не удалена; после обновления её можно проверить ещё раз.'; }
  };
  if (storageKey && storage) {
    try {
      const raw = storage.getItem(storageKey);
      if (raw !== null) {
        if (raw.length > 4096) invalid();
        const value = JSON.parse(raw) as unknown;
        if (!object(value) || value.schema !== 'soty.human-signin-pending.v1' || value.accountId !== options.accountId || value.deviceId !== options.deviceId
          || !object(value.intent) || Object.keys(value.intent).sort().join(',') !== 'entryId,expectedAccountId,expectedRevision,requestId,scope'
          || value.intent.expectedAccountId !== options.accountId || !['current-device', 'account'].includes(String(value.intent.scope))
          || !opaque(value.intent.entryId) || !opaque(value.intent.requestId) || !digest(value.intent.expectedRevision)
          || !text(value.clientLabel, 512) || !text(value.targetDevice)
          || (value.kind === 'oidc-session' ? value.revokeScope !== 'exact-session' : value.kind !== 'remembered-consent' || value.revokeScope !== 'app-device-version')) invalid();
        pending = value as unknown as Pending;
      }
    } catch { localUnreadable = true; }
  }
  const control = (key: string, label: string, run: () => void, disabled = false, danger = false): HTMLButtonElement => {
    const node = button(label, undefined, danger ? 'sw-button-danger' : 'sw-button-quiet', run);
    node.dataset.signinControl = key; node.disabled = disabled; return node;
  };
  const focus = (key: string): void => {
    const target = root.querySelector<HTMLElement>(`[data-signin-control="${key}"]`);
    (target && !(target as HTMLButtonElement).disabled ? target : root.querySelector<HTMLElement>('[data-signin-control="refresh"]'))?.focus();
  };
  function render(focusKey?: string): void {
    if (!current()) return;
    root.setAttribute('aria-busy', String(Boolean(busy)));
    const profile = el('p', 'sw-human-signins-profile', `Профиль · ${options.accountLabel}`);
    const filters = el('div', 'sw-human-signins-scopes'); filters.setAttribute('role', 'group'); filters.setAttribute('aria-label', 'Устройства входа');
    for (const [value, label] of [['current-device', 'Это устройство'], ['account', 'Все мои устройства']] as const) {
      if (value === 'account' && !caps?.scopes.includes('account')) continue;
      const selected = control('scope-' + value, label, () => {
        if (!current() || busy || !caps?.scopes.includes(value)) return;
        scope = value; confirmation = null; void load(false);
      }, Boolean(busy)); selected.setAttribute('aria-pressed', String(scope === value)); filters.append(selected);
    }
    const status = el('p', error ? 'sw-error' : 'sw-human-signins-status', error || message || (busy === 'load' ? 'Загружаем входы…' : ''));
    status.setAttribute('role', error ? 'alert' : 'status'); status.setAttribute('aria-live', 'polite'); status.tabIndex = -1; status.dataset.signinControl = 'status';
    root.replaceChildren(profile, filters, el('p', 'sw-human-signins-profile', 'Приложения с доступом по текущему профилю Сот здесь не перечисляются.'), status);
    if (!options.deviceId) root.append(el('p', 'sw-human-signins-note', 'Текущее устройство не подтверждено. Список можно читать; для отзыва заново откройте настройки профиля.'));
    if (!storage) root.append(el('p', 'sw-human-signins-note', 'На этом устройстве недоступно сохранение проверки. Список можно читать; отзыв не отправляется.'));
    if (localUnreadable || pending) {
      const box = el('section', 'sw-human-signins-pending'); box.setAttribute('aria-label', 'Проверка прежнего отзыва');
      box.append(el('h3', '', localUnreadable ? 'Не удалось прочитать локальную проверку' : 'Прежний отзыв ещё проверяется'));
      if (pending) {
        box.append(el('strong', '', pending.clientLabel), el('p', 'sw-muted', deviceLabel(pending.targetDevice, options.deviceId)),
          el('p', '', pendingStatus === 'active' ? 'Сервер пока не подтверждает отзыв этой записи. Её срок и текущий доступ приложения проверяются отдельно.'
            : pendingStatus === 'unavailable' ? 'Точную запись больше нельзя проверить: она могла измениться или быть удалена. Результат отзыва неизвестен.'
              : 'Ответ на отзыв не получен. Проверка читает точное состояние на сервере и не повторяет отзыв.'),
          control('check', busy === 'check' ? 'Проверяем…' : 'Проверить результат', () => { void checkPending(); }, Boolean(busy) || !caps?.available || !caps.checkRevoke));
      }
      box.append(el('small', 'sw-muted', 'Можно скрыть только эту локальную проверку. Входы и согласия на сервере от этого не изменятся.'),
        control('dismiss-pending', 'Скрыть локальную проверку', () => {
          if (!current() || busy) return; message = 'Локальная проверка скрыта. Входы на сервере не изменялись.'; clearPending(); render('status');
        }, Boolean(busy)));
      root.append(box);
    }
    if (caps && !caps.available) root.append(el('p', 'sw-human-signins-note', 'На этом сервере управление сохранёнными входами не включено. Это не означает, что у приложений нет своего доступа.'));
    if (ready) {
      for (const [kind, title] of [['oidc-session', 'Входы через Соты'], ['remembered-consent', 'Запомненные согласия']] as const) {
        if (kind === 'remembered-consent' && !caps?.rememberedConsents) { root.append(el('p', 'sw-muted', 'Управление запомненными согласиями здесь недоступно.')); continue; }
        const rows = entries.filter(value => value.kind === kind), section = el('section', 'sw-human-signins-group');
        section.setAttribute('aria-label', title); section.append(el('h3', '', title));
        if (!rows.length) section.append(el('p', 'sw-muted', nextCursor ? 'В загруженной части списка нет записей.' : 'Таких записей на сервере нет.'));
        for (const row of rows) {
          const card = el('article', 'sw-human-signin'), index = entries.indexOf(row);
          const names: Record<HumanSigninEntry['status'], string> = { active: 'Действует', expired: 'Срок истёк', 'device-revoked': 'Устройство отключено', 'client-changed': 'Версия приложения изменилась' };
          const head = el('div', 'sw-human-signin-head'); head.append(el('strong', '', row.clientLabel), el('span', 'sw-human-signin-status', names[row.status]));
          card.append(head, el('p', 'sw-muted', `${deviceLabel(row.deviceId, options.deviceId)} · версия ${row.clientGeneration}`),
            el('p', 'sw-muted', `Выдано ${date(row.createdAt)}${row.expiresAt === null ? '' : ` · подтверждение через Соты до ${date(row.expiresAt)}`}`));
          if (kind === 'oidc-session' && row.expiresAt === null) card.append(el('p', 'sw-muted', 'Срок сервером не указан; это не бессрочный вход.'));
          if (kind === 'oidc-session') card.append(el('p', '', row.renewable ? row.expiresAt === null ? 'Продление разрешено, но его срок здесь не подтверждён.' : 'Продление этого входа разрешено в пределах указанного срока.' : 'Автопродление этого входа не разрешено.'));
          else card.append(el('p', '', row.status === 'active' ? 'Позволяет не подтверждать заново вход этой версии приложения на этом устройстве.' : 'Согласие прежнего входа. Его текущий статус указан выше.'));
          if (confirmation === row) {
            const review = el('div', 'sw-human-signin-review'); review.setAttribute('role', 'group'); review.setAttribute('aria-label', 'Подтвердите точный отзыв');
            review.append(el('p', '', kind === 'oidc-session'
              ? 'Отозвать только этот вход? Его дальнейшее подтверждение и продление через Соты станут недоступны. Запомненное согласие и другие входы сохранятся.'
              : 'Забыть согласие на выбранном устройстве? Также будут отозваны входы этой версии приложения на этом устройстве. Следующий вход потребует нового подтверждения.'),
              control('confirm', kind === 'oidc-session' ? 'Отозвать этот вход' : 'Забыть согласие и отозвать входы', () => { void revoke(row); }, Boolean(busy), true),
              control('cancel-confirm', 'Отмена', () => { if (current() && !busy) { confirmation = null; render(`revoke-${index}`); } }, Boolean(busy)));
            card.append(review);
          } else card.append(control(`revoke-${index}`, kind === 'oidc-session' ? 'Отозвать вход' : 'Забыть согласие', () => {
            if (!current() || busy || pending || localUnreadable || !allowed(row) || !options.deviceId || !storage) return;
            confirmation = row; error = ''; render('confirm');
          }, Boolean(busy || pending || localUnreadable || !allowed(row) || !options.deviceId || !storage)));
          section.append(card);
        }
        root.append(section);
      }
      if (nextCursor) root.append(control('more', 'Показать ещё', () => { void load(true); }, Boolean(busy)));
    }
    root.append(control('refresh', 'Обновить список', () => { void load(false); }, Boolean(busy)));
    const explanation = el('details', 'sw-human-signins-explanation');
    explanation.append(el('summary', '', 'Какой доступ здесь показан'), el('p', '', 'Здесь показаны входы во внешние приложения через Соты и запомненные согласия. Доступ приложений внутри Сот определяется текущим профилем и правами в самих приложениях. Пустой список не означает отсутствие доступа к ним.'),
      el('p', '', 'Отзыв прекращает дальнейшее подтверждение и продление входа через Соты. Собственная сессия приложения может завершиться при следующей проверке.'),
      el('p', '', 'Разрешённые действия ИИ и приложений, доступ к данным и устройства размещения управляются отдельно.'));
    root.append(explanation); if (focusKey) focus(focusKey);
  }
  async function load(more: boolean, notice = ''): Promise<void> {
    if (!current() || busy || more && !nextCursor) return;
    const active = globalThis.document?.activeElement, returnControl = active && root.contains(active) ? (active as HTMLElement).dataset?.signinControl : undefined;
    const captured = ++sequence, selectedScope = scope, cursor = more ? nextCursor : null;
    busy = 'load'; error = ''; message = notice; confirmation = null;
    if (!more) { entries = []; ready = false; nextCursor = null; } render();
    try {
      if (!more) {
        const value = options.readCapability ? await options.readCapability() : await (async () => {
          const response = await fetch('/api/apps/capabilities', { cache: 'no-store', signal: AbortSignal.timeout(8000) });
          if (!response.ok) throw Error(); return (await response.json() as { humanSignins?: unknown }).humanSignins;
        })();
        if (!live(captured)) return; caps = capability(value);
      }
      if (!caps?.available) return;
      if (!caps.scopes.includes(selectedScope)) invalid();
      const value = await options.api.request<HumanSigninList>('identity.human.signins.list', { expectedAccountId: options.accountId,
        scope: selectedScope, limit: Math.min(20, caps.maximumPageSize), ...(cursor ? { cursor } : {}) });
      if (!live(captured)) return;
      if (!object(value) || value.schema !== 'soty.human-signins.v1' || value.scope !== selectedScope || !Array.isArray(value.entries) || value.entries.length > 20
        || value.nextCursor !== null && (!text(value.nextCursor, 1024) || !value.entries.length)) invalid();
      const actual = capability(value.capabilities); if (!actual.available || !actual.scopes.includes(selectedScope)) invalid();
      const rows = value.entries.map(row => entry(row, selectedScope, options.deviceId, actual)), ids = new Set(more ? entries.map(row => row.entryId) : []);
      for (const row of rows) { if (ids.has(row.entryId)) invalid(); ids.add(row.entryId); }
      caps = actual; entries = more ? [...entries, ...rows] : rows; nextCursor = value.nextCursor; ready = true;
    } catch (caught) {
      if (live(captured)) { error = failure(caught); entries = []; nextCursor = null; ready = false; }
    } finally {
      if (live(captured)) { busy = null; render(returnControl); if (!more && pending && caps?.available && caps.checkRevoke) void checkPending(); }
    }
  }
  async function checkPending(): Promise<void> {
    if (!current() || busy || !pending || !caps?.available || !caps.checkRevoke) return;
    const original = pending, captured = ++sequence; let refreshAfter = false; busy = 'check'; error = ''; render('status');
    try {
      const value = await options.api.request<HumanSigninStatus>('identity.human.signins.checkRevoke', { ...original.intent });
      if (!live(captured) || pending !== original) return;
      if (value?.schema !== 'soty.human-signin-status.v1' || value.entryId !== original.intent.entryId || value.requestId !== original.intent.requestId
        || value.scope !== original.intent.scope || !['active', 'revoked', 'unavailable'].includes(value.status)
        || (value.status === 'revoked' ? !timestamp(value.revokedAt) : value.revokedAt !== null)) invalid();
      if (value.status === 'revoked') { message = 'Сервер подтвердил отзыв этого разрешения.'; clearPending(); refreshAfter = true; }
      else pendingStatus = value.status;
    } catch (caught) { if (live(captured)) { error = failure(caught); pendingStatus = 'unknown'; } }
    finally { if (live(captured)) { busy = null; render('status'); if (refreshAfter) void load(false, message); } }
  }
  async function revoke(row: HumanSigninEntry): Promise<void> {
    if (!current() || busy || confirmation !== row || !entries.includes(row) || pending || localUnreadable || !options.deviceId || !allowed(row)) return;
    const intent: Intent = { expectedAccountId: options.accountId, scope, entryId: row.entryId, expectedRevision: row.revision, requestId: row.revokeRequestId };
    const original: Pending = { schema: 'soty.human-signin-pending.v1', accountId: options.accountId, deviceId: options.deviceId,
      intent, clientLabel: row.clientLabel, targetDevice: row.deviceId, kind: row.kind, revokeScope: row.revokeScope };
    try { if (!storage || !storageKey) throw Error(); const raw = JSON.stringify(original); if (raw.length > 4096) throw Error(); storage.setItem(storageKey, raw); if (storage.getItem(storageKey) !== raw) throw Error(); }
    catch { error = 'Не удалось сохранить запрос на этом устройстве. Отзыв не отправлен.'; render('status'); return; }
    if (!current()) return;
    pending = original; pendingStatus = 'unknown'; confirmation = null; busy = 'revoke'; error = ''; message = 'Отзываем выбранное разрешение…';
    const captured = ++sequence; let refreshAfter = false; render('status');
    try {
      const value = await options.api.request<HumanSigninRevoke>('identity.human.signins.revoke', { ...intent });
      if (!live(captured)) return;
      if (value?.schema !== 'soty.human-signin-revocation.v1' || value.entryId !== intent.entryId || value.requestId !== intent.requestId || value.scope !== intent.scope
        || value.kind !== row.kind || value.revokeScope !== row.revokeScope || !['revoked', 'already_revoked'].includes(value.outcome) || !timestamp(value.revokedAt)) invalid();
      message = 'Сервер подтвердил отзыв этого разрешения.'; clearPending(); entries = []; ready = false; nextCursor = null; refreshAfter = true;
    } catch (caught) { if (live(captured)) { message = ''; error = 'Отзыв пока не подтверждён. Проверьте результат прежнего запроса; новый отзыв не отправляется.'; } }
    finally { if (live(captured)) { busy = null; render('status'); if (refreshAfter) void load(false, message); } }
  }
  render(); void load(false);
  return { refresh: () => load(false), dispose() { if (disposed) return; disposed = true; sequence++; entries = []; confirmation = null; root.remove(); } };
}
