import { button, el } from './dom';
import type { HumanLoginContext } from '../platform/human-context.mjs';
import './world.css';
import './oauth-consent.css';

interface Account { accountId: string | null; label: string; revoked: boolean; }
interface Decision { expectedAccountId: string; interactionId: string; browserNonce: string; csrf: string;
  requestId: string; decision: 'approve' | 'deny'; stayInAppSeconds?: 86400; rememberForDevice?: true; forgetForDevice?: true; }
interface Continuation { expectedAccountId: string; interactionId: string; browserNonce: string; csrf: string; requestId: string; }
export interface HumanLoginPorts {
  presentation?: 'login' | 'forget';
  account(): Promise<Account>; context(): Promise<Readonly<HumanLoginContext>>;
  decide(args: Readonly<Decision>): Promise<void>; complete(csrf: string): Promise<void>;
  continue?(args: Readonly<Continuation>): Promise<boolean>;
  createProfile(): Promise<{ accountId: string }>; openAccount(): Promise<void>; observeAccount(listener: () => void): () => void;
}

/** Initial approval is a deliberate action on a captured Connect profile.
 * Continuation is a new signed proof gated by an exact server-held preference.
 * Unknown acknowledgements retain the identical decision for explicit retry. */
export function mountHumanLogin(host: HTMLElement, ports: HumanLoginPorts): { canReload(): boolean; dispose(): void } {
  let disposed = false, sequence = 0, busy = false, invalidated = false, creatingProfile = false, error = '', deadline = 0;
  const forgetting = ports.presentation === 'forget'; // Presentation only; the signed server decision supplies authority.
  let account: Account = { accountId: null, label: '', revoked: false };
  let context: Readonly<HumanLoginContext> | null = null, pending: Readonly<Decision> | null = null;
  let stayInApp = false, rememberForDevice = false, continuationAttempted = false;
  let pendingContinuation: Readonly<Continuation> | null = null;
  let expiryTimer: ReturnType<typeof setTimeout> | undefined;
  const root = el('section', 'so-consent'), card = el('div', 'so-card'); root.append(card); host.replaceChildren(root);
  const alive = (ticket: number) => !disposed && sequence === ticket;
  const unavailable = (failure: unknown) => {
    const code = failure && typeof failure === 'object' && 'code' in failure ? String(failure.code) : '';
    return failure instanceof TypeError && failure.message === 'profile_changed' || [
      'human_identity_intent_conflict', 'human_identity_context_mismatch', 'human_identity_interaction_expired',
      'human_identity_browser_mismatch', 'human_identity_profile_changed', 'human_identity_actor_revoked',
      'human_identity_decision_conflict', 'human_identity_account_mismatch', 'human_identity_renewal_disabled', 'human_identity_renewal_invalid',
      'human_identity_device_consent_revoked', 'human_identity_device_consent_disabled',
      'human_identity_approval_required',
      'ACTIVE_PROFILE_CHANGED', 'DEVICE_REVOKED', 'device_revoked', 'authentication_required'].includes(code);
  };
  const expire = () => { sequence++; busy = false; invalidated = true; error = 'Запрос истёк. Начните вход в приложении ещё раз.'; render(); };
  const valid = () => { if (performance.now() >= deadline) { expire(); return false; } return !invalidated; };

  async function refresh(): Promise<void> {
    if (disposed || busy || pending || pendingContinuation) return;
    const ticket = ++sequence; busy = true; error = ''; render();
    try {
      const [nextAccount, nextContext] = await Promise.all([ports.account(), ports.context()]);
      if (!alive(ticket)) return;
      account = nextAccount; context = nextContext; invalidated = false;
      stayInApp = context.renewal?.approvedSessionSeconds === 86400;
      rememberForDevice = context.deviceConsent?.approvedRememberForDevice === true;
      deadline = performance.now() + context.remainingMs;
      clearTimeout(expiryTimer); expiryTimer = setTimeout(expire, context.remainingMs);
    } catch { if (alive(ticket)) { context = null; error = 'Не удалось проверить запрос входа. Проверьте соединение и повторите.'; } }
    finally { if (alive(ticket)) { busy = false; render();
      if (!forgetting && !continuationAttempted && context?.decision === 'pending' && context.deviceConsent?.available && account.accountId && !account.revoked && ports.continue) {
        continuationAttempted = true; void continueLogin();
      }
    } }
  }
  async function complete(): Promise<void> {
    if (!context || !valid()) return;
    await ports.complete(context.csrf);
  }
  async function continueLogin(): Promise<void> {
    if (busy || !context || !valid() || !ports.continue) return;
    const ticket = sequence; busy = true; error = ''; render();
    try {
      const current = await ports.account(); if (!alive(ticket)) return;
      if (!current.accountId || current.revoked || current.accountId !== account.accountId
        || pendingContinuation && pendingContinuation.expectedAccountId !== current.accountId) throw new TypeError('profile_changed');
      pendingContinuation ??= Object.freeze({ expectedAccountId: current.accountId, interactionId: context.interactionId,
        browserNonce: context.browserNonce, csrf: context.csrf, requestId: crypto.randomUUID() });
      if (!valid()) return;
      const continued = await ports.continue(pendingContinuation); if (!alive(ticket)) return;
      const confirmed = await ports.account(); if (!alive(ticket)) return;
      if (confirmed.accountId !== current.accountId || confirmed.revoked || !valid()) throw new TypeError('profile_changed');
      if (!continued) { pendingContinuation = null; return; }
      stayInApp = true; rememberForDevice = true;
      context = Object.freeze({ ...context, decision: 'approved',
        renewal: { maximumSessionSeconds: 86400 as const, approvedSessionSeconds: 86400 as const },
        deviceConsent: { available: context.deviceConsent?.available === true, approvedRememberForDevice: true } }); await complete();
    } catch (failure) {
      if (alive(ticket)) {
        if (unavailable(failure)) { invalidated = true; error = 'Этот вход недоступен с выбранным профилем. Начните вход в приложении ещё раз.'; }
        else error = 'Продолжение входа пока не подтверждено. Нажмите «Проверить вход».';
      }
    } finally { if (alive(ticket)) { busy = false; render(); } }
  }
  async function decide(kind: 'approve' | 'deny', create = false): Promise<void> {
    if (busy || pendingContinuation || !context || !valid() || context.decision === 'denied') return;
    const ticket = sequence; busy = true; error = ''; render();
    try {
      let created: { accountId: string } | null = null;
      if (create) {
        creatingProfile = true;
        try { created = await ports.createProfile(); } finally { creatingProfile = false; }
      }
      if (!alive(ticket)) return;
      const selected = await ports.account();
      if (!alive(ticket)) return;
      if (!selected.accountId || selected.revoked || created && created.accountId !== selected.accountId || pending && pending.expectedAccountId !== selected.accountId
        || !create && account.accountId !== selected.accountId) throw new TypeError('profile_changed');
      account = selected;
      pending ??= Object.freeze({ expectedAccountId: selected.accountId, interactionId: context.interactionId,
        browserNonce: context.browserNonce, csrf: context.csrf, requestId: crypto.randomUUID(), decision: kind,
        ...(kind === 'approve' && context.renewal && stayInApp ? { stayInAppSeconds: 86400 as const } : {}),
        ...(kind === 'approve' && rememberForDevice ? { rememberForDevice: true as const } : {}),
        ...(kind === 'deny' && forgetting ? { forgetForDevice: true as const } : {}) });
      if (pending.decision !== kind || !valid()) return;
      await ports.decide(pending);
      if (!alive(ticket)) return;
      const confirmed = await ports.account();
      if (!alive(ticket)) return;
      if (confirmed.accountId !== pending.expectedAccountId || confirmed.revoked || !valid()) throw new TypeError('profile_changed');
      context = Object.freeze({ ...context, decision: pending.decision === 'approve' ? 'approved' : 'denied' });
      await complete();
    } catch (failure) {
      if (alive(ticket)) {
        if (unavailable(failure)) {
          invalidated = true; error = 'Этот вход недоступен с выбранным профилем. Начните вход в приложении ещё раз.';
        } else error = pending
          ? 'Ответ пока не подтверждён. Нажмите «Проверить вход»: будет проверено то же решение.'
          : 'Профиль не удалось открыть. Проверьте выбранный профиль и попробуйте снова.';
      }
    } finally { if (alive(ticket)) { busy = false; render(); } }
  }
  async function openAccount(): Promise<void> {
    if (busy || pending || pendingContinuation || invalidated) return;
    busy = true; render();
    try { await ports.openAccount(); } catch { error = 'Не удалось открыть профиль. Попробуйте снова.'; }
    finally { if (!disposed) { busy = false; invalidated = false; await refresh(); } }
  }
  function render(): void {
    if (disposed) return;
    const previous = root.ownerDocument.activeElement as HTMLElement | null, focus = previous?.dataset.loginControl;
    const brand = el('div', 'so-brand', 'Соты'), title = el('h1', 'so-title', context ? `Войти в ${context.client.label}` : 'Вход через Соты');
    title.tabIndex = -1; const contents = el('div', 'so-content'), actions = el('div', 'so-actions'), notice = el('p', 'so-error', error);
    notice.setAttribute('role', 'status'); notice.setAttribute('aria-live', 'polite');
    if (context) {
      contents.append(el('p', 'so-summary', context.decision === 'denied' ? 'Вход отменён.' : forgetting
        ? 'Вход в этом приложении на выбранном устройстве будет отключён.' : 'Приложение получит ваш идентификатор для входа.'));
      if (context.scopes.includes('profile')) contents.append(el('p', 'so-details', 'Также запрошены данные профиля, разрешённые для этого входа.'));
      const profile = button(account.accountId ? `${account.label || 'Ваш профиль'} · ${account.accountId.slice(-8)}` : 'У меня уже есть профиль', 'person', 'so-account', () => void openAccount());
      profile.disabled = busy || !!pending || !!pendingContinuation || invalidated; profile.dataset.loginControl = 'account'; contents.append(profile);
      if (!forgetting && context.renewal && context.decision !== 'denied' && !rememberForDevice) {
        const choice = el('label', 'so-stay'), checkbox = el('input', 'so-stay-checkbox');
        checkbox.type = 'checkbox'; checkbox.checked = pending ? pending.stayInAppSeconds === 86400 : stayInApp;
        checkbox.disabled = busy || !!pending || !!pendingContinuation || rememberForDevice || invalidated || context.decision !== 'pending'; checkbox.dataset.loginControl = 'stay';
        checkbox.addEventListener('change', () => { if (!busy && !pending && !invalidated) stayInApp = checkbox.checked; });
        choice.append(checkbox, el('span', '', 'Оставаться в этом приложении до 24 часов')); contents.append(choice);
      }
      if (!forgetting && context.deviceConsent && context.renewal && context.decision !== 'denied') {
        const choice = el('label', 'so-stay'), checkbox = el('input', 'so-stay-checkbox');
        checkbox.type = 'checkbox'; checkbox.checked = pending ? pending.rememberForDevice === true : rememberForDevice;
        checkbox.disabled = busy || !!pending || !!pendingContinuation || invalidated || context.decision !== 'pending' || !context.deviceConsent.available;
        checkbox.dataset.loginControl = 'remember'; checkbox.addEventListener('change', () => {
          if (!busy && !pending && !pendingContinuation && !invalidated) { rememberForDevice = checkbox.checked; if (rememberForDevice) stayInApp = true; render(); }
        });
        choice.append(checkbox, el('span', '', 'Запомнить вход на этом устройстве')); contents.append(choice);
      }
      if (context.decision === 'denied') {
        const back = button('Вернуться в приложение', undefined, 'sw-button-primary', () => {
          if (busy || !valid()) return; busy = true; render();
          void complete().catch(() => { error = 'Возврат пока не подтверждён. Попробуйте снова.'; }).finally(() => { if (!disposed) { busy = false; render(); } });
        }); back.disabled = busy || invalidated; back.dataset.loginControl = 'return'; actions.append(back);
      } else {
        const kind = pending?.decision ?? (forgetting ? 'deny' : 'approve');
        const approve = button(pending || pendingContinuation ? 'Проверить вход' : forgetting ? 'Выйти и забыть на этом устройстве' : account.accountId ? 'Войти' : 'Создать профиль и войти', undefined, 'sw-button-primary',
          () => pendingContinuation ? void continueLogin() : void decide(kind, !pending && !account.accountId));
        approve.disabled = busy || invalidated || account.revoked || forgetting && (!account.accountId || !context.deviceConsent);
        approve.dataset.loginControl = 'approve'; actions.append(approve);
        if (!forgetting && !pending && !pendingContinuation && account.accountId && context.decision === 'pending') {
          const deny = button('Отмена', undefined, 'so-decline', () => void decide('deny'));
          deny.disabled = busy || invalidated; deny.dataset.loginControl = 'deny'; actions.append(deny);
        }
      }
    } else if (!busy) {
      const retry = button('Повторить', 'refresh', 'sw-button-primary', () => void refresh()); retry.dataset.loginControl = 'retry'; actions.append(retry);
    } else contents.append(el('p', 'so-summary', 'Проверяем запрос…'));
    card.replaceChildren(brand, title, contents, notice, actions);
    card.setAttribute('aria-busy', String(busy));
    if (focus) card.querySelector<HTMLElement>(`[data-login-control="${focus}"]`)?.focus();
  }
  const unobserve = ports.observeAccount(() => {
    if (disposed) return;
    if (creatingProfile) return; // The returned new account is checked before signing.
    sequence++; invalidated = true; busy = false;
    error = 'Профиль изменился. Начните вход в приложении ещё раз.'; render();
  });
  void refresh();
  return { canReload() { return !busy && !creatingProfile && (invalidated || !pending && !pendingContinuation); },
    dispose() { if (!disposed) { disposed = true; sequence++; clearTimeout(expiryTimer); unobserve(); root.remove(); } } };
}
