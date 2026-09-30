import { button, el } from './dom';
import { icon } from './icons';
import './world.css';
import './oauth-consent.css';

export interface OAuthConsentContext {
  interactionId: string; contextDigest: string; browserNonce: string; clientProfile: string; clientLabel: string;
  resource: string; scope: 'notes.createDraft'; durationMs: number; budgetLimit: number;
  expiresAt: number; checkedAt: number; decision: 'pending' | 'approved' | 'denied'; decidedAccountId: string | null;
}
export interface OAuthConsentAccount { accountId: string | null; label: string }
export interface OAuthConsentPorts {
  account(): Promise<OAuthConsentAccount>;
  observeAccount(listener: () => void): () => void;
  context(): Promise<OAuthConsentContext>;
  decide(kind: 'approve' | 'deny', args: { expectedAccountId: string; interactionId: string; contextDigest: string; browserNonce: string }): Promise<void>;
  complete(expectedAccountId: string): void | Promise<void>;
  openAccount(): Promise<void>;
}

/** One short decision card. No client-supplied HTML, auto approval, offline
 * outbox or raw OAuth token belongs in this controller. */
export function mountOAuthConsent(host: HTMLElement, ports: OAuthConsentPorts): { dispose(): void } {
  let disposed = false, sequence = 0, busy = false, stale = false, error = '';
  let account: OAuthConsentAccount = { accountId: null, label: '' }, context: OAuthConsentContext | null = null;
  let deadline = 0, disclosureOpen = false;
  let expiryTimer: ReturnType<typeof setTimeout> | undefined;
  const root = el('section', 'so-consent');
  const card = el('div', 'so-card'); root.append(card); host.replaceChildren(root);
  const alive = (ticket: number) => !disposed && sequence === ticket;

  function expire(): void {
    sequence++; busy = false; stale = true; clearTimeout(expiryTimer);
    error = 'Запрос истёк. Начните подключение в клиенте ещё раз.'; render();
  }
  async function complete(): Promise<void> {
    if (disposed || busy || stale || !context || context.decision === 'pending') return;
    if (performance.now() >= deadline) { expire(); return; }
    const ticket = sequence, expected = context.decidedAccountId;
    if (!expected || expected !== account.accountId) { invalidate(); return; }
    busy = true; error = ''; render();
    try {
      const selected = await ports.account();
      if (!alive(ticket)) return;
      if (selected.accountId !== expected) { invalidate(); return; }
      if (performance.now() >= deadline) { expire(); return; }
      await ports.complete(expected);
    } catch (failure) {
      if (alive(ticket)) {
        busy = false; stale = true;
        const blocked = failure && typeof failure === 'object' && 'code' in failure && failure.code === 'navigation_policy_blocked';
        error = blocked ? 'Браузер заблокировал возврат. Проверьте запрос или откройте клиент.'
          : 'Возврат в клиент не подтверждён. Проверьте состояние запроса.'; render();
      }
    }
  }
  async function decide(kind: 'approve' | 'deny'): Promise<void> {
    if (busy || stale || !context || !account.accountId || context.decision !== 'pending') return;
    if (performance.now() >= deadline) { expire(); return; }
    const ticket = sequence, expected = account.accountId, proposal = context;
    busy = true; error = ''; render();
    try {
      if ((await ports.account()).accountId !== expected) { if (alive(ticket)) invalidate(); return; }
      if (!alive(ticket)) return;
      await ports.decide(kind, { expectedAccountId: expected, interactionId: proposal.interactionId,
        contextDigest: proposal.contextDigest, browserNonce: proposal.browserNonce });
      if (!alive(ticket)) return;
      if ((await ports.account()).accountId !== expected) { if (alive(ticket)) invalidate(); return; }
      if (!alive(ticket)) return;
      context = { ...proposal, decision: kind === 'approve' ? 'approved' : 'denied', decidedAccountId: expected };
      busy = false; render(); void complete();
    } catch {
      if (alive(ticket)) {
        error = 'Решение пока не подтверждено. Проверьте его состояние.';
        busy = false; stale = true; render();
      }
    }
  }
  function invalidate(): void {
    if (disposed) return;
    sequence++; busy = false; stale = true; clearTimeout(expiryTimer);
    error = 'Аккаунт изменился. Проверьте запрос заново.'; render();
  }
  async function load(): Promise<void> {
    if (disposed) return;
    const ticket = ++sequence, startedAt = performance.now();
    context = null; busy = true; stale = false; error = ''; clearTimeout(expiryTimer); render();
    try {
      const [selected, proposal] = await Promise.all([ports.account(), ports.context()]);
      if (!alive(ticket)) return;
      account = selected; context = proposal; busy = false;
      // Charge the whole read round-trip conservatively. Wall-clock skew on the
      // user's computer cannot renew or prematurely expire a server proposal.
      deadline = startedAt + Math.min(600000, Math.max(0, proposal.expiresAt - proposal.checkedAt));
      if (performance.now() >= deadline) { expire(); return; }
      if (proposal.decision !== 'pending' && proposal.decidedAccountId !== selected.accountId) {
        stale = true; error = 'Решение относится к другому аккаунту. Выберите его или начните новое подключение.';
      }
      expiryTimer = setTimeout(() => { if (alive(ticket)) expire(); }, Math.max(0, deadline - performance.now()));
      render();
    } catch {
      if (alive(ticket)) { busy = false; error = 'Запрос недоступен. Проверьте связь или начните подключение ещё раз.'; render(); }
    }
  }
  function render(): void {
    if (disposed) return;
    const previousFocus = document.activeElement instanceof HTMLElement && card.contains(document.activeElement)
      ? document.activeElement.getAttribute('data-so-focus') : null;
    const currentDisclosure = card.querySelector('details');
    if (currentDisclosure) disclosureOpen = currentDisclosure.open;
    const focusKey = <T extends HTMLElement>(node: T, key: string): T => { node.setAttribute('data-so-focus', key); return node; };
    const busyControl = (node: HTMLButtonElement): HTMLButtonElement => { node.setAttribute('aria-disabled', String(busy)); return node; };
    card.setAttribute('aria-busy', String(busy));
    const header = el('div', 'so-brand');
    const mark = el('span', 'so-mark soty-hex'); mark.setAttribute('aria-hidden', 'true'); mark.append(icon('cells'));
    header.append(mark, el('span', '', 'соты'));
    const title = el('h1', 'so-title', context?.decision === 'approved' ? 'Доступ разрешён'
      : context?.decision === 'denied' ? 'Доступ не выдан' : 'Создавать записки в Сотах?');
    title.id = 'soty-oauth-title'; title.tabIndex = -1; focusKey(title, 'title'); root.setAttribute('aria-labelledby', title.id);
    const content = el('div', 'so-content');
    if (context) {
      const client = el('p', 'so-client', context.clientLabel); client.append(el('span', '', 'Внешнее приложение'));
      content.append(client);
      const owner = button(account.accountId ? account.label || 'Ваш аккаунт' : 'Выбрать аккаунт', 'person', 'so-account', () => {
          if (busy || disposed) return;
          void ports.openAccount().then(() => { if (!disposed) void load(); }).catch(() => { if (!disposed) { error = 'Не удалось открыть аккаунт.'; render(); } });
        });
      busyControl(focusKey(owner, 'account'));
      owner.setAttribute('aria-label', account.accountId ? `Аккаунт: ${account.label || 'Ваш аккаунт'}. Изменить` : 'Выбрать аккаунт Сот');
      if (account.accountId) owner.append(el('small', '', account.accountId.slice(-8)));
      content.append(owner);
      if (context.decision !== 'pending') {
        content.append(el('p', 'so-summary', `Решение для аккаунта · ${context.decidedAccountId?.slice(-8) || '—'}`));
        if (!stale) content.append(el('p', 'so-summary', 'Вернитесь в клиент, чтобы продолжить.'));
      } else {
        const allowed = el('div', 'so-effect'); allowed.append(icon('note'));
        const explanation = el('div'); explanation.append(el('strong', '', 'Новые приватные записки'), el('span', '', 'Без чтения ваших записок'));
        allowed.append(explanation);
        const hours = context.durationMs / 3600000;
        const duration = hours >= 1 ? `${Math.floor(hours)} ч` : `${Math.max(1, Math.floor(context.durationMs / 60000))} мин`;
        const limits = el('div', 'so-limits');
        limits.append(el('span', '', `До ${context.budgetLimit} записок`), el('span', '', duration), el('span', '', 'Бесплатно'));
        const details = el('details', 'so-details'); details.open = disclosureOpen;
        details.append(focusKey(el('summary', '', 'Об этом подключении'), 'details'));
        details.append(el('p', '', 'Имя клиента не подтверждает его разработчика. Разрешайте доступ только программе, в которой начали подключение.'));
        details.append(el('p', '', 'Отозвать доступ можно в Сотах → Доступы и действия. Созданные записки останутся у вас.'));
        details.append(el('span', 'so-resource', context.resource));
        content.append(allowed, limits, details);
      }
    } else if (busy) {
      const loading = el('p', 'so-summary', 'Проверяем запрос…'); loading.setAttribute('role', 'status'); content.append(loading);
    }
    const message = el('p', 'so-error', error); message.setAttribute('role', 'status'); message.setAttribute('aria-live', 'polite');
    const actions = el('div', 'so-actions');
    if (context && context.decision !== 'pending' && !stale) actions.append(busyControl(focusKey(button('Вернуться в клиент', 'diagonal', 'sw-button-primary', () => { void complete(); }), 'complete')));
    else if (!context || stale) {
      const retry = button('Проверить запрос', 'refresh', 'sw-button-primary', () => { if (!busy) void load(); }); actions.append(busyControl(focusKey(retry, 'retry')));
    } else {
      const decline = button('Отказать', undefined, 'so-decline', () => { void decide('deny'); });
      const allow = button(busy ? 'Подтверждаем…' : 'Разрешить', 'shield', 'sw-button-primary', () => { void decide('approve'); });
      decline.disabled = !account.accountId; allow.disabled = !account.accountId;
      actions.append(busyControl(focusKey(decline, 'deny')), busyControl(focusKey(allow, 'approve')));
    }
    const home = focusKey(el('a', 'so-home', 'Открыть Соты'), 'home'); home.href = '/';
    card.replaceChildren(header, title, content, message, actions, home);
    if (previousFocus) {
      const replacement = card.querySelector<HTMLElement>(`[data-so-focus="${previousFocus}"]`);
      if (replacement && !(replacement instanceof HTMLButtonElement && replacement.disabled)) replacement.focus({ preventScroll: true });
      else title.focus({ preventScroll: true });
    }
  }
  const unobserve = ports.observeAccount(invalidate);
  void load();
  return { dispose() { if (disposed) return; disposed = true; sequence++; clearTimeout(expiryTimer); unobserve(); root.remove(); } };
}
