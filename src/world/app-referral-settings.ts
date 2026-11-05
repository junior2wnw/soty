import { button, el, textInput } from './dom';
import { createReferralSettingsState, dispatchReferralSettingsIntent, referralPercentToBps, referralBpsToPercent, validateReferralSettingsConfig } from './referral-settings-state.mjs';
import type { ReferralSettingsConfig, ReferralSettingsIntent } from './referral-settings-state.mjs';
import type { WorldApi } from './types';

const sameRates = (a: number[], b: number[]): boolean => a.length === b.length && a.every((value, i) => value === b[i]);
const code = (reason: unknown): string => reason && typeof reason === 'object' && 'code' in reason ? String(reason.code) : '';
function failureText(reason: unknown): string {
  const messages: Record<string, string> = {
    referral_settings_storage_unavailable: 'Браузер не подтвердил сохранение запроса. Ваш ввод остаётся здесь.',
    referral_settings_lock_unavailable: 'Для безопасного сохранения нужен браузер с поддержкой Web Locks.',
    referral_settings_pending_changed: 'Запрос изменился в другом окне. Проверьте его перед сохранением.',
    referral_settings_pending_unconfirmed: 'Сначала проверьте ранее отправленный запрос.',
    referral_settings_account_changed: 'Аккаунт изменился. Запрос сохранён для прежнего аккаунта.',
    referral_settings_invalid_receipt: 'Результат не подтверждён. Можно повторить проверку того же запроса.',
    referrals_revision_conflict: 'Ставки уже изменились в другом окне. Ваш ввод сохранён.',
    referrals_forbidden: 'Изменить ставки может только текущий владелец приложения.',
    apps_owner_required: 'Изменить ставки может только текущий владелец приложения.',
  };
  return messages[code(reason)] ?? 'Не удалось подтвердить результат. Проверьте сохранение ещё раз.';
}

export function createAppReferralSettings(options: { accountId: string; appId: string; api: WorldApi; isCurrent(): boolean }) {
  const { accountId, appId, api } = options;
  const store = createReferralSettingsState({ accountId, appId,
    storage: { getItem: key => localStorage.getItem(key), setItem: (key, value) => localStorage.setItem(key, value) },
    ...(navigator.locks ? { locks: navigator.locks } : {}),
  });
  const element = el('details', 'sw-app-settings-section sw-referral-settings');
  const summary = el('summary', '', 'Реферальная система');
  const body = el('div', 'sw-referral-body'), saved = el('p', 'sw-app-settings-hint sw-muted');
  const rows = el('div', 'sw-referral-rates'), validation = el('p', 'sw-app-settings-hint'), status = el('p', 'sw-app-settings-hint');
  status.setAttribute('role', 'status'); status.setAttribute('aria-live', 'polite');
  const total = el('p', 'sw-app-settings-hint');
  const advanced = el('details', 'sw-referral-advanced'); advanced.append(el('summary', '', 'Дополнительно'));
  const actions = el('div', 'sw-app-settings-actions');
  const add = button('Добавить линию', 'plus', 'sw-button-quiet', () => { fields.push('0'); notice = ''; render(); });
  const remove = button('Убрать последнюю', undefined, 'sw-button-quiet', () => { fields.pop(); notice = ''; render(); });
  const disable = button('Отключить начисления', undefined, 'sw-button-quiet', () => { fields = ['0']; notice = ''; render(); });
  advanced.append(el('p', 'sw-app-settings-hint sw-muted', 'Изменения действуют на будущие начисления. Выплаты не подключены.'), add, remove, disable);
  const save = button('Сохранить', 'check', '', () => { void submit(); });
  const reset = button('Взять текущие', undefined, 'sw-button-quiet', () => { void takeCurrent(); });
  actions.append(save, reset); body.append(saved, rows, total, validation, advanced, status, actions); element.append(summary, body);
  const inputs: HTMLInputElement[] = [];
  let head: ReferralSettingsConfig | null = null, pending: ReferralSettingsIntent | null = null, fields: string[] = [];
  let draftRevision: number | null = null;
  let busy = false, editable = false, disposed = false, failure = '', notice = '', rejected: unknown = null;
  const current = (): boolean => !disposed && options.isCurrent();
  const request = (op: string, args: Record<string, unknown>): Promise<unknown> => api.request(op, args);
  function syncPending(): void {
    try {
      pending = store.read().pending;
      if (rejected && (!pending || typeof rejected !== 'object' || !('mutationId' in rejected) || rejected.mutationId !== pending.mutationId)) rejected = null;
    } catch (reason) { failure = failureText(reason); }
  }
  function parsed(): number[] | null {
    try {
      const values = fields.map(referralPercentToBps);
      return values.length >= 1 && values.length <= 10 && values.reduce((sum, value) => sum + value, 0) <= 10000 ? values : null;
    } catch { return null; }
  }
  const dirty = (): boolean => Boolean(fields.length && (!head || !parsed() || !sameRates(parsed()!, head.ratesBps)));
  function render(): void {
    if (!current()) return;
    const values = parsed(), blocked = busy || !editable || !head || Boolean(pending && !rejected);
    while (inputs.length > fields.length) { inputs.pop()!.closest('label')!.remove(); }
    fields.forEach((value, i) => {
      let input = inputs[i];
      if (!input) {
        const label = el('label', 'sw-referral-rate'), text = `${i + 1}-я линия`;
        input = textInput(); input.inputMode = 'decimal'; input.setAttribute('aria-label', text); input.maxLength = 6;
        const target = input;
        input.addEventListener('input', () => { fields[i] = target.value; notice = ''; failure = ''; render(); });
        label.append(el('span', '', text), input, el('span', 'sw-muted', '%')); rows.append(label); inputs.push(input);
      }
      if (input.value !== value) input.value = value;
      input.readOnly = blocked;
      try { referralPercentToBps(value); input.setAttribute('aria-invalid', 'false'); }
      catch { input.setAttribute('aria-invalid', 'true'); }
    });
    saved.textContent = head ? `Действует: ${head.ratesBps.map(value => referralBpsToPercent(value) + '%').join(' · ')}` : '';
    saved.hidden = !head;
    total.textContent = values ? `Всего: ${referralBpsToPercent(values.reduce((sum, value) => sum + value, 0))}%` : '';
    validation.textContent = fields.length && !values ? 'От 0 до 100% с точностью до 0,01%. Общая доля — не больше 100%.' : '';
    validation.hidden = !validation.textContent;
    status.textContent = busy ? 'Проверяем…' : failure || notice;
    status.hidden = !status.textContent; status.setAttribute('role', failure ? 'alert' : 'status');
    save.querySelector('span')!.textContent = !head ? 'Повторить загрузку' : rejected ? 'Сохранить мои ставки' : pending ? 'Проверить сохранение' : 'Сохранить';
    save.disabled = busy || !editable || Boolean(head && !pending && (!dirty() || !values)) || Boolean(rejected && !values);
    save.setAttribute('aria-busy', String(busy));
    reset.hidden = !head || !dirty() && !pending || Boolean(pending && !rejected); reset.disabled = busy || !editable;
    add.disabled = blocked || fields.length >= 10; remove.disabled = blocked || fields.length <= 1; disable.disabled = blocked || sameRates(values ?? [], [0]);
  }
  async function load(): Promise<void> {
    if (!current() || busy) return;
    const keep = dirty() || Boolean(pending); busy = true; failure = ''; render();
    try {
      const latest = validateReferralSettingsConfig(await request('referrals.config.get', { expectedAccountId: accountId, appId }), appId);
      if (!current()) return;
      head = latest;
      if (!keep) { fields = latest.ratesBps.map(referralBpsToPercent); draftRevision = latest.revision; }
      else if (!pending && parsed() && sameRates(parsed()!, latest.ratesBps)) draftRevision = latest.revision;
      else if (draftRevision !== null && draftRevision !== latest.revision) notice = 'Ставки изменились в другом окне. Ваш ввод сохранён.';
    } catch (reason) { if (current()) failure = failureText(reason); }
    finally { busy = false; render(); }
  }
  async function submit(): Promise<void> {
    if (!current() || busy || !editable) return;
    if (!head) { await load(); return; }
    const values = parsed(); if (!values && (!pending || rejected)) return;
    busy = true; failure = ''; notice = ''; render();
    try {
      if (pending && rejected) {
        pending = await store.resolveRejected(pending, rejected, { expectedRevision: head.revision, ratesBps: values! }, current);
        rejected = null; draftRevision = pending?.expectedRevision ?? draftRevision;
      } else if (!pending) pending = await store.prepare({ expectedRevision: draftRevision ?? head.revision, ratesBps: values! });
      if (!pending || !current()) return;
      const submitted = pending, draftMatches = Boolean(values && sameRates(values, pending.ratesBps));
      const latest = await dispatchReferralSettingsIntent({ state: store, pending, request, isCurrent: current });
      if (!current()) return;
      head = latest; rejected = null;
      const matches = sameRates(latest.ratesBps, submitted.ratesBps);
      if (draftMatches && matches) { fields = latest.ratesBps.map(referralBpsToPercent); draftRevision = latest.revision; }
      notice = !draftMatches ? 'Прежний запрос подтверждён. Ваши новые ставки ещё не сохранены.'
        : matches ? 'Сохранено' : 'Запрос подтверждён. Действующие ставки уже изменились; ваш ввод сохранён.';
    } catch (reason) {
      if (!current()) return;
      failure = failureText(reason);
      if (code(reason) === 'referrals_revision_conflict') {
        rejected = null;
        try {
          const latest = validateReferralSettingsConfig(await request('referrals.config.get', { expectedAccountId: accountId, appId }), appId);
          if (current()) { head = latest; rejected = reason; }
        } catch { if (current()) notice = 'Не удалось обновить действующие ставки. Повторите проверку сохранения.'; }
      }
    } finally { busy = false; if (current()) { syncPending(); render(); } }
  }
  async function takeCurrent(): Promise<void> {
    if (!current() || busy || !editable || !head || pending && !rejected) return;
    busy = true; render();
    try {
      if (pending) await store.resolveRejected(pending, rejected, null, current);
      if (!current()) return;
      fields = head.ratesBps.map(referralBpsToPercent); draftRevision = head.revision; rejected = null; failure = ''; notice = '';
    } catch (reason) { if (current()) failure = failureText(reason); }
    finally { busy = false; if (current()) { syncPending(); render(); } }
  }
  const changed = (event: StorageEvent): void => {
    if (!current() || event.key !== store.key) return;
    const prior = pending?.mutationId; syncPending();
    if (prior !== pending?.mutationId) rejected = null;
    if (element.open && !busy) void load(); else render();
  };
  try { pending = store.read().pending; } catch (reason) { failure = failureText(reason); }
  if (pending) { fields = pending.ratesBps.map(referralBpsToPercent); draftRevision = pending.expectedRevision; notice = 'Есть неподтверждённый запрос. Проверьте сохранение.'; }
  element.addEventListener('toggle', () => { if (element.open && !head && !busy) void load(); });
  window.addEventListener('storage', changed); render();
  return { element,
    setEditable(value: boolean): void { editable = value; render(); },
    hasUnsaved(): boolean { return busy || Boolean(pending) || dirty(); },
    dispose(): void { disposed = true; window.removeEventListener('storage', changed); },
  };
}
