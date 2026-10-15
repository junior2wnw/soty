import { button, el, labeledField } from './dom';
import type { WorldApi } from './types';

interface Reference { capabilityId: string; version: number; digest: string }
interface SourceApp {
  appId: string; title: string; proposalDigest: string; resources: string[]; recipients: string[];
  functions: { reference: Reference; title: string; effects: string[] }[];
  status: { state: string; mode: string | null; expiresAt: number | null };
}

/** Uses the current signed client; tokens never enter the browser. */
export function mountAgentSourceAccess(host: HTMLElement, options: { api: WorldApi; accountId: string }) {
  let disposed = false, sequence = 0;
  const pending = new Set<Promise<void>>();
  const root = el('section', 'sa-action'); root.setAttribute('aria-label', 'Приложения личного агента'); host.append(root);
  const request = <T>(op: string, args: Record<string, unknown> = {}) => options.api.request<T>(op, { ...args, expectedAccountId: options.accountId });
  function run(action: () => Promise<void>) {
    const promise = action(); pending.add(promise); void promise.finally(() => pending.delete(promise));
  }
  async function load() {
    const turn = ++sequence;
    root.replaceChildren(el('h2', '', 'Приложения личного агента'), el('p', 'sa-muted', 'Проверяем доступные приложения…'));
    try {
      const result = await request<{ accountId: string; deviceId: string; apps: SourceApp[] }>('agent.sources.list');
      if (disposed || turn !== sequence) return;
      if (result.accountId !== options.accountId || !Array.isArray(result.apps) || result.apps.length > 64) throw Error('unconfirmed');
      root.replaceChildren(el('h2', '', 'Приложения личного агента'),
        el('p', 'sa-muted', 'Разрешения действуют для вашего аккаунта на этом устройстве. Доступ к данным внутри приложения проверяется отдельно при каждом действии.'));
      for (const app of result.apps) renderApp(app);
      if (!result.apps.length) root.append(el('p', 'sa-muted', 'Нет доступных приложений с подключёнными инструментами.'));
      root.append(button('Обновить разрешения', 'refresh', 'sw-button-quiet', () => { if (!pending.size) void load(); }));
    } catch {
      if (!disposed && turn === sequence) root.replaceChildren(el('h2', '', 'Приложения личного агента'),
        el('p', 'sa-muted', 'Подключение инструментов сейчас недоступно.'), button('Проверить снова', 'refresh', 'sw-button-quiet', () => { void load(); }));
    }
  }
  function renderApp(app: SourceApp) {
    const block = el('section', 'sa-action'), form = el('form', 'sa-create-form');
    const state = app.status.state === 'active' ? `Разрешено: ${app.status.mode === 'write' ? 'чтение и действия' : 'чтение'} до ${new Date(app.status.expiresAt!).toLocaleString('ru-RU')}`
      : app.status.state === 'none' ? 'Доступ не выдан' : 'Доступ не действует';
    block.append(el('h3', '', app.title), el('p', 'sa-muted', state));
    const mode = el('select', 'sw-select');
    for (const [value, title] of [['read', 'Только чтение'], ['write', 'Чтение и действия']]) {
      const option = el('option', '', title); option.value = value!; mode.append(option);
    }
    const duration = el('select', 'sw-select');
    for (const [value, title] of [['3600', '1 час'], ['86400', '1 день'], ['604800', '7 дней']]) {
      const option = el('option', '', title); option.value = value!; duration.append(option);
    }
    const budget = el('input', 'sw-input'); budget.type = 'number'; budget.min = '1'; budget.max = '1000'; budget.value = '20'; budget.required = true;
    const details = el('div', 'sa-consent-summary');
    const showFunctions = () => {
      details.replaceChildren(el('p', '', 'Ваше рабочее пространство приложения. Разрешены только следующие функции:'));
      for (const entry of app.functions.filter(value => mode.value === 'write' || value.effects.length === 0))
        details.append(el('p', '', `${entry.title} · ${entry.effects.length ? entry.effects.map(effect => ({ create: 'создание', update: 'изменение', delete: 'удаление', send: 'отправка', execute: 'выполнение' }[effect] ?? 'изменение данных')).join(', ') : 'чтение'}`));
      details.append(el('p', 'sa-muted', 'Передача разрешений другим помощникам запрещена. Лимит общий для всех выбранных функций.'));
    };
    mode.addEventListener('change', showFunctions); showFunctions();
    const consent = el('input'); consent.type = 'checkbox'; consent.required = true;
    const consentLabel = el('label'); consentLabel.append(consent, document.createTextNode(' Разрешаю личному агенту эти функции в указанных пределах'));
    const message = el('p', 'sa-form-message'); message.setAttribute('role', 'status');
    const submit = button('Разрешить', undefined, 'sw-button-primary'); submit.type = 'submit';
    let attempted: Record<string, unknown> | null = null, busy = false;
    form.addEventListener('submit', event => {
      event.preventDefault(); if (busy || disposed || !form.reportValidity()) return;
      attempted ??= { appId: app.appId, proposalDigest: app.proposalDigest, mode: mode.value,
        durationSeconds: Number(duration.value), budget: Number(budget.value), requestId: crypto.randomUUID() };
      const args = attempted; busy = true; submit.disabled = true;
      for (const control of [mode, duration, budget, consent]) control.disabled = true;
      run(async () => {
        try {
          const reply = await request<{ appId: string; status: { state: string } }>('agent.sources.allow', args);
          if (disposed) return;
          if (reply.appId !== app.appId || reply.status.state !== 'active') throw Error('unconfirmed');
          await load();
        } catch { if (!disposed) { message.textContent = 'Результат не подтверждён. Повтор проверит тот же запрос; новый доступ автоматически не создаётся.'; submit.textContent = 'Проверить тот же запрос'; submit.disabled = false; } }
        finally { busy = false; }
      });
    });
    const revoke = button('Отключить доступ агента', 'lock', 'sw-button-quiet', () => {
      if (busy || disposed) return; busy = true; revoke.disabled = true; submit.disabled = true;
      run(async () => {
        try { await request('agent.sources.revoke', { appId: app.appId }); if (!disposed) await load(); }
        catch { if (!disposed) { message.textContent = 'Отзыв не подтверждён. Проверьте снова.'; revoke.disabled = false; } }
        finally { busy = false; }
      });
    });
    form.append(labeledField('Что разрешить', mode), labeledField('Срок', duration), labeledField('Лимит вызовов', budget), details, consentLabel, submit, revoke, message);
    block.append(form); root.append(block);
  }
  void load();
  return { dispose() { disposed = true; sequence++; root.remove(); }, async flush() { await Promise.allSettled([...pending]); }, hasUnsavedChanges() { return pending.size > 0; } };
}
