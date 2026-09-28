import type { ConnectClient } from '../../modules/connect/browser/index.mjs';
import { button, el, emptyState, labeledField, textInput } from '../world/dom';
import { icon } from '../world/icons';
import { createDialog, errorText, type WorldDialog } from '../world/dialogs';
import './local-apps.css';

interface HostDevice { hostDeviceId: string; connectorId: string; name: string; online: boolean }
interface Proposal { schema: 'soty.local-app.v1'; name: string; port: number; entryPath: string; sourceJobId: string }
interface Pending { hostDeviceId: string; connectorId: string; jobId: string }
interface JobResult { job: { id: string; status: string; result: { text: string; textTruncated?: boolean; appProposal?: Proposal } | null }; done?: boolean; cursor?: number; events?: { seq: number; type: string; text: string }[] }

function friendly(error: unknown): string {
  const code = typeof error === 'object' && error !== null && 'code' in error ? String(error.code) : '';
  const messages: Record<string, string> = {
    apps_device_already_owned: 'Этот компьютер уже связан с другим аккаунтом.',
    apps_connector_offline: 'Коннектор пока не подключён к Сотам.',
    apps_device_not_owned: 'Сначала подключите выбранный компьютер к своему аккаунту.',
    apps_origin_not_configured: 'Адрес приложений ещё не настроен на сервере.',
    app_job_failed: 'Не удалось запустить задачу. Проверьте подключение компьютера.',
    app_model_unavailable: 'Подключение к ИИ ещё не настроено. Попробуйте позже.',
    invalid_workspace: 'Укажите папку проекта на выбранном компьютере.',
    job_request_conflict: 'Параметры задачи изменились. Откройте новую задачу.',
    authentication_required: 'Аккаунт изменился. Откройте задачу заново.',
  };
  return messages[code] ?? errorText(error);
}
function remember(key: string, value: Pending | null): void {
  try { if (value) localStorage.setItem(key, JSON.stringify(value)); else localStorage.removeItem(key); } catch { /* The running job still lives on the server. */ }
}
function recalled(key: string): Pending | null {
  try {
    const value = JSON.parse(localStorage.getItem(key) || 'null') as Pending | null;
    return value && [value.hostDeviceId, value.connectorId, value.jobId].every(item => typeof item === 'string' && /^[A-Za-z0-9_.:-]{3,180}$/u.test(item)) ? value : null;
  } catch { return null; }
}
function forgetPending(key: string, expected: Pending): void {
  // A late registration ACK must not erase a newer task started in the meantime.
  if (recalled(key)?.jobId === expected.jobId) remember(key, null);
}

export function createAppActions(client: ConnectClient, refresh: () => Promise<void>, openAccount?: () => Promise<void>) {
  const dialogs = new Set<WorldDialog>();
  const dialog = (title: string, close?: () => void) => {
    const value = createDialog(title, () => { dialogs.delete(value); close?.(); }); dialogs.add(value); return value;
  };
  async function connectDevice(): Promise<void> {
    let closed = false;
    const panel = dialog('Подключить устройство', () => { closed = true; });
    const status = el('p', 'sw-muted', 'Соединяемся с вашим коннектором…'); status.setAttribute('role', 'status');
    const body = el('div', 'sw-stack'); body.append(status); panel.body.append(body);
    let busy = false;
    const connect = async () => {
      if (busy || closed) return; busy = true;
      let stage: 'service' | 'connector' | 'claim' = 'service';
      body.replaceChildren(status); status.textContent = 'Соединяемся с вашим коннектором…';
      try {
        const response = await fetch('/api/apps/capabilities', { cache: 'no-store', signal: AbortSignal.timeout(8000) });
        if (!response.ok) throw new TypeError('Network unavailable');
        const capabilities = await response.json() as { localConnectorOrigin: string };
        const local = new URL(capabilities.localConnectorOrigin);
        if (local.protocol !== 'http:' || local.hostname !== '127.0.0.1' || local.pathname !== '/' || local.search || local.hash || local.username || local.password) throw new Error('invalid_local_endpoint');
        stage = 'connector';
        const claimed = await fetch(new URL('/apps/claim', local), {
          method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}', signal: AbortSignal.timeout(8000),
          targetAddressSpace: 'loopback',
        } as RequestInit & { targetAddressSpace: 'loopback' });
        const claim = await claimed.json() as { ok: boolean; hostDeviceId: string; connectorId: string; claimCode: string };
        if (!claimed.ok || !claim.ok) throw Object.assign(new Error('Connector unavailable'), { code: 'apps_connector_offline' });
        if (closed) return;
        stage = 'claim';
        const result = await client.extension<{ device: HostDevice }>('apps.claim', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, claimCode: claim.claimCode });
        claim.claimCode = '';
        if (closed) return;
        status.textContent = 'Теперь его приложения могут появляться в ваших сотах.';
        body.replaceChildren(el('h3', '', result.device.name), status, button('Готово', 'check', 'sw-button-primary', () => panel.close()));
        await refresh();
      } catch (error) {
        if (closed) return;
        status.textContent = stage === 'connector' ? 'Не найдено подключение к этому компьютеру.' : friendly(error);
        const windows = /Windows/i.test(navigator.userAgent);
        const mobile = /Android|iPhone|iPad/i.test(navigator.userAgent);
        const installer = (forWindows: boolean, primary = false): HTMLAnchorElement => {
          const link = el('a', `sw-button ${primary ? 'sw-button-primary' : 'sw-button-quiet'}`, forWindows ? 'Для Windows' : 'Для macOS / Linux');
          link.href = forWindows ? '/agent/install-windows-machine.cmd' : '/agent/install-macos-linux.sh';
          link.download = forWindows ? 'soty-connector.cmd' : 'soty-connector.sh'; return link;
        };
        const downloads = el('div', 'sw-row');
        downloads.append(installer(windows, !mobile), installer(!windows));
        body.replaceChildren(status, el('p', 'sw-muted', 'Установите коннектор на этом компьютере, откройте его и нажмите «Повторить».'),
          button('Повторить', 'refresh', '', () => { void connect(); }), downloads);
        if (openAccount) body.append(button('Открыть мой профиль на другом устройстве', 'phone', 'sw-button-quiet', () => { panel.close(); void openAccount(); }));
      } finally { busy = false; }
    };
    const choice = (title: string, detail: string, symbol: string, action: () => void) => {
      const target = el('button', 'sw-connect-choice'); target.type = 'button'; const copy = el('span'); copy.append(el('strong', '', title), el('small', '', detail)); target.append(icon(symbol), copy, icon('next')); target.addEventListener('click', action); return target;
    };
    body.replaceChildren(choice('Подключить этот компьютер', 'Открывать его приложения и управлять им из Сот', 'laptop', () => { void connect(); }));
    if (openAccount) body.append(choice('Мой профиль на другом устройстве', 'Те же записки, контакты и сообщества — по QR', 'phone', () => { panel.close(); void openAccount(); }));
    if (/Android|iPhone|iPad/i.test(navigator.userAgent)) body.append(el('p', 'sw-muted', 'Чтобы подключить компьютер, откройте Соты на нём. На телефоне можно открыть свой профиль по QR.'));
  }

  async function agentCreate(communityId?: string): Promise<void> {
    let closed = false, timer: ReturnType<typeof setTimeout> | undefined;
    const panel = dialog('Создать с ИИ', () => { closed = true; if (timer) clearTimeout(timer); });
    const state = await client.getLocalState();
    if (!state.accountId || closed) return;
    const accountId = state.accountId, pendingKey = `soty.world.agent:${accountId}`;
    const note = el('p', 'sw-muted', 'Загружаем ваши устройства…'); note.setAttribute('role', 'status'); panel.body.append(note);
    const request = <T>(op: string, args: Record<string, unknown>) => client.extension<T>(op, args);
    async function current(): Promise<boolean> { return !closed && (await client.getLocalState()).accountId === accountId; }

    async function showResult(result: JobResult, pending: Pending): Promise<void> {
      // Keep a completed result reachable until the user accepts it or starts a new task.
      remember(pendingKey, pending);
      const proposal = result.job.result?.appProposal;
      const success = result.job.status === 'succeeded' && proposal?.schema === 'soty.local-app.v1';
      const content = el('div', 'sw-stack');
      content.append(el('h3', '', success ? proposal.name : result.job.status === 'cancelled' ? 'Задача остановлена' : 'Приложение пока не готово'));
      if (success) {
        content.append(el('p', 'sw-muted', 'Работает на вашем компьютере. Пока доступно только вам.'));
        const add = button('Добавить в мои соты', 'plus', 'sw-button-primary', () => {
          add.disabled = true;
          void (async () => {
            try {
              await request('apps.register', { hostDeviceId: pending.hostDeviceId, connectorId: pending.connectorId,
                name: proposal.name, port: proposal.port, entryPath: proposal.entryPath, grants: { accountIds: [], communityIds: [] } });
              forgetPending(pendingKey, pending);
              if (!await current()) return;
              note.textContent = 'Приложение добавлено'; content.replaceChildren(el('h3', '', proposal.name), note, button('Готово', 'check', 'sw-button-primary', () => panel.close()));
              await refresh();
            } catch (error) { note.textContent = friendly(error); content.append(note); add.disabled = false; }
          })();
        });
        content.append(add);
        if (communityId) content.append(el('small', 'sw-muted', 'Доступ сообществу можно открыть в настройках приложения.'));
      }
      const detail = el('details', 'sw-agent-details'); detail.append(el('summary', '', 'Ответ агента'), el('pre', '', result.job.result?.text || 'Результат недоступен.'));
      if (result.job.result?.textTruncated) {
        const download = button('Скачать полный ответ', 'download', 'sw-button-quiet', () => {
          download.disabled = true;
          void (async () => {
            try {
              const parts: string[] = []; let offset: number | null = 0;
              while (offset !== null && await current()) {
                const page: { text: string; nextOffset: number | null } = await request('apps.agent.result', { ...pending, offset });
                parts.push(page.text); offset = page.nextOffset;
              }
              if (!await current()) return;
              const url = URL.createObjectURL(new Blob(parts, { type: 'text/plain;charset=utf-8' }));
              const link = el('a'); link.href = url; link.download = 'soty-agent-result.txt'; link.click();
              setTimeout(() => URL.revokeObjectURL(url), 1000);
            } catch (cause) { note.textContent = friendly(cause); content.append(note); }
            finally { download.disabled = false; }
          })();
        });
        detail.append(el('small', 'sw-muted', 'Показан фрагмент длинного ответа.'), download);
      }
      content.append(detail, button('Новая задача', 'plus', 'sw-button-quiet', () => { forgetPending(pendingKey, pending); panel.close(); void agentCreate(communityId); })); panel.body.replaceChildren(content);
    }

    function follow(pending: Pending): void {
      remember(pendingKey, pending);
      const activity = el('div', 'sw-stack');
      const progress = el('p', 'sw-agent-progress', 'Задача принята'); progress.setAttribute('role', 'status');
      const events = el('ol', 'sw-agent-events'); events.setAttribute('aria-label', 'Ход создания');
      const cancel = button('Остановить', 'close', 'sw-button-quiet', () => {
        cancel.disabled = true;
        void request<JobResult>('apps.agent.cancel', { ...pending }).then(() => { progress.textContent = 'Останавливаем…'; })
          .catch(error => { progress.textContent = friendly(error); cancel.disabled = false; });
      });
      activity.append(progress, events, el('small', 'sw-muted', 'Можно закрыть окно — задача продолжится.'), cancel); panel.body.replaceChildren(activity);
      let cursor = 0;
      const poll = async () => {
        if (!await current()) return;
        try {
          const result = await request<JobResult>('apps.agent.read', { ...pending, after: cursor });
          if (!await current()) return;
          for (const event of result.events || []) {
            if (!event.text || event.seq <= cursor) continue;
            const text = event.text.length > 280 ? event.text.slice(0, 280) + '…' : event.text;
            events.append(el('li', '', text)); while (events.children.length > 5) events.firstElementChild?.remove();
          }
          cursor = result.cursor ?? cursor;
          progress.textContent = ({ queued: 'Ждём ваш компьютер', leased: 'Агент подключился', running: 'Создаём приложение', cancelled: 'Остановлено', failed: 'Нужно проверить результат', succeeded: 'Приложение готово' } as Record<string, string>)[result.job.status] || 'Выполняется';
          if (result.done) { await showResult(result, pending); return; }
        } catch (error) {
          if (!await current()) return;
          progress.textContent = friendly(error);
          const code = typeof error === 'object' && error && 'code' in error ? String(error.code) : '';
          if (code === 'job_not_found') {
            panel.body.replaceChildren(emptyState('Эта задача больше недоступна', 'Можно начать новую. Уже добавленные приложения остаются в ваших сотах.', button('Новая задача', 'plus', 'sw-button-primary', () => { forgetPending(pendingKey, pending); panel.close(); void agentCreate(communityId); }), 'sparkle'));
            return;
          }
        }
        if (!closed) timer = setTimeout(() => { void poll(); }, document.hidden ? 8000 : 2000);
      };
      void poll();
    }

    const pending = recalled(pendingKey);
    if (pending) { follow(pending); return; }
    try {
      const { devices } = await request<{ devices: HostDevice[] }>('apps.devices', {});
      if (!await current()) return;
      if (!devices.length) {
        panel.body.replaceChildren(emptyState('Подключите компьютер', 'На нём агент создаст и запустит приложение.', button('Подключить', 'laptop', 'sw-button-primary', () => { panel.close(); void connectDevice(); }), 'laptop'));
        return;
      }
      const capabilityResponse = await fetch('/api/apps/capabilities', { cache: 'no-store', signal: AbortSignal.timeout(8000) });
      if (!capabilityResponse.ok) throw new TypeError('Network unavailable');
      const capabilities = await capabilityResponse.json() as { agentConfigured: boolean };
      if (!await current()) return;
      if (!capabilities.agentConfigured) {
        panel.body.replaceChildren(emptyState('ИИ пока недоступен', 'Подключение к модели ещё не настроено.',
          button('Проверить снова', 'refresh', 'sw-button-primary', () => { panel.close(); void agentCreate(communityId); }), 'sparkle'));
        return;
      }
      const form = el('form', 'sw-stack');
      const hosts = el('select', 'sw-input');
      for (const [index, device] of devices.entries()) { const item = el('option', '', `${device.name}${device.online ? '' : ' · не в сети'}`); item.value = String(index); hosts.append(item); }
      const prompt = el('textarea', 'sw-input'); prompt.rows = 4; prompt.maxLength = 16_000; prompt.required = true; prompt.placeholder = 'Например, общий список покупок';
      const cwd = textInput('', 'Выбрать существующую папку', 2000);
      const advanced = el('details', 'sw-agent-details');
      advanced.append(el('summary', '', 'Папка проекта'), labeledField('Полный путь', cwd, 'Оставьте пустым — создадим отдельную папку.'));
      const error = el('p', 'sw-error'); error.setAttribute('role', 'alert');
      const create = button('Создать', 'sparkle', 'sw-button-primary'); create.type = 'submit';
      form.append(labeledField('Что создаём?', prompt), labeledField('На устройстве', hosts), advanced, error, create);
      let requestId = crypto.randomUUID(), fingerprint = '', busy = false;
      form.addEventListener('submit', event => {
        event.preventDefault(); if (busy) return;
        const host = devices[Number(hosts.value)]; if (!host) return;
        const value = { hostDeviceId: host.hostDeviceId, connectorId: host.connectorId, text: prompt.value.trim(), cwd: cwd.value.trim() };
        const next = JSON.stringify(value); if (fingerprint && fingerprint !== next) requestId = crypto.randomUUID(); fingerprint = next;
        busy = true; create.disabled = true; error.textContent = '';
        void request<JobResult>('apps.agent.create', { ...value, requestId }).then(async result => {
          if (await current()) follow({ hostDeviceId: host.hostDeviceId, connectorId: host.connectorId, jobId: result.job.id });
        }).catch(cause => { if (!closed) error.textContent = friendly(cause); }).finally(() => { busy = false; create.disabled = false; });
      });
      panel.body.replaceChildren(form); prompt.focus();
    } catch (error) { if (!closed) note.textContent = friendly(error); }
  }
  return { connectDevice, agentCreate, destroy: () => { for (const panel of [...dialogs]) panel.close(); dialogs.clear(); } };
}
