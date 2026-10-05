import type { ConnectClient } from '../../modules/connect/browser/index.mjs';
import { button, el, emptyState, labeledField, textInput } from '../world/dom';
import { icon } from '../world/icons';
import { createDialog, errorText, type WorldDialog } from '../world/dialogs';
import { createAppCreateState } from './app-create-state.mjs';
import { createAppBuilderAction, type AppActionDependencies } from './app-builder';
export type { AppActionDependencies } from './app-builder';
import { registerUpdateGuard } from './pwa';
import { parseDeviceLink } from './device-link.mjs';
import './local-apps.css';

interface HostDevice { hostDeviceId: string; connectorId: string; name: string; online: boolean }
function friendly(error: unknown): string {
  const code = typeof error === 'object' && error !== null && 'code' in error ? String(error.code) : error instanceof Error ? error.message : '';
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
    app_create_storage_unavailable: 'Браузер не смог сохранить отправку. Освободите место и повторите. Текст остаётся в этом окне.',
    app_create_lock_unavailable: 'Этот браузер не поддерживает безопасную отправку между вкладками. Откройте Соты в обновлённом браузере.',
    app_create_pending_unconfirmed: 'Сначала проверьте предыдущую отправку. Её устройство и текст сохранены.',
    app_create_result_pending: 'Предыдущая задача уже принята. Откройте её результат.',
    invalid_app_create_receipt: 'Ответ не подтверждает эту отправку. Повторная проверка использует тот же запрос.',
    invalid_app_create_target: 'Выберите доступное устройство.',
    invalid_app_create_prompt: 'Проверьте текст задачи и полный путь к папке.',
  };
  return messages[code] ?? errorText(error);
}
export function createAppActions(client: ConnectClient, refresh: () => Promise<void>, openAccount?: () => Promise<void>, deps: AppActionDependencies = {}) {
  const dialogs = new Set<WorldDialog>();
  const createStates = new Map<string, ReturnType<typeof createAppCreateState>>();
  const beforeUnload = (event: BeforeUnloadEvent) => {
    if ([...createStates.values()].some(state => state.hasVolatileDraft())) { event.preventDefault(); event.returnValue = ''; }
  };
  window.addEventListener('beforeunload', beforeUnload);
  const unregisterUpdateGuard = registerUpdateGuard(async () => {
    try { await Promise.all([...createStates.values()].map(state => state.flush())); return [...createStates.values()].every(state => !state.hasUnsavedChanges()); }
    catch { return false; }
  });
  const dialog = (title: string, close?: () => void) => {
    const value = createDialog(title, () => { dialogs.delete(value); close?.(); }); dialogs.add(value); return value;
  };
  async function connectDevice(): Promise<void> {
    let closed = false;
    const panel = dialog('Подключить устройство', () => { closed = true; });
    const status = el('p', 'sw-muted', 'Соединяемся с вашим коннектором…'); status.setAttribute('role', 'status');
    const body = el('div', 'sw-stack'); body.append(status); panel.body.append(body);
    let busy = false, forwardedPort: number | null = null;
    const attach = async (claim: { hostDeviceId: string; connectorId: string; claimCode: string }) => {
      const result = await client.extension<{ device: HostDevice }>('apps.claim', claim);
      claim.claimCode = '';
      if (closed) return;
      status.textContent = 'Его приложения теперь можно добавить в ваши соты.';
      body.replaceChildren(el('h3', '', result.device.name), status, button('Готово', 'check', 'sw-button-primary', () => panel.close()));
      await refresh();
    };
    const fileConnection = () => {
      if (busy || closed) return;
      const input = document.createElement('input'); input.type = 'file'; input.accept = '.json,application/json';
      input.addEventListener('change', () => { void (async () => {
        const file = input.files?.[0]; if (!file || busy || closed) return; busy = true;
        try {
          if (file.size > 4096) throw new Error('device_link_invalid');
          const claim = parseDeviceLink(await file.text(), window.location.origin);
          if (closed) return;
          body.replaceChildren(status); status.textContent = 'Подключаем устройство…';
          await attach(claim);
        } catch (error) {
          if (closed) return;
          status.textContent = error instanceof Error && error.message === 'device_link_invalid'
            ? 'Нужен файл подключения от коннектора для этого адреса Сот.' : friendly(error);
          body.replaceChildren(status, el('p', 'sw-muted', 'Файл действует 5 минут. Создайте новый, если время истекло.'),
            button('Выбрать файл', 'folder', 'sw-button-primary', fileConnection));
        } finally { busy = false; }
      })(); }); input.click();
    };
    const connect = async () => {
      if (busy || closed) return; busy = true;
      let stage: 'service' | 'connector' | 'claim' = 'service';
      body.replaceChildren(status); status.textContent = 'Соединяемся с вашим коннектором…';
      try {
        const response = await fetch('/api/apps/capabilities', { cache: 'no-store', signal: AbortSignal.timeout(8000) });
        if (!response.ok) throw new TypeError('Network unavailable');
        const capabilities = await response.json() as { localConnectorOrigin: string };
        const local = new URL(forwardedPort ? `http://127.0.0.1:${forwardedPort}` : capabilities.localConnectorOrigin);
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
        await attach({ hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, claimCode: claim.claimCode }); claim.claimCode = '';
      } catch (error) {
        if (closed) return;
        status.textContent = stage === 'connector' ? forwardedPort ? `Не удалось подключиться через порт ${forwardedPort}.` : 'Не найдено подключение к этому компьютеру.' : friendly(error);
        const windows = /Windows/i.test(navigator.userAgent);
        const mobile = /Android|iPhone|iPad/i.test(navigator.userAgent);
        const installer = (forWindows: boolean, primary = false): HTMLAnchorElement => {
          const link = el('a', `sw-button ${primary ? 'sw-button-primary' : 'sw-button-quiet'}`, forWindows ? 'Для Windows' : 'Для macOS / Linux');
          link.href = forWindows ? '/agent/install-windows-machine.cmd' : '/agent/install-macos-linux.sh';
          link.download = forWindows ? 'soty-connector.cmd' : 'soty-connector.sh'; return link;
        };
        const downloads = el('div', 'sw-row');
        downloads.append(installer(windows, !mobile), installer(!windows));
        body.replaceChildren(status, el('p', 'sw-muted', forwardedPort ? 'Проверьте SSH-туннель и разрешение браузера на доступ к приложениям этого устройства. Можно подключить сервер файлом от его коннектора.' : 'Откройте коннектор и разрешите браузеру подключение к нему. Затем нажмите «Повторить».'),
          button('Повторить', 'refresh', '', () => { void connect(); }),
          button('Подключить по файлу', 'folder', 'sw-button-quiet', fileConnection),
          button('Другое подключение', 'settings', 'sw-button-quiet', forwardedConnection));
        if (!forwardedPort) body.append(downloads);
        if (openAccount) body.append(button('Открыть мой профиль на другом устройстве', 'phone', 'sw-button-quiet', () => { panel.close(); void openAccount(); }));
      } finally { busy = false; }
    };
    const choice = (title: string, detail: string, symbol: string, action: () => void) => {
      const target = el('button', 'sw-connect-choice'); target.type = 'button'; const copy = el('span'); copy.append(el('strong', '', title), el('small', '', detail)); target.append(icon(symbol), copy, icon('next')); target.addEventListener('click', action); return target;
    };
    function forwardedConnection(): void {
      const form = el('form', 'sw-stack'), port = textInput('', 'Например, 49427', 5);
      port.type = 'number'; port.min = '1024'; port.max = '65535'; port.required = true;
      const submit = button('Подключить сервер', 'laptop', 'sw-button-primary'); submit.type = 'submit';
      form.append(el('p', 'sw-muted', 'Для сервера с SSH подключите его коннектор через локальный туннель. Соты обращаются только к вашему компьютеру, а туннель передаёт запрос серверу.'),
        labeledField('Локальный порт туннеля', port, 'Сам коннектор на сервере должен быть подключён к этому адресу Сот.'), submit,
        button('Подключить по файлу', 'folder', 'sw-button-quiet', fileConnection));
      form.addEventListener('submit', event => { event.preventDefault(); if (!form.reportValidity()) return; const value = Number(port.value);
        if (!Number.isSafeInteger(value) || value < 1024 || value > 65535) return; forwardedPort = value; void connect(); });
      body.replaceChildren(form); port.focus();
    }
    body.replaceChildren(choice('Подключить этот компьютер', 'Открывать его приложения и управлять им из Сот', 'laptop', () => { void connect(); }));
    body.append(choice('Подключить сервер по SSH', 'Для проекта, который уже работает на сервере', 'server', forwardedConnection));
    if (openAccount) body.append(choice('Мой профиль на другом устройстве', 'Те же записки, контакты и сообщества — по QR', 'phone', () => { panel.close(); void openAccount(); }));
    if (/Android|iPhone|iPad/i.test(navigator.userAgent)) body.append(el('p', 'sw-muted', 'Чтобы подключить компьютер, откройте Соты на нём. На телефоне можно открыть свой профиль по QR.'));
  }

  const builder = createAppBuilderAction({ client, refresh, connectDevice, openAccount, dialog, friendly, createStates, ...deps });
  const agentCreate = builder.open;
  const resetAccount = () => { builder.destroy(); for (const panel of [...dialogs]) panel.close(); dialogs.clear(); createStates.clear(); };
  return { connectDevice, agentCreate, mountAppBuilder: builder.mountInline, resetAccount, destroy: () => { resetAccount(); unregisterUpdateGuard(); window.removeEventListener('beforeunload', beforeUnload); } };
}
