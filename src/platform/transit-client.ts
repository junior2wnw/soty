import { accountClient, observeAccount } from '../core/connect-client';
import { createDialog } from '../world/dialogs';
import { checkLocalCompanionAgent, downloadAgentInstallerForDevice } from '../features/connector';
// @ts-expect-error The browser-neutral profile policy is shared with the agent.
import { normalizeTransitClientProfile } from '../../scripts/agent-modules/transit-client.mjs';
// @ts-expect-error The account boundary is tested directly without a browser.
import { loadTransitAccountState, saveTransitAccountProfile, saveTransitController } from './transit-profile-store.mjs';
import './transit-client.css';

interface TransitState { configured: boolean; active: boolean; running: boolean; phase: string; version: string; lastError: string; entryCount: number; availableEntryCount?: number; }
interface TransitReply { ok?: boolean; deviceId?: string; supported?: boolean; runtime?: TransitState; profile?: unknown; error?: string; }

const messages: Record<string, string> = {
  'transit-profile-invalid': 'Этот файл не является защищённым профилем TRANSIT.',
  'transit-profile-required': 'Сначала добавьте существующий профиль доступа.',
  'transit-stop-before-import': 'Выключите подключение перед заменой профиля.',
  'transit-device-mismatch': 'Привязка компьютера изменилась. Откройте управление заново.',
  'transit-control-not-paired': 'Разрешите управление этим компьютером через установщик Сот.',
  'transit-core-pin-mismatch': 'Проверка сетевого ядра не прошла. Обновите агент Сот.',
  'transit-underlay-unavailable': 'Не найдено активное подключение Ethernet или Wi-Fi.',
  'transit-probe-failed': 'Сеть TRANSIT сейчас не прошла проверку. Прежнее подключение сохранено.',
  'transit-start-unconfirmed': 'Системное подключение не подтверждено. Проверьте состояние агента.',
  'transit-stop-unconfirmed': 'Остановка не подтверждена. Подключение требует проверки.',
  'transit-platform-unsupported': 'Управление клиентом пока подготовлено для Windows x64.',
  'transit_access_capacity': 'Все доступы сейчас заняты. Попробуйте позже: существующие профили продолжают работать.',
  'transit_access_unavailable': 'Выдача профилей временно недоступна. Попробуйте ещё раз.',
};

async function localTransit(body?: Record<string, unknown>): Promise<TransitReply> {
  const response = await fetch('http://127.0.0.1:49424/operator/transit/client', {
    method: body ? 'POST' : 'GET', cache: 'no-store',
    ...(body ? { headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) } : {}),
    signal: AbortSignal.timeout(body ? 150_000 : 2500), targetAddressSpace: 'loopback',
  } as RequestInit & { targetAddressSpace: 'loopback' });
  const value = await response.json().catch(() => ({})) as TransitReply;
  if (!response.ok || value.ok !== true) throw new Error(value.error || 'transit-agent-unavailable');
  return value;
}

function downloadProfile(profile: unknown): void {
  const portable = structuredClone(profile) as { inbounds?: Record<string, unknown>[] };
  for (const inbound of portable.inbounds || []) delete inbound.interface_name;
  const url = URL.createObjectURL(new Blob([`${JSON.stringify(portable, null, 2)}\n`], { type: 'application/json' }));
  const link = document.createElement('a'); link.href = url; link.download = 'transit.json';
  document.body.append(link); link.click(); link.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 10_000);
}

export async function openTransitClient(options: { openSharedInternet?: () => void } = {}): Promise<void> {
  let unobserve = () => {};
  let refreshTimer: ReturnType<typeof setInterval> | undefined;
  let generation = 0, disposed = false;
  const dialog = createDialog('TRANSIT', () => {
    disposed = true; generation++; unobserve(); clearInterval(refreshTimer);
  });
  dialog.element.classList.add('transit-client-dialog');
  const selectedAccount = (await accountClient.getLocalState()).accountId;
  if (disposed || !dialog.element.isConnected) return;
  if (!selectedAccount) { dialog.body.textContent = 'Откройте свой профиль Сот, чтобы получить доступ TRANSIT.'; return; }
  const owner: string = selectedAccount;
  unobserve = observeAccount(state => { if (state.accountId !== owner) dialog.close(); });
  const mobile = /android|iphone|ipad|ipod|mobile/iu.test(`${navigator.userAgent} ${navigator.platform}`);
  let saved: unknown = null, local: TransitReply | null = null, working = false, issuingEnabled = false;
  let hostDeviceId = '', controlToken = '', authorized = false;
  const live = (ticket: number) => !disposed && ticket === generation && dialog.element.isConnected;
  const current = async (ticket = generation) => {
    if (!live(ticket)) return false;
    const state = await accountClient.getLocalState();
    return live(ticket) && state.accountId === owner;
  };
  const ownerPost = async (body: Record<string, unknown>): Promise<TransitReply> => {
    const ticket = generation;
    if (!(await current(ticket)) || !live(ticket)) throw new Error('transit-owner-changed');
    return localTransit(body);
  };
  const node = <K extends keyof HTMLElementTagNameMap>(tag: K, text = '', className = '') => {
    const element = document.createElement(tag); element.textContent = text; element.className = className; return element;
  };
  const notice = node('p', '', 'transit-client-notice'); notice.setAttribute('role', 'status');
  const errorMessage = (error: unknown) => {
    const key = error instanceof Error ? error.message : '';
    return messages[key] || 'Не удалось выполнить действие. Доступ и существующие профили сохранены.';
  };
  async function refresh(): Promise<void> {
    const [account, agent, companion, access] = await Promise.all([
      loadTransitAccountState(accountClient, owner),
      mobile ? Promise.resolve(null) : localTransit().catch(() => null),
      mobile ? Promise.resolve(null) : checkLocalCompanionAgent(2000).catch(() => null),
      accountClient.extension('transit.access.status', { expectedAccountId: owner }, { expectedAccountId: owner }).catch(() => ({ enabled: false })),
    ]);
    if (!(await current())) return;
    saved = account.profile; local = agent; issuingEnabled = access.enabled === true;
    hostDeviceId = agent?.deviceId || companion?.deviceId || hostDeviceId;
    controlToken = account.controllers[hostDeviceId] || ''; authorized = false;
    if (agent?.deviceId && controlToken) {
      authorized = await ownerPost({ action: 'authorize', deviceId: agent.deviceId, controlToken }).then(() => true, () => false);
    }
    if (await current()) render();
  }
  const agentAction = (body: Record<string, unknown>) => {
    if (!authorized || !controlToken) throw new Error('transit-control-not-paired');
    return ownerPost({ ...body, deviceId: local?.deviceId, controlToken });
  };
  async function pairComputer(): Promise<void> {
    hostDeviceId ||= `device-${crypto.randomUUID()}`;
    if (!controlToken) {
      const bytes = crypto.getRandomValues(new Uint8Array(32));
      controlToken = btoa(String.fromCharCode(...bytes)).replace(/\+/gu, '-').replace(/\//gu, '_').replace(/=+$/gu, '');
      await saveTransitController(accountClient, owner, hostDeviceId, controlToken);
    }
    if (!(await current())) return;
    const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(controlToken)));
    const hash = [...digest].map(byte => byte.toString(16).padStart(2, '0')).join('');
    const ticket = generation;
    if (!(await current(ticket)) || !live(ticket)) return;
    downloadAgentInstallerForDevice('machine', { id: hostDeviceId }, '', hash);
  }
  async function perform(action: () => Promise<void>, success: string): Promise<void> {
    if (working || !(await current())) return;
    working = true; notice.textContent = 'Проверяем подключение…'; render();
    try {
      await action();
      if (!(await current())) return;
      await refresh(); notice.textContent = success;
    } catch (error) {
      if (await current()) {
        await refresh().catch(() => { local = null; });
        notice.textContent = errorMessage(error);
      }
    }
    finally { working = false; if (await current()) render(); }
  }
  function render(): void {
    const body = node('section', '', 'transit-client');
    body.append(node('p', 'Личный профиль TRANSIT для sing-box на телефоне, планшете или компьютере.', 'transit-client-lead'));
    const state = local?.runtime;
    const status = node('div', '', 'transit-client-state');
    status.dataset.active = String(state?.active === true);
    status.append(node('strong', mobile ? (saved ? 'Профиль доступен' : 'Добавьте ваш доступ')
      : !state ? 'Агент недоступен или требует обновления' : state.active ? 'Включён на этом компьютере'
        : state.running || state.phase === 'unknown' || state.phase === 'error' ? 'Подключение требует проверки' : 'Выключен на этом компьютере'));
    if (!mobile && state?.version) status.append(node('span', `sing-box ${state.version} · выходов: ${state.entryCount}`));
    if (state?.active && state.availableEntryCount && state.availableEntryCount < state.entryCount)
      status.append(node('span', `Проверку прошли ${state.availableEntryCount} из ${state.entryCount} выходов. Остальные временно исключены.`));
    body.append(status, notice);
    const actions = node('div', '', 'transit-client-actions');
    const actionButton = (label: string, action: () => void, primary = false) => {
      const button = node('button', label, `sw-button${primary ? ' primary' : ''}`); button.type = 'button'; button.disabled = working;
      button.addEventListener('click', action); actions.append(button); return button;
    };
    if (!saved && issuingEnabled) actionButton('Получить профиль sing-box', () => void perform(async () => {
      const ticket = generation;
      const result = await accountClient.extension('transit.access.profile', { expectedAccountId: owner }, { expectedAccountId: owner });
      if (!(await current(ticket))) return;
      const profile = normalizeTransitClientProfile(result.profile);
      await saveTransitAccountProfile(accountClient, owner, profile);
    }, 'Личный профиль готов. Скачайте его и добавьте в sing-box.'), true);
    if (saved) actionButton('Скачать профиль sing-box', () => downloadProfile(saved), true);
    if (!mobile && local?.supported && state && authorized) {
      const shouldStop = state.running || state.phase === 'unknown';
      if (shouldStop || state.configured || saved) actionButton(shouldStop ? 'Выключить' : 'Включить', () => void perform(async () => {
        if (!shouldStop && !state.configured) {
          if (!saved) throw new Error('transit-profile-required');
          await agentAction({ action: 'import', profile: saved });
        }
        const reply = await agentAction({ action: shouldStop ? 'stop' : 'start' });
        if (!reply.runtime || (shouldStop ? reply.runtime.running || reply.runtime.phase === 'unknown' : !reply.runtime.active)) throw new Error(shouldStop ? 'transit-stop-unconfirmed' : 'transit-start-unconfirmed');
        local = { ...local, ...reply };
      }, shouldStop ? 'Подключение выключено.' : 'Подключение включено и проверено.'), true);
      if (state.configured && !saved) actionButton('Сохранить доступ для телефона', () => void perform(async () => {
        const exported = await agentAction({ action: 'export' });
        await saveTransitAccountProfile(accountClient, owner, normalizeTransitClientProfile(exported.profile));
      }, 'Доступ сохранён в зашифрованном профиле Сот. На телефоне откройте тот же профиль.'));
    } else if (!mobile) {
      body.append(node('p', 'Нужен обновлённый агент Сот с клиентом TRANSIT. Сетевое подключение включается отдельной кнопкой.'));
      actionButton(local ? 'Разрешить управление этим компьютером' : 'Обновить агент Сот', () => void perform(pairComputer, 'Запустите скачанный установщик, затем обновите состояние. Подключение включается отдельной кнопкой.'), true);
    }
    const file = node('input'); file.type = 'file'; file.accept = 'application/json,.json'; file.hidden = true;
    file.addEventListener('change', () => {
      const chosen = file.files?.[0]; if (!chosen) return;
      void perform(async () => {
        if (state?.running) throw new Error('transit-stop-before-import');
        if (chosen.size > 256_000) throw new Error('transit-profile-invalid');
        let profile;
        try { profile = normalizeTransitClientProfile(JSON.parse(await chosen.text())); }
        catch { throw new Error('transit-profile-invalid'); }
        await saveTransitAccountProfile(accountClient, owner, profile);
        if ((await current()) && local?.supported && authorized) await agentAction({ action: 'import', profile });
      }, 'Существующий доступ добавлен. Его можно использовать на ваших устройствах.');
    });
    const importButton = actionButton(saved ? 'Заменить сохранённый профиль' : 'Добавить существующий профиль', () => file.click());
    importButton.disabled ||= state?.running === true;
    actionButton('Обновить состояние', () => void perform(async () => {}, 'Состояние обновлено.'));
    body.append(actions, file);
    body.append(node('p', 'Установите sing-box, добавьте локальный профиль из файла transit.json и включите его. Для скачивания профиля агент не требуется. Бесплатный доступ работает по доступной мощности сети.', 'transit-client-help'));
    const clients = node('a', 'Установить sing-box'); clients.href = 'https://sing-box.sagernet.org/clients/'; clients.target = '_blank'; clients.rel = 'noopener noreferrer'; body.append(clients);
    body.append(node('p', 'Файл содержит ваш доступ к интернету. Соты хранят его в зашифрованном хранилище вашего профиля.', 'transit-client-help'));
    if (options.openSharedInternet) {
      const sharing = node('button', 'Общий интернет через компьютер', 'sw-button'); sharing.type = 'button';
      sharing.addEventListener('click', () => { dialog.close(); options.openSharedInternet?.(); }); body.append(sharing);
    }
    dialog.body.replaceChildren(body);
  }
  dialog.body.textContent = 'Проверяем доступ…';
  try { await refresh(); }
  catch (error) { notice.textContent = errorMessage(error); render(); }
  if (dialog.element.isConnected && !mobile) refreshTimer = setInterval(() => {
    if (!working && document.visibilityState === 'visible') void refresh().catch(() => undefined);
  }, 15_000);
}
