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
interface TransitUsage { uploadBytes: string; downloadBytes: string; freshEntries: number; expectedEntries: number; }

function formatTraffic(bytes: string): string {
  const units = ['Б', 'КиБ', 'МиБ', 'ГиБ', 'ТиБ', 'ПиБ', 'ЭиБ'];
  let value = Number(bytes), index = 0;
  while (value >= 1024 && index < units.length - 1) { value /= 1024; index++; }
  return new Intl.NumberFormat('ru', { maximumFractionDigits: index ? 1 : 0 }).format(value) + ' ' + units[index];
}

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
  const portable = structuredClone(profile) as {
    inbounds?: Record<string, unknown>[];
    route?: { default_domain_resolver?: string | { server?: string }; rules?: Record<string, unknown>[] };
  };
  for (const inbound of portable.inbounds || []) {
    delete inbound.interface_name;
    // Portable downloads must also start in clients built without gVisor.
    if (inbound.type === 'tun') inbound.stack = 'system';
  }
  const route = portable.route;
  const resolver = route?.default_domain_resolver;
  const resolverTag = typeof resolver === 'string' ? resolver : resolver?.server;
  if (typeof resolverTag === 'string' && resolverTag && Array.isArray(route?.rules)
    && !route.rules.some(rule => rule.action === 'resolve' && rule.network === 'udp')) {
    // Resolve through Transit before URLTest's UDP adapter and the IP deny rules.
    // Pinned cores otherwise lose unresolved UDP destinations in outbound groups.
    const deny = route.rules.findIndex(rule => rule.action === 'reject' && rule.network === 'udp');
    route.rules.splice(deny < 0 ? route.rules.length : deny, 0,
      { network: 'udp', action: 'resolve', server: resolverTag, strategy: 'ipv4_only' });
  }
  const url = URL.createObjectURL(new Blob([`${JSON.stringify(portable, null, 2)}\n`], { type: 'application/json' }));
  const link = document.createElement('a'); link.href = url; link.download = 'transit.json';
  document.body.append(link); link.click(); link.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 10_000);
}

export async function openTransitClient(options: { openSharedInternet?: () => void } = {}): Promise<void> {
  let unobserve = () => {};
  let refreshTimer: ReturnType<typeof setInterval> | undefined;
  let generation = 0, disposed = false, subscriptionUrl = '';
  const dialog = createDialog('TRANSIT', () => {
    disposed = true; generation++; subscriptionUrl = ''; unobserve(); clearInterval(refreshTimer);
  });
  dialog.element.classList.add('transit-client-dialog');
  const selectedAccount = (await accountClient.getLocalState()).accountId;
  if (disposed || !dialog.element.isConnected) return;
  if (!selectedAccount) { dialog.body.textContent = 'Откройте свой профиль Сот, чтобы получить доступ TRANSIT.'; return; }
  const owner: string = selectedAccount;
  unobserve = observeAccount(state => { if (state.accountId !== owner) dialog.close(); });
  const mobile = /android|iphone|ipad|ipod|mobile/iu.test(`${navigator.userAgent} ${navigator.platform}`);
  let saved: unknown = null, local: TransitReply | null = null, working = false, issuingEnabled = false;
  let usage: TransitUsage | null = null, usageFailed = false, usageRequest = 0;
  let subscriptionEnabled = false, subscriptionRevoked = false;
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
  async function refreshUsage(enabled: boolean): Promise<void> {
    const request = ++usageRequest;
    if (!enabled) { usage = null; usageFailed = false; return; }
    const ticket = generation;
    const result = await accountClient.extension('transit.access.usage', { expectedAccountId: owner }, { expectedAccountId: owner })
      .catch(() => null);
    if (!(await current(ticket)) || request !== usageRequest) return;
    usageFailed = result === null;
    if (result) usage = result.usage as TransitUsage | null;
    render();
  }
  async function refresh(): Promise<void> {
    const [account, agent, companion, access] = await Promise.all([
      loadTransitAccountState(accountClient, owner),
      mobile ? Promise.resolve(null) : localTransit().catch(() => null),
      mobile ? Promise.resolve(null) : checkLocalCompanionAgent(2000).catch(() => null),
      accountClient.extension('transit.access.status', { expectedAccountId: owner }, { expectedAccountId: owner })
        .catch(() => ({ enabled: false, usageEnabled: false })),
    ]);
    if (!(await current())) return;
    saved = account.profile; local = agent; issuingEnabled = access.enabled === true;
    subscriptionEnabled = access.subscriptionEnabled === true;
    if (!subscriptionEnabled) subscriptionUrl = '';
    hostDeviceId = agent?.deviceId || companion?.deviceId || hostDeviceId;
    controlToken = account.controllers[hostDeviceId] || ''; authorized = false;
    if (agent?.deviceId && controlToken) {
      authorized = await ownerPost({ action: 'authorize', deviceId: agent.deviceId, controlToken }).then(() => true, () => false);
    }
    if (await current()) {
      void refreshUsage(access.usageEnabled === true).catch(() => undefined);
      render();
    }
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
  async function requestIssuedProfile(): Promise<unknown> {
    const ticket = generation;
    const result = await accountClient.extension('transit.access.profile', { expectedAccountId: owner }, { expectedAccountId: owner });
    return await current(ticket) ? normalizeTransitClientProfile(result.profile) : null;
  }
  async function requestSubscription(action = ''): Promise<void> {
    const ticket = generation;
    const result = await accountClient.extension(`transit.access.subscription${action}`, { expectedAccountId: owner }, { expectedAccountId: owner });
    if (!(await current(ticket))) return;
    const subscription = result.subscription as { url?: unknown; revoked?: unknown };
    // This capability is kept in the open dialog only, outside profile storage.
    if (!subscription || typeof subscription.revoked !== 'boolean'
      || (!subscription.revoked && (typeof subscription.url !== 'string'
        || !/^https:\/\/[^/?#]+\/transit\/subscriptions\/[A-Za-z0-9_-]{43}\/profile\.json$/u.test(subscription.url)))) throw new Error('transit_access_unavailable');
    subscriptionUrl = subscription.revoked ? '' : subscription.url as string;
    subscriptionRevoked = subscription.revoked;
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
    status.append(node('strong', saved ? 'Личный профиль готов' : 'Получите личный профиль'));
    if (!mobile && state && (state.configured || state.running || state.phase !== 'off')) {
      status.append(node('span', state.active ? 'Включён на этом компьютере'
        : state.running || state.phase === 'unknown' || state.phase === 'error' ? 'Подключение требует проверки' : 'Выключен на этом компьютере'));
      status.append(node('span', `sing-box ${state.version} · выходов: ${state.entryCount}`));
    }
    if (state?.active && state.availableEntryCount && state.availableEntryCount < state.entryCount)
      status.append(node('span', `Проверку прошли ${state.availableEntryCount} из ${state.entryCount} выходов. Остальные временно исключены.`));
    if (usage) {
      const traffic = node('span', `Трафик выданного профиля · Получено ${formatTraffic(usage.downloadBytes)} · Отправлено ${formatTraffic(usage.uploadBytes)}`);
      traffic.title = `Получено ${usage.downloadBytes} байт · Отправлено ${usage.uploadBytes} байт`;
      status.append(traffic);
      if (usageFailed || usage.freshEntries < usage.expectedEntries)
        status.append(node('span', 'Данные о трафике обновляются.'));
    } else if (usageFailed && saved) {
      status.append(node('span', 'Данные о трафике сейчас недоступны.'));
    }
    body.append(status, notice);
    const actions = node('div', '', 'transit-client-actions');
    const settings = node('details', '', 'transit-client-settings');
    settings.append(node('summary', 'Управление ссылкой и запасной файл'));
    const extra = node('div', '', 'transit-client-actions'); settings.append(extra);
    const actionButton = (label: string, action: () => void, primary = false, container = actions) => {
      const button = node('button', label, `sw-button${primary ? ' primary' : ''}`); button.type = 'button'; button.disabled = working;
      button.addEventListener('click', action); container.append(button); return button;
    };
    if (!saved && issuingEnabled) actionButton('Получить профиль sing-box', () => void perform(async () => {
      const profile = await requestIssuedProfile();
      if (profile) {
        await saveTransitAccountProfile(accountClient, owner, profile);
        if (subscriptionEnabled && await current()) await requestSubscription();
      }
    }, subscriptionEnabled ? 'Личный профиль готов. Добавьте его в sing-box по ссылке.' : 'Личный профиль готов. Скачайте его и добавьте в sing-box.'), true);
    if (saved && subscriptionEnabled && !subscriptionUrl) actionButton(subscriptionRevoked ? 'Создать новую ссылку sing-box' : 'Получить ссылку sing-box',
      () => void perform(() => requestSubscription(subscriptionRevoked ? '.rotate' : ''), 'Ссылка готова. Добавьте её в sing-box.'), true);
    if (subscriptionUrl) {
      const add = node('a', 'Добавить в sing-box', 'sw-button primary');
      add.href = `sing-box://import-remote-profile?url=${encodeURIComponent(subscriptionUrl)}#TRANSIT`;
      add.rel = 'noreferrer'; add.setAttribute('referrerpolicy', 'no-referrer');
      actions.append(add);
      actionButton('Скопировать ссылку', () => void perform(async () => {
        if (await current()) await navigator.clipboard.writeText(subscriptionUrl);
      }, 'Ссылка скопирована. В sing-box выберите удалённый профиль (Remote) и вставьте её.'));
      actionButton('Заменить ссылку', () => void perform(() => requestSubscription('.rotate'), 'Новая ссылка готова. Замените её в sing-box на своих устройствах.'), false, extra);
      actionButton('Отключить ссылку обновления', () => void perform(() => requestSubscription('.revoke'), 'Обновление по этой ссылке отключено. Уже скачанные профили продолжают работать.'), false, extra);
    }
    if (saved) actionButton('Скачать профиль sing-box', () => downloadProfile(saved), !subscriptionEnabled);
    if (saved && issuingEnabled) actionButton('Скачать актуальный профиль Transit', () => void perform(async () => {
      const profile = await requestIssuedProfile();
      if (profile) downloadProfile(profile);
    }, 'Добавьте скачанный файл в sing-box.'), false, extra);
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
      body.append(node('p', local ? 'Чтобы включать VPN на этом компьютере через агент Сот, разрешите управление. Для приложения sing-box достаточно скачать профиль.'
        : 'Для управления VPN через Соты установите агент. Скачать профиль и подключиться в sing-box можно без него.'));
      actionButton(local ? 'Разрешить управление этим компьютером' : 'Обновить агент Сот', () => void perform(pairComputer, 'Запустите скачанный установщик, затем обновите состояние. Подключение включается отдельной кнопкой.'), !issuingEnabled && !saved);
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
    if ((saved && issuingEnabled) || subscriptionUrl) body.append(settings);
    body.append(node('p', subscriptionEnabled
      ? 'Добавьте ссылку как удалённый профиль (Remote) и включите автоматическое обновление в sing-box. Если кнопка не открывает приложение, скопируйте ссылку. Агент Сот не требуется.'
      : 'Установите sing-box, добавьте локальный профиль из файла transit.json и включите его. Для скачивания профиля агент не требуется.', 'transit-client-help'));
    if (subscriptionEnabled) body.append(node('p', 'Ранее добавленный локальный файл сам не обновится. Один раз добавьте удалённый профиль по ссылке: далее sing-box будет получать изменения сети автоматически. Файл остаётся запасным способом подключения.', 'transit-client-help'));
    const clients = node('a', 'Установить sing-box'); clients.href = 'https://sing-box.sagernet.org/clients/'; clients.target = '_blank'; clients.rel = 'noopener noreferrer'; body.append(clients);
    body.append(node('p', 'Файл содержит ваш доступ к интернету. Соты хранят его в зашифрованном хранилище вашего профиля.', 'transit-client-help'));
    if (subscriptionEnabled) body.append(node('p', 'Ссылка тоже даёт доступ к вашему профилю: передавайте её только своим устройствам. Замена или отключение ссылки прекращает обновления по старой ссылке, но не отзывает уже скачанный доступ.', 'transit-client-help'));
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
