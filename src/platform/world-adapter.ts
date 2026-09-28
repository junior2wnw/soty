import { accountClient, observeAccount } from '../core/connect-client';
import { mountWorldApp, type WorldAppHandle } from '../world/app';
import type { WorldProfile } from '../world/types';
import { createAppActions } from './local-apps';

export async function startWorld(root: HTMLElement): Promise<void> {
  // An existing durable identity can open local drafts before the network returns.
  // Every server operation still verifies the active installation through Connect.
  if (!(await accountClient.getLocalState()).accountId) await accountClient.bootstrap('Мои соты');
  let activeAccount = (await accountClient.getLocalState()).accountId;
  let world: WorldAppHandle;
  const refresh = async () => { await world?.refresh(); };
  const actions = createAppActions(accountClient, refresh, () => openAccount('devices'));
  const openLegacy = (tool?: 'notes' | 'files' | 'chess' | 'terminal' | 'internet') => {
    const url = new URL(window.location.href); url.search = ''; url.hash = ''; url.searchParams.set('view', 'classic');
    if (tool) url.searchParams.set('tool', tool);
    window.location.assign(url.href);
  };
  const openAccount = async (initialTab: 'profile' | 'people' | 'devices' | 'recovery' = 'profile') => {
    const [{ openConnectPanel, parseConnectLink }, { default: QRCode }] = await Promise.all([
      import('../../modules/connect/ui/index.mjs'), import('qrcode'), import('../../modules/connect/ui/style.css'),
    ]);
    const state = await accountClient.getLocalState();
    const panel = openConnectPanel({ client: accountClient, label: state.label || 'Мой профиль', productName: 'Соты',
      qr: url => QRCode.toDataURL(url, { width: 256, margin: 2, color: { dark: '#252923', light: '#ffffff' } }),
      initialIntent: parseConnectLink(window.location.href), initialTab,
      snapshotDescription: 'Профиль и сообщества сохранены в Сотах. Сохранение прежних комнат доступно в разделе «Комнаты и инструменты».',
      onRename: async displayName => {
        const result = await accountClient.extension<{ profile: WorldProfile }>('world.profile.get');
        await accountClient.extension('world.profile.update', { expectedRevision: result.profile.revision, displayName });
      },
    });
    const dialog = document.querySelector<HTMLDialogElement>('dialog.connect-panel');
    if (dialog) await new Promise<void>(resolve => dialog.addEventListener('close', () => resolve(), { once: true }));
    else panel.close();
    await refresh();
  };
  const options = { api: { request: <T>(method: string, args?: Record<string, unknown>) => accountClient.extension<T>(method, args ?? {}) },
    localAccount: async () => { const state = await accountClient.getLocalState(); return { accountId: state.accountId ?? null, label: state.label || 'Я' }; },
    openLegacy, openAccount, connectDevice: actions.connectDevice, agentCreate: actions.agentCreate,
    requestContact: async (profile: WorldProfile) => { await accountClient.extension('contacts.requestAccount', { accountId: profile.profileId }); },
  };
  world = mountWorldApp(root, options);
  const unobserve = observeAccount(state => {
    if (state.accountId && state.accountId !== activeAccount) {
      activeAccount = state.accountId; actions.destroy(); world.destroy(); world = mountWorldApp(root, options);
    }
  });
  // A back/forward-cache entry is frozen by the browser and resumes with its DOM.
  // Destroying that DOM on pagehide would restore an empty screen on Back.
  window.addEventListener('pagehide', event => {
    if (event.persisted) return;
    unobserve(); actions.destroy(); world.destroy();
  });
  if (new URL(window.location.href).searchParams.has('connect')) await openAccount();
  const actionUrl = new URL(window.location.href);
  if (actionUrl.searchParams.get('action') === 'connect-device') {
    actionUrl.searchParams.delete('action'); history.replaceState({}, '', actionUrl);
    await actions.connectDevice();
  }
}
