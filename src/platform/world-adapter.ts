import { accountClient, observeAccount } from '../core/connect-client';
import { mountWorldApp, type WorldAppHandle } from '../world/app';
import type { WorldAppOptions, WorldProfile, WorldHumanCallPort, WorldHumanCallState } from '../world/types';
import { createAppActions } from './local-apps';
import { mountAssistant } from './assistant';
import { bindCallIdentity, type CallIdentityLifecycle } from './call-identity';
import { createHumanCallPorts } from '../sync';
// The existing browser-neutral JS owner is tested directly; its runtime is unchanged.
// @ts-expect-error The reviewed JS component has no TypeScript declaration.
import { createCallLifecycle } from '../../modules/personal-agent/call/lifecycle.mjs';

interface HumanLifecycle extends CallIdentityLifecycle {
  snapshot(): { state: string; microphone: WorldHumanCallState['microphone'];
    scope: { accountId: string; roomId: string; mode: 'one-to-one' | 'group' } | null; code: string | null };
  join(target: { roomId: string; mode: 'one-to-one' | 'group' }): Promise<{ ok: boolean; code?: string }>;
  enableMicrophone(): Promise<{ ok: boolean; code?: string }>;
  mute(): unknown;
}

export async function startWorld(root: HTMLElement, dependencies: { callLifecycle?: CallIdentityLifecycle } = {}): Promise<void> {
  const callObservers = new Set<() => void>();
  const notifyCall = () => { for (const listener of [...callObservers]) { try { void Promise.resolve(listener()).catch(() => {}); } catch {} } };
  let ownedLifecycle: HumanLifecycle | undefined;
  const callPorts = dependencies.callLifecycle ? undefined : createHumanCallPorts({ client: accountClient,
    onInterrupted: reason => { if (reason === 'signaling') ownedLifecycle?.end(); else ownedLifecycle?.mute(); } });
  if (callPorts) ownedLifecycle = createCallLifecycle({ ...callPorts, onState: notifyCall }) as HumanLifecycle;
  const lifecycle = dependencies.callLifecycle ?? ownedLifecycle!;
  const unobserveCall = callPorts?.subscribe(notifyCall);
  let detachIdentity: (() => void) | undefined;
  const detachCall = () => { detachIdentity?.(); unobserveCall?.(); callObservers.clear(); };
  try {
  detachIdentity = bindCallIdentity({ lifecycle, client: accountClient, observeAccount, pageEvents: window });
  // An existing durable identity can open local drafts before the network returns.
  // Every server operation still verifies the active installation through Connect.
  if (!(await accountClient.getLocalState()).accountId) await accountClient.bootstrap('Мои соты');
  let activeAccount = (await accountClient.getLocalState()).accountId;
  let world: WorldAppHandle;
  let mountGeneration = 0, currentCallView: WorldHumanCallPort | undefined;
  const refresh = async () => { await world?.refresh(); };
  const actions = createAppActions(accountClient, refresh, () => openAccount('devices'));
  const openLegacy = (tool?: 'notes' | 'files' | 'chess' | 'terminal' | 'internet') => {
    const url = new URL(window.location.href); url.search = ''; url.hash = ''; url.searchParams.set('view', 'classic');
    if (tool) url.searchParams.set('tool', tool);
    window.location.assign(url.href);
  };
  const openAccount = async (initialTab: 'profile' | 'people' | 'devices' | 'recovery' = 'profile') => {
    const callView = currentCallView;
    const [{ openConnectPanel, parseConnectLink }, { default: QRCode }] = await Promise.all([
      import('../../modules/connect/ui/index.mjs'), import('qrcode'), import('../../modules/connect/ui/style.css'),
    ]);
    const state = await accountClient.getLocalState();
    const panel = openConnectPanel({ client: accountClient, label: state.label || 'Мой профиль', productName: 'Соты',
      qr: url => QRCode.toDataURL(url, { width: 256, margin: 2, color: { dark: '#252923', light: '#ffffff' } }),
      initialIntent: parseConnectLink(window.location.href), initialTab,
      ...(callView ? { onContactCall: async ({ peerAccountId }: { peerAccountId: string; label: string }) => {
        const result = await callView.join({ roomId: peerAccountId, mode: 'one-to-one' });
        if (!result.ok) throw Object.assign(new Error('Call did not start.'), { code: result.code });
      } } : {}),
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
  const options: Omit<WorldAppOptions, 'api' | 'requestContact'> = {
    localAccount: async () => { const state = await accountClient.getLocalState(); return { accountId: state.accountId ?? null, label: state.label || 'Я' }; },
    openLegacy, openAccount, connectDevice: actions.connectDevice, agentCreate: actions.agentCreate,
    appSlotCleanup:reply=>accountClient.appSlotCleanup(reply),
    openAppBuilder: (host: HTMLElement) => actions.mountAppBuilder(host),
    openAssistant: (host: HTMLElement) => mountAssistant(host, { client: accountClient, connectDevice: actions.connectDevice,
      createApp: () => actions.agentCreate(), resumeApp: target => actions.agentCreate(undefined, target) }),
    accessAvailability: async () => {
      const response = await fetch('/api/capabilities/v1/status', { cache: 'no-store', signal: AbortSignal.timeout(8000) });
      if (!response.ok) throw new TypeError('Capabilities unavailable');
      const value = await response.json() as { notesCreateEnabled?: boolean; audience?: string | null };
      return { notesCreateEnabled: value.notesCreateEnabled === true, audience: typeof value.audience === 'string' ? value.audience : null };
    },
  };
  const mount = (accountId: string | null): WorldAppHandle => {
    const generation = ++mountGeneration;
    const current = () => Boolean(accountId && accountId === activeAccount && generation === mountGeneration);
    const stale = () => ({ ok: false, code: 'ACTIVE_PROFILE_CHANGED' });
    let humanCalls: WorldHumanCallPort | undefined;
    if (ownedLifecycle && callPorts) {
      const owner = ownedLifecycle, ports = callPorts;
      const belongs = () => current() && owner.snapshot().scope?.accountId === accountId;
      const snapshot = (): WorldHumanCallState => {
        const local = owner.snapshot(), connection = ports.snapshot();
        if (!current()) return { state: 'ended', microphone: 'off', connection: 'closed', peers: 0,
          playbackBlocked: false, roomId: null, mode: null, code: 'ACTIVE_PROFILE_CHANGED', turnConfigured: false };
        return { state: local.state, microphone: local.microphone, ...connection,
          roomId: local.scope?.roomId ?? null, mode: local.scope?.mode ?? null, code: local.code };
      };
      humanCalls = {
        snapshot,
        subscribe(listener) { const notify = () => listener(snapshot()); callObservers.add(notify); return () => callObservers.delete(notify); },
        join(target) { return current() ? owner.join(target) : Promise.resolve(stale()); },
        enableMicrophone() { return belongs() ? owner.enableMicrophone() : Promise.resolve(stale()); },
        disableMicrophone() { if (belongs()) owner.mute(); },
        end() { if (belongs()) owner.end(); },
        async resumeListening() { if (belongs()) await ports.resumeListening(); },
      };
    }
    currentCallView = humanCalls;
    // Each controller keeps its original admission context. An old callback
    // cannot adopt a new account merely because the shared selection changed.
    const api = { request: <T>(method: string, args?: Record<string, unknown>): Promise<T> => {
      if (!accountId) return Promise.reject(Object.assign(new Error('No local account is selected.'), { code: 'NO_LOCAL_PROFILE' }));
      return accountClient.extension<T>(method, args ?? {}, { expectedAccountId: accountId });
    } };
    return mountWorldApp(root, { ...options, api, ...(humanCalls ? { humanCalls } : {}),
      requestContact: async (profile: WorldProfile) => { await api.request('contacts.requestAccount', { accountId: profile.profileId }); },
    });
  };
  world = mount(activeAccount);
  const unobserve = observeAccount(state => {
    if (state.accountId !== activeAccount) {
      activeAccount = state.accountId; actions.resetAccount(); world.destroy(); world = mount(activeAccount);
    }
  });
  // A back/forward-cache entry is frozen by the browser and resumes with its DOM.
  // Destroying that DOM on pagehide would restore an empty screen on Back.
  window.addEventListener('pagehide', event => {
    if (event.persisted) return;
    mountGeneration++; unobserve(); actions.destroy(); world.destroy(); detachCall();
  });
  if (new URL(window.location.href).searchParams.has('connect')) await openAccount();
  const actionUrl = new URL(window.location.href);
  if (actionUrl.searchParams.get('action') === 'connect-device') {
    actionUrl.searchParams.delete('action'); history.replaceState({}, '', actionUrl);
    await actions.connectDevice();
  }
  } catch (error) { try { detachCall(); } catch {} try { lifecycle.dispose(); } catch {} throw error; }
}
