import { accountClient, observeAccount } from '../core/connect-client';
import { mountWorldApp, type WorldAppHandle } from '../world/app';
import type { WorldAppOptions, WorldIdentity, WorldHumanCallPort, WorldHumanCallState, WorldIncomingCall } from '../world/types';
import { createAppActions } from './local-apps';
import { mountAssistant } from './assistant';
import { bindCallIdentity, type CallIdentityLifecycle } from './call-identity';
import { createHumanCallPorts } from '../sync';
import { ensureConnectionAccount, openConnectionHub, type ConnectionHandoff } from './connection-hub';
import { parseConnectionIntent, type ConnectionIntent } from './connection-intent.mjs';
import { takeConnectionNavigation } from './connection-navigation.mjs';
import { createNativeVoiceDescriptor } from './native-voice-descriptor.mjs';
import { createNotificationsController, type NotificationsController } from './notifications-client.mjs';
import type { ConnectIntent, ConnectTab } from '../../modules/connect/ui/index.mjs';
const startupReadOperations = new Set(['world.profile.get', 'world.community.list', 'world.field.get', 'world.directory.resolve',
  'apps.directory.resolve', 'apps.directory.search', 'apps.saved.list', 'apps.list', 'apps.discussion.inbox']);
// The existing browser-neutral JS owner is tested directly; its runtime is unchanged.
// @ts-expect-error The reviewed JS component has no TypeScript declaration.
import { createCallLifecycle } from '../../modules/personal-agent/call/lifecycle.mjs';

interface HumanLifecycle extends CallIdentityLifecycle {
  snapshot(): { state: string; microphone: WorldHumanCallState['microphone'];
    capacity: { occupied: number; limit: number };
    scope: { accountId: string; roomId: string; mode: 'one-to-one' | 'group'; generation: number;
      expectedCallId?: string; expectedEpoch?: number } | null; code: string | null };
  join(target: { roomId: string; mode: 'one-to-one' | 'group'; expectedCallId?: string; expectedEpoch?: number }): Promise<{ ok: boolean; code?: string }>;
  enableMicrophone(): Promise<{ ok: boolean; code?: string }>;
  mute(): unknown;
}

export async function startWorld(root: HTMLElement, dependencies: { callLifecycle?: CallIdentityLifecycle } = {}): Promise<void> {
  const callObservers = new Set<() => void>();
  const notifyCall = () => { for (const listener of [...callObservers]) { try { void Promise.resolve(listener()).catch(() => {}); } catch {} } };
  let ownedLifecycle: HumanLifecycle | undefined;
  let orbCaptureOwner: symbol | null = null;
  const acquireVoiceCapture = (): (() => void) | null => {
    const current = ownedLifecycle?.snapshot();
    if (orbCaptureOwner || !current || !['idle', 'ended'].includes(current.state) || current.capacity.occupied) return null;
    const owner = Symbol('voice-orb-capture'); orbCaptureOwner = owner;
    return () => { if (orbCaptureOwner === owner) orbCaptureOwner = null; };
  };
  const callPorts = dependencies.callLifecycle ? undefined : createHumanCallPorts({ client: accountClient,
    onInterrupted: reason => { if (reason === 'signaling') ownedLifecycle?.end(); else ownedLifecycle?.mute(); } });
  if (callPorts) ownedLifecycle = createCallLifecycle({ ...callPorts, onState: notifyCall }) as HumanLifecycle;
  const lifecycle = dependencies.callLifecycle ?? ownedLifecycle!;
  const unobserveCall = callPorts?.subscribe(notifyCall);
  let detachIdentity: (() => void) | undefined;
  const detachCall = () => { detachIdentity?.(); unobserveCall?.(); callObservers.clear(); };
  const pageLifetime = new AbortController(), entryInput = window.location.href;
  let world: WorldAppHandle, activeAccount: string | null = null, activeDevice: string | null = null;
  let mountGeneration = 0, currentCallView: WorldHumanCallPort | undefined;
  let identityEvents = 0, pageClosed = false, unobserveAccount: (() => void) | undefined, destroyActions: (() => void) | undefined;
  let restoreOwnedRoute: (() => void) | undefined, worldMounted = false, entryScopeChanged = false;
  let notifications: NotificationsController | undefined;
  const closePage = () => {
    if (pageClosed) return;
    pageClosed = true; pageLifetime.abort(); mountGeneration++;
    window.removeEventListener('pagehide', pagehide);
    for (const cleanup of [unobserveAccount, destroyActions, () => notifications?.dispose(), () => world?.destroy(), detachCall, () => lifecycle.dispose()]) {
      try { cleanup?.(); } catch { /* Retire the remaining owners too. */ }
    }
  };
  const pagehide = (event: PageTransitionEvent) => { if (!event.persisted) closePage(); };
  window.addEventListener('pagehide', pagehide);
  try {
  detachIdentity = bindCallIdentity({ lifecycle, client: accountClient, observeAccount, pageEvents: window });
  const entry = await ensureConnectionAccount(root, { client: accountClient, origin: window.location.origin,
    isCurrent: () => root.isConnected && !pageLifetime.signal.aborted, signal: pageLifetime.signal,
    observeAccount: listener => observeAccount(() => listener()), initialInput: entryInput,
    onIntent: (intent, handoff) => {
      if (intent.kind !== 'platform-invitation') throw Error('connection_route_unavailable');
      window.location.assign(takeConnectionNavigation(intent, window.location.origin, handoff));
    },
  });
  if (pageLifetime.signal.aborted || !root.isConnected || !entry.state.accountId || !entry.state.deviceId) { closePage(); return; }
  activeAccount = entry.state.accountId; activeDevice = entry.state.deviceId;
  const parseHostEntry = (raw: string): { connection: ConnectionIntent | null; rejectedTyped: boolean } => {
    const connection = parseConnectionIntent(raw, window.location.origin);
    if (connection) return { connection, rejectedTyped: false };
    let url: URL;
    try { url = new URL(raw, window.location.origin); } catch { return { connection: null, rejectedTyped: false }; }
    if (!/^#(?:profile|community|app|launch)(?:[/?]|$)/u.test(url.hash)) return { connection: null, rejectedTyped: false };
    const typed = parseConnectionIntent(url.hash, window.location.origin);
    const allowed = typed && ['profile', 'community', 'application'].includes(typed.kind);
    // Host compatibility only: never relax the closed QR parser or grant carrier.
    if (allowed && url.origin === window.location.origin && url.pathname === '/' && !url.username && !url.password && url.search === '?pwa=1')
      return { connection: typed, rejectedTyped: false };
    return { connection: null, rejectedTyped: true };
  };
  const hostEntry = parseHostEntry(entryInput);
  if (!entry.intent && hostEntry.rejectedTyped && window.location.href === entryInput)
    throw Object.assign(Error('The connection link contains unsupported parameters'), { code: 'connection_route_invalid' });
  const offeredConnection = entry.intent ?? hostEntry.connection;
  let ownedConnection = offeredConnection && ['profile', 'community', 'application'].includes(offeredConnection.kind) ? offeredConnection : null;
  let ownedRoute: string | null = null;
  // A typed cold continuation owns its destination. A normal deep link owns
  // only the original URL; Back/Forward to another route is a newer intent.
  if (ownedConnection && !entry.intent && window.location.href !== entryInput) ownedConnection = null;
  if (ownedConnection || entry.intent && hostEntry.rejectedTyped && window.location.href === entryInput) {
    const source = new URL(window.location.href);
    if (source.origin !== window.location.origin || source.pathname !== '/') throw Error('connection_entry_changed');
    const neutral = new URL('/', window.location.origin); neutral.hash = 'mine'; ownedRoute = neutral.href;
    window.history.replaceState({ soty: true }, '', ownedRoute);
    restoreOwnedRoute = () => {
      if (!worldMounted && !entryScopeChanged && !pageClosed && root.isConnected && !pageLifetime.signal.aborted
        && activeAccount === entry.state.accountId && activeDevice === entry.state.deviceId && window.location.href === ownedRoute)
        window.history.replaceState({ soty: true }, '', entryInput);
    };
  }
  // Subscribe before optional IO. This observer has no replay; a durable read
  // below must also reject any older snapshot crossed by a committed event.
  const removeAccountObserver = observeAccount(state => {
    if (pageClosed) return;
    identityEvents++;
    const nextAccount = state.current?.revoked ? null : state.accountId, nextDevice = nextAccount ? state.deviceId : null;
    if (nextAccount === activeAccount && nextDevice === activeDevice) { void notifications?.refresh(); return; }
    notifications?.dispose({ accountChanged: true }); notifications = undefined;
    entryScopeChanged = true; activeAccount = nextAccount; activeDevice = nextDevice;
    if (world) { try { actions.resetAccount(); world.destroy(); world = mount(activeAccount); } catch (error) { closePage(); throw error; } }
  });
  if (pageClosed) { removeAccountObserver(); return; }
  unobserveAccount = removeAccountObserver;
  const readStartupIdentity = async (): Promise<boolean> => {
    for (let attempt = 0; attempt < 3; attempt++) {
      if (pageClosed || !root.isConnected) { closePage(); return false; }
      const ticket = identityEvents, state = await accountClient.getLocalState();
      if (pageClosed || !root.isConnected) { closePage(); return false; }
      if (ticket !== identityEvents) continue;
      if (state.current?.revoked) throw Object.assign(Error('Device revoked'), { code: 'DEVICE_REVOKED' });
      if (!state.accountId || !state.deviceId) throw Object.assign(Error('No local account'), { code: 'ACTIVE_PROFILE_CHANGED' });
      if (activeAccount !== state.accountId || activeDevice !== state.deviceId) entryScopeChanged = true;
      activeAccount = state.accountId; activeDevice = state.deviceId; return true;
    }
    throw Object.assign(Error('The local profile keeps changing'), { code: 'ACTIVE_PROFILE_CHANGED' });
  };
  const refresh = async () => { await world?.refresh(); };
  const actions = createAppActions(accountClient, refresh, () => openAccount('devices'));
  destroyActions = () => actions.destroy();
  const openLegacy = (tool?: 'notes' | 'files' | 'chess' | 'terminal' | 'internet') => {
    if (tool === 'internet') {
      void import('./transit-client').then(({ openTransitClient }) => openTransitClient({ openSharedInternet: () => {
        const shared = new URL(window.location.href); shared.search = ''; shared.hash = '';
        shared.searchParams.set('view', 'classic'); shared.searchParams.set('tool', 'internet'); window.location.assign(shared.href);
      } }));
      return;
    }
    const url = new URL(window.location.href); url.search = ''; url.hash = ''; url.searchParams.set('view', 'classic');
    if (tool) url.searchParams.set('tool', tool);
    window.location.assign(url.href);
  };
  const openAccount = async (initialTab: ConnectTab = 'profile', initialIntent?: ConnectIntent) => {
    const callView = currentCallView;
    const mountedWorld = world, accountAtOpen = activeAccount, generationAtOpen = mountGeneration, routeAtOpen = window.location.href;
    let editProfileRequested = false;
    const current = () => !pageLifetime.signal.aborted && world === mountedWorld && activeAccount === accountAtOpen && mountGeneration === generationAtOpen && window.location.href === routeAtOpen;
    const [{ openConnectPanel }, { default: QRCode }] = await Promise.all([
      import('../../modules/connect/ui/index.mjs'), import('qrcode'), import('../../modules/connect/ui/style.css'),
    ]);
    const state = await accountClient.getLocalState();
    if (!current() || state.accountId !== accountAtOpen) return;
    const panel = openConnectPanel({ client: accountClient, label: state.label || 'Мой профиль', productName: 'Соты',
      qr: url => QRCode.toDataURL(url, { width: 256, margin: 2, color: { dark: '#252923', light: '#ffffff' } }),
      ...(initialIntent ? { initialIntent } : {}), initialTab, bootstrapOnOpen: false,
      onEditProfile: () => { editProfileRequested = true; },
      onOpenDataTools: () => openLegacy(),
      ...(callView ? { onContactCall: async ({ peerAccountId, label }: { peerAccountId: string; label: string }) => {
        const result = await callView.join({ roomId: peerAccountId, mode: 'one-to-one', displayLabel: label });
        if (!result.ok) throw Object.assign(new Error('Call did not start.'), { code: result.code });
      } } : {}),
      snapshotDescription: 'Профиль и сообщества сохранены в Сотах. Сохранение прежних комнат доступно в разделе «Комнаты и инструменты».',
    });
    const stopObserving = observeAccount(() => { if (!current()) panel.close(); });
    const aborted = () => panel.close(); pageLifetime.signal.addEventListener('abort', aborted, { once: true });
    const routeChanged = () => { if (!current()) panel.close(); };
    window.addEventListener('hashchange', routeChanged); window.addEventListener('popstate', routeChanged);
    const dialog = document.querySelector<HTMLDialogElement>('dialog.connect-panel');
    try { if (dialog) await new Promise<void>(resolve => dialog.addEventListener('close', () => resolve(), { once: true }));
      else panel.close(); }
    finally { stopObserving(); pageLifetime.signal.removeEventListener('abort', aborted);
      window.removeEventListener('hashchange', routeChanged); window.removeEventListener('popstate', routeChanged); }
    if (!current()) return;
    await refresh();
    if (editProfileRequested && world === mountedWorld && activeAccount === accountAtOpen && mountGeneration === generationAtOpen) mountedWorld.editProfile();
  };
  let connectionOpening = false;
  const openConnections = async (initialInput?: string): Promise<void> => {
    if (connectionOpening) return;
    const mounted = world, accountId = activeAccount, generation = mountGeneration, route = window.location.href;
    const state = await accountClient.getLocalState();
    const current = () => !pageLifetime.signal.aborted && mounted === world && activeAccount === accountId
      && generation === mountGeneration && window.location.href === route;
    if (!current() || state.accountId !== accountId || connectionOpening) return;
    connectionOpening = true;
    const lifetime = new AbortController();
    const closeIfStale = () => { if (!current()) lifetime.abort(); };
    const unobserve = observeAccount(closeIfStale);
    window.addEventListener('hashchange', closeIfStale); window.addEventListener('popstate', closeIfStale);
    pageLifetime.signal.addEventListener('abort', closeIfStale);
    const hub = openConnectionHub({ client: accountClient, origin: window.location.origin, isCurrent: current,
      signal: lifetime.signal, observeAccount: listener => observeAccount(() => listener()), ...(initialInput ? { initialInput } : {}),
      openAccount: (tab, intent) => openAccount(tab, intent), onConnectComputer: () => actions.connectDevice(),
      onIntent: async (intent: ConnectionIntent, handoff?: ConnectionHandoff) => {
        if (!current()) throw Error('connection_entry_changed');
        if (intent.kind === 'contact' || intent.kind === 'device') await openAccount('people', intent);
        else if (intent.kind === 'profile' || intent.kind === 'community' || intent.kind === 'application') await mounted.followConnection(intent);
        else await mounted.followConnection(intent, () => {
          if (!current()) throw Error('connection_entry_changed');
          window.location.assign(takeConnectionNavigation(intent, window.location.origin, handoff));
        });
        hub.close();
      },
    });
    try { await hub.closed; }
    finally { connectionOpening = false; lifetime.abort(); unobserve(); window.removeEventListener('hashchange', closeIfStale);
      window.removeEventListener('popstate', closeIfStale); pageLifetime.signal.removeEventListener('abort', closeIfStale); }
  };
  const voiceDescriptor = createNativeVoiceDescriptor({ fetch, signal: pageLifetime.signal });
  if (!await readStartupIdentity()) return;
  const options: Omit<WorldAppOptions, 'api' | 'requestContact'> = {
    readNativeVoice: async options => {
      const mount = await voiceDescriptor.read(options);
      return mount ? { client: accountClient, mount } : null;
    },
    localAccount: async () => { const state = await accountClient.getLocalState(); return { accountId: state.accountId ?? null, label: state.label || 'Я' }; },
    openLegacy, openAccount, openConnections, connectDevice: actions.connectDevice, agentCreate: actions.agentCreate,
    appSlotCleanup:reply=>accountClient.appSlotCleanup(reply),
    openAppBuilder: (host: HTMLElement) => actions.mountAppBuilder(host),
    openAssistant: (host: HTMLElement, context) => mountAssistant(host, { client: accountClient, connectDevice: actions.connectDevice, acquireVoiceCapture,
      createApp: () => actions.agentCreate(), resumeApp: target => actions.agentCreate(undefined, target) }, context),
    accessAvailability: async () => {
      const response = await fetch('/api/capabilities/v1/status', { cache: 'no-store', signal: AbortSignal.timeout(8000) });
      if (!response.ok) throw new TypeError('Capabilities unavailable');
      const value = await response.json() as { notesCreateEnabled?: boolean; audience?: string | null; appFeedbackEnabled?: boolean; mcpAudience?: string | null };
      return { notesCreateEnabled: value.notesCreateEnabled === true, audience: typeof value.audience === 'string' ? value.audience : null,
        appFeedbackEnabled: value.appFeedbackEnabled === true, mcpAudience: typeof value.mcpAudience === 'string' ? value.mcpAudience : null };
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
      let presentation: { accountId: string; mountGeneration: number; scopeGeneration: number; roomId: string; mode: 'one-to-one' | 'group'; label: string } | null = null;
      let incomingInvitation: WorldIncomingCall | null = null, inboxGeneration = 0;
      const cleanDisplayLabel = (value: unknown): string => typeof value === 'string'
        ? value.replace(/[\u0000-\u001f\u007f]/gu, ' ').replace(/\s+/gu, ' ').trim().slice(0, 160).trim()
        : '';
      const snapshot = (): WorldHumanCallState => {
        const local = owner.snapshot(), connection = ports.snapshot();
        if (!current()) return { state: 'ended', microphone: 'off', connection: 'closed', peers: 0,
          playbackBlocked: false, roomId: null, mode: null, code: 'ACTIVE_PROFILE_CHANGED', turnConfigured: false };
        const scope = local.scope;
        const targetLabel = scope && presentation && presentation.accountId === accountId && presentation.mountGeneration === generation
          && presentation.roomId === scope.roomId && presentation.mode === scope.mode && scope.accountId === accountId
          && presentation.scopeGeneration === scope.generation
          && (local.state === 'joining' || local.state === 'listening') ? presentation.label : null;
        return { state: local.state, microphone: local.microphone, ...connection,
          roomId: scope?.roomId ?? null, mode: scope?.mode ?? null, code: local.code, targetLabel,
          incomingCall: current() ? incomingInvitation : null };
      };
      humanCalls = {
        snapshot,
        subscribe(listener) { const notify = () => listener(snapshot()); callObservers.add(notify); return () => callObservers.delete(notify); },
        async checkIncoming() {
          if (!current() || !accountId) return null;
          const request = ++inboxGeneration;
          try {
            const response = await ports.inbox(accountId);
            if (!current() || request !== inboxGeneration) return null;
            const value = response?.invitation;
            incomingInvitation = value && /^[A-Za-z0-9_.:-]{1,160}$/u.test(value.callId)
              && Number.isSafeInteger(value.epoch) && value.epoch > 0
              && /^[A-Za-z0-9_.:-]{1,160}$/u.test(value.roomId)
              ? { callId: value.callId, epoch: value.epoch, roomId: value.roomId, label: cleanDisplayLabel(value.label) }
              : null;
            notifyCall(); return incomingInvitation;
          } catch { return null; }
        },
        cancelIncomingChecks() { inboxGeneration++; },
        async acceptIncoming(invitation) {
          if (orbCaptureOwner) return { ok: false, code: 'call_active' };
          if (!current() || !accountId || !incomingInvitation || invitation.callId !== incomingInvitation.callId
            || invitation.epoch !== incomingInvitation.epoch || invitation.roomId !== incomingInvitation.roomId)
            return stale();
          const label = cleanDisplayLabel(incomingInvitation.label), pending = owner.join({ roomId: invitation.roomId,
            mode: 'one-to-one', expectedCallId: invitation.callId, expectedEpoch: invitation.epoch });
          const startedScope = owner.snapshot().scope;
          const generationAtStart = startedScope?.accountId === accountId && startedScope.roomId === invitation.roomId
            && startedScope.mode === 'one-to-one' && startedScope.expectedCallId === invitation.callId
            && startedScope.expectedEpoch === invitation.epoch ? startedScope.generation : null;
          const result = await pending;
          const local = owner.snapshot(), scope = local.scope;
          if (result.ok && generationAtStart !== null && current() && scope?.accountId === accountId
            && scope.roomId === invitation.roomId && scope.mode === 'one-to-one' && scope.expectedCallId === invitation.callId
            && scope.expectedEpoch === invitation.epoch && scope.generation === generationAtStart
            && (local.state === 'joining' || local.state === 'listening')) {
            presentation = label ? { accountId, mountGeneration: generation, scopeGeneration: generationAtStart,
              roomId: invitation.roomId, mode: 'one-to-one', label } : null;
            if (incomingInvitation?.callId === invitation.callId && incomingInvitation.epoch === invitation.epoch) incomingInvitation = null;
            notifyCall();
          }
          return result;
        },
        async declineIncoming(invitation) {
          if (!current() || !accountId || !incomingInvitation || invitation.callId !== incomingInvitation.callId
            || invitation.epoch !== incomingInvitation.epoch || invitation.roomId !== incomingInvitation.roomId) return stale();
          try {
            const response = await ports.decline(accountId, invitation);
            if (!current()) return stale();
            const result = { ok: response.declined === true, ...(response.declined ? {} : { code: 'human_call_stale' }) };
            if (result.ok && incomingInvitation?.callId === invitation.callId && incomingInvitation.epoch === invitation.epoch) {
              incomingInvitation = null; notifyCall();
            }
            return result;
          } catch { return { ok: false, code: 'human_call_stale' }; }
        },
        join(target) {
          if (orbCaptureOwner) return Promise.resolve({ ok: false, code: 'call_active' });
          if (!current()) return Promise.resolve(stale());
          const ownerTarget = { roomId: target.roomId, mode: target.mode };
          const label = cleanDisplayLabel(target.displayLabel);
          const pending = owner.join(ownerTarget);
          const startedScope = owner.snapshot().scope;
          const scopeGeneration = startedScope?.accountId === accountId && startedScope.roomId === target.roomId
            && startedScope.mode === target.mode ? startedScope.generation : null;
          return pending.then(result => {
            const local = owner.snapshot(), scope = local.scope;
            if (result.ok && scopeGeneration !== null && current() && scope?.accountId === accountId && scope.roomId === target.roomId
              && scope.mode === target.mode && scope.generation === scopeGeneration
              && (local.state === 'joining' || local.state === 'listening')) {
              presentation = label ? { accountId, mountGeneration: generation, scopeGeneration,
                roomId: target.roomId, mode: target.mode, label } : null;
              notifyCall();
            }
            return result;
          });
        },
        enableMicrophone() { return belongs() ? owner.enableMicrophone() : Promise.resolve(stale()); },
        disableMicrophone() { if (belongs()) owner.mute(); },
        end() { if (belongs()) owner.end(); },
        async resumeListening() { if (belongs()) await ports.resumeListening(); },
      };
    }
    currentCallView = humanCalls;
    // Each controller keeps its original admission context. An old callback
    // cannot adopt a new account merely because the shared selection changed.
    const traceStartedAt = Date.now(), requestTimings: { operation: string; startedAt: number; completedAt: number; ok: boolean }[] = [];
    if (root.dataset) root.dataset.startupRequestTimings = '[]';
    const api = { request: <T>(method: string, args?: Record<string, unknown>): Promise<T> => {
      if (!accountId) return Promise.reject(Object.assign(new Error('No local account is selected.'), { code: 'NO_LOCAL_PROFILE' }));
      const startedAt = Date.now(), result = accountClient.extension<T>(method, args ?? {}, { expectedAccountId: accountId });
      if (!startupReadOperations.has(method) || startedAt - traceStartedAt > 30000) return result;
      const trace = (ok: boolean) => {
        try {
          if (!current() || !root.dataset || requestTimings.length >= 32) return;
          requestTimings.push({ operation: method, startedAt, completedAt: Date.now(), ok });
          root.dataset.startupRequestTimings = JSON.stringify(requestTimings);
        } catch { /* Diagnostics must never change a request result. */ }
      };
      return result.then(value => { trace(true); return value; }, error => { trace(false); throw error; });
    } };
    let mounted: WorldAppHandle;
    const ownNotifications = accountId ? createNotificationsController({ client: accountClient, accountId,
      isCurrent: () => !pageClosed && root.isConnected && current(),
      onOpen: (target, context) => mounted.openNotification(target, context) }) : undefined;
    notifications = ownNotifications;
    mounted = mountWorldApp(root, { ...options, api, ...(humanCalls ? { humanCalls } : {}), ...(ownNotifications ? { notifications: ownNotifications } : {}),
      syncContactName: async displayName => {
        if (!accountId || !current()) throw Object.assign(new Error('The displayed profile changed.'), { code: 'ACTIVE_PROFILE_CHANGED' });
        await accountClient.rename(displayName, accountId);
        if (!current()) throw Object.assign(new Error('The displayed profile changed.'), { code: 'ACTIVE_PROFILE_CHANGED' });
      },
      requestContact: async (profile: WorldIdentity) => { await api.request('contacts.requestAccount', { accountId: profile.profileId }); },
    });
    void ownNotifications?.refresh();
    return mounted;
  };
  if (pageClosed || !root.isConnected) { closePage(); return; }
  world = mount(activeAccount); worldMounted = true;
  const entryWorld = world, entryGeneration = mountGeneration;
  const entryCurrent = () => !entryScopeChanged && !pageClosed && !pageLifetime.signal.aborted && root.isConnected && world === entryWorld
    && mountGeneration === entryGeneration && activeAccount === entry.state.accountId && activeDevice === entry.state.deviceId;
  if (ownedConnection) {
    if (!entryCurrent() || window.location.href !== ownedRoute) return;
    await entryWorld.refresh();
    if (!entryCurrent() || window.location.href !== ownedRoute) return;
    await entryWorld.followConnection(ownedConnection);
  } else if (entry.intent) {
    if (!entryCurrent()) return;
    if (entry.intent.kind === 'contact' || entry.intent.kind === 'device') await openAccount('people', entry.intent);
    else await entryWorld.followConnection(entry.intent, () => {
      if (!entryCurrent()) throw Error('connection_entry_changed');
      window.location.assign(takeConnectionNavigation(entry.intent!, window.location.origin, entry.handoff));
    });
  } else {
    const initial = parseConnectionIntent(entryInput, window.location.origin);
    if ((initial?.kind === 'contact' || initial?.kind === 'device') && entryCurrent()) await openConnections(entryInput);
  }
  const actionUrl = new URL(window.location.href);
  if (actionUrl.searchParams.get('action') === 'connect-device') {
    actionUrl.searchParams.delete('action'); history.replaceState({}, '', actionUrl);
    await actions.connectDevice();
  }
  } catch (error) { try { restoreOwnedRoute?.(); } catch {} closePage(); throw error; }
}
