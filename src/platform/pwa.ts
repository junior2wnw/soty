export type PwaInstallState = 'unavailable' | 'available' | 'prompting' | 'accepted' | 'installed';
export type PwaUpdateState = 'idle' | 'available' | 'saving' | 'activating' | 'blocked' | 'failed';
export interface PwaState {
  install: PwaInstallState;
  update: PwaUpdateState;
  connection: 'checking' | 'online' | 'offline' | 'unreachable';
  worker: 'unsupported' | 'development' | 'registering' | 'active' | 'failed';
  offlineReady: boolean;
}
export interface PwaController {
  get(): PwaState;
  subscribe(listener: (state: PwaState) => void): () => void;
  requestInstall(): Promise<'accepted' | 'dismissed' | 'unavailable'>;
  checkConnection(): Promise<void>;
  checkUpdate(): Promise<void>;
  applyUpdate(): Promise<boolean>;
  destroy(): void;
}
type InstallPrompt = Event & { prompt(): Promise<void>; userChoice: Promise<{ outcome: 'accepted' | 'dismissed' }> };
type UpdateGuard = () => boolean | void | Promise<boolean | void>;
const guards = new Set<UpdateGuard>();
let singleton: PwaController | undefined;

/** A guard must save durable drafts or return false. Nothing is implicitly discarded. */
export function registerUpdateGuard(guard: UpdateGuard): () => void { guards.add(guard); return () => guards.delete(guard); }
export function getPwaController(): PwaController { return singleton ??= createPwaController(); }

export function createPwaController(): PwaController {
  const supported = 'serviceWorker' in navigator, development = import.meta.env.DEV;
  const display = matchMedia('(display-mode: standalone)');
  const installed = () => display.matches || (navigator as Navigator & { standalone?: boolean }).standalone === true;
  let state: PwaState = { install: installed() ? 'installed' : 'unavailable', update: 'idle', connection: 'checking',
    worker: development ? 'development' : supported ? 'registering' : 'unsupported', offlineReady: false };
  let prompt: InstallPrompt | undefined, registration: ServiceWorkerRegistration | undefined, destroyed = false;
  let connectionFlight: Promise<void> | undefined, guardFlight: Promise<boolean> | undefined, updateFlight: Promise<boolean> | undefined;
  let reloadArmed = false, reloadRequired = false, reloading = false, lastUpdateCheck = 0;
  const listeners = new Set<(state: PwaState) => void>(), abort = new AbortController();
  const publish = (patch: Partial<PwaState>) => {
    if (destroyed || Object.entries(patch).every(([key, value]) => state[key as keyof PwaState] === value)) return;
    state = { ...state, ...patch }; for (const listener of listeners) listener({ ...state });
  };
  const saveDrafts = (): Promise<boolean> => guardFlight ??= (async () => {
    try { for (const guard of guards) if (await guard() === false) return false; return true; }
    catch { return false; }
  })().finally(() => { guardFlight = undefined; });
  const workerRequest = <T>(worker: ServiceWorker, type: string, timeout = 5000): Promise<T> => new Promise((resolve, reject) => {
    const channel = new MessageChannel();
    const timer = setTimeout(() => { channel.port1.close(); reject(new Error('worker_timeout')); }, timeout);
    channel.port1.onmessage = event => { clearTimeout(timer); channel.port1.close(); resolve(event.data as T); };
    try { worker.postMessage({ type }, [channel.port2]); } catch (error) { clearTimeout(timer); channel.port1.close(); reject(error); }
  });
  const refreshOffline = async () => {
    const worker = navigator.serviceWorker?.controller;
    if (!worker || development) return;
    try {
      const response = await workerRequest<{ offlineReady?: boolean }>(worker, 'SOTY_OFFLINE_STATUS');
      publish({ worker: 'active', offlineReady: response.offlineReady === true });
    } catch { publish({ offlineReady: false }); }
  };
  const checkConnection = (): Promise<void> => connectionFlight ??= (async () => {
    try {
      const response = await fetch('/health', { cache: 'no-store', credentials: 'same-origin', signal: AbortSignal.any([abort.signal, AbortSignal.timeout(5000)]) });
      const data: unknown = response.ok ? await response.json() : null;
      if (data && typeof data === 'object' && 'ok' in data && data.ok === true) publish({ connection: 'online' });
      else publish({ connection: 'unreachable' });
    } catch { publish({ connection: navigator.onLine ? 'unreachable' : 'offline' }); }
  })().finally(() => { connectionFlight = undefined; });
  const checkUpdate = async () => {
    if (!registration || development || destroyed) return;
    lastUpdateCheck = Date.now();
    try { await registration.update(); if (registration.waiting) publish({ update: 'available' }); }
    catch { /* An update check is independent of editing and connection state. */ }
  };
  const beforeInstall = (event: Event) => { event.preventDefault(); prompt = event as InstallPrompt; publish({ install: 'available' }); };
  const appInstalled = () => { prompt = undefined; publish({ install: 'installed' }); };
  const displayChanged = () => { if (installed()) appInstalled(); };
  const refreshConnection = () => { void checkConnection(); };
  const visible = () => {
    if (document.visibilityState !== 'visible') return;
    void checkConnection(); void refreshOffline();
    if (Date.now() - lastUpdateCheck > 60 * 60 * 1000) void checkUpdate();
  };
  const message = (event: MessageEvent) => {
    // Only a worker registered at this origin may initiate an update handshake.
    const source = event.source;
    if (!(source instanceof ServiceWorker) || new URL(source.scriptURL).origin !== location.origin) return;
    if (event.data?.type === 'SOTY_PREPARE_UPDATE') {
      const port = event.ports[0]; if (!port) return;
      publish({ update: 'saving' });
      void saveDrafts().then(ready => { port.postMessage({ ready }); port.close(); publish({ update: ready ? 'available' : 'blocked' }); });
    } else if (event.data?.type === 'SOTY_UPDATE_COMMIT') {
      reloadArmed = true; reloadRequired = true; publish({ update: 'activating' });
    }
  };
  const controllerChanged = () => {
    void refreshOffline();
    if (!reloadArmed || reloading) return;
    // Typing may resume between prepare and activation: recheck before navigation.
    void saveDrafts().then(ready => {
      if (!ready) { reloadArmed = false; publish({ update: 'blocked' }); return; }
      reloading = true; location.reload();
    });
  };
  window.addEventListener('beforeinstallprompt', beforeInstall, { signal: abort.signal });
  window.addEventListener('appinstalled', appInstalled, { signal: abort.signal });
  window.addEventListener('online', refreshConnection, { signal: abort.signal });
  window.addEventListener('offline', refreshConnection, { signal: abort.signal });
  document.addEventListener('visibilitychange', visible, { signal: abort.signal });
  display.addEventListener('change', displayChanged, { signal: abort.signal });
  if (supported) {
    navigator.serviceWorker.addEventListener('message', message, { signal: abort.signal });
    navigator.serviceWorker.addEventListener('controllerchange', controllerChanged, { signal: abort.signal });
    void (async () => {
      try {
        // A Vite worker retires an existing production-preview cache at this origin.
        if (development && !await navigator.serviceWorker.getRegistration('/')) return;
        registration = await navigator.serviceWorker.register('/sw.js', { updateViaCache: 'none' });
        if (destroyed || development) return;
        if (registration.waiting) publish({ update: 'available' });
        registration.addEventListener('updatefound', () => {
          const installing = registration?.installing; if (!installing) return;
          installing.addEventListener('statechange', () => {
            if (installing.state === 'installed' && navigator.serviceWorker.controller) publish({ update: 'available' });
            if (installing.state === 'activated') void refreshOffline();
          }, { signal: abort.signal });
        }, { signal: abort.signal });
        void navigator.serviceWorker.ready.then(() => { if (!destroyed) void refreshOffline(); });
        void refreshOffline();
      } catch { publish({ worker: 'failed', offlineReady: false }); }
    })();
  }
  const interval = setInterval(() => { if (document.visibilityState === 'visible') void checkConnection(); }, 30_000);
  void checkConnection();
  return {
    get: () => ({ ...state }),
    subscribe(listener) { listeners.add(listener); listener({ ...state }); return () => listeners.delete(listener); },
    async requestInstall() {
      if (!prompt || state.install !== 'available') return 'unavailable';
      const event = prompt; prompt = undefined; publish({ install: 'prompting' });
      try { await event.prompt(); const choice = await event.userChoice; publish({ install: choice.outcome === 'accepted' ? 'accepted' : 'unavailable' }); return choice.outcome; }
      catch { publish({ install: 'unavailable' }); return 'unavailable'; }
    },
    checkConnection, checkUpdate,
    applyUpdate() {
      if (updateFlight) return updateFlight;
      updateFlight = (async () => {
        if (development || (!registration?.waiting && !reloadRequired)) return false;
        publish({ update: 'saving' });
        if (!await saveDrafts()) { publish({ update: 'blocked' }); return false; }
        if (!registration?.waiting) { reloading = true; location.reload(); return true; }
        try {
          const result = await workerRequest<{ ready?: boolean }>(registration.waiting, 'SOTY_ACTIVATE_UPDATE', 20_000);
          if (!result.ready) { publish({ update: 'blocked' }); return false; }
          publish({ update: 'activating' }); return true;
        } catch { publish({ update: 'failed' }); return false; }
      })().finally(() => { updateFlight = undefined; }); return updateFlight;
    },
    destroy() { destroyed = true; abort.abort(); clearInterval(interval); listeners.clear(); if (singleton) singleton = undefined; },
  };
}

/** Tracks real editing, not programmatic prefill. Domain drafts should use their own durable guard. */
export function watchFormEdits(root: Document | HTMLElement = document): { hasUnsavedChanges(): boolean; markSaved(element?: HTMLElement): void; destroy(): void } {
  const values = new Map<HTMLInputElement | HTMLTextAreaElement, string>(), edited = new Set<HTMLInputElement | HTMLTextAreaElement>();
  const abort = new AbortController();
  const prune = () => { for (const input of values.keys()) if (!input.isConnected) { values.delete(input); edited.delete(input); } };
  const field = (target: EventTarget | null) => target instanceof HTMLTextAreaElement || target instanceof HTMLInputElement && ['text', 'email', 'url', 'tel', 'password', 'number'].includes(target.type) ? target : null;
  const remember = (event: Event) => { prune(); const input = field(event.target); if (input && !input.closest('[data-pwa-ignore]') && !values.has(input)) values.set(input, input.value); };
  const changed = (event: Event) => { const input = field(event.target); if (input && values.has(input)) edited.add(input); };
  root.addEventListener('focusin', remember, { signal: abort.signal }); root.addEventListener('beforeinput', remember, { signal: abort.signal });
  root.addEventListener('input', changed, { signal: abort.signal });
  return {
    hasUnsavedChanges() { prune(); return [...edited].some(input => !input.closest('[data-pwa-ignore]') && input.value !== values.get(input)); },
    markSaved(element) { prune(); for (const input of edited) if (!element || element === input || element.contains(input)) { values.set(input, input.value); edited.delete(input); } },
    destroy() { abort.abort(); values.clear(); edited.clear(); },
  };
}
