import QRCode from 'qrcode';
import jsQR from 'jsqr';
import type { ConnectClient, LocalState, ContactCard } from '../../modules/connect/browser/index.mjs';
import { openConnectPanel, type ConnectIntent } from '../../modules/connect/ui/index.mjs';
import { button, el, textInput } from '../world/dom';
import { createDialog, errorText } from '../world/dialogs';
import { safeRasterSize, rasterFileDimensions } from '../world/feedback-media.mjs';
import { parseConnectionIntent, MAX_CONNECTION_INPUT_BYTES, type ConnectionIntent } from './connection-intent.mjs';
import { sharedMediaCaptureCoordinator, type MediaCaptureCoordinator, type MediaCaptureLease } from './media-capture-lease.mjs';
import '../../modules/connect/ui/style.css';
import './connection-hub.css';

export interface ConnectionHandoff { takePlatformInvitationLink(): string | null; }
export interface ConnectionEntryResult { state: LocalState; intent: ConnectionIntent | null; handoff?: ConnectionHandoff; }
export interface ConnectionScanner {
  acquire(): Promise<MediaStream>;
  detect(video: HTMLVideoElement): Promise<string>;
  file(file: File): Promise<string>;
}
export interface ConnectionHubOptions {
  client: ConnectClient; origin: string; isCurrent(): boolean;
  onIntent?: (intent: ConnectionIntent, handoff?: ConnectionHandoff) => void | Promise<void>;
  onConnectComputer?: () => void | Promise<void>;
  openAccount?: (tab: 'connect' | 'devices' | 'recovery' | 'profile', intent?: ConnectIntent) => Promise<void>;
  observeAccount?: (listener: () => void) => () => void;
  onDispose?: () => void;
  qr?: (url: string) => Promise<string>; scanner?: ConnectionScanner; captures?: MediaCaptureCoordinator;
  initialInput?: string; entry?: boolean; signal?: AbortSignal;
}
export interface ConnectionHubHandle { ready: Promise<ConnectionEntryResult>; dispose(): void; }
const stale = (): Error => Object.assign(new Error('Connection entry changed'), { code: 'ACTIVE_PROFILE_CHANGED' });
const qrImage = (url: string) => QRCode.toDataURL(url, { width: 256, margin: 2, color: { dark: '#252923', light: '#ffffff' } });

function checkedState(state: LocalState): LocalState {
  if (state.current?.revoked) throw Object.assign(new Error('Device revoked'), { code: 'DEVICE_REVOKED' });
  return state;
}

/** Existing account UI, with registration explicitly disabled. No bootstrap fallback. */
export async function openConnectionAccountPanel(options: Pick<ConnectionHubOptions, 'client' | 'isCurrent' | 'observeAccount' | 'signal' | 'qr'>,
  tab: 'connect' | 'devices' | 'recovery' | 'profile', intent?: ConnectIntent): Promise<void> {
  if (!options.isCurrent() || options.signal?.aborted) throw stale();
  const state = await options.client.getLocalState();
  if (!options.isCurrent() || options.signal?.aborted) throw stale();
  const panel = openConnectPanel({ client: options.client, label: state.label || 'Мой профиль', productName: 'Соты',
    bootstrapOnOpen: false, initialTab: tab, qr: options.qr ?? qrImage, ...(intent ? { initialIntent: intent } : {}) });
  const dialog = document.querySelector<HTMLDialogElement>('dialog.connect-panel');
  if (!dialog) { panel.close(); throw new Error('connection_panel_unavailable'); }
  const closed = new Promise<void>(resolve => dialog.addEventListener('close', () => resolve(), { once: true }));
  const cancel = () => { if (!options.isCurrent() || options.signal?.aborted) panel.close(); };
  const unsubscribe = options.observeAccount?.(cancel);
  options.signal?.addEventListener('abort', cancel);
  cancel();
  try { await closed; }
  finally { unsubscribe?.(); options.signal?.removeEventListener('abort', cancel); }
  if (!options.isCurrent() || options.signal?.aborted) throw stale();
}

/** Native IO remains below the controller. Files are decoded locally and never uploaded. */
export function createConnectionScanner(): ConnectionScanner {
  type Detector = { detect(source: CanvasImageSource): Promise<Array<{ rawValue?: string }>> };
  const Constructor = (globalThis as typeof globalThis & { BarcodeDetector?: new (options: { formats: string[] }) => Detector }).BarcodeDetector;
  let detector: Detector | null = null;
  try { if (Constructor) detector = new Constructor({ formats: ['qr_code'] }); } catch { /* jsQR is the existing fallback. */ }
  const decode = async (source: CanvasImageSource, width: number, height: number): Promise<string> => {
    const size = safeRasterSize(width, height, 1024), canvas = document.createElement('canvas');
    canvas.width = size.width; canvas.height = size.height;
    const context = canvas.getContext('2d', { willReadFrequently: true }); if (!context) throw new Error('connection_canvas_unavailable');
    context.drawImage(source, 0, 0, size.width, size.height);
    if (detector) { try { const codes = await detector.detect(canvas); const raw = codes.find(value => value.rawValue)?.rawValue; if (raw) return raw; } catch { /* Keep the same local fallback. */ } }
    const image = context.getImageData(0, 0, size.width, size.height);
    return jsQR(image.data, image.width, image.height)?.data ?? '';
  };
  return {
    acquire: () => navigator.mediaDevices.getUserMedia({ audio: false, video: { facingMode: { ideal: 'environment' }, width: { ideal: 1024 }, height: { ideal: 768 } } }),
    detect: video => video.videoWidth && video.videoHeight ? decode(video, video.videoWidth, video.videoHeight) : Promise.resolve(''),
    async file(file) {
      if (file.type === 'text/plain' || file.name.endsWith('.txt')) {
        if (file.size > MAX_CONNECTION_INPUT_BYTES) throw new Error('connection_file_too_large'); return file.text();
      }
      if (!['image/png', 'image/jpeg', 'image/webp'].includes(file.type) || file.size > 8 * 1024 * 1024) throw new Error('connection_image_invalid');
      rasterFileDimensions(await file.arrayBuffer(), file.type);
      const bitmap = await createImageBitmap(file);
      try { return await decode(bitmap, bitmap.width, bitmap.height); } finally { bitmap.close(); }
    },
  };
}

export function mountConnectionHub(host: HTMLElement, options: ConnectionHubOptions): ConnectionHubHandle {
  const ui = el('section', 'connection-hub'), status = el('p', 'ch-status'), qrHost = el('div', 'ch-qr'), preview = el('div', 'ch-preview');
  status.setAttribute('role', 'status'); status.setAttribute('aria-live', 'polite'); preview.hidden = true;
  const title = el('h1', '', options.entry ? 'Ваши соты' : 'Подключить');
  title.hidden = !options.entry;
  const explanation = el('p', 'sw-muted', options.entry ? 'Один профиль на всех устройствах. Откройте свой или создайте новый.' : 'Покажите свой QR или откройте присланную ссылку.');
  const input = textInput('', 'Ссылка из сообщения', MAX_CONNECTION_INPUT_BYTES); input.setAttribute('aria-label', 'Ссылка для подключения'); input.autocomplete = 'off';
  const video = el('video', 'ch-video'); video.playsInline = true; video.muted = true; video.hidden = true;
  const tools = el('div', 'ch-actions ch-scan-tools'), accounts = el('div', 'ch-actions');
  const scanner = options.scanner ?? createConnectionScanner(), captures = options.captures ?? sharedMediaCaptureCoordinator;
  const owner = Object.freeze({});
  let disposed = false, busy = false, decodePending = false, filePending = false, sequence = 0, state: LocalState | null = null, mountedDevice: string | null = null;
  let pending: ConnectionIntent | null = null, platformHandoff: ConnectionHandoff | null = null, settled = false;
  let proceedButton: HTMLButtonElement | null = null;
  let resolveReady!: (value: ConnectionEntryResult) => void, rejectReady!: (reason: unknown) => void;
  const ready = new Promise<ConnectionEntryResult>((resolve, reject) => { resolveReady = resolve; rejectReady = reject; });
  void ready.catch(() => {});
  type Camera = { lease: MediaCaptureLease; cancelled: boolean; pending: boolean; stream: MediaStream | null; timer?: ReturnType<typeof setTimeout> };
  let camera: Camera | null = null;
  const live = () => !disposed && host.isConnected && options.isCurrent() && !options.signal?.aborted;
  const note = (message: string) => { if (live()) status.textContent = message; };
  const friendly = (error: unknown): string => {
    const code = error && typeof error === 'object' && 'code' in error ? String(error.code).toLowerCase() : '';
    if (code === 'device_revoked') return 'Доступ устройства завершён. Используйте другое устройство или восстановление.';
    if (code === 'platform_invitation_required') return 'Для нового профиля нужно приглашение. Свой прежний профиль можно открыть без него.';
    return errorText(error);
  };
  const read = async (): Promise<LocalState> => {
    if (!live()) throw stale();
    const next = checkedState(await options.client.getLocalState());
    if (!live() || !options.entry && mountedDevice !== null && next.deviceId !== mountedDevice) throw stale();
    state = next; return next;
  };
  const finish = async (intent: ConnectionIntent | null, ticket = sequence): Promise<void> => {
    const current = await read(); if (ticket !== sequence) return;
    if (!current.accountId) throw new Error('connection_account_required');
    const expected = current.accountId, device = current.deviceId;
    const confirmed = await read();
    if (confirmed.accountId !== expected || confirmed.deviceId !== device) throw stale();
    if (ticket !== sequence) return;
    const handoff = platformHandoff ?? undefined;
    if (options.entry) {
      if (!settled) { settled = true; resolveReady({ state: confirmed, intent, ...(handoff ? { handoff } : {}) }); }
    } else if (intent) {
      if (options.onIntent) await options.onIntent(intent, handoff);
      else if (intent.kind === 'contact' || intent.kind === 'device') await openAccount('profile', intent);
      else throw new Error('connection_route_unavailable');
      if (live() && ticket === sequence && pending === intent) { pending = null; platformHandoff = null; proceedButton = null; preview.hidden = true; preview.replaceChildren(); }
    }
  };
  const stopOwned = (capture: Camera): void => {
    if (!capture.stream) { if (!capture.pending) capture.lease.release(); return; }
    let stopped = true;
    try {
      for (const track of capture.stream.getTracks()) {
        try { track.enabled = false; } catch { stopped = false; }
        try { track.stop(); } catch { stopped = false; }
        if (track.readyState !== 'ended') stopped = false;
      }
    } catch { stopped = false; }
    if (stopped) { capture.stream = null; capture.lease.release(); }
    else note('Камера ещё освобождается. Пока можно вставить ссылку или выбрать файл.');
  };
  const stopCamera = (): void => {
    const capture = camera; camera = null; video.hidden = true; video.srcObject = null; stop.hidden = true;
    if (!capture) return; capture.cancelled = true; clearTimeout(capture.timer); stopOwned(capture);
  };
  const run = (action: () => Promise<void>) => async (): Promise<void> => {
    if (busy || !live()) return; busy = true; ui.setAttribute('aria-busy', 'true');
    try { await action(); } catch (error) { note(friendly(error)); }
    finally { busy = false; if (live()) ui.setAttribute('aria-busy', 'false'); }
  };
  async function openAccount(tab: 'connect' | 'devices' | 'recovery' | 'profile', intent?: ConnectIntent): Promise<void> {
    stopCamera();
    if (options.openAccount) await options.openAccount(tab, intent);
    else await openConnectionAccountPanel(options, tab, intent);
    const current = await read();
    update();
    if (options.entry && current.accountId && !pending) await finish(null);
  }
  const clearPreview = (): void => {
    sequence++; pending = null; platformHandoff = null; proceedButton = null; preview.hidden = true; preview.replaceChildren();
  };
  const previewIntent = (value: string, keepInput = false, fromQr = false): void => {
    const intent = parseConnectionIntent(value, options.origin);
    if (!intent) { note('Это не поддерживаемая ссылка Сот. Проверьте полный адрес.'); return; }
    stopCamera(); sequence++; pending = intent; if (!keepInput) input.value = ''; qrHost.replaceChildren(); update();
    let held = intent.kind === 'platform-invitation' ? value.trim() : null;
    platformHandoff = held ? { takePlatformInvitationLink() { const result = held; held = null; return result; } } : null;
    if (fromQr && intent.kind === 'device' && state?.accountId && !options.entry) {
      const ticket = sequence; preview.hidden = true; preview.replaceChildren();
      // The next view is the one explicit approval, not an automatic grant.
      void run(() => finish(intent, ticket))(); return;
    }
    const labels = { contact: 'Добавить человека', device: 'Подтвердить своё устройство', 'legacy-room': 'Открыть приглашение в комнату',
      profile: 'Посмотреть профиль', community: 'Открыть комнату', application: 'Открыть приложение', 'platform-invitation': 'Приглашение в Соты' };
    preview.replaceChildren(el('h2', '', labels[intent.kind]), el('p', '', 'Сначала проверим назначение. Подключение и доступ не выдаются сканированием.'));
    const proceed = button(state?.accountId || intent.kind === 'platform-invitation' ? 'Продолжить' : 'Открыть мой профиль', 'next', '', () => { void run(async () => {
      const ticket = sequence, offeredHandoff = platformHandoff;
      const current = await read();
      if (ticket !== sequence || pending !== intent) return;
      if (intent.kind === 'platform-invitation') {
        if (!options.onIntent) throw new Error('connection_route_unavailable');
        await options.onIntent(intent, offeredHandoff ?? undefined); return;
      }
      if (!current.accountId) { await openAccount('connect'); if (live() && ticket === sequence && pending === intent) previewIntent(value); return; }
      await finish(intent, ticket);
    })(); });
    proceedButton = proceed;
    const cancel = button('Отмена', 'close', '', () => { clearPreview(); note(''); input.focus();
      // Read-only cold completion must not wait for or be dropped by an older busy Continue.
      if (options.entry && state?.accountId) void finish(null).catch(error => note(friendly(error))); });
    preview.append(proceed, cancel); preview.hidden = false; if (!keepInput) proceed.focus();
  };
  const showCard = async (): Promise<void> => {
    clearPreview(); stopCamera(); const ticket = ++sequence, current = await read();
    qrHost.replaceChildren(); update();
    if (!current.accountId) { note('Откройте свой профиль на этом устройстве или создайте новый.'); return; }
    const card = await options.client.extension<ContactCard>('card.get', {}, { expectedAccountId: current.accountId });
    const after = await read();
    if (!live() || ticket !== sequence || after.accountId !== current.accountId || after.deviceId !== current.deviceId) return;
    if (!/^[A-Za-z0-9_-]{16,160}$/u.test(card.cardId)) throw new Error('connection_card_invalid');
    const url = new URL('/', options.origin); url.searchParams.set('connect', 'contact'); url.searchParams.set('card', card.cardId);
    const source = await (options.qr ?? qrImage)(url.href);
    const displayed = await read();
    if (!live() || ticket !== sequence || displayed.accountId !== current.accountId || displayed.deviceId !== current.deviceId) return;
    const image = el('img'); image.src = source; image.width = 224; image.height = 224; image.alt = 'QR: добавить меня в друзья';
    const link = textInput(url.href); link.readOnly = true; link.hidden = true; link.setAttribute('aria-label', 'Моя ссылка для знакомства');
    const copy = button('Скопировать ссылку', 'copy', '', () => { void run(async () => {
      try { await navigator.clipboard.writeText(url.href); note('Ссылка скопирована.'); }
      catch { if (live()) { link.hidden = false; link.focus(); link.select(); note('Скопируйте выделенную ссылку.'); } }
    })(); });
    const restoreFocus = document.activeElement === show;
    qrHost.replaceChildren(image, el('p', '', 'Добавить меня в друзья'), link, copy); update();
    if (restoreFocus) copy.focus();
  };
  const pageHidden = (): boolean => document.visibilityState === 'hidden';
  const startCamera = async (): Promise<void> => {
    stopCamera(); sequence++; qrHost.replaceChildren(); update(); if (!live() || pageHidden()) return;
    const lease = captures.acquire('camera', owner);
    if (!lease) { note('Камера используется. Вставьте ссылку или выберите QR из файла.'); return; }
    const capture: Camera = { lease, cancelled: false, pending: true, stream: null }; camera = capture; stop.hidden = false;
    try {
      capture.stream = await scanner.acquire(); capture.pending = false;
      if (!live() || capture.cancelled || camera !== capture || pageHidden()) { stopOwned(capture); return; }
      const tracks = capture.stream.getTracks();
      if (!tracks.length || tracks.some(track => track.kind !== 'video' || track.readyState !== 'live')) throw new Error('connection_camera_invalid');
      video.srcObject = capture.stream; video.hidden = false; await video.play();
      const scan = async (): Promise<void> => {
        if (!live() || camera !== capture || capture.cancelled) return;
        if (decodePending) { capture.timer = setTimeout(() => { void scan(); }, 250); return; }
        decodePending = true;
        try {
          const raw = await scanner.detect(video);
          if (!live() || camera !== capture || capture.cancelled) return;
          if (raw) previewIntent(raw, false, true);
        } catch { if (live() && camera === capture && !capture.cancelled) note('Не удалось прочитать кадр. Можно вставить ссылку или выбрать файл.'); }
        finally { decodePending = false; }
        if (live() && camera === capture && !capture.cancelled) capture.timer = setTimeout(() => { void scan(); }, 250);
      };
      if (!live() || capture.cancelled || camera !== capture) { stopOwned(capture); return; }
      note('Наведите камеру на QR Сот.'); void scan();
    } catch {
      capture.pending = false; capture.cancelled = true; const current = camera === capture;
      if (current) { camera = null; video.hidden = true; video.srcObject = null; stop.hidden = true; }
      stopOwned(capture);
      if (current) note('Камера недоступна. Вставьте ссылку или выберите QR из файла.');
    }
  };
  const show = button('Мой QR', 'qr', '', () => { void showCard().catch(error => note(friendly(error))); });
  const scan = button('Сканировать', 'scan', '', () => { void startCamera(); });
  const stop = button('Выключить камеру', 'close', 'ch-stop', stopCamera);
  stop.hidden = true;
  const fromFile = button('Выбрать файл', 'folder', '', () => {
    if (!live()) return;
    if (filePending) { note('Файл ещё читается. Можно вставить ссылку.'); return; }
    stopCamera(); const ticket = ++sequence, picker = el('input'); picker.type = 'file'; picker.accept = 'image/png,image/jpeg,image/webp,text/plain,.txt';
    picker.addEventListener('change', () => { const file = picker.files?.[0]; if (!file) return;
      if (filePending) { note('Файл ещё читается. Можно вставить ссылку.'); return; }
      filePending = true;
      void scanner.file(file).then(raw => { if (live() && ticket === sequence) previewIntent(raw, false, true); }, error => { if (live() && ticket === sequence) note(friendly(error)); }).finally(() => { filePending = false; }); }); picker.click();
  });
  const paste = button('Открыть ссылку', 'next', '', () => { stopCamera(); if (live()) previewIntent(input.value); });
  input.addEventListener('keydown', event => { if (event.key === 'Enter' && !event.isComposing) { event.preventDefault(); paste.click(); } });
  input.addEventListener('input', event => {
    if (!live()) return;
    clearPreview();
    if (!busy && state && (event as InputEvent).inputType === 'insertFromPaste' && parseConnectionIntent(input.value, options.origin)) previewIntent(input.value, true);
  });
  const existing = button('Мой профиль на этом устройстве', 'phone', options.entry ? 'ch-primary' : '', () => { void run(() => openAccount('connect'))(); });
  const recovery = button('Восстановить профиль', 'lock', '', () => { void run(() => openAccount('recovery'))(); });
  const computer = button('Приложения с компьютера', 'laptop', '', () => { void run(async () => {
    const current = await read(); if (!current.accountId || !options.onConnectComputer) throw new Error('connection_account_required');
    stopCamera(); await options.onConnectComputer();
  })(); });
  const create = button('Создать новый профиль', 'plus', '', () => { void run(async () => {
    const before = await read(); if (before.accountId) throw stale();
    stopCamera(); await options.client.bootstrap('Мои соты'); const after = await read();
    if (!after.accountId) throw new Error('connection_account_unconfirmed'); update();
    if (pending) note('Профиль открыт. Продолжите выбранное подключение.'); else await finish(null);
  })(); });
  function update(): void {
    existing.textContent = state?.accountId ? 'Мой профиль на другом устройстве' : 'Открыть мой профиль';
    if (proceedButton) proceedButton.textContent = state?.accountId || pending?.kind === 'platform-invitation' ? 'Продолжить' : 'Открыть мой профиль';
    create.hidden = !options.entry || !state || Boolean(state.accountId); computer.disabled = !state?.accountId || !options.onConnectComputer;
    tools.dataset.account = state?.accountId ? 'ready' : 'empty';
    show.hidden = !state?.accountId || qrHost.children.length > 0; computer.hidden = !state?.accountId || !options.onConnectComputer;
  }
  update();
  tools.append(show, scan, fromFile, stop); accounts.append(existing, create, recovery, computer);
  if (options.entry) ui.append(title, explanation, status, accounts, qrHost, tools, video, input, paste, preview);
  else ui.append(title, explanation, status, qrHost, tools, video, input, paste, preview, accounts);
  host.replaceChildren(ui);
  const changed = (): void => { stopCamera(); if (!live()) { dispose(); return; }
    void read().then(async current => { if (live()) { update(); if (options.entry && current.accountId && !pending) await finish(null); } }).catch(error => { qrHost.replaceChildren(); state = null; update(); note(friendly(error)); }); };
  const visibility = (): void => { if (document.visibilityState === 'hidden') stopCamera(); };
  const pagehide = (): void => { stopCamera(); };
  document.addEventListener('visibilitychange', visibility); window.addEventListener('pagehide', pagehide);
  let unsubscribe: (() => void) | undefined;
  unsubscribe = options.observeAccount?.(changed);
  if (disposed) unsubscribe?.();
  options.signal?.addEventListener('abort', dispose);
  function dispose(): void {
    if (disposed) return; stopCamera(); disposed = true; sequence++; pending = null; platformHandoff = null;
    unsubscribe?.(); options.signal?.removeEventListener('abort', dispose); document.removeEventListener('visibilitychange', visibility); window.removeEventListener('pagehide', pagehide);
    ui.replaceChildren();
    if (!settled) { settled = true; rejectReady(stale()); }
    options.onDispose?.();
  }
  if (!live()) dispose();
  void (async () => { try {
    const initial = await read(); mountedDevice = initial.deviceId; update();
    if (options.initialInput && parseConnectionIntent(options.initialInput, options.origin)) previewIntent(options.initialInput);
    else if (initial.accountId && options.entry) await finish(null);
    else if (initial.accountId && !options.entry) await showCard();
  } catch (error) { note(friendly(error)); } })();
  return { ready, dispose };
}

export function openConnectionHub(options: ConnectionHubOptions): { close(): void; closed: Promise<void>; ready: Promise<ConnectionEntryResult> } {
  let handle: ConnectionHubHandle | null = null;
  let resolveClosed!: () => void; const closed = new Promise<void>(resolve => { resolveClosed = resolve; });
  const dialog = createDialog('Подключить', () => { handle?.dispose(); resolveClosed(); }); dialog.element.classList.add('connection-hub-dialog');
  handle = mountConnectionHub(dialog.body, { ...options, onDispose: () => { dialog.close(); options.onDispose?.(); } });
  return { close: () => dialog.close(), closed, ready: handle.ready };
}

/** A new device opens immediately; saved profiles and transfers keep their explicit recovery path. */
export async function ensureConnectionAccount(host: HTMLElement, options: ConnectionHubOptions): Promise<ConnectionEntryResult> {
  if (!options.isCurrent() || options.signal?.aborted) throw stale();
  let state = checkedState(await options.client.getLocalState());
  if (!options.isCurrent() || options.signal?.aborted) throw stale();
  if (!state.accountId && !state.profiles.length && !state.pendingEnrollment && !state.pendingRecovery) {
    let invitationRequired = false;
    try { await options.client.bootstrap('Мои соты'); }
    catch (error) {
      if (!(error && typeof error === 'object' && 'code' in error && error.code === 'platform_invitation_required')) throw error;
      invitationRequired = true;
    }
    if (!options.isCurrent() || options.signal?.aborted) throw stale();
    state = checkedState(await options.client.getLocalState());
    if (!options.isCurrent() || options.signal?.aborted) throw stale();
    if (!state.accountId && !invitationRequired) throw new Error('connection_account_unconfirmed');
  }
  if (state.accountId) return { state, intent: null };
  const page = el('main', 'connection-hub-entry'); host.replaceChildren(page);
  const handle = mountConnectionHub(page, { ...options, entry: true });
  try { return await handle.ready; } finally { handle.dispose(); }
}
