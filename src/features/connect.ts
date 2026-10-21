import QRCode from 'qrcode';
import { accountClient as client, observeAccount } from '../core/connect-client';
import { openConnectPanel } from '../../modules/connect/ui/index.mjs';
import { ensureConnectionAccount, type ConnectionEntryResult } from '../platform/connection-hub';
import { parseConnectionIntent, type ConnectionIntent } from '../platform/connection-intent.mjs';
import { takeConnectionNavigation } from '../platform/connection-navigation.mjs';
import '../../modules/connect/ui/style.css';

let opening = false;
let firstIntent: ConnectionIntent | null = parseConnectionIntent(window.location.href, window.location.origin);

export async function initializeConnect(_label: string, host: HTMLElement): Promise<boolean> {
  const lifetime = new AbortController();
  const pagehide = (event: PageTransitionEvent) => { if (!event.persisted) lifetime.abort(); };
  window.addEventListener('pagehide', pagehide);
  let entry: ConnectionEntryResult;
  try { entry = await ensureConnectionAccount(host, { client, origin: window.location.origin,
    isCurrent: () => host.isConnected && !lifetime.signal.aborted, signal: lifetime.signal,
    observeAccount: listener => observeAccount(() => listener()),
    onIntent: (intent, handoff) => {
      if (intent.kind !== 'platform-invitation') throw Error('connection_route_unavailable');
      window.location.assign(takeConnectionNavigation(intent, window.location.origin, handoff));
    },
  }); } finally { window.removeEventListener('pagehide', pagehide); }
  if (!entry.state.accountId || lifetime.signal.aborted || !host.isConnected) return false;
  if (entry.intent) {
    if (entry.intent.kind === 'contact' || entry.intent.kind === 'device') firstIntent = entry.intent;
    else { window.location.assign(takeConnectionNavigation(entry.intent, window.location.origin, entry.handoff)); return false; }
  }
  return true;
}

export function showConnect(options: { label: string; snapshot: () => unknown; restore: (payload: unknown) => Promise<void>; invitation: () => Promise<{ url: string; label: string }>; onRename: (label: string) => Promise<void>; link?: string }): void {
  if (opening) return;
  opening = true;
  const candidate = options.link ? parseConnectionIntent(options.link, window.location.origin) : firstIntent;
  const intent = candidate?.kind === 'contact' || candidate?.kind === 'device' ? candidate : null;
  firstIntent = null;
  const panel = openConnectPanel({ client, label: options.label, productName: 'Соты',
    qr: url => QRCode.toDataURL(url, { width: 256, margin: 2, color: { dark: '#0c1810', light: '#ffffff' } }),
    snapshot: options.snapshot, restore: options.restore, invitation: options.invitation, onRename: options.onRename, initialIntent: intent, bootstrapOnOpen: false,
    snapshotDescription: 'Сохраняются обычные комнаты и тексты. Вложения, подключённые агенты и доступ к компьютеру не переносятся.'
  });
  const dialog = document.querySelector<HTMLDialogElement>('dialog.connect-panel');
  if (dialog) dialog.addEventListener('close', () => { opening = false; }, { once: true });
  else { panel.close(); opening = false; }
}

export function hasPendingConnect(): boolean { return firstIntent?.kind === 'contact' || firstIntent?.kind === 'device'; }
export function isConnectLink(value: string): boolean { const intent = parseConnectionIntent(value, window.location.origin); return intent?.kind === 'contact' || intent?.kind === 'device'; }
