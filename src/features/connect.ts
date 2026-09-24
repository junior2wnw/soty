import QRCode from 'qrcode';
import { createConnectClient } from '../../modules/connect/browser/index.mjs';
import { openConnectPanel, parseConnectLink } from '../../modules/connect/ui/index.mjs';
import '../../modules/connect/ui/style.css';

const client = createConnectClient({ projectId: 'soty', endpoint: '/api/connect/rpc', dbName: 'soty-connect-v1' });
let opening = false;
let bootstrapping: Promise<unknown> | null = null;
let firstIntent = parseConnectLink(window.location.href);

export function initializeConnect(label: string): void {
  // Account state lives outside application assets and the legacy Soty key store.
  if (!bootstrapping) bootstrapping = client.bootstrap(label).catch(() => { bootstrapping = null; });
}

export function showConnect(options: { label: string; snapshot: () => unknown; restore: (payload: unknown) => Promise<void>; invitation: () => Promise<{ url: string; label: string }>; onRename: (label: string) => Promise<void>; link?: string }): void {
  if (opening) return;
  opening = true;
  const intent = options.link ? parseConnectLink(options.link) : firstIntent;
  firstIntent = null;
  const panel = openConnectPanel({ client, label: options.label, productName: 'Соты',
    qr: url => QRCode.toDataURL(url, { width: 256, margin: 2, color: { dark: '#0c1810', light: '#ffffff' } }),
    snapshot: options.snapshot, restore: options.restore, invitation: options.invitation, onRename: options.onRename, initialIntent: intent,
    snapshotDescription: 'Сохраняются обычные комнаты и тексты. Вложения, подключённые агенты и доступ к компьютеру не переносятся.'
  });
  const dialog = document.querySelector<HTMLDialogElement>('dialog.connect-panel');
  if (dialog) dialog.addEventListener('close', () => { opening = false; }, { once: true });
  else { panel.close(); opening = false; }
}

export function hasPendingConnect(): boolean { return firstIntent !== null; }
export function isConnectLink(value: string): boolean { return parseConnectLink(value) !== null; }
