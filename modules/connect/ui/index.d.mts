export interface ConnectIntent { kind: 'contact' | 'device'; id: string; }
export function parseConnectLink(value: string, origin?: string): ConnectIntent | null;
export function openConnectPanel(options: {
  client: unknown; label?: string; productName?: string;
  qr?: (url: string) => Promise<string>;
  snapshot?: () => unknown | Promise<unknown>;
  restore?: (payload: unknown) => void | Promise<void>;
  invitation?: () => { url: string; label: string } | Promise<{ url: string; label: string }>;
  onRename?: (label: string) => void | Promise<void>;
  snapshotDescription?: string; initialIntent?: ConnectIntent | null;
}): { close(): void; refresh(): Promise<void> };
