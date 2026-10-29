export interface ConnectIntent { kind: 'contact' | 'device'; id: string; }
export type ConnectTab = 'profile' | 'people' | 'devices' | 'recovery' | 'data' | 'profiles' | 'connect';
export function parseConnectLink(value: string, origin?: string): ConnectIntent | null;
export function openConnectPanel(options: {
  client: unknown; label?: string; productName?: string;
  qr?: (url: string) => Promise<string>;
  snapshot?: () => unknown | Promise<unknown>;
  restore?: (payload: unknown) => void | Promise<void>;
  invitation?: () => { url: string; label: string } | Promise<{ url: string; label: string }>;
  onRename?: (label: string) => void | Promise<void>;
  onEditProfile?: () => void;
  onOpenDataTools?: () => void | Promise<void>;
  onContactCall?: (contact: { peerAccountId: string; label: string }) => void | Promise<void>;
  onContactMessage?: (contact: { peerAccountId: string; relationshipId: string; label: string }) => void | Promise<void>;
  snapshotDescription?: string; initialIntent?: ConnectIntent | null; initialTab?: ConnectTab;
  bootstrapOnOpen?: boolean;
}): { close(): void; refresh(): Promise<void> };
