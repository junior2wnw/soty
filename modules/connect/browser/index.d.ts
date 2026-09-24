export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };
export type PublicP256Key = { kty: 'EC'; crv: 'P-256'; x: string; y: string };

export class ConnectError extends Error {
  readonly code: string;
  readonly status?: number;
  constructor(code: string, message: string, options?: { cause?: unknown; status?: number });
}

export interface LocalProfile {
  accountId: string | null;
  deviceId: string;
  label: string;
  active: boolean;
  revoked: boolean;
  vaultRevision: number;
  createdAt: string;
}

/** Safe view only. No private keys, account data keys, recovery secrets or full QR contents. */
export interface LocalState {
  schema: 'connect.local-view.v1';
  accountId: string | null;
  deviceId: string | null;
  label: string | null;
  current: LocalProfile | null;
  profiles: LocalProfile[];
  pendingEnrollment: { requestId: string | null; label: string; expiresAt: number | null } | null;
  pendingRecovery: { accountId: string; recoveryId: string; label: string } | null;
  recoveryPrepared: boolean;
  notificationError: { code: string; message: string } | null;
}

export interface ClientOptions {
  projectId: string;
  endpoint: string;
  dbName?: string;
  fetch?: typeof globalThis.fetch;
  /** Receives committed state without blocking operations; may call client methods. */
  onState?: (state: LocalState) => void | Promise<void>;
  /** Reports state-notification or observer errors; never contains credential material. */
  onError?: (error: ConnectError) => void | Promise<void>;
}

export interface AccountResult {
  accountId: string;
  deviceId: string;
  label: string;
  alreadyCompleted?: boolean;
  recoveryConsumed?: boolean;
}
export interface DeviceStatus { deviceId: string; label: string; state: string; revokedAt: number | null }
export interface AccountStatus extends AccountResult {
  devices: DeviceStatus[];
  recovery: { verified: boolean; recoveryId?: string };
}
export interface ContactCard { cardId: string; label: string; projectId?: string; purpose?: 'contact.discover' }
export interface ContactRequest { requestId: string; peerAccountId: string; label: string; createdAt: number; expiresAt: number }
export interface Contact { relationshipId: string; peerAccountId: string; label: string; createdAt: number }
export interface ContactList {
  requests: { incoming: ContactRequest[]; outgoing: ContactRequest[] };
  contacts: Contact[];
  blocked: { peerAccountId: string; label: string }[];
  invitations: { invitationId: string; relationshipId: string; peerAccountId: string; peerLabel: string; label: string; url: string; createdAt: number; expiresAt: number }[];
}
export type ContactRequestResult = { requestId: string; status: 'pending' } | { relationshipId: string; status: 'active' };
export interface EnrollmentStart { requestId: string; expiresAt: number }
export interface EnrollmentInspection extends EnrollmentStart {
  label: string;
  deviceId: string;
  publicJwk: PublicP256Key;
  encryptionPublicJwk: PublicP256Key;
  accountId: string;
  projectId: string;
  status: string;
}
export interface EnrollmentPreview extends EnrollmentStart {
  recipientDeviceId: string;
  status: 'pending' | 'approved' | 'finished';
  account: { accountId: string; label: string } | null;
  source: { deviceId: string; label: string } | null;
}
export interface RecoveryEnvelope {
  schema: 'connect.recovery-key.v1'; projectId: string; accountId: string; iv: string; ciphertext: string;
}
/** Secret material: export only after an explicit user action; never log or put in URLs. */
export interface RecoveryKit {
  schema: 'connect.recovery-kit.v1';
  projectId: string;
  accountId: string;
  recoveryId: string;
  secret: string;
  wrappedKey: RecoveryEnvelope;
  createdAt: string;
}

export interface ConnectClient {
  bootstrap(label: string): Promise<AccountResult>;
  status(): Promise<AccountStatus>;
  rename(label: string): Promise<{ label: string }>;
  card(): Promise<ContactCard>;
  rotateCard(): Promise<ContactCard>;
  resolveCard(cardId: string): Promise<ContactCard>;
  requestContact(cardId: string): Promise<ContactRequestResult>;
  contacts(): Promise<ContactList>;
  acceptContact(requestId: string): Promise<{ relationshipId: string; status: 'active' }>;
  declineContact(requestId: string): Promise<{ declined: true }>;
  cancelContact(requestId: string): Promise<{ cancelled: true }>;
  removeContact(relationshipId: string): Promise<{ removed: true }>;
  blockContact(peerAccountId: string): Promise<{ blocked: true }>;
  unblockContact(peerAccountId: string): Promise<{ unblocked: true }>;
  sendContactInvite(relationshipId: string, url: string, label: string): Promise<{ invitationId: string; status: 'pending' }>;
  dismissContactInvite(invitationId: string): Promise<{ dismissed: true }>;
  startEnrollment(label: string): Promise<EnrollmentStart>;
  inspectEnrollment(requestId: string): Promise<EnrollmentInspection>;
  /** Pass the account shown in the confirmation UI to reject a stale cross-tab confirmation. */
  approveEnrollment(requestId: string, expectedAccountId?: string): Promise<{ requestId: string; approved: true }>;
  /** Read only; signed by the durable recipient. Required again after a reload or profile switch. */
  previewEnrollment(requestId: string): Promise<EnrollmentPreview>;
  /** Opens exactly the previewed account; rejects a profile change since preview, retains old profiles. */
  finishEnrollment(requestId: string, expectedAccountId: string): Promise<AccountResult>;
  /** Abandons only local enrollment UI state; it does not delete keys or revoke a server grant. */
  discardPendingEnrollment(): Promise<void>;
  switchProfile(accountId: string): Promise<LocalState>;
  revokeDevice(deviceId: string): Promise<{ revoked: true; deviceId?: string }>;
  /** Pass the displayed account before asynchronously preparing a product snapshot. */
  saveVault(payload: JsonValue, expectedRevision?: number, expectedAccountId?: string): Promise<{ revision: number }>;
  loadVault<T extends JsonValue = JsonValue>(expectedAccountId?: string): Promise<{ revision: number; payload: T | null }>;
  prepareRecovery(): Promise<RecoveryKit>;
  /** Confirms a supplied kit by opening it and comparing its account key to the current key. */
  confirmRecovery(kit: RecoveryKit): Promise<{ recoveryId: string; verified: true }>;
  /** Explicitly activates recovered access; earlier local profiles remain available. */
  recover(kit: RecoveryKit, label: string): Promise<AccountResult>;
  getLocalState(): Promise<LocalState>;
  /** Closes the cross-tab change channel. Already-running explicit operations can finish. */
  dispose(): void;
}

export function createConnectClient(options: ClientOptions): ConnectClient;
