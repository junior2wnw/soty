export type HumanSigninScope = 'current-device' | 'account';
export interface HumanSigninCapabilities {
  schema: 'soty.human-signins-capability.v1'; available: boolean;
  sessions: boolean; rememberedConsents: boolean; checkRevoke: boolean;
  scopes: readonly HumanSigninScope[]; sourceRootSessions: false; maximumPageSize: number;
}
export interface HumanSigninEntry {
  entryId: string; kind: 'oidc-session' | 'remembered-consent';
  clientId: string; clientLabel: string; deviceId: string; currentDevice: boolean;
  clientGeneration: number; createdAt: number; expiresAt: number | null;
  status: 'active' | 'expired' | 'device-revoked' | 'client-changed';
  renewable: boolean; remembered: boolean; revision: string;
  revokeScope: 'exact-session' | 'app-device-version'; revokeRequestId: string;
}
export interface HumanSigninList {
  schema: 'soty.human-signins.v1'; scope: HumanSigninScope;
  capabilities: HumanSigninCapabilities; entries: HumanSigninEntry[]; nextCursor: string | null;
}
export interface HumanSigninRevoke {
  schema: 'soty.human-signin-revocation.v1'; scope: HumanSigninScope;
  entryId: string; requestId: string; kind: HumanSigninEntry['kind'];
  revokeScope: HumanSigninEntry['revokeScope']; outcome: 'revoked' | 'already_revoked'; revokedAt: number;
}
export interface HumanSigninStatus {
  schema: 'soty.human-signin-status.v1'; scope: HumanSigninScope;
  entryId: string; requestId: string; status: 'active' | 'revoked' | 'unavailable'; revokedAt: number | null;
}
