import type { WorldApi, WorldCommunity } from './types';
export interface AppInspection {
  schema: 'soty.app-inspection.v1';
  checkedAt: number;
  app: { id: string; name: string; state: 'enabled' | 'revoked'; revision: number; grants: { accountIds: string[]; communityIds: string[] } };
  addresses: {
    revision: number; claimOrigin: string | null;
    canonical: { id: string; origin: string; shareUrl: string | null } | null;
    aliases: { id: string; slug: string; origin: string; state: 'bound' | 'tombstone'; active: boolean; shareUrl: string | null; createdAt: number; retiredAt: number | null }[];
    limits: { perApp: number; perAccount: number; usedByApp: number; usedByAccount: number };
  };
  publication: { policyEpoch: number; launchPolicy: 'restricted' | 'anyone'; listed: boolean; activeDomainIds: string[]; activeTargetRevision: number };
  source: { hostDeviceId: string; connectorId: string; deviceName: string; port: number; entryPath: string; revision: number; digest: string; profile: string;
    observation: { state: 'offline' | 'unknown' | 'responding' | 'unreachable'; observedAt: number | null; freshUntil: number | null; evidence: 'connector-offline' | 'not-observed' | 'connector-v1-observation' | 'connector-v2-observation' } };
  actions: { canReserveName: boolean; canEdit: boolean; canPublish: boolean; canPreview: boolean };
}
export interface SettingsDraft { name: string; communityIds: string[]; launchPolicy: 'restricted' | 'anyone'; activeDomainIds: string[]; exposureConfirmed: boolean; slug: string }
export interface SettingsPending { op: 'apps.publication.update' | 'apps.domains.claim' | 'apps.domains.retire'; args: Record<string, unknown> & { appId: string; expectedAccountId: string; requestId: string } }
export interface SettingsResponse { requestId: string; replayed: boolean; receipt: Record<string, unknown>; current?: { appId: string; policyEpoch: number; launchPolicy: string; activeDomainIds: string[]; appState: string } }
export interface AppSettingsOptions {
  host: HTMLElement; accountId: string; appId: string; api: WorldApi; communities: WorldCommunity[];
  isCurrent(): boolean; onChanged(snapshot: AppInspection): void;
  onPreview(target: { domainId: string; path: string }): void;
  onClose(): void;
}
