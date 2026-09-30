import type { AppLaunchRequest, AppResolvedEntry } from './app-launch.mjs';

export type WorldColor = 'honey' | 'sage' | 'lilac' | 'coral' | 'blue';
export interface WorldProfile {
  profileId: string;
  displayName: string;
  bio: string;
  interests: string[];
  avatarColor: string;
  avatarUrl?: string;
  avatarRevision?: number | null;
  revision: number;
  discoverable?: boolean;
  showPresence?: boolean;
  showMemberships?: boolean;
  contactPolicy?: 'everyone' | 'members' | 'nobody';
}
export interface WorldMembership {
  state: 'active' | 'requested' | 'invited' | 'left' | 'removed' | 'banned';
  role: 'owner' | 'moderator' | 'member';
  pinned: boolean;
  muted: boolean;
  showInProfile: boolean;
  revision: number;
}
export interface WorldCommunity {
  communityId: string;
  name: string;
  description: string;
  topics: string[];
  joinPolicy: 'open' | 'request' | 'invite';
  showcase: string;
  symbol: string;
  color: string;
  revision: number;
  memberCount: number;
  previewMembers: WorldProfile[];
  membership: WorldMembership | null;
  permissions: { canManage: boolean; canModerate: boolean; canWrite: boolean };
  unreadCount: number;
  pendingCount?: number;
}
export interface WorldMessage {
  messageId: string;
  seq: number;
  text: string;
  author: WorldProfile;
  createdAt: number;
  replyTo: string | null;
  removed: boolean;
}
export interface WorldMember extends WorldMembership { profile: WorldProfile }
export interface WorldSearch { people: WorldProfile[]; communities: WorldCommunity[]; nextCursor: string | null; totals: { people: number; communities: number; peopleExact?: boolean; communitiesExact?: boolean } }
export interface WorldApi { request<T>(method: string, params?: Record<string, unknown>): Promise<T> }
export interface WorldDevice { deviceId: string; label: string; state: string; }
export interface WorldAppRecord {
  appId: string;
  name: string;
  description?: string;
  deviceLabel?: string;
  deviceId?: string;
  communityId?: string;
  status: string;
  audience?: string;
  symbol?: string;
  color?: string;
  ownerAccountId?: string;
  grants?: { accountIds: string[]; communityIds: string[] };
}
export interface WorldAssistantHandle {
  dispose(): void;
  refresh?(): void | Promise<void>;
  flush?(): Promise<void>;
  hasUnsavedChanges?(): boolean;
}
export interface WorldAppOptions {
  api: WorldApi;
  localAccount?: () => Promise<{ accountId: string | null; label: string }>;
  openLegacy: (tool?: 'notes' | 'files' | 'chess' | 'terminal' | 'internet') => void | Promise<void>;
  openAccount: (tab?: 'profile' | 'people' | 'devices' | 'recovery') => void | Promise<void>;
  connectDevice: () => void | Promise<void>;
  agentCreate: (communityId?: string) => void | Promise<void>;
  openAssistant?: (host: HTMLElement) => WorldAssistantHandle | Promise<WorldAssistantHandle>;
  accessAvailability?: () => Promise<{ notesCreateEnabled: boolean; audience: string | null }>;
  requestContact?: (profile: WorldProfile) => void | Promise<void>;
  listDevices?: () => Promise<WorldDevice[]>;
  listApps?: (communityId?: string) => Promise<WorldAppRecord[]>;
  addApp?: (communityId?: string) => void | Promise<void>;
  openApp?: (app: WorldAppRecord, launch?: AppLaunchRequest) => Promise<{ url: string; entry: AppResolvedEntry; status?: string }>;
}
export type WorldEntity = { type: 'community'; value: WorldCommunity } | { type: 'person'; value: WorldProfile };

export function entityId(entity: WorldEntity): string { return entity.type === 'community' ? `community:${entity.value.communityId}` : `person:${entity.value.profileId}`; }
export function entityName(entity: WorldEntity): string { return entity.type === 'community' ? entity.value.name : entity.value.displayName; }
export function worldColor(value: string | undefined): WorldColor {
  if ((['honey', 'sage', 'lilac', 'coral', 'blue'] as string[]).includes(value ?? '')) return value as WorldColor;
  if (!/^#[0-9a-f]{6}$/i.test(value ?? '')) return 'honey';
  const rgb = [1, 3, 5].map(offset => parseInt(value!.slice(offset, offset + 2), 16));
  return (Object.entries(worldColors) as [WorldColor, string][]).sort((a, b) => {
    const score = (hex: string): number => [1, 3, 5].reduce((sum, offset, index) => sum + Math.pow(parseInt(hex.slice(offset, offset + 2), 16) - rgb[index]!, 2), 0);
    return score(a[1]) - score(b[1]);
  })[0]![0];
}
export const worldColors: Record<WorldColor, string> = { honey: '#DDA149', sage: '#81A386', lilac: '#A68ACF', coral: '#DF8D7E', blue: '#84A6BD' };
