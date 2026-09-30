export type CommunityJoinPolicy = 'open' | 'request' | 'invite';
export type CommunityRole = 'owner' | 'moderator' | 'member';
export type MembershipState = 'active' | 'requested' | 'invited' | 'left' | 'removed' | 'banned' | 'declined';
export interface WorldProfile {
  profileId: string;
  displayName: string;
  bio: string;
  interests: string[];
  avatarColor: string;
  avatarRevision: number | null;
  revision: number;
  discoverable?: boolean;
  showPresence?: boolean;
  showMemberships?: boolean;
  contactPolicy?: 'everyone' | 'members' | 'nobody';
}
export interface WorldMembership {
  state: MembershipState;
  role: CommunityRole;
  revision: number;
  pinned: boolean;
  muted: boolean;
  showInProfile: boolean;
  joinedAt: number | null;
}
export interface WorldAvatar {
  profileId: string;
  avatarUrl: string | null;
  avatarRevision: number | null;
}
export interface WorldCommunity {
  communityId: string;
  name: string;
  description: string;
  topics: string[];
  joinPolicy: CommunityJoinPolicy;
  showMembers: boolean;
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
  createdAt: number;
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
export interface WorldSearchResult {
  people: WorldProfile[];
  communities: WorldCommunity[];
  nextCursor: string | null;
  totals: { people: number; communities: number; peopleExact?: boolean; communitiesExact?: boolean };
}
export interface WorldActor { accountId: string; deviceId: string; label: string }
export interface MembershipEvent { communityId: string; profileId: string | null; state: MembershipState | 'archived'; revision: number }
export interface WorldService {
  projectId: string;
  schemaVersion: number;
  operations: Set<string>;
  execute(input: { op: string; args?: Record<string, unknown>; actor: Readonly<WorldActor> }): Record<string, unknown>;
  /** Host-only synchronous boundary; callback must not return a Promise or mutate World. */
  withCommunityAuthorityFence<T>(callback: () => T extends PromiseLike<unknown> ? never : T): T;
  canAccessCommunity(accountId: string, communityId: string): boolean;
  activeCommunityIds(accountId: string): string[];
  /** Only inside the host authority fence; returns a subset of the bounded candidates. */
  appCommunityAuthority(accountId: string, ownerAccountId: string, relevantCommunityIds: readonly string[]): readonly string[];
  isGroupAdmin(accountId: string, communityId: string): boolean;
  canRequestContact(accountId: string, targetId: string): boolean;
  subscribeMembership(listener: (event: MembershipEvent) => unknown): () => void;
  close(): void;
}
export declare function createWorldService(options: { databasePath: string; projectId: string; clock?: () => number }): WorldService;
