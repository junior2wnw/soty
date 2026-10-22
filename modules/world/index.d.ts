import type { FieldDocument, FieldEntityRef } from '../field/contract.mjs';
export type { FieldDocument, FieldEntityRef } from '../field/contract.mjs';

export type CommunityJoinPolicy = 'open' | 'request' | 'invite';
export type CommunityRole = 'owner' | 'moderator' | 'member';
export type MembershipState = 'active' | 'requested' | 'invited' | 'left' | 'removed' | 'banned' | 'declined';
export type WorldProfileAudience = 'private' | 'friends' | 'link' | 'public';
export interface WorldIdentity {
  profileId: string;
  displayName: string;
  avatarColor: string;
  avatarRevision: number | null;
  revision: number;
}
export interface WorldProfile extends WorldIdentity {
  bio: string;
  interests: string[];
  audience?: WorldProfileAudience;
  audienceWriteVersion?: 1;
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
  previewMembers: WorldIdentity[];
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
  author: WorldIdentity;
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
export interface WorldMember {
  profile: WorldIdentity;
  state: MembershipState;
  role: CommunityRole;
  revision: number;
  joinedAt: number | null;
}
export interface WorldActor { accountId: string; deviceId: string; label: string }
export interface WorldProfileAudienceUpdateArgs {
  expectedAccountId: string;
  requestId: string;
  expectedRevision: number;
  audience: WorldProfileAudience;
}
export interface WorldProfileAudienceReceipt {
  schema: 'soty.world-profile-audience.receipt.v1';
  requestId: string;
  accountId: string;
  audience: WorldProfileAudience;
  expectedRevision: number;
  acceptedRevision: number;
}
export interface WorldProfileAudienceUpdateResult { profile: WorldProfile; replayed: boolean; receipt: WorldProfileAudienceReceipt }
export interface WorldFieldGetArgs { expectedAccountId: string }
export interface WorldFieldPutArgs extends WorldFieldGetArgs { expectedRevision: number; requestId: string; document: FieldDocument; contentHash?: string }
export interface WorldFieldEnvelope { revision: number; document: FieldDocument; contentHash: string; updatedAt: number }
export interface WorldFieldPutResult { replayed: boolean; receipt: { revision: number; contentHash: string; committedAt: number }; current: WorldFieldEnvelope }
export interface WorldDirectorySearchArgs { expectedAccountId: string; query?: string; kind?: 'all' | 'people' | 'communities'; scope?: 'all' | 'mine' | 'public'; limit?: number; cursor?: string }
export interface WorldDirectorySearchResult { schema: 'soty.world-directory.v1'; people: WorldProfile[]; communities: WorldCommunity[]; nextCursor: string | null }
export type WorldDirectoryRef = Pick<FieldEntityRef, 'id'> & { kind: 'person' | 'community' };
export interface WorldDirectoryResolveArgs { expectedAccountId: string; entities: WorldDirectoryRef[] }
export type WorldDirectoryResolveItem = { ref: WorldDirectoryRef; available: false }
  | { ref: WorldDirectoryRef; available: true; access: 'owner' | 'friends' | 'link' | 'public'; person: WorldProfile }
  | { ref: WorldDirectoryRef; available: true; access: 'member'; person: WorldIdentity }
  | { ref: WorldDirectoryRef; available: true; community: WorldCommunity };
export interface WorldDirectoryResolveResult { schema: 'soty.world-directory.v1'; items: WorldDirectoryResolveItem[] }
export interface WorldFieldOperationMap {
  'world.field.get': { args: WorldFieldGetArgs; result: WorldFieldEnvelope };
  'world.field.put': { args: WorldFieldPutArgs; result: WorldFieldPutResult };
  'world.directory.search': { args: WorldDirectorySearchArgs; result: WorldDirectorySearchResult };
  'world.directory.resolve': { args: WorldDirectoryResolveArgs; result: WorldDirectoryResolveResult };
}
export interface MembershipEvent { communityId: string; profileId: string | null; state: MembershipState | 'archived'; revision: number }
export interface WorldMessageEvent { kind: 'room' | 'direct'; conversationId: string; messageId: string }
export type WorldNotificationTarget = { kind: 'room'; communityId: string }
  | { kind: 'direct'; threadId: string; peerAccountId: string; relationshipId: string };
export interface WorldCurrentContacts {
  readonly accountId: string;
  readonly deviceId: string;
  has(peerAccountId: string): boolean;
  accountIds(): readonly string[];
}
export interface WorldService {
  projectId: string;
  schemaVersion: number;
  operations: Set<string>;
  execute<K extends keyof WorldFieldOperationMap>(input: { op: K; args: WorldFieldOperationMap[K]['args']; actor: Readonly<WorldActor> }): WorldFieldOperationMap[K]['result'];
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
  subscribeMessages(listener: (event: WorldMessageEvent) => unknown): () => void;
  notificationRecipients(event: WorldMessageEvent): string[];
  canNotify(actor: Readonly<Pick<WorldActor, 'accountId' | 'deviceId'>>, event: WorldMessageEvent): WorldNotificationTarget | null;
  close(): void;
}
export declare function createWorldService(options: { databasePath: string; projectId: string; clock?: () => number;
  allowAudienceMigration?: boolean;
  withCurrentContacts?: <T>(actor: Readonly<Pick<WorldActor, 'accountId' | 'deviceId'>>, callback: (view: Readonly<WorldCurrentContacts>) => T) => T }): WorldService;
