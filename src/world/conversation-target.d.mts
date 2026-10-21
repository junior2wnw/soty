import type { WorldApi, WorldCommunity, WorldDirectThread } from './types';
export interface ConversationTarget {
  readonly kind: 'community' | 'direct'; readonly id: string; readonly draftKey: string; readonly avatarContext: string | undefined;
  readonly record: WorldCommunity | WorldDirectThread; readonly name: string; readonly subtitle: number | string;
  readonly canWrite: boolean; readonly canModerate: boolean;
  readonly preferences: { muted: boolean; pinned: boolean } | null;
  acceptCommunity?(next: WorldCommunity | null): void;
  refresh(): Promise<void>;
  request<T>(action: 'list' | 'send' | 'remove' | 'read', params?: Record<string, unknown>): Promise<T>;
  setPreference(key: 'muted' | 'pinned', value: boolean): Promise<void>;
  setUnread(count: number): void;
}
export function createCommunityConversation(options: { api: WorldApi; community: WorldCommunity; isCurrent?(): boolean; onUpdate?(value: WorldCommunity): void }): ConversationTarget;
export function createDirectConversation(options: { api: WorldApi; thread: WorldDirectThread; accountId: string; peerLabel?: string | (() => string); isCurrent?(): boolean; onUpdate?(value: WorldDirectThread): void }): ConversationTarget;
