import type { AppEntry } from './app-saved-state.mjs';
import type { WorldApi } from './types';
export interface AppDiscussionInboxRow {
  appId: string; name: string; entry: AppEntry; conversationId: string | null; unreadCount: number | null;
  source: 'requested' | 'known';
  lastMessage: null | { id: string; author: { accountId: string; label: string }; preview: string; createdAt: number };
}
export function appDiscussionInboxUnsupported(error: unknown): boolean;
export function loadAppDiscussionInbox(options: { api: WorldApi; accountId: string; entries?: readonly AppEntry[]; cursor?: string | null; maxKnownPages?: number; isCurrent(): boolean }): Promise<null | {
  supported: boolean; discussions: AppDiscussionInboxRow[]; checkedAt: number | null; readState?: 'synced' | 'unavailable';
  nextCursor: string | null; knownPages: number; resetRequired: boolean;
}>;
export interface AppDiscussionRead { appId: string; conversationId: string; messageId: string; unreadCount: number; readAt: number; }
export function markAppDiscussionRead(options: { api: WorldApi; accountId: string; entry: AppEntry; conversationId: string; messageId: string; isCurrent(): boolean }): Promise<AppDiscussionRead | null>;
