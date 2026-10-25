import type { AppEntry } from './app-saved-state.mjs';
import type { WorldApi } from './types';
import type { AppLaunchTarget } from './app-launch.mjs';
export interface AppDiscussionInboxRow {
  appId: string; name: string; entry: AppEntry; conversationId: string | null; unreadCount: number | null;
  lastMessage: null | { id: string; author: { accountId: string; label: string }; preview: string; createdAt: number };
}
export function appDiscussionInboxUnsupported(error: unknown): boolean;
export function recentAppDiscussionTargets(recent: readonly { route: string }[]): AppLaunchTarget[];
export function loadAppDiscussionInbox(options: { api: WorldApi; accountId: string; entries: readonly AppEntry[]; isCurrent(): boolean }): Promise<null | { supported: boolean; discussions: AppDiscussionInboxRow[]; checkedAt: number | null; readState?: 'synced' | 'unavailable' }>;
export interface AppDiscussionRead { appId: string; conversationId: string; messageId: string; unreadCount: number; readAt: number; }
export function markAppDiscussionRead(options: { api: WorldApi; accountId: string; entry: AppEntry; conversationId: string; messageId: string; isCurrent(): boolean }): Promise<AppDiscussionRead | null>;
