import type { WorldApi } from './types';
import type { AppEntry } from './app-saved-state.mjs';
export interface DiscussionScope { accountId: string; appId: string; conversationId: string; entry: AppEntry; }
export interface DiscussionDraft { text: string; replyTo: string | null; revision: string; }
export interface DiscussionPending {
  scope: DiscussionScope; draftRevision: string;
  args: { expectedAccountId: string; appId: string; conversationId: string; domainId: string; path: string; requestId: string; body: string; replyTo?: string };
}
export interface DiscussionContext {
  appId: string; entry: AppEntry | null; conversationId: string; mode: 'current' | 'archive'; isCurrent: boolean;
  ownerAdministrative: boolean; audience: 'public' | 'shared' | 'owner'; canPost: boolean; canModerate: boolean;
}
export interface DiscussionMessage {
  id: string; conversationId: string; author: { accountId: string; label: string }; body: string | null;
  replyTo: string | null; createdAt: number; removedAt: number | null; canRemove: boolean;
  /** Local verified remove receipt, while the actual server timestamp has not been fetched. */
  removalConfirmed?: true;
}
export interface DiscussionSendResponse {
  requestId: string; replayed: boolean; receipt: { id: string; conversationId: string; createdAt: number };
  ownCurrent: { removed: boolean }; message: DiscussionMessage | null;
}
export interface RetainedDiscussionDraft { scope: DiscussionScope; draft: DiscussionDraft; pending: DiscussionPending | null; }
export interface AppDiscussionDraftState {
  key: string; scope: DiscussionScope;
  read(): { draft: DiscussionDraft; pending: DiscussionPending | null; durable: boolean; conflict: boolean; remoteDraft: DiscussionDraft | null; error: string | null };
  readRetained(): RetainedDiscussionDraft[];
  edit(value: { text: string; replyTo: string | null }): void;
  flush(): Promise<boolean>; hasUnsavedChanges(): boolean;
  refreshLocal(): void; subscribe(listener: () => void): () => void; dispose(): void;
  prepareSend(expectedDraft: DiscussionDraft, beforeCreate: () => boolean): Promise<DiscussionPending>;
  pendingForDispatch(expectedPending: DiscussionPending): Promise<DiscussionPending>;
  acknowledge(expected: DiscussionPending, response: unknown): Promise<boolean>;
  abandon(expected: DiscussionPending): Promise<boolean>;
  discardDraft(expected: DiscussionDraft): Promise<boolean>;
  chooseRemote(expectedRemoteRevision: string | null): Promise<boolean>;
  keepMine(expectedRemoteRevision: string | null): Promise<boolean>;
}
export function createAppDiscussionDraftState(options: { scope: DiscussionScope; storage: Pick<Storage, 'getItem' | 'setItem'>;
  locks?: Pick<LockManager, 'request'>; randomId?: () => string }): AppDiscussionDraftState;
export function listAppDiscussionDrafts(options: { accountId: string; appId: string; domainId: string; path: string; origin?: string;
  storage: Pick<Storage, 'getItem'> }): RetainedDiscussionDraft[];
export function dispatchAppDiscussionIntent(options: { state: AppDiscussionDraftState; api: WorldApi; isCurrent(): boolean } & (
  { expectedDraft: DiscussionDraft; beforeCreate(): boolean; expectedPending?: never }
  | { expectedDraft?: never; beforeCreate?: never; expectedPending: DiscussionPending }
)): Promise<{ status: 'accepted' | 'stale' | 'superseded'; pending?: DiscussionPending; response?: DiscussionSendResponse }>;
export interface AppDiscussionFeed {
  read(): { context: DiscussionContext | null; messages: DiscussionMessage[]; historyCursor: string | null; changeCursor: string | null;
    archives: DiscussionContext[]; nextArchiveCursor: string | null; loading: boolean; stale: boolean; resetRequired: boolean;
    hasNewer: boolean; window: 'latest' | 'history'; windowRevision: number; error: string | null };
  subscribe(listener: () => void): () => void;
  load(options: { api: WorldApi; isCurrent(): boolean; conversationId?: string }): Promise<'accepted' | 'stale'>;
  older(options: { api: WorldApi; isCurrent(): boolean }): Promise<'accepted' | 'stale' | 'reset'>;
  poll(options: { api: WorldApi; isCurrent(): boolean }): Promise<'accepted' | 'stale' | 'reset'>;
  archives(options: { api: WorldApi; isCurrent(): boolean; older?: boolean }): Promise<'accepted' | 'stale' | 'reset'>;
  acceptSend(expected: DiscussionPending, response: unknown): boolean;
  acceptRemoval(response: { id: string; conversationId: string; removed: true }): boolean;
  invalidate(): void; dispose(): void;
}
export function createAppDiscussionFeed(options: { accountId: string; appId: string; entry: AppEntry | null; administrative?: true }): AppDiscussionFeed;
