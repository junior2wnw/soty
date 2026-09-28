import type { WorldMessage } from './types';
export interface ChatDraft { text: string; pending: { clientId: string; text: string } | null; }
export interface ChatDraftStore {
  read(accountId: string, communityId: string): ChatDraft;
  edit(accountId: string, communityId: string, text: string): boolean;
  beginSend(accountId: string, communityId: string, text: string): { clientId: string; text: string; durable: boolean };
  acknowledge(accountId: string, communityId: string, clientId: string): ChatDraft;
  retrySave(accountId: string, communityId: string): boolean;
  subscribe(accountId: string, communityId: string, listener: (draft: ChatDraft) => void): () => void;
  storageChanged(key: string | null): void;
  hasVolatile(): boolean;
  isVolatile(accountId: string, communityId: string): boolean;
}
export function createChatDraftStore(storage: Pick<Storage, 'getItem' | 'setItem' | 'removeItem'>, makeId?: () => string): ChatDraftStore;
export function readChatForward(options: {
  after: number;
  fetchPage(after: number): Promise<{ messages: WorldMessage[]; hasMore: boolean }>;
  append(message: WorldMessage): void;
  active?(): boolean;
  pageBudget?: number;
}): Promise<{ cursor: number; hasMore: boolean }>;
