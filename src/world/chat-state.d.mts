import type { WorldMessage } from './types';
export interface ChatDraft { text: string; replyTo: string | null; attachments?: string[]; pending: { clientId: string; text: string; replyTo: string | null; attachments?: string[] } | null; }
export interface ChatDraftStore {
  read(accountId: string, communityId: string): ChatDraft;
  edit(accountId: string, communityId: string, text: string, replyTo?: string | null): boolean;
  setAttachments(accountId: string, communityId: string, attachments: string[]): boolean;
  beginSend(accountId: string, communityId: string, text: string, replyTo?: string | null, attachments?: string[]): { clientId: string; text: string; replyTo: string | null; attachments?: string[]; durable: boolean };
  recoverPending(accountId: string, communityId: string, pending: {clientId:string;text:string;replyTo?:string|null;attachments:string[]}): ChatDraft;
  discardUnsubmitted(accountId: string, communityId: string, clientId: string): ChatDraft;
  acknowledge(accountId: string, communityId: string, clientId: string, acceptedAttachments?: string[]): ChatDraft;
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
