import type { Note } from './state.mjs';
export interface NoteSnapshot { note: Note; verifiedAt: number }
export interface NoteCache {
  capture(): Promise<string>;
  isCurrent(token: string | null): Promise<boolean>;
  remember(note: Note, token: string | null, verifiedAt?: number): Promise<boolean>;
  get(noteId: string, token?: string): Promise<NoteSnapshot | null>;
  list(token?: string): Promise<NoteSnapshot[]>;
  remove(noteId: string): Promise<void>;
  clear(): Promise<void>;
}
export interface CachedNoteRead extends NoteSnapshot { source: 'server' | 'cache'; cached: boolean }
export const NOTE_CACHE_LIMITS: Readonly<{ scopeCount: number; scopeBytes: number; count: number; bytes: number; entryBytes: number }>;
export function createNoteCache(accountId: string, projectId?: string, options?: { origin?: string; clock?: () => number; limits?: Partial<typeof NOTE_CACHE_LIMITS> }): NoteCache;
export function noteCacheErrorPolicy(error: unknown): 'network' | 'authorization' | 'missing' | 'other';
export function invalidateNoteCache(cache: NoteCache, error: unknown, noteId?: string): Promise<'network' | 'authorization' | 'missing' | 'other'>;
export function readNoteWithCache(options: { api: { request<T>(method: string, params?: Record<string, unknown>): Promise<T> }; accountId: string; noteId: string; cache: NoteCache; allowStale?: boolean; active?: () => boolean }): Promise<CachedNoteRead>;
