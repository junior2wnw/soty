import type { NoteCache } from './cache.mjs';
import type {NativeDraftStore} from './native-local.mjs';
export type NoteState = 'active' | 'archived' | 'trashed';
export type NoteColor = 'plain' | 'honey' | 'sage' | 'lilac' | 'blue' | 'coral';
export interface NoteItem { id: string; text: string; done: boolean }
export interface NoteDocument { title: string; body: string; items: NoteItem[]; color: NoteColor; pinned: boolean; state: NoteState }
export interface NoteMetadata { noteId: string; title: string; preview: string; color: NoteColor; pinned: boolean; state: NoteState; revision: number; createdAt: number; updatedAt: number }
export interface Note extends NoteMetadata { body: string; items: NoteItem[] }
export interface NotesList { notes: NoteMetadata[]; nextCursor: string | null; usage: { bytes: number; maxBytes: number; maxNotes: number; counts: Record<NoteState, number> } }
export interface Draft { note: Note; branchId: string; generation: number; committedGeneration: number; pending: { mutationId: string; expectedRevision: number; generation: number; document: NoteDocument } | null; conflict: boolean; savedAt: number; retiredMutations?: {mutationId:string;generation:number}[] }
export interface DraftStore { put(draft: Draft): Promise<void>; list(): Promise<Draft[]>; remove(noteId: string, branchId: string, savedAt?: number): Promise<void>;native?:Pick<NativeDraftStore['native'],'register'|'sessions'> }
export interface SessionState { note: Note; dirty: boolean; localDurable: boolean; saving: boolean; conflict: boolean; error: string; localError: string; cacheError: string; confirmedAt: number; branchId: string;localProtection?:'pending'|'sealed' }
export interface NoteSession { state(): SessionState; edit(patch: Partial<NoteDocument>): void; retry(): Promise<void>; keepLocal():Promise<void>; flush(): Promise<void>; flushLocal():Promise<void>; hasUnsavedChanges(): boolean; discardBranch(): Promise<void>; dispose(): void }
export const NOTE_COLORS: readonly NoteColor[];
export function uid(): string;
export function blankNote(): Note;
export function noteErrorCode(error: unknown): string;
export function createDraftStore(accountId: string, projectId?: string, options?:{origin?:string;indexedDB?:IDBFactory}): NativeDraftStore;
export function createNoteSession(options: { api: { request<T>(method: string, params?: Record<string, unknown>): Promise<T> }; accountId: string; store: DraftStore; note: Note; draft?: Draft; cache?: NoteCache; onChange?: (state: SessionState) => void; debounceMs?: number }): NoteSession;
