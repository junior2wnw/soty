import type { WorldApi } from './types';
export interface AppEntry { appId: string; domainId: string; origin: string; path: string; }
export interface SavedEntry extends AppEntry {
  label: string; savedRevision: number; updatedAt: number;
  current: { name: string; status: 'ready' | 'starting' | 'offline' | 'stopped'; canManage: boolean } | null;
}
export interface SavedSnapshot { revision: number; entry: SavedEntry | null; }
export interface SavedPending {
  op: 'apps.saved.set'; entry: AppEntry;
  args: { appId: string; expectedAccountId: string; saved: boolean; expectedRevision: number; requestId: string; domainId?: string; path?: string };
}
export interface SavedIntent { entry: AppEntry; saved: boolean; expectedRevision: number; currentEntry: SavedEntry | null; replace?: true; }
export interface SavedResponse { requestId: string; replayed: boolean; receipt: { appId: string; saved: boolean; revision: number; committedAt: number }; current: SavedSnapshot; }
export interface AppSavedState {
  key: string; read(): { revision: number; pending: SavedPending | null };
  subscribe(listener: () => void): () => void; refreshLocal(): void; dispose(): void;
  prepare(intent: SavedIntent): Promise<SavedPending>;
  pendingForDispatch(expected: SavedPending): Promise<SavedPending>;
  acknowledge(expected: SavedPending, response: unknown): Promise<boolean>;
  /** Forgets only local tracking; never claims that a server effect was cancelled. */
  abandon(expected: SavedPending): Promise<boolean>;
}
export function normalizeAppEntry(value: unknown): AppEntry;
export function normalizeAppSavedSnapshot(value: unknown, appId: string): SavedSnapshot;
export function createAppSavedState(options: { accountId: string; storage: Pick<Storage, 'getItem' | 'setItem'>;
  locks?: Pick<LockManager, 'request'>; randomId?: () => string }): AppSavedState;
export function dispatchAppSavedIntent(options: { state: AppSavedState; api: WorldApi; isCurrent(): boolean } & (
  { intent: SavedIntent; expectedPending?: never } | { intent?: never; expectedPending: SavedPending }
)): Promise<{ status: 'accepted' | 'stale' | 'superseded'; pending?: SavedPending; response?: SavedResponse }>;
export interface AppSavedLibraryState {
  read(): { entries: SavedEntry[]; revision: number | null; nextCursor: string | null; loading: boolean; stale: boolean; resetRequired: boolean; error: string | null };
  subscribe(listener: () => void): () => void;
  load(options: { api: WorldApi; isCurrent(): boolean; older?: boolean }): Promise<'accepted' | 'stale' | 'reset'>;
  invalidate(): void; dispose(): void;
}
export function createAppSavedLibraryState(options: { accountId: string }): AppSavedLibraryState;
