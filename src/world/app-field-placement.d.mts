import type { FieldDocument, FieldEntityRef } from '../../modules/field/contract.mjs';
import type { FieldPersistenceState, createFieldPersistence } from './field-persistence';
export interface FieldPlacementResult { document: FieldDocument; changed: boolean; shortcutId: string; createdContext: boolean }
export function createPersonalFieldDocument(options?: {includeBuiltins?: boolean; initialAppIds?: readonly string[]}): FieldDocument;
export function ensureFieldEntityPlacement(document: FieldDocument, options: {entity: FieldEntityRef; contextId: string; shortcutId: string;
  initializePersonal?: boolean; includeBuiltins?: boolean; initialAppIds?: readonly string[]}): FieldPlacementResult;
export interface AppFieldPlacementSnapshot {
  appId: string; generation: number; revision: number; projectedRevision: number;
  persistence: FieldPersistenceState['state']; localDurable: boolean; pendingCount: number; errorCode?: string;
  contexts: {contextId: string; title: string; placed: boolean; willCreate: boolean}[];
  placements: {contextId: string; shortcutId: string}[];
}
export interface AppFieldPlacementResult {
  status: 'saved' | 'pending' | 'conflict' | 'storage-error'; changed: boolean; shortcutId: string | null;
  snapshot: AppFieldPlacementSnapshot; errorCode?: string;
}
export interface AppFieldPlacementController {
  load(signal?: AbortSignal): Promise<AppFieldPlacementSnapshot>;
  refresh(signal?: AbortSignal): Promise<AppFieldPlacementSnapshot>;
  place(options: {contextId: string; generation: number; signal?: AbortSignal}): Promise<AppFieldPlacementResult>;
  retry(signal?: AbortSignal): Promise<AppFieldPlacementSnapshot>;
  getSnapshot(): AppFieldPlacementSnapshot | null;
  subscribe(listener: (snapshot: AppFieldPlacementSnapshot) => void): () => void;
  hasUnsavedChanges(): boolean;
  flush(): Promise<AppFieldPlacementSnapshot>;
  dispose(): void;
}
export function createAppFieldPlacementController(options: {persistence: ReturnType<typeof createFieldPersistence>; accountId: string; appId: string;
  isCurrent?: () => boolean; signal?: AbortSignal; randomId?: () => string; initialAppIds?: readonly string[]}): AppFieldPlacementController;
