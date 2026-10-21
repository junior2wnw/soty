import type { CoverApi, CoverIdentity, CoverHead, CoverVersion, CoverCrop, CoverImage } from './cover-api.mjs';
import type { LocalCoverRaster } from './cover-local-raster.mjs';
export interface CoverPreview { url: string; width: number; height: number; byteCount: number; dispose(): void }
export interface CoverEditorSnapshot {
  phase: string; busy: boolean; error: string; notice: string; current: CoverHead | null; versions: CoverVersion[];
  source: { url: string; width: number; height: number; kind: string } | null;
  crop: CoverCrop; dirty: boolean; pending: { method: string; draftId: string | null } | null; canSave: boolean; hasUnsavedChanges: boolean;
}
export interface CoverJournal { read(identity: CoverIdentity): unknown; save(value: unknown, previous?: unknown): void; clear(value: unknown): void }
export interface CoverCommitted { identity: CoverIdentity; current: CoverHead }
export interface CoverEditorState { read(): CoverEditorSnapshot; load(): Promise<void>; choose(file: File): Promise<void>; setCrop(crop: CoverCrop): void;
  previewVersion(id: string): Promise<void>; save(): Promise<void>; changeMode(mode: 'none' | 'inherit'): Promise<void>; restore(id: string): Promise<void>;
  recheck(): Promise<void>; cancelPreparation(): Promise<void>; discard(): Promise<void>; dispose(): void }
export function createCoverJournal(storage: Pick<Storage, 'getItem' | 'setItem'>, now?: () => number): CoverJournal;
export function ownedCoverImage(image: CoverImage, urls?: Pick<typeof URL, 'createObjectURL' | 'revokeObjectURL'>): CoverPreview;
export function createCoverEditorState(options: { sdk: CoverApi; prepareFile(file: File, isCurrent: () => boolean): Promise<LocalCoverRaster>; journal: CoverJournal; isCurrent(): boolean;
  onChange?(snapshot: CoverEditorSnapshot): void; onCommitted?(result: CoverCommitted): void; makeImage?(image: CoverImage): CoverPreview; now?: () => number; random?: (bytes: Uint8Array) => Uint8Array }): CoverEditorState;
