export interface LocalCoverRaster { readonly width: number; readonly height: number; readonly mime: string; readonly url: string; readonly byteCount: number; base64(): string; dispose(): void }
export const COVER_FILE_LIMIT: number;
export const COVER_DECODE_PIXELS: number;
export function inspectCoverRaster(bytes: Uint8Array, mime: string): { width: number; height: number };
export function coverCropRect(width: number, height: number, crop?: { x: number; y: number; zoom: number }): { left: number; top: number; size: number };
export function createCoverRasterPreparer(platform: { decode(blob: Blob): Promise<ImageBitmap>; encode(bitmap: ImageBitmap, size: { width: number; height: number }, quality: number, kind: 'app' | 'profile'): Promise<Blob | null>; createUrl(blob: Blob): string; revokeUrl(url: string): void }): (file: File, isCurrent: () => boolean, kind?: 'app' | 'profile') => Promise<LocalCoverRaster>;
export function prepareCoverFile(file: File, isCurrent: () => boolean, kind?: 'app' | 'profile'): Promise<LocalCoverRaster>;
