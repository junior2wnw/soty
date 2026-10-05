export interface AppArtPalette { readonly base: string; readonly accent: string; readonly ink: string; }
export interface AppArt {
  readonly key: string | null;
  readonly status: 'ready' | 'fallback';
  readonly src: string | null;
  readonly srcset: string;
  readonly sizes: string;
  readonly width: number;
  readonly height: number;
  readonly alt: string;
  readonly palette: AppArtPalette;
  readonly focalPoint: { readonly x: number; readonly y: number };
  readonly compactFocalPoint: { readonly x: number; readonly y: number };
  readonly version: number | null;
}
export type AppArtInput = string | { appId?: string; id?: string; coverKey?: string } | null | undefined;
export function resolveAppArt(input: AppArtInput): AppArt;
export function createAppArtResolver(manifest: unknown): (input: AppArtInput) => AppArt;
export function installAppArtFallback(image: HTMLImageElement, onFallback?: (image: HTMLImageElement) => void): () => void;
