import type { CoverRead, CoverImage } from './cover-api.mjs';
import type { CoverPreview } from './cover-editor-state.mjs';
export type CoverVisual = { mode: 'loading' | 'inherit' | 'none'; revision?: number } | { mode: 'custom'; url: string; revision: number };
export interface AppCoverBinding { setVisible(visible: boolean): void; refresh(): void; dispose(): void }
export interface AppCoverPresenter {
  watch(options: { appId: string; identity: string; isCurrent(): boolean; onVisual(value: CoverVisual): void }): AppCoverBinding | null;
  invalidate(appId: string): void; invalidateAll(): void; refreshVisible(): void;
  stats(): { running: number; queued: number; targets: number; bindings: number; bytes: number }; dispose(): void;
}
export function createAppCoverPresenter(options: { accountId: string; isCurrent(): boolean; readApp(appId: string, width: number): Promise<CoverRead>; makeImage?(image: CoverImage): CoverPreview;
  now?: () => number; maximumTargets?: number; maximumBindings?: number; maximumBytes?: number }): AppCoverPresenter;
export function mountAppCoverField(options: { root: HTMLElement; presenter: AppCoverPresenter; isCurrent(): boolean; Observer?: typeof IntersectionObserver; Mutations?: typeof MutationObserver; maximumNodes?: number }): { dispose(): void };
