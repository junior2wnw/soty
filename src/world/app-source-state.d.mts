import type { AppInspection, AppSourceDraft, AppSourcePreparation, AppSourceTarget, SettingsPending } from './app-settings.types';
import type { WorldApi } from './types';
export interface AppSourceIntent {
  op: 'apps.source.promote'; args: Record<string, unknown>; expectedSource: AppSourceTarget;
  /** Synchronous and ephemeral; called inside the settings Web Lock only before a new durable command. */
  beforeCreate(): true;
}
export interface AppSourceStateView {
  snapshot: AppInspection; base: AppInspection; draft: AppSourceDraft; dirty: boolean; conflict: boolean;
  publicationDirty: boolean; preparing: boolean; preparation: AppSourcePreparation | null; remainingMs: number;
  canPrepare: boolean; canPromote: boolean; canRecheckPromote: boolean;
  history: { loading: boolean; targets: (AppSourceTarget & { createdAt: number })[]; nextCursor: string | null; stale: boolean };
}
export interface AppSourceState {
  read(): AppSourceStateView;
  patch(value: Partial<Pick<AppSourceDraft, 'hostDeviceId' | 'connectorId' | 'port' | 'entryPath' | 'launchPolicy' | 'exposureConfirmed'>>): void;
  selectHistory(target: AppSourceTarget | (AppSourceTarget & { createdAt: number })): void;
  observe(next: AppInspection, options?: { publicationDirty?: boolean; completed?: SettingsPending }): void;
  reset(): void;
  invalidatePreparation(reason?: string): void;
  prepare(options: { api: WorldApi; isCurrent(): boolean }): Promise<{ status: 'ready' | 'stale' }>;
  promoteIntent(): AppSourceIntent;
  recheckPromotion(options: { api: WorldApi; isCurrent(): boolean }): Promise<{ status: 'ready'; intent: AppSourceIntent } | { status: 'review' | 'stale' }>;
  loadHistory(options: { api: WorldApi; isCurrent(): boolean; older?: boolean }): Promise<'accepted' | 'stale'>;
  dispose(): void;
}
export function createAppSourceState(options: { accountId: string; appId: string; snapshot: AppInspection; now?: () => number }): AppSourceState;
export function normalizeAppSourceTarget(value: unknown): AppSourceTarget;
export function appSourcePreparationRemaining(preparation: Pick<AppSourcePreparation, 'checkedAt' | 'expiresAt'>, requestElapsedMs: number): number;
