import type { AppInspection, AppSourceTarget, SettingsDraft, SettingsPending, SettingsResponse } from './app-settings.types';
import type { WorldApi } from './types';
export interface AppSettingsState {
  key: string;
  read(): { schema: 1; accountId: string; appId: string; revision: number; pending: SettingsPending | null };
  canDispatch(): boolean;
  prepare(input: { op: SettingsPending['op']; args: Record<string, unknown>; expectedSource?: AppSourceTarget; beforeCreate?: () => boolean }): Promise<SettingsPending>;
  pendingForDispatch(expectedPending: SettingsPending): Promise<SettingsPending>;
  acknowledge(expected: SettingsPending, response: SettingsResponse): Promise<boolean>;
  abandon(expected: SettingsPending): Promise<boolean>;
}
export function createAppSettingsState(options: {
  accountId: string; appId: string; storage: Pick<Storage, 'getItem' | 'setItem'>;
  locks?: Pick<LockManager, 'request'>; randomId?: () => string;
}): AppSettingsState;
export function dispatchAppSettingsIntent(options: { state: AppSettingsState; api: WorldApi; isCurrent(): boolean } & (
  { op: SettingsPending['op']; args: Record<string, unknown>; expectedSource?: AppSourceTarget; beforeCreate?: () => boolean; expectedPending?: never }
  | { op?: undefined; expectedPending: SettingsPending; args?: never; expectedSource?: never; beforeCreate?: never }
)): Promise<{ status: 'accepted' | 'stale' | 'superseded'; pending?: SettingsPending; response?: SettingsResponse }>;
export function appSettingsUpdateArgs(snapshot: AppInspection, draft: Pick<SettingsDraft, 'name' | 'communityIds'>, kind: 'name' | 'grants', accountId: string): Record<string, unknown>;
export function appPublicationArgs(snapshot: AppInspection, draft: Pick<SettingsDraft, 'launchPolicy' | 'activeDomainIds' | 'exposureConfirmed'> & { listed?: boolean }, accountId: string): Record<string, unknown>;
export function appSettingsObservationRemaining(snapshot: AppInspection, requestElapsedMs: number): number;
export function createAppSettingsDraftState(initial: AppInspection): {
  read(): { snapshot: AppInspection; draft: SettingsDraft; nameDirty: boolean; grantsDirty: boolean; publicationDirty: boolean; nameConflict: boolean; grantsConflict: boolean; publicationConflict: boolean; unavailableDomainIds: string[] };
  base(kind: 'name' | 'grants' | 'publication'): AppInspection;
  patch(value: Partial<SettingsDraft>): void;
  observe(next: AppInspection, completed?: { kind: 'name' | 'grants' | 'publication'; args: Record<string, unknown> }): void;
  reset(): void;
  resetPublication(): void;
};
