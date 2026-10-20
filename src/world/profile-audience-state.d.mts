import type { WorldApi, WorldProfile } from './types';
import type { AudienceMode } from './audience-control';
export interface ProfileAudienceIntent {
  schema: 'soty.profile-audience.intent.v1'; accountId: string; profileId: string;
  requestId: string; expectedRevision: number; audience: AudienceMode;
}
export interface ProfileAudienceState {
  profile: WorldProfile; draft: AudienceMode;
  phase: 'ready' | 'unsupported' | 'unavailable' | 'saving' | 'checking' | 'conflict' | 'unknown' | 'storage-error';
  message: string; dirty: boolean; canSave: boolean;
  pending: ProfileAudienceIntent | null; pendingExists: boolean;
}
export interface ProfileAudienceStorage {
  getItem(key: string): string | null; setItem(key: string, value: string): void; removeItem(key: string): void;
}
export interface ProfileAudienceLocks {
  request<T>(name: string, callback: () => T | Promise<T>): Promise<T>;
}
export function createProfileAudienceState(options: {
  profile: WorldProfile; api: WorldApi; isCurrent(): boolean;
  storage: ProfileAudienceStorage; locks: ProfileAudienceLocks | null;
  onChange?(state: ProfileAudienceState): void;
}): {
  readonly key: string; read(): ProfileAudienceState; choose(value: AudienceMode): boolean;
  save(): Promise<ProfileAudienceState | false>; check(): Promise<ProfileAudienceState | false>;
  refreshPending(): ProfileAudienceState; dispose(): void;
};
