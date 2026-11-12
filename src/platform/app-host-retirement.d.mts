export interface AppHostRetirementScope { accountId: string; deviceId: string }
export interface AppHostRetirementProof { appId: string; frameId: string; lease: string; revision: number }
export interface AppHostRetirementSession {
  schema: 'soty.app-host-session.v1'; frameId: string; lease: string; revision: number; expiresAt: number;
}
export interface AppHostRetirement {
  beforeLaunch(scope: AppHostRetirementScope, isCurrent: () => boolean): Promise<boolean>;
  /** confirmed is only for a signed exact-proof confirmation/current-context reply. */
  record(scope: AppHostRetirementScope, value: { appId: string; hostSession: AppHostRetirementSession }, confirmed?: boolean): () => void;
}
export function createAppHostRetirement(options: {
  storage?: Pick<Storage, 'getItem' | 'setItem'> | null;
  locks?: Pick<LockManager, 'request'> | null;
  close: (scope: AppHostRetirementScope, proof: AppHostRetirementProof) => Promise<{ closed: boolean }>;
  randomId?: () => string;
}): AppHostRetirement;
