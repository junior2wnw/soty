import type { AppBuilderProposal, AppBuilderTarget } from './app-builder-result.mjs';

export function createAppBuilderRegistration(options: {
  accountId: string;
  pending: AppBuilderTarget;
  proposal: AppBuilderProposal;
  storage: Pick<Storage, 'getItem' | 'setItem' | 'removeItem'>;
  locks: Pick<LockManager, 'request'> | undefined;
  randomId?: () => string;
}): {
  begin(): Promise<{ dispatch: boolean; attempt: string }>;
  retry(expectedAttempt: string): Promise<{ dispatch: boolean; attempt: string | null }>;
  inspect(): Promise<string | null>;
  clear(expectedAttempt: string | null): Promise<void>;
};
