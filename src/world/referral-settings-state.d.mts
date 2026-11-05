export interface ReferralSettingsIntent {
  expectedAccountId: string; appId: string; mutationId: string; expectedRevision: number; ratesBps: number[];
}
export interface ReferralSettingsConfig {
  appId: string; revision: number; ratesBps: number[]; rounding: string; payoutsEnabled: false; updatedAt: number; replayed?: boolean;
}
export interface ReferralSettingsState {
  key: string;
  read(): { schema: 1; accountId: string; appId: string; revision: number; pending: ReferralSettingsIntent | null };
  prepare(input: { expectedRevision: number; ratesBps: number[] }): Promise<ReferralSettingsIntent>;
  pendingForDispatch(displayed: ReferralSettingsIntent): Promise<ReferralSettingsIntent>;
  acknowledge(displayed: ReferralSettingsIntent, result: ReferralSettingsConfig, isCurrent?: () => boolean): Promise<boolean>;
  resolveRejected(displayed: ReferralSettingsIntent, rejection: unknown, replacement: { expectedRevision: number; ratesBps: number[] } | null, isCurrent: () => boolean): Promise<ReferralSettingsIntent | null>;
}
export function createReferralSettingsState(options: {
  accountId: string; appId: string; storage: Pick<Storage, 'getItem' | 'setItem'>; locks?: Pick<LockManager, 'request'>; randomId?: () => string;
}): ReferralSettingsState;
export function dispatchReferralSettingsIntent(options: {
  state: ReferralSettingsState; pending: ReferralSettingsIntent; request(op: string, args: Record<string, unknown>): Promise<unknown>; isCurrent(): boolean;
}): Promise<ReferralSettingsConfig>;
export function validateReferralSettingsConfig(value: unknown, appId: string): ReferralSettingsConfig;
export function referralPercentToBps(text: string): number;
export function referralBpsToPercent(value: number): string;
