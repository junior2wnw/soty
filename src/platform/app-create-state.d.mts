export interface AppCreateDraft { text: string; cwd: string; hostDeviceId: string; connectorId: string }
export interface AppCreatePayload extends AppCreateDraft { expectedAccountId: string; requestId: string }
export interface AppCreateTarget { hostDeviceId: string; connectorId: string; jobId: string }
export interface AppCreatePending { payload: AppCreatePayload; draftRevision: number }
export interface AppCreateSnapshot { schema: 1; accountId: string; draft: AppCreateDraft; revision: number; pending: AppCreatePending | null; accepted: AppCreateTarget | null }
export interface AppCreateReceipt { status: string; requestId: string; reason: string }
export function createAppCreateState(options: {
  accountId: string; storage: Pick<Storage, 'getItem' | 'setItem'>;
  locks?: Pick<LockManager, 'request'>; randomId?: () => string;
}): {
  key: string; read(): AppCreateSnapshot; canDispatch(): boolean; hasUnsavedChanges(): boolean; hasVolatileDraft(): boolean; discardLocalDraft(): void;
  stageDraft(patch: Partial<AppCreateDraft>): number; persistDraft(generation: number): Promise<AppCreateSnapshot>;
  saveDraft(patch: Partial<AppCreateDraft>): Promise<AppCreateSnapshot>; flush(): Promise<void>;
  prepare(input: Omit<AppCreatePayload, 'requestId'>): Promise<AppCreatePending>;
  acknowledge(expected: AppCreatePayload, job: AppCreateTarget): Promise<boolean>;
  reject(expected: AppCreatePayload, receipt: AppCreateReceipt): Promise<boolean>;
  adoptAccepted(job: AppCreateTarget): Promise<boolean>; clearAccepted(job: AppCreateTarget): Promise<boolean>;
};
