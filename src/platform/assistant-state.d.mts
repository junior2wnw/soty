export interface AssistantTarget { hostDeviceId: string; connectorId: string; jobId: string }
export interface AssistantPayload { expectedAccountId: string; hostDeviceId: string; connectorId: string; conversationId: string; text: string; cwd: string; previousJobId?: string }
export interface AssistantPending { requestId: string; payload: AssistantPayload }
export interface AssistantState { text: string; cwd: string; deviceKey: string; conversationId: string; previousJobId: string; lastJob: AssistantTarget | null; pending: AssistantPending | null }
export function createAssistantState(options: { accountId: string; storage: Pick<Storage, 'getItem' | 'setItem'>; tabStorage?: Pick<Storage, 'getItem' | 'setItem'>; randomId?: () => string }): {
  key: string;
  read(): AssistantState;
  listDrafts(): Omit<AssistantState, 'pending'>[];
  saveDraft(patch: Partial<Pick<AssistantState, 'text' | 'cwd' | 'deviceKey'>>): AssistantState;
  prepare(payload: AssistantPayload): AssistantPending;
  acknowledge(requestId: string, job: AssistantTarget): boolean;
  restore(conversationId: string): AssistantState;
  select(value: { conversationId: string; previousJobId?: string; lastJob?: AssistantTarget | null; cwd?: string; deviceKey?: string }): AssistantState;
  forgetRejected(requestId: string): boolean;
  flush(): void;
  hasUnsavedChanges(): boolean;
};
