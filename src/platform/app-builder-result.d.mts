export interface AppBuilderTarget { hostDeviceId: string; connectorId: string; jobId: string }
export interface AppBuilderProposal { schema: 'soty.local-app.v1'; name: string; port: number; entryPath: string; sourceJobId: string }
export interface AppBuilderRegisteredApp { id: string; name: string; ownerAccountId: string; hostDeviceId: string; connectorId: string; port: number; entryPath: string; state: string; grants?: { accountIds: string[]; communityIds: string[] } }
export function appBuilderProposal(result: { job: { status: string; executionUncertain?: boolean; result: { appProposal?: AppBuilderProposal } | null } }, pending: AppBuilderTarget): AppBuilderProposal | null;
export function appBuilderReceipt(job: { schema?: string; kind?: string; id: string; deviceId: string; connectorId: string; status: string; attempts?: number }, payload: Pick<AppBuilderTarget, 'hostDeviceId' | 'connectorId'>): boolean;
export function matchingAppBuilderRegistration(apps: AppBuilderRegisteredApp[], pending: AppBuilderTarget, proposal: AppBuilderProposal, accountId: string): AppBuilderRegisteredApp | null;
export function appBuilderLaunchUrl(value: unknown, pageUrl: string): string | null;
