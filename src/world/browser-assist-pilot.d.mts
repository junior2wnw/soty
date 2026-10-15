export function createBrowserAssistHost(options: { view: Window; readPeer: () => unknown; approve?: (command: unknown) => Promise<boolean> }): {
  probe(): Promise<boolean>; enable(): Record<string, unknown>; binding(): Record<string, unknown> | null;
  dispatch(command: unknown, signal?: AbortSignal, guard?: () => Promise<void>): Promise<unknown>; revoke(): void; close(): void;
};
export function pumpBrowserAssist(options: { client: { extension<T>(op: string, args: Record<string, unknown>, context: { expectedAccountId: string }): Promise<T> };
  accountId: string; requestId: string; binding: Record<string, unknown>; host: { binding(): Record<string, unknown> | null; dispatch(command: unknown, signal: AbortSignal, guard?: () => Promise<void>): Promise<unknown> }; signal: AbortSignal; assertCurrent: () => Promise<void> }): Promise<void>;
