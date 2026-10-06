export type AppDirectoryScope = 'all' | 'mine' | 'public';
export interface AppDirectorySearchArgs { expectedAccountId: string; query?: string; scope?: AppDirectoryScope; limit?: number; cursor?: string }
export interface AppDirectoryEntry { appId: string; domainId: string; origin: string; path: string }
export interface AppDirectoryItem {
  id: string; name: string; ownerAccountId: string; state: 'ready' | 'starting' | 'stopped' | 'offline';
  createdAt: number; updatedAt: number; access: 'owner' | 'granted' | 'public'; canManage: boolean; entry: AppDirectoryEntry;
}
export interface AppDirectorySearchResult { schema: 'soty.app-directory.v1'; apps: AppDirectoryItem[]; nextCursor: string | null }
export interface AppDirectoryResolveArgs { expectedAccountId: string; appIds: string[] }
export interface AppDirectoryResolveResult { schema: 'soty.app-directory.v1'; items: ({ ref: { kind: 'app'; id: string }; available: false }
  | { ref: { kind: 'app'; id: string }; available: true; app: AppDirectoryItem })[] }
export interface AppDirectoryOperationMap {
  'apps.directory.search': { args: AppDirectorySearchArgs; result: AppDirectorySearchResult };
  'apps.directory.resolve': { args: AppDirectoryResolveArgs; result: AppDirectoryResolveResult };
}
export const directoryOperations: readonly ['apps.directory.search', 'apps.directory.resolve'];
/** Trusted host constructor; never construct actor from user-supplied arguments. */
export function createAppDirectory(options: Record<string, unknown>): {
  execute(input: { op: 'apps.directory.search' | 'apps.directory.resolve'; args: Record<string, unknown>; actor: { accountId: string; deviceId: string } }): AppDirectorySearchResult | AppDirectoryResolveResult;
};
