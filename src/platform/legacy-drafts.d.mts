export interface LegacyDraftStore {
  get(id: string): string | undefined;
  set(id: string, value: string): LegacyDraftStore;
  delete(id: string): boolean;
  flush(): boolean;
  hasUnsavedChanges(): boolean;
}
export function createLegacyDraftStore(storage: Pick<Storage, 'getItem' | 'setItem' | 'removeItem'>, scope: () => string): LegacyDraftStore;
