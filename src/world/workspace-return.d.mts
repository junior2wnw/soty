export interface WorkspaceReturn {
  accountId: string; route: string; scroll: number;
  presentation?: 'saved' | 'list'; listQuery?: string; focusEntity?: string;
  libraryPages?: number; libraryScroll?: number; libraryFocus?: string;
}
export function readWorkspaceReturn(value: unknown, accountId: string): WorkspaceReturn | null;
