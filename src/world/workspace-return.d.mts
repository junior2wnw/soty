export interface WorkspaceCanvasView {
  mine?: {x: number; y: number; scale: number};
  mineContext?: string; mineSelection?: string; mineFit?: 'manual' | 'overview' | 'context';
}
export interface WorkspaceReturn {
  accountId: string; route: string; scroll: number;
  presentation?: 'saved' | 'list'; listQuery?: string; focusEntity?: string;
  libraryPages?: number; libraryScroll?: number; libraryFocus?: string;
  canvasView?: WorkspaceCanvasView;
}
export interface CanvasPageReturnHandoff {
  route: string; launchRoute: string; accountId: string | null; deviceId: string | null; canvasView?: WorkspaceCanvasView;
}
export function readCanvasPageHandoff(value: unknown): CanvasPageReturnHandoff | null;
export function bindCanvasPageReturn(value: unknown, identity: {accountId: string; deviceId: string; launchRoute: string}): WorkspaceReturn | null;
export function readWorkspaceReturn(value: unknown, accountId: string): WorkspaceReturn | null;
