export interface WorkspaceWidget { id: string; route: string; name: string; visible: boolean; x: number; y: number; width: number; height: number }
export interface WidgetWorkspace { hidden: boolean; widgets: WorkspaceWidget[] }
export interface WidgetViewport { left: number; top: number; width: number; height: number; bottomInset?: number }
export interface WidgetRect { x: number; y: number; width: number; height: number }
type Store = Pick<Storage, 'getItem' | 'setItem'>;
export const widgetLimit: number;
export function normalizeWidget(value: unknown): WorkspaceWidget | null;
export function readWidgetWorkspace(storage: Store | null, account: string): WidgetWorkspace;
export function editWidgetWorkspace(storage: Store | null, account: string, change: (state: WidgetWorkspace) => void): { state: WidgetWorkspace; saved: boolean };
export function widgetStorageKey(account: string): string;
export function widgetRect(widget: WorkspaceWidget, viewport: WidgetViewport): WidgetRect;
export function widgetPosition(rect: WidgetRect, viewport: WidgetViewport, previous?: { x: number; y: number }): { x: number; y: number };
