export type GlassWidgetMode = 'glance' | 'interactive';
export interface GlassWidgetContext { dispose(): void; }
export interface GlassWidgetHost {
  setMode(mode: GlassWidgetMode): void;
  dispose(): void;
}
/** UI-only mode; parentOrigin must be a trusted exact http(s) origin. */
export function bindGlassWidgetContext(root: Element, options: {
  parentOrigin: string;
  view?: Window;
}): GlassWidgetContext;
/** Create a new binding for each iframe/reset lifetime. Default mode is interactive. */
export function createGlassWidgetHost(frame: HTMLIFrameElement, options: {
  origin: string;
  initialMode?: GlassWidgetMode;
  view?: Window;
  isCurrent?: () => boolean;
  /** Once per current document epoch, after its exact UI mode ACK. */
  onReady?: () => void;
  /** Clear adoption UI on bind/load; only a current document ACK restores readiness. */
  onReset?: () => void;
}): GlassWidgetHost;
