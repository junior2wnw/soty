export function mountAppFullscreen(options: {
  screen: HTMLElement; isCurrent(): boolean;
  onChange(state: { active: boolean; pending: boolean }): void;
}): { toggle(): Promise<void>; leave(): Promise<void>; active(): boolean; dispose(): void };
