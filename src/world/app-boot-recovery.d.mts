import type { AppLaunchBinding } from './app-launch.mjs';
export function isAppBootRetryHint(data: unknown, expected: { appId: string; nonce: string }): boolean;
export function createAppBootRecovery(options: { view: Window; appId: string; getFrame(): HTMLIFrameElement | null;
  isCurrent(): boolean; canRecover?(): boolean; recover(isCurrent: () => boolean): void | Promise<void>; onFailure?(): void }): {
  bind(frame: HTMLIFrameElement, context: { origin: string; binding: AppLaunchBinding | null }): void;
  cancel(): void; dispose(): void; attempted(): boolean;
};
