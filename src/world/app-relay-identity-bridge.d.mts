export interface RelayIdentityEntry { appId: string; domainId: string; origin: string }
export function createRelayIdentityBridge(options: {
  view: Window; isCurrent(): boolean; getFrame(): HTMLIFrameElement | null;
  getEntry(): RelayIdentityEntry | null; getBinding(): unknown;
  request(value: { appId: string; domainId: string; launchBinding: unknown }): Promise<unknown>;
  close(value: { appId: string; grant: string }): Promise<unknown>;
}): { clear(): Promise<void>; hasActive(): boolean; resume(): void; dispose(): void };
