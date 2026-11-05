export interface RelayIdentityEntry { appId: string; domainId: string; origin: string }
export interface RelayIdentityHostSession { frameId:string;lease:string;revision:number }
export function createRelayIdentityBridge(options: {
  view: Window; isCurrent(): boolean; getFrame(): HTMLIFrameElement | null;
  getEntry(): RelayIdentityEntry | null; getBinding(): unknown;
  getRuntimeScope?(): {instanceId:string;runtimeOrigin:string} | null;
  getHostSession?():RelayIdentityHostSession|null;
  request(value: { appId: string; domainId: string; launchBinding: unknown; instanceId?:string;runtimeOrigin?:string;hostSession?:RelayIdentityHostSession }): Promise<unknown>;
  close(value: { appId: string; grant: string }): Promise<unknown>;
}): { clear(): Promise<void>; reset(): void; hasActive(): boolean; resume(): void; dispose(): void };
