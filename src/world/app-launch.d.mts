export interface AppLaunchTarget { readonly appId: string; readonly domainId?: string; readonly path?: string }
export interface AppLaunchRequest extends AppLaunchTarget { readonly expectedAccountId: string; readonly instanceId?: string; readonly hostSession?: true }
export interface AppResolvedEntry { readonly appId: string; readonly domainId: string; readonly origin: string; readonly path: string }
export interface AppLaunchBinding { readonly schema: 'soty.app-launch-binding.v1'; readonly policyEpoch: number; readonly targetRevision: number; readonly targetDigest: string; readonly profile: 'soty.relay-restricted.v1' | 'soty.relay-self-ip.v1'; readonly bindingFloor: 1 | 2 }
export interface AppHostSession {readonly schema:'soty.app-host-session.v1';readonly frameId:string;readonly lease:string;readonly revision:number;readonly expiresAt:number}
export interface AppHostSessionBinding {readonly hostSession:AppHostSession;readonly hostSessionExpiresAt:number|null;readonly entry:AppResolvedEntry;readonly launchBinding:AppLaunchBinding;readonly runtimeOrigin:string;readonly slot:object;readonly cleanup?:()=>Promise<void>}
export interface AppHostSessionState {readonly hostSession:AppHostSession;readonly hostSessionExpiresAt:number|null;readonly entry:AppResolvedEntry;readonly launchBinding:AppLaunchBinding;readonly runtimeOrigin?:string;readonly cleanup?:()=>Promise<void>}
export interface AppLaunchPresentation { readonly panel: 'discussion'; readonly conversationId?: string; readonly messageId?: string; readonly administrative?: true }
export interface AppLaunchIntent { readonly kind: 'app' | 'launch'; readonly takeInvitation?: () => string | null; readonly target: AppLaunchTarget; readonly communityId?: string; readonly presentation?: AppLaunchPresentation; readonly route: string }
export interface AppLaunchPopup { opener: unknown; readonly closed: boolean; location: { replace(url: string): void }; close(): void }
export class AppLaunchError extends Error { code: string; constructor(code: string) }
export function isScopedRuntimeProfile(value: unknown): value is 'soty.selected-human-embed.v1'|'soty.selected-human-embed.v2';
export function validateAppLaunchPath(value: unknown): string;
export function normalizeAppLaunchTarget(value: unknown): AppLaunchTarget;
export function formatAppLaunchRoute(target: AppLaunchTarget, communityId?: string, presentation?: AppLaunchPresentation): string;
export function parseAppLaunchRoute(hash: string): AppLaunchIntent | null;
export function sameAppLaunchLocation(first: AppLaunchIntent | null, next: AppLaunchIntent | null, resolved?: AppResolvedEntry | null): boolean;
export function validateAppLaunchUrl(value: unknown, shellUrl: string): string;
export function validateAppEntry(value: unknown, target: AppLaunchTarget, shellUrl: string, launchUrl?: string): AppResolvedEntry;
export function validateAppLaunchBinding(value: unknown): AppLaunchBinding;
export function sameAppLaunchBinding(first: AppLaunchBinding | null, next: AppLaunchBinding | null): boolean;
export function validateAppHostSession(value:unknown):AppHostSession;
export function createAppLauncher(options: {
  instanceId?: string;
  target: AppLaunchTarget; accountId: string; shellUrl: string;
  isCurrent(accountId: string): boolean;
  request(parameters: AppLaunchRequest): Promise<{ url: string; entry: AppResolvedEntry;runtimeOrigin?:string;launchBinding?:AppLaunchBinding;hostSession?:AppHostSession;hostSessionExpiresAt?:number;hostSessionCleanup?:()=>Promise<void>;runtimeProfile?:string;scopedCloseHandle?:string;scopedFrameId?:string;scopedCleanup?:()=>Promise<void>;scopedSource?:{id:string;version:number;digest:string} }>;
  abandonScoped?(parameters:{appId:string;handle:string}):Promise<unknown>;
  abandonHost?(parameters:{appId:string;frameId:string;lease:string;revision:number}):Promise<unknown>;
  resolveEntry?(parameters: AppLaunchRequest): Promise<{ entry: AppResolvedEntry }>;
  resolveExternalEntry?(parameters: { appId: string; expectedAccountId: string }): Promise<{ entry: AppResolvedEntry | null }>;
}): {
  readonly parameters: AppLaunchRequest;
  entry(): AppResolvedEntry | null;
  /** Accepted browser peer only; never substitutes canonical app/Source entry. */
  runtimeOrigin(): string | null;
  hostSession():AppHostSessionBinding|null;
  commitHostSession(previous:AppHostSessionBinding,next:AppHostSessionState):boolean;
  binding(): AppLaunchBinding | null;
  latestBinding(): AppLaunchBinding | null;
  launch(options?: { requireSameBinding?: boolean }): Promise<string | null>;
  canOpenExternal(): Promise<boolean>;
  /** Resolve and pin the permitted entry without creating an app runtime/session. */
  resolve(): Promise<AppResolvedEntry | null>;
  isCurrent(): boolean;
  runtimeProfile():string|null;
  scopedCapture():{handle:string;source:unknown;slot:object;frameId?:string;entry?:AppResolvedEntry|null}|null;
  prepareRenewal(result:{url:string;entry:AppResolvedEntry;runtimeProfile:string;scopedCloseHandle:string;scopedFrameId?:string;scopedSlotExpiresAt:number;scopedRenewalRequestId:string;scopedSource:unknown;scopedRenewalBindingDigest?:string;scopedCleanup?:(()=>Promise<void>)|undefined}):import('./app-scoped-renewal.mjs').ScopedRenewReply;
  commitRenewal(previous:{slot:object},next:import('./app-scoped-renewal.mjs').ScopedRenewReply):boolean;
  openExternal(openPopup: () => AppLaunchPopup | null): Promise<'opened' | 'blocked' | 'stale' | 'busy' | 'unavailable'>;
  dispose(): void;
};
