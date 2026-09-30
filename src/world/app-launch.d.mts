export interface AppLaunchTarget { readonly appId: string; readonly domainId?: string; readonly path?: string }
export interface AppLaunchRequest extends AppLaunchTarget { readonly expectedAccountId: string }
export interface AppResolvedEntry { readonly appId: string; readonly domainId: string; readonly origin: string; readonly path: string }
export interface AppLaunchPresentation { readonly panel: 'discussion'; readonly conversationId?: string; readonly administrative?: true }
export interface AppLaunchIntent { readonly kind: 'app' | 'launch'; readonly target: AppLaunchTarget; readonly communityId?: string; readonly presentation?: AppLaunchPresentation; readonly route: string }
export interface AppLaunchPopup { opener: unknown; readonly closed: boolean; location: { replace(url: string): void }; close(): void }
export class AppLaunchError extends Error { code: string; constructor(code: string) }
export function validateAppLaunchPath(value: unknown): string;
export function normalizeAppLaunchTarget(value: unknown): AppLaunchTarget;
export function formatAppLaunchRoute(target: AppLaunchTarget, communityId?: string, presentation?: AppLaunchPresentation): string;
export function parseAppLaunchRoute(hash: string): AppLaunchIntent | null;
export function sameAppLaunchLocation(first: AppLaunchIntent | null, next: AppLaunchIntent | null, resolved?: AppResolvedEntry | null): boolean;
export function validateAppLaunchUrl(value: unknown, shellUrl: string): string;
export function validateAppEntry(value: unknown, target: AppLaunchTarget, shellUrl: string, launchUrl?: string): AppResolvedEntry;
export function createAppLauncher(options: {
  target: AppLaunchTarget; accountId: string; shellUrl: string;
  isCurrent(accountId: string): boolean;
  request(parameters: AppLaunchRequest): Promise<{ url: string; entry: AppResolvedEntry }>;
  resolveEntry?(parameters: AppLaunchRequest): Promise<{ entry: AppResolvedEntry }>;
}): {
  readonly parameters: AppLaunchRequest;
  entry(): AppResolvedEntry | null;
  launch(): Promise<string | null>;
  isCurrent(): boolean;
  openExternal(openPopup: () => AppLaunchPopup | null): Promise<'opened' | 'blocked' | 'stale' | 'busy'>;
  dispose(): void;
};
