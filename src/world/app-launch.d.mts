export interface AppLaunchTarget { readonly appId: string; readonly domainId?: string; readonly path?: string }
export interface AppLaunchRequest extends AppLaunchTarget { readonly expectedAccountId: string }
export interface AppLaunchIntent { readonly kind: 'app' | 'launch'; readonly target: AppLaunchTarget; readonly communityId?: string; readonly route: string }
export interface AppLaunchPopup { opener: unknown; readonly closed: boolean; location: { replace(url: string): void }; close(): void }
export class AppLaunchError extends Error { code: string; constructor(code: string) }
export function validateAppLaunchPath(value: unknown): string;
export function normalizeAppLaunchTarget(value: unknown): AppLaunchTarget;
export function formatAppLaunchRoute(target: AppLaunchTarget, communityId?: string): string;
export function parseAppLaunchRoute(hash: string): AppLaunchIntent | null;
export function validateAppLaunchUrl(value: unknown, shellUrl: string): string;
export function createAppLauncher(options: {
  target: AppLaunchTarget; accountId: string; shellUrl: string;
  isCurrent(accountId: string): boolean;
  request(parameters: AppLaunchRequest): Promise<{ url: string }>;
}): {
  readonly parameters: AppLaunchRequest;
  launch(): Promise<string | null>;
  isCurrent(): boolean;
  openExternal(openPopup: () => AppLaunchPopup | null): Promise<'opened' | 'blocked' | 'stale' | 'busy'>;
  dispose(): void;
};
