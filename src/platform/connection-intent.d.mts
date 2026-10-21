import type { AppLaunchIntent } from '../world/app-launch.mjs';

export const MAX_CONNECTION_INPUT_BYTES: 32768;
export type ConnectionIntent =
  | Readonly<{ kind: 'contact' | 'device'; id: string }>
  | Readonly<{ kind: 'legacy-room'; joinCode: string; route: string }>
  | Readonly<{ kind: 'profile'; id: string; route: string }>
  | Readonly<{ kind: 'community'; id: string; tab: 'about' | 'chat' | 'apps'; route: string }>
  | Readonly<{ kind: 'application'; launch: AppLaunchIntent }>
  | Readonly<{ kind: 'platform-invitation' }>;

/** An intent is presentation/routing data, never an actor or granted access.
 * A platform-invitation marker deliberately contains no token or executable URL. */
export function parseConnectionIntent(value: unknown, expectedOrigin: string): ConnectionIntent | null;
