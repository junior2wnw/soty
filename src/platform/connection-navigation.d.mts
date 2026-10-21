import type { ConnectionIntent } from './connection-intent.mjs';
export function takeConnectionNavigation(intent: ConnectionIntent, origin: string, handoff?: { takePlatformInvitationLink(): string | null }): string;
