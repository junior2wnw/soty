export interface HumanLoginContext {
  schema: 'soty.human-login-context.v1'; interactionId: string; browserNonce: string; csrf: string;
  client: Readonly<{ id: string; label: string }>; scopes: readonly ('openid' | 'profile')[];
  expiresAt: number; decision: 'pending' | 'approved' | 'denied'; remainingMs: number;
}
export function parseHumanLoginContext(value: unknown, options: { interactionId: string; checkedAt: number }): Readonly<HumanLoginContext>;
