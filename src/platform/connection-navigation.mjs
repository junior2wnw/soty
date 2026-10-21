import { parseConnectionIntent } from './connection-intent.mjs';

/** Cross-shell handoff only. A returned address never grants access. The actual
 * single-use app/platform carrier survives until this explicit navigation. */
export function takeConnectionNavigation(intent, origin, handoff) {
  const url = new URL('/', origin);
  if (url.origin !== origin || !['http:', 'https:'].includes(url.protocol)) throw new TypeError('connection_origin_invalid');
  if (intent.kind === 'platform-invitation') {
    const link = handoff?.takePlatformInvitationLink();
    if (!link || parseConnectionIntent(link, origin)?.kind !== intent.kind) throw new TypeError('connection_invitation_expired');
    return new URL(link, origin).href;
  }
  if (intent.kind === 'application') {
    url.hash = intent.launch.route;
    if (intent.launch.takeInvitation) {
      const invitation = intent.launch.takeInvitation();
      if (!invitation || !/^[a-f0-9]{64}$/u.test(invitation)) throw new TypeError('connection_invitation_expired');
      url.hash += `${url.hash.includes('?') ? '&' : '?'}invitation=${invitation}`;
    }
  } else if (intent.kind === 'legacy-room') url.searchParams.set('j', intent.joinCode);
  else if (intent.kind === 'profile' || intent.kind === 'community') url.hash = intent.route;
  else throw new TypeError('connection_route_unavailable');
  if (parseConnectionIntent(url.href, origin)?.kind !== intent.kind) throw new TypeError('connection_route_invalid');
  return url.href;
}
