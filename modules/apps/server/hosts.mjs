import { isIP } from 'node:net';
import { validDnsHostname } from './domain-policy.mjs';

// Parse authority syntax, not a URL. Do not trim or repair malformed Host values,
// and never substitute a caller-controlled forwarded header.
export function parseAppAuthority(rawHost) {
  if (typeof rawHost !== 'string' || rawHost.length === 0 || rawHost.length > 259 || /[\s/@?#%\\]/u.test(rawHost)) return null;
  let hostname, port = '';
  if (rawHost.startsWith('[')) {
    const match = rawHost.match(/^(\[([0-9A-Fa-f:.]+)\])(?::([0-9]+))?$/u);
    if (!match || isIP(match[2]) !== 6) return null;
    hostname = match[1].toLowerCase(); port = match[3] ?? '';
  } else {
    const match = rawHost.match(/^([A-Za-z0-9.-]+)(?::([0-9]+))?$/u);
    if (!match) return null;
    hostname = match[1].toLowerCase(); port = match[2] ?? '';
    if (!validDnsHostname(hostname)) return null;
  }
  if (port && (!/^[1-9][0-9]{0,4}$/u.test(port) || Number(port) > 65535)) return null;
  return { hostname, port };
}

function matchesOrigin(authority, origin) {
  const url = new URL(origin);
  return authority.hostname === url.hostname && (authority.port
    ? authority.port === (url.port || (url.protocol === 'https:' ? '443' : '80')) : url.port === '');
}
const within = (hostname, suffix) => hostname === suffix || hostname.endsWith(`.${suffix}`);

export function createHostClassifier({ db, shellOrigins = [], allowShellZoneRoot = false }) {
  const findDomain = db.prepare('SELECT id,app_id,hostname,origin,role,state FROM app_domains WHERE hostname=?');
  const findZones = db.prepare('SELECT kind,suffix FROM app_domain_zones');
  return {
    classifyHost(rawHost) {
      const authority = parseAppAuthority(rawHost);
      if (!authority) return { kind: 'invalid' };
      const domain = findDomain.get(authority.hostname);
      // Even a mistaken trusted-shell configuration cannot claim an allocated
      // app host or turn its alternate port into a shell/connector origin.
      if (domain) return matchesOrigin(authority, domain.origin)
        ? { kind: domain.role, domainId: domain.id, appId: domain.app_id, origin: domain.origin, hostname: domain.hostname, state: domain.state }
        : { kind: 'unknown-app-zone' };
      const zones = findZones.all();
      const named = zones.some(zone => zone.kind === 'named' && within(authority.hostname, zone.suffix));
      if (shellOrigins.some(origin => matchesOrigin(authority, origin)) && (!named || (allowShellZoneRoot
        && zones.some(zone => zone.kind === 'named' && authority.hostname === zone.suffix)))) return { kind: 'outside' };
      if (zones.some(zone => within(authority.hostname, zone.suffix))) return { kind: 'unknown-app-zone' };
      return { kind: 'outside' };
    },
    allowsTlsDomain(hostname) {
      if (typeof hostname !== 'string' || hostname !== hostname.toLowerCase() || !validDnsHostname(hostname)) return false;
      const domain = findDomain.get(hostname);
      // Certificates serve retained status pages as well as enabled apps.
      // They confer no runtime, session, app or account authorization.
      return Boolean(domain && new URL(domain.origin).protocol === 'https:');
    },
  };
}
