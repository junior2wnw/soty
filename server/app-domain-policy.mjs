import { isIP } from 'node:net';
import { parse } from 'tldts';
import { legacyZone, normalizeLegacyTemplate, normalizeNamedAppZone } from '../modules/apps/server/domain-policy.mjs';

function requireValue(condition, code) {
  if (!condition) throw new Error(code);
}

function baseOrigin(value) {
  requireValue(typeof value === 'string' && value.length <= 512 && !/[\s\\%]/u.test(value), 'apps_zone_invalid_origin');
  let url;
  try { url = new URL(value); } catch { throw new Error('apps_zone_invalid_origin'); }
  requireValue(['http:', 'https:'].includes(url.protocol) && url.pathname === '/' && !url.search && !url.hash &&
    !url.username && !url.password && !url.hostname.endsWith('.'), 'apps_zone_invalid_origin');
  return url;
}

function loopback(host) {
  return host === 'localhost' || host.endsWith('.localhost') || host === '[::1]' || /^127\.(?:\d{1,3}\.){2}\d{1,3}$/u.test(host);
}

function overlap(left, right) {
  return left === right || left.endsWith(`.${right}`) || right.endsWith(`.${left}`);
}

function registrable(host) {
  const result = parse(host, { allowPrivateDomains: true, detectSpecialUse: true });
  requireValue(!result.isIp && !result.isSpecialUse && (result.isIcann || result.isPrivate) && result.domain,
    'apps_zone_public_domain_required');
  return result.domain;
}

export function legacyAppFrameSource(template = '') {
  if (!template) return '';
  const zone = legacyZone(normalizeLegacyTemplate(template));
  // CSP permits a leftmost wildcard, not prefix-* or fixed.* in the middle.
  return `${zone.scheme}://*.${zone.suffix}${zone.port ? `:${zone.port}` : ''}`;
}

/** Configuration admission only: this does not prove DNS ownership or a live TLS deployment. */
export function validateNamedAppZone({ namedAppZone = '', shellOrigins = [], appOriginTemplate = '' } = {}) {
  if (namedAppZone === '') return '';
  const zone = baseOrigin(namedAppZone);
  requireValue(shellOrigins.length > 0, 'apps_zone_shell_origins_required');
  const shells = shellOrigins.map(baseOrigin);
  requireValue(!isIP(zone.hostname) && !zone.hostname.startsWith('['), 'apps_zone_invalid_hostname');
  // Slug + separator + zone length is governed by the registry's DNS contract.
  try { normalizeNamedAppZone(zone.origin); } catch { throw new Error('apps_zone_invalid_hostname'); }
  const local = zone.hostname === 'localhost' || zone.hostname.endsWith('.localhost');
  if (local) {
    requireValue(shells.every(shell => loopback(shell.hostname)), 'apps_zone_local_shell_required');
    requireValue(shells.every(shell => shell.host !== zone.host && !shell.hostname.endsWith(`.${zone.hostname}`)), 'apps_zone_shell_overlap');
  } else {
    requireValue(zone.protocol === 'https:' && !zone.port, 'apps_zone_https_required');
    const domain = registrable(zone.hostname);
    for (const shell of shells) {
      // Changing scheme or port does not separate parent-domain cookies.
      requireValue(!loopback(shell.hostname), 'apps_zone_trusted_sites_required');
      requireValue(!overlap(zone.hostname, shell.hostname) && registrable(shell.hostname) !== domain,
        'apps_zone_separate_site_required');
    }
  }
  if (appOriginTemplate) {
    // Use the registry's exact parser: legacy templates can have fixed labels
    // before the variable, or a prefix inside its label.
    const { suffix } = legacyZone(normalizeLegacyTemplate(appOriginTemplate));
    requireValue(!overlap(zone.hostname, suffix), 'apps_zone_legacy_overlap');
  }
  return zone.origin;
}

/** Node otherwise retains only one value when duplicate Host headers arrive. */
export function hasSingleHostHeader(request) {
  if (!Array.isArray(request.rawHeaders) || typeof request.headers?.host !== 'string') return false;
  let count = 0;
  for (let index = 0; index < request.rawHeaders.length; index += 2) {
    if (String(request.rawHeaders[index]).toLowerCase() === 'host') count++;
  }
  return count === 1;
}
