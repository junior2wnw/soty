import { assertApps } from './protocol.mjs';

const probeId = `app-${'0'.repeat(32)}`;
export const DEFAULT_DOMAIN_LIMITS = Object.freeze({ perApp: 3, perAccount: 100 });
export const RESERVED_APP_NAMES = Object.freeze([
  'admin', 'api', 'app', 'apps', 'assets', 'auth', 'cdn', 'connect', 'docs', 'download', 'downloads',
  'help', 'mail', 'mcp', 'ns', 'ns1', 'ns2', 'privacy', 'security', 'soty', 'static', 'status',
  'support', 'terms', 'www', 'webmail', 'ftp', 'localhost', 'accounts', 'account', 'oauth', 'login',
]);
const reserved = new Set(RESERVED_APP_NAMES);

export function normalizeDomainLimits(value = {}) {
  assertApps(value && typeof value === 'object' && !Array.isArray(value)
    && Object.keys(value).every(key => ['perApp', 'perAccount'].includes(key)), 'invalid_app_domain_limits');
  const result = { ...DEFAULT_DOMAIN_LIMITS, ...value };
  assertApps(Number.isSafeInteger(result.perApp) && result.perApp >= 1 && result.perApp <= 100
    && Number.isSafeInteger(result.perAccount) && result.perAccount >= 1 && result.perAccount <= 10_000,
  'invalid_app_domain_limits');
  return Object.freeze(result);
}

export function normalizeAppSlug(value) {
  assertApps(typeof value === 'string' && /^[A-Za-z0-9](?:[A-Za-z0-9-]{1,46})[A-Za-z0-9]$/u.test(value), 'invalid_app_slug');
  return value.toLowerCase();
}

export function isReservedAppSlug(slug) {
  return reserved.has(slug) || slug.startsWith('app-') || slug.startsWith('xn--');
}

export function validDnsHostname(hostname) {
  return typeof hostname === 'string' && hostname.length > 0 && hostname.length <= 253
    && hostname.split('.').every(label => label.length <= 63 && /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/u.test(label));
}

function parseOrigin(value, code) {
  assertApps(typeof value === 'string' && value.length <= 512 && !/[\s\\]/u.test(value), code);
  let url;
  try { url = new URL(value); } catch { assertApps(false, code); }
  assertApps(url.pathname === '/' && !url.search && !url.hash && !url.username && !url.password
    && validDnsHostname(url.hostname), code);
  assertApps(url.protocol === 'https:' || (url.protocol === 'http:'
    && (url.hostname === 'localhost' || url.hostname.endsWith('.localhost'))), 'apps_origin_requires_https');
  return url;
}

// Canonicalization changes only spelling (case/default port), never the browser origin.
export function normalizeLegacyTemplate(value = '') {
  if (value === '') return '';
  assertApps(typeof value === 'string' && value.split('{appId}').length === 2, 'invalid_apps_origin');
  const url = parseOrigin(value.replace('{appId}', probeId), 'invalid_apps_origin');
  assertApps(url.hostname.includes(probeId), 'invalid_apps_origin');
  const template = url.origin.replace(probeId, '{appId}');
  assertApps(template.includes('{appId}') && legacyZone(template).suffix, 'invalid_apps_origin');
  return template;
}

export function normalizeNamedAppZone(value = '') {
  if (value === '') return '';
  const url = parseOrigin(value, 'invalid_named_app_zone');
  // A slug must fit in the DNS wire limits without trimming the author's name.
  assertApps(url.hostname.length + 49 <= 253, 'invalid_named_app_zone');
  return url.origin;
}

export function canonicalOrigin(template, id) {
  return template ? new URL(template.replace('{appId}', id)).origin : null;
}

export function legacyZone(template) {
  const url = new URL(template.replace('{appId}', probeId));
  const labels = url.hostname.split('.');
  const variable = labels.findIndex(label => label.includes(probeId));
  return { kind: 'legacy', template, suffix: labels.slice(variable + 1).join('.'), scheme: url.protocol.slice(0, -1), port: url.port };
}

export function namedZone(origin) {
  const url = new URL(origin);
  return { kind: 'named', template: `${url.protocol}//{slug}.${url.host}`, suffix: url.hostname, scheme: url.protocol.slice(0, -1), port: url.port };
}

export function validateNamedOrigins(values, { shellOrigins = [], validateNamedZone = () => {}, allowShellZoneRoot = false } = {}) {
  assertApps(typeof validateNamedZone === 'function', 'invalid_named_app_zone_validator');
  const shellHosts = shellOrigins.map(value => new URL(value).hostname);
  for (const value of new Set(values.filter(Boolean))) {
    const origin = normalizeNamedAppZone(value), hostname = new URL(origin).hostname;
    assertApps(!shellHosts.some(host => (!allowShellZoneRoot && host === hostname) || host.endsWith(`.${hostname}`)), 'apps_named_zone_shell_overlap', 409);
    const verdict = validateNamedZone(origin);
    assertApps(verdict !== false && !(verdict && typeof verdict.then === 'function'), 'invalid_named_app_zone_validator');
  }
}
