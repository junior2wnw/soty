import { openSync, closeSync, fstatSync, readSync, constants } from 'node:fs';
import path from 'node:path';
import { TextDecoder } from 'node:util';
import { forcedLegacyMode } from './universal-mode.js';

const invalid = () => { throw Object.assign(new Error('universal_configuration_invalid'), { code: 'universal_configuration_invalid' }); };
const check = value => { if (!value) invalid(); };
const field = (env, name) => { const value = env[name]; check(value === undefined || typeof value === 'string'); return value ?? ''; };
function fileValue(filename, maximum) {
  check(path.isAbsolute(filename) && filename.length <= 4096 && !filename.includes('\0'));
  let fd;
  const bytes = Buffer.alloc(maximum + 1);
  try {
    fd = openSync(filename, constants.O_RDONLY | (constants.O_NONBLOCK || 0));
    const stat = fstatSync(fd); check(stat.isFile() && stat.size > 0 && stat.size <= maximum);
    let used = 0;
    while (used < bytes.length) { const count = readSync(fd, bytes, used, bytes.length - used, used); if (!count) break; used += count; }
    check(used > 0 && used <= maximum);
    const value = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes.subarray(0, used)));
    check(value && typeof value === 'object' && !Array.isArray(value)); return value;
  } catch { invalid(); }
  finally { bytes.fill(0); if (fd !== undefined) closeSync(fd); }
}
function humanSecrets(filename) {
  const value = fileValue(filename, 65536), names = ['clients', 'jwks', 'cookieKeys', 'artifactKey', 'artifactKeyId'];
  check(names.every(name => Object.hasOwn(value, name)) && Object.keys(value).every(name => names.includes(name) || name === 'renewal'));
  check(typeof value.artifactKey === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(value.artifactKey));
  const key = Buffer.from(value.artifactKey, 'base64url'); check(key.length === 32 && key.toString('base64url') === value.artifactKey);
  return { clients: value.clients, jwks: value.jwks, cookieKeys: value.cookieKeys, artifactKey: key, artifactKeyId: value.artifactKeyId,
    ...(Object.hasOwn(value, 'renewal') ? { renewal: value.renewal } : {}) };
}

/** Private process-entry inputs only. No environment enumeration, automatic
 * client registration, secret generation or configuration/status publication. */
export function loadUniversalConfiguration(env = process.env, { legacyMode = forcedLegacyMode } = {}) {
  try {
    check(typeof legacyMode === 'boolean');
    if (legacyMode || field(env, 'SOTY_UNIVERSAL_APPS_ENABLED') === 'false') return {};
    const flag = field(env, 'SOTY_HUMAN_IDENTITY_ENABLED'); check(['', '0', '1'].includes(flag));
    const issuer = field(env, 'SOTY_HUMAN_IDENTITY_ISSUER'), filename = field(env, 'SOTY_HUMAN_IDENTITY_KEYS_FILE');
    check(issuer.length <= 1024 && (flag !== '1' || issuer && filename) && (issuer || !filename));
    const reviews = field(env, 'SOTY_REVIEWS_BINDINGS_FILE');
    return {
      ...(issuer ? { humanIdentity: { enabled: flag === '1', issuer, registryId: 'soty', environmentId: 'production',
        ...(flag === '1' ? humanSecrets(filename) : {}) } } : {}),
      ...(reviews ? { reviewsConfiguration: fileValue(reviews, 65536) } : {}),
    };
  } catch { invalid(); }
}
