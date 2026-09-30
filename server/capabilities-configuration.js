import { openSync, closeSync, fstatSync, readSync, constants } from 'node:fs';
import path from 'node:path';
import { TextDecoder } from 'node:util';

const invalid = () => { throw Object.assign(new Error('capability_configuration_invalid'), { code: 'capability_configuration_invalid' }); };
const check = value => { if (!value) invalid(); };
const field = (env, name) => {
  const value = env[name];
  check(value === undefined || typeof value === 'string');
  return value ?? '';
};
function flag(env, name) {
  const value = field(env, name);
  check(['', '0', '1'].includes(value)); return value === '1';
}
function secretFile(filename) {
  check(filename.length <= 4096 && path.isAbsolute(filename) && !filename.includes('\0'));
  let fd;
  const bytes = Buffer.alloc(32769);
  try {
    // O_NONBLOCK also prevents accidentally waiting forever on an operator's
    // FIFO path. fstat admits only a regular file; reads have a separate cap.
    fd = openSync(filename, constants.O_RDONLY | (constants.O_NONBLOCK || 0));
    const stat = fstatSync(fd);
    check(stat.isFile() && stat.size > 0 && stat.size <= 32768);
    let used = 0;
    while (used < bytes.length) {
      const count = readSync(fd, bytes, used, bytes.length - used, used);
      if (!count) break;
      used += count;
    }
    check(used > 0 && used <= 32768);
    const value = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes.subarray(0, used)));
    check(value && typeof value === 'object' && !Array.isArray(value));
    const names = ['jwks', 'cookieKeys', 'artifactKey', 'artifactKeyId'];
    check(Object.keys(value).length === names.length && names.every(name => Object.hasOwn(value, name)));
    check(typeof value.artifactKey === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(value.artifactKey));
    const key = Buffer.from(value.artifactKey, 'base64url');
    check(key.length === 32 && key.toString('base64url') === value.artifactKey);
    // Detailed JWKS/cookie/issuer validation belongs to the single host profile
    // and is performed before storage opens. No key is generated here.
    return { jwks: value.jwks, cookieKeys: value.cookieKeys, artifactKey: key, artifactKeyId: value.artifactKeyId };
  } catch { invalid(); }
  finally { bytes.fill(0); if (fd !== undefined) closeSync(fd); }
}

/** Process-entry opt-in, returning private options for createHttpApp only.
 * The environment is neither enumerated nor copied to status/log output. */
export function loadCapabilityConfiguration(env = process.env) {
  try {
    const capabilityAudience = field(env, 'SOTY_CAPABILITY_AUDIENCE');
    const nativeNotesEnabled = flag(env, 'SOTY_NATIVE_NOTES_ENABLED');
    const enabled = flag(env, 'SOTY_OAUTH_ENABLED');
    const issuer = field(env, 'SOTY_OAUTH_ISSUER');
    const filename = field(env, 'SOTY_OAUTH_KEYS_FILE');
    check(capabilityAudience.length <= 2048 && issuer.length <= 2054);
    check(!nativeNotesEnabled || capabilityAudience.length > 0);
    check(!enabled || (issuer.length > 0 && filename.length > 0));
    check(issuer.length > 0 || filename.length === 0);
    return { capabilityAudience, nativeNotesEnabled,
      ...(issuer ? { oauth: { enabled, issuer, ...(filename ? secretFile(filename) : {}) } } : {}) };
  } catch { invalid(); }
}
