import { createHash } from 'node:crypto';
import { canonicalJson, freezeDeep } from '../capabilities/server/validation.mjs';
import { HUMAN_IDENTITY_PROFILE, HUMAN_RENEWAL_PROFILE, HUMAN_RENEWAL_LIMITS } from '../human-identity/profile.mjs';
import { createReviewsService } from '../reviews/server/index.mjs';

export const UNIVERSAL_RUNTIME_SCHEMA = 'soty.universal-preparedness.v1';
export const UNIVERSAL_RUNTIME_MAX_BYTES = 65536;
export class UniversalPreparednessError extends Error {
  constructor(code = 'universal_policy_runtime_invalid') { super(code); this.name = 'UniversalPreparednessError'; this.code = code; }
}
const check = ok => { if (!ok) throw new UniversalPreparednessError(); };
const hash = value => createHash('sha256').update(canonicalJson(value)).digest('hex');
const hex = value => typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value);
const number = (value, max) => Number.isSafeInteger(value) && value >= 0 && value <= max;
function record(value, required, optional = []) {
  check(value && typeof value === 'object' && !Array.isArray(value) && [Object.prototype, null].includes(Object.getPrototypeOf(value)));
  const properties = Object.getOwnPropertyDescriptors(value);
  check(Reflect.ownKeys(properties).every(key => typeof key === 'string' && properties[key].enumerable && 'value' in properties[key]
    && (required.includes(key) || optional.includes(key))) && required.every(key => Object.hasOwn(properties, key)));
}
function issuer(value) {
  check(typeof value === 'string' && value.length <= 512 && !/[\s%\\?#]/u.test(value));
  let url; try { url = new URL(value); } catch { throw new UniversalPreparednessError(); }
  check(value === url.origin + '/human-identity' && !url.username && !url.password
    && (url.protocol === 'https:' || url.protocol === 'http:' && ['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname)));
}
function humanWire(value) {
  record(value, ['configured'], ['issuer', 'profile', 'protocolDigest', 'clientsDigest', 'clientCount', 'signingPublicKeysDigest', 'renewal']);
  if (value.configured === false) { record(value, ['configured']); return { configured: false }; }
  record(value, ['configured', 'issuer', 'profile', 'protocolDigest', 'clientsDigest', 'clientCount', 'signingPublicKeysDigest'], ['renewal']);
  check(value.configured === true && value.profile === HUMAN_IDENTITY_PROFILE && hex(value.protocolDigest) && hex(value.clientsDigest)
    && hex(value.signingPublicKeysDigest) && number(value.clientCount, 64) && value.clientCount >= 1); issuer(value.issuer);
  const result = Object.fromEntries(['configured', 'issuer', 'profile', 'protocolDigest', 'clientsDigest', 'clientCount', 'signingPublicKeysDigest'].map(key => [key, value[key]]));
  if (value.renewal !== undefined) {
    record(value.renewal, ['profile', 'admissionEnabled', 'maximumSessionSeconds', 'eligibleClientsDigest', 'eligibleClientCount']);
    check(value.renewal.profile === HUMAN_RENEWAL_PROFILE && typeof value.renewal.admissionEnabled === 'boolean'
      && value.renewal.maximumSessionSeconds === HUMAN_RENEWAL_LIMITS.sessionSeconds && hex(value.renewal.eligibleClientsDigest)
      && number(value.renewal.eligibleClientCount, value.clientCount));
    result.renewal = { ...value.renewal };
  }
  return result;
}
function reviewsWire(value) {
  record(value, ['configurationDigest', 'providerCount', 'bindingCount']);
  check(hex(value.configurationDigest) && number(value.providerCount, 128) && number(value.bindingCount, 128)
    && (value.bindingCount === 0 || value.providerCount > 0));
  return { configurationDigest: value.configurationDigest, providerCount: value.providerCount, bindingCount: value.bindingCount };
}
/** Sanitize an exact bounded measurement, never turn its metadata into authority. */
export function validateUniversalPreparedness(value) {
  try {
    record(value, ['schema', 'compiledLegacyMode', 'universalConfigured', 'reviewsConfigured', 'humanHttpEnabled', 'human', 'reviews']);
    check(value.schema === UNIVERSAL_RUNTIME_SCHEMA && [value.compiledLegacyMode, value.universalConfigured, value.reviewsConfigured, value.humanHttpEnabled]
      .every(item => typeof item === 'boolean'));
    const human = humanWire(value.human), reviews = reviewsWire(value.reviews);
    check(value.humanHttpEnabled === human.configured && (!value.reviewsConfigured || value.universalConfigured)
      && (!value.humanHttpEnabled || value.universalConfigured)
      && (!value.compiledLegacyMode || !value.universalConfigured && !value.reviewsConfigured && !value.humanHttpEnabled)
      && (value.reviewsConfigured || reviews.providerCount === 0 && reviews.bindingCount === 0));
    const result = { schema: value.schema, compiledLegacyMode: value.compiledLegacyMode, universalConfigured: value.universalConfigured,
      reviewsConfigured: value.reviewsConfigured, humanHttpEnabled: value.humanHttpEnabled, human, reviews };
    check(Buffer.byteLength(canonicalJson(result)) <= UNIVERSAL_RUNTIME_MAX_BYTES); return freezeDeep(result);
  } catch { throw new UniversalPreparednessError(); }
}
/** Trusted instantiated host profile only; never serialize credentials/key bodies. */
export function captureHumanPreparedness(profile) {
  try {
    if (!profile?.enabled) return Object.freeze({ configured: false });
    check(Object.isFrozen(profile) && Array.isArray(profile.publicClients) && profile.publicClients.length >= 1 && profile.publicClients.length <= 64
      && typeof profile.providerKeys === 'function' && profile.providerKeys.constructor?.name !== 'AsyncFunction');
    const clients = profile.publicClients.map(({ id, redirectUri, version, profileDigest }) => ({ id, redirectUri, version, profileDigest }));
    const keys = profile.providerKeys().jwks.keys;
    check(Array.isArray(keys) && keys.length >= 1 && keys.length <= 2);
    const publicKeys = keys.map(({ kty, kid, use, alg, n, e }) => ({ kty, kid, use, alg, n, e }));
    const result = { configured: true, issuer: profile.issuer, profile: profile.profile, protocolDigest: profile.protocolDigest,
      clientsDigest: hash(clients.sort((a, b) => a.id < b.id ? -1 : a.id > b.id ? 1 : 0)), clientCount: clients.length,
      signingPublicKeysDigest: hash(publicKeys.sort((a, b) => a.kid < b.kid ? -1 : a.kid > b.kid ? 1 : 0)) };
    if (profile.renewalAdmissionEnabled !== undefined) {
      check(typeof profile.renewalAdmissionEnabled === 'boolean' && typeof profile.renewalAllowed === 'function'
        && profile.renewalAllowed.constructor?.name !== 'AsyncFunction');
      const eligible = clients.filter(client => { const allowed = profile.renewalAllowed(client.id); check(typeof allowed === 'boolean'); return allowed; }).map(client => client.id).sort();
      result.renewal = { profile: HUMAN_RENEWAL_PROFILE, admissionEnabled: profile.renewalAdmissionEnabled,
        maximumSessionSeconds: HUMAN_RENEWAL_LIMITS.sessionSeconds, eligibleClientsDigest: hash(eligible), eligibleClientCount: eligible.length };
    }
    return freezeDeep(humanWire(result));
  } catch { throw new UniversalPreparednessError(); }
}
/** Pure host measurement after service/HTTP construction. No IO, DB, network or deployment imports. */
export function captureUniversalPreparedness({ compiledLegacyMode, universalConfigured, reviewsConfigured,
  humanProfile, humanHttpEnabled, reviewsPreparedness, reviewsConfiguration = { providers: [], bindings: [] } }) {
  let service;
  try {
    let reviews;
    if (reviewsPreparedness !== undefined) reviews = reviewsWire(reviewsPreparedness);
    else {
      // Compatibility for the existing trusted policy tests/config constructor.
      // This validates bounded static data; it neither executes nor probes a provider.
      service = createReviewsService({ configuration: reviewsConfiguration, actorActive: () => false, withAppAuthority: () => null });
      reviews = service.preparedness();
    }
    check(typeof humanHttpEnabled === 'boolean' && humanHttpEnabled === Boolean(humanProfile?.enabled));
    return validateUniversalPreparedness({ schema: UNIVERSAL_RUNTIME_SCHEMA, compiledLegacyMode, universalConfigured, reviewsConfigured,
      humanHttpEnabled, human: captureHumanPreparedness(humanProfile), reviews });
  } catch { throw new UniversalPreparednessError(); } finally { service?.close(); }
}
