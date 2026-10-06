import { createAdmissionHost, closeAdmissionHost, contractDigest } from '../index.mjs';
import { snapshot } from '../json.mjs';
import { freezeDeep } from '../../capabilities/server/validation.mjs';

export class RegistrationError extends Error {
  constructor(code, status = 400) { super(code); this.name = 'RegistrationError'; this.code = code; this.status = status; }
}
export const requireRegistration = (condition, code, status = 400) => {
  if (!condition) throw new RegistrationError(code, status);
};
export function closed(value, required, optional = [], code = 'registration_fields_invalid') {
  requireRegistration(value && typeof value === 'object' && !Array.isArray(value)
    && required.every(key => Object.hasOwn(value, key))
    && Object.keys(value).every(key => required.includes(key) || optional.includes(key)), code);
}
export const literalId = value => {
  requireRegistration(typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,95}$/u.test(value)
    && !value.includes('..') && !value.includes('//'), 'registration_id_invalid'); return value;
};
export const integer = (value, min = 0, max = 1000000) => {
  requireRegistration(Number.isSafeInteger(value) && value >= min && value <= max, 'registration_revision_invalid'); return value;
};
export const synchronous = (value, code = 'registration_async_boundary') => {
  if (value && typeof value.then === 'function') { Promise.resolve(value).catch(() => {}); throw new RegistrationError(code, 500); }
  return value;
};
export const refPart = value => ({ id: value.id, version: value.version, digest: value.digest });
export const REGISTRIES = Object.freeze(['bindings', 'providers', 'profiles', 'skills', 'docs', 'placements', 'publicSubjects']);
const same = (a, b) => contractDigest(a) === contractDigest(b);

/** Trusted host data only. This is not an HTTP Host resolver or source-ownership proof. */
export function captureReviewedConfiguration(reviewedProfile, approvedReferences = {}) {
  const profile = snapshot(reviewedProfile), extra = snapshot(approvedReferences);
  closed(profile, ['id', 'version', 'digest', 'feedback']); closed(extra, [], REGISTRIES);
  const baseline = {
    providers: [{ ...refPart(profile.feedback?.provider || {}), kind: 'feedback', publicRead: false }],
    profiles: [
      { ...refPart(profile.feedback?.captureProfile || {}), kind: 'capture' },
      { ...refPart(profile.feedback?.retentionProfile || {}), kind: 'retention' },
    ],
  };
  const references = Object.fromEntries(REGISTRIES.map(kind => {
    const entries = [...(baseline[kind] || [])];
    requireRegistration(extra[kind] === undefined || Array.isArray(extra[kind]), 'registration_configuration_invalid');
    for (const entry of extra[kind] || []) {
      const prior = entries.find(value => value.id === entry.id && value.version === entry.version);
      requireRegistration(!prior || same(prior, entry), 'immutable_pin_conflict', 409);
      if (!prior) entries.push(entry);
    }
    return [kind, entries];
  }));
  // Reuse the bounded closed contract validator, even before the first app exists.
  const host = createAdmissionHost({
    context: { scope: { registryId: 'configuration', tenantId: 'configuration', appId: 'configuration', environmentId: 'configuration' },
      namespace: 'configuration', ownerId: 'configuration', authorityRevision: 1, visibility: 'public',
      source: { id: 'configuration:source', revision: 1, digest: '0'.repeat(64) }, auth: { mode: 'public' } },
    ...references, authorProfile: profile,
  });
  closeAdmissionHost(host);
  return freezeDeep({ reviewedProfile: profile, approvedReferences: references });
}

/** Snapshot comes from the Apps owner/source transaction, never from registration args. */
export function buildTrustedAppConfiguration({ sourceSnapshot, actor, registryId, environmentId, authorityRevision = 1,
  reviewedProfile, approvedReferences }) {
  const value = snapshot(sourceSnapshot);
  closed(value, ['appId', 'ownerId', 'accountId', 'appRevision', 'policyEpoch', 'target', 'visibility', 'grants']);
  closed(value.target, ['revision', 'digest', 'profile']);
  literalId(value.appId); literalId(value.ownerId); literalId(value.accountId);
  literalId(registryId); literalId(environmentId); integer(value.appRevision, 1); integer(value.policyEpoch, 1);
  integer(value.target.revision, 1); integer(authorityRevision, 1);
  requireRegistration(typeof value.target.digest === 'string' && /^[a-f0-9]{64}$/u.test(value.target.digest), 'registration_source_invalid');
  requireRegistration(typeof value.target.profile === 'string' && value.target.profile.length > 0
    && value.target.profile.length <= 96, 'registration_source_invalid');
  requireRegistration(value.accountId === actor.accountId && value.ownerId === actor.accountId,
    'registration_app_not_owned', 403);
  requireRegistration(['private', 'public'].includes(value.visibility), 'registration_source_invalid');
  closed(value.grants, ['accountIds', 'communityIds']);
  for (const ids of Object.values(value.grants)) {
    requireRegistration(Array.isArray(ids) && ids.length <= 64 && new Set(ids).size === ids.length, 'registration_source_invalid');
    ids.forEach(literalId);
  }
  const configuration = {
    context: { scope: { registryId, tenantId: actor.accountId, appId: value.appId, environmentId },
      namespace: 'app.' + value.appId, ownerId: value.ownerId, authorityRevision,
      visibility: value.visibility,
      source: { id: 'apps:' + value.appId + '/target', revision: value.target.revision, digest: value.target.digest },
      // The app's own authentication is unspecified/public; this does not claim working Soty shared login.
      auth: { mode: 'public' } },
    ...approvedReferences, authorProfile: reviewedProfile,
  };
  const host = createAdmissionHost(configuration); closeAdmissionHost(host);
  // Operational source-policy generations fence readiness; they do not modify a capability semantic contract.
  const fingerprint = contractDigest({ configuration: { ...configuration,
    context: { ...configuration.context, authorityRevision: 1 } }, sourceSnapshot: value });
  return freezeDeep({ configuration, fingerprint, scopeKey: contractDigest(configuration.context.scope) });
}

export function configurationPins(configuration) {
  const pins = REGISTRIES.flatMap(kind => configuration[kind].map(value => ({ kind, value })));
  pins.push({ kind: 'authorProfiles', value: configuration.authorProfile });
  for (const binding of configuration.bindings) pins.push({ kind: 'capabilityContracts', value: binding.capability });
  return pins;
}
