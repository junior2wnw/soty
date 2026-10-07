import { capture, closed, connector, approvedOrigin, identifier, pin, sha, hash, need } from './profile.mjs';

export const SELECTED_RESOURCE_PROFILE = 'soty.selected-human-embed.v2';
export const RESOURCE_SOURCE_PROOF = 'soty.selected-source-request.v2';
export const RESOURCE_LAUNCH_CONTEXT = 'soty.verified-launch-continuation.v2';
const validatedProfiles = new WeakSet();

/** Native identifiers are opaque. No normalization, percent decoding or case folding. */
export function nativeResourceId(value) {
  need(typeof value === 'string' && value.length > 0 && value.isWellFormed()
    && Buffer.byteLength(value, 'utf8') <= 4096 && !/[\u0000-\u001f\u007f-\u009f]/u.test(value),
    'scoped_embed_native_id_invalid');
  return value;
}
export function resourceKind(value) {
  need(typeof value === 'string' && /^[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*\.v[1-9][0-9]*$/u.test(value)
    && value.length <= 128, 'scoped_embed_resource_kind_invalid');
  return value;
}
export function selectedResource(value) {
  closed(value, ['registryId', 'tenantId', 'appId', 'environmentId', 'resourceId', 'selection']);
  for (const field of ['registryId', 'tenantId', 'appId', 'environmentId', 'resourceId']) identifier(value[field]);
  closed(value.selection, ['kind', 'nativeId', 'incarnationId']);
  resourceKind(value.selection.kind); nativeResourceId(value.selection.nativeId); identifier(value.selection.incarnationId);
  return value;
}

/** Trusted host configuration. Parsing does not install an adapter or grant access. */
export function selectedResourceProfile(input) {
  if (input && typeof input === 'object' && validatedProfiles.has(input)) return input;
  const value = capture(input);
  closed(value, ['schema', 'appId', 'connector', 'target', 'sourceProfile', 'resource', 'issuer', 'clientId',
    'embedOrigin', 'nativeOrigin', 'parentOrigin']);
  need(value.schema === SELECTED_RESOURCE_PROFILE && /^app-[a-f0-9]{32}$/u.test(value.appId));
  connector(value.connector); closed(value.target, ['revision', 'digest']);
  need(Number.isSafeInteger(value.target.revision) && value.target.revision > 0); sha(value.target.digest);
  pin(value.sourceProfile); selectedResource(value.resource); need(value.resource.appId === value.appId);
  approvedOrigin(value.embedOrigin); approvedOrigin(value.nativeOrigin); approvedOrigin(value.parentOrigin);
  need(new URL(value.nativeOrigin).hostname !== new URL(value.embedOrigin).hostname && value.parentOrigin !== value.embedOrigin);
  const issuer = new URL(value.issuer);
  need(issuer.href === value.issuer && issuer.pathname === '/human-identity' && issuer.origin === value.parentOrigin
    && !issuer.search && !issuer.hash);
  identifier(value.clientId);
  need(Buffer.byteLength(JSON.stringify(value), 'utf8') <= 16384, 'scoped_embed_profile_too_large');
  const profile = Object.freeze({ ...value, digest: hash(value) });
  validatedProfiles.add(profile); return profile;
}

export function resourceConsentDigest(input) {
  const profile = selectedResourceProfile(input);
  return hash({ schema: 'soty.selected-source-consent.v2', appId: profile.appId, sourceProfile: profile.sourceProfile,
    resource: profile.resource, issuer: profile.issuer, clientId: profile.clientId, embedOrigin: profile.embedOrigin,
    nativeOrigin: profile.nativeOrigin, parentOrigin: profile.parentOrigin });
}

export const RESOURCE_TRANSPORT_LIMITS = Object.freeze({ proofHeaderBytes: 12288, headerBytes: 16384,
  continuations: 256, continuationMs: 300000, proofMs: 10000, inflight: 4, callMs: 8000 });
