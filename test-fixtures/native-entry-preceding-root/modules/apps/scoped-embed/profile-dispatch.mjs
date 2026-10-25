import { scopedEmbedProfile, scopedRoute, sourceConsentDigest, SCOPED_EMBED_PROFILE, need, SCOPED_EMBED_LIMITS } from './profile.mjs';
import { selectedResourceProfile, resourceConsentDigest, SELECTED_RESOURCE_PROFILE, RESOURCE_LAUNCH_CONTEXT } from './resource-profile.mjs';
import { selectedRoute, selectedRouteAdapter } from './resource-route-adapters.mjs';

export const isSelectedProfile = value => [SCOPED_EMBED_PROFILE, SELECTED_RESOURCE_PROFILE].includes(value);
export function approvedEmbedProfile(input) {
  return input?.schema === SELECTED_RESOURCE_PROFILE ? selectedResourceProfile(input) : scopedEmbedProfile(input);
}
export function requireEmbedAdapter(input) {
  const profile = approvedEmbedProfile(input);
  if (profile.schema === SELECTED_RESOURCE_PROFILE) selectedRouteAdapter(input);
  return profile;
}
export function embedRoute(profile, method, path) {
  if (profile.schema === SELECTED_RESOURCE_PROFILE) return selectedRoute(profile, method, path);
  need(profile.schema === SCOPED_EMBED_PROFILE);
  return Object.freeze({ kind: scopedRoute(method, path), requestBytes: SCOPED_EMBED_LIMITS.requestBytes,
    responseBytes: SCOPED_EMBED_LIMITS.responseBytes });
}
export const embedConsentDigest = input => input?.schema === SELECTED_RESOURCE_PROFILE ? resourceConsentDigest(input) : sourceConsentDigest(input);
export const embedContextSchema = profile => profile.schema === SELECTED_RESOURCE_PROFILE ? RESOURCE_LAUNCH_CONTEXT : 'soty.verified-launch-continuation.v1';
