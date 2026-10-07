// Portable private Source entry. A profile/locator alone grants no resource.
export { selectedResourceProfile, resourceConsentDigest, SELECTED_RESOURCE_PROFILE } from './resource-profile.mjs';
export { HIVE_SELECTED_SOURCE,HIVE_SELECTED_KERNEL_SOURCE, selectedRoute, selectedNativeHandoff } from './resource-route-adapters.mjs';
export { createResourceSourceProofVerifier } from './resource-proof.mjs';
export { createSourceAuthorityClient } from './source-authority-client.mjs';
