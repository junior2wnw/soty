import { capabilityDigest, validateDescriptor, validateAuthorDraft, materializeAuthorDraft, SCHEMA } from './index.mjs';
import { check, snapshot } from './json.mjs';
export { materializeAuthorDraft };
export const createAuthorDraft = input => validateAuthorDraft(input);

/** Metadata factory. No authority, provider access or executable handler is created. */
export function createDescriptor(input) {
  const value = snapshot(input);
  if (!Object.hasOwn(value, 'schema')) value.schema = SCHEMA;
  check(value.schema === SCHEMA, 'unsupported_schema');
  for (const field of ['capabilities', 'skills', 'docs']) if (!Object.hasOwn(value, field)) value[field] = [];
  if (!Object.hasOwn(value, 'reviews')) value.reviews = { mode: 'disabled' };
  check(Array.isArray(value.capabilities), 'invalid_descriptor');
  for (const cap of value.capabilities) {
    const computed = capabilityDigest(cap);
    if (!Object.hasOwn(cap, 'digest')) cap.digest = computed;
    else check(cap.digest === computed, 'capability_digest_mismatch');
  }
  return validateDescriptor(value);
}
