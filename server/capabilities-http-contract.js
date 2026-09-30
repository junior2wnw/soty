/** Transport identifiers are shared by the router and its OpenAPI document. */
export const CAPABILITIES_BASE = '/api/capabilities/v1';
export const NOTES_DRAFT_PATH = `${CAPABILITIES_BASE}/notes/drafts`;
export const INVOCATIONS_PATH = `${CAPABILITIES_BASE}/invocations`;
export const SERVICE_DELEGATION_PATH = `${CAPABILITIES_BASE}/grants/derive`;
export const SERVICE_DELEGATION_BODY_BYTES = 16384;
export const INVOCATION_ID_PATTERN = '^inv_(?:[a-f0-9]{32}|[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})$';
export const NATIVE_NOTE_ID_PATTERN = '^n_[a-f0-9]{64}$';
