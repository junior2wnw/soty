/** Closed native Povédai read DTO. The native publicReview helper deliberately
 * returns known redacted fields as undefined in-process; only those named
 * omissions are accepted. JSON/network projections omit them normally. */
export class ManagedReviewProjectionError extends Error {
  constructor() { super('managed_reviews_source_projection_invalid'); this.code = this.message; this.status = 502; }
}
const need = value => { if (!value) throw new ManagedReviewProjectionError(); };
function record(value, required, optional = [], redacted = []) {
  need(value && typeof value === 'object' && !Array.isArray(value)
    && [Object.prototype, null].includes(Object.getPrototypeOf(value)));
  need(Object.getOwnPropertySymbols(value).length === 0);
  const fields = Object.getOwnPropertyDescriptors(value), allowed = [...required, ...optional, ...redacted];
  need(Object.keys(fields).length <= allowed.length && required.every(key => Object.hasOwn(fields, key)));
  const result = {};
  for (const [key, field] of Object.entries(fields)) {
    need(allowed.includes(key) && field.enumerable && Object.hasOwn(field, 'value'));
    if (redacted.includes(key)) { need(field.value === undefined); continue; }
    if (field.value === undefined && optional.includes(key)) continue;
    result[key] = field.value;
  }
  return result;
}
const string = (value, max = 10000) => need(typeof value === 'string' && value.length <= max && value.isWellFormed());
const id = value => { string(value, 200); need(value.length > 0 && !/[\u0000-\u001f\u007f]/u.test(value)); };
const integer = (value, min = 0, max = Number.MAX_SAFE_INTEGER) => need(Number.isSafeInteger(value) && !Object.is(value, -0) && value >= min && value <= max);
const enumeration = (value, values) => need(values.includes(value));
function array(value, max = 128) {
  need(Array.isArray(value) && Object.getPrototypeOf(value) === Array.prototype && value.length <= max && Object.getOwnPropertySymbols(value).length === 0);
  const fields = Object.getOwnPropertyDescriptors(value);
  need(Object.keys(fields).length === value.length + 1);
  return Array.from({ length: value.length }, (_, index) => { const field = fields[index]; need(field && field.enumerable && Object.hasOwn(field, 'value')); return field.value; });
}
function timestamp(value) {
  string(value, 40);
  need(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?(?:Z|[+-]\d{2}:\d{2})$/u.test(value) && Number.isFinite(Date.parse(value)));
}
function asset(value) {
  string(value, 2048); let url;
  try { url = new URL(value); } catch { need(false); }
  need(['https:', 'http:'].includes(url.protocol) && !url.username && !url.password);
}
function author(value) {
  const out = record(value, ['name'], ['avatarUrl', 'verified']);
  string(out.name, 200);
  if (Object.hasOwn(out, 'avatarUrl')) asset(out.avatarUrl);
  if (Object.hasOwn(out, 'verified')) need(typeof out.verified === 'boolean');
  return out;
}
function tag(value) {
  const out = record(value, ['key', 'label', 'kind', 'source', 'inherited']);
  string(out.key, 160); string(out.label, 80);
  enumeration(out.kind, ['topic', 'complex', 'developer', 'city', 'district']);
  enumeration(out.source, ['manual', 'inferred', 'object']);
  need(typeof out.inherited === 'boolean'); return out;
}
const required = ['id', 'objectId', 'type', 'rootId', 'depth', 'version', 'source', 'status', 'appearance', 'appearanceMeta', 'body', 'author', 'mediaUrls', 'tags', 'inferredTags', 'excludedTags', 'up', 'down', 'meta', 'createdAt', 'updatedAt'];
const optional = ['subjectId', 'parentId', 'publishedAt', 'deletedAt', 'moderatorEditedAt', 'rating', 'title', 'pros', 'cons', 'trustOutcome', 'trustCause', 'trustProvenance', 'effectiveTags', 'aiAssisted', 'importedAt'];
const redacted = ['originSiteId', 'originObjectId', 'channelId', 'authorId', 'externalId', 'externalParentId', 'legacyId', 'sourceUpdatedAt', 'lastSeenSyncRunId', 'statusOverriddenAt', 'deletedBy', 'deleteReason', 'deletionKind', 'moderatorEditedBy', 'consentEvidence'];
function review(value, binding) {
  const out = record(value, required, optional, redacted);
  for (const key of ['id', 'objectId', 'rootId', 'subjectId', 'parentId']) if (Object.hasOwn(out, key)) id(out[key]);
  need(out.objectId === binding.objectId && (!Object.hasOwn(out, 'subjectId') || out.subjectId === binding.subjectId));
  integer(out.depth, 0, 64); integer(out.version, 1); integer(out.up); integer(out.down);
  enumeration(out.type, ['review', 'comment']); enumeration(out.status, ['pending', 'approved', 'rejected', 'spam', 'deleted']);
  enumeration(out.appearance, ['standard', 'featured', 'expert', 'spotlight', 'official']); string(out.source, 160);
  string(out.body); for (const key of ['title', 'pros', 'cons']) if (Object.hasOwn(out, key)) string(out[key]);
  if (Object.hasOwn(out, 'rating')) integer(out.rating, 1, 5);
  if (Object.hasOwn(out, 'trustOutcome')) enumeration(out.trustOutcome, ['matched', 'partial', 'failed']);
  if (Object.hasOwn(out, 'trustCause')) enumeration(out.trustCause, ['quality', 'compatibility', 'description', 'seller', 'delivery', 'platform', 'personal', 'other']);
  if (Object.hasOwn(out, 'trustProvenance')) enumeration(out.trustProvenance, ['self_reported', 'authenticated', 'evidence']);
  if (Object.hasOwn(out, 'aiAssisted')) need(typeof out.aiAssisted === 'boolean');
  for (const key of ['createdAt', 'updatedAt', 'publishedAt', 'deletedAt', 'moderatorEditedAt', 'importedAt']) if (Object.hasOwn(out, key)) timestamp(out[key]);
  out.author = author(out.author); out.mediaUrls = array(out.mediaUrls).map(value => { asset(value); return value; });
  out.appearanceMeta = record(out.appearanceMeta, []); out.meta = record(out.meta, []);
  for (const key of ['tags', 'inferredTags', 'excludedTags']) out[key] = array(out[key], 0);
  if (Object.hasOwn(out, 'effectiveTags')) out.effectiveTags = array(out.effectiveTags).map(tag);
  return out;
}
export function projectManagedReviewList(value, { limit, binding }) {
  const out = record(value, ['items', 'nextCursor']);
  out.items = array(out.items, limit).map(value => review(value, binding));
  need(out.nextCursor === null || typeof out.nextCursor === 'string');
  if (out.nextCursor !== null) id(out.nextCursor);
  need(Buffer.byteLength(JSON.stringify(out)) <= 524288);
  return out;
}
