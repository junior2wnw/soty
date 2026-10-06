// Read-only Povédai API 1.1 adapter. Binding authority is the signed host
// context, never a public subject's name, domain or presence at the provider.
const types = Object.freeze({ app: ['product', 'service'], project: ['project'], person: ['profile'] });
const subjectPattern = /^subject_[0-9a-f]{20,64}$/u;
const count = value => Number.isSafeInteger(value) && value >= 0;
const record = value => value && typeof value === 'object' && !Array.isArray(value);
const fail = (code = 'reviews_response_invalid') => { throw Object.assign(new Error(code), { code }); };
const requireThat = (value, code) => { if (!value) fail(code); };
const text = (value, max, optional = false) => { if (optional && value === undefined) return undefined; requireThat(typeof value === 'string' && value.length <= max); return value; };
const date = value => { requireThat(typeof value === 'string' && value.length <= 64 && Number.isFinite(Date.parse(value))); return value; };
const closed = (value, keys, optional = []) => requireThat(record(value) && keys.every(key => Object.hasOwn(value, key)) && Object.keys(value).every(key => keys.includes(key) || optional.includes(key)), 'reviews_context_invalid');
function reference(value) {
  closed(value, ['id', 'version', 'digest']);
  requireThat(typeof value.id === 'string' && /^[A-Za-z][A-Za-z0-9._-]{0,63}:[A-Za-z][A-Za-z0-9._/-]{0,95}$/u.test(value.id) && !value.id.includes('..') && !value.id.includes('//') &&
    Number.isSafeInteger(value.version) && value.version >= 1 && value.version <= 1000000 && /^[a-f0-9]{64}$/u.test(value.digest), 'reviews_context_invalid');
  return Object.freeze({ id: value.id, version: value.version, digest: value.digest });
}
function originFor(value, fixtures) {
  let url; try { url = new URL(value); } catch { fail('reviews_context_invalid'); }
  const local = fixtures && url.protocol === 'http:' && ['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname) && Number(url.port) >= 1024 && Number(url.port) <= 65535;
  requireThat(!url.username && !url.password && (url.protocol === 'https:' || local), 'reviews_context_invalid');
  return url.origin;
}
export function reviewSubjectKey(binding) {
  return JSON.stringify([binding.subjectKind, binding.providerRef.id, binding.providerRef.version, binding.providerRef.digest,
    binding.subjectRef.id, binding.subjectRef.version, binding.subjectRef.digest, binding.providerSubjectId]);
}
export function parseReviewsContext(input, { allowFixtureOrigins = false } = {}) {
  closed(input, ['mode', 'subjects'], ['availability', 'ok']);
  requireThat(input.ok === undefined || input.ok === true, 'reviews_context_invalid');
  requireThat(Array.isArray(input.subjects) && input.subjects.length <= 3, 'reviews_context_invalid');
  if (input.mode === 'disabled') { requireThat(input.subjects.length === 0 && input.availability === undefined, 'reviews_context_invalid'); return Object.freeze({ mode: 'disabled', subjects: Object.freeze([]) }); }
  requireThat(input.mode === 'public-read', 'reviews_mode_unsupported');
  requireThat(input.availability === 'unprobed' && input.subjects.length > 0, 'reviews_context_invalid');
  const kinds = new Set(), providerOrigins = new Map();
  const subjects = input.subjects.map(value => {
    closed(value, ['subjectKind', 'providerRef', 'subjectRef', 'providerSubjectId', 'providerEntityType', 'api', 'publicPageUrl']);
    requireThat(Object.hasOwn(types, value.subjectKind) && types[value.subjectKind].includes(value.providerEntityType) && !kinds.has(value.subjectKind) && subjectPattern.test(value.providerSubjectId), 'reviews_context_invalid');
    kinds.add(value.subjectKind);
    const providerRef = reference(value.providerRef), subjectRef = reference(value.subjectRef);
    closed(value.api, ['subject', 'rating', 'reviews']);
    const origin = originFor(value.api.subject, allowFixtureOrigins), base = `${origin}/api/public/v1/subjects/${value.providerSubjectId}`;
    requireThat(value.api.subject === base && value.api.rating === `${base}/rating` && value.api.reviews === `${base}/reviews` && value.publicPageUrl === `${origin}/subjects/${value.providerSubjectId}`, 'reviews_context_invalid');
    const key = JSON.stringify(providerRef), prior = providerOrigins.get(key);
    requireThat(!prior || prior === origin, 'reviews_context_invalid'); providerOrigins.set(key, origin);
    return Object.freeze({ subjectKind: value.subjectKind, providerRef, subjectRef, providerSubjectId: value.providerSubjectId, providerEntityType: value.providerEntityType,
      api: Object.freeze({ ...value.api }), publicPageUrl: value.publicPageUrl, origin });
  });
  return Object.freeze({ mode: 'public-read', availability: 'unprobed', subjects: Object.freeze(subjects) });
}
function rating(value) {
  requireThat(record(value) && count(value.count) && (value.average === null || typeof value.average === 'number' && Number.isFinite(value.average) && value.average >= 1 && value.average <= 5));
  requireThat(record(value.distribution) && ['1', '2', '3', '4', '5'].every(key => count(value.distribution[key])) && Object.keys(value.distribution).length === 5);
  const distributionCount = Object.values(value.distribution).reduce((sum, n) => sum + n, 0);
  requireThat(count(distributionCount) && value.distributionCount === distributionCount && typeof value.distributionComplete === 'boolean' &&
    (!value.distributionComplete || distributionCount === value.count));
  requireThat(record(value.provenance) && ['native', 'imported'].includes(value.provenance.kind) && typeof value.provenance.source === 'string' && value.provenance.source.length <= 80);
  if (value.updatedAt !== null) date(value.updatedAt);
  return Object.freeze({ average: value.average, count: value.count, distributionCount, distributionComplete: value.distributionComplete, updatedAt: value.updatedAt,
    provenance: Object.freeze({ kind: value.provenance.kind, source: value.provenance.source }) });
}
export function parsePublicReviewSubject(value, binding) {
  requireThat(record(value) && value.apiVersion === '1.1' && record(value.subject) && value.subject.id === binding.providerSubjectId && value.subject.entityType === binding.providerEntityType && value.subject.publication?.status === 'published');
  requireThat(record(value.counts) && ['publishedItems', 'publishedReviews', 'discussionItems'].every(key => count(value.counts[key])));
  requireThat(value.links?.publicPage === `/subjects/${binding.providerSubjectId}`);
  return Object.freeze({ id: value.subject.id, entityType: value.subject.entityType, title: text(value.subject.title, 500), description: text(value.subject.description, 4000, true),
    rating: rating(value.rating), counts: Object.freeze({ publishedItems: value.counts.publishedItems, publishedReviews: value.counts.publishedReviews, discussionItems: value.counts.discussionItems }) });
}
function item(value) {
  requireThat(record(value) && typeof value.id === 'string' && value.id.length > 0 && value.id.length <= 160 && value.type === 'review' && value.parentId === undefined && value.moderation?.status === 'published');
  requireThat(value.rating === undefined || Number.isInteger(value.rating) && value.rating >= 1 && value.rating <= 5);
  requireThat(record(value.author) && typeof value.author.verified === 'boolean' && record(value.provenance) && ['native', 'imported'].includes(value.provenance.kind));
  return Object.freeze({ id: value.id, type: 'review', title: text(value.title, 240, true), body: text(value.body, 20000), pros: text(value.pros, 4000, true), cons: text(value.cons, 4000, true),
    rating: value.rating, author: Object.freeze({ name: text(value.author.name, 160), verified: value.author.verified }), createdAt: date(value.createdAt), imported: value.provenance.kind === 'imported' });
}
export function parsePublicReviewPage(value, binding) {
  requireThat(record(value) && value.apiVersion === '1.1' && value.subjectId === binding.providerSubjectId && Array.isArray(value.items) && value.items.length <= 10 && record(value.page));
  requireThat(value.page.count === value.items.length && count(value.page.total) && typeof value.page.hasMore === 'boolean');
  const cursor = value.page.nextCursor;
  requireThat(value.page.hasMore ? typeof cursor === 'string' && /^[A-Za-z0-9_.-]{1,4096}$/u.test(cursor) : cursor === null);
  const items = value.items.map(item); requireThat(new Set(items.map(value => value.id)).size === items.length);
  return Object.freeze({ items: Object.freeze(items), total: value.page.total, nextCursor: cursor });
}
export async function fetchPublicReviewJson(binding, endpoint, { cursor, signal, fetch: fetcher = globalThis.fetch } = {}) {
  requireThat(['subject', 'reviews'].includes(endpoint), 'reviews_context_invalid');
  const url = new URL(binding.api[endpoint]);
  if (endpoint === 'reviews') { url.searchParams.set('limit', '10'); if (cursor !== undefined) { requireThat(typeof cursor === 'string' && /^[A-Za-z0-9_.-]{1,4096}$/u.test(cursor)); url.searchParams.set('cursor', cursor); } }
  const controller = new AbortController(), cancel = () => controller.abort(signal?.reason);
  if (signal?.aborted) cancel(); else signal?.addEventListener('abort', cancel, { once: true });
  const timeout = setTimeout(() => controller.abort(), 10000);
  try {
    const response = await fetcher(url.href, { method: 'GET', credentials: 'omit', redirect: 'error', referrerPolicy: 'no-referrer', signal: controller.signal, headers: { accept: 'application/json' } });
    if (response.redirected || response.url && response.url !== url.href) fail('reviews_redirect_denied');
    if (!response.ok) fail(response.status === 404 ? 'reviews_subject_unavailable' : response.status === 429 ? 'reviews_rate_limited' : 'reviews_provider_unavailable');
    const declared = response.headers.get('content-length');
    if (declared && (!/^\d+$/u.test(declared) || Number(declared) > 524288)) fail('reviews_response_limit');
    requireThat(/^application\/(?:json|[^;]+\+json)(?:;|$)/iu.test(response.headers.get('content-type') ?? ''), 'reviews_response_invalid');
    requireThat(response.body, 'reviews_response_invalid');
    const reader = response.body.getReader(), parts = []; let length = 0;
    try { while (true) { const { value, done } = await reader.read(); if (done) break; length += value.byteLength; if (length > 524288) { await reader.cancel(); fail('reviews_response_limit'); } parts.push(value); } }
    finally { reader.releaseLock(); }
    const bytes = new Uint8Array(length); let offset = 0; for (const part of parts) { bytes.set(part, offset); offset += part.length; }
    let value; try { value = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)); } catch { fail('reviews_response_invalid'); }
    return endpoint === 'subject' ? parsePublicReviewSubject(value, binding) : parsePublicReviewPage(value, binding);
  } finally { clearTimeout(timeout); signal?.removeEventListener('abort', cancel); }
}
