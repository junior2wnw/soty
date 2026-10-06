import test from 'node:test';
import assert from 'node:assert/strict';
import { parseReviewsContext, parsePublicReviewSubject, parsePublicReviewPage, fetchPublicReviewJson, reviewSubjectKey } from './reviews-public.mjs';

// Independent wire fixture from Povédai API 1.1 source DTO. No production UGC.
const id = 'subject_0123456789abcdef0123', origin = 'https://reviews.example';
const ref = { id: 'provider:reviews', version: 1, digest: 'a'.repeat(64) };
const input = { ok: true, mode: 'public-read', availability: 'unprobed', subjects: [{ subjectKind: 'app', providerRef: ref,
  subjectRef: { ...ref, id: 'subject:application' }, providerSubjectId: id, providerEntityType: 'product',
  api: { subject: `${origin}/api/public/v1/subjects/${id}`, rating: `${origin}/api/public/v1/subjects/${id}/rating`, reviews: `${origin}/api/public/v1/subjects/${id}/reviews` }, publicPageUrl: `${origin}/subjects/${id}` }] };
const binding = parseReviewsContext(input).subjects[0];
const rating = { average: 4.5, count: 8, distribution: { 1: 0, 2: 0, 3: 0, 4: 1, 5: 1 }, distributionCount: 2, distributionComplete: false,
  updatedAt: '2026-10-06T12:00:00.000Z', provenance: { kind: 'imported', source: 'other' } };
const subject = { apiVersion: '1.1', subject: { id, entityType: 'product', title: 'Fixture', publication: { status: 'published' } },
  rating, counts: { publishedItems: 12, publishedReviews: 3, discussionItems: 9 }, links: { publicPage: `/subjects/${id}` } };
const review = { id: 'review_123', type: 'review', body: '<img src=x onerror=alert(1)>', author: { name: 'Fixture author', verified: true },
  createdAt: '2026-10-06T12:00:00.000Z', provenance: { kind: 'native', source: 'povedai' }, moderation: { status: 'published' }, internal: 'not projected' };
const page = { apiVersion: '1.1', subjectId: id, items: [review], page: { count: 1, total: 3, hasMore: true, nextCursor: 'cursor.signed_123' } };
const fails = code => error => error.code === code;
test('signed context pins exact URLs and keeps app/project/person namespaces separate', () => {
  assert.equal(parseReviewsContext({ ok: true, mode: 'disabled', subjects: [] }).mode, 'disabled');
  for (const patch of [ { api: { ...input.subjects[0].api, reviews: `${origin}/admin` } }, { publicPageUrl: 'javascript:alert(1)' },
    { providerEntityType: 'profile' }, { api: { ...input.subjects[0].api, subject: 'https://other.example/api/public/v1/subjects/'+id } } ]) {
    assert.throws(() => parseReviewsContext({ ...input, subjects: [{ ...input.subjects[0], ...patch }] }), fails('reviews_context_invalid'));
  }
  const person = { ...input.subjects[0], subjectKind: 'person', providerEntityType: 'profile' };
  const parsed = parseReviewsContext({ ...input, subjects: [input.subjects[0], person] });
  assert.notEqual(reviewSubjectKey(parsed.subjects[0]), reviewSubjectKey(parsed.subjects[1]));
  assert.throws(() => parseReviewsContext({ ...input, subjects: [input.subjects[0], input.subjects[0]] }));
});
test('a public subject must match the stable ID and type; imported aggregate stays distinct from review/comment counts', () => {
  const value = parsePublicReviewSubject(subject, binding);
  assert.equal(value.rating.count, 8); assert.equal(value.counts.publishedReviews, 3); assert.equal(value.counts.discussionItems, 9);
  assert.equal(value.rating.distributionComplete, false); assert.equal(value.rating.provenance.kind, 'imported');
  assert.throws(() => parsePublicReviewSubject({ ...subject, subject: { ...subject.subject, entityType: 'profile' } }, binding));
  assert.throws(() => parsePublicReviewSubject({ ...subject, subject: { ...subject.subject, id: 'subject_'+'f'.repeat(20) } }, binding));
  assert.throws(() => parsePublicReviewSubject({ ...subject, rating: { ...rating, count: Number.MAX_SAFE_INTEGER+1 } }, binding));
});
test('only published top-level reviews survive projection; verified text remains provider information', () => {
  const projected = parsePublicReviewPage(page, binding).items[0];
  assert.equal(projected.body, review.body); assert.equal(projected.author.verified, true); assert.equal(Object.hasOwn(projected, 'internal'), false);
  for (const patch of [{ type: 'comment' }, { parentId: 'parent_123' }, { moderation: { status: 'pending' } }, { rating: 6 }]) {
    assert.throws(() => parsePublicReviewPage({ ...page, items: [{ ...review, ...patch }] }, binding));
  }
  assert.throws(() => parsePublicReviewPage({ ...page, page: { ...page.page, nextCursor: 'https://untrusted.example' } }, binding));
  assert.throws(() => parsePublicReviewPage({ ...page, subjectId: 'subject_'+'f'.repeat(20) }, binding));
});
test('anonymous GET never supplies credentials or referrer and fails closed on redirects/status failures', async () => {
  let sent;
  const result = await fetchPublicReviewJson(binding, 'reviews', { cursor: page.page.nextCursor, fetch: async (url, options) => {
    sent = { url, options }; return new Response(JSON.stringify(page), { headers: { 'content-type': 'application/json' } });
  } });
  assert.equal(result.total, 3); assert.equal(sent.options.credentials, 'omit'); assert.equal(sent.options.redirect, 'error');
  assert.equal(sent.options.referrerPolicy, 'no-referrer'); assert.equal(sent.options.method, 'GET');
  assert.equal(new URL(sent.url).pathname, new URL(binding.api.reviews).pathname);
  assert.deepEqual(sent.options.headers, { accept: 'application/json' });
  await assert.rejects(() => fetchPublicReviewJson(binding, 'subject', { fetch: async () => ({ redirected: true, url: origin, ok: true }) }), fails('reviews_redirect_denied'));
  await assert.rejects(() => fetchPublicReviewJson(binding, 'subject', { fetch: async () => new Response('', { status: 404 }) }), fails('reviews_subject_unavailable'));
  await assert.rejects(() => fetchPublicReviewJson(binding, 'subject', { fetch: async () => new Response('', { status: 429 }) }), fails('reviews_rate_limited'));
});
test('declared and streaming response bounds reject an oversized provider before parsing', async () => {
  await assert.rejects(() => fetchPublicReviewJson(binding, 'subject', { fetch: async () => new Response('{}', { headers: { 'content-type': 'application/json', 'content-length': '524289' } }) }), fails('reviews_response_limit'));
  let cancelled = false;
  const stream = new ReadableStream({ start(controller) { controller.enqueue(new Uint8Array(300000)); controller.enqueue(new Uint8Array(300000)); }, cancel() { cancelled = true; } });
  await assert.rejects(() => fetchPublicReviewJson(binding, 'subject', { fetch: async () => new Response(stream, { headers: { 'content-type': 'application/json' } }) }), fails('reviews_response_limit'));
  assert.equal(cancelled, true);
});
test('loopback provider URLs require an explicit fixture mode and a canonical fixed path', () => {
  const local = 'http://127.0.0.1:5398', wire = structuredClone(input);
  for (const key of ['subject', 'rating', 'reviews']) wire.subjects[0].api[key] = wire.subjects[0].api[key].replace(origin, local);
  wire.subjects[0].publicPageUrl = wire.subjects[0].publicPageUrl.replace(origin, local);
  assert.throws(() => parseReviewsContext(wire)); assert.equal(parseReviewsContext(wire, { allowFixtureOrigins: true }).subjects[0].origin, local);
  wire.subjects[0].api.subject += '?token=untrusted'; assert.throws(() => parseReviewsContext(wire, { allowFixtureOrigins: true }));
});
