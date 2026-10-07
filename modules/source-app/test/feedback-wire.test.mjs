import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createSourceFeedbackClient } from '../browser/feedback-client.mjs';
import { feedbackInput, feedbackOutput, SOURCE_FEEDBACK_LIMITS } from '../shared/feedback-wire.mjs';
import { parseSourceJson } from '../shared/strict-json.mjs';
import { validateSourceFeedbackInput } from '../server/feedback-wire.mjs';

test('feedback wire rejects body authority, getters, broadened support status, malformed and excessive media', async () => {
  const args = { requestId: 'fixture-submit-0001', body: 'Synthetic issue', attachments: [] };
  for (const field of ['actor', 'recipient', 'resource', 'rootOwner', 'url', 'token']) assert.throws(() => feedbackInput('submit', { ...args, [field]: 'self-claim' }));
  let reads = 0; assert.throws(() => feedbackInput('submit', { ...args, get actor() { reads++; return true; } })); assert.equal(reads, 0);
  assert.throws(() => feedbackInput('status', { requestId: 'fixture-status-0001', ticketId: 'one', expectedRevision: 1, status: 'resolved' }));
  const png = await readFile(new URL('../../feedback/test/fixtures/chrome.png', import.meta.url));
  const media = { kind: 'image', name: 'synthetic.png', mimeType: 'image/png', dataBase64: png.toString('base64') };
  assert.equal(validateSourceFeedbackInput('submit', { ...args, attachments: [media] }).attachments.length, 1);
  assert.throws(() => validateSourceFeedbackInput('submit', { ...args, attachments: [{ ...media, dataBase64: 'YWJjZA==' }] }));
  assert.throws(() => feedbackInput('submit', { ...args, attachments: Array(4).fill(media) }));
  const long = await readFile(new URL('../../feedback/test/fixtures/ffmpeg-opus-121s.ogg', import.meta.url));
  assert.throws(() => validateSourceFeedbackInput('submit', { ...args, attachments: [{ kind: 'audio', name: 'synthetic.ogg', mimeType: 'audio/ogg', dataBase64: long.toString('base64') }] }), error => error.code === 'feedback_audio_duration_limit');
});

test('portable JSON guard rejects decoded duplicate keys, nested duplicates and complexity before Native hook', () => {
  for (const input of ['{"requestId":"first","request\\u0049d":"second"}', '{"input":{"actor":1,"actor":2}}', '[1,]', '{"body":"a"} trailing']) assert.throws(() => parseSourceJson(input));
  assert.deepEqual(parseSourceJson('{"input":{"title":"Synthetic"},"requestId":"one"}'), { input: { title: 'Synthetic' }, requestId: 'one' });
});

test('browser fixed client validates current context, response bytes/duplicate keys and Source private projection', async () => {
  const context = { schema: 'soty.source-feedback.context.v1', bindingDigest: 'a'.repeat(64), ready: true, canSubmit: true,
    recipientLabel: 'Поддержка приложения', limits: SOURCE_FEEDBACK_LIMITS, capabilities: { text: true, screenshot: true, voice: true, asr: false } };
  const calls = [], client = createSourceFeedbackClient({ fetch: async (url, options) => { calls.push({ url, options });
    return new Response(JSON.stringify({ ok: true, data: context }), { headers: { 'content-type': 'application/json' } }); } });
  assert.equal((await client.context()).bindingDigest, context.bindingDigest); assert.equal(calls[0].url, '/api/embed/feedback/context');
  assert.equal(calls[0].options.credentials, 'same-origin'); assert.equal(calls[0].options.redirect, 'error');
  for (const text of ['{"ok":true,"ok":false,"data":{}}', JSON.stringify({ ok: true, data: { ...context, principal: 'private-native-id' } }), ' '.repeat(1500001)]) {
    const bad = createSourceFeedbackClient({ fetch: async () => new Response(text, { headers: { 'content-type': 'application/json' } }) });
    await assert.rejects(bad.context(), error => error.code === 'source_feedback_response_invalid');
  }
  assert.throws(() => feedbackOutput('context', { ...context, capabilities: { ...context.capabilities, asr: true } }));
});
