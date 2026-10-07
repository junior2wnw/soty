import { SourceFeedbackClientError, feedbackInput, feedbackFields, feedbackOutput } from '../shared/feedback-wire.mjs';
import { parseSourceJson } from '../shared/strict-json.mjs';
export { SourceFeedbackClientError };
/** Cookie/Source authority stays on this exact current origin. No actor, token,
 * resource, recipient or arbitrary URL is accepted from the feedback form. */
export function createSourceFeedbackClient({ fetch: fetcher = globalThis.fetch } = {}) {
  const root = '/api/embed/feedback';
  async function request(path, args, method = 'POST', signal, operation) {
    const response = await fetcher(root + path, { method, credentials: 'same-origin', redirect: 'error', referrerPolicy: 'no-referrer', signal,
      headers: { accept: 'application/json', ...(args === undefined ? {} : { 'content-type': 'application/json' }) },
      ...(args === undefined ? {} : { body: JSON.stringify(args) }) });
    if (response.redirected || !/^application\/json(?:;|$)/iu.test(response.headers.get('content-type') || '')) throw new SourceFeedbackClientError('source_feedback_response_invalid');
    let value;
    try {
      const reader = response.body.getReader(), chunks = []; let size = 0;
      try { while (true) { const { done, value: chunk } = await reader.read(); if (done) break;
        size += chunk.byteLength; if (size > 1500000) throw new Error('limit'); chunks.push(chunk); } }
      finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
      const bytes = new Uint8Array(size); let offset = 0; for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
      value = parseSourceJson(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
    } catch { throw new SourceFeedbackClientError('source_feedback_response_invalid'); }
    if (!response.ok) throw new SourceFeedbackClientError(typeof value?.error?.code === 'string' && /^[a-z0-9_]{1,128}$/u.test(value.error.code)
      ? value.error.code : 'source_feedback_unknown', response.status);
    try { feedbackFields(value, ['ok', 'data']); } catch { throw new SourceFeedbackClientError('source_feedback_response_invalid'); }
    if (value.ok !== true) throw new SourceFeedbackClientError('source_feedback_response_invalid');
    try { return feedbackOutput(operation, value.data); } catch { throw new SourceFeedbackClientError('source_feedback_response_invalid'); }
  }
  return Object.freeze({
    context: signal => request('/context', undefined, 'GET', signal, 'context'),
    list: ({ limit = 20, cursor } = {}, signal) => {
      feedbackInput('list', { limit, ...(cursor === undefined ? {} : { cursor }) });
      const query = '?limit=' + limit + (cursor ? '&cursor=' + encodeURIComponent(cursor) : '');
      return request(query, undefined, 'GET', signal, 'list');
    },
    get: (ticketId, signal) => {
      feedbackInput('get', { ticketId });
      return request('/ticket?ticketId=' + encodeURIComponent(ticketId), undefined, 'GET', signal, 'get');
    },
    submit(args, signal) {
      return request('', feedbackInput('submit', args), 'POST', signal, 'submit');
    },
    reply: (args, signal) => request('/reply', feedbackInput('reply', args), 'POST', signal, 'reply'),
    status: (args, signal) => request('/status', feedbackInput('status', args), 'POST', signal, 'status'),
    accept: (args, signal) => request('/accept', feedbackInput('accept', args), 'POST', signal, 'accept'),
  });
}
