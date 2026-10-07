import { GLM_MODEL, ProviderError, check, boundedPolicy, freeze, identifier } from './validation.mjs';
import { capturedTools, captureRequest } from './request.mjs';
import { createAssembler, createSseDecoder } from './assembler.mjs';
export { GLM_MODEL, ProviderError, HARD_LIMITS } from './validation.mjs';

const SAFE_CODES = new Set(['provider_invalid_input', 'provider_invalid_json', 'provider_invalid_chunk', 'provider_output_limit',
  'provider_invalid_usage', 'provider_usage_changed', 'provider_token_limit', 'provider_data_after_done', 'provider_data_after_finish',
  'provider_upstream_failure', 'provider_model_mismatch', 'provider_model_unverified', 'provider_completion_changed',
  'provider_invalid_choice', 'provider_role_mismatch', 'provider_refused', 'provider_invalid_tool', 'provider_unknown_tool',
  'provider_duplicate_tool_id', 'provider_invalid_tool_json', 'provider_tool_validation_failed', 'provider_invalid_finish',
  'provider_finish_changed', 'provider_output_incomplete', 'provider_stream_incomplete', 'provider_empty_response',
  'provider_observer_failed', 'provider_event_limit', 'provider_event_count_limit', 'provider_invalid_stream', 'provider_stream_limit', 'provider_invalid_utf8',
  'provider_cancelled', 'provider_timeout', 'provider_closed', 'provider_transport_failed', 'provider_http_failed', 'provider_body_failed']);
function safeResponseId(value) {
  if (value === null || value === undefined) return null;
  try {
    const result = identifier(value);
    return /^(sk-|obk-|gm_|gc_|gk_)/iu.test(result) ? null : result;
  } catch { return null; }
}

/** Transport is a trusted backend callback. This module neither constructs an URL nor handles a bearer. */
export function createGlmProvider({ transport, policy: inputPolicy = {}, tools: inputTools = [] } = {}) {
  check(typeof transport === 'function', 'provider_transport_required');
  const policy = boundedPolicy(inputPolicy), tools = capturedTools(inputTools, policy), active = new Set();
  let closed = false;
  async function complete(input) {
    check(!closed, 'provider_closed');
    const request = captureRequest(input, tools, policy);
    check(active.size < policy.concurrency, 'provider_busy');
    const controller = new AbortController(), signal = AbortSignal.any([controller.signal, ...(request.signal ? [request.signal] : [])]);
    const state = { requestId: request.requestId, requestDigest: request.requestDigest, dispatchAttempted: false,
      usage: Object.freeze({ status: 'unknown' }), completionId: null, providerRequestId: null, httpStatus: null };
    active.add(controller);
    const timer = setTimeout(() => controller.abort('timeout'), policy.timeoutMs);
    const abortCode = () => closed ? 'provider_closed' : (request.signal?.aborted ? 'provider_cancelled' : 'provider_timeout');
    const assertActive = () => { check(!signal.aborted, abortCode()); };
    const accounting = () => freeze({ requestId: state.requestId, requestDigest: state.requestDigest,
      dispatchAttempted: state.dispatchAttempted, usage: state.usage, completionId: state.completionId,
      providerRequestId: state.providerRequestId, httpStatus: state.httpStatus });
    let abortListener;
    const aborted = new Promise((resolve, reject) => {
      abortListener = () => reject(new ProviderError(abortCode()));
      if (signal.aborted) abortListener(); else signal.addEventListener('abort', abortListener, { once: true });
    });
    const work = Promise.resolve().then(async () => {
      assertActive(); state.dispatchAttempted = true;
      let response;
      try { response = await transport(Object.freeze({ requestId: request.requestId, requestDigest: request.requestDigest, body: request.body, signal })); }
      catch { if (signal.aborted) throw new ProviderError(abortCode()); throw new ProviderError('provider_transport_failed'); }
      let reader, cancelPromise, removeCancel = () => {};
      function cancelReader() {
        if (reader && !cancelPromise) {
          try { cancelPromise = Promise.resolve(reader.cancel()).catch(() => {}); }
          catch { cancelPromise = Promise.resolve(); }
        }
      }
      try {
        check(response && Number.isInteger(response.status) && response.status >= 100 && response.status <= 599
          && typeof response.headers?.get === 'function', 'provider_invalid_stream');
        state.httpStatus = response.status; state.providerRequestId = safeResponseId(response.headers.get('x-request-id'));
        check(typeof response.body?.getReader === 'function', 'provider_invalid_stream'); reader = response.body.getReader();
        check(typeof reader.read === 'function' && typeof reader.cancel === 'function' && typeof reader.releaseLock === 'function', 'provider_invalid_stream');
        signal.addEventListener('abort', cancelReader, { once: true }); removeCancel = () => signal.removeEventListener('abort', cancelReader);
        if (signal.aborted) cancelReader(); assertActive();
        check(response.status === 200, 'provider_http_failed');
        const contentType = response.headers.get('content-type')?.split(';', 1)[0].trim().toLowerCase();
        check(contentType === 'text/event-stream', 'provider_invalid_stream');
        const assembler = createAssembler(policy, tools, { assertActive, onText: request.onText, state });
        const decoder = createSseDecoder(policy, assembler);
        while (!assembler.done) {
          assertActive(); let next;
          try { next = await reader.read(); } catch { if (signal.aborted) throw new ProviderError(abortCode()); throw new ProviderError('provider_body_failed'); }
          assertActive(); check(next && typeof next.done === 'boolean', 'provider_invalid_stream');
          if (next.done) break; decoder.push(next.value);
        }
        const result = decoder.finish(); assertActive();
        return freeze({ requestId: request.requestId, requestDigest: request.requestDigest, providerRequestId: state.providerRequestId, ...result });
      } finally {
        cancelReader(); if (cancelPromise) await cancelPromise;
        removeCancel(); if (reader) { try { reader.releaseLock(); } catch { /* Pending reads retain the slot until the work settles. */ } }
      }
    });
    // Timeout/cancel returns promptly, but uncooperative transport/body retains its bounded slot.
    work.then(() => active.delete(controller), () => active.delete(controller));
    try { return await Promise.race([work, aborted]); }
    catch (error) {
      const code = error instanceof ProviderError && SAFE_CODES.has(error.code) ? error.code : 'provider_transport_failed';
      throw new ProviderError(code, accounting());
    } finally { clearTimeout(timer); signal.removeEventListener('abort', abortListener); }
  }
  function close() { if (!closed) { closed = true; for (const controller of active) controller.abort('closed'); } }
  return Object.freeze({ complete, close });
}
