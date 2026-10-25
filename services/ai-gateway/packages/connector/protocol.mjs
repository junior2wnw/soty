import { ConnectorError } from './errors.mjs';
import { resolveAlias } from './catalog.mjs';

export function validUsage(value) {
  if (!value || !Number.isSafeInteger(value.prompt_tokens) || value.prompt_tokens < 0 || !Number.isSafeInteger(value.completion_tokens) || value.completion_tokens < 0) return null;
  const details = {};
  for (const field of ['prompt_tokens_details', 'completion_tokens_details']) {
    const source = value[field];
    if (source && typeof source === 'object' && !Array.isArray(source)) details[field] = Object.fromEntries(Object.entries(source).filter(([, count]) => Number.isSafeInteger(count) && count >= 0));
  }
  return { prompt_tokens: value.prompt_tokens, completion_tokens: value.completion_tokens, total_tokens: value.prompt_tokens + value.completion_tokens, ...details };
}

function useful(message) {
  return typeof message?.content === 'string' && message.content.trim().length > 0 || typeof message?.refusal === 'string' && message.refusal.length > 0 || Array.isArray(message?.tool_calls) && message.tool_calls.length > 0;
}

function validateTools(calls) {
  if (!calls) return;
  if (!Array.isArray(calls) || calls.length === 0) throw new ConnectorError('upstream_invalid_tools');
  for (const call of calls) {
    if (!call?.id || call.type !== 'function' || !call.function?.name || typeof call.function.arguments !== 'string') throw new ConnectorError('upstream_invalid_tools');
    try { const args = JSON.parse(call.function.arguments); if (!args || typeof args !== 'object' || Array.isArray(args)) throw new Error(); }
    catch { throw new ConnectorError('upstream_invalid_tool_arguments'); }
  }
}

export function validateCompletion(body, request) {
  if (!body || !Array.isArray(body.choices) || body.choices.length !== 1 || body.error) throw new ConnectorError('upstream_invalid_completion');
  if (typeof body.model !== 'string' || resolveAlias(body.model) !== request.model) throw new ConnectorError('upstream_model_mismatch');
  const choice = body.choices[0];
  if (!['stop', 'length', 'tool_calls', 'content_filter', 'function_call'].includes(choice.finish_reason)) throw new ConnectorError('upstream_incomplete');
  if (!useful(choice.message) && choice.finish_reason !== 'content_filter') throw new ConnectorError('upstream_empty_completion');
  validateTools(choice.message?.tool_calls);
  if (request.response_format?.type === 'json_object' || request.response_format?.type === 'json_schema') {
    try { const json = JSON.parse(choice.message.content); if (request.response_format.type === 'json_object' && (!json || typeof json !== 'object' || Array.isArray(json))) throw new Error(); }
    catch { throw new ConnectorError('upstream_invalid_structured_output'); }
  }
  return { ...body, model: request.model, ...(validUsage(body.usage) ? { usage: validUsage(body.usage) } : {}) };
}

export async function prepareStream(response, request, { controller, onFirstUseful, onCommit = () => {}, onFinish, now = Date.now, idleMs = 30_000, maximumBytes = 8 * 1024 * 1024 }) {
  const reader = response.body?.getReader();
  if (!reader) throw new ConnectorError('upstream_empty_response');
  const decoder = new TextDecoder('utf-8', { fatal: true });
  const encoder = new TextEncoder();
  let buffer = '', bytes = 0, content = '', refusal = '', terminal = null, usage = null, doneMarker = false, ended = false, firstUseful = false;
  const calls = new Map();
  const queue = [];
  let finishResolve;
  const completion = new Promise((resolve) => { finishResolve = resolve; });
  const wholeBeforeCommit = Boolean(request.response_format?.type === 'json_object' || request.response_format?.type === 'json_schema' || request.tools?.length);
  let observedUseful = false;
  function observeUseful() {
    if (observedUseful) return;
    observedUseful = true;
    onFirstUseful(now());
  }
  async function finish(outcome, error) {
    if (ended) return;
    ended = true;
    await reader.cancel().catch(() => {});
    const result = { outcome, usage, toolsVerified: outcome === 'success' && calls.size > 0, errorCode: error?.code ?? null };
    try { await onFinish(result); } finally { finishResolve(result); }
  }
  function consume(frame) {
    const data = frame.split('\n').filter((line) => line.startsWith('data:')).map((line) => line.slice(5).trimStart()).join('\n');
    if (!data) return;
    if (data === '[DONE]') {
      if (!terminal) throw new ConnectorError('upstream_incomplete');
      const toolCalls = [...calls.values()].map((x) => ({ id: x.id, type: 'function', function: { name: x.name, arguments: x.arguments } }));
      validateCompletion({ model: request.model, choices: [{ message: { content, refusal, ...(toolCalls.length ? { tool_calls: toolCalls } : {}) }, finish_reason: terminal }] }, request);
      doneMarker = true;
      queue.push(encoder.encode('data: [DONE]\n\n'));
      return;
    }
    if (doneMarker) throw new ConnectorError('upstream_event_after_done');
    let value;
    try { value = JSON.parse(data); } catch { throw new ConnectorError('upstream_invalid_stream_json'); }
    if (!value || value.error || !Array.isArray(value.choices) || value.choices.length > 1) throw new ConnectorError('upstream_invalid_stream');
    if (value.model && resolveAlias(value.model) !== request.model) throw new ConnectorError('upstream_model_mismatch');
    if (validUsage(value.usage)) usage = validUsage(value.usage);
    const choice = value.choices[0];
    if (choice) {
      const delta = choice.delta ?? {};
      if (typeof delta.content === 'string') { content += delta.content; if (delta.content.trim()) { firstUseful = true; observeUseful(); } }
      if (typeof delta.refusal === 'string') { refusal += delta.refusal; firstUseful = true; observeUseful(); }
      if (delta.tool_calls) {
        if (!Array.isArray(delta.tool_calls)) throw new ConnectorError('upstream_invalid_tools');
        for (const call of delta.tool_calls) {
          if (!Number.isInteger(call.index) || call.index < 0 || call.index > 127) throw new ConnectorError('upstream_invalid_tools');
          const current = calls.get(call.index) ?? { id: '', name: '', arguments: '' };
          if (call.id) current.id = call.id;
          if (call.function?.name) current.name += call.function.name;
          if (call.function?.arguments) current.arguments += call.function.arguments;
          if (call.function?.name || call.function?.arguments) observeUseful();
          calls.set(call.index, current);
        }
      }
      if (choice.finish_reason) terminal = choice.finish_reason;
    }
    queue.push(encoder.encode(`data: ${JSON.stringify({ ...value, ...(value.model ? { model: request.model } : {}) })}\n\n`));
  }
  async function read() {
    const idle = setTimeout(() => controller.abort(new ConnectorError('upstream_stream_idle_timeout', 504)), idleMs);
    try {
      const item = await reader.read();
      if (item.done) {
        buffer += decoder.decode();
        if (buffer.trim()) consume(buffer);
        buffer = '';
        if (!doneMarker) throw new ConnectorError('upstream_incomplete');
        return false;
      }
      bytes += item.value.length;
      if (bytes > maximumBytes) throw new ConnectorError('upstream_response_too_large');
      try { buffer += decoder.decode(item.value, { stream: true }); }
      catch { throw new ConnectorError('upstream_invalid_utf8'); }
      buffer = buffer.replace(/\r\n/g, '\n');
      let separator;
      while ((separator = buffer.indexOf('\n\n')) >= 0) {
        const frame = buffer.slice(0, separator);
        buffer = buffer.slice(separator + 2);
        consume(frame);
      }
      return true;
    } finally { clearTimeout(idle); }
  }
  try {
    while (!doneMarker && !(firstUseful && !wholeBeforeCommit)) if (!await read()) break;
    if (!firstUseful && !doneMarker) throw new ConnectorError('upstream_empty_completion');
    onCommit(now());
  } catch (error) { if (usage && error && typeof error === 'object') error.safeUsage = usage; await reader.cancel().catch(() => {}); throw error; }
  const stream = new ReadableStream({
    async pull(output) {
      try {
        while (queue.length === 0 && !doneMarker) await read();
        if (queue.length) { output.enqueue(queue.shift()); return; }
        if (doneMarker) { await finish('success'); output.close(); }
      } catch (error) {
        const reason = controller.signal.aborted ? controller.signal.reason : error;
        await finish(reason?.code === 'request_canceled' ? 'canceled' : 'error', reason);
        output.error(reason);
      }
    },
    async cancel() { controller.abort(new ConnectorError('request_canceled', 499, { retryable: false })); await finish('canceled'); },
  });
  return { stream, completion, cancel: async () => { controller.abort(new ConnectorError('race_loser', 499, { retryable: false })); await finish('canceled'); } };
}
