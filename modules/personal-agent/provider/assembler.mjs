import { GLM_MODEL, ProviderError, check, responseIdentifier, integer, parseJson, freeze } from './validation.mjs';
import { types } from 'node:util';

function unexpectedAsync(value) {
  if (types.isPromise(value)) { Promise.prototype.then.call(value, () => {}, () => {}); return true; }
  return value !== null && typeof value === 'object' && typeof value.then === 'function';
}
export function createAssembler(policy, tools, { assertActive, onText, state }) {
  let content = '', reasoning = '', finishReason = null, modelSeen = false, done = false, completionId = null;
  const calls = new Map();
  const parsed = raw => parseJson(raw, { maxBytes: policy.eventBytes, maxDepth: policy.jsonDepth, maxNodes: policy.jsonNodes });
  function append(current, fragment, maximum) {
    check(typeof fragment === 'string' && fragment.isWellFormed() && !fragment.includes('\0'), 'provider_invalid_chunk');
    check(Buffer.byteLength(current) + Buffer.byteLength(fragment) <= maximum, 'provider_output_limit'); return current + fragment;
  }
  function usage(value) {
    if (value === null || value === undefined) return;
    const previous = state.usage;
    // A contradictory or malformed later report invalidates earlier optimistic observations.
    state.usage = Object.freeze({ status: 'unknown' });
    check(value && typeof value === 'object' && !Array.isArray(value), 'provider_invalid_usage');
    const input = integer(value.prompt_tokens), output = integer(value.completion_tokens), total = value.total_tokens === undefined ? input + output : integer(value.total_tokens);
    check(Number.isSafeInteger(input + output) && total === input + output, 'provider_invalid_usage');
    check(output <= policy.maxOutputTokens, 'provider_token_limit');
    const current = { status: 'reported', promptTokens: input, completionTokens: output, totalTokens: total };
    check(previous.status === 'unknown' || JSON.stringify(previous) === JSON.stringify(current), 'provider_usage_changed'); state.usage = Object.freeze(current);
  }
  function event(raw) {
    assertActive();
    check(!done, 'provider_data_after_done');
    if (raw.trim() === '[DONE]') { done = true; return; }
    const chunk = parsed(raw); check(chunk && typeof chunk === 'object' && !Array.isArray(chunk), 'provider_invalid_chunk');
    check(!chunk.error, 'provider_upstream_failure');
    if (chunk.model !== undefined) { check(chunk.model === GLM_MODEL, 'provider_model_mismatch'); modelSeen = true; }
    if (chunk.id !== undefined) {
      const id = responseIdentifier(chunk.id); check(completionId === null || completionId === id, 'provider_completion_changed'); completionId = id; state.completionId = id;
    }
    usage(chunk.usage);
    check(Array.isArray(chunk.choices) && chunk.choices.length <= 1, 'provider_invalid_choice');
    if (!chunk.choices.length) return;
    const choice = chunk.choices[0]; check(choice && typeof choice === 'object' && choice.index === 0, 'provider_invalid_choice');
    const delta = choice.delta ?? {}; check(delta && typeof delta === 'object' && !Array.isArray(delta), 'provider_invalid_chunk');
    check(delta.role === undefined || delta.role === 'assistant', 'provider_role_mismatch');
    check(delta.refusal === undefined || delta.refusal === null || delta.refusal === '', 'provider_refused');
    check(delta.function_call === undefined, 'provider_invalid_tool');
    const hasData = ['content', 'reasoning_content', 'reasoning'].some(key => typeof delta[key] === 'string' && delta[key].length)
      || (Array.isArray(delta.tool_calls) && delta.tool_calls.length);
    check(modelSeen || !hasData, 'provider_model_unverified');
    check(!finishReason || !hasData, 'provider_data_after_finish');
    if (delta.content !== undefined && delta.content !== null) {
      const fragment = delta.content; content = append(content, fragment, policy.textBytes);
      if (fragment && onText) {
        let result; try { result = onText(fragment); } catch { throw new ProviderError('provider_observer_failed'); }
        check(!unexpectedAsync(result), 'provider_observer_failed'); assertActive();
      }
    }
    for (const key of ['reasoning_content', 'reasoning']) if (delta[key] !== undefined && delta[key] !== null) reasoning = append(reasoning, delta[key], policy.reasoningBytes);
    if (delta.tool_calls !== undefined) {
      check(Array.isArray(delta.tool_calls) && delta.tool_calls.length <= policy.toolCalls, 'provider_invalid_tool');
      for (const part of delta.tool_calls) {
        check(part && typeof part === 'object', 'provider_invalid_tool'); const index = integer(part.index, 0, policy.toolCalls - 1);
        const previous = calls.get(index) ?? { id: '', name: '', arguments: '', typeSeen: false };
        check(part.type === undefined || part.type === 'function', 'provider_invalid_tool'); if (part.type === 'function') previous.typeSeen = true;
        if (part.id !== undefined) previous.id = append(previous.id, part.id, 180);
        if (part.function !== undefined) {
          check(part.function && typeof part.function === 'object' && !Array.isArray(part.function), 'provider_invalid_tool');
          if (part.function.name !== undefined) previous.name = append(previous.name, part.function.name, 64);
          if (part.function.arguments !== undefined) previous.arguments = append(previous.arguments, part.function.arguments, policy.toolArgumentBytes);
        }
        calls.set(index, previous);
      }
    }
    if (choice.finish_reason !== undefined && choice.finish_reason !== null) {
      check(['stop', 'tool_calls'].includes(choice.finish_reason), choice.finish_reason === 'length' ? 'provider_output_incomplete' : 'provider_invalid_finish');
      check(!finishReason, 'provider_finish_changed'); finishReason = choice.finish_reason;
    }
  }
  function result() {
    assertActive(); check(done && finishReason, 'provider_stream_incomplete'); check(modelSeen, 'provider_model_unverified');
    check((calls.size > 0) === (finishReason === 'tool_calls'), 'provider_invalid_finish');
    check(calls.size > 0 || content.trim().length > 0, 'provider_empty_response');
    const ready = [], ids = new Set();
    for (const [index, call] of [...calls].sort(([a], [b]) => a - b)) {
      check(index === ready.length && call.typeSeen && /^[A-Za-z_][A-Za-z0-9_-]{0,63}$/u.test(call.name), 'provider_invalid_tool');
      const id = responseIdentifier(call.id); check(!ids.has(id), 'provider_duplicate_tool_id'); ids.add(id);
      check(tools.has(call.name), 'provider_unknown_tool');
      const args = parseJson(call.arguments, { maxBytes: policy.toolArgumentBytes, maxDepth: policy.jsonDepth, maxNodes: policy.jsonNodes }, 'provider_invalid_tool_json');
      check(args && typeof args === 'object' && !Array.isArray(args), 'provider_invalid_tool_json');
      ready.push({ id, name: call.name, arguments: freeze(args) });
    }
    // Pure trusted validators run only once every call is complete. No executor is called here.
    for (const call of ready) {
      let valid; try { valid = tools.get(call.name).validateArguments(call.arguments); } catch { throw new ProviderError('provider_tool_validation_failed'); }
      check(!unexpectedAsync(valid) && valid === true, 'provider_tool_validation_failed'); assertActive();
    }
    return freeze({ model: GLM_MODEL, completionId, finishReason, message: { role: 'assistant', content, toolCalls: ready },
      diagnostics: { reasoningBytes: Buffer.byteLength(reasoning), ...(policy.returnReasoning ? { reasoning } : {}) }, usage: state.usage });
  }
  return Object.freeze({ event, result, get done() { return done; } });
}

export function createSseDecoder(policy, assembler) {
  const decoder = new TextDecoder('utf-8', { fatal: true }); let buffer = '', total = 0, events = 0;
  function block(value) {
    check(++events <= policy.events, 'provider_event_count_limit');
    check(Buffer.byteLength(value) <= policy.eventBytes, 'provider_event_limit');
    const data = value.split(/\r?\n/u).filter(line => line.startsWith('data:')).map(line => line.slice(5).replace(/^ /u, '')).join('\n');
    if (data) assembler.event(data);
  }
  function extract() {
    let match;
    while ((match = /\r?\n\r?\n/u.exec(buffer))) {
      const item = buffer.slice(0, match.index); buffer = buffer.slice(match.index + match[0].length); block(item);
    }
    check(Buffer.byteLength(buffer) <= policy.eventBytes, 'provider_event_limit');
    check(!assembler.done || buffer.trim().length === 0, 'provider_data_after_done');
  }
  return {
    push(value) {
      check(value instanceof Uint8Array, 'provider_invalid_stream'); total += value.byteLength;
      check(total <= policy.streamBytes, 'provider_stream_limit');
      try { buffer += decoder.decode(value, { stream: true }); } catch { throw new ProviderError('provider_invalid_utf8'); }
      extract();
    },
    finish() {
      try { buffer += decoder.decode(); } catch { throw new ProviderError('provider_invalid_utf8'); }
      if (buffer.trim()) block(buffer); buffer = ''; return assembler.result();
    },
  };
}
