import { GLM_MODEL, check, fields, string, identifier, canonical, parseJson, freeze, hash } from './validation.mjs';

export function capturedTools(value, policy) {
  check(Array.isArray(value) && value.length <= policy.toolCalls);
  const result = new Map();
  for (const item of value) {
    fields(item, ['name', 'description', 'parameters', 'validateArguments']);
    check(typeof item.name === 'string' && /^[A-Za-z_][A-Za-z0-9_-]{0,63}$/u.test(item.name)
      && !result.has(item.name) && typeof item.validateArguments === 'function'
      && item.validateArguments.constructor?.name !== 'AsyncFunction');
    const description = string(item.description, 4000, { empty: true });
    const parameters = parseJson(canonical(item.parameters, policy.requestBytes, policy.jsonDepth),
      { maxBytes: policy.requestBytes, maxDepth: policy.jsonDepth, maxNodes: policy.jsonNodes });
    check(parameters.type === 'object' && parameters.additionalProperties === false
      && parameters.properties !== null && typeof parameters.properties === 'object' && !Array.isArray(parameters.properties));
    result.set(item.name, Object.freeze({ name: item.name, description, parameters: freeze(parameters), validateArguments: item.validateArguments.bind(item) }));
  }
  return result;
}
export function captureRequest(value, tools, policy) {
  fields(value, ['requestId', 'messages'], ['signal', 'onText']);
  check(value.signal === undefined || value.signal instanceof AbortSignal);
  check(value.onText === undefined || (typeof value.onText === 'function' && value.onText.constructor?.name !== 'AsyncFunction'));
  const requestId = identifier(value.requestId, 160);
  check(Array.isArray(value.messages) && value.messages.length > 0 && value.messages.length <= policy.messages);
  const messages = value.messages.map(item => {
    check(item !== null && typeof item === 'object');
    if (item.role === 'tool') {
      fields(item, ['role', 'tool_call_id', 'content']);
      return { role: 'tool', tool_call_id: identifier(item.tool_call_id), content: string(item.content, policy.messageBytes, { empty: true }) };
    }
    if (item.role === 'assistant') {
      fields(item, ['role', 'content'], ['tool_calls']);
      const content = item.content === null ? null : string(item.content, policy.messageBytes, { empty: true });
      const message = { role: 'assistant', content };
      if (item.tool_calls !== undefined) {
        check(Array.isArray(item.tool_calls) && item.tool_calls.length > 0 && item.tool_calls.length <= policy.toolCalls);
        message.tool_calls = item.tool_calls.map(call => {
          fields(call, ['id', 'type', 'function']); fields(call.function, ['name', 'arguments']);
          check(call.type === 'function' && tools.has(call.function.name));
          const raw = string(call.function.arguments, policy.toolArgumentBytes), argumentsValue = parseJson(raw,
            { maxBytes: policy.toolArgumentBytes, maxDepth: policy.jsonDepth, maxNodes: policy.jsonNodes });
          check(argumentsValue && typeof argumentsValue === 'object' && !Array.isArray(argumentsValue));
          return { id: identifier(call.id), type: 'function', function: { name: call.function.name,
            arguments: canonical(argumentsValue, policy.toolArgumentBytes, policy.jsonDepth) } };
        });
      }
      return message;
    }
    fields(item, ['role', 'content']); check(['system', 'developer', 'user'].includes(item.role));
    return { role: item.role, content: string(item.content, policy.messageBytes, { empty: true }) };
  });
  const pending = new Set();
  for (const message of messages) {
    if (message.role === 'tool') { check(pending.delete(message.tool_call_id)); }
    else {
      check(pending.size === 0);
      for (const call of message.tool_calls ?? []) { check(!pending.has(call.id)); pending.add(call.id); }
    }
  }
  check(pending.size === 0);
  const wireTools = [...tools.values()].map(tool => ({ type: 'function', function: { name: tool.name, description: tool.description, parameters: tool.parameters } }));
  const body = { model: GLM_MODEL, messages, max_tokens: policy.maxOutputTokens, stream: true, stream_options: { include_usage: true },
    ...(wireTools.length ? { tools: wireTools, tool_choice: 'auto' } : {}) };
  const serialized = canonical(body, policy.requestBytes, policy.jsonDepth);
  return Object.freeze({ requestId, requestDigest: hash(canonical({ schema: 'soty.glm-request.v1', requestId, body }, policy.requestBytes + 1024, policy.jsonDepth + 1)),
    body: freeze(body), signal: value.signal, onText: value.onText });
}
