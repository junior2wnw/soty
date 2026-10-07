import test from 'node:test';
import assert from 'node:assert/strict';
import { createGlmProvider, GLM_MODEL, ProviderError } from '../index.mjs';
import { chunk, finish, request, fail, streamed, tool, provider, deferred, tick, wire } from './fixture.mjs';

test('trusted transport receives exact key-free GLM body and stable identity; reasoning stays separate', async t => {
  const sent = [];
  const value = provider(t, { transport: async data => { sent.push(data); return streamed([
    chunk({ role: 'assistant', reasoning_content: 'Synthetic private reasoning.' }), chunk({ content: 'Привет 🌍' }),
    finish('stop', { usage: { prompt_tokens: 10, completion_tokens: 20, total_tokens: 30 } }),
  ]); } });
  let visible = ''; const result = await value.complete(request({ onText: text => { visible += text; } }));
  const replay = await value.complete(request());
  assert.equal(result.model, GLM_MODEL); assert.equal(sent[0].body.model, GLM_MODEL);
  assert.equal(sent[0].body.stream, true); assert.equal(sent[0].body.stream_options.include_usage, true);
  assert.equal(result.message.content, 'Привет 🌍'); assert.equal(visible, 'Привет 🌍');
  assert.equal(Object.hasOwn(result.diagnostics, 'reasoning'), false); assert.equal(result.diagnostics.reasoningBytes > 0, true);
  assert.deepEqual(result.usage, { status: 'reported', promptTokens: 10, completionTokens: 20, totalTokens: 30 });
  assert.equal(result.requestDigest, replay.requestDigest); assert.equal(Object.isFrozen(sent[0].body), true);
  assert.deepEqual(Object.keys(sent[0]).sort(), ['body', 'requestDigest', 'requestId', 'signal']);
});

test('interleaved tools are fully assembled, bounded, frozen and validated only after DONE', async t => {
  let validations = 0; const gate = deferred();
  const events = [chunk({ tool_calls: [
    { index: 0, type: 'function', id: 'call_', function: { name: 're', arguments: '{"pa' } },
    { index: 1, type: 'function', id: 'call_b', function: { name: 'read', arguments: '{"path":"' } },
  ] }), chunk({ tool_calls: [
    { index: 1, function: { arguments: 'second"}' } }, { index: 0, id: 'a', function: { name: 'ad', arguments: 'th":"first"}' } },
  ] }), finish('tool_calls')];
  const preDone = new TextEncoder().encode(wire(events, { done: false })); let pulls = 0;
  const value = provider(t, { tools: [tool('read', args => { validations++; return typeof args.path === 'string'; })], transport: async () =>
    new Response(new ReadableStream({ async pull(controller) {
      if (++pulls === 1) controller.enqueue(preDone); else { await gate.promise; controller.enqueue(new TextEncoder().encode('data: [DONE]\n\n')); controller.close(); }
    } }), { headers: { 'Content-Type': 'text/event-stream' } }) });
  const work = value.complete(request()); await tick(); assert.equal(validations, 0);
  gate.resolve(); const result = await work;
  assert.equal(validations, 2); assert.deepEqual(result.message.toolCalls.map(call => [call.id, call.name, call.arguments.path]),
    [['call_a', 'read', 'first'], ['call_b', 'read', 'second']]);
  assert.equal(Object.isFrozen(result.message.toolCalls[0].arguments), true); assert.deepEqual(result.usage, { status: 'unknown' });
});

test('invalid/truncated tool JSON never reaches even a trusted validator', async t => {
  for (const argumentsText of ['{"path":"unfinished', '{"path":"a","path":"b"}', '{"p\\u0061th":"a","path":"b"}', '{"__proto__":{}}', '[]']) {
    let validations = 0;
    const value = provider(t, { tools: [tool('read', () => { validations++; return true; })], transport: async () => streamed([
      chunk({ tool_calls: [{ index: 0, id: 'call_a', type: 'function', function: { name: 'read', arguments: argumentsText } }] }), finish('tool_calls'),
    ]) });
    await assert.rejects(value.complete(request()), fail('provider_invalid_tool_json')); assert.equal(validations, 0);
  }
});

test('the complete tool set is parsed before validation; invalid second call cannot leak first invocation', async t => {
  let validations = 0;
  const value = provider(t, { tools: [tool('read', () => { validations++; return true; })], transport: async () => streamed([
    chunk({ tool_calls: [{ index: 0, id: 'call_a', type: 'function', function: { name: 'read', arguments: '{"path":"first"}' } },
      { index: 1, id: 'call_b', type: 'function', function: { name: 'read', arguments: 'partial' } }] }), finish('tool_calls'),
  ]) });
  await assert.rejects(value.complete(request()), fail('provider_invalid_tool_json')); assert.equal(validations, 0);
});

test('missing DONE, missing finish, length finish and premature DONE are never successful', async t => {
  const cases = [
    { events: [chunk({ content: 'partial' }), finish()], options: { done: false }, code: 'provider_stream_incomplete' },
    { events: [chunk({ content: 'partial' })], code: 'provider_stream_incomplete' },
    { events: [chunk({ content: 'partial' }), finish('length')], code: 'provider_output_incomplete' },
    { events: [], code: 'provider_stream_incomplete' },
  ];
  for (const item of cases) {
    const value = provider(t, { transport: async () => streamed(item.events, item.options) });
    await assert.rejects(value.complete(request()), fail(item.code));
  }
});

test('choice, role and exact model mismatch fail before content observer delivery', async t => {
  for (const event of [chunk({ content: 'wrong' }, { model: 'deepseek-ai/other' }),
    chunk({ role: 'user', content: 'wrong' }), chunk({}, { choices: [{ index: 1, delta: { content: 'wrong' } }] }),
    { choices: [{ index: 0, delta: { content: 'missing model' }, finish_reason: null }] }]) {
    let visible = '';
    const value = provider(t, { transport: async () => streamed([event, finish()]) });
    await assert.rejects(value.complete(request({ onText: text => { visible += text; } }))); assert.equal(visible, '');
  }
});

test('malformed event JSON, invalid UTF8, duplicate fields and data after terminal fail closed', async t => {
  const examples = [
    new TextEncoder().encode('data: {bad}\n\n'),
    new Uint8Array([0xff]),
    new TextEncoder().encode('data: {"model":"zai-org/GLM-5.3-Flash","choices":[],"choices":[]}\n\n'),
    new TextEncoder().encode(wire([chunk({ content: 'ok' }), finish()]) + wire([chunk({ content: 'after terminal' })], { done: false })),
  ];
  for (const bytes of examples) {
    const value = provider(t, { transport: async () => streamed([], { bytes, width: bytes.length }) });
    await assert.rejects(value.complete(request()));
  }
});

test('request and response byte/depth/node/tool limits are enforced without dispatching oversized input', async t => {
  let dispatches = 0;
  const value = provider(t, { policy: { messageBytes: 8 }, transport: async () => { dispatches++; return streamed([chunk({ content: 'ok' }), finish()]); } });
  await assert.rejects(value.complete(request()), fail('provider_invalid_input')); assert.equal(dispatches, 0);
  for (const [policy, events] of [
    [{ textBytes: 2 }, [chunk({ content: 'large' }), finish()]],
    [{ reasoningBytes: 2 }, [chunk({ reasoning_content: 'large' }), finish()]],
    [{ toolArgumentBytes: 2 }, [chunk({ tool_calls: [{ index: 0, id: 'call_a', type: 'function', function: { name: 'read', arguments: '{"path":"a"}' } }] }), finish('tool_calls')]],
    [{ eventBytes: 8 }, [chunk({ content: 'a' }), finish()]],
    [{ streamBytes: 8 }, [chunk({ content: 'a' }), finish()]],
  ]) {
    const limited = provider(t, { policy, tools: [tool()], transport: async () => streamed(events) });
    await assert.rejects(limited.complete(request()));
  }
  const deeplyNested = '{"path":' + '['.repeat(20) + '1' + ']'.repeat(20) + '}';
  const deep = provider(t, { tools: [tool()], transport: async () => streamed([chunk({ tool_calls: [{ index: 0, id: 'call_a', type: 'function', function: { name: 'read', arguments: deeplyNested } }] }), finish('tool_calls')]) });
  await assert.rejects(deep.complete(request()), fail('provider_invalid_tool_json'));
});

test('duplicate IDs, unknown tools, sparse indices and inconsistent finish reasons are rejected', async t => {
  for (const calls of [
    [{ index: 0, id: 'same', type: 'function', function: { name: 'read', arguments: '{"path":"a"}' } }, { index: 1, id: 'same', type: 'function', function: { name: 'read', arguments: '{"path":"b"}' } }],
    [{ index: 0, id: 'a', type: 'function', function: { name: 'not_registered', arguments: '{}' } }],
    [{ index: 1, id: 'a', type: 'function', function: { name: 'read', arguments: '{"path":"a"}' } }],
  ]) {
    const value = provider(t, { tools: [tool()], transport: async () => streamed([chunk({ tool_calls: calls }), finish('tool_calls')]) });
    await assert.rejects(value.complete(request()));
  }
  const mismatch = provider(t, { tools: [tool()], transport: async () => streamed([chunk({ tool_calls: [{ index: 0, id: 'a', type: 'function', function: { name: 'read', arguments: '{"path":"a"}' } }] }), finish('stop')]) });
  await assert.rejects(mismatch.complete(request()), fail('provider_invalid_finish'));
});

test('validator denial/async rejection and transport/observer errors are redacted', async t => {
  const events = [chunk({ tool_calls: [{ index: 0, id: 'a', type: 'function', function: { name: 'read', arguments: '{"path":"a"}' } }] }), finish('tool_calls')];
  for (const validateArguments of [() => false, () => Promise.reject(new Error('synthetic_private_marker')), () => { throw new ProviderError('synthetic_private_marker'); }]) {
    const value = provider(t, { tools: [tool('read', validateArguments)], transport: async () => streamed(events) });
    await assert.rejects(value.complete(request()), fail('provider_tool_validation_failed'));
  }
  const bad = provider(t, { transport: async () => { throw new ProviderError('synthetic_private_marker', { secret: 'synthetic_private_marker' }); } });
  await assert.rejects(bad.complete(request()), error => { assert.equal(error.code, 'provider_transport_failed'); assert.equal(JSON.stringify(error).includes('synthetic_private_marker'), false); return true; });
  const observer = provider(t, { transport: async () => streamed([chunk({ content: 'ok' }), finish()]) });
  await assert.rejects(observer.complete(request({ onText: () => { throw new Error('synthetic_private_marker'); } })), fail('provider_observer_failed'));
  await tick();
});

test('HTTP error body never leaks; request identity and unknown usage are retained for reconciliation', async t => {
  const value = provider(t, { transport: async () => new Response('{"error":"synthetic_private_marker"}', { status: 502, headers: { 'X-Request-Id': 'broker_synthetic' } }) });
  await assert.rejects(value.complete(request()), error => {
    assert.equal(error.code, 'provider_http_failed'); assert.equal(error.accounting.dispatchAttempted, true);
    assert.equal(error.accounting.providerRequestId, 'broker_synthetic'); assert.equal(error.accounting.httpStatus, 502);
    assert.deepEqual(error.accounting.usage, { status: 'unknown' }); assert.equal(JSON.stringify(error).includes('synthetic_private_marker'), false); return true;
  });
});

test('reported usage is exact, changing/invalid/over-budget usage cannot be accepted', async t => {
  for (const usage of [{ prompt_tokens: -1, completion_tokens: 1 }, { prompt_tokens: 1, completion_tokens: 2, total_tokens: 7 }, { prompt_tokens: 1, completion_tokens: 99999 }]) {
    const value = provider(t, { transport: async () => streamed([chunk({ content: 'ok' }), finish('stop', { usage })]) }); await assert.rejects(value.complete(request()));
  }
  const changed = provider(t, { transport: async () => streamed([chunk({ content: 'ok' }, { usage: { prompt_tokens: 1, completion_tokens: 2 } }), finish('stop', { usage: { prompt_tokens: 1, completion_tokens: 3 } })]) });
  await assert.rejects(changed.complete(request()), fail('provider_usage_changed'));
});

test('cancel before dispatch performs no transport; ignored transport retains bounded slot until settlement', async t => {
  let sent = 0; const gate = deferred();
  const value = provider(t, { policy: { concurrency: 1 }, transport: async () => { sent++; return gate.promise; } });
  const cancelled = new AbortController(); cancelled.abort('synthetic_private_marker');
  await assert.rejects(value.complete(request({ signal: cancelled.signal })), error => { assert.equal(error.code, 'provider_cancelled'); assert.equal(error.accounting.dispatchAttempted, false); return true; });
  assert.equal(sent, 0); await tick();
  const abort = new AbortController(), work = value.complete(request({ signal: abort.signal })); await tick(); abort.abort();
  await assert.rejects(work, fail('provider_cancelled'));
  await assert.rejects(value.complete(request({ requestId: 'other' })), fail('provider_busy'));
  gate.resolve(streamed([chunk({ content: 'discarded' }), finish()])); await tick();
  assert.equal(sent, 1);
});

test('request timeout/close returns safe unknown accounting and never returns late valid tools', async t => {
  const gate = deferred(), value = provider(t, { policy: { timeoutMs: 5 }, transport: async () => gate.promise });
  await assert.rejects(value.complete(request()), error => { assert.equal(error.code, 'provider_timeout'); assert.deepEqual(error.accounting.usage, { status: 'unknown' }); return true; });
  gate.resolve(streamed([chunk({ content: 'late' }), finish()])); await tick();
  const otherGate = deferred(), other = provider(t, { transport: async () => otherGate.promise });
  const work = other.complete(request()); await tick(); other.close(); await assert.rejects(work, fail('provider_closed'));
  otherGate.resolve(streamed([chunk({ content: 'late' }), finish()])); await tick();
});

test('unsafe request configuration and reasoning history cannot supply credentials or alter model route', async t => {
  const value = provider(t, { transport: async () => streamed([chunk({ content: 'ok' }), finish()]) });
  for (const key of ['model', 'apiKey', 'url', 'headers', 'tenantId', 'max_tokens']) await assert.rejects(value.complete(request({ [key]: 'caller-controlled' })), fail('provider_invalid_input'));
  await assert.rejects(value.complete(request({ messages: [{ role: 'assistant', content: 'x', reasoning_content: 'private' }] })), fail('provider_invalid_input'));
  await assert.rejects(value.complete(request({ messages: [{ role: 'tool', tool_call_id: 'unknown', content: 'x' }] })), fail('provider_invalid_input'));
});

test('usage-only terminal chunk and explicit reasoning opt-in preserve separate namespaces', async t => {
  const value = provider(t, { policy: { returnReasoning: true }, transport: async () => streamed([
    chunk({ reasoning_content: 'Synthetic thought.', content: 'Visible.' }), finish(),
    { choices: [], usage: { prompt_tokens: 3, completion_tokens: 4, total_tokens: 7 } },
  ]) });
  const result = await value.complete(request());
  assert.equal(result.diagnostics.reasoning, 'Synthetic thought.'); assert.equal(result.message.content, 'Visible.');
  assert.equal(Object.hasOwn(result.message, 'reasoning_content'), false); assert.equal(result.usage.totalTokens, 7);
});

test('provider errors and completion/header IDs cannot smuggle secret-like diagnostics', async t => {
  const errorStream = provider(t, { transport: async () => streamed([{ error: { code: 'synthetic_private_marker', message: 'synthetic_private_marker' }, choices: [] }]) });
  await assert.rejects(errorStream.complete(request()), failure => {
    assert.equal(failure.code, 'provider_upstream_failure'); assert.equal(JSON.stringify(failure).includes('synthetic_private_marker'), false); return true;
  });
  const badId = provider(t, { transport: async () => streamed([chunk({ content: 'ok' }, { id: 'sk-synthetic_marker' }), finish()]) });
  await assert.rejects(badId.complete(request()), failure => { assert.equal(failure.accounting.completionId, null); return true; });
  const header = provider(t, { transport: async () => new Response('', { status: 502, headers: { 'X-Request-Id': 'obk-synthetic_marker' } }) });
  await assert.rejects(header.complete(request()), failure => { assert.equal(failure.accounting.providerRequestId, null); return true; });
});

test('ignored reader cancellation retains its slot and never emits partial tool execution', async t => {
  const read = deferred(); let validations = 0, cancelled = 0;
  const value = provider(t, { policy: { concurrency: 1 }, tools: [tool('read', () => { validations++; return true; })], transport: async () => ({
    status: 200, headers: new Headers({ 'Content-Type': 'text/event-stream' }), body: { getReader: () => ({
      read: () => read.promise, cancel: () => { cancelled++; return Promise.resolve(); }, releaseLock: () => {},
    }) },
  }) });
  const abort = new AbortController(), work = value.complete(request({ signal: abort.signal }));
  await tick(); abort.abort(); await assert.rejects(work, fail('provider_cancelled')); assert.equal(cancelled, 1);
  await assert.rejects(value.complete(request({ requestId: 'second' })), fail('provider_busy'));
  read.resolve({ done: false, value: new TextEncoder().encode(wire([chunk({ tool_calls: [{ index: 0, id: 'call_a', type: 'function', function: { name: 'read', arguments: '{"path":"a"}' } }] }), finish('tool_calls')])) });
  await tick(); assert.equal(validations, 0);
});

test('revocation signal during validation stops the next tool validator and caller delivery', async t => {
  const abort = new AbortController(); let validations = 0;
  const value = provider(t, { tools: [tool('read', () => { validations++; abort.abort(); return true; })], transport: async () => streamed([
    chunk({ tool_calls: [{ index: 0, id: 'call_a', type: 'function', function: { name: 'read', arguments: '{"path":"a"}' } },
      { index: 1, id: 'call_b', type: 'function', function: { name: 'read', arguments: '{"path":"b"}' } }] }), finish('tool_calls'),
  ]) });
  await assert.rejects(value.complete(request({ signal: abort.signal })), fail('provider_cancelled')); assert.equal(validations, 1);
});

test('async observer rejection is consumed and exposed only as a safe code', async t => {
  const value = provider(t, { transport: async () => streamed([chunk({ content: 'ok' }), finish()]) });
  await assert.rejects(value.complete(request({ onText: () => Promise.reject(new Error('synthetic_private_marker')) })), fail('provider_observer_failed'));
  await tick();
});

test('semantic request change alters digest; ordered resolved tool history supports ID reuse across turns', async t => {
  const sent = [];
  const value = provider(t, { tools: [tool()], transport: async data => { sent.push(data); return streamed([chunk({ content: 'ok' }), finish()]); } });
  const first = await value.complete(request()), second = await value.complete(request({ messages: [{ role: 'user', content: 'Different synthetic request' }] }));
  assert.notEqual(first.requestDigest, second.requestDigest);
  const call = { id: 'call_a', type: 'function', function: { name: 'read', arguments: '{"path":"a"}' } };
  await value.complete(request({ messages: [{ role: 'user', content: 'Read.' }, { role: 'assistant', content: null, tool_calls: [call] },
    { role: 'tool', tool_call_id: 'call_a', content: 'First result.' }, { role: 'assistant', content: null, tool_calls: [call] },
    { role: 'tool', tool_call_id: 'call_a', content: 'Second result.' }, { role: 'user', content: 'Continue.' }] }));
  assert.equal(sent.length, 3);
});

test('conflicting or malformed later usage demotes earlier reported counts to unknown accounting', async t => {
  for (const usage of [{ prompt_tokens: 900, completion_tokens: 1000 }, { prompt_tokens: -1, completion_tokens: 3 }]) {
    const value = provider(t, { transport: async () => streamed([chunk({ content: 'ok' }, { usage: { prompt_tokens: 1, completion_tokens: 2 } }), finish('stop', { usage })]) });
    await assert.rejects(value.complete(request()), failure => {
      assert.deepEqual(failure.accounting.usage, { status: 'unknown' }); assert.equal(failure.accounting.dispatchAttempted, true); return true;
    });
  }
});

test('SSE event count including keepalive/empty blocks is bounded before unbounded small-event work', async t => {
  const bytes = new TextEncoder().encode(': keepalive\n\n'.repeat(3) + wire([chunk({ content: 'ok' }), finish()]));
  const value = provider(t, { policy: { events: 2 }, transport: async () => streamed([], { bytes }) });
  await assert.rejects(value.complete(request()), fail('provider_event_count_limit'));
});
