import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { createGlmProvider, GLM_MODEL } from '../../modules/personal-agent/provider/index.mjs';
import { canonical, hash } from '../../modules/personal-agent/provider/validation.mjs';
import { request, deferred, tool } from '../../modules/personal-agent/provider/test/fixture.mjs';
import { createPersonalAgentTransport } from '../personal-agent-transport.js';

const completion = () => ({ id: 'completion_json', model: GLM_MODEL, choices: [{ index: 0,
  message: { role: 'assistant', content: null, reasoning_content: 'Synthetic reasoning', tool_calls: [
    { id: 'call_json', type: 'function', function: { name: 'read', arguments: '{"path":"safe"}' } },
  ] }, finish_reason: 'tool_calls' }], usage: { prompt_tokens: 10, completion_tokens: 2, total_tokens: 12 } });
const response = body => new Response(typeof body === 'string' ? body : JSON.stringify(body), { headers: { 'content-type': 'application/json' } });

test('trusted JSON profile captures its exact wire body before digest and shares strict tool validation', async t => {
  let sent, validations = 0;
  const provider = createGlmProvider({ responseMode: 'json', tools: [tool('read', value => { validations++; return value.path === 'safe'; })],
    transport: async packet => { sent = packet; return response(completion()); } });
  t.after(() => provider.close());
  const result = await provider.complete(request());
  assert.equal(sent.body.stream, false); assert.equal(Object.hasOwn(sent.body, 'stream_options'), false);
  assert.equal(sent.body.reasoning_effort, 'low');
  assert.equal(sent.requestDigest, hash(canonical({ schema: 'soty.glm-request.v1', requestId: sent.requestId, body: sent.body })));
  assert.notEqual(sent.requestDigest, hash(canonical({ schema: 'soty.glm-request.v1', requestId: sent.requestId,
    body: { ...sent.body, stream: true, stream_options: { include_usage: true } } })));
  assert.equal(validations, 1); assert.equal(result.message.toolCalls[0].arguments.path, 'safe');
  assert.equal(result.usage.status, 'reported'); assert.equal(result.diagnostics.reasoningBytes, 19);
  assert.equal(Object.hasOwn(result.diagnostics, 'reasoning'), false);
  await assert.rejects(provider.complete(request({ responseMode: 'sse' })), { code: 'provider_invalid_input' });
  await assert.rejects(provider.complete(request({ onText() {} })), { code: 'provider_invalid_input' });
});

test('serial offered tools bind wire and response without reoffering resolved history', async t => {
  let sent, validations = 0, returnedName = 'finish_answer';
  const provider = createGlmProvider({ responseMode: 'json',
    tools: [tool('read', () => { validations++; return true; }), tool('finish_answer', () => { validations++; return true; })],
    transport: async packet => { sent = packet; const body = completion();
      body.choices[0].message.tool_calls[0].function.name = returnedName; return response(body); } });
  t.after(() => provider.close());
  const history = [...request().messages,
    { role: 'assistant', content: null, tool_calls: [{ id: 'prior_read', type: 'function', function: { name: 'read', arguments: '{"path":"safe"}' } }] },
    { role: 'tool', tool_call_id: 'prior_read', content: '{}' }];
  const names = ['finish_answer'], input = request({ messages: history, offeredToolNames: names, parallelToolCalls: false });
  const pending = provider.complete(input); names[0] = 'read';
  const result = await pending;
  assert.equal(sent.body.parallel_tool_calls, false); assert.equal(sent.body.tool_choice, 'auto');
  assert.deepEqual(sent.body.tools.map(value => value.function.name), ['finish_answer']);
  assert.equal(sent.body.messages[1].tool_calls[0].function.name, 'read');
  assert.equal(result.message.toolCalls[0].name, 'finish_answer'); assert.equal(validations, 1);
  const previousBody = { ...sent.body }; delete previousBody.parallel_tool_calls;
  assert.notEqual(sent.requestDigest, hash(canonical({ schema: 'soty.glm-request.v1', requestId: sent.requestId, body: previousBody })));
  returnedName = 'read';
  await assert.rejects(provider.complete(request({ messages: history, offeredToolNames: ['finish_answer'], parallelToolCalls: false })), { code: 'provider_unknown_tool' });
  assert.equal(validations, 1, 'a known historical tool cannot reach its validator when not offered');
});

test('offered tool names are a bounded registered subset and serial control is closed', async t => {
  let dispatches = 0;
  const provider = createGlmProvider({ responseMode: 'json', tools: [tool()],
    transport: async () => { dispatches++; return response(completion()); } });
  t.after(() => provider.close());
  for (const changes of [{ offeredToolNames: ['unknown'] }, { offeredToolNames: ['read', 'read'] },
    { offeredToolNames: 'read' }, { offeredToolNames: null }, { offeredToolNames: new Array(13).fill('read') }, { offeredToolNames: [null] },
    { parallelToolCalls: true }, { parallelToolCalls: 0 }, { tool_choice: 'required' }]) {
    await assert.rejects(provider.complete(request(changes)), { code: 'provider_invalid_input' });
  }
  assert.equal(dispatches, 0);
  await assert.rejects(provider.complete(request({ offeredToolNames: [] })), { code: 'provider_unknown_tool' });
  assert.equal(dispatches, 1);
});

const invalid = [
  ['malformed JSON', () => '{', 'provider_invalid_json'],
  ['duplicate JSON key', () => JSON.stringify(completion()).replace('"model":', '"id":"other","model":'), 'provider_invalid_json'],
  ['wrong model', body => { body.model = 'other/model'; }, 'provider_model_mismatch'],
  ['missing model', body => { delete body.model; }, 'provider_model_unverified'],
  ['wrong role', body => { body.choices[0].message.role = 'user'; }, 'provider_role_mismatch'],
  ['multiple choices', body => { body.choices.push(body.choices[0]); }, 'provider_invalid_choice'],
  ['length finish', body => { body.choices[0].finish_reason = 'length'; }, 'provider_output_incomplete'],
  ['missing finish', body => { delete body.choices[0].finish_reason; }, 'provider_stream_incomplete'],
  ['tool result with stop finish', body => { body.choices[0].finish_reason = 'stop'; }, 'provider_invalid_finish'],
  ['missing tool for tool finish', body => { delete body.choices[0].message.tool_calls; }, 'provider_invalid_finish'],
  ['unregistered tool', body => { body.choices[0].message.tool_calls[0].function.name = 'not_offered'; }, 'provider_unknown_tool'],
  ['invalid tool type', body => { body.choices[0].message.tool_calls[0].type = 'custom'; }, 'provider_invalid_tool'],
  ['duplicate tool IDs', body => { body.choices[0].message.tool_calls.push(body.choices[0].message.tool_calls[0]); }, 'provider_duplicate_tool_id'],
  ['malformed arguments', body => { body.choices[0].message.tool_calls[0].function.arguments = '{'; }, 'provider_invalid_tool_json'],
  ['duplicate argument key', body => { body.choices[0].message.tool_calls[0].function.arguments = '{"path":"safe","path":"other"}'; }, 'provider_invalid_tool_json'],
  ['invalid argument schema', body => { body.choices[0].message.tool_calls[0].function.arguments = '{"unexpected":true}'; }, 'provider_tool_validation_failed'],
  ['token limit', body => { body.usage = { prompt_tokens: 10, completion_tokens: 9, total_tokens: 19 }; }, 'provider_token_limit'],
  ['contradictory usage', body => { body.usage.total_tokens = 99; }, 'provider_invalid_usage'],
  ['trailing JSON document', () => JSON.stringify(completion()) + '{}', 'provider_invalid_json'],
];
for (const [label, mutate, code] of invalid) test(`JSON rejects ${label}`, async t => {
  const body = completion(), raw = mutate(body) ?? body;
  const provider = createGlmProvider({ responseMode: 'json', tools: [tool()], policy: { maxOutputTokens: 8 }, transport: async () => response(raw) });
  t.after(() => provider.close());
  await assert.rejects(provider.complete(request()), { code });
});

test('JSON decoding rejects invalid UTF8 and total body overflow before accepting a completion', async t => {
  for (const [bytes, policy, code] of [
    [new Uint8Array([0xff]), {}, 'provider_invalid_utf8'],
    [new TextEncoder().encode(JSON.stringify(completion())), { streamBytes: 20 }, 'provider_stream_limit'],
  ]) {
    const provider = createGlmProvider({ responseMode: 'json', policy, tools: [tool()], transport: async () => new Response(bytes,
      { headers: { 'content-type': 'application/json' } }) });
    t.after(() => provider.close()); await assert.rejects(provider.complete(request()), { code });
  }
});

test('JSON remains bounded while awaiting EOF and preserves cancellation', async t => {
  for (const mode of ['timeout', 'cancel']) {
    const sent = deferred(); let cancellations = 0;
    const controller = new AbortController();
    const provider = createGlmProvider({ responseMode: 'json', tools: [tool()], policy: { timeoutMs: mode === 'timeout' ? 30 : 3000 },
      transport: async () => new Response(new ReadableStream({ start(output) {
        output.enqueue(new TextEncoder().encode(JSON.stringify(completion()))); sent.resolve();
      }, cancel() { cancellations++; } }), { headers: { 'content-type': 'application/json' } }) });
    t.after(() => provider.close());
    const pending = provider.complete(request({ signal: controller.signal }));
    const rejected = assert.rejects(pending, { code: mode === 'timeout' ? 'provider_timeout' : 'provider_cancelled' });
    await sent.promise; if (mode === 'cancel') controller.abort(); await rejected;
    assert.equal(cancellations, 1);
  }
});

test('Root JSON transport waits for complete HTTP body and verifies the committed receipt', { timeout: 5000 }, async t => {
  const gate = deferred(), entered = deferred(), bodyGate = deferred(), bodySent = deferred();
  let committed = false, dispatches = 0, receiptReads = 0, closeBeforeEnd = false;
  const id = '11111111-1111-4111-8111-111111111111', keyRef = {}, tenant = 'synthetic-json';
  const server = http.createServer(async (req, res) => {
    dispatches++; let raw = ''; for await (const part of req) raw += part;
    const body = JSON.parse(raw);
    assert.equal(body.stream, false); assert.equal(Object.hasOwn(body, 'stream_options'), false);
    assert.equal(body.autoFallback, false); assert.equal(req.headers.accept, 'application/json');
    res.once('close', () => { if (!res.writableFinished) closeBeforeEnd = true; });
    entered.resolve(); await gate.promise; committed = true;
    res.setHeader('content-type', 'application/json'); res.setHeader('x-soty-request-id', id);
    const json = JSON.stringify(completion()); res.write(json); bodySent.resolve();
    await bodyGate.promise; res.end();
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const transport = createPersonalAgentTransport({ gatewayOrigin: `http://127.0.0.1:${server.address().port}`, allowLoopbackFixture: true,
    billingTenant: tenant, keyRef, authorizeActor: () => true,
    resolveCredential: async () => ({ keyRef, tenantId: tenant, bearer: 'synthetic-only-json-test-bearer' }),
    readReceipt: async () => { receiptReads++; assert.equal(committed, true); return { tenantId: tenant, request: {
      id, status: 'completed', model_requested: GLM_MODEL, model_resolved: GLM_MODEL,
      input_tokens: 10, output_tokens: 2, charge_urub: '7',
    } }; },
  });
  const bound = transport.bind({ actor: {}, scope: 'synthetic', turnId: 'synthetic-turn', assertCurrent() {} });
  const provider = createGlmProvider({ responseMode: 'json', tools: [tool()], transport: bound.complete });
  t.after(async () => { gate.resolve(); bodyGate.resolve(); provider.close(); transport.close(); server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); });
  let settled = false;
  const pending = provider.complete(request()).finally(() => { settled = true; });
  await entered.promise; assert.equal(committed, false); assert.equal(settled, false); gate.resolve();
  await bodySent.promise; await new Promise(resolve => setTimeout(resolve, 25)); assert.equal(settled, false);
  bodyGate.resolve(); const result = await pending;
  const receipt = await bound.account('request_synthetic', result);
  assert.equal(receipt.status, 'completed'); assert.equal(receipt.chargeMicrorubles, '7');
  assert.equal(dispatches, 1); assert.equal(receiptReads, 1); assert.equal(closeBeforeEnd, false);
});
