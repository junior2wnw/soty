import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { createConnector, resolveAlias } from '../../packages/connector/index.mjs';
import { createHealthWorker } from '../../packages/connector/worker.mjs';
import { normalizeSnapshot } from '../../packages/connector/catalog.mjs';

const MODEL = 'MiniMaxAI/MiniMax-M2.7';
const DEEP = 'deepseek-ai/DeepSeek-V4-Flash-0731';
const completion = (model = MODEL, content = 'OK') => ({ id: 'test-completion', object: 'chat.completion', model, choices: [{ index: 0, message: { role: 'assistant', content }, finish_reason: 'stop' }], usage: { prompt_tokens: 5, completion_tokens: 1, total_tokens: 6, prompt_tokens_details: { cached_tokens: 2 }, completion_tokens_details: { reasoning_tokens: 0 } } });

async function provider(handler, ids = [MODEL]) {
  const received = [];
  const server = createServer(async (request, response) => {
    if (request.url === '/v1/models') { response.setHeader('content-type', 'application/json'); response.end(JSON.stringify({ data: ids.map((id) => ({ id })) })); return; }
    let text = '';
    for await (const chunk of request) text += chunk;
    const body = JSON.parse(text);
    received.push(body);
    try { await handler(body, response, received.length); } catch { response.destroy(); }
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return { baseUrl: `http://127.0.0.1:${server.address().port}/v1`, apiKey: 'synthetic-fixture-not-a-real-key', received, close: () => new Promise((resolve) => { server.closeAllConnections(); server.close(resolve); }) };
}

function instance(providers, more = {}) {
  return createConnector({ providers: providers.map((p, i) => ({ ...p, id: `provider${i}` })), allowLocalHttp: true, liveEnabled: true, timeouts: { deadlineMs: 5000, primaryMs: 150, firstUsefulMs: 100, streamIdleMs: 200, streamMaximumMs: 2000 }, ...more });
}

const chat = (body = {}) => ({ model: MODEL, messages: [{ role: 'user', content: 'synthetic' }], max_tokens: 20, ...body });

async function until(predicate) {
  const deadline = Date.now() + 3000;
  while (!predicate()) { if (Date.now() >= deadline) throw new Error('Synthetic condition timed out'); await new Promise(resolve => setTimeout(resolve, 5)); }
}

test('final authorization runs after queue admission; queued revocation and abort dispatch nothing', async () => {
  let release; const gate = new Promise(resolve => { release = resolve; });
  const source = await provider(async (body, res, index) => { if (index === 1) await gate; res.end(JSON.stringify(completion(body.model))); });
  let admitted = 0; let active = true;
  const connector = instance([source], { providerConcurrency: 1, timeouts: { deadlineMs: 10000, primaryMs: 9000 } });
  const beforeDispatch = async () => { admitted++; if (!active) throw Object.assign(new Error('Denied'), { code: 'authorization_changed', status: 403 }); };
  try {
    const first = connector.complete({ body: chat(), beforeDispatch });
    await until(() => source.received.length === 1);
    const revoked = connector.complete({ body: chat(), beforeDispatch }).catch(error => error);
    const canceledController = new AbortController();
    const canceled = connector.complete({ body: chat(), beforeDispatch, signal: canceledController.signal }).catch(error => error);
    await until(() => connector.snapshot().providers[0].queued === 2);
    active = false; canceledController.abort(); release();
    await first;
    assert.equal((await revoked).code, 'authorization_changed');
    assert.equal((await revoked).noUpstreamDispatch, true);
    assert.equal((await canceled).code, 'request_canceled');
    assert.equal(source.received.length, 1); assert.equal(admitted, 2);
    assert.equal(connector.snapshot().providers[0].queued, 0);
  } finally { release(); await source.close(); }
});

test('fallback reauthorizes every provider attempt and stops after authority is revoked', async () => {
  let active = true; const admitted = [];
  const first = await provider((_body, response) => { active = false; response.statusCode = 503; response.end(); });
  const second = await provider((body, response) => response.end(JSON.stringify(completion(body.model))));
  try {
    const connector = instance([first, second]);
    const failure = await connector.complete({ body: chat(), beforeDispatch: async candidate => {
      admitted.push(candidate.providerId);
      if (!active) throw Object.assign(new Error('Denied'), { code: 'dispatch_authorization_denied', status: 403 });
    } }).catch(error => error);
    assert.equal(failure.code, 'dispatch_authorization_denied'); assert.equal(failure.noUpstreamDispatch, false);
    assert.deepEqual(admitted, ['provider0', 'provider1']); assert.equal(first.received.length, 1); assert.equal(second.received.length, 0);
  } finally { await first.close(); await second.close(); }
});

test('same model fallback preserves parameters and cached usage, at most two attempts', async () => {
  const first = await provider((body, res) => { res.statusCode = 503; res.end(); });
  const second = await provider((body, res) => { res.end(JSON.stringify(completion(body.model))); });
  try {
    const connector = instance([first, second]);
    const result = await connector.complete({ body: chat({ temperature: 0.2, response_format: { type: 'json_object' } , messages: [{ role: 'user', content: 'return JSON' }] }) }).catch((error) => error);
    assert.equal(result.code, 'upstream_invalid_structured_output');
    const success = await connector.complete({ body: chat({ temperature: 0.2, presence_penalty: 0.1 }) });
    assert.equal(success.providerId, 'provider1');
    assert.equal(success.attempts.length, 2);
    assert.equal(second.received.at(-1).temperature, 0.2);
    assert.equal(second.received.at(-1).presence_penalty, 0.1);
    assert.equal(second.received.at(-1).model, MODEL);
    assert.equal(success.body.usage.prompt_tokens_details.cached_tokens, 2);
  } finally { await first.close(); await second.close(); }
});

test('upstream request error does not trigger redundant retries', async () => {
  const first = await provider((body, res) => { res.statusCode = 400; res.end('do-not-surface-upstream-message'); });
  const second = await provider((body, res) => res.end(JSON.stringify(completion())));
  try {
    await assert.rejects(instance([first, second]).complete({ body: chat() }), { code: 'upstream_invalid_request', statusCode: 400 });
    assert.equal(second.received.length, 0);
  } finally { await first.close(); await second.close(); }
});

test('primary total timeout is bounded and gives fallback time', async () => {
  const first = await provider(async (body, res) => { await new Promise((r) => setTimeout(r, 500)); res.end(JSON.stringify(completion())); });
  const second = await provider((body, res) => res.end(JSON.stringify(completion())));
  try {
    const start = performance.now();
    const result = await instance([first, second]).complete({ body: chat() });
    assert.equal(result.providerId, 'provider1');
    assert.equal(result.attempts[0].errorCode, 'upstream_timeout');
    assert.ok(performance.now() - start < 800);
  } finally { await first.close(); await second.close(); }
});

test('race chooses validated result, rejects invalid fast result, records each attempt once', async () => {
  const first = await provider((body, res) => res.end(JSON.stringify({ choices: [] })));
  const second = await provider(async (body, res) => { await new Promise((r) => setTimeout(r, 40)); res.end(JSON.stringify(completion())); });
  try {
    const result = await instance([first, second], { strategy: 'race' }).complete({ body: chat() });
    assert.equal(result.providerId, 'provider1');
    assert.equal(result.attempts.length, 2);
    assert.equal(result.attempts.filter((x) => x.outcome === 'success').length, 1);
  } finally { await first.close(); await second.close(); }
});

test('model is never changed without explicit auto permission', async () => {
  const first = await provider((body, res) => { res.statusCode = body.model === MODEL ? 503 : 200; res.end(JSON.stringify(completion(body.model))); }, [MODEL, DEEP]);
  try {
    const connector = instance([first]);
    await assert.rejects(connector.complete({ body: chat() }), { code: 'upstream_unavailable' });
    assert.deepEqual(first.received.map((x) => x.model), [MODEL]);
    const result = await connector.complete({ body: chat(), policy: { autoFallback: true, allowedModels: [MODEL, DEEP] } });
    assert.equal(result.model, DEEP);
    assert.equal(result.body.model, DEEP);
  } finally { await first.close(); }
});

test('legacy aliases and exact dynamically discovered IDs coexist', () => {
  assert.equal(resolveAlias('deepseek-chat'), DEEP);
  assert.equal(resolveAlias('deepseek-ai/DeepSeek-V3.2'), DEEP);
  assert.equal(resolveAlias('minimax'), MODEL);
  assert.equal(resolveAlias('MiniMaxAI/MiniMax-M9', [{ id: 'MiniMaxAI/MiniMax-M9' }]), 'MiniMaxAI/MiniMax-M9');
});

test('catalog display splits known families and versions without changing canonical IDs', () => {
  const models = normalizeSnapshot({ data: [MODEL, DEEP, 'zai-org/GLM-5.3-Flash', 'future/Novel-Model-7'].map((id) => ({ id })) }, { id: 'test' }, Date.now());
  assert.deepEqual(models.map(({ name, version }) => ({ name, version })), [
    { name: 'MiniMax', version: 'M2.7' }, { name: 'DeepSeek', version: 'V4 Flash' }, { name: 'GLM', version: '5.3 Flash' }, { name: 'Novel-Model-7', version: '' },
  ]);
  assert.equal(models[1].id, DEEP);
  assert.equal(models[1].canonicalVersion, DEEP);
});

test('auto switches to a healthy alternative even when both providers advertise the failing model', async () => {
  const first = await provider((body, res) => { res.statusCode = body.model === MODEL ? 503 : 200; res.end(JSON.stringify(completion(body.model))); }, [MODEL, DEEP]);
  const second = await provider((body, res) => { res.statusCode = body.model === MODEL ? 503 : 200; res.end(JSON.stringify(completion(body.model))); }, [MODEL, DEEP]);
  try {
    const connector = instance([first, second]);
    await connector.complete({ body: chat({ model: DEEP }) });
    const result = await connector.complete({ body: chat(), policy: { autoFallback: true, allowedModels: [MODEL, DEEP] } });
    assert.equal(result.model, DEEP);
    assert.equal(result.attempts.length, 2);
    assert.equal(result.attempts[0].model, MODEL);
    assert.equal(result.attempts[1].model, DEEP);
    assert.equal(second.received.length, 0);
  } finally { await first.close(); await second.close(); }
});

test('auto cannot use an unqualified tool route or exceed its price ceiling', async () => {
  const source = await provider((body, res) => { res.statusCode = body.model === MODEL ? 503 : 200; res.end(JSON.stringify(completion(body.model))); }, [MODEL, DEEP]);
  try {
    const connector = instance([source], { retail: { input: '100', output: '100' } });
    await connector.complete({ body: chat({ model: DEEP }) });
    await assert.rejects(connector.complete({ body: chat(), policy: { autoFallback: true, maxPriceMicrorublesPerMillion: '99' } }), { code: 'upstream_unavailable' });
    await assert.rejects(connector.complete({ body: chat({ tools: [{ type: 'function', function: { name: 'run', parameters: { type: 'object' } } }] }), policy: { autoFallback: true } }), { code: 'upstream_unavailable' });
    assert.equal(source.received.filter((x) => x.model === DEEP).length, 1);
  } finally { await source.close(); }
});

test('persisted JSON qualification survives a connector restart without inventing healthy status', async () => {
  const observations = [];
  const first = await provider((body, res) => { res.statusCode = body.model === MODEL ? 503 : 200; res.end(JSON.stringify(completion(body.model, '{"ok":true}'))); }, [MODEL, DEEP]);
  const second = await provider((body, res) => { res.statusCode = 503; res.end(); }, [MODEL, DEEP]);
  try {
    const before = instance([first, second], { store: { saveObservation: async (event) => observations.push(event) } });
    await before.complete({ body: chat({ model: DEEP, response_format: { type: 'json_object' } }) });
    const stored = observations.find((event) => event.model === DEEP && event.outcome === 'success');
    const after = instance([first, second], { store: { loadQualifications: async () => [{ providerId: stored.providerId, model: DEEP, jsonVerifiedAt: Date.parse(stored.checkedAt), toolsVerifiedAt: null }] } });
    await after.refreshCatalog();
    const freshModel = after.models().find((model) => model.id === DEEP);
    assert.equal(freshModel.status, 'unknown');
    assert.equal(freshModel.checkedAt, null);
    assert.equal(freshModel.capabilities.json, 'tested');
    assert.equal(freshModel.routes[0].jsonVerifiedAt, stored.checkedAt);
    const result = await after.complete({ body: chat({ response_format: { type: 'json_object' } }), policy: { autoFallback: true } });
    assert.equal(result.model, DEEP);
    assert.equal(result.attempts.length, 2);
  } finally { await first.close(); await second.close(); }
});

test('expired, future, and foreign-provider qualifications never enable auto tools or JSON', async () => {
  const time = Date.now();
  const source = await provider((body, res) => { res.statusCode = 503; res.end(); }, [MODEL, DEEP]);
  try {
    for (const entry of [
      { providerId: 'provider0', model: DEEP, jsonVerifiedAt: time - 86_400_001, toolsVerifiedAt: time - 86_400_001 },
      { providerId: 'provider0', model: DEEP, jsonVerifiedAt: time + 1000, toolsVerifiedAt: time + 1000 },
      { providerId: 'unconfigured', model: DEEP, jsonVerifiedAt: time, toolsVerifiedAt: time },
    ]) {
      const connector = instance([source], { now: () => time, store: { loadQualifications: async () => [entry] } });
      await connector.refreshCatalog();
      const model = connector.models().find((x) => x.id === DEEP);
      assert.equal(model.capabilities.json, 'unknown');
      assert.equal(model.capabilities.tools, 'unknown');
      await assert.rejects(connector.complete({ body: chat({ response_format: { type: 'json_object' } }), policy: { autoFallback: true } }), { code: 'upstream_unavailable' });
    }
    assert.equal(source.received.some((body) => body.model === DEEP), false);
  } finally { await source.close(); }
});

test('fresh stored tool qualification is exposed separately and expires without a new observation', async () => {
  let time = Date.now();
  const source = await provider((body, res) => res.end(JSON.stringify(completion())), [MODEL]);
  try {
    const connector = instance([source], { now: () => time, store: { loadQualifications: async () => [{ providerId: 'provider0', model: MODEL, jsonVerifiedAt: null, toolsVerifiedAt: time - 1000 }] } });
    await connector.refreshCatalog();
    assert.equal(connector.models()[0].capabilities.tools, 'tested');
    assert.equal(connector.models()[0].status, 'unknown');
    assert.equal(connector.models()[0].checkedAt, null);
    time += 86_400_000;
    assert.equal(connector.models()[0].capabilities.tools, 'unknown');
    assert.equal(connector.models()[0].routes[0].toolsVerifiedAt, null);
  } finally { await source.close(); }
});

test('strict JSON schema is rejected explicitly before upstream', async () => {
  const source = await provider((body, res) => res.end(JSON.stringify(completion())));
  try {
    await assert.rejects(instance([source]).complete({ body: chat({ response_format: { type: 'json_schema', json_schema: { schema: { type: 'object' } } } }) }), { code: 'json_schema_not_supported' });
    assert.equal(source.received.length, 0);
  } finally { await source.close(); }
});

test('credential failure excludes the account across models while preserving the backup', async () => {
  const first = await provider((body, res) => { res.statusCode = 401; res.end(); }, [MODEL, DEEP]);
  const second = await provider((body, res) => res.end(JSON.stringify(completion(body.model))), [MODEL, DEEP]);
  try {
    const connector = instance([first, second]);
    await connector.complete({ body: chat() });
    const result = await connector.complete({ body: chat({ model: DEEP }) });
    assert.equal(result.providerId, 'provider1');
    assert.equal(first.received.length, 1);
    const route = connector.models().find((x) => x.id === DEEP).routes.find((x) => x.providerId === 'provider0');
    assert.equal(route.status, 'credential_error');
    assert.ok(Date.parse(route.cooldownUntil) > Date.now());
  } finally { await first.close(); await second.close(); }
});

test('old successful observations become unknown rather than stay green forever', async () => {
  let now = Date.now();
  const source = await provider((body, res) => res.end(JSON.stringify(completion())));
  try {
    const connector = instance([source], { now: () => now });
    await connector.complete({ body: chat() });
    const verified = connector.models()[0].checkedAt;
    assert.equal(connector.models()[0].status, 'healthy');
    now += 61_000;
    assert.equal(connector.models()[0].status, 'unknown');
    assert.equal(connector.models()[0].checkedAt, verified);
  } finally { await source.close(); }
});

const frame = (delta, finish = null) => `data: ${JSON.stringify({ id: 's', object: 'chat.completion.chunk', model: MODEL, choices: [{ index: 0, delta, finish_reason: finish }] })}\n\n`;

test('SSE emits terminal only after valid completion, includes real usage', async () => {
  const source = await provider((body, res) => {
    res.setHeader('content-type', 'text/event-stream');
    res.write(frame({ content: 'OK' }));
    res.write(frame({}, 'stop'));
    res.write(`data: ${JSON.stringify({ model: MODEL, choices: [], usage: completion().usage })}\n\n`);
    res.end('data: [DONE]\n\n');
  });
  try {
    const result = await instance([source]).complete({ body: chat({ stream: true }) });
    const content = await new Response(result.stream).text();
    const final = await result.completion;
    assert.ok(content.endsWith('data: [DONE]\n\n'));
    assert.equal(final.outcome, 'success');
    assert.equal(final.usage.prompt_tokens_details.cached_tokens, 2);
    assert.equal(result.attempts.length, 1);
  } finally { await source.close(); }
});

test('broken SSE after first content never falls back or fabricates DONE', async () => {
  const first = await provider((body, res) => { res.setHeader('content-type', 'text/event-stream'); res.end(frame({ content: 'partial' })); });
  const second = await provider((body, res) => res.end(JSON.stringify(completion())));
  try {
    const result = await instance([first, second]).complete({ body: chat({ stream: true }) });
    await assert.rejects(new Response(result.stream).text(), { code: 'upstream_incomplete' });
    assert.equal((await result.completion).outcome, 'error');
    assert.equal(second.received.length, 0);
  } finally { await first.close(); await second.close(); }
});

test('client abort after stream starts is cancellation, not an upstream availability failure', async () => {
  const source = await provider((body, res) => { res.setHeader('content-type', 'text/event-stream'); res.write(frame({ content: 'first' })); });
  const signal = new AbortController();
  try {
    const connector = instance([source]);
    const result = await connector.complete({ body: chat({ stream: true }), signal: signal.signal });
    const reader = result.stream.getReader();
    assert.equal((await reader.read()).done, false);
    signal.abort();
    await assert.rejects(reader.read(), { code: 'request_canceled' });
    assert.equal((await result.completion).outcome, 'canceled');
    assert.equal(result.attempts[0].outcome, 'canceled');
    assert.equal(connector.models()[0].status, 'unknown');
  } finally { await source.close(); }
});

test('tool stream first activity clears first-useful timeout but stays buffered until complete JSON arguments', async () => {
  const source = await provider(async (body, res) => {
    res.setHeader('content-type', 'text/event-stream');
    res.write(frame({ tool_calls: [{ index: 0, id: 'call1', type: 'function', function: { name: 'weather', arguments: '{"city":' } }] }));
    await new Promise((r) => setTimeout(r, 160));
    res.write(frame({ tool_calls: [{ index: 0, function: { arguments: '"Ufa"}' } }] }));
    res.write(frame({}, 'tool_calls'));
    res.end('data: [DONE]\n\n');
  });
  try {
    const result = await instance([source]).complete({ body: chat({ stream: true, tools: [{ type: 'function', function: { name: 'weather', parameters: { type: 'object' } } }] }) });
    assert.ok((await new Response(result.stream).text()).includes('weather'));
    assert.equal((await result.completion).outcome, 'success');
  } finally { await source.close(); }
});

test('catalog malformed refresh and mass removals keep last good snapshot', async () => {
  let payload = { data: [{ id: MODEL }, { id: DEEP }, { id: 'zai-org/GLM-5.3-Flash' }] };
  const connector = createConnector({ providers: [{ id: 'test', baseUrl: 'https://broker.invalid/v1', apiKey: 'fixture' }], liveEnabled: true, fetchImpl: async () => Response.json(payload) });
  await connector.refreshCatalog();
  assert.equal(connector.models().length, 3);
  payload = { data: [] };
  await connector.refreshCatalog();
  assert.equal(connector.models().length, 3);
  payload = { data: [{ id: MODEL }] };
  await connector.refreshCatalog();
  assert.equal(connector.models().length, 3);
  assert.equal(connector.snapshot().sources[0].errorCode, 'catalog_mass_removal_quarantined');
});

test('health evaluation respects no overlap, slots, fresh traffic and atomic budget refusal', async () => {
  let time = Date.now(), calls = 0, reservations = 0;
  const health = new Map();
  const modelList = () => [{ id: MODEL, routes: [{ providerId: 'p1', catalogState: 'present' }, { providerId: 'p2', catalogState: 'present' }, { providerId: 'p3', catalogState: 'present' }] }];
  let finish;
  const pending = new Promise((resolve) => { finish = resolve; });
  const worker = createHealthWorker({ models: modelList, refreshCatalog: async () => {}, probe: async () => { calls += 1; await pending; }, health, now: () => time, live: true, enabled: true, reserveUrub: '10', store: { reserveProbeSpend: async () => { reservations += 1; return reservations < 3 ? { id: String(reservations) } : null; } } });
  await worker.tick();
  assert.equal(calls, 2);
  assert.equal(health.get(`p3:${MODEL}`).skippedReason, 'probe_concurrency');
  time += 10_000;
  await worker.tick();
  assert.equal(calls, 2);
  finish(); await new Promise((r) => setTimeout(r, 0));
  health.set(`p1:${MODEL}`, { lastSuccessAt: time });
  time += 500;
  await worker.tick();
  time += 10_000;
  await worker.tick();
  assert.equal(calls, 2);
  assert.equal(health.get(`p3:${MODEL}`).skippedReason, 'monitoring_budget_exhausted');
  await worker.stop();
});

test('monitoring without cost bound makes no paid request and has no fake checkedAt', async () => {
  const health = new Map();
  let calls = 0;
  const worker = createHealthWorker({ models: () => [{ id: MODEL, routes: [{ providerId: 'p', catalogState: 'present' }] }], refreshCatalog: async () => {}, probe: async () => { calls += 1; }, health, live: true, enabled: true });
  await worker.tick();
  assert.equal(calls, 0);
  assert.equal(health.get(`p:${MODEL}`).skippedReason, 'probe_cost_unknown');
  assert.equal(health.get(`p:${MODEL}`).checkedAt, undefined);
});

test('health profile gives reasoning room without altering customer requests', async () => {
  const calls = [];
  const health = new Map();
  const worker = createHealthWorker({ models: () => [{ id: DEEP, routes: [{ providerId: 'p', catalogState: 'present' }] }], refreshCatalog: async () => {}, probe: async (provider, model, profile) => { calls.push({ provider, model, profile }); }, health, live: true, enabled: true, reserveUrub: '50000' });
  await worker.tick();
  assert.equal(calls.length, 1);
  assert.equal(calls[0].profile.maxTokens, 1024);
  assert.equal(calls[0].profile.timeoutMs, 45000);
  assert.equal(worker.snapshot().dailyReservedUrub, '50000');
  await worker.stop();
});

test('probe settles measured usage at conservative retail instead of consuming the whole hold', async () => {
  const settled = [];
  const worker = createHealthWorker({ models: () => [{ id: DEEP, inputPriceMicrorublesPerMillion: '23000000', outputPriceMicrorublesPerMillion: '23000000', routes: [{ providerId: 'p', catalogState: 'present' }] }], refreshCatalog: async () => {}, probe: async () => ({ usage: { prompt_tokens: 10, completion_tokens: 2 } }), health: new Map(), live: true, enabled: true, reserveUrub: '50000', store: { reserveProbeSpend: async () => ({ id: 'reservation' }), settleProbeSpend: async (id, amount) => { settled.push({ id, amount }); } } });
  await worker.tick();
  await new Promise((r) => setTimeout(r, 0));
  assert.deepEqual(settled, [{ id: 'reservation', amount: '276' }]);
  assert.equal(worker.snapshot().dailyReservedUrub, '276');
  await worker.stop();
});

test('monitoring budget resets at the same Yekaterinburg midnight as PostgreSQL accounting', async () => {
  let time = Date.parse('2026-09-24T18:59:59Z'), loads = 0, calls = 0;
  const worker = createHealthWorker({ models: () => [{ id: MODEL, routes: [{ providerId: 'p', catalogState: 'present' }] }], refreshCatalog: async () => {}, probe: async () => { calls += 1; }, now: () => time, health: new Map(), live: true, enabled: true, dailyBudgetUrub: '100', reserveUrub: '10', store: { loadDailyProbeSpend: async () => { loads += 1; return loads === 1 ? '100' : '0'; } } });
  await worker.tick();
  assert.equal(calls, 0);
  time += 11_000;
  await worker.tick();
  assert.equal(loads, 2);
  assert.equal(calls, 1);
  assert.equal(worker.snapshot().budgetTimezone, 'Asia/Yekaterinburg');
  await worker.stop();
});

test('invalid structured output retains known supplier token usage while failing the request', async () => {
  const observations = [];
  const source = await provider((body, res) => res.end(JSON.stringify(completion(MODEL, '{"broken":'))));
  try {
    const connector = instance([source], { onAttempt: async (event) => observations.push(event) });
    await assert.rejects(connector.complete({ body: chat({ response_format: { type: 'json_object' } }) }), { code: 'upstream_invalid_structured_output' });
    assert.equal(observations[0].outcome, 'error');
    assert.equal(observations[0].inputTokens, 5);
    assert.equal(observations[0].outputTokens, 1);
    assert.equal(observations[0].profile, 'json_object');
  } finally { await source.close(); }
});
