import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createGonkaProxy, defaultGonkaProxyModel, miniMaxProxyModel } from '../server/gonka-proxy.js';

const clientToken = 'synthetic-client-'.repeat(4);
const primaryKey = 'synthetic-primary-provider-key';
const fallbackKey = 'synthetic-fallback-provider-key';
const aliases = [defaultGonkaProxyModel, miniMaxProxyModel, 'MiniMax-M2.7', 'minimax-m2.7',
  'MiniMaxAI/minimax-m2.7', 'minimax/minimax-m2.7', 'minimax', 'DeepSeek-V4-Flash-0731',
  'deepseek-chat', 'deepseek-reasoner', 'deepseek-r1', 'deepseek-ai/DeepSeek-R1', 'deepseek', '  MINIMAX-M2.7  '];
let checks = 0;

async function scenario(options, verify) {
  const observed = [];
  const proxy = createGonkaProxy({
    store: { authenticateModelToken: async token => token === clientToken },
    baseUrl: 'https://primary.synthetic.invalid/v1', apiKey: primaryKey,
    fallbackBaseUrl: 'https://fallback.synthetic.invalid/v1', fallbackApiKey: fallbackKey,
    upstreamModel: defaultGonkaProxyModel, fallbackModels: miniMaxProxyModel,
    onEvent: () => {},
    ...options,
    fetchImpl: async (url, init) => {
      const body = JSON.parse(init.body);
      const provider = new URL(url).hostname.startsWith('primary.') ? 'primary' : 'fallback';
      observed.push({ provider, body, authorization: init.headers.Authorization || init.headers.authorization });
      if (options.respond) return options.respond({ provider, body });
      return Response.json({ model: body.model, choices: [{ message: { role: 'assistant', content: '4' }, finish_reason: 'stop' }] });
    }
  });
  const server = createServer(async (req, res) => {
    let raw = ''; for await (const part of req) raw += part;
    req.body = JSON.parse(raw);
    res.status = status => { res.statusCode = status; return res; };
    res.json = value => res.end(JSON.stringify(value));
    await proxy.handleChatCompletions(req, res);
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const call = async (model, extra = {}, token = clientToken) => {
    const response = await fetch(`http://127.0.0.1:${server.address().port}`, {
      method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ model, stream: false, messages: [{ role: 'user', content: 'synthetic arithmetic' }], max_tokens: 128, ...extra })
    });
    return { status: response.status, body: await response.json() };
  };
  try { await verify({ proxy, observed, call }); checks++; }
  finally { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
}

await scenario({}, async ({ proxy, observed, call }) => {
  assert.equal(proxy.ready, true);
  assert.equal(proxy.upstreamModel, defaultGonkaProxyModel);
  assert.deepEqual(proxy.upstreamStatus().map(p => p.name), ['primary']);
  for (const alias of aliases) {
    const response = await call(alias);
    assert.equal(response.status, 200, alias);
    assert.equal(response.body.model, defaultGonkaProxyModel);
    const sent = observed.at(-1);
    assert.equal(sent.provider, 'primary');
    assert.equal(sent.body.model, defaultGonkaProxyModel);
    assert.equal(sent.body.max_tokens, 128);
    assert.equal(sent.authorization, `Bearer ${primaryKey}`);
    assert.notEqual(sent.authorization, `Bearer ${clientToken}`);
  }
  const count = observed.length;
  for (const invalid of [undefined, null, 42, {}, '', 'gpt-4o', 'not-minimax', 'deepseek/../../other', 'minimax\nother']) {
    assert.equal((await call(invalid)).status, 400);
  }
  assert.equal((await call('MiniMax-M2.7', {}, 'wrong-client-'.repeat(4))).status, 401);
  assert.equal(observed.length, count);
});

await scenario({ respond: () => Response.json({ error: { message: 'synthetic outage' } }, { status: 503 }) }, async ({ observed, call }) => {
  assert.equal((await call(miniMaxProxyModel)).status, 503);
  assert.deepEqual(observed.map(p => p.provider), ['primary'], 'quarantined secondary must never receive a failed DeepSeek request');
});

await scenario({ fallbackModels: miniMaxProxyModel, upstreamModel: miniMaxProxyModel, providerStrategy: 'fallback',
  respond: ({ provider, body }) => provider === 'primary'
    ? Response.json({ error: { message: 'synthetic outage' } }, { status: 503 })
    : Response.json({ model: body.model, choices: [{ message: { role: 'assistant', content: '4' }, finish_reason: 'stop' }] })
}, async ({ proxy, observed, call }) => {
  assert.deepEqual(proxy.upstreamStatus().map(p => p.name), ['primary', 'fallback']);
  assert.equal((await call(defaultGonkaProxyModel)).status, 200);
  assert.deepEqual(observed.map(p => p.provider), ['primary', 'fallback']);
  assert.equal(observed[1].authorization, `Bearer ${fallbackKey}`);
});

await scenario({ fallbackModels: '*', providerStrategy: 'fallback',
  respond: ({ provider, body }) => provider === 'primary'
    ? Response.json({ error: { message: 'synthetic outage' } }, { status: 503 })
    : Response.json({ model: body.model, choices: [{ message: { role: 'assistant', content: '4' }, finish_reason: 'stop' }] })
}, async ({ proxy, observed, call }) => {
  assert.equal(proxy.ready, true);
  assert.equal((await call('deepseek')).status, 200);
  assert.deepEqual(observed.map(p => p.provider), ['primary', 'fallback'], 'operator can re-enable the recovered provider without replacing credentials');
});

await scenario({ fallbackModels: 'none', upstreamModel: miniMaxProxyModel }, async ({ proxy, call }) => {
  assert.deepEqual(proxy.upstreamStatus().map(p => p.name), ['primary']);
  assert.equal((await call('minimax')).status, 200);
});

for (const policy of ['', 'unsupported', '*,MiniMaxAI/MiniMax-M2.7', 'none,deepseek-ai/DeepSeek-V4-Flash-0731']) {
  await scenario({ fallbackModels: policy }, async ({ proxy, observed, call }) => {
    assert.equal(proxy.ready, false);
    assert.equal((await call('deepseek')).status, 503);
    assert.equal(observed.length, 0);
  });
}

const tools = [{ type: 'function', function: { name: 'qa_reference', description: 'Synthetic reference',
  parameters: { type: 'object', properties: { excluded: { type: 'string', pattern: '^(?!)$' } }, additionalProperties: false } } }];
await scenario({ respond: ({ body }) => Response.json({ model: body.model, choices: [{ finish_reason: 'stop',
  message: { role: 'assistant', content: null, tool_calls: [{ id: 'qa_tool_1', type: 'function', function: { name: 'qa_reference', arguments: '{}' } }] } }] })
}, async ({ observed, call }) => {
  const response = await call('MiniMax-M2.7', { tools, tool_choice: { type: 'function', function: { name: 'qa_reference' } } });
  assert.equal(response.status, 200);
  assert.deepEqual(observed[0].body.tools, tools, 'DeepSeek must receive the original tool schema, without MiniMax RE2 rewrites');
  assert.equal(response.body.choices[0].message.tool_calls[0].id, 'qa_tool_1');
  assert.equal(response.body.choices[0].message.tool_calls[0].function.arguments, '{}');
});

console.log(JSON.stringify({ ok: true, checks, aliasCount: aliases.length,
  route: defaultGonkaProxyModel, primaryOnlyForDeepSeek: true, miniMaxRollbackPreserved: true,
  providerCredentialsRemainSeparateFromClientCredentials: true, originalToolSchemaPreserved: true }));
