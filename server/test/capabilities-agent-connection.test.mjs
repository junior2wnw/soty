import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import express from 'express';
import { attachCapabilitiesDiscovery } from '../capabilities-discovery.js';
import { buildCapabilitiesOpenApi } from '../capabilities-openapi.js';

async function fixture(t) {
  const app = express(), state = { catalogReads: 0, statusReads: 0, notesReady: false };
  attachCapabilitiesDiscovery(app, {
    origin: 'https://docs.soty.test',
    catalog: { search() { state.catalogReads++; return { scope: 'public', revision: 'a'.repeat(64), items: [], total: 0, cursor: null }; },
      get() { throw new Error('private_contract_must_not_be_read'); } },
    openApi: buildCapabilitiesOpenApi({ mcpConfigured: true, externalConfigured: true, guidanceConfigured: true, queryConfigured: true }),
    status() { state.statusReads++; return { notesCreateEnabled: state.notesReady, audience: 'https://api.soty.test',
      accountId: 'PRIVATE_STATUS_ACCOUNT', token: 'SYNTHETIC_PRIVATE_STATUS_KEY' }; },
  });
  app.use((_req, res) => res.status(404).end('fallback'));
  const server = createServer(app); await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); });
  const call = (path, { method = 'GET', headers = {} } = {}) => new Promise((resolve, reject) => {
    const req = request({ hostname: '127.0.0.1', port: server.address().port, path, method, headers }, res => {
      const chunks = []; res.on('data', chunk => chunks.push(chunk)); res.on('error', reject);
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, text: Buffer.concat(chunks).toString('utf8') }));
    }); req.on('error', reject); req.end();
  });
  return { call, state };
}

test('connection GET/HEAD use only public protocol and readiness, never account catalog or authorization data', async t => {
  const f = await fixture(t), get = await f.call('/agents/connect');
  assert.equal(get.status, 200); assert.ok(get.text.includes('Подключить агента')); assert.ok(get.text.includes('https://api.soty.test/mcp'));
  assert.equal(f.state.catalogReads, 0); assert.equal(f.state.statusReads, 1);
  assert.equal(get.headers['cache-control'], 'no-store'); assert.equal(get.headers.etag, undefined); assert.equal(get.headers['set-cookie'], undefined);
  assert.match(get.headers['content-security-policy'], /frame-ancestors 'none'/u); assert.equal(get.headers['access-control-allow-origin'], undefined);
  const withCredentials = await f.call('/agents/connect', { headers: { authorization: 'Bearer SYNTHETIC_CALLER_KEY',
    cookie: 'account=PRIVATE_CALLER_ACCOUNT', host: 'evil.example', 'x-forwarded-host': 'forged.example', origin: 'https://forged.example' } });
  assert.equal(withCredentials.status, 200); assert.equal(withCredentials.text, get.text);
  assert.doesNotMatch(withCredentials.text, /PRIVATE_|SYNTHETIC_|evil\.example|forged\.example/u);
  const head = await f.call('/agents/connect', { method: 'HEAD' }); assert.equal(head.status, 200); assert.equal(head.text, '');
  assert.equal(head.headers['content-length'], get.headers['content-length']); assert.equal(f.state.catalogReads, 0);
});

test('readiness is freshly rendered with no conditional-cache shortcut and never announces generic app availability', async t => {
  const f = await fixture(t), first = await f.call('/agents/connect');
  assert.ok(first.text.includes('Создание записок через API сейчас недоступно'));
  f.state.notesReady = true;
  const next = await f.call('/agents/connect', { headers: { 'if-none-match': '*' } });
  assert.equal(next.status, 200); assert.equal(next.headers.etag, undefined); assert.ok(next.text.includes('Обработчик создания записок готов'));
  assert.ok(next.text.includes('не становится MCP-доступом при смене URL')); assert.equal(f.state.catalogReads, 0);
});

test('the new route rejects mutations, private query fields and path aliases without reflecting supplied secrets', async t => {
  const f = await fixture(t);
  for (const method of ['POST', 'PUT', 'DELETE']) {
    const response = await f.call('/agents/connect', { method }); assert.equal(response.status, 405);
    assert.equal(response.headers.allow, 'GET, HEAD'); assert.equal(response.headers['cache-control'], 'no-store');
  }
  for (const path of ['/agents/connect?accountId=PRIVATE_QUERY', '/agents/connect?token=SYNTHETIC_QUERY_KEY', '/agents/connect?query=test']) {
    const response = await f.call(path); assert.equal(response.status, 400); assert.doesNotMatch(response.text, /PRIVATE_|SYNTHETIC_|token|accountId/u);
  }
  for (const path of ['/agents/connect/execute', '/agents/%63onnect', '/agents/connect%2fexecute']) assert.equal((await f.call(path)).status, 404);
  assert.equal(f.state.catalogReads, 0); assert.equal(f.state.statusReads, 0);
});

test('index and sitemap link the real read-only connection page under the unchanged configured canonical origin', async t => {
  const f = await fixture(t), index = await f.call('/agents');
  assert.equal(index.status, 200); assert.ok(index.text.includes('href="/agents/connect"'));
  const sitemap = await f.call('/agents/sitemap.xml'); assert.equal(sitemap.status, 200);
  assert.ok(sitemap.text.includes('<loc>https://docs.soty.test/agents/connect</loc>'));
  assert.equal([...sitemap.text.matchAll(/<loc>https:\/\/docs\.soty\.test\/agents\/connect<\/loc>/gu)].length, 1);
});
