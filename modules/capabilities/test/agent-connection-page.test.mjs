import test from 'node:test';
import assert from 'node:assert/strict';
import { createAgentConnectionProfile, renderAgentConnectionPage, DISCOVERY_HTML_LIMITS } from '../server/discovery-pages.mjs';
import { buildCapabilitiesOpenApi } from '../../../server/capabilities-openapi.js';
import { buildDiscoveryOpenApi } from '../server/openapi.mjs';

const origin = 'https://docs.soty.test', audience = 'https://api.soty.test';
const configured = () => buildCapabilitiesOpenApi({ mcpConfigured: true, externalConfigured: true, guidanceConfigured: true, queryConfigured: true });
const render = (document = configured(), status = { notesCreateEnabled: false, audience }) => renderAgentConnectionPage({
  profile: createAgentConnectionProfile(document), status, origin,
});
const rejects = error => error?.code === 'discovery_render_invalid';

test('the real composed public contract produces exact MCP/HTTP paths and an honest Notes-specific unavailable status', () => {
  const document = configured(), page = render(document);
  assert.match(page, /<h1 id="page-title">Подключить агента<\/h1>/u);
  assert.ok(page.includes('POST /mcp')); assert.ok(page.includes('Streamable HTTP'));
  assert.ok(page.includes(`${audience}/mcp`)); assert.ok(page.includes(`href="${origin}/agents/connect"`));
  assert.doesNotMatch(page, /https:\/\/docs\.soty\.test\/mcp/u, 'public canonical is not credential audience');
  assert.ok(page.includes('OAuth-подключение пока недоступно')); assert.ok(page.includes('Этот статус относится только к Notes'));
  for (const [path, name] of Object.entries({ catalog: 'apps_catalog_search', contract: 'apps_catalog_get', invoke: 'apps_invoke',
    get: 'apps_invocation_get', cancel: 'apps_invocation_cancel', query: 'apps_query', 'guidance-list': 'apps_guidance_list', 'guidance-get': 'apps_guidance_get' })) {
    const full = `/api/capabilities/v1/app-actions/${path}`;
    assert.equal(document.paths[full].post.operationId, name);
    assert.ok(page.includes(`POST ${full}`), name); assert.ok(page.includes(name), name);
    assert.ok(!page.includes(`href="${full}"`), 'a typed POST operation is documented, not exposed as a broken GET link');
  }
  for (const path of ['/agents', '/#access', '/api/capabilities/v1/status', '/api/capabilities/v1/openapi.json']) assert.ok(page.includes(`href="${path}"`));
  assert.ok(page.includes('unprobed')); assert.ok(page.includes('idempotencyKey')); assert.ok(page.includes('effectState'));
  assert.ok(page.includes('не доказывает, что данные с тех пор не изменились'));
});

test('unconfigured transports, unsupported protocol aliases and metadata-only hosts do not promise executable app access', () => {
  const discovery = render(buildDiscoveryOpenApi(), { notesCreateEnabled: false, audience: null });
  assert.ok(discovery.includes('MCP: не настроен')); assert.ok(discovery.includes('каталог действий приложений этим сервером не объявлен'));
  assert.ok(discovery.includes('Канонический audience для доступа не объявлен'));
  assert.doesNotMatch(discovery, /POST \/mcp|POST \/api\/capabilities\/v1\/app-actions\/invoke/u);
  const metadataOnly = structuredClone(configured());
  for (const name of ['invoke', 'get', 'cancel', 'query', 'guidance-list', 'guidance-get']) delete metadataOnly.paths[`/api/capabilities/v1/app-actions/${name}`];
  const page = render(metadataOnly);
  assert.ok(page.includes('apps_catalog_search')); assert.ok(!page.includes('Первое действие с проверяемым результатом'));
  assert.ok(!page.includes('POST /api/capabilities/v1/app-actions/invoke'));
  metadataOnly['x-soty-mcp'].endpoint = 'https://outside.invalid/mcp';
  metadataOnly.paths['/api/capabilities/v1/app-actions/catalog'].post.operationId = 'unknown_handler';
  const unsupported = render(metadataOnly); assert.ok(unsupported.includes('MCP: не настроен')); assert.ok(!unsupported.includes('outside.invalid'));
});

test('OAuth metadata links are the actual published paths and do not claim that authorization issuance is live', () => {
  const page = render(buildCapabilitiesOpenApi({ oauthConfigured: true, mcpConfigured: true }));
  assert.ok(page.includes('OAuth: профиль объявлен')); assert.ok(page.includes('не подтверждает доступность выдачи'));
  assert.ok(page.includes('href="/.well-known/oauth-authorization-server/oauth"'));
  assert.ok(page.includes('href="/.well-known/oauth-protected-resource/mcp"'));
  assert.ok(!page.includes('href="/.well-known/oauth-authorization-server"'));
  assert.ok(page.includes('созданию записок, а не ко всем действиям приложений'));
  assert.ok(!render().includes('href="/.well-known/oauth-authorization-server/oauth"'), 'unconfigured OAuth does not send the reader into a known unavailable endpoint');
});

test('raw private metadata, unknown extensions and arbitrary tool descriptions never reach the connection HTML', () => {
  const document = structuredClone(configured());
  document.info.description = 'PRIVATE_APP_DESCRIPTION'; document.components.schemas.privateResource = { example: 'PRIVATE_DOCUMENT_TEXT' };
  document['x-private'] = { accountId: 'PRIVATE_ACCOUNT_ID', token: 'SYNTHETIC_PRIVATE_CREDENTIAL' };
  document['x-soty-mcp'].tools.push({ name: 'PRIVATE_APP_NAME', description: '<img src=x onerror=STEAL()>' });
  document.paths['/private-workspace'] = { post: { operationId: 'PRIVATE_CAPABILITY_NAME' } };
  const page = render(document);
  assert.doesNotMatch(page, /PRIVATE_|SYNTHETIC_PRIVATE_CREDENTIAL|STEAL|<img\b/u);
  assert.ok(page.includes('apps_catalog_search'));
  const profile = createAgentConnectionProfile(document);
  assert.ok(Object.isFrozen(profile)); assert.ok(Object.values(profile).every(value => typeof value === 'boolean'));
  assert.throws(() => renderAgentConnectionPage({ profile: { ...profile, accountId: 'PRIVATE_ACCOUNT_ID' }, status: { notesCreateEnabled: false, audience } }), rejects);
  assert.throws(() => renderAgentConnectionPage({ profile, status: { notesCreateEnabled: false, audience, token: 'SYNTHETIC_PRIVATE_CREDENTIAL' } }), rejects);
});

test('credential audience is a validated explicit origin and ready Notes do not imply generic or MCP permissions', () => {
  const page = render(configured(), { notesCreateEnabled: true, audience });
  assert.ok(page.includes('Обработчик создания записок готов')); assert.ok(page.includes('указать клиента, срок и лимит'));
  assert.ok(page.includes('не становится MCP-доступом при смене URL'));
  for (const value of ['https://user:SYNTHETIC_SECRET@soty.test', 'https://soty.test/?token=SYNTHETIC_SECRET', 'javascript:STEAL()', 'https://soty.test/path', 'https://soty.test/']) {
    assert.throws(() => render(configured(), { notesCreateEnabled: false, audience: value }), rejects);
  }
});

test('connection reading works without JavaScript, has safe fixed navigation and offers no pretend run or secret input', () => {
  const page = render(), withoutScript = page.replace(/<script\b[^>]*>[\s\S]*?<\/script>/gu, '');
  assert.match(page, /^<!doctype html><html lang="ru">/u); assert.ok(Buffer.byteLength(page) <= DISCOVERY_HTML_LIMITS.connect);
  assert.equal([...page.matchAll(/<script\b/gu)].length, 1); assert.ok(page.includes('defer src="/capability-docs-update.js"'));
  assert.doesNotMatch(page, /<(?:form|input|button|iframe|canvas)\b|\son(?:click|load|error|submit)=/u);
  assert.ok(withoutScript.includes('href="/#access"')); assert.ok(withoutScript.includes('href="/api/capabilities/v1/openapi.json"'));
  assert.ok(page.includes('min-height:44px')); assert.ok(page.includes('prefers-reduced-motion:reduce')); assert.ok(page.includes('overflow-wrap:anywhere'));
});
