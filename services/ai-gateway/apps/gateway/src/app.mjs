import Fastify from 'fastify';
import { randomUUID } from 'node:crypto';
import { readFile, stat } from 'node:fs/promises';
import { resolve, extname, sep } from 'node:path';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { registerInstallations } from '../../../packages/installation/index.mjs';
import { resolveAlias } from '../../../packages/connector/index.mjs';
import { DomainError, fingerprint, safeEqual, validateChatBody, estimateReservationTokens, csvCell, publicDigest } from '../../../packages/domain/index.mjs';
import { publicSotyModels } from './soty-policy.mjs';

const COOKIE = 'soty_workspace';
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const SESSION_TTL_SECONDS = 30 * 86400;
const mutation = method => ['POST', 'PATCH', 'PUT', 'DELETE'].includes(method);
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.json': 'application/json; charset=utf-8', '.webmanifest': 'application/manifest+json', '.png': 'image/png', '.svg': 'image/svg+xml', '.ico': 'image/x-icon', '.woff2': 'font/woff2', '.jpg': 'image/jpeg', '.webp': 'image/webp' };

function parseCookies(header = '') {
  const result = {};
  for (const part of header.split(';')) {
    const index = part.indexOf('=');
    if (index < 0) continue;
    const name = part.slice(0, index).trim();
    if (name === COOKIE) result[name] = part.slice(index + 1).trim();
  }
  return result;
}

function modelView(model) {
  const status = model.capabilities ?? {};
  const verified = Array.isArray(status) ? status : Object.entries(status)
    .filter(([, state]) => state === true || state === 'verified' || state === 'supported' || state === 'tested')
    .map(([capability]) => capability);
  if (['healthy', 'slow', 'recovering'].includes(model.status) && !verified.includes('text')) verified.push('text');
  return { ...model, capabilities: verified, capabilityStatus: Array.isArray(status) ? undefined : status };
}

function safeError(error) {
  if (error instanceof DomainError) return { status: error.status, code: error.code, message: error.message, details: error.details };
  const status = Number(error.status ?? error.statusCode);
  const code = typeof error.code === 'string' && /^[a-zA-Z0-9_]{1,80}$/.test(error.code) ? error.code : 'internal_error';
  const messages = {
    upstream_timeout: 'Модель не успела ответить. Попробуйте другую модель или повторите запрос.',
    upstream_first_useful_timeout: 'Модель слишком долго начинает ответ.',
    upstream_rate_limited: 'Поставщик временно ограничил запросы.',
    upstream_unavailable: 'Поставщик временно недоступен.',
    upstream_credentials_invalid: 'Администратору нужно проверить доступ к поставщику.',
    upstream_funding_required: 'Требуется пополнение счёта поставщика.',
    model_unavailable: 'Модель сейчас недоступна.',
    unsupported_parameter: 'Один из параметров не поддерживается моделью.',
    FST_ERR_CTP_BODY_TOO_LARGE: 'Размер запроса превышает 1 МБ.',
    FST_ERR_CTP_INVALID_JSON_BODY: 'Некорректный JSON.',
  };
  return { status: status >= 400 && status <= 599 ? status : 500, code, message: messages[code] ?? (status >= 400 && status < 500 ? 'Проверьте параметры запроса.' : 'Сервис временно не смог выполнить запрос.') };
}

function rateLimiter({ maxEntries = 50000 } = {}) {
  const entries = new Map();
  return (key, maximum, intervalMs) => {
    const now = Date.now();
    if (entries.size >= maxEntries) for (const [name, entry] of entries) if (entry.until <= now) entries.delete(name);
    let bucket = entries.get(key);
    if (!bucket || bucket.until <= now) {
      if (entries.size >= maxEntries) throw new DomainError('gateway_busy', 'Сервис временно перегружен.', 503);
      bucket = { count: 0, until: now + intervalMs }; entries.set(key, bucket);
    }
    bucket.count++;
    if (bucket.count > maximum) throw new DomainError('rate_limited', 'Слишком много запросов. Повторите немного позже.', 429);
  };
}

export async function createApp({ store, connector, baseUrl, staticDirectory, cookieSecure = true, admin = false, installEnabled = true, trustProxy = false, releaseId = 'development', customerDeadlineMs = 55_000, sotyBridge = null }) {
  const app = Fastify({ logger: false, bodyLimit: 1_048_576, requestTimeout: 60_000, connectionTimeout: Math.max(65_000, customerDeadlineMs + 10_000), keepAliveTimeout: 5000, trustProxy });
  const allowedOrigin = new URL(baseUrl).origin;
  const checkRate = rateLimiter();
  app.decorateRequest('workspaceId', null);
  app.decorateRequest('csrfToken', null);
  app.decorateRequest('serverRequestId', null);
  app.decorateRequest('sessionExpiresAt', null);
  app.decorateRequest('bridgeRawBody', null);
  app.decorateRequest('sotyProof', null);
  const parseJson = app.getDefaultJsonParser('error', 'error');
  app.removeContentTypeParser('application/json');
  app.addContentTypeParser('application/json', { parseAs: 'buffer' }, (request, body, done) => {
    request.bridgeRawBody = body;
    parseJson(request, body.toString('utf8'), done);
  });

  function originGuard(request) {
    const origin = request.headers.origin;
    if (!origin) return;
    const origins = new Set([allowedOrigin]);
    if (admin || !cookieSecure) {
      try {
        const host = new URL(`${request.protocol}://${request.headers.host}`);
        if (['localhost', '127.0.0.1', '[::1]'].includes(host.hostname)) origins.add(host.origin);
      } catch { /* Invalid hosts are never admitted as origins. */ }
    }
    if (!origins.has(origin)) throw new DomainError('origin_not_allowed', 'Этот сайт не может управлять кабинетом.', 403);
  }

  async function managementGuard(request) {
    if (!request.workspaceId) throw new DomainError('workspace_required', 'Откройте временный кабинет.', 401);
    if (mutation(request.method)) {
      originGuard(request);
      if (!safeEqual(request.headers['x-csrf-token'], request.csrfToken)) throw new DomainError('csrf_required', 'Обновите страницу кабинета.', 403);
    }
  }

  app.addHook('onRequest', async (request, reply) => {
    request.serverRequestId = randomUUID();
    reply.header('X-Request-Id', request.serverRequestId);
    reply.header('X-Content-Type-Options', 'nosniff').header('Referrer-Policy', 'strict-origin-when-cross-origin');
    reply.header('X-Frame-Options', 'DENY').header('Permissions-Policy', 'camera=(), microphone=(), geolocation=()');
    const path = request.url.split('?')[0];
    if (path.startsWith('/api/') || path.startsWith('/setup/') || path.startsWith('/internal/')) reply.header('Cache-Control', 'no-store');
    if (path === '/api/session' || path.startsWith('/setup/')) {
      const ipKey = fingerprint(`rate:${request.ip}`, store.pepper);
      checkRate(`${path}:${ipKey}`, path === '/api/session' ? 20 : 10, 60_000);
    }
    if (path.startsWith('/api/') && !path.startsWith('/api/inference/') && !path.startsWith('/api/connectors/gonka/')) {
      const secret = parseCookies(request.headers.cookie)[COOKIE];
      const session = await store.getSession(secret);
      if (session) {
        request.workspaceId = session.tenantId; request.sessionExpiresAt = session.expiresAt;
        request.csrfToken = fingerprint(`csrf:${secret}`, store.pepper);
      }
      if (path !== '/api/session') await managementGuard(request);
      if (request.workspaceId) checkRate(`workspace:${request.workspaceId}`, 180, 60_000);
    }
  });

  app.setErrorHandler((error, request, reply) => {
    const safe = safeError(error);
    if (reply.raw.headersSent) { reply.raw.destroy(); return; }
    if (safe.status === 429) reply.header('Retry-After', Math.max(1, Math.min(60, error.retryAfter ?? 5)));
    reply.status(safe.status).send({ error: { message: safe.message, type: safe.status >= 500 ? 'server_error' : 'invalid_request_error', code: safe.code, request_id: request.serverRequestId, ...(safe.details ? { details: safe.details } : {}) } });
  });

  app.get('/healthz', async () => ({ status: 'ok', service: 'soty-inference', version: '0.2.0', releaseId }));
  app.get('/readyz', async (_request, reply) => {
    try { await store.pool.query('SELECT 1'); return { status: 'ready', catalogModels: connector.models().length }; }
    catch { return reply.code(503).send({ status: 'database_unavailable' }); }
  });

  app.post('/api/session', async (request, reply) => {
    originGuard(request);
    if (request.workspaceId) return { temporary: true, expiresAt: request.sessionExpiresAt, csrfToken: request.csrfToken };
    const session = await store.createSession();
    reply.header('Set-Cookie', `${COOKIE}=${session.secret}; Path=/; HttpOnly; SameSite=Strict; Max-Age=${SESSION_TTL_SECONDS}${cookieSecure ? '; Secure' : ''}`);
    return { temporary: true, expiresAt: session.expiresAt, csrfToken: fingerprint(`csrf:${session.secret}`, store.pepper) };
  });

  const capabilities = () => ({ managementEnabled: true, chatEnabled: true, demoTopupEnabled: true, installerEnabled: installEnabled, registrationEnabled: false, paymentsEnabled: false, streamSupported: true, responsesSupported: false, messagesSupported: false });
  app.get('/api/dashboard', async request => {
    const days = Number(request.query.days ?? 30);
    if (!Number.isInteger(days) || days < 1 || days > 365) throw new DomainError('invalid_period', 'Период: от 1 до 365 дней.');
    return store.dashboard(request.workspaceId, { models: connector.models().map(modelView), baseUrl: `${baseUrl.replace(/\/$/, '')}/v1`, capabilities: capabilities(), days });
  });
  app.get('/api/models', async () => ({ models: connector.models().map(modelView), updatedAt: new Date().toISOString() }));
  app.get('/api/capabilities', async () => capabilities());
  app.get('/api/keys', async request => ({ keys: await store.listKeys(request.workspaceId) }));
  app.post('/api/keys', async (request, reply) => reply.code(201).send(await store.createKeyOperation(request.workspaceId, request.body ?? {}, request.headers['idempotency-key'])));
  for (const method of ['PATCH', 'DELETE']) {
    app.route({ method, url: '/api/keys/:id', handler: async request => {
      if (!UUID.test(request.params.id)) throw new DomainError('key_not_found', 'Ключ не найден.', 404);
      return { key: await store.patchKey(request.workspaceId, request.params.id, method === 'DELETE' ? { status: 'revoked' } : request.body) };
    } });
  }
  app.post('/api/keys/:id/rotate', async request => {
    if (!UUID.test(request.params.id)) throw new DomainError('key_not_found', 'Ключ не найден.', 404);
    return store.rotateKey(request.workspaceId, request.params.id, request.headers['idempotency-key']);
  });
  app.post('/api/topups/demo', async request => store.topupDemo(request.workspaceId, request.body?.amountMicrorubles, request.headers['idempotency-key']));
  app.get('/api/payments', async request => ({ payments: await store.payments(request.workspaceId) }));
  app.get('/api/requests/:id', async request => {
    if (!UUID.test(request.params.id)) throw new DomainError('request_not_found', 'Запрос не найден.', 404);
    const result = await store.pool.query(`SELECT id,status,model_requested,model_resolved,input_tokens,output_tokens,
      charge_urub::text,created_at,finished_at,error_code FROM inference_requests WHERE tenant_id=$1 AND id=$2`, [request.workspaceId, request.params.id]);
    if (!result.rowCount) throw new DomainError('request_not_found', 'Запрос не найден.', 404);
    return { request: result.rows[0] };
  });
  app.patch('/api/settings', async request => ({ settings: await store.updateSettings(request.workspaceId, request.body) }));

  async function profile(request) {
    const input = request.method === 'GET' ? request.query : request.body;
    const model = connector.models().find(model => model.id === input?.model);
    if (!model) throw new DomainError('model_unavailable', 'Выберите доступную модель.', 400);
    if (input.keyId) {
      const keys = await store.listKeys(request.workspaceId);
      if (!keys.some(key => key.id === input.keyId)) throw new DomainError('key_not_found', 'Ключ не найден.', 404);
    }
    const apiBaseUrl = `${baseUrl.replace(/\/$/, '')}/v1`;
    return { baseUrl: apiBaseUrl, model: model.id, config: { provider: 'openai-compatible', baseURL: apiBaseUrl, model: model.id, apiKey: '<YOUR_PERSONAL_KEY>' }, compatibility: { chatCompletions: true, responses: false, messages: false } };
  }
  app.get('/api/connect/profile', profile); app.post('/api/connect/profile', profile);
  app.get('/api/usage/export', async (request, reply) => {
    const days = Number(request.query.days ?? 30);
    if (!Number.isInteger(days) || days < 1 || days > 365) throw new DomainError('invalid_period', 'Период: от 1 до 365 дней.');
    const records = await store.exportUsage(request.workspaceId, { days });
    const columns = ['id', 'created_at', 'key_name', 'model_requested', 'model_resolved', 'status', 'input_tokens', 'output_tokens', 'charge_urub', 'duration_ms'];
    const data = '\ufeff' + [columns.map(csvCell).join(','), ...records.map(row => columns.map(column => csvCell(row[column] instanceof Date ? row[column].toISOString() : row[column])).join(','))].join('\r\n');
    return reply.type('text/csv; charset=utf-8').header('Content-Disposition', 'attachment; filename="soty-usage.csv"').header('X-Export-Max-Rows', '10000').send(data);
  });

  async function apiKey(request, prefix) {
    const raw = request.headers.authorization;
    if (typeof raw !== 'string' || !/^Bearer [^\s]+$/i.test(raw)) throw new DomainError('invalid_api_key', 'Укажите API-ключ в заголовке Authorization: Bearer.', 401);
    const key = await store.authenticate(raw.slice(7));
    if (prefix && !key.allowed_paths.includes(prefix)) throw new DomainError('key_scope_not_allowed', 'Ключ не разрешён для этого API.', 403);
    return key;
  }

  function cors(reply) { reply.header('Access-Control-Allow-Origin', '*').header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS').header('Access-Control-Allow-Headers', 'Authorization, Content-Type, Idempotency-Key').header('Access-Control-Expose-Headers', 'X-Request-Id, X-Soty-Request-Id, X-Actual-Model, X-Soty-Cost-Microrubles'); }
  for (const prefix of ['/v1', '/api/inference/v1', '/api/connectors/gonka/v1']) {
    app.options(`${prefix}/*`, async (_request, reply) => { cors(reply); return reply.code(204).send(); });
    app.get(`${prefix}/models`, async (request, reply) => {
      cors(reply); const key = await apiKey(request, prefix);
      return { object: 'list', data: connector.models().filter(model => !key.allowed_models || key.allowed_models.includes(model.id)).map(model => ({ id: model.id, object: 'model', created: 0, owned_by: 'soty', context_window: model.contextWindow })) };
    });
    app.post(`${prefix}/chat/completions`, async (request, reply) => { cors(reply); return inference(request, reply, await apiKey(request, prefix)); });
    for (const unsupported of ['responses', 'messages']) app.post(`${prefix}/${unsupported}`, async request => { await apiKey(request, prefix); throw new DomainError('unsupported_protocol', 'Для этого ключа используйте /v1/chat/completions.', 501); });
  }

  app.post('/api/chat', async (request, reply) => inference(request, reply, await store.getChatKey(request.workspaceId)));

  if (sotyBridge) {
    function serviceRoute(method, path, operation, handler) {
      app.route({ method, url: `/internal/soty/v1${path}`, preHandler: async request => {
        const proof = await sotyBridge.verify({ method: request.method, path: request.raw.url, body: request.bridgeRawBody ?? '', headers: request.headers });
        if (proof.operation !== operation || (proof.deviceId === 'service:provisioner' && !['account.ensure', 'account.status'].includes(operation))) throw new DomainError('bridge_scope_denied', 'Недопустимая служебная операция.', 403);
        if (proof.deviceId.startsWith('service:') && operation.startsWith('keys.')) throw new DomainError('bridge_scope_denied', 'Ключами управляет владелец аккаунта.', 403);
        request.sotyProof = proof;
      }, handler });
    }
    const status = async request => {
      const models = connector.models();
      const account = await store.sotyAccountStatus(request.sotyProof, { models, configured: connector.snapshot?.().configured ?? true });
      return { ...account, models: publicSotyModels(models) };
    };
    serviceRoute('POST', '/accounts/ensure', 'account.ensure', async request => {
      if (!request.body || typeof request.body !== 'object' || Array.isArray(request.body) || Object.keys(request.body).length) throw new DomainError('invalid_ensure_request', 'Параметры подключения берутся из проверенного аккаунта.');
      await store.ensureSotyAccount(request.sotyProof); return status(request);
    });
    serviceRoute('GET', '/account', 'account.status', status);
    serviceRoute('POST', '/chat/completions', 'chat.complete', async (request, reply) => {
      const account = await store.sotyAccount(request.sotyProof);
      return inference(request, reply, await store.getChatKey(account.tenant_id), request.sotyProof);
    });
    serviceRoute('GET', '/requests/:id', 'request.receipt', async request => {
      if (!UUID.test(request.params.id)) throw new DomainError('request_not_found', 'Запрос не найден.', 404);
      const account = await store.sotyAccount(request.sotyProof);
      return store.requestReceipt(account.tenant_id, request.params.id);
    });
    serviceRoute('GET', '/keys', 'keys.list', async request => {
      const account = await store.sotyAccount(request.sotyProof);
      return { keys: (await store.listKeys(account.tenant_id)).filter(key => key.purpose === 'customer') };
    });
    for (const action of ['create', 'update', 'rotate', 'revoke']) serviceRoute('POST', `/keys/${action}`, `keys.${action}`, async (request, reply) => {
      const account = await store.sotyAccount(request.sotyProof);
      const input = request.body;
      if (!input || typeof input !== 'object' || Array.isArray(input)) throw new DomainError('invalid_key_request', 'Некорректные параметры ключа.');
      const idempotencyKey = publicDigest(JSON.stringify([request.sotyProof.iss, request.sotyProof.accountId, request.sotyProof.requestId]));
      if (action === 'create') return reply.code(201).send(await store.createKeyOperation(account.tenant_id, input, idempotencyKey));
      const { keyId, expectedPolicyRevision, ...change } = input;
      if (!UUID.test(keyId)) throw new DomainError('key_not_found', 'Ключ не найден.', 404);
      if (action !== 'update' && expectedPolicyRevision !== undefined) throw new DomainError('invalid_key_request', 'Неизвестные параметры ключа.');
      if (['rotate', 'revoke'].includes(action) && Object.keys(change).length) throw new DomainError('invalid_key_request', 'Неизвестные параметры ключа.');
      if (action === 'rotate') return store.rotateKey(account.tenant_id, keyId, idempotencyKey);
      return store.patchKeyOperation(account.tenant_id, keyId, action === 'revoke' ? { status: 'revoked' } : change, idempotencyKey, `key.${action}`, expectedPolicyRevision);
    });
  }

  async function inference(request, reply, key, proof = null) {
    validateChatBody(request.body);
    const body = { ...request.body };
    const workspaceSettings = (await store.pool.query('SELECT settings FROM tenants WHERE id=$1', [key.tenant_id])).rows[0]?.settings ?? {};
    const autoFallback = body.autoFallback === undefined ? (key.purpose === 'chat' ? workspaceSettings.autoFallback === true : key.auto_fallback === true) : body.autoFallback === true;
    if (body.autoFallback !== undefined && typeof body.autoFallback !== 'boolean') throw new DomainError('invalid_policy', 'autoFallback должен быть true или false.');
    delete body.autoFallback;
    const canonicalModel = resolveAlias(body.model, connector.models());
    const model = connector.models().find(entry => entry.id === canonicalModel) ?? connector.models().find(entry => entry.id === body.model);
    if (!model) throw new DomainError('model_unavailable', 'Модель отсутствует в текущем каталоге.', 404);
    const maxOutput = body.max_completion_tokens ?? body.max_tokens ?? Math.min(key.limits.maxOutputTokens ?? 2048, 2048);
    if (body.max_completion_tokens === undefined) body.max_tokens = maxOutput;
    const estimate = estimateReservationTokens(body, maxOutput);
    const idempotencyKey = proof ? publicDigest(JSON.stringify([proof.iss, proof.accountId, proof.requestId])) : request.headers['idempotency-key'];
    if (idempotencyKey && (typeof idempotencyKey !== 'string' || idempotencyKey.length < 8 || idempotencyKey.length > 200)) throw new DomainError('invalid_idempotency_key', 'Idempotency-Key: от 8 до 200 символов.');
    const sourceContext = proof ? { issuer: proof.iss, accountId: proof.accountId, deviceId: proof.deviceId, requestId: proof.requestId, authorizationId: proof.authorizationId, bodySha256: proof.bodySha256 } : {};
    const controller = new AbortController();
    const disconnected = () => { if (!reply.raw.writableFinished) controller.abort(new DomainError('client_disconnected', 'Клиент закрыл соединение.', 499)); };
    reply.raw.once('close', disconnected);
    let reservation;
    try { reservation = await store.reserve({ key, model, ...estimate, inputRate: model.inputPriceMicrorublesPerMillion ?? null, outputRate: model.outputPriceMicrorublesPerMillion ?? null, body: { ...body, autoFallback }, idempotencyKey, autoFallback, sourceContext }); }
    catch (error) { reply.raw.off('close', disconnected); throw error; }
    if (reservation.replay) {
      reply.raw.off('close', disconnected);
      if (body.stream) throw new DomainError('stream_replay_unavailable', 'Повтор этого потока не отправлен поставщику. Получите статус по исходному запросу.', 409, { requestId: reservation.request.id });
      reply.header('X-Soty-Request-Id', reservation.request.id).header('X-Idempotent-Replay', 'true');
      return store.replay(reservation.request);
    }
    const start = performance.now();
    let timer = setTimeout(() => controller.abort(new DomainError('upstream_timeout', 'Превышено время ожидания.', 504)), customerDeadlineMs);
    let completed = false;
    const admittedPrices = new Map();
    try {
      const eligibleModels = autoFallback ? connector.models().filter(candidate => {
        if (key.allowed_models && !key.allowed_models.includes(candidate.id)) return false;
        if (candidate.inputPriceMicrorublesPerMillion == null || candidate.outputPriceMicrorublesPerMillion == null) return false;
        return BigInt(candidate.inputPriceMicrorublesPerMillion) <= BigInt(model.inputPriceMicrorublesPerMillion ?? 0) && BigInt(candidate.outputPriceMicrorublesPerMillion) <= BigInt(model.outputPriceMicrorublesPerMillion ?? 0);
      }).map(candidate => candidate.id) : key.allowed_models;
      const priceCeiling = model.inputPriceMicrorublesPerMillion != null && model.outputPriceMicrorublesPerMillion != null
        ? (BigInt(model.inputPriceMicrorublesPerMillion) > BigInt(model.outputPriceMicrorublesPerMillion) ? model.inputPriceMicrorublesPerMillion : model.outputPriceMicrorublesPerMillion) : null;
      const beforeDispatch = async ({ providerId, model: candidate, signal }) => {
        if (signal.aborted) throw new DomainError('request_canceled', 'Запрос отменён.', 499);
        const actualModel = connector.models().find(entry => entry.id === candidate);
        if (!key.legacy && (actualModel?.inputPriceMicrorublesPerMillion == null || actualModel?.outputPriceMicrorublesPerMillion == null
          || BigInt(actualModel.inputPriceMicrorublesPerMillion) > BigInt(model.inputPriceMicrorublesPerMillion ?? 0)
          || BigInt(actualModel.outputPriceMicrorublesPerMillion) > BigInt(model.outputPriceMicrorublesPerMillion ?? 0))) throw new DomainError('price_changed', 'Тариф модели изменился до отправки запроса.', 409);
        const priceSnapshot = actualModel?.inputPriceMicrorublesPerMillion != null && actualModel?.outputPriceMicrorublesPerMillion != null
          ? { inputRate: actualModel.inputPriceMicrorublesPerMillion, outputRate: actualModel.outputPriceMicrorublesPerMillion } : null;
        await store.admitDispatch({ key, reservation, model: candidate });
        if (proof) await sotyBridge.authorizeDispatch({ proof, gatewayRequestId: reservation.id, providerId, model: candidate, signal });
        if (signal.aborted) throw new DomainError('request_canceled', 'Запрос отменён.', 499);
        await store.admitDispatch({ key, reservation, model: candidate, commit: true });
        admittedPrices.set(`${providerId}:${candidate}`, priceSnapshot);
        return { priceSnapshot };
      };
      const result = await connector.complete({ body, signal: controller.signal, logicalRequestId: reservation.id, policy: { autoFallback, allowedModels: eligibleModels, strategy: workspaceSettings.routingMode, maxPriceMicrorublesPerMillion: priceCeiling }, purpose: key.purpose, beforeDispatch });
      if (controller.signal.aborted) throw new DomainError('request_canceled', 'Запрос отменён.', 499);
      reply.header('X-Soty-Request-Id', reservation.id).header('X-Actual-Model', result.model ?? model.id);
      if (result.kind === 'stream') {
        clearTimeout(timer); timer = setTimeout(() => controller.abort(), 600_000);
        reply.type('text/event-stream; charset=utf-8').header('Cache-Control', 'no-cache, no-transform').header('X-Accel-Buffering', 'no');
        reply.hijack();
        for (const [name, value] of Object.entries(reply.getHeaders())) reply.raw.setHeader(name, value);
        reply.raw.statusCode = 200;
        await pipeline(Readable.fromWeb(result.stream), reply.raw, { signal: controller.signal });
        const terminal = await result.completion;
        if (terminal?.outcome && !['success', 'completed'].includes(terminal.outcome)) throw new DomainError('incomplete_stream', 'Поток модели оборвался.', 502);
        await store.finish(reservation.id, { outcome: 'completed', usage: terminal?.usage, model: result.model, providerId: result.providerId, durationMs: performance.now() - start, finishReason: terminal?.finishReason, reportedPriceSnapshot: admittedPrices.get(`${result.providerId}:${result.model}`) });
        completed = true; return reply;
      }
      const settled = await store.finish(reservation.id, { outcome: 'completed', usage: result.usage ?? result.body.usage, model: result.model, providerId: result.providerId, durationMs: performance.now() - start, finishReason: result.body.choices?.[0]?.finish_reason, response: result.body, reportedPriceSnapshot: admittedPrices.get(`${result.providerId}:${result.model}`) });
      completed = true;
      reply.header('X-Soty-Cost-Microrubles', settled.chargeMicrorubles);
      return result.body;
    } catch (error) {
      if (!completed) {
        const preflightCodes = ['connector_not_configured', 'invalid_chat_request', 'request_too_large', 'invalid_stream_parameter', 'json_schema_not_supported', 'invalid_token_limit', 'model_not_found', 'model_not_allowed', 'no_healthy_route', 'invalid_routing_strategy'];
        try { await store.finish(reservation.id, { outcome: 'failed', errorCode: safeError(error).code, durationMs: performance.now() - start, noUpstreamDispatch: error.noUpstreamDispatch === true || preflightCodes.includes(error.code) }); }
        catch { /* Durable hold is reconciled after its lease; inference is never repeated here. */ }
      }
      throw error;
    } finally { clearTimeout(timer); reply.raw.off('close', disconnected); }
  }

  if (installEnabled) await registerInstallations(app, {
    pool: store.pool, getTenant: request => request.workspaceId,
    issueKey: (client, { tenantId, name, pendingActivation, autoFallback }) => store.createKey(client, tenantId, { name, autoFallback }, { pendingActivation }),
    activateKey: (client, keyId) => store.activateKey(client, keyId),
    findKeyBySecret: (client, secret) => store.findKeyBySecret(client, secret),
    apiBaseUrl: baseUrl, managementGuard, getModels: async () => connector.models(),
  });

  if (staticDirectory) {
    const root = resolve(staticDirectory);
    app.get('/*', async (request, reply) => {
      let path;
      try { path = decodeURIComponent(request.url.split('?')[0]); } catch { return reply.code(400).send(); }
      if (path.startsWith('/api/') || path.startsWith('/v1/') || path.startsWith('/setup/') || path.startsWith('/internal/')) throw new DomainError('not_found', 'Маршрут не найден.', 404);
      let filename = resolve(root, `.${path}`);
      if (filename !== root && !filename.startsWith(root + sep)) return reply.code(404).send();
      if (path.split('/').some(segment => segment.startsWith('.'))) return reply.code(404).send();
      try { if (!(await stat(filename)).isFile()) filename = resolve(root, 'index.html'); }
      catch { if (extname(path)) return reply.code(404).send(); filename = resolve(root, 'index.html'); }
      try {
        const content = await readFile(filename);
        reply.type(MIME[extname(filename)] ?? 'application/octet-stream');
        reply.header('Cache-Control', filename.endsWith('index.html') || filename.endsWith('sw.js') ? 'no-cache' : 'public, max-age=3600');
        reply.header('Content-Security-Policy', "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self' data:; connect-src 'self'; worker-src 'self'; manifest-src 'self'; object-src 'none'; base-uri 'self'; frame-ancestors 'none'");
        return reply.send(content);
      } catch { return reply.code(503).type('text/plain; charset=utf-8').send('Кабинет обновляется. API доступен отдельно.'); }
    });
  }
  return app;
}
