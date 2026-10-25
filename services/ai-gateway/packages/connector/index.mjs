import { createCatalog, resolveAlias } from './catalog.mjs';
import { boundedJson, ConnectorError, retryAfterMs, safeError } from './errors.mjs';
import { prepareStream, validateCompletion, validUsage } from './protocol.mjs';
import { createHealthWorker } from './worker.mjs';

export { ConnectorError, resolveAlias };

export function providersFromEnv(env = process.env) {
  const definitions = [
    ['openbroker', env.GONKA_PRIMARY_BASE_URL ?? env.SOTY_GONKA_BASE_URL, env.GONKA_PRIMARY_API_KEY ?? env.SOTY_GONKA_API_KEY],
    ['joingonka', env.GONKA_FALLBACK_BASE_URL ?? env.SOTY_GONKA_FALLBACK_BASE_URL, env.GONKA_FALLBACK_API_KEY ?? env.SOTY_GONKA_FALLBACK_API_KEY],
  ];
  return definitions.filter(([, baseUrl, apiKey]) => baseUrl && apiKey).map(([id, baseUrl, apiKey]) => ({ id, baseUrl, apiKey }));
}

function normalizeProviders(providers, allowLocalHttp) {
  const ids = new Set();
  return providers.map((provider) => {
    const url = new URL(provider.baseUrl);
    if (url.username || url.password || url.search || url.hash || (url.protocol !== 'https:' && !(allowLocalHttp && url.protocol === 'http:' && ['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname)))) throw new Error('Invalid provider URL');
    if (!/^[a-z][a-z0-9_-]{0,63}$/.test(provider.id) || ids.has(provider.id)) throw new Error('Invalid provider identity');
    ids.add(provider.id);
    return { ...provider, baseUrl: url.href.replace(/\/$/, ''), apiKey: String(provider.apiKey) };
  });
}

function queue(limit = 8, maximum = 64, waitMs = 5000) {
  let active = 0;
  const waiting = [];
  const release = () => {
    active -= 1;
    const next = waiting.shift();
    if (next) { next.cleanup(); active += 1; next.resolve(release); }
  };
  return {
    acquire(signal) {
      if (signal?.aborted) return Promise.reject(safeError(null, signal));
      if (active < limit) { active += 1; return Promise.resolve(release); }
      if (waiting.length >= maximum) return Promise.reject(new ConnectorError('provider_queue_full', 503, { retryAfter: 1 }));
      return new Promise((resolve, reject) => {
        const entry = { resolve, cleanup: () => { clearTimeout(timer); signal?.removeEventListener('abort', abort); } };
        const fail = (error) => { const index = waiting.indexOf(entry); if (index >= 0) waiting.splice(index, 1); entry.cleanup(); reject(error); };
        const timer = setTimeout(() => fail(new ConnectorError('provider_queue_timeout', 503, { retryAfter: 1 })), waitMs);
        const abort = () => fail(safeError(null, signal));
        signal?.addEventListener('abort', abort, { once: true });
        waiting.push(entry);
      });
    },
    snapshot: () => ({ active, queued: waiting.length, limit }),
  };
}

export function createConnector(options = {}) {
  const env = options.env ?? process.env;
  const now = options.now ?? Date.now;
  const fetchImpl = options.fetchImpl ?? fetch;
  const providers = normalizeProviders(options.providers ?? providersFromEnv(env), options.allowLocalHttp ?? false);
  const store = options.store ?? {};
  const live = options.liveEnabled ?? env.CONNECTOR_LIVE_ENABLED === 'true';
  const strategy = options.strategy ?? env.CONNECTOR_ROUTING_STRATEGY ?? 'fallback';
  if (!['fallback', 'race'].includes(strategy)) throw new Error('Invalid routing strategy');
  const limits = {
    deadlineMs: 55_000, primaryMs: 24_000, firstUsefulMs: 15_000,
    streamIdleMs: 30_000, streamMaximumMs: 600_000, queueMs: 5000,
    ...options.timeouts,
  };
  const retail = {
    input: positiveMoney(env.GONKA_RETAIL_INPUT_URUB_PER_MILLION),
    output: positiveMoney(env.GONKA_RETAIL_OUTPUT_URUB_PER_MILLION),
    ...options.retail,
  };
  const catalog = createCatalog({ providers, store, fetchImpl, now, retail });
  const health = new Map();
  const providerBlocks = new Map();
  const queues = new Map(providers.map((provider) => [provider.id, queue(options.providerConcurrency ?? 8, 64, limits.queueMs)]));
  let worker;
  let metricWriteError = null;
  let qualificationLoad;

  async function initializeQualifications() {
    if (qualificationLoad) return qualificationLoad;
    qualificationLoad = (async () => {
      const entries = await store.loadQualifications?.() ?? [];
      if (!Array.isArray(entries)) throw new ConnectorError('qualification_store_invalid', 503);
      for (const entry of entries) {
        if (!providers.some((provider) => provider.id === entry.providerId) || typeof entry.model !== 'string' || entry.model.length > 240) continue;
        const key = `${entry.providerId}:${entry.model}`;
        const previous = health.get(key) ?? { failures: 0, recoveries: 0, cooldownStep: 0 };
        const verified = {};
        for (const field of ['jsonVerifiedAt', 'toolsVerifiedAt']) {
          const timestamp = entry[field];
          if (typeof timestamp === 'number' && Number.isFinite(timestamp) && timestamp > 0 && timestamp <= now() && now() - timestamp <= 86_400_000) verified[field] = Math.max(timestamp, previous[field] ?? 0);
        }
        if (Object.keys(verified).length) health.set(key, { ...previous, ...verified });
      }
    })().catch((error) => { qualificationLoad = undefined; throw error; });
    return qualificationLoad;
  }

  const qualificationFresh = (timestamp) => typeof timestamp === 'number' && timestamp > 0 && timestamp <= now() && now() - timestamp <= 86_400_000;

  function modelList() {
    return catalog.models().map((model) => {
      const routes = model.routes.map((route) => {
        const state = health.get(`${route.providerId}:${route.upstreamModel}`);
        const accountBlock = providerBlocks.get(route.providerId);
        const accountBlocked = accountBlock?.until > now();
        const fresh = state?.checkedAt && now() - state.checkedAt <= 60_000;
        const status = route.catalogState !== 'present' ? 'unavailable' : accountBlocked ? accountBlock.code === 'upstream_credentials_invalid' ? 'credential_error' : 'funding_required' : fresh ? state.status : 'unknown';
        const cooldown = Math.max(state?.cooldownUntil ?? 0, accountBlocked ? accountBlock.until : 0);
        const jsonQualified = qualificationFresh(state?.jsonVerifiedAt);
        const toolsQualified = qualificationFresh(state?.toolsVerifiedAt);
        return { ...route, status, capabilities: { ...route.capabilities, json: jsonQualified ? 'tested' : 'unknown', tools: toolsQualified ? 'tested' : 'unknown' }, jsonVerifiedAt: jsonQualified ? new Date(state.jsonVerifiedAt).toISOString() : null, toolsVerifiedAt: toolsQualified ? new Date(state.toolsVerifiedAt).toISOString() : null, checkedAt: state?.checkedAt ? new Date(state.checkedAt).toISOString() : null, lastSuccessAt: state?.lastSuccessAt ? new Date(state.lastSuccessAt).toISOString() : null, latencyMs: state?.latencyMs ?? null, errorCode: accountBlocked ? accountBlock.code : state?.errorCode ?? null, skippedReason: state?.skippedReason ?? null, cooldownUntil: cooldown ? new Date(cooldown).toISOString() : null };
      });
      const successful = routes.filter((route) => ['healthy', 'slow', 'recovering'].includes(route.status));
      const verified = routes.filter((route) => route.checkedAt).sort((a, b) => Date.parse(b.checkedAt) - Date.parse(a.checkedAt));
      return { ...model, capabilities: { ...model.capabilities, json: routes.some((route) => route.capabilities.json === 'tested') ? 'tested' : 'unknown', tools: routes.some((route) => route.capabilities.tools === 'tested') ? 'tested' : 'unknown' }, routes, status: successful.some((x) => x.status === 'healthy') ? 'healthy' : successful[0]?.status ?? (routes.some((r) => r.status === 'unknown') ? 'unknown' : 'unavailable'), checkedAt: verified[0]?.checkedAt ?? null, latencyMs: successful[0]?.latencyMs ?? null, catalogState: successful.length ? 'qualified_text' : 'discovered' };
    });
  }

  async function observe(event, callback) {
    const key = `${event.providerId}:${event.model}`;
    const previous = health.get(key) ?? { failures: 0, recoveries: 0, cooldownStep: 0 };
    if (event.outcome === 'success') {
      providerBlocks.delete(event.providerId);
      const intervalOk = !previous.lastSuccessAt || now() - previous.lastSuccessAt >= 10_000;
      const recovering = previous.failures >= 3 || previous.status === 'recovering';
      const recoveries = previous.recoveries + (intervalOk ? 1 : 0);
      health.set(key, { ...previous, status: recovering && recoveries < 2 ? 'recovering' : event.durationMs > 15_000 ? 'slow' : 'healthy', checkedAt: now(), lastSuccessAt: now(), latencyMs: event.durationMs, errorCode: null, skippedReason: null, cooldownUntil: 0, failures: recovering && recoveries < 2 ? previous.failures : 0, recoveries, cooldownStep: recovering && recoveries < 2 ? previous.cooldownStep : 0, ...(event.profile === 'json_object' ? { jsonVerifiedAt: now() } : {}), ...(event.profile === 'tools' && event.toolsVerified ? { toolsVerifiedAt: now() } : {}) });
    } else if (event.outcome === 'error' && !['upstream_invalid_request', 'provider_queue_full', 'provider_queue_timeout'].includes(event.errorCode)) {
      const failures = previous.failures + 1;
      const cooldownStep = failures >= 3 ? Math.min(previous.cooldownStep + 1, 4) : previous.cooldownStep;
      const accountFailure = ['upstream_credentials_invalid', 'upstream_funding_required'].includes(event.errorCode);
      // A 429 without Retry-After still needs a per-model pause, including its first occurrence.
      const defaultCooldownMs = accountFailure ? 300_000 : failures >= 3 ? [30_000, 60_000, 120_000, 300_000][cooldownStep - 1] : 0;
      const cooldownMs = event.retryAfterMs || (event.errorCode === 'upstream_rate_limited' ? Math.max(30_000, defaultCooldownMs) : defaultCooldownMs);
      if (accountFailure) providerBlocks.set(event.providerId, { until: now() + cooldownMs, code: event.errorCode });
      health.set(key, { ...previous, failures, recoveries: 0, cooldownStep, status: event.errorCode === 'upstream_credentials_invalid' ? 'credential_error' : event.errorCode === 'upstream_funding_required' ? 'funding_required' : event.errorCode === 'upstream_rate_limited' ? 'rate_limited' : 'unavailable', checkedAt: now(), errorCode: event.errorCode, cooldownUntil: now() + cooldownMs });
    }
    const callbacks = [store.saveObservation, options.onAttempt, callback].filter(Boolean);
    const results = await Promise.allSettled(callbacks.map((fn) => fn(event)));
    if (results.some((x) => x.status === 'rejected')) metricWriteError = 'observation_persistence_failed';
  }

  async function perform(provider, model, body, { signal, timeoutMs, purpose, logicalRequestId, attempts, callback, beforeDispatch }) {
    const started = now();
    const monotonicStart = performance.now();
    const controller = new AbortController();
    const abort = () => controller.abort(signal?.reason instanceof ConnectorError ? signal.reason : new ConnectorError('request_canceled', 499, { retryable: false }));
    if (signal?.aborted) abort();
    else signal?.addEventListener('abort', abort, { once: true });
    let timer = setTimeout(() => controller.abort(new ConnectorError('upstream_timeout', 504)), timeoutMs);
    let firstTimer;
    let release;
    let handedStream = false;
    let recorded = false;
    let ttftMs = null;
    let observedUsage = null;
    let dispatched = false;
    const advertisedPrice = catalog.models().find(entry => entry.id === model);
    let priceSnapshot = advertisedPrice ? { inputRate: advertisedPrice.inputPriceMicrorublesPerMillion, outputRate: advertisedPrice.outputPriceMicrorublesPerMillion } : null;
    const cleanup = () => { clearTimeout(timer); clearTimeout(firstTimer); signal?.removeEventListener('abort', abort); release?.(); release = undefined; };
    const record = async ({ outcome, usage, errorCode, retryAfterMs: retryMs, toolsVerified = false }) => {
      if (recorded) return;
      recorded = true;
      const event = { logicalRequestId, providerId: provider.id, model, purpose, outcome, dispatched, priceSnapshot, profile: body.tools?.length ? 'tools' : body.response_format?.type === 'json_object' ? 'json_object' : 'text', toolsVerified, startedAt: new Date(started).toISOString(), checkedAt: new Date(now()).toISOString(), durationMs: Math.round(performance.now() - monotonicStart), ttftMs, inputTokens: usage?.prompt_tokens ?? null, outputTokens: usage?.completion_tokens ?? null, errorCode: errorCode ?? null, retryAfterMs: retryMs ?? 0 };
      attempts.push(event);
      if (dispatched) await observe(event, callback);
      else {
        const writes = await Promise.allSettled([options.onAttempt, callback].filter(Boolean).map(fn => fn(event)));
        if (writes.some(result => result.status === 'rejected')) metricWriteError = 'observation_persistence_failed';
      }
    };
    try {
      release = await queues.get(provider.id).acquire(controller.signal);
      if (controller.signal.aborted) throw safeError(null, controller.signal);
      if (beforeDispatch) {
        try {
          const admission = await beforeDispatch({ providerId: provider.id, model, signal: controller.signal });
          if (admission?.priceSnapshot) priceSnapshot = admission.priceSnapshot;
        }
        catch (error) { throw new ConnectorError(/^[a-z][a-z0-9_]{0,79}$/.test(error?.code ?? '') ? error.code : 'dispatch_authorization_denied', Number(error.status) >= 400 && Number(error.status) <= 599 ? Number(error.status) : 403, { retryable: false }); }
      }
      if (controller.signal.aborted) throw safeError(null, controller.signal);
      if (body.stream) firstTimer = setTimeout(() => controller.abort(new ConnectorError('upstream_first_useful_timeout', 504)), Math.min(timeoutMs, limits.firstUsefulMs));
      dispatched = true;
      const response = await fetchImpl(`${provider.baseUrl}/chat/completions`, {
        method: 'POST', headers: { Authorization: `Bearer ${provider.apiKey}`, 'Content-Type': 'application/json', Accept: body.stream ? 'text/event-stream' : 'application/json' },
        body: JSON.stringify({ ...body, model }), signal: controller.signal, redirect: 'error',
      });
      if (!response.ok) {
        const retryMs = retryAfterMs(response.headers.get('retry-after'), now());
        await response.body?.cancel();
        const status = response.status;
        const code = status === 429 ? 'upstream_rate_limited' : status === 401 || status === 403 ? 'upstream_credentials_invalid' : status === 402 ? 'upstream_funding_required' : status === 404 ? 'upstream_model_unavailable' : status === 400 || status === 422 ? 'upstream_invalid_request' : 'upstream_unavailable';
        const error = new ConnectorError(code, code === 'upstream_invalid_request' ? 400 : status === 429 ? 429 : 502, { retryable: code !== 'upstream_invalid_request', retryAfter: Math.max(1, Math.ceil(retryMs / 1000)), providerStatus: status });
        error.retryAfterMs = retryMs;
        throw error;
      }
      if (body.stream) {
        if (!response.headers.get('content-type')?.toLowerCase().includes('text/event-stream')) { await response.body?.cancel(); throw new ConnectorError('upstream_stream_content_type'); }
        const result = await prepareStream(response, { ...body, model }, {
          controller, now, idleMs: limits.streamIdleMs,
          onFirstUseful: () => {
            ttftMs = Math.round(performance.now() - monotonicStart);
            clearTimeout(firstTimer);
          },
          onCommit: () => {
            clearTimeout(timer);
            timer = setTimeout(() => controller.abort(new ConnectorError('upstream_stream_timeout', 504)), Math.max(1, limits.streamMaximumMs - ttftMs));
          },
          onFinish: async (result) => { try { await record(result); } finally { cleanup(); } },
        });
        handedStream = true;
        return { kind: 'stream', stream: result.stream, completion: result.completion, cancel: result.cancel, providerId: provider.id, model, resolvedModel: model, attempts };
      }
      const parsedResult = await boundedJson(response);
      observedUsage = validUsage(parsedResult?.usage);
      const bodyResult = validateCompletion(parsedResult, { ...body, model });
      await record({ outcome: 'success', usage: validUsage(bodyResult.usage), toolsVerified: bodyResult.choices[0].message.tool_calls?.length > 0 });
      return { kind: 'json', body: bodyResult, providerId: provider.id, model, resolvedModel: model, usage: validUsage(bodyResult.usage), attempts };
    } catch (raw) {
      const error = safeError(raw, controller.signal);
      await record({ outcome: ['request_canceled', 'race_loser'].includes(error.code) ? 'canceled' : 'error', usage: raw?.safeUsage ?? observedUsage, errorCode: error.code, retryAfterMs: error.retryAfterMs });
      error.noUpstreamDispatch = attempts.every(attempt => !attempt.dispatched);
      throw error;
    } finally { if (!handedStream) cleanup(); }
  }

  function routesFor(model, { includeCooldown = false, body = {} } = {}) {
    const entry = catalog.models().find((x) => x.id === model);
    if (!entry) return [];
    const at = now();
    const recentLatency = (provider) => {
      const state = health.get(`${provider.id}:${model}`);
      if (!state || !['healthy', 'slow'].includes(state.status) || !state.lastSuccessAt
          || state.lastSuccessAt > at || at - state.lastSuccessAt > 60_000
          || !Number.isFinite(state.latencyMs) || state.latencyMs < 0) return null;
      if (body.tools?.length && !qualificationFresh(state.toolsVerifiedAt)) return null;
      if (body.response_format?.type === 'json_object' && !qualificationFresh(state.jsonVerifiedAt)) return null;
      return state.latencyMs;
    };
    return providers.filter((provider) => entry.routes.some((r) => r.providerId === provider.id && r.catalogState === 'present')).filter((provider) => {
      const state = health.get(`${provider.id}:${model}`);
      return includeCooldown || ((providerBlocks.get(provider.id)?.until ?? 0) <= now() && (!state?.cooldownUntil || state.cooldownUntil <= now()));
    }).sort((a, b) => {
      const left = recentLatency(a), right = recentLatency(b);
      if (left === null && right === null) return 0;
      if (left === null) return 1;
      if (right === null) return -1;
      return left - right;
    }).map((provider) => ({ provider, model }));
  }

  async function complete({ body, signal, logicalRequestId = crypto.randomUUID(), policy = {}, onAttempt, beforeDispatch, purpose = 'inference' }) {
    const started = now();
    if (!live || providers.length === 0) throw new ConnectorError('connector_not_configured', 503, { retryAfter: 10 });
    if (!body || !Array.isArray(body.messages) || body.messages.length === 0 || body.messages.length > 4096 || typeof body.model !== 'string' || (body.n !== undefined && body.n !== 1)) throw new ConnectorError('invalid_chat_request', 400, { retryable: false });
    if (Buffer.byteLength(JSON.stringify(body)) > 8 * 1024 * 1024) throw new ConnectorError('request_too_large', 413, { retryable: false });
    if (body.stream !== undefined && typeof body.stream !== 'boolean') throw new ConnectorError('invalid_stream_parameter', 400, { retryable: false });
    if (body.response_format?.type === 'json_schema') throw new ConnectorError('json_schema_not_supported', 400, { retryable: false });
    for (const field of ['max_tokens', 'max_completion_tokens']) if (body[field] !== undefined && (!Number.isSafeInteger(body[field]) || body[field] < 1 || body[field] > 1_048_576)) throw new ConnectorError('invalid_token_limit', 400, { retryable: false });
    await initializeQualifications();
    if (!catalog.models().length) await catalog.refresh();
    const advertised = catalog.models();
    const allowed = policy.allowedModels?.map((x) => resolveAlias(x, advertised));
    const automatic = body.model === 'soty/auto' || policy.autoFallback === true;
    const selected = body.model === 'soty/auto' ? (allowed?.[0] ?? advertised[0]?.id) : resolveAlias(body.model, advertised);
    if (!selected || !advertised.some((x) => x.id === selected)) throw new ConnectorError('model_not_found', 404, { retryable: false });
    if (allowed && !allowed.includes(selected)) throw new ConnectorError('model_not_allowed', 403, { retryable: false });
    let routes = routesFor(selected, { body });
    if (automatic) {
      const alternates = (allowed ?? advertised.map((x) => x.id)).filter((id) => id !== selected);
      const ceiling = policy.priceCeilingMicrorublesPerMillion ?? policy.maxPriceMicrorublesPerMillion;
      const eligible = (route) => {
        const state = health.get(`${route.provider.id}:${route.model}`);
        if (body.tools?.length && !qualificationFresh(state?.toolsVerifiedAt)) return false;
        if (body.response_format?.type === 'json_object' && !qualificationFresh(state?.jsonVerifiedAt)) return false;
        if (ceiling !== undefined && ceiling !== null) {
          const model = advertised.find((x) => x.id === route.model);
          if (!/^[0-9]+$/.test(String(ceiling)) || !model?.inputPriceMicrorublesPerMillion || !model?.outputPriceMicrorublesPerMillion) return false;
          if (BigInt(model.inputPriceMicrorublesPerMillion) > BigInt(ceiling) || BigInt(model.outputPriceMicrorublesPerMillion) > BigInt(ceiling)) return false;
        }
        return true;
      };
      const recentlyHealthy = (route) => {
        const state = health.get(`${route.provider.id}:${route.model}`);
        return state?.lastSuccessAt && now() - state.lastSuccessAt <= 60_000 && ['healthy', 'slow'].includes(state.status);
      };
      const alternativeRoutes = alternates.flatMap((id) => routesFor(id, { body })).filter(eligible);
      const bestAlternative = alternativeRoutes.find(recentlyHealthy) ?? alternativeRoutes[0];
      const healthySame = routes.slice(1).find((route) => recentlyHealthy(route) && eligible(route));
      const initial = routes[0];
      // Two attempts total. Unknown same-model backups must not consume the only
      // recovery slot when the caller explicitly requested cross-model recovery.
      const second = healthySame ?? bestAlternative ?? routes[1];
      routes = initial ? [initial, ...(second ? [second] : [])] : alternativeRoutes.slice(0, 2);
    }
    routes = routes.slice(0, 2);
    if (!routes.length) throw new ConnectorError('no_healthy_route', 503, { retryAfter: 10 });
    const attempts = [];
    const remaining = () => Math.max(1, limits.deadlineMs - (now() - started) - 2000);
    const routing = policy.strategy ?? strategy;
    if (!['fallback', 'race'].includes(routing)) throw new ConnectorError('invalid_routing_strategy', 400, { retryable: false });
    if (routing === 'race' && routes.length > 1) {
      const controllers = routes.map(() => new AbortController());
      const joinedSignals = controllers.map((controller) => signal ? AbortSignal.any([controller.signal, signal]) : controller.signal);
      const pending = routes.map(({ provider, model }, index) => perform(provider, model, body, { signal: joinedSignals[index], timeoutMs: remaining(), purpose, logicalRequestId, attempts, callback: onAttempt, beforeDispatch }).then((result) => ({ result, index })));
      let winner;
      try { winner = await Promise.any(pending); }
      catch (error) {
        const failure = error.errors?.find((e) => !e.retryable) ?? error.errors?.[0] ?? new ConnectorError('upstream_unavailable');
        failure.noUpstreamDispatch = attempts.every(attempt => !attempt.dispatched);
        throw failure;
      }
      controllers.forEach((controller, index) => { if (index !== winner.index) controller.abort(new ConnectorError('race_loser', 499, { retryable: false })); });
      await Promise.allSettled(pending.map(async (promise, index) => { if (index !== winner.index) { const losing = await promise; await losing.result.cancel?.(); } }));
      return winner.result;
    }
    let lastError;
    for (const [index, { provider, model }] of routes.entries()) {
      if (signal?.aborted) throw safeError(null, signal);
      if (remaining() < 500) break;
      try { return await perform(provider, model, body, { signal, timeoutMs: index === 0 && routes.length > 1 ? Math.min(limits.primaryMs, remaining()) : remaining(), purpose, logicalRequestId, attempts, callback: onAttempt, beforeDispatch }); }
      catch (error) { lastError = error; if (!error.retryable || error.code === 'request_canceled') throw error; }
    }
    throw lastError ?? new ConnectorError('request_deadline_exceeded', 504);
  }

  async function probe(providerId, model, options = {}) {
    const provider = providers.find((p) => p.id === providerId);
    if (!provider || !live) throw new ConnectorError('connector_not_configured', 503);
    if ((providerBlocks.get(providerId)?.until ?? 0) > now()) throw new ConnectorError('upstream_account_cooldown', 503);
    return perform(provider, model, { model, messages: [{ role: 'user', content: 'Return exactly the word OK.' }], max_tokens: options.maxTokens ?? 64, stream: false }, { signal: options.signal, timeoutMs: options.timeoutMs ?? 20_000, purpose: 'health_probe', logicalRequestId: crypto.randomUUID(), attempts: [] });
  }

  return {
    complete, probe, models: modelList, async refreshCatalog() { await initializeQualifications(); return live ? catalog.refresh().then(modelList) : catalog.load().then(modelList); },
    snapshot: () => ({ configured: live && providers.length > 0, strategy, metricWriteError, sources: catalog.sources(), providers: providers.map(({ id }) => ({ id, ...queues.get(id).snapshot() })), worker: worker?.snapshot() ?? { running: false } }),
    async startHealthWorker(workerOptions = {}) {
      await initializeQualifications();
      await catalog.load();
      if (live) await catalog.refresh();
      if (worker) return worker;
      worker = createHealthWorker({ models: modelList, refreshCatalog: () => catalog.refresh(), probe, store, now, health, live, enabled: env.HEALTH_PROBES_ENABLED === 'true', ...workerOptions });
      worker.start();
      return worker;
    },
    async stop() { await worker?.stop(); worker = undefined; },
  };
}

function positiveMoney(value) {
  return typeof value === 'string' && /^[1-9][0-9]*$/.test(value) ? value : null;
}
