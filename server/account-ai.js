import { createHash, randomUUID } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import path from 'node:path';
import { types } from 'node:util';
import { isAccountAiConfiguration, GATEWAY_AI_ISSUER } from './account-ai-configuration.js';
import { isConnectorModelAuthorization } from './connector-model-authorization.js';

const services = new WeakSet();
const id = value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{1,200}$/u.test(value);
const accountIdValid = value => typeof value === 'string' && /^(?:acct|svc)_[A-Za-z0-9_-]{32}$/u.test(value);
const digest = value => createHash('sha256').update(value).digest('hex');
const exact = (value, required, optional = []) => value && typeof value === 'object' && !Array.isArray(value)
  && required.every(key => Object.hasOwn(value, key)) && Object.keys(value).every(key => [...required, ...optional].includes(key));
export const aiError = (code = 'ai_unavailable', status = 503) => Object.assign(new Error(code), { code, status });
const need = (value, code = 'ai_invalid_request', status = 400) => { if (!value) throw aiError(code, status); };
export const isAccountAiService = value => services.has(value);
const AI_OPERATIONS = new Set(['ai.account.get', 'ai.keys.list', 'ai.keys.create', 'ai.keys.update', 'ai.keys.rotate', 'ai.keys.revoke']);
const safeGatewayErrors = new Map([
  ['idempotency_conflict', 'ai_request_conflict'], ['intent_conflict', 'ai_request_conflict'], ['operation_conflict', 'ai_request_conflict'],
  ['key_not_found', 'ai_key_unavailable'], ['not_found', 'ai_key_unavailable'], ['invalid_name', 'ai_invalid_request'],
  ['invalid_limits', 'ai_invalid_request'], ['invalid_models', 'ai_invalid_request'], ['invalid_expiry', 'ai_invalid_request'],
  ['policy_revision_conflict', 'ai_key_changed'],
]);
const limitNames = ['dailyMicrorubles', 'monthlyMicrorubles', 'totalMicrorubles', 'maxRequestMicrorubles', 'rpm', 'tpm', 'concurrency', 'dailyTokens', 'monthlyTokens', 'maxOutputTokens'];
const legacyModels = ['deepseek-ai/DeepSeek-V4-Flash-0731', 'MiniMaxAI/MiniMax-M2.7', 'zai-org/GLM-5.3-Flash'];
function limitsView(value) {
  need(value && typeof value === 'object' && !Array.isArray(value), 'ai_unavailable', 503);
  return Object.fromEntries(limitNames.filter(key => Object.hasOwn(value, key)).map(key => {
    const item = value[key]; need(item === null || Number.isSafeInteger(item) && item >= 0
      || typeof item === 'string' && /^\d{1,30}$/u.test(item), 'ai_unavailable', 503); return [key, item];
  }));
}
function keyView(value, internal = false) {
  need(value && typeof value === 'object' && typeof value.id === 'string' && value.id.length <= 200, 'ai_unavailable', 503);
  const fields = internal ? ['id', 'name', 'status', 'createdAt'] : ['id', 'name', 'prefix', 'status', 'spentMicrorubles', 'tokens', 'createdAt', 'lastUsedAt', 'expiresAt', 'allowedModels', 'autoFallback', 'legacy', 'purpose', 'policyRevision'];
  const result = Object.fromEntries(fields.filter(key => Object.hasOwn(value, key)).map(key => [key, value[key]]));
  if (!internal) result.limits = limitsView(value.limits ?? {});
  if (result.allowedModels !== undefined && result.allowedModels !== null) need(Array.isArray(result.allowedModels)
    && result.allowedModels.length <= 100 && result.allowedModels.every(model => typeof model === 'string' && model.length <= 250), 'ai_unavailable', 503);
  return result;
}

async function readJson(response, maximum = 131072) {
  need(response.body, 'ai_unavailable', 503); const reader = response.body.getReader(), chunks = []; let size = 0;
  try {
    for (;;) { const { done, value } = await reader.read(); if (done) break;
      size += value.byteLength; need(size <= maximum, 'ai_unavailable', 503); chunks.push(Buffer.from(value)); }
    return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks)));
  } catch { throw aiError(); }
  finally { try { await reader.cancel(); } catch {} reader.releaseLock(); }
}
function accountView(value, expectedAccountId) {
  need(exact(value, ['schema', 'accountId', 'state', 'defaultKey', 'allowance', 'usage', 'models', 'updatedAt'])
    && value.schema === 'soty.ai-account.v1' && value.accountId === expectedAccountId
    && ['provisioning', 'ready', 'limited', 'suspended', 'unavailable'].includes(value.state), 'ai_unavailable', 503);
  need(exact(value.allowance, ['kind', 'unit', 'grantedMicrorubles', 'availableMicrorubles', 'heldMicrorubles', 'spentMicrorubles', 'pendingMicrorubles', 'limits'])
    && value.allowance.kind === 'included' && value.allowance.unit === 'microrubles'
    && ['grantedMicrorubles', 'availableMicrorubles', 'heldMicrorubles', 'spentMicrorubles', 'pendingMicrorubles']
      .every(key => typeof value.allowance[key] === 'string' && /^\d{1,30}$/u.test(value.allowance[key]))
    && Array.isArray(value.models) && value.models.length <= 100 && value.usage && typeof value.usage === 'object', 'ai_unavailable', 503);
  // Project a closed DTO: gateway diagnostics can never become a browser field.
  const usage = {};
  for (const key of ['requests', 'tokens', 'inputTokens', 'outputTokens', 'pendingTokens', 'pendingRequests']) if (Object.hasOwn(value.usage, key)) {
    const item = value.usage[key]; need(Number.isSafeInteger(item) && item >= 0 || typeof item === 'string' && /^\d{1,30}$/u.test(item), 'ai_unavailable', 503); usage[key] = item;
  }
  const models = value.models.map(model => {
    if (typeof model === 'string') return model;
    need(model && typeof model.id === 'string', 'ai_unavailable', 503);
    return Object.fromEntries(['id', 'name', 'label', 'provider', 'contextWindow', 'maxOutputTokens', 'status', 'capabilities']
      .filter(key => Object.hasOwn(model, key)).map(key => [key, model[key]]));
  });
  return { schema: value.schema, accountId: expectedAccountId, state: value.state, defaultKey: value.defaultKey === null ? null : keyView(value.defaultKey, true),
    allowance: { ...value.allowance, limits: limitsView(value.allowance.limits) }, usage, models, updatedAt: value.updatedAt };
}

/** One host-owned bridge for every AI ingress. Account identity is captured from
 * original Connect/capability authority, never an incoming account selector. */
export function createAccountAiService({ configuration, databasePath, getConnect, fetchImpl = fetch, clock = Date.now,
  reconcileMs = 60000, requestTimeoutMs = 10000, dispatchLeaseMs = 600000, maxAuthorizations = 256 } = {}) {
  need(isAccountAiConfiguration(configuration) && typeof getConnect === 'function' && typeof fetchImpl === 'function', 'ai_configuration_invalid', 503);
  need(Number.isSafeInteger(reconcileMs) && reconcileMs >= 20 && reconcileMs <= 3600000
    && Number.isSafeInteger(requestTimeoutMs) && requestTimeoutMs > 0 && requestTimeoutMs <= 30000
    && Number.isSafeInteger(dispatchLeaseMs) && dispatchLeaseMs > 0 && dispatchLeaseMs <= 600000
    && Number.isSafeInteger(maxAuthorizations) && maxAuthorizations >= 1 && maxAuthorizations <= 4096, 'ai_configuration_invalid', 503);
  need(typeof databasePath === 'string' && databasePath.length > 0, 'ai_configuration_invalid', 503);
  if (databasePath !== ':memory:') mkdirSync(path.dirname(path.resolve(databasePath)), { recursive: true, mode: 0o700 });
  const db = new DatabaseSync(databasePath);
  db.exec('PRAGMA journal_mode=WAL; PRAGMA busy_timeout=1000; CREATE TABLE IF NOT EXISTS callback_nonces (issuer TEXT NOT NULL,kid TEXT NOT NULL,jti TEXT NOT NULL,expires_at INTEGER NOT NULL,PRIMARY KEY(issuer,kid,jti)); CREATE INDEX IF NOT EXISTS callback_nonce_expiry ON callback_nonces(expires_at)');
  let closed = false, started = false, scanning = null, timer, unsubscribeAccounts, unsubscribeRevoke;
  const stopping = new AbortController(), pending = new Map(), cached = new Map(), authorizations = new Map();
  const signer = configuration.signer();
  const verifier = configuration.verifier(proof => {
    if (closed) return false;
    try { db.prepare('DELETE FROM callback_nonces WHERE expires_at<=?').run(Math.floor(clock() / 1000));
      return db.prepare('INSERT OR IGNORE INTO callback_nonces(issuer,kid,jti,expires_at) VALUES (?,?,?,?)')
        .run(proof.iss, proof.kid, proof.jti, proof.exp).changes === 1; } catch { return false; }
  });
  const alive = () => need(!closed, 'ai_unavailable', 503);
  const currentActor = actor => {
    alive();
    try { return getConnect().withActorAuthorityFence(actor, () => true); }
    catch { throw aiError('ai_context_changed', 403); }
  };
  const existingAccount = accountId => { alive(); need(accountIdValid(accountId)
    && (accountId.startsWith('svc_') ? configuration.isSponsorAccount(accountId) : getConnect()?.hasAccount(accountId)), 'ai_context_changed', 403); };
  function remember(account) {
    cached.delete(account.accountId); cached.set(account.accountId, { account, at: clock() });
    while (cached.size > 2048) cached.delete(cached.keys().next().value);
  }
  async function wire({ method = 'GET', route, operation, accountId, deviceId, requestId = randomUUID(), authorizationId, body = '', signal }) {
    alive(); const combined = AbortSignal.any([stopping.signal, ...(signal ? [signal] : [])]);
    need(!combined.aborted, 'ai_context_changed', 403);
    return fetchImpl(configuration.gatewayOrigin + route, { method, redirect: 'error', signal: combined,
      headers: { ...signer.headers({ method, path: route, body, operation, accountId, deviceId, requestId, authorizationId }),
        accept: 'application/json', ...(body ? { 'content-type': 'application/json' } : {}), 'idempotency-key': requestId },
      ...(body ? { body } : {}) });
  }
  async function jsonWire(request, guard = alive, mutation = false) {
    guard(); let response;
    try {
      response = await wire({ ...request, signal: AbortSignal.any([AbortSignal.timeout(requestTimeoutMs), ...(request.signal ? [request.signal] : [])]) }); guard();
      const value = await readJson(response); guard();
      if (!response.ok) { const code = value?.error?.code ?? value?.error?.type ?? value?.code;
        throw aiError(safeGatewayErrors.get(code) ?? (response.status === 409 ? 'ai_request_conflict' : response.status === 400 ? 'ai_invalid_request'
          : mutation && response.status >= 500 ? 'ai_operation_unknown' : 'ai_unavailable'), response.status); }
      return value;
    } catch (error) {
      try { await response?.body?.cancel(); } catch {}
      guard();
      if (typeof error?.code === 'string' && error.code.startsWith('ai_') && !(mutation && response?.ok && error.code === 'ai_unavailable')) throw error;
      throw aiError(mutation ? 'ai_operation_unknown' : 'ai_unavailable');
    }
  }
  async function ensure(accountId) {
    existingAccount(accountId); if (pending.has(accountId)) return pending.get(accountId);
    need(pending.size < 256, 'ai_capacity', 429);
    const work = (async () => {
      try {
        const value = await jsonWire({ method: 'POST', route: '/internal/soty/v1/accounts/ensure', operation: 'account.ensure',
          accountId, deviceId: 'service:provisioner', requestId: 'ensure-' + digest(configuration.issuer + '\0' + accountId), body: '{}' }, () => existingAccount(accountId));
        const account = accountView(value, accountId); remember(account); return account;
      } catch (error) { cached.delete(accountId); throw error; }
      finally { pending.delete(accountId); }
    })();
    pending.set(accountId, work); return work;
  }
  function capture({ actor, assertCurrent = () => {} }) {
    need(typeof assertCurrent === 'function' && !types.isAsyncFunction(assertCurrent) && !types.isProxy(assertCurrent), 'ai_context_changed', 403);
    currentActor(actor); const accountId = actor.accountId, deviceId = actor.deviceId;
    return bound({ accountId, deviceId, guard: () => { currentActor(actor); return assertCurrent(); },
      subscribe: listener => getConnect().subscribeRevocations(event => { if (event.deviceId === deviceId) listener(); }) });
  }
  function bound({ accountId, deviceId, guard: suppliedGuard, subscribe, models }) {
    need(accountIdValid(accountId) && id(deviceId) && typeof suppliedGuard === 'function' && !types.isAsyncFunction(suppliedGuard)
      && !types.isProxy(suppliedGuard), 'ai_context_changed', 403);
    let invalidated = false;
    const guard = () => { alive(); need(!invalidated, 'ai_context_changed', 403);
      try { const result = suppliedGuard();
        if (result && typeof result.then === 'function') { void Promise.resolve(result).catch(() => {}); throw aiError('ai_context_changed', 403); }
        need(result !== false, 'ai_context_changed', 403); }
      catch { invalidated = true; throw aiError('ai_context_changed', 403); } };
    return Object.freeze({ accountId, deviceId, guard,
      async complete({ requestId, body, signal }) {
        guard(); need(id(requestId) && body && typeof body === 'object' && signal instanceof AbortSignal);
        need(!models || models.includes(body.model), 'ai_model_denied', 403);
        await ensure(accountId); guard(); need(!signal.aborted, 'ai_context_changed', 403);
        for (const [key, entry] of authorizations) if (entry.expiresAt <= clock()) entry.finish();
        need(authorizations.size < maxAuthorizations, 'ai_capacity', 429);
        const encoded = JSON.stringify(body), bodySha256 = digest(encoded), authorizationId = randomUUID();
        const controller = new AbortController(), combined = AbortSignal.any([signal, stopping.signal, controller.signal, AbortSignal.timeout(dispatchLeaseMs)]);
        let finished = false, removeRevocation;
        const entry = { accountId, deviceId, requestId, bodySha256, models: body.autoFallback === true ? (models ?? legacyModels) : [body.model], expiresAt: clock() + dispatchLeaseMs,
          current() { if (finished || combined.aborted || clock() >= entry.expiresAt) return false;
            try { guard(); return true; } catch { entry.finish(); return false; } },
          finish() { if (finished) return; finished = true; authorizations.delete(authorizationId); combined.removeEventListener('abort', entry.finish);
            removeRevocation?.(); controller.abort(); },
        };
        authorizations.set(authorizationId, entry);
        combined.addEventListener('abort', entry.finish, { once: true });
        removeRevocation = subscribe?.(() => { invalidated = true; entry.finish(); });
        if (!entry.current()) { entry.finish(); throw aiError('ai_context_changed', 403); }
        let response;
        try {
          response = await wire({ method: 'POST', route: '/internal/soty/v1/chat/completions', operation: 'chat.complete', accountId,
            deviceId, requestId, authorizationId, body: encoded, signal: combined });
          guard(); need(entry.current(), 'ai_context_changed', 403);
          if (!response.body) { entry.finish(); return response; }
          const reader = response.body.getReader();
          const stream = new ReadableStream({
            async pull(output) {
              try { guard(); need(entry.current(), 'ai_context_changed', 403);
                const chunk = await reader.read(); guard(); need(entry.current(), 'ai_context_changed', 403);
                if (chunk.done) { output.close(); entry.finish(); } else output.enqueue(chunk.value);
              } catch (error) { entry.finish(); try { await reader.cancel(); } catch {} output.error(aiError('ai_context_changed', 403)); }
            },
            async cancel() { entry.finish(); try { await reader.cancel(); } catch {} },
          });
          return new Response(stream, { status: response.status, statusText: response.statusText, headers: response.headers });
        } catch (error) {
          entry.finish(); try { await response?.body?.cancel(); } catch {}
          guard(); throw error?.code?.startsWith('ai_') ? error : aiError('ai_inference_unknown');
        }
      },
      async readReceipt({ requestId, logicalRequestId, signal }) {
        guard(); need(typeof requestId === 'string' && /^[0-9a-f-]{36}$/iu.test(requestId));
        const value = await jsonWire({ route: '/internal/soty/v1/requests/' + requestId, operation: 'request.receipt', accountId, deviceId, signal }, guard);
        need(value?.request?.id === requestId && (logicalRequestId === undefined || value.request.logicalRequestId === logicalRequestId), 'ai_receipt_unknown', 503);
        const receipt = { ...value.request };
        for (const key of ['input_tokens', 'output_tokens']) if (typeof receipt[key] === 'string') {
          need(/^\d{1,15}$/u.test(receipt[key]) && Number.isSafeInteger(Number(receipt[key])), 'ai_receipt_unknown', 503); receipt[key] = Number(receipt[key]); }
        return { tenantId: accountId, request: receipt };
      },
    });
  }
  async function status(actor) {
    currentActor(actor); await ensure(actor.accountId); currentActor(actor);
    const account = accountView(await jsonWire({ route: '/internal/soty/v1/account', operation: 'account.status',
      accountId: actor.accountId, deviceId: actor.deviceId }, () => currentActor(actor)), actor.accountId);
    remember(account); return account;
  }
  async function reconcile() {
    if (closed || scanning) return scanning;
    scanning = (async () => {
      await Promise.allSettled(configuration.sponsorAccountIds().map(ensure));
      let after = '';
      do { const ids = getConnect().listAccountIds({ after, limit: 100 }); if (!ids.length) break;
        for (let offset = 0; offset < ids.length && !closed; offset += 4) await Promise.allSettled(ids.slice(offset, offset + 4).map(ensure));
        after = ids.at(-1);
      } while (!closed);
    })().catch(() => {}).finally(() => { scanning = null; }); return scanning;
  }
  const service = Object.freeze({ configured: true, operations: AI_OPERATIONS, capture,
    eligible(actor) {
      currentActor(actor); const status = cached.get(actor.accountId)?.account;
      return !status || ['provisioning', 'ready', 'unavailable'].includes(status.state);
    },
    captureConnector(authorization) {
      need(isConnectorModelAuthorization(authorization), 'ai_billing_identity_required', 403);
      const owner = authorization.owner;
      if (owner) return bound({ accountId: owner.accountId, deviceId: owner.deviceId,
        guard: authorization.assertCurrent, subscribe: authorization.subscribe });
      const sponsor = configuration.legacyConnectorFor(authorization.connectorId);
      need(sponsor, 'ai_billing_identity_required', 403); existingAccount(sponsor.accountId);
      return bound({ accountId: sponsor.accountId, deviceId: 'service:connector:' + authorization.connectorId,
        guard: authorization.assertCurrent, subscribe: authorization.subscribe, models: sponsor.models });
    },
    captureApplication(appId, assertCurrent) {
      const sponsor = configuration.applicationFor(appId); need(sponsor && typeof assertCurrent === 'function', 'ai_billing_identity_required', 403);
      existingAccount(sponsor.accountId);
      return bound({ accountId: sponsor.accountId, deviceId: 'service:application:' + appId, guard: assertCurrent, models: sponsor.models });
    },
    ensure, reconcile,
    start() {
      alive(); if (started) return; started = true;
      unsubscribeAccounts = getConnect().subscribeAccountCreated(() => { void reconcile(); });
      unsubscribeRevoke = getConnect().subscribeRevocations(event => {
        for (const entry of [...authorizations.values()]) if (entry.deviceId === event.deviceId) entry.finish();
      });
      timer = setInterval(() => { void reconcile(); }, reconcileMs); timer.unref?.(); void reconcile();
    },
    async executeAsync({ op, actor, args = {} }) {
      currentActor(actor); need(AI_OPERATIONS.has(op));
      const mutation = !['ai.account.get', 'ai.keys.list'].includes(op);
      const required = mutation ? ['expectedAccountId', 'operationId'] : ['expectedAccountId'];
      const optional = op.endsWith('.create') ? ['name', 'limits', 'allowedModels', 'expiresAt', 'autoFallback']
        : op.endsWith('.update') ? ['keyId', 'name', 'limits', 'allowedModels', 'expiresAt', 'autoFallback', 'status', 'expectedPolicyRevision']
          : op.endsWith('.rotate') || op.endsWith('.revoke') ? ['keyId'] : [];
      need(exact(args, required, optional)); need(args.expectedAccountId === actor.accountId, 'ai_context_changed', 403);
      if (mutation) need(id(args.operationId));
      const guard = () => { currentActor(actor); need(args.expectedAccountId === actor.accountId, 'ai_context_changed', 403); };
      if (op === 'ai.account.get') {
        try { return await status(actor); }
        catch (error) { guard(); throw error; }
      }
      await ensure(actor.accountId); guard();
      const action = op.slice('ai.'.length), payload = Object.fromEntries(Object.entries(args).filter(([key]) => !['expectedAccountId', 'operationId'].includes(key)));
      if (op.endsWith('.create')) need(typeof payload.name === 'string' && payload.name.trim().length > 0 && payload.name.length <= 100);
      if (op.endsWith('.rotate') || op.endsWith('.revoke') || op.endsWith('.update')) need(id(payload.keyId));
      if (payload.expectedPolicyRevision !== undefined) need(Number.isSafeInteger(payload.expectedPolicyRevision) && payload.expectedPolicyRevision >= 1);
      const value = await jsonWire({ method: mutation ? 'POST' : 'GET', route: '/internal/soty/v1/keys' + (mutation ? '/' + action.split('.')[1] : ''),
        operation: action, accountId: actor.accountId, deviceId: actor.deviceId, requestId: args.operationId ?? randomUUID(), body: mutation ? JSON.stringify(payload) : '' }, guard, mutation);
      if (!mutation) { need(Array.isArray(value?.keys) && value.keys.length <= 100, 'ai_unavailable', 503); return { keys: value.keys.map(key => keyView(key)) }; }
      need(value?.key && typeof value.key === 'object', 'ai_operation_unknown', 503);
      return { key: keyView(value.key), ...(['ai.keys.create', 'ai.keys.rotate'].includes(op) ? { secret: typeof value.secret === 'string' ? value.secret : null } : {}),
        ...(value.replayed === undefined ? {} : { replayed: value.replayed === true }) };
    },
    async authorizeDispatch({ method, route, body, headers }) {
      alive(); const proof = await verifier.verify({ method, path: route, body, headers });
      need(proof.iss === GATEWAY_AI_ISSUER && proof.operation === 'dispatch.authorize'
        && route === '/internal/soty-ai/v1/authorize-dispatch' && method === 'POST', 'ai_dispatch_denied', 403);
      let value; try { value = JSON.parse(body); } catch { throw aiError('ai_dispatch_denied', 403); }
      need(exact(value, ['issuer', 'accountId', 'deviceId', 'authorizationId', 'requestId', 'gatewayRequestId', 'bodySha256', 'providerId', 'model'])
        && value.issuer === configuration.issuer && value.accountId === proof.accountId && value.deviceId === proof.deviceId
        && value.authorizationId === proof.authorizationId && value.requestId === proof.requestId
        && typeof value.gatewayRequestId === 'string' && /^[0-9a-f-]{36}$/iu.test(value.gatewayRequestId)
        && typeof value.providerId === 'string' && value.providerId.length > 0 && value.providerId.length <= 160, 'ai_dispatch_denied', 403);
      const entry = authorizations.get(value.authorizationId);
      need(entry && entry.accountId === value.accountId && entry.deviceId === value.deviceId && entry.requestId === value.requestId
        && entry.bodySha256 === value.bodySha256 && entry.models.includes(value.model) && entry.current(), 'ai_dispatch_denied', 403);
      return { authorized: true };
    },
    async close() {
      if (closed) return; closed = true; stopping.abort(); clearInterval(timer); unsubscribeAccounts?.(); unsubscribeRevoke?.();
      for (const entry of [...authorizations.values()]) entry.finish();
      await Promise.allSettled([...pending.values(), ...(scanning ? [scanning] : [])]); cached.clear(); db.close();
    },
  });
  services.add(service); return service;
}
