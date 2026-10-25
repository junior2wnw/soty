import { randomUUID, randomBytes, createCipheriv, createDecipheriv, hkdfSync } from 'node:crypto';
import {
  DomainError, money, createSecret, fingerprint, canonicalJson, publicDigest, bodyDigest,
  normalizeLimits, normalizeModels, validateName, validateExpiry, DEFAULT_LIMITS,
  enforceLimit, periodDate, chargeTokens, normalizeUsage,
} from '../../../packages/domain/index.mjs';

const MAX_KEYS_PER_WORKSPACE = 100;
const DEMO_TOPUP_MAX = 10000_000000n;

function keyView(row) {
  return {
    id: row.id, name: row.name, prefix: `${row.prefix}…${row.last_four}`,
    status: row.status !== 'revoked' && row.expires_at && new Date(row.expires_at) <= new Date() ? 'expired' : row.status,
    spentMicrorubles: String(row.spent_urub ?? '0'), tokens: Number(row.tokens ?? 0),
    createdAt: row.created_at, lastUsedAt: row.last_used_at, expiresAt: row.expires_at,
    limits: row.limits, allowedModels: row.allowed_models, autoFallback: row.auto_fallback,
    legacy: row.legacy, purpose: row.purpose, policyRevision: row.policy_revision,
  };
}

function assertActive(row) {
  if (!row || row.status !== 'active' || row.revoked_at || (row.expires_at && new Date(row.expires_at) <= new Date()) || (row.credential_expires_at && new Date(row.credential_expires_at) <= new Date())) {
    throw new DomainError('invalid_api_key', 'Ключ недействителен, приостановлен или истёк.', 401);
  }
}

export class Store {
  constructor(pool, { pepper, dailyServiceBudgetMicrorubles = '50000000', dailyProbeBudgetMicrorubles = '10000000', responseCacheEnabled = true, clock = () => new Date() } = {}) {
    fingerprint('configuration-check', pepper);
    this.pool = pool; this.pepper = pepper; this.dailyServiceBudget = money(dailyServiceBudgetMicrorubles);
    const probeBudget = money(dailyProbeBudgetMicrorubles);
    this.dailyProbeBudget = probeBudget < this.dailyServiceBudget ? probeBudget : this.dailyServiceBudget;
    this.responseCacheEnabled = responseCacheEnabled; this.clock = clock;
    this.cacheKey = Buffer.from(hkdfSync('sha256', pepper, 'soty-cache-v1', 'encrypted-idempotency-results', 32));
  }

  async transaction(callback) {
    for (let attempt = 0; ; attempt++) {
      const client = await this.pool.connect();
      try {
        await client.query('BEGIN');
        await client.query("SET LOCAL statement_timeout = '10000ms'");
        const result = await callback(client);
        await client.query('COMMIT');
        return result;
      } catch (error) {
        await client.query('ROLLBACK');
        if (['40P01', '40001'].includes(error.code) && attempt < 2) continue;
        throw error;
      } finally { client.release(); }
    }
  }

  async audit(client, tenantId, type, resourceId, data = {}) {
    const id = randomUUID();
    await client.query('INSERT INTO audit_events(id,tenant_id,type,resource_id,data) VALUES($1,$2,$3,$4,$5)', [id, tenantId, type, resourceId, data]);
    await client.query('INSERT INTO outbox(id,tenant_id,type,data) VALUES($1,$2,$3,$4)', [id, tenantId, type, { resourceId, ...data }]);
  }

  async createSession() {
    const secret = createSecret(''); const tenantId = randomUUID();
    const expiresAt = new Date(this.clock().valueOf() + 30 * 86400_000).toISOString();
    await this.transaction(async client => {
      await client.query("SELECT pg_advisory_xact_lock(hashtext('anonymous-session-admission'))");
      const recent = (await client.query("SELECT count(*) FROM tenants WHERE kind='anonymous' AND created_at>now()-interval '1 day'")).rows[0];
      if (Number(recent.count) >= 5000) throw new DomainError('workspace_capacity', 'Сегодня создано слишком много временных кабинетов. Попробуйте позже.', 429);
      await client.query("INSERT INTO tenants(id,name,kind) VALUES($1,'Временный кабинет','anonymous')", [tenantId]);
      await client.query('INSERT INTO wallets(tenant_id) VALUES($1)', [tenantId]);
      await client.query('INSERT INTO device_sessions(fingerprint,tenant_id,expires_at) VALUES($1,$2,$3)', [fingerprint(secret, this.pepper), tenantId, expiresAt]);
      await this.audit(client, tenantId, 'workspace.created', tenantId, { temporary: true });
    });
    return { secret, tenantId, expiresAt };
  }

  async getSession(secret) {
    if (!secret || secret.length > 200) return null;
    const result = await this.pool.query('SELECT tenant_id,expires_at FROM device_sessions WHERE fingerprint=$1 AND expires_at>now()', [fingerprint(secret, this.pepper)]);
    return result.rows[0] ? { tenantId: result.rows[0].tenant_id, expiresAt: result.rows[0].expires_at } : null;
  }

  async findKeyBySecret(client, secret) {
    if (typeof secret !== 'string' || secret.length < 10 || secret.length > 1024) return null;
    const { rows } = await client.query(`SELECT k.*,c.id AS credential_id,c.revoked_at,c.expires_at AS credential_expires_at
      FROM credentials c JOIN api_keys k ON k.id=c.key_id AND k.tenant_id=c.tenant_id
      WHERE (c.fingerprint=$1 AND c.fingerprint_algorithm='hmac_sha256')
      OR (k.legacy=true AND c.fingerprint=$2 AND c.fingerprint_algorithm='legacy_sha256')`, [fingerprint(secret, this.pepper), publicDigest(secret)]);
    const row = rows[0];
    return row ? { ...row, tenantId: row.tenant_id, keyId: row.id, expiresAt: row.expires_at } : null;
  }

  async authenticate(secret) {
    const row = await this.findKeyBySecret(this.pool, secret); assertActive(row); return row;
  }

  async createKey(client, tenantId, input, { pendingActivation = false } = {}) {
    await client.query('SELECT tenant_id FROM wallets WHERE tenant_id=$1 FOR UPDATE', [tenantId]);
    const name = validateName(input.name); const limits = normalizeLimits(input.limits);
    const allowedModels = normalizeModels(input.allowedModels);
    const expiresAt = pendingActivation ? new Date(this.clock().valueOf() + 600_000).toISOString() : validateExpiry(input.expiresAt);
    const count = await client.query("SELECT count(*) FROM api_keys WHERE tenant_id=$1 AND status<>'revoked' AND purpose='customer'", [tenantId]);
    if (Number(count.rows[0].count) >= MAX_KEYS_PER_WORKSPACE) throw new DomainError('key_limit', 'В кабинете уже 100 действующих ключей.', 409);
    const secret = createSecret(); const id = randomUUID();
    const result = await client.query(`INSERT INTO api_keys(id,tenant_id,name,prefix,last_four,status,limits,allowed_models,auto_fallback,expires_at)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *`, [id, tenantId, name, secret.slice(0, 17), secret.slice(-4), pendingActivation ? 'pending_activation' : 'active', limits, allowedModels === null ? null : JSON.stringify(allowedModels), input.autoFallback === true, expiresAt]);
    await client.query('INSERT INTO credentials(id,tenant_id,key_id,fingerprint) VALUES($1,$2,$3,$4)', [randomUUID(), tenantId, id, fingerprint(secret, this.pepper)]);
    await client.query('INSERT INTO policy_history(key_id,revision,policy) VALUES($1,1,$2)', [id, { limits, allowedModels, autoFallback: input.autoFallback === true }]);
    await this.audit(client, tenantId, 'key.created', id, { pendingActivation });
    return { key: keyView(result.rows[0]), secret };
  }

  async activateKey(client, keyId) {
    const result = await client.query("UPDATE api_keys SET status='active',expires_at=NULL,updated_at=now() WHERE id=$1 AND status='pending_activation' AND expires_at>now() RETURNING *", [keyId]);
    if (!result.rowCount) throw new DomainError('activation_expired', 'Ключ уже активирован или срок активации истёк.', 409);
    await this.audit(client, result.rows[0].tenant_id, 'key.activated', keyId);
    return keyView(result.rows[0]);
  }

  async createKeyOperation(tenantId, input, idempotencyKey) {
    return this.transaction(async client => {
      await client.query('SELECT tenant_id FROM wallets WHERE tenant_id=$1 FOR UPDATE', [tenantId]);
      const operation = await this.claimOperation(client, tenantId, 'key.create', idempotencyKey, input);
      if (operation.existing) return { ...operation.existing, secret: null, replayed: true };
      const created = await this.createKey(client, tenantId, input);
      await this.saveOperation(client, tenantId, 'key.create', operation.digest, created.key.id, { key: created.key });
      return created;
    });
  }

  async claimOperation(client, tenantId, operation, idempotencyKey, body) {
    if (typeof idempotencyKey !== 'string' || idempotencyKey.length < 8 || idempotencyKey.length > 200) throw new DomainError('idempotency_required', 'Нужен заголовок Idempotency-Key длиной 8–200 символов.');
    const digest = fingerprint(idempotencyKey, this.pepper); const contentDigest = bodyDigest(body, this.pepper);
    const current = await client.query('SELECT * FROM idempotency_operations WHERE tenant_id=$1 AND operation=$2 AND digest=$3 FOR UPDATE', [tenantId, operation, digest]);
    if (current.rows[0]) {
      if (current.rows[0].body_digest !== contentDigest) throw new DomainError('idempotency_conflict', 'Этот Idempotency-Key уже использован с другими параметрами.', 409);
      return { digest, existing: current.rows[0].safe_result ?? { id: current.rows[0].resource_id } };
    }
    await client.query('INSERT INTO idempotency_operations(tenant_id,operation,digest,body_digest,resource_id) VALUES($1,$2,$3,$4,$5)', [tenantId, operation, digest, contentDigest, randomUUID()]);
    return { digest };
  }

  async saveOperation(client, tenantId, operation, digest, resourceId, safeResult) {
    await client.query('UPDATE idempotency_operations SET resource_id=$4,safe_result=$5 WHERE tenant_id=$1 AND operation=$2 AND digest=$3', [tenantId, operation, digest, resourceId, safeResult]);
  }

  async listKeys(tenantId) {
    const result = await this.pool.query(`SELECT k.*,coalesce(u.spent_urub,0) AS spent_urub,coalesce(u.tokens,0) AS tokens
      FROM api_keys k LEFT JOIN (SELECT key_id,sum(charge_urub) AS spent_urub,sum(coalesce(input_tokens,0)+coalesce(output_tokens,0)) AS tokens
      FROM inference_requests WHERE tenant_id=$1 GROUP BY key_id) u ON u.key_id=k.id
      WHERE k.tenant_id=$1 AND k.purpose<>'chat' ORDER BY k.created_at DESC LIMIT 200`, [tenantId]);
    return result.rows.map(keyView);
  }

  async patchKey(tenantId, keyId, input) {
    const supported = ['name', 'status', 'limits', 'allowedModels', 'autoFallback', 'expiresAt'];
    if (!input || typeof input !== 'object' || Object.keys(input).some(field => !supported.includes(field))) throw new DomainError('invalid_key_update', 'Неизвестные поля настройки ключа.');
    return this.transaction(async client => {
      const result = await client.query("SELECT * FROM api_keys WHERE tenant_id=$1 AND id=$2 AND purpose<>'chat' FOR UPDATE", [tenantId, keyId]);
      const row = result.rows[0];
      if (!row) throw new DomainError('key_not_found', 'Ключ не найден.', 404);
      if (row.status === 'revoked') throw new DomainError('key_revoked', 'Отозванный ключ изменить нельзя.', 409);
      if (input.status !== undefined && !['active', 'paused', 'revoked'].includes(input.status)) throw new DomainError('invalid_status', 'Некорректный статус.');
      const name = input.name === undefined ? row.name : validateName(input.name);
      const limits = normalizeLimits(input.limits ?? {}, row.limits);
      const allowedModels = input.allowedModels === undefined ? row.allowed_models : normalizeModels(input.allowedModels);
      const autoFallback = input.autoFallback === undefined ? row.auto_fallback : input.autoFallback;
      if (typeof autoFallback !== 'boolean') throw new DomainError('invalid_policy', 'autoFallback должен быть true или false.');
      const expiry = input.expiresAt === undefined ? row.expires_at : validateExpiry(input.expiresAt);
      const status = input.status ?? row.status;
      if (row.status === 'pending_activation' && status === 'active') throw new DomainError('activation_required', 'Ключ должен быть активирован установщиком.', 409);
      const changed = await client.query(`UPDATE api_keys SET name=$3,limits=$4,allowed_models=$5,auto_fallback=$6,status=$7,expires_at=$8,
        policy_revision=policy_revision+1,updated_at=now() WHERE tenant_id=$1 AND id=$2 RETURNING *`, [tenantId, keyId, name, limits, allowedModels === null ? null : JSON.stringify(allowedModels), autoFallback, status, expiry]);
      await client.query('INSERT INTO policy_history(key_id,revision,policy) VALUES($1,$2,$3)', [keyId, changed.rows[0].policy_revision, { limits, allowedModels, autoFallback }]);
      if (status === 'revoked') await client.query('UPDATE credentials SET revoked_at=now() WHERE key_id=$1 AND revoked_at IS NULL', [keyId]);
      await this.audit(client, tenantId, `key.${status === row.status ? 'updated' : status}`, keyId, { revision: changed.rows[0].policy_revision });
      return keyView(changed.rows[0]);
    });
  }

  async rotateKey(tenantId, keyId, idempotencyKey) {
    return this.transaction(async client => {
      const result = await client.query('SELECT * FROM api_keys WHERE tenant_id=$1 AND id=$2 FOR UPDATE', [tenantId, keyId]);
      const row = result.rows[0];
      if (!row) throw new DomainError('key_not_found', 'Ключ не найден.', 404);
      assertActive(row);
      const operation = await this.claimOperation(client, tenantId, 'key.rotate', idempotencyKey, { keyId });
      if (operation.existing) return { ...operation.existing, secret: null, replayed: true };
      const secret = createSecret();
      const versions = await client.query('SELECT coalesce(max(version),0)+1 AS next FROM credentials WHERE key_id=$1', [keyId]);
      await client.query("UPDATE credentials SET expires_at=least(coalesce(expires_at,'infinity'::timestamptz),now()+interval '24 hours') WHERE key_id=$1 AND revoked_at IS NULL", [keyId]);
      await client.query('INSERT INTO credentials(id,tenant_id,key_id,fingerprint,version) VALUES($1,$2,$3,$4,$5)', [randomUUID(), tenantId, keyId, fingerprint(secret, this.pepper), versions.rows[0].next]);
      const updated = await client.query('UPDATE api_keys SET prefix=$3,last_four=$4,updated_at=now() WHERE tenant_id=$1 AND id=$2 RETURNING *', [tenantId, keyId, secret.slice(0, 17), secret.slice(-4)]);
      const safe = { key: keyView(updated.rows[0]), overlapHours: 24 };
      await this.saveOperation(client, tenantId, 'key.rotate', operation.digest, keyId, safe);
      await this.audit(client, tenantId, 'key.rotated', keyId, { version: versions.rows[0].next });
      return { ...safe, secret };
    });
  }

  async getChatKey(tenantId) {
    return this.transaction(async client => {
      await client.query('SELECT tenant_id FROM wallets WHERE tenant_id=$1 FOR UPDATE', [tenantId]);
      const existing = await client.query("SELECT * FROM api_keys WHERE tenant_id=$1 AND purpose='chat'", [tenantId]);
      if (existing.rows[0]) return existing.rows[0];
      const limits = { ...DEFAULT_LIMITS, rpm: 12, concurrency: 2, maxOutputTokens: 2048 };
      const result = await client.query(`INSERT INTO api_keys(id,tenant_id,name,prefix,last_four,purpose,limits)
        VALUES($1,$2,'Чат кабинета','internal','chat','chat',$3) RETURNING *`, [randomUUID(), tenantId, limits]);
      return result.rows[0];
    });
  }

  async topupDemo(tenantId, amount, idempotencyKey) {
    const credit = money(amount);
    if (credit < 1_000_000n || credit > DEMO_TOPUP_MAX) throw new DomainError('invalid_demo_amount', 'Демо-пополнение: от 1 до 10 000 ₽.');
    return this.transaction(async client => {
      await client.query('SELECT tenant_id FROM wallets WHERE tenant_id=$1 FOR UPDATE', [tenantId]);
      const operation = await this.claimOperation(client, tenantId, 'demo.topup', idempotencyKey, { amount });
      if (operation.existing) return { ...operation.existing, replayed: true };
      const id = randomUUID();
      await this.postLedger(client, tenantId, id, `demo-topup:${tenantId}:${operation.digest}`, 'demo_topup', credit, null);
      const wallet = await client.query('UPDATE wallets SET balance_urub=balance_urub+$2 WHERE tenant_id=$1 RETURNING *', [tenantId, credit.toString()]);
      const response = { id, isDemo: true, amountMicrorubles: amount, wallet: this.walletView(wallet.rows[0]), message: 'Учебные средства. Деньги не списывались.' };
      await this.saveOperation(client, tenantId, 'demo.topup', operation.digest, id, response);
      await this.audit(client, tenantId, 'demo.credit.created', id, { amountMicrorubles: amount, isDemo: true });
      return response;
    });
  }

  async postLedger(client, tenantId, id, businessKey, operation, amount, requestId) {
    await client.query('INSERT INTO ledger_transactions(id,tenant_id,operation,business_key,request_id,amount_urub) VALUES($1,$2,$3,$4,$5,$6)', [id, tenantId, operation, businessKey, requestId, amount.toString()]);
    const debit = operation === 'demo_topup' ? amount : -amount;
    const counterparty = operation === 'demo_topup' ? 'demo_credit' : 'consumption';
    await client.query("INSERT INTO ledger_entries(transaction_id,account,amount_urub) VALUES($1,'wallet',$2),($1,$3,$4)", [id, debit.toString(), counterparty, (-debit).toString()]);
  }

  walletView(row) {
    return { balanceMicrorubles: String(row.balance_urub), reservedMicrorubles: String(row.held_urub), availableMicrorubles: (BigInt(row.balance_urub) - BigInt(row.held_urub)).toString(), isDemo: true };
  }

  async reserveServiceBudget(client, amount, purpose) {
    const day = periodDate(this.clock());
    await client.query('INSERT INTO service_daily_budget(day) VALUES($1) ON CONFLICT DO NOTHING', [day]);
    const row = (await client.query('SELECT * FROM service_daily_budget WHERE day=$1 FOR UPDATE', [day])).rows[0];
    enforceLimit(this.dailyServiceBudget, BigInt(row.spent_urub) + BigInt(row.held_urub), amount, 'service_daily_budget_exhausted', 'Дневной бюджет общего демо-сервиса');
    if (purpose === 'probe') {
      const probes = (await client.query(`SELECT coalesce(sum(CASE WHEN state='active' THEN amount_urub
        WHEN state='settled' THEN actual_urub ELSE 0 END),0) AS amount FROM service_budget_holds WHERE day=$1 AND purpose='probe'`, [day])).rows[0];
      enforceLimit(this.dailyProbeBudget, probes.amount, amount, 'monitoring_daily_budget_exhausted', 'Дневной бюджет проверок моделей');
    }
    const id = randomUUID();
    await client.query('UPDATE service_daily_budget SET held_urub=held_urub+$2 WHERE day=$1', [day, amount.toString()]);
    await client.query('INSERT INTO service_budget_holds(id,day,amount_urub,purpose) VALUES($1,$2,$3,$4)', [id, day, amount.toString(), purpose]);
    return id;
  }

  async settleServiceBudget(client, id, amount) {
    if (!id) return;
    const row = (await client.query('SELECT * FROM service_budget_holds WHERE id=$1 FOR UPDATE', [id])).rows[0];
    if (!row || row.state !== 'active') return;
    await client.query('UPDATE service_daily_budget SET held_urub=held_urub-$2,spent_urub=spent_urub+$3 WHERE day=$1', [row.day, row.amount_urub, amount.toString()]);
    await client.query("UPDATE service_budget_holds SET state=$2,actual_urub=$3 WHERE id=$1", [id, amount === 0n ? 'released' : 'settled', amount.toString()]);
  }

  async reserve({ key, model, inputTokens, outputTokens, inputRate, outputRate, body, idempotencyKey, autoFallback }) {
    const priced = inputRate !== null && outputRate !== null;
    if (!priced && !key.legacy) throw new DomainError('price_unavailable', 'Тариф модели ещё не подтверждён. Выберите другую модель.', 503);
    const hold = priced ? chargeTokens(inputTokens, outputTokens, inputRate, outputRate) : 0n;
    const digest = idempotencyKey ? fingerprint(idempotencyKey, this.pepper) : null;
    const contentDigest = bodyDigest(body, this.pepper);
    const id = randomUUID();
    return this.transaction(async client => {
      await client.query('SELECT tenant_id FROM wallets WHERE tenant_id=$1 FOR UPDATE', [key.tenant_id]);
      const locked = (await client.query('SELECT * FROM api_keys WHERE id=$1 AND tenant_id=$2 FOR UPDATE', [key.id, key.tenant_id])).rows[0]; assertActive(locked);
      if (key.credential_id) {
        const credential = (await client.query('SELECT revoked_at,expires_at AS credential_expires_at FROM credentials WHERE id=$1', [key.credential_id])).rows[0];
        assertActive({ ...locked, ...credential });
      }
      if (digest) {
        const previous = (await client.query('SELECT * FROM inference_requests WHERE tenant_id=$1 AND key_id=$2 AND idempotency_digest=$3', [key.tenant_id, key.id, digest])).rows[0];
        if (previous) {
          if (previous.body_digest !== contentDigest) throw new DomainError('idempotency_conflict', 'Idempotency-Key уже использован для другого запроса.', 409);
          return { replay: true, request: previous };
        }
      }
      if (locked.allowed_models && !locked.allowed_models.includes(model.id)) throw new DomainError('model_not_allowed', 'Эта модель не разрешена для ключа.', 403);
      if (autoFallback && !locked.auto_fallback && locked.purpose !== 'chat') throw new DomainError('fallback_not_allowed', 'Автоматическая смена модели не разрешена для ключа.', 403);
      const limits = locked.limits; const localDay = periodDate(this.clock()); const localMonth = `${localDay.slice(0, 7)}-01`;
      const counters = (await client.query(`SELECT
        count(*) FILTER(WHERE created_at>now()-interval '1 minute') AS rpm,
        coalesce(sum(CASE WHEN status='running' THEN reserved_input_tokens+reserved_output_tokens ELSE coalesce(input_tokens,0)+coalesce(output_tokens,0) END) FILTER(WHERE created_at>now()-interval '1 minute'),0) AS tpm,
        count(*) FILTER(WHERE status='running') AS concurrency,
        coalesce(sum(charge_urub+CASE WHEN status='running' THEN hold_urub ELSE 0 END),0) AS total_spend,
        coalesce(sum(charge_urub+CASE WHEN status='running' THEN hold_urub ELSE 0 END) FILTER(WHERE local_day=$2),0) AS day_spend,
        coalesce(sum(charge_urub+CASE WHEN status='running' THEN hold_urub ELSE 0 END) FILTER(WHERE local_month=$3),0) AS month_spend,
        coalesce(sum(CASE WHEN status='running' THEN reserved_input_tokens+reserved_output_tokens ELSE coalesce(input_tokens,0)+coalesce(output_tokens,0) END) FILTER(WHERE local_day=$2),0) AS day_tokens,
        coalesce(sum(CASE WHEN status='running' THEN reserved_input_tokens+reserved_output_tokens ELSE coalesce(input_tokens,0)+coalesce(output_tokens,0) END) FILTER(WHERE local_month=$3),0) AS month_tokens
        FROM inference_requests WHERE key_id=$1`, [key.id, localDay, localMonth])).rows[0];
      enforceLimit(limits.rpm, counters.rpm, 1, 'rpm_limit', 'Лимит запросов в минуту');
      enforceLimit(limits.tpm, counters.tpm, inputTokens + outputTokens, 'tpm_limit', 'Лимит токенов в минуту');
      enforceLimit(limits.concurrency, counters.concurrency, 1, 'concurrency_limit', 'Лимит одновременных запросов');
      enforceLimit(limits.maxOutputTokens, 0, outputTokens, 'output_token_limit', 'Максимальная длина ответа');
      enforceLimit(limits.maxRequestMicrorubles, 0, hold, 'request_budget_limit', 'Бюджет одного запроса');
      enforceLimit(limits.dailyMicrorubles, counters.day_spend, hold, 'daily_budget_limit', 'Дневной бюджет ключа');
      enforceLimit(limits.monthlyMicrorubles, counters.month_spend, hold, 'monthly_budget_limit', 'Месячный бюджет ключа');
      enforceLimit(limits.totalMicrorubles, counters.total_spend, hold, 'total_budget_limit', 'Общий бюджет ключа');
      enforceLimit(limits.dailyTokens, counters.day_tokens, inputTokens + outputTokens, 'daily_token_limit', 'Дневной лимит токенов');
      enforceLimit(limits.monthlyTokens, counters.month_tokens, inputTokens + outputTokens, 'monthly_token_limit', 'Месячный лимит токенов');
      const chargeHold = locked.legacy ? 0n : hold;
      const wallet = (await client.query('SELECT * FROM wallets WHERE tenant_id=$1', [key.tenant_id])).rows[0];
      if (BigInt(wallet.balance_urub) - BigInt(wallet.held_urub) < chargeHold) throw new DomainError('insufficient_balance', 'Пополните демо-баланс для этого запроса.', 402);
      const serviceHoldId = locked.legacy ? null : await this.reserveServiceBudget(client, hold * 2n, locked.purpose);
      await client.query('UPDATE wallets SET held_urub=held_urub+$2 WHERE tenant_id=$1', [key.tenant_id, chargeHold.toString()]);
      await client.query(`INSERT INTO inference_requests(id,tenant_id,key_id,credential_id,purpose,model_requested,reserved_input_tokens,reserved_output_tokens,
        hold_urub,price_snapshot,policy_snapshot,policy_revision,local_day,local_month,service_budget_hold_id,body_digest,idempotency_digest)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`, [id, key.tenant_id, key.id, key.credential_id ?? null, locked.purpose, body.model, inputTokens, outputTokens, chargeHold.toString(), { inputRate, outputRate, isDemo: true }, { limits, allowedModels: locked.allowed_models, autoFallback, legacy: locked.legacy }, locked.policy_revision, localDay, localMonth, serviceHoldId, contentDigest, digest]);
      await client.query('UPDATE api_keys SET last_used_at=now() WHERE id=$1', [key.id]);
      await this.audit(client, key.tenant_id, 'request.accepted', id, { model: body.model, holdMicrorubles: chargeHold.toString(), purpose: locked.purpose });
      return { id, holdMicrorubles: chargeHold.toString(), legacy: locked.legacy };
    });
  }

  async replay(request) {
    if (request.status === 'running') throw new DomainError('request_in_progress', 'Этот запрос уже выполняется.', 409, { requestId: request.id });
    const cache = (await this.pool.query('SELECT * FROM response_cache WHERE request_id=$1 AND expires_at>now()', [request.id])).rows[0];
    if (cache) {
      const decipher = createDecipheriv('aes-256-gcm', this.cacheKey, cache.iv); decipher.setAuthTag(cache.tag); decipher.setAAD(Buffer.from(request.id));
      try { return JSON.parse(Buffer.concat([decipher.update(cache.encrypted), decipher.final()]).toString('utf8')); }
      catch { throw new DomainError('result_unavailable', 'Сохранённый результат недоступен. Повторное начисление не выполнялось.', 409, { requestId: request.id }); }
    }
    throw new DomainError('result_expired', 'Результат больше не хранится. Повторная отправка и списание не выполнялись.', 409, { requestId: request.id, status: request.status });
  }

  async finish(requestId, { outcome, usage, model, providerId, durationMs, errorCode, finishReason, response, noUpstreamDispatch = false }) {
    const normalized = normalizeUsage(usage);
    return this.transaction(async client => {
      const request = (await client.query('SELECT * FROM inference_requests WHERE id=$1 FOR UPDATE', [requestId])).rows[0];
      if (!request || request.status !== 'running') return request ? { chargeMicrorubles: String(request.charge_urub), duplicate: true } : null;
      await client.query('SELECT tenant_id FROM wallets WHERE tenant_id=$1 FOR UPDATE', [request.tenant_id]);
      const priced = request.price_snapshot.inputRate !== null && request.price_snapshot.outputRate !== null;
      const calculated = normalized && priced ? chargeTokens(normalized.inputTokens, normalized.outputTokens, request.price_snapshot.inputRate, request.price_snapshot.outputRate) : 0n;
      // An underestimated reservation is absorbed by the service; it never creates debt silently.
      const hold = BigInt(request.hold_urub);
      const charge = outcome === 'completed' && normalized && !request.policy_snapshot.legacy ? (calculated < hold ? calculated : hold) : 0n;
      const status = outcome === 'completed' && !normalized ? 'usage_pending' : outcome;
      if (charge > 0n) await this.postLedger(client, request.tenant_id, randomUUID(), `request:${requestId}`, 'consumption', charge, requestId);
      await client.query('UPDATE wallets SET held_urub=held_urub-$2,balance_urub=balance_urub-$3 WHERE tenant_id=$1', [request.tenant_id, request.hold_urub, charge.toString()]);
      // Unknown/failed upstream outcomes may have consumed money. Keep their service allowance spent.
      let serviceCharge = 0n;
      if (request.service_budget_hold_id) {
        const allowance = BigInt((await client.query('SELECT amount_urub FROM service_budget_holds WHERE id=$1', [request.service_budget_hold_id])).rows[0].amount_urub);
        const attempts = (await client.query('SELECT input_tokens,output_tokens,outcome FROM provider_attempts WHERE request_id=$1', [requestId])).rows;
        serviceCharge = noUpstreamDispatch ? 0n : attempts.length ? attempts.reduce((sum, attempt) => {
          if (attempt.input_tokens !== null && attempt.output_tokens !== null && priced) return sum + chargeTokens(Number(attempt.input_tokens), Number(attempt.output_tokens), request.price_snapshot.inputRate, request.price_snapshot.outputRate);
          return sum + allowance / 2n;
        }, 0n) : outcome === 'completed' && normalized ? calculated : allowance;
      }
      await this.settleServiceBudget(client, request.service_budget_hold_id, serviceCharge);
      await client.query(`UPDATE inference_requests SET status=$2,model_resolved=$3,provider_id=$4,input_tokens=$5,output_tokens=$6,
        charge_urub=$7,error_code=$8,finish_reason=$9,duration_ms=$10,finished_at=now() WHERE id=$1`, [requestId, status, model ?? request.model_requested, providerId ?? null, normalized?.inputTokens ?? null, normalized?.outputTokens ?? null, charge.toString(), errorCode ?? null, finishReason ?? null, Math.min(Math.max(0, Math.round(durationMs ?? 0)), 2147483647)]);
      if (outcome === 'completed' && response && request.idempotency_digest && this.responseCacheEnabled) {
        const iv = randomBytes(12); const cipher = createCipheriv('aes-256-gcm', this.cacheKey, iv); cipher.setAAD(Buffer.from(requestId));
        const encrypted = Buffer.concat([cipher.update(JSON.stringify(response), 'utf8'), cipher.final()]);
        await client.query('INSERT INTO response_cache(request_id,encrypted,iv,tag) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING', [requestId, encrypted, iv, cipher.getAuthTag()]);
      }
      await this.audit(client, request.tenant_id, `request.${status}`, requestId, { chargeMicrorubles: charge.toString(), usageKnown: normalized !== null });
      return { chargeMicrorubles: charge.toString(), status };
    });
  }

  async dashboard(tenantId, { models, baseUrl, capabilities, days = 30 }) {
    const periodEnd = periodDate(this.clock());
    const firstDay = new Date(`${periodEnd}T00:00:00Z`); firstDay.setUTCDate(firstDay.getUTCDate() - days + 1);
    const periodStart = firstDay.toISOString().slice(0, 10);
    const [wallet, keys, totals, daily, tenant, serviceBudget] = await Promise.all([
      this.pool.query('SELECT * FROM wallets WHERE tenant_id=$1', [tenantId]),
      this.listKeys(tenantId),
      this.pool.query(`SELECT count(*) AS requests,count(*) FILTER(WHERE status IN ('completed','usage_pending')) AS successful_requests,
        coalesce(sum(charge_urub),0) AS spent_urub,coalesce(sum(coalesce(input_tokens,0)+coalesce(output_tokens,0)),0) AS tokens,
        percentile_cont(0.5) WITHIN GROUP(ORDER BY duration_ms) FILTER(WHERE status='completed') AS p50,
        percentile_cont(0.95) WITHIN GROUP(ORDER BY duration_ms) FILTER(WHERE status='completed') AS p95
        FROM inference_requests WHERE tenant_id=$1 AND local_day >= $2::date AND local_day <= $3::date`, [tenantId, periodStart, periodEnd]),
      this.pool.query(`SELECT local_day::text AS date,count(*) AS requests,count(*) FILTER(WHERE status IN ('completed','usage_pending')) AS successful_requests,
        sum(charge_urub) AS spent_urub,sum(coalesce(input_tokens,0)+coalesce(output_tokens,0)) AS tokens
        FROM inference_requests WHERE tenant_id=$1 AND local_day >= $2::date AND local_day <= $3::date GROUP BY local_day ORDER BY local_day`, [tenantId, periodStart, periodEnd]),
      this.pool.query('SELECT kind,settings FROM tenants WHERE id=$1', [tenantId]),
      this.pool.query('SELECT spent_urub,held_urub FROM service_daily_budget WHERE day=$1', [periodDate(this.clock())]),
    ]);
    const total = totals.rows[0]; const count = Number(total.requests); const successes = Number(total.successful_requests);
    const dailyByDate = new Map(daily.rows.map(row => [row.date, row]));
    const calendar = Array.from({ length: days }, (_, index) => {
      const day = new Date(firstDay); day.setUTCDate(day.getUTCDate() + index);
      const date = day.toISOString().slice(0, 10); const row = dailyByDate.get(date);
      return { date, spentMicrorubles: String(row?.spent_urub ?? '0'), tokens: Number(row?.tokens ?? 0), requests: Number(row?.requests ?? 0), successfulRequests: Number(row?.successful_requests ?? 0) };
    });
    return {
      models, keys, wallet: this.walletView(wallet.rows[0]),
      usage: { spentMicrorubles: String(total.spent_urub), tokens: Number(total.tokens), requests: count, successfulRequests: successes,
        successRate: count ? successes / count * 100 : null, medianMs: total.p50, p95Ms: total.p95,
        periodDays: days, periodStart, periodEnd, timezone: 'Asia/Yekaterinburg', sampleSize: count, daily: calendar },
      settings: { ...tenant.rows[0].settings, baseUrl }, capabilities,
      temporaryWorkspace: tenant.rows[0].kind === 'anonymous', isDemo: true,
      serviceBudget: { limited: true, exhausted: serviceBudget.rowCount ? BigInt(serviceBudget.rows[0].spent_urub) + BigInt(serviceBudget.rows[0].held_urub) >= this.dailyServiceBudget : false },
      updatedAt: this.clock().toISOString(),
    };
  }

  async updateSettings(tenantId, input) {
    if (!input || Object.keys(input).some(key => !['routingMode', 'autoFallback'].includes(key))) throw new DomainError('invalid_settings', 'Неизвестная настройка.');
    if (input.routingMode !== undefined && !['fallback', 'race'].includes(input.routingMode)) throw new DomainError('invalid_routing_mode', 'Режим: fallback или race.');
    if (input.autoFallback !== undefined && typeof input.autoFallback !== 'boolean') throw new DomainError('invalid_settings', 'autoFallback должен быть true или false.');
    return this.transaction(async client => {
      const row = (await client.query('UPDATE tenants SET settings=settings||$2::jsonb WHERE id=$1 RETURNING settings', [tenantId, input])).rows[0];
      await this.audit(client, tenantId, 'settings.updated', tenantId, input);
      return row.settings;
    });
  }

  async exportUsage(tenantId, { days = 30, limit = 10000 } = {}) {
    const periodEnd = periodDate(this.clock());
    const firstDay = new Date(`${periodEnd}T00:00:00Z`); firstDay.setUTCDate(firstDay.getUTCDate() - days + 1);
    const periodStart = firstDay.toISOString().slice(0, 10);
    return (await this.pool.query(`SELECT r.id,r.created_at,r.model_requested,r.model_resolved,r.status,r.input_tokens,r.output_tokens,
      r.charge_urub,r.duration_ms,k.name AS key_name FROM inference_requests r JOIN api_keys k ON k.id=r.key_id
      WHERE r.tenant_id=$1 AND r.local_day >= $2::date AND r.local_day <= $3::date ORDER BY r.created_at DESC LIMIT $4`, [tenantId, periodStart, periodEnd, limit])).rows;
  }

  async payments(tenantId) {
    return (await this.pool.query("SELECT id,operation,amount_urub::text,recorded_at,is_demo FROM ledger_transactions WHERE tenant_id=$1 AND operation='demo_topup' ORDER BY recorded_at DESC LIMIT 100", [tenantId])).rows.map(row => ({ id: row.id, amountMicrorubles: row.amount_urub, createdAt: row.recorded_at, isDemo: row.is_demo, status: 'completed' }));
  }

  connectorHooks() {
    return {
      loadCatalog: async () => (await this.pool.query('SELECT models FROM catalog_snapshots ORDER BY recorded_at DESC LIMIT 1')).rows[0]?.models ?? [],
      saveCatalog: async models => { await this.pool.query('INSERT INTO catalog_snapshots(digest,models) VALUES($1,$2) ON CONFLICT(digest) DO NOTHING', [publicDigest(canonicalJson(models)), JSON.stringify(models)]); },
      saveObservation: async observation => { await this.pool.query(`INSERT INTO health_observations(provider_id,model,purpose,outcome,duration_ms,ttft_ms,error_code,profile,tools_verified)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, [observation.providerId ?? null, observation.model ?? observation.modelId ?? 'unknown', observation.purpose ?? 'probe', observation.outcome ?? observation.status ?? 'unknown', observation.durationMs ?? null, observation.ttftMs ?? null, observation.errorCode ?? null, ['text', 'json_object', 'tools'].includes(observation.profile) ? observation.profile : null, observation.toolsVerified === true]); },
      loadQualifications: async () => {
        const result = await this.pool.query(`SELECT provider_id,model,
          max(recorded_at) FILTER(WHERE profile='json_object') AS json_verified_at,
          max(recorded_at) FILTER(WHERE profile='tools' AND tools_verified=true) AS tools_verified_at
          FROM health_observations WHERE outcome='success' AND provider_id IS NOT NULL
          AND recorded_at>now()-interval '24 hours' AND recorded_at<=now()
          AND (profile='json_object' OR (profile='tools' AND tools_verified=true)) GROUP BY provider_id,model`);
        return result.rows.map(row => ({ providerId: row.provider_id, model: row.model,
          jsonVerifiedAt: row.json_verified_at ? new Date(row.json_verified_at).valueOf() : null,
          toolsVerifiedAt: row.tools_verified_at ? new Date(row.tools_verified_at).valueOf() : null }));
      },
      loadDailyProbeSpend: async () => {
        const result = await this.pool.query(`SELECT coalesce(sum(CASE WHEN state='active' THEN amount_urub
          WHEN state='settled' THEN actual_urub ELSE 0 END),0) AS amount FROM service_budget_holds WHERE day=$1 AND purpose='probe'`, [periodDate(this.clock())]);
        return String(result.rows[0]?.amount ?? '0');
      },
      addDailyProbeSpend: async amount => this.transaction(async client => {
        const cost = money(amount); const id = await this.reserveServiceBudget(client, cost, 'probe');
        await this.settleServiceBudget(client, id, cost);
      }),
      reserveProbeSpend: async amount => {
        try { return { id: await this.transaction(client => this.reserveServiceBudget(client, money(amount), 'probe')) }; }
        catch (error) { if (['service_daily_budget_exhausted', 'monitoring_daily_budget_exhausted'].includes(error.code)) return null; throw error; }
      },
      settleProbeSpend: async (id, amount) => this.transaction(client => this.settleServiceBudget(client, id, money(amount))),
    };
  }

  async recordAttempt(attempt) {
    const requestId = attempt.purpose !== 'health_probe' && typeof attempt.logicalRequestId === 'string' && /^[0-9a-f-]{36}$/i.test(attempt.logicalRequestId) ? attempt.logicalRequestId : null;
    await this.pool.query(`INSERT INTO provider_attempts(id,request_id,provider_id,model,purpose,outcome,duration_ms,ttft_ms,input_tokens,output_tokens,error_code)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, [randomUUID(), requestId, attempt.providerId ?? 'unknown', attempt.model ?? 'unknown', attempt.purpose ?? 'inference', attempt.outcome ?? 'unknown', attempt.durationMs ?? null, attempt.ttftMs ?? null, attempt.inputTokens ?? null, attempt.outputTokens ?? null, attempt.errorCode ?? null]);
  }

  async sweep() {
    const expired = await this.pool.query("SELECT id FROM inference_requests WHERE status='running' AND lease_expires_at<now() LIMIT 100");
    for (const row of expired.rows) await this.finish(row.id, { outcome: 'abandoned', errorCode: 'worker_lease_expired' });
    await this.pool.query('DELETE FROM response_cache WHERE expires_at<now()');
    await this.pool.query('DELETE FROM device_sessions WHERE expires_at<now()');
    await this.pool.query("DELETE FROM health_observations WHERE recorded_at<now()-interval '7 days'");
    await this.pool.query('UPDATE outbox SET processed_at=now() WHERE processed_at IS NULL');
  }
}

export { keyView, assertActive };
