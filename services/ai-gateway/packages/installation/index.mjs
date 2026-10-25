import { randomBytes, randomUUID, createHash } from 'node:crypto';
import { renderInstaller, validateBaseUrl } from './installer.mjs';

export const INSTALLATION_SCHEMA = `
CREATE TABLE IF NOT EXISTS installation_claims (
 id text PRIMARY KEY, tenant_id text NOT NULL, claim_hash text NOT NULL UNIQUE,
 model_id text NOT NULL, models_json jsonb NOT NULL DEFAULT '[]', auto_fallback boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL DEFAULT now(), expires_at timestamptz NOT NULL,
 redeemed_at timestamptz, nonce_hash text, key_id text, activated_at timestamptz
);
CREATE INDEX IF NOT EXISTS installation_claims_tenant_created ON installation_claims(tenant_id,created_at);
ALTER TABLE installation_claims ADD COLUMN IF NOT EXISTS auto_fallback boolean NOT NULL DEFAULT false;
`;
const hash = value => createHash('sha256').update(value).digest('hex');
const failure = (statusCode, code, message) => Object.assign(new Error(message), { statusCode, code });
const validNonce = value => typeof value === 'string' && /^[a-zA-Z0-9_-]{16,128}$/.test(value);

export async function registerInstallations(app, options) {
  const { pool, getTenant, issueKey, activateKey, findKeyBySecret, managementGuard, getModels } = options;
  const baseUrl = validateBaseUrl(options.apiBaseUrl.replace(/\/v1\/?$/, ''));
  const inferenceBaseUrl = `${baseUrl}/v1`;
  await pool.query(INSTALLATION_SCHEMA);

  app.post('/api/installations', { preHandler: managementGuard, config: { logLevel: 'silent' } }, async (request, reply) => {
    const tenantId = await getTenant(request);
    if (!tenantId) throw failure(401, 'WORKSPACE_REQUIRED', 'Откройте кабинет ещё раз.');
    const model = request.body?.model;
    const autoFallback = request.body?.autoFallback ?? false;
    if (typeof autoFallback !== 'boolean') throw failure(400, 'INVALID_POLICY', 'Проверьте режим выбора модели.');
    const models = (await getModels()).map(m => typeof m === 'string' ? m : m.id);
    if (typeof model !== 'string' || !models.includes(model)) throw failure(400, 'MODEL_UNAVAILABLE', 'Выберите доступную модель.');
    const claim = randomBytes(32).toString('base64url');
    // Validate and render before committing the claim. Download responses never get cached.
    const file = renderInstaller({ baseUrl, claim, model });
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`install:${tenantId}`]);
      const existing = await client.query("SELECT count(*) AS count FROM installation_claims WHERE tenant_id=$1 AND created_at > now()-interval '10 minutes'", [tenantId]);
      if (Number(existing.rows[0].count) >= 5) throw failure(429, 'SETUP_RATE_LIMIT', 'Подождите перед созданием новой установки.');
      await client.query("INSERT INTO installation_claims(id,tenant_id,claim_hash,model_id,models_json,auto_fallback,expires_at) VALUES($1,$2,$3,$4,$5,$6,now()+interval '10 minutes')", [randomUUID(), tenantId, hash(claim), model, JSON.stringify(models), autoFallback]);
      await client.query('COMMIT');
    } catch (error) { await client.query('ROLLBACK'); throw error; }
    finally { client.release(); }
    return reply.header('Cache-Control', 'no-store').header('Content-Disposition', 'attachment; filename="OpenCode-Soty.cmd"').type('application/octet-stream').send(file);
  });

  app.post('/setup/redeem', { config: { logLevel: 'silent' } }, async (request, reply) => {
    const { claim, installationNonce } = request.body ?? {};
    if (typeof claim !== 'string' || !/^[\w-]{43}$/.test(claim) || !validNonce(installationNonce)) throw failure(400, 'INVALID_SETUP', 'Неверные данные установки.');
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const result = await client.query('SELECT *, expires_at > now() AS fresh FROM installation_claims WHERE claim_hash=$1 FOR UPDATE', [hash(claim)]);
      const row = result.rows[0];
      if (!row) throw failure(410, 'SETUP_EXPIRED', 'Скачайте новый установщик в кабинете.');
      if (row.redeemed_at) {
        if (row.nonce_hash !== hash(installationNonce)) throw failure(409, 'SETUP_USED', 'Эта установка уже использована.');
        await client.query('COMMIT');
        return reply.header('Cache-Control', 'no-store').send({ keyId: row.key_id, secret: null, models: row.models_json, apiBaseUrl: inferenceBaseUrl, recovery: 'download_new_installer_if_local_receipt_is_missing' });
      }
      if (!row.fresh) throw failure(410, 'SETUP_EXPIRED', 'Скачайте новый установщик в кабинете.');
      await client.query('SELECT tenant_id FROM wallets WHERE tenant_id=$1 FOR UPDATE', [row.tenant_id]);
      const issued = await issueKey(client, { tenantId: row.tenant_id, name: 'OpenCode', pendingActivation: true, autoFallback: row.auto_fallback });
      const keyId = issued.key.id;
      await client.query('UPDATE installation_claims SET redeemed_at=now(),nonce_hash=$2,key_id=$3 WHERE id=$1', [row.id, hash(installationNonce), keyId]);
      await client.query('COMMIT');
      return reply.header('Cache-Control', 'no-store').send({ keyId, secret: issued.secret, models: row.models_json, apiBaseUrl: inferenceBaseUrl });
    } catch (error) { await client.query('ROLLBACK'); throw error; }
    finally { client.release(); }
  });

  app.post('/setup/activate', { config: { logLevel: 'silent' } }, async (request, reply) => {
    const secret = /^Bearer ([^\s]+)$/.exec(request.headers.authorization ?? '')?.[1];
    const keyId = request.body?.keyId;
    if (!secret || typeof keyId !== 'string' || keyId.length > 100) throw failure(401, 'INVALID_CREDENTIAL', 'Требуется выданный ключ.');
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const row = (await client.query('SELECT *, redeemed_at > now()-interval \'10 minutes\' AS activation_fresh FROM installation_claims WHERE key_id=$1 FOR UPDATE', [keyId])).rows[0];
      const key = await findKeyBySecret(client, secret);
      if (!row || !key || key.id !== keyId || key.tenantId !== row.tenant_id) throw failure(401, 'INVALID_CREDENTIAL', 'Ключ не соответствует установке.');
      if (!row.activated_at) {
        if (!row.activation_fresh || key.status !== 'pending_activation') throw failure(410, 'SETUP_EXPIRED', 'Скачайте новый установщик.');
        await activateKey(client, keyId);
        await client.query('UPDATE installation_claims SET activated_at=now() WHERE id=$1', [row.id]);
      } else if (key.status !== 'active') throw failure(403, 'KEY_INACTIVE', 'Ключ приостановлен или отозван.');
      await client.query('COMMIT');
      return reply.header('Cache-Control', 'no-store').send({ active: true, inferenceVerified: false });
    } catch (error) { await client.query('ROLLBACK'); throw error; }
    finally { client.release(); }
  });
}
