import { readFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { DEFAULT_LIMITS, normalizeLimits, normalizeModels, validateName } from '../../../packages/domain/index.mjs';

export async function importLegacyKeys(store, filename) {
  if (!filename) return { imported: 0, unchanged: 0 };
  const parsed = JSON.parse(await readFile(filename, 'utf8'));
  const keys = Array.isArray(parsed) ? parsed : parsed.keys;
  if (!Array.isArray(keys) || keys.length > 10000) throw new Error('Invalid legacy migration document');
  for (const record of keys) {
    if (!record || !/^[a-f0-9]{64}$/i.test(record.fingerprint ?? '') || !['hmac_sha256', 'legacy_sha256'].includes(record.fingerprintAlgorithm ?? 'hmac_sha256')) throw new Error('Legacy migration must contain tagged fingerprints only');
    if (record.secret || record.apiKey || record.token) throw new Error('Plaintext credentials are forbidden in legacy migration');
  }
  let imported = 0; let unchanged = 0;
  await store.transaction(async client => {
    await client.query("SELECT pg_advisory_xact_lock(hashtext('legacy-key-import'))");
    for (const record of keys) {
      const legacyId = String(record.legacyId ?? record.name ?? '').slice(0, 200);
      if (!legacyId) throw new Error('Legacy key needs a stable identity');
      const existing = (await client.query('SELECT id FROM api_keys WHERE legacy_id=$1', [legacyId])).rows[0];
      if (existing) { unchanged++; continue; }
      const tenantId = record.tenantId && /^[0-9a-f-]{36}$/i.test(record.tenantId) ? record.tenantId : randomUUID();
      const id = randomUUID();
      const allowedModels = normalizeModels(record.allowedModels);
      const paths = record.allowedPaths ?? (record.legacyKind === 'agent' ? ['/api/connectors/gonka/v1'] : ['/api/inference/v1', '/v1']);
      if (!Array.isArray(paths) || paths.some(path => !['/v1', '/api/inference/v1', '/api/connectors/gonka/v1'].includes(path))) throw new Error('Unknown legacy endpoint scope');
      const unlimited = Object.fromEntries(Object.keys(DEFAULT_LIMITS).map(key => [key, null]));
      const limits = normalizeLimits(record.limits ?? {}, unlimited);
      await client.query("INSERT INTO tenants(id,name,kind) VALUES($1,$2,'legacy') ON CONFLICT DO NOTHING", [tenantId, validateName(record.name ?? legacyId)]);
      await client.query('INSERT INTO wallets(tenant_id) VALUES($1) ON CONFLICT DO NOTHING', [tenantId]);
      await client.query(`INSERT INTO api_keys(id,tenant_id,name,prefix,last_four,purpose,legacy,legacy_id,limits,allowed_models,auto_fallback,allowed_paths)
        VALUES($1,$2,$3,$4,$5,'legacy',true,$6,$7,$8,$9,$10)`, [id, tenantId, validateName(record.name ?? legacyId), String(record.prefix ?? 'legacy').slice(0, 30), String(record.lastFour ?? '****').slice(-4), legacyId, limits, allowedModels === null ? null : JSON.stringify(allowedModels), record.autoFallback === true, JSON.stringify(paths)]);
      await client.query('INSERT INTO credentials(id,tenant_id,key_id,fingerprint,fingerprint_algorithm) VALUES($1,$2,$3,$4,$5)', [randomUUID(), tenantId, id, record.fingerprint.toLowerCase(), record.fingerprintAlgorithm ?? 'hmac_sha256']);
      await client.query('INSERT INTO policy_history(key_id,revision,policy) VALUES($1,1,$2)', [id, { limits, allowedModels, legacy: true, source: 'authorized-migration' }]);
      await store.audit(client, tenantId, 'legacy.key.imported', id, { legacyId, algorithm: record.fingerprintAlgorithm ?? 'hmac_sha256' });
      imported++;
    }
  });
  return { imported, unchanged };
}
