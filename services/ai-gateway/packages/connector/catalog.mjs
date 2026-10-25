import { createHash } from 'node:crypto';
import { boundedJson, ConnectorError } from './errors.mjs';

export const LEGACY_MODELS = Object.freeze({
  deepseek: 'deepseek-ai/DeepSeek-V4-Flash-0731',
  minimax: 'MiniMaxAI/MiniMax-M2.7',
  glm: 'zai-org/GLM-5.3-Flash',
});

// Existing family aliases retain their historical family; exact IDs are never remapped.
export function resolveAlias(value, models = []) {
  if (models.some((model) => model.id === value)) return value;
  const name = String(value ?? '').trim().toLowerCase();
  for (const id of Object.values(LEGACY_MODELS)) if (id.toLowerCase() === name) return id;
  if (['deepseek', 'deepseek-chat', 'deepseek-reasoner', 'deepseek-v3', 'deepseek-v3.1', 'deepseek-v3.2', 'deepseek-v4', 'deepseek-v4-flash', 'deepseek-v4-flash-0731', 'deepseek/deepseek-v4-flash', 'deepseek-ai/deepseek-v4-flash'].includes(name)) return LEGACY_MODELS.deepseek;
  if (/^(?:deepseek-ai\/|deepseek\/)?deepseek(?:[-_.][a-z0-9][a-z0-9._-]*)?$/.test(name)) return LEGACY_MODELS.deepseek;
  if (/^(?:minimaxai\/|minimax\/)?minimax(?:[-_.][a-z0-9][a-z0-9._-]*)?$/.test(name)) return LEGACY_MODELS.minimax;
  if (['glm', 'glm-5.3-flash', 'zai/glm-5.3-flash'].includes(name)) return LEGACY_MODELS.glm;
  return value;
}

export function normalizeSnapshot(payload, provider, now, retail = {}) {
  if (!payload || !Array.isArray(payload.data) || payload.data.length < 1 || payload.data.length > 1000) throw new ConnectorError('catalog_invalid_schema');
  const seen = new Set();
  return payload.data.map((item) => {
    if (!item || typeof item.id !== 'string' || item.id.length > 240 || !/^[A-Za-z0-9][A-Za-z0-9._:/-]+$/.test(item.id) || seen.has(item.id)) throw new ConnectorError('catalog_invalid_model');
    seen.add(item.id);
    const rawContext = item.context_length ?? item.context_window ?? item.top_provider?.context_length;
    const contextWindow = Number.isSafeInteger(rawContext) && rawContext > 0 ? rawContext : null;
    const capabilities = { text: 'advertised', stream: 'unknown', tools: 'unknown', json: 'unknown' };
    const label = displayLabel(item.id);
    return {
      id: item.id, name: label.name, version: label.version, canonicalVersion: item.id, contextWindow, status: 'unknown', checkedAt: null,
      inputPriceMicrorublesPerMillion: retail.input ?? null,
      outputPriceMicrorublesPerMillion: retail.output ?? null,
      priceStatus: retail.input && retail.output ? 'operator_configured' : 'unknown',
      capabilities, catalogState: 'discovered', discoveredAt: new Date(now).toISOString(),
      routes: [{ providerId: provider.id, upstreamModel: item.id, catalogState: 'present', catalogSeenAt: new Date(now).toISOString(), capabilities }],
    };
  });
}

export function displayLabel(id) {
  const basename = id.split('/').at(-1);
  for (const [pattern, name] of [[/^minimax[-_](.+)$/i, 'MiniMax'], [/^deepseek[-_](.+)$/i, 'DeepSeek'], [/^glm[-_](.+)$/i, 'GLM']]) {
    const match = basename.match(pattern);
    if (match) return { name, version: match[1].replace(/-\d{4}$/, '').replaceAll('-', ' ') };
  }
  return { name: basename, version: '' };
}

export function createCatalog({ providers, store = {}, fetchImpl = fetch, now = Date.now, retail = {} }) {
  const snapshots = new Map();
  const states = new Map();
  const missing = new Map();
  let lastGood = [];
  let loaded = false;
  let refreshing;
  async function load() {
    if (loaded) return;
    loaded = true;
    const prior = await store.loadCatalog?.();
    if (Array.isArray(prior)) {
      lastGood = prior.filter((x) => typeof x.id === 'string' && Array.isArray(x.routes));
      for (const provider of providers) snapshots.set(provider.id, lastGood.filter((x) => x.routes.some((r) => r.providerId === provider.id)).map((x) => ({ ...x, routes: x.routes.filter((r) => r.providerId === provider.id) })));
    }
  }
  async function refresh() {
    if (refreshing) return refreshing;
    refreshing = (async () => {
      await load();
      await Promise.allSettled(providers.map(async (provider) => {
        const at = now();
        try {
          const response = await fetchImpl(`${provider.baseUrl}/models`, { headers: { Authorization: `Bearer ${provider.apiKey}`, Accept: 'application/json' }, signal: AbortSignal.timeout(10_000), redirect: 'error' });
          if (!response.ok) { await response.body?.cancel(); throw new ConnectorError(`catalog_http_${response.status}`); }
          const candidate = normalizeSnapshot(await boundedJson(response, 2 * 1024 * 1024), provider, at, retail);
          const previous = snapshots.get(provider.id) ?? [];
          const nextIds = new Set(candidate.map((x) => x.id));
          const disappeared = previous.filter((x) => !nextIds.has(x.id));
          if (previous.length > 1 && disappeared.length / previous.length > 0.5) throw new ConnectorError('catalog_mass_removal_quarantined');
          for (const model of disappeared) {
            const key = `${provider.id}:${model.id}`;
            const occurrence = missing.get(key) ?? { count: 0, firstAt: at };
            occurrence.count += 1;
            missing.set(key, occurrence);
            const confirmed = occurrence.count >= 3 && at - occurrence.firstAt >= 180_000;
            candidate.push({ ...model, routes: model.routes.map((route) => ({ ...route, catalogState: confirmed ? 'temporarily_missing' : 'present' })) });
          }
          for (const id of nextIds) missing.delete(`${provider.id}:${id}`);
          snapshots.set(provider.id, candidate);
          states.set(provider.id, { providerId: provider.id, lastSuccessAt: new Date(at).toISOString(), lastAttemptAt: new Date(at).toISOString(), errorCode: null, checksum: createHash('sha256').update(JSON.stringify(candidate.map((m) => [m.id, m.contextWindow]))).digest('hex') });
        } catch (error) {
          states.set(provider.id, { ...states.get(provider.id), providerId: provider.id, lastAttemptAt: new Date(at).toISOString(), errorCode: error instanceof ConnectorError ? error.code : 'catalog_unavailable' });
        }
      }));
      const merged = new Map();
      for (const entries of snapshots.values()) for (const entry of entries) {
        const existing = merged.get(entry.id);
        if (existing) existing.routes.push(...entry.routes);
        else merged.set(entry.id, structuredClone(entry));
      }
      if (merged.size) {
        lastGood = [...merged.values()];
        await store.saveCatalog?.(lastGood);
      }
      return structuredClone(lastGood);
    })().finally(() => { refreshing = undefined; });
    return refreshing;
  }
  return { load, refresh, models: () => structuredClone(lastGood), sources: () => [...states.values()] };
}
