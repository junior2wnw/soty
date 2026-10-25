import { normalizeLimits, money, DomainError } from '../../../packages/domain/index.mjs';

export const DEFAULT_SOTY_ALLOWANCE = Object.freeze({
  grantedMicrorubles: '50000000',
  limits: Object.freeze({
    dailyMicrorubles: '5000000', monthlyMicrorubles: '50000000', totalMicrorubles: '50000000',
    maxRequestMicrorubles: '1000000', concurrency: 2, rpm: 30, tpm: 100000, maxOutputTokens: 8192,
    dailyTokens: 500000, monthlyTokens: 3000000,
  }),
});

export function sotyAllowance(input = {}) {
  if (!input || typeof input !== 'object' || Array.isArray(input) || Object.keys(input).some(key => !['grantedMicrorubles', 'limits'].includes(key))) throw new Error('Invalid included allowance configuration.');
  const grantedMicrorubles = money(input.grantedMicrorubles ?? DEFAULT_SOTY_ALLOWANCE.grantedMicrorubles).toString();
  const limits = normalizeLimits(input.limits ?? {}, DEFAULT_SOTY_ALLOWANCE.limits);
  if (BigInt(grantedMicrorubles) <= 0n || Object.values(limits).some(value => value === null || value === undefined)
      || BigInt(limits.totalMicrorubles) > BigInt(grantedMicrorubles)
      || BigInt(limits.monthlyMicrorubles) > BigInt(limits.totalMicrorubles)
      || BigInt(limits.dailyMicrorubles) > BigInt(limits.monthlyMicrorubles)
      || BigInt(limits.maxRequestMicrorubles) > BigInt(limits.dailyMicrorubles)) throw new Error('Invalid finite included allowance configuration.');
  return Object.freeze({ grantedMicrorubles, limits: Object.freeze(limits) });
}

export function validateSotyModels(models) {
  if (models !== null && models.some(id => id.length > 240 || !/^[A-Za-z0-9][A-Za-z0-9._:/-]+$/.test(id))) throw new DomainError('invalid_models', 'Идентификаторы моделей должны соответствовать каталогу ИИ.');
  return models;
}

export function publicSotyModels(models) {
  const preferred = ['deepseek-ai/DeepSeek-V4-Flash-0731', 'MiniMaxAI/MiniMax-M2.7', 'zai-org/GLM-5.3-Flash'];
  const capabilities = ['text', 'stream', 'json', 'tools', 'vision', 'audio'];
  const statuses = ['healthy', 'slow', 'recovering', 'unknown', 'unavailable', 'credential_error', 'funding_required', 'rate_limited'];
  const rate = value => { try { return money(value).toString(); } catch { return null; } };
  const priority = id => preferred.includes(id) ? preferred.indexOf(id) : preferred.length;
  const unique = new Map();
  for (const model of models) {
    if (typeof model?.id !== 'string' || !model.id || model.id.length > 250 || /[\u0000-\u0020\u007f]/.test(model.id)) continue;
    const known = Array.isArray(model.capabilities) ? model.capabilities : Object.entries(model.capabilities ?? {}).filter(([, state]) => [true, 'verified', 'supported', 'tested'].includes(state)).map(([name]) => name);
    unique.set(model.id, { id: model.id, name: typeof model.name === 'string' ? model.name.slice(0, 160) : model.id.slice(0, 160), status: statuses.includes(model.status) ? model.status : 'unavailable',
      contextWindow: Number.isSafeInteger(model.contextWindow) && model.contextWindow > 0 ? model.contextWindow : null,
      inputPriceMicrorublesPerMillion: rate(model.inputPriceMicrorublesPerMillion), outputPriceMicrorublesPerMillion: rate(model.outputPriceMicrorublesPerMillion),
      capabilities: capabilities.filter(name => known.includes(name)) });
  }
  const sorted = [...unique.values()].sort((a, b) => priority(a.id) - priority(b.id) || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  const result = []; let bytes = 2;
  for (const model of sorted) {
    const size = Buffer.byteLength(JSON.stringify(model)) + 1;
    if (result.length >= 300 || bytes + size > 384 * 1024) break;
    result.push(model); bytes += size;
  }
  return result;
}
