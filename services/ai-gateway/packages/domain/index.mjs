import { createHash, createHmac, randomBytes, timingSafeEqual } from 'node:crypto';

export class DomainError extends Error {
  constructor(code, message, status = 400, details) {
    super(message); this.code = code; this.status = status; this.details = details;
  }
}

export const MICRORUBLES_PER_RUBLE = 1_000_000n;
export const MAX_MONEY = 9_000_000_000_000_000_000n;

export function money(value, { nullable = false, name = 'amount' } = {}) {
  if (nullable && (value === null || value === undefined)) return null;
  if (typeof value !== 'string' || !/^(0|[1-9]\d{0,18})$/.test(value)) {
    throw new DomainError('invalid_amount', `${name}: ожидается целая сумма в микрорублях строкой.`);
  }
  const amount = BigInt(value);
  if (amount > MAX_MONEY) throw new DomainError('invalid_amount', `${name}: слишком большая сумма.`);
  return amount;
}

export function ceilDiv(numerator, denominator) {
  if (numerator < 0n || denominator <= 0n) throw new RangeError('Nonnegative numerator and positive denominator required');
  return (numerator + denominator - 1n) / denominator;
}

export function chargeTokens(inputTokens, outputTokens, inputRate, outputRate) {
  for (const value of [inputTokens, outputTokens]) {
    if (!Number.isSafeInteger(value) || value < 0) throw new DomainError('invalid_usage', 'Поставщик вернул некорректный расход токенов.', 502);
  }
  return ceilDiv(BigInt(inputTokens) * money(inputRate) + BigInt(outputTokens) * money(outputRate), 1_000_000n);
}

export function canonicalJson(value) {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(',')}}`;
}

export function fingerprint(secret, pepper) {
  if (!pepper || Buffer.byteLength(pepper) < 32) throw new Error('API_KEY_PEPPER must contain at least 32 bytes');
  return createHmac('sha256', pepper).update(secret).digest('hex');
}

export function safeEqual(a, b) {
  if (typeof a !== 'string' || typeof b !== 'string') return false;
  const left = Buffer.from(a); const right = Buffer.from(b);
  return left.length === right.length && timingSafeEqual(left, right);
}

export function createSecret(prefix = 'sk-soty-live-') {
  return prefix + randomBytes(32).toString('base64url');
}

export function bodyDigest(body, pepper) { return fingerprint(canonicalJson(body), pepper); }
export function publicDigest(value) { return createHash('sha256').update(value).digest('hex'); }

const monetaryLimits = ['dailyMicrorubles', 'monthlyMicrorubles', 'totalMicrorubles', 'maxRequestMicrorubles'];
const numberLimits = ['rpm', 'tpm', 'concurrency', 'dailyTokens', 'monthlyTokens', 'maxOutputTokens'];
export const DEFAULT_LIMITS = Object.freeze({
  dailyMicrorubles: null, monthlyMicrorubles: null, totalMicrorubles: null,
  maxRequestMicrorubles: '5000000', rpm: 60, tpm: 200000, concurrency: 3,
  dailyTokens: null, monthlyTokens: null, maxOutputTokens: 2048,
});

export function normalizeLimits(input = {}, base = DEFAULT_LIMITS) {
  if (!input || typeof input !== 'object' || Array.isArray(input)) throw new DomainError('invalid_limits', 'Лимиты должны быть объектом.');
  const result = { ...base };
  for (const [key, value] of Object.entries(input)) {
    if (monetaryLimits.includes(key)) {
      result[key] = money(value, { nullable: true, name: key })?.toString() ?? null;
    } else if (numberLimits.includes(key)) {
      if (value !== null && (!Number.isSafeInteger(value) || value < 0 || value > 1_000_000_000)) throw new DomainError('invalid_limits', `${key}: требуется целое неотрицательное число или null.`);
      result[key] = value;
    } else throw new DomainError('invalid_limits', `Неизвестный лимит: ${key}.`);
  }
  return result;
}

export function normalizeModels(value) {
  if (value === null || value === undefined) return null;
  if (!Array.isArray(value) || value.length > 100 || value.some(item => typeof item !== 'string' || !item || item.length > 250)) throw new DomainError('invalid_models', 'Некорректный список разрешённых моделей.');
  return [...new Set(value)];
}

export function validateName(value) {
  if (typeof value !== 'string' || !value.trim() || value.trim().length > 100) throw new DomainError('invalid_name', 'Название должно содержать от 1 до 100 символов.');
  return value.trim();
}

export function validateExpiry(value) {
  if (value === null || value === undefined) return null;
  const instant = new Date(value);
  if (typeof value !== 'string' || !Number.isFinite(instant.valueOf()) || instant <= new Date()) throw new DomainError('invalid_expiry', 'Срок действия должен быть в будущем.');
  return instant.toISOString();
}

export function periodDate(now = new Date(), timezone = 'Asia/Yekaterinburg') {
  return new Intl.DateTimeFormat('en-CA', { timeZone: timezone, year: 'numeric', month: '2-digit', day: '2-digit' }).format(now);
}

export function enforceLimit(limit, current, requested, code, label) {
  if (limit !== null && limit !== undefined && BigInt(current) + BigInt(requested) > BigInt(limit)) throw new DomainError(code, `${label} исчерпан.`, 429);
}

export function validateChatBody(body) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) throw new DomainError('invalid_request', 'Ожидается JSON-объект.');
  if (typeof body.model !== 'string' || !body.model || body.model.length > 250) throw new DomainError('invalid_model', 'Выберите модель.');
  if (!Array.isArray(body.messages) || body.messages.length === 0 || body.messages.length > 256) throw new DomainError('invalid_messages', 'Нужен список от 1 до 256 сообщений.');
  for (const message of body.messages) {
    if (!message || typeof message !== 'object' || !['system', 'developer', 'user', 'assistant', 'tool'].includes(message.role)) throw new DomainError('invalid_messages', 'Некорректная роль сообщения.');
    if (message.content !== null && message.content !== undefined && typeof message.content !== 'string' && !Array.isArray(message.content)) throw new DomainError('invalid_messages', 'Некорректное содержимое сообщения.');
  }
  if (body.stream !== undefined && typeof body.stream !== 'boolean') throw new DomainError('invalid_stream', 'stream должен быть true или false.');
  if (body.n !== undefined && body.n !== 1) throw new DomainError('unsupported_parameter', 'Поддерживается n=1.');
  if (body.max_tokens !== undefined && body.max_completion_tokens !== undefined) throw new DomainError('ambiguous_token_limit', 'Укажите только один параметр ограничения выхода.');
  const limit = body.max_completion_tokens ?? body.max_tokens;
  if (limit !== undefined && (!Number.isSafeInteger(limit) || limit < 1 || limit > 32768)) throw new DomainError('invalid_max_tokens', 'Максимальная длина ответа: 1–32768 токенов.');
  return body;
}

export function estimateReservationTokens(body, maxOutputTokens) {
  // This is an admission bound, never reported as measured provider usage.
  // Tool schemas and message wrappers are included in the conservative byte allowance.
  const input = Buffer.byteLength(JSON.stringify({ messages: body.messages, tools: body.tools, response_format: body.response_format }), 'utf8') + body.messages.length * 64 + 512;
  return { inputTokens: input, outputTokens: maxOutputTokens, method: 'utf8_bytes_plus_wrappers_v1' };
}

export function normalizeUsage(usage) {
  if (!usage || typeof usage !== 'object') return null;
  const input = usage.prompt_tokens ?? usage.input_tokens ?? usage.inputTokens;
  const output = usage.completion_tokens ?? usage.output_tokens ?? usage.outputTokens;
  if (![input, output].every(value => Number.isSafeInteger(value) && value >= 0)) return null;
  const total = usage.total_tokens ?? input + output;
  if (!Number.isSafeInteger(total) || total < input + output) return null;
  return { inputTokens: input, outputTokens: output, totalTokens: input + output };
}

export function csvCell(value) {
  let text = value === null || value === undefined ? '' : String(value);
  if (/^[\s\u0000-\u001f]*[=+@-]/.test(text)) text = `'${text}`;
  return `"${text.replaceAll('"', '""')}"`;
}
