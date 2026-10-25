export class ConnectorError extends Error {
  constructor(code, status = 502, options = {}) {
    super(code);
    this.name = 'ConnectorError';
    this.code = code;
    this.statusCode = status;
    this.httpStatus = status;
    this.retryable = options.retryable ?? status >= 500;
    this.retryAfter = options.retryAfter;
    this.providerStatus = options.providerStatus;
  }
}

export function safeError(error, signal) {
  if (error instanceof ConnectorError) return error;
  if (signal?.aborted) return signal.reason instanceof ConnectorError ? signal.reason : new ConnectorError('request_canceled', 499, { retryable: false });
  return new ConnectorError('upstream_unavailable', 502);
}

export function retryAfterMs(value, now = Date.now()) {
  if (!value) return 0;
  if (/^\d+(?:\.\d+)?$/.test(value)) return Math.min(86_400_000, Math.ceil(Number(value) * 1000));
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? Math.min(86_400_000, Math.max(0, parsed - now)) : 0;
}

export async function boundedJson(response, maxBytes = 8 * 1024 * 1024) {
  const reader = response.body?.getReader();
  if (!reader) throw new ConnectorError('upstream_empty_response');
  let size = 0;
  const chunks = [];
  try {
    while (true) {
      const item = await reader.read();
      if (item.done) break;
      size += item.value.length;
      if (size > maxBytes) throw new ConnectorError('upstream_response_too_large');
      chunks.push(item.value);
    }
    try { return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks))); }
    catch { throw new ConnectorError('upstream_invalid_json'); }
  } finally { await reader.cancel().catch(() => {}); }
}
