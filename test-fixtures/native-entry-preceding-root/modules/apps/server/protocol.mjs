export const APPS_SCHEMA = 'soty.local-app.v1';
export const CHANNEL_SCHEMA = 'soty.apps-channel.v1';
export const CHUNK_BYTES = 48 * 1024;
export const FRAME_BYTES = 72 * 1024;
export const LIMITS = Object.freeze({ streams: 32, requestBytes: 8 * 1024 * 1024, responseBytes: 64 * 1024 * 1024, headMs: 30_000, idleMs: 120_000, ackMs: 30_000 });
export class AppsError extends Error {
  constructor(code, status = 400) { super(code); this.code = code; this.status = status; }
}
export function assertApps(condition, code, status = 400) { if (!condition) throw new AppsError(code, status); }
export function textId(value) { assertApps(typeof value === 'string' && /^[A-Za-z0-9_.:-]{1,180}$/u.test(value), 'invalid_id'); return value; }
export function appId(value) { assertApps(typeof value === 'string' && /^app-[a-f0-9]{32}$/u.test(value), 'invalid_app_id'); return value; }
export function appName(value) { assertApps(typeof value === 'string' && value.trim().length > 0 && value.trim().length <= 64 && !/[\u0000-\u001f\u007f]/u.test(value), 'invalid_app_name'); return value.trim(); }
export function appPort(value, blocked = []) { assertApps(Number.isSafeInteger(value) && value >= 1024 && value <= 65535 && ![49424, ...blocked].includes(value), 'invalid_app_port'); return value; }
export function requestPath(value) {
  assertApps(typeof value === 'string' && value.length <= 8192 && value.startsWith('/') && !value.startsWith('//') && !/[\\\u0000-\u0020\u007f]/u.test(value), 'invalid_app_path');
  let decoded; try { decoded = decodeURIComponent(value.split('?')[0]); } catch { throw new AppsError('invalid_app_path'); }
  assertApps(!decoded.startsWith('//') && !/[\\\u0000-\u001f\u007f]/u.test(decoded), 'invalid_app_path');
  return value;
}
// Shared by admission and owner inspection. An unsafe legacy entry may still
// be inspected/disabled, but must never become a launch or share target.
export function runtimePath(value) {
  const path = requestPath(value);
  const decoded = decodeURIComponent(new URL(path, 'https://runtime.invalid').pathname);
  const normalized = new URL(decodeURIComponent(path.split('?', 1)[0]), 'https://runtime.invalid').pathname;
  assertApps(!decoded.startsWith('//') && !normalized.startsWith('//'), 'invalid_app_path');
  assertApps(![decoded, normalized].some(value => value.startsWith('/_soty/') || value === '/_soty'), 'app_reserved_path', 404);
  return path;
}
export function normalizeManifest(value, blocked = []) {
  assertApps(value && typeof value === 'object' && !Array.isArray(value), 'invalid_app_manifest');
  assertApps(Object.keys(value).every(key => ['schema', 'name', 'port', 'entryPath'].includes(key)) && value.schema === APPS_SCHEMA, 'invalid_app_manifest');
  const entryPath = requestPath(value.entryPath ?? '/');
  assertApps(!entryPath.startsWith('/_soty/'), 'reserved_app_path');
  return { schema: APPS_SCHEMA, name: appName(value.name), port: appPort(value.port, blocked), entryPath };
}
export function cleanGrants(value = {}) {
  assertApps(value && typeof value === 'object' && !Array.isArray(value), 'invalid_grants');
  assertApps(Object.keys(value).every(key => ['accountIds', 'communityIds'].includes(key)), 'invalid_grants');
  const list = input => { assertApps(Array.isArray(input) && input.length <= 64, 'invalid_grants'); return [...new Set(input.map(textId))].sort(); };
  return { accountIds: list(value.accountIds ?? []), communityIds: list(value.communityIds ?? []) };
}
export function connectorKey(identity) { return [textId(identity.linkId), textId(identity.hostDeviceId), textId(identity.connectorId)].join('|'); }
const REQUEST_HEADERS = new Set(['accept', 'accept-language', 'content-type', 'content-encoding', 'if-none-match', 'if-modified-since', 'range', 'if-range']);
const RESPONSE_HEADERS = new Set(['content-type', 'content-encoding', 'content-language', 'content-disposition', 'etag', 'last-modified', 'accept-ranges', 'content-range', 'vary']);
const UPGRADE_HEADERS = new Set(['sec-websocket-accept', 'sec-websocket-protocol', 'sec-websocket-extensions']);
export function cleanHeaders(headers, direction = 'request') {
  const allow = direction === 'response' ? RESPONSE_HEADERS : direction === 'upgrade' ? UPGRADE_HEADERS : REQUEST_HEADERS;
  const result = {}; let bytes = 0;
  for (const [key, raw] of Object.entries(headers || {})) {
    const name = key.toLowerCase();
    if (!allow.has(name) || typeof raw !== 'string' || /[\r\n\u0000]/u.test(raw)) continue;
    bytes += name.length + raw.length; assertApps(bytes <= 16 * 1024, 'app_headers_too_large'); result[name] = raw;
  }
  return result;
}

// One structural parser for both the compatibility limiter and the streaming
// relay. No extensions are negotiated. Data payload is never retained/unmasked;
// only a header and a complete control frame fit in the 139-byte workspace.
export function createWebSocketFrameParser({ masked, maxMessageBytes = 1024 * 1024 } = {}) {
  assertApps(typeof masked === 'boolean' && Number.isSafeInteger(maxMessageBytes) && maxMessageBytes > 0, 'invalid_websocket_parser');
  const storage = Buffer.alloc(139);
  let used = 0, wanted = 2, remaining = 0, phase = 'header', opcode = 0, headerBytes = 0, controlUsed = 0;
  let fragmented = false, messageBytes = 0;
  const reset = () => { used = 0; wanted = 2; remaining = 0; phase = 'header'; };
  return {
    get atBoundary() { return phase === 'header' && used === 0; },
    // Returned byte views are borrowed until the next read. Consumers must
    // finish inspecting/copying them before yielding or calling read again.
    read(bytes, offset = 0, maxBytes = bytes.length) {
      assertApps(Buffer.isBuffer(bytes) && Number.isSafeInteger(offset) && offset >= 0 && offset < bytes.length && Number.isSafeInteger(maxBytes) && maxBytes > 0, 'invalid_websocket_input');
      const initial = offset, started = phase === 'header' && used === 0;
      if (phase === 'header') {
        while (offset < bytes.length && used < wanted) {
          const count = Math.min(wanted - used, bytes.length - offset);
          bytes.copy(storage, used, offset, offset + count); used += count; offset += count;
          if (used !== wanted) break;
          if (wanted === 2) {
            const encoded = storage[1] & 127;
            assertApps((storage[0] & 0x70) === 0 && Boolean(storage[1] & 0x80) === masked, 'app_websocket_invalid_frame');
            wanted = 2 + (encoded === 126 ? 2 : encoded === 127 ? 8 : 0) + (masked ? 4 : 0);
          }
        }
        if (used < wanted) return { consumed: offset - initial, started, complete: false, bytes: null };
        const fin = Boolean(storage[0] & 0x80), encoded = storage[1] & 127;
        opcode = storage[0] & 15;
        let length = encoded;
        if (encoded === 126) { length = storage.readUInt16BE(2); assertApps(length >= 126, 'app_websocket_invalid_frame'); }
        if (encoded === 127) { const wide = storage.readBigUInt64BE(2); assertApps(wide >= 65536n && wide <= BigInt(maxMessageBytes), 'app_websocket_message_too_large'); length = Number(wide); }
        if (opcode >= 8) assertApps([8, 9, 10].includes(opcode) && fin && length <= 125, 'app_websocket_invalid_control');
        else {
          assertApps([0, 1, 2].includes(opcode) && (opcode === 0 ? fragmented : !fragmented), 'app_websocket_invalid_fragment');
          if (opcode !== 0) messageBytes = 0;
          messageBytes += length;
          assertApps(messageBytes <= maxMessageBytes, 'app_websocket_message_too_large');
          fragmented = !fin;
        }
        headerBytes = used; remaining = length; controlUsed = 0;
        phase = opcode >= 8 ? 'control' : 'data';
        const complete = remaining === 0, control = opcode >= 8;
        const output = !control || complete ? storage.subarray(0, headerBytes) : null;
        if (complete) reset();
        return { consumed: offset - initial, started, complete, opcode, control, headerBytes, bytes: output };
      }
      const count = Math.min(remaining, bytes.length - offset, phase === 'data' ? maxBytes : remaining);
      remaining -= count;
      const control = phase === 'control', complete = remaining === 0;
      let output;
      if (control) {
        bytes.copy(storage, headerBytes + controlUsed, offset, offset + count); controlUsed += count;
        output = complete ? storage.subarray(0, headerBytes + controlUsed) : null;
      } else output = bytes.subarray(offset, offset + count);
      if (complete) reset();
      return { consumed: count, started: false, complete, opcode, control, headerBytes, bytes: output };
    },
  };
}

export function createWebSocketLimiter(options) {
  const parser = createWebSocketFrameParser(options);
  return { push(bytes) { let offset = 0; while (offset < bytes.length) offset += parser.read(bytes, offset).consumed; } };
}
