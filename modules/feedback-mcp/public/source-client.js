import { createSourceFrameFetch } from './source-frame-fetch.js';

const reads = new Set(['projects_list', 'integration_get', 'tickets_list', 'ticket_get']);
const writes = new Set(['project_create', 'ticket_submit', 'ticket_reply', 'ticket_status', 'ticket_accept']);
const failure = (code, status = 503) => Object.assign(new Error(code), { code, status });
const id = () => 'feedback-ui-' + crypto.randomUUID();
function canonical(value) {
  if (value === null || typeof value === 'boolean' || typeof value === 'string') return JSON.stringify(value);
  if (typeof value === 'number' && Number.isFinite(value)) return JSON.stringify(value);
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  if (value && Object.getPrototypeOf(value) === Object.prototype)
    return '{' + Object.keys(value).sort().map(key => JSON.stringify(key) + ':' + canonical(value[key])).join(',') + '}';
  throw failure('invalid_arguments', 400);
}
async function hash(text) {
  return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text))), byte => byte.toString(16).padStart(2, '0')).join('');
}

/** Uses only the current Source frame and HttpOnly session. No user/owner proof
 * is accepted from browser JSON; the maintained BFF rechecks every operation. */
export function createFeedbackSourceClient(options = {}) {
  const sourceFetch = createSourceFrameFetch({ fetch: options.fetch ?? globalThis.fetch,
    href: options.href ?? globalThis.location?.href, shortSessionHandoff: true });
  let contextRef, acquiring;
  const pending = new Map();
  async function request(path, input) {
    const body = JSON.stringify(input);
    if (new TextEncoder().encode(body).length > 65536) throw failure('source_app_payload_limit', 413);
    const response = await sourceFetch('/api/embed/' + path, { method: 'POST', credentials: 'same-origin',
      redirect: 'error', referrerPolicy: 'no-referrer', headers: { 'content-type': 'application/json', accept: 'application/json' }, body });
    if (response.redirected || !/^application\/json(?:;|$)/iu.test(response.headers.get('content-type') || '')) throw failure('source_app_response_invalid');
    const reader = response.body.getReader(), parts = []; let size = 0;
    try { for (;;) {
      const next = await reader.read(); if (next.done) break;
      size += next.value.length; if (size > 65536) throw failure('source_app_response_invalid'); parts.push(next.value);
    } } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
    const bytes = new Uint8Array(size); let offset = 0;
    for (const part of parts) { bytes.set(part, offset); offset += part.length; }
    let value; try { value = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)); }
    catch { throw failure('source_app_response_invalid'); }
    if (path === 'session-continue' && response.ok && value.schema === 'soty.source-session-continuation.v1' && value.ready === true) return value;
    if (!response.ok || value.ok !== true) throw failure(value.error?.code || 'source_app_unknown', response.status);
    const data = value.data;
    if (path === 'query' && data?.nextCursor === '') data.nextCursor = null;
    if (path === 'query' && data?.messagesPage?.nextOffset === -1) data.messagesPage.nextOffset = null;
    return data;
  }
  async function acquire() {
    if (contextRef) return;
    if (!acquiring) acquiring = (async () => {
      await request('session-continue', { requestId: id() });
      const context = await request('query', { requestId: id(), input: { operation: 'soty.current_context' } });
      if (!/^[A-Za-z0-9_-]{43}$/u.test(context?.context?.ref)) throw failure('source_workspace_context_denied');
      contextRef = context.context.ref;
    })().finally(() => { acquiring = undefined; });
    await acquiring;
  }
  async function read(operation, args) {
    await acquire(); return request('query', { requestId: id(), contextRef, input: { ...args, operation } });
  }
  async function finish(entry, proof) {
    if (proof?.requestId !== entry.requestId || proof.inputDigest !== entry.digest || proof.outcome !== 'committed'
      || typeof proof.receipt?.id !== 'string') throw failure('source_app_effect_unknown');
    // Read the actual current domain row. A successful invoke alone is not a
    // substitute for the normal UI project/ticket DTO.
    if (entry.input.operation === 'project_create') {
      const value = await read('integration_get', { projectId: proof.receipt.id });
      return { project: value.project, replayed: Boolean(proof.replayed) };
    }
    return read('ticket_get', { projectId: entry.input.projectId, ticketId: proof.receipt.id });
  }
  return Object.freeze({ async call(operation, args = {}) {
    if (!args || Object.getPrototypeOf(args) !== Object.prototype || Object.hasOwn(args, 'operation')) throw failure('invalid_arguments', 400);
    if (operation === 'attachment_get') {
      if (Object.keys(args).sort().join(',') !== 'attachmentId,projectId,ticketId') throw failure('invalid_arguments', 400);
      let offset = 0, total, metadata, dataBase64 = '';
      do {
        const part = await read('attachment_chunk', { ...args, offset });
        if (part.offset !== offset || !Number.isInteger(part.totalChars) || part.totalChars <= 0 || part.totalChars > 1398104
          || part.totalChars % 4 !== 0 || typeof part.dataBase64 !== 'string' || !/^[A-Za-z0-9+/]+={0,2}$/u.test(part.dataBase64)
          || part.dataBase64.length === 0 || part.dataBase64.length > 49152 || part.dataBase64.length % 4 !== 0
          || part.attachment?.id !== args.attachmentId || offset + part.dataBase64.length > part.totalChars
          || (offset + part.dataBase64.length === part.totalChars ? part.nextOffset !== null : part.nextOffset !== offset + part.dataBase64.length)
          || total !== undefined && (total !== part.totalChars || canonical(metadata) !== canonical(part.attachment)))
          throw failure('source_attachment_changed');
        total = part.totalChars; metadata = part.attachment; dataBase64 += part.dataBase64; offset += part.dataBase64.length;
      } while (offset < total);
      return { attachment: { ...metadata, dataBase64 } };
    }
    if (reads.has(operation)) return read(operation, args);
    if (!writes.has(operation)) throw failure('feedback_source_operation_denied', 403);
    const { requestId, ...parameters } = args;
    if (typeof requestId !== 'string' || !/^[A-Za-z0-9_.:-]{16,128}$/u.test(requestId)) throw failure('invalid_arguments', 400);
    const input = JSON.parse(canonical({ ...parameters, operation })), serialized = canonical(input);
    let entry = pending.get(requestId);
    if (entry && entry.serialized !== serialized) throw failure('request_conflict', 409);
    await acquire();
    if (!entry) {
      if (pending.size >= 128) throw failure('source_capacity', 503);
      entry = { requestId, input, serialized, contextRef, digest: await hash(serialized) };
      // Reserve before the first await which can send an effect.
      const concurrent = pending.get(requestId);
      if (concurrent) { if (concurrent.serialized !== serialized) throw failure('request_conflict', 409); entry = concurrent; }
      else pending.set(requestId, entry);
    }
    if (!entry.flight) entry.flight = (async () => {
      let proof;
      if (!entry.sent) {
        entry.sent = true;
        try { proof = await request('invoke', { requestId, contextRef: entry.contextRef, input: entry.input }); }
        catch { /* Only the original receipt may resolve an unknown write. */ }
      }
      if (!proof) proof = await request('receipt', { requestId, contextRef: entry.contextRef, input: { inputDigest: entry.digest } });
      return finish(entry, proof);
    })().finally(() => { entry.flight = undefined; });
    return entry.flight;
  } });
}
