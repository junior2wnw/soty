import { validateAttachmentBudget } from './feedback-media.mjs';

export const PROJECT_CAPTURE_PROTOCOL = 'soty.feedback.capture.v1';
const id = value => typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/u.test(value);
const locator = value => typeof value === 'string' && value.length > 0 && value.length <= 4096 && value.isWellFormed()
  && !/[\u0000-\u001f\u007f]/u.test(value) && new TextEncoder().encode(value).byteLength <= 4096;
function record(value, keys) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || ![Object.prototype, null].includes(Object.getPrototypeOf(value))) return null;
  const fields = Object.getOwnPropertyDescriptors(value);
  if (Reflect.ownKeys(fields).length !== keys.length || !keys.every(key => fields[key]?.enumerable && Object.hasOwn(fields[key], 'value'))) return null;
  return Object.fromEntries(keys.map(key => [key, fields[key].value]));
}
export function projectCaptureRequest(input) {
  const value = record(input, ['schema', 'type', 'requestId', 'sourceId', 'projectId', 'contextRevision', 'kind']);
  if (!value || value.schema !== PROJECT_CAPTURE_PROTOCOL || value.type !== 'capture_request' || !id(value.requestId) || !id(value.sourceId)
    || !locator(value.projectId) || !Number.isSafeInteger(value.contextRevision) || value.contextRevision < 1
    || !['image', 'audio'].includes(value.kind)) return null;
  return Object.freeze(value);
}
function selectedPayload(input, kind) {
  if (!Array.isArray(input) || input.length < 1 || input.length > 3) throw new Error('capture_payload_invalid');
  const attachments = input.map(item => {
    const value = record(item, ['kind', 'name', 'mimeType', 'dataBase64']);
    if (!value || value.kind !== kind || typeof value.name !== 'string' || !value.name.isWellFormed()
      || value.name.length < 1 || value.name.length > 120 || /[\u0000-\u001f\u007f/\\]/u.test(value.name)
      || !(kind === 'image' ? ['image/png', 'image/jpeg', 'image/webp'] : ['audio/webm', 'audio/ogg']).includes(value.mimeType)) throw new Error('capture_payload_invalid');
    return Object.freeze(value);
  });
  validateAttachmentBudget(attachments);
  return Object.freeze(attachments);
}
const samePeer = (left, right) => left && right && left.approved === true && right.approved === true
  && left.window === right.window && left.origin === right.origin && left.sourceId === right.sourceId
  && left.appId === right.appId && left.accountId === right.accountId && left.generation === right.generation
  && left.slot === right.slot;
const origin = value => { try { const url = new URL(value); return url.origin === value && ['http:', 'https:'].includes(url.protocol); } catch { return false; } };
const peerSnapshot = value => value && Object.freeze({ approved: value.approved, window: value.window, origin: value.origin,
  sourceId: value.sourceId, appId: value.appId, accountId: value.accountId, generation: value.generation, slot: value.slot, title: value.title });

/** Host-only peer comes from the current approved app slot, never its message.
 * assertPeer checks original account/target/slot with the installed host.
 * capture MUST show an explicit Root picker/recorder/preview before returning
 * selected bytes. It does not submit/upload or create a feedback grant. */
export function mountProjectCaptureBridge({ view, readPeer, assertPeer, capture,preparePeer,onCaptureActive, timeoutMs = 180000 } = {}) {
  if (!view?.addEventListener || [readPeer, assertPeer, capture].some(value => typeof value !== 'function')
    || !Number.isSafeInteger(timeoutMs) || timeoutMs < 10 || timeoutMs > 180000) throw new Error('capture_host_invalid');
  let disposed = false, outstanding = null, seenPeer = null;
  const seen = new Set();
  const live = peer => !disposed && samePeer(readPeer(), peer);
  const send = (port, request, type, extra) => {
    try { port.postMessage({ schema: PROJECT_CAPTURE_PROTOCOL, type, requestId: request.requestId, sourceId: request.sourceId,
      projectId: request.projectId, contextRevision: request.contextRevision, kind: request.kind, ...extra }); } catch { /* A detached Source gets no data. */ }
  };
  const listener = event => {
    const request = projectCaptureRequest(event.data);let peer = peerSnapshot(readPeer());
    if (!request || !peer || peer.approved !== true || !origin(peer.origin) || !id(peer.appId) || !id(peer.sourceId)
      || !id(peer.accountId) || !Number.isSafeInteger(peer.generation) || !peer.slot || event.source !== peer.window
      || event.origin !== peer.origin || request.sourceId !== peer.sourceId || event.ports?.length !== 1) return;
    const port = event.ports[0];
    if (outstanding) { send(port, request, 'capture_error', { code: 'capture_busy' }); port.close(); return; }
    if (!samePeer(peer, seenPeer)) { seen.clear(); seenPeer = peer; }
    if (seen.has(request.requestId) || seen.size >= 32) {
      send(port, request, 'capture_error', { code: seen.has(request.requestId) ? 'capture_replayed' : 'capture_rate_limited' }); port.close(); return;
    }
    seen.add(request.requestId);
    const controller = new AbortController(), task = { controller, port, request, peer };
    outstanding = task;
    let active = true, completed = false,leaseActive=false;
    const releaseLease=()=>{if(!leaseActive)return;leaseActive=false;onCaptureActive?.(false);};
    const fail = code => {
      if (!active || completed) return;
      completed = true; active = false; controller.abort();
      releaseLease();
      send(port, request, 'capture_error', { code }); port.close();
    };
    task.fail = fail;
    const current = async () => {
      if (!active || !live(peer) || controller.signal.aborted) throw new Error('capture_context_changed');
      if (await assertPeer(peer, controller.signal) !== true || !active || !live(peer) || controller.signal.aborted) throw new Error('capture_context_changed');
    };
    port.onmessage = cancellation => {
      const value = record(cancellation.data, ['schema', 'type', 'requestId']);
      if (value?.schema === PROJECT_CAPTURE_PROTOCOL && value.type === 'capture_cancel' && value.requestId === request.requestId) fail('capture_cancelled');
    };
    port.start?.();
    const timer = setTimeout(() => fail('capture_timeout'), timeoutMs);
    let preparing=typeof preparePeer==='function';
    const guard = setInterval(() => { if(!preparing&&!live(peer)) fail('capture_context_changed'); }, 250);
    const operation = (async () => {
      try {
        if(preparing){const before=peer;
          if(await preparePeer(controller.signal)!==true||controller.signal.aborted||!active)throw new Error('capture_context_changed');
          const next=peerSnapshot(readPeer());
          if(!next||next.window!==before.window||next.origin!==before.origin||next.sourceId!==before.sourceId||next.appId!==before.appId
            ||next.accountId!==before.accountId)throw new Error('capture_context_changed');
          peer=next;task.peer=next;preparing=false;
        }
        await current();
        leaseActive=true;onCaptureActive?.(true);
        const selected = await capture({ appId: peer.appId, title: peer.title, request, signal: controller.signal });
        await current();
        const attachments = selectedPayload(selected, request.kind);
        await current();
        if (completed) return;
        completed = true; active = false;
        send(port, request, 'capture_result', { attachments }); port.close();
      } catch (error) {
        fail(error?.name === 'AbortError' ? 'capture_cancelled' : error?.message === 'capture_context_changed' ? 'capture_context_changed' : 'capture_failed');
      } finally {
        active = false; controller.abort();releaseLease(); clearTimeout(timer); clearInterval(guard); port.onmessage = null;
        if (outstanding === task) outstanding = null;
      }
    })();
    operation.catch(() => {});
  };
  view.addEventListener('message', listener);
  return Object.freeze({ dispose() {
    if (disposed) return; disposed = true; view.removeEventListener('message', listener);
    if (outstanding) {
      outstanding.fail('capture_context_changed');
    }
  } });
}

/** Source-side selection request. Caller derives parentOrigin from its reviewed
 * embedding profile, captures current account/project UI generation, and MUST
 * recheck native feedback context and ask explicit Send after this resolves. */
export function requestProjectCapture({ parent, parentOrigin, sourceId, projectId, contextRevision, kind, isCurrent,
  signal, timeoutMs = 180000, channelFactory = () => new MessageChannel() } = {}) {
  const request = projectCaptureRequest({ schema: PROJECT_CAPTURE_PROTOCOL, type: 'capture_request',
    requestId: crypto.randomUUID(), sourceId, projectId, contextRevision, kind });
  const error = code => Object.assign(new Error(code), { code });
  if (!request || !origin(parentOrigin) || !parent?.postMessage || typeof isCurrent !== 'function'
    || !Number.isSafeInteger(timeoutMs) || timeoutMs < 10 || timeoutMs > 180000) return Promise.reject(error('capture_unavailable'));
  if (signal?.aborted || !isCurrent()) return Promise.reject(error('capture_context_changed'));
  return new Promise((resolve, reject) => {
    const channel = channelFactory(); let settled = false, timer;
    const cleanup = () => { clearTimeout(timer); signal?.removeEventListener('abort', cancel); channel.port1.onmessage = null;
      channel.port1.close(); channel.port2.close(); };
    const finish = (failure, value) => { if (settled) return; settled = true; cleanup(); failure ? reject(failure) : resolve(value); };
    const cancel = () => {
      try { channel.port1.postMessage({ schema: PROJECT_CAPTURE_PROTOCOL, type: 'capture_cancel', requestId: request.requestId }); } catch { /* Already detached. */ }
      finish(error('capture_cancelled'));
    };
    channel.port1.onmessage = event => {
      if (settled) return;
      try {
        const type = Object.getOwnPropertyDescriptor(event.data || {}, 'type')?.value;
        const value = record(event.data, ['schema', 'type', 'requestId', 'sourceId', 'projectId', 'contextRevision', 'kind',
          ...(type === 'capture_result' ? ['attachments'] : ['code'])]);
        if (!value || value.schema !== PROJECT_CAPTURE_PROTOCOL || !['capture_result', 'capture_error'].includes(value.type)
          || ['requestId', 'sourceId', 'projectId', 'contextRevision', 'kind'].some(key => value[key] !== request[key])) throw error('capture_response_invalid');
        if (!isCurrent() || signal?.aborted) throw error('capture_context_changed');
        if (value.type === 'capture_error') {
          if (!['capture_busy', 'capture_cancelled', 'capture_context_changed', 'capture_timeout', 'capture_failed', 'capture_replayed', 'capture_rate_limited'].includes(value.code)) throw error('capture_response_invalid');
          throw error(value.code);
        }
        finish(null, selectedPayload(value.attachments, kind));
      } catch (failure) { finish(error(['capture_busy', 'capture_cancelled', 'capture_context_changed', 'capture_timeout', 'capture_failed',
        'capture_replayed', 'capture_rate_limited'].includes(failure?.code) ? failure.code : 'capture_response_invalid')); }
    };
    channel.port1.start?.();
    signal?.addEventListener('abort', cancel, { once: true });
    timer = setTimeout(() => {
      try { channel.port1.postMessage({ schema: PROJECT_CAPTURE_PROTOCOL, type: 'capture_cancel', requestId: request.requestId }); } catch { /* Detached. */ }
      finish(error('capture_timeout'));
    }, timeoutMs);
    try { parent.postMessage(request, parentOrigin, [channel.port2]); }
    catch { finish(error('capture_unavailable')); }
  });
}
