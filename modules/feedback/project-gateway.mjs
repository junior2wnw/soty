// Portable Source-owned coordinator. No Node imports, Root database or network
// discovery: the installed host supplies the reviewed native ACL/store ports.
export const PROJECT_FEEDBACK_PROTOCOL = 'soty.feedback.project.v1';
export const PROJECT_FEEDBACK_OPERATIONS = Object.freeze(['context', 'submit', 'list', 'get', 'reply', 'status', 'accept']);
const KEYS = Object.freeze({
  context: ['projectId'], submit: ['projectId', 'requestId', 'body', 'attachments'],
  list: ['projectId', 'limit', 'cursor'], get: ['projectId', 'ticketId'],
  reply: ['projectId', 'ticketId', 'requestId', 'expectedRevision', 'body'],
  status: ['projectId', 'ticketId', 'requestId', 'expectedRevision', 'status'],
  accept: ['projectId', 'ticketId', 'requestId', 'expectedRevision'],
});
const OPTIONAL = Object.freeze({ list: ['limit', 'cursor'] });
const WRITES = new Set(['submit', 'reply', 'status', 'accept']);
export class ProjectFeedbackError extends Error {
  constructor(code, status = 400) { super(code); this.name = 'ProjectFeedbackError'; this.code = code; this.status = status; }
}
const check = (ok, code = 'project_feedback_invalid_arguments', status) => { if (!ok) throw new ProjectFeedbackError(code, status); };
const identifier = value => typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/u.test(value);
// Native resource IDs are opaque values, not capability IDs or URL paths.
// Preserve Unicode, spaces and case. Longer legacy IDs need a Source-owned
// locator; no trim/rename or inference of permission is performed here.
const projectIdentifier = value => typeof value === 'string' && value.length > 0 && value.length <= 4096
  && value.isWellFormed() && !/[\u0000-\u001f\u007f]/u.test(value)
  && new TextEncoder().encode(value).byteLength <= 4096;
function capture(value, maxBytes) {
  let nodes = 0, stringBytes = 0;
  const visit = (entry, depth) => {
    check(++nodes <= 4096 && depth <= 18);
    if (entry === null || typeof entry === 'boolean') return entry;
    if (typeof entry === 'number') { check(Number.isFinite(entry)); return entry; }
    if (typeof entry === 'string') { check(entry.length <= maxBytes && entry.isWellFormed());
      stringBytes += new TextEncoder().encode(entry).byteLength;
      check(stringBytes <= maxBytes, 'project_feedback_payload_too_large', 413); return entry; }
    check(entry && typeof entry === 'object');
    const prototype = Object.getPrototypeOf(entry);
    check(Array.isArray(entry) ? prototype === Array.prototype : prototype === Object.prototype || prototype === null);
    const descriptors = Object.getOwnPropertyDescriptors(entry), result = Array.isArray(entry) ? [] : Object.create(null);
    for (const name of Reflect.ownKeys(descriptors)) {
      check(typeof name === 'string' && !['__proto__', 'prototype', 'constructor'].includes(name));
      if (Array.isArray(entry) && name === 'length') continue;
      const descriptor = descriptors[name]; check(Object.hasOwn(descriptor, 'value') && descriptor.enumerable);
      if (Array.isArray(entry)) check(/^(0|[1-9][0-9]*)$/u.test(name) && Number(name) < entry.length);
      result[name] = visit(descriptor.value, depth + 1);
    }
    if (Array.isArray(entry)) check(Object.keys(result).length === entry.length);
    return Object.freeze(result);
  };
  const result = visit(value, 0);
  check(new TextEncoder().encode(JSON.stringify(result)).byteLength <= maxBytes, 'project_feedback_payload_too_large', 413);
  return result;
}
function exact(value, keys, optional = []) {
  check(value && typeof value === 'object' && !Array.isArray(value)
    && Object.keys(value).every(key => keys.includes(key))
    && keys.filter(key => !optional.includes(key)).every(key => Object.hasOwn(value, key)));
}
function argumentsFor(op, input) {
  const args = capture(input, op === 'submit' ? 1500000 : 32768); exact(args, KEYS[op], OPTIONAL[op] || []);
  check(projectIdentifier(args.projectId));
  for (const key of ['requestId', 'ticketId']) if (Object.hasOwn(args, key)) check(identifier(args[key]));
  if (Object.hasOwn(args, 'expectedRevision')) check(Number.isSafeInteger(args.expectedRevision) && args.expectedRevision >= 1);
  if (Object.hasOwn(args, 'body')) check(typeof args.body === 'string' && args.body.trim().length > 0 && args.body.length <= 8000
    && !/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/u.test(args.body));
  if (op === 'submit') check(Array.isArray(args.attachments) && args.attachments.length <= 3);
  if (Object.hasOwn(args, 'limit')) check(Number.isSafeInteger(args.limit) && args.limit >= 1 && args.limit <= 50);
  if (Object.hasOwn(args, 'cursor')) check(args.cursor === null || typeof args.cursor === 'string' && args.cursor.length <= 768);
  if (op === 'status') check(['in_progress', 'needs_action', 'ready_to_check'].includes(args.status));
  return args;
}
const text = (value, maximum) => typeof value === 'string' && value.isWellFormed() && value.length <= maximum
  && !/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/u.test(value);
const timestamp = value => Number.isSafeInteger(value) && value >= 0;
function projection(op, reply, args, sourceId, canSupport, canWrite) {
  const bad = 'project_feedback_response_invalid';
  if (op === 'context') {
    const c = reply.context;
    exact(c, ['projectId', 'sourceId', 'title', 'canSubmit', 'canManage', 'recipientLabel', 'ticketVisibility', 'limits', 'capabilities']);
    check(c.projectId === args.projectId && c.sourceId === sourceId && text(c.title, 200)
      && c.canSubmit === canWrite && c.canManage === (canSupport && canWrite) && c.recipientLabel === 'Поддержка проекта'
      && c.ticketVisibility === 'reporter-and-support', bad, 502);
    exact(c.limits, ['bodyChars', 'totalAttachmentBytes', 'maxAttachments', 'maxAudioSeconds']);
    check(c.limits.bodyChars === 8000 && c.limits.totalAttachmentBytes === 1048576
      && c.limits.maxAttachments === 3 && c.limits.maxAudioSeconds === 120, bad, 502);
    exact(c.capabilities, ['text', 'voice', 'screenshot', 'asr']);
    check(c.capabilities.text === true && c.capabilities.voice === true && c.capabilities.screenshot === true
      && c.capabilities.asr === false, bad, 502);
    return;
  }
  if (WRITES.has(op)) {
    exact(reply.receipt, ['ticketId', 'revision', 'createdAt']);
    check(identifier(reply.receipt.ticketId) && Number.isSafeInteger(reply.receipt.revision) && reply.receipt.revision >= 1
      && timestamp(reply.receipt.createdAt), bad, 502);
  }
  if (op === 'list') check(reply.nextCursor === null || text(reply.nextCursor, 768), bad, 502);
  for (const ticket of op === 'list' ? reply.tickets : [reply.ticket]) {
    exact(ticket, ['id', 'projectId', 'body', 'status', 'revision', 'createdAt', 'updatedAt', 'attachments', 'messages',
      'canReply', 'canManage', 'canAccept']);
    check(identifier(ticket.id) && ticket.projectId === args.projectId && text(ticket.body, op === 'list' ? 240 : 8000)
      && ['received', 'in_progress', 'needs_action', 'ready_to_check', 'resolved'].includes(ticket.status)
      && Number.isSafeInteger(ticket.revision) && ticket.revision >= 1 && timestamp(ticket.createdAt) && timestamp(ticket.updatedAt)
      && typeof ticket.canReply === 'boolean' && ticket.canManage === (canSupport && canWrite) && typeof ticket.canAccept === 'boolean'
      && Array.isArray(ticket.attachments) && ticket.attachments.length <= 3 && Array.isArray(ticket.messages)
      && ticket.messages.length <= 128, bad, 502);
    if (!canWrite) check(!ticket.canReply && !ticket.canManage && !ticket.canAccept, bad, 502);
    if (WRITES.has(op)) check(ticket.id === reply.receipt.ticketId && ticket.revision >= reply.receipt.revision, bad, 502);
    if (args.ticketId) check(ticket.id === args.ticketId, bad, 502);
    let bytes = 0;
    for (const attachment of ticket.attachments) {
      exact(attachment, ['id', 'kind', 'name', 'mimeType', 'byteLength', ...(op === 'get' ? ['dataBase64'] : [])]);
      check(identifier(attachment.id) && ['image', 'audio'].includes(attachment.kind) && text(attachment.name, 160)
        && (attachment.kind === 'image' ? ['image/png', 'image/jpeg', 'image/webp']
          : ['audio/webm', 'audio/webm;codecs=opus', 'audio/ogg', 'audio/ogg;codecs=opus']).includes(attachment.mimeType)
        && Number.isSafeInteger(attachment.byteLength) && attachment.byteLength > 0
        && (bytes += attachment.byteLength) <= 1048576, bad, 502);
      if (op === 'get') check(typeof attachment.dataBase64 === 'string'
        && /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/u.test(attachment.dataBase64)
        && atob(attachment.dataBase64).length === attachment.byteLength, bad, 502);
    }
    let conversationBytes = 0;
    for (const message of ticket.messages) {
      exact(message, ['id', 'kind', 'body', 'createdAt']);
      check(identifier(message.id) && ['reporter', 'support'].includes(message.kind) && text(message.body, 8000)
        && timestamp(message.createdAt) && (conversationBytes += new TextEncoder().encode(message.body).byteLength) <= 196608, bad, 502);
    }
    if (op === 'list') check(ticket.messages.length === 0, bad, 502);
  }
}

/** withAuthority(request, callback) must resolve native authentication/project
 * ACL, call callback exactly once, and recheck its native authority after all
 * awaits. Store commits MUST independently enforce the same current native
 * ACL in their atomic Source transaction. This coordinator cannot make two
 * databases atomic; unknown ACKs keep the original requestId for reconciliation.
 * Verified actor references and context references stay private host objects. */
export function createProjectFeedbackGateway({ sourceId, captureVerifiedActor, withAuthority, read, commit,
  validateAttachments, maxOutstanding = 8, timeoutMs = 8000 } = {}) {
  check(identifier(sourceId) && [captureVerifiedActor, withAuthority, read, commit, validateAttachments].every(fn => typeof fn === 'function'),
    'project_feedback_host_required', 500);
  check(Number.isSafeInteger(maxOutstanding) && maxOutstanding >= 1 && maxOutstanding <= 32, 'project_feedback_host_required', 500);
  check(Number.isSafeInteger(timeoutMs) && timeoutMs >= 10 && timeoutMs <= 10000, 'project_feedback_host_required', 500);
  let outstanding = 0, closed = false;
  const authorities = new WeakSet();
  return Object.freeze({
    protocol: PROJECT_FEEDBACK_PROTOCOL,
    operations: PROJECT_FEEDBACK_OPERATIONS,
    async execute({ op, verifiedActor, args: input }) {
      check(!closed, 'project_feedback_unavailable', 503);
      check(PROJECT_FEEDBACK_OPERATIONS.includes(op));
      const args = argumentsFor(op, input);
      check(outstanding < maxOutstanding, 'project_feedback_busy', 503);
      outstanding++;
      let entered = false, active = true, poisoned = false, callbackSettled = false, result, callbackCompletion;
      const controller = new AbortController();
      let timer;
      const deadline = new Promise((_resolve, reject) => { timer = setTimeout(() => {
        active = false; poisoned = true; controller.abort(); reject(new ProjectFeedbackError('project_feedback_timeout', 504));
      }, timeoutMs); });
      const operation = (async () => { try {
        const actor = await captureVerifiedActor(verifiedActor);
        check(active && !closed, 'project_feedback_authority_invalid', 500);
        check(actor && typeof actor === 'object' && Object.isFrozen(actor), 'project_feedback_authentication_required', 401);
        const callback = context => {
          if (!active || entered) {
            poisoned = true;
            const rejected = Promise.reject(new ProjectFeedbackError('project_feedback_authority_invalid', 500));
            // A native timer may ignore its return. Preserve rejection for a
            // proper caller without crashing the Source on ignored late work.
            rejected.catch(() => {});
            return rejected;
          }
          entered = true;
          callbackCompletion = (async () => {
            check(context && typeof context === 'object' && Object.isFrozen(context) && context.projectId === args.projectId
              && context.sourceId === sourceId && context.verifiedActor === actor
              && context.canRead === true && typeof context.canSupport === 'boolean' && typeof context.canWrite === 'boolean'
              && typeof context.assertCurrent === 'function', 'project_feedback_authority_invalid', 500);
            authorities.add(context);
            const current = async () => {
              check(active && !poisoned && !closed && authorities.has(context), 'project_feedback_authority_invalid', 500);
              check(await context.assertCurrent(controller.signal) === true, 'project_feedback_access_denied', 403);
              check(active && !poisoned && !closed, 'project_feedback_authority_invalid', 500);
            };
            await current();
            if (WRITES.has(op)) check(context.canWrite, 'project_feedback_read_only', 403);
            if (op === 'status') check(context.canSupport, 'project_feedback_support_required', 403);
            if (op === 'submit') {
              // Decoder/parser is host-reviewed; a manifest cannot replace it.
              await validateAttachments(args.attachments);
              await current();
            }
            const value = await (WRITES.has(op) ? commit : read)({ protocol: PROJECT_FEEDBACK_PROTOCOL,
              op, args, authority: context, assertCurrent: current, signal: controller.signal });
            await current();
            // A lawful 1MiB attachment expands in base64, and the bounded
            // conversation can double when JSON escapes quotes/backslashes.
            // Individual media/conversation limits still apply below.
            const reply = capture(value, op === 'get' ? 2097152 : 262144);
            exact(reply, op === 'context' ? ['context'] : op === 'list' ? ['tickets', 'nextCursor']
              : WRITES.has(op) ? ['requestId', 'replayed', 'receipt', 'ticket'] : ['ticket']);
            if (WRITES.has(op)) check(reply.requestId === args.requestId && typeof reply.replayed === 'boolean', 'project_feedback_receipt_invalid', 502);
            if (op === 'list') check(Array.isArray(reply.tickets) && reply.tickets.length <= (args.limit || 30), 'project_feedback_response_invalid', 502);
            projection(op, reply, args, sourceId, context.canSupport, context.canWrite);
            // No host/native actor, authority, key, worker instructions or
            // project data outside the closed feedback projection is returned.
            result = reply;
            return reply;
          })().finally(() => { callbackSettled = true; });
          callbackCompletion.catch(() => {});
          return callbackCompletion;
        };
        await withAuthority({ protocol: PROJECT_FEEDBACK_PROTOCOL, op, actor, projectId: args.projectId, signal: controller.signal }, callback);
        check(entered && callbackCompletion && callbackSettled && active && !poisoned, 'project_feedback_authority_invalid', 500);
        await callbackCompletion;
        check(active && !poisoned && !closed, 'project_feedback_authority_invalid', 500);
        return result;
      } finally {
        active = false;
        // A host that returns before callback completion cannot release a slot
        // while the underlying store action is still running.
        if (callbackCompletion) await callbackCompletion.catch(() => {});
        outstanding--;
        clearTimeout(timer);
      } })();
      operation.catch(() => {});
      return await Promise.race([operation, deadline]);
    },
    close() { closed = true; },
  });
}
