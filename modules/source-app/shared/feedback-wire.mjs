export const SOURCE_FEEDBACK_LIMITS = Object.freeze({ bodyChars: 8000, totalAttachmentBytes: 1048576, maxAttachments: 3, maxAudioSeconds: 120 });
export class SourceFeedbackClientError extends Error {
  constructor(code, status = 503) { super(code); this.code = code; this.status = status; }
}
const fail = () => { throw new SourceFeedbackClientError('source_feedback_input_invalid', 400); };
const need = value => { if (!value) fail(); };
export function feedbackFields(input, required, optional = []) {
  need(input && typeof input === 'object' && !Array.isArray(input) && [Object.prototype, null].includes(Object.getPrototypeOf(input)));
  const descriptors = Object.getOwnPropertyDescriptors(input);
  need(required.every(key => Object.hasOwn(descriptors, key)) && Reflect.ownKeys(descriptors).every(key => typeof key === 'string'
    && [...required, ...optional].includes(key) && descriptors[key].enumerable && Object.hasOwn(descriptors[key], 'value')));
  return Object.fromEntries(Object.entries(descriptors).map(([key, field]) => [key, field.value]));
}
export const feedbackId = value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{1,128}$/u.test(value);
const requestId = value => feedbackId(value) && value.length >= 16;
const text = (value, max, empty = false) => typeof value === 'string' && value.isWellFormed() && value.length <= max
  && (empty || value.trim().length > 0) && !/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/u.test(value);
function attachments(input) {
  need(Array.isArray(input) && input.length <= SOURCE_FEEDBACK_LIMITS.maxAttachments);
  const descriptors = Object.getOwnPropertyDescriptors(input); need(Reflect.ownKeys(descriptors).length === input.length + 1);
  let total = 0;
  return Array.from({ length: input.length }, (_, index) => {
    need(descriptors[index] && Object.hasOwn(descriptors[index], 'value'));
    const value = feedbackFields(descriptors[index].value, ['kind', 'name', 'mimeType', 'dataBase64']);
    need(text(value.name, 160) && !/[/\\\u0000-\u001f]/u.test(value.name));
    const image = ['image/png', 'image/jpeg', 'image/webp'].includes(value.mimeType);
    const audio = ['audio/webm', 'audio/webm;codecs=opus', 'audio/ogg', 'audio/ogg;codecs=opus'].includes(value.mimeType);
    need(value.kind === (image ? 'image' : audio ? 'audio' : null) && typeof value.dataBase64 === 'string'
      && value.dataBase64.length > 0 && value.dataBase64.length <= 4 * Math.ceil(SOURCE_FEEDBACK_LIMITS.totalAttachmentBytes / 3)
      && value.dataBase64.length % 4 === 0 && /^[A-Za-z0-9+/]+={0,2}$/u.test(value.dataBase64));
    const padding = value.dataBase64.endsWith('==') ? 2 : value.dataBase64.endsWith('=') ? 1 : 0;
    total += value.dataBase64.length / 4 * 3 - padding; need(total <= SOURCE_FEEDBACK_LIMITS.totalAttachmentBytes); return value;
  });
}
export function feedbackInput(operation, input = {}) {
  if (operation === 'context') return feedbackFields(input, []);
  if (operation === 'list') {
    const value = feedbackFields(input, [], ['limit', 'cursor']);
    if (typeof value.limit === 'string' && /^[1-9][0-9]?$/u.test(value.limit)) value.limit = Number(value.limit);
    need(value.limit === undefined || Number.isSafeInteger(value.limit) && value.limit >= 1 && value.limit <= 50);
    need(value.cursor === undefined || typeof value.cursor === 'string' && /^[A-Za-z0-9_-]{1,2000}$/u.test(value.cursor)); return value;
  }
  if (operation === 'get') { const value = feedbackFields(input, ['ticketId']); need(feedbackId(value.ticketId)); return value; }
  if (operation === 'submit') {
    const value = feedbackFields(input, ['requestId', 'body', 'attachments']); need(requestId(value.requestId) && text(value.body, 8000, true));
    value.attachments = attachments(value.attachments); need(value.body.trim().length > 0 || value.attachments.length > 0); return value;
  }
  need(['reply', 'status', 'accept'].includes(operation));
  const value = feedbackFields(input, ['ticketId', 'requestId', 'expectedRevision', ...(operation === 'reply' ? ['body'] : operation === 'status' ? ['status'] : [])]);
  need(feedbackId(value.ticketId) && requestId(value.requestId) && Number.isSafeInteger(value.expectedRevision) && value.expectedRevision >= 1);
  if (operation === 'reply') need(text(value.body, 8000));
  if (operation === 'status') need(['in_progress', 'needs_action', 'ready_to_check'].includes(value.status)); return value;
}
export function feedbackContext(input) {
  const value = feedbackFields(input, ['schema', 'bindingDigest', 'ready', 'canSubmit', 'recipientLabel', 'limits', 'capabilities']);
  need(value.schema === 'soty.source-feedback.context.v1' && /^[a-f0-9]{64}$/u.test(value.bindingDigest)
    && typeof value.ready === 'boolean' && typeof value.canSubmit === 'boolean' && text(value.recipientLabel, 160));
  value.limits = feedbackFields(value.limits, Object.keys(SOURCE_FEEDBACK_LIMITS));
  for (const key of Object.keys(SOURCE_FEEDBACK_LIMITS)) need(Number.isSafeInteger(value.limits[key]) && value.limits[key] > 0 && value.limits[key] <= SOURCE_FEEDBACK_LIMITS[key]);
  value.capabilities = feedbackFields(value.capabilities, ['text', 'voice', 'screenshot', 'asr']);
  need(Object.values(value.capabilities).every(item => typeof item === 'boolean') && value.capabilities.asr === false); return value;
}
function ticket(input) {
  const value = feedbackFields(input, ['id', 'revision', 'status', 'body', 'createdAt', 'updatedAt', 'canReply', 'canManage', 'canAccept', 'attachments', 'messages']);
  need(feedbackId(value.id) && Number.isSafeInteger(value.revision) && value.revision >= 1
    && ['received', 'in_progress', 'needs_action', 'ready_to_check', 'resolved'].includes(value.status) && text(value.body, 8000, true)
    && Number.isSafeInteger(value.createdAt) && value.createdAt > 0 && Number.isSafeInteger(value.updatedAt) && value.updatedAt >= value.createdAt
    && ['canReply', 'canManage', 'canAccept'].every(key => typeof value[key] === 'boolean') && Array.isArray(value.attachments)
    && value.attachments.length <= 3 && Array.isArray(value.messages) && value.messages.length <= 128);
  value.attachments = value.attachments.map(item => {
    const media = feedbackFields(item, ['kind', 'name', 'mimeType', 'byteLength'], ['dataBase64', 'width', 'height', 'durationMs']);
    need(['image', 'audio'].includes(media.kind) && text(media.name, 160) && text(media.mimeType, 80) && Number.isSafeInteger(media.byteLength)
      && media.byteLength > 0 && media.byteLength <= SOURCE_FEEDBACK_LIMITS.totalAttachmentBytes);
    if (media.dataBase64 !== undefined) attachments([{ kind: media.kind, name: media.name, mimeType: media.mimeType, dataBase64: media.dataBase64 }]);
    for (const key of ['width', 'height']) if (media[key] !== undefined) need(Number.isSafeInteger(media[key]) && media[key] >= 1 && media[key] <= 8192);
    if (media.durationMs !== undefined) need(Number.isSafeInteger(media.durationMs) && media.durationMs > 0 && media.durationMs <= 120000); return media;
  });
  need(value.attachments.reduce((size, media) => size + media.byteLength, 0) <= SOURCE_FEEDBACK_LIMITS.totalAttachmentBytes);
  value.messages = value.messages.map(item => { const message = feedbackFields(item, ['id', 'kind', 'body', 'createdAt']);
    need(feedbackId(message.id) && ['reporter', 'support'].includes(message.kind) && text(message.body, 8000)
      && Number.isSafeInteger(message.createdAt) && message.createdAt > 0); return message; });
  need(new TextEncoder().encode(JSON.stringify(value.messages)).length <= 196608); return value;
}
/** Canonical Source feedback DTO. Private Native principals/keys/support grants
 * stay in Source storage; a Root owner field has no place in this wire. */
export function feedbackOutput(operation, input) {
  if (operation === 'context') return feedbackContext(input);
  if (operation === 'list') { const value = feedbackFields(input, ['tickets', 'nextCursor']);
    need(Array.isArray(value.tickets) && value.tickets.length <= 50 && (value.nextCursor === null
      || typeof value.nextCursor === 'string' && /^[A-Za-z0-9_-]{1,2000}$/u.test(value.nextCursor)));
    value.tickets = value.tickets.map(ticket); return value; }
  if (operation === 'get') { const value = feedbackFields(input, ['ticket']); value.ticket = ticket(value.ticket); return value; }
  const value = feedbackFields(input, ['requestId', 'replayed', 'receipt', 'ticket']); need(requestId(value.requestId) && typeof value.replayed === 'boolean');
  value.receipt = feedbackFields(value.receipt, ['ticketId', 'revision', 'createdAt']); value.ticket = ticket(value.ticket);
  need(value.receipt.ticketId === value.ticket.id && value.receipt.revision === value.ticket.revision
    && Number.isSafeInteger(value.receipt.createdAt) && value.receipt.createdAt > 0); return value;
}
