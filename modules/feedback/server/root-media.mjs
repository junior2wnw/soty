import { inspectMaterialVideo } from '../../materials/server/media.mjs';
import { FeedbackMediaError, FEEDBACK_MEDIA_LIMITS, validateFeedbackAttachments } from './media.mjs';
import { FEEDBACK_VIDEO_LIMITS } from './profile.mjs';

const check = (ok, code = 'feedback_attachment_invalid') => { if (!ok) throw new FeedbackMediaError(code); };

// Root-only extension: the frozen Source SDK keeps its existing validator and dependencies.
export function validateRootFeedbackAttachments(input, { limits: configured = {} } = {}) {
  validateFeedbackAttachments([], { limits: configured });
  const limits = { ...FEEDBACK_MEDIA_LIMITS, ...configured };
  check(Array.isArray(input) && input.length <= limits.maxAttachments, 'feedback_attachment_limit');
  const entries = Object.getOwnPropertyDescriptors(input);
  check(Reflect.ownKeys(entries).length === input.length + 1
    && Array.from({ length: input.length }, (_, index) => entries[index] && Object.hasOwn(entries[index], 'value')).every(Boolean));
  const output = []; let total = 0;
  for (let index = 0; index < input.length; index++) {
    const value = entries[index].value;
    check(value && typeof value === 'object' && !Array.isArray(value) && [Object.prototype, null].includes(Object.getPrototypeOf(value)));
    const fields = Object.getOwnPropertyDescriptors(value);
    if (fields.kind?.value !== 'video') {
      const media = validateFeedbackAttachments([value], { limits })[0]; total += media.byteLength;
      check(total <= limits.totalAttachmentBytes, 'feedback_attachment_limit'); output.push(media); continue;
    }
    const keys = ['kind', 'name', 'mimeType', 'dataBase64'];
    check(Reflect.ownKeys(fields).length === keys.length && keys.every(key => fields[key]?.enumerable && Object.hasOwn(fields[key], 'value')));
    const item = Object.fromEntries(keys.map(key => [key, fields[key].value]));
    check(typeof item.name === 'string' && item.name.trim().length > 0 && item.name.length <= 160
      && !/[\u0000-\u001f\u007f/\\]/u.test(item.name) && item.name.isWellFormed());
    check(typeof item.mimeType === 'string' && item.mimeType.toLowerCase() === 'video/webm', 'feedback_media_unsupported');
    const encoded = item.dataBase64;
    check(typeof encoded === 'string' && encoded.length > 0 && encoded.length <= 4 * Math.ceil(limits.totalAttachmentBytes / 3), 'feedback_attachment_limit');
    check(encoded.length % 4 === 0 && /^[A-Za-z0-9+/]+={0,2}$/u.test(encoded));
    const bytes = Buffer.from(encoded, 'base64'); check(bytes.toString('base64') === encoded);
    total += bytes.length; check(bytes.length > 0 && total <= limits.totalAttachmentBytes, 'feedback_attachment_limit');
    let metadata;
    try { metadata = inspectMaterialVideo(bytes, 'video/webm', { fileBytes: limits.totalAttachmentBytes,
      videoSeconds: FEEDBACK_VIDEO_LIMITS.maxVideoSeconds, videoSide: FEEDBACK_VIDEO_LIMITS.maxVideoSide, videoPixels: FEEDBACK_VIDEO_LIMITS.maxVideoPixels }); }
    catch (error) {
      if (error?.code === 'materials_media_duration') throw new FeedbackMediaError('feedback_video_duration_limit');
      if (['materials_video_unsupported', 'materials_audio_unsupported'].includes(error?.code)) throw new FeedbackMediaError('feedback_media_unsupported');
      throw new FeedbackMediaError('feedback_media_invalid');
    }
    output.push(Object.freeze({ kind: 'video', name: item.name, mimeType: 'video/webm', byteLength: bytes.length, bytes, ...metadata }));
  }
  return Object.freeze(output);
}
