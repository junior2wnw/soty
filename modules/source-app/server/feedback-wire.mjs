import { feedbackInput } from '../shared/feedback-wire.mjs';
import { validateFeedbackAttachments } from '../../feedback/server/media.mjs';

/** Shared validator reused from real native feedback. Container/frame checks
 * bound PNG/JPEG/WebP and Opus duration; this makes no OCR/ASR claim. */
export function validateSourceFeedbackInput(operation, input) {
  const value = feedbackInput(operation, input);
  if (operation === 'submit') validateFeedbackAttachments(value.attachments);
  return value;
}
