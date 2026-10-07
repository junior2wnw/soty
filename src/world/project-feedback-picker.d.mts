import type { FeedbackAttachment } from './feedback-media.mjs';
import type { ProjectCaptureRequest } from './project-feedback-capture.mjs';
export function pickProjectFeedbackMedia(options:{appId:string;title:string;request:ProjectCaptureRequest;signal:AbortSignal;document?:Document}):Promise<readonly FeedbackAttachment[]>;
