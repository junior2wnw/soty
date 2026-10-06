import { createHash } from 'node:crypto';
import { canonicalContractJson } from '../../app-contract/json.mjs';

export const FEEDBACK_LIMITS = Object.freeze({ bodyChars: 8000, totalAttachmentBytes: 1048576,
  maxAttachments: 3, maxAudioSeconds: 120, ticketsPerInstallation: 10000,
  receiptsPerInstallation: 20000, maxMessagesPerTicket: 128, conversationBytes: 196608, pageSize: 30, maxPageSize: 50 });
const ref = (id, contract) => Object.freeze({ id, version: 1,
  digest: createHash('sha256').update(canonicalContractJson(contract)).digest('hex') });
export const FEEDBACK_PROVIDER = ref('soty.feedback:provider/native', { protocol: 'soty.feedback.sqlite.v1', durable: true });
const capture = ref('soty.feedback:capture/inline', { formats: ['png', 'jpeg', 'webp', 'webm-opus', 'ogg-opus'],
  bytes: FEEDBACK_LIMITS.totalAttachmentBytes, count: FEEDBACK_LIMITS.maxAttachments, seconds: FEEDBACK_LIMITS.maxAudioSeconds });
const retention = ref('soty.feedback:retention/private', { visibility: 'reporter-and-support',
  recipient: 'verified-app-owner', explicitMediaSelection: true, automaticCapture: false, publicPublication: false });
const feedback = Object.freeze({ mode: 'required', provider: FEEDBACK_PROVIDER, captureProfile: capture,
  retentionProfile: retention, submitAudience: 'members', ticketVisibility: 'reporter-and-support' });
export const DEFAULT_FEEDBACK_PROFILE = Object.freeze({ ...ref('soty.feedback:author/private', feedback), feedback });
