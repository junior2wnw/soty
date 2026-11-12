export interface FeedbackAttachment { kind: 'image' | 'audio' | 'video'; name: string; mimeType: string; dataBase64: string; }
export interface MediaLimits { totalAttachmentBytes: number; maxAttachments: number; maxAudioSeconds: number; }
export interface RasterRect { x: number; y: number; width: number; height: number; }
export const FEEDBACK_MEDIA_LIMITS: Readonly<MediaLimits>;
export function attachmentBytes(value: FeedbackAttachment): number;
export function validateAttachmentBudget(attachments: FeedbackAttachment[], limits?: MediaLimits): number;
export function safeRasterSize(width: number, height: number, maxSide?: number): { width: number; height: number };
export function rasterFileDimensions(bytes: ArrayBuffer | Uint8Array, mimeType: string): { width: number; height: number };
export function rasterSelection(rect: RasterRect, width: number, height: number): RasterRect;
export function blobAttachment(blob: Blob, kind: 'image' | 'audio' | 'video', name: string, signal?: AbortSignal): Promise<FeedbackAttachment>;
export function videoFile(file: File, maxBytes: number, signal?: AbortSignal): Promise<FeedbackAttachment>;
export function rasterFile(file: File, maxBytes: number, signal?: AbortSignal): Promise<FeedbackAttachment>;
export function editRaster(attachment: FeedbackAttachment, rect: RasterRect, operation: 'crop' | 'redact', maxBytes: number, signal?: AbortSignal): Promise<FeedbackAttachment>;
export function captureSelectedDisplay(maxBytes: number, signal?: AbortSignal): Promise<FeedbackAttachment>;
export interface FeedbackRecorderOptions { maxBytes: number; maxSeconds?: number; signal?: AbortSignal; onTick?(seconds: number): void; }
export interface FeedbackRecorder<T> { start(): Promise<T>; stop(): void; cancel(): void; active(): boolean; }
export function createFeedbackRecorder(options: FeedbackRecorderOptions & { output: 'blob' }): FeedbackRecorder<Blob>;
export function createFeedbackRecorder(options: FeedbackRecorderOptions & { output?: 'attachment' }): FeedbackRecorder<FeedbackAttachment>;
