export type MediaCaptureResource = 'camera' | 'microphone';
export interface MediaCaptureLease { isCurrent(): boolean; release(): void; }
export interface MediaCaptureCoordinator {
  busy(resource: MediaCaptureResource): boolean;
  acquire(resource: MediaCaptureResource, owner: object): MediaCaptureLease | null;
}
export function createMediaCaptureCoordinator(): MediaCaptureCoordinator;
export const sharedMediaCaptureCoordinator: MediaCaptureCoordinator;
