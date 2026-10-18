import type { WorldApi, WorldAttachment } from './types';
export const PREVIEW_LIMITS: Readonly<{ reads:number;queue:number;liveUrls:number;liveBytes:number;pixels:number;side:number;thumbnailSide:number;gallerySide:number;thumbnailBytes:number;galleryBytes:number }>;
export function isRasterAttachment(attachment: WorldAttachment): boolean;
export function inspectRaster(bytes: Uint8Array, type: string): Readonly<{mime:string;width:number;height:number}>;
export interface AttachmentImageLease {readonly url:string;readonly name:string;readonly bytes:number;readonly kind:string;release():void;}
export interface AttachmentPreviewPool {load(attachment:WorldAttachment,options?:{kind?:'thumbnail'|'gallery'|'download';current?():boolean;released?():void}):Promise<AttachmentImageLease>;clear():void;close():void;snapshot():{running:number;queued:number;liveUrls:number;liveBytes:number;closed:boolean};}
export function createAttachmentPreviewPool(options:{api:WorldApi;accountId:string;communityId:string;current?():boolean}):AttachmentPreviewPool;
