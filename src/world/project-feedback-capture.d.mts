import type { FeedbackAttachment } from './feedback-media.mjs';
export interface ProjectCaptureRequest {readonly schema:'soty.feedback.capture.v1';readonly type:'capture_request';readonly requestId:string;readonly sourceId:string;readonly projectId:string;readonly contextRevision:number;readonly kind:'image'|'audio'}
export interface ProjectCapturePeer {readonly approved:true;readonly window:WindowProxy;readonly origin:string;readonly sourceId:string;readonly appId:string;readonly accountId:string;readonly generation:number;readonly slot:object;readonly title:string}
export const PROJECT_CAPTURE_PROTOCOL:'soty.feedback.capture.v1';
export function projectCaptureRequest(value:unknown):ProjectCaptureRequest|null;
export function mountProjectCaptureBridge(options:{view:Window;readPeer():ProjectCapturePeer|null;preparePeer?(signal:AbortSignal):Promise<boolean>;onCaptureActive?(active:boolean):void;assertPeer(peer:ProjectCapturePeer,signal:AbortSignal):Promise<boolean>;capture(options:{appId:string;title:string;request:ProjectCaptureRequest;signal:AbortSignal}):Promise<readonly FeedbackAttachment[]>;timeoutMs?:number}):{dispose():void};
