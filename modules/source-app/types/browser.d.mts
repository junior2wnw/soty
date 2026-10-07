export interface FeedbackAttachment {kind:'image'|'audio';name:string;mimeType:string;dataBase64:string}
export interface FeedbackDraft {body:string;attachments:FeedbackAttachment[]}
export interface FeedbackContext {schema:'soty.source-feedback.context.v1';bindingDigest:string;ready:boolean;canSubmit:boolean;recipientLabel:string;
  limits:{bodyChars:number;totalAttachmentBytes:number;maxAttachments:number;maxAudioSeconds:number};capabilities:{text:boolean;voice:boolean;screenshot:boolean;asr:false}}
export interface FeedbackTicket {id:string;revision:number;status:'received'|'in_progress'|'needs_action'|'ready_to_check'|'resolved';body:string;createdAt:number;updatedAt:number;
  canReply:boolean;canManage:boolean;canAccept:boolean;attachments:unknown[];messages:{id:string;kind:'reporter'|'support';body:string;createdAt:number}[]}
export interface FeedbackWrite {requestId:string;replayed:boolean;receipt:{ticketId:string;revision:number;createdAt:number};ticket:FeedbackTicket}
export interface FeedbackAction {ticketId:string;requestId:string;expectedRevision:number}
export interface FeedbackClient {
  context(signal?:AbortSignal):Promise<FeedbackContext>;list(input?:{limit?:number;cursor?:string},signal?:AbortSignal):Promise<{tickets:FeedbackTicket[];nextCursor:string|null}>;
  get(ticketId:string,signal?:AbortSignal):Promise<{ticket:FeedbackTicket}>;submit(input:FeedbackDraft&{requestId:string},signal?:AbortSignal):Promise<FeedbackWrite>;
  reply(input:FeedbackAction&{body:string},signal?:AbortSignal):Promise<FeedbackWrite>;
  status(input:FeedbackAction&{status:'in_progress'|'needs_action'|'ready_to_check'},signal?:AbortSignal):Promise<FeedbackWrite>;
  accept(input:FeedbackAction,signal?:AbortSignal):Promise<FeedbackWrite>;
}
export function createSourceFeedbackClient(options?:{fetch?:typeof fetch}):FeedbackClient;
export class SourceFeedbackClientError extends Error {readonly code:string;readonly status:number}
export const SOURCE_FEEDBACK_LIMITS:Readonly<{bodyChars:8000;totalAttachmentBytes:1048576;maxAttachments:3;maxAudioSeconds:120}>;
export function createSourceFeedbackController(options?:{api?:Pick<FeedbackClient,'context'|'submit'>;isCurrent?:()=>boolean;requestId?:()=>string;onChange?:(value:unknown)=>void}):{
  snapshot():unknown;open():Promise<boolean>;setDraft(value:FeedbackDraft):boolean;send():Promise<boolean>;invalidate():void;dispose():void;
};
export {createSourceFeedbackProcessingClient} from './processing.mjs';
