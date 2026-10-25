export interface FeedbackAttachment {kind:'image'|'audio';name:string;mimeType:string;dataBase64:string}
export interface FeedbackDraft {body:string;attachments:FeedbackAttachment[]}
export interface FeedbackContext {schema:'soty.source-feedback.context.v1';bindingDigest:string;ready:boolean;canSubmit:boolean;canManage?:boolean;recipientLabel:string;
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
export function createSourceFeedbackClient(options?:{fetch?:typeof fetch;profileVersion?:1|2|3;contextRef?:string}):FeedbackClient;
export class SourceFeedbackClientError extends Error {readonly code:string;readonly status:number}
export const SOURCE_FEEDBACK_LIMITS:Readonly<{bodyChars:8000;totalAttachmentBytes:1048576;maxAttachments:3;maxAudioSeconds:120}>;
export function createSourceFeedbackController(options?:{api?:Pick<FeedbackClient,'context'|'submit'>;isCurrent?:()=>boolean;requestId?:()=>string;onChange?:(value:unknown)=>void}):{
  snapshot():unknown;open():Promise<boolean>;setDraft(value:FeedbackDraft):boolean;send():Promise<boolean>;invalidate():void;dispose():void;
};
export interface FeedbackInboxSnapshot {state:string;context:FeedbackContext|null;tickets:FeedbackTicket[];ticket:FeedbackTicket|null;
  nextCursor:string|null;pending:boolean;replyDraft:string}
export function createSourceFeedbackInbox(options?:{api?:FeedbackClient;isCurrent?:()=>boolean;requestId?:()=>string;onChange?:(value:FeedbackInboxSnapshot)=>void}):{
  snapshot():FeedbackInboxSnapshot;open():Promise<boolean>;more():Promise<boolean>;select(ticketId:string):Promise<boolean>;setReply(body:string):boolean;
  write(operation?:'reply'|'status'|'accept',status?:'in_progress'|'needs_action'|'ready_to_check'):Promise<boolean>;invalidate():void;dispose():void;
};
export {createSourceFeedbackProcessingClient} from './processing.mjs';
/** Public locator and guarded same-parent short-session wake-up for reads only. */
export function createSourceFrameFetch(options?:{fetch?:typeof fetch;href?:string;shortSessionHandoff?:boolean;view?:Window;parentOrigin?:string}):typeof fetch;

export type SourceJson = null | boolean | number | string | SourceJson[] | { [key: string]: SourceJson };
export interface SourceWorkspaceContext {
  readonly schema: 'soty.current-context.v2';
  readonly selection: { readonly kind: 'soty.workspace.v1'; readonly nativeId: string; readonly incarnationId: string };
  readonly context: { readonly ref: string; readonly generation: number; readonly expiresAt: number };
  /** Non-authorizing actor/device/Native-child continuity witness across short leases. */
  readonly continuityDigest?: string;
  readonly title: string; readonly role: 'owner' | 'editor' | 'viewer' | 'reader'; readonly revision: number;
}
export interface SourceWorkspaceRequest { requestId: string; input: { operation: string; [key: string]: SourceJson }; }
export interface SourceReceiptReference { readonly requestId: string; readonly contextRef: string; readonly inputDigest: string; }
/** Prepared by this client, pinned to the original context; JSON cannot restore write authority. */
export interface SourceWriteIntent extends SourceReceiptReference { readonly input: Readonly<SourceWorkspaceRequest['input']>; }
export interface SourceWriteResult {
  readonly requestId: string; readonly inputDigest: string; readonly outcome: 'committed' | 'not_applied';
  readonly replayed?: boolean; readonly receipt?: SourceJson; readonly data?: SourceJson; readonly result?: SourceJson;
  readonly contextAfter?: Readonly<Record<string, SourceJson>>; readonly workspaceReceipt?: Readonly<Record<string, SourceJson>>;
  readonly recovery?: Readonly<{schema: 'soty.workspace-receipt-recovery.v1'; contextRef: string; receiptDigest: string; contextAfter: Readonly<Record<string, SourceJson>>}>;
}
export class SourceWorkspaceClientError extends Error { readonly code: string; readonly status: number; readonly effectUnknown: boolean; }
export interface SourceWorkspaceClient {
  /** Cold/resume read handshake under one local account/frame epoch; no business effect replay. */
  connect(args: {requestId: string}, signal?: AbortSignal): Promise<SourceWorkspaceContext>;
  continueSession(args: {requestId: string}, signal?: AbortSignal): Promise<Readonly<{
    schema: 'soty.source-session-continuation.v1'; ready: true; renewable: boolean;
    sessionExpiresAt: number; accessExpiresAt: number; receiptDigest: string;
  }>>;
  currentContext(args: {requestId: string}, signal?: AbortSignal): Promise<SourceWorkspaceContext>;
  selectContext(args: {requestId: string; nativeId: string; generation: number}, signal?: AbortSignal): Promise<SourceWorkspaceContext>;
  context(): SourceWorkspaceContext | null;
  query<T extends {requestId: string} = {requestId: string; [key: string]: SourceJson}>(args: SourceWorkspaceRequest, signal?: AbortSignal): Promise<Readonly<T>>;
  /** Refreshes read-only authority before preparing a new intent; sent intents never move. */
  prepareInvoke(args: SourceWorkspaceRequest, signal?: AbortSignal): Promise<SourceWriteIntent>;
  invoke(intent: SourceWriteIntent, signal?: AbortSignal): Promise<SourceWriteResult>;
  /** Reads an original receipt; never sends an invoke. A retained reference may be used after reload only in its same current context. */
  receipt(reference: SourceReceiptReference, signal?: AbortSignal): Promise<SourceWriteResult>;
  /** Fresh current context may read the sealed original result; never rewrites its intent or sends an invoke. */
  recoverReceipt(reference: SourceReceiptReference, signal?: AbortSignal): Promise<SourceWriteResult>;
  invalidate(): void; dispose(): void;
}
export function createSourceWorkspaceClient(options?: {fetch?: typeof fetch; href?: string; view?: Window; parentOrigin?: string; isCurrent?(): boolean}): SourceWorkspaceClient;
