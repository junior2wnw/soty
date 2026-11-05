import type {CanvasVaultClient} from './canvas-vault-client.mjs';
import type {NativeDraftStore,NativeProvider} from '../../modules/notes/browser/native-local.mjs';
import type {Draft} from '../../modules/notes/browser/state.mjs';
import type {NoteCache} from '../../modules/notes/browser/cache.mjs';
import type {CanvasCreateStore} from './canvas-create-flow.mjs';
export interface NativeSourceIdentity {providerId:NativeProvider;ref:string;ownerId:string}
export interface NativeTransitionTicket {schema:'soty.native-cache-ticket.v1';operationId:string;objectId:string;expectedObjectRevision:number;source:NativeSourceIdentity}
export interface NativeTransitionState {objectId:string;applicable:boolean;server:'unknown'|'unprotected'|'pending'|'protected';device:'pending'|'complete';protectionComplete:boolean;reason?:string;operationId?:string}
export type NativePrepareResult=({state:'ready';ticket:NativeTransitionTicket}&NativeTransitionState)|({state:'pending'|'complete';ticket?:NativeTransitionTicket}&NativeTransitionState);
export interface NativeCreationInspection {operationId:string;stage:string;holdsPlaintext:boolean;sourceConfirmed:boolean}
export interface NativeCacheTransition {
 prepare(input:{objectId:string;expectedObjectRevision:number;operationId:string}):Promise<NativePrepareResult>;
 finalize(input:{ticket:NativeTransitionTicket;result?:unknown;failure?:{kind:'server-rejected'|'transport-uncertain';code?:string}}):Promise<NativeTransitionState>;
 liveReconcile(input:{objectId:string}):Promise<NativeTransitionState>;
 resumeAfterUnlock(objectId:string):Promise<NativeTransitionState>;
 cancel(input:{ticket:NativeTransitionTicket;neverSubmitted:true}):Promise<NativeTransitionState>;
 /** Remember only an authorized owned native Source. No content enters this map. */
 remember(objectId:string,source:NativeSourceIdentity):void;
 /** Synchronous DOM/session suspension before an engine disposes old aliases. */
 observeProtected(objectId:string):void;
 snapshot(objectId:string):NativeTransitionState|null;
 /** Includes detached private editor RAM whose local COMMIT failed. */
 hasUnsavedChanges():boolean;
 /** Local durability only; never submits a sealed legacy Notes mutation. */
 flush():Promise<void>;
 dispose():void;
}
export interface NativeCacheTransitionOptions {
 api:{request<T>(operation:string,args?:Record<string,unknown>):Promise<T>};accountId:string;projectId?:string;isCurrent():boolean;
 vault:CanvasVaultClient&{archiveNativeDrafts?:(objectId:string,source:NativeSourceIdentity,drafts:Draft[])=>Promise<unknown>};
 store?:NativeDraftStore;cache?:NoteCache;creationStore?:CanvasCreateStore;
 inspectCreation?:(source:NativeSourceIdentity)=>Promise<NativeCreationInspection[]>;
 onChange?:(state:NativeTransitionState)=>void;
 canConvertNative?:()=>boolean;
}
export function createNativeCacheTransition(options:NativeCacheTransitionOptions):NativeCacheTransition;
