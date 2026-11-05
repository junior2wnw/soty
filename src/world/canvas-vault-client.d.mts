import type {Note,NoteDocument,DraftStore,Draft} from '../../modules/notes/browser/state.mjs';
import type {CanvasVaultLocalStore} from './canvas-vault-local.mjs';
import type {NativeSourceIdentity} from './native-cache-transition.mjs';
import type {NativeNoteRecovery} from './canvas-native-recovery.mjs';
import type {CanvasCreateRecord} from './canvas-create-flow.mjs';
import type {CanvasCreationRecovery,CanvasCreationRecoveryReceipt} from './canvas-creation-recovery.mjs';
import type {CanvasVaultLocalRecovery} from './canvas-vault-source-migration.mjs';
export interface CanvasVaultApi{request<T>(operation:string,args?:Record<string,unknown>):Promise<T>}
export interface CanvasVaultContent{contentType:string;bytesBase64:string}
export interface CanvasVaultStatus{objectId:string;locked:boolean;expiresAt?:number;revision?:number;protection?:{kind:'none'|'password'};state?:'sealing';capabilities?:{nativeConversion:boolean;placementProtection:boolean}}
export const CANVAS_VAULT_NOTE_TYPE:'application/vnd.soty.note+json';
export function canvasVaultNoteContent(input:Partial<Note>&Pick<Note,'title'|'body'>):CanvasVaultContent;
export interface CanvasVaultClient{
 unlock(objectId:string,password:string,options?:{ttlMs?:number}):Promise<{objectId:string;expiresAt:number}>;
 status(objectId:string,options?:{includeCapabilities?:boolean}):Promise<CanvasVaultStatus>;
 relock(objectId:string,options?:{scope?:'device'|'all';requestId?:string}):Promise<unknown>;
 protect(objectId:string,expectedObjectRevision:number,password:string,options?:{requestId?:string}):Promise<unknown>;
 rotate(objectId:string,input:{oldPassword:string;newPassword:string;expectedRevision?:number;requestId?:string}):Promise<unknown>;
 localRecoveryState(objectId:string):Promise<CanvasVaultLocalRecovery>;
 recoverLocalPassword(objectId:string,passwords:{currentSourcePassword:string;priorLocalPassword:string}):Promise<CanvasVaultLocalRecovery>;
 readContent(objectId:string,options?:{signal?:AbortSignal;fresh?:boolean}):Promise<{contentType:string;bytes:Uint8Array<ArrayBuffer>;revision:number}>;
 readNote(objectId:string,options?:{fresh?:boolean}):Promise<Note>;
 writeNote(objectId:string,args:Record<string,unknown>):Promise<{noteId:string;revision:number;updatedAt:number}>;
 draftStore(objectId:string):DraftStore;
 archiveNativeDrafts(objectId:string,source:NativeSourceIdentity,drafts:Draft[]):Promise<unknown>;
 nativeRecoveries(objectId:string):Promise<NativeNoteRecovery[]>;
 restoreNativeRecovery(objectId:string,recordId:string):Promise<{branchId:string}>;
 pruneNativeRecoveries(objectId:string):Promise<void>;
 archiveCreationIntents(objectId:string,source:NativeSourceIdentity,records:CanvasCreateRecord[]):Promise<CanvasCreationRecoveryReceipt>;
 verifyCreationRecoveries(objectId:string,source:NativeSourceIdentity,records:CanvasCreateRecord[],receipt?:CanvasCreationRecoveryReceipt):Promise<CanvasCreationRecoveryReceipt>;
 creationRecoveries(objectId:string,options?:{archiveId?:string}):Promise<CanvasCreationRecovery[]>;
 restoreCreationRecovery(objectId:string,archiveId:string,operationId:string):Promise<{branchId:string}>;
 isUnlocked(objectId:string):boolean;
 generation(objectId:string):number;
 noteId(objectId:string):string|null;
 expiresAt(objectId:string):number|null;
 invalidate(objectId:string,reason?:string):void;
 subscribe(listener:(objectId:string,reason:string)=>void):()=>void;
 dispose():void;
}
export interface CanvasVaultClientOptions{api:CanvasVaultApi;accountId:string;projectId?:string;isCurrent():boolean;onChange?:(objectId:string)=>void|Promise<void>;store?:CanvasVaultLocalStore;crypto?:Crypto;clock?:()=>number;setTimer?:(callback:()=>void,delay:number)=>unknown;clearTimer?:(handle:unknown)=>void}
export function createCanvasVaultClient(options:CanvasVaultClientOptions):CanvasVaultClient;
