import type {Draft,DraftStore} from './state.mjs';
export type NativeProvider='soty.notes'|'soty.materials';
export interface NativeConversionTarget {objectId:string;expectedObjectRevision:number}
export interface NativeGate {scope:string;providerId:NativeProvider;ref:string;operationId:string;state:'pending'|'sealed';epoch:string;target?:NativeConversionTarget}
export interface NativeSessionPort {suspend():void;flush():Promise<void>;flushLocal():Promise<void>;snapshot():Draft;releaseRecovered(generation:number):void;hasUnsavedChanges?():boolean;isDisposed?():boolean;onGate?(state:'pending'|'sealed'|null):void}
export interface NativeDraftStore extends DraftStore {native:{scope:string;gate(providerId:NativeProvider,ref:string):Promise<NativeGate|null>;mark(input:{providerId:NativeProvider;ref:string;operationId:string;state?:'pending'|'sealed';target?:NativeConversionTarget}):Promise<NativeGate>;inspect(noteId:string):Promise<Draft[]>;
 finalize(input:{providerId:NativeProvider;ref:string;epoch:string;expected?:Pick<Draft,'branchId'|'savedAt'|'generation'>[]}):Promise<boolean>;cancel(input:{providerId:NativeProvider;ref:string;epoch:string}):Promise<boolean>;
 register(noteId:string,port:NativeSessionPort):()=>void;sessions(noteId:string):NativeSessionPort[];subscribe(noteId:string,callback:(state:'pending'|'sealed'|null)=>void):()=>void;suspend(noteId:string):void;resume(noteId:string):void}}
export function createNativeDraftStore(accountId:string,projectId?:string,options?:{origin?:string;indexedDB?:IDBFactory}):NativeDraftStore;
export function nativeDraftDirty(value:Draft):boolean;
export function readNativeSourceGate(scope:string,providerId:NativeProvider,ref:string,indexedDB?:IDBFactory):Promise<NativeGate|null>;
/** Content-free shell guard across disposed views/accounts in this page realm. */
export function hasRetainedNativeDrafts():boolean;
/** Local scoped durability only. Never calls a Source API or returns text/IDs. */
export function flushRetainedNativeDrafts():Promise<void>;
/** Registration only; no plaintext Notes database/Source call. */
export function registerRetainedNativeDraft(port:Pick<NativeSessionPort,'flushLocal'|'hasUnsavedChanges'>):()=>void;
