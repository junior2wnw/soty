import type {IncomingMessage, ServerResponse} from 'node:http';
import type {SourceRpProfile, SourceRpMarker, SourceRpCurrentProof} from './source-rp.d.mts';
export {createSourceRpProtocol, createSourceRpSessionService, sourceRpCipherBinding, SOURCE_RP_PROFILE} from './source-rp.mjs';
export interface SourceProfile {schema:'soty.selected-human-embed.v2';appId:string;connector:{linkId:string;hostDeviceId:string;connectorId:string};target:{revision:number;digest:string};
  sourceProfile:{id:string;version:number;digest:string};resource:{registryId:string;environmentId:string;tenantId:string;appId:string;resourceId:string;selection:{kind:'soty.resource.v1'|'soty.workspace.v1';nativeId:string;incarnationId:string}};
  issuer:string;clientId:string;embedOrigin:string;nativeOrigin:string;parentOrigin:string}
declare const nativePortBrand:unique symbol, commitPortBrand:unique symbol;
export interface NativeContextMetadata {title:string;role:'owner'|'editor'|'viewer'|'reader';revision:number}
export interface NativeContextAfter extends NativeContextMetadata {schema:'soty.context-after.v1';mutationRef:string;generation:number}
export interface NativeWorkspaceReceipt {schema:'soty.workspace-receipt.v1';
  effects:{kind:'created'|'updated'|'deleted'|'published'|'sent'|'charged';resourceType:string;resourceId:string;revision?:number}[];
  receipt:{verificationMethod:'domain_read';artifacts:{type:string;id:string;revision?:number}[]}}
export interface NativeCommitPort {readonly [commitPortBrand]:true;/** Call once synchronously INSIDE the Native transaction. */commit<T>(action:()=>T):T;
  /** Workspace only: inside commit(action), after the effect; persist with the original receipt. */contextAfter?():Readonly<NativeContextAfter>}
export interface NativeBinding {identity:{issuer:string;subject:string};rootPrincipal:{accountId:string;deviceId:string};humanPrincipal:unknown;resource:SourceProfile['resource'];semanticDigest:string;operation:string;sessionIdHash?:string;rootReferenceDigest?:string;rootExpiresAt?:number}
export interface SourceNativeAuthorityPort {readonly [nativePortBrand]:true}
export interface NativeHooks<Proof=unknown> {
  capture(binding:Readonly<NativeBinding>,nativeBrowserRequest?:IncomingMessage):Promise<Proof>;
  /** Fresh authoritative Source SQL check outside transaction locks; returns
   * no rows/permission DTO. Required by an async-SQL Native implementation. */
  assertCurrent?:(proof:Proof,binding:Readonly<NativeBinding>)=>Promise<void>;
  /** Synchronous final checkpoint inside the actual Native transaction. */
  withCurrent<T>(proof:Proof,binding:Readonly<NativeBinding>,action:()=>T):T;
  /** Workspace only. Private child/incarnation key; current Native ACL under withCurrent. */
  currentContext?:(proof:Proof,binding:Readonly<NativeBinding>)=>NativeContextMetadata&{key:string;generation:number;expiresAt:number};
  /** UI locator, not authority. Validate own ordering/ACL; update only this proof and Native slot. */
  selectContext?:(proof:Proof,binding:Readonly<NativeBinding>,locator:Readonly<{nativeId:string;generation:number}>)=>void;
  rememberLogin?:(proof:Proof,binding:Readonly<NativeBinding>,intent:{interactionIdHash:string;expiresAt:number})=>{idHash:string;version:1;bindingDigest:string};
  recoverLogin?:(binding:Readonly<NativeBinding>,marker:{idHash:string;version:1;bindingDigest:string},intent:{interactionIdHash:string;expiresAt:number})=>Promise<Proof>;
  linkVerifiedIdentity?:(proof:Proof,binding:Readonly<NativeBinding>,identity:{issuer:string;subject:string})=>unknown;
  createEmptyGuest?:(proof:Proof,binding:Readonly<NativeBinding>,identity:{issuer:string;subject:string})=>unknown;
  read?:(proof:Proof,binding:Readonly<NativeBinding>,input:unknown)=>Promise<unknown>;
  execute?:(proof:Proof,binding:Readonly<NativeBinding>,input:unknown,commit:NativeCommitPort)=>Promise<unknown>;
  readProof?:(proof:Proof,binding:Readonly<NativeBinding>,input:unknown)=>Promise<unknown>;
  feedback?:{context:Function;list:Function;get:Function;submit:Function;reply?:Function;status?:Function;accept?:Function};
}
export function createSourceNativeAuthorityPort<P>(options:NativeHooks<P>):SourceNativeAuthorityPort;
export function isSourceNativeAuthorityPort(value:unknown):value is SourceNativeAuthorityPort;
export function isSourceNativeCommitPort(value:unknown):value is NativeCommitPort;
/** Source implements durable encrypted storage. Identity/session writes call
 * the supplied final callback once synchronously INSIDE the Native transaction. */
export interface SourceStoragePort {
  consumeNonce(nonce:string,expiresAt:number):Promise<boolean>;createInteraction(record:unknown):Promise<void>;getInteraction(hash:string):Promise<any>;
  claimInteraction(hash:string,revision:number,privatePkce:unknown,finalRememberNative?:()=>unknown):Promise<boolean>;claimCallback(hash:string,revision:number):Promise<boolean>;
  completeInteraction(record:unknown,finalNativeLink:()=>unknown):Promise<boolean>;
  readSession(hash:string):Promise<any>;readCompletion(hash:string):Promise<any>;consumeCompletion(hash:string,sessionHash:string,finalNativeAssert:()=>unknown):Promise<{token:string}|null>;
  readTokenProof(session:unknown):Promise<{accessToken:string;expiresAt:number}>;revokeSession(hash:string):Promise<void>;
  /** Private constructor-only Long CAS; Basic storage omits this method. */
  rebindSession?(input:{idHash:string;expectedReference:string;context:unknown},finalNativeAssert:()=>unknown):Promise<boolean>;
  /** Optional host-only S2 resume. Search only bounded sessions of the linked
   * Native account in the exact verified context namespace; never Basic.
   * The locator is reread and all Root/RP/Native checks still apply. */
  findRenewableSession?(input:Readonly<{context:unknown;semanticDigest:string}>):Promise<{idHash:string}|null>;
}
/** Host-owned synchronous SQLite connection. The helper creates no schema and
 * never closes the connection or owns keys. Native callbacks run inside the
 * supplied transaction, on this same connection. */
export function createSqliteSourceStorage(options:{
  database:{prepare(sql:string):{get(...params:any[]):any;all(...params:any[]):any[];run(...params:any[]):{changes:number|bigint}}};
  current:()=>void;transaction:<T>(action:()=>T)=>T;
  encrypt:(model:string,id:string,revision:number,input:unknown)=>string;
  decrypt:(model:string,id:string,revision:number,cipher:string,keyId:string)=>any;
  clock:()=>number;keyId:string;
}):SourceStoragePort;
export interface SourceRpRootFrame {readonly context:unknown;assertRootCurrent():Promise<unknown>}
export interface SourceAppRenewalPort {
  prepareSession(basic:unknown,identity:{issuer:string;subject:string;accessToken:string;expiresAt:number;refreshToken?:string;nonce?:string},context:unknown,root:SourceRpRootFrame):Promise<unknown>;
  currentProof(marker:SourceRpMarker,hostOptions:undefined|{minimumAccessRemainingMs?:number},root:SourceRpRootFrame):Promise<SourceRpCurrentProof>;
}
/** Existing Basic/current-proof constructor port. Own RT preparation is
 * admitted only when the runtime also finds prepareSession on its CODE port. */
export interface SourceAppLegacyRpPort {
  currentProof(marker:SourceRpMarker,hostOptions?:{minimumAccessRemainingMs?:number}):Promise<SourceRpCurrentProof>;
}
export function createSourceAppBff(options:{profile:SourceProfile;transportKey:Buffer;connectorPort:number;storage:SourceStoragePort;native:SourceNativeAuthorityPort;rp:SourceRpProfile;
  /** Host-only, workspace profile only: default128, integer1..65536. No permission or lease extension. */
  workspaceCapacity?:{maxSlots?:number};
  clock?:()=>number;allowCreateEmptyGuest?:boolean;processingProofEnabled?:boolean;ui?:{appLabel:string;resourceLabel:string};rpSessions?:SourceAppRenewalPort|SourceAppLegacyRpPort}):{
  readonly profile:SourceProfile;/** Private host proof; never a response/body/persisted permission. */currentFeedbackJobProof(sessionHash:string):Promise<object>;
  handleRequest(req:IncomingMessage,res:ServerResponse):Promise<boolean>;close():void;
};
export const STANDARD_SELECTED_SOURCE:Readonly<{id:'soty.standard-resource';version:1;digest:string}>;
export const STANDARD_SOURCE_CONTRACT:Readonly<unknown>;
export const STANDARD_SELECTED_SOURCE_V2:Readonly<{id:'soty.standard-resource';version:2;digest:string}>;
export const STANDARD_SOURCE_CONTRACT_2:Readonly<unknown>;
export const STANDARD_SELECTED_SOURCE_V3:Readonly<{id:'soty.standard-resource';version:3;digest:string}>;
export const STANDARD_SOURCE_CONTRACT_3:Readonly<unknown>;
export const STANDARD_WORKSPACE_SOURCE:Readonly<{id:'soty.standard-workspace';version:1;digest:string}>;
export const STANDARD_WORKSPACE_CONTRACT:Readonly<unknown>;
export {SOURCE_FEEDBACK_LIMITS} from './browser.mjs';
export {createFeedbackProcessorEngine,createFeedbackJobEnforcer,feedbackProcessingPolicy,FEEDBACK_JOB_LIMITS} from './processing.mjs';
