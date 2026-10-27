import type {IncomingMessage, ServerResponse} from 'node:http';
/** consume must atomically and durably mark a nonce as used until its expiry. */
export declare function createRelayIdentityVerifier(options:{issuer:string;appId:string;fetch?:typeof globalThis.fetch;clock?:()=>number;
  consume:(id:string,expiresAt:number)=>boolean|Promise<boolean>}):(request:IncomingMessage)=>Promise<Readonly<{issuer:string;subject:string}>>;
/** Use a dedicated SQLite file on one local filesystem; it is not an app data database. */
export declare function createSqliteRelayNonceStore(options:{databasePath:string;maxItems?:number;busyTimeoutMs?:number;clock?:()=>number}):Readonly<{
  consume(id:string,expiresAt:number):boolean;close():void}>;
/** Use one dedicated local database for all workers of this application. */
export declare function createSqliteRelayIdentity(options:{issuer:string;appId:string;databasePath:string;fetch?:typeof globalThis.fetch;
  clock?:()=>number;maxItems?:number;busyTimeoutMs?:number}):Readonly<{
  verify(request:IncomingMessage):Promise<Readonly<{issuer:string;subject:string}>>;close():void}>;
import type {SourceRpProfile, SourceRpMarker, SourceRpCurrentProof} from './source-rp.d.mts';
export {createSourceRpProtocol, createSourceRpSessionService, sourceRpCipherBinding, SOURCE_RP_PROFILE} from './source-rp.mjs';
export interface SourceProfile {schema:'soty.selected-human-embed.v2';appId:string;connector:{linkId:string;hostDeviceId:string;connectorId:string};target:{revision:number;digest:string};
  sourceProfile:{id:string;version:number;digest:string};resource:{registryId:string;environmentId:string;tenantId:string;appId:string;resourceId:string;selection:{kind:'soty.resource.v1'|'soty.workspace.v1';nativeId:string;incarnationId:string}};
  issuer:string;clientId:string;embedOrigin:string;nativeOrigin:string;parentOrigin:string}
declare const nativePortBrand:unique symbol, commitPortBrand:unique symbol, recoveryPortBrand:unique symbol;
export interface NativeContextMetadata {title:string;role:'owner'|'editor'|'viewer'|'reader';revision:number}
export interface NativeContextAfter extends NativeContextMetadata {schema:'soty.context-after.v1';mutationRef:string;generation:number}
export interface NativeWorkspaceReceipt {schema:'soty.workspace-receipt.v1';
  effects:{kind:'created'|'updated'|'deleted'|'published'|'sent'|'charged';resourceType:string;resourceId:string;revision?:number}[];
  receipt:{verificationMethod:'domain_read';artifacts:{type:string;id:string;revision?:number}[]}}
/** Opaque SDK-MAC-sealed data. Persist beside the immutable original receipt,
 * atomically in its Native transaction. It grants no current authority. */
export interface NativeReceiptBinding {schema:'soty.workspace-receipt-binding.v1';contextRef:string;scopeDigest:string;receiptDigest:string;mac:string}
export interface NativeReceiptRecoveryPort {readonly [recoveryPortBrand]:true;
  /** Call once INSIDE the current Native receipt-read transaction/ACL fence. */recover<T>(original:T,persisted:NativeReceiptBinding):T&{
    recovery:{schema:'soty.workspace-receipt-recovery.v1';contextRef:string;receiptDigest:string;contextAfter:NativeContextAfter}}}
export interface NativeCommitPort {readonly [commitPortBrand]:true;/** Call once synchronously INSIDE the Native transaction. */commit<T>(action:()=>T):T;
  /** Workspace only: inside commit(action), after the effect; persist with the original receipt. */contextAfter?():Readonly<NativeContextAfter>;
  /** Inside that same commit(action), using its ORIGINAL branded contextAfter. */receiptBinding?(original:unknown):Readonly<NativeReceiptBinding>}
export interface NativeBinding {identity:{issuer:string;subject:string};rootPrincipal:{accountId:string;deviceId:string};humanPrincipal:unknown;resource:SourceProfile['resource'];semanticDigest:string;operation:string;sessionIdHash?:string;rootReferenceDigest?:string;rootExpiresAt?:number;/** Workspace UI locator, never authority. */instanceId?:'main'|`widget-${string}`}
export interface SourceNativeAuthorityPort {readonly [nativePortBrand]:true}
export interface NativeHooks<Proof=unknown> {
  capture(binding:Readonly<NativeBinding>,nativeBrowserRequest?:IncomingMessage):Promise<Proof>;
  /** Workspace2 only: original SDK-branded, unexpired Root context, never JSON identity. */
  captureRoot?:(binding:Readonly<NativeBinding>,verifiedRootContext:object)=>Promise<Proof>;
  /** Called synchronously under withCurrent INSIDE Source session SQL transaction. */
  commitRootSession?:(proof:Proof,binding:Readonly<NativeBinding>)=>{nativeSessionHash:string;principalId:string;resourceId:string};
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
  readProof?:(proof:Proof,binding:Readonly<NativeBinding>,input:unknown,recovery?:NativeReceiptRecoveryPort)=>Promise<unknown>;
  /** Workspace2's existing HTTP contextRef is checked by the SDK before/after
   * reads and inside the final synchronous mutation fence. Native inputs stay
   * the ordinary closed feedback DTO; no browser project ID creates authority. */
  feedback?:{context:Function;list:Function;get:Function;submit:Function;reply?:Function;status?:Function;accept?:Function};
}
export function createSourceNativeAuthorityPort<P>(options:NativeHooks<P>):SourceNativeAuthorityPort;
export function isSourceNativeAuthorityPort(value:unknown):value is SourceNativeAuthorityPort;
export function isSourceNativeCommitPort(value:unknown):value is NativeCommitPort;
export function isSourceNativeReceiptRecoveryPort(value:unknown):value is NativeReceiptRecoveryPort;
/** Source implements durable encrypted storage. Identity/session writes call
 * the supplied final callback once synchronously INSIDE the Native transaction. */
export interface SourceStoragePort {
  /** Workspace2 only. Same Native transaction; replay still calls the final fence. */
  createRootSession?(record:unknown,finalNativeLink:()=>unknown):Promise<{idHash:string}>;
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
export function createSourceAppBff(options:{profile:SourceProfile;transportKey:Buffer;connectorPort:number;storage:SourceStoragePort;native:SourceNativeAuthorityPort;rp?:SourceRpProfile;
  /** Optional historical diagnostics. Throw/rejection never changes an applied
   * business result. Contains only static pins/time/operation/status, no data. */
  onVerifiedOperation?:(observation:SourceVerifiedOperation)=>void;
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
export const STANDARD_ROOT_WORKSPACE_SOURCE:Readonly<{id:'soty.standard-workspace';version:2;digest:string}>;
export const STANDARD_ROOT_WORKSPACE_CONTRACT:Readonly<unknown>;
export interface VerifiedRootContext {readonly profileDigest:string;readonly expiresAt:number;readonly instanceId?:string;
  readonly reference:Readonly<{id:string;version:number;digest:string}>;readonly rootPrincipal:Readonly<{accountId:string;deviceId:string}>;
  readonly humanPrincipal:Readonly<{issuer:string;subject:string;clientId:string;clientProfileDigest:string;clientGeneration:number}>;
  readonly resource:SourceProfile['resource'];readonly sourceProfile:SourceProfile['sourceProfile'];readonly appId:string;}
export function isVerifiedRootContext(proof:unknown):proof is VerifiedRootContext;
/** Rejects copied JSON, wrong profile and expired JWT; returns original frozen context. */
export function readVerifiedRootContext(proof:unknown,profileDigest:string):VerifiedRootContext;
export {SOURCE_FEEDBACK_LIMITS} from './browser.mjs';
export {createFeedbackProcessorEngine,createFeedbackJobEnforcer,feedbackProcessingPolicy,FEEDBACK_JOB_LIMITS} from './processing.mjs';
/** Opaque, process-local app-policy lease. JSON copies carry no authority.
 * Native Human/session/ACL/CSRF checks remain mandatory and independent. */
export interface NativeEntryLease {readonly schema:'soty.native-entry-lease.v1';readonly appId:string;
  readonly policyEpoch:number;readonly admissionKeyHash:string;readonly primaryTargetDigest:string;readonly nativeTargetDigest:string;}
export interface NativeEntryStream {beforeEffect():Promise<NativeEntryLease>;assertCurrent():NativeEntryLease;close(reason?:string):void;}
export function createNativeEntryGuard(options:{rootCheckUrl:string;appId:string;origin:string;primaryTargetDigest:string;nativeTargetDigest:string;
  /** These bounds may only be shortened: timeout<=1000ms, heartbeat<=250ms, lease<=1000ms, streams<=128. */
  timeoutMs?:number;heartbeatMs?:number;leaseMs?:number;maxStreams?:number;maxRequests?:number;}):{
  acquire(options?:{signal?:AbortSignal}):Promise<NativeEntryLease>;assertCurrent(lease:NativeEntryLease):NativeEntryLease;
  openStream(options:{onClose:(reason:string)=>void}):Promise<NativeEntryStream>;close():void;};

declare const verifiedOperationBrand:unique symbol;
export interface SourceOperationObservation {readonly schema:'soty.source-readiness.v1';readonly appId:string;readonly profileDigest:string;
  readonly target:SourceProfile['target'];readonly sourceProfile:SourceProfile['sourceProfile'];readonly observedAt:number;
  readonly operation:'continue'|'read'|'execute'|'readProof';readonly status:'ready'|'context_verified'|'read_verified'|'commit_verified'|'receipt_verified'}
export interface SourceVerifiedOperation extends SourceOperationObservation {readonly [verifiedOperationBrand]:true}
export interface SqliteWorkspaceDomainContext {readonly database:import('node:sqlite').DatabaseSync;readonly principalId:string;readonly resourceId:string}
/** All callbacks are synchronous, on the same actual Native transaction. The
 * principal/resource come from the host, never from the request's JSON. */
export interface SqliteWorkspaceDomain {
  initialize?:(database:import('node:sqlite').DatabaseSync)=>void;
  inspect?:(database:import('node:sqlite').DatabaseSync)=>void;
  read:(current:SqliteWorkspaceDomainContext,args:{requestId:string;input:unknown})=>Record<string,unknown>;
  execute?:(current:SqliteWorkspaceDomainContext,args:{requestId:string;input:unknown})=>{workspaceReceipt:NativeWorkspaceReceipt;result?:unknown;data?:unknown};
}
/** Small optional store for trusted new domains. It is valid only during the
 * synchronous callback; `create` is available only inside `execute`. */
export interface SqlitePersonalDocumentsReader {
  list(collection:string,cursor?:string|null):{items:Array<{id:string;value:unknown}>;nextCursor:string|null};
}
export interface SqlitePersonalDocuments extends SqlitePersonalDocumentsReader {
  create(collection:string,id:string,value:unknown):void;
}
export interface SqlitePersonalDocumentsReadContext {readonly personal:SqlitePersonalDocumentsReader;readonly resourceId:string}
export interface SqlitePersonalDocumentsContext {readonly personal:SqlitePersonalDocuments;readonly resourceId:string}
export interface SqlitePersonalDocumentsDomain {
  personalDocuments:true;
  read:(current:SqlitePersonalDocumentsReadContext,args:{requestId:string;input:unknown})=>Record<string,unknown>;
  execute?:(current:SqlitePersonalDocumentsContext,args:{requestId:string;input:unknown})=>{workspaceReceipt:NativeWorkspaceReceipt;result?:unknown;data?:unknown};
}
/** Experimental new relay app; caller supplies trusted app/issuer and runs a
 * trusted domain. No sandbox, launcher or Source commit-time recheck. */
export function createRelayPersonalServer(options:{appId:string;issuer:string;databasePath:string;nonceDatabasePath:string;
  domain:SqlitePersonalDocumentsDomain;ui:string;listen:{host:'127.0.0.1';port:number};initialize?:boolean;fetch?:typeof globalThis.fetch;clock?:()=>number}):{
  server:import('node:http').Server;listen():Promise<number>;close():Promise<void>};
export interface SqliteRootWorkspaceHost {readonly native:SourceNativeAuthorityPort;readonly storage:SourceStoragePort;
  readonly profile:SourceProfile&{digest:string};inspect():{schema:'soty.sqlite-root-workspace.v1';appId:string;initialized:true;
    personalWorkspaces:boolean;keyCorrectness:'sample_verified'|'unproved_empty';connected:false};
  /** Trusted Node/operator control only; no public HTTP endpoint. Generation
   * changes on both revoke and re-enable, so old proofs cannot survive ABA. */
  setIdentityAccess(identity:{issuer:string;subject:string},active:boolean):void;close():void;
}
export const SQLITE_ROOT_WORKSPACE_LIMITS:Readonly<{sessions:4096;nonces:4096;receiptsPerPerson:20000}>;
export function createSqliteRootWorkspaceHost(options:{profile:SourceProfile;databasePath:string;cipherKey:Buffer;keyId:string;domain:SqliteWorkspaceDomain|SqlitePersonalDocumentsDomain;
  initialize?:boolean;/** Explicit opt-in; default false. No guest or Root app-owner grant. */personalWorkspaces?:boolean;clock?:()=>number;title?:string}):SqliteRootWorkspaceHost;
export function createRootWorkspaceServer(options:{profile:SourceProfile;transportKey:Buffer;connectorPort:number;host:SqliteRootWorkspaceHost;
  ui:{document:string|Uint8Array;script:string|Uint8Array;style:string|Uint8Array;sdk:string|Uint8Array};listen?:{host:'127.0.0.1';port:number};
  clock?:()=>number;onVerifiedOperation?:(observation:SourceVerifiedOperation)=>void}):{
    server:import('node:http').Server;bff:ReturnType<typeof createSourceAppBff>;host:SqliteRootWorkspaceHost;listen():Promise<number>;close():Promise<void>};
/** Optional filesystem diagnostics, also usable by a non-SQLite Native host.
 * Historical observations do not authorize a current operation. */
export function createSourceReadinessObserver(options:{directory:string;profile:SourceProfile;key:Buffer}):{
  observe(observation:SourceVerifiedOperation):void;
  inspect():{schema:'soty.source-doctor.v1';basis:'last_verified_server_observation';integrity:'missing'|'invalid'|'verified';
    lastObservation:SourceOperationObservation|null;writeStatus:'not_attempted'|'recorded'|'observation_unavailable';
    currentAuthority:'not_checked';clientAcknowledgement:'not_observed';connected:false;releaseReady:false};close():void;
};

/** Public error UI only; call from the existing HTTP error handler. No auth retry or authority. */
export function sendNativeEntryHttpError(request:IncomingMessage,response:ServerResponse,error:unknown):boolean;
