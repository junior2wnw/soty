import type {IncomingMessage, ServerResponse} from 'node:http';
import type {SourceRpProfile, SourceRpMarker, SourceRpCurrentProof} from './source-rp.d.mts';
export {createSourceRpProtocol, createSourceRpSessionService, sourceRpCipherBinding, SOURCE_RP_PROFILE} from './source-rp.mjs';
export interface SourceProfile {schema:'soty.selected-human-embed.v2';appId:string;connector:{linkId:string;hostDeviceId:string;connectorId:string};target:{revision:number;digest:string};
  sourceProfile:{id:string;version:number;digest:string};resource:{registryId:string;environmentId:string;tenantId:string;appId:string;resourceId:string;selection:{kind:'soty.resource.v1';nativeId:string;incarnationId:string}};
  issuer:string;clientId:string;embedOrigin:string;nativeOrigin:string;parentOrigin:string}
declare const nativePortBrand:unique symbol, commitPortBrand:unique symbol;
export interface NativeCommitPort {readonly [commitPortBrand]:true;/** Call once synchronously INSIDE the Native transaction. */commit<T>(action:()=>T):T}
export interface NativeBinding {identity:{issuer:string;subject:string};rootPrincipal:{accountId:string;deviceId:string};humanPrincipal:unknown;resource:SourceProfile['resource'];semanticDigest:string;operation:string;sessionIdHash?:string}
export interface SourceNativeAuthorityPort {readonly [nativePortBrand]:true}
export interface NativeHooks<Proof=unknown> {
  capture(binding:Readonly<NativeBinding>,nativeBrowserRequest?:IncomingMessage):Promise<Proof>;
  withCurrent<T>(proof:Proof,binding:Readonly<NativeBinding>,action:()=>T):T;
  verifyLegacy?:Function;linkVerifiedIdentity?:(proof:Proof,binding:Readonly<NativeBinding>,identity:{issuer:string;subject:string})=>unknown;
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
  claimInteraction(hash:string,revision:number,privatePkce:unknown):Promise<boolean>;claimCallback(hash:string,revision:number):Promise<boolean>;
  completeInteraction(record:unknown,finalNativeLink:()=>unknown):Promise<boolean>;
  readSession(hash:string):Promise<any>;readCompletion(hash:string):Promise<any>;consumeCompletion(hash:string,sessionHash:string,finalNativeAssert:()=>unknown):Promise<{token:string}|null>;
  readTokenProof(session:unknown):Promise<{accessToken:string;expiresAt:number}>;revokeSession(hash:string):Promise<void>;
}
export function createSourceAppBff(options:{profile:SourceProfile;transportKey:Buffer;connectorPort:number;storage:SourceStoragePort;native:SourceNativeAuthorityPort;rp:SourceRpProfile;
  clock?:()=>number;allowCreateEmptyGuest?:boolean;rpSessions?:{currentProof(marker:SourceRpMarker,hostOptions?:{minimumAccessRemainingMs?:number}):Promise<SourceRpCurrentProof>}}):{
  readonly profile:SourceProfile;handleRequest(req:IncomingMessage,res:ServerResponse):Promise<boolean>;close():void;
};
export const STANDARD_SELECTED_SOURCE:Readonly<{id:'soty.standard-resource';version:1;digest:string}>;
export const STANDARD_SOURCE_CONTRACT:Readonly<unknown>;
export {SOURCE_FEEDBACK_LIMITS} from './browser.mjs';
