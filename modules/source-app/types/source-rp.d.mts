/** Private server-only Source RP ports. These types grant no account/tenant/resource permission. */
export const SOURCE_RP_PROFILE:'soty.human-rp-renewal.v1';
export interface SourceRpProfile {issuer:string;clientId:string;clientSecret:string;redirectUri:string;renewalProfile?:typeof SOURCE_RP_PROFILE}
export interface SourceRpTokenProof {accessToken:string;refreshToken:string;nonce:string}
export interface SourceRpMarker {
  sessionIdHash:string;profileDigest:string;bindingDigest:string;issuer:string;subject:string;
  sessionExpiresAt:number;createdAt:number;
}
export interface SourceRpHead {
  sessionIdHash:string;profileDigest:string;bindingDigest:string;sessionExpiresAt:number;accessExpiresAt:number;
  revision:number;state:'idle'|'refreshing'|'unknown'|'revoked';proofCipher:string;keyId:string;
  claimId:string|null;claimedAt:number|null;lastAttemptId:string|null;createdAt:number;updatedAt:number;
}
export interface SourceRpProtocol {
  ready():Promise<void>;
  start():Promise<{verifier:string;state:string;nonce:string;location:string}>;
  exchange(callback:URL,intent:{verifier:string;state:string;nonce:string}):Promise<{issuer:string;subject:string;accessToken:string;expiresAt:number;refreshToken?:string;nonce?:string}>;
  renew(proof:SourceRpTokenProof&{subject:string}):Promise<SourceRpTokenProof&{expiresAt:number}>;
  currentSubject(accessToken:string,expectedSubject:string):Promise<string>;
}
/** Each CAS must include the Source-owned native session/link/current profile/binding
 * conditions captured by captureSourceAuthority. No remote IO runs in that transaction. */
export interface SourceRpStoragePort<Authority=unknown> {
  read(sessionIdHash:string):Promise<SourceRpHead|null>;
  captureSourceAuthority(marker:SourceRpMarker):Promise<Authority>;
  assertSourceAuthority(marker:SourceRpMarker,authority:Authority):Promise<void>;
  claim(input:{marker:SourceRpMarker;authority:Authority;expected:SourceRpHead;claimId:string;claimedAt:number}):Promise<boolean>;
  finish(input:{marker:SourceRpMarker;authority:Authority;expected:SourceRpHead;claimId:string;proofCipher:string;accessExpiresAt:number;updatedAt:number}):Promise<boolean>;
  block(input:{expected:SourceRpHead;claimId:string;state:'unknown'|'revoked';updatedAt:number}):Promise<void>;
  compactExpired?(input:{now:number;limit:number}):Promise<void>;
}
export interface SourceRpServiceOptions<Authority=unknown> {
  storagePort:SourceRpStoragePort<Authority>;protocol:SourceRpProtocol;profileDigest:string;keyId:string;
  encrypt(model:'SourceRpRenewal',binding:string,value:SourceRpTokenProof):Promise<string>;
  decrypt(model:'SourceRpRenewal',binding:string,cipher:string,keyId:string):Promise<SourceRpTokenProof>;
  clock?:()=>number;
}
/** Opaque host result after fresh userinfo. Credentials remain inside the service. */
export interface SourceRpCurrentProof {readonly issuer:string;readonly sub:string;readonly expiresAt:number;readonly sessionExpiresAt:number;readonly sessionGeneration:number;assertCurrent():Promise<void>}
/** Trusted Source host only, never an HTTP/manifest/author-controlled option.
 * One bounded rotation may be attempted; the result's ACTUAL expiry must still
 * be checked. Insufficient absolute lifetime/short provider AT does not loop. */
export interface SourceRpCurrentProofOptions {minimumAccessRemainingMs?:number}
export class SourceRpError extends Error {readonly code:string;readonly status:number;constructor(code:string,status?:number)}
export function createSourceRpProtocol(profile:SourceRpProfile,options?:{clock?:()=>number}):SourceRpProtocol&{readonly profile:SourceRpProfile};
export const SOURCE_RP_LIMITS:Readonly<{seconds:86400;heads:4096;perAccount:8;inFlight:16;gcBatch:128;rotations:512;earlyRefreshMs:30000;minimumAccessRemainingMaxMs:240000;waitMs:6000;staleClaimMs:20000}>;
export function sourceRpCipherBinding(marker:SourceRpMarker,revision:number):string;
export function createSourceRpSessionService<Authority>(options:SourceRpServiceOptions<Authority>):{
  currentProof(marker:SourceRpMarker,hostOptions?:SourceRpCurrentProofOptions):Promise<SourceRpCurrentProof>;
  compactExpired():Promise<void>;
};
