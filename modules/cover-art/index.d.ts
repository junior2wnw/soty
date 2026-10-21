export type CoverKind='app'|'profile';
export type CoverActor={accountId:string;deviceId:string;label?:string;[key:string]:unknown};
export type CoverCrop={x:number;y:number;zoom:number};
export type CoverExpected={requestId:string;targetId:string;expectedTargetRevision:number;expectedPolicyEpoch:number;expectedCoverRevision:number};
export type CoverTarget={ownerAccountId:string;targetRevision:number;policyEpoch:number;state:'active'|'revoked';canManage:boolean;canRead:boolean};
export type CoverHead={revision:number;mode:'inherit'|'none'|'custom';version:string|null;assetRef:string|null;artVersion?:number;cropApplied?:true;crop?:CoverCrop};
export type CoverDraft={id:string;state:'processing'|'ready'|'committed'|'cancelled'|'expired'|'failed';expiresAt:number;assetRef:string|null};
export type CoverReply={receipt:Record<string,unknown>;current:CoverHead;replayed:boolean;draft?:CoverDraft|null;ownerProjection?:unknown;maintenancePending?:true};
export interface CoverStore {
  prepareUpload(actor:CoverActor,args:CoverExpected&{mime:'image/png'|'image/jpeg'|'image/webp';bytes:Uint8Array;crop?:CoverCrop}):Promise<CoverReply>;
  prepareFromVersion(actor:CoverActor,args:CoverExpected&{versionId:string;crop?:CoverCrop}):Promise<CoverReply>;
  commit(actor:CoverActor,args:{requestId:string;targetId:string;draftId:string}):CoverReply;
  restore(actor:CoverActor,args:CoverExpected&{versionId:string}):CoverReply;
  remove(actor:CoverActor,args:CoverExpected&{mode?:'none'|'inherit'}):CoverReply;
  cancel(actor:CoverActor,args:{requestId:string;targetId:string;draftId:string}):CoverReply;
  read(actor:CoverActor,args:{targetId:string;versionId?:string;width?:number}):{current:CoverHead;versionId?:string;image:null|{role:string;width:number;height:number;mime:string;sha256:string;bytes:Uint8Array}};
  status(actor:CoverActor,args:{targetId:string;requestId?:string;draftId?:string}):{current:CoverHead;scope:{storeInstanceId:string;kind:CoverKind;targetId:string;ownerAccountId:string;targetRevision:number;policyEpoch:number;coverRevision:number};receipt:Record<string,unknown>|null;draft:CoverDraft|null;serverTime:number};
  versions(actor:CoverActor,args:{targetId:string;limit?:number}):{current:CoverHead;versions:{id:string;version:number;mode:'inherit'|'none'|'custom';createdAt:number;sourceOrigin:'upload'|'legacy'|null;restorable:boolean}[]};
  generationAvailability():{available:false;reason:'cover_generation_provider_unavailable'};
  /** Host maintenance only, never grant this whole object as an agent tool. */
  collect(options?:{pressure?:boolean}):{changed:number;freedBytes:number};
  audit():{ok:true;usage:{encodedBytes:number;reservedBytes:number;drafts:number;receipts:number}};
  dispose():void;
}
export interface CoverAuthorityPorts {
  kind:CoverKind;
  instanceId:string;
  /** Same DatabaseSync used by the owning target service. Engine owns its
   * individual BEGIN/COMMIT; call it before legacy service transactions. */
  db:unknown;
  withAuthority<T>(actor:CoverActor,callback:()=>T):T;
  readTarget(actor:CoverActor,targetId:string):CoverTarget|null;
  onCommit(input:{actor:CoverActor;targetId:string;mode:'inherit'|'none'|'custom';assetId:string|null;revision:number;now:number}):void;
  projectOwner?(actor:CoverActor,targetId:string):unknown;
  /** Share one bounded decoder across the two stores in one worker process. */
  decoder:{decode(ownerId:string,input:unknown):Promise<unknown>};
  clock?:()=>number;
  limits?:Record<string,number>;
}
/** Future configured-provider adapter contract; there is intentionally no
 * implementation/registration in this module. A host durable ledger must save
 * requestKey/binding/budget BEFORE a first dispatch, and must never redispatch
 * an unknown accepted request. Codex image_gen is not such a provider. */
export interface DurableCoverGenerationProvider {
  readonly bindingDigest:string;
  readAccountPolicy():Promise<{available:boolean;accountReference:string;currency:string;boundedPricing:boolean}>;
  dispatchOnce(input:{providerRequestKey:string;brief:string;selectedAssetRefs:string[];budget:{maxCalls:1;maxCostMicrounits:number;currency:string}}):Promise<{state:'accepted';providerJobRef:string}|{state:'unknown';providerJobRef?:string}>;
  readStatus(providerJobRef:string):Promise<{state:'pending'|'unknown'|'failed'}|{state:'complete';selectedResultRef:string;actualCostMicrounits:number}>;
}
