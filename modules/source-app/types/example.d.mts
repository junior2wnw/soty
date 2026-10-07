import type {SourceProfile} from './server.mjs';
import type {SourceRpProfile} from './source-rp.mjs';
import type {Server} from 'node:http';
import type {ProcessingPolicy,ProcessingEngine,ProcessingEnforcer} from './processing.mjs';
export function createOrdinaryAppServer(options:{profile:SourceProfile;transportKey:Buffer;connectorPort:number;rp:SourceRpProfile;
  databasePath:string;realmId:string;cipherKey:Buffer;keyId:string;initialize?:boolean;allowLoginProofMigration?:boolean;allowFeedbackJobsMigration?:boolean;
  feedbackProcessing?:{policy:ProcessingPolicy;engines:ProcessingEngine[];enforcer?:ProcessingEnforcer};newResource?:{title:string;guestEmpty?:boolean};appLabel?:string;allowEmptyGuest?:boolean;allowLinkedLogin?:boolean;clock?:()=>number}):Promise<{
  readonly server:Server;readonly store:unknown;readonly bff:unknown;readonly processing:null|{process(jobId:string,options?:{signal?:AbortSignal}):Promise<unknown>;close():void};listen():Promise<number>;close():Promise<void>;
}>;
