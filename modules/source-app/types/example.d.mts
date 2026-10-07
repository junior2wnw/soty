import type {SourceProfile} from './server.mjs';
import type {SourceRpProfile} from './source-rp.mjs';
import type {Server} from 'node:http';
export function createOrdinaryAppServer(options:{profile:SourceProfile;transportKey:Buffer;connectorPort:number;rp:SourceRpProfile;
  databasePath:string;realmId:string;cipherKey:Buffer;keyId:string;initialize?:boolean;allowLoginProofMigration?:boolean;newResource?:{title:string;guestEmpty?:boolean};appLabel?:string;allowEmptyGuest?:boolean;clock?:()=>number}):Promise<{
  readonly server:Server;readonly store:unknown;readonly bff:unknown;listen():Promise<number>;close():Promise<void>;
}>;
