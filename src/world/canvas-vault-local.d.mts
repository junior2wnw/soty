import type {NoteCache} from '../../modules/notes/browser/cache.mjs';
import type {CanvasVaultLocalKeyScope} from './canvas-vault-scope.mjs';
export const CANVAS_VAULT_LOCAL_LIMITS:Readonly<{payloadBytes:number;recordBytes:number;totalBytes:number;records:number;profiles:number;iterations:number}>;
export const CANVAS_VAULT_RECOVERY_LIMITS:Readonly<{totalBytes:number;records:number}>;
export class CanvasVaultError extends Error{readonly code:string;operationId?:string;localDurable?:boolean;constructor(code?:string,details?:Record<string,unknown>)}
export interface CanvasVaultProfile{schema:'soty.vault.local-key.v1';accountId:string;projectId:string;slot:string;profileId:string;salt:string;iterations:number}
export interface CanvasVaultKey{profile:CanvasVaultProfile;key:CryptoKey}
export interface CanvasVaultHeader{schema:'soty.vault.local-record.v1';accountId:string;projectId:string;slot:string;recordId:string;purpose:'create'|'draft'|'write'|'rotate'|'recovery';version:number}
export interface CanvasVaultRecord extends CanvasVaultHeader{cipher:{schema:'soty.vault.local-aes-gcm.v1';profileId:string;salt:string;iv:string;data:string}}
export interface CanvasVaultSourceControl{schema:'soty.vault.local-source-control.v1';slot:string;source:CanvasVaultLocalKeyScope;profileId:string;generation:number;phase:'ready'|'rekeying';operationId:string|null;recordId:string|null;mode?:'source-rotate'|'password-recovery'}
export interface CanvasVaultLegacyInventory{schema:'soty.vault.local-inventory.v1';generation:number;profiles:Array<{slot:string;profileId:string;state:'pending'|'migrated';destination:string|null}>}
export interface CanvasVaultScopeSnapshot{inventory:CanvasVaultLegacyInventory|null;profiles:CanvasVaultProfile[];controls:CanvasVaultSourceControl[];links:Array<{slot:string;targetSlot:string;kind:'object'|'operation'}>;operations:Array<{slot:string;profileId:string}>}
export type CanvasVaultRecordVersion=Pick<CanvasVaultRecord,'recordId'|'version'>;
export interface CanvasVaultLocalStore{
 readonly scopeKey?:string;
 ensureProfile(profile:CanvasVaultProfile):Promise<CanvasVaultProfile>;
 ensureOperationProfile(profile:CanvasVaultProfile):Promise<CanvasVaultProfile>;
 ensureScopedProfile(scope:CanvasVaultLocalKeyScope,profile:CanvasVaultProfile,options?:{objectId?:string}):Promise<{profile:CanvasVaultProfile;control:CanvasVaultSourceControl;inventory:CanvasVaultLegacyInventory}>;
 inspectScopes():Promise<CanvasVaultScopeSnapshot>;
 get(id:string):Promise<CanvasVaultRecord|null>;list(slot?:string):Promise<CanvasVaultRecord[]>;listExact(slot:string):Promise<CanvasVaultRecord[]>;put(record:CanvasVaultRecord,expectedVersion:number):Promise<boolean>;remove(id:string,expectedVersion?:number):Promise<void>;
 replaceProfile(oldProfileId:string,profile:CanvasVaultProfile,expected:CanvasVaultRecordVersion[],records:CanvasVaultRecord[],options?:{retire?:CanvasVaultRecordVersion[]}):Promise<boolean>;
 beginSourceRekey(slot:string,oldProfileId:string,intent:CanvasVaultRecord,options?:{inventoryGeneration?:number;mode?:'source-rotate'|'password-recovery'}):Promise<CanvasVaultSourceControl>;
 commitSourceRekey(admission:CanvasVaultSourceControl,profile:CanvasVaultProfile,expected:CanvasVaultRecordVersion[],records:CanvasVaultRecord[],options?:{retire?:CanvasVaultRecordVersion[]}):Promise<boolean>;
 cancelSourceRekey(admission:CanvasVaultSourceControl,expectedRecordVersion:number):Promise<boolean>;
 migrateScopes(slot:string,from:Array<{slot:string;profileId:string;expected:CanvasVaultRecordVersion[]}>,destination:{profileId:string;expected:CanvasVaultRecordVersion[]},records:CanvasVaultRecord[],options?:{inventoryGeneration?:number;objectIds?:string[]}):Promise<boolean>;
 close?():void;
}
export function vaultCheck(value:unknown,code?:string):asserts value;
export function vaultId(value:unknown):value is string;
export function vaultErrorCode(error:unknown):string;
export function vaultClone<T>(value:T):T;
export function vaultBase64(bytes:Uint8Array):string;
export function vaultBytes(value:string,max?:number):Uint8Array<ArrayBuffer>;
export function canvasVaultProfile(accountId:string,projectId:string,slot:string,crypto?:Crypto):CanvasVaultProfile;
export function deriveCanvasVaultKey(profile:CanvasVaultProfile,password:string,options?:{crypto?:Crypto}):Promise<CanvasVaultKey>;
export function sealCanvasVaultRecord(key:CanvasVaultKey,header:CanvasVaultHeader,payload:unknown,options?:{crypto?:Crypto}):Promise<CanvasVaultRecord>;
export function openCanvasVaultRecord<T=unknown>(key:CanvasVaultKey,record:CanvasVaultRecord,options?:{crypto?:Crypto}):Promise<T>;
export function validateCanvasVaultRecord(value:unknown,accountId:string,projectId:string):CanvasVaultRecord;
export function createCanvasVaultLocalStore(accountId:string,projectId:string,options?:{origin?:string;indexedDB?:IDBFactory}):CanvasVaultLocalStore;
export function createCanvasVaultMemoryCache():NoteCache;
