import type {CanvasObject,CanvasObjectAuthority} from '../canvas-objects/contract.mjs';
export interface VaultActor{accountId:string;deviceId:string}
export interface VaultContent{contentType:string;bytesBase64:string}
export interface VaultRecord{schema:'soty.canvas-vault.v1';vaultId:string;ownerId:string;revision:number;kind:'native'|'placement'}
/** Metadata after current signed Object ACL, never a content grant or bearer token. */
export interface VaultLocalKeyScope{schema:'soty.vault.local-scope.v1';kind:'source'|'placement';id:string}
export type VaultCreateReceipt={schema:'soty.vault-create-receipt.v1';state:'missing'}|{schema:'soty.vault-create-receipt.v1';state:'committed';sourceRequestId:string;localKeyScope:VaultLocalKeyScope;binding:{providerId:'soty.vault';ref:string};revision:number};
export type VaultBindingScope={schema:'soty.vault-binding-scope.v1';state:'unavailable'}|{schema:'soty.vault-binding-scope.v1';state:'available';localKeyScope:VaultLocalKeyScope};
export type VaultRotateReceipt={schema:'soty.vault-rotate-receipt.v1';state:'missing'}|{schema:'soty.vault-rotate-receipt.v1';state:'committed';commandId:string;receipt:{vaultId:string;revision:number;committedAt:number};current:VaultRecord;localKeyScope:VaultLocalKeyScope};
export interface VaultChange extends VaultRecord{objectId?:string;reason:'create'|'protect'|'unlock'|'relock'|'write'|'rotate'|'sealing'|'convert'|'cancel';nativeBinding?:{providerId:string;ref:string};claimId?:string}
export interface VaultInvalidation{objectId:string;ownerId?:string;widgetId?:string;accountId?:string;deviceId?:string}
export interface VaultProtection{kind:'none'|'password';unlocked:boolean;generation:string|number;state?:'sealing'}
export interface VaultReadContent{contentType:string;bytes:Uint8Array;index:number;chunks:number;byteLength:number;revision:number}
export interface VaultNativeCapture{schema:'soty.native-vault-capture.v1';providerId:string;ownerId:string;ref:string;sourceStoreId:string;revision:number;size:number;contentType:string;metadata:Record<string,unknown>;readChunk(index:number):Uint8Array}
/** Host branded at runtime. A DTO, serialized copy or request JSON never grants this authority. */
export interface VaultCipherReceipt{schema:'soty.vault-cipher-authority.v1';claimId:string;providerId:string;ownerId:string;ref:string;sourceStoreId:string;sourceRevision:number;vaultId:string;cipherDigest:string}
export interface VaultErasureReceipt extends Omit<VaultCipherReceipt,'schema'>{schema:'soty.native-vault-erasure.v1';sealedRevision:number;state:'pending'|'confirmed';confirmedAt:number|null}
export interface VaultNativeProvider{providerId:string;capture(request:{ownerId:string;sourceId:string;expectedRevision:number},callback:(capture:VaultNativeCapture)=>unknown):unknown;eraseIfUnchanged(request:{cipherReceipt:VaultCipherReceipt}):VaultErasureReceipt;readErasureReceipt(request:{claimId:string}):VaultErasureReceipt|null;confirmErasure(request:{claimId:string}):VaultErasureReceipt}
export interface VaultConversion{schema:'soty.vault-conversion.v1';claimId:string;vaultId:string;state:'pending'|'protected';protected:boolean;source:{providerId:string;ref:string};revision:number;reason?:string;replayed?:boolean}
export const VAULT_CONTENT_BYTES:number;
export const VAULT_CHUNK_BYTES:number;
export const VAULT_FILE_BYTES:number;
export const canvasVaultOperations:Set<string>;
export function createCanvasVaultService(options:{databasePath:string;projectId:string;actorActive:(actor:VaultActor)=>boolean;
 withActorAuthorityFence:(originalActor:unknown,callback:()=>unknown)=>unknown;
 withProtectionTarget:(request:{actor:unknown;objectId:string;mode:'owner'|'read'},callback:(object:CanvasObject&{actor:VaultActor;source?:{revision:number;storeId?:string}})=>unknown)=>unknown;
 withObjectAuthority:(context:CanvasObjectAuthority,callback:(context:CanvasObjectAuthority)=>unknown)=>unknown;
 withRuntimeTarget?:(request:{actor:VaultActor;objectId:string},callback:(object:CanvasObject&{actor:VaultActor})=>unknown)=>unknown;
 placementProviders?:string[];allowFormat2Migration?:boolean;nativeProviders?:VaultNativeProvider[];onChange?:(change:VaultChange)=>void;onInvalidate?:(change:VaultInvalidation)=>void;clock?:()=>number;
}):Readonly<{
 operations:Set<string>;challengeStorage:'memory';execute(request:{actor:unknown;op:string;args:Record<string,unknown>}):Promise<unknown>;
 provider:{providerId:'soty.vault';withBindingAuthority:(request:unknown,callback:(scope:unknown)=>unknown)=>unknown;project:(context:CanvasObjectAuthority)=>unknown};
 withProtectionAuthority(request:{actor:VaultActor|null;object:CanvasObject},callback:(state:VaultProtection)=>unknown):unknown;
 readContent(request:{context:CanvasObjectAuthority;index?:number}):VaultReadContent;
 withCipherReceiptAuthority(receipt:VaultCipherReceipt,callback:(proof:VaultCipherReceipt)=>unknown):unknown;
 readConvertedSource(request:{context:CanvasObjectAuthority;part:'note'|'noteMetadata'|'materialMetadata'|'chunk';index?:number}):unknown;
 reconcileConversions():{activated:number;pending:number};storageIdentity():Readonly<{projectId:string;schemaVersion:1|2}>;
 checkWidgetRuntime(request:{widgetId:string;actor:VaultActor}):true;close():void;
}>;
