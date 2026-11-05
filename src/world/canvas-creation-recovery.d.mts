import type {CanvasCreateRecord} from './canvas-create-flow.mjs';
import type {CanvasVaultKey,CanvasVaultLocalStore} from './canvas-vault-local.mjs';
import type {CanvasVaultRecoveryScope} from './canvas-vault-scope.mjs';
export const CANVAS_CREATION_RECOVERY_LIMITS:Readonly<{records:64;partBytes:524288;metadataBytes:number;blobBytes:number}>;
export interface CanvasCreationRecoverySource{providerId:'soty.notes'|'soty.materials';ref:string;ownerId:string}
export interface CanvasCreationRecoveryOptions{
 session:CanvasVaultKey;store:CanvasVaultLocalStore;accountId:string;projectId:string;objectId:string;source:CanvasCreationRecoverySource;
 /** Must capture the EXACT current Vault session/epoch, not only the account. */
 isCurrent():boolean;crypto?:Crypto;
 /** Internal SDK-branded adapter, not a JSON cryptoSlot or an ACL. */
 cryptoScope?:CanvasVaultRecoveryScope;
}
export interface CanvasCreationRecoveryReceipt{
 readonly schema:'soty.creation-recovery.receipt.v1';readonly archiveId:string;readonly objectId:string;
 readonly source:Readonly<CanvasCreationRecoverySource>;readonly profileId:string;
 readonly cipherRecords:ReadonlyArray<Readonly<{recordId:string;version:number}>>;
 readonly creations:ReadonlyArray<Readonly<{operationId:string;version:number}>>;
}
export interface CanvasCreationRecovery{archiveId:string;source:CanvasCreationRecoverySource;records:CanvasCreateRecord[];receipt:CanvasCreationRecoveryReceipt}
/** Private only. Never calls a Source or retires/deletes the plaintext journal. */
export function archiveCreationIntents(options:CanvasCreationRecoveryOptions&{records:CanvasCreateRecord[]}):Promise<CanvasCreationRecoveryReceipt>;
/** A receipt is a selector, not proof: freshly authenticates every committed row against original records. */
export function verifyCreationRecoveries(options:CanvasCreationRecoveryOptions&{records:CanvasCreateRecord[];receipt?:CanvasCreationRecoveryReceipt}):Promise<CanvasCreationRecoveryReceipt>;
/** Full private original data/Blob is returned in RAM only for explicit recovery. */
export function readCreationRecoveries(options:CanvasCreationRecoveryOptions&{archiveId?:string}):Promise<CanvasCreationRecovery[]>;
