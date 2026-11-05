import type {Draft} from '../../modules/notes/browser/state.mjs';
import type {CanvasVaultKey,CanvasVaultLocalStore} from './canvas-vault-local.mjs';
import type {NativeSourceIdentity} from './native-cache-transition.mjs';
import type {CanvasVaultRecoveryScope} from './canvas-vault-scope.mjs';
export interface NativeNoteRecovery {recordId:string;version:number;source:NativeSourceIdentity;draft:Draft}
export function archiveNativeNoteDrafts(input:{session:CanvasVaultKey;store:CanvasVaultLocalStore;accountId:string;projectId:string;objectId:string;source:NativeSourceIdentity;drafts:Draft[];cryptoScope?:CanvasVaultRecoveryScope;crypto?:Crypto}):Promise<{schema:'soty.native-note-recovery-receipt.v1';objectId:string;records:Pick<NativeNoteRecovery,'recordId'|'version'>[]}>;
export function readNativeNoteRecoveries(input:{session:CanvasVaultKey;store:CanvasVaultLocalStore;objectId:string;accountId:string;cryptoScope?:CanvasVaultRecoveryScope;crypto?:Crypto}):Promise<NativeNoteRecovery[]>;
