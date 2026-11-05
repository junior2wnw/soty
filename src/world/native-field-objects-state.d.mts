import type {AnyFieldEntityRef as FieldEntityRef} from '../../modules/field/contract.mjs';
import type {SessionState} from '../../modules/notes/browser/state.mjs';
export function nativeFieldRef(value:FieldEntityRef):{provider:'notes'|'materials';id:string}|null;
export function nativeNoteFontSize(value:unknown):number;
export function mayRefreshNativeNote(state:SessionState|null,focused:boolean,remoteRevision?:number):boolean;
export function nativeNoteStatus(state:SessionState,stale?:boolean):string;
