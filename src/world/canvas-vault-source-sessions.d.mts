import type {NativeSessionPort} from '../../modules/notes/browser/native-local.mjs';
import type {Draft} from '../../modules/notes/browser/state.mjs';
export function registerCanvasVaultDraft(options:{scopeKey:string;objectId:string;noteId:string;port:NativeSessionPort;isCurrent():boolean}):()=>void;
export function drainCanvasVaultDrafts(scopeKey:string):Promise<void>;
export function canvasVaultDraftSessions(scopeKey:string,objectId:string,noteId:string):NativeSessionPort[];
export function recoverCanvasVaultDrafts(scopeKey:string,options:{isCurrent():boolean;write(value:{objectId:string;noteId:string;draft:Draft}):Promise<void>}):Promise<void>;
