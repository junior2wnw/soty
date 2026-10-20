import type {OtsovProjectArtifact,EditorReadback} from '../../modules/otsov-editor-handoff/wire.mjs';
export interface EditorPeer {window:WindowProxy;origin:string;appId:string;accountId:string;generation:number;slot:object}
export function createOtsovEditorHost(options:{view:Window;readPeer():EditorPeer|null;assertPeer(peer:EditorPeer,signal:AbortSignal):Promise<boolean>;makeChannel?():MessageChannel;timeoutMs?:number}):{open(artifact:OtsovProjectArtifact,options:{requestId:string;signal:AbortSignal;assertCurrent():Promise<void>}):Promise<EditorReadback>;close():void};
