import type { ChatDraftDisk } from './attachment-outbox.mjs';
import type { LegacyDraft } from './chat-state.mjs';
export interface ChatDraftHead { id:string; writer:string; text:string; replyTo:string|null }
export interface ChatDraftView { value:{text:string;replyTo:string|null}; heads:ChatDraftHead[]; selectedId:string|null; dirty:boolean; error:string|null;generation:number;pendingLegacy:(LegacyDraft&{id:string})[] }
export interface ChatDraftSession { ready:Promise<unknown>; scope:{accountId:string;conversationKey:string}; read():ChatDraftView; refresh():Promise<void>; isReady():boolean; edit(text:string,replyTo:string|null):void; flush():Promise<boolean>; hasUnsavedChanges():boolean; recoverLegacy(candidate:LegacyDraft):void; select(expectedIds:string[],id:string):Promise<boolean>; acknowledge(id:string|undefined):Promise<void>; close():void; reopen(callback:(state:ChatDraftView)=>void):ChatDraftSession }
export function createChatDraftSession(options:{disk:ChatDraftDisk;accountId:string;conversationKey:string;legacy:{text:string;replyTo:string|null};legacyCandidates?:LegacyDraft[];retainLegacy?:boolean;changed?(state:ChatDraftView):void}):ChatDraftSession;
