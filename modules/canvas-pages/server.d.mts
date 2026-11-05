import type {FieldDocumentV2,AnyFieldEntityRef} from '../field/contract.mjs';
import type {CanvasObjectAuthority,CanvasPresentation} from '../canvas-objects/contract.mjs';
export interface CanvasPageActor{accountId:string;deviceId:string}
export interface CanvasPageSelection{schema:'soty.canvas-page-selection.v1';fieldRevision:number;items:{shortcutId:string;entity:{kind:'object'|'app';id:string};expectedAccessRevision?:number}[]}
export interface CanvasPage{schema:'soty.canvas-page.v1';pageId:string;ownerId:string;revision:number;enabled:boolean;audience:'private'|'unlisted'|'public';selection?:CanvasPageSelection}
export type CanvasPageBanner={kind:'generic';title:'Полотно'}|{kind:'profile';profile:{profileId:string;displayName:string;avatarColor:string;avatarRevision:number|null}};
export interface CanvasPageView{schema:'soty.canvas-page-view.v1';pageId:string;viewRevision:string;banner:CanvasPageBanner;field:FieldDocumentV2;
 items:{shortcutId:string;entity:AnyFieldEntityRef;presentation:CanvasPresentation;protection:{kind:'none'|'password'};view:unknown;canUnlock?:boolean}[];nextCursor:string|null}
export interface CanvasPageAuthority{schema:'soty.canvas-page-authority.v1';pageId:string;objectId:string;viewer:{kind:'public'}|({kind:'account'}&CanvasPageActor)}
export const CANVAS_PAGE_BYTES:number;
export const canvasPageOperations:Set<string>;
export function createCanvasPagesService(options:{databasePath:string;projectId:string;actorActive:(actor:CanvasPageActor)=>boolean;
 withActorAuthorityFence:(originalActor:unknown,callback:()=>unknown)=>unknown;withCanvasSource:(request:{ownerId:string;viewer:CanvasPageActor|null},callback:(source:unknown)=>unknown)=>unknown;
 objects:{project:(request:{actor?:unknown;objectIds:string[]})=>unknown;withObjectRead:(request:{actor?:unknown;objectId:string},callback:(context:CanvasObjectAuthority)=>unknown)=>unknown;withObjectAuthority:(context:CanvasObjectAuthority,callback:(context:CanvasObjectAuthority)=>unknown)=>unknown;
 pageSelectionConfigured?:()=>boolean;withPageSelectionTarget?:(request:{actor:unknown;objectId:string;expectedAccessRevision:number},callback:(object:unknown)=>unknown)=>unknown;projectPageObject?:(request:{actor?:unknown;objectId:string;pageContext:object})=>unknown;withPageObjectRead?:(request:{actor?:unknown;objectId:string;pageContext:object},callback:(context:CanvasObjectAuthority)=>unknown)=>unknown};
 allowFormat2Migration?:boolean;
 withLegacyAppAuthority?:(request:{actor:CanvasPageActor|null;appId:string},callback:(snapshot:unknown)=>unknown)=>unknown;onChange?:(page:CanvasPage)=>void;clock?:()=>number;
}):Readonly<{
 operations:Set<string>;execute(request:{actor:unknown;op:string;args:Record<string,unknown>}):unknown;
 read(request:{pageId:string;actor?:unknown;cursor?:string;limit?:number}):CanvasPageView;
 withPageObjectRead(request:{pageId:string;objectId:string;actor?:unknown},callback:(context:CanvasPageAuthority)=>unknown):unknown;
 withPageObjectAuthority(context:CanvasPageAuthority,callback:(context:CanvasObjectAuthority)=>unknown):unknown;close():void;
 withSelectionAuthority(context:object,callback:(scope:unknown)=>unknown):unknown;
}>;
