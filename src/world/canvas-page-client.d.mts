import type {AnyFieldDocument,AnyFieldEntityRef} from '../../modules/field/contract.mjs';
import type {CanvasPresentation} from '../../modules/canvas-objects/contract.mjs';
import type {CanvasProjection} from './canvas-field-content';
import type {DirectoryApi} from './field-directory';
export interface CanvasPageAppView {schema:'soty.canvas-page-app.v1';kind:'app';app:{appId:string;name:string;status:string};entry:{appId:string;domainId:string;origin:string;path:string};runtimeAllowed:boolean}
export interface CanvasPageItem {shortcutId:string;entity:AnyFieldEntityRef;presentation:CanvasPresentation;protection:{kind:'none'|'password'};canUnlock?:boolean;view:NonNullable<CanvasProjection['view']>|CanvasPageAppView}
export interface CanvasPageView {schema:'soty.canvas-page-view.v1';pageId:string;viewRevision:string;banner:{kind:'generic';title:string}|{kind:'profile';profile:{profileId:string;displayName:string;avatarColor:string;avatarRevision:number|null}};field:AnyFieldDocument;items:CanvasPageItem[];nextCursor:string|null}
export function parseCanvasPageRoute(value:string):string|null;
export function canvasPageLink(pageId:string,origin:string):string;
export function createCanvasPageClient(options:{pageId:string;api?:DirectoryApi;accountId?:string;fetch?:typeof fetch;isCurrent?:()=>boolean;signal?:AbortSignal}):{
 load(first?:CanvasPageView,options?:{signal?:AbortSignal}):Promise<CanvasPageView>;
 wait(after:string,options?:{signal?:AbortSignal}):Promise<{schema:'soty.canvas-page-update.v1';changed:boolean;viewRevision:string;view:CanvasPageView|null}>;
 content(objectId:string,part:string,index?:number,options?:{signal?:AbortSignal}):Promise<unknown>;
};
