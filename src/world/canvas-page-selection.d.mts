import type {AnyFieldEntityRef} from '../../modules/field/contract.mjs';
import type {CanvasMask} from '../../modules/canvas-objects/contract.mjs';
import type {DirectoryApi} from './field-directory';
import type {CanvasPageSelection} from '../../modules/canvas-pages/server.mjs';
export interface CanvasPageChoice {shortcutId:string;entity:{kind:'object'|'app';id:string};title:string;kind:string;mask:CanvasMask;available:boolean;expectedAccessRevision?:number;defaultSelected:boolean;hint:string;snippet:string;visitorAccess:'unavailable'|'blocked'|'locked'|'page-only'|'conditional'|'normal'|'unknown';accessTarget?:{kind:'app'|'object';id:string}}
export interface CanvasPageChoiceSnapshot {fieldRevision:number;choices:CanvasPageChoice[];excludedCount:number}
export function loadCanvasPageChoices(options:{api:DirectoryApi;accountId:string;isCurrent?:()=>boolean;prepare?:()=>Promise<void>}):Promise<CanvasPageChoiceSnapshot>;
export function createCanvasPageSelection(snapshot:CanvasPageChoiceSnapshot,selectedIds:Iterable<string>):CanvasPageSelection;
export function canvasPageSelectionKey(items:CanvasPageSelection['items']):string;
