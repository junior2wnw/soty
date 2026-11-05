export interface CanvasWidgetSourcePin{revision:number;digest:string;profile:string}
export interface CanvasWidgetEntry{domainId:string;path:string}
export interface CanvasWidget{schema:'soty.canvas-widget.v1';widgetId:string;ownerId:string;revision:number;appId:string;entry:CanvasWidgetEntry;source:CanvasWidgetSourcePin;contractVersion:1;state:'active'|'removed'}
export interface CanvasWidgetView{schema:'soty.canvas-widget-view.v1';kind:'widget';widgetId:string;instanceId:string;state:string;app:{appId:string;name:string;status:string;ownerAccountId:string;entry:{appId:string;domainId:string;origin:string;path:string};audience?:string};entry:{appId:string;domainId:string;origin:string;path:string};source:CanvasWidgetSourcePin;expectedSource:CanvasWidgetSourcePin;runtimeAllowed:boolean;canRefresh:boolean}
export const WIDGET_SCHEMA:'soty.canvas-widget.v1';
export const WIDGET_LIMITS:Readonly<{instances:1024;commands:50000}>;
export class CanvasWidgetError extends Error{readonly code:string;readonly status:number;constructor(code?:string,status?:number)}
export function check(value:unknown,code?:string,status?:number):asserts value;
export function exact(value:unknown,keys:string[],required?:string[]):void;
export function identity(value:unknown):string;
export function widgetIdentity(value:unknown):string;
export function sourcePin(value:unknown):CanvasWidgetSourcePin;
export function samePin(left:CanvasWidgetSourcePin,right:CanvasWidgetSourcePin):boolean;
