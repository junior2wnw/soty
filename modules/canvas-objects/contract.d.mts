import type {AnyFieldEntityRef} from '../field/contract.mjs';
export type CanvasMask={shape:'hex'|'rect'|'ellipse'}|{shape:'polygon';points:{x:number;y:number}[]};
export type CanvasOwnerRepresentation=AnyFieldEntityRef[];
export interface CanvasBinding{providerId:string;ref:string}
export interface CanvasPresentation{mask:CanvasMask;fontSize?:number}
export interface CanvasVisibility{audience:'private'|'public'|'restricted';accountIds:string[];communityIds:string[]}
export interface CanvasObject{schema:'soty.canvas-object.v1';objectId:string;ownerId:string;revision:number;accessRevision?:number;binding:CanvasBinding;presentation:CanvasPresentation;visibility:CanvasVisibility;hidden:{owner:boolean;others:boolean};state:'active'|'removed';protection:{kind:'none'|'password'}}
export interface CanvasObjectAuthority{schema:'soty.canvas-object-authority.v1';right:'read';objectId:string;objectRevision:number;objectAccessRevision:number;ownerId:string;providerId:string;ref:string;audience:CanvasVisibility['audience'];viewer:{kind:'public'}|{kind:'account';accountId:string;deviceId:string}}
export const OBJECT_SCHEMA:'soty.canvas-object.v1';
export const OBJECT_LIMITS:Readonly<{objects:2048;recipients:256;projectionBytes:65536;projectionBatchBytes:1048576;contentBytes:1048576;polygonPoints:128}>;
export class CanvasObjectError extends Error{readonly code:string;readonly status:number;constructor(code?:string,status?:number)}
export function check(value:unknown,code?:string,status?:number):asserts value;
export function exact(value:unknown,keys:string[],required?:string[]):void;
export function identity(value:unknown):string;
export function binding(value:unknown):CanvasBinding;
export function ownerRepresentation(value:unknown):CanvasOwnerRepresentation;
export function validateCanvasMask(value:unknown):CanvasMask;
export function presentation(value:unknown):CanvasPresentation;
export function visibility(value:unknown):CanvasVisibility;
export function hidden(value:unknown):CanvasObject['hidden'];
export function protection(value:unknown):CanvasObject['protection'];
export function jsonProjection(value:unknown):unknown;
export function boundedContent(value:unknown):unknown;
