export interface FieldCamera {x:number;y:number;scale:number}
export interface FieldPoint {x:number;y:number}
export interface FieldViewport {width:number;height:number}
export interface FieldBounds {left:number;top:number;right:number;bottom:number}
export const UNIFIED_SCALE_MIN:number;
export const UNIFIED_SCALE_MAX:number;
export function normalizeFieldCamera(camera?:Partial<FieldCamera>):FieldCamera;
export function fieldWorldToScreen(point:FieldPoint,camera:FieldCamera,viewport:FieldViewport):FieldPoint;
export function fieldScreenToWorld(point:FieldPoint,camera:FieldCamera,viewport:FieldViewport):FieldPoint;
export function zoomFieldCamera(camera:FieldCamera,nextScale:number,anchor:FieldPoint,viewport:FieldViewport):FieldCamera;
export function fitFieldCamera(bounds:FieldBounds|null,viewport:FieldViewport,options?:{padding?:number;maxScale?:number;minScale?:number}):FieldCamera;
export function fieldLevelOfDetail(scale:number,previous?:'overview'|'context'|'detail'):'overview'|'context'|'detail';
