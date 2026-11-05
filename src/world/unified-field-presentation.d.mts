import type {FieldPoint} from './unified-field-camera.mjs';
export type FieldPresentation={shape:'hex'|'rect'|'ellipse'}|{shape:'polygon';points:FieldPoint[]};
export function validateFieldPresentation(value:unknown):FieldPresentation|null;
export function fieldPresentationPoints(presentation:FieldPresentation):FieldPoint[];
export function fieldPresentationClip(presentation:FieldPresentation):string;
export function fieldPointInPolygon(point:FieldPoint,polygon:readonly FieldPoint[]):boolean;
/** Local-pixel anchor on the actual mask, before camera transformation. */
export function fieldResizeAnchor(presentation:FieldPresentation,width:number,height:number):FieldPoint;
export function fieldResizeDimensions(presentation:FieldPresentation,width:number,height:number,delta:FieldPoint):{width:number;height:number};
