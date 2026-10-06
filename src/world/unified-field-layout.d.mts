import type {FieldContext,FieldShortcut,FieldDocument,FieldEntityRef} from '../../modules/field/contract.mjs';
import type {FieldBounds,FieldPoint} from './unified-field-camera.mjs';
export interface FieldDirectoryEntity {entity:FieldEntityRef;title:string;description?:string;symbol?:string;color?:string;avatarUrl?:string;avatarRevision?:number|null;coverKey?:string;source?:'public'|'member'|'owner'|'builtin';updatedAt?:number;unreadCount?:number;online?:boolean}
export interface FieldScene {contexts:FieldContext[];shortcuts:FieldShortcut[]}
export interface UnifiedFieldMetrics {radius:number;gap:number;placementRadius:number;placementGap:number;person:number;personLabel:number;personLabelWidth:number;deviceWidth:number;deviceHeight:number;contextTitleWidth:number;contextTitleHeight:number;contourPadding:number;contextGap:number}
export interface FieldNode {shortcutId:string;entity:FieldDirectoryEntity;contextId:string;slot:[number,number];kind:FieldEntityRef['kind'];cx:number;cy:number;x:number;y:number;width:number;height:number;footprint:FieldPoint[];bounds:FieldBounds}
export interface FieldSceneContext extends FieldContext {children:string[];bounds:FieldBounds;body:FieldBounds;header:{x:number;y:number;width:number;height:number};contour:string}
export interface UnifiedFieldLayout {nodes:FieldNode[];contexts:FieldSceneContext[];bounds:FieldBounds|null}
export const UNIFIED_FIELD_METRICS:Readonly<UnifiedFieldMetrics>;
export function fieldSlotToPoint(slot:readonly[number,number],metrics?:UnifiedFieldMetrics):FieldPoint;
export function fieldPointToSlot(point:FieldPoint,metrics?:UnifiedFieldMetrics):[number,number];
export function fieldFootprint(kind:FieldEntityRef['kind'],point:FieldPoint,metrics?:UnifiedFieldMetrics):FieldPoint[];
export function rectPoints(bounds:FieldBounds):FieldPoint[];
export function fieldBounds(points:FieldPoint[],padding?:number):FieldBounds;
export function fieldBoundsOverlap(a:FieldBounds,b:FieldBounds,gap?:number):boolean;
export function fieldPolygonsOverlap(a:FieldPoint[],b:FieldPoint[],gap?:number):boolean;
export function layoutUnifiedField(scene:FieldScene,entities:readonly FieldDirectoryEntity[],metrics?:UnifiedFieldMetrics):UnifiedFieldLayout;
export interface FieldSpatialIndex {query(bounds:FieldBounds,overscan?:number):FieldNode[]}
export function createFieldSpatialIndex(nodes:FieldNode[],bucketSize?:number):FieldSpatialIndex;
export function nearestFieldNode(nodes:FieldNode[],currentId:string,direction:readonly[number,number]):FieldNode|null;
export interface FieldMoveTarget {contextId:string;slot:[number,number];swap?:boolean}
export interface FieldMovePreview {valid:boolean;reason:'missing'|'collision'|'swap'|'free';target:FieldMoveTarget;occupied?:string|undefined;layout?:UnifiedFieldLayout}
export function fieldMovePreview(document:FieldDocument,entities:readonly FieldDirectoryEntity[],shortcutId:string,target:FieldMoveTarget,metrics?:UnifiedFieldMetrics):FieldMovePreview;
