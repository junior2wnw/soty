import type {MaterialApi,MaterialKind} from '../../modules/materials/browser/client.mjs';
import type {CanvasMask,CanvasPresentation} from '../../modules/canvas-objects/contract.mjs';
export type CanvasCreateKind='note'|MaterialKind|'widget';
export type CanvasCreateStage='connect'|'source'|'object'|'placement'|'settling'|'complete';
export interface CanvasCreatePlacement {shortcutId:string;status:'saved'|'pending';localDurable:boolean}
export interface CanvasCreateResult extends CanvasCreatePlacement {operationId:string;objectId:string}
export interface CanvasCreateRecord {schema:'soty.canvas-create.v1'|'soty.canvas-create.v2';operationId:string;accountId:string;projectId:string;kind:CanvasCreateKind;createdAt:number;version:number;stage:CanvasCreateStage;objectRequestId:string;sourceIntent:Record<string,any>|null;sourceRef:string|null;objectId:string|null;presentation:CanvasPresentation;width:number;height:number;placement:CanvasCreatePlacement|null;intentHash:string;connection?:{requestId:string;appId:string|null;settled:boolean}}
export interface CanvasCreatePending {operationId:string;kind:CanvasCreateKind;stage:CanvasCreateStage;createdAt:number;label:string;objectId:string|null;localDurable:boolean;connectionRequestId?:string}
export interface CanvasCreateProgress {operationId?:string|undefined;kind?:CanvasCreateKind|undefined;phase:CanvasCreateStage|'preparing'|'local-error';localDurable:boolean;uploaded?:number;total?:number}
export interface CanvasCreateSource {providerId:'soty.notes'|'soty.materials';ref:string}
export interface CanvasCreateSourceInspection {operationId:string;stage:string;holdsPlaintext:boolean;sourceConfirmed:boolean}
export interface CanvasCreateFence {epoch:string;operationId:string;state:'pending'|'sealed';target?:{objectId:string;expectedObjectRevision:number}}
export interface CanvasCreateStore {get(id:string):Promise<CanvasCreateRecord|null>;list():Promise<CanvasCreateRecord[]>;put(record:CanvasCreateRecord,expectedVersion:number):Promise<boolean>;remove(id:string):Promise<void>;close?():void;
 subscribeSourceRetire?(callback:(source:CanvasCreateSource)=>void):()=>void;
 snapshotSourceIntents?(source:CanvasCreateSource):Promise<CanvasCreateRecord[]>;
 sourceFence?(source:CanvasCreateSource):Promise<CanvasCreateFence|null>;
 sealSource?(source:CanvasCreateSource,operationId:string,target?:{objectId:string;expectedObjectRevision:number}):Promise<CanvasCreateFence>;
 cancelSourceFence?(source:CanvasCreateSource,epoch:string):Promise<boolean>;
 retireSourceIntents?(source:CanvasCreateSource,epoch:string,expected:Pick<CanvasCreateRecord,'operationId'|'version'|'intentHash'>[]):Promise<boolean>;
 /** Private metadata-only inspection: no labels, body, Blob or content hash. */
 inspectSourceIntents?(source:CanvasCreateSource):Promise<CanvasCreateSourceInspection[]>;
 /** Production store reads the shared durable privacy fence before replay. */
 nativeGate?(source:CanvasCreateSource):Promise<unknown|null>}
export interface CanvasCreateFlowOptions {api:MaterialApi;accountId:string;projectId:string;isCurrent():boolean;place(objectId:string,size:{width:number;height:number}):Promise<CanvasCreatePlacement>;onObjectReady?:(objectId:string)=>void|Promise<void>;onProgress?:(value:CanvasCreateProgress)=>void;store?:CanvasCreateStore}
export interface CanvasCreateFlow {startNote(input:{title?:string;body:string;mask?:CanvasMask;fontSize?:number}):Promise<CanvasCreateResult>;startFile(file:Blob,options?:{kind?:MaterialKind;mask?:CanvasMask}):Promise<CanvasCreateResult>;startWidget(input:{appId:string;entry?:{domainId:string;path:string};mask?:CanvasMask}):Promise<CanvasCreateResult>;prepareConnectedWidget(requestId:string,options?:{mask?:CanvasMask}):Promise<{operationId:string}>;completeConnectedWidget(input:{requestId:string;appId:string}):Promise<CanvasCreateResult>;settleConnectedWidget(requestId:string):Promise<void>;resume(id:string):Promise<CanvasCreateResult>;listPending():Promise<CanvasCreatePending[]>;removeLocal(id:string):Promise<void>;dispose():void}
export class CanvasCreateError extends Error {readonly code:string;operationId?:string;localDurable?:boolean;constructor(code:string,context?:Record<string,unknown>)}
export const CANVAS_CREATE_LIMITS:Readonly<{pending:8;completed:32;blobBytes:26214400;metadataBytes:4194304}>;
export function createCanvasCreateStore(accountId:string,projectId:string,options?:{origin?:string;indexedDB?:IDBFactory}):CanvasCreateStore;
export function validateCanvasCreateRecord(value:unknown,accountId:string,projectId:string):CanvasCreateRecord;
export function createCanvasCreateFlow(options:CanvasCreateFlowOptions):CanvasCreateFlow;
export function canvasCreateFileKind(file:Blob,kind?:MaterialKind):MaterialKind;
