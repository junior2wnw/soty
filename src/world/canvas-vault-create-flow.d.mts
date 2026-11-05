import type {CanvasCreatePlacement,CanvasCreateResult} from './canvas-create-flow.mjs';
import type {CanvasMask} from '../../modules/canvas-objects/contract.mjs';
import type {CanvasVaultApi} from './canvas-vault-client.mjs';
import type {CanvasVaultLocalStore} from './canvas-vault-local.mjs';
import type {MaterialKind} from '../../modules/materials/browser/client.mjs';
export interface CanvasVaultCreateOptions{api:CanvasVaultApi;accountId:string;projectId?:string;isCurrent():boolean;place(objectId:string,size:{width:number;height:number}):Promise<CanvasCreatePlacement>;onObjectReady?:(objectId:string)=>void|Promise<void>;onProgress?:(value:{operationId:string;phase:'source'|'object'|'placement'|'complete'|'local-error';localDurable:boolean})=>void;store?:CanvasVaultLocalStore;crypto?:Crypto}
export interface CanvasVaultCreateFlow{startNote(input:{title?:string;body:string;mask?:CanvasMask;fontSize?:number},password:string):Promise<CanvasCreateResult>;startFile(file:Blob,password:string,input?:{kind?:MaterialKind;mask?:CanvasMask}):Promise<CanvasCreateResult>;resume(operationId:string,password:string):Promise<CanvasCreateResult>;listPending():Promise<{operationId:string;label:string;localDurable:boolean}[]>;removeLocal(operationId:string):Promise<void>;dispose():void}
export function createCanvasVaultCreateFlow(options:CanvasVaultCreateOptions):CanvasVaultCreateFlow;
