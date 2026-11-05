export const CANVAS_VAULT_READ_LIMITS:Readonly<{bytes:26214400;partBytes:524288;parts:50}>;
export interface CanvasVaultChunk {
 contentType:string;bytesBase64:string;index:number;chunks:number;byteLength:number;revision:number;
 /** Existing Root wire has no objectId. If present, it must match the request. */
 objectId?:string;ok?:true;
}
export interface CanvasVaultReadOptions {
 objectId:string;
 /** Existing current Object/Page ACL port; no copied owner or authority token. */
 readPart(request:Readonly<{objectId:string;part:'vault';index:number}>,options?:{signal:AbortSignal}):Promise<CanvasVaultChunk>;
 /** Must capture the original SDK session/epoch and account, not just account. */
 isCurrent():boolean;
 signal?:AbortSignal;
 expected?:{revision?:number;contentType?:string;byteLength?:number};
}
/** No partial-byte/progress callback. The whole verified result is transferred
 * only after every part and final current-session check succeeds. */
export function readCanvasVaultContent(options:CanvasVaultReadOptions):Promise<{contentType:string;bytes:Uint8Array<ArrayBuffer>;revision:number}>;
