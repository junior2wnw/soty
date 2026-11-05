export type MaterialKind='image'|'audio'|'video';
export interface MaterialMetadata {materialId:string;kind:MaterialKind;name:string;mediaType:string;size:number;fileSha256:string;manifestSha256:string;chunks:{bytes:number;sha256:string}[];width:number|null;height:number|null;durationMs:number|null;state:'active'|'trashed';revision:number;createdAt:number;updatedAt:number}
export interface MaterialApi {request<T=any>(method:string,params?:Record<string,unknown>):Promise<T>}
export interface MaterialOptions {accountId:string;isCurrent?:()=>boolean;signal?:AbortSignal}
export interface MaterialUploadIntent {blob:Blob;materialId:string;mutationId:string;commitMutationId:string;name:string;kind:MaterialKind;mediaType:string;size:number;fileSha256:string;manifestSha256:string;chunks:{bytes:number;sha256:string}[]}
export function materialUid():string;
export function materialSha256(value:string|BufferSource):Promise<string>;
export function validateMaterialMetadata(value:unknown):MaterialMetadata;
export function prepareMaterialUpload(blob:Blob,options:{kind:MaterialKind;name?:string;materialId?:string;mutationId?:string;commitMutationId?:string}):Promise<MaterialUploadIntent>;
export function uploadMaterial(api:MaterialApi,options:MaterialOptions&{onProgress?:(progress:{uploaded:number;total:number;phase:'upload'|'committed'})=>void},intent:MaterialUploadIntent):Promise<MaterialMetadata>;
export function readMaterialBlob(api:MaterialApi,options:MaterialOptions&{materialId:string}):Promise<{material:MaterialMetadata;blob:Blob}>;
