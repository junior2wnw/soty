import { materialCheck as check, materialId, materialMediaType, materialFileName, materialManifest,
  materialHash, MATERIAL_LIMITS, MaterialsError } from '../contract.mjs';
export const materialUid = () => globalThis.crypto.randomUUID();
export async function materialSha256(value) {
  const bytes=typeof value==='string'?new TextEncoder().encode(value):value;
  return Array.from(new Uint8Array(await globalThis.crypto.subtle.digest('SHA-256',bytes)),byte=>byte.toString(16).padStart(2,'0')).join('');
}
const base64=bytes=>{let text='';for(let at=0;at<bytes.length;at+=32768)text+=String.fromCharCode(...bytes.subarray(at,at+32768));return btoa(text);};
function decode(text,expected) {
  check(typeof text==='string'&&text.length<=Math.ceil(expected/3)*4,'materials_chunk_invalid');
  let bytes;try {bytes=Uint8Array.from(atob(text),char=>char.charCodeAt(0));}catch{throw new MaterialsError('materials_chunk_invalid');}
  check(bytes.length===expected&&base64(bytes)===text,'materials_chunk_invalid');return bytes;
}
function current(options) {if(options?.signal?.aborted)throw new DOMException('Cancelled','AbortError');check(!options?.isCurrent||options.isCurrent(),'materials_account_changed');}
export function validateMaterialMetadata(value) {
  check(value&&typeof value==='object','materials_metadata_invalid');materialId(value.materialId);materialHash(value.fileSha256);materialHash(value.manifestSha256);
  check(['active','trashed'].includes(value.state)&&Number.isSafeInteger(value.revision)&&value.revision>0,'materials_metadata_invalid');
  check(materialMediaType(value.kind,value.mediaType)===value.mediaType&&materialFileName(value.name)===value.name,'materials_metadata_invalid');
  materialManifest(value.size,value.chunks);
  check(Number.isSafeInteger(value.createdAt)&&value.createdAt>=0&&Number.isSafeInteger(value.updatedAt)&&value.updatedAt>=value.createdAt,'materials_metadata_invalid');
  check(value.kind==='image'?(Number.isSafeInteger(value.width)&&Number.isSafeInteger(value.height)&&value.width>0&&value.height>0
    &&value.width<=MATERIAL_LIMITS.imageSide&&value.height<=MATERIAL_LIMITS.imageSide&&value.width*value.height<=MATERIAL_LIMITS.imagePixels&&value.durationMs===null)
    :value.kind==='video'?(Number.isSafeInteger(value.width)&&Number.isSafeInteger(value.height)&&value.width>0&&value.height>0
      &&value.width<=MATERIAL_LIMITS.videoSide&&value.height<=MATERIAL_LIMITS.videoSide&&value.width*value.height<=MATERIAL_LIMITS.videoPixels
      &&(value.durationMs===null||(Number.isSafeInteger(value.durationMs)&&value.durationMs>0&&value.durationMs<=MATERIAL_LIMITS.videoSeconds*1000)))
    :(value.width===null&&value.height===null&&(value.durationMs===null||(Number.isSafeInteger(value.durationMs)&&value.durationMs>=0&&value.durationMs<=MATERIAL_LIMITS.audioSeconds*1000))),'materials_metadata_invalid');
  return value;
}
/** Keep this intent for retries. Uncertain acknowledgements must not generate another identity. */
export async function prepareMaterialUpload(blob,{kind,name=blob?.name??'Материал',materialId:id=materialUid(),mutationId=materialUid(),commitMutationId=materialUid()}={}) {
  check(blob instanceof Blob&&blob.size>0&&blob.size<=MATERIAL_LIMITS.fileBytes,'materials_file_size');
  materialId(id);materialId(mutationId);materialId(commitMutationId);
  const mediaType=materialMediaType(kind,blob.type),chunks=[];
  const bytes=new Uint8Array(await blob.arrayBuffer());
  for(let index=0;index<Math.ceil(bytes.length/MATERIAL_LIMITS.chunkBytes);index++) {
    const part=bytes.subarray(index*MATERIAL_LIMITS.chunkBytes,Math.min(bytes.length,(index+1)*MATERIAL_LIMITS.chunkBytes));chunks.push({bytes:part.length,sha256:await materialSha256(part)});
  }
  return {blob,materialId:id,mutationId,commitMutationId,name:materialFileName(name),kind,mediaType,size:blob.size,
    fileSha256:await materialSha256(bytes),manifestSha256:await materialSha256(materialManifest(blob.size,chunks)),chunks};
}
export async function uploadMaterial(api,options,intent) {
  const {accountId,onProgress=()=>{}}=options;materialId(accountId);current(options);
  const scope={expectedAccountId:accountId,materialId:intent.materialId};
  await api.request('materials.begin',{...scope,mutationId:intent.mutationId,name:intent.name,kind:intent.kind,mediaType:intent.mediaType,size:intent.size,fileSha256:intent.fileSha256});current(options);
  const status=await api.request('materials.status',scope);current(options);
  if(status.state==='active') {
    const material=validateMaterialMetadata(status.material);check(material.fileSha256===intent.fileSha256&&material.manifestSha256===intent.manifestSha256,'materials_identity_reused');return material;
  }
  check(status.state==='pending','materials_upload_closed');
  const received=new Map(status.received.map(part=>[part.index,part]));
  for(let index=0;index<intent.chunks.length;index++) {
    current(options);const declared=intent.chunks[index],existing=received.get(index);
    if(existing)check(existing.size===declared.bytes&&existing.sha256===declared.sha256,'materials_chunk_reused');
    else {
      const bytes=new Uint8Array(await intent.blob.slice(index*MATERIAL_LIMITS.chunkBytes,index*MATERIAL_LIMITS.chunkBytes+declared.bytes).arrayBuffer());current(options);
      check(await materialSha256(bytes)===declared.sha256,'materials_chunk_invalid');current(options);
      await api.request('materials.chunk',{...scope,index,sha256:declared.sha256,data:base64(bytes)});current(options);
    }
    onProgress({uploaded:Math.min(intent.size,(index+1)*MATERIAL_LIMITS.chunkBytes),total:intent.size,phase:'upload'});
  }
  const result=await api.request('materials.commit',{...scope,mutationId:intent.commitMutationId,manifestSha256:intent.manifestSha256});current(options);
  const material=validateMaterialMetadata(result.material);check(material.fileSha256===intent.fileSha256&&material.manifestSha256===intent.manifestSha256,'materials_file_hash');
  onProgress({uploaded:intent.size,total:intent.size,phase:'committed'});return material;
}
export async function readMaterialBlob(api,{accountId,materialId:id,...options}) {
  materialId(accountId);materialId(id);current(options);
  const {material}=await api.request('materials.get',{expectedAccountId:accountId,materialId:id});current(options);
  validateMaterialMetadata(material);check(material.state==='active','materials_material_not_found');
  check(await materialSha256(materialManifest(material.size,material.chunks))===material.manifestSha256,'materials_manifest_hash');current(options);
  const parts=[];
  for(let index=0;index<material.chunks.length;index++) {
    const declared=material.chunks[index],part=await api.request('materials.read',{expectedAccountId:accountId,materialId:id,index});current(options);
    check(part.index===index&&part.size===declared.bytes&&part.sha256===declared.sha256,'materials_chunk_invalid');
    const bytes=decode(part.data,declared.bytes);check(await materialSha256(bytes)===declared.sha256,'materials_chunk_invalid');current(options);parts.push(bytes);
  }
  const blob=new Blob(parts,{type:material.mediaType});check(blob.size===material.size&&await materialSha256(await blob.arrayBuffer())===material.fileSha256,'materials_file_hash');current(options);
  // The original bytes are never put in the field document or a public download URL.
  return {material,blob};
}
