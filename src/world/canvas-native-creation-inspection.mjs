// Read-only Source-intent inspection. Content/Blob/intentHash never leave this
// private reader; a pending known Source is not permission to drop its journal.
export function nativeCreationSource(record){
  if(record?.kind==='note')return{providerId:'soty.notes',ref:record.sourceIntent?.noteId??record.sourceRef};
  if(['image','audio','video'].includes(record?.kind))return{providerId:'soty.materials',ref:record.sourceIntent?.materialId??record.sourceRef};
  return null;
}
export function inspectNativeCreationRecords(records,source){
  if(!Array.isArray(records)||records.length>64)throw new Error('native_creation_inspection_unavailable');
  return records.filter(record=>{const target=nativeCreationSource(record);return target?.providerId===source.providerId&&target.ref===source.ref;}).map(record=>({
    operationId:record.operationId,stage:record.stage,holdsPlaintext:record.stage==='source'&&record.sourceIntent!==null,sourceConfirmed:record.stage!=='source',
  }));
}
