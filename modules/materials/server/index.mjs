import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import { mkdirSync, realpathSync } from 'node:fs';
import { basename, dirname, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { materialCheck as check, materialKeys as keys, materialId as id, materialHash, materialMediaType,
  materialFileName, materialManifest, MATERIAL_LIMITS, MATERIAL_OPERATIONS, MaterialsError } from '../contract.mjs';
import { openMaterialSchema } from './schema.mjs';
import { inspectMaterialMedia } from './media.mjs';
import {createMaterialsVaultPort} from './vault-port.mjs';
export { MATERIAL_LIMITS, MATERIAL_OPERATIONS, MaterialsError } from '../contract.mjs';
const hash = value => createHash('sha256').update(value).digest('hex');
const moduleRoot = dirname(dirname(fileURLToPath(import.meta.url)));
function physical(file) { try { return realpathSync(file); } catch(error) { if(error.code!=='ENOENT')throw error;return resolve(physical(dirname(file)),basename(file)); } }
function outsideModule(file) {const fold=value=>process.platform==='win32'?value.toLowerCase():value;const root=fold(physical(moduleRoot));return ![file,physical(file)].some(value=>fold(value)===root||fold(value).startsWith(root+sep));}
const metadata = row => ({materialId:row.id,kind:row.kind,name:row.name,mediaType:row.media_type,size:row.size,
  fileSha256:row.file_sha256,manifestSha256:row.manifest_sha256,chunks:row.manifest?JSON.parse(row.manifest).chunks:[],
  width:row.width,height:row.height,durationMs:row.duration_ms,state:row.state,revision:row.revision,createdAt:row.created_at,updatedAt:row.updated_at});

/** Trusted signed Connect extension. Never construct actor or guard from request JSON. */
export function createMaterialsService({databasePath,projectId,clock=Date.now,limits:overrides={},verifyObjectAuthority,allowVaultMigration=false,verifyVaultCipherReceipt,readVaultSource,onVaultChange}={}) {
  check(typeof databasePath==='string'&&databasePath.length>0,'materials_database_path_required');
  check(typeof projectId==='string'&&/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u.test(projectId),'materials_project_id_required');
  check(typeof clock==='function');keys(overrides,Object.keys(MATERIAL_LIMITS).filter(key=>key!=='chunkBytes'));
  check(verifyObjectAuthority===undefined||typeof verifyObjectAuthority==='function'&&verifyObjectAuthority.constructor?.name!=='AsyncFunction','materials_object_verifier_invalid');
  check([verifyVaultCipherReceipt,readVaultSource,onVaultChange].every(value=>value===undefined||typeof value==='function'),'materials_vault_configuration_invalid');
  const limits={...MATERIAL_LIMITS,...overrides};
  for(const [key,value] of Object.entries(limits))check(Number.isSafeInteger(value)&&value>0&&value<=MATERIAL_LIMITS[key]);
  const file=databasePath===':memory:'?databasePath:resolve(databasePath);
  if(file!==':memory:'){check(outsideModule(file),'materials_database_must_be_outside_module');mkdirSync(dirname(file),{recursive:true,mode:0o700});}
  const db=new DatabaseSync(file);let storage;
  try {storage=openMaterialSchema(db,projectId,{allowVaultMigration});}catch(error){db.close();throw error;}
  let closing=false,closed=false,queue=Promise.resolve();const statements=new Map();
  const prepare=sql=>{if(!statements.has(sql))statements.set(sql,db.prepare(sql));return statements.get(sql);};
  const get=(sql,...args)=>prepare(sql).get(...args), all=(sql,...args)=>prepare(sql).all(...args), run=(sql,...args)=>prepare(sql).run(...args);
  const rowFor=(owner,materialId)=>get('SELECT * FROM materials WHERE owner_id=? AND id=?',owner,materialId);
  const ensure=()=>{check(!closed,'materials_service_closed');check(Number(get('PRAGMA user_version').user_version)===storage.schemaVersion,'materials_store_format_changed');};
  function validateGuard(guard) {if(guard===undefined)return;check(typeof guard==='function'&&guard.constructor?.name!=='AsyncFunction','materials_guard_invalid');const result=guard();check(!result||typeof result.then!=='function','materials_guard_invalid');check(result!==false,'authentication_required');}
  function transaction(action,guard) {
    ensure();check(!db.isTransaction,'materials_nested_transaction');validateGuard(guard);db.exec('BEGIN IMMEDIATE');
    try {ensure();const result=action();check(!result||typeof result.then!=='function','materials_async_transaction_forbidden');validateGuard(guard);db.exec('COMMIT');return result;}
    catch(error){if(db.isTransaction)db.exec('ROLLBACK');throw error;}
  }
  function receipt(owner,materialId,mutationId,digest) {
    const row=get('SELECT digest,result FROM material_receipts WHERE owner_id=? AND material_id=? AND mutation_id=?',owner,materialId,mutationId);
    if(!row)return null;check(row.digest===digest,'materials_mutation_reused');return {...JSON.parse(row.result),replayed:true};
  }
  function record(owner,materialId,mutationId,digest,result) {
    run('INSERT INTO material_receipts(owner_id,material_id,mutation_id,digest,result) VALUES(?,?,?,?,?)',owner,materialId,mutationId,digest,JSON.stringify(result));
    run(`DELETE FROM material_receipts WHERE owner_id=? AND material_id=? AND sequence IN
      (SELECT sequence FROM material_receipts WHERE owner_id=? AND material_id=? ORDER BY sequence DESC LIMIT -1 OFFSET ?)`,owner,materialId,owner,materialId,limits.receipts);
    return result;
  }
  function expire(now) {
    run("DELETE FROM material_chunks WHERE EXISTS(SELECT 1 FROM materials m WHERE m.owner_id=material_chunks.owner_id AND m.id=material_chunks.material_id AND m.state='pending' AND m.created_at<?)",now-limits.pendingMs);
    run("UPDATE materials SET state='expired',updated_at=? WHERE state='pending' AND created_at<?",now,now-limits.pendingMs);
  }
  function diskQuota(extra=0) {
    const pages=get('PRAGMA page_count').page_count, pageSize=get('PRAGMA page_size').page_size;
    check(pages*pageSize+extra<=limits.databaseBytes,'materials_database_quota');
  }
  const usage=owner=>{const row=get("SELECT count(*) AS identities,coalesce(sum(CASE WHEN state IN('pending','active','trashed') THEN size ELSE 0 END),0) AS bytes,sum(state='pending') AS pending,sum(state='active') AS active,sum(state='trashed') AS trashed FROM materials WHERE owner_id=?",owner);return {bytes:row.bytes,maxBytes:limits.accountBytes,maxRecords:limits.accountRecords,counts:{active:row.active??0,trashed:row.trashed??0,pending:row.pending??0}};};
  function begin(args,owner,now) {
    keys(args,['expectedAccountId','materialId','mutationId','name','kind','mediaType','size','fileSha256']);
    const materialId=id(args.materialId),mutationId=id(args.mutationId),name=materialFileName(args.name),kind=args.kind,
      mediaType=materialMediaType(kind,args.mediaType),size=args.size,fileSha256=materialHash(args.fileSha256);
    check(!vault.seal(owner,materialId),'materials_material_sealed');
    check(Number.isSafeInteger(size)&&size>0&&size<=limits.fileBytes,'materials_file_size');
    const digest=hash(JSON.stringify(['begin',name,kind,mediaType,size,fileSha256]));
    const replay=receipt(owner,materialId,mutationId,digest);if(replay)return replay;
    const previous=rowFor(owner,materialId);
    if(previous) {check(previous.begin_digest===digest,'materials_identity_reused');check(previous.state==='pending'||previous.state==='active','materials_upload_closed');return {materialId,state:previous.state,revision:previous.revision,replayed:true};}
    const account=usage(owner),count=get('SELECT count(*) AS count FROM materials').count,
      reserved=get("SELECT coalesce(sum(size),0) AS bytes FROM materials WHERE state IN('pending','active','trashed')").bytes;
    check(get('SELECT count(*) AS count FROM materials WHERE owner_id=?',owner).count<limits.accountRecords&&count<limits.records,'materials_count_quota');
    check(account.bytes+size<=limits.accountBytes&&reserved+size<=limits.storeBytes,'materials_storage_quota');
    check(account.counts.pending<limits.pending,'materials_pending_quota');diskQuota();
    run("INSERT INTO materials(owner_id,id,kind,name,media_type,size,file_sha256,begin_digest,state,revision,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,'pending',0,?,?)",owner,materialId,kind,name,mediaType,size,fileSha256,digest,now,now);
    return record(owner,materialId,mutationId,digest,{materialId,state:'pending',revision:0});
  }
  function chunk(args,owner,now) {
    keys(args,['expectedAccountId','materialId','index','sha256','data']);const materialId=id(args.materialId),index=args.index,sha256=materialHash(args.sha256);
    check(!vault.seal(owner,materialId),'materials_material_sealed');
    const row=rowFor(owner,materialId);check(row&&row.state==='pending','materials_upload_not_found');
    check(Number.isSafeInteger(index)&&index>=0&&index<Math.ceil(row.size/limits.chunkBytes),'materials_chunk_index');
    const expected=Math.min(limits.chunkBytes,row.size-index*limits.chunkBytes);
    check(typeof args.data==='string'&&args.data.length<=Math.ceil(expected/3)*4,'materials_chunk_invalid');
    const bytes=Buffer.from(args.data,'base64');
    check(bytes.length===expected&&bytes.toString('base64')===args.data&&hash(bytes)===sha256,'materials_chunk_invalid');
    const previous=get('SELECT size,sha256,bytes FROM material_chunks WHERE owner_id=? AND material_id=? AND part_index=?',owner,materialId,index);
    if(previous){check(previous.size===expected&&previous.sha256===sha256&&Buffer.from(previous.bytes).equals(bytes),'materials_chunk_reused');return {materialId,index,size:expected,sha256,replayed:true};}
    diskQuota(expected+4096);
    run('INSERT INTO material_chunks(owner_id,material_id,part_index,size,sha256,bytes) VALUES(?,?,?,?,?,?)',owner,materialId,index,expected,sha256,bytes);
    run('UPDATE materials SET updated_at=? WHERE owner_id=? AND id=?',Math.max(now,row.updated_at+1),owner,materialId);
    return {materialId,index,size:expected,sha256};
  }
  function pieces(owner,row) {
    const chunks=all('SELECT part_index,size,sha256,bytes FROM material_chunks WHERE owner_id=? AND material_id=? ORDER BY part_index',owner,row.id);
    check(chunks.length===Math.ceil(row.size/limits.chunkBytes),'materials_upload_incomplete');
    for(let index=0;index<chunks.length;index++){const part=chunks[index];check(part.part_index===index&&part.bytes.length===part.size&&hash(part.bytes)===part.sha256,'materials_storage_corrupt');}
    const manifest=materialManifest(row.size,chunks.map(part=>({bytes:part.size,sha256:part.sha256})));
    const bytes=Buffer.concat(chunks.map(part=>Buffer.from(part.bytes)),row.size);
    check(hash(bytes)===row.file_sha256,'materials_file_hash');return {manifest,manifestSha256:hash(manifest),bytes};
  }
  async function commit(args,owner,now,guard) {
    keys(args,['expectedAccountId','materialId','mutationId','manifestSha256']);const materialId=id(args.materialId),mutationId=id(args.mutationId),manifestSha256=materialHash(args.manifestSha256),digest=hash(JSON.stringify(['commit',manifestSha256]));
    check(!vault.seal(owner,materialId),'materials_material_sealed');
    const replay=receipt(owner,materialId,mutationId,digest);if(replay)return replay;
    const row=rowFor(owner,materialId);check(row&&row.state==='pending','materials_upload_not_found');
    const content=pieces(owner,row);check(content.manifestSha256===manifestSha256,'materials_manifest_hash');
    const dimensions=await inspectMaterialMedia(content.bytes,row.kind,row.media_type,limits);
    ensure();validateGuard(guard);
    return transaction(()=>{
      check(!vault.seal(owner,materialId),'materials_material_sealed');
      const concurrentReplay=receipt(owner,materialId,mutationId,digest);if(concurrentReplay)return concurrentReplay;
      const fresh=rowFor(owner,materialId);check(fresh&&fresh.state==='pending'&&fresh.begin_digest===row.begin_digest,'materials_upload_changed');
      // A second service may have touched the same SQLite store during decoding.
      const verified=pieces(owner,fresh);check(verified.manifestSha256===manifestSha256,'materials_upload_changed');
      check(clock()-fresh.created_at<=limits.pendingMs,'materials_upload_expired');
      const updatedAt=Math.max(now,fresh.updated_at+1);
      run("UPDATE materials SET state='active',revision=1,manifest=?,manifest_sha256=?,width=?,height=?,duration_ms=?,updated_at=? WHERE owner_id=? AND id=?",content.manifest,manifestSha256,dimensions.width,dimensions.height,dimensions.durationMs,updatedAt,owner,materialId);
      return record(owner,materialId,mutationId,digest,{material:metadata(rowFor(owner,materialId))});
    },guard);
  }
  function stateChange(op,args,owner,now) {
    keys(args,['expectedAccountId','materialId','mutationId','expectedRevision']);const materialId=id(args.materialId),mutationId=id(args.mutationId),expected=args.expectedRevision;
    check(Number.isSafeInteger(expected)&&expected>=0);check(!vault.seal(owner,materialId),'materials_material_sealed');const digest=hash(JSON.stringify([op,expected]));const replay=receipt(owner,materialId,mutationId,digest);if(replay)return replay;
    const row=rowFor(owner,materialId);check(row,'materials_material_not_found');
    const from=op==='materials.trash'?'active':op==='materials.restore'?'trashed':'pending',to=op==='materials.trash'?'trashed':op==='materials.restore'?'active':'cancelled';
    check(row.revision===expected,'materials_revision_conflict');check(row.state===from,'materials_state_conflict');
    const updatedAt=Math.max(now,row.updated_at+1);
    if(to==='cancelled')run('DELETE FROM material_chunks WHERE owner_id=? AND material_id=?',owner,materialId);
    run('UPDATE materials SET state=?,revision=revision+1,updated_at=? WHERE owner_id=? AND id=?',to,updatedAt,owner,materialId);
    return record(owner,materialId,mutationId,digest,{materialId,state:to,revision:row.revision+1,updatedAt});
  }
  function read(op,args,owner) {
    if(op==='materials.list') {
      keys(args,['expectedAccountId','bucket','cursor','limit']);const bucket=args.bucket??'active',limit=args.limit??30;
      check(['active','trashed'].includes(bucket)&&Number.isSafeInteger(limit)&&limit>0&&limit<=40);
      const scope=hash(JSON.stringify([owner,bucket]));let cursor;
      if(args.cursor!==undefined&&args.cursor!==null){
        check(typeof args.cursor==='string'&&args.cursor.length<=512,'materials_cursor_invalid');
        try {cursor=JSON.parse(Buffer.from(args.cursor,'base64url').toString());}catch{throw new MaterialsError('materials_cursor_invalid');}
        check(cursor&&cursor.scope===scope&&Number.isSafeInteger(cursor.time)&&cursor.time>=0,'materials_cursor_invalid');id(cursor.id);
      }
      const rows=cursor?all('SELECT * FROM materials WHERE owner_id=? AND state=? AND (updated_at<? OR (updated_at=? AND id>?)) ORDER BY updated_at DESC,id ASC LIMIT ?',owner,bucket,cursor.time,cursor.time,cursor.id,limit+1)
        :all('SELECT * FROM materials WHERE owner_id=? AND state=? ORDER BY updated_at DESC,id ASC LIMIT ?',owner,bucket,limit+1);
      const page=rows.slice(0,limit),last=page.at(-1);return {materials:page.map(row=>vault.seal(owner,row.id)?{...metadata(row),name:'Защищённый файл',sealed:true,protection:{kind:'password'}}:metadata(row)),nextCursor:rows.length>limit?Buffer.from(JSON.stringify({scope,time:last.updated_at,id:last.id})).toString('base64url'):null,usage:usage(owner)};
    }
    keys(args,op==='materials.read'?['expectedAccountId','materialId','index']:['expectedAccountId','materialId']);const materialId=id(args.materialId),row=rowFor(owner,materialId);
    if(vault.seal(owner,materialId)){if(op==='materials.status')return {materialId,state:'sealed',revision:row.revision,sealed:true,protection:{kind:'password'}};check(false,'materials_material_sealed');}
    if(op==='materials.status') {
      check(row,'materials_material_not_found');return {materialId,state:row.state,revision:row.revision,
        received:all('SELECT part_index,size,sha256 FROM material_chunks WHERE owner_id=? AND material_id=? ORDER BY part_index',owner,materialId).map(part=>({index:part.part_index,size:part.size,sha256:part.sha256})),
        material:['active','trashed'].includes(row.state)?metadata(row):null};
    }
    check(row&&['active','trashed'].includes(row.state),'materials_material_not_found');
    if(op==='materials.get')return {material:metadata(row)};
    check(row.state==='active','materials_material_not_found');const index=args.index;check(Number.isSafeInteger(index)&&index>=0&&index<Math.ceil(row.size/limits.chunkBytes),'materials_chunk_index');
    const part=get('SELECT size,sha256,bytes FROM material_chunks WHERE owner_id=? AND material_id=? AND part_index=?',owner,materialId,index);
    const declared=JSON.parse(row.manifest).chunks[index];
    check(part&&part.size===declared.bytes&&part.sha256===declared.sha256&&part.bytes.length===part.size&&hash(part.bytes)===part.sha256,'materials_storage_corrupt');
    return {index,size:part.size,sha256:part.sha256,data:Buffer.from(part.bytes).toString('base64')};
  }
  async function dispatch({op,args={},actor,guard}={}) {
    ensure();check(MATERIAL_OPERATIONS.includes(op),'unsupported_operation');
    check(actor&&typeof actor.accountId==='string'&&typeof actor.deviceId==='string','authentication_required');
    const owner=id(actor.accountId);id(actor.deviceId);keys(args,Object.keys(args));check(args.expectedAccountId===owner,'materials_account_changed');validateGuard(guard);
    const now=clock();check(Number.isSafeInteger(now)&&now>=0,'materials_clock_invalid');
    if(op==='materials.commit')return commit(args,owner,now,guard);
    if(op==='materials.begin'||op==='materials.chunk'||['materials.trash','materials.restore','materials.cancel'].includes(op))return transaction(()=>{expire(now);return op==='materials.begin'?begin(args,owner,now):op==='materials.chunk'?chunk(args,owner,now):stateChange(op,args,owner,now);},guard);
    return transaction(()=>read(op,args,owner),guard);
  }
  function sync(value) {if(value&&typeof value.then==='function'){Promise.resolve(value).catch(()=>{});throw new MaterialsError('materials_async_authority');}return value;}
  function sameSource(before,after) {check(after&&after.owner_id===before.owner_id&&after.id===before.id&&after.state==='active'
    &&after.revision===before.revision&&after.file_sha256===before.file_sha256&&after.manifest_sha256===before.manifest_sha256,'materials_source_changed');}
  function objectScope(context) {
    check(!closing&&!closed&&typeof verifyObjectAuthority==='function','materials_object_authority_required');
    let active=true,entered=false,result;
    try {
      sync(verifyObjectAuthority(context,scope=>{
        check(active&&!entered,'materials_object_authority_required');entered=true;
        keys(scope,['providerId','ownerId','ref','right']);check(scope.providerId==='soty.materials'&&scope.right==='read','materials_object_authority_required');
        result=Object.freeze({ownerId:id(scope.ownerId),materialId:id(scope.ref)});return result;
      }));
      check(entered,'materials_object_authority_required');return result;
    }finally{active=false;}
  }
  function nativeRead(args,part) {
    keys(args,part?['context','index']:['context']);
    const expected=objectScope(args.context),guard=()=>{const next=objectScope(args.context);check(next.ownerId===expected.ownerId&&next.materialId===expected.materialId,'materials_object_authority_required');};
    return transaction(()=>{
      const row=rowFor(expected.ownerId,expected.materialId);check(row&&row.state==='active','materials_material_not_found');
      if(vault.seal(expected.ownerId,expected.materialId)){check(typeof readVaultSource==='function','materials_vault_unavailable');const value=sync(readVaultSource({context:args.context,part:part?'chunk':'materialMetadata',...(part?{index:args.index}:{})}));guard();sameSource(row,rowFor(expected.ownerId,expected.materialId));return value;}
      const result=read(part?'materials.read':'materials.get',{expectedAccountId:expected.ownerId,materialId:expected.materialId,...(part?{index:args.index}:{})},expected.ownerId);
      guard();sameSource(row,rowFor(expected.ownerId,expected.materialId));return result;
    },guard);
  }
  const native=Object.freeze({
    /** Host-only source fence. ownerId on read MUST originate in the ObjectService's validated object, never a public body. */
    withSourceAuthority(args,callback) {
      check(!closing&&!closed,'materials_service_closed');ensure();keys(args,['actor','materialId','ownerId','mode','purpose']);check(args.purpose===undefined||args.purpose==='protection','materials_object_authority_required');
      check(typeof callback==='function'&&callback.constructor?.name!=='AsyncFunction','materials_async_authority');check(args.mode==='create'||args.mode==='read');
      const materialId=id(args.materialId);let ownerId;
      if(args.mode==='create'){
        check(args.actor&&typeof args.actor.accountId==='string'&&typeof args.actor.deviceId==='string','authentication_required');ownerId=id(args.actor.accountId);id(args.actor.deviceId);
        check(args.ownerId===undefined||args.ownerId===ownerId,'materials_owner_required');
      }else {ownerId=id(args.ownerId);if(args.actor){id(args.actor.accountId);id(args.actor.deviceId);}}
      const before=rowFor(ownerId,materialId),wasSealed=vault.seal(ownerId,materialId);check(before&&before.state==='active','materials_material_not_found');
      // No Materials SQL transaction spans this callback. ObjectService may mint its own live read context and call nativeRead.
      const result=sync(callback(Object.freeze({allowed:true,sourceRevision:before.revision,sourceStoreId:storage.registryId,protection:Object.freeze(wasSealed?{kind:'password',sealed:true}:{kind:'none'})})));
      check(!closing&&!closed,'materials_service_closed');const after=rowFor(ownerId,materialId),nowSealed=vault.seal(ownerId,materialId);if(!(args.purpose==='protection'&&!wasSealed&&nowSealed?.source_revision===before.revision&&nowSealed?.sealed_revision===after?.revision))sameSource(before,after);return result;
    },
    readMetadata(args){return nativeRead(args,false);},
    readChunk(args){return nativeRead(args,true);},
  });
  const vault=createMaterialsVaultPort({db,storage,get,run,rowFor,transaction,ensure,metadata,verifyCipherReceipt:verifyVaultCipherReceipt,onChange:onVaultChange,clock,chunkBytes:limits.chunkBytes});
  return Object.freeze({operations:new Set(MATERIAL_OPERATIONS),challengeStorage:'memory',
    execute(input) {
      check(!closing&&!closed,'materials_service_closed');
      // Hold a request snapshot across the asynchronous decode/queue boundary.
      const request={...input,args:structuredClone(input?.args??{}),actor:input?.actor?{...input.actor}:undefined};
      const result=queue.then(()=>dispatch(request));queue=result.catch(()=>{});return result;
    },
    storageIdentity(){ensure();return {...storage};},
    native,vault,
    async close(){if(closed)return;closing=true;await queue;if(!closed){statements.clear();db.close();closed=true;}},
  });
}
