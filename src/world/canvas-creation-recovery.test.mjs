import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash,randomUUID} from 'node:crypto';
import {indexedNativePort} from '../../modules/notes/test/indexed-native-port.mjs';
import {createCanvasCreateStore} from './canvas-create-flow.mjs';
import {prepareMaterialUpload} from '../../modules/materials/browser/client.mjs';
import {canvasVaultProfile,deriveCanvasVaultKey,createCanvasVaultLocalStore,sealCanvasVaultRecord,openCanvasVaultRecord} from './canvas-vault-local.mjs';
import {archiveCreationIntents,verifyCreationRecoveries,readCreationRecoveries,CANVAS_CREATION_RECOVERY_LIMITS} from './canvas-creation-recovery.mjs';

const accountId='creation_archive_owner',projectId='soty',objectId='creation_archive_object',password='Synthetic creation archive password';
const source={providerId:'soty.notes',ref:'original_note_source_123',ownerId:accountId};
const secret='CREATION_ARCHIVE_PRIVATE_SENTINEL_20261104';
const hash=value=>createHash('sha256').update(value).digest('hex');
const tick=()=>new Promise(resolve=>setTimeout(resolve,5));
async function until(predicate){const end=Date.now()+10000;while(!predicate()){assert.ok(Date.now()<end,'Expected actual owned IDB commit boundary');await tick();}}
async function fixture(){
  const port=indexedNativePort(),origin='https://creation-archive.example',store=createCanvasVaultLocalStore(accountId,projectId,{origin,indexedDB:port.indexedDB});
  const profile=await store.ensureProfile(canvasVaultProfile(accountId,projectId,objectId)),session=await deriveCanvasVaultKey(profile,password);
  let current=true;
  const options={session,store,accountId,projectId,objectId,source,isCurrent:()=>current};
  return{port,origin,store,profile,session,options,setCurrent:value=>{current=value;},close(){while(port.held)port.release();store.close();}};
}
function noteRecord(stage='source'){
  const intent={noteId:source.ref,mutationId:randomUUID(),expectedRevision:0,title:'Private original title '+secret,body:secret,
    items:[{id:'original_check_item_123',text:'Original private checklist',done:false}],color:'plain',pinned:false,state:'active'};
  return{schema:'soty.canvas-create.v1',operationId:randomUUID(),accountId,projectId,kind:'note',createdAt:7,
    version:['source','object','placement','complete'].indexOf(stage)+1,stage,objectRequestId:randomUUID(),
    sourceIntent:stage==='source'?intent:null,sourceRef:stage==='source'?null:source.ref,
    objectId:['placement','complete'].includes(stage)?'original_other_placement_object':null,
    presentation:{mask:{shape:'rect'},fontSize:16},width:340,height:300,
    placement:stage==='complete'?{shortcutId:'original_shortcut_123',status:'pending',localDurable:true}:null,
    intentHash:hash(JSON.stringify(intent))};
}
async function rotateAll(f){
  const before=await f.store.list(objectId),profile=canvasVaultProfile(accountId,projectId,objectId),session=await deriveCanvasVaultKey(profile,password+' next'),next=[];
  for(const record of before){const {cipher,...header}=record;next.push(await sealCanvasVaultRecord(session,{...header,version:record.version+1},await openCanvasVaultRecord(f.session,record)));}
  assert.equal(await f.store.replaceProfile(f.profile.profileId,profile,before,next),true);return{session,profile};
}
const summary=rows=>rows.map(row=>({recordId:row.recordId,version:row.version,purpose:row.purpose}));

test('all original Creation stages/mutations archive with actual AEAD, reopen, verify, and no plaintext hashes in headers',async()=>{
  const f=await fixture();try{
    const records=['source','object','placement','complete'].map(noteRecord),receipt=await archiveCreationIntents({...f.options,records});
    assert.equal(receipt.creations.length,4);assert.ok(receipt.cipherRecords.length>=5);
    const stored=JSON.stringify(await f.store.list());
    for(const text of [secret,records[0].sourceIntent.title,records[0].intentHash,password])assert.equal(stored.includes(text),false);
    for(const text of [secret,records[0].intentHash])assert.equal(JSON.stringify(receipt).includes(text),false);
    const before=summary(await f.store.list());
    f.store.close();const reopened=createCanvasVaultLocalStore(accountId,projectId,{origin:f.origin,indexedDB:f.port.indexedDB});
    try{
      const options={...f.options,store:reopened},result=await readCreationRecoveries(options);
      assert.equal(result.length,1);assert.deepEqual(result[0].records,records.slice().sort((a,b)=>a.operationId.localeCompare(b.operationId)));
      assert.deepEqual(await verifyCreationRecoveries({...options,records,receipt}),receipt);
      assert.deepEqual(await archiveCreationIntents({...options,records}),receipt);assert.deepEqual(summary(await reopened.list()),before);
    }finally{reopened.close();}
  }finally{f.close();}
});

test('archiving does not retire plaintext; authenticated receipt is compatible with actual Creation fence/exact CAS',async()=>{
  const f=await fixture(),creation=createCanvasCreateStore(accountId,projectId,{origin:f.origin,indexedDB:f.port.indexedDB});
  try{
    const record=noteRecord();assert.equal(await creation.put(record,0),true);
    const fence=await creation.sealSource(source,'native_operation_123',{objectId,expectedObjectRevision:1}),records=await creation.snapshotSourceIntents(source);
    const receipt=await archiveCreationIntents({...f.options,records});assert.equal((await creation.get(record.operationId)).sourceIntent.body,secret);
    await verifyCreationRecoveries({...f.options,records,receipt});
    assert.equal(await creation.retireSourceIntents(source,fence.epoch,records.map(({operationId,version,intentHash})=>({operationId,version,intentHash}))),true);
    assert.equal(await creation.get(record.operationId),null);assert.equal((await creation.sourceFence(source)).state,'sealed');
    assert.deepEqual((await readCreationRecoveries(f.options))[0].records,records);
  }finally{creation.close();f.close();}
});

test('exact25MiB File survives encrypted512KiB parts, reopen and full-byte hash/name/mtime restoration', {timeout:120000},async t=>{
  const f=await fixture();try{
    const bytes=new Uint8Array(25*1024*1024);for(let index=0;index<bytes.length;index++)bytes[index]=(index*17+31)%251;
    const file=new File([bytes],'Private exact25MiB original.webm',{type:'video/webm',lastModified:1793700000123}),expectedHash=hash(bytes);bytes.fill(0);
    const mediaSource={providerId:'soty.materials',ref:'original_video_source_123',ownerId:accountId};
    const intent=await prepareMaterialUpload(file,{kind:'video',name:file.name,materialId:mediaSource.ref});
    const record={...noteRecord(),kind:'video',sourceIntent:intent,width:420,height:280,intentHash:hash(JSON.stringify({...intent,blob:undefined}))};
    const options={...f.options,source:mediaSource,isCurrent:()=>!t.signal.aborted},receipt=await archiveCreationIntents({...options,records:[record]});
    assert.equal(receipt.cipherRecords.length,52,'50 Blob parts, one metadata part, one manifest');
    const wire=JSON.stringify(await f.store.list());assert.equal(wire.includes(file.name),false);assert.equal(wire.includes(expectedHash),false);
    f.store.close();const reopened=createCanvasVaultLocalStore(accountId,projectId,{origin:f.origin,indexedDB:f.port.indexedDB});
    try{const recovered=(await readCreationRecoveries({...options,store:reopened}))[0].records[0],restored=recovered.sourceIntent.blob;
      assert.equal(restored.size,25*1024*1024);assert.equal(restored.type,file.type);assert.equal(restored.name,file.name);assert.equal(restored.lastModified,file.lastModified);
      assert.equal(hash(new Uint8Array(await restored.arrayBuffer())),expectedHash);
      const {blob:originalBlob,...originalIntent}=record.sourceIntent,{blob:recoveredBlob,...recoveredIntent}=recovered.sourceIntent;
      assert.deepEqual(recoveredIntent,originalIntent);assert.deepEqual({...recovered,sourceIntent:null},{...record,sourceIntent:null});
      assert.deepEqual(await verifyCreationRecoveries({...options,store:reopened,records:[record],receipt}),receipt);
    }finally{reopened.close();}
  }finally{f.close();}
});

test('bounded multi-part metadata and source key-order changes do not alter stable archive identities',async()=>{
  const f=await fixture();try{
    const record=noteRecord();record.sourceIntent.legacyRecoveryData='m'.repeat(1100000);
    const receipt=await archiveCreationIntents({...f.options,records:[record]}),options={...f.options,source:{ownerId:accountId,ref:source.ref,providerId:source.providerId}};
    assert.equal(receipt.cipherRecords.length,4);assert.deepEqual((await readCreationRecoveries(options))[0].records,[record]);
    assert.deepEqual(await archiveCreationIntents({...options,records:[record]}),receipt);
  }finally{f.close();}
});

test('stable IDs use locale-independent operation ordering and concurrent retries preserve the same committed rows',async()=>{
  const f=await fixture();try{
    const a=noteRecord(),b=noteRecord();a.operationId='a_1';b.operationId='a.1';
    const receipts=await Promise.all([archiveCreationIntents({...f.options,records:[a,b]}),archiveCreationIntents({...f.options,records:[b,a]})]);
    assert.deepEqual(receipts[0],receipts[1]);assert.deepEqual(receipts[0].creations.map(value=>value.operationId),['a.1','a_1']);
    assert.equal((await f.store.list()).length,3);
  }finally{f.close();}
});

async function heldManifest(f,archiver){
  let resolved=false,started=false;
  const store={...f.store,put(record,version){if(record.recordId.startsWith('creation_manifest_')&&!started){started=true;f.port.hold('soty-canvas-vault-local-v1','cipher');}return f.store.put(record,version);}};
  const work=archiver({...f.options,store,records:[noteRecord()]}).then(value=>{resolved=true;return value;});void work.catch(()=>{});
  await until(()=>f.port.held);return{work,get resolved(){return resolved;}};
}
const commitOracle=value=>assert.equal(value.resolved,false,'No archive acknowledgement before manifest COMMIT');

test('every manifest waits for actual IDB COMMIT and readback before any receipt',async()=>{
  const f=await fixture();let pending;try{
    const result=await heldManifest(f,archiveCreationIntents);pending=result.work;commitOracle(result);
    assert.equal(f.port.rows('soty-canvas-vault-local-v1','cipher').filter(row=>row.value?.recordId?.startsWith('creation_manifest_')).length,0);
    f.port.release();const receipt=await result.work;assert.ok(receipt.archiveId);
  }finally{while(f.port.held)f.port.release();await pending?.catch(()=>{});f.close();}
});

test('an intentionally premature ACK archiver fails the identical durable-COMMIT oracle',async()=>{
  const f=await fixture();let background;try{
    const early=async options=>{background=archiveCreationIntents(options);void background.catch(()=>{});return{schema:'intentionally-broken-early-ACK'};};
    const result=await heldManifest(f,early);assert.throws(()=>commitOracle(result),/No archive acknowledgement/u);
    f.port.release();await background;await result.work;
  }finally{while(f.port.held)f.port.release();await background?.catch(()=>{});f.close();}
});

test('actual recovery count quota and aborted manifest produce no success receipt or silent eviction',async()=>{
  const f=await fixture();try{
    for(let index=0;index<128;index++)assert.equal(await f.store.put(await sealCanvasVaultRecord(f.session,{schema:'soty.vault.local-record.v1',accountId,projectId,slot:objectId,
      recordId:'other_recovery_'+index,purpose:'recovery',version:1},{other:index}),0),true);
    const before=summary(await f.store.list());await assert.rejects(archiveCreationIntents({...f.options,records:[noteRecord()]}),{code:'vault_local_quota'});
    assert.deepEqual(summary(await f.store.list()),before);
  }finally{f.close();}
  const g=await fixture();try{
    const record=noteRecord(),store={...g.store,put(value,version){if(value.recordId.startsWith('creation_manifest_'))g.port.abort('soty-canvas-vault-local-v1','cipher');return g.store.put(value,version);}};
    await assert.rejects(archiveCreationIntents({...g.options,store,records:[record]}));
    assert.equal((await g.store.list()).some(value=>value.recordId.startsWith('creation_manifest_')),false);
    await assert.rejects(readCreationRecoveries(g.options),{code:'vault_local_creation_incomplete'});
    assert.ok((await archiveCreationIntents({...g.options,records:[record]})).archiveId,'Retry uses existing committed parts');
  }finally{g.close();}
});

test('missing part/manifest and forged JSON receipt cannot replace actual authenticated recovery bytes',async()=>{
  for(const missing of ['part','manifest']){
    const f=await fixture();try{
      const records=[noteRecord()],receipt=await archiveCreationIntents({...f.options,records}),row=receipt.cipherRecords.find(value=>missing==='manifest'?value.recordId===receipt.archiveId:value.recordId!==receipt.archiveId);
      await f.store.remove(row.recordId,row.version);
      await assert.rejects(verifyCreationRecoveries({...f.options,records,receipt}),{code:'vault_local_creation_missing'});
      await assert.rejects(readCreationRecoveries({...f.options,archiveId:receipt.archiveId}),{code:'vault_local_creation_missing'});
    }finally{f.close();}
  }
  const f=await fixture();try{const records=[noteRecord()];await assert.rejects(verifyCreationRecoveries({...f.options,records,receipt:{schema:'soty.creation-recovery.receipt.v1',archiveId:'creation_manifest_forged',objectId,source}}),{code:'vault_local_creation_scope'});}finally{f.close();}
});

test('wrong key, changed owner/account/object and same-version content collision fail without fallback',async()=>{
  const f=await fixture();try{
    const record=noteRecord(),receipt=await archiveCreationIntents({...f.options,records:[record]}),before=summary(await f.store.list());
    const wrong=await deriveCanvasVaultKey(f.profile,password+' wrong');
    await assert.rejects(readCreationRecoveries({...f.options,session:wrong}),{code:'vault_local_password_or_data_invalid'});
    await assert.rejects(readCreationRecoveries({...f.options,source:{...source,ownerId:'another_owner'}}),{code:'vault_local_creation_scope'});
    await assert.rejects(readCreationRecoveries({...f.options,accountId:'another_owner'}),{code:'vault_local_creation_scope'});
    await assert.rejects(readCreationRecoveries({...f.options,objectId:'another_alias_object'}),{code:'vault_local_creation_scope'});
    const changed=structuredClone(record);changed.sourceIntent.body='newer same-version body';
    await assert.rejects(archiveCreationIntents({...f.options,records:[changed]}),{code:'vault_local_creation_conflict'});
    await assert.rejects(verifyCreationRecoveries({...f.options,records:[changed],receipt}),{code:'vault_local_creation_conflict'});
    assert.deepEqual(summary(await f.store.list()),before);
  }finally{f.close();}
});

test('even a valid AEAD part with changed internal bytes cannot pass manifest hash authentication',async()=>{
  const f=await fixture();try{
    const records=[noteRecord()],receipt=await archiveCreationIntents({...f.options,records}),identity=receipt.cipherRecords.find(value=>value.recordId!==receipt.archiveId);
    const old=await f.store.get(identity.recordId),payload=await openCanvasVaultRecord(f.session,old),{cipher,...header}=old;
    payload.data='AAAA';const changed=await sealCanvasVaultRecord(f.session,{...header,version:old.version+1},payload);
    assert.equal(await f.store.put(changed,old.version),true);
    await assert.rejects(verifyCreationRecoveries({...f.options,records,receipt}),{code:'vault_local_creation_invalid'});
  }finally{f.close();}
});

test('rotation migrates every archived part; old RAM key cannot ACK and current password reopens/reuses exact identities',async()=>{
  const f=await fixture();try{
    const records=[noteRecord()],oldReceipt=await archiveCreationIntents({...f.options,records}),next=await rotateAll(f);
    await assert.rejects(archiveCreationIntents({...f.options,records}),{code:'vault_local_creation_profile_changed'});
    await assert.rejects(verifyCreationRecoveries({...f.options,records,receipt:oldReceipt}),{code:'vault_local_creation_profile_changed'});
    const options={...f.options,session:next.session},receipt=await verifyCreationRecoveries({...options,records,receipt:oldReceipt});
    assert.equal(receipt.archiveId,oldReceipt.archiveId);assert.equal(receipt.profileId,next.profile.profileId);assert.ok(receipt.cipherRecords.every(row=>row.version===2));
    assert.deepEqual((await readCreationRecoveries(options))[0].records,records);
    assert.deepEqual(await archiveCreationIntents({...options,records}),receipt);
  }finally{f.close();}
});

test('a rotation after old-key readback cannot acknowledge an obsolete profile even while old AES decryption still works',async()=>{
  const f=await fixture();try{
    const records=[noteRecord()],receipt=await archiveCreationIntents({...f.options,records});let next,opened=false;
    const store={...f.store,async get(id){const record=await f.store.get(id);
      if(!next&&id!==receipt.archiveId&&id.startsWith('creation_part_')){
        assert.equal((await openCanvasVaultRecord(f.session,record)).schema,'soty.creation-recovery.part.v1');opened=true;next=await rotateAll(f);
      }
      return record;
    }};
    await assert.rejects(verifyCreationRecoveries({...f.options,store,records,receipt}),{code:'vault_local_creation_profile_changed'});
    assert.equal(opened,true);assert.ok(next);
    assert.deepEqual((await readCreationRecoveries({...f.options,session:next.session}))[0].records,records);
  }finally{f.close();}
});

test('account/relock epoch changes before or during work cannot yield a receipt',async()=>{
  const f=await fixture();try{
    f.setCurrent(false);const before=summary(await f.store.list());
    await assert.rejects(archiveCreationIntents({...f.options,records:[noteRecord()]}),{code:'vault_account_changed'});assert.deepEqual(summary(await f.store.list()),before);
    f.setCurrent(true);let writes=0;
    const store={...f.store,async put(record,version){const result=await f.store.put(record,version);writes++;f.setCurrent(false);return result;}};
    await assert.rejects(archiveCreationIntents({...f.options,store,records:[noteRecord()]}),{code:'vault_account_changed'});assert.equal(writes,1);
    assert.equal((await f.store.list()).some(value=>value.recordId.startsWith('creation_manifest_')),false);
  }finally{f.close();}
});

test('a committed part changed after decryption is detected by the final exact stored-version check',async()=>{
  const f=await fixture();try{
    const records=[noteRecord()],receipt=await archiveCreationIntents({...f.options,records});let removed=false;
    const store={...f.store,async list(slot){if(!removed){removed=true;const part=receipt.cipherRecords.find(row=>row.recordId!==receipt.archiveId);await f.store.remove(part.recordId,part.version);}return f.store.list(slot);}};
    await assert.rejects(verifyCreationRecoveries({...f.options,store,records,receipt}),{code:'vault_local_creation_changed'});
  }finally{f.close();}
});

test('25MiB/4MiB admission does not expand original caps or leave partial archive on oversized input',async()=>{
  const f=await fixture();try{
    const record=noteRecord();record.sourceIntent.extra='m'.repeat(CANVAS_CREATION_RECOVERY_LIMITS.metadataBytes);
    await assert.rejects(archiveCreationIntents({...f.options,records:[record]}));
    const media={...noteRecord(),kind:'video',sourceIntent:{materialId:'original_video_source_123',blob:new Blob([new Uint8Array(25*1024*1024+1)],{type:'video/webm'})}};
    await assert.rejects(archiveCreationIntents({...f.options,source:{providerId:'soty.materials',ref:'original_video_source_123',ownerId:accountId},records:[media]}));
    assert.equal((await f.store.list()).length,0);
  }finally{f.close();}
});
