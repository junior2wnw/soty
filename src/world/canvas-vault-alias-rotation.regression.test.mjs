// OPEN acceptance: these expectations intentionally expose the current alias
// rotation defect. Do not count a detector/skip as completed cohort recovery.
import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,realpathSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,dirname,basename} from 'node:path';
import {createNotesService} from '../../modules/notes/server/index.mjs';
import {createCanvasObjectsService} from '../../modules/canvas-objects/server.mjs';
import {createCanvasVaultService} from '../../modules/canvas-vault/server.mjs';
import {createCanvasVaultClient,canvasVaultNoteContent} from './canvas-vault-client.mjs';
import {createCanvasVaultLocalStore,canvasVaultProfile,deriveCanvasVaultKey,sealCanvasVaultRecord,openCanvasVaultRecord} from './canvas-vault-local.mjs';
import {indexedNativePort} from '../../modules/notes/test/indexed-native-port.mjs';
import {seedVaultLegacyV2} from './test-support/vault-legacy-v2.mjs';

const actor={accountId:'alias_rotation_owner',deviceId:'alias_rotation_device'},projectId='alias_rotation',password='Original alias-source password18',nextPassword='New alias-source password72';
const canonical='Current Source content must not be overwritten',dirty='Unsaved B must block a Source password change',history='Recovery history B must survive rotation through A';
async function fixture(t,kind,{historyRecordId}={}){
  const parent=realpathSync(tmpdir()),folder=mkdtempSync(join(parent,'soty-alias-rotation-'));let notes,objects,vault,client,current=true;
  const fence=(original,callback)=>{assert.equal(original,actor);return callback();};
  const active=original=>original?.accountId===actor.accountId&&original?.deviceId===actor.deviceId;
  notes=createNotesService({databasePath:join(folder,'notes.sqlite'),projectId,allowNativeMigration:true,allowVaultMigration:true,verifyCanvasObjectAuthority:(context,callback)=>objects.withObjectAuthority(context,callback),verifyVaultCipherReceipt:(proof,callback)=>vault.withCipherReceiptAuthority(proof,callback),readVaultSource:request=>vault.readConvertedSource(request)});
  objects=createCanvasObjectsService({databasePath:join(folder,'objects.sqlite'),projectId,actorActive:active,withActorAuthorityFence:fence,withProtectionAuthority:(request,callback)=>vault.withProtectionAuthority(request,callback),providers:[{providerId:'soty.notes',
    withBindingAuthority:(request,callback)=>notes.canvas.withSourceAuthority({actor:request.actor,ownerId:request.objectOwnerId,mode:request.mode,...(request.purpose?{purpose:request.purpose}:{}),noteId:request.ref},callback),project:context=>({kind:'note',...notes.canvas.readMetadata({context})})}]});
  vault=createCanvasVaultService({databasePath:join(folder,'vault.sqlite'),projectId,allowFormat2Migration:true,actorActive:active,withActorAuthorityFence:fence,nativeProviders:[{providerId:'soty.notes',...notes.vault}],withProtectionTarget:(request,callback)=>objects.withProtectionTarget(request,callback),withObjectAuthority:(context,callback)=>objects.withObjectAuthority(context,callback)});objects.registerProvider(vault.provider);
  const createObject=binding=>objects.execute({actor,op:'canvas.objects.create',args:{expectedAccountId:actor.accountId,requestId:crypto.randomUUID(),binding,presentation:{mask:{shape:'rect'}},visibility:{audience:'private',accountIds:[],communityIds:[]},hidden:{owner:false,others:false},protection:{kind:binding.providerId==='soty.vault'?'password':'none'}}}).current;
  let binding;
  if(kind==='native'){
    const noteId=crypto.randomUUID();notes.execute({actor,op:'notes.put',args:{expectedAccountId:actor.accountId,noteId,mutationId:crypto.randomUUID(),expectedRevision:0,title:'Source',body:canonical,items:[],color:'plain',pinned:false,state:'active'}});binding={providerId:'soty.notes',ref:noteId};
  }else binding=(await vault.execute({actor,op:'canvas.vault.create',args:{expectedAccountId:actor.accountId,requestId:crypto.randomUUID(),password,content:canvasVaultNoteContent({title:'Source',body:canonical})}})).binding;
  const a=createObject(binding),b=createObject(binding);if(kind==='native'){const converted=await vault.execute({actor,op:'canvas.vault.protect',args:{expectedAccountId:actor.accountId,requestId:crypto.randomUUID(),objectId:a.objectId,expectedObjectRevision:a.revision,password}});assert.equal(converted.protected,true);}
  const calls=[],api={async request(op,args){calls.push({op,args:structuredClone(args)});if(op==='canvas.objects.content'){const {bytes,...wire}=objects.withObjectRead({actor,objectId:args.objectId},context=>vault.readContent({context,index:args.index??0}));return{...wire,bytesBase64:Buffer.from(bytes).toString('base64url')};}return(op.startsWith('canvas.objects.')?objects:vault).execute({actor,op,args:structuredClone(args)});}};
  const port=indexedNativePort(),origin='https://alias-rotation.example';
  if(historyRecordId){const profile=canvasVaultProfile(actor.accountId,projectId,b.objectId),oldKey=await deriveCanvasVaultKey(profile,password),record=await sealCanvasVaultRecord(oldKey,{schema:'soty.vault.local-record.v1',accountId:actor.accountId,projectId,slot:b.objectId,recordId:historyRecordId,purpose:'recovery',version:1},{schema:'soty.rotation-recovery-probe.v1',source:binding,body:history});await seedVaultLegacyV2({indexedDB:port.indexedDB,origin,accountId:actor.accountId,projectId,profiles:[profile],records:[record]});assert.equal(port.databases.get('soty-canvas-vault-local-v1').version,2);}
  const store=createCanvasVaultLocalStore(actor.accountId,projectId,{origin,indexedDB:port.indexedDB}),options={api,accountId:actor.accountId,projectId,isCurrent:()=>current,store};client=createCanvasVaultClient(options);
  t.after(async()=>{current=false;client.dispose();store.close();vault.close();objects.close();notes.close();const actual=realpathSync(folder);assert.equal(dirname(actual).toLowerCase(),parent.toLowerCase());assert.ok(basename(actual).startsWith('soty-alias-rotation-'));rmSync(actual,{recursive:true,force:true});});
  await client.unlock(a.objectId,password);await client.unlock(b.objectId,password);
  return{a,b,binding,calls,store,port,options,get client(){return client;},vault,objects,reopen(){client.dispose();client=createCanvasVaultClient(options);return client;}};
}

for(const kind of['native','vault']){
  test(`OPEN: ${kind} shared Source dirtyB blocks rotateA before password mutation`,async t=>{
    const f=await fixture(t,kind),note=await f.client.readNote(f.b.objectId),document={title:note.title,body:dirty,items:note.items,color:note.color,pinned:note.pinned,state:note.state},draft={note:{...note,...document},branchId:crypto.randomUUID(),generation:1,committedGeneration:0,pending:{mutationId:crypto.randomUUID(),expectedRevision:note.revision,generation:1,document},conflict:false,savedAt:1};
    await f.client.draftStore(f.b.objectId).put(draft);const before=f.calls.filter(value=>value.op==='canvas.vault.rotate').length;
    await assert.rejects(f.client.rotate(f.a.objectId,{oldPassword:password,newPassword:nextPassword,requestId:'alias_dirty_rotate_'+kind}),error=>error.code==='vault_local_draft_pending','An actual unsaved branch on B must block rotating their shared Source through A');
    assert.equal(f.calls.filter(value=>value.op==='canvas.vault.rotate').length,before);assert.equal((await f.client.draftStore(f.b.objectId).list())[0].note.body,dirty);assert.equal((await f.client.readNote(f.a.objectId)).body,canonical);
  });

  test(`OPEN: ${kind} shared Source recoveryB rekeys transactionally when A rotates and newPW restores B`,async t=>{
    // The former Object-B profile now originates in an ACTUAL version2 DB,
    // before current-client upgrade. Core recovery/data/RPC oracles below stay.
    const recordId='alias_recovery_'+kind,f=await fixture(t,kind,{historyRecordId:recordId});
    await f.client.rotate(f.a.objectId,{oldPassword:password,newPassword:nextPassword,requestId:'alias_history_rotate_'+kind});assert.ok(await f.store.get(recordId),'Rotation must not delete B archives');
    // The real backend accepts the new Source password for B: a later local
    // decrypt error cannot be disguised as server denial or changed Object ACL.
    const serverGrant=await f.vault.execute({actor,op:'canvas.vault.unlock',args:{expectedAccountId:actor.accountId,objectId:f.b.objectId,password:nextPassword}});assert.equal(serverGrant.locked,false);
    const reopened=f.reopen();await reopened.unlock(f.b.objectId,nextPassword);
    const nextProfile=await f.store.ensureProfile(canvasVaultProfile(actor.accountId,projectId,f.b.objectId)),nextKey=await deriveCanvasVaultKey(nextProfile,nextPassword),recovered=await openCanvasVaultRecord(nextKey,await f.store.get(recordId));assert.equal(recovered.body,history);assert.equal((await reopened.readNote(f.b.objectId)).body,canonical,'Rekey is never an automatic replay of old content to Source');
    const rotations=f.calls.filter(value=>value.op==='canvas.vault.rotate');assert.equal(rotations.length,1);assert.equal(rotations[0].args.requestId,'alias_history_rotate_'+kind);assert.equal(f.calls.some(value=>value.op==='canvas.vault.write'),false);
    const raw=JSON.stringify(f.port.rows('soty-canvas-vault-local-v1','cipher'));for(const value of[history,password,nextPassword])assert.equal(raw.includes(value),false);
  });
}
