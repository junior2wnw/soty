import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createNotesService} from '../../modules/notes/server/index.mjs';
import {createNativeDraftStore} from '../../modules/notes/browser/native-local.mjs';
import {createNoteCache,createIndexedNoteCacheStorage} from '../../modules/notes/browser/cache.mjs';
import {indexedNativePort} from '../../modules/notes/test/indexed-native-port.mjs';
import {createNativeCacheTransition} from './native-cache-transition.mjs';
import {createCanvasCreateStore} from './canvas-create-flow.mjs';

const actor={accountId:'orphan_owner',deviceId:'orphan_device'},projectId='orphan_test',objectId='object_orphan_123',origin='https://orphan.example';
async function scenario(t,{conflict=false,create=createNativeCacheTransition}={}){
  const notes=createNotesService({databasePath:':memory:',projectId}),port=indexedNativePort(),store=createNativeDraftStore(actor.accountId,projectId,{origin,indexedDB:port.indexedDB}),cache=createNoteCache(actor.accountId,projectId,{origin,storage:createIndexedNoteCacheStorage(port.indexedDB)}),calls=[];
  t.after(()=>notes.close());const noteId=crypto.randomUUID(),input={title:'Original',body:'Original',items:[],color:'plain',pinned:false,state:'active'};
  notes.execute({actor,op:'notes.put',args:{expectedAccountId:actor.accountId,noteId,expectedRevision:0,mutationId:crypto.randomUUID(),...input}});
  const note=notes.execute({actor,op:'notes.get',args:{expectedAccountId:actor.accountId,noteId}}).note,mutationId=crypto.randomUUID(),document={...input,body:'Exact lost ACK orphan draft'};
  if(!conflict)notes.execute({actor,op:'notes.put',args:{expectedAccountId:actor.accountId,noteId,expectedRevision:1,mutationId,...document}});
  else notes.execute({actor,op:'notes.put',args:{expectedAccountId:actor.accountId,noteId,expectedRevision:1,mutationId:crypto.randomUUID(),...input,body:'Newer remote writer'}});
  await store.put({note:{...note,...document},branchId:'orphan_branch_123',generation:1,committedGeneration:0,pending:{mutationId,expectedRevision:1,generation:1,document},conflict:false,savedAt:1});
  const api={async request(op,args){calls.push({op,args:structuredClone(args)});if(op==='canvas.objects.get')return{object:{objectId,ownerId:actor.accountId,revision:1,state:'active',binding:{providerId:'soty.notes',ref:noteId},protection:{kind:'none'}}};
    if(op==='canvas.vault.status')return{protection:{kind:'none'}};return notes.execute({actor,op,args});}};
  const creationStore=createCanvasCreateStore(actor.accountId,projectId,{origin,indexedDB:port.indexedDB});
  const flow=create({api,accountId:actor.accountId,projectId,isCurrent:()=>true,store,cache,creationStore,vault:{isUnlocked:()=>false},inspectCreation:async()=>[]});t.after(()=>flow.dispose());
  const result=await flow.prepare({objectId,expectedObjectRevision:1,operationId:'orphan_conversion_123'});
  if(conflict){assert.equal(result.state,'pending');assert.equal(result.protectionComplete,false);const rows=await store.native.inspect(noteId);assert.ok(rows.some(row=>row.note.body===document.body&&row.conflict),'An actual CAS conflict stays recoverable and pending');assert.equal(notes.execute({actor,op:'notes.get',args:{expectedAccountId:actor.accountId,noteId}}).note.body,'Newer remote writer');}
  else{assert.equal(result.state,'ready','A healthy persisted orphan must drain through the existing NoteSession before conversion');assert.equal((await store.native.inspect(noteId)).length,0);const puts=calls.filter(value=>value.op==='notes.put');assert.equal(puts.length,1);assert.equal(puts[0].args.mutationId,mutationId);assert.equal(notes.execute({actor,op:'notes.get',args:{expectedAccountId:actor.accountId,noteId}}).note.revision,2,'Lost ACK is retried with the original mutation, not duplicated');}
}
test('prepare drains a healthy persisted orphan with its exact durable Notes mutation',t=>scenario(t));
test('prepare preserves a conflicting orphan and cannot replace the newer Source text',t=>scenario(t,{conflict:true}));
test('negative control: treating persisted orphans as passive rows fails the readiness oracle',async t=>{
  const url=new URL('./native-cache-transition.mjs',import.meta.url),source=readFileSync(url,'utf8'),needle='await drainOrphans(source,ports);';assert.ok(source.includes(needle));
  const broken=source.replace(needle,''),absolute=broken.replace(/from '(\.\.?\/[^']+)'/gu,(_match,path)=>`from '${new URL(path,url).href}'`),module=await import(`data:text/javascript;base64,${Buffer.from(absolute).toString('base64')}`);
  await assert.rejects(scenario(t,{create:module.createNativeCacheTransition}),/A healthy persisted orphan must drain/);
});
