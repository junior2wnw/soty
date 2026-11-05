import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createCanvasCreateFlow,createCanvasCreateStore} from './canvas-create-flow.mjs';
import {createNativeDraftStore} from '../../modules/notes/browser/native-local.mjs';
import {indexedNativePort} from '../../modules/notes/test/indexed-native-port.mjs';

const accountId='creation_privacy_owner',projectId='creation_privacy',origin='https://creation-privacy.example';
function record(kind='note'){
  const ref=crypto.randomUUID();return{schema:'soty.canvas-create.v1',operationId:crypto.randomUUID(),accountId,projectId,kind,createdAt:1,version:1,stage:'source',objectRequestId:crypto.randomUUID(),
    sourceIntent:kind==='note'?{noteId:ref,mutationId:crypto.randomUUID(),expectedRevision:0,title:'Private lost ACK title',body:'Private lost ACK content',items:[],color:'plain',pinned:false,state:'active'}
      :{materialId:ref,name:'Private picture name.png',blob:new Blob(['private file'],{type:'image/png'})},sourceRef:null,objectId:null,presentation:{mask:{shape:'rect'},fontSize:16},width:340,height:300,placement:null,intentHash:'b'.repeat(64)};
}
async function scenario(t,create=createCanvasCreateFlow){
  const port=indexedNativePort(),store=createCanvasCreateStore(accountId,projectId,{origin,indexedDB:port.indexedDB}),drafts=createNativeDraftStore(accountId,projectId,{origin,indexedDB:port.indexedDB});
  const row=record(),source={providerId:'soty.notes',ref:row.sourceIntent.noteId};await store.put(row,0);await drafts.native.mark({...source,operationId:'remote_conversion',state:'sealed'});
  let calls=0;const flow=create({api:{async request(){calls++;throw Error('sealed_upload_replayed');}},accountId,projectId,isCurrent:()=>true,place:async()=>{throw Error('No placement while recovery is pending');},store});
  t.after(()=>{flow.dispose();store.close();});
  await assert.rejects(flow.resume(row.operationId),error=>error.code==='canvas_create_native_recovery_pending','A known protected Source cannot replay a plaintext outbox');
  assert.equal(calls,0,'The guard must precede every Source network action');
  const pending=await flow.listPending();assert.equal(pending.length,1);assert.equal(pending[0].label,'Нужен перенос на этом устройстве');
  assert.doesNotMatch(JSON.stringify(pending),/Private lost ACK|body|intentHash/u);
  assert.equal((await store.get(row.operationId)).sourceIntent.body,row.sourceIntent.body,'Pending recovery never deletes an unacknowledged intent');
  const inspected=await store.inspectSourceIntents(source);assert.deepEqual(inspected,[{operationId:row.operationId,stage:'source',holdsPlaintext:true,sourceConfirmed:false}]);
}
test('the actual creation journal blocks a sealed Source replay, hides its label and preserves the lost-ACK payload',scenario);

test('source inspection is scoped to stable identity and never exposes a Blob, title or content fingerprint',async()=>{
  const port=indexedNativePort(),store=createCanvasCreateStore(accountId,projectId,{origin,indexedDB:port.indexedDB}),row=record('image');await store.put(row,0);
  const output=await store.inspectSourceIntents({providerId:'soty.materials',ref:row.sourceIntent.materialId});
  assert.deepEqual(output,[{operationId:row.operationId,stage:'source',holdsPlaintext:true,sourceConfirmed:false}]);assert.doesNotMatch(JSON.stringify(output),/Private|name|blob|intentHash|body/u);
  assert.deepEqual(await store.inspectSourceIntents({providerId:'soty.notes',ref:row.sourceIntent.materialId}),[]);store.close();
});

test('negative control: removing the Source fence fails the same zero-network privacy oracle',async t=>{
  const url=new URL('./canvas-create-flow.mjs',import.meta.url),source=readFileSync(url,'utf8'),needle='await assertSourceOpen(record);';
  assert.ok(source.includes(needle),'Production replay fence is present');
  const broken=source.replaceAll(needle,''),absolute=broken.replace(/from '(\.\.?\/[^']+)'/gu,(_match,path)=>`from '${new URL(path,url).href}'`);
  const module=await import(`data:text/javascript;base64,${Buffer.from(absolute).toString('base64')}`);
  await assert.rejects(scenario(t,module.createCanvasCreateFlow),/A known protected Source cannot replay a plaintext outbox/);
});
