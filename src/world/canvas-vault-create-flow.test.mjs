import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {vaultIndexedPort as indexedPort} from './test-support/vault-indexed-port.mjs';
import {createCanvasObjectsService} from '../../modules/canvas-objects/server.mjs';
import {createCanvasVaultService} from '../../modules/canvas-vault/server.mjs';
import {createFieldDocument,validateAnyFieldDocument} from '../../modules/field/contract.mjs';
import {applyFieldCommand} from './unified-field-state.mjs';
import {createCanvasVaultCreateFlow} from './canvas-vault-create-flow.mjs';
import {createCanvasVaultLocalStore} from './canvas-vault-local.mjs';
const owner={accountId:'vault_create_owner',deviceId:'vault_create_device'},password='Protected create synthetic password',secret='This note only enters cipher sources 7fc428';
function setup(t,{lost,store:givenStore}={}){
  const port=indexedPort(),store=givenStore??createCanvasVaultLocalStore(owner.accountId,'vault-create',{origin:'http://localhost',indexedDB:port.indexedDB});
  let objects,vault,current=true,failed=false;const calls=[],originals=new WeakSet([owner]);
  const authority=(actor,callback)=>{assert.ok(originals.has(actor));return callback();};
  objects=createCanvasObjectsService({databasePath:':memory:',projectId:'vault-create',actorActive:()=>true,withActorAuthorityFence:authority,withProtectionAuthority:(value,callback)=>vault.withProtectionAuthority(value,callback)});
  vault=createCanvasVaultService({databasePath:':memory:',projectId:'vault-create',actorActive:()=>true,withActorAuthorityFence:authority,withProtectionTarget:(value,callback)=>objects.withProtectionTarget(value,callback),withObjectAuthority:(value,callback)=>objects.withObjectAuthority(value,callback)});objects.registerProvider(vault.provider);
  let field=createFieldDocument();field.contexts.push({contextId:'personal',title:'Личное',x:0,y:0});
  const checkLocal=()=>{
    if(![...port.rows.values()].some(row=>row.value.purpose==='create'))throw Object.assign(new Error('Source before cipher journal COMMIT'),{code:'vault_source_before_journal'});
    for(const row of port.rows.values())for(const marker of[secret,password])assert.equal(JSON.stringify(row).includes(marker),false);
  };
  const api={async request(op,args){calls.push({op,args:structuredClone(args)});checkLocal();let result;
    if(op.startsWith('canvas.vault.'))result=await vault.execute({op,args:structuredClone(args),actor:owner});
    else if(op==='canvas.objects.create')result=objects.execute({op,args:structuredClone(args),actor:owner});
    else assert.fail(`No plaintext Notes/Materials or unexpected operation ${op}`);
    if(!failed&&lost===op){failed=true;throw Object.assign(new Error('Lost signed ACK'),{code:'network_error'});}return result;
  }};
  const place=async(objectId,size)=>{
    checkLocal();const projection=objects.project({actor:owner,objectIds:[objectId]}).items[0];assert.equal(projection.available,true);assert.equal(projection.view.state,'locked');
    const existing=field.shortcuts.find(value=>value.entity.kind==='object'&&value.entity.id===objectId);
    if(!existing)field=applyFieldCommand(field,{type:'add-shortcut',shortcutId:crypto.randomUUID(),entity:{kind:'object',id:objectId},contextId:'personal',placement:{kind:'free',x:0,y:0,...size}}).document;
    const shortcut=field.shortcuts.find(value=>value.entity.id===objectId);if(!failed&&lost==='placement'){failed=true;throw Object.assign(new Error('Lost placement ACK'),{code:'network_error'});}
    return{shortcutId:shortcut.shortcutId,status:'saved',localDurable:true};
  };
  const options={api,accountId:owner.accountId,projectId:'vault-create',isCurrent:()=>current,store,place};
  t.after(()=>{store.close?.();vault.close();objects.close();});
  return{port,store,calls,options,objects,vault,get field(){return field;},changeAccount(){current=false;},
    read(objectId){return objects.withObjectRead({actor:owner,objectId},context=>vault.readContent({context}));},
    unlock:objectId=>vault.execute({actor:owner,op:'canvas.vault.unlock',args:{expectedAccountId:owner.accountId,objectId,password}}),
  };
}
test('protected note creates one real encrypted Source/Object/placement; no Notes or plaintext local journal',async t=>{
  const f=setup(t),flow=createCanvasVaultCreateFlow(f.options);t.after(()=>flow.dispose());
  const result=await flow.startNote({title:'Секрет',body:secret},password);assert.equal(result.status,'saved');assert.equal(result.localDurable,true);
  validateAnyFieldDocument(f.field);assert.equal(f.field.schema,'soty.field.v2');assert.equal(f.field.shortcuts.length,1);assert.deepEqual(f.field.shortcuts[0].entity,{kind:'object',id:result.objectId});
  assert.equal(JSON.stringify(f.field).includes(secret),false);assert.equal(f.calls.some(call=>/^(notes|materials)\./u.test(call.op)),false);assert.deepEqual(await flow.listPending(),[]);
  assert.throws(()=>f.read(result.objectId),{code:'canvas_object_locked'});await f.unlock(result.objectId);assert.equal(JSON.parse(f.read(result.objectId).bytes).body,secret);
});
for(const lost of['canvas.vault.create','canvas.objects.create','placement'])test(`protected creation lost ${lost} ACK reopens the encrypted journal with the same stable requests`,async t=>{
  const f=setup(t,{lost}),first=createCanvasVaultCreateFlow(f.options);let operationId;
  await assert.rejects(first.startNote({body:secret},password),error=>{operationId=error.operationId;assert.equal(error.localDurable,true);return error.code==='network_error';});
  assert.equal((await first.listPending())[0].operationId,operationId);first.dispose();const reopened=createCanvasVaultCreateFlow(f.options);t.after(()=>reopened.dispose());
  const before=f.calls.length;await assert.rejects(reopened.resume(operationId,'Incorrect reopening password'),{code:'vault_local_password_or_data_invalid'});assert.equal(f.calls.length,before);
  const result=await reopened.resume(operationId,password);assert.equal(f.field.shortcuts.length,1);assert.equal(result.objectId,f.field.shortcuts[0].entity.id);
  for(const operation of['canvas.vault.create','canvas.objects.create']){
    const ids=f.calls.filter(call=>call.op===operation).map(call=>call.args.requestId);assert.equal(new Set(ids).size,1,`${operation} retains its idempotency identity`);
  }
  await f.unlock(result.objectId);assert.equal(JSON.parse(f.read(result.objectId).bytes).body,secret);assert.deepEqual(await reopened.listPending(),[]);
});
test('small actual protected image/audio/video files use Vault bytes only and reopen correctly',async t=>{
  for(const[type,path]of[['image/png','../../public/icons/soty-192.png'],['audio/webm','../../modules/materials/test/fixtures/tone.webm'],['video/webm','../../modules/materials/test/fixtures/video.webm']]){
    const f=setup(t),flow=createCanvasVaultCreateFlow(f.options);t.after(()=>flow.dispose());const bytes=readFileSync(new URL(path,import.meta.url)),file=new Blob([bytes],{type});
    const result=await flow.startFile(file,password);await f.unlock(result.objectId);const source=f.read(result.objectId);assert.equal(source.contentType,type);assert.deepEqual(Buffer.from(source.bytes),bytes);
  }
});
test('a Windows-labelled video/webm file containing only Opus is classified before encrypted journal/source creation',async t=>{
  const f=setup(t),flow=createCanvasVaultCreateFlow(f.options);t.after(()=>flow.dispose());
  const blob=new Blob([readFileSync(new URL('../../modules/materials/test/fixtures/tone.webm',import.meta.url))],{type:'video/webm'});
  const result=await flow.startFile(blob,password);assert.equal(f.field.shortcuts[0].placement.height,156);
  await f.unlock(result.objectId);assert.equal(f.read(result.objectId).contentType,'audio/webm');
});
test('wire cap, unsupported MIME, unavailable crypto and initial storage failure send no source mutation',async t=>{
  const f=setup(t),flow=createCanvasVaultCreateFlow(f.options);t.after(()=>flow.dispose());
  await assert.rejects(flow.startFile(new Blob([new Uint8Array(1024*1024+1)],{type:'image/png'}),password),{code:'vault_wire_size'});
  await assert.rejects(flow.startFile(new Blob(['<svg>'],{type:'image/svg+xml'}),password),{code:'canvas_create_media_type_unsupported'});
  const insecure=createCanvasVaultCreateFlow({...f.options,crypto:{}});await assert.rejects(insecure.startNote({body:secret},password),{code:'vault_secure_context_required'});insecure.dispose();
  const failed=createCanvasVaultCreateFlow({...f.options,store:{ensureProfile:async()=>{throw Object.assign(new Error('Disabled storage'),{code:'vault_local_unavailable'});}}});
  await assert.rejects(failed.startNote({body:secret},password),error=>error.code==='vault_local_unavailable'&&error.localDurable===false);failed.dispose();assert.equal(f.calls.length,0);
});
test('pending field placement needs a real durable outbox and an account change stops later source stages',async t=>{
  const f=setup(t),flow=createCanvasVaultCreateFlow({...f.options,place:async()=>({shortcutId:'unconfirmed',status:'pending',localDurable:false})});t.after(()=>flow.dispose());
  await assert.rejects(flow.startNote({body:secret},password),error=>error.code==='vault_create_placement_not_durable'&&error.localDurable===true);
  assert.equal((await flow.listPending()).length,1);f.changeAccount();const before=f.calls.length;await assert.rejects(flow.resume((await f.store.list()).find(row=>row.purpose==='create').recordId,password),{code:'vault_account_changed'});assert.equal(f.calls.length,before);
});
test('negative control: a premature protected Source call is caught by the unchanged cipher-journal COMMIT oracle',async t=>{
  const url=new URL('./canvas-vault-create-flow.mjs',import.meta.url),source=readFileSync(url,'utf8');
  const target='volatile.set(operationId,{profile,record});';assert.equal(source.split(target).length,2);
  const broken=source.replace(target,target+"await request('canvas.vault.create',{requestId:payload.sourceRequestId,password,content});")
    .replace(/from '([^']+)'/gu,(_,path)=>`from '${new URL(path,url)}'`);
  const module=await import('data:text/javascript;base64,'+Buffer.from(broken).toString('base64'));
  const f=setup(t),flow=module.createCanvasVaultCreateFlow(f.options);t.after(()=>flow.dispose());
  await assert.rejects(flow.startNote({body:secret},password),{code:'vault_source_before_journal'});
});
