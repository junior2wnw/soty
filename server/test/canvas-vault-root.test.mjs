import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {DatabaseSync} from 'node:sqlite';
import {join} from 'node:path';
import {existsSync,readFileSync} from 'node:fs';
import {nativeHttpFixture,nativeIdentity,good} from './support/native-capability-http.mjs';
import {digestArgs} from '../../modules/connect/server/index.mjs';

const id=()=>randomUUID().replaceAll('-',''),password='Root-wire-password-4184',nextPassword='Rotated-root-password-b278',secret='Root encrypted note — no plaintext source dc2f1703';
const content=text=>({contentType:'application/vnd.soty.note+json',bytesBase64:Buffer.from(JSON.stringify({title:'Защищённая записка',body:text})).toString('base64url')});
async function fixture(t){
 const f=await nativeHttpFixture(t),owner=nativeIdentity('Vault real owner'),viewer=nativeIdentity('Vault real viewer'),outsider=nativeIdentity('Vault outsider'),accounts=new Map(),devices=new Map();
 for(const actor of [owner,viewer,outsider]){const boot=await f.bootstrap(actor);accounts.set(actor,boot.accountId);devices.set(actor,boot.deviceId);}
 const call=(actor,op,args={})=>f.call(actor,op,{expectedAccountId:accounts.get(actor),...args});
 const mutate=(actor,op,args={})=>call(actor,op,{requestId:id(),...args});
 const create=async(extra={})=>{const source=good(await mutate(owner,'canvas.vault.create',{password,content:content(secret)}));const object=good(await mutate(owner,'canvas.objects.create',{binding:source.binding,presentation:{mask:{shape:'rect'},fontSize:16},visibility:{audience:'private',accountIds:[],communityIds:[]},hidden:{owner:false,others:false},protection:{kind:'password'},...extra})).current;return {source,object};};
 const read=async(actor,objectId)=>{const value=good(await call(actor,'canvas.objects.content',{objectId,part:'vault'}));return JSON.parse(Buffer.from(value.bytesBase64,'base64url'));};
 const project=async(actor,objectId)=>good(await call(actor,'canvas.objects.project',{objectIds:[objectId]})).items[0];
 const wait=async(actor,objectId,cursor)=>good(await call(actor,'platform.live.wait',{watch:[{kind:'object',id:objectId}],waitMs:0,...(cursor?{cursor}:{})}));
 const vaultFile=join(f.directory,'canvas','vault.sqlite'),sql=callback=>{const db=new DatabaseSync(vaultFile);try{return callback(db);}finally{db.close();}};
 return {...f,get app(){return f.app;},owner,viewer,outsider,accounts,devices,call,mutate,create,read,project,wait,vaultFile,sql};
}
const denied=(response,error)=>{assert.equal(response.ok,false);if(error)assert.equal(response.error.code,error);return response;};

test('Root signed Vault: encrypted create→locked/wrong password→unlock→read/write/rotate/relock→restart; no source or cache plaintext',{timeout:30000},async t=>{
 const f=await fixture(t),{source,object}=await f.create(),objectId=object.objectId;
 assert.equal((await f.project(f.owner,objectId)).view.state,'locked');denied(await f.call(f.owner,'canvas.objects.content',{objectId,part:'vault'}),'canvas_object_locked');
 denied(await f.call(f.owner,'canvas.vault.unlock',{objectId,password:'incorrect-password'}),'password_container_unavailable');
 denied(await f.call(f.viewer,'canvas.vault.unlock',{objectId,password}),'canvas_object_unavailable');
 const copiedActor={accountId:f.accounts.get(f.owner),deviceId:f.devices.get(f.owner)};assert.equal(f.app.locals.connectService.isActorActive(copiedActor),true);
 await assert.rejects(f.app.locals.canvasVaultService.execute({actor:copiedActor,op:'canvas.vault.unlock',args:{expectedAccountId:copiedActor.accountId,objectId,password}}),{code:'connect_authority_actor_invalid'});
 good(await f.call(f.owner,'canvas.vault.unlock',{objectId,password}));assert.equal((await f.read(f.owner,objectId)).body,secret);assert.equal((await f.project(f.owner,objectId)).view.kind,'vault');
 const update={requestId:'root_encrypted_write',objectId,expectedRevision:1,content:content('Root encrypted updated note')};const saved=good(await f.call(f.owner,'canvas.vault.write',update));assert.equal(saved.current.revision,2);assert.equal(good(await f.call(f.owner,'canvas.vault.write',update)).replayed,true);assert.equal((await f.read(f.owner,objectId)).body,'Root encrypted updated note');
 const rotation={requestId:'root_encrypted_rotate',objectId,expectedRevision:2,oldPassword:password,newPassword:nextPassword};assert.equal(good(await f.call(f.owner,'canvas.vault.rotate',rotation)).current.revision,3);assert.equal(good(await f.call(f.owner,'canvas.vault.rotate',rotation)).replayed,true);
 good(await f.mutate(f.owner,'canvas.vault.relock',{objectId,scope:'all'}));denied(await f.call(f.owner,'canvas.objects.content',{objectId,part:'vault'}),'canvas_object_locked');denied(await f.call(f.owner,'canvas.vault.unlock',{objectId,password}),'password_container_unavailable');
 for(const suffix of ['','-wal','-shm'])if(existsSync(f.vaultFile+suffix)){const bytes=readFileSync(f.vaultFile+suffix);for(const value of [password,nextPassword,secret,'Root encrypted updated note',content(secret).bytesBase64])assert.equal(bytes.includes(Buffer.from(value)),false);}
 const connectFile=join(f.directory,'connect','accounts.sqlite'),unlockDigest=digestArgs({expectedAccountId:f.accounts.get(f.owner),objectId,password});
 const connectSql=new DatabaseSync(connectFile,{readOnly:true});try{assert.equal(connectSql.prepare("SELECT count(*) n FROM challenges WHERE operation LIKE 'canvas.vault.%'").get().n,0);assert.ok(connectSql.prepare("SELECT count(*) n FROM challenges WHERE operation='bootstrap'").get().n>0,'ordinary durable proofs retain their original storage');}finally{connectSql.close();}
 for(const suffix of ['','-wal','-shm','-journal'])if(existsSync(connectFile+suffix))assert.equal(readFileSync(connectFile+suffix).includes(Buffer.from(unlockDigest)),false,'Root must not retain a cheap known-args password oracle');
 assert.equal(f.sql(db=>db.prepare('SELECT count(*) n FROM vault_sources').get().n),1);assert.equal(f.sql(db=>db.prepare('SELECT revision FROM vault_sources WHERE vault_id=?').get(source.current.vaultId).revision),3);
 assert.equal(f.sql(db=>JSON.stringify(db.prepare('SELECT * FROM vault_commands').all())).includes(password),false);
 await f.restart({enabled:true});denied(await f.call(f.owner,'canvas.objects.content',{objectId,part:'vault'}),'canvas_object_locked');good(await f.call(f.owner,'canvas.vault.unlock',{objectId,password:nextPassword}));assert.equal((await f.read(f.owner,objectId)).body,'Root encrypted updated note');
 assert.equal((await f.project(f.viewer,objectId)).available,false);
});

test('Root Vault live: two current accounts receive content-free changes, outsider stays silent, revoked viewer gets one removal then silence',{timeout:30000},async t=>{
 const f=await fixture(t),{object}=await f.create({visibility:{audience:'restricted',accountIds:[f.accounts.get(f.viewer)],communityIds:[]}}),objectId=object.objectId;
 const ownerBefore=await f.wait(f.owner,objectId),viewerBefore=await f.wait(f.viewer,objectId),outsideBefore=await f.wait(f.outsider,objectId);
 good(await f.call(f.owner,'canvas.vault.unlock',{objectId,password}));good(await f.call(f.viewer,'canvas.vault.unlock',{objectId,password}));assert.equal((await f.read(f.viewer,objectId)).body,secret);
 const write=good(await f.mutate(f.owner,'canvas.vault.write',{objectId,expectedRevision:1,content:content('Private shared encrypted edit')}));assert.equal(write.current.revision,2);
 assert.equal((await f.wait(f.owner,objectId,ownerBefore.cursor)).changed,true);let viewerState=await f.wait(f.viewer,objectId,viewerBefore.cursor);assert.equal(viewerState.changed,true);assert.equal((await f.wait(f.outsider,objectId,outsideBefore.cursor)).changed,false);
 denied(await f.call(f.viewer,'canvas.objects.content',{objectId,part:'vault'}),'canvas_object_locked','source version invalidates old DEK permit');
 good(await f.mutate(f.owner,'canvas.objects.update',{objectId,expectedRevision:1,visibility:{audience:'private',accountIds:[],communityIds:[]}}));viewerState=await f.wait(f.viewer,objectId,viewerState.cursor);assert.equal(viewerState.changed,true);assert.equal((await f.project(f.viewer,objectId)).available,false);
 denied(await f.call(f.viewer,'canvas.vault.unlock',{objectId,password}),'canvas_object_unavailable');good(await f.call(f.owner,'canvas.vault.unlock',{objectId,password}));good(await f.mutate(f.owner,'canvas.vault.write',{objectId,expectedRevision:2,content:content('Later owner only encrypted edit')}));
 assert.equal((await f.wait(f.viewer,objectId,viewerState.cursor)).changed,false);assert.equal((await f.wait(f.outsider,objectId,outsideBefore.cursor)).changed,false);
});

test('Root Vault SQLite fault fails signed mutation, current data/permit remain usable, same request succeeds after removing fault',{timeout:30000},async t=>{
 const f=await fixture(t),{object}=await f.create(),objectId=object.objectId;good(await f.call(f.owner,'canvas.vault.unlock',{objectId,password}));const before=await f.wait(f.owner,objectId);
 f.sql(db=>db.exec("CREATE TRIGGER fault_root_vault BEFORE INSERT ON vault_commands BEGIN SELECT RAISE(ABORT,'root_vault_fault'); END"));
 const args={requestId:'root_fault_retry',objectId,expectedRevision:1,content:content('Successfully retried encrypted edit')};denied(await f.call(f.owner,'canvas.vault.write',args));assert.equal((await f.read(f.owner,objectId)).body,secret);assert.equal((await f.wait(f.owner,objectId,before.cursor)).changed,false);
 f.sql(db=>db.exec('DROP TRIGGER fault_root_vault'));assert.equal(good(await f.call(f.owner,'canvas.vault.write',args)).current.revision,2);assert.equal((await f.read(f.owner,objectId)).body,'Successfully retried encrypted edit');assert.equal((await f.wait(f.owner,objectId,before.cursor)).changed,true);
});

test('Root Vault does not claim large whole-content RPC support or accept a plaintext native Source password flag',{timeout:30000},async t=>{
 const f=await fixture(t),large={contentType:'application/octet-stream',bytesBase64:Buffer.alloc(1700000,7).toString('base64url')};
 const args={expectedAccountId:f.accounts.get(f.owner),requestId:id(),password,content:large},signed=await f.proof(f.owner,'canvas.vault.create',args),body=JSON.stringify({protocol:1,...signed});
 const wire=await f.http('/api/connect/rpc',{method:'POST',headers:{origin:f.origin,'content-length':Buffer.byteLength(body)},body});assert.equal(wire.status,413);const rejected=denied(wire.body);assert.ok(['request_too_large','arguments_too_large'].includes(rejected.error.code),rejected.error.code);assert.equal(f.sql(db=>db.prepare('SELECT count(*) n FROM vault_sources').get().n),0);
 const noteId='note_'+id();good(await f.call(f.owner,'notes.put',{noteId,expectedRevision:0,mutationId:'native_note_'+id(),title:'Existing plain note',body:'A real existing plaintext source',items:[],color:'plain',pinned:false,state:'active'}));
 const object=good(await f.mutate(f.owner,'canvas.objects.create',{binding:{providerId:'soty.notes',ref:noteId},presentation:{mask:{shape:'rect'}},visibility:{audience:'private',accountIds:[],communityIds:[]},hidden:{owner:false,others:false},protection:{kind:'none'}})).current;
 denied(await f.mutate(f.owner,'canvas.vault.protect',{objectId:object.objectId,expectedObjectRevision:1,password}),'vault_conversion_required');assert.equal(good(await f.call(f.owner,'notes.get',{noteId})).note.body,'A real existing plaintext source');
 assert.equal(good(await f.call(f.owner,'canvas.vault.status',{objectId:object.objectId})).protection.kind,'none');
});
