import test from 'node:test';
import assert from 'node:assert/strict';
import {createCanvasObjectSecurityActions} from './canvas-object-security-actions.mjs';

const complete={server:'protected',device:'complete',protectionComplete:true};
const unprotected={server:'unprotected',device:'complete',protectionComplete:false};
const pending={server:'unknown',device:'pending',protectionComplete:false};
const reject=code=>Object.assign(new Error(code),{code});
function fixture({native=true,capability=true,prepare,finalize,protect,status,owner='owner',transitionAvailable=true,objects={}}={}){
 const calls=[];let current=true,sourceStatus=status??{locked:false,protection:{kind:'none'},capabilities:{nativeConversion:native&&capability,placementProtection:!native&&capability}};
 const object={objectId:'object_1',ownerId:owner,revision:7,binding:{providerId:native?'soty.notes':'soty.apps',ref:'source_1'}};
 const record=(name,args)=>calls.push({name,args});
 const api={async request(op,args){record(op,args);return{object:args.objectId===object.objectId?object:objects[args.objectId]};}};
 const vault={isUnlocked(){return sourceStatus.locked===false;},async status(id,options){record('status',{id,options});return sourceStatus;},async unlock(...args){record('unlock',args);},
  async protect(...args){record('protect',args);if(protect)return protect(...args);sourceStatus={...sourceStatus,protection:{kind:'password'}};return{current:{revision:1}};},
  async relock(...args){record('relock',args);},async rotate(...args){record('rotate',args);}};
 const transition={remember(...args){record('remember',args);},async liveReconcile(...args){record('reconcile',args);return unprotected;},
  async prepare(args){record('prepare',args);return prepare??{...unprotected,state:'ready',ticket:{operationId:args.operationId,objectId:'object_1',expectedObjectRevision:7}};},
  async finalize(args){record('finalize',args);return finalize??complete;},async resumeAfterUnlock(id){record('resume',id);return complete;},cancel(){throw Error('Never cancel a submitted request');}};
 const actions=createCanvasObjectSecurityActions({api,accountId:'owner',objectId:'object_1',isCurrent:()=>current,vault,...(transitionAvailable?{nativeTransition:transition}:{})});
 return{actions,calls,transition,vault,object,setStatus:value=>{sourceStatus=value;},changeAccount:()=>{current=false;}};
}
test('native protect follows fresh owner/capability, prepare, original Vault request, and final device proof',async()=>{
 const f=fixture();assert.deepEqual(await f.actions.protect('synthetic-password','operation_1'),complete);
 const prepare=f.calls.find(c=>c.name==='prepare'),write=f.calls.find(c=>c.name==='protect'),final=f.calls.find(c=>c.name==='finalize');
 assert.deepEqual(prepare.args,{objectId:'object_1',expectedObjectRevision:7,operationId:'operation_1'});
 assert.deepEqual(write.args,['object_1',7,'synthetic-password',{requestId:'operation_1'}]);
 assert.equal(final.args.ticket.operationId,'operation_1');assert.equal(f.calls[0].name,'canvas.objects.get');assert.equal(f.calls[0].args.expectedAccountId,'owner');
});

test('a retry opened through alias B preserves the original trusted target A and operation identity',async()=>{
 const original={objectId:'object_original',ownerId:'owner',revision:3,binding:{providerId:'soty.notes',ref:'source_1'}};
 const ticket={objectId:'object_original',expectedObjectRevision:3,operationId:'original_operation'};
 const f=fixture({prepare:{...pending,state:'ready',ticket},objects:{object_original:original}});
 assert.deepEqual(await f.actions.protect('synthetic-password','current_ui_operation'),complete);
 assert.deepEqual(f.calls.find(c=>c.name==='protect').args,['object_original',3,'synthetic-password',{requestId:'original_operation'}]);
 assert.equal(f.calls.filter(c=>c.name==='canvas.objects.get').at(-1).args.objectId,'object_original');
});
test('a changed or foreign original alias cannot retarget a conversion to another Source',async()=>{
 for(const original of [{objectId:'object_original',ownerId:'owner',revision:3,binding:{providerId:'soty.notes',ref:'another_source'}},{objectId:'object_original',ownerId:'another',revision:3,binding:{providerId:'soty.notes',ref:'source_1'}}]){
  const f=fixture({prepare:{...pending,state:'ready',ticket:{objectId:'object_original',expectedObjectRevision:3,operationId:'old_operation'}},objects:{object_original:original}});
  await assert.rejects(f.actions.protect('synthetic-password','old_operation'));assert.equal(f.calls.some(c=>c.name==='protect'),false);
 }
});
test('unsupported Native capability and unresolved local preparation never submit a conversion',async()=>{
 const disabled=fixture({capability:false});await assert.rejects(disabled.actions.protect('password','op'),error=>error.code==='vault_conversion_required');assert.equal(disabled.calls.some(c=>c.name==='prepare'||c.name==='protect'),false);
 const held=fixture({prepare:{...pending,state:'pending'}});assert.deepEqual(await held.actions.protect('password','op'),{...pending,state:'pending'});assert.equal(held.calls.some(c=>c.name==='protect'),false);
});
test('lost ACK remains uncertain until the Native coordinator proves completion',async()=>{
 const f=fixture({protect:async()=>{throw reject('network_timeout');},finalize:pending});assert.deepEqual(await f.actions.protect('password','same_op'),pending);
 assert.equal(f.calls.find(c=>c.name==='finalize').args.failure.kind,'transport-uncertain');
 const confirmed=fixture({protect:async()=>{throw reject('network_timeout');}});assert.deepEqual(await confirmed.actions.protect('password','same_op'),complete,'fresh coordinator proof can recover a lost response');
});
test('only a known pre-mutation rejection plus verified unprotected state returns the rejection',async()=>{
 const f=fixture({protect:async()=>{throw reject('vault_format2_migration_required');},finalize:unprotected});await assert.rejects(f.actions.protect('password','op'),error=>error.code==='vault_format2_migration_required');
 assert.equal(f.calls.find(c=>c.name==='finalize').args.failure.kind,'server-rejected');
 const unsafe=fixture({protect:async()=>{throw reject('vault_format2_migration_required');},finalize:pending});assert.deepEqual(await unsafe.actions.protect('password','op'),pending);
});
test('already encrypted Native data unlocks for local recovery instead of creating another Source',async()=>{
 const f=fixture({status:{locked:true,protection:{kind:'password'},capabilities:{nativeConversion:true}}});assert.deepEqual(await f.actions.protect('password','old_op'),complete);
 assert.equal(f.calls.some(c=>c.name==='unlock'),true);assert.equal(f.calls.some(c=>c.name==='resume'),true);assert.equal(f.calls.some(c=>c.name==='prepare'||c.name==='protect'),false);
});
test('one protection submission uses the entered password for actual local creation recovery and closes its temporary grant',async()=>{
 const f=fixture({finalize:{server:'protected',device:'pending',protectionComplete:false,reason:'native_password_required_for_creation_recovery'}});
 assert.deepEqual(await f.actions.protect('synthetic-password','stable_conversion'),complete);
 assert.deepEqual(f.calls.filter(value=>['protect','unlock','resume','relock'].includes(value.name)).map(value=>value.name),['protect','unlock','resume','relock']);
 assert.deepEqual(f.calls.find(value=>value.name==='unlock').args,['object_1','synthetic-password']);
 assert.equal(f.calls.find(value=>value.name==='relock').args[1].scope,'device');
});
test('failed local recovery closes the temporary grant and never reports full protection',async()=>{
 const f=fixture({finalize:{server:'protected',device:'pending',protectionComplete:false,reason:'native_password_required_for_creation_recovery'}});
 f.transition.resumeAfterUnlock=async()=>{throw reject('vault_local_quota');};
 await assert.rejects(f.actions.protect('synthetic-password','same_conversion'),{code:'vault_local_quota'});
 assert.equal(f.calls.filter(value=>value.name==='protect').length,1);assert.equal(f.calls.filter(value=>value.name==='relock').length,1);
});
test('a new temporary unlock receives a fresh device relock request rather than replaying an earlier relock receipt',async()=>{
 const f=fixture({status:{locked:true,protection:{kind:'password'},capabilities:{nativeConversion:true}}});
 await f.actions.protect('synthetic-password','same_conversion');await f.actions.protect('synthetic-password','same_conversion');
 const relocks=f.calls.filter(value=>value.name==='relock');assert.equal(relocks.length,2);assert.notEqual(relocks[0].args[1].requestId,relocks[1].args[1].requestId);
 assert.equal(f.calls.filter(value=>value.name==='protect').length,0,'resuming a protected Source never converts it again');
});
test('placement protection requires a fresh confirmed password barrier',async()=>{
 const f=fixture({native:false});assert.deepEqual(await f.actions.protect('password','op'),complete);assert.equal(f.calls.some(c=>c.name==='prepare'),false);
 const missing=fixture({native:false,protect:async()=>({current:{revision:1}})});await assert.rejects(missing.actions.protect('password','op'),error=>error.code==='vault_protection_unconfirmed');
});
test('foreign owner and stale account cannot reach sensitive actions; pending Native data cannot rotate',async()=>{
 const foreign=fixture({owner:'another'});await assert.rejects(foreign.actions.relock('all','op'),error=>error.code==='vault_owner_required');assert.equal(foreign.calls.some(c=>c.name==='relock'),false);
 const stale=fixture();stale.changeAccount();await assert.rejects(stale.actions.protect('password','op'),error=>error.code==='vault_account_changed');assert.equal(stale.calls.length,0);
 const unresolved=fixture();await assert.rejects(unresolved.actions.rotate('old-password','new-password','op'),error=>error.code==='vault_local_draft_pending');assert.equal(unresolved.calls.some(c=>c.name==='rotate'),false);
});

test('known Native without its local coordinator is explicitly unfinished and cannot protect or rotate',async()=>{
 const f=fixture({transitionAvailable:false,status:{locked:true,protection:{kind:'password'},capabilities:{nativeConversion:true}}}),status=await f.actions.status();
 assert.equal(status.device,'pending');assert.equal(status.protectionComplete,false);assert.equal(status.reason,'native_coordinator_unavailable');assert.equal(status.capabilities.nativeConversion,false);
 await assert.rejects(f.actions.protect('synthetic-password','stable_protect_op'),error=>error.code==='native_coordinator_unavailable');
 await assert.rejects(f.actions.rotate('old-password','new-password'),error=>error.code==='native_coordinator_unavailable');
 assert.equal(f.calls.some(c=>['protect','rotate','unlock','prepare','reconcile'].includes(c.name)),false);
});

test('ordinary placement protection stays available without a Native coordinator and rotation delegates its durable ID',async()=>{
 const f=fixture({native:false,transitionAvailable:false});assert.deepEqual(await f.actions.protect('password','same_protect_op'),complete);
 assert.equal(f.calls.find(c=>c.name==='protect').args[3].requestId,'same_protect_op','protection still owns its explicit transition identity');
 await f.actions.rotate('old-password','new-password');const args=f.calls.find(c=>c.name==='rotate').args;
 assert.deepEqual(args,['object_1',{oldPassword:'old-password',newPassword:'new-password'}]);assert.equal(Object.hasOwn(args[1],'requestId'),false);
});

test('an existing server password grant cannot label a missing local crypto session as open',async()=>{
 const f=fixture({status:{locked:false,protection:{kind:'password'},capabilities:{nativeConversion:true,placementProtection:false}}});
 f.vault.isUnlocked=()=>false;assert.equal((await f.actions.status()).locked,true,'An unavailable local key must remain visibly closed despite a server grant');
 f.vault.isUnlocked=()=>true;assert.equal((await f.actions.status()).locked,false);
});
