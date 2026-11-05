import test from 'node:test';
import assert from 'node:assert/strict';
import {createVaultCrypto,newPayload,context} from './crypto.mjs';
import {sealPasswordContainer,sealPasswordContainerWithKeys,readPasswordContainerWithKey} from '../canvas-objects/password-container.mjs';

test('async scrypt leaves the event loop responsive and rejects excess work at the configured bounded queue',async()=>{
 const crypto=createVaultCrypto({parallelism:1,queueLimit:0}),row={owner_id:'owner',vault_id:'vault_pool',revision:1,kind:'native'},scope=context(row),body=newPayload('native',{contentType:'text/plain',bytesBase64:Buffer.from('pool content').toString('base64url')});let keys,ticked=false;
 try{const tick=new Promise(resolve=>setTimeout(()=>{ticked=true;resolve();},0)),pending=crypto.create(scope,body,'bounded-password');await assert.rejects(crypto.create(scope,body,'bounded-password'),{code:'vault_busy'});keys=await pending;assert.equal(ticked,true,'the timer must fire BEFORE the KDF returns');await tick;const opened=await crypto.open({...row,container_json:JSON.stringify(keys.container)},'bounded-password');try{assert.equal(opened.body.content.bytesBase64,body.content.bytesBase64);}finally{opened.dataKey.fill(0);opened.wrappingKey.fill(0);}}
 finally{keys?.dataKey.fill(0);keys?.wrappingKey.fill(0);crypto.close();}
 await assert.rejects(crypto.create(scope,body,'bounded-password'),{code:'vault_closed'});
 let controlTicked=false;const controlTick=new Promise(resolve=>setTimeout(()=>{controlTicked=true;resolve();},0));await Promise.resolve(sealPasswordContainer({context:scope,password:'bounded-password',bytes:Buffer.from('intentional sync control')}));assert.throws(()=>assert.equal(controlTicked,true),assert.AssertionError,'the same timer oracle detects the deliberately blocking old primitive');await controlTick;
});

test('placement AEAD refuses rebinding object or source ref and write version changes both payload and wrapper authentication',async()=>{
 const crypto=createVaultCrypto(),row={owner_id:'owner',vault_id:'vault_binding',revision:1,kind:'placement',object_id:'object_a',binding_json:JSON.stringify({providerId:'soty.apps',ref:'widget_a'})};let keys,next;
 try{keys=await crypto.create(context(row),newPayload('placement'),'binding-password');const serialized=JSON.stringify(keys.container);
  for(const altered of [{...row,object_id:'object_b'},{...row,binding_json:JSON.stringify({providerId:'soty.apps',ref:'widget_b'})},{...row,revision:2}])await assert.rejects(crypto.open({...altered,container_json:serialized},'binding-password'),{code:'password_container_unavailable'});
  next=crypto.reseal({...row,revision:2},keys.body,keys.wrappingKey,keys.container.key.kdf);
  const mixed={...next.container,key:keys.container.key};assert.throws(()=>readPasswordContainerWithKey({container:mixed,context:context({...row,revision:2}),dataKey:keys.dataKey}),{code:'password_container_unavailable'});
  assert.throws(()=>sealPasswordContainerWithKeys({context:context(row),bytes:Buffer.from('data'),dataKey:keys.dataKey,wrappingKey:keys.wrappingKey,keyDerivation:{...keys.container.key.kdf,p:1}}),{code:'password_container_invalid'});
 }finally{keys?.dataKey.fill(0);keys?.wrappingKey.fill(0);next?.dataKey.fill(0);next?.wrappingKey.fill(0);crypto.close();}
});
