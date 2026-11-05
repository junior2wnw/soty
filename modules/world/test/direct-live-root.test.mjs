import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {DatabaseSync} from 'node:sqlite';
import {join} from 'node:path';
import {nativeHttpFixture,nativeIdentity,good} from '../../../server/test/support/native-capability-http.mjs';

const id=()=>randomUUID().replaceAll('-','');
const denied=(value,code)=>{assert.equal(value.ok,false);if(code)assert.equal(value.error.code,code);};
async function fixture(t){
 const f=await nativeHttpFixture(t),actors=[];
 for(const label of ['Direct live author','Direct live recipient','Direct live outsider']){const actor=nativeIdentity(label),boot=await f.bootstrap(actor);actors.push({...actor,accountId:boot.accountId,deviceId:boot.deviceId});}
 const [a,b,c]=actors,call=(who,op,args={})=>f.call(who,op,{...(op.startsWith('world.direct.')||op==='platform.live.wait'?{expectedAccountId:who.accountId}:{}),...args});
 const world=async(who,op,args={})=>good(await call(who,'world.'+op,args));
 for(const actor of actors)await world(actor,'profile.get');
 const card=good(await call(b,'card.get')),request=good(await call(a,'contacts.request',{cardId:card.cardId})),relation=good(await call(b,'contacts.accept',{requestId:request.requestId})).relationshipId;
 const thread=(await world(a,'direct.open',{requestId:id(),peerAccountId:b.accountId,expectedRelationshipId:relation})).current,base={threadId:thread.threadId,expectedRelationshipId:relation};
 await world(b,'direct.open',{requestId:id(),peerAccountId:a.accountId,expectedRelationshipId:relation});
 const wait=async(who,cursor,waitMs=0)=>good(await call(who,'platform.live.wait',{watch:[],waitMs,...(cursor?{cursor}:{})}));
 const sql=callback=>{const db=new DatabaseSync(join(f.directory,'world','world.sqlite'));try{return callback(db);}finally{db.close();}};
 return {...f,get app(){return f.app;},a,b,c,relation,thread,base,call,world,wait,sql};
}
async function waiting(f,count){const deadline=Date.now()+3000;while(f.app.locals.platformLive.stats().waiting!==count){assert.ok(Date.now()<deadline,'signed wait must be registered before the mutation');await new Promise(resolve=>setTimeout(resolve,5));}}

test('actual Root signed wait BEFORE → direct send → recipient wake, sender wake and unrelated-account silence',{timeout:15000},async t=>{
 const f=await fixture(t),profile=(await f.world(f.a,'profile.get')).profile;await f.world(f.a,'profile.update',{expectedRevision:profile.revision,audience:'public'});
 const a=await f.wait(f.a),b=await f.wait(f.b),c=await f.wait(f.c),pending=f.wait(f.b,b.cursor,1500);await waiting(f,1);
 const sent=await f.world(f.a,'direct.chat.send',{...f.base,clientId:id(),text:'Direct recipient must learn only that its own inbox changed'}),after=await pending;
 assert.equal(after.changed,true,'an actual receiver wait must wake after the durable message COMMIT');assert.deepEqual(Object.keys(after).sort(),['changed','cursor','ok','reset','schema']);assert.equal((await f.wait(f.a,a.cursor)).changed,true);assert.equal((await f.wait(f.c,c.cursor)).changed,false,'public author profile must not publish private chat activity');
 assert.equal((await f.world(f.b,'direct.get',f.base)).thread.unreadCount,1);assert.equal((await f.world(f.b,'direct.chat.list',f.base)).messages[0].messageId,sent.message.messageId);
});

test('actual Root direct read/remove notify only the real pair; personal preferences and denied mutations stay private',{timeout:15000},async t=>{
 const f=await fixture(t),sent=(await f.world(f.a,'direct.chat.send',{...f.base,clientId:id(),text:'Private body removed before the next read'})).message;
 let a=await f.wait(f.a),b=await f.wait(f.b),c=await f.wait(f.c);await f.world(f.b,'direct.chat.read',{...f.base,throughSeq:sent.seq});assert.equal((await f.wait(f.a,a.cursor)).changed,true,'read receipt belongs to the actual other participant');
 a=await f.wait(f.a);b=await f.wait(f.b);await f.world(f.b,'direct.preferences',{...f.base,pinned:true,muted:true});assert.equal((await f.wait(f.b,b.cursor)).changed,true);assert.equal((await f.wait(f.a,a.cursor)).changed,false,'the peer cannot observe private pinned/muted changes');
 b=await f.wait(f.b);await f.world(f.a,'direct.chat.remove',{...f.base,messageId:sent.messageId});assert.equal((await f.wait(f.b,b.cursor)).changed,true);const removed=(await f.world(f.b,'direct.chat.list',f.base)).messages[0];assert.equal(removed.removed,true);assert.equal(removed.text,'');
 b=await f.wait(f.b);denied(await f.call(f.c,'world.direct.chat.send',{...f.base,clientId:id(),text:'An unrelated actor cannot route by a known thread id'}),'direct_not_found');assert.equal((await f.wait(f.b,b.cursor)).changed,false);assert.equal((await f.wait(f.c,c.cursor)).changed,false);
});

test('failed direct SQLite write emits no recipient success; same intent succeeds and contact revocation prevents later hints/read',{timeout:15000},async t=>{
 const f=await fixture(t),args={...f.base,clientId:'direct_durable_fault_retry',text:'Durable only'},b=await f.wait(f.b),a=await f.wait(f.a);
 f.sql(db=>db.exec("CREATE TRIGGER direct_live_fault BEFORE INSERT ON world_direct_messages BEGIN SELECT RAISE(ABORT,'direct_live_fault'); END"));denied(await f.call(f.a,'world.direct.chat.send',args));assert.equal((await f.wait(f.b,b.cursor)).changed,false);assert.equal((await f.wait(f.a,a.cursor)).changed,false);assert.equal(f.sql(db=>db.prepare('SELECT count(*) n FROM world_direct_messages').get().n),0);f.sql(db=>db.exec('DROP TRIGGER direct_live_fault'));
 await f.world(f.a,'direct.chat.send',args);assert.equal((await f.wait(f.b,b.cursor)).changed,true);await f.call(f.a,'contacts.remove',{relationshipId:f.relation});const afterRemoval=await f.wait(f.b);
 denied(await f.call(f.a,'world.direct.chat.send',{...f.base,clientId:id(),text:'No post-revoke message or hint'}),'direct_not_found');denied(await f.call(f.b,'world.direct.chat.list',f.base),'direct_not_found');assert.equal((await f.wait(f.b,afterRemoval.cursor)).changed,false);
});

test('a revoked real recipient device cannot turn a pending signed wait into a successful direct notification',{timeout:15000},async t=>{
 const f=await fixture(t),phone=nativeIdentity('Direct recipient second device'),start=good(await f.call(phone,'enrollment.start',{label:phone.label,encryptionPublicJwk:phone.encryptionPublicJwk}));
 good(await f.call(f.b,'enrollment.approve',{requestId:start.requestId,wrappedKey:{schema:'test.encrypted',ciphertext:'synthetic'}}));const enrolled=good(await f.call(phone,'enrollment.finish',{requestId:start.requestId,expectedAccountId:f.b.accountId}));Object.assign(phone,{accountId:enrolled.accountId,deviceId:enrolled.deviceId});
 const before=await f.wait(phone),pending=f.call(phone,'platform.live.wait',{expectedAccountId:phone.accountId,watch:[],cursor:before.cursor,waitMs:1500});await waiting(f,1);
 good(await f.call(f.b,'device.revoke',{deviceId:phone.deviceId}));await f.world(f.a,'direct.chat.send',{...f.base,clientId:id(),text:'Only a current recipient device can receive the invalidation'});const result=await pending;denied(result);assert.ok(['live_authentication_required','device_revoked','authentication_required'].includes(result.error.code),result.error.code);
 denied(await f.call(phone,'world.direct.chat.list',f.base),'device_revoked');assert.equal((await f.world(f.b,'direct.get',f.base)).thread.unreadCount,1,'another actual active device of the account retains its ordinary rights');
});
