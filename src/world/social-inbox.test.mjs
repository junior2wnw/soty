import test from 'node:test';
import assert from 'node:assert/strict';
import {createSocialInbox} from './social-inbox.mjs';
const thread=(id,unreadCount=1)=>({threadId:'direct_'+id,relationshipId:'relationship_'+id,revision:1,unreadCount,preferences:{muted:false}});
const tick=()=>new Promise(resolve=>setImmediate(resolve));
function fixture({request,enabled=true,onDirect}={}){
 const calls=[],events={direct:[],communities:[],contacts:[],errors:[]};let active=true;
 const inbox=createSocialInbox({api:{async request(op,args){calls.push({op,args});return request?request(op,args):op==='world.direct.list'?{threads:[thread(1)],nextCursor:null}:op==='world.community.list'?{communities:[]}:{requests:{incoming:[{}]}};}},
  accountId:'owner',isCurrent:()=>active,directEnabled:()=>enabled,
  onDirect:(value,epoch)=>{events.direct.push(value);return onDirect?.(value,epoch);},onCommunities:value=>events.communities.push(value),onContacts:value=>events.contacts.push(value),onFailure:(kind,error)=>events.errors.push({kind,error})});
 return{inbox,calls,events,retire(){active=false;inbox.dispose();}};
}
test('global inbox reads current Sources with their original contracts, independently of a Messages screen',async()=>{
 const f=fixture();await f.inbox.refresh();assert.equal(f.events.direct[0][0].unreadCount,1);assert.deepEqual(f.events.contacts,[1]);
 assert.deepEqual(f.calls.find(c=>c.op==='world.direct.list').args,{expectedAccountId:'owner',limit:60});
 assert.deepEqual(f.calls.find(c=>c.op==='world.community.list').args,{});assert.deepEqual(f.calls.find(c=>c.op==='contacts.list').args,{});
});
test('a held conversation read cannot delay the independent incoming-contact signal',async()=>{
 let release;const f=fixture({request:async op=>op==='world.direct.list'?new Promise(resolve=>{release=resolve;}):op==='contacts.list'?{requests:{incoming:[{},{}]}}:{communities:[]}});
 const refreshing=f.inbox.refresh();await tick();assert.deepEqual(f.events.contacts,[2]);assert.equal(f.events.direct.length,0);
 release({threads:[],nextCursor:null});await refreshing;
});
test('all 73 conversations survive pagination; repeated cursors fail instead of claiming completeness',async()=>{
 const f=fixture({request:async(op,args)=>op==='world.direct.list'?{threads:Array.from({length:args.cursor?13:60},(_,i)=>thread(i+(args.cursor?60:0))),nextCursor:args.cursor?null:'page2'}:op==='contacts.list'?{requests:{incoming:[]}}:{communities:[]}});
 await f.inbox.refresh();assert.equal(f.events.direct[0].length,73);
 const broken=fixture({request:async op=>op==='world.direct.list'?{threads:[],nextCursor:'same'}:op==='contacts.list'?{requests:{incoming:[]}}:{communities:[]}});
 await assert.rejects(broken.inbox.refresh(),{code:'inbox_cursor_repeated'});assert.equal(broken.events.direct.length,0);assert.equal(broken.inbox.pending,false);
});
test('coalescing retains a wake-up arriving during the second pass without overlapping Direct reads',async()=>{
 const releases=[];let active=0,peak=0;
 const f=fixture({request:async op=>{if(op!=='world.direct.list')return op==='contacts.list'?{requests:{incoming:[]}}:{communities:[]};active++;peak=Math.max(peak,active);return new Promise(resolve=>releases.push(()=>{active--;resolve({threads:[thread(releases.length)],nextCursor:null});}));}});
 const first=f.inbox.refresh();f.inbox.refresh();releases[0]();await tick();f.inbox.refresh();releases[1]();await first;await tick();assert.equal(releases.length,3);releases[2]();await tick();assert.equal(f.events.direct.length,3);assert.equal(peak,1);
});
test('retiring an account discards every late Source result and error',async()=>{
 const held=[];const f=fixture({request:async()=>new Promise(resolve=>held.push(resolve))});const refreshing=f.inbox.refresh();f.retire();
 for(const resolve of held)resolve({threads:[thread('old-account')],nextCursor:null,communities:[],requests:{incoming:[{}]}});
 await refreshing;assert.equal(Object.values(f.events).flat().length,0);assert.equal(f.inbox.pending,false);
});
test('an offline Direct read preserves the last successful projection while other Sources still update',async()=>{
 let offline=false;const f=fixture({request:async op=>{if(op==='world.direct.list'){if(offline)throw Object.assign(new Error('offline'),{code:'network_unavailable'});return{threads:[thread(1,3)],nextCursor:null};}return op==='contacts.list'?{requests:{incoming:[{}]}}:{communities:[]};}});
 await f.inbox.refresh();offline=true;await assert.rejects(f.inbox.refresh(),{code:'network_unavailable'});
 assert.equal(f.events.direct.length,1);assert.equal(f.events.direct[0][0].unreadCount,3);assert.equal(f.events.contacts.length,2);
});
test('old profiles without Direct support still refresh communities and contacts',async()=>{
 const f=fixture({enabled:false});await f.inbox.refresh();assert.equal(f.calls.some(c=>c.op==='world.direct.list'),false);assert.deepEqual(f.events.direct,[[]]);assert.deepEqual(f.events.contacts,[1]);
});
