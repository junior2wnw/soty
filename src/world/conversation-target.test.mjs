import test from 'node:test';
import assert from 'node:assert/strict';
import { createCommunityConversation, createDirectConversation } from './conversation-target.mjs';
import { createChatDraftStore } from './chat-state.mjs';

const thread = relationshipId => ({ schema:'soty.world-direct.v1',threadId:'direct-fixture',revision:1,relationshipId,
  peer:{accountId:'peer-fixture',label:null,avatarRevision:null},unreadCount:0,preferences:{muted:false,pinned:false},createdAt:1 });
const api = reply => ({ calls:[], async request(op,args) { this.calls.push({op,args});return reply(op,args); } });

test('direct requests keep the originally selected account, peer thread and relationship for every action', async () => {
  const service=api(() => ({messages:[]}));
  const target=createDirectConversation({api:service,thread:thread('rel-one'),accountId:'account-one',peerLabel:'Друг'});
  for(const action of ['list','send','remove','read']) await target.request(action,{expectedAccountId:'foreign',threadId:'other',expectedRelationshipId:'rel-two'});
  for(const call of service.calls) {
    assert.ok(call.op.startsWith('world.direct.chat.'));assert.equal(call.args.expectedAccountId,'account-one');
    assert.equal(call.args.threadId,'direct-fixture');assert.equal(call.args.expectedRelationshipId,'rel-one');
  }
  assert.equal(target.canModerate,false);assert.equal(target.avatarContext,undefined);assert.equal(target.name,'Друг');
  await assert.rejects(target.request('invite'),{code:'unsupported_chat_action'});
});

test('a relationship replacement is rejected and cannot consume or send its predecessor draft', async () => {
  const service=api(() => ({thread:thread('rel-two')})), updates=[];
  const old=createDirectConversation({api:service,thread:thread('rel-one'),accountId:'owner',onUpdate:value=>updates.push(value)});
  await assert.rejects(old.refresh(),{code:'direct_not_found'});assert.equal(old.record.relationshipId,'rel-one');assert.deepEqual(updates,[]);
  const next=createDirectConversation({api:service,thread:thread('rel-two'),accountId:'owner'});
  const values=new Map(),storage={getItem:k=>values.get(k)??null,setItem:(k,v)=>values.set(k,v),removeItem:k=>values.delete(k)};
  const drafts=createChatDraftStore(storage,()=> 'client-fixture'); drafts.edit('owner',old.draftKey,'Не отправлять в новое знакомство');
  const pending=drafts.beginSend('owner',old.draftKey,'Не отправлять в новое знакомство');
  assert.equal(drafts.read('owner',next.draftKey).pending,null);assert.equal(drafts.read('owner',next.draftKey).text,'');
  assert.equal(drafts.read('owner',old.draftKey).pending.clientId,pending.clientId);
});

test('direct history permits minimal absent-profile identity without manufacturing a server profile', async () => {
  const service=api(()=>({messages:[{messageId:'one',author:{profileId:'peer-fixture',displayName:null,avatarColor:null,revision:null},text:'Привет'}]}));
  const target=createDirectConversation({api:service,thread:thread('rel-one'),accountId:'owner',peerLabel:'Друг'});
  const result=await target.request('list');assert.equal(result.messages[0].author.displayName,'Друг');
  assert.equal(target.record.peer.label,null);assert.equal(service.calls.length,1);
});

test('community target retains its actual operations, roles and current membership semantics', async () => {
  const community={communityId:'group-one',name:'Комната',memberCount:2,permissions:{canWrite:true,canModerate:true},membership:{state:'active',muted:false,pinned:false}};
  const service=api(()=>({community})), target=createCommunityConversation({api:service,community});
  await target.request('list',{communityId:'foreign',limit:1});assert.equal(service.calls[0].op,'world.chat.list');assert.equal(service.calls[0].args.communityId,'group-one');
  assert.equal(target.canModerate,true);assert.equal(target.draftKey,'group-one');assert.equal(target.avatarContext,'group-one');
  await target.setPreference('muted',true);assert.equal(service.calls[1].op,'world.membership.preferences');
  assert.throws(()=>target.acceptCommunity({...community,communityId:'foreign'}),{code:'community_membership_required'});
  assert.throws(()=>target.acceptCommunity({...community,membership:{state:'banned'}}),{code:'community_membership_required'});
});

test('late metadata and preference acknowledgements cannot update a departed screen or account', async () => {
  let resolve, current=true; const updates=[];
  const service=api(()=>new Promise(yes=>{resolve=yes;}));
  const target=createDirectConversation({api:service,thread:thread('rel-one'),accountId:'owner',isCurrent:()=>current,onUpdate:next=>updates.push(next)});
  const pending=target.refresh();current=false;resolve({thread:thread('rel-one')});
  await assert.rejects(pending,{code:'conversation_changed'});assert.deepEqual(updates,[]);
  current=true;const preference=target.setPreference('muted',true);current=false;resolve({thread:thread('rel-one')});
  await assert.rejects(preference,{code:'conversation_changed'});assert.deepEqual(updates,[]);
});

for (const kind of ['direct','community']) test(`${kind} preference acknowledgement preserves a later read acknowledgement`, async () => {
  let resolve;
  const before=kind==='direct'?{...thread('rel-one'),unreadCount:7}:{communityId:'group-one',revision:1,name:'Комната',unreadCount:7,
    permissions:{canWrite:true,canModerate:false},membership:{revision:1,state:'active',pinned:false,muted:false}};
  const service=api(()=>new Promise(yes=>{resolve=yes;}));
  const target=kind==='direct'?createDirectConversation({api:service,thread:before,accountId:'owner'}):createCommunityConversation({api:service,community:before});
  const accepted=structuredClone(before);
  if(kind==='direct') accepted.preferences.pinned=true; else {accepted.membership.pinned=true;accepted.membership.revision=2;}
  const pending=target.setPreference('pinned',true);await new Promise(resolve=>setImmediate(resolve));
  target.setUnread(0);
  resolve(kind==='direct'?{thread:accepted}:{community:accepted});await pending;
  assert.equal(target.record.unreadCount,0);assert.equal(target.preferences.pinned,true);
});

test('an older preference snapshot cannot revert newer community metadata or membership permissions', async () => {
  let resolve;
  const before={communityId:'group-one',revision:1,name:'Прежнее имя',unreadCount:0,permissions:{canWrite:true,canModerate:true},
    membership:{revision:1,state:'active',pinned:false,muted:false}};
  const target=createCommunityConversation({community:before,api:api(()=>new Promise(yes=>{resolve=yes;}))});
  const pending=target.setPreference('pinned',true);await new Promise(resolve=>setImmediate(resolve));
  target.acceptCommunity({...before,revision:3,name:'Новое имя',permissions:{canWrite:true,canModerate:false},membership:{...before.membership,revision:4}});
  resolve({community:{...before,membership:{...before.membership,revision:2,pinned:true}}});await pending;
  assert.equal(target.name,'Новое имя');assert.equal(target.record.revision,3);assert.equal(target.record.membership.revision,4);assert.equal(target.canModerate,false);
});
