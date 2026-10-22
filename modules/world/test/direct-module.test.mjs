import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync,rmSync,realpathSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join,dirname,basename } from 'node:path';
import { migrateWorld } from '../server/schema.mjs';
import { createModel } from '../server/model.mjs';
import { communityOperation } from '../server/communities.mjs';
import { directoryOperation } from '../server/directory.mjs';
import { assertDirectSchema } from '../server/direct-schema.mjs';
import { DIRECT_OPERATIONS,directOperation } from '../server/direct.mjs';
import { signedFixture,PROJECT } from '../../connect/test/support/direct-signed-fixture.mjs';
const denies=(fn,code)=>assert.throws(fn,e=>e.code===code);

function fixture(t) {
  const parent=realpathSync(tmpdir()),dir=mkdtempSync(join(parent,'soty-direct-module-')),file=join(dir,'world.sqlite');
  let db=new DatabaseSync(file);migrateWorld(db,PROJECT);assertDirectSchema(db);
  let m=createModel(db,()=>{}),connect;const actors=new Map();
  const extension={operations:new Set(DIRECT_OPERATIONS),execute({actor,op,args}) {
    actors.set(actor.accountId,actor);
    return connect.withCurrentContacts(actor,view=>{
      db.exec('BEGIN IMMEDIATE');
      try {m.ensurePerson(actor,f.now());const value=directOperation(m,op,args,actor,f.now(),{projectId:PROJECT,relationshipId:peer=>view.relationshipId(peer)});
        db.exec('COMMIT');return value;
      }catch(error){db.exec('ROLLBACK');throw error;}
    });
  }};
  const f=signedFixture(t,[extension]);connect=f.service;
  t.after(()=>{db.close();const actual=realpathSync(dir);assert.equal(dirname(actual),parent);assert.match(basename(actual),/^soty-direct-module-/u);rmSync(actual,{recursive:true});});
  const call=(a,op,args={})=>f.call(a,op,DIRECT_OPERATIONS.includes(op)?{expectedAccountId:a.accountId,...args}:args);
  const raw=(a,op,args={})=>f.raw(a,op,DIRECT_OPERATIONS.includes(op)?{expectedAccountId:a.accountId,...args}:args);
  const open=(a,b,relationshipId,requestId='direct_open_first')=>call(a,'world.direct.open',{peerAccountId:b.accountId,expectedRelationshipId:relationshipId,requestId});
  const args=thread=>({threadId:thread.threadId,expectedRelationshipId:thread.relationshipId});
  const rejected=async(a,op,values,code)=>{const result=await raw(a,op,values);assert.equal(result.ok,false);assert.equal(result.error.code,code);};
  return {...f,call,raw,open,args,rejected,actors,get db(){return db;},get m(){return m;},
    reopen(){db.close();db=new DatabaseSync(file);db.exec('PRAGMA foreign_keys=ON');assertDirectSchema(db);m=createModel(db,()=>{});}};
}
async function pair(t){const f=fixture(t),a=await f.person('Alice'),b=await f.person('Bob'),c=await f.person('Other'),rel=await f.contacts(a,b);return {f,a,b,c,rel};}

test('actual Connect-signed open is unique, does not fabricate peer World profile and has durable receipts/current projection',async t=>{
  const {f,a,b,rel}=await pair(t);assert.equal(f.m.person(b.accountId),undefined);
  const first=await f.open(a,b,rel);assert.equal(first.current.peer.label,null);assert.equal(f.m.person(b.accountId),undefined);
  const second=await f.open(b,a,rel,'direct_open_other');assert.equal(second.current.threadId,first.current.threadId);
  const retry=await f.open(a,b,rel);assert.equal(retry.replayed,true);assert.deepEqual(retry.receipt,first.receipt);
  assert.equal(retry.current.peer.label,'Bob');assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM world_direct_threads').get().n,1);
  f.reopen();assert.deepEqual((await f.open(a,b,rel)).receipt,first.receipt);
  assert.deepEqual(f.db.prepare('PRAGMA foreign_key_check').all(),[]);
});

test('closed args, actor fence, third party and missing authority never read/send a known pair',async t=>{
  const {f,a,b,c,rel}=await pair(t),thread=(await f.open(a,b,rel)).current;
  await f.rejected(c,'world.direct.get',f.args(thread),'direct_not_found');
  await f.rejected(c,'world.direct.chat.send',{...f.args(thread),clientId:'third_client',text:'intrusion'},'direct_not_found');
  await f.rejected(a,'world.direct.get',{...f.args(thread),ownerAccountId:b.accountId},'invalid_arguments');
  await f.rejected(a,'world.direct.get',{...f.args(thread),expectedAccountId:b.accountId},'direct_account_changed');
  await f.rejected(a,'world.direct.open',{peerAccountId:c.accountId,expectedRelationshipId:rel,requestId:'not_friend'},'direct_contact_required');
  f.db.exec('BEGIN');try{denies(()=>directOperation(f.m,'world.direct.get',{expectedAccountId:a.accountId,...f.args(thread)},f.actors.get(a.accountId),f.now(),null),'direct_authority_unavailable');}finally{f.db.exec('ROLLBACK');}
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM world_direct_messages').get().n,0);
});

test('actual contact remove/block/re-add requires explicit open and preserves exact history without reviving old intents',async t=>{
  const {f,a,b,rel}=await pair(t),thread=(await f.open(a,b,rel)).current;
  const sent=await f.call(a,'world.direct.chat.send',{...f.args(thread),clientId:'old_intent',text:'History'});
  const before=f.db.prepare('SELECT * FROM world_direct_messages ORDER BY seq').all();
  await f.call(a,'contacts.remove',{relationshipId:rel});
  await f.rejected(a,'world.direct.chat.list',f.args(thread),'direct_not_found');
  f.advance();const next=await f.contacts(a,b);assert.notEqual(next,rel);
  assert.deepEqual((await f.call(a,'world.direct.list')).threads,[]);
  await f.rejected(a,'world.direct.chat.send',{...f.args(thread),clientId:'old_intent',text:'History'},'direct_not_found');
  await f.rejected(a,'world.direct.open',{peerAccountId:b.accountId,expectedRelationshipId:rel,requestId:'direct_open_first'},'direct_contact_required');
  const reopened=(await f.open(a,b,next,'direct_open_new_relation')).current;
  assert.equal(reopened.threadId,thread.threadId);assert.equal(reopened.revision,2);
  assert.deepEqual(f.db.prepare('SELECT * FROM world_direct_messages ORDER BY seq').all(),before);
  assert.equal((await f.call(a,'world.direct.chat.list',f.args(reopened))).messages[0].messageId,sent.message.messageId);
  await f.rejected(a,'world.direct.chat.send',{...f.args(reopened),clientId:'old_intent',text:'History'},'request_conflict');
  await f.call(b,'contacts.block',{peerAccountId:a.accountId});
  await f.rejected(a,'world.direct.get',f.args(reopened),'direct_not_found');
});

test('send/reply/read/remove use one actor, stable client intent, thread boundaries and tombstones',async t=>{
  const {f,a,b,c,rel}=await pair(t),thread=(await f.open(a,b,rel)).current,base=f.args(thread);
  const sent=await f.call(a,'world.direct.chat.send',{...base,clientId:'client_first',text:'First'});
  assert.equal((await f.call(a,'world.direct.chat.send',{...base,clientId:'client_first',text:'First'})).message.messageId,sent.message.messageId);
  await f.rejected(a,'world.direct.chat.send',{...base,clientId:'client_first',text:'Changed'},'request_conflict');
  const reply=await f.call(b,'world.direct.chat.send',{...base,clientId:'client_reply',text:'Reply',replyTo:sent.message.messageId});
  assert.equal((await f.call(a,'world.direct.get',base)).thread.unreadCount,1);
  assert.equal((await f.call(a,'world.direct.chat.read',{...base,throughSeq:reply.message.seq})).unreadCount,0);
  await f.rejected(b,'world.direct.chat.remove',{...base,messageId:sent.message.messageId},'direct_permission_denied');
  await f.call(a,'world.direct.chat.remove',{...base,messageId:sent.message.messageId});
  const replay=await f.call(a,'world.direct.chat.send',{...base,clientId:'client_first',text:'Never restore'});assert.equal(replay.message.removed,true);assert.equal(replay.message.text,'');
  f.advance();const otherRel=await f.contacts(a,c),other=(await f.open(a,c,otherRel,'direct_other_pair')).current;
  await f.rejected(a,'world.direct.chat.send',{...f.args(other),clientId:'client_cross_reply',text:'Cross',replyTo:reply.message.messageId},'message_not_found');
  const page=await f.call(a,'world.direct.chat.list',{...base,limit:1});assert.equal(page.messages.length,1);assert.equal(page.hasMore,true);
});

test('direct send recovery metadata stays author-only and reads follow the exact current peer',async t=>{
  const {f,a,b,c,rel}=await pair(t),thread=(await f.open(a,b,rel)).current,base=f.args(thread);
  const sent=(await f.call(a,'world.direct.chat.send',{...base,clientId:'read_receipt_one',text:'Disposable private text'})).message;
  assert.equal(sent.clientId,'read_receipt_one');assert.equal(sent.read,false);
  const peerView=(await f.call(b,'world.direct.chat.list',base)).messages[0];
  assert.equal(Object.hasOwn(peerView,'clientId'),false);assert.equal(Object.hasOwn(peerView,'read'),false);
  await f.call(b,'world.direct.chat.read',{...base,throughSeq:sent.seq});f.reopen();
  const read=(await f.call(a,'world.direct.chat.list',base)).messages[0];assert.equal(read.read,true);assert.equal(read.clientId,sent.clientId);
  await f.rejected(c,'world.direct.chat.list',base,'direct_not_found');
  await f.call(b,'contacts.block',{peerAccountId:a.accountId});
  await f.rejected(a,'world.direct.chat.list',base,'direct_not_found');
});

test('direct identifiers never become old community, public directory, roster or group/App authority',async t=>{
  const {f,a,b,rel}=await pair(t),thread=(await f.open(a,b,rel)).current,actor=f.actors.get(a.accountId);
  f.db.exec('BEGIN');try {
    for(const [op,args] of [['world.community.get',{communityId:thread.threadId}],['world.membership.join',{communityId:thread.threadId}],
      ['world.membership.invite',{communityId:thread.threadId,profileId:b.accountId}],['world.membership.transfer',{communityId:thread.threadId,profileId:b.accountId,requestId:'not_transfer'}]])
      denies(()=>communityOperation(f.m,op,args,actor,f.now()),'community_not_found');
    assert.equal(f.m.canAccess(thread.threadId,a.accountId),false);
    assert.equal(f.m.membership(thread.threadId,a.accountId),undefined);
    assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM memberships').get().n,0);
    const visible=directoryOperation(f.m,'world.directory.resolve',{expectedAccountId:a.accountId,entities:[{kind:'community',id:thread.threadId}]},actor);
    assert.equal(visible.items[0].available,false);
    assert.equal(f.db.prepare("SELECT COUNT(*) AS n FROM world_directory WHERE kind='community'").get().n,0);
  }finally{f.db.exec('ROLLBACK');}
});

test('filtering precedes cursor/limit and cursor cannot cross actor/device',async t=>{
  const {f,a,b,c,rel}=await pair(t),first=(await f.open(a,b,rel)).current;f.advance();
  const secondRel=await f.contacts(a,c);await f.open(a,c,secondRel,'direct_pair_second');
  const page=await f.call(a,'world.direct.list',{limit:1});assert.equal(page.threads.length,1);assert.ok(page.nextCursor);
  await f.rejected(c,'world.direct.list',{cursor:page.nextCursor},'invalid_direct_cursor');
  await f.call(a,'contacts.remove',{relationshipId:rel});
  const after=await f.call(a,'world.direct.list',{limit:1});assert.equal(after.threads.length,1);assert.notEqual(after.threads[0].threadId,first.threadId);assert.equal(after.nextCursor,null);
});

test('DDL invariants reject third author/read and identity mutation; admission detects changed guards',async t=>{
  const {f,a,b,c,rel}=await pair(t),thread=(await f.open(a,b,rel)).current;
  assert.throws(()=>f.db.prepare(`INSERT INTO world_direct_messages(id,thread_id,author_id,relationship_id,client_id,text,created_at) VALUES ('bad_message',?,?,?,?,?,1)`)
    .run(thread.threadId,c.accountId,rel,'bad_client','bad'),/direct_author_denied/u);
  assert.throws(()=>f.db.prepare('INSERT INTO world_direct_read_state(thread_id,account_id) VALUES (?,?)').run(thread.threadId,c.accountId),/direct_reader_denied/u);
  assert.throws(()=>f.db.prepare('UPDATE world_direct_threads SET account_high=? WHERE id=?').run(c.accountId,thread.threadId),/direct_pair_immutable/u);
  assertDirectSchema(f.db);f.db.exec('DROP TRIGGER world_direct_message_author');
  denies(()=>assertDirectSchema(f.db),'world_direct_schema_mismatch');
});
