import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {createMonitoringService} from '../server/index.mjs';
const actor=Object.freeze({accountId:'admin-owner',deviceId:'admin-device'}),scope={kind:'platform'};
async function fixture(t){
 const root=await mkdtemp(join(tmpdir(),'soty-monitoring-admin-module-')),file=join(root,'monitoring.sqlite'),domain=new DatabaseSync(join(root,'canonical.sqlite'));
 domain.exec('CREATE TABLE policy(revision INTEGER,enabled INTEGER,pin TEXT);INSERT INTO policy VALUES(0,1,\'source-a\');CREATE TABLE commands(id TEXT PRIMARY KEY,intent TEXT,revision INTEGER)');let executions=0,prepares=0,hide=false,lose=false,early=false,active=true,inAuthority=false;
 const provider={id:'platform.fixture',label:'Synthetic canonical provider',scope:'platform',fields:[{name:'enabled',label:'Enabled',type:'boolean'}],
 read(){assert.equal(inAuthority,false,'canonical read cannot nest inside Apps/module authority transaction');const row=domain.prepare('SELECT * FROM policy').get();return{revision:row.revision,current:{enabled:row.enabled===1}};},
 prepare(request){prepares++;return{requestId:request.requestId,expectedRevision:request.expectedRevision,enabled:request.input.enabled,pin:domain.prepare('SELECT pin FROM policy').get().pin};},
 execute({intent}){assert.equal(inAuthority,false);executions++;if(early)return{revision:intent.expectedRevision+1};domain.exec('BEGIN IMMEDIATE');try{const old=domain.prepare('SELECT * FROM commands WHERE id=?').get(intent.requestId);if(old){assert.equal(old.intent,JSON.stringify(intent));domain.exec('COMMIT');return;}const row=domain.prepare('SELECT * FROM policy').get();assert.equal(row.revision,intent.expectedRevision);domain.prepare('UPDATE policy SET revision=revision+1,enabled=?').run(Number(intent.enabled));domain.prepare('INSERT INTO commands VALUES(?,?,?)').run(intent.requestId,JSON.stringify(intent),row.revision+1);domain.exec('COMMIT');}catch(error){if(domain.isTransaction)domain.exec('ROLLBACK');throw error;}if(lose){hide=true;throw new Error('lost ACK');}},
 receipt({intent}){assert.equal(inAuthority,false);if(hide)return{state:'unknown'};const row=domain.prepare('SELECT * FROM commands WHERE id=?').get(intent.requestId);if(row){assert.equal(row.intent,JSON.stringify(intent),'retry must preserve original Source pin/metadata');return{state:'committed',revision:row.revision};}return{state:'missing'};}};
 const options={databasePath:file,projectId:'soty',mode:'writer',definitions:[],adminActions:[provider],currentActor:current=>active&&current===actor,withAuthority({actor:current},callback){assert.equal(current,actor);assert.equal(inAuthority,false);inAuthority=true;try{return callback();}finally{inAuthority=false;}}};
 let service=createMonitoringService(options);t.after(async()=>{service.close();domain.close();await rm(root,{recursive:true,force:true});});
 const call=(op,args={})=>service.execute({op,actor,args:{scope,expectedAccountId:actor.accountId,...args}}),command={actionId:provider.id,requestId:'original-admin',expectedRevision:0,input:{enabled:false}};
 return{call,command,domain,counts:()=>({executions,prepares}),hide(value){hide=value;},lose(value){lose=value;},early(value){early=value;},reopen(){service.close();service=createMonitoringService(options);},revoke(){active=false;}};
}
test('canonical admin COMMIT then lost ACK/reopen keeps original prepared intent and verifies receipt',async t=>{
 const f=await fixture(t),description=f.call('monitoring.describe');assert.equal(description.actions[1].right,'admin');assert.deepEqual(description.actions[1].current,{enabled:true});f.lose(true);
 assert.equal(f.call('monitoring.admin.invoke',f.command).state,'unknown');assert.deepEqual(f.counts(),{executions:1,prepares:1});assert.equal(f.call('monitoring.audit.list').entries.length,0);
 f.domain.prepare('UPDATE policy SET pin=?,revision=5').run('source-b');f.reopen();f.hide(false);
 const recovered=f.call('monitoring.admin.invoke',f.command);assert.equal(recovered.state,'committed');assert.equal(recovered.revision,1);assert.equal(recovered.replayed,true);assert.deepEqual(f.counts(),{executions:1,prepares:1});assert.equal(f.call('monitoring.audit.list').entries.length,1);
 assert.equal(f.call('monitoring.admin.invoke',f.command).state,'committed');assert.equal(f.call('monitoring.audit.list').entries.length,1);assert.throws(()=>f.call('monitoring.admin.invoke',{...f.command,input:{enabled:true}}),{code:'monitoring_request_conflict'});
});
test('early adapter ACK cannot purge pending; default retry never repeats unknown/missing execution',async t=>{
 const f=await fixture(t);f.early(true);assert.equal(f.call('monitoring.admin.invoke',f.command).state,'not-applied');assert.equal(f.call('monitoring.audit.list').entries.length,0);
 f.early(false);assert.equal(f.call('monitoring.admin.invoke',f.command).state,'not-applied');assert.equal(f.counts().executions,1,'ordinary retry is receipt-only');
 f.hide(true);assert.equal(f.call('monitoring.admin.invoke',{...f.command,retry:true}).state,'unknown');assert.equal(f.counts().executions,1,'explicit retry cannot override unknown receipt');
 f.hide(false);assert.equal(f.call('monitoring.admin.invoke',{...f.command,retry:true}).state,'committed');assert.equal(f.counts().executions,2);assert.equal(f.call('monitoring.audit.list').entries.length,1);
});
test('stale revision, invalid typed input and revoked scope cannot mutate canonical service',async t=>{
 const f=await fixture(t);assert.throws(()=>f.call('monitoring.admin.invoke',{...f.command,expectedRevision:4}),{code:'monitoring_revision_conflict'});assert.throws(()=>f.call('monitoring.admin.invoke',{...f.command,input:{enabled:false,url:'http://private'}}),{code:'monitoring_invalid_input'});assert.equal(f.counts().executions,0);f.revoke();assert.throws(()=>f.call('monitoring.admin.invoke',f.command),{code:'monitoring_forbidden'});assert.equal(f.domain.prepare('SELECT revision FROM policy').get().revision,0);
});
