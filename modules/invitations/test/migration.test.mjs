import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {mkdtempSync,copyFileSync,rmSync,realpathSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,dirname,basename} from 'node:path';
import {createInvitationsService as createV1} from './support/server-v1.mjs';
import {createInvitationsService} from '../server.mjs';

const appId='app-'+'9'.repeat(32),owner={accountId:'owner',deviceId:'owner_device'},member={accountId:'member',deviceId:'member_device'},another={accountId:'another',deviceId:'another_device'};
const code=value=>error=>{assert.equal(error.code,value);return true;};
const baseTables=['policies','links','requests','members','commands'];
function snapshot(path){const db=new DatabaseSync(path);try{return {version:db.prepare('PRAGMA user_version').get().user_version,
 tables:Object.fromEntries(baseTables.map(name=>[name,db.prepare('SELECT * FROM '+name+' ORDER BY rowid').all().map(row=>({...row}))]))};}finally{db.close();}}
function fixture(t){
 const parent=realpathSync(tmpdir()),folder=mkdtempSync(join(parent,'soty-invite-migration-')),path=join(folder,'invitations.sqlite'),backup=join(folder,'before-v2.sqlite');let service,serial=0;
 const options={databasePath:path,clock:()=>1900000000000,actorActive:()=>true,withTarget:({appId:id},callback)=>{assert.equal(id,appId);return callback({appId,ownerId:owner.accountId,title:'Old app'});},ownedTargets:actor=>actor.accountId===owner.accountId?[appId]:[]};
 t.after(()=>{service?.close();const actual=realpathSync(folder);assert.equal(dirname(actual).toLowerCase(),parent.toLowerCase());assert.ok(basename(actual).startsWith('soty-invite-migration-'));rmSync(actual,{recursive:true,force:true});});
 const call=(actor,op,args={})=>service.execute({actor,op:'invitations.'+op,args:{expectedAccountId:actor.accountId,...args}}),mutate=(actor,op,args)=>call(actor,op,{requestId:'command_'+(++serial),...args});
 service=createV1(options);mutate(owner,'configure',{appId,expectedRevision:0,enabled:true,mode:'direct',description:'Old policy',fields:[]});
 const link=mutate(owner,'issue',{appId,label:'Old stable link',expiresAt:1900001000000,maxUses:10}).link;
 const approved=mutate(member,'submit',{token:link.token,formRevision:1,answers:{},consent:true});service.close();service=null;copyFileSync(path,backup);
 return {path,backup,options,link,approved,call,mutate,get service(){return service;},open(factory,extra={}){service?.close();service=factory({...options,...extra});return service;},close(){service?.close();service=null;}};
}

test('existing v1 requires explicit migration, preserves every old row, and a v1 backup remains a verified rollback',t=>{
 const f=fixture(t),before=snapshot(f.path);assert.equal(before.version,1);
 assert.throws(()=>f.open(createInvitationsService),code('invitation_migration_required'));assert.deepEqual(snapshot(f.path),before);
 f.open(createV1);assert.equal(f.call(member,'status',{token:f.link.token}).state,'approved');assert.equal(f.service.hasAccess({appId,ownerId:owner.accountId,accountId:member.accountId}),true);f.close();
 f.open(createInvitationsService,{allowMigration2:true});assert.equal(snapshot(f.path).version,2);assert.deepEqual(snapshot(f.path).tables,before.tables);
 assert.equal(f.call(owner,'get',{appId}).links[0].token,f.link.token);assert.equal(f.call(member,'status',{token:f.link.token}).request.id,f.approved.request.id);
 const redeemed=f.mutate(another,'submit',{token:f.link.token,formRevision:1,answers:{},consent:true});assert.equal(redeemed.admission.state,'admitted');assert.equal(f.service.hasAccess({appId,ownerId:owner.accountId,accountId:another.accountId}),true);f.close();
 assert.throws(()=>f.open(createV1),code('invitation_schema_unsupported'),'the old reader must fail closed rather than corrupt v2');
 f.open(createInvitationsService);assert.equal(f.call(another,'status',{token:f.link.token}).state,'approved');f.close();
 const restored=createV1({...f.options,databasePath:f.backup});try{assert.equal(restored.preview(f.link.token).title,'Old app');assert.equal(restored.hasAccess({appId,ownerId:owner.accountId,accountId:member.accountId}),true);assert.equal(restored.hasAccess({appId,ownerId:owner.accountId,accountId:another.accountId}),false);}finally{restored.close();}
 assert.deepEqual(snapshot(f.backup),before,'rollback is the closed pre-migration snapshot, not a silent downgrade losing v2 writes');
});

test('unrecognized v1 layout fails without a partial migration',t=>{
 const f=fixture(t),db=new DatabaseSync(f.path);db.exec('CREATE TABLE unexpected_data(value TEXT)');db.close();const before=snapshot(f.path);
 assert.throws(()=>f.open(createInvitationsService,{allowMigration2:true}),code('invitation_store_invalid'));assert.deepEqual(snapshot(f.path),before);
 const inspect=new DatabaseSync(f.path);try{assert.equal(inspect.prepare("SELECT count(*) n FROM sqlite_schema WHERE name='admissions'").get().n,0);assert.equal(inspect.prepare('PRAGMA user_version').get().user_version,1);}finally{inspect.close();}
});
