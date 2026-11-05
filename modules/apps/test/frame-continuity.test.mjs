import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {randomBytes} from 'node:crypto';
import {createFrameContinuity,frameContinuityDdl,hasFrameContinuitySchema,FRAME_CONTINUITY_STORAGE} from '../server/frame-continuity.mjs';
import {migrateAppsSchema,inspectAppsSchema} from '../server/schema.mjs';
const actor={accountId:'account-a',deviceId:'device-a'};
const source={id:'soty.standard-workspace',version:2,digest:'23061fc95610e3011ec42b3ecf229308bce44b0a1268408ca9b6ee138c98f7e9'};
const record=(char='a')=>({handleHash:char.repeat(64),rootInstanceId:'widget-11111111-1111-4111-8111-111111111111',context:{reference:{id:char.repeat(43)},rootPrincipal:{...actor},appId:'app-one',
 profileDigest:'1'.repeat(64),policyEpoch:1,entry:{domainId:'domain-one',origin:'https://source.example'},target:{revision:2,digest:'2'.repeat(64)},sourceProfile:source,resource:{nativeId:'personal',resourceId:'reading'},humanPrincipal:{issuer:'https://root.example',subject:'account-a'},expiresAt:300000}});
function db(t){const d=new DatabaseSync(':memory:');t.after(()=>d.close());d.exec('CREATE TABLE local_apps(id TEXT PRIMARY KEY);INSERT INTO local_apps VALUES(\'app-one\');CREATE TABLE apps_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);');d.exec(frameContinuityDdl());d.prepare('INSERT INTO apps_meta VALUES(?,?)').run('frame_continuity',FRAME_CONTINUITY_STORAGE);return d;}
const store=(d,id)=>createFrameContinuity({db:d,clock:()=>1000,hostId:id.repeat(43)});
function initial(t){const d=db(t),a=store(d,'A'),r=record();a.attach(r,{family:{frameId:r.context.reference.id},entryPath:'/embed'});return{d,a,r,id:r.context.reference.id};}
test('fresh authorization after host restart uses only passive binding; no cookie/ACK/Actor DTO at rest',t=>{
 const{d,a,r,id}=initial(t);assert.throws(()=>a.recover(actor,'app-one',id,r.handleHash),/app_scoped_context_closed/);
 const b=store(d,'B'),claim=b.recover(actor,'app-one',id,r.handleHash),next=record('b');b.prepare(next,id,claim,'/embed');b.assertRecord(next);b.promote(next);b.assertRecord(next);
 const persisted=d.prepare('SELECT * FROM app_frame_continuity').get();assert.equal(persisted.host_id,'B'.repeat(43));assert.equal(persisted.revision,2);
 const metadata=JSON.parse(persisted.binding_json);assert.equal(metadata.resourceDigest.length,64);assert.equal(metadata.rootInstanceId,r.rootInstanceId);
 for(const key of ['actor','rootPrincipal','humanPrincipal','cookie','session','sourceSession','ready','resource'])assert.equal(Object.hasOwn(metadata,key),false,key);
 assert.throws(()=>a.assertRecord(r),/app_scoped_context_closed/);
 assert.throws(()=>store(d,'C').recover(actor,'app-one',id,r.handleHash),/app_scoped_context_closed/,'a prior lease never claims its successor after a second restart');
 store(d,'C').recover(actor,'app-one',id,next.handleHash);
});
test('explicit close survives restart; wrong account/device denied; changed source/resource/policy/entry denied',t=>{
 const{d,a,r,id}=initial(t),b=store(d,'B');
 for(const change of [{accountId:'account-b'},{deviceId:'device-b'}])assert.throws(()=>b.recover({...actor,...change},'app-one',id,r.handleHash),/app_scoped_context_closed/);
 for(const badHash of [undefined,'f'.repeat(64)])assert.throws(()=>b.recover(actor,'app-one',id,badHash),/app_scoped_context_closed/);
 for(const breakIt of [x=>x.context.target.revision++,x=>x.context.target.digest='3'.repeat(64),x=>x.context.resource.nativeId='other',x=>x.context.profileDigest='4'.repeat(64),x=>x.context.sourceProfile={...source,digest:'5'.repeat(64)},x=>x.context.policyEpoch++,x=>x.context.entry.domainId='other',x=>x.context.humanPrincipal.subject='other',x=>x.rootInstanceId='main']){
  const next=record('b');breakIt(next);assert.throws(()=>b.prepare(next,id,b.recover(actor,'app-one',id,r.handleHash),'/embed'),/app_scoped_context_closed/);
 }
 a.close(r);assert.throws(()=>b.recover(actor,'app-one',id,r.handleHash),/app_scoped_context_closed/);
});
test('old cleanup cannot close new lease; competing claim/late ACK cannot promote a different lease',t=>{
 const{d,a,r,id}=initial(t),b=store(d,'B'),one=record('b'),two=record('c');const claim=b.recover(actor,'app-one',id,r.handleHash);
 b.prepare(one,id,claim,'/embed');b.prepare(two,id,claim,'/embed');b.promote(one);a.close(r);b.assertRecord(one);
 assert.throws(()=>b.promote(two),/app_scoped_context_closed/);assert.throws(()=>b.prepare(record('d'),id,claim,'/embed'),/app_scoped_context_closed/);
 b.closeLost(actor,'app-one',one.handleHash);assert.throws(()=>store(d,'C').recover(actor,'app-one',id,one.handleHash),/app_scoped_context_closed/);
});
test('closed candidate and failed durable COMMIT never become recovered readiness',t=>{
 const{d,a,r,id}=initial(t),b=store(d,'B'),candidate=record('b');b.prepare(candidate,id,b.recover(actor,'app-one',id,r.handleHash),'/embed');a.close(r);
 assert.throws(()=>b.promote(candidate),/app_scoped_context_closed/);
 const fresh=record('d');d.exec("CREATE TRIGGER reject_frame BEFORE INSERT ON app_frame_continuity BEGIN SELECT RAISE(ABORT,'injected_commit_failure');END;");
 assert.throws(()=>b.attach(fresh,{family:{frameId:fresh.context.reference.id},entryPath:'/embed'}),/injected_commit_failure/);assert.equal(fresh.frameContinuity,undefined);
});
test('empty install auto enables; existing store explicit migration once; known format works with false',t=>{
 const d=new DatabaseSync(':memory:');t.after(()=>d.close());const options={legacyTemplate:'http://{appId}.localhost'};
 migrateAppsSchema(d,options);assert.equal(hasFrameContinuitySchema(d),true);const state=inspectAppsSchema(d);
 d.exec("DROP INDEX app_frame_continuity_owner;DROP INDEX app_frame_continuity_app;DROP TABLE app_frame_continuity;DELETE FROM apps_meta WHERE key='frame_continuity'");
 migrateAppsSchema(d,options);assert.equal(hasFrameContinuitySchema(d),false);assert.equal(inspectAppsSchema(d),state);
 migrateAppsSchema(d,{...options,allowFrameContinuityMigration:true});assert.equal(hasFrameContinuitySchema(d),true);
 migrateAppsSchema(d,{...options,allowFrameContinuityMigration:false});assert.equal(hasFrameContinuitySchema(d),true);assert.equal(inspectAppsSchema(d),state);
 assert.equal(d.prepare('PRAGMA foreign_key_check').get(),undefined);
});
test('strict prior-object-set control refuses added objects; marker/table mismatch and future version fail closed',t=>{
 const d=new DatabaseSync(':memory:');t.after(()=>d.close());migrateAppsSchema(d,{legacyTemplate:'http://{appId}.localhost'});
 const oldKnown=new Set(d.prepare("SELECT name FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'").all().map(x=>x.name).filter(n=>!n.startsWith('app_frame_continuity')));
 const oldReader=()=>assert.ok(d.prepare("SELECT name FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'").all().every(x=>oldKnown.has(x.name)),'old_reader_unknown_object');
 assert.throws(oldReader,/old_reader_unknown_object/);
 d.prepare("UPDATE apps_meta SET value=? WHERE key='frame_continuity'").run('soty.apps-frame-continuity.v3');assert.throws(()=>inspectAppsSchema(d),/apps_schema_unsupported/);
 d.prepare("DELETE FROM apps_meta WHERE key='frame_continuity'").run();assert.throws(()=>inspectAppsSchema(d),/apps_schema_unsupported/);
});
test('closed lifecycle does not exhaust owner cap, while 256 actually active frames remain bounded',t=>{
 const d=db(t),a=store(d,'A');const next=()=>{const r=record();r.context.reference.id=randomBytes(32).toString('base64url');r.handleHash=randomBytes(32).toString('hex');return r;};
 const attach=r=>a.attach(r,{family:{frameId:r.context.reference.id},entryPath:'/embed'});
 for(let i=0;i<300;i++){const r=next();attach(r);a.close(r);}
 for(let i=0;i<256;i++)attach(next());assert.throws(()=>attach(next()),/apps_sessions_busy/);
 assert.equal(d.prepare("SELECT count(*) n FROM app_frame_continuity WHERE state='open'").get().n,256);
});

test('bounded global tombstone pruning cannot revive a stale claim or exhaust active capacity',t=>{
 const{d,a,r,id}=initial(t),b=store(d,'B'),candidate=record('b');b.prepare(candidate,id,b.recover(actor,'app-one',id,r.handleHash),'/embed');a.close(r);
 d.prepare('UPDATE app_frame_continuity SET updated_at=0 WHERE frame_id=?').run(id);
 const next=()=>{const r=record();r.context.reference.id=randomBytes(32).toString('base64url');r.handleHash=randomBytes(32).toString('hex');return r;};
 for(let i=0;i<4096;i++){const r=next();a.attach(r,{family:{frameId:r.context.reference.id},entryPath:'/embed'});a.close(r);}
 assert.equal(d.prepare('SELECT count(*) n FROM app_frame_continuity').get().n,4096);
 assert.equal(d.prepare('SELECT 1 FROM app_frame_continuity WHERE frame_id=?').get(id),undefined);
 assert.throws(()=>b.promote(candidate),/app_scoped_context_closed/);
 const current=next();a.attach(current,{family:{frameId:current.context.reference.id},entryPath:'/embed'});a.close(r);a.assertRecord(current);
 assert.equal(d.prepare("SELECT count(*) n FROM app_frame_continuity WHERE state='open'").get().n,1);
});

test('passive continuity never persists a Source invitation bearer in an entry fragment',t=>{
 const d=db(t),a=store(d,'A'),r=record(),secret='private-source-invitation-'+randomBytes(32).toString('base64url');
 const path='/embed#document=page&invite='+secret;a.attach(r,{family:{frameId:r.context.reference.id},entryPath:path});
 assert.equal(JSON.stringify(d.prepare('SELECT * FROM app_frame_continuity').all()).includes(secret),false,'scope provenance must not persist a Source bearer');
 const b=store(d,'B'),claim=b.recover(actor,'app-one',r.context.reference.id,r.handleHash);
 for(const bad of [undefined,'/embed','/embed#document=other&invite='+secret])assert.throws(()=>b.prepare(record('b'),r.context.reference.id,claim,bad),/app_scoped_context_closed/);
 b.prepare(record('c'),r.context.reference.id,claim,path);
});
