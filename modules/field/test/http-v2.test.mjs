import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {DatabaseSync} from 'node:sqlite';
import {mkdtemp,mkdir,writeFile,rm,realpath} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,dirname,basename} from 'node:path';
import {createHttpApp} from '../../../server/http-app.js';
import {createClientWithStorage} from '../../connect/browser/client.mjs';
import {convertFieldDocumentToV2} from '../contract.mjs';

const v1=()=>({schema:'soty.field.v1',contexts:[{contextId:'personal',title:'Моё пространство',x:140,y:220}],shortcuts:[{shortcutId:'stable_instance',entity:{kind:'app',id:'opaque_source_ref'},contextId:'personal',slot:[1,-2]}]});
const v2=()=>{const document=convertFieldDocumentToV2(v1());document.shortcuts[0].placement={kind:'free',x:128,y:-96,width:720,height:405};return document;};
const code=value=>error=>{assert.equal(error.code,value);return true;};
function identityStore(){let state=null;return {async read(){return structuredClone(state);},async claim(value){state??=structuredClone(value);return structuredClone(state);},async compareAndSwap(revision,next){assert.equal(state.localRevision,revision);state=structuredClone(next);return structuredClone(state);}};}
async function fixture(t){
 const parent=await realpath(tmpdir()),directory=await mkdtemp(join(parent,'soty-field-http-v2-')),dataDir=join(directory,'data'),distDir=join(directory,'dist');await mkdir(distDir);await writeFile(join(distDir,'index.html'),'<!doctype html><title>Field storage acceptance</title>');
 let app;const clients=[],sockets=new Set(),server=createServer((req,res)=>app?app(req,res):res.writeHead(503).end());server.on('connection',socket=>{sockets.add(socket);socket.once('close',()=>sockets.delete(socket));});
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const port=server.address().port,origin=`http://127.0.0.1:${port}`;
 const open=()=>createHttpApp(distDir,{dataDir,connectOrigins:[origin],appOriginTemplate:`http://{appId}.legacy.localhost:${port}`,namedAppZone:`http://named.localhost:${port}`});app=open();
 t.after(async()=>{clients.forEach(client=>client.dispose());await app.locals.closeServices();const stopped=new Promise(resolve=>server.close(resolve));sockets.forEach(socket=>socket.destroy());await stopped;const actual=await realpath(directory);assert.equal(dirname(actual).toLowerCase(),parent.toLowerCase());assert.ok(basename(actual).startsWith('soty-field-http-v2-'));await rm(actual,{recursive:true,force:true,maxRetries:3,retryDelay:50});});
 const clientFor=store=>{const client=createClientWithStorage({projectId:'soty',endpoint:origin+'/api/connect/rpc',fetch:(url,options)=>fetch(url,{...options,headers:{...options.headers,origin}})},store);clients.push(client);return client;};
 async function person(label){const store=identityStore(),client=clientFor(store),account=await client.bootstrap(label);await client.extension('world.profile.get',{}, {expectedAccountId:account.accountId});return {store,client,account};}
 const owner=await person('Layout author'),outside=await person('Outside viewer');
 const call=(person,op,args={})=>person.client.extension('world.field.'+op,{expectedAccountId:person.account.accountId,...args},{expectedAccountId:person.account.accountId});
 const sql=callback=>{const db=new DatabaseSync(join(dataDir,'world','world.sqlite'));try{return callback(db);}finally{db.close();}};
 const snapshot=()=>sql(db=>({epoch:db.prepare("SELECT value FROM world_meta WHERE key='field_schema_version'").get().value,documents:db.prepare('SELECT * FROM world_field_documents ORDER BY account_id').all().map(row=>({...row})),receipts:db.prepare('SELECT * FROM world_field_receipts ORDER BY account_id,request_key').all().map(row=>({...row}))}));
 return {owner,outside,call,sql,snapshot,async restart(){await app.locals.closeServices();app=open();owner.client.dispose();owner.client=clientFor(owner.store);}};
}

test('real signed HTTP: explicit free resize survives Root restart/new client, preserves v1 receipt replay and keeps another account isolated',{timeout:20000},async t=>{
 const f=await fixture(t),initial=await f.call(f.owner,'get');assert.equal(initial.revision,0);assert.equal(initial.document.schema,'soty.field.v1');
 const oldArgs={expectedRevision:0,requestId:'old_layout_intent',document:v1()},first=await f.call(f.owner,'put',oldArgs);assert.equal(f.snapshot().epoch,'1');
 const next={expectedRevision:1,requestId:'explicit_resize',document:v2()},resized=await f.call(f.owner,'put',next);assert.equal(f.snapshot().epoch,'2');assert.deepEqual(resized.current.document,v2());
 assert.equal((await f.call(f.outside,'get')).document.shortcuts.length,0);await assert.rejects(f.call(f.outside,'get',{expectedAccountId:f.owner.account.accountId}),code('field_account_changed'));
 await f.restart();assert.deepEqual((await f.call(f.owner,'get')).document,v2());assert.equal((await f.call(f.owner,'get')).contentHash,resized.current.contentHash);
 const replay=await f.call(f.owner,'put',oldArgs);assert.equal(replay.replayed,true);assert.deepEqual(replay.receipt,first.receipt);assert.deepEqual(replay.current,resized.current);
 await assert.rejects(f.call(f.owner,'put',{expectedRevision:2,requestId:'old_client_rewrite',document:v1()}),code('field_document_downgrade_forbidden'));assert.deepEqual((await f.call(f.owner,'get')).document,v2());
});

test('real signed HTTP negative control: failed receipt write cannot publish epoch2 or geometry, then same request succeeds',{timeout:20000},async t=>{
 const f=await fixture(t);await f.call(f.owner,'put',{expectedRevision:0,requestId:'baseline',document:v1()});const before=f.snapshot(),args={expectedRevision:1,requestId:'retryable_resize',document:v2()};
 f.sql(db=>db.exec("CREATE TRIGGER fail_field2_http BEFORE INSERT ON world_field_receipts WHEN NEW.accepted_revision=2 BEGIN SELECT RAISE(ABORT,'field2_http_fault'); END"));
 await assert.rejects(f.call(f.owner,'put',args));assert.deepEqual(f.snapshot(),before,'the selected persistence oracle must detect the deliberately failed real write');
 f.sql(db=>db.exec('DROP TRIGGER fail_field2_http'));const accepted=await f.call(f.owner,'put',args);assert.equal(f.snapshot().epoch,'2');assert.deepEqual(accepted.current.document,v2());await f.restart();assert.deepEqual((await f.call(f.owner,'get')).document,v2());
});
