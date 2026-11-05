import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:net';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,dirname,resolve} from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {startDevelopment} from './dev.mjs';

async function freePorts(){const servers=[];try{for(let index=0;index<3;index++){const server=createServer();servers.push(server);await new Promise((done,reject)=>{server.once('error',reject);server.listen(0,'127.0.0.1',done);});}return servers.map(server=>server.address().port);}finally{await Promise.all(servers.map(server=>new Promise(done=>server.close(done))));}}
function stores(dataDir){return['objects','pages'].map(name=>{const db=new DatabaseSync(join(dataDir,'canvas',name+'.sqlite'),{readOnly:true});try{return{version:db.prepare('PRAGMA user_version').get().user_version,layout:db.prepare("SELECT name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all()};}finally{db.close();}});}
async function fixture(t){const dataDir=await mkdtemp(join(tmpdir(),'soty-dev-page-selection-')),ports=await freePorts();let running;
 t.after(async()=>{await running?.close();assert.equal(dirname(resolve(dataDir)),resolve(tmpdir()));assert.match(dataDir,/soty-dev-page-selection-/u);await rm(dataDir,{recursive:true,force:true,maxRetries:5,retryDelay:100});});
 return{dataDir,async start(canvasPageSelection){running=await startDevelopment({dataDir,port:ports[0],apiPort:ports[1],connectorPort:ports[2],...(canvasPageSelection===undefined?{}:{canvasPageSelection}),quiet:true,signal:t.signal});return running;},async stop(){await running.close();assert.equal(await running.done,null);running=null;}};
}
test('page selection rejects nonboolean opt-in before reading paths or launching a child',async()=>{
 for(const value of['true','false',1,0,null,{},[]]){let reads=0;await assert.rejects(startDevelopment({canvasPageSelection:value,get dataDir(){reads++;throw Error('path read too early');}}),/canvasPageSelection must be a boolean/u);assert.equal(reads,0);}
});
test('explicit page selection reaches the actual child, creates Objects2/Pages2, and false later preserves both stores',{timeout:35000},async t=>{
 const f=await fixture(t),first=await f.start(true);assert.equal((await fetch(first.origin+'/health',{signal:t.signal})).status,200);const before=stores(f.dataDir);assert.deepEqual(before.map(item=>item.version),[2,2]);await f.stop();
 await f.start(false);assert.deepEqual(stores(f.dataDir),before,'a migration flag is not permission to downgrade a known layout');await f.stop();
});
test('default ignores inherited production page-selection flag and leaves fresh Objects1/Pages1',{timeout:35000},async t=>{
 const prior=process.env.SOTY_CANVAS_PAGE_SELECTION_ENABLED;process.env.SOTY_CANVAS_PAGE_SELECTION_ENABLED='1';
 t.after(()=>{if(prior===undefined)delete process.env.SOTY_CANVAS_PAGE_SELECTION_ENABLED;else process.env.SOTY_CANVAS_PAGE_SELECTION_ENABLED=prior;});
 const f=await fixture(t);await f.start();assert.deepEqual(stores(f.dataDir).map(item=>item.version),[1,1]);await f.stop();
});
