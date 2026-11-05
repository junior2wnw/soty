import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,readFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomUUID} from 'node:crypto';
import {createGlassCounter} from './app.mjs';

test('a real ordinary Source uses shared UI, commits once and retains the accepted action across restart',{timeout:15000},async t=>{
 const directory=await mkdtemp(join(tmpdir(),'soty-glass-counter-'));let app;t.after(async()=>{await app?.close();await rm(directory,{recursive:true,force:true});});
 const glassCss=await readFile(new URL('../../../../vendor/optical-glass/src/ui-kit.css',import.meta.url),'utf8');
 const sourceSdk=await readFile(new URL('../../../source-app/browser/app-work.mjs',import.meta.url),'utf8');
 const widgetContext=await readFile(new URL('../../../../vendor/optical-glass/src/widget-context.js',import.meta.url),'utf8');
 const options={databasePath:join(directory,'state.sqlite'),glassCss,sourceSdk,widgetContext,parentOrigin:'http://root.local'};
 app=await createGlassCounter(options);let origin='http://127.0.0.1:'+app.port;
 const initial=await(await fetch(origin+'/api/state')).json(),id=randomUUID();
 const mutate=(contentType='application/json')=>fetch(origin+'/api/increment',{method:'POST',headers:{'Content-Type':contentType},body:JSON.stringify({requestId:id})}).then(r=>r.json());
 const receipt=await mutate();assert.equal(receipt.value,initial.value+1);assert.equal(receipt.receipt.requestId,id);
 assert.equal((await mutate('application/json; charset=utf-8')).value,receipt.value,'retrying an unconfirmed request never increments twice, including JSON charset');
 await app.close();app=await createGlassCounter(options);origin='http://127.0.0.1:'+app.port;
 assert.equal((await(await fetch(origin+'/api/state')).json()).value,receipt.value);
 assert.equal((await mutate()).value,receipt.value,'the same receipt remains idempotent after process reopen');
 const css=await(await fetch(origin+'/assets/ui.css')).text();assert.ok(css.startsWith(glassCss),'the app consumes the maintained shared material');
 assert.equal(await(await fetch(origin+'/assets/widget-context.js')).text(),widgetContext,'presentation uses the shared library without app-specific messages');
 for(const body of ['{}','{']){const bad=await fetch(origin+'/api/increment',{method:'POST',headers:{'Content-Type':'application/json'},body});assert.equal(bad.status,400);assert.equal(app.state().value,receipt.value);}
 const rejected=await fetch(origin+'/api/increment',{method:'POST',headers:{Origin:'http://127.0.0.1:59999','Content-Type':'text/plain'},body:JSON.stringify({requestId:randomUUID()})});
 assert.equal(app.state().value,receipt.value,'A simple cross-origin POST must not mutate the Source');assert.equal(rejected.status,415);
 const preflight=await fetch(origin+'/api/increment',{method:'OPTIONS',headers:{Origin:'http://127.0.0.1:59999','Access-Control-Request-Method':'POST','Access-Control-Request-Headers':'content-type'}});
 assert.equal(preflight.status,404);assert.equal(preflight.headers.get('access-control-allow-origin'),null);assert.equal(preflight.headers.get('access-control-allow-methods'),null);assert.equal(app.state().value,receipt.value);
});
