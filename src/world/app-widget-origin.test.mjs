import test from 'node:test';
import assert from 'node:assert/strict';
import {createAppLauncher,validateAppEntry} from './app-launch.mjs';

const appId='app-'+ 'a'.repeat(32),domainId='dom_'+ 'b'.repeat(32);
const instanceId='widget-11111111-1111-4111-8111-111111111111';
const canonicalOrigin='https://reader.apps.example';
const runtimeOrigin='https://app-w'+instanceId.slice(7).replaceAll('-','')+'-'+ 'c'.repeat(24)+'.apps.example';
const entry={appId,domainId,origin:canonicalOrigin,path:'/reader?page=2'};
const launchBinding={schema:'soty.app-launch-binding.v1',policyEpoch:1,targetRevision:2,targetDigest:'d'.repeat(64),profile:'soty.relay-restricted.v1',bindingFloor:2};
const boot=origin=>origin+'/_soty/boot?path=%2Freader%3Fpage%3D2#'+'A'.repeat(43);
const reply=()=>({url:boot(runtimeOrigin),entry:{...entry},runtimeOrigin,launchBinding:{...launchBinding}});
function fixture(read=reply,extra={}){
  let current=true;const calls=[];
  const launcher=createAppLauncher({target:{appId,domainId,path:entry.path},accountId:'own-viewer',instanceId,shellUrl:'https://soty.example/#mine',isCurrent:()=>current,
    request:async args=>{calls.push(args);return read();},...extra});
  return{launcher,calls,invalidate(){current=false;}};
}

test('signed widget runtime host opens while canonical entry and subsequent request stay unchanged',async()=>{
  const f=fixture();assert.equal(await f.launcher.launch(),boot(runtimeOrigin));
  assert.deepEqual(f.launcher.entry(),entry);assert.equal(f.launcher.runtimeOrigin(),runtimeOrigin);
  assert.equal(await f.launcher.launch({requireSameBinding:true}),boot(runtimeOrigin));
  assert.equal(f.calls.length,2);
  for(const args of f.calls)assert.deepEqual(args,{appId,domainId,path:entry.path,expectedAccountId:'own-viewer',instanceId,hostSession:true});
});

test('canonical entry validator still rejects a different browser origin without the signed widget seam',()=>{
  assert.throws(()=>validateAppEntry(entry,{appId,domainId,path:entry.path},'https://soty.example',boot(runtimeOrigin)),e=>e.code==='invalid_app_entry');
});

test('origin declarations cannot authorize wrong widget, namespace, zone, protocol, port or URL decorations',async()=>{
  for(const origin of [
    runtimeOrigin.replace('11111111111141118111111111111111','22222222222242228222222222222222'),
    runtimeOrigin.replace('app-w','other-w'), runtimeOrigin.replace('.apps.example','.foreign.example'),
    runtimeOrigin.replace('https:','http:'),runtimeOrigin+':444',runtimeOrigin+'/',runtimeOrigin+'?x=1',runtimeOrigin+'#x',
    runtimeOrigin.replace('c'.repeat(24),'c'.repeat(23)),
  ]){
    const f=fixture(()=>({...reply(),runtimeOrigin:origin,url:boot(origin)}));
    await assert.rejects(f.launcher.launch(),e=>['invalid_app_runtime_origin','invalid_app_launch_url','invalid_app_entry'].includes(e.code),origin);
    assert.equal(f.launcher.entry(),null,'rejected reply must not adopt logical entry');
  }
});

test('new widget URL requires explicit origin and real ordinary launch binding',async()=>{
  for(const key of ['runtimeOrigin','launchBinding']){
    const f=fixture(()=>{const value=reply();delete value[key];return value;});
    await assert.rejects(f.launcher.launch(),e=>['invalid_app_runtime_origin','invalid_app_entry'].includes(e.code));
  }
});

test('ordinary runtime host is never accepted for main or a selected Source profile',async()=>{
  const main=fixture(reply,{instanceId:'main'});
  await assert.rejects(main.launcher.launch(),e=>e.code==='invalid_app_runtime_origin');
  const scoped=fixture(()=>({...reply(),runtimeProfile:'soty.selected-human-embed.v2',scopedCloseHandle:'C'.repeat(43),scopedSource:{id:'soty.standard-workspace',version:2,digest:'23061fc95610e3011ec42b3ecf229308bce44b0a1268408ca9b6ee138c98f7e9'}}));
  await assert.rejects(scoped.launcher.launch(),e=>e.code==='invalid_app_runtime_origin');
});

test('boot origin and path must match the separately declared runtime and canonical path',async()=>{
  for(const url of [boot(canonicalOrigin),boot(runtimeOrigin).replace('%2Freader%3Fpage%3D2','%2Fother'),boot(runtimeOrigin).replace('?path=','?extra=1&path=')]){
    const f=fixture(()=>({...reply(),url}));
    await assert.rejects(f.launcher.launch(),e=>['invalid_app_runtime_origin','invalid_app_entry'].includes(e.code));
  }
});

test('same-binding automatic re-entry cannot swap runtime origin or fall back to the shared main cookie',async()=>{
  let value=reply();const f=fixture(()=>value);
  await f.launcher.launch();const original=f.launcher.runtimeOrigin();
  for(const next of [{...reply(),runtimeOrigin:runtimeOrigin.replace('c'.repeat(24),'e'.repeat(24)),url:boot(runtimeOrigin.replace('c'.repeat(24),'e'.repeat(24)))},{entry:{...entry},url:boot(canonicalOrigin),launchBinding}]){
    value=next;await assert.rejects(f.launcher.launch({requireSameBinding:true}),e=>e.code==='app_runtime_origin_changed');
    assert.equal(f.launcher.runtimeOrigin(),original);assert.deepEqual(f.launcher.entry(),entry);
  }
});

test('late own-account reply cannot install an origin after its stage is no longer current',async()=>{
  let finish;const f=fixture(()=>new Promise(resolve=>{finish=resolve;}));
  const pending=f.launcher.launch();await Promise.resolve();f.invalidate();finish(reply());
  assert.equal(await pending,null);assert.equal(f.launcher.entry(),null);assert.equal(f.launcher.runtimeOrigin(),null);
});

test('legacy main stays on its canonical origin without a runtime-origin selector or Source identity',async()=>{
  const f=fixture(()=>({entry:{...entry},url:boot(canonicalOrigin),launchBinding}),{instanceId:'main'});
  assert.equal(await f.launcher.launch(),boot(canonicalOrigin));assert.equal(f.launcher.runtimeOrigin(),canonicalOrigin);
  assert.equal(f.calls.length,1);assert.equal(Object.hasOwn(f.calls[0],'runtimeOrigin'),false);
});
