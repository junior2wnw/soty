import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

const source=readFileSync(new URL('../../public/sw.js',import.meta.url),'utf8')
  .replace('const buildAssets = [];','const buildAssets = ["/assets/current.js"];')
  .replace('const classicAssets = [];','const classicAssets = ["/assets/classic.js"];');
function fixture({present=true,cacheFails=false}={}){
  const handlers=new Map(),calls=[],writes=[];
  const cache={async match(request){if(cacheFails)throw Error('cache unavailable');return present?new Response('current cached build'):undefined;},async put(request,response){writes.push([request.url,await response.text()]);}};
  vm.runInNewContext(source,{URL,Response,fetch:async request=>{calls.push(request.url);return new Response('fresh network');},
    caches:{async open(){return cache;},match:async()=>new Response('older cache')},
    self:{location:{origin:'https://soty.test'},addEventListener:(name,handler)=>handlers.set(name,handler)}});
  return {calls,writes,async get(path,{mode='cors',method='GET'}={}){
    let response;const work=[];
    handlers.get('fetch')({request:{url:'https://soty.test'+path,mode,method},respondWith:promise=>{response=promise;},waitUntil:promise=>{work.push(promise);}});
    const result=response?await response:undefined;await Promise.all(work);return result;
  }};
}
test('installed immutable modules return immediately from this build cache without a network request or redundant cache write',async()=>{
  const f=fixture();for(const path of ['/assets/current.js','/assets/classic.js'])assert.equal(await(await f.get(path)).text(),'current cached build');
  assert.deepEqual(f.calls,[]);assert.deepEqual(f.writes,[]);
});
test('a missing current-build module fetches its exact URL instead of a previous build cache',async()=>{
  const f=fixture({present:false});assert.equal(await(await f.get('/assets/current.js')).text(),'fresh network');
  assert.deepEqual(f.calls,['https://soty.test/assets/current.js']);assert.deepEqual(f.writes,[['https://soty.test/assets/current.js','fresh network']]);
});
test('cache storage failure does not prevent a current module loading from the network',async()=>{
  const f=fixture({cacheFails:true});assert.equal(await(await f.get('/assets/current.js')).text(),'fresh network');assert.equal(f.calls.length,1);
});
test('documents, unlisted modules, manifests and protected APIs retain their network behavior',async()=>{
  const f=fixture();for(const [path,mode] of [['/','navigate'],['/assets/not-current.js','cors'],['/manifest.webmanifest','cors'],['/api/private','cors'],['/oauth/authorize','navigate']]){
    assert.equal(await(await f.get(path,{mode})).text(),'fresh network');
  }
  assert.equal(f.calls.length,5);assert.equal(await f.get('/assets/current.js?different=1'),undefined);assert.equal(await f.get('/assets/current.js',{method:'POST'}),undefined);
});
