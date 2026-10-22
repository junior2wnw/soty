import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

const source=readFileSync(new URL('../../public/sw.js',import.meta.url),'utf8');
const owner='a'.repeat(64),other='b'.repeat(64),request='N'.repeat(32);
const payload=(bindingId=owner)=>({schema:'soty.push.v1',notificationId:request,bindingId});
const defer=()=>{let resolve;const promise=new Promise(done=>{resolve=done;});return{promise,resolve};};
function fixture(origin='https://4-2.xn--p1ai',options={}){
  const handlers=new Map(),records=options.records??new Map(),shown=options.shown??[],opened=[];let at=options.now??1000;
  const keyUrl=value=>new URL(value?.url??value,origin).href;
  const matches=(left,right,ignoreSearch)=>ignoreSearch?left.split('?')[0]===right.split('?')[0]:left===right;
  const cache=name=>{if(!records.has(name))records.set(name,new Map());const entries=records.get(name);return{
    match:async(key,settings={})=>[...entries].find(([url])=>matches(url,keyUrl(key),settings.ignoreSearch))?.[1].clone(),
    put:async(key,value)=>{entries.set(keyUrl(key),value.clone());},
    delete:async(key,settings={})=>{let removed=false;for(const url of [...entries.keys()])if(matches(url,keyUrl(key),settings.ignoreSearch)){entries.delete(url);removed=true;}return removed;},
    keys:async()=>[...entries.keys()].map(url=>({url})),addAll:async()=>{},
  };};
  vm.runInNewContext(source,{URL,Response,Date:{now:()=>at},setTimeout,clearTimeout,MessageChannel,fetch(){throw Error('no network in SW controller fixture');},
    caches:{open:async name=>cache(name),keys:async()=>[],delete:async()=>true},self:{location:{origin},addEventListener:(name,handler)=>handlers.set(name,handler),
      registration:{async showNotification(title,settings){await options.beforeShow?.(settings);for(const prior of shown)if(!prior.closed&&prior.tag===settings.tag)prior.close();
        const value={title,options:settings,data:settings.data,tag:settings.tag,closed:false,close(){this.closed=true;}};shown.push(value);},
        getNotifications:async(settings={})=>shown.filter(value=>!value.closed&&(!settings.tag||value.tag===settings.tag))},
      clients:{claim:async()=>{},matchAll:async()=>[],openWindow:async value=>{opened.push(value);}},skipWaiting:async()=>{}}});
  const emit=async(type,values={})=>{let work=Promise.resolve(),response;handlers.get(type)({ports:[{postMessage:value=>{response=value;}}],...values,waitUntil:value=>{work=value;}});await work;return response;};
  return{shown,opened,emit,records,cache,at(value){at=value;},
    bind:(bindingId=owner,url=origin+'/',enabled=true)=>emit('message',{source:{url},data:{type:'SOTY_PUSH_BIND',bindingId,enabled}}),
    clear:(bindingId=owner)=>emit('message',{source:{url:origin+'/'},data:{type:'SOTY_PUSH_CLEAR',bindingId}}),
    push:value=>emit('push',{data:{text:()=>JSON.stringify(value),json:()=>value}}),
    click:value=>emit('notificationclick',{notification:shown.find(card=>!card.closed&&card.data.bindingId===value.bindingId&&card.data.notificationId===value.notificationId)??{data:value,close(){}}}),
  };
}
test('the actual SW shows only a generic visible notification for its current bound account/device',async()=>{
  const f=fixture();await f.push(payload());assert.equal(f.shown.length,0);
  assert.equal((await f.bind()).ok,true);await f.push(payload());assert.equal(f.shown.length,1);
  assert.equal(f.shown[0].title,'Соты');assert.equal(f.shown[0].options.body,'Новое сообщение');
  assert.equal(f.shown[0].options.tag,'soty-'+owner+'-'+request);assert.equal(f.shown[0].options.renotify,false);
  await f.push(payload(other));assert.equal(f.shown.length,1);
});

test('a dismissed or clicked ID is deduplicated across worker restart until the bounded durable TTL expires',async()=>{
  const f=fixture();await f.bind();await f.push(payload());f.shown[0].close();
  await f.push(payload());assert.equal(f.shown.length,1);
  const restarted=fixture('https://4-2.xn--p1ai',{records:f.records,shown:f.shown,now:2000});
  await restarted.push(payload());assert.equal(f.shown.length,1);
  restarted.at(3*3600_000);await restarted.push(payload());assert.equal(f.shown.length,2);
  await restarted.click(payload());assert.equal(f.shown[1].closed,true);await restarted.push(payload());assert.equal(f.shown.length,2);
});

test('a held old-account show completing after the new account binds cannot leave an old card or remove the new one',async()=>{
  const entered=defer(),release=defer(),f=fixture('https://4-2.xn--p1ai',{beforeShow:async settings=>{if(settings.data.bindingId===owner){entered.resolve();await release.promise;}}});
  await f.bind();const oldPush=f.push(payload());await entered.promise;
  await f.bind(other);await f.push(payload(other));assert.equal(f.shown.filter(value=>!value.closed&&value.data.bindingId===other).length,1);
  release.resolve();await oldPush;
  assert.equal(f.shown.filter(value=>!value.closed&&value.data.bindingId===owner).length,0);
  assert.equal(f.shown.filter(value=>!value.closed&&value.data.bindingId===other).length,1);
});

test('two concurrent retries reserve one ID until showing and durably remembering it finish',async()=>{
  const entered=defer(),release=defer(),f=fixture('https://4-2.xn--p1ai',{beforeShow:async()=>{entered.resolve();await release.promise;}});
  await f.bind();const first=f.push(payload());await entered.promise;await f.push(payload());assert.equal(f.shown.length,0);
  release.resolve();await first;assert.equal(f.shown.length,1);f.shown[0].close();await f.push(payload());assert.equal(f.shown.length,1);
});

test('OS show failure releases its reservation and does not permanently mark an unseen notification as delivered',async()=>{
  let failed=false;const f=fixture('https://4-2.xn--p1ai',{beforeShow:async()=>{if(!failed){failed=true;throw Error('synthetic OS failure');}}});
  await f.bind();await assert.rejects(f.push(payload()),/synthetic OS failure/u);assert.equal(f.shown.length,0);
  await f.push(payload());assert.equal(f.shown.length,1);
});

test('durable seen records prune expired IDs and retain at most 4096 entries',async()=>{
  const f=fixture(),cache=f.cache('soty-notifications-seen-v1'),until=3601000;
  const path=id=>'/__soty_push_seen/'+owner+'/'+id;
  await cache.put(path('expired')+'?until=999',new Response('',{headers:{'X-Soty-Seen-Until':'999'}}));
  for(let index=0;index<4096;index++)await cache.put(path(String(index).padStart(32,'Z'))+'?until='+until,new Response('',{headers:{'X-Soty-Seen-Until':String(until)}}));
  await f.bind();await f.push(payload());assert.equal((await cache.keys()).length,4096);
  assert.equal(await cache.match(path('expired'),{ignoreSearch:true}),undefined);
  assert.equal(await cache.match(path('0'.padStart(32,'Z')),{ignoreSearch:true}),undefined);
  assert.ok(await cache.match(path(request),{ignoreSearch:true}));
});
test('account replacement removes old OS notifications and a late old Clear cannot clear the new binding',async()=>{
  const f=fixture();await f.bind();await f.push(payload());await f.bind(other);assert.equal(f.shown[0].closed,true);
  await f.clear(owner);await f.push(payload(other));assert.equal(f.shown.length,2);assert.equal(f.shown[1].closed,false);
  await f.clear(other);assert.equal(f.shown[1].closed,true);await f.push(payload(other));assert.equal(f.shown.length,2);
});
test('a fresh account with notifications OFF suppresses the prior account even without its old controller',async()=>{
  const f=fixture();await f.bind();await f.push(payload());await f.bind(other,'https://4-2.xn--p1ai/',false);
  assert.equal(f.shown[0].closed,true);await f.push(payload());await f.push(payload(other));assert.equal(f.shown.length,1);
});
test('notification click opens only the fixed Soty shell and opaque ID; stale or arbitrary URL payloads are ignored',async()=>{
  const f=fixture();await f.bind();await f.click(payload());assert.equal(f.opened.length,1);
  const url=new URL(f.opened[0]);assert.equal(url.origin,'https://4-2.xn--p1ai');assert.equal(url.pathname,'/');assert.equal(url.searchParams.get('notification'),request);assert.equal(url.hash,'');
  await f.click({...payload(),url:'https://external.invalid/'});await f.click({...payload(),notificationId:'../private'});await f.click(payload(other));
  assert.equal(f.opened.length,1);
});
test('a retained HIVE document on the shared origin cannot bind the Soty notification receiver',async()=>{
  const f=fixture();assert.equal(await f.bind(owner,'https://4-2.xn--p1ai/?project=untrusted'),undefined);
  await f.push(payload());assert.equal(f.shown.length,0);
  assert.equal(await f.bind(owner,'https://external.invalid/'),undefined);await f.push(payload());assert.equal(f.shown.length,0);
});
test('the legacy Soty shell is used without exposing chat identifiers or credentials',async()=>{
  const f=fixture('https://xn--n1afe0b.online');await f.bind(owner,'https://xn--n1afe0b.online/__soty');await f.click(payload());
  assert.equal(new URL(f.opened[0]).pathname,'/__soty');assert.deepEqual([...new URL(f.opened[0]).searchParams.keys()],['notification']);
});
