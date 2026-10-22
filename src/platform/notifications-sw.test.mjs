import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

const source=readFileSync(new URL('../../public/sw.js',import.meta.url),'utf8');
const owner='a'.repeat(64),other='b'.repeat(64),request='N'.repeat(32);
const payload=(bindingId=owner)=>({schema:'soty.push.v1',notificationId:request,bindingId});
function fixture(origin='https://4-2.xn--p1ai'){
  const handlers=new Map(),records=new Map(),shown=[],opened=[];
  const cache=name=>{if(!records.has(name))records.set(name,new Map());const entries=records.get(name);return{
    match:async key=>entries.get(key)?.clone(),put:async(key,value)=>{entries.set(key,value.clone());},delete:async key=>entries.delete(key),addAll:async()=>{},
  };};
  vm.runInNewContext(source,{URL,Response,setTimeout,clearTimeout,MessageChannel,fetch(){throw Error('no network in SW controller fixture');},
    caches:{open:async name=>cache(name),keys:async()=>[],delete:async()=>true},self:{location:{origin},addEventListener:(name,handler)=>handlers.set(name,handler),
      registration:{async showNotification(title,options){const value={title,options,data:options.data,closed:false,close(){this.closed=true;}};shown.push(value);},getNotifications:async()=>shown.filter(value=>!value.closed)},
      clients:{claim:async()=>{},matchAll:async()=>[],openWindow:async value=>{opened.push(value);}},skipWaiting:async()=>{}}});
  const emit=async(type,values={})=>{let work=Promise.resolve(),response;handlers.get(type)({ports:[{postMessage:value=>{response=value;}}],...values,waitUntil:value=>{work=value;}});await work;return response;};
  return{shown,opened,emit,
    bind:(bindingId=owner,url=origin+'/',enabled=true)=>emit('message',{source:{url},data:{type:'SOTY_PUSH_BIND',bindingId,enabled}}),
    clear:(bindingId=owner)=>emit('message',{source:{url:origin+'/'},data:{type:'SOTY_PUSH_CLEAR',bindingId}}),
    push:value=>emit('push',{data:{text:()=>JSON.stringify(value),json:()=>value}}),
    click:value=>emit('notificationclick',{notification:{data:value,close(){}}}),
  };
}
test('the actual SW shows only a generic visible notification for its current bound account/device',async()=>{
  const f=fixture();await f.push(payload());assert.equal(f.shown.length,0);
  assert.equal((await f.bind()).ok,true);await f.push(payload());assert.equal(f.shown.length,1);
  assert.equal(f.shown[0].title,'Соты');assert.equal(f.shown[0].options.body,'Новое сообщение');
  assert.equal(f.shown[0].options.tag,'soty-'+request);assert.equal(f.shown[0].options.renotify,false);
  await f.push(payload(other));assert.equal(f.shown.length,1);
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
