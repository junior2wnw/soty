import test from 'node:test';
import assert from 'node:assert/strict';
import { mountHiveDeviceBridge, hiveDeviceLinkMessage } from './hive-device-bridge.mjs';

const origin = 'https://hive.4-2.xn--p1ai', schema = 'hive.soty-device-link.v1';
const oldId = 'dev_'+ 'a'.repeat(32), id = 'dev_' + 'b'.repeat(32), challenge = 'c'.repeat(43);

test('only the active HIVE frame can obtain a purpose-bound proof; no key leaves the shell', async () => {
  const pair = await crypto.subtle.generateKey({name:'ECDSA',namedCurve:'P-256'},false,['sign','verify']);
  const publicJwk = await crypto.subtle.exportKey('jwk',pair.publicKey);
  let listener, active = true, reads = 0;
  const frame = {src: origin + '/_soty/boot',contentWindow:{}};
  const view = {location:{origin:'https://4-2.xn--p1ai'},crypto,
    addEventListener: (_,value) => {listener=value;},removeEventListener: () => {listener=null;}};
  const dispose = mountHiveDeviceBridge({view,getFrame:()=>frame,isCurrent:()=>active,
    readIdentity:async()=>{reads++;return {id:oldId,publicJwk,privateKey:pair.privateKey};}});
  const run = async (method, overrides = {}, fields = {}) => {
    const messages = []; const port = {postMessage:value=>messages.push(value),close(){}};
    await listener({source:frame.contentWindow,origin,ports:[port],data:{schema,requestId:crypto.randomUUID(),method,...fields},...overrides});
    return messages;
  };
  assert.deepEqual((await run('info'))[0].value,{id:oldId});
  for (const overrides of [{origin:'https://elsewhere.example'},{source:{}},{ports:[]}]) {
    assert.deepEqual(await run('info',overrides),[]);
  }
  assert.equal(reads,1);
  const result = (await run('sign',{}, {oldId,id,challenge}))[0];
  assert.deepEqual(Object.keys(result.value),['signature']);
  const signature = Buffer.from(result.value.signature,'base64url');
  assert.equal(await crypto.subtle.verify({name:'ECDSA',hash:'SHA-256'},pair.publicKey,signature,
    new TextEncoder().encode(hiveDeviceLinkMessage({oldId,id,challenge}))),true);
  assert.equal(await crypto.subtle.verify({name:'ECDSA',hash:'SHA-256'},pair.publicKey,signature,
    new TextEncoder().encode(`hive.device.login.v1\n${challenge}\n${oldId}`)),false);
  assert.equal((await run('sign',{}, {oldId:id,id:oldId,challenge}))[0].ok,false);
  assert.equal((await run('sign',{}, {oldId,id,challenge:'wrong'}))[0].ok,false);
  assert.deepEqual(await run('sign',{}, {oldId,id,challenge,privateKey:'forbidden'}),[]);
  active=false;assert.deepEqual(await run('info'),[]);dispose();assert.equal(listener,null);
});

test('the signature broker aborts when its frame changes during an asynchronous vault read', async () => {
  let listener, resolve, current = true;
  const frame={src:origin,contentWindow:{}};const messages=[];
  const view={location:{origin:'https://4-2.xn--p1ai'},crypto,addEventListener:(_,value)=>{listener=value;},removeEventListener(){}};
  const dispose=mountHiveDeviceBridge({view,getFrame:()=>frame,isCurrent:()=>current,readIdentity:()=>new Promise(done=>{resolve=done;})});
  const pending=listener({source:frame.contentWindow,origin,ports:[{postMessage:value=>messages.push(value),close(){}}],
    data:{schema,requestId:crypto.randomUUID(),method:'info'}});
  current=false;resolve({});await pending;assert.deepEqual(messages,[]);dispose();
});
