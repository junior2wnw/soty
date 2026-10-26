import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer, request} from 'node:http';
import {once} from 'node:events';
import {readFile} from 'node:fs/promises';
import {sendNativeEntryHttpError} from '../server/native-entry-http-error.mjs';

const failure = status => Object.assign(Error('PRIVATE_INTERNAL_DETAIL'), {code:'native_entry_policy_unavailable',status});
async function fixture(t) {
  const server=createServer((req,res)=>{
    if(req.url==='/queued'){
      for(const [name,value]of Object.entries({'Content-Length':'1','Content-Encoding':'gzip','Content-Range':'bytes 0-0/1',
        'Location':'/auth/callback?code=PRIVATE_QUEUED','Refresh':'0; url=/auth/callback?code=PRIVATE_QUEUED',
        'Set-Cookie':'PRIVATE_COOKIE=1','Retry-After':'0','Content-Security-Policy':"frame-ancestors https://root.example",'X-Frame-Options':'SAMEORIGIN','Vary':'Origin'}))res.setHeader(name,value);
    }
    assert.equal(sendNativeEntryHttpError(req,res,failure(req.url==='/denied'?403:503)),true);
  });
  server.listen(0,'127.0.0.1'); await once(server,'listening'); t.after(()=>new Promise(done=>server.close(done)));
  return (method,headers,path='/')=>new Promise((resolve,reject)=>{
    const req=request({host:'127.0.0.1',port:server.address().port,method,path,headers},res=>{
      const chunks=[];res.on('data',chunk=>chunks.push(chunk));res.on('end',()=>resolve({status:res.statusCode,headers:res.headers,body:Buffer.concat(chunks).toString('utf8')}));
    });req.on('error',reject);req.end();
  });
}

test('real document503 has one fixed fresh-entry action, no callback/secret echo or unsafe framing override',async t=>{
  const get=await fixture(t),response=await get('GET',{'accept':'text/html','sec-fetch-mode':'navigate','sec-fetch-dest':'document'},'/auth/callback?code=PRIVATE_CODE&state=PRIVATE_STATE&access_token=PRIVATE_BEARER');
  assert.equal(response.status,503);assert.equal(response.headers['content-type'],'text/html; charset=utf-8');
  assert.equal(response.headers['cache-control'],'no-store');assert.equal(response.headers['referrer-policy'],'no-referrer');
  assert.equal(response.headers['x-content-type-options'],'nosniff');assert.match(response.headers.vary,/Accept/u);
  assert.match(response.headers['content-security-policy'],/default-src 'none'/u);
  assert.doesNotMatch(response.headers['content-security-policy'],/frame-ancestors/u);assert.equal(response.headers['x-frame-options'],undefined);
  assert.deepEqual([...response.body.matchAll(/href="([^"]+)"/gu)].map(x=>x[1]),['/']);
  assert.match(response.body,/rel="noreferrer"/u);assert.doesNotMatch(response.body,/PRIVATE_|\/auth\/callback|<script|<form|http-equiv|native_entry_policy_unavailable/iu);
  assert.equal(response.headers.location,undefined);assert.equal(response.headers.refresh,undefined);
  const python=await readFile(new URL('../python/soty_native_entry.py',import.meta.url),'utf8');
  const page=python.match(/_NATIVE_ENTRY_HTML = """([\s\S]*?)"""\.encode\('utf-8'\)/u)?.[1].replaceAll('\r\n','\n');
  assert.equal(response.body,page,'the two bindings embed identical finite HTML without an unpinned sidecar');
});

test('API, HEAD, writes and explicit non-navigation formats keep JSON and failure status',async t=>{
  const get=await fixture(t);
  for(const [method,headers]of [['POST',{accept:'text/html'}],['HEAD',{accept:'text/html'}],['GET',{accept:'application/json'}],
    ['GET',{accept:'text/html;q=0'}],['GET',{accept:'text/html','sec-fetch-mode':'cors'}],['GET',{accept:'text/html, application/json'}]]){
    const response=await get(method,headers);assert.equal(response.status,503);assert.equal(response.headers['content-type'],'application/json');
    if(method==='HEAD')assert.equal(response.body,'');else assert.deepEqual(JSON.parse(response.body),{code:'native_entry_policy_unavailable'});
  }
  const denied=await get('GET',{accept:'text/html'},'/denied');assert.equal(denied.status,403);
  assert.match(denied.body,/Этот вход недоступен/u);assert.doesNotMatch(denied.body,/Сейчас не получилось проверить доступ/u);
});

test('a started HTTP response is terminated; WebSocket and unrelated errors remain host-owned',()=>{
  let destroyed=0;
  const response={headersSent:true,destroy(){destroyed++;},writeHead(){assert.fail('must not send second headers');},end(){assert.fail('must not append a public document');}};
  assert.equal(sendNativeEntryHttpError({method:'GET',headers:{accept:'text/html'}},response,failure(503)),true);assert.equal(destroyed,1);
  assert.equal(sendNativeEntryHttpError({method:'GET',headers:{upgrade:'websocket'}},response,failure(503)),false);assert.equal(destroyed,1);
  assert.equal(sendNativeEntryHttpError({method:'GET',headers:{}},response,Error('PRIVATE_UNRELATED')),false);assert.equal(destroyed,1);
});

test('queued app body, cookie and callback headers are replaced while host framing policy stays intact',async t=>{
  const get=await fixture(t),response=await get('GET',{accept:'text/html'},'/queued');
  assert.equal(response.status,503);assert.match(response.body,/<\/html>$/u);
  for(const name of ['content-length','content-encoding','content-range','location','refresh','set-cookie','retry-after'])assert.equal(response.headers[name],undefined,name);
  assert.match(response.headers['content-security-policy'],/frame-ancestors https:\/\/root.example/u);
  assert.match(response.headers['content-security-policy'],/default-src 'none'/u);
  assert.equal(response.headers['x-frame-options'],'SAMEORIGIN');assert.match(response.headers.vary,/Origin/u);
  assert.equal(response.headers['referrer-policy'],'no-referrer');assert.doesNotMatch(response.body,/PRIVATE_/u);
});
