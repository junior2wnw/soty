import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { setTimeout as sleep } from 'node:timers/promises';
import { createGonkaProxy, miniMaxProxyModel } from '../server/gonka-proxy.js';

const applicationTokens = ['a','b','c'].map(value=>value.repeat(48));
const connectorTokens = ['d','e'].map(value=>value.repeat(48));
const upstreamKey='synthetic-upstream-key-for-race-test';
const hits={primary:0,fallback:0}, cancelled={primary:0,fallback:0}, metrics=[];
const proxy=createGonkaProxy({
  store:{authenticateModelToken:async token=>connectorTokens.includes(token)?'connector-'+connectorTokens.indexOf(token):false},
  baseUrl:'https://primary.invalid/v1',apiKey:upstreamKey,
  fallbackBaseUrl:'https://fallback.invalid/v1',fallbackApiKey:upstreamKey,
  upstreamModel:miniMaxProxyModel,onEvent:event=>metrics.push(event),
  fetchImpl:async(url,options)=>{
    const provider=new URL(url).hostname.split('.')[0];hits[provider]++;
    assert.equal(options.headers.Authorization,'Bearer '+upstreamKey);
    const body=JSON.parse(options.body),label=body.messages[0].content;
    let timer,ended=false;
    return new Response(new ReadableStream({
      start(controller){
        const abort=()=>{if(ended)return;ended=true;clearTimeout(timer);cancelled[provider]++;controller.error(options.signal.reason);};
        options.signal.addEventListener('abort',abort,{once:true});
        timer=setTimeout(()=>{
          if(ended)return;ended=true;options.signal.removeEventListener('abort',abort);
          const chunk={choices:[{index:0,delta:{content:label},finish_reason:'stop'}]};
          controller.enqueue(new TextEncoder().encode('data: '+JSON.stringify(chunk)+'\n\ndata: [DONE]\n\n'));controller.close();
        },provider==='primary'?10:80);
      },
      cancel(){clearTimeout(timer);}
    }),{headers:{'Content-Type':'text/event-stream'}});
  }
});
assert.equal(proxy.providerStrategy,'race');
const server=createServer(async(req,res)=>{
  let raw='';for await(const part of req)raw+=part;req.body=JSON.parse(raw);
  res.status=code=>{res.statusCode=code;return res;};res.json=value=>res.end(JSON.stringify(value));
  await proxy.handleChatCompletions(req,res,req.url==='/application'?{
    client:'application',authenticateToken:async token=>applicationTokens.includes(token)?'app-'+applicationTokens.indexOf(token):false
  }:{});
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const request=(path,token,stream,label)=>fetch('http://127.0.0.1:'+server.address().port+path,{
  method:'POST',headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},
  body:JSON.stringify({model:miniMaxProxyModel,stream,messages:[{role:'user',content:label}]})
});
try{
  let cases=0;
  for(const [path,tokens] of [['/application',applicationTokens],['/connector',connectorTokens]])for(const token of tokens)for(const stream of [false,true]){
    const label='isolated-case-'+cases++,response=await request(path,token,stream,label);assert.equal(response.status,200);
    const text=await response.text();assert.ok(text.includes(label));
    if(stream)assert.ok(text.includes('[DONE]'));else assert.equal(JSON.parse(text).choices[0].message.content,label);
  }
  const before={...hits};
  for(const [path,token] of [['/application','x'.repeat(48)],['/application',connectorTokens[0]],['/connector',applicationTokens[0]]]){
    const response=await request(path,token,false,'not-authorized');assert.equal(response.status,401);await response.text();
  }
  await sleep(30);assert.deepEqual(hits,before);assert.deepEqual(hits,{primary:cases,fallback:cases});
  assert.equal(cancelled.fallback,cases);assert.ok(proxy.upstreamStatus().every(state=>state.active===0&&state.queued===0));
  const serialized=JSON.stringify(metrics);for(const secret of [...applicationTokens,...connectorTokens,upstreamKey])assert.equal(serialized.includes(secret),false);
  assert.equal(serialized.includes('isolated-case-'),false);
  console.log(JSON.stringify({ok:true,keys:applicationTokens.length+connectorTokens.length,cases,bothProvidersDispatched:true,losersCancelled:true,keyIsolationPreserved:true}));
}finally{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}
