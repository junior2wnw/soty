import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createGonkaProxy, defaultGonkaProxyModel, miniMaxProxyModel } from '../server/gonka-proxy.js';

const token = 'test-client-token-'.repeat(4), upstreamKey = 'test-upstream-key-'.repeat(4);
let observed;
const proxy = createGonkaProxy({
  store:{authenticateModelToken:async candidate => candidate === token},
  baseUrl:'https://provider.invalid/v1', apiKey:upstreamKey, upstreamModel:miniMaxProxyModel,
  fetchImpl:async (url, init) => {
    observed = {url:String(url), auth:init.headers.Authorization, body:JSON.parse(init.body)};
    const tool = Boolean(observed.body.tools);
    const calls = [{index:0,id:'call_fixture',type:'function',function:{name:'test',arguments:'{"text":"Привет 🌍"}'}}];
    if (!observed.body.stream) return Response.json({model:miniMaxProxyModel,choices:[{index:0,finish_reason:'stop',message:{role:'assistant',content:tool?null:'Привет 🌍',...(tool?{tool_calls:calls}:{})}}],usage:{total_tokens:7}});
    const parts = [
      {model:miniMaxProxyModel,choices:[{index:0,delta:tool?{tool_calls:calls}:{content:'Привет 🌍'},finish_reason:null}]},
      {model:miniMaxProxyModel,choices:[{index:0,delta:{},finish_reason:'stop'}]},
      {choices:[],usage:{total_tokens:7}}
    ];
    const raw = Buffer.from(': keep-alive\r\n\r\n' + parts.map(x => 'data: '+JSON.stringify(x)+'\r\n\r\n').join('')+'data: [DONE]\r\n\r\n');
    let position=0;
    return new Response(new ReadableStream({pull(controller){
      if(position===raw.length) return controller.close();
      // Exercise separators and multibyte UTF-8 split across transport chunks.
      controller.enqueue(raw.subarray(position,position+1));position++;
    }}),{headers:{'Content-Type':'text/event-stream'}});
  }
});
const server=createServer(async(req,res)=>{
  let raw='';for await(const chunk of req) raw+=chunk;
  req.body=JSON.parse(raw);res.status=code=>{res.statusCode=code;return res;};res.json=value=>res.end(JSON.stringify(value));
  await proxy.handleChatCompletions(req,res);
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
try {
  for(const stream of [false,true]) for(const tool of [false,true]) {
    const response=await fetch('http://127.0.0.1:'+server.address().port,{
      method:'POST',headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},
      body:JSON.stringify({model:defaultGonkaProxyModel,stream,messages:[{role:'user',content:'fixture'}],...(tool?{tools:[{type:'function',function:{name:'test'}}]}:{})})
    });
    assert.equal(response.status,200);
    assert.equal(observed.body.model,miniMaxProxyModel);
    assert.equal(observed.auth,'Bearer '+upstreamKey);
    const text=await response.text();
    assert.ok(text.includes('Привет 🌍'));
    assert.equal(text.includes('\uFFFD'),false);
    assert.ok(text.includes('"finish_reason":"'+(tool?'tool_calls':'stop')+'"'));
    if(stream){assert.ok(text.includes(': keep-alive'));assert.ok(text.includes('data: [DONE]'));}
    assert.ok(text.includes('"total_tokens":7'));
  }
  console.log(JSON.stringify({ok:true,checks:['json-tool-finish','sse-tool-finish','ordinary-stop-unchanged','utf8-fragments','sse-comments-and-done','usage-preserved','alias-routing','upstream-auth-isolated']}));
}finally{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}
