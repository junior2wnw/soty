import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createGonkaProxy, defaultGonkaProxyModel, miniMaxProxyModel } from '../server/gonka-proxy.js';

const token='synthetic-client-'.repeat(4);
let observed=[];
const proxy=createGonkaProxy({
  store:{authenticateModelToken:async t=>t===token},
  baseUrl:'https://synthetic.invalid/v1',apiKey:'synthetic-upstream-key-for-test',upstreamModel:miniMaxProxyModel,
  fetchImpl:async(_url,init)=>{
    const body=JSON.parse(init.body);observed.push(body);
    return Response.json({model:body.model,choices:[{message:{role:'assistant',content:'fixture'},finish_reason:'stop'}]});
  }
});
const server=createServer(async(req,res)=>{
  let raw='';for await(const chunk of req)raw+=chunk;
  req.body=JSON.parse(raw);res.status=c=>{res.statusCode=c;return res;};res.json=v=>res.end(JSON.stringify(v));
  await proxy.handleChatCompletions(req,res);
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
async function call(model,auth=token){
  return fetch('http://127.0.0.1:'+server.address().port,{
    method:'POST',headers:{Authorization:'Bearer '+auth,'Content-Type':'application/json'},
    body:JSON.stringify({model,stream:false,messages:[{role:'user',content:'synthetic'}],max_tokens:128})
  });
}
try{
  const aliases=[defaultGonkaProxyModel,miniMaxProxyModel,'MiniMax-M2.7','minimax-m2.7','MiniMaxAI/minimax-m2.7','minimax/minimax-m2.7','minimax','DeepSeek-V4-Flash-0731','deepseek-chat','deepseek-reasoner','deepseek-r1','deepseek-ai/DeepSeek-R1','deepseek','  MINIMAX-M2.7  '];
  for(const name of aliases){
    const response=await call(name);assert.equal(response.status,200,name);
    assert.equal((await response.json()).model,miniMaxProxyModel);
    assert.equal(observed.at(-1).model,miniMaxProxyModel);
    assert.equal(observed.at(-1).max_tokens,128);
  }
  const before=observed.length;
  for(const name of [undefined,null,42,{},'', 'gpt-4o','not-minimax','deepseek/../../other','minimax\nother']){
    const response=await call(name);assert.equal(response.status,400);await response.arrayBuffer();
  }
  assert.equal(observed.length,before);
  const response=await call('MiniMax-M2.7','wrong-client-'.repeat(4));assert.equal(response.status,401);await response.arrayBuffer();
  assert.equal(observed.length,before);
  console.log(JSON.stringify({ok:true,aliasCount:aliases.length,rejectedInvalidNames:true,unauthorizedNotForwarded:true,allAliasesRoutedTo:miniMaxProxyModel}));
}finally{server.closeAllConnections();await new Promise(r=>server.close(r));}
