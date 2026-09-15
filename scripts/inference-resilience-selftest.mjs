import assert from "node:assert/strict";
import { createServer } from "node:http";
import { setTimeout as sleep } from "node:timers/promises";
import { createInferenceRelay } from "../server/inference-relay.js";

const model = "MiniMaxAI/MiniMax-M2.7";
const secret = "synthetic-provider-secret-not-for-clients";
const metrics = [];
const chunk = (content, finish = null) => ({ id: "test-completion", object: "chat.completion.chunk", model,
  choices: [{ index: 0, delta: { content }, finish_reason: finish }] });
const encode = (value) => `data: ${JSON.stringify(value)}\n\n`;
const success = (res, content = "Привет 🌍") => {
  res.writeHead(200, { "Content-Type": "text/event-stream" });
  res.end(encode(chunk(content)) + encode(chunk("", "stop"))
    + encode({ choices: [], usage: { prompt_tokens: 3, completion_tokens: 2, total_tokens: 5 }, x_joingonka: { balance_ngonka: "PRIVATE_PROVIDER_BALANCE" } }) + "data: [DONE]\n\n");
};

async function scenario(primary, fallback, options = {}) {
  const servers = [];
  const hits = [0, 0];
  const start = async (handler) => {
    const server = createServer(handler); servers.push(server);
    await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
    return `http://127.0.0.1:${server.address().port}`;
  };
  const providers = [];
  for (const [index, handler] of [primary, fallback].entries()) {
    if (!handler) continue;
    const baseUrl = await start(async (req, res) => {
      hits[index]++;
      assert.equal(req.headers.authorization, `Bearer ${secret}`);
      let raw = ""; for await (const part of req) raw += part;
      const body = JSON.parse(raw);
      assert.equal(body.stream, true, "provider stream must also be used for JSON clients");
      assert.equal(body.model, model);
      try { await handler(req, res, body); } catch { if (!res.destroyed) res.destroy(); }
    });
    providers.push({ name: index === 0 ? "primary" : "fallback", baseUrl, apiKey: secret });
  }
  const relay = createInferenceRelay({ providers, firstTokenTimeoutMs: 200, requestTimeoutMs: 1200,
    idleTimeoutMs: 500, onEvent: event => metrics.push(event), ...options });
  const base = await start(async (req, res) => {
    let raw = ""; for await (const part of req) raw += part;
    res.status = code => { res.statusCode = code; return res; };
    res.json = value => res.end(JSON.stringify(value));
    const controller = new AbortController();
    res.once("close", () => { if (!res.writableEnded) controller.abort(new Error("client-disconnected")); });
    try { await relay.forward({ body: JSON.parse(raw), res, signal: controller.signal }); }
    catch (error) {
      if (res.destroyed) return;
      if (res.headersSent) res.end(encode({ error: { type: "upstream_error", message: "model-upstream-incomplete" } }));
      else { res.status(error.httpStatus || 502); res.json({ error: { message: error.publicCode || "model-upstream-unavailable" } }); }
    }
  });
  return {
    hits, relay,
    request: (stream = false, signal, extra = {}) => fetch(base, { method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ model, stream, messages: [{ role: "user", content: "PRIVATE_SYNTHETIC_PROMPT" }], ...extra }), signal }),
    close: async () => { for (const server of servers) server.closeAllConnections(); await Promise.all(servers.map(server => new Promise(resolve => server.close(resolve)))); }
  };
}

let checks = 0;
async function test(name, run) { await run(); checks++; console.log(JSON.stringify({ check: name, passed: true })); }

await test("retry-after-fallback-json-and-account-isolation", async () => {
  const s = await scenario((_req, res) => { res.writeHead(429, { "Retry-After": "20" }); res.end(); }, (_req, res) => success(res));
  try {
    for (let i = 0; i < 2; i++) {
      const response = await s.request(); assert.equal(response.status, 200);
      const value = await response.json();
      assert.equal(value.choices[0].message.content, "Привет 🌍");
      assert.equal(value.usage.total_tokens, 5);
      assert.equal(value.x_joingonka, undefined);
      assert.equal(value.object, "chat.completion");
    }
    assert.deepEqual(s.hits, [1, 2]);
    assert.equal(s.relay.snapshot()[0].unavailable, true);
  } finally { await s.close(); }
});

await test("first-output-timeout-cancels-and-falls-back", async () => {
  let cancelled = false;
  const s = await scenario((_req, res) => { res.on("close", () => { cancelled = true; }); }, (_req, res) => success(res));
  try {
    const response = await s.request(); assert.equal(response.status, 200); await response.json();
    await sleep(30); assert.equal(cancelled, true); assert.deepEqual(s.hits, [1, 1]);
  } finally { await s.close(); }
});

await test("aborted-json-is-not-success", async () => {
  const s = await scenario((_req, res) => {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ model, choices: [{ index: 0, message: { role: "assistant", content: "partial" }, finish_reason: "abort" }] }));
  }, (_req, res) => success(res, "complete"));
  try {
    const response = await s.request(); assert.equal(response.status, 200);
    assert.equal((await response.json()).choices[0].message.content, "complete"); assert.deepEqual(s.hits, [1, 1]);
  } finally { await s.close(); }
});

await test("never-replay-after-visible-text", async () => {
  const s = await scenario((_req, res) => {
    res.writeHead(200, { "Content-Type": "text/event-stream" });
    res.end(encode(chunk("visible partial")) + encode(chunk("", "abort")));
  }, (_req, res) => success(res));
  try {
    const response = await s.request(true); const text = await response.text();
    assert.equal(response.status, 200); assert.match(text, /visible partial/); assert.match(text, /upstream_error/);
    assert.doesNotMatch(text, /\[DONE\]/); assert.deepEqual(s.hits, [1, 0]);
  } finally { await s.close(); }
});

await test("bounded-queue-and-cancelled-waiter-release", async () => {
  let release;
  const held = new Promise(resolve => { release = resolve; });
  const s = await scenario(async (_req, res) => { await held; success(res); }, null,
    { concurrency: 1, maximumQueued: 1, queueWaitMs: 2000, firstTokenTimeoutMs: 3000, requestTimeoutMs: 5000 });
  try {
    const first = s.request();
    for (let i = 0; !s.hits[0] && i < 100; i++) await sleep(10);
    const controller = new AbortController();
    const second = s.request(false, controller.signal).catch(error => error.name);
    for (let i = 0; !s.relay.snapshot()[0].queued && i < 100; i++) await sleep(10);
    assert.equal(s.relay.snapshot()[0].queued, 1);
    const rejected = await s.request(); assert.equal(rejected.status, 503); await rejected.json();
    controller.abort(); await second;
    for (let i = 0; s.relay.snapshot()[0].queued && i < 100; i++) await sleep(10);
    assert.equal(s.relay.snapshot()[0].queued, 0);
    release(); assert.equal((await first).status, 200);
    await sleep(30); assert.equal(s.relay.snapshot()[0].active, 0);
  } finally { release(); await s.close(); }
});

await test("active-stream-outlives-initial-deadline", async () => {
  const s = await scenario(async (_req, res) => {
    res.writeHead(200, { "Content-Type": "text/event-stream" }); res.write(encode(chunk("start")));
    for (let i = 0; i < 4; i++) { await sleep(150); if (res.destroyed) return; res.write(encode(chunk("."))); }
    res.end(encode(chunk("", "stop")) + "data: [DONE]\n\n");
  }, null, { requestTimeoutMs: 300, idleTimeoutMs: 400, streamTimeoutMs: 2000 });
  try {
    const response = await s.request(true); const text = await response.text();
    assert.match(text, /\[DONE\]/); assert.doesNotMatch(text, /upstream_error/);
  } finally { await s.close(); }
});

await test("idle-stream-fails-without-success-marker-or-replay", async () => {
  const s = await scenario((_req, res) => {
    res.writeHead(200, { "Content-Type": "text/event-stream" }); res.write(encode(chunk("partial")));
  }, (_req, res) => success(res), { idleTimeoutMs: 100 });
  try {
    const response = await s.request(true); const text = await response.text();
    assert.match(text, /upstream_error/); assert.doesNotMatch(text, /\[DONE\]/); assert.deepEqual(s.hits, [1, 0]);
  } finally { await s.close(); }
});

await test("invalid-request-is-not-retried", async () => {
  const s = await scenario((_req, res) => { res.writeHead(400); res.end(); }, (_req, res) => success(res));
  try {
    for (let i = 0; i < 4; i++) { const response = await s.request(); assert.equal(response.status, 400); await response.json(); }
    assert.deepEqual(s.hits, [4, 0]); assert.equal(s.relay.snapshot()[0].unavailable, false);
  }
  finally { await s.close(); }
});

await test("heartbeats-and-role-deltas-do-not-hide-a-stalled-provider", async () => {
  let cancelled = false;
  const s = await scenario((_req, res) => {
    res.writeHead(200, { "Content-Type": "text/event-stream" });
    const timer = setInterval(() => res.write(': heartbeat\n\n' + encode({ choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }] })), 20);
    res.once("close", () => { clearInterval(timer); cancelled = true; });
  }, (_req, res) => success(res), { firstTokenTimeoutMs: 100 });
  try {
    const response = await s.request(true); const text = await response.text();
    assert.match(text, /\[DONE\]/); assert.doesNotMatch(text, /heartbeat/);
    await sleep(30); assert.equal(cancelled, true); assert.deepEqual(s.hits, [1, 1]);
  } finally { await s.close(); }
});

await test("json-deadline-is-bounded-even-while-tokens-arrive", async () => {
  const s = await scenario((_req, res) => {
    res.writeHead(200, { "Content-Type": "text/event-stream" });
    res.write(encode(chunk("start")));
    const timer = setInterval(() => res.write(encode(chunk("."))), 20);
    res.once("close", () => clearInterval(timer));
  }, (_req, res) => success(res), { requestTimeoutMs: 150 });
  try {
    const response = await s.request(); assert.equal(response.status, 504);
    assert.equal((await response.json()).error.message, "model-upstream-timeout");
    assert.deepEqual(s.hits, [1, 0]);
  } finally { await s.close(); }
});

await test("circuit-recovers-with-one-probe", async () => {
  let calls = 0;
  let release;
  const held = new Promise(resolve => { release = resolve; });
  const s = await scenario(async (_req, res) => {
    calls++;
    if (calls <= 2) { res.writeHead(502); res.end(); return; }
    if (calls === 3) await held;
    success(res, "primary recovered");
  }, (_req, res) => success(res, "fallback"), { cooldownMs: 150, failureThreshold: 2, firstTokenTimeoutMs: 1500 });
  try {
    for (let i = 0; i < 3; i++) assert.equal((await (await s.request()).json()).choices[0].message.content, "fallback");
    assert.deepEqual(s.hits, [2, 3]);
    await sleep(180);
    const probe = s.request();
    for (let i = 0; calls < 3 && i < 100; i++) await sleep(5);
    assert.equal(s.relay.snapshot()[0].recovering, true);
    assert.equal((await (await s.request()).json()).choices[0].message.content, "fallback");
    assert.deepEqual(s.hits, [3, 4]);
    release(); assert.equal((await (await probe).json()).choices[0].message.content, "primary recovered");
    assert.equal((await (await s.request()).json()).choices[0].message.content, "primary recovered");
    assert.deepEqual(s.hits, [4, 4]);
  } finally { release(); await s.close(); }
});

await test("legacy-function-call-refusal-and-empty-finish-remain-valid", async () => {
  let calls = 0;
  const s = await scenario((_req, res) => {
    res.writeHead(200, { "Content-Type": "text/event-stream" });
    const choices = calls++ === 0 ? [
      { delta: { function_call: { name: "lookup", arguments: '{"city":' } } },
      { delta: { function_call: { arguments: '"Уфа"}' } }, finish_reason: "function_call" }
    ] : calls === 2 ? [{ delta: { refusal: "Unable to answer" }, finish_reason: "stop" }]
      : [{ delta: {}, finish_reason: "content_filter" }];
    res.end(choices.map(choice => encode({ model, choices: [{ index: 0, finish_reason: null, ...choice }] })).join("") + 'data: [DONE]\n\n');
  }, (_req, res) => success(res));
  try {
    const legacy = (await (await s.request()).json()).choices[0];
    assert.deepEqual(legacy.message.function_call, { name: "lookup", arguments: '{"city":"Уфа"}' });
    assert.equal(legacy.finish_reason, "function_call");
    assert.equal((await (await s.request()).json()).choices[0].message.refusal, "Unable to answer");
    assert.equal((await (await s.request()).json()).choices[0].finish_reason, "content_filter");
    assert.deepEqual(s.hits, [3, 0]);
  } finally { await s.close(); }
});

await test("race-dispatches-both-and-cancels-reasoning-only-loser", async () => {
  let cancelled = false;
  const s = await scenario((_req, res) => {
    res.writeHead(200, { "Content-Type": "text/event-stream" });
    res.write(encode(chunk("<th")) + encode(chunk("ink>PRIVATE_LOSER_REASONING")));
    res.once("close", () => { cancelled = true; });
  }, async (_req, res) => { await sleep(40); success(res, "winner"); }, { providerStrategy: "race" });
  try {
    const response = await s.request(true), text = await response.text();
    assert.equal(response.status, 200); assert.match(text, /winner/); assert.doesNotMatch(text, /PRIVATE_LOSER|<think>/);
    await sleep(30); assert.equal(cancelled, true); assert.deepEqual(s.hits, [1, 1]);
    assert.ok(s.relay.snapshot().every(state => state.active === 0 && state.queued === 0 && !state.unavailable));
  } finally { await s.close(); }
});

await test("race-json-waits-for-a-complete-response", async () => {
  const s = await scenario(async (_req, res) => {
    res.writeHead(200, { "Content-Type": "text/event-stream" }); res.write(encode(chunk("INCOMPLETE_WINNER")));
    await sleep(20); res.end();
  }, async (_req, res) => { await sleep(45); success(res, "complete winner"); }, { providerStrategy: "race" });
  try {
    const response = await s.request(); assert.equal(response.status, 200);
    assert.equal((await response.json()).choices[0].message.content, "complete winner"); assert.deepEqual(s.hits, [1, 1]);
  } finally { await s.close(); }
});

await test("race-stream-selects-first-visible-text-and-never-mixes", async () => {
  const s = await scenario(async (_req, res) => {
    res.writeHead(200, { "Content-Type": "text/event-stream" });
    res.write(encode(chunk("<think>kept for model</think>primary")));
    await sleep(70); if (!res.destroyed) res.end(encode(chunk(" end", "stop")) + "data: [DONE]\n\n");
  }, async (_req, res) => { await sleep(35); if (!res.destroyed) success(res, "WRONG_FALLBACK"); }, { providerStrategy: "race" });
  try {
    const response = await s.request(true), text = await response.text();
    assert.match(text, /primary/); assert.match(text, /kept for model/); assert.match(text, /\[DONE\]/);
    assert.doesNotMatch(text, /WRONG_FALLBACK|upstream_error/); assert.deepEqual(s.hits, [1, 1]);
  } finally { await s.close(); }
});

await test("race-validates-tool-arguments-before-committing", async () => {
  const tools = args => ({ model, choices: [{ index: 0, delta: { tool_calls: [{ index: 0, id: "call_1", type: "function", function: { name: "lookup", arguments: args } }] }, finish_reason: "stop" }] });
  const s = await scenario((_req, res) => {
    res.writeHead(200, { "Content-Type": "text/event-stream" });
    res.end(encode(tools('{"broken":')) + 'data: [DONE]\n\n');
  }, async (_req, res) => {
    await sleep(35); res.writeHead(200, { "Content-Type": "text/event-stream" });
    res.end(encode(chunk('<think>TOOL_REASONING</think>')) + encode(tools('{"label":"ok"}')) + 'data: [DONE]\n\n');
  }, { providerStrategy: "race" });
  try {
    const text = await (await s.request(true)).text();
    assert.match(text, /lookup/); assert.match(text, /TOOL_REASONING/); assert.match(text, /tool_calls/);
    assert.doesNotMatch(text, /broken|upstream_error/); assert.match(text, /\[DONE\]/);
  } finally { await s.close(); }
});

await test("race-rejects-reasoning-only-json-and-empty-stop", async () => {
  for (const content of ['<think>unfinished', '<think>finished</think>', '']) {
    const s = await scenario((_req, res) => {
      res.writeHead(200, { 'Content-Type':'application/json' });
      res.end(JSON.stringify({ choices:[{index:0,message:{role:'assistant',content,reasoning_content:'still not an answer'},finish_reason:'stop'}] }));
    }, async (_req, res) => { await sleep(20); success(res, 'real answer'); }, { providerStrategy:'race' });
    try { assert.equal((await (await s.request()).json()).choices[0].message.content, 'real answer'); }
    finally { await s.close(); }
  }
});

await test("race-accepts-explicit-refusal-without-retrying-it", async () => {
  const s = await scenario((_req, res) => {
    res.writeHead(200, { 'Content-Type':'application/json' });
    res.end(JSON.stringify({choices:[{index:0,message:{role:'assistant',content:null,refusal:'Unable to answer'},finish_reason:'stop'}]}));
  }, async (_req, res) => { await sleep(100); if (!res.destroyed) success(res, 'OTHER'); }, { providerStrategy:'race' });
  try { assert.equal((await (await s.request()).json()).choices[0].message.refusal, 'Unable to answer'); }
  finally { await s.close(); }
});

await test("race-provider-400-does-not-discard-the-other-valid-result", async () => {
  const s = await scenario((_req,res) => {res.writeHead(400);res.end();}, async(_req,res)=>{await sleep(20);success(res,'supported');}, {providerStrategy:'race'});
  try { assert.equal((await (await s.request()).json()).choices[0].message.content,'supported');assert.deepEqual(s.hits,[1,1]); }
  finally {await s.close();}
});

await test("race-client-disconnect-cancels-both-and-releases-all-slots", async () => {
  let closed = 0;
  const stall = (_req,res)=>{res.once('close',()=>{closed++;});};
  const s = await scenario(stall,stall,{providerStrategy:'race'});
  try {
    const controller=new AbortController();const pending=s.request(true,controller.signal).catch(error=>error.name);
    for(let n=0;s.hits.some(h=>!h)&&n<100;n++)await sleep(5);
    assert.deepEqual(s.hits,[1,1]);controller.abort();await pending;
    for(let n=0;(closed!==2||s.relay.snapshot().some(p=>p.active))&&n<100;n++)await sleep(5);
    assert.equal(closed,2);assert.ok(s.relay.snapshot().every(p=>p.active===0&&p.queued===0&&!p.unavailable));
  }finally{await s.close();}
});

await test("race-all-providers-fail-with-bounded-deadline", async () => {
  const stall=(_req,res)=>{res.writeHead(200,{'Content-Type':'text/event-stream'});res.write(encode(chunk('<think>')));};
  const s=await scenario(stall,stall,{providerStrategy:'race',requestTimeoutMs:120,idleTimeoutMs:500});
  try{const response=await s.request();assert.equal(response.status,504);await response.json();await sleep(20);
    assert.ok(s.relay.snapshot().every(p=>p.active===0&&p.queued===0));}
  finally{await s.close();}
});

await test("race-concurrent-requests-stay-bounded-and-cancelled-losers-do-not-trip-circuit", async () => {
  const s=await scenario(async(_req,res)=>{await sleep(12);if(!res.destroyed)success(res,'fast');},
    async(_req,res)=>{await sleep(70);if(!res.destroyed)success(res,'slow');},
    {providerStrategy:'race',concurrency:2,maximumQueued:32,queueWaitMs:2000,requestTimeoutMs:3000});
  try {
    const results=await Promise.all(Array.from({length:20},async()=>{const response=await s.request();assert.equal(response.status,200);return(await response.json()).choices[0].message.content;}));
    assert.equal(results.length,20);assert.ok(results.every(text=>text==='fast'));
    await sleep(30);assert.ok(s.relay.snapshot().every(p=>p.active===0&&p.queued===0&&!p.unavailable));
  }finally{await s.close();}
});

const optionalTools = [{type:'function',function:{name:'lookup',parameters:{type:'object',properties:{}}}}];
await test("minimax-auto-tool-empty-answer-retries-once-and-preserves-context", async () => {
  const bodies=[];
  const s=await scenario((_req,res,body)=>{
    bodies.push(body);success(res,body.tools?'<think>EMPTY_AUTO_TOOL_REASONING':'answer restored');
  },null,{providerStrategy:'race',emptyToolFallback:true});
  try {
    const text=await (await s.request(true,undefined,{tools:optionalTools,tool_choice:'auto',temperature:0.85})).text();
    assert.match(text,/answer restored/);assert.doesNotMatch(text,/EMPTY_AUTO_TOOL_REASONING/);
    assert.equal(bodies.length,2);assert.deepEqual(bodies[0].messages,bodies[1].messages);
    assert.equal(bodies[1].temperature,0.85);assert.equal(bodies[1].tools,undefined);assert.equal(bodies[1].tool_choice,undefined);
  }finally{await s.close();}
});

await test("required-tools-are-never-disabled-by-compatibility-retry", async () => {
  for(const tool_choice of ['required',{type:'function',function:{name:'lookup'}}]){
    const s=await scenario((_req,res)=>success(res,'<think>empty'),null,{providerStrategy:'race',emptyToolFallback:true});
    try{const response=await s.request(false,undefined,{tools:optionalTools,tool_choice});assert.equal(response.status,502);await response.json();assert.deepEqual(s.hits,[1,0]);}
    finally{await s.close();}
  }
});

await test("auto-tool-compatibility-retry-is-bounded-and-does-not-repeat-tool-calls", async () => {
  const s=await scenario((_req,res)=>success(res,'<think>empty'),null,{providerStrategy:'race',emptyToolFallback:true});
  try{const response=await s.request(false,undefined,{tools:optionalTools});assert.equal(response.status,502);await response.json();assert.deepEqual(s.hits,[2,0]);}
  finally{await s.close();}
  const malformed=await scenario((_req,res)=>{
    res.writeHead(200,{'Content-Type':'application/json'});
    res.end(JSON.stringify({choices:[{index:0,message:{role:'assistant',tool_calls:[{id:'call_1',type:'function',function:{name:'lookup',arguments:'{'}}]},finish_reason:'stop'}]}));
  },null,{providerStrategy:'race',emptyToolFallback:true});
  try{const response=await malformed.request(false,undefined,{tools:optionalTools});assert.equal(response.status,502);await response.json();assert.deepEqual(malformed.hits,[1,0]);}
  finally{await malformed.close();}
});

assert.doesNotMatch(JSON.stringify(metrics), /synthetic-provider-secret|PRIVATE_SYNTHETIC_PROMPT|PRIVATE_PROVIDER_BALANCE|PRIVATE_LOSER_REASONING|TOOL_REASONING|EMPTY_AUTO_TOOL_REASONING/);
console.log(JSON.stringify({ ok: true, checks, sensitiveMetricsAbsent: true }));
