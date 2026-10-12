import test from 'node:test';
import assert from 'node:assert/strict';
import {createSourceFrameFetch} from '../browser/index.mjs';

test('one browser origin selects each frame without changing requests, bodies, signals or credentials',async()=>{
  const calls=[],fetch=async(input,init)=>{calls.push({input,init});return new Response('{}');};
  const main=createSourceFrameFetch({fetch,href:'https://native.test/embed?sotyFrame='+'A'.repeat(43)});
  const widget=createSourceFrameFetch({fetch,href:'https://native.test/embed?sotyFrame='+'B'.repeat(43)});
  const signal=new AbortController().signal,body=JSON.stringify({operation:'soty.context.select'});
  await main('/api/embed/query',{method:'POST',body,signal,credentials:'same-origin',headers:{'content-type':'application/json'}});
  await widget('/api/embed/feedback/context',{headers:{accept:'application/json'}});
  assert.equal(calls[0].init.headers.get('x-soty-frame'),'A'.repeat(43));
  assert.equal(calls[1].init.headers.get('x-soty-frame'),'B'.repeat(43));
  assert.equal(calls[0].input,'/api/embed/query');assert.equal(calls[0].init.body,body);
  assert.equal(calls[0].init.signal,signal);assert.equal(calls[0].init.credentials,'same-origin');
  assert.equal(calls[0].init.headers.get('content-type'),'application/json');
  const request=new Request('https://native.test/api/embed/query',{headers:{accept:'application/json'}});
  await main(request);assert.equal(calls[2].input,request);assert.equal(calls[2].init.headers.get('accept'),'application/json');
});

test('no selector on standalone/old pages or foreign/non-Source requests; malformed/conflicting selectors fail closed',async()=>{
  const calls=[],fetch=async(input,init)=>{calls.push({input,init});return new Response('{}');};
  assert.equal(createSourceFrameFetch({fetch,href:'https://native.test/embed'}),fetch);
  assert.equal(createSourceFrameFetch({fetch}),fetch);
  const wrapped=createSourceFrameFetch({fetch,href:'https://native.test/embed?sotyFrame='+'A'.repeat(43)});
  for(const path of ['https://other.test/api/embed/query','/api/account/session','/api/embed-other','/assets/app.js']){
    const init={headers:{accept:'application/json'}};await wrapped(path,init);assert.equal(calls.at(-1).init,init);
  }
  for(const suffix of ['short','A'.repeat(43)+'&sotyFrame='+'B'.repeat(43)])
    assert.throws(()=>createSourceFrameFetch({fetch,href:'https://native.test/embed?sotyFrame='+suffix}),/source_frame_invalid/);
  assert.throws(()=>wrapped('/api/embed/query',{headers:{'x-soty-frame':'B'.repeat(43)}}),/source_frame_invalid/);
});
