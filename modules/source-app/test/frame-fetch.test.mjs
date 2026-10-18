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

test('short refresh wake checks the exact parent and requestId, retries one read, and never repeats writes or network-unknown outcomes',async()=>{
  const view=new EventTarget(),parentOrigin='https://root.test',calls=[],wakes=[];
  view.setTimeout=setTimeout;view.clearTimeout=clearTimeout;
  const emit=(data,source,origin)=>{const event=new Event('message');Object.assign(event,{data,source,origin});view.dispatchEvent(event);};
  view.parent={postMessage(data,origin){assert.equal(origin,parentOrigin);wakes.push(data);
    queueMicrotask(()=>{const reply={schema:'soty.source-frame-refreshed.v1',requestId:data.requestId,ready:true};
      emit(reply,{},parentOrigin);emit(reply,view.parent,'https://foreign.test');emit({...reply,requestId:'wrong-request-id'},view.parent,parentOrigin);
      emit(reply,view.parent,parentOrigin);});}};
  const fetch=async(path,init)=>{calls.push({path,init});if(path==='/api/embed/invoke')throw Error('lost write ACK');
    return new Response('{}',{status:calls.length===1?401:200});};
  const wrapped=createSourceFrameFetch({fetch,href:'https://app.test/embed',shortSessionHandoff:true,view,parentOrigin});
  assert.equal((await wrapped('/api/embed/context')).status,200);assert.equal(wakes.length,1);assert.equal(calls.length,2);
  const body=JSON.stringify({requestId:'original-write-intent',input:{operation:'write'}});
  await assert.rejects(wrapped('/api/embed/invoke',{method:'POST',body}));
  assert.equal(calls.length,3);assert.equal(calls.at(-1).init.body,body);assert.equal(wakes.length,1);
});

test('masked or malformed ancestor origins keep standalone fetch usable without a wildcard or a refresh',async()=>{
  for(const parentOrigin of [undefined,null,'null','','not an origin','https://root.test/','https://root.test/path','data:text/plain,hello','file://','ws://root.test']){
    const view=new EventTarget();view.location={ancestorOrigins:[parentOrigin]};
    view.parent={postMessage(){assert.fail('untrusted parent must never receive a refresh');}};
    const fetch=async()=>new Response('{}',{status:401});
    assert.equal(createSourceFrameFetch({fetch,href:'https://app.test/embed',shortSessionHandoff:true,view}),fetch);
  }
});

test('a masked ancestor preserves the per-frame selector and returns an unauthorized read without replay',async()=>{
  const view=new EventTarget(),calls=[];view.location={ancestorOrigins:['null']};
  view.parent={postMessage(){assert.fail('masked parent must never receive a refresh');}};
  const fetch=async(input,init)=>{calls.push({input,init});return new Response('{}',{status:401});};
  const wrapped=createSourceFrameFetch({fetch,href:'https://app.test/embed?sotyFrame='+'A'.repeat(43),shortSessionHandoff:true,view});
  const signal=new AbortController().signal;
  const response=await wrapped('/api/embed/context',{credentials:'same-origin',signal});
  assert.equal(response.status,401);assert.equal(calls.length,1);
  assert.equal(calls[0].init.headers.get('x-soty-frame'),'A'.repeat(43));
  assert.equal(calls[0].init.signal,signal);assert.equal(calls[0].init.credentials,'same-origin');
});
