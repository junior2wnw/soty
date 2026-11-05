import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

// Actual UI code with DOM/fetch/Work ports. Browser paint and iframe identity
// are separate gates; no HTTP request or Source database is used here.
const source=readFileSync(process.env.SOTY_COUNTER_UI_TEST_SOURCE??new URL('./ui.js',import.meta.url),'utf8').replace(/^import .*;\r?\n/gmu,'');
const settle=()=>new Promise(setImmediate);
function fixture(reply){
 const element=()=>({dataset:{},attributes:{},textContent:'',disabled:false,style:{setProperty(){}},listeners:new Map(),
  setAttribute(name,value){this.attributes[name]=value;},addEventListener(name,callback){this.listeners.set(name,callback);}});
 const root=element(),output=element(),button=element(),status=element(),symbol=element();button.disabled=true;button.querySelector=()=>symbol;output.textContent='…';
 const ports={main:root,output,button,'[role=status]':status,'meta[name=soty-parent-origin]':{content:'http://root.test'}};
 const requests=[];let work,changes=0,reloads=0;
 const window={parent:{},location:{reload(){reloads++;}},addEventListener(){}};
 const environment={window,document:{querySelector:selector=>ports[selector]},crypto,
  bindGlassWidgetContext:()=>({dispose(){}}),createSotyAppWork:options=>{work=options;return{changed(){changes++;},dispose(){}};},
  fetch:async(path,options={})=>{const request={path,method:options.method??'GET',...options};requests.push(request);return reply(request,requests);}};
 vm.runInNewContext(source,environment,{filename:'actual-counter-ui.js'});
 return{root,output,button,status,symbol,requests,get work(){return work;},get reloads(){return reloads;},get changes(){return changes;},click(){assert.equal(button.disabled,false);button.listeners.get('click')();}};
}
const json=state=>({ok:true,json:async()=>state});

test('initial read failure has a recoverable GET action on the same UI nodes, never an increment',async()=>{
 const f=fixture((_request,requests)=>{if(requests.length===1)throw Error('offline');return json({value:3,revision:4});});
 const original={output:f.output,button:f.button,status:f.status};await settle();
 assert.equal(f.button.disabled,false,'A failed initial read must offer a usable retry');assert.equal(f.button.attributes['aria-label'],'Повторить подключение');assert.equal(f.symbol.textContent,'↻');
 assert.equal(f.status.dataset.state,'error');assert.match(f.status.textContent,/Нет связи/u);assert.equal(f.work.isDirty(),false);
 f.click();await settle();assert.deepEqual(f.requests.map(r=>[r.path,r.method]),[['/api/state','GET'],['/api/state','GET']]);
 assert.equal(f.output.textContent,'3');assert.equal(f.status.dataset.state,'ready');assert.equal(f.button.attributes['aria-label'],'Прибавить');assert.equal(f.symbol.textContent,'+');assert.equal(f.button.disabled,false);
 assert.equal(f.output,original.output);assert.equal(f.button,original.button);assert.equal(f.status,original.status);assert.equal(f.reloads,0);assert.equal(f.work.isDirty(),false);
});

test('an unknown write is retried with its original requestId while Work keeps the close guard',async()=>{
 let attempts=0;const f=fixture(request=>{
  if(request.method==='GET')return json({value:3,revision:4});
  if(++attempts===1)throw Error('lost acknowledgment');const{requestId}=JSON.parse(request.body);return json({value:4,revision:5,receipt:{requestId}});
 });await settle();f.click();await settle();
 assert.equal(f.button.attributes['aria-label'],'Повторить сохранение');assert.equal(f.symbol.textContent,'↻');assert.equal(f.status.dataset.state,'error');assert.equal(f.work.isDirty(),true);await assert.rejects(f.work.save(),/counter_action_unconfirmed/u);
 assert.equal(f.requests.length,2,'A failed action is never retried automatically');f.click();await settle();
 const writes=f.requests.filter(r=>r.method==='POST');assert.equal(writes.length,2);assert.equal(JSON.parse(writes[0].body).requestId,JSON.parse(writes[1].body).requestId);
 assert.equal(f.requests.filter(r=>r.method==='GET').length,1,'A write retry is not replaced by a read');assert.equal(f.output.textContent,'4');assert.equal(f.button.attributes['aria-label'],'Прибавить');assert.equal(f.work.isDirty(),false);assert.equal(f.reloads,0);
});
