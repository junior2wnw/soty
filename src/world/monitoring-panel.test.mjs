import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync,realpathSync} from 'node:fs';
import {createRequire} from 'node:module';
import {pathToFileURL} from 'node:url';
import ts from 'typescript';
import {createMonitoringClient} from '../../modules/monitoring/sdk.mjs';
import * as model from './monitoring-panel-model.mjs';

const source=readFileSync(new URL('./monitoring-panel.ts',import.meta.url),'utf8');
const css=readFileSync(new URL('./monitoring-panel.css',import.meta.url),'utf8');
const settle=async()=>{for(let i=0;i<10;i++)await new Promise(resolve=>setImmediate(resolve));};
const clone=value=>structuredClone(value);
const scope={kind:'app',appId:'app-'+ 'a'.repeat(32)},now=1800000000000;
const storagePort=()=>{const rows=new Map();return{rows,getItem:key=>rows.get(key)??null,setItem:(key,value)=>rows.set(key,value),removeItem:key=>rows.delete(key)};};
const deferred=()=>{let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b;});return{promise,resolve,reject};};

function service(targetScope=scope){
 const state={scope:targetScope,config:{enabled:true,revision:3,updatedAt:now},permissions:{read:true,configure:true,admin:true},mode:'normal',nextQuery:null,queryValue:4,receipts:new Map(),calls:[],adminAction:null,adminMode:'normal',adminReceipts:new Map(),adminAttempts:new Set(),adminEffects:0};
 state.description=()=>({schema:'soty.monitoring.describe.v1',scope:clone(state.scope),generatedAt:now,storage:{mode:'writer',installed:true},...(state.collector?{collector:clone(state.collector)}:{}),config:clone(state.config),permissions:clone(state.permissions),
  metrics:[{id:'host.calls',label:'Запросы',unit:'count',kind:'counter',source:'host',status:'partial',current:4,lastAt:now-1}],actions:[{id:'collection.setEnabled',label:'Сбор показателей',right:'configure',enabled:state.permissions.configure},...(state.adminAction?[clone(state.adminAction)]:[])]});
 state.query=args=>({schema:'soty.monitoring.query.v1',scope:clone(state.scope),generatedAt:now,config:clone(state.config),series:[{metricId:args.metricIds[0],unit:'count',kind:'counter',source:'host',status:state.config.enabled?'partial':'disabled',
  points:[{at:args.from,value:0,samples:1},{at:args.from+1000,value:null,samples:0},{at:args.to-1,value:state.queryValue,samples:2}],coverage:{from:args.from,to:args.to,observedPoints:2,totalPoints:3,oldestAt:args.from},lastAt:args.to-1}]});
 state.request=async(op,args)=>{state.calls.push({op,args:clone(args)});
  if(op==='monitoring.describe'){if(state.mode==='denied')throw Object.assign(Error('denied'),{status:403,code:'monitoring_forbidden'});return clone(state.description());}
  if(op==='monitoring.query'){const result=clone(state.query(args));if(state.nextQuery){const held=state.nextQuery;state.nextQuery=null;await held.promise;}return result;}
  if(op==='monitoring.audit.list')return{schema:'soty.monitoring.audit.v1',entries:[{sequence:1,operation:'monitoring.config.put',revision:3,at:now}],nextCursor:null};
  if(op==='monitoring.admin.invoke'){
   if(state.adminReceipts.has(args.requestId))return{...clone(state.adminReceipts.get(args.requestId)),replayed:true};
   const old=state.adminAttempts.has(args.requestId);state.adminAttempts.add(args.requestId);
   if(state.adminMode==='unknown')return{schema:'soty.monitoring.admin.v1',actionId:args.actionId,state:'unknown',revision:null,replayed:old};
   if(state.adminMode==='missing'&&!args.retry)return{schema:'soty.monitoring.admin.v1',actionId:args.actionId,state:'not-applied',revision:null,replayed:old};
   assert.ok(state.adminAction);if(state.adminAction.revision!==args.expectedRevision)throw Object.assign(Error('conflict'),{code:'monitoring_revision_conflict',status:409});
   state.adminEffects++;state.adminAction.current=clone(args.input);state.adminAction.revision++;
   const result={schema:'soty.monitoring.admin.v1',actionId:args.actionId,state:'committed',revision:state.adminAction.revision,replayed:false};state.adminReceipts.set(args.requestId,clone(result));
   if(state.adminMode==='lost-ack'){state.adminMode='normal';throw Object.assign(Error('unknown transport'),{code:'network_error'});}return result;
  }
  if(op==='monitoring.config.put'){
   if(state.receipts.has(args.requestId))return{...clone(state.receipts.get(args.requestId)),replayed:true};
   if(state.mode==='conflict'){state.config={enabled:true,revision:4,updatedAt:now};throw Object.assign(Error('conflict'),{code:'monitoring_revision_conflict',status:409});}
   assert.equal(args.expectedRevision,state.config.revision);state.config={enabled:args.enabled,revision:state.config.revision+1,updatedAt:now};
   const result={schema:'soty.monitoring.config.v1',scope:clone(scope),config:clone(state.config),replayed:false};state.receipts.set(args.requestId,clone(result));
   if(state.mode==='lost-ack'){state.mode='normal';throw Object.assign(Error('unknown transport'),{code:'network_error'});}return result;
  }
  assert.fail('unexpected operation '+op);
 };
 return state;
}
function adminService(){const backend=service();backend.adminAction={id:'app.listing',label:'Приложение в каталоге',right:'admin',enabled:true,revision:4,current:{listed:false},fields:[{name:'listed',label:'Показывать в каталоге',type:'boolean'}]};return backend;}

// Production panel + canonical SDK. Only DOM/canvas, scheduling and signed
// service replies are controlled ports; this is not browser/GPU/authority proof.
function harness({code=source,backend=service(),storage=storagePort(),models=model}={}){
 let active=true,nextId=0;const timers=new Map(),plots=[],observers=[],media=[];
 const doc={visibilityState:'visible',activeElement:null,listeners:new Map(),addEventListener(type,handler){this.listeners.set(type,handler);}};
 class Node{
  constructor(tag,className='',text=''){this.tagName=tag.toUpperCase();this.className=className;this._text=text;this.children=[];this.parentElement=null;this.ownerDocument=doc;this.dataset={};this.attributes=new Map();this.listeners=new Map();this.hidden=false;this.disabled=false;this.checked=false;this.value='';this.open=false;this.clientWidth=640;this.clientHeight=250;}
  get classList(){return{add:(...values)=>{this.className=[...new Set([...this.className.split(' '),...values])].join(' ');}};}
  get isConnected(){return this===host||!!this.parentElement?.isConnected;}
  get textContent(){return this._text+this.children.map(node=>node.textContent).join('');}
  set textContent(text){for(const child of this.children)child.parentElement=null;this.children=[];this._text=String(text);}
  append(...children){for(const child of children){child.remove();child.parentElement=this;this.children.push(child);}}
  replaceChildren(...children){for(const child of this.children)child.parentElement=null;this.children=[];this._text='';this.append(...children);}
  remove(){if(this.parentElement){this.parentElement.children=this.parentElement.children.filter(child=>child!==this);this.parentElement=null;}}
  setAttribute(key,value){this.attributes.set(key,String(value));}getAttribute(key){return this.attributes.get(key)??null;}removeAttribute(key){this.attributes.delete(key);}
  addEventListener(type,handler,options={}){if(!this.listeners.has(type))this.listeners.set(type,[]);this.listeners.get(type).push(handler);options.signal?.addEventListener('abort',()=>{this.listeners.set(type,this.listeners.get(type).filter(value=>value!==handler));},{once:true});}
  emit(type,input={}){const event={type,target:this,isComposing:false,preventDefault(){this.prevented=true;},stopPropagation(){this.stopped=true;},...input};for(const handler of this.listeners.get(type)??[])handler(event);return event;}
  click(){if(!this.disabled)this.emit('click');}focus(){doc.activeElement=this;}
 }
 const host=new Node('div');
 class Plot{
  constructor(options,data,target){this.options=options;this.data=data;this.target=target;this.cursor={idx:null};this.select={width:0};this.scales={x:{min:data[0][0],max:data[0].at(-1)}};this.destroyed=false;plots.push(this);}
  setData(data){this.data=data;this.fireScale();}setScale(key,limits){this.scales[key]=limits;this.fireScale();}
  fireScale(){queueMicrotask(()=>{if(!this.destroyed)for(const hook of this.options.hooks.setScale??[])hook(this,'x');});}
  setSize(size){this.size=size;}redraw(){this.redraws=(this.redraws??0)+1;}destroy(){this.destroyed=true;}valToPos(value){return value;}setCursor(){for(const hook of this.options.hooks.setCursor??[])hook(this);}
  selectRange(){this.select.width=100;for(const hook of this.options.hooks.setSelect??[])hook(this);}
 }
 class Resize{constructor(callback){this.callback=callback;observers.push(this);}observe(){}disconnect(){this.disconnected=true;}}
 const view={crypto:{randomUUID:()=>`request-${++nextId}`},setTimeout(fn,delay){const id=++nextId;timers.set(id,{fn,delay});return id;},clearTimeout:id=>timers.delete(id),
  getComputedStyle:()=>({color:'rgb(30,30,40)',getPropertyValue:()=>''}),matchMedia:()=>{const value={matches:false,addEventListener(){},removeEventListener(){this.removed=true;}};media.push(value);return value;},addEventListener(){}};
 doc.defaultView=view;
 const element=(tag,className='',text='')=>new Node(tag,className,text);
 const makeButton=(label,_icon,className='',action)=>{const node=element('button','sw-button '+className,label);node.type='button';if(action)node.addEventListener('click',action);return node;};
 const labeled=(label,input)=>{const node=element('label','sw-field');node.append(element('span','sw-field-label',label),input);return node;};
 const compiled=ts.transpileModule(code,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.ESNext}}).outputText.replace(/^import[^\n]*\n/gmu,'').replace(/^export /gmu,'');
 const helpers=Object.keys(models);
 const mount=new Function('uPlot','createMonitoringClient','el','button','labeledField','ResizeObserver',...helpers,compiled+'\nreturn mountMonitoringPanel;')
  (Plot,opts=>createMonitoringClient({...opts,storage}),element,makeButton,labeled,Resize,...helpers.map(key=>models[key]));
 const panel=mount(host,{api:{request:backend.request},accountId:'owner-A',scope:clone(backend.scope),isCurrent:()=>active});
 const flatten=node=>[node,...node.children.flatMap(flatten)];
 return{panel,backend,storage,host,plots,timers,observers,media,node:className=>flatten(host).find(node=>node.className.split(' ').includes(className)),field:name=>flatten(host).find(node=>node.name===name),focused:()=>doc.activeElement,
  accountChanged(){active=false;},visibility(value){doc.visibilityState=value;doc.listeners.get('visibilitychange')?.();},async tick(){const next=timers.entries().next().value;assert.ok(next);timers.delete(next[0]);next[1].fn();await settle();}};
}

test('real async reads produce real chart values, a missing gap, a semantic table and coverage without claiming application success',async()=>{
 const h=harness();await h.panel.ready;
 assert.deepEqual(h.backend.calls.map(call=>call.op),['monitoring.describe','monitoring.query']);
 assert.deepEqual(h.plots[0].data[1],[0,null,4]);assert.equal(h.plots[0].options.series[1].spanGaps,false);
 assert.equal(h.node('mon-value').textContent,'4');assert.equal(h.node('mon-table').children[1].children.length,3);
 assert.match(h.node('mon-coverage').textContent,/2 из 3/u);assert.match(h.node('mon-boundary').textContent,/не подтверждает/u);
 assert.ok(h.backend.calls.every(call=>call.args.expectedAccountId==='owner-A'&&call.args.scope.appId===scope.appId));h.panel.dispose();
});

test('collector runtime status never turns stopped, failed or unsupported collection into invented zero observations',async()=>{
 const backend=service();backend.collector={running:false,lastSuccessAt:now-1000,lastFailureAt:null,failures:0,supportedMetricIds:[]};const h=harness({backend});await h.panel.ready;
 try{assert.match(h.node('mon-state').textContent,/сейчас не выполняется/u);assert.match(h.node('mon-coverage').textContent,/Сейчас этот показатель не собирается/u);assert.deepEqual(h.plots[0].data[1],[0,null,4]);
  backend.collector={running:true,lastSuccessAt:now-1000,lastFailureAt:now,failures:1,supportedMetricIds:['host.calls']};await h.panel.refresh();assert.match(h.node('mon-state').textContent,/последнее обновление не удалось/u);assert.equal(h.node('mon-value').textContent,'4');assert.doesNotMatch(h.node('mon-coverage').textContent,/Сейчас этот показатель не собирается/u);
 }finally{h.panel.dispose();}
});

test('ordinary plot commits do not pause Live; explicit chart selection does and keyboard can read a measured zero or gap',async()=>{
 const h=harness();await h.panel.ready;await settle();assert.equal(h.node('mon-live').getAttribute('aria-pressed'),'true');assert.equal(h.timers.size,1);
 h.node('mon-chart').emit('keydown',{key:'Home'});assert.equal(h.node('mon-value').textContent,'0');
 h.node('mon-chart').emit('keydown',{key:'ArrowRight'});assert.equal(h.node('mon-value').textContent,'Нет данных');
 h.node('mon-chart').emit('keydown',{key:'End',isComposing:true});assert.equal(h.node('mon-value').textContent,'Нет данных');
 h.plots[0].selectRange();assert.equal(h.node('mon-live').getAttribute('aria-pressed'),'false');assert.equal(h.timers.size,0);assert.equal(h.node('mon-reset-zoom').hidden,false);
 const escape=h.node('mon-chart').emit('keydown',{key:'Escape'});assert.equal(escape.prevented,true);assert.equal(h.node('mon-reset-zoom').hidden,true);h.panel.dispose();
});

async function staleRangeOracle(code){
 const h=harness({code});await h.panel.ready;h.backend.queryValue=10;const held=deferred();h.backend.nextQuery=held;const old=h.panel.refresh();await settle();
 h.backend.queryValue=20;h.node('mon-range').value='day';h.node('mon-range').emit('change');await settle();held.resolve();await old;await settle();
 try{assert.equal(h.node('mon-value').textContent,'20','An old range reply must not replace the newer range');}finally{h.panel.dispose();}
}
test('a held older range reply cannot overwrite the newer requested range',()=>staleRangeOracle(source));
test('switching to an unsubmitted custom range retires the pending hour reply without inventing a period',async()=>{
 const h=harness();await h.panel.ready;h.backend.queryValue=10;const held=deferred();h.backend.nextQuery=held;const flight=h.panel.refresh();await settle();
 h.node('mon-range').value='custom';h.node('mon-range').emit('change');const count=h.backend.calls.length;held.resolve();await flight;
 try{assert.equal(h.node('mon-value').textContent,'—','A pending hour reply must not populate an unsubmitted custom range');assert.equal(h.backend.calls.length,count);}
 finally{h.panel.dispose();}
});
test('refreshing an invalid custom range makes no server read',async()=>{
 const h=harness();await h.panel.ready;h.node('mon-range').value='custom';h.node('mon-range').emit('change');const count=h.backend.calls.length;
 h.node('mon-refresh').click();await settle();try{assert.equal(h.backend.calls.length,count,'An invalid custom range must fail before any network read');}finally{h.panel.dispose();}
});
test('pausing auto updates also retires an already pending refresh',async()=>{
 const h=harness();await h.panel.ready;h.backend.queryValue=10;const held=deferred();h.backend.nextQuery=held;const flight=h.panel.refresh();await settle();h.node('mon-live').click();held.resolve();await flight;
 try{assert.equal(h.node('mon-value').textContent,'4','A paused view must not apply the old in-flight refresh');assert.equal(h.timers.size,0);}finally{h.panel.dispose();}
});
test('negative control: removing the actual read-generation fence fails the unchanged held-range oracle',async()=>{
 const anchor='return epoch!==undefined&&epoch!==readEpoch;';assert.equal(source.split(anchor).length,2);
 await assert.rejects(staleRangeOracle(source.replace(anchor,'return false;')),/An old range reply/u);
});

test('loss of permission or account clears chart, table and own labels, including a held reply',async()=>{
 for(const mode of ['permission','account']){const h=harness();await h.panel.ready;
  if(mode==='permission'){h.backend.mode='denied';await h.panel.refresh();}
  else{const held=deferred();h.backend.nextQuery=held;const flight=h.panel.refresh();await settle();h.accountChanged();held.resolve();await flight;}
  assert.equal(h.node('mon-value').textContent,'—');assert.equal(h.node('mon-table').children[1].children.length,0);assert.equal(h.node('mon-controls').hidden,true);assert.equal(h.node('mon-audit-list').children.length,0);assert.ok(h.plots[0].destroyed);h.panel.dispose();
 }
});

test('read-only permissions prevent a programmatically emitted configure action and do not load the admin audit',async()=>{
 const backend=service();backend.permissions.configure=false;backend.permissions.admin=false;const h=harness({backend});await h.panel.ready;
 assert.equal(h.node('mon-collection').children[0].disabled,true);h.node('mon-collection').children[0].checked=false;h.node('mon-collection').children[0].emit('change');
 h.node('mon-audit').open=true;h.node('mon-audit').emit('toggle');await settle();assert.equal(backend.calls.filter(call=>call.op==='monitoring.config.put'||call.op==='monitoring.audit.list').length,0);h.panel.dispose();
});

test('lost mutation ACK survives panel reopening and retries the exact SDK-journaled ID and revision',async()=>{
 const backend=service(),storage=storagePort();backend.mode='lost-ack';const first=harness({backend,storage});await first.panel.ready;
 const input=first.node('mon-collection').children[0];input.checked=false;input.emit('change');await settle();assert.equal(first.node('mon-pending').hidden,false);assert.equal(storage.rows.size,1);first.panel.dispose();
 const second=harness({backend,storage});await second.panel.ready;assert.equal(second.node('mon-pending').hidden,false);
 second.node('mon-pending-actions').children[0].click();await settle();
 const writes=backend.calls.filter(call=>call.op==='monitoring.config.put');assert.equal(writes.length,2);assert.deepEqual(writes[0].args,writes[1].args);
 assert.equal(backend.config.revision,4);assert.equal(storage.rows.size,0);assert.equal(second.node('mon-collection').children[0].checked,false);second.panel.dispose();
});

test('an explicit CAS refusal preserves intent; taking current makes only a fresh read and no rebased mutation',async()=>{
 const backend=service();backend.mode='conflict';const h=harness({backend});await h.panel.ready;
 const input=h.node('mon-collection').children[0];input.checked=false;input.emit('change');await settle();assert.equal(h.node('mon-pending').hidden,false);
 const take=h.node('mon-pending-actions').children[1];assert.equal(take.hidden,false);const count=backend.calls.filter(call=>call.op==='monitoring.config.put').length;
 take.click();await settle();assert.equal(h.storage.rows.size,0);assert.equal(backend.calls.filter(call=>call.op==='monitoring.config.put').length,count);assert.equal(input.checked,true);assert.match(h.node('mon-mutation-status').textContent,/не отправлялось/u);h.panel.dispose();
});

test('journal failure occurs before configure dispatch; invalid custom dates also cause no new request',async()=>{
 const storage=storagePort();storage.setItem=()=>{throw Error('quota');};const h=harness({storage});await h.panel.ready;
 h.node('mon-collection').children[0].checked=false;h.node('mon-collection').children[0].emit('change');await settle();assert.equal(h.backend.calls.filter(call=>call.op==='monitoring.config.put').length,0);assert.match(h.node('mon-error').textContent,/не отправлена/u);
 h.node('mon-range').value='custom';h.node('mon-range').emit('change');const count=h.backend.calls.length;h.node('mon-custom').emit('submit');await settle();assert.equal(h.backend.calls.length,count);assert.equal(h.timers.size,0);h.panel.dispose();
});

test('audit is lazy and current-permission scoped; background and dispose leave no timer or plot',async()=>{
 const h=harness();await h.panel.ready;assert.equal(h.backend.calls.some(call=>call.op==='monitoring.audit.list'),false);
 h.node('mon-audit').open=true;h.node('mon-audit').emit('toggle');await settle();assert.equal(h.node('mon-audit-list').children.length,1);
 h.backend.permissions.admin=false;await h.panel.refresh();assert.equal(h.node('mon-audit-list').children.length,0);assert.equal(h.node('mon-audit').hidden,true);
 h.visibility('hidden');assert.equal(h.timers.size,0);h.panel.dispose();assert.equal(h.host.children.length,0);assert.ok(h.plots.every(plot=>plot.destroyed));assert.ok(h.observers.every(observer=>observer.disconnected));
 assert.match(css,/touch-action:pan-y/u);assert.match(css,/min-height:44px/u);assert.match(css,/forced-colors:active/u);assert.match(css,/prefers-reduced-motion:reduce/u);
});

test('a canonical admin edit needs explicit confirmation; cancel is safe and success uses the actual SDK action',async()=>{
 const backend=adminService(),h=harness({backend});await h.panel.ready;const form=h.node('mon-admin-action'),input=h.field('listed');
 input.checked=true;input.emit('input');assert.equal(form.children[2].children[0].disabled,false);assert.equal(h.panel.hasUnsavedChanges(),true);
 form.emit('submit');const confirm=h.node('mon-action-confirm');assert.equal(confirm.hidden,false);assert.equal(h.focused(),confirm.children[1]);
 assert.equal(backend.adminEffects,0);confirm.children[1].click();assert.equal(confirm.hidden,true);assert.equal(backend.adminEffects,0);
 form.emit('submit');confirm.children[2].click();await settle();
 const call=backend.calls.find(call=>call.op==='monitoring.admin.invoke');assert.equal(call.args.actionId,'app.listing');assert.deepEqual(call.args.input,{listed:true});assert.equal(call.args.expectedRevision,4);
 assert.equal(backend.adminEffects,1);assert.equal(backend.adminAction.current.listed,true);assert.equal(h.panel.hasUnsavedChanges(),false);h.panel.dispose();
});

async function dirtyAdminOracle(code){
 const backend=adminService(),h=harness({code,backend});try{await h.panel.ready;const input=h.field('listed');input.checked=true;input.emit('input');
  backend.adminAction.revision=5;await h.panel.refresh();assert.equal(input.checked,true,'A passive refresh must keep typed values');
  h.node('mon-admin-action').emit('submit');h.node('mon-action-confirm').children[2].click();await settle();
  const writes=backend.calls.filter(call=>call.op==='monitoring.admin.invoke');assert.equal(writes.length,1);
  assert.equal(writes[0].args.expectedRevision,4,'The typed draft must keep its original revision');assert.equal(backend.adminEffects,0);
 }finally{h.panel.dispose();}
}
test('passive admin metadata never rebases a dirty edit onto a newer action revision',()=>dirtyAdminOracle(source));
test('negative control: silently rebasing a dirty admin draft fails the same actual handler oracle',async()=>{
 const anchor='else{record.action=action;if(!record.dirty&&!intent)resetAction(record);}';assert.equal(source.split(anchor).length,2);
 const broken=source.replace(anchor,'else{record.action=action;if(record.dirty)record.baseRevision=action.revision;if(!record.dirty&&!intent)resetAction(record);}');
 await assert.rejects(dirtyAdminOracle(broken),/The typed draft must keep its original revision/u);
});

test('admin lost ACK recovery after reopening queries the exact old command receipt and does not repeat its effect',async()=>{
 const backend=adminService(),storage=storagePort();backend.adminMode='lost-ack';const first=harness({backend,storage});await first.panel.ready;
 first.field('listed').checked=true;first.field('listed').emit('input');first.node('mon-admin-action').emit('submit');first.node('mon-action-confirm').children[2].click();await settle();
 assert.equal(backend.adminEffects,1);assert.equal(storage.rows.size,1);assert.equal(first.node('mon-admin-pending').hidden,false);first.panel.dispose();
 const second=harness({backend,storage});await second.panel.ready;second.node('mon-admin-pending').children[1].children[0].click();await settle();
 const calls=backend.calls.filter(call=>call.op==='monitoring.admin.invoke');assert.equal(calls.length,2);assert.deepEqual(calls[0].args,calls[1].args);assert.equal(calls[1].args.retry,false);
 assert.equal(backend.adminEffects,1);assert.equal(storage.rows.size,0);second.panel.dispose();
});

test('unknown admin outcome cannot execute again; a canonical not-applied outcome requires a separate explicit send',async()=>{
 for(const mode of ['unknown','missing']){const backend=adminService();backend.adminMode=mode;const h=harness({backend});await h.panel.ready;
  h.field('listed').checked=true;h.field('listed').emit('input');h.node('mon-admin-action').emit('submit');h.node('mon-action-confirm').children[2].click();await settle();
  const buttons=h.node('mon-admin-pending').children[1],send=buttons.children[1];assert.equal(backend.adminEffects,0);
  if(mode==='unknown'){assert.equal(send.hidden,true);const count=backend.calls.length;send.click();await settle();assert.equal(backend.calls.length,count);assert.equal(backend.adminEffects,0);}
  else{assert.equal(send.hidden,false);send.click();await settle();const calls=backend.calls.filter(call=>call.op==='monitoring.admin.invoke');assert.equal(calls.length,2);assert.equal(calls[1].args.requestId,calls[0].args.requestId);assert.equal(calls[1].args.expectedRevision,4);assert.equal(calls[1].args.retry,true);assert.equal(backend.adminEffects,1);}
  h.panel.dispose();
 }
});

test('admin integer bounds and unknown current fields never dispatch an invented default',async()=>{
 const backend=service({kind:'platform'});backend.adminAction={id:'platform.admission',label:'Подключение к Сотам',right:'admin',enabled:true,revision:2,current:{defaultQuota:5},fields:[{name:'defaultQuota',label:'Начальный лимит',type:'integer',minimum:1,maximum:100}]};
 const h=harness({backend});await h.panel.ready;const quota=h.field('defaultQuota');quota.value='999';quota.emit('input');h.node('mon-admin-action').emit('submit');assert.equal(h.node('mon-action-confirm').hidden,true);assert.equal(backend.adminEffects,0);
 quota.value='7';quota.emit('input');h.node('mon-admin-action').emit('submit');h.node('mon-action-confirm').children[2].click();await settle();assert.equal(backend.adminAction.current.defaultQuota,7);h.panel.dispose();
 const missing=adminService();missing.adminAction.current={};const noDefault=harness({backend:missing});await noDefault.panel.ready;
 noDefault.field('listed').checked=true;noDefault.field('listed').emit('input');noDefault.node('mon-admin-action').emit('submit');noDefault.node('mon-action-confirm').children[2].click();await settle();assert.equal(missing.adminEffects,0);assert.equal(missing.calls.some(call=>call.op==='monitoring.admin.invoke'),false);noDefault.panel.dispose();
});

test('an old admin confirmation cannot run after permissions or field schema changed',async()=>{
 for(const change of ['permission','schema']){const backend=adminService(),h=harness({backend});await h.panel.ready;
  h.field('listed').checked=true;h.field('listed').emit('input');h.node('mon-admin-action').emit('submit');const confirm=h.node('mon-action-confirm').children[2];
  if(change==='permission')backend.permissions.admin=false;else backend.adminAction.fields=[{name:'listed',label:'Новый выбор',type:'enum',options:['yes','no']}];
  await h.panel.refresh();confirm.emit('click');await settle();assert.equal(backend.adminEffects,0);assert.equal(backend.calls.some(call=>call.op==='monitoring.admin.invoke'),false);h.panel.dispose();
 }
});

test('host flush ignores held read-only queries, preserves typed admin edits and waits for an actual command ACK',async()=>{
 const read=harness();await read.panel.ready;const held=deferred();read.backend.nextQuery=held;
 const query=read.panel.refresh();await settle();let drained=false;
 const drain=read.panel.flush().then(()=>{drained=true;});await settle();
 try{assert.equal(drained,true,'A read-only chart request must not block closing or switching account');}
 finally{held.resolve();await query;await drain;read.panel.dispose();}

 const dirty=harness({backend:adminService()});await dirty.panel.ready;dirty.field('listed').checked=true;dirty.field('listed').emit('input');
 try{assert.equal(dirty.panel.hasUnsavedChanges(),true);await assert.rejects(dirty.panel.flush(),{code:'monitoring_pending_unconfirmed'});assert.equal(dirty.backend.adminEffects,0);}
 finally{dirty.panel.dispose();}

 const backend=service(),ack=deferred(),original=backend.request;
 backend.request=async(op,args)=>{const result=await original(op,args);if(op==='monitoring.config.put')await ack.promise;return result;};
 const write=harness({backend});await write.panel.ready;write.node('mon-collection').children[0].checked=false;write.node('mon-collection').children[0].emit('change');await settle();
 assert.equal(write.panel.hasUnconfirmedWork(),true);let acknowledged=false;const commit=write.panel.flush().then(()=>{acknowledged=true;});await settle();
 try{assert.equal(acknowledged,false,'An in-flight mutation must settle before the host can finish flushing');ack.resolve();await commit;assert.equal(write.panel.hasUnsavedChanges(),false);assert.equal(write.panel.hasUnconfirmedWork(),false);}
 finally{ack.resolve();write.panel.dispose();}
});

function comparisonBackend(){
 const backend=service(),description=backend.description;const start=now-3600000,end=now+60000,span=3660000;
 backend.description=()=>({...description(),generatedAt:now+10000,metrics:description().metrics.map(metric=>({...metric,id:'http.requests'}))});
 backend.query=args=>{const previous=args.to<=start,from=previous?start-span:start,to=previous?start:end;
  const points=Array.from({length:61},(_,index)=>({at:from+index*60000,value:previous?(index===0?7:index===1?0:null):(index===0?0:index===60?12:null),samples:previous?(index<=1?1:0):(index===0||index===60?1:0)}));
  return{schema:'soty.monitoring.query.v1',scope:clone(backend.scope),generatedAt:now+10000,config:clone(backend.config),series:[{metricId:args.metricIds[0],unit:'count',kind:'counter',source:'host',status:'partial',points,
   coverage:{from,to,observedPoints:2,totalPoints:61,oldestAt:from},lastAt:previous?from+60000:now}]};};return backend;
}
async function comparisonOracle(models=model){
 const backend=comparisonBackend(),h=harness({backend,models});await h.panel.ready;
 try{const compare=h.node('mon-compare');assert.ok(compare,'Period must offer opt-in previous-period comparison');compare.click();await settle();
  const calls=backend.calls.filter(call=>call.op==='monitoring.query').slice(-2);assert.equal(calls.length,2);
  assert.deepEqual({from:calls[1].args.from,to:calls[1].args.to},{from:now-7260000,to:now-3600000},'Previous query must be the adjacent equally long displayed minute range, without an overlapping boundary bucket');
  assert.ok(calls.every(call=>call.args.maxPoints===120&&call.args.metricIds.length===1&&call.args.metricIds[0]==='http.requests'));
  const plot=h.plots.at(-1);assert.equal(plot.data.length,3);assert.equal(plot.data[0].length,61);assert.deepEqual([plot.data[1][0],plot.data[1][1],plot.data[1][60]],[0,null,12]);assert.deepEqual([plot.data[2][0],plot.data[2][1],plot.data[2][60]],[7,0,null],'The previous trace must retain its measured zero and unobserved gap');
  assert.equal(plot.options.series[2].spanGaps,false);assert.deepEqual(plot.options.series[2].dash,[6,4]);
  h.node('mon-chart').emit('keydown',{key:'Home'});assert.equal(h.node('mon-value').textContent,'0');assert.equal(h.node('mon-previous-value').textContent,'7');
  h.node('mon-chart').emit('keydown',{key:'ArrowRight'});assert.equal(h.node('mon-value').textContent,'Нет данных');assert.equal(h.node('mon-previous-value').textContent,'0');
  assert.equal(h.timers.size,1,'Comparison uses the existing visible refresh timer');assert.equal(h.panel.hasUnsavedChanges(),false);
 }finally{h.panel.dispose();}
}
test('opt-in comparison queries the previous equal minute range and plots real values, aligned timestamps and genuine gaps',()=>comparisonOracle());
test('broken previous-range and null-as-zero projections fail the SAME full comparison oracle',async()=>{
 await assert.rejects(comparisonOracle({...model,monitoringPreviousWindow:coverage=>({from:coverage.from,to:coverage.to})}),/adjacent equally long displayed minute range/u);
 await assert.rejects(comparisonOracle({...model,monitoringComparisonData:(a,b)=>{const result=model.monitoringComparisonData(a,b);result.data[2]=result.data[2].map(value=>value??0);return result;}}),/previous trace must retain/u);
});
test('platform and application collection hints name their actual scope',async()=>{
 for(const scope of[{kind:'platform'},{kind:'app',appId:'app-'+'b'.repeat(32)}]){const h=harness({backend:service(scope)});await h.panel.ready;
  const hint=h.node('mon-management-body').children[1].textContent;assert.match(hint,scope.kind==='platform'?/показателей Сот/u:/этого приложения/u);h.panel.dispose();}
});
test('turning comparison off retires an older held previous query and never revives its graph or a second timer',async()=>{
 const backend=comparisonBackend(),held=deferred(),original=backend.request;let waiting=false;
 backend.request=async(op,args)=>{const result=await original(op,args);if(op==='monitoring.query'&&args.to===now-3600000&&!waiting){waiting=true;await held.promise;}return result;};
 const h=harness({backend});await h.panel.ready;h.node('mon-compare').click();await settle();assert.equal(waiting,true);h.node('mon-compare').click();await settle();
 assert.equal(h.plots.at(-1).data.length,2);assert.equal(h.node('mon-previous-readout').hidden,true);held.resolve();await settle();assert.equal(h.plots.at(-1).data.length,2);assert.equal(h.node('mon-previous-readout').hidden,true);assert.equal(h.timers.size,1);h.panel.dispose();
});
test('comparison rejects mismatched metadata and access loss clears both traces, including a held previous response',async()=>{
 for(const mode of ['unit','permission','account']){const backend=comparisonBackend(),original=backend.request,held=deferred();let waiting=false;
  backend.request=async(op,args)=>{const result=await original(op,args);if(op==='monitoring.query'&&args.to===now-3600000){if(mode==='unit')result.series[0].unit='ms';else{waiting=true;await held.promise;}}return result;};
  const h=harness({backend});await h.panel.ready;h.node('mon-compare').click();await settle();
  if(mode!=='unit'){assert.equal(waiting,true);if(mode==='permission'){backend.mode='denied';await h.panel.refresh();}else h.accountChanged();held.resolve();await settle();}
  assert.equal(h.node('mon-value').textContent,'—');assert.equal(h.node('mon-previous-value').textContent,'');assert.equal(h.node('mon-previous-readout').hidden,true);assert.equal(h.node('mon-table').children[1].children.length,0);if(mode==='unit')assert.match(h.node('mon-error').textContent,/сопоставить/u);h.panel.dispose();
 }
});
test('previous-only and wholly empty periods remain honest: no zero invention, NaN time or hidden previous observations',async()=>{
 for(const previousEmpty of [false,true]){const backend=comparisonBackend(),query=backend.query;
  backend.query=args=>{const result=query(args);if(args.to>now-3600000||previousEmpty){const value=result.series[0];value.points=value.points.map(point=>({...point,value:null,samples:0}));value.status='no-data';value.lastAt=null;value.coverage.observedPoints=0;}return result;};
  const h=harness({backend});await h.panel.ready;h.node('mon-compare').click();await settle();assert.equal(h.node('mon-value').textContent,'Нет данных');assert.equal(h.node('mon-previous-value').textContent,previousEmpty?'Нет данных':'0');assert.equal(h.node('mon-chart').hidden,previousEmpty);assert.doesNotMatch(h.node('mon-point-time').textContent,/Invalid|NaN/u);h.panel.dispose();
 }
});
test('view comparison and retained hide/resume do not dispatch writes, rebase dirty admin input, or poll while hidden',async()=>{
 const h=harness({backend:adminService()});await h.panel.ready;const input=h.field('listed');input.checked=true;input.emit('input');h.node('mon-compare').click();await settle();
 assert.equal(h.field('listed'),input);assert.equal(input.checked,true);assert.equal(h.panel.hasUnsavedChanges(),true);assert.equal(h.backend.adminEffects,0);
 h.panel.setVisible(false);const count=h.backend.calls.length;await h.panel.refresh();assert.equal(h.backend.calls.length,count);assert.equal(h.timers.size,0);h.panel.setVisible(true);await settle();assert.equal(h.field('listed'),input);assert.equal(input.checked,true);assert.equal(h.plots.at(-1).data.length,3);assert.equal(h.backend.calls.some(call=>call.op==='monitoring.admin.invoke'||call.op==='monitoring.config.put'),false);await assert.rejects(h.panel.flush(),{code:'monitoring_pending_unconfirmed'});h.panel.dispose();
});
test('a changed metric, custom range and return to Live clear both old traces before the new network result',async()=>{
 const backend=comparisonBackend(),description=backend.description;backend.description=()=>{const value=description();return{...value,metrics:[...value.metrics,{...value.metrics[0],id:'http.errors',label:'Ошибки'}]};};
 const h=harness({backend});await h.panel.ready;h.node('mon-compare').click();await settle();assert.equal(h.node('mon-previous-readout').hidden,false);
 const held=deferred();backend.nextQuery=held;h.node('mon-metric').value='http.errors';h.node('mon-metric').emit('change');assert.equal(h.node('mon-value').textContent,'—');assert.equal(h.node('mon-previous-readout').hidden,true);assert.equal(h.node('mon-table').children[1].children.length,0);await settle();held.resolve();await settle();assert.match(h.node('mon-chart').getAttribute('aria-label'),/Ошибки/u);
 h.node('mon-range').value='custom';h.node('mon-range').emit('change');assert.equal(h.node('mon-previous-readout').hidden,true);assert.equal(h.node('mon-value').textContent,'—');
 const localDate=value=>{const date=new Date(value),pad=part=>String(part).padStart(2,'0');return`${date.getFullYear()}-${pad(date.getMonth()+1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;};
 const form=h.node('mon-custom');form.children[0].children[1].value=localDate(now-3600000);form.children[1].children[1].value=localDate(now+60000);form.emit('submit');await settle();assert.equal(h.node('mon-previous-readout').hidden,false,'A real loaded custom comparison must exist before returning to Live');
 const liveReply=deferred();backend.nextQuery=liveReply;h.node('mon-live').click();assert.equal(h.node('mon-range').value,'hour');assert.equal(h.node('mon-custom').hidden,true);assert.equal(h.node('mon-previous-readout').hidden,true,'Both old custom traces disappear before the hour reply');await settle();liveReply.resolve();await settle();assert.equal(h.node('mon-previous-readout').hidden,false);assert.equal(h.timers.size,1);h.panel.dispose();
});

async function visualTimeOracle(code=source){
 const h=harness({code});await h.panel.ready;
 try{
  const axis=h.plots[0].options.axes[0];assert.equal(typeof axis.values,'function','Russian UI must not fall back to uPlot English dates/12h');
  const tick=Date.parse('2026-11-05T13:15:00Z')/1000,labels=axis.values(h.plots[0],[tick,tick+300]);
  assert.doesNotMatch(labels.join(' '),/am|pm|11\/5\/26/iu);assert.match(labels[0],/нояб/u);assert.match(labels[1],/\d{2}:\d{2}/u);
  const actual=h.backend.calls.find(call=>call.op==='monitoring.query').args;
  assert.equal(h.node('mon-period-caption')?.textContent,model.monitoringPeriod(actual),'The full actual coverage must be visible independently of the preset select');
  assert.equal(h.node('mon-range').children.find(option=>option.value==='hour').textContent,'За час');
 }finally{h.panel.dispose();}
}
test('axis uses Russian 24-hour ticks and a full actual period remains visible beside a compact preset',()=>visualTimeOracle());

test('point meaning follows actual interval boundaries and metric aggregation, and is cleared with the old data',async()=>{
 const backend=service(),query=backend.query;
 backend.query=args=>{const result=query(args),row=result.series[0];row.points=[{at:args.from,value:27,samples:27},{at:args.from+60000,value:null,samples:0}];row.coverage.to=args.from+120000;row.coverage.totalPoints=2;return result;};
 const h=harness({backend});await h.panel.ready;
 try{
  assert.equal(h.node('mon-point-meaning')?.textContent,'За 1 мин','27 is an interval count, never the total requested hour');
  assert.match(h.node('mon-point-time').textContent,/^\d{2}:\d{2}$/u);
  h.node('mon-range').value='custom';h.node('mon-range').emit('change');
  assert.equal(h.node('mon-period-caption').textContent,'');assert.equal(h.node('mon-point-meaning').textContent,'');
  assert.equal(h.backend.calls.filter(call=>call.op==='monitoring.query').length,1);
 }finally{h.panel.dispose();}
});

const postcss=createRequire(pathToFileURL(realpathSync(new URL('../../node_modules/vite/package.json',import.meta.url))))('postcss');
const sharedFormRules=postcss.parse(readFileSync(new URL('./world.css',import.meta.url),'utf8')).nodes.filter(node=>node.type==='rule'&&node.selector==='.sw-dialog form');
assert.equal(sharedFormRules.length,1);
// Only the actual simple descendant selectors in this defect; browser geometry
// and optical surfaces are verified independently by Root's actual CSS control.
function customFormCascade(styles,width,classes){
 const lineage=[{tag:'dialog',classes:['sw-dialog']},{tag:'section',classes:['mon-panel']},{tag:'form',classes}],out={};let order=0;
 const compound=(part,node)=>{if(!/^(?:[a-z][\w-]*)?(?:\.[\w-]+)*$/iu.test(part))return false;const tag=/^[a-z][\w-]*/iu.exec(part)?.[0];return(!tag||tag===node.tag)&&[...part.matchAll(/\.([\w-]+)/gu)].every(value=>node.classes.includes(value[1]));};
 const matches=selector=>{const parts=selector.trim().split(/\s+/u);if(!compound(parts.pop(),lineage.at(-1)))return false;let index=lineage.length-2;while(parts.length){const part=parts.pop();while(index>=0&&!compound(part,lineage[index]))index--;if(index--<0)return false;}return true;};
 postcss.parse(styles).walkRules(rule=>{let active=true;for(let parent=rule.parent;parent;parent=parent.parent)if(parent.type==='atrule'){
  const bounds=[...parent.params.matchAll(/\(max-(width|height):\s*(\d+)px\)/gu)];if(parent.name!=='media'||!bounds.length||bounds.some(([,axis,limit])=>(axis==='width'?width:900)>Number(limit)))active=false;
 }if(!active)return;for(const selector of rule.selector.split(',')){if(!matches(selector))continue;
  const rank=(selector.match(/\.[\w-]+/gu)??[]).length*100+selector.trim().split(/\s+/u).filter(part=>/^[a-z]/iu.test(part)).length;
  for(const decl of rule.nodes){if(decl.type!=='decl')continue;order++;const old=out[decl.prop],score=rank+(decl.important?1000000:0);if(!old||score>old.score||score===old.score&&order>old.order)out[decl.prop]={value:decl.value,score,order};}
 }});return Object.fromEntries(Object.entries(out).map(([name,item])=>[name,item.value]));
}
test('actual shared form rule cannot turn component custom dates into an end-aligned flex column in either stylesheet order',()=>{
 const generic=sharedFormRules[0].toString();
 for(const width of[1280,390])for(const styles of[generic+'\n'+css,css+'\n'+generic]){
  const style=customFormCascade(styles,width,['mon-custom']);assert.equal(style.display,'grid');assert.equal(style.gap,'10px');
  assert.equal(style['grid-template-columns'],width===1280?'minmax(0,1fr) minmax(0,1fr) auto':'minmax(0,1fr)');
  assert.equal(customFormCascade(styles,width,[]).display,'flex','An unrelated dialog form keeps its existing generic behavior');
 }
});
test('admin form retains component spacing without changing the shared generic form',()=>{
 const generic=sharedFormRules[0].toString();
 for(const styles of[generic+'\n'+css,css+'\n'+generic]){assert.equal(customFormCascade(styles,1280,['mon-admin-action']).gap,'10px');assert.equal(customFormCascade(styles,1280,[]).gap,'19px');}
});
