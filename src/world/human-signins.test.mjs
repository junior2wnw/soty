import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';
const source=await readFile(new URL('./human-signins.ts',import.meta.url),'utf8');
const compiled=ts.transpileModule(source,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
const turn=()=>new Promise(done=>setImmediate(done));
const deferred=()=>{let resolve;const promise=new Promise(done=>{resolve=done;});return{promise,resolve};};
const caps={schema:'soty.human-signins-capability.v1',available:true,sessions:true,rememberedConsents:true,checkRevoke:true,scopes:['current-device','account'],sourceRootSessions:false,maximumPageSize:20};
const family={entryId:'E'.repeat(43),kind:'oidc-session',clientId:'oidc-client-not-a-root-app',clientLabel:'Личный архив',deviceId:'device-A',currentDevice:true,
  clientGeneration:3,createdAt:1800000000000,expiresAt:1800000300000,status:'active',renewable:true,remembered:false,revision:'a'.repeat(64),revokeScope:'exact-session',revokeRequestId:'R'.repeat(43)};
const consent={...family,entryId:'C'.repeat(43),kind:'remembered-consent',expiresAt:null,renewable:false,remembered:true,revokeScope:'app-device-version',revokeRequestId:'S'.repeat(43)};
class Element extends EventTarget{
  constructor(tag,text=''){super();Object.assign(this,{tagName:tag,ownText:text,children:[],dataset:{},attributes:new Map(),isConnected:true,disabled:false});}
  get textContent(){return this.ownText+this.children.map(child=>child.textContent).join(' ');}
  append(...children){for(const child of children){child.parent=this;this.children.push(child);}}
  replaceChildren(...children){this.children=[];this.append(...children);}
  setAttribute(key,value){this.attributes.set(key,String(value));}
  getAttribute(key){return this.attributes.get(key);}
  remove(){this.isConnected=false;if(this.parent)this.parent.children=this.parent.children.filter(child=>child!==this);}
  all(){return[this,...this.children.flatMap(child=>child.all())];}
  querySelector(selector){const key=/^\[data-signin-control="([^"]+)"\]$/u.exec(selector)?.[1];return this.all().find(node=>node.dataset.signinControl===key)??null;}
  focus(){this.focused=true;}
  click(){if(!this.disabled)this.dispatchEvent(new Event('click'));}
}
function memory(){const values=new Map();return{values,getItem:key=>values.get(key)??null,setItem:(key,value)=>values.set(key,value),removeItem:key=>values.delete(key)};}
function mount({account='account-A',device='device-A',storage=memory(),capability=caps,rows=[family,consent],request,readCapability,fetcher}={}){
  const host=new Element('main'),calls=[];let current=true,revoked=false;
  const make=(tag,_class='',text='')=>new Element(tag,text);
  const module={exports:{}};
  vm.runInNewContext(compiled,{module,exports:module.exports,AbortSignal,fetch:fetcher,sessionStorage:storage,
    require:id=>id==='./dom'?{el:make,button(label,_icon,_class,run){const node=make('button','',label);node.addEventListener('click',run);return node;}}:{}});
  const api={async request(method,args){calls.push({method,args:structuredClone(args)});
    if(request)return request(method,structuredClone(args),{calls,storage});
    if(method.endsWith('.list'))return{schema:'soty.human-signins.v1',scope:args.scope,capabilities:capability,entries:revoked?[]:rows,nextCursor:null};
    if(method.endsWith('.checkRevoke'))return{schema:'soty.human-signin-status.v1',...args,status:revoked?'revoked':'active',revokedAt:revoked?1800000001000:null};
    revoked=true;const row=rows.find(row=>row.entryId===args.entryId);
    return{schema:'soty.human-signin-revocation.v1',...args,kind:row.kind,revokeScope:row.revokeScope,outcome:'revoked',revokedAt:1800000001000};}};
  const handle=module.exports.mountHumanSignins({host,api,accountId:account,deviceId:device,accountLabel:'Мой профиль',isCurrent:()=>current,storage,
    ...(fetcher?{}:{readCapability:readCapability??(async()=>capability)})});
  return{host,handle,calls,storage,retire(){current=false;},control:key=>host.all().find(node=>node.dataset.signinControl===key),
    async settled(){for(let i=0;i<8;i++)await turn();},text:()=>host.textContent};
}
const list=(args,rows,extra={})=>({schema:'soty.human-signins.v1',scope:args.scope,capabilities:caps,entries:rows,nextCursor:null,...extra});
const intent=(row=family,scope='current-device')=>({expectedAccountId:'account-A',scope,entryId:row.entryId,expectedRevision:row.revision,requestId:row.revokeRequestId});

test('current device is the default; only explicit all-my-devices click broadens the signed read',async()=>{
  const f=mount({request:async(method,args)=>list(args,[family])});await f.settled();
  assert.deepEqual(f.calls[0],{method:'identity.human.signins.list',args:{expectedAccountId:'account-A',scope:'current-device',limit:20}});
  assert.equal(f.control('scope-current-device').getAttribute('aria-pressed'),'true');
  f.control('scope-account').click();await f.settled();assert.equal(f.calls.at(-1).args.scope,'account');
  assert.match(f.text(),/Входы через Соты/u);assert.match(f.text(),/Запомненные согласия/u);
  assert.equal(f.host.all().some(node=>node.tagName==='a'),false,'an RP clientId never becomes an app URL');
});

test('revoke saves the exact server-issued intent BEFORE the signed call and reviews one-family scope',async()=>{
  let storedAtSend=false,revoked=false;
  const f=mount({request:async(method,args,{storage})=>{
    if(method.endsWith('.list'))return list(args,revoked?[]:[family,consent]);
    storedAtSend=[...storage.values.values()].some(raw=>JSON.parse(raw).intent.requestId===family.revokeRequestId);revoked=true;
    return{schema:'soty.human-signin-revocation.v1',...args,kind:family.kind,revokeScope:family.revokeScope,outcome:'revoked',revokedAt:1800000001000};}});
  await f.settled();f.control('revoke-0').click();assert.match(f.text(),/Запомненное согласие и другие входы сохранятся/u);
  f.control('confirm').click();await f.settled();
  assert.equal(storedAtSend,true);assert.deepEqual(f.calls.find(call=>call.method.endsWith('.revoke')).args,intent());
  assert.equal(f.storage.values.size,0);assert.match(f.text(),/Сервер подтвердил отзыв/u);
});

test('forget explicitly describes same app/device/version families; request carries no substituted device/client selector',async()=>{
  const f=mount({rows:[consent]});await f.settled();f.control('revoke-0').click();assert.match(f.text(),/Также будут отозваны входы этой версии приложения/u);
  f.control('confirm').click();await f.settled();assert.deepEqual(f.calls.find(call=>call.method.endsWith('.revoke')).args,intent(consent));
});

test('unavailable or quota-limited pending storage sends ZERO revocations',async()=>{
  const storage=memory();storage.setItem=()=>{throw Error('synthetic quota');};
  const f=mount({storage});await f.settled();f.control('revoke-0').click();f.control('confirm').click();await f.settled();
  assert.equal(f.calls.filter(call=>call.method.endsWith('.revoke')).length,0);assert.match(f.text(),/Отзыв не отправлен/u);
  const unavailable=mount({storage:null});await unavailable.settled();assert.equal(unavailable.control('revoke-0').disabled,true);
  assert.match(unavailable.text(),/Список можно читать; отзыв не отправляется/u);assert.equal(unavailable.calls.filter(call=>call.method.endsWith('.revoke')).length,0);
  const silent=memory();silent.setItem=()=>{};const dropped=mount({storage:silent});await dropped.settled();dropped.control('revoke-0').click();dropped.control('confirm').click();await dropped.settled();
  assert.equal(dropped.calls.filter(call=>call.method.endsWith('.revoke')).length,0);assert.match(dropped.text(),/Отзыв не отправлен/u);
});

test('lost ACK then reload uses ONLY readonly checkRevoke with the exact stored args; no automatic second write',async()=>{
  const storage=memory();let applied=false,writes=0,checks=0;
  const request=async(method,args)=>{
    if(method.endsWith('.list'))return list(args,applied?[]:[family]);
    if(method.endsWith('.checkRevoke')){checks++;assert.deepEqual(args,intent());return{schema:'soty.human-signin-status.v1',scope:args.scope,entryId:args.entryId,requestId:args.requestId,status:'revoked',revokedAt:1800000001000};}
    writes++;applied=true;throw Error('synthetic lost ACK');};
  const first=mount({storage,request});await first.settled();first.control('revoke-0').click();first.control('confirm').click();await first.settled();
  assert.equal(storage.values.size,1);assert.match(first.text(),/Отзыв пока не подтверждён/u);first.handle.dispose();
  const reloaded=mount({storage,request});await reloaded.settled();assert.equal(writes,1);assert.equal(checks,1);assert.equal(storage.values.size,0);assert.match(reloaded.text(),/Сервер подтвердил отзыв/u);
});

test('active and unavailable readonly states preserve unknown; only explicit local dismissal drops the reference',async()=>{
  for(const status of ['active','unavailable']){
    const storage=memory();let writes=0;
    const request=async(method,args)=>{if(method.endsWith('.list'))return list(args,[family]);if(method.endsWith('.checkRevoke'))return{schema:'soty.human-signin-status.v1',scope:args.scope,entryId:args.entryId,requestId:args.requestId,status,revokedAt:null};writes++;throw Error('lost');};
    const a=mount({storage,request});await a.settled();a.control('revoke-0').click();a.control('confirm').click();await a.settled();a.handle.dispose();
    const b=mount({storage,request});await b.settled();assert.equal(writes,1);assert.equal(storage.values.size,1);assert.doesNotMatch(b.text(),/Сервер подтвердил отзыв/u);
    b.control('check').click();await b.settled();assert.equal(writes,1);assert.equal(storage.values.size,1);
    b.control('dismiss-pending').click();assert.equal(storage.values.size,0);assert.match(b.text(),/Входы на сервере не изменялись/u);
  }
});

test('pending references never cross account or device; old callbacks fail even on A→B→A',async()=>{
  const storage=memory(),a=mount({storage,request:async(method,args)=>method.endsWith('.list')?list(args,[family]):Promise.reject(Error('lost'))});
  await a.settled();a.control('revoke-0').click();const oldConfirm=a.control('confirm');oldConfirm.click();await a.settled();a.retire();oldConfirm.click();
  for(const[account,device]of[['account-B','device-A'],['account-A','device-B']]){
    const b=mount({account,device,storage,rows:[{...family,deviceId:device}]});await b.settled();assert.equal(b.calls.some(call=>call.method.endsWith('.checkRevoke')),false);assert.doesNotMatch(b.text(),/Прежний отзыв/u);b.handle.dispose();
  }
  assert.equal(storage.values.size,1);assert.equal(a.calls.filter(call=>call.method.endsWith('.revoke')).length,1);
});

test('late list response after identity retirement cannot render private rows or authorize an old control',async()=>{
  const pending=deferred(),f=mount({request:()=>pending.promise});await turn();f.retire();pending.resolve(list({scope:'current-device'},[family]));await f.settled();
  assert.equal(f.host.children.length,0);assert.equal(f.calls.length,1);
});

test('schema1/unconfigured and unknown capabilities stay honest, with no list or revoke action',async()=>{
  for(const capability of [{...caps,available:false,sessions:false,rememberedConsents:false,checkRevoke:false,scopes:[],maximumPageSize:0},{schema:'future-unreviewed'}]){
    const f=mount({capability});await f.settled();assert.equal(f.calls.length,0);assert.equal(f.control('revoke-0'),undefined);assert.match(f.text(),/не включено|не подтвердил поддерживаемый формат/u);
  }
});

test('changed snapshot on next page clears old rows rather than mixing pages; scope change uses no old cursor',async()=>{
  const f=mount({request:async(method,args)=>{if(args.cursor)throw Object.assign(Error(),{code:'human_identity_signins_changed'});return list(args,[family],{nextCursor:'synthetic.opaque-cursor'});}});
  await f.settled();f.control('more').click();await f.settled();assert.match(f.text(),/Список изменился/u);assert.equal(f.control('revoke-0'),undefined);
  f.control('scope-account').click();await f.settled();assert.equal(f.calls.at(-1).args.scope,'account');assert.equal(Object.hasOwn(f.calls.at(-1).args,'cursor'),false);
});

test('malformed or foreign revoke acknowledgements stay unknown and retain the original request',async()=>{
  const f=mount({request:async(method,args)=>method.endsWith('.list')?list(args,[family]):{schema:'soty.human-signin-revocation.v1',...args,requestId:'X'.repeat(43),kind:family.kind,revokeScope:family.revokeScope,outcome:'revoked',revokedAt:1800000001000}});
  await f.settled();f.control('revoke-0').click();f.control('confirm').click();await f.settled();assert.equal(f.storage.values.size,1);assert.doesNotMatch(f.text(),/Сервер подтвердил отзыв/u);
});

test('the default capability read uses the verified Apps endpoint; null expiry and Source exclusions do not imply forever or no access',async()=>{
  let url,cache;const f=mount({rows:[{...family,expiresAt:null,clientLabel:'<img src=x onerror=bad()>'}],fetcher:async(input,options)=>{url=input;cache=options.cache;return{ok:true,json:async()=>({humanSignins:caps})};}});
  await f.settled();assert.equal(url,'/api/apps/capabilities');assert.equal(cache,'no-store');assert.match(f.text(),/это не бессрочный вход/u);assert.match(f.text(),/Пустой список не означает отсутствие доступа/u);
  assert.doesNotMatch(f.text(),/Root Source|Native/u);
  assert.equal(f.host.all().some(node=>node.tagName==='img'),false);
});
