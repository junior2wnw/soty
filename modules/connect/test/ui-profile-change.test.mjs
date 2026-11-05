import {createActivationFixture,password} from './support/activation-fixture.mjs';
import {watchDeviceEnrollment} from '../ui/device-enrollment.mjs';
import {mountCredentialLogin,mountCredentialSettings} from '../ui/credential-forms.mjs';
import test from 'node:test';import assert from 'node:assert/strict';import vm from 'node:vm';import{readFile}from'node:fs/promises';
const source=await readFile(process.env.SOTY_PROFILE_UI_SOURCE??new URL('../ui/index.mjs',import.meta.url),'utf8');
const compiled=source.replace(/^import \{.*\} from '\.\/(?:platform-invitations|device-enrollment|credential-forms)\.mjs';\r?\n/gmu,'').replace(/^export /gmu,'');
const turn=()=>new Promise(resolve=>setImmediate(resolve));
const deferred=()=>{let resolve,reject;return{promise:new Promise((yes,no)=>{resolve=yes;reject=no;}),resolve,reject};};
async function until(predicate){for(let n=0;n<40&&!predicate();n++)await turn();assert.ok(predicate(),'actual Connect panel settled');}

// Actual UI callbacks and renderer; deterministic DOM/local-client ports only.
// No browser, real identity transfer or credential/key storage is accessed.
function fixture({beforeProfileChange,initialTab='profiles',initialAccount='A',bootstrapOnOpen=false,pendingEnrollment=null,nativeConfirm=true,clientOverride}={}){
 let document;const timers=new Map();let timer=0;
 class Node{
  children=[];handlers=new Map();attributes=new Map();dataset={};disabled=false;open=false;parent=null;ownText='';
  constructor(tag){this.tagName=tag;}
  get ownerDocument(){return document;}
  get isConnected(){return this===document.body||!!this.parent?.isConnected;}
  set textContent(value){this.ownText=String(value);this.replaceChildren();}get textContent(){return this.ownText+this.children.map(n=>n.textContent).join('');}
  set innerHTML(markup){this.replaceChildren();for(const m of markup.matchAll(/<([a-z][a-z0-9]*)\b([^>]*)>/giu)){const n=new Node(m[1]);for(const a of m[2].matchAll(/([^\s=]+)(?:="([^"]*)")?/gu))n.setAttribute(a[1],a[2]??'');this.append(n);}}
  setAttribute(k,v){this.attributes.set(k,String(v));if(k==='class')this.className=String(v);if(k.startsWith('data-'))this.dataset[k.slice(5).replace(/-([a-z])/gu,(_,c)=>c.toUpperCase())]=String(v);}
  getAttribute(k){return this.attributes.get(k)??null;}
  querySelectorAll(s){return this.children.flatMap(n=>[...(s.startsWith('.')?n.className?.split(' ').includes(s.slice(1)):s.startsWith('[')?n.attributes.has(s.slice(1,-1)):n.tagName===s)?[n]:[],...n.querySelectorAll(s)]);}
  querySelector(s){return this.querySelectorAll(s)[0]??null;}
  append(...nodes){for(const n of nodes){n.parent=this;this.children.push(n);}}prepend(...nodes){for(const n of nodes)n.parent=this;this.children.unshift(...nodes);}
  replaceChildren(...nodes){for(const n of this.children)n.parent=null;this.children=[];this.append(...nodes);}
  addEventListener(type,fn){this.handlers.set(type,fn);}showModal(){this.open=true;}
  contains(n){return n===this||this.children.some(c=>c.contains(n));}
  close(){if(!this.open)return;this.open=false;this.handlers.get('close')?.();}remove(){if(this.contains(document.activeElement))document.activeElement=document.body;if(this.parent)this.parent.children=this.parent.children.filter(n=>n!==this);this.parent=null;}
  focus(){if(this.isConnected&&!this.disabled)document.activeElement=this;}
 }
 document={activeElement:null,body:null,createElement:tag=>new Node(tag)};document.body=new Node('body');
 const state={accountId:initialAccount,pendingEnrollment,profiles:[{accountId:'A',label:'A',active:initialAccount==='A'},{accountId:'B',label:'B',active:false}]},switches=[],mutations=[],calls=[];
 const client={async getLocalState(){calls.push('local');return structuredClone(state);},async status(){calls.push('status');return{accountId:state.accountId,label:state.accountId,devices:[]};},
  async contacts(){calls.push('contacts');return{contacts:[],incoming:[],outgoing:[],invitations:[],blocked:[]};},
  async switchProfile(id){switches.push(id);mutations.push('switch');state.accountId=id;state.profiles=state.profiles.map(p=>({...p,active:p.accountId===id}));},
  async previewEnrollment(){calls.push('preview');return{account:{accountId:'B',label:'B'},source:{label:'Own synthetic device'}};},
  async finishEnrollment(id,account){mutations.push('enrollment');assert.equal(account,'B');state.accountId=account;state.pendingEnrollment=null;},
  async recover(){mutations.push('recovery');state.accountId='B';},async bootstrap(){mutations.push('bootstrap');state.accountId='A';}};
 Object.assign(client,clientOverride);
 const context={document,structuredClone,watchDeviceEnrollment,mountCredentialLogin,mountCredentialSettings,AbortController,location:{origin:'https://profile-fixture.invalid'},confirm:()=>{calls.push('native-confirm');return nativeConfirm;},setTimeout(fn){const id=++timer;timers.set(id,fn);return id;},clearTimeout:id=>timers.delete(id),mountPlatformInvitations:()=>({destroy(){}}),module:{exports:{}}};
 vm.runInNewContext(compiled+'\nmodule.exports.open=openConnectPanel;',context);
 const panel=context.module.exports.open({client,bootstrapOnOpen,initialTab,...(beforeProfileChange===undefined?{}:{beforeProfileChange})});
 const dialog=panel.element,content=dialog.querySelector('.connect-content'),message=dialog.querySelector('.connect-message');
 const row=id=>content.querySelector('[data-profile-list]')?.children.find(n=>n.dataset.profileId===id);
 const click=async node=>{assert.ok(node,'real produced action exists');node.focus();return node.handlers.get('click')({preventDefault(){},currentTarget:node});};
 return{state,client,calls,switches,mutations,panel,dialog,content,message,document,row,click,async ready(){await until(()=>content.getAttribute('aria-busy')==='false');},
  async profiles(){await this.ready();await click(dialog.querySelectorAll('[data-tab]').find(n=>n.dataset.tab==='profiles'));await until(()=>!!row('B'));}};
}

test('declining actual saved-profile switch keeps account/work panel and restores selected-row focus',async t=>{
 const requests=[],f=fixture({beforeProfileChange:async value=>{requests.push({...value});return false;}});t.after(()=>f.panel.close());await f.ready();const row=f.row('B');
 await f.click(row);assert.deepEqual(f.switches,[],'Denied host consent must not switch the actual local client');assert.deepEqual(requests,[{fromAccountId:'A',toAccountId:'B'}]);assert.equal(f.state.accountId,'A');
 assert.equal(f.dialog.open,true);assert.equal(f.document.activeElement,row);assert.equal(row.disabled,false);assert.equal(f.row('A').disabled,true);
});
test('positive trusted consent switches once; absent callback preserves standalone behavior',async t=>{
 for(const accepted of [true,'absent']){let calls=0;const f=fixture(accepted==='absent'?{}:{beforeProfileChange:async()=>{calls++;return true;}});t.after(()=>f.panel.close());await f.ready();await f.click(f.row('B'));
  assert.deepEqual(f.switches,['B']);assert.equal(f.state.accountId,'B');assert.equal(f.dialog.open,false);assert.equal(calls,accepted==='absent'?0:1);}
});
test('false, rejected or unsupported consent never switches and leaves a retryable focused action',async t=>{
 for(const value of [false,undefined,'yes',Error('own callback failed')]){const f=fixture({beforeProfileChange:async()=>{if(value instanceof Error)throw value;return value;}});t.after(()=>f.panel.close());await f.ready();const row=f.row('B');await f.click(row);
  assert.deepEqual(f.switches,[]);assert.equal(f.dialog.open,true);assert.equal(f.document.activeElement,row);assert.equal(f.content.getAttribute('aria-busy'),'false');}
});
test('pending consent cannot switch after close, external identity change or target revocation/removal',async t=>{
 for(const mode of ['close','external','revoked','removed']){const gate=deferred();let entered=false;const f=fixture({beforeProfileChange:()=>{entered=true;return gate.promise;}});t.after(()=>f.panel.close());await f.ready();const row=f.row('B'),pending=f.click(row);await until(()=>entered);
  assert.equal(row.disabled,true);if(mode==='close')f.panel.close();if(mode==='external')f.state.accountId='C';if(mode==='revoked')f.state.profiles[1].revoked=true;if(mode==='removed')f.state.profiles.pop();
  gate.resolve(true);await pending;assert.deepEqual(f.switches,[],mode);assert.equal(f.state.accountId,mode==='external'?'C':'A');if(mode==='close')assert.notEqual(f.document.activeElement,row);}
});
test('all six initial tabs use the same mutation boundary without blocking contacts/settings opening',async t=>{
 for(const tab of ['profile','people','devices','recovery','data','profiles']){let calls=0;const f=fixture({initialTab:tab,beforeProfileChange:async()=>{calls++;return false;}});t.after(()=>f.panel.close());await f.profiles();assert.equal(calls,0,tab+' browsing is not a profile change');await f.click(f.row('B'));assert.equal(calls,1);assert.deepEqual(f.switches,[],tab);}
});

const enrollment={requestId:'synthetic_enrollment_12345678',label:'Own fixture',expiresAt:1800000000000};
async function activation(kind,beforeProfileChange,extra={}){
 const f=fixture({beforeProfileChange,initialTab:kind==='enrollment'?'devices':kind==='recovery'?'recovery':'profile',
  ...(kind==='enrollment'?{pendingEnrollment:enrollment}:kind==='bootstrap'?{initialAccount:null,bootstrapOnOpen:true}:{}),...extra});
 if(kind==='bootstrap')return{f,pending:null};await f.ready();
 if(kind==='enrollment'){
  const finish=f.content.querySelectorAll('button').find(n=>n.textContent==='Проверить подтверждение');await f.click(finish);
  return{f,action:()=>f.click(finish)};
 }
 f.content.querySelector('[data-recover-file]').files=[{size:2,async text(){return JSON.stringify({schema:'synthetic-file-only',accountId:'UNTRUSTED_FILE_ACCOUNT'});}}];
 return{f,action:()=>f.click(f.content.querySelector('[data-recover]'))};
}
test('enrollment preview is readable; activating its verified target uses the same guard',async t=>{
 let accepted=false;const requests=[],{f,action}=await activation('enrollment',async value=>{requests.push({...value});return accepted;});t.after(()=>f.panel.close());
 assert.equal(f.calls.filter(v=>v==='preview').length,1);assert.equal(requests.length,0);
 await action();assert.deepEqual(f.mutations,[]);assert.deepEqual(requests,[{fromAccountId:'A',toAccountId:'B'}]);
 accepted=true;await action();assert.deepEqual(f.mutations,['enrollment']);assert.equal(f.state.accountId,'B');
});
test('a failed enrollment host callback cannot activate and requires reviewing the preview again',async t=>{
 const {f,action}=await activation('enrollment',async()=>{throw Error('synthetic host failure');});t.after(()=>f.panel.close());await action();
 assert.deepEqual(f.mutations,[]);assert.ok(f.content.querySelectorAll('button').some(n=>n.textContent==='Проверить подтверждение'));
});
test('recovery keeps native file confirmation and never treats raw kit account text as authority',async t=>{
 for(const mode of ['native-cancel','deny','throw','accept']){const requests=[],{f,action}=await activation('recovery',async value=>{requests.push({...value});if(mode==='throw')throw Error('synthetic host failure');return mode==='accept';},{nativeConfirm:mode!=='native-cancel'});t.after(()=>f.panel.close());
  await action();assert.deepEqual(f.mutations,mode==='accept'?['recovery']:[],mode);assert.equal(f.calls.filter(v=>v==='native-confirm').length,1);
  assert.deepEqual(requests,mode==='native-cancel'?[]:[{fromAccountId:'A',toAccountId:null}]);}
});
test('bootstrap with a provided host guard cannot bypass denial or failure',async t=>{
 for(const mode of ['deny','throw','accept']){const requests=[],{f}=await activation('bootstrap',async value=>{requests.push({...value});if(mode==='throw')throw Error('synthetic host failure');return mode==='accept';});t.after(()=>f.panel.close());await f.ready();
  assert.deepEqual(requests,[{fromAccountId:null,toAccountId:null}]);assert.deepEqual(f.mutations,mode==='accept'?['bootstrap']:[],mode);}
});
test('enrollment, recovery and bootstrap recheck close/account after pending host consent',async t=>{
 for(const kind of ['enrollment','recovery','bootstrap'])for(const mode of ['close','external']){
  const gate=deferred();let entered=false;const {f,action}=await activation(kind,()=>{entered=true;return gate.promise;});t.after(()=>f.panel.close());const pending=action?.();await until(()=>entered);
  if(mode==='close')f.panel.close();else f.state.accountId='C';gate.resolve(true);await pending;if(kind==='bootstrap')await until(()=>f.content.getAttribute('aria-busy')==='false');
  assert.deepEqual(f.mutations,[],kind+'/'+mode);assert.equal(f.state.accountId,mode==='external'?'C':kind==='bootstrap'?null:'A');
 }
});
test('an enrollment request replaced during host consent cannot activate the old preview',async t=>{
 const gate=deferred();let entered=false;const {f,action}=await activation('enrollment',()=>{entered=true;return gate.promise;});t.after(()=>f.panel.close());const pending=action();await until(()=>entered);
 f.state.pendingEnrollment=null;gate.resolve(true);await pending;assert.deepEqual(f.mutations,[]);assert.equal(f.state.accountId,'A');
});
test('a provided malformed host guard fails before the panel can mutate anything',()=>{
 assert.throws(()=>fixture({beforeProfileChange:'unsupported'}),/invalid_profile_change_guard/u);
});
test('closing during the final identity read cannot switch after a positive host answer',async t=>{
 const gate=deferred();let reading=false,f;f=fixture({beforeProfileChange:async()=>{f.client.getLocalState=()=>{reading=true;return gate.promise;};return true;}});t.after(()=>f.panel.close());await f.ready();
 const pending=f.click(f.row('B'));await until(()=>reading);f.panel.close();gate.resolve(structuredClone(f.state));await pending;assert.deepEqual(f.switches,[]);
});

function entryFixture(beforeProfileChange,{approved=false}={}){
 const f=fixture({initialTab:'connect',beforeProfileChange}),request={requestId:'enroll_'+ 'q'.repeat(32),expiresAt:Date.now()+60000};
 Object.assign(f.state,{deviceId:'own-device-A',localRevision:1});
 f.client.startEnrollment=async()=>{f.calls.push('entry-start');f.state.pendingEnrollment={...request};f.state.localRevision++;return{...request};};
 f.client.previewEnrollment=async()=>{f.calls.push('entry-preview');return{...request,status:approved?'approved':'pending',account:approved?{accountId:'B',label:'B'}:null};};
 const complete=(kind,options)=>{assert.equal(options.signal.aborted,false);f.mutations.push(kind);Object.assign(f.state,{accountId:'B',deviceId:'own-device-B',localRevision:f.state.localRevision+1,pendingEnrollment:null});return{accountId:'B',deviceId:'own-device-B'};};
 f.client.finishEnrollment=async(_id,id,options)=>{assert.equal(id,'B');return complete('entry-enrollment',options);};
 f.client.loginWithCredential=async(_kind,_input,_label,options)=>complete('credential',options);
 f.client.recover=async(_kit,_label,options)=>complete('entry-recovery',options);
 f.submitCredential=async()=>{const form=f.content.querySelectorAll('form').find(n=>n.dataset.credentialKind==='password');assert.ok(form,'actual credential form is reachable');for(const input of form.querySelectorAll('input'))input.value='own-synthetic-input';return form.handlers.get('submit')({preventDefault(){},currentTarget:form});};
 f.submitFile=async()=>{const input=f.content.querySelector('[data-entry-file]');assert.ok(input,'actual recovery file entry exists');input.files=[{size:2,async text(){return '{}';}}];return input.handlers.get('change')({preventDefault(){},currentTarget:input});};
 return f;
}

test('credential login and entry-file recovery invoke the same strict host guard',async t=>{
 for(const kind of ['credential','file'])for(const consent of [false,undefined,true]){
  const requests=[],f=entryFixture(async change=>{requests.push({...change});return consent;});t.after(()=>f.panel.close());await f.ready();
  await(kind==='credential'?f.submitCredential():f.submitFile());
  assert.deepEqual(requests,[{fromAccountId:'A',toAccountId:null}],kind);
  assert.deepEqual(f.mutations,consent===true?[kind==='file'?'entry-recovery':'credential']:[],kind);
  assert.equal(f.state.accountId,consent===true?'B':'A');
  if(consent!==true){assert.equal(f.dialog.open,true);assert.equal(f.content.querySelector('[data-new-qr]').hidden,false);}
 }
});

test('credential login and entry-file recovery cannot activate after close or an external change during host consent',async t=>{
 for(const kind of ['credential','file'])for(const mode of ['close','external']){
  const gate=deferred();let entered=false;const f=entryFixture(()=>{entered=true;return gate.promise;});t.after(()=>f.panel.close());await f.ready();
  const pending=kind==='credential'?f.submitCredential():f.submitFile();await until(()=>entered);
  if(mode==='close')f.panel.close();else f.state.accountId='C';gate.resolve(true);await pending;
  assert.deepEqual(f.mutations,[],kind+'/'+mode);assert.equal(f.state.accountId,mode==='external'?'C':'A');
 }
});

test('automatic QR completion invokes the host guard before actual activation and keeps a denied request retryable',async t=>{
 for(const consent of [false,true]){
  const requests=[],f=entryFixture(async change=>{requests.push({...change});return consent;},{approved:true});t.after(()=>f.panel.close());await f.ready();
  await until(()=>requests.length>0);await until(()=>consent?f.mutations.length===1:f.content.querySelector('[data-new-qr]')?.hidden===false);
  assert.deepEqual(requests,[{fromAccountId:'A',toAccountId:'B'}]);assert.deepEqual(f.mutations,consent?['entry-enrollment']:[]);
  assert.equal(f.state.accountId,consent?'B':'A');if(!consent)assert.ok(f.state.pendingEnrollment,'denial must not erase the current pending recipient');
 }
});

test('automatic QR completion rejects a closed, replaced or changed context while host consent is pending',async t=>{
 for(const mode of ['close','external','request']){
  const gate=deferred();let entered=false;const f=entryFixture(()=>{entered=true;return gate.promise;},{approved:true});t.after(()=>f.panel.close());await until(()=>entered);assert.equal(f.content.getAttribute('aria-busy'),'true');
  if(mode==='close')f.panel.close();if(mode==='external')f.state.accountId='C';if(mode==='request')f.state.pendingEnrollment.requestId='enroll_'+ 'r'.repeat(32);
  gate.resolve(true);for(let n=0;n<20;n++)await turn();assert.deepEqual(f.mutations,[],mode);
 }
});

test('actual credential helper and signed SDK retain A consent when a microtask selects C before login starts',async t=>{
 const real=await createActivationFixture(t);let arm=false,switched;
 const clientOverride={...real.main,getLocalState:async()=>{const race=arm;arm=false;const state=await real.main.getLocalState();if(race)switched=real.main.switchProfile(real.c.accountId);return state;}};
 const f=fixture({initialTab:'connect',initialAccount:real.a.accountId,clientOverride,beforeProfileChange:async()=>{arm=true;return true;}});t.after(()=>f.panel.close());await f.ready();
 const form=f.content.querySelectorAll('form').find(n=>n.dataset.credentialKind==='password');assert.ok(form,'real rendered credential helper exists');
 const inputs=form.querySelectorAll('input');inputs.find(n=>n.type==='text').value=password.login;inputs.find(n=>n.type==='password').value=password.secret;
 const before=real.operations.filter(op=>op==='credentials.login').length;
 await form.handlers.get('submit')({preventDefault(){},currentTarget:form});await switched;
 assert.equal((await real.main.getLocalState()).accountId,real.c.accountId,'A consent cannot activate B after another explicit C selection');
 assert.equal(real.operations.filter(op=>op==='credentials.login').length,before,'stale A consent cannot register a recipient');
});

test('multiple local credential recipients produce one profile choice per account and prefer the active device',async t=>{
 const f=fixture({initialAccount:'B'});t.after(()=>f.panel.close());f.state.profiles.push({accountId:'B',label:'Active B',active:true},{accountId:'B',label:'Revoked B',revoked:true});await f.ready();
 const rows=f.content.querySelector('[data-profile-list]').children.filter(node=>node.dataset.profileId==='B');
 assert.equal(rows.length,1);assert.equal(rows[0].textContent,'Active B · текущий');assert.equal(rows[0].disabled,true);
});
