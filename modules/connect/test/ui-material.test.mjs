import {watchDeviceEnrollment} from '../ui/device-enrollment.mjs';
import {mountCredentialLogin,mountCredentialSettings} from '../ui/credential-forms.mjs';
import assert from 'node:assert/strict';
import test from 'node:test';
import nodeVm from 'node:vm';
import {readFile} from 'node:fs/promises';

const vm={runInNewContext(code,context,...rest){Object.assign(context,{watchDeviceEnrollment,mountCredentialLogin,mountCredentialSettings,AbortController});return nodeVm.runInNewContext(code,context,...rest);}};
class Element {
  children=[];handlers=new Map();attrs=new Map();dataset={};isConnected=true;value='';textContent='';
  constructor(tag){this.tagName=tag;}
  set innerHTML(markup){this.children=[];for(const match of markup.matchAll(/<([a-z][a-z0-9]*)\b([^>]*)>/giu)){const node=new Element(match[1]);node.ownerDocument=this.ownerDocument;for(const attr of match[2].matchAll(/([^\s=]+)(?:="([^"]*)")?/gu))node.setAttribute(attr[1],attr[2]??'');this.children.push(node);}}
  setAttribute(key,value){this.attrs.set(key,String(value));if(key==='class')this.className=String(value);if(key==='value')this.value=String(value);if(key.startsWith('data-'))this.dataset[key.slice(5)]=String(value);}
  getAttribute(key){return this.attrs.get(key)??null;}
  querySelectorAll(selector){return this.children.flatMap(node=>[...((selector.startsWith('.')?node.className?.split(' ').includes(selector.slice(1)):selector.startsWith('[')?node.attrs.has(selector.slice(1,-1)):node.tagName===selector)?[node]:[]),...node.querySelectorAll(selector)]);}
  querySelector(selector){return this.querySelectorAll(selector)[0]??null;}
  append(...nodes){this.children.push(...nodes);}
  prepend(...nodes){this.children.unshift(...nodes);}
  replaceChildren(...nodes){this.children=nodes;}
  addEventListener(type,callback){this.handlers.set(type,callback);}
  showModal(){this.open=true;}
  close(){this.open=false;this.handlers.get('close')?.();}
  remove(){this.isConnected=false;}
}
const turn=()=>new Promise(resolve=>setImmediate(resolve));
async function until(predicate){for(let n=0;n<30&&!predicate();n++)await turn();assert.ok(predicate(),'actual producer completes render');}
function assertRoles(root){
  for(const tag of ['button','summary','input'])for(const node of root.querySelectorAll(tag)){
    const type=node.getAttribute('type')||node.type;
    const expected=tag==='input'?(['file','checkbox','radio','range'].includes(type)?null:'field'):'control';
    assert.equal(node.getAttribute('data-glass-role'),expected,`${tag}/${type||'default'} has its producer role`);
    if(expected)assert.equal(node.getAttribute('data-glass-safe-area'),'off');
  }
}
function fixture(){
  const body=new Element('body'),document={activeElement:null,body,createElement:tag=>{const node=new Element(tag);node.ownerDocument=document;return node;}};body.ownerDocument=document;
  const state={accountId:'synthetic-A',label:'A',profiles:[{accountId:'synthetic-A',label:'A',active:true},{accountId:'synthetic-B',label:'B'}]};
  const client={async getLocalState(){return state;},async status(){return {accountId:state.accountId,label:state.label,devices:[{deviceId:'device-A',label:'Device A'}]};},async contacts(){return {contacts:[{peerAccountId:'peer-B',relationshipId:'rel-B',label:'B'}]};},async card(){return {cardId:'card_1234567890123456'};}};
  return {body,document,state,client};
}
const source=(await readFile(new URL('../ui/index.mjs',import.meta.url),'utf8')).replace(/^import \{.*\} from '\.\/(?:platform-invitations|device-enrollment|credential-forms)\.mjs';\r?\n/gmu,'').replace(/^export /gmu,'');

test('the additional credential entry produces Glass roles on real helper controls',async()=>{
  const f=fixture(),request={requestId:'enroll_'+ 'm'.repeat(32),expiresAt:Date.now()+60000};
  Object.assign(f.state,{deviceId:'own-device',localRevision:1});
  f.client.startEnrollment=async()=>{f.state.pendingEnrollment=request;return request;};
  f.client.previewEnrollment=async()=>({...request,status:'pending',account:null});
  const context={document:f.document,mountPlatformInvitations:()=>({destroy(){}}),setTimeout,clearTimeout,URL,Blob,location:{origin:'https://synthetic.invalid'}};
  vm.runInNewContext(source+';globalThis.openPanel=openConnectPanel;',context);
  const panel=context.openPanel({client:f.client,bootstrapOnOpen:false,initialTab:'connect'});
  try{await until(()=>panel.element.querySelectorAll('form').length===2);assertRoles(panel.element);}finally{panel.close();}
});

test('Connect produces its material root before append and exposes an additive element handle',async()=>{
  const f=fixture(),context={document:f.document,mountPlatformInvitations:()=>({destroy(){}}),setTimeout,clearTimeout,URL,Blob,location:{origin:'https://synthetic.invalid'}};
  vm.runInNewContext(source+';globalThis.openPanel=openConnectPanel;',context);
  const panel=context.openPanel({client:f.client,bootstrapOnOpen:false}),dialog=f.body.children[0];
  try{assert.equal(dialog.getAttribute('data-glass-role'),'overlay');assert.equal(dialog.getAttribute('data-glass-safe-area'),'off');assertRoles(dialog);assert.equal(panel.element,dialog);assert.equal(typeof panel.close,'function');assert.equal(typeof panel.refresh,'function');await until(()=>dialog.querySelector('[data-name]'));assertRoles(dialog);await panel.refresh();assert.equal(panel.element,dialog);}finally{panel.close();}
});

test('every Connect tab and generated QR/contact/device/profile control carries explicit roles',async()=>{
  for(const tab of ['profile','people','devices','recovery','data','profiles']){
    const f=fixture(),context={document:f.document,mountPlatformInvitations:()=>({destroy(){}}),setTimeout,clearTimeout,URL,Blob,location:{origin:'https://synthetic.invalid'}};
    vm.runInNewContext(source+';globalThis.openPanel=openConnectPanel;',context);
    const panel=context.openPanel({client:f.client,initialTab:tab,bootstrapOnOpen:false}),dialog=f.body.children[0];
    try{await until(()=>dialog.querySelector('.connect-content').getAttribute('aria-busy')==='false');assertRoles(dialog);if(tab==='profile'){await dialog.querySelector('[data-card]').handlers.get('click')({preventDefault(){}});assertRoles(dialog);assert.ok(dialog.querySelector('input').getAttribute('data-glass-role'));}}finally{panel.close();}
  }
});

test('invitation producer declares generated actions, details and readonly links without styling checkbox/file inputs',async()=>{
  const f=fixture(),storage=new Map([['connect.platform-invites:synthetic-A',JSON.stringify({links:{'invite-A':'synthetic-token'}})]]),host=new Element('section');
  f.client.extension=async op=>op==='platform.admission.get'?{inviteOnly:true,manageAllowed:true,defaultQuota:3,revision:1}:{remaining:2,quota:3,invitations:[{id:'invite-A',expiresAt:Date.now()+60000}]};
  const invitationSource=(await readFile(new URL('../ui/platform-invitations.mjs',import.meta.url),'utf8')).replace(/^export /gmu,'');
  const context={document:f.document,location:{origin:'https://synthetic.invalid'},sessionStorage:{getItem:key=>storage.get(key),setItem:(key,value)=>storage.set(key,value)},navigator:{clipboard:{writeText:async()=>{}}}};
  vm.runInNewContext(invitationSource+';globalThis.mount=mountPlatformInvitations;',context);
  const panel=context.mount(host,{client:f.client,accountId:'synthetic-A'});
  try{await until(()=>host.querySelector('summary'));assertRoles(host);const show=host.querySelectorAll('button').find(node=>node.textContent==='Показать ссылку и QR');await show.handlers.get('click')();assertRoles(host);assert.ok(host.querySelectorAll('input').some(node=>node.readOnly&&node.getAttribute('data-glass-role')==='field'));}finally{panel.destroy();}
});
