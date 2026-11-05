import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';
import {randomUUID} from 'node:crypto';
import {createHttpApp} from '../../../server/http-app.js';
import {nativeHttpFixture,nativeIdentity} from '../../../server/test/support/native-capability-http.mjs';
import * as selection from '../canvas-page-selection.mjs';
import {canvasPageLink} from '../canvas-page-client.mjs';

export const settle=()=>new Promise(resolve=>setImmediate(resolve));
export const until=async predicate=>{const end=Date.now()+5000;while(!predicate()){assert.ok(Date.now()<end,'Expected actual Page/UI boundary');await new Promise(resolve=>setTimeout(resolve,2));}};
export const deferred=()=>{let resolve;const promise=new Promise(done=>{resolve=done;});return{promise,resolve};};
class Element{
 constructor(tag,className='',text=''){Object.assign(this,{tagName:tag,className,textContent:text,children:[],parent:null,dataset:{},value:'',hidden:false,disabled:false,checked:false,handlers:new Map(),width:0,height:0,classList:{add(){}}});}
 append(...nodes){for(const node of nodes){node.parent=this;this.children.push(node);if(this.tagName==='select'&&!this.value)this.value=node.value;}}
 replaceChildren(...nodes){this.children=[];this.append(...nodes);}setAttribute(key,value){this[key]=value;}getAttribute(key){return this[key];}
 addEventListener(type,fn,{signal}={}){if(signal?.aborted)return;if(!this.handlers.has(type))this.handlers.set(type,new Set());this.handlers.get(type).add(fn);signal?.addEventListener('abort',()=>this.handlers.get(type)?.delete(fn),{once:true});}
 emit(type){for(const fn of [...(this.handlers.get(type)??[])])fn({preventDefault(){},target:this});}
 remove(){if(this.parent)this.parent.children=this.parent.children.filter(value=>value!==this);this.parent=null;}
 all(){return[this,...this.children.flatMap(value=>value.all())];}focus(){this.focused=true;}select(){this.selected=true;}
}
const source=await readFile(new URL('../canvas-page-share.ts',import.meta.url),'utf8'),compiled=ts.transpileModule(source,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
function mount(api,options,origin,shareSource){
 let current=true;const body=new Element('div'),root=new Element('dialog'),copied=[],previews=[],footer=[];root.open=true;root.append(body);
 const button=(label,_symbol,css,callback)=>{const node=new Element('button',css,label);node.type='button';if(callback)node.addEventListener('click',callback);return node;};
 const ports={'./dom':{el:(...args)=>new Element(...args),button,labeledField:(label,input)=>{const node=new Element('label','',label);node.append(input);return node;}},'./icons':{icon:()=>new Element('svg')},'./dialogs':{pinDialogSubmit:(_dialog,_form,save)=>{save.type='submit';footer.push(save);root.append(save);}},'./canvas-page-selection.mjs':selection,'./canvas-page-client.mjs':{canvasPageLink},'./canvas-page-share.css':{},qrcode:{default:{toCanvas:async(canvas)=>{canvas.width=canvas.height=140;}}}};
 const code=shareSource===undefined?compiled:ts.transpileModule(shareSource,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
 const module={exports:{}};vm.runInNewContext(code,{module,exports:module.exports,require:id=>{assert.ok(Object.hasOwn(ports,id),'Unexpected UI import '+id);return ports[id];},AbortController,crypto:globalThis.crypto,location:{origin},navigator:{clipboard:{async writeText(value){copied.push(value);}}},document:{execCommand(){return false;}}});
 const view=module.exports.mountCanvasPageShare({element:root,body,close(){root.open=false;}},{api,...options,isCurrent:()=>current,onPreview:id=>previews.push(id)});
 const find=className=>root.all().find(value=>value.className.split(' ').includes(className));
 return{view,root,body,footer,copied,previews,find,form:find('cp-share'),select:()=>root.all().find(value=>value.tagName==='select'),checkboxes:()=>root.all().filter(value=>value.tagName==='input'&&value.type==='checkbox'),retire(){current=false;},dispose(){view.dispose();},text(){return root.all().map(value=>value.textContent).join(' ');}};
}

export async function pageFrontendFixture(t,{shareSource}={}){
 const http=await nativeHttpFixture(t,{appFactory:(dist,options)=>createHttpApp(dist,{...options,allowCanvasPageSelectionMigration:true})}),owner=nativeIdentity('Independent Page owner'),identity=await http.bootstrap(owner),accountId=identity.accountId,calls=[];
 const raw=async(operation,args={})=>{const result=await http.call(owner,operation,{expectedAccountId:accountId,...args});if(!result.ok)throw Object.assign(Error(operation+':'+(result.error?.code??'page_test_rpc_failed')),{code:result.error?.code});return result;};
 const profile=await http.call(owner,'world.profile.get',{});assert.equal(profile.ok,true,profile.error?.code);
 const create=async(title,extra={})=>{const noteId=randomUUID();await raw('notes.put',{noteId,mutationId:randomUUID(),expectedRevision:0,title,body:'Synthetic private body '+title,items:[],color:'plain',pinned:false,state:'active'});return(await raw('canvas.objects.create',{requestId:randomUUID(),binding:{providerId:'soty.notes',ref:noteId},presentation:{mask:{shape:'rect'},fontSize:16},visibility:{audience:'private',accountIds:[],communityIds:[]},hidden:{owner:false,others:false},protection:{kind:'none'},...extra})).current;};
 const field=()=>raw('world.field.get');
 const place=async(entries)=>{const previous=await field();return raw('world.field.put',{requestId:randomUUID(),expectedRevision:previous.revision,document:{schema:'soty.field.v2',contexts:[{contextId:'independent_personal',title:'Independent own canvas',x:0,y:0}],shortcuts:entries.map((entry,index)=>({shortcutId:entry.shortcutId??'shortcut_'+entry.objectId,entity:entry.entity??{kind:'object',id:entry.objectId},contextId:'independent_personal',placement:{kind:'free',x:index*300,y:0,width:240,height:160}}))}});};
 const publish=async(entries,{audience='unlisted',expectedRevision=0}={})=>{const snapshot=await field();return raw('canvas.pages.put',{requestId:randomUUID(),expectedRevision,enabled:true,audience,selection:{schema:'soty.canvas-page-selection.v1',fieldRevision:snapshot.revision,items:entries.map(entry=>({shortcutId:entry.shortcutId??'shortcut_'+entry.objectId,entity:{kind:'object',id:entry.objectId},expectedAccessRevision:entry.accessRevision}))}});};
 const mounts=[];const frontend=({prepare,intercept}={})=>{const api={async request(operation,args){calls.push({operation,args:structuredClone(args)});const result=await raw(operation,args);return intercept?intercept(operation,args,result):result;}};const value=mount(api,{accountId,...(prepare?{prepare}:{})},http.origin,shareSource);mounts.push(value);return value;};
 t.after(()=>{for(const value of mounts)value.dispose();});
 const anonymous=async pageId=>{const response=await fetch(http.origin+'/api/canvas/pages/'+pageId);assert.equal(response.status,200);return response.json();};
 return{http,accountId,raw,create,field,place,publish,frontend,anonymous,calls};
}
