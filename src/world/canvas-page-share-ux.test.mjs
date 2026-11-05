import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';
import * as selection from './canvas-page-selection.mjs';
import {canvasPageLink} from './canvas-page-client.mjs';
import {pageFrontendFixture,until,deferred,settle} from './test-support/canvas-page-share-independent.mjs';

const refresh=ui=>ui.find('cp-share-selection-head').children.find(value=>value.tagName==='button').emit('click');
const puts=f=>f.calls.filter(value=>value.operation==='canvas.pages.put');
const gets=f=>f.calls.filter(value=>value.operation==='canvas.pages.get');

async function dirtyRevisionOracle(t,options){
 const f=await pageFrontendFixture(t,options),a=await f.create('UX original A'),b=await f.create('UX draft B');await f.place([a,b]);await f.publish([a]);
 const ui=f.frontend();await ui.view.ready;const chosen=ui.checkboxes().find(value=>value['aria-label']==='UX draft B');chosen.checked=true;chosen.emit('change');ui.select().value='public';ui.select().emit('change');
 await f.raw('canvas.pages.put',{requestId:randomUUID(),expectedRevision:1,enabled:true,audience:'unlisted'});
 refresh(ui);await until(()=>gets(f).length===2);await until(()=>ui.footer[0].disabled===false);
 ui.form.emit('submit');await until(()=>puts(f).length===1);await until(()=>ui.footer[0].textContent!=='Сохраняем…');
 assert.equal(puts(f)[0].args.expectedRevision,1,'a passive refresh must not rebase a dirty choice onto another tabs Page revision');
 assert.equal((await f.raw('canvas.pages.get')).page.revision,2,'the other publication stays intact until an explicit conflict decision');
 assert.equal(ui.checkboxes().find(value=>value['aria-label']==='UX draft B').checked,true);assert.equal(ui.select().value,'public');
 refresh(ui);await until(()=>gets(f).length===3);await until(()=>ui.footer[0].disabled===false);
 assert.equal(ui.footer[0].textContent,'Сохранить мой выбор');ui.form.emit('submit');await until(()=>puts(f).length===2);await until(()=>ui.footer[0].textContent==='Сохранить');
 assert.equal(puts(f)[1].args.expectedRevision,2);assert.notEqual(puts(f)[1].args.requestId,puts(f)[0].args.requestId);assert.equal((await f.raw('canvas.pages.get')).page.revision,3);
}
test('refreshing dirty publication choices keeps their original Page revision until explicit conflict resolution',async t=>dirtyRevisionOracle(t));
test('the unchanged dirty-selection oracle detects substitution of the refreshed head revision',async t=>{
 const source=await readFile(new URL('./canvas-page-share.ts',import.meta.url),'utf8'),guard='expectedRevision:conflict?.freshRevision??draftRevision';assert.equal(source.split(guard).length,2);
 await assert.rejects(dirtyRevisionOracle(t,{shareSource:source.replace(guard,'expectedRevision:page?.revision??0')}),error=>{assert.equal(error.code,'ERR_ASSERTION');assert.match(error.message,/passive refresh must not rebase/u);return true;});
});

test('taking the published choice after a real CAS rejection restores server choices without publishing again',async t=>{
 const f=await pageFrontendFixture(t),a=await f.create('UX published A'),b=await f.create('UX unsaved B');await f.place([a,b]);await f.publish([a]);
 const ui=f.frontend();await ui.view.ready;const chosen=ui.checkboxes().find(value=>value['aria-label']==='UX unsaved B');chosen.checked=true;chosen.emit('change');ui.select().value='public';ui.select().emit('change');
 await f.raw('canvas.pages.put',{requestId:randomUUID(),expectedRevision:1,enabled:true,audience:'unlisted'});
 ui.form.emit('submit');await until(()=>ui.find('co-error').hidden===false);
 const take=ui.find('cp-share-take-current');assert.ok(take&&!take.hidden,'a confirmed conflict offers an explicit read-only way to take the published choice');take.emit('click');
 await until(()=>gets(f).length===2);await until(()=>take.hidden&&ui.footer[0].textContent==='Сохранить'&&ui.footer[0].disabled===true);
 assert.equal(ui.select().value,'unlisted');assert.equal(ui.checkboxes().find(value=>value['aria-label']==='UX unsaved B').checked,false);
 assert.equal(puts(f).length,1,'taking current never emits another Page mutation');assert.equal((await f.raw('canvas.pages.get')).page.revision,2);
});
test('closing a conflicted Page uses its fresh config even while unrelated Source preparation is held',async t=>{
 const f=await pageFrontendFixture(t),a=await f.create('UX close without Source');await f.place([a]);await f.publish([a]);const held=deferred();let hold=false,preparing=false;const ui=f.frontend({prepare:()=>{if(!hold)return Promise.resolve();preparing=true;return held.promise;}});t.after(()=>held.resolve());await ui.view.ready;
 ui.select().value='private';ui.select().emit('change');await f.raw('canvas.pages.put',{requestId:randomUUID(),expectedRevision:1,enabled:true,audience:'public'});ui.form.emit('submit');await until(()=>ui.find('co-error').hidden===false);hold=true;refresh(ui);await until(()=>preparing);
 assert.equal(ui.footer[0].disabled,false,'closing does not require the unrelated Source read to settle');ui.form.emit('submit');await until(()=>puts(f).length===2);await until(()=>ui.footer[0].textContent==='Сохранить');assert.equal((await f.raw('canvas.pages.get')).page.enabled,false);assert.equal(puts(f)[1].args.expectedRevision,2);
});

const accountId='share-ux-owner',appId='app-'+'a'.repeat(32),objectId='share_ux_object';
function choiceApi({provider='soty.apps',objectAudience='private',appAudience='private',appOwner=accountId}={}){
 const calls=[],object={objectId,ownerId:accountId,revision:1,accessRevision:1,state:'active',visibility:{audience:objectAudience,accountIds:[],communityIds:[]},hidden:{owner:false,others:false},binding:{providerId:provider,ref:'share_ux_source'}},app={appId,name:'UX window',ownerAccountId:appOwner,audience:appAudience};
 const field={schema:'soty.field.v2',contexts:[{contextId:'personal',title:'Personal',x:0,y:0}],shortcuts:[{shortcutId:'share_ux_shortcut',entity:{kind:'object',id:objectId},contextId:'personal',placement:{kind:'free',x:0,y:0,width:340,height:240}}]};
 const page={schema:'soty.canvas-page.v1',pageId:'page_'+'a'.repeat(32),ownerId:accountId,revision:1,enabled:true,audience:'unlisted',selection:{schema:'soty.canvas-page-selection.v1',fieldRevision:1,items:[{shortcutId:'share_ux_shortcut',entity:{kind:'object',id:objectId},expectedAccessRevision:1}]}};
 const api={async request(operation,args){calls.push({operation,args:structuredClone(args)});assert.equal(args.expectedAccountId,accountId);switch(operation){case'canvas.pages.get':return{page:structuredClone(page),selectionAvailable:true};case'world.field.get':return{revision:1,document:field};case'canvas.objects.project':return{items:[{objectId,available:true,accessRevision:1,presentation:{mask:{shape:'rect'}},protection:{kind:'none'},view:provider==='soty.apps'?{kind:'widget',app}:{kind:'note',note:{title:'UX note',preview:'synthetic'}}}]};case'canvas.objects.get':return{object};default:throw Error('Unexpected transport '+operation);}}};return{api,calls,object,page};
}
test('owner-selectable private windows remain selectable but explain both visibility barriers without app-wide writes',async()=>{
 for(const config of [{},{objectAudience:'public'},{appAudience:'public'}]){
  const f=choiceApi(config),snapshot=await selection.loadCanvasPageChoices({api:f.api,accountId}),choice=snapshot.choices[0];assert.equal(choice.available,true);assert.equal(choice.visitorAccess,'blocked');assert.match(choice.hint,/посетител|личным/iu);assert.deepEqual(choice.accessTarget,config.objectAudience==='public'?{kind:'app',id:appId}:{kind:'object',id:objectId});
  assert.equal(selection.createCanvasPageSelection(snapshot,[choice.shortcutId]).items.length,1);assert.equal(f.calls.some(value=>/put|update|register/u.test(value.operation)),false);
 }
 const foreign=await selection.loadCanvasPageChoices({api:choiceApi({objectAudience:'public',appOwner:'other-owner'}).api,accountId});assert.equal(foreign.choices[0].accessTarget,undefined);
 const publicWindow=await selection.loadCanvasPageChoices({api:choiceApi({objectAudience:'public',appAudience:'public'}).api,accountId});assert.equal(publicWindow.choices[0].visitorAccess,'conditional');assert.match(publicWindow.choices[0].hint,/не даёт гостям вход/u);
 const native=await selection.loadCanvasPageChoices({api:choiceApi({provider:'soty.notes'}).api,accountId});assert.equal(native.choices[0].visitorAccess,'page-only');assert.match(native.choices[0].hint,/только на этой странице/u);
});

class Port{
 constructor(tagName,className='',textContent=''){Object.assign(this,{tagName,className,textContent,children:[],parent:null,dataset:{},value:'',hidden:false,disabled:false,checked:false,handlers:new Map(),classList:{add(){}}});}
 append(...nodes){for(const node of nodes){node.parent=this;this.children.push(node);if(this.tagName==='select'&&!this.value)this.value=node.value;}}replaceChildren(...nodes){this.children=[];this.append(...nodes);}setAttribute(name,value){this[name]=value;}
 addEventListener(type,fn,{signal}={}){if(signal?.aborted)return;if(!this.handlers.has(type))this.handlers.set(type,new Set());this.handlers.get(type).add(fn);signal?.addEventListener('abort',()=>this.handlers.get(type)?.delete(fn),{once:true});}emit(type){for(const fn of this.handlers.get(type)??[])fn({preventDefault(){},target:this});}all(){return[this,...this.children.flatMap(node=>node.all())];}remove(){if(this.parent)this.parent.children=this.parent.children.filter(node=>node!==this);}focus(){if(this.ownerDocument)this.ownerDocument.activeElement=this;}select(){}
 querySelectorAll(selector){assert.equal(selector,'[data-access-shortcut]');return this.all().filter(node=>node.dataset.accessShortcut);}
}
async function mountPort(f,options={}){
 const source=await readFile(new URL('./canvas-page-share.ts',import.meta.url),'utf8'),code=ts.transpileModule(source,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText,module={exports:{}},root=new Port('dialog'),body=new Port('div'),footer=[];root.open=true;root.append(body);let live=true;
 const document=new EventTarget();document.body=new Port('body');document.activeElement=document.body;document.querySelectorAll=selector=>{assert.equal(selector,'dialog[open]');return root.open?[root]:[];};
 const element=(...args)=>Object.assign(new Port(...args),{ownerDocument:document});
 const button=(label,_icon,css,fn)=>{const node=element('button',css,label);node.type='button';if(fn)node.addEventListener('click',fn);return node;};
 const ports={'./dom':{el:element,button,labeledField:(label,input)=>{const node=element('label','',label);node.append(input);return node;}},'./icons':{icon:()=>element('svg')},'./dialogs':{pinDialogSubmit:(_dialog,_form,save)=>{footer.push(save);root.append(save);}},'./canvas-page-selection.mjs':selection,'./canvas-page-client.mjs':{canvasPageLink},'./canvas-page-share.css':{},qrcode:{default:{toCanvas:async()=>{}}}};
 vm.runInNewContext(code,{module,exports:module.exports,require:id=>{assert.ok(Object.hasOwn(ports,id));return ports[id];},AbortController,crypto:globalThis.crypto,location:{origin:'http://127.0.0.1'},navigator:{clipboard:{async writeText(){}}},document});
 const view=module.exports.mountCanvasPageShare({element:root,body,close(){root.open=false;}},{api:f.api,accountId,isCurrent:()=>live,onPreview(){},...options});await view.ready;return{view,root,footer,document,find:css=>root.all().find(node=>node.className.split(' ').includes(css)),retire(){live=false;view.dispose();}};
}
test('access action is outside its checkbox label and returning from settings preserves the selected draft',async()=>{
 const f=choiceApi(),held=deferred(),opened=[],ui=await mountPort(f,{onOpenObjectAccess:async id=>{opened.push(id);await held.promise;}}),action=ui.find('cp-share-access');
 assert.ok(action);assert.equal(action.type,'button');assert.equal(action.parent.tagName,'div');assert.equal(action.parent.children.find(node=>node.tagName==='label').children.includes(action),false);
 const checkbox=ui.root.all().find(node=>node.type==='checkbox');assert.equal(checkbox.checked,true);action.emit('click');await until(()=>opened.length===1);assert.equal(ui.footer[0].disabled,true);held.resolve();await until(()=>f.calls.filter(value=>value.operation==='canvas.pages.get').length===2);await settle();
 assert.equal(ui.root.all().find(node=>node.type==='checkbox').checked,true);assert.deepEqual(opened,[objectId]);assert.equal(f.calls.some(value=>/put|update/u.test(value.operation)),false);
 assert.notEqual(ui.find('cp-share-access'),action,'the row really was replaced');assert.equal(ui.document.activeElement,ui.find('cp-share-access'),'focus returns to the fresh action rather than its detached predecessor');ui.retire();
});

test('returning from settings does not steal focus after an intervening user action',async()=>{
 const f=choiceApi(),held=deferred();let hold=false,reading=false;const transport=f.api.request;
 f.api.request=async(op,args)=>{if(hold&&op==='canvas.objects.project'){reading=true;await held.promise;}return transport(op,args);};
 const ui=await mountPort(f,{onOpenObjectAccess:async()=>{hold=true;}});ui.find('cp-share-access').emit('click');await until(()=>reading);
 ui.document.dispatchEvent(new Event('keydown'));held.resolve();await settle();await settle();await settle();
 assert.equal(ui.document.activeElement,ui.document.body);assert.equal(f.calls.some(value=>/put|update/u.test(value.operation)),false);ui.retire();
});
test('account retirement before the scheduled access callback prevents opening old-account settings',async()=>{
 const f=choiceApi(),opened=[],ui=await mountPort(f,{onOpenObjectAccess:async id=>{opened.push(id);}});ui.find('cp-share-access').emit('click');ui.retire();await settle();assert.deepEqual(opened,[]);assert.equal(f.calls.filter(value=>value.operation==='canvas.pages.get').length,1);
});

async function objectPageLabel(provider,source){
 source??=await readFile(new URL('./canvas-object-settings.ts',import.meta.url),'utf8');const parsed=ts.createSourceFile('object-settings.ts',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS);let block;const visit=node=>{if(ts.isIfStatement(node)&&node.expression.getText(parsed)==="results[2].status==='fulfilled'")block=node;ts.forEachChild(node,visit);};visit(parsed);assert.ok(block);
 const code=ts.transpileModule(ts.createPrinter().printNode(ts.EmitHint.Unspecified,block,parsed),{compilerOptions:{target:ts.ScriptTarget.ES2022}}).outputText,object={objectId,ownerId:accountId,revision:1,accessRevision:1,binding:{providerId:provider},visibility:{audience:'private'},hidden:{others:false}},page={enabled:true,audience:'public',selection:{items:[{entity:{kind:'object',id:objectId},expectedAccessRevision:1}]}},option={value:'private',textContent:'Только мне'},pageStatus={textContent:''},pageAccess={hidden:true};vm.runInNewContext(code,{object,options:{accountId},results:[null,null,{status:'fulfilled',value:{page}}],audience:{options:[option]},pageStatus,pageAccess});return{option,pageStatus,pageAccess};
}
async function widgetLabelOracle(source){const value=await objectPageLabel('soty.apps',source);assert.equal(value.option.textContent,'Только мне','private widget must not promise the Native-only Page grant');assert.match(value.pageStatus.textContent,/проверяется отдельно/u);}
test('Object settings distinguish selected private widgets from Native-only Page visibility',async()=>{await widgetLabelOracle();const native=await objectPageLabel('soty.notes');assert.equal(native.option.textContent,'Лично + выбранная страница');});
test('the same Page label oracle rejects treating every selected provider as Native',async()=>{
 const source=await readFile(new URL('./canvas-object-settings.ts',import.meta.url),'utf8'),guard="const nativePage=Boolean(page.selection)&&object.ownerId===options.accountId&&['soty.notes','soty.materials','soty.vault'].includes(object.binding.providerId)&&!object.hidden.others";assert.equal(source.split(guard).length,2);await assert.rejects(widgetLabelOracle(source.replace(guard,'const nativePage=true')),error=>{assert.equal(error.code,'ERR_ASSERTION');assert.match(error.message,/private widget must not promise/u);return true;});
});
