import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { resolveObjectURL } from 'node:buffer';
import ts from 'typescript';
import { signedAppsCoverFixture, raster } from './test-support/cover-signed-apps.mjs';
import { signedFixture } from '../../modules/world/test/support/world6-signed-fixture.mjs';
import { pngFixture } from '../../modules/world/test/avatar-fixtures.mjs';
import { createCoverApi } from './cover-api.mjs';
import { createCoverEditorState, createCoverJournal } from './cover-editor-state.mjs';
import { createAppCoverPresenter } from './app-cover-presenter.mjs';
const tick=()=>new Promise(r=>setImmediate(r));
async function until(predicate){for(let i=0;i<1000;i++){if(predicate())return;await tick();}assert.fail('Actual host flow did not settle');}
const local=base64=>async()=>({url:'blob:review-only',width:80,height:80,mime:'image/png',byteCount:Buffer.from(base64,'base64').length,base64:()=>base64,dispose(){}});
/** Source instrumentation only for thin browser host ports, extracted with the
 * real TS AST (not a reimplementation). SDK/controller/presenter and signed
 * services below are their actual modules. This is not DOM/pixel acceptance. */
function actualHost(ports){
  const text=readFileSync(new URL('./app.ts',import.meta.url),'utf8'),source=ts.createSourceFile('app.ts',text,ts.ScriptTarget.Latest,true);
  const owner=source.statements.find(node=>ts.isClassDeclaration(node)&&node.name?.text==='WorldApplication');assert.ok(owner);
  const names=['mountFieldCovers','openCoverEditor','afterNoteSaved','openRoute'];
  const members=names.map(name=>{const member=owner.members.find(node=>node.name?.getText(source)===name);assert.ok(member,'Actual host method '+name);return member.getText(source);});
  const output=ts.transpileModule('class Host {\n'+members.join('\n')+'\n}',{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.None}}).outputText;
  return new (Function('mountCoverEditor','createCoverApi','createAppCoverPresenter','mountAppCoverField','document','setInterval','clearInterval','history','location',output+';return Host;')(
    ports.mountCoverEditor,createCoverApi,createAppCoverPresenter,ports.mountField,ports.document,ports.setInterval,ports.clearInterval,ports.history,ports.location))();
}
function harness({api,accountId,input,profile=null}){
  let generation=1,timer=0;const timers=new Set(),visual=[],states=[],loads=[],disk=new Map();
  const document={visibilityState:'visible'},field={element:{},refreshes:[],camera:{x:41,y:72,scale:1.35},async refresh(value){this.refreshes.push(value);}};
  const ports={document,setInterval(fn){const id=++timer;timers.add(id);return id;},clearInterval(id){timers.delete(id);},history:{pushState(){}},location:{hash:'#mine'},
    mountField({presenter,isCurrent}){const binding=presenter.watch({appId:'app-'+'c'.repeat(32),identity:'actual-source-one',isCurrent,onVisual:v=>visual.push(v)});binding.setVisible(true);return {dispose(){binding.dispose();}};},
    mountCoverEditor(options){const sdk=createCoverApi(options),state=createCoverEditorState({sdk,prepareFile:local(input),journal:createCoverJournal({getItem:key=>disk.get(key)??null,setItem:(key,value)=>disk.set(key,value)}),isCurrent:options.isCurrent,onCommitted:options.onCommitted});
      states.push(state);loads.push(state.load());return {dispose:()=>state.dispose(),hasUnsavedChanges:()=>state.read().hasUnsavedChanges,requestClose:callback=>{host.coverDialog.close();callback?.();}};},
  };
  const host=actualHost(ports);Object.assign(host,{deskAccount:accountId,destroyed:false,screenSequence:7,profile,api,group:null,unifiedField:field,fieldCoverPresenter:null,fieldCoverBinding:null,fieldCoverTimer:null,
    coverDialog:null,coverHandle:null,coverTargetCurrent:null,noteActionPending:false,widgets:null,avatars:{calls:[],setContext(value){this.calls.push(value);}},rendered:0,renderNavigation(){this.rendered++;},toast(){},
    accountTask(){const captured=generation;return()=>!host.destroyed&&generation===captured;},screenHasUnsavedChanges:()=>false,
    dialog(title,onClose){const dialog={element:{open:true},close(){if(!dialog.element.open)return;dialog.element.open=false;onClose?.();}};return dialog;},
  });
  return {host,ports,field,visual,states,loads,timers,switchAccount(){generation++;},dispose(){host.coverDialog?.close();host.mountFieldCovers(null);}};
}
test('actual host hook invalidates the existing visible cell and persists through genuine Apps restart without a Source revision bump',async t=>{
  const f=await signedAppsCoverFixture(t);f.enable();const api={async request(op,args){const result=await f.raw('owner',op,args);if(!result.ok)throw Object.assign(Error(result.error.code),{code:result.error.code});return result;}};
  const h=harness({api,accountId:f.accounts.owner.accountId,input:await raster('#4488aa')});t.after(()=>h.dispose());h.host.mountFieldCovers(h.field);await until(()=>h.visual.at(-1)?.mode==='inherit');
  const revision=f.read(db=>db.prepare('SELECT revision FROM local_apps WHERE id=?').get(f.id).revision),camera=h.field.camera;
  h.host.openCoverEditor('app',f.id,'Actual synthetic cover');await h.loads.at(-1);const editor=h.states.at(-1);await editor.choose({});await editor.save();await until(()=>h.visual.at(-1)?.mode==='custom');
  const url=h.visual.at(-1).url,bytes=Buffer.from(await resolveObjectURL(url).arrayBuffer());assert.ok(bytes.length>12);assert.equal(f.read(db=>db.prepare('SELECT revision FROM local_apps WHERE id=?').get(f.id).revision),revision);
  assert.equal(h.field.camera,camera);assert.deepEqual(h.field.refreshes,[],'app cover hook replaces its image without whole field rebuild');h.host.coverDialog.close();
  h.ports.document.visibilityState='hidden';h.host.mountFieldCovers(h.field);assert.equal(h.timers.size,0);assert.equal(resolveObjectURL(url),undefined);
  f.reopen();h.ports.document.visibilityState='visible';h.host.mountFieldCovers(h.field);await until(()=>h.visual.at(-1)?.mode==='custom'&&h.visual.at(-1).url!==url);
  assert.deepEqual(Buffer.from(await resolveObjectURL(h.visual.at(-1).url).arrayBuffer()),bytes);assert.equal(h.timers.size,1);assert.equal(h.field.camera,camera);
});
test('actual profile hook reuses avatar hydration/current projection and rejects a late read after account generation changes',async t=>{
  const f=await signedFixture(t),person=f.actors[0],initial=await f.profile(person);let held=false,release;
  const api={async request(op,args){const result=await f.rpc(person,op,args);if(!result.ok)throw Object.assign(Error(result.error.code),{code:result.error.code});
    if(op==='world.profile.get'&&held)await new Promise(resolve=>release=resolve);return result;}};
  const h=harness({api,accountId:person.accountId,input:pngFixture(80,80).toString('base64'),profile:initial});t.after(()=>h.dispose());const observed=[];
  h.host.openCoverEditor('profile',person.accountId,person.label,value=>observed.push(value));await h.loads.at(-1);await h.states.at(-1).choose({});await h.states.at(-1).save();await until(()=>observed.length===1);
  assert.equal(h.host.profile.avatarRevision,h.host.profile.revision);assert.equal(h.host.avatars.calls.length,1);assert.deepEqual(h.field.refreshes,[{preserveView:true}]);h.host.coverDialog.close();
  held=true;h.host.openCoverEditor('profile',person.accountId,person.label,value=>observed.push(value));await h.loads.at(-1);await h.states.at(-1).choose({});await h.states.at(-1).save();await until(()=>typeof release==='function');
  h.switchAccount();h.host.profile={profileId:'different-account',revision:99};release();await tick();assert.equal(h.host.profile.profileId,'different-account');assert.equal(observed.length,1);assert.equal(h.host.avatars.calls.length,1);assert.equal(h.field.refreshes.length,1);
});
test('actual internal navigation hook waits for explicit close and its continuation is fenced against account ABA',()=>{
  const h=harness({api:{},accountId:'owner',input:'aW1hZ2U='});let continuation=null,actions=0;
  h.host.coverDialog={element:{open:true}};h.host.coverHandle={requestClose(callback){continuation=callback;}};
  h.host.afterNoteSaved(()=>actions++);assert.equal(actions,0);assert.equal(typeof continuation,'function');h.host.coverDialog=null;h.host.coverHandle=null;continuation();assert.equal(actions,1);
  h.host.coverDialog={element:{open:true}};h.host.coverHandle={requestClose(callback){continuation=callback;}};h.host.afterNoteSaved(()=>actions++);h.switchAccount();h.switchAccount();h.host.coverDialog=null;h.host.coverHandle=null;continuation();assert.equal(actions,1);h.dispose();
});
