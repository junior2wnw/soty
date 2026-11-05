import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import {nativeFieldSafeArea} from './native-field-safe-area.mjs';
const source=readFileSync(new URL('./community-field-object.ts',import.meta.url),'utf8'),overview=readFileSync(new URL('./field-object-overview.ts',import.meta.url),'utf8'),dom=readFileSync(new URL('./dom.ts',import.meta.url),'utf8');
const existing=readFileSync(new URL('./canvas-field-content.test.mjs',import.meta.url),'utf8'),ast=ts.createSourceFile('ports.mjs',existing,ts.ScriptTarget.Latest,true),declaration=ast.statements.find(node=>ts.isClassDeclaration(node)&&node.name?.text==='NodePort');
const flatten=node=>[node,...node.children.flatMap(flatten)],Base=vm.runInNewContext(`(${ts.createPrinter().printNode(ts.EmitHint.Unspecified,declaration,ast)})`,{flatten});
class Port extends Base{
  get childElementCount(){return this.children.length;}
  prepend(...nodes){for(const node of nodes)node.remove();for(const node of nodes)node.parentElement=this;this.children.unshift(...nodes);}
  querySelectorAll(selector){return flatten(this).slice(1).filter(node=>selector==='*'||selector.split(',').some(value=>value.startsWith('.')?node.className.split(/\s+/u).includes(value.slice(1)):node.tag===value));}
  addEventListener(type,handler,options={}){if(!this.listeners.has(type))this.listeners.set(type,[]);this.listeners.get(type).push({handler,capture:options.capture===true});}
  emit(type,value={}){let stopped=false;const event={target:this,currentTarget:this,preventDefault(){this.defaultPrevented=true;},stopImmediatePropagation(){stopped=true;},...value};for(const entry of [...this.listeners.get(type)??[]].sort((a,b)=>Number(b.capture)-Number(a.capture))){entry.handler(event);if(stopped)break;}return event;}
  click(){if(!this.disabled)this.emit('click');}
}
const compile=text=>ts.transpileModule(text,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.ESNext}}).outputText.replace(/^import[^\n]*\n/gmu,'').replace(/^export /gmu,'');
const group=()=>({communityId:'community_room',name:'Allowed room',description:'Allowed description',topics:[],joinPolicy:'open',showcase:'',symbol:'people',color:'lilac',revision:1,memberCount:9,previewMembers:[{profileId:'not_in_voice',displayName:'Roster member never voice'}],membership:{state:'active',role:'member',pinned:false,muted:false,showInProfile:false,revision:1},permissions:{canManage:false,canModerate:false,canWrite:true},unreadCount:0});
function fixture(t,code=source){
  let current=true,record=group(),state=null,listener,subscribed=0,unsubscribed=0,mounts=0,disposed=0,flushes=0,listenCommands=0,rootCallEnds=0,receivedName='';
  const document={activeElement:null,createElement(tag){const node=new Port(tag);node.ownerDocument=document;return node;}};
  const env={document,icon:()=>document.createElement('svg'),nativeFieldSafeArea,exports:{}};vm.createContext(env);vm.runInContext(compile(dom)+compile(overview)+compile(code)+'\nexports.create=createCommunityFieldObject;exports.button=button;',env);
  const node={shortcutId:'room_placement',kind:'community',entity:{entity:{kind:'community',id:record.communityId},title:record.name},width:640,height:420,presentation:{shape:'hex'}};
  const host=document.createElement('div'),handles=[],buttons=[];
  const options={readCommunity:()=>record,isCurrent:()=>current,createListenButton(bound){const control=env.exports.button('Слушать','activity');control.addEventListener('click',()=>{listenCommands++;receivedName=bound.name;});buttons.push(control);return control;},
    mountConversation(target){mounts++;let active=true,dirty=true;const input=document.createElement('textarea');input.value='Unconfirmed private draft';input.selectionStart=4;input.selectionEnd=13;target.append(input);const handle={input,isCurrent:()=>active,flush:async()=>{flushes++;},hasUnsavedChanges:()=>dirty,dispose(){disposed++;active=false;},deny(){active=false;},clean(){dirty=false;}};handles.push(handle);return handle;},
    onJoin:async()=>{record={...record,membership:{...group().membership},revision:record.revision+1};return record;},onDetails(){},onMembers(){},onApps(){},readCallState:()=>state,subscribeCall(fn){listener=fn;subscribed++;return()=>{unsubscribed++;};}};
  const view=env.exports.create(node,options);host.append(view.element,view.overview);t.after(()=>view.dispose());
  return{view,node,options,host,buttons,handles,document,set group(value){record=value;},get group(){return record;},set state(value){state=value;listener?.();},deny(){current=false;view.update(node);},restore(){current=true;},update(){view.update(node);},notify(){listener?.();},get counts(){return{mounts,disposed,flushes,listenCommands,rootCallEnds,subscribed,unsubscribed,receivedName};}};
}
const find=(root,name)=>flatten(root).find(node=>node.className.split(/\s+/u).includes(name));
const call=(roomId='community_room')=>({state:'listening',roomId,mode:'group',connection:'connected',microphone:'off',peers:7,participants:Object.freeze([Object.freeze({accountId:'voice_user',deviceId:'voice_device',label:'Actual WS member',microphone:false,isSelf:false})])});

test('voice is primary; metadata/glance never eagerly mount private conversation or turn membership previews into voice users',t=>{
 const f=fixture(t);f.view.setVisible(true);f.view.setPresentation('glance');assert.equal(f.counts.mounts,0);assert.equal(f.buttons.length,1);assert.equal(find(f.view.element,'cf-roster').textContent,'');assert.equal(f.view.overview.textContent,'Allowed room');assert.equal(f.view.element.textContent.includes('Roster member never voice'),false);
 f.view.showChat();assert.equal(f.counts.mounts,0,'only explicit interactive chat opens the mount');f.view.setPresentation('interactive');assert.equal(f.counts.mounts,1);
});

test('updates and presentation changes preserve the same chat DOM/draft/caret and Root button, while its command receives current metadata',async t=>{
 const f=fixture(t);f.view.setVisible(true);f.view.setPresentation('interactive');f.view.showChat();const input=f.handles[0].input,listen=f.buttons[0],original=f.group;
 f.group={...f.group,name:'Fresh allowed name',revision:2};f.update();for(const mode of['overview','glance','interactive'])f.view.setPresentation(mode);f.view.showVoice();f.view.showChat();
 assert.equal(f.counts.mounts,1);assert.equal(find(f.view.element,'cf-chat').children[0],input);assert.equal(input.value,'Unconfirmed private draft');assert.deepEqual([input.selectionStart,input.selectionEnd],[4,13]);assert.equal(f.buttons[0],listen);assert.equal(f.buttons.length,1);assert.equal(original.name,'Allowed room','Root projection is never mutated');
 listen.click();assert.equal(f.counts.receivedName,'Fresh allowed name');await f.view.flush();assert.equal(f.counts.flushes,1);assert.equal(f.view.hasUnsavedChanges(),true);
});

async function rosterScenario(t,code=source){const f=fixture(t,code);f.state=call('foreign_room');assert.equal(find(f.view.element,'cf-roster').textContent,'','foreign room participants must never appear');f.state=call();assert.match(find(f.view.element,'cf-roster').textContent,/Actual WS member/u);const heldLabel=find(f.view.element,'cf-participant-name');f.state=call('retired_room');assert.equal(heldLabel.textContent,'','detached participant label must scrub on room change');f.state=call();assert.doesNotMatch(f.view.element.textContent,/говорит|speaking/iu);f.state={...call(),participants:undefined};assert.equal(find(f.view.element,'cf-roster').textContent,'','peers number does not invent a named roster');f.state={...call(),state:'ended'};assert.equal(find(f.view.element,'cf-roster').textContent,'');}
test('only authoritative current active-room WS members render, without speaking/RTC/avatar inventions',rosterScenario);
test('broken room binding is detected by the unchanged roster privacy oracle',async t=>{const anchor='state.roomId===communityId&&';assert.equal(source.split(anchor).length,2);await assert.rejects(rosterScenario(t,source.replace(anchor,'true&&')),/foreign room participants/);});

function revokeScenario(t,code=source){const f=fixture(t,code);f.view.setVisible(true);f.view.setPresentation('interactive');f.view.showChat();const input=f.handles[0].input,title=find(f.view.element,'cf-name');f.group=null;f.update();assert.equal(input.value,'','held private input must scrub before replacement');assert.equal(title.textContent,'','private name must scrub before a new projection');assert.equal(f.view.overview.textContent,'');assert.equal(f.counts.disposed,1);}
test('read denial scrubs held private chat input, identity, and its own mount synchronously',revokeScenario);
test('broken early return on denial fails the same private-input scrub criterion',t=>{const anchor='const group=read();if(!group){scrub();return;}\n    const active=';assert.equal(source.split(anchor).length,2);assert.throws(()=>revokeScenario(t,source.replace(anchor,'const group=read();if(!group)return;\n    const active=')),/held private input/);});

test('membership loss retires private chat; stale or foreign-node callbacks cannot revive it',t=>{
 const f=fixture(t);f.view.setVisible(true);f.view.setPresentation('interactive');f.view.showChat();const input=f.handles[0].input;f.state=call();f.group={...f.group,membership:{...f.group.membership,state:'removed'}};f.update();assert.equal(input.value,'');assert.equal(f.counts.disposed,1);assert.equal(find(f.view.element,'cf-roster').textContent,'');
 f.view.update({...f.node,entity:{entity:{kind:'community',id:'different_room'},title:'Other'}});f.group=group();f.notify();assert.equal(f.view.element.hidden,true);assert.equal(f.view.overview.textContent,'');
});

function disposeScenario(t,code=source){const f=fixture(t,code);f.view.setVisible(true);f.view.setPresentation('interactive');f.view.showChat();const input=f.handles[0].input,listen=f.buttons[0];f.view.setVisible(false);assert.equal(f.counts.disposed,0);f.view.dispose();assert.equal(f.counts.disposed,1,'dispose must close exactly its own conversation');assert.equal(input.value,'');listen.emit('click');f.notify();assert.equal(f.counts.listenCommands,0);assert.equal(f.counts.rootCallEnds,0);assert.equal(f.counts.unsubscribed,1);assert.equal(f.view.overview.parentElement,null);assert.equal(f.view.element.textContent,'');}
test('invisibility retains work; disposal closes its mount, never the Root call, and fences detached buttons/late subscription',disposeScenario);
test('broken missing conversation disposal is detected without weakening cleanup or the call-lifetime criterion',t=>{const anchor='const handle=conversation;conversation=null;';assert.equal(source.split(anchor).length,2);assert.throws(()=>disposeScenario(t,source.replace(anchor,'const handle=null;conversation=null;')),/dispose must close/);});

test('late join after account loss neither repaints nor emits a false success',async t=>{
 const f=fixture(t);f.group={...f.group,membership:null};f.update();let resolve;f.options.onJoin=()=>new Promise(done=>{resolve=done;});find(f.view.element,'cf-join').click();f.deny();resolve(group());await new Promise(done=>setImmediate(done));assert.equal(f.view.element.hidden,true);assert.equal(f.view.overview.textContent,'');assert.equal(f.view.element.textContent.includes('Allowed room'),false);assert.equal(f.counts.mounts,0);
});

test('pane callback reports only explicit actual changes, without route effects from refresh, LOD or permission cleanup',t=>{
 const f=fixture(t),events=[];f.options.onPaneChange=pane=>events.push(pane);f.view.showChat();f.view.showChat();f.update();f.view.setPresentation('glance');f.view.showVoice();f.view.showVoice();f.view.showChat();f.deny();
 assert.deepEqual(events,['chat','voice','chat']);assert.equal(f.view.overview.textContent,'');
});
