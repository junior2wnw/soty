import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import vm from 'node:vm';
import ts from 'typescript';
import {createCommunityConversation,createDirectConversation} from './conversation-target.mjs';

// Execute current controller methods. Ports supply only IO/DOM owners; no copied
// route transition, sidebar cleanup, membership guard or canvas lifecycle.
const source=readFileSync(new URL('./app.ts',import.meta.url),'utf8'),ast=ts.createSourceFile('app.ts',source,ts.ScriptTarget.Latest,true),printer=ts.createPrinter();
const appClass=ast.statements.find(node=>ts.isClassDeclaration(node)&&node.members.some(member=>member.name?.getText(ast)==='openGroup'));
const method=name=>printer.printNode(ts.EmitHint.Unspecified,appClass.members.find(member=>member.name?.getText(ast)===name),ast);
const names=['hideMainApplication','isRetainedMain','mountChat','mountDirectChat','writeCommunityRoute','communityFocusChanged','closeCommunityDialogs','closeCommunitySurface','openGroup','communityFocusChangedAfterFailure','renderGroup','cleanScreen'];
const compile=text=>ts.transpileModule(text,{compilerOptions:{target:ts.ScriptTarget.ES2022}}).outputText;
const record=(id='room_A')=>({communityId:id,name:id,revision:1,memberCount:1,membership:{state:'active',revision:1,muted:false},permissions:{canWrite:true,canModerate:false},unreadCount:0});
const deferred=()=>{let resolve;const promise=new Promise(done=>{resolve=done;});return{promise,resolve};};
function historyPort(initial='#mine',hasEarlier=false){
 const location={hash:initial},entries=hasEarlier?['#outside',initial]:[initial],states=entries.map(()=>null);let index=entries.length-1,pushes=0,replaces=0,backs=0;
 const history={get state(){return states[index];},pushState(state,_,url){pushes++;entries.splice(index+1);states.splice(index+1);entries.push(url);states.push(state);index++;location.hash=url;},replaceState(state,_,url){replaces++;entries[index]=url;states[index]=state;location.hash=url;},back(){backs++;if(index){index--;location.hash=entries[index];}}};
 return{history,location,get counts(){return{pushes,replaces,backs};}};
}
function fixture({initial='#mine',hasEarlier=false,change={}}={}){
 const io=historyPort(initial,hasEarlier),context=vm.createContext({...io,clearInterval,createCommunityConversation,createDirectConversation});
 vm.runInContext(compile('class Probe{'+names.map(name=>change[name]??method(name)).join('\n')+'}\nglobalThis.Probe=Probe;'),context);
 const app=new context.Probe(),counts={inboxDisposed:0,chatDisposed:0,fieldDisposed:0,callEnded:0,mainReplaced:0,refreshed:0},updates=[],apps=[],remembered=[];let account=true;
 const editor={value:'Unconfirmed note',selection:[4,13]},frame={window:{}},camera={x:7,y:-19,scale:.51};
 const field={editor,frame,camera,hasUnsavedChanges:()=>true,dispose(){counts.fieldDisposed++;editor.value='';},refresh:async()=>{counts.refreshed++;},openCommunity:async id=>record(id),closeCommunity(){}};
 Object.assign(app,{destroyed:false,screenSequence:9,deskAccount:'synthetic_account',profile:{profileId:'synthetic_account'},view:'mine',activeRoute:initial,desk:{},preferences:{},group:null,groupTab:'about',communitySurface:null,communityReturnRoute:null,communityOpenTicket:0,communityPending:0,dialogs:new Set(),root:{dataset:{}},live:{textContent:''},main:{replaceChildren(){counts.mainReplaced++;}},unifiedField:field,
  options:{humanCalls:{end(){counts.callEnded++;}}},directLabels:new Map(),directThreads:[],api:{},accountTask:()=>()=>account,
  chatCleanup:()=>{counts.inboxDisposed++;},mountConversation:()=>({isCurrent:()=>true,flush:async()=>{},hasUnsavedChanges:()=>true,dispose(){counts.chatDisposed++;}}),
  mountFieldCovers(){},renderNavigation(){},renderUnifiedField(){throw Error('retained field may not be recreated');},saveDesk(){},unifiedFieldRoute:()=> 'mine',updateCommunity:value=>updates.push(value.communityId),remember:(...args)=>remembered.push(args),openCommunityApplications:value=>apps.push(value.communityId),toast(){},screenHasUnsavedChanges:()=>true,flushScreen:async()=>{throw Error('in-place community must not flush unrelated work');}});
 return{app,io,counts,updates,apps,remembered,field,editor,frame,camera,deny(){account=false;}};
}

function sidebar(kind,change={}){
 const f=fixture({change}),inbox=f.app.chatCleanup;
 if(kind==='community')f.app.mountChat({},record(),true);
 else f.app.mountDirectChat({},{threadId:'thread_1',relationshipId:'relationship_1',peer:{accountId:'peer_1',label:'Peer'},preferences:{},unreadCount:0});
 assert.equal(f.counts.inboxDisposed,0,'opening a selected conversation must not unsubscribe its still-visible inbox');
 f.app.chatCleanup();assert.equal(f.counts.inboxDisposed,1);assert.equal(f.counts.chatDisposed,1);assert.notEqual(f.app.chatCleanup,inbox);return f;
}
for(const kind of['community','direct'])test(`selected ${kind} composes sidebar and own chat cleanup only at screen close`,()=>sidebar(kind));
test('BROKEN old eager sidebar disposal fails the same observable cleanup oracle',()=>{
 const original=method('mountChat'),anchor='const screenCleanup = this.chatCleanup;';assert.equal(original.split(anchor).length,2);
 assert.throws(()=>sidebar('community',{mountChat:original.replace(anchor,anchor+' screenCleanup?.();')}),/still-visible inbox/);
});
test('a FieldObject-owned chat never captures or invokes the surrounding screen cleanup',()=>{
 const f=fixture(),inbox=f.app.chatCleanup,handle=f.app.mountChat({},record(),false,{isCurrent:()=>true});assert.equal(f.app.chatCleanup,inbox);assert.equal(f.counts.inboxDisposed,0);handle.dispose();assert.equal(f.counts.chatDisposed,1);assert.equal(f.counts.inboxDisposed,0);
});

function cold(initial='#community/room_A/about',hasEarlier=false,change={}){
 const f=fixture({initial,hasEarlier,change});f.app.activeRoute='#mine';f.app.communitySurface={communityId:'room_A',tab:'about',returnRoute:'#mine'};f.app.writeCommunityRoute();f.app.communityFocusChanged(null);
 assert.equal(f.io.counts.backs,0,'a cold or externally-created community entry cannot pop unknown history');assert.equal(f.io.location.hash,'#mine','close must leave the cold link on the retained canvas');return f;
}
test('cold community links close to canvas with one entry or an external predecessor',()=>{cold();cold('#community/room_A/about',true);cold('#community/room_A',true);});
test('warm canvas→room→chat retains one real return entry and goes back exactly once',()=>{
 const f=fixture();f.app.communitySurface={communityId:'room_A',tab:'about',returnRoute:'#mine'};f.app.writeCommunityRoute();f.app.communitySurface.tab='chat';f.app.writeCommunityRoute();f.app.communityFocusChanged(null);
 assert.equal(f.io.counts.pushes,1);assert.equal(f.io.counts.backs,1);assert.equal(f.io.location.hash,'#mine');
});
test('BROKEN old cold-return assumption is rejected without removing valid warm Back',()=>{
 const original=method('communityFocusChanged'),anchor='&& destination.pushed === true';assert.equal(original.split(anchor).length,2);
 assert.throws(()=>cold('#community/room_A/about',true,{communityFocusChanged:original.replace(anchor,'')}),/unknown history/);
});

async function retained(change={}){
 const f=fixture({change}),held=f.field;await f.app.openGroup('room_A','chat');assert.equal(f.app.unifiedField,held,'open must retain the exact field instance');assert.equal(f.counts.fieldDisposed,0);assert.equal(f.counts.mainReplaced,0);assert.equal(f.counts.callEnded,0);assert.equal(f.editor.value,'Unconfirmed note');assert.deepEqual(f.editor.selection,[4,13]);assert.equal(f.field.frame,f.frame);assert.equal(f.field.camera,f.camera);assert.equal(f.app.communitySurface.tab,'chat');return f;
}
test('retained opening does not flush/dispose unrelated dirty input, iframe, camera or Root call',()=>retained());
test('BROKEN former cleanScreen boundary is detected even if metadata can reload later',async()=>{
 const original=method('openGroup'),anchor='const field = this.unifiedField;';assert.equal(original.split(anchor).length,2);
 await assert.rejects(retained({openGroup:original.replace(anchor,'this.cleanScreen(); '+anchor)}),/exact field/);
});
test('late room A resolution cannot replace newer B, or repaint after account loss',async()=>{
 const f=fixture(),pending=deferred();f.field.openCommunity=id=>id==='room_A'?pending.promise:Promise.resolve(record(id));const first=f.app.openGroup('room_A','apps');await f.app.openGroup('room_B','chat');pending.resolve(record('room_A'));await first;
 assert.equal(f.app.communitySurface.communityId,'room_B');assert.deepEqual(f.updates,['room_B']);assert.deepEqual(f.apps,[]);
 const g=fixture(),late=deferred();g.field.openCommunity=()=>late.promise;const opening=g.app.openGroup('room_A');g.deny();late.resolve(record());await opening;assert.deepEqual(g.updates,[]);assert.equal(g.counts.callEnded,0);
});

function find(node,predicate){if(predicate(node))return node;let result;ts.forEachChild(node,child=>{result??=find(child,predicate);});return result;}
async function staleManagement(broken=false){
 const management=appClass.members.find(member=>member.name?.getText(ast)==='openGroupManagement'),call=find(management,node=>ts.isCallExpression(node)&&node.expression.getText(ast)==='button'&&node.arguments[0]?.getText(ast).startsWith('group.membership.muted'));
 const guard=find(management,node=>ts.isVariableDeclaration(node)&&node.name.getText(ast)==='active');assert.ok(call&&guard);
 let callback=printer.printNode(ts.EmitHint.Unspecified,call.arguments[3],ast);if(broken){assert.ok(callback.includes('if (!active())'));callback=callback.replace('if (!active())','if (false)');}
 const f=fixture(),pending=deferred(),dialog={element:{open:true},close(){this.element.open=false;closes++;}},group=record('room_A'),updates=[];let closes=0;
 f.app.api={request:()=>pending.promise};f.app.updateCommunity=value=>updates.push(value.communityId);
 const context=vm.createContext({app:f.app,group,dialog,error:{},accountCurrent:()=>true,sequence:f.app.screenSequence});
 vm.runInContext(compile('function bind(){const active='+printer.printNode(ts.EmitHint.Expression,guard.initializer,ast)+';return '+callback+';}globalThis.invoke=bind.call(app);'),context);
 context.invoke();f.app.communitySurface={communityId:'room_B',tab:'chat',returnRoute:'#mine'};dialog.element.open=false;pending.resolve({community:record('room_A')});await new Promise(resolve=>setImmediate(resolve));
 assert.deepEqual(updates,[],'a closed A tool cannot apply its ACK while B owns the surface');assert.equal(closes,0);assert.equal(f.app.group,null);assert.equal(f.app.communitySurface.communityId,'room_B');
}
test('closed room A management ACK is fenced after opening B',()=>staleManagement());
test('BROKEN missing management fence fails the same A→B late-ACK oracle',async()=>{await assert.rejects(staleManagement(true),/closed A tool/);});

test('review pins the actual methods under test without claiming browser/audio authority',t=>{t.diagnostic('app.ts sha256 '+createHash('sha256').update(source).digest('hex'));assert.ok(names.every(name=>method(name).length));});
