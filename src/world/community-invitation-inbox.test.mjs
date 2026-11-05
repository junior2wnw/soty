import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';

const source=readFileSync(new URL('./app.ts',import.meta.url),'utf8');
const ast=ts.createSourceFile('app.ts',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS),printer=ts.createPrinter();
const appClass=ast.statements.find(node=>ts.isClassDeclaration(node)&&node.name?.text==='WorldApplication');
const method=name=>appClass.members.find(node=>ts.isMethodDeclaration(node)&&node.name.getText(ast)===name);
const local=(body,name)=>body.statements.filter(ts.isVariableStatement).flatMap(node=>node.declarationList.declarations).find(node=>node.name.getText(ast)===name);
const print=node=>printer.printNode(ts.EmitHint.Unspecified,node,ast);
const compile=code=>ts.transpileModule(code,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;

class Element {
 constructor(tag='div',className='',text=''){this.tag=tag;this.className=className??'';this.text=text;this.children=[];this.attrs={};this.dataset={};this.listeners={};this.hidden=false;this.value='';this.disabled=false;this.classList={toggle(){}};}
 get textContent(){return this.text+this.children.map(node=>node.textContent??'').join('');}
 set textContent(value){this.text=value;this.children=[];}
 append(...nodes){this.children.push(...nodes);}
 replaceChildren(...nodes){this.text='';this.children=[...nodes];}
 setAttribute(name,value){this.attrs[name]=String(value);}
 getAttribute(name){return this.attrs[name]??null;}
 addEventListener(name,callback){this.listeners[name]=callback;}
 contains(node){return this.children.includes(node)||this.children.some(child=>child.contains?.(node));}
 click(){this.listeners.click?.({detail:1});}
 focus(){}
 remove(){this.removed=true;}
}
const el=(...args)=>new Element(...args),button=(label,_symbol,className,action)=>{const node=el('button',className,label);node.addEventListener('click',action);return node;};
const group=(id,state='invited')=>({communityId:id,name:'Приглашение '+id,description:'',topics:[],unreadCount:0,memberCount:1,membership:{state,role:'member',muted:false},permissions:{canWrite:state==='active'}});

function listFixture(communities,{query='',unreadOnly=false}={}){
 const body=method('renderMessages').body,groups=local(body,'groups'),render=local(body,'renderList'),paint=local(body,'paintInvitation');
 const conversations=el(),conversationRows=new Map(),opened=[];let alive=true;
 const app={communities,profile:{profileId:'viewer'},deskAccount:'viewer',directThreads:[],directLabels:new Map(),applicationDiscussions:[],applicationInboxStatus:'ready',applicationReadState:'synced',applicationInboxNextCursor:null,applicationInboxPending:false,messagesView:{},openGroup(id,tab){opened.push([id,tab]);}};
 const env=vm.createContext({exports:{},app,el,button,icon:()=>el('svg'),communityEmblem:()=>el('span','emblem'),active:()=>alive,
  directEnabled:false,selectedId:undefined,unreadOnly,query:{value:query},clearSearch:el(),allFilter:el(),unreadFilter:el(),
  conversations,conversationRows,listStatus:el(),applicationStatus:el(),moreApplications:el(),previews:new Map(),
  paintConversation:value=>{const row=el('button','sw-conversation',value.name);row.dataset.communityId=value.communityId;return row;},
  document:{activeElement:null,createTextNode:text=>el('text','',text)},HTMLButtonElement:Element,emptyState:()=>el('div','empty'),
 });
 vm.runInContext(compile(`(function(){const groups=${print(groups.initializer)};const accountId='viewer';${paint?`const paintInvitation=${print(paint.initializer)};`:''}const renderList=${print(render.initializer)};exports.render=renderList;exports.activeGroups=groups;}).call(app);`),env);
 env.exports.render();
 return{app,opened,render:env.exports.render,rows:()=>conversations.children.filter(node=>node.className.split(' ').includes('sw-conversation')),activeGroups:env.exports.activeGroups,retire(){alive=false;}};
}

function invitationOracle(){
 const invited=group('private_invited'),f=listFixture([invited]);
 const rows=f.rows().filter(row=>row.dataset.invitationId===invited.communityId);
 assert.equal(rows.length,1,'an invited private room has an actionable row before becoming an active conversation');
 assert.match(rows[0].textContent,/Приглашение private_invited/u);assert.match(rows[0].textContent,/Приглашение в комнату/u);
 assert.equal(f.activeGroups.length,0,'an invitation must not enter the active chat gate');
 assert.deepEqual(f.opened,[],'rendering does not join or launch a private room');
 rows[0].click();assert.deepEqual(f.opened,[[invited.communityId,'about']]);
}

test('an authoritative pending invitation appears before joining and opens the existing acceptance screen',invitationOracle);
test('removed, requested, banned and unrelated rooms do not become invitation rows',()=>{
 const f=listFixture(['active','requested','removed','banned','left'].map(state=>group('room_'+state,state)));
 assert.equal(f.rows().filter(row=>row.dataset.invitationId).length,0);assert.equal(f.activeGroups.length,1);
});
test('invitation search keeps its real name and an unread filter does not hide a pending action',()=>{
 const f=listFixture([group('Альфа'),group('Бета')],{query:'АЛЬФА',unreadOnly:true});
 assert.equal(f.rows().length,1);assert.equal(f.rows()[0].dataset.invitationId,'Альфа');
});
test('a stale invitation action cannot join after removal, acceptance or an account/screen retirement',()=>{
 for(const next of ['removed','active']){
  const invited=group('pending_'+next),f=listFixture([invited]),row=f.rows()[0];
  assert.ok(row,'the test must exercise an actual invitation action');f.app.communities=[group(invited.communityId,next)];row.click();assert.deepEqual(f.opened,[]);f.render();assert.equal(f.rows().some(value=>value.dataset.invitationId),false);
 }
 const f=listFixture([group('old_account')]),row=f.rows()[0];assert.ok(row);f.retire();row.click();assert.deepEqual(f.opened,[]);
 const switched=listFixture([group('switched_account')]),stale=switched.rows()[0];switched.app.profile={profileId:'another_account'};stale.click();assert.deepEqual(switched.opened,[]);
});

function navFixture(communities,{portal=false,portalOpen=true}={}){
 const nav=el('button'),contacts=el('button'),portalNav=el('button'),summaries=[];
 nav.querySelector=()=>nav.children.find(node=>node.className==='sx-nav-unread')??null;
 portalNav.querySelector=()=>portalNav.children.find(node=>node.className==='sx-nav-unread')??null;
 contacts.querySelector=()=>null;
 const app={communities,directThreads:[],applicationDiscussions:[],applicationInboxNextCursor:null,applicationReadState:'synced',applicationInboxStatus:'ready',profile:{},incomingContactRequests:0,
  ...(portal?{fieldMenu:{element:{open:portalOpen,querySelectorAll:selector=>selector==='[data-nav-view="messages"]'?[portalNav]:[]}}}:{}),
  unifiedField:{setInboxSummary(value){summaries.push(value);}},root:{querySelectorAll(selector){return selector==='[data-nav-view="messages"]'?[nav]:selector==='[data-inbox-requests]'?[contacts]:[];}}};
 const env=vm.createContext({exports:{},app,el});
 vm.runInContext(compile(`function update()${print(method('updateNavUnread').body)};exports.update=update;`),env);
 env.exports.update.call(app);return{nav,portalNav,summaries};
}
test('the canvas and Messages count distinguish invitations from messages and contact requests',()=>{
 const invited=group('private_pending');invited.unreadCount=999;
 const f=navFixture([invited]);assert.equal(f.summaries[0].messages,0);assert.equal(f.summaries[0].requests,0);assert.equal(f.summaries[0].invitations,1);
 assert.match(f.nav.attrs['aria-label'],/приглашений в комнаты: 1/u);assert.doesNotMatch(f.nav.attrs['aria-label'],/непрочитанных/u);assert.equal(f.nav.children[0].textContent,'1');
});
test('opening the current menu portal paints invitations immediately without a follow-up Source event',()=>{
 const f=navFixture([group('portal_pending')],{portal:true});
 assert.match(f.portalNav.attrs['aria-label']??'',/приглашений в комнаты: 1/u);assert.equal(f.portalNav.children[0]?.textContent,'1');
 const closed=navFixture([group('retired_portal')],{portal:true,portalOpen:false});assert.equal(closed.portalNav.attrs['aria-label'],undefined);assert.equal(closed.portalNav.children.length,0);
});
