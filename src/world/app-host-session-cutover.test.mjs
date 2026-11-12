import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {DatabaseSync} from 'node:sqlite';
import vm from 'node:vm';
import ts from 'typescript';
import {createAppLauncher,isScopedRuntimeProfile} from './app-launch.mjs';
import {createHostSessionContinuity} from '../../modules/apps/server/host-session-continuity.mjs';
import {FRAME_CONTINUITY_STORAGE,frameContinuityDdl} from '../../modules/apps/server/frame-continuity.mjs';

// Run the actual Stage cutover with the real launcher and real SQLite member
// store. Only browser nodes, transport and unrelated Stage services are fixtures.
const source=readFileSync(new URL('./app-stage.ts',import.meta.url),'utf8');
const parsed=ts.createSourceFile('app-stage.ts',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS),printer=ts.createPrinter();
const mount=parsed.statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text==='mountAppStage');
const launch=mount.body.statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text==='launchRuntime');
const compiled=ts.transpileModule(`exports.create=(launcher,runtime,state,appWork,relayIdentity)=>{
 const current=()=>state.current,serverNow=()=>state.at,app=state.app,accountId=state.actor.accountId;
 let runtimePending=false,runtimeStarted=false,liveAuthorityEpoch=0,liveAuthorityDenied=false,frameGeneration=0,relayPeer=null;
 let externalAvailability='unknown',entryRecoveryEpoch=0,entryRecoveryPending=false,lastRuntimeLaunchAt=0;
 let hostRenewal=null,voiceResolved=true,nativeVoice=null,widgetContext=null,widgetMode='interactive';
 const view={location:{origin:'https://soty.example',href:'https://soty.example/#mine'},setTimeout,clearTimeout};
 const options={api:{request:async()=>{}},instanceId:'main'},screen={append(){},dataset:{}};
 const saved={setEntryVerified(){}},refresh={},recoveryRetry={},externalRecovery={},external={},currentEntryRetry={};
 const message={textContent:''},showMessage=value=>{message.textContent=value;state.messages.push(value);};
 const stopAssist=()=>{},resetRuntimeDisplay=()=>{},captureEntry=()=>{},ensureDiscussion=()=>{},showPanel=()=>{},checkExternalAvailability=()=>{};
 const voiceOriginAllowed=()=>true,resolveVoice=async()=>{},watchRuntimeDisplay=()=>{};
 const runtimeTrace={beginManual(){},attach(){},inserted(){},launchFailed(){state.failed++;}};
 const recovery={bind(){}},createAppHostSessionRenewal=()=>({start(){},dispose(){}});
 const el=(tag,_class,text)=>state.node(tag,text),button=()=>({}),emptyState=()=>state.node('error');
 const appLaunchFailure=error=>({title:error.code??error.message,detail:'fixture'});
 ${printer.printNode(ts.EmitHint.Unspecified,launch,parsed)}
 return{launch:()=>launchRuntime(),busy:()=>runtimePending};
};`,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;

const appId='app-'+'1'.repeat(32),domainId='dom_'+'2'.repeat(32),actor={accountId:'synthetic_owner',deviceId:'synthetic_device'};
const entry={appId,domainId,origin:'https://app.example',path:'/reader'};
const runtimeOrigin='https://app-m'+appId.slice(4)+'-'+'8'.repeat(24)+'.example';
const pin={schema:'soty.app-launch-binding.v1',policyEpoch:1,targetRevision:1,targetDigest:'4'.repeat(64),profile:'soty.relay-restricted.v1',bindingFloor:1};
const scope=(person=actor)=>({schema:'soty.app-host-scope.v1',...person,appId,policyEpoch:1,
 entry:{domainId,origin:entry.origin,pathDigest:createHash('sha256').update(entry.path).digest('hex')},
 target:{revision:1,digest:pin.targetDigest,profile:pin.profile,bindingFloor:1},instanceId:'main',runtimeOrigin,runtimeScopeDigest:'8'.repeat(64)});
const tick=()=>new Promise(setImmediate);

function fixture(t){
 const db=new DatabaseSync(':memory:');db.exec('CREATE TABLE local_apps(id TEXT PRIMARY KEY);CREATE TABLE apps_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);');
 db.prepare('INSERT INTO local_apps VALUES(?)').run(appId);db.exec(frameContinuityDdl());db.prepare('INSERT INTO apps_meta VALUES(?,?)').run('frame_continuity',FRAME_CONTINUITY_STORAGE);
 const state={actor,app:{appId,name:'Synthetic'},at:Date.now(),current:true,issued:[],closed:[],messages:[],failed:0,requiresCheck:false,checks:0,dirty:false,closing:false};
 const store=createHostSessionContinuity({db,clock:()=>state.at}),byUrl=new Map();let child=null;
 const runtime={querySelector:()=>child?.tag==='iframe'?child:null,setAttribute(){},
   replaceChildren(value){if(child)child.isConnected=false;child=value??null;if(child)child.isConnected=true;}};
 state.node=(tag,text)=>({tag,textContent:text,isConnected:false,setAttribute(){},addEventListener(){},removeEventListener(){},
   set src(value){this.frameId=byUrl.get(value);},contentWindow:{postMessage(){}}});
 const close=async args=>{
   state.closed.push({...args});
   assert.ok(state.closing||runtime.querySelector('iframe')?.frameId!==args.frameId,'a live frame must keep its own lease');
   if(state.closeError)throw state.closeError;
   if(state.delayClose)await new Promise(resolve=>state.releaseClose=resolve);
   return{closed:store.close(actor,appId,args)};
 };
 const launcher=createAppLauncher({target:{appId,domainId,path:entry.path},accountId:actor.accountId,instanceId:'main',shellUrl:'https://soty.example/#mine',isCurrent:()=>state.current,
   request:async()=>{
     if(state.requestError)throw state.requestError;
     const member=store.create(scope()),claim=store.activateInitial(member.claim),hostSession={...member.descriptor,expiresAt:claim.expiresAt};
     const url=runtimeOrigin+'/_soty/boot?path=%2Freader#'+member.descriptor.frameId;byUrl.set(url,member.descriptor.frameId);
     state.issued.push({descriptor:hostSession,claim});
     await state.afterIssue?.();
     return{url:state.invalidUrl?'https://foreign.example/':url,entry,runtimeOrigin,launchBinding:pin,hostSession,hostSessionExpiresAt:state.at+120000};
   },abandonHost:close});
 const work={requiresCheck:()=>state.requiresCheck,attach(){},async flush(){state.checks++;if(state.saveError)throw state.saveError;if(state.dirty)throw Error('soty_work_not_saved');}};
 const identity={hasActive:()=>!!state.identityActive,clear:async()=>{await state.onIdentityClose?.();state.identityActive=false;},resume(){}};
 const context=vm.createContext({exports:{},isScopedRuntimeProfile,setTimeout,clearTimeout,URL});new vm.Script(compiled).runInContext(context);
 const stage=context.exports.create(launcher,runtime,state,work,identity);
 const active=()=>db.prepare("SELECT count(*) n FROM app_frame_continuity WHERE state='open'").get().n;
 const current=descriptor=>store.current(descriptor,actor,appId);
 t.after(async()=>{state.closing=true;state.closeError=null;state.releaseClose?.();launcher.dispose();await tick();db.close();});
 return{state,store,db,runtime,launcher,stage,active,current,extra(){const member=store.create(scope());member.claim=store.activateInitial(member.claim);return member;}};
}

test('twenty actual Stage reopens retire each replaced member only after its frame leaves and never hit family capacity',async t=>{
 const f=fixture(t);
 for(let index=0;index<20;index++){
   await f.stage.launch();await tick();
   assert.equal(f.state.failed,0,`reopen ${index+1} must succeed`);
   assert.equal(f.state.issued.length,index+1);assert.equal(f.active(),1);
   assert.equal(f.runtime.querySelector('iframe').frameId,f.launcher.hostSession().hostSession.frameId);
 }
 assert.equal(f.state.closed.length,19);
});

test('a failed or malformed candidate preserves the existing frame and live lease, closing only any unused candidate',async t=>{
 for(const invalid of [false,true]){
   const f=fixture(t);await f.stage.launch();const frame=f.runtime.querySelector('iframe'),before=f.launcher.hostSession();
   if(invalid)f.state.invalidUrl=true;else f.state.requestError=Object.assign(Error('busy'),{code:'apps_sessions_busy'});
   await f.stage.launch();await tick();
   assert.equal(f.runtime.querySelector('iframe'),frame);assert.equal(f.launcher.hostSession(),before);f.current(before.hostSession);
   assert.equal(f.active(),1);assert.equal(f.state.closed.length,invalid?1:0);
 }
});

test('dirty or unknown save before launch never allocates or retires another member',async t=>{
 const f=fixture(t);await f.stage.launch();const frame=f.runtime.querySelector('iframe'),before=f.launcher.hostSession();
 f.state.requiresCheck=true;
 for(const failure of [Error('soty_work_not_saved'),Error('soty_work_timeout')]){
   f.state.saveError=failure;await f.stage.launch();await tick();
   assert.equal(f.state.issued.length,1);assert.equal(f.state.closed.length,0);assert.equal(f.runtime.querySelector('iframe'),frame);f.current(before.hostSession);
 }
});

test('an edit or unknown save while admission is pending keeps the original frame and retires only the unshown candidate',async t=>{
 for(const unknown of [false,true]){
   const f=fixture(t);await f.stage.launch();const frame=f.runtime.querySelector('iframe'),before=f.launcher.hostSession();f.state.requiresCheck=true;
   f.state.afterIssue=async()=>{if(unknown)f.state.saveError=Error('soty_work_timeout');else f.state.dirty=true;};
   await f.stage.launch();await tick();
   assert.equal(f.runtime.querySelector('iframe'),frame);assert.equal(f.launcher.hostSession(),before);f.current(before.hostSession);
   assert.equal(f.active(),1);assert.equal(f.state.closed.length,1);assert.notEqual(f.state.closed[0].frameId,before.hostSession.frameId);
 }
});

test('a late account departure response cannot retire the still retained old frame',async t=>{
 const f=fixture(t);await f.stage.launch();const before=f.launcher.hostSession(),frame=f.runtime.querySelector('iframe');
 f.state.afterIssue=async()=>{f.state.current=false;};await f.stage.launch();await tick();
 assert.equal(f.runtime.querySelector('iframe'),frame);f.current(before.hostSession);assert.equal(f.active(),1);
 assert.equal(f.state.closed.length,1);assert.notEqual(f.state.closed[0].frameId,before.hostSession.frameId);
});

test('the last identity-close wait cannot hide a new edit or a now-unknown work state',async t=>{
 for(const unknown of [false,true]){
   const f=fixture(t);await f.stage.launch();const before=f.launcher.hostSession(),frame=f.runtime.querySelector('iframe');f.state.requiresCheck=true;
   f.state.afterIssue=async()=>{f.state.identityActive=true;};
   f.state.onIdentityClose=async()=>{if(unknown)f.state.requiresCheck=false;else f.state.dirty=true;};
   await f.stage.launch();await tick();
   assert.equal(f.runtime.querySelector('iframe'),frame);assert.equal(f.launcher.hostSession(),before);f.current(before.hostSession);
   assert.equal(f.active(),1);assert.equal(f.state.closed.length,1);assert.notEqual(f.state.closed[0].frameId,before.hostSession.frameId);
 }
});

test('delayed exact retirement cannot close a promoted same-member successor or a second live tab',async t=>{
 const f=fixture(t);await f.stage.launch();const old=f.state.issued[0],other=f.extra();f.state.delayClose=true;
 await f.stage.launch();await tick();assert.equal(f.state.closed.length,1);
 const successor=f.store.prepare(old.claim);f.store.promote(successor.claim);
 f.state.releaseClose();f.state.delayClose=false;await tick();
 assert.doesNotThrow(()=>f.current(successor.descriptor));assert.doesNotThrow(()=>f.current(other.descriptor));
 assert.doesNotThrow(()=>f.current(f.launcher.hostSession().hostSession));
});

test('an unconfirmed retirement never closes the replacement or sweeps another tab',async t=>{
 const f=fixture(t);await f.stage.launch();const other=f.extra();f.state.closeError=Object.assign(Error('unknown close'),{code:'NETWORK_TIMEOUT'});
 await f.stage.launch();await tick();
 assert.equal(f.state.closed.length,1);assert.notEqual(f.state.closed[0].frameId,f.launcher.hostSession().hostSession.frameId);
 assert.doesNotThrow(()=>f.current(other.descriptor));assert.doesNotThrow(()=>f.current(f.launcher.hostSession().hostSession));
});
