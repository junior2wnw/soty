import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createHash,randomUUID} from 'node:crypto';
import {DatabaseSync} from 'node:sqlite';
import vm from 'node:vm';
import ts from 'typescript';
import {createAppLauncher,isScopedRuntimeProfile} from './app-launch.mjs';
import {createRelayIdentityBridge} from './app-relay-identity-bridge.mjs';
import {createHostSessionContinuity} from '../../modules/apps/server/host-session-continuity.mjs';
import {FRAME_CONTINUITY_STORAGE,frameContinuityDdl} from '../../modules/apps/server/frame-continuity.mjs';

// Run the actual Stage cutover with the real launcher and real SQLite member
// store. Only browser nodes, transport and unrelated Stage services are fixtures.
const source=readFileSync(new URL('./app-stage.ts',import.meta.url),'utf8');
const parsed=ts.createSourceFile('app-stage.ts',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS),printer=ts.createPrinter();
const mount=parsed.statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text==='mountAppStage');
const launch=mount.body.statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text==='launchRuntime');
const captureEntry=mount.body.statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text==='captureEntry');
const compiled=ts.transpileModule(`exports.create=(launcher,runtime,state,appWork,relayIdentity)=>{
 const current=()=>state.current,serverNow=()=>state.at,app=state.app,accountId=state.actor.accountId;
 let runtimePending=false,runtimeStarted=false,liveAuthorityEpoch=0,liveAuthorityDenied=false,frameGeneration=0,relayPeer=null;
 let externalAvailability='unknown',entryRecoveryEpoch=0,entryRecoveryPending=false,lastRuntimeLaunchAt=0;
 let hostRenewal=null,voiceResolved=true,nativeVoice=null,widgetContext=null,widgetMode='interactive';
 const view={location:{origin:'https://soty.example',href:'https://soty.example/#mine'},setTimeout,clearTimeout};
 const options={api:{request:async()=>{}},instanceId:'main',onNavigate:value=>state.routes.push(value.path),onRemember:()=>state.remembered++},screen={append(){},dataset:{}};
 let saved=null,selectedEntry=null,intent={presentation:undefined};const field={},widget={},bookmark={},savedHost={},savedReplacementHost={hidden:true};
 const more={},moreBody={hidden:true},presented=current,routeWith=()=>({path:selectedEntry?.path});
 const mountAppSaved=(_host,value)=>{state.bookmarkEntry=value.entry;return{setEntryVerified:value=>state.verified.push(value)};};
 const discussion={updateEntry:async value=>state.discussionEntries.push(value.path)};
 const refresh={},recoveryRetry={},externalRecovery={},external={},currentEntryRetry={};
 const message={textContent:''},showMessage=value=>{message.textContent=value;state.messages.push(value);};
 const stopAssist=()=>{},resetRuntimeDisplay=()=>{},ensureDiscussion=()=>{},showPanel=()=>{},checkExternalAvailability=()=>{state.externalReads++;};
 const voiceOriginAllowed=()=>true,resolveVoice=async()=>{},watchRuntimeDisplay=()=>{};
 const runtimeTrace={beginManual(){},attach(){},inserted(){},launchFailed(){state.failed++;}};
 const recovery={bind(){}},createAppHostSessionRenewal=()=>({start(){},dispose(){}});
 const el=(tag,_class,text)=>state.node(tag,text),button=()=>({}),emptyState=()=>state.node('error');
 const appLaunchFailure=error=>({title:error.code??error.message,detail:'fixture'});
 ${printer.printNode(ts.EmitHint.Unspecified,captureEntry,parsed)}
 ${printer.printNode(ts.EmitHint.Unspecified,launch,parsed)}
 return{launch:()=>launchRuntime(),busy:()=>runtimePending,entry:()=>selectedEntry,peer:()=>relayPeer,
   loseSource(){liveAuthorityDenied=true;liveAuthorityEpoch++;},navigateFrame(){frameGeneration++;}};
};`,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;

const appId='app-'+'1'.repeat(32),domainId='dom_'+'2'.repeat(32),actor={accountId:'synthetic_owner',deviceId:'synthetic_device'};
const entry={appId,domainId,origin:'https://app.example',path:'/reader'};
const runtimeOrigin='https://app-m'+appId.slice(4)+'-'+'8'.repeat(24)+'.example';
const pin={schema:'soty.app-launch-binding.v1',policyEpoch:1,targetRevision:1,targetDigest:'4'.repeat(64),profile:'soty.relay-restricted.v1',bindingFloor:1};
const scope=(person=actor)=>({schema:'soty.app-host-scope.v1',...person,appId,policyEpoch:1,
 entry:{domainId,origin:entry.origin,pathDigest:createHash('sha256').update(entry.path).digest('hex')},
 target:{revision:1,digest:pin.targetDigest,profile:pin.profile,bindingFloor:1},instanceId:'main',runtimeOrigin,runtimeScopeDigest:'8'.repeat(64)});
const tick=()=>new Promise(setImmediate);

function fixture(t,{realIdentity=false}={}){
 const db=new DatabaseSync(':memory:');db.exec('CREATE TABLE local_apps(id TEXT PRIMARY KEY);CREATE TABLE apps_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);');
 db.prepare('INSERT INTO local_apps VALUES(?)').run(appId);db.exec(frameContinuityDdl());db.prepare('INSERT INTO apps_meta VALUES(?,?)').run('frame_continuity',FRAME_CONTINUITY_STORAGE);
 const state={actor,app:{appId,name:'Synthetic'},at:Date.now(),current:true,issued:[],closed:[],messages:[],failed:0,requiresCheck:false,checks:0,dirty:false,closing:false,
   verified:[],routes:[],remembered:0,bookmarkEntry:null,discussionEntries:[],externalReads:0};
 const store=createHostSessionContinuity({db,clock:()=>state.at}),byUrl=new Map();let child=null;
 const runtime={querySelector:()=>child?.tag==='iframe'?child:null,setAttribute(){},
   replaceChildren(value){if(child)child.isConnected=false;child=value??null;if(child)child.isConnected=true;}};
 state.node=(tag,text)=>({tag,textContent:text,isConnected:false,setAttribute(){},addEventListener(){},removeEventListener(){},
   set src(value){this.frameId=byUrl.get(value);},contentWindow:{postMessage(value){state.identityReplies?.push(value.type);}}});
 const close=async args=>{
   state.closed.push({...args});
   assert.ok(state.closing||runtime.querySelector('iframe')?.frameId!==args.frameId,'a live frame must keep its own lease');
   if(state.closeError)throw state.closeError;
   if(state.delayClose)await new Promise(resolve=>state.releaseClose=resolve);
   return{closed:store.close(actor,appId,args)};
 };
 const launcher=createAppLauncher({target:{appId,domainId,path:entry.path},accountId:actor.accountId,instanceId:'main',shellUrl:'https://soty.example/#mine',isCurrent:()=>state.current,
   request:async()=>{
     if(state.fixedReply)return state.fixedReply;
     if(state.requestError)throw state.requestError;
     const member=store.create(scope()),claim=store.activateInitial(member.claim),hostSession={...member.descriptor,expiresAt:claim.expiresAt};
     const url=runtimeOrigin+'/_soty/boot?path=%2Freader#'+member.descriptor.frameId;byUrl.set(url,member.descriptor.frameId);
     state.issued.push({descriptor:hostSession,claim});
     await state.afterIssue?.();
     return{url:state.invalidUrl?'https://foreign.example/':url,entry,runtimeOrigin,launchBinding:pin,hostSession,hostSessionExpiresAt:state.at+120000};
   },abandonHost:close});
 const work={requiresCheck:()=>state.requiresCheck,attach(){},async flush(){state.checks++;if(state.saveError)throw state.saveError;if(state.dirty)throw Error('soty_work_not_saved');}};
 let stage,identity;
 if(realIdentity){
   const listeners=new Set();state.identityRequests=[];state.identityReplies=[];state.identityResumes=0;
   const view={performance:{now:()=>state.at},setTimeout,clearTimeout,
     addEventListener(type,listener){if(type==='message')listeners.add(listener);},removeEventListener(type,listener){if(type==='message')listeners.delete(listener);}};
   const bridge=createRelayIdentityBridge({view,isCurrent:()=>state.current,
     getFrame:()=>{const peer=stage?.peer(),host=launcher.hostSession()?.hostSession;return runtime.querySelector('iframe')===peer?.frame
       &&(peer.hostFrameId===null||host?.frameId===peer.hostFrameId)?peer.frame:null;},
     getEntry:()=>stage?.peer()?.entry,getBinding:()=>stage?.peer()?.binding,getRuntimeScope:()=>stage?.peer()?.runtimeScope,
     getHostSession:()=>{const value=launcher.hostSession()?.hostSession;return value&&value.frameId===stage?.peer()?.hostFrameId
       ?{frameId:value.frameId,lease:value.lease,revision:value.revision}:null;},
     request:async args=>{store.current(args.hostSession,actor,appId);state.identityRequests.push(args);
       return{schema:'soty.relay-window-grant.v1',grant:'G'.repeat(43),expiresAt:state.at+30000};},
     close:async()=>{await state.onIdentityClose?.();return{closed:true};}});
   identity={...bridge,resume(){state.identityResumes++;bridge.resume();}};
   state.askIdentity=()=>{const frame=runtime.querySelector('iframe');for(const listener of listeners)listener({source:frame?.contentWindow,origin:runtimeOrigin,
     data:{schema:'soty.relay-window-identity.v1',type:'request',requestId:randomUUID()}});};
 }else identity={hasActive:()=>!!state.identityActive,clear:async()=>{await state.onIdentityClose?.();state.identityActive=false;},resume(){}};
 const context=vm.createContext({exports:{},isScopedRuntimeProfile,setTimeout,clearTimeout,URL});new vm.Script(compiled).runInContext(context);
 stage=context.exports.create(launcher,runtime,state,work,identity);
 const active=()=>db.prepare("SELECT count(*) n FROM app_frame_continuity WHERE state='open'").get().n;
 const current=descriptor=>store.current(descriptor,actor,appId);
 t.after(async()=>{state.closing=true;state.closeError=null;state.onIdentityClose=null;identity.dispose?.();state.releaseClose?.();launcher.dispose();await tick();db.close();});
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

for(const refusal of ['save-failed','timeout','new-edit','unknown-work'])test(`real relay: a retained frame gets its exact old lease back after ${refusal}`,async t=>{
 const f=fixture(t,{realIdentity:true});await f.stage.launch();const frame=f.runtime.querySelector('iframe'),before=f.launcher.hostSession();
 f.state.askIdentity();await tick();assert.equal(f.state.identityRequests.length,1);
 f.state.requiresCheck=true;f.state.afterIssue=async()=>{
   if(refusal==='new-edit')f.state.dirty=true;
   else if(refusal==='unknown-work')f.state.requiresCheck=false;
   else f.state.saveError=Error(refusal==='timeout'?'soty_work_timeout':'save_failed');
 };
 await f.stage.launch();await tick();
 assert.equal(f.runtime.querySelector('iframe'),frame);assert.equal(f.launcher.hostSession(),before);f.current(before.hostSession);
 assert.equal(f.active(),1);assert.equal(f.stage.busy(),false);assert.equal(f.state.closed.length,1);
 f.state.askIdentity();await tick();assert.equal(f.state.identityRequests.length,2);assert.equal(f.state.identityReplies.at(-1),'grant');
 assert.deepEqual(f.state.identityRequests.at(-1).hostSession,{frameId:before.hostSession.frameId,lease:before.hostSession.lease,revision:before.hostSession.revision});
 assert.equal(f.state.identityRequests.at(-1).launchBinding,before.launchBinding);
});

test('real relay: pending old-host renewal keeps its newest exact proof after a declined cutover',async t=>{
 const f=fixture(t,{realIdentity:true});await f.stage.launch();const before=f.launcher.hostSession(),old=f.state.issued[0];
 f.state.askIdentity();await tick();f.state.requiresCheck=true;let successor;
 f.state.afterIssue=async()=>{
   successor=f.store.prepare(old.claim);f.store.promote(successor.claim);
   assert.equal(f.launcher.commitHostSession(before,{...before,hostSession:successor.descriptor}),true);f.state.dirty=true;
 };
 await f.stage.launch();await tick();f.state.askIdentity();await tick();
 const current=f.launcher.hostSession().hostSession;
 assert.equal(current.frameId,before.hostSession.frameId);assert.equal(current.revision,2);assert.equal(f.active(),1);
 assert.equal(f.state.identityRequests.length,2);assert.deepEqual(f.state.identityRequests.at(-1).hostSession,
   {frameId:current.frameId,lease:successor.descriptor.lease,revision:2});
});

for(const change of ['actor','source','replaced-frame','navigated-frame'])test(`real relay: no retained-identity resume after ${change} departure`,async t=>{
 const f=fixture(t,{realIdentity:true});await f.stage.launch();f.state.askIdentity();await tick();const resumes=f.state.identityResumes;
 f.state.afterIssue=async()=>{
   if(change==='actor')f.state.current=false;
   else if(change==='source')f.stage.loseSource();
   else if(change==='navigated-frame')f.stage.navigateFrame();
   else f.runtime.replaceChildren(f.state.node('iframe'));
 };
 await f.stage.launch();await tick();assert.equal(f.state.identityResumes,resumes);
 f.state.askIdentity();await tick();assert.equal(f.state.identityRequests.length,1);
});

for(const timing of ['before-admission','after-admission'])test(`real relay: unknown identity close stays paused ${timing} until that close is confirmed`,async t=>{
 const f=fixture(t,{realIdentity:true});await f.stage.launch();const frame=f.runtime.querySelector('iframe'),before=f.launcher.hostSession(),resumes=f.state.identityResumes;
 const unknown=()=>{throw Object.assign(Error('unknown_close'),{code:'NETWORK_TIMEOUT'});};
 if(timing==='before-admission'){f.state.askIdentity();await tick();f.state.onIdentityClose=unknown;}
 else f.state.afterIssue=async()=>{f.state.askIdentity();await tick();f.state.onIdentityClose=unknown;};
 await f.stage.launch();await tick();assert.equal(f.state.identityResumes,resumes);
 assert.equal(f.runtime.querySelector('iframe'),frame);assert.equal(f.launcher.hostSession(),before);f.current(before.hostSession);
 f.state.askIdentity();await tick();assert.equal(f.state.identityRequests.length,1);
 f.state.onIdentityClose=null;f.state.afterIssue=null;await f.stage.launch();await tick();
 assert.notEqual(f.runtime.querySelector('iframe'),frame);f.state.askIdentity();await tick();assert.equal(f.state.identityRequests.length,2);
});

test('real relay: a transient admission refusal restores the still-authorized old frame',async t=>{
 const f=fixture(t,{realIdentity:true});await f.stage.launch();f.state.askIdentity();await tick();
 const before=f.launcher.hostSession();f.state.requestError=Object.assign(Error('busy'),{code:'apps_sessions_busy'});
 await f.stage.launch();await tick();f.state.askIdentity();await tick();
 assert.equal(f.state.identityRequests.length,2);assert.equal(f.launcher.hostSession(),before);f.current(before.hostSession);
});

for(const phase of ['admission','late-save'])for(const denial of [{code:'app_source_changed'},{code:'ACTIVE_PROFILE_CHANGED'},
 {code:'apps_access_denied',status:403}])test(`real relay: explicit ${denial.code} at ${phase} cannot resume before a live invalidation arrives`,async t=>{
 const f=fixture(t,{realIdentity:true});await f.stage.launch();f.state.askIdentity();await tick();const resumes=f.state.identityResumes;
 const error=Object.assign(Error(denial.code),denial);
 if(phase==='admission')f.state.requestError=error;
 else{f.state.requiresCheck=true;f.state.afterIssue=async()=>{f.state.saveError=error;};}
 await f.stage.launch();await tick();assert.equal(f.state.identityResumes,resumes);f.state.askIdentity();await tick();
 assert.equal(f.state.identityRequests.length,1);
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

test('a refused cutover publishes no candidate verification or external entry while keeping the once-captured live entry',async t=>{
 const f=fixture(t);await f.stage.launch();const before=f.stage.entry(),frame=f.runtime.querySelector('iframe'),routes=f.state.routes.length,remembered=f.state.remembered;
 f.state.verified=[];f.state.externalReads=0;f.state.requiresCheck=true;
 f.state.afterIssue=async()=>{f.state.saveError=Error('soty_work_timeout');};await f.stage.launch();await tick();
 assert.equal(f.runtime.querySelector('iframe'),frame);assert.equal(f.stage.entry(),before);assert.equal(f.state.bookmarkEntry,before);
 assert.equal(f.state.routes.length,routes);assert.equal(f.state.remembered,remembered);assert.deepEqual(f.state.verified,[false]);assert.equal(f.state.externalReads,0);
});

test('a first candidate rejected after admission cannot pin stage entry, bookmark, discussion or history before rollback',async t=>{
 const f=fixture(t);f.state.afterIssue=async()=>{f.state.identityActive=true;};f.state.onIdentityClose=async()=>{throw Error('identity close unconfirmed');};
 await f.stage.launch();await tick();
 assert.equal(f.stage.entry(),null);assert.equal(f.launcher.entry(),null);assert.equal(f.state.bookmarkEntry,null);assert.equal(f.active(),0);
 assert.equal(f.state.routes.length,0);assert.equal(f.state.remembered,0);assert.equal(f.state.discussionEntries.length,0);assert.equal(f.state.externalReads,0);
});

test('disposing a pending same-frame candidate cannot close a promoted successor using its newer proof',async t=>{
 const f=fixture(t);await f.stage.launch();const before=f.launcher.hostSession(),old=f.state.issued[0],other=f.extra();
 const successor=f.store.prepare(old.claim);f.store.promote(successor.claim);
 f.state.fixedReply={url:runtimeOrigin+'/_soty/boot?path=%2Freader#'+'T'.repeat(43),entry,runtimeOrigin,launchBinding:pin,
   hostSession:successor.descriptor,hostSessionExpiresAt:f.state.at+120000};
 f.launcher.beginFrameCutover();await f.launcher.launch();assert.equal(f.launcher.hostSession(),before);
 f.state.closing=true;f.launcher.dispose();await tick();
 assert.equal(f.state.closed.length,1);assert.equal(f.state.closed[0].lease,before.hostSession.lease);assert.equal(f.state.closed[0].revision,before.hostSession.revision);
 assert.doesNotThrow(()=>f.current(successor.descriptor));assert.doesNotThrow(()=>f.current(other.descriptor));
});
