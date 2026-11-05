import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import {createRelayIdentityBridge} from './app-relay-identity-bridge.mjs';
import {refreshAppStageLive} from './app-stage-live.mjs';

const source=readFileSync(new URL('./app-stage.ts',import.meta.url),'utf8'),ast=ts.createSourceFile('stage.ts',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS);
const mount=ast.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text==='mountAppStage');assert.ok(mount);
const method=mount.body.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text==='invalidateLive');assert.ok(method);
const returned=mount.body.statements.find(node=>ts.isReturnStatement(node)).expression;
const renew=returned.properties.find(node=>node.name?.getText(ast)==='renewHostSession')?.initializer;assert.ok(renew);
const relayDeclaration=mount.body.statements.find(node=>ts.isVariableStatement(node)&&node.declarationList.declarations.some(value=>value.name.getText(ast)==='relayIdentity')).declarationList.declarations[0];
const relayOptions=relayDeclaration.initializer.arguments[0];
const relayPort=name=>{const value=relayOptions.properties.find(node=>node.name?.getText(ast)===name)?.initializer;assert.ok(value);return value.getText(ast);};
const compile=code=>ts.transpileModule(code,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
const turn=()=>new Promise(resolve=>setImmediate(resolve));
const schema='soty.relay-window-identity.v1';

// Actual Stage method/return arrow + actual bridge. Metadata/remote token RAM
// below are explicit IO models; no Root transport, Source rights or browser.
function fixture({bridgeFactory=createRelayIdentityBridge,stageSource=method.getText(ast),renewSource=renew.getText(ast),holdFirst=false}={}){
 let receive,active=true,sequence=0,release;const calls=[],closes=[],sent=[],liveTokens=new Set(),transport=[];
 const entry={appId:'app-'+ 'a'.repeat(32),domainId:'dom_'+ 'b'.repeat(32),origin:'https://reader.apps.example',path:'/'};
 const runtimeOrigin='https://app-m'+entry.appId.slice(4)+'-'+ 'c'.repeat(24)+'.apps.example';
 const frame={isConnected:true,contentWindow:{postMessage:(data,origin)=>sent.push({data,origin})}};
 const view={performance:{now:()=>0},setTimeout,clearTimeout,addEventListener:(_name,fn)=>{receive=fn;},removeEventListener:()=>{receive=null;}};
 const bridge=bridgeFactory({view,isCurrent:()=>active,getFrame:()=>frame,getEntry:()=>entry,getBinding:()=>({targetRevision:2}),getRuntimeScope:()=>({instanceId:'main',runtimeOrigin}),
  getHostSession:()=>({frameId:'F'.repeat(43),lease:'L'.repeat(43),revision:1}),
  async request(args){calls.push(args);const token=String.fromCharCode(65+sequence++).repeat(43);liveTokens.add(token);
   if(holdFirst&&sequence===1)await new Promise(resolve=>{release=resolve;});return{schema:'soty.relay-window-grant.v1',grant:token,expiresAt:Date.now()+30000};},
  async close(args){closes.push(args);return{closed:liveTokens.delete(args.grant)};}});
 const env={exports:{},refreshAppStageLive,relayIdentity:bridge,options:{api:{async request(op){if(op==='apps.entry.get')return{entry};assert.equal(op,'apps.directory.resolve');return{schema:'soty.app-directory.v1',items:[]};}}},
  selectedEntry:entry,liveReadSequence:0,accountId:'original-A',app:{appId:entry.appId},current:()=>active,updateApp:()=>{},denyRuntime:()=>assert.fail('allowed metadata cannot deny or replace this frame'),
  hostRenewal:{async renew(reason){transport.push(reason);return true;}}};vm.createContext(env);
 new vm.Script(compile(stageSource+'\nexports.invalidateLive=invalidateLive;exports.renew='+renewSource+';')).runInContext(env);
 return{bridge,frame,calls,closes,sent,transport,liveTokens,
  restart(){liveTokens.clear();},dispose(){active=false;release?.();bridge.dispose();},release(){release?.();},
  invalidate:options=>env.exports.invalidateLive(options),renew:reason=>env.exports.renew(reason),
  message(id='a'.repeat(36)){receive?.({source:frame.contentWindow,origin:runtimeOrigin,data:{schema,type:'request',requestId:id}});},
 };
}
async function resetOracle(f,reset){
 const frame=f.frame;f.message();await turn();assert.equal(f.sent.at(-1)?.data.grant,'A'.repeat(43));assert.equal(f.calls.length,1);
 f.restart();await reset();f.message('b'.repeat(36));await turn();
 assert.equal(f.calls.length,2,'boot reset must not reuse a grant from the old server RAM');assert.equal(f.sent.at(-1)?.data.grant,'B'.repeat(43));
 assert.equal(f.liveTokens.has(f.sent.at(-1).data.grant),true);assert.equal(f.frame,frame,'cache refresh is not iframe navigation/replacement');
}
test('actual main Stage boot-reset invalidation retires only Root grant cache and Source asks for a fresh proof in the same document',async()=>{
 const f=fixture();try{await resetOracle(f,()=>f.invalidate({reset:true}));assert.equal(f.transport.length,0);}finally{f.dispose();}
});
test('actual widget reset renewal clears the same bridge cache without requiring a Source SDK ACK or remount',async()=>{
 const f=fixture();try{await resetOracle(f,()=>f.renew('reset'));assert.deepEqual(f.transport,['reset']);}finally{f.dispose();}
});
test('normal metadata/near-expiry host renewal preserves current grant and original grant lifetime',async()=>{
 const f=fixture();try{f.message();await turn();await f.invalidate({reset:false});await f.renew('near-expiry');f.message('b'.repeat(36));await turn();
  assert.equal(f.calls.length,1);assert.equal(f.sent.at(-1).data.grant,'A'.repeat(43));assert.equal(f.closes.length,0);assert.deepEqual(f.transport,['near-expiry']);
 }finally{f.dispose();}
});
test('a grant issued before reset cannot enter the unchanged Source Window after the new boot generation',async()=>{
 const f=fixture({holdFirst:true});try{f.message();await turn();assert.equal(f.calls.length,1);await f.invalidate({reset:true});
  f.message('b'.repeat(36));await turn();f.release();await turn();await turn();assert.equal(f.calls.length,2);
  assert.equal(f.sent.some(item=>item.data.type==='grant'&&item.data.grant==='A'.repeat(43)),false,'late proof from old generation must not be recached/delivered');
  assert.equal(f.sent.some(item=>item.data.type==='grant'&&item.data.grant==='B'.repeat(43)),true);
 }finally{f.dispose();}
});
test('the old reset-discarding Stage call site fails the unchanged same-document fresh-proof oracle',async()=>{
 const text=method.getText(ast),anchor='if(invalidation?.reset)relayIdentity.reset();';assert.equal(text.split(anchor).length,2);
 const f=fixture({stageSource:text.replace(anchor,'/* broken: boot reset discarded */')});
 try{await assert.rejects(resetOracle(f,()=>f.invalidate({reset:true})),/boot reset must not reuse/u);}finally{f.dispose();}
});

function frameProofOracle(getFrameSource=relayPort('getFrame')){
 const old={isConnected:true},next={isConnected:true};let mounted=old,capture={hostSession:{frameId:'F'.repeat(43),lease:'L'.repeat(43),revision:1}};
 const env={relayPeer:{frame:old,hostFrameId:capture.hostSession.frameId},runtime:{querySelector:()=>mounted},launcher:{hostSession:()=>capture},exports:{}};vm.createContext(env);
 new vm.Script(compile('exports.frame='+getFrameSource+';exports.proof='+relayPort('getHostSession')+';')).runInContext(env);
 assert.equal(env.exports.frame(),old);assert.equal(env.exports.proof().lease,'L'.repeat(43));
 capture={hostSession:{frameId:'N'.repeat(43),lease:'Q'.repeat(43),revision:1}};
 assert.equal(env.exports.frame(),null,'new launcher member before append cannot authorize the previous visible frame');assert.equal(env.exports.proof(),null);
 mounted=next;env.relayPeer={frame:next,hostFrameId:capture.hostSession.frameId};assert.equal(env.exports.frame(),next);assert.equal(env.exports.proof().lease,'Q'.repeat(43));
 capture={hostSession:{...capture.hostSession,lease:'R'.repeat(43),revision:2}};assert.equal(env.exports.frame(),next);assert.equal(env.exports.proof().lease,'R'.repeat(43));assert.equal(env.exports.proof().revision,2);
}
test('actual Stage frame/proof ports suppress a new-member-before-append race and read current same-member CAS proof',()=>frameProofOracle());
test('the old frame-only port fails the unchanged physical-member coherence oracle',()=>{
 assert.throws(()=>frameProofOracle("()=>runtime.querySelector('iframe')===relayPeer?.frame?relayPeer.frame:null"),/new launcher member before append/u);
});
