import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import ts from 'typescript';
import {refreshAppStageLive} from './app-stage-live.mjs';

const source=await readFile(new URL('./app-stage.ts',import.meta.url),'utf8');
const ast=ts.createSourceFile('app-stage.ts',source,ts.ScriptTarget.Latest,true),printer=ts.createPrinter();
const mount=ast.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text==='mountAppStage');
const fn=name=>{const node=mount.body.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text===name);assert.ok(node,name);return printer.printNode(ts.EmitHint.Unspecified,node,ast);};
const entry={appId:'app-current',domainId:'dom-current',origin:'https://app.example',path:'/embed?mode=one#saved'};
const metadata=(name='New title')=>({schema:'soty.app-directory.v1',items:[{ref:{kind:'app',id:entry.appId},available:true,app:{id:entry.appId,name,state:'ready',ownerAccountId:'owner'}}]});

function fixture(overrides={}){
  const calls=[],frame={title:'Old title'},draft={text:'My own unsubmitted draft'},state={current:true,frame,draft,clears:0,discussions:0,widgetDisposals:0,widgetMarker:{widgetContext:'ready'}};
  const api={async request(op,args){calls.push({op,args});return overrides.request?overrides.request(op,args):op==='apps.entry.get'?{entry:{...entry}}:metadata();}};
  const compiled=ts.transpileModule(`return function(state,api){
    const accountId='reader',options={api},current=()=>state.current,initial={target:{appId:'app-current'}};
    let app={appId:'app-current',name:'Old title',status:'ready',ownerAccountId:'owner'},selectedEntry=${JSON.stringify(entry)};
    let liveReadSequence=0,liveAuthorityEpoch=0,liveAuthorityDenied=false,frameGeneration=0,runtimePending=false,entryRecoveryEpoch=0,entryRecoveryPending=false,displayEpoch=0,releaseRuntimeDisplay=null;
    let externalAvailability='available',relayPeer={frame:state.frame},hostRenewal=null;
    let monitoringPanel=null,monitoringOpening=false,monitoringOpenEpoch=0;
    let widgetContext={dispose(){state.widgetDisposals++;}};const screen={dataset:state.widgetMarker};
    const name={textContent:'Old title',title:'Old title'},subtitle={textContent:'Old device'},settings={},archives={},monitoringAction={};
    const external={},externalRecovery={},currentEntryRetry={},recoveryRetry={},refresh={};
    const runtime={children:[state.frame],querySelector(){return this.children.includes(state.frame)?state.frame:null;},setAttribute(){},replaceChildren(...children){this.children=children;state.clears++;}};
    const runtimeTrace={reset(){}},recovery={cancel(){}},editorHost={close(){}},stopAssist=()=>{},saved={setEntryVerified(){}},relayIdentity={async clear(){}};
    const discussion={draft:state.draft,async refresh(){state.discussions++;}};
    const showMessage=()=>{},showPanel=()=>{},emptyState=(title,detail)=>({title,detail}),appLaunchFailure=()=>({title:'This entry is unavailable',detail:'Access was withdrawn'});
    ${fn('resetRuntimeDisplay')}
    ${fn('updateApp')}
    ${fn('denyRuntime')}
    ${fn('invalidateLive')}
    return {invalidateLive,setEntry:value=>{selectedEntry=value;},snapshot:()=>({frame:runtime.querySelector('iframe'),name:name.textContent,entry:selectedEntry,draft:discussion.draft,denied:liveAuthorityDenied,clears:state.clears,epoch:liveAuthorityEpoch})};
  }`,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.None}}).outputText;
  const stage=new Function('refreshAppStageLive',compiled)(refreshAppStageLive)(state,api);
  return {stage,state,calls,frame,draft};
}

test('actual stage rename preserves the exact running frame, entry and own draft',async()=>{
  const f=fixture();await f.stage.invalidateLive();const result=f.stage.snapshot();
  assert.equal(result.frame,f.frame);assert.equal(result.frame.title,'New title');assert.equal(result.name,'New title');assert.equal(result.draft,f.draft);assert.equal(result.clears,0);assert.deepEqual(result.entry,entry);
  assert.deepEqual(f.calls.find(call=>call.op==='apps.entry.get').args,{expectedAccountId:'reader',appId:entry.appId,domainId:entry.domainId,path:entry.path});
  assert.equal(f.calls.some(call=>call.op==='apps.launch'),false);
  assert.equal(f.state.widgetDisposals,0,'Metadata changes retain the live presentation binding');
});

test('actual stage revocation removes the old frame, keeps local discussion draft and never rebinds',async()=>{
  const f=fixture({request(op){if(op==='apps.entry.get')throw Object.assign(new Error('denied'),{code:'apps_access_denied'});return metadata();}});
  await f.stage.invalidateLive();const result=f.stage.snapshot();assert.equal(result.frame,null);assert.equal(result.denied,true);assert.equal(result.epoch,1);assert.equal(result.draft,f.draft);assert.equal(f.state.discussions,1);assert.deepEqual(result.entry,entry);
  assert.equal(f.calls.some(call=>call.op==='apps.launch'),false);
  assert.equal(f.state.widgetDisposals,1,'Revocation disposes the presentation bridge as well as its frame');assert.equal(f.state.widgetMarker.widgetContext,undefined);
});

test('negative control: the previous metadata-only stage keeps a revoked frame and fails the same oracle',async()=>{
  const requireRemoved=value=>assert.equal(value.frame,null,'a proven revoked entry must not retain its iframe');
  const old=fixture();assert.throws(()=>requireRemoved(old.stage.snapshot()),/must not retain/);
  const fixed=fixture({request(op){if(op==='apps.entry.get')throw Object.assign(new Error('denied'),{code:'apps_access_denied'});return metadata();}});await fixed.stage.invalidateLive();requireRemoved(fixed.stage.snapshot());
});

test('network errors and malformed replacement entries preserve the current frame and remain retryable',async()=>{
  for(const mode of ['offline','replacement']){
    const f=fixture({request(op){if(op==='apps.entry.get'){if(mode==='offline')throw new TypeError('Network offline');return {entry:{...entry,domainId:'other-domain',origin:'https://other.example'}};}return metadata();}});
    await assert.rejects(f.stage.invalidateLive());assert.equal(f.stage.snapshot().frame,f.frame);assert.equal(f.stage.snapshot().denied,false);assert.deepEqual(f.stage.snapshot().entry,entry);
  }
});

test('unlisted discovery absence does not revoke an otherwise authorized exact entry',async()=>{
  const f=fixture({request:op=>op==='apps.entry.get'?{entry}:{schema:'soty.app-directory.v1',items:[{ref:{kind:'app',id:entry.appId},available:false}]}});
  await f.stage.invalidateLive();assert.equal(f.stage.snapshot().frame,f.frame);assert.equal(f.stage.snapshot().denied,false);
});

test('a response from a retired stage/account cannot repaint or remove a newer frame',async()=>{
  let finish;const f=fixture({request:op=>op==='apps.entry.get'?new Promise(resolve=>{finish=resolve;}):metadata()});const pending=f.stage.invalidateLive();
  f.state.current=false;finish({entry});await pending;assert.equal(f.stage.snapshot().name,'Old title');assert.equal(f.stage.snapshot().frame,f.frame);
});
