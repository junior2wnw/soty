import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';
import {isLiveIdentityLoss} from '../platform/platform-live-client.mjs';
import {createRetainedAppRuntimes} from './app-retained-runtime.mjs';

// Execute the actual application controller methods; rendering and the Source
// network are explicit ports. Native DOM scrubbing is tested by its own host.
const source=await readFile(new URL('./app.ts',import.meta.url),'utf8');
const ast=ts.createSourceFile('app.ts',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS);
const controller=ast.statements.find(node=>ts.isClassDeclaration(node)&&node.name?.text==='WorldApplication');
const printer=ts.createPrinter();
const methods=['appRuntimeStages','invalidateLive','loseAccess'].map(name=>printer.printNode(ts.EmitHint.Unspecified,controller.members.find(node=>node.name?.getText(ast)===name),ast)).join('\n');
function fixture({broken=false}={}){
  let resolveProfile,rejectProfile,fieldReads=0,stageReads=0,inboxReads=0,secret=true,active=true,recoveries=0;
  const profile=new Promise((resolve,reject)=>{resolveProfile=resolve;rejectProfile=reject;});
  const exports={};
  const script=broken?methods.replace('field.refresh({ preserveView: true })','Promise.resolve()'):methods;
  if(broken)assert.notEqual(script,methods,'negative control must actually remove the authority refresh');
  const compiled=ts.transpileModule(`class Controller {${script}};exports.Controller=Controller;`,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
  vm.runInNewContext(compiled,{exports,isLiveIdentityLoss,emptyState:(title,detail,action)=>({title,detail,action,append(){}}),errorText:error=>error?.code||'',button:(label,_icon,_class,run)=>({label,run})});
  const app=new exports.Controller();Object.assign(app,{
    destroyed:false,deskAccount:'owner',requestSequence:0,profile:{revision:1},group:null,
    accountTask:()=>()=>active,api:{request:()=>profile},
    avatars:{setContext(){}},renderNavigation(){},fieldCoverPresenter:{invalidateAll(){},refreshVisible(){}},
    unifiedField:{async refresh(){fieldReads++;secret=false;}},appStage:{async invalidateLive(){stageReads++;}},
    async refreshSocialInbox(){inboxReads++;},
    transitionAccount(value){active=false;app.deskAccount=value;secret=false;app.unifiedField=null;app.appStage=null;return true;},
    cleanScreen(){secret=false;},main:{replaceChildren(value){app.display=value;}},
    options:{openAccount(){}},runHook:action=>action(),async refresh(){recoveries++;},
  });
  app.retainedMainApps=createRetainedAppRuntimes();
  app.retainedMainApps.add({appId:'held-app',stage:app.appStage,host:{},closeEpoch:0});
  return{app,resolveProfile,rejectProfile,reads:()=>({fieldReads,stageReads,inboxReads,secret,active,recoveries})};
}

async function profileFailureOracle(f){
  const updating=f.app.invalidateLive();
  assert.equal(f.reads().fieldReads,1,'object ACL refresh starts even while the unrelated profile response is unresolved');
  assert.equal(f.reads().stageReads,1);
  f.rejectProfile(Object.assign(new Error('offline'),{code:'network_unavailable'}));
  await assert.rejects(updating,{code:'network_unavailable'});
  assert.equal(f.reads().secret,false,'the revoked object was cleared despite profile failure');
  assert.equal(f.reads().active,true,'a transient profile error does not destroy account drafts');
}
test('profile failure cannot postpone or suppress the field and iframe authority refresh',async()=>{await profileFailureOracle(fixture());});

test('a live update refreshes the inbox while the personal canvas remains open',async()=>{
  const f=fixture(),updating=f.app.invalidateLive();
  try{assert.equal(f.reads().inboxReads,1,'incoming messages must refresh independently of opening the Messages screen');}
  finally{f.resolveProfile({profile:{revision:2}});await updating;}
});

test('the real retained pool refreshes a hidden owner as well as the active stage before profile completion',async()=>{
  const f=fixture();let hiddenReads=0;
  f.app.retainedMainApps.add({appId:'hidden-app',stage:{async invalidateLive(){hiddenReads++;}},host:{},closeEpoch:0});
  const pending=f.app.invalidateLive();assert.equal(hiddenReads,1);assert.equal(f.reads().stageReads,1);assert.equal(f.reads().fieldReads,1);
  f.resolveProfile({profile:{revision:2}});await pending;assert.equal(f.app.retainedMainApps.all().length,2);
});
test('negative control: suppressing the field refresh fails the same revoked-content oracle',async()=>{
  const f=fixture({broken:true});await assert.rejects(profileFailureOracle(f),/object ACL refresh starts/);
  f.resolveProfile({profile:{revision:2}});
});
test('device revocation retires displayed account state synchronously before any recovery request',()=>{
  const f=fixture();f.app.loseAccess({code:'device_revoked'});
  assert.equal(f.reads().active,false);assert.equal(f.reads().secret,false);assert.equal(f.app.deskAccount,null);
  assert.equal(f.app.unifiedField,null);assert.equal(f.app.appStage,null);assert.equal(f.reads().recoveries,0);
  assert.equal(f.app.display.title,'Доступ к аккаунту изменился');
});
test('fatal identity errors close the account; an app-specific revoke does not impersonate account loss',()=>{
  for(const code of ['device_revoked','authentication_required','live_authentication_required','ACTIVE_PROFILE_CHANGED'])assert.equal(isLiveIdentityLoss({code}),true);
  for(const code of ['app_access_revoked','apps_access_denied','invitation_revoked','network_unavailable'])assert.equal(isLiveIdentityLoss({code}),false);
});
