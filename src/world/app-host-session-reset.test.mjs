import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import {isLiveIdentityLoss} from '../platform/platform-live-client.mjs';
import {isScopedRuntimeProfile} from './app-launch.mjs';
import {createRetainedAppRuntimes} from './app-retained-runtime.mjs';

const source=readFileSync(new URL('./app.ts',import.meta.url),'utf8');
const ast=ts.createSourceFile('app.ts',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS);
const appClass=ast.statements.find(node=>ts.isClassDeclaration(node)&&node.name?.text==='WorldApplication');
const methods=['appRuntimeStages','invalidateLive','appRuntime'].map(name=>{const method=appClass.members.find(node=>node.name?.getText(ast)===name);assert.ok(method);return method.getText(ast);}).join('\n');
const mount=ast.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text==='mountWorldApp');
const returned=mount.body.statements.find(node=>ts.isReturnStatement(node)).expression;
const forwarding=returned.properties.find(node=>node.name?.getText(ast)==='invalidateLive').initializer.getText(ast);
const adapterSource=readFileSync(new URL('../platform/world-adapter.ts',import.meta.url),'utf8');
const adapter=ts.createSourceFile('world-adapter.ts',adapterSource,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS);
let invalidation;
function visit(node){if(ts.isCallExpression(node)&&node.expression.getText(adapter)==='createPlatformLiveClient'){
 const object=node.arguments[0];invalidation=object.properties.find(value=>value.name?.getText(adapter)==='onInvalidate')?.initializer;
}ts.forEachChild(node,visit);}visit(adapter);assert.ok(invalidation);
const compile=code=>ts.transpileModule(code,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;

// Actual adapter callback -> actual public forwarding arrow -> actual methods.
// Only IO/scheduling ports are synthetic; no grant or Root-browser claim.
function fixture({callback=invalidation.getText(adapter),hostError,identityError}={}){
 const calls=[],lost=[],env={exports:{},isLiveIdentityLoss,isScopedRuntimeProfile};vm.createContext(env);
 new vm.Script(compile('class Controller{'+methods+'}\nexports.Controller=Controller;')).runInContext(env);
 const app=new env.exports.Controller(),field={async refresh(value){calls.push(['field-refresh',value]);},async renewHostSessions(value){calls.push(['field-renew',value]);}};
 const stage={async invalidateLive(value){calls.push(['stage-refresh',value]);},async renewHostSession(value){calls.push(['stage-renew',value]);if(hostError)throw hostError;return true;}};
 Object.assign(app,{destroyed:false,deskAccount:'account-A',profile:{revision:1},unifiedField:field,appStage:stage,
  accountTask:()=>()=>true,fieldCoverPresenter:{invalidateAll(){},refreshVisible(){}},avatars:{setContext(){}},
  api:{async request(op){assert.equal(op,'world.profile.get');calls.push(['profile']);if(identityError)throw identityError;return{profile:{revision:2}};}},
  renderNavigation(){},async refreshSocialInbox(){calls.push(['inbox']);},loseAccess:error=>lost.push(error),options:{}});
 app.retainedMainApps=createRetainedAppRuntimes();
 app.retainedMainApps.add({appId:'held-app',stage,host:{},closeEpoch:0});
 env.app=app;new vm.Script(compile('forward='+forwarding+';')).runInContext(env);
 env.mounted={invalidateLive:env.forward,refresh(){assert.fail('a reset must not reload the mounted world');}};
 new vm.Script(compile('invalidate='+callback+';')).runInContext(env);
 return{app,field,stage,calls,lost,emit:event=>env.invalidate(event)};
}
async function assertReset(f){await f.emit({reset:true});assert.deepEqual(f.calls.filter(value=>value[0].endsWith('-renew')),[['stage-renew','reset'],['field-renew','reset']]);assert.equal(f.calls.find(value=>value[0]==='stage-refresh')?.[1]?.reset,true);assert.equal(f.app.unifiedField,f.field);assert.equal(f.app.appStage,f.stage);}

test('a real boot reset reaches existing main/field host owners without remounting either surface',()=>assertReset(fixture()));
test('ordinary metadata invalidation preserves old refresh behavior without renewing all sessions',async()=>{
 const f=fixture();await f.emit({reset:false});assert.equal(f.calls.some(value=>value[0].endsWith('-renew')),false);assert.ok(f.calls.some(value=>value[0]==='field-refresh'));await f.app.invalidateLive();
});
test('an optional app transport failure does not pin the global live cursor at reset forever',async()=>{
 const f=fixture({hostError:Object.assign(Error('offline'),{code:'app_offline',status:503})});await f.emit({reset:true});assert.equal(f.lost.length,0);assert.ok(f.calls.some(value=>value[0]==='field-refresh'));
});
test('actual identity loss during reset still invalidates the whole original-account view',async()=>{
 const reason=Object.assign(Error('device revoked'),{code:'device_revoked'}),f=fixture({hostError:reason});await assert.rejects(f.emit({reset:true}),{code:'device_revoked'});assert.deepEqual(f.lost,[reason]);
});
test('the old discarded-reset callback fails the unchanged mounted-owner oracle',async()=>{
 const original=invalidation.getText(adapter),anchor='mounted.invalidateLive?.(event)';assert.equal(original.split(anchor).length,2);
 await assert.rejects(assertReset(fixture({callback:original.replace(anchor,'mounted.invalidateLive?.()')})),assert.AssertionError);
});

function runtimeFixture(reply,{custom,cleanup}={}){
 const f=fixture(),calls=[];f.app.api={async request(op,args){calls.push({op,args});return reply;}};f.app.options={...(custom?{openApp:custom}:{}),...(cleanup?{appSlotCleanup:cleanup}:{})};
 return{calls,runtime:f.app.appRuntime({appId:'app-'+ 'a'.repeat(32),name:'Own app'})};
}
const launch={launchUrl:'https://reader.apps.example/_soty/boot?path=%2F#'+'T'.repeat(43),entry:{appId:'app-'+ 'a'.repeat(32),domainId:'dom_'+ 'b'.repeat(32),origin:'https://reader.apps.example',path:'/'}};
const parameters={appId:launch.entry.appId,expectedAccountId:'own-user',instanceId:'main',hostSession:true};

test('the actual normal signed Root adapter refuses missing ordinary host metadata after its explicit opt-in',async()=>{
 for(const value of [launch,{...launch,hostSession:{schema:'soty.app-host-session.v1'}},{...launch,hostSessionExpiresAt:Date.now()+120000}]){
  const f=runtimeFixture(value);await assert.rejects(f.runtime.request(parameters),{code:'app_host_session_unavailable'});assert.equal(f.calls[0].op,'apps.launch');assert.equal(f.calls[0].args.hostSession,true);
 }
});
test('actual normal Root forwarding preserves complete host metadata without leaking it into a new selector',async()=>{
 const member={schema:'soty.app-host-session.v1',frameId:'F'.repeat(43),lease:'A'.repeat(43),revision:1,expiresAt:Date.now()+86400000};
 const f=runtimeFixture({...launch,hostSession:member,hostSessionExpiresAt:Date.now()+120000}),value=await f.runtime.request(parameters);
 assert.equal(value.hostSession,member);assert.equal(value.url,launch.launchUrl);assert.equal(Object.hasOwn(f.calls[0].args,'lease'),false);
});
test('custom legacy openers and old external admission keep their explicit compatibility boundary',async()=>{
 const legacy={url:launch.launchUrl,entry:launch.entry},custom=runtimeFixture(null,{custom:async()=>legacy});assert.equal(await custom.runtime.request(parameters),legacy);assert.equal(custom.calls.length,0);
 const f=runtimeFixture(launch),external={...parameters};delete external.hostSession;assert.equal((await f.runtime.request(external)).url,launch.launchUrl);
});
test('declared Source profiles retain their distinct existing private continuation rather than fake host metadata',async()=>{
 const f=runtimeFixture({...launch,runtimeProfile:'soty.selected-human-embed.v2',scopedCloseHandle:'S'.repeat(43)}),value=await f.runtime.request(parameters);
 assert.equal(value.runtimeProfile,'soty.selected-human-embed.v2');assert.equal(Object.hasOwn(value,'hostSession'),false);
});

test('the actual normal Root adapter forwards only the existing private raw-reply cleanup capability',async()=>{
 const close=async()=>{},member={schema:'soty.app-host-session.v1',frameId:'F'.repeat(43),lease:'A'.repeat(43),revision:1,expiresAt:Date.now()+86400000};
 const raw={...launch,hostSession:member,hostSessionExpiresAt:Date.now()+120000},lookup=value=>value===raw?close:null;
 const f=runtimeFixture(raw,{cleanup:lookup}),reply=await f.runtime.request(parameters);
 assert.equal(reply.hostSessionCleanup,close);assert.equal(Object.hasOwn(reply,'scopedCleanup'),false);assert.equal(f.runtime.cleanupForReply,lookup);assert.equal(lookup(structuredClone(raw)),null);
 const source=runtimeFixture({...launch,runtimeProfile:'soty.selected-human-embed.v2'},{cleanup:()=>close});
 const selected=await source.runtime.request(parameters);assert.equal(selected.scopedCleanup,close);assert.equal(Object.hasOwn(selected,'hostSessionCleanup'),false);
});
