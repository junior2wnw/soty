import test from 'node:test';
import assert from 'node:assert/strict';
import {createAppLauncher} from './app-launch.mjs';
import {createScopedSlotRenewal} from './app-scoped-renewal.mjs';
const source={id:'soty.standard-workspace',version:2,digest:'23061fc95610e3011ec42b3ecf229308bce44b0a1268408ca9b6ee138c98f7e9'};
const appId='app-'+'a'.repeat(32),entry={appId,domainId:'dom_'+'b'.repeat(32),origin:'https://app.example',path:'/embed'},frame='F'.repeat(43);
const reply=(renew=false)=>({url:`https://app.example/_soty/boot?path=%2Fembed${renew?'&mode=renew':''}#${'T'.repeat(43)}`,entry,
 runtimeProfile:'soty.selected-human-embed.v2',scopedCloseHandle:'H'.repeat(43),scopedSource:source,scopedFrameId:frame,
 scopedSlotExpiresAt:Date.now()+300000,...(renew?{scopedRenewalRequestId:'renew-request-000001'}:{})});
test('launcher keeps the same passive locator through current-slot commit; another frame/normal boot cannot replace it',async()=>{
 const l=createAppLauncher({target:{appId,domainId:entry.domainId,path:'/embed'},accountId:'account-a',shellUrl:'https://root.example/#mine',isCurrent:()=>true,request:async()=>reply()});
 await l.launch();const old=l.scopedCapture();assert.equal(old.frameId,frame);assert.equal(old.entry,l.entry());
 const renewed=l.prepareRenewal(reply(true));assert.equal(renewed.frameId,frame);assert.equal(l.commitRenewal(old,renewed),true);assert.equal(l.scopedCapture().frameId,frame);
 assert.throws(()=>l.prepareRenewal({...reply(true),scopedFrameId:'G'.repeat(43)}));
 assert.throws(()=>l.prepareRenewal(reply(false)),'literal fresh launch is not a renewal carrier');l.dispose();
});
test('lost RAM Source refresh forwards passive frameId with the same intent and waits for fresh server ACK',async()=>{
 let active={handle:'old',source,slot:{},frameId:frame,entry},requests=0,commits=0;
 const helper=createScopedSlotRenewal({readBinding:()=>active,isCurrent:()=>true,clock:()=>1000,
  readContext:async handle=>{if(handle==='old')throw Object.assign(Error('closed'),{code:'app_scoped_context_closed'});return{ready:true,scopedSource:source,expiresAt:301000,sourceSession:{ready:true,renewable:true,sessionExpiresAt:301000,accessExpiresAt:301000}};},
  request:async args=>{requests++;assert.equal(args.frameId,frame);assert.equal(args.currentEntryPath,entry.path);assert.equal(args.handle,'old');return{handle:'new',source,slot:{},frameId:frame,entry,requestId:args.requestId,url:'https://app.example/_soty/boot',expiresAt:301000};},
  bootstrap:async()=> 'ready',commit:(old,next)=>{assert.equal(old.slot,active.slot);active=next;commits++;return true;}});
 assert.equal(await helper.refresh(),true);assert.equal(requests,1);assert.equal(commits,1);assert.equal(active.frameId,frame);helper.dispose();
});
