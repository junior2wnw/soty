import test,{before,after}from'node:test';
import assert from'node:assert/strict';
import{createRequire}from'node:module';
import{readFileSync}from'node:fs';
import{dirname}from'node:path';
import{createHash}from'node:crypto';
const require=createRequire(import.meta.url),{build}=require('esbuild');
// Browser test IO only; reuse an installed Playwright without adding production dependencies.
const{chromium}=await import(process.env.SOTY_PLAYWRIGHT_MODULE||'playwright');
let browser,bundle,css;
before(async()=>{
 const source=readFileSync(process.env.SOTY_AVATAR_TEST_SOURCE||new URL('./avatars.ts',import.meta.url),'utf8');
 console.log('Actual AvatarHydrator source SHA256 '+createHash('sha256').update(source).digest('hex'));
 const result=await build({entryPoints:['src/world/avatars.ts'],outfile:'avatar-test.js',bundle:true,write:false,format:'esm',platform:'browser',
  plugins:[{name:'actual-avatar-source-control',setup(build){build.onLoad({filter:/[/\\]avatars\.ts$/},args=>({contents:source,loader:'ts',resolveDir:dirname(args.path)}));}}]});
 bundle=Buffer.from(result.outputFiles.find(file=>file.path.endsWith('.js')).contents).toString('base64');
 css=result.outputFiles.find(file=>file.path.endsWith('.css'))?.text??'';
 browser=await chromium.launch({channel:'chrome',headless:true});
});
after(async()=>{await browser?.close();});
async function fixture(t,{held=[],context}={}){
 const page=await browser.newPage({viewport:{width:900,height:700}});t.after(()=>page.close());
 await page.setContent('<!doctype html><main id="root"></main>');
 if(css)await page.addStyleTag({content:css});
 await page.evaluate(async({bundle,held,context})=>{
  const{AvatarHydrator}=await import('data:text/javascript;base64,'+bundle),root=document.querySelector('#root');
  const canvas=document.createElement('canvas');canvas.width=16;canvas.height=16;canvas.getContext('2d').fillRect(0,0,16,16);
  const image=canvas.toDataURL('image/png'),calls=[],pending=[];
  const api={request(operation,args){calls.push({operation,args:structuredClone(args)});
   const result={avatars:args.communityId==='room_forbidden_B'?[]:args.profileIds.map(profileId=>({profileId,avatarRevision:1,avatarUrl:image}))};
   if(held.includes(args.communityId??'personal'))return new Promise(resolve=>pending.push({communityId:args.communityId,resolve:()=>resolve(result)}));
   return Promise.resolve(result);
  }};
  const make=(id,communityId,profileId='same_profile')=>{
   const holder=document.createElement('section');holder.id=id;if(communityId!==undefined)holder.dataset.avatarCommunity=communityId;
   const avatar=document.createElement('span');avatar.className='sw-avatar';avatar.dataset.profileId=profileId;avatar.dataset.avatarRevision='1';avatar.dataset.avatarLabel='С';
   avatar.style.cssText='display:inline-flex;width:24px;height:24px';avatar.textContent='С';holder.append(avatar);root.append(holder);return avatar;
  };
  const hydrator=new AvatarHydrator(api,root);if(context!==undefined)hydrator.setContext(context);
  window.avatarTest={root,hydrator,make,calls,pending};
 },{bundle,held,context});
 return page;
}
const count=(page,id)=>page.locator('#'+id+' img').count();
async function called(page,scope){await page.waitForFunction(scope=>window.avatarTest.calls.some(call=>call.args.communityId===scope),scope,{timeout:4000});}
async function settle(page){await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));}

test('same profile/revision in permitted and forbidden rooms batches by exact per-node scope, never global context',async t=>{
 const page=await fixture(t,{context:'room_global'});
 await page.evaluate(()=>{const f=window.avatarTest;f.make('allowed','room_allowed_A');f.make('forbidden','room_forbidden_B');});
 await called(page,'room_forbidden_B');await page.waitForFunction(()=>document.querySelector('#allowed img')?.naturalWidth===16);
 assert.equal(await count(page,'forbidden'),0);
 const calls=await page.evaluate(()=>window.avatarTest.calls);assert.deepEqual(calls.map(call=>call.args.communityId).sort(),['room_allowed_A','room_forbidden_B']);
 assert.ok(calls.every(call=>call.operation==='world.profile.avatars'&&JSON.stringify(call.args.profileIds)==='["same_profile"]'));
});

test('default personal hydration remains unscoped and duplicate people share one request',async t=>{
 const page=await fixture(t);await page.evaluate(()=>{const f=window.avatarTest;f.make('one');f.make('two');});
 await page.waitForFunction(()=>document.querySelectorAll('img').length===2);
 assert.equal(await page.evaluate(()=>window.avatarTest.calls.length),1);
 assert.equal(await page.evaluate(()=>Object.hasOwn(window.avatarTest.calls[0].args,'communityId')),false);
 assert.equal(await page.evaluate(()=>window.avatarTest.calls[0].args.profileIds.length),1);
});

test('positive avatar cache from one room cannot paint a later forbidden room clone',async t=>{
 const page=await fixture(t);await page.evaluate(()=>window.avatarTest.make('allowed','room_allowed_A'));
 await page.waitForFunction(()=>document.querySelector('#allowed img')?.naturalWidth===16);
 await page.evaluate(()=>window.avatarTest.make('allowed_clone','room_allowed_A'));
 await page.waitForFunction(()=>document.querySelector('#allowed_clone img')?.naturalWidth===16);
 assert.equal(await page.evaluate(()=>window.avatarTest.calls.filter(call=>call.args.communityId==='room_allowed_A').length),1,'the privacy fix must preserve useful same-scope caching');
 await page.evaluate(()=>window.avatarTest.make('forbidden','room_forbidden_B'));await called(page,'room_forbidden_B');await settle(page);
 assert.equal(await count(page,'forbidden'),0);assert.equal(await count(page,'allowed'),1);
});

test('a late A reply cannot repaint a newer A permission generation after A-B-A',async t=>{
 const page=await fixture(t,{held:['room_allowed_A'],context:'room_allowed_A'});await page.evaluate(()=>window.avatarTest.make('target'));
 await called(page,'room_allowed_A');await page.evaluate(()=>window.avatarTest.hydrator.setContext('room_forbidden_B'));await called(page,'room_forbidden_B');
 await page.evaluate(()=>window.avatarTest.hydrator.setContext('room_allowed_A'));
 await page.waitForFunction(()=>window.avatarTest.pending.filter(row=>row.communityId==='room_allowed_A').length===2);
 await page.evaluate(()=>window.avatarTest.pending[0].resolve());await settle(page);assert.equal(await count(page,'target'),0);
 await page.evaluate(()=>window.avatarTest.pending.find((row,n)=>n>0&&row.communityId==='room_allowed_A').resolve());
 await page.waitForFunction(()=>document.querySelector('#target img')?.naturalWidth===16);
});

test('batch limits and deduplication never mix distinct room authorities',async t=>{
 const page=await fixture(t);await page.evaluate(()=>{const f=window.avatarTest;for(let n=0;n<25;n++)f.make('a_'+n,'room_allowed_A','profile_'+n);
  f.make('b_0','room_forbidden_B','profile_0');f.make('b_1','room_forbidden_B','profile_1');});
 await called(page,'room_forbidden_B');await page.waitForFunction(()=>document.querySelectorAll('img').length===25);
 const calls=await page.evaluate(()=>window.avatarTest.calls);assert.deepEqual(calls.filter(row=>row.args.communityId==='room_allowed_A').map(row=>row.args.profileIds.length).sort((a,b)=>a-b),[1,24]);
 assert.deepEqual(calls.filter(row=>row.args.communityId==='room_forbidden_B').map(row=>row.args.profileIds.length),[2]);
 assert.ok(calls.every(row=>row.args.profileIds.length<=24));assert.equal(await count(page,'b_0'),0);assert.equal(await count(page,'b_1'),0);
});

test('a late reply cannot repaint after global permission context switch',async t=>{
 const page=await fixture(t,{held:['room_allowed_A'],context:'room_allowed_A'});
 await page.evaluate(()=>window.avatarTest.make('target'));await called(page,'room_allowed_A');
 await page.evaluate(()=>{const f=window.avatarTest;f.hydrator.setContext('room_forbidden_B');f.pending[0].resolve();});
 await called(page,'room_forbidden_B');await settle(page);assert.equal(await count(page,'target'),0);
});

test('delayed replies check each per-node room/profile/revision boundary independently',async t=>{
 for(const change of ['room','profile','revision'])await t.test(change,async child=>{
  const page=await fixture(child,{held:['room_allowed_A']});await page.evaluate(()=>window.avatarTest.make('target','room_allowed_A'));await called(page,'room_allowed_A');
  await page.evaluate(change=>{const f=window.avatarTest,holder=document.querySelector('#target'),avatar=holder.firstElementChild;
   if(change==='room')holder.dataset.avatarCommunity='room_forbidden_B';
   else if(change==='profile')avatar.dataset.profileId='another_profile';else avatar.dataset.avatarRevision='2';f.pending[0].resolve();},change);
  await settle(page);assert.equal(await count(page,'target'),0);
 });
});

test('destroy prevents a delayed permitted reply from painting a still-connected root',async t=>{
 const page=await fixture(t,{held:['room_allowed_A']});await page.evaluate(()=>window.avatarTest.make('target','room_allowed_A'));await called(page,'room_allowed_A');
 await page.evaluate(()=>{const f=window.avatarTest;f.hydrator.destroy();f.pending[0].resolve();});await settle(page);
 assert.equal(await count(page,'target'),0);assert.equal(await page.evaluate(()=>window.avatarTest.calls.length),1);
});

test('moving a painted avatar into a forbidden room scrubs the previous image before a delayed response',async t=>{
 const page=await fixture(t,{held:['room_forbidden_B']});await page.evaluate(()=>window.avatarTest.make('allowed','room_allowed_A'));
 await page.waitForFunction(()=>document.querySelector('#allowed img')?.naturalWidth===16);
 await page.evaluate(()=>{const f=window.avatarTest,holder=document.createElement('section');holder.id='forbidden';holder.dataset.avatarCommunity='room_forbidden_B';
  f.root.append(holder);holder.append(document.querySelector('#allowed .sw-avatar'));});
 await called(page,'room_forbidden_B');assert.equal(await count(page,'forbidden'),0,'previous permitted pixels must not remain during forbidden lookup');
});

test('changing an ancestor permission scope retires already painted pixels before a new response',async t=>{
 const page=await fixture(t,{held:['room_forbidden_B']});await page.evaluate(()=>window.avatarTest.make('target','room_allowed_A'));
 await page.waitForFunction(()=>document.querySelector('#target img')?.naturalWidth===16);
 await page.evaluate(()=>{document.querySelector('#target').dataset.avatarCommunity='room_forbidden_B';});
 await settle(page);assert.equal(await count(page,'target'),0,'old permitted pixels cannot remain after an authority-context mutation');
 await called(page,'room_forbidden_B');
});
