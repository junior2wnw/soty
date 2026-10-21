import test from 'node:test';
import assert from 'node:assert/strict';
import { createAppCoverPresenter } from './app-cover-presenter.mjs';
const tick=()=>new Promise(r=>setImmediate(r));
const read=(mode='custom',revision=1)=>({current:{mode,revision,version:mode==='custom'?'version_one':null},image:mode==='custom'?{mime:'image/webp',width:320,height:320,bytes:new Uint8Array(16)}:null});
function resources(){let serial=0;const disposed=[];return {disposed,makeImage(image){const url='blob:private-'+ ++serial;image.bytes.fill(0);return {url,width:320,height:320,dispose(){disposed.push(url);}};}};}
test('only visible targets read; three actual workers, bounded targets/bindings/queue and zero idle requests',async()=>{
  const held=[],calls=[],r=resources();let active=0,maximum=0;
  const p=createAppCoverPresenter({accountId:'owner',isCurrent:()=>true,maximumTargets:5,maximumBindings:10,makeImage:r.makeImage,readApp:(id,width)=>new Promise(resolve=>{calls.push({id,width});active++;maximum=Math.max(maximum,active);held.push(()=>{active--;resolve(read());});})});
  const bindings=[];for(let n=0;n<10;n++)bindings.push(p.watch({identity:'source-one',appId:'app'+n,isCurrent:()=>true,onVisual(){}}));assert.equal(calls.length,0);
  for(const b of bindings)b.setVisible(true);assert.equal(calls.length,3);assert.equal(p.stats().targets,5);assert.equal(p.stats().queued,2);assert.equal(p.watch({identity:'source-one',appId:'extra',isCurrent:()=>true,onVisual(){}}),null);
  for(let i=0;i<5;i++){const release=held.shift();assert.ok(release);release();await tick();}
  assert.equal(maximum,3);assert.equal(calls.every(v=>v.width===320),true);assert.equal(p.stats().targets,5);
  for(const b of bindings)b.dispose();assert.equal(p.stats().bytes,0);assert.equal(p.stats().targets,0);p.dispose();for(const release of held)release();await tick();assert.equal(p.stats().running,0);
});
test('commit invalidation does not need an app revision and stale in-flight reads cannot paint/repopulate',async()=>{
  const held=[],visual=[],r=resources();const p=createAppCoverPresenter({accountId:'owner',isCurrent:()=>true,makeImage:r.makeImage,readApp:()=>new Promise(resolve=>held.push(resolve))});
  const b=p.watch({identity:'source-one',appId:'same-app',isCurrent:()=>true,onVisual:v=>visual.push(v)});b.setVisible(true);assert.equal(held.length,1);p.invalidate('same-app');held.shift()(read('custom',1));await tick();
  assert.equal(visual.some(v=>v.mode==='custom'),false);assert.equal(held.length,1);held.shift()(read('custom',2));await tick();assert.equal(visual.at(-1).revision,2);
  const url=visual.at(-1).url;p.invalidate('same-app');assert.ok(r.disposed.includes(url));assert.equal(visual.at(-1).mode,'loading');held.shift()(read('none',3));await tick();assert.equal(visual.at(-1).mode,'none');b.dispose();p.dispose();
});
test('leaving visibility drops owned bytes; entering again reads current ACL; denial restores static fallback',async()=>{
  let allowed=true,calls=0;const visual=[],r=resources(),p=createAppCoverPresenter({accountId:'owner',isCurrent:()=>true,makeImage:r.makeImage,readApp:async()=>{calls++;if(!allowed)throw Error('denied');return read();}});
  const b=p.watch({identity:'source-one',appId:'app',isCurrent:()=>true,onVisual:v=>visual.push(v)});b.setVisible(true);await tick();assert.equal(visual.at(-1).mode,'custom');b.setVisible(false);assert.equal(p.stats().bytes,0);assert.equal(r.disposed.length,1);
  allowed=false;b.setVisible(true);await tick();assert.equal(calls,2);assert.equal(visual.at(-1).mode,'inherit');assert.equal(p.stats().bytes,0);b.dispose();p.dispose();
});
test('account ABA, app identity replacement and disposed binding never receive late private pictures',async()=>{
  let release,context=1,identity=1;const painted=[],r=resources(),p=createAppCoverPresenter({accountId:'owner',isCurrent:()=>context===1,makeImage:r.makeImage,readApp:()=>new Promise(resolve=>release=resolve)});
  const b=p.watch({identity:'source-one',appId:'app',isCurrent:()=>identity===1,onVisual:v=>painted.push(v)});b.setVisible(true);context=3;identity=3;const value=read();release(value);await tick();assert.equal(painted.some(v=>v.mode==='custom'),false);assert.equal(value.image.bytes.every(v=>v===0),true);assert.equal(p.stats().bytes,0);b.dispose();p.dispose();
});
test('rapid offscreen churn does not accumulate an unbounded retired queue or exceed worker cap',async()=>{
  const held=[],p=createAppCoverPresenter({accountId:'owner',isCurrent:()=>true,maximumTargets:4,maximumBindings:8,readApp:()=>new Promise(r=>held.push(r)),makeImage:resources().makeImage});
  for(let n=0;n<1000;n++){const b=p.watch({identity:'source-one',appId:'app'+n,isCurrent:()=>true,onVisual(){}});b.setVisible(true);b.dispose();assert.ok(p.stats().queued<=4);assert.ok(p.stats().running<=3);assert.ok(p.stats().bindings<=1);}
  assert.equal(held.length,3);p.dispose();for(const release of held)release(read());await tick();assert.equal(p.stats().running,0);assert.equal(p.stats().bytes,0);
});
test('a new actual app identity retires the old in-flight cache even while the account stays current',async()=>{
  const held=[],first=[],second=[],r=resources();let oldCurrent=true;
  const p=createAppCoverPresenter({accountId:'owner',isCurrent:()=>true,makeImage:r.makeImage,readApp:()=>new Promise(resolve=>held.push(resolve))});
  const old=p.watch({appId:'same-app',identity:'actual-target-revision-1',isCurrent:()=>oldCurrent,onVisual:v=>first.push(v)});old.setVisible(true);oldCurrent=false;
  const next=p.watch({appId:'same-app',identity:'actual-target-revision-2',isCurrent:()=>true,onVisual:v=>second.push(v)});next.setVisible(true);assert.equal(held.length,2);
  const stale=read('custom',1);held[0](stale);await tick();assert.equal(stale.image.bytes.every(v=>v===0),true);assert.equal(second.some(v=>v.mode==='custom'),false);
  held[1](read('custom',2));await tick();assert.equal(second.at(-1).revision,2);assert.equal(first.some(v=>v.mode==='custom'),false);old.dispose();next.dispose();p.dispose();
});
