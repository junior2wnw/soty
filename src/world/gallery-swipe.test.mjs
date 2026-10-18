import test from 'node:test';import assert from 'node:assert/strict';import {bindGallerySwipe} from './gallery-swipe.mjs';
function setup(options={},configure=()=>{}){const stage=new EventTarget(),capture=new Set(),moves=[];let active=true;configure(stage);
 stage.hasPointerCapture=id=>capture.has(id);stage.setPointerCapture=id=>capture.add(id);stage.releasePointerCapture=id=>{capture.delete(id);emit('lostpointercapture',{pointerId:id});};
 function emit(type,values={}){const e=new Event(type,{cancelable:true});for(const[k,v]of Object.entries({pointerId:1,pointerType:'touch',isPrimary:true,button:0,clientX:100,clientY:100,...values}))Object.defineProperty(e,k,{value:v});stage.dispatchEvent(e);return e;}
 const dispose=bindGallerySwipe(stage,{current:()=>active,previous:()=>moves.push('previous'),next:()=>moves.push('next'),...options});return{stage,moves,capture,emit,dispose,stale(){active=false;}};}
test('single primary touch/pen ≥48px commits once on up; buttons/keyboard need no gesture',()=>{
 const f=setup();f.emit('pointerdown');f.emit('pointermove',{clientX:52});assert.equal(f.moves.length,0);f.emit('pointerup',{clientX:52});assert.deepEqual(f.moves,['next']);assert.equal(f.capture.size,0);
 f.emit('pointerdown',{pointerType:'pen'});f.emit('pointerup',{pointerType:'pen',clientX:148});assert.deepEqual(f.moves,['next','previous']);f.emit('pointerup',{clientX:300});assert.equal(f.moves.length,2);f.dispose();
});
test('vertical intent, diagonal/short movement, mouse and nonprimary cannot advance or prevent native scroll',()=>{
 for(const events of [ [['pointerdown',{}],['pointermove',{clientY:125}],['pointerup',{clientX:20,clientY:125}]], [['pointerdown',{}],['pointerup',{clientX:53}]], [['pointerdown',{}],['pointerup',{clientX:40,clientY:150}]], [['pointerdown',{pointerType:'mouse'}],['pointerup',{pointerType:'mouse',clientX:0}]], [['pointerdown',{isPrimary:false}],['pointerup',{isPrimary:false,clientX:0}]] ]){
   const f=setup();for(const[type,args]of events){const e=f.emit(type,args);assert.equal(e.defaultPrevented,false);}assert.equal(f.moves.length,0);f.dispose();
 }
});
test('multitouch/pinch/pen+touch cancel until all contacts end; subsequent primary gesture works',()=>{
 const f=setup();f.emit('pointerdown');f.emit('pointerdown',{pointerId:2,isPrimary:false});f.emit('pointerdown',{pointerId:3,pointerType:'pen'});f.emit('pointerup',{pointerId:3,pointerType:'pen',clientX:0});f.emit('pointerup',{clientX:0});f.emit('pointerup',{pointerId:2,isPrimary:false,clientX:0});assert.equal(f.moves.length,0);
 f.emit('pointerdown',{pointerId:4});f.emit('pointerup',{pointerId:4,clientX:40});assert.deepEqual(f.moves,['next']);f.dispose();
});
test('pointercancel, lost capture, entry change and close dispose before up, with no outside handler',()=>{
 for(const boundary of ['pointercancel','lostpointercapture','stale','close']){
  const f=setup();f.emit('pointerdown');if(boundary==='stale')f.stale();else if(boundary==='close')f.dispose();else f.emit(boundary);f.emit('pointerup',{clientX:0});assert.equal(f.moves.length,0);f.dispose();assert.equal(f.capture.size,0);
  f.emit('pointerdown');f.emit('pointerup',{clientX:0});assert.equal(f.moves.length,0);
 }
});
test('selection epoch changes and explicit retry cancellation disarm the old contact',()=>{
 let selected=1;const f=setup({selectionEpoch:()=>selected});
 f.emit('pointerdown');selected=2;
 assert.equal(f.emit('pointermove',{clientX:0}).defaultPrevented,false);
 f.emit('pointerup',{clientX:0});assert.deepEqual(f.moves,[]);assert.equal(f.capture.size,0);
 f.emit('pointerdown');f.dispose.cancel();f.emit('pointerup',{clientX:0});assert.deepEqual(f.moves,[]);
 f.emit('pointerdown');f.emit('pointerup',{clientX:0});assert.deepEqual(f.moves,['next']);f.dispose();
});
test('default actual viewport blocks initial zoom, move zoom and up-only zoom without consuming input',()=>{
 for(const boundary of ['initial','move','up']){
  let scale=boundary==='initial'?2:1,reads=0;const viewport={get scale(){reads++;return scale;}};
  const f=setup({},stage=>{stage.ownerDocument={defaultView:{visualViewport:viewport}};});
  f.emit('pointerdown');scale=2;
  if(boundary!=='up')assert.equal(f.emit('pointermove',{clientX:0}).defaultPrevented,false);
  assert.equal(f.emit('pointerup',{clientX:0}).defaultPrevented,false);
  assert.deepEqual(f.moves,[]);assert.equal(f.capture.size,0);assert.ok(reads>=3);
  scale=1;f.emit('pointerdown');f.emit('pointerup',{clientX:0});assert.deepEqual(f.moves,['next']);f.dispose();
 }
});
test('zoom resize cancels capture, permits native two-axis pan and restores only the owned inline style/listener',()=>{
 const viewport=new EventTarget();viewport.scale=1;let value='pan-y',priority='important',adds=0,removes=0;
 const add=viewport.addEventListener.bind(viewport),remove=viewport.removeEventListener.bind(viewport);
 viewport.addEventListener=(...args)=>{adds++;return add(...args);};viewport.removeEventListener=(...args)=>{removes++;return remove(...args);};
 const f=setup({viewport},stage=>{stage.style={getPropertyValue:()=>value,getPropertyPriority:()=>priority,setProperty:(key,v,p='')=>{assert.equal(key,'touch-action');value=v;priority=p;},removeProperty:key=>{assert.equal(key,'touch-action');value='';priority='';}};});
 f.emit('pointerdown');assert.equal(f.capture.size,1);viewport.scale=2;viewport.dispatchEvent(new Event('resize'));
 assert.equal(f.capture.size,0);assert.equal(value,'pan-x pan-y pinch-zoom');assert.equal(priority,'');
 assert.equal(f.emit('pointerup',{clientX:0}).defaultPrevented,false);assert.deepEqual(f.moves,[]);
 viewport.scale=1;viewport.dispatchEvent(new Event('resize'));assert.equal(value,'pan-y');assert.equal(priority,'important');
 f.emit('pointerdown');f.emit('pointerup',{clientX:0});assert.deepEqual(f.moves,['next']);
 viewport.scale=2;viewport.dispatchEvent(new Event('resize'));f.dispose();assert.equal(value,'pan-y');assert.equal(priority,'important');
 assert.equal(adds,1);assert.ok(removes>=1);viewport.scale=3;viewport.dispatchEvent(new Event('resize'));assert.equal(value,'pan-y');
});
test('invalid scale cannot coerce into custom swipe permission; absent viewport keeps button-independent swipe',()=>{
 for(const scale of [NaN,0,'1',Infinity,1.02]){
  const f=setup({viewport:{scale}});f.emit('pointerdown');assert.equal(f.emit('pointermove',{clientX:0}).defaultPrevented,false);assert.equal(f.emit('pointerup',{clientX:0}).defaultPrevented,false);assert.deepEqual(f.moves,[]);f.dispose();
 }
 for(const viewport of [null,{scale:1.005}]){const f=setup({viewport});f.emit('pointerdown');f.emit('pointerup',{clientX:0});assert.deepEqual(f.moves,['next']);f.dispose();}
});
