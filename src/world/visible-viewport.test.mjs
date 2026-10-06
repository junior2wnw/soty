import test from 'node:test';
import assert from 'node:assert/strict';
import { resolveVisibleViewport, observeVisibleViewport } from './visible-viewport.ts';

test('keyboard shrinks the visible frame, including a panned mobile viewport, without mistaking the address bar for a keyboard', () => {
  const value = { layoutHeight: 659, height: 350, top: 160, scale: 1, editable: true };
  assert.deepEqual(resolveVisibleViewport(value), { height:350, top:160, keyboard:true, short:true });
  assert.equal(resolveVisibleViewport({ ...value, height:615, top:0 }).keyboard, false);
  assert.equal(resolveVisibleViewport({ ...value, editable:false }).keyboard, false);
  assert.deepEqual(resolveVisibleViewport({ ...value, top:500 }), { height:350, top:309, keyboard:true, short:true });
});
test('native pinch zoom and invalid geometry are never forced into keyboard layout', () => {
  const value = { layoutHeight:659, height:350, top:0, scale:1, editable:true };
  for (const scale of [0,.5,1.5,2,NaN]) assert.equal(resolveVisibleViewport({ ...value, scale }),null);
  for (const height of [0,-1,NaN,Infinity]) assert.equal(resolveVisibleViewport({ ...value, height }),null);
  assert.deepEqual(resolveVisibleViewport({ ...value, height:900, top:-10 }), { height:659, top:0, keyboard:false, short:false });
});
function fixture() {
  const values=new Map(),frames=new Map();let frameId=0;
  const doc=new EventTarget(), view=new EventTarget(), viewport=new EventTarget();
  doc.documentElement={style:{setProperty:(k,v)=>values.set(k,v),removeProperty:k=>values.delete(k)},dataset:{}};
  doc.activeElement={matches:()=>true};doc.defaultView=view;
  view.innerHeight=659;view.visualViewport=viewport;
  Object.assign(viewport,{height:350,offsetTop:0,scale:1});
  view.requestAnimationFrame=fn=>{frames.set(++frameId,fn);return frameId;};view.cancelAnimationFrame=id=>frames.delete(id);
  const flush=()=>{const batch=[...frames.values()];frames.clear();batch.forEach(fn=>fn());};
  return {doc,view,viewport,values,frames,flush};
}
test('multiple mounts share one observer, events coalesce, and only the last disposal removes viewport state', () => {
  const f=fixture(), first=observeVisibleViewport(f.doc), second=observeVisibleViewport(f.doc);
  assert.equal(f.values.get('--soty-visible-height'),'350px');assert.equal(f.doc.documentElement.dataset.sotyKeyboard,'true');
  f.viewport.height=380;for(let i=0;i<50;i++)f.viewport.dispatchEvent(new Event('resize'));
  assert.equal(f.frames.size,1);f.flush();assert.equal(f.values.get('--soty-visible-height'),'380px');
  first();first();assert.equal(f.values.size,2);
  second();assert.equal(f.values.size,0);assert.deepEqual(f.doc.documentElement.dataset,{});
  f.viewport.dispatchEvent(new Event('resize'));assert.equal(f.frames.size,0);
});
test('pinching clears adaptive styles, blur clears keyboard state, and disposal cancels pending work', () => {
  const f=fixture(), stop=observeVisibleViewport(f.doc);
  f.doc.activeElement={matches:()=>false};f.doc.dispatchEvent(new Event('focusout'));f.flush();assert.equal(f.doc.documentElement.dataset.sotyKeyboard,undefined);
  f.viewport.scale=2;f.viewport.dispatchEvent(new Event('resize'));f.flush();assert.equal(f.values.size,0);
  f.viewport.scale=1;f.viewport.dispatchEvent(new Event('resize'));assert.equal(f.frames.size,1);stop();assert.equal(f.frames.size,0);
});
