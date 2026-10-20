import test from 'node:test';
import assert from 'node:assert/strict';
import * as state from './unified-field-state.mjs';
import {layoutUnifiedField,fieldPolygonsOverlap,fieldSlotToPoint} from './unified-field-layout.mjs';

const document=()=>({schema:'soty.field.v1',contexts:[{contextId:'personal',title:'Личное',x:0,y:0}],shortcuts:[]});
const metadata=value=>value.shortcuts.map(shortcut=>({entity:shortcut.entity,title:shortcut.entity.id}));
const kinds=['app','person','device','builtin','community','app','person'];
const noOverlap=value=>{
  const nodes=layoutUnifiedField(value,metadata(value)).nodes;
  for(let a=0;a<nodes.length;a++)for(let b=a+1;b<nodes.length;b++)assert.equal(fieldPolygonsOverlap(nodes[a].footprint,nodes[b].footprint,10),false);
};
const legacy=()=>({schema:'soty.field.v1',contexts:[{contextId:'personal',title:'Личное',x:41,y:-19},{contextId:'other',title:'Другое',x:1200,y:200}],shortcuts:[
  {shortcutId:'person',entity:{kind:'person',id:'synthetic-person'},contextId:'personal',slot:[-24,-3]},
  {shortcutId:'foreign',entity:{kind:'app',id:'synthetic-foreign'},contextId:'other',slot:[7,-4]},
  {shortcutId:'app',entity:{kind:'app',id:'synthetic-app'},contextId:'personal',slot:[0,0]},
  {shortcutId:'device',entity:{kind:'device',id:'synthetic-device'},contextId:'personal',slot:[-17,34]},
]});

test('every kind uses the same regular hex footprint while saved centre coordinates and document stay unchanged',()=>{
  const value=legacy(),before=structuredClone(value),nodes=layoutUnifiedField(value,metadata(value)).nodes;
  assert.deepEqual(value,before);assert.equal(nodes.length,value.shortcuts.length);
  for(const node of nodes){
    assert.equal(node.footprint.length,6);assert.equal(node.width,nodes[0].width);assert.equal(node.height,nodes[0].height);
    const shortcut=value.shortcuts.find(item=>item.shortcutId===node.shortcutId),context=value.contexts.find(item=>item.contextId===shortcut.contextId),point=fieldSlotToPoint(shortcut.slot);
    assert.equal(node.cx,context.x+point.x);assert.equal(node.cy,context.y+point.y);
  }
});
test('new mixed references form one stable seven-cell flower with common collision footprints',()=>{
  let value=document();
  for(const [index,kind]of kinds.entries())value=state.applyFieldCommand(value,{type:'add-shortcut',shortcutId:'s'+index,entity:{kind,id:'synthetic-'+index},contextId:'personal'}).document;
  assert.deepEqual(value.shortcuts.map(item=>item.slot),[[0,0],[0,-21],[21,-21],[-21,0],[21,0],[-21,21],[0,21]]);
  noOverlap(value);const before=structuredClone(value);
  value=state.applyFieldCommand(value,{type:'add-shortcut',shortcutId:'appended',entity:{kind:'app',id:'synthetic-appended'},contextId:'personal'}).document;
  assert.deepEqual(value.shortcuts.slice(0,7),before.shortcuts);noOverlap(value);
});
test('compact preview is detached, deterministic and preserves every opaque ref, ID, order and other space',()=>{
  assert.equal(typeof state.compactFieldContext,'function');
  const value=legacy(),before=structuredClone(value),packed=state.compactFieldContext(value,'personal');
  assert.deepEqual(value,before);assert.deepEqual(packed.contexts,before.contexts);
  assert.deepEqual(packed.shortcuts.map(({slot,...item})=>item),before.shortcuts.map(({slot,...item})=>item));
  assert.deepEqual(packed.shortcuts.find(item=>item.shortcutId==='foreign'),before.shortcuts.find(item=>item.shortcutId==='foreign'));
  assert.deepEqual(state.compactFieldContext(packed,'personal'),packed);noOverlap(packed);
  assert.throws(()=>state.compactFieldContext(value,'missing'),error=>error.code==='field_context_missing');
});
test('pack respects a foreign space occupying the origin instead of overwriting or colliding with it',()=>{
  assert.equal(typeof state.compactFieldContext,'function');
  const value=legacy();value.contexts[0].x=0;value.contexts[0].y=0;value.contexts[1].x=0;value.contexts[1].y=0;
  const foreign=value.shortcuts.find(item=>item.shortcutId==='foreign');foreign.slot=[0,0];
  const before=structuredClone(value),packed=state.compactFieldContext(value,'personal');
  assert.deepEqual(value,before);assert.deepEqual(packed.shortcuts.find(item=>item.shortcutId==='foreign'),foreign);
  assert.equal(packed.shortcuts.filter(item=>item.contextId==='personal').some(item=>item.slot[0]===0&&item.slot[1]===0),false);noOverlap(packed);
});
test('confirmed exact pack is one existing restore history entry; undo restores the whole original and changed preview is rejected',()=>{
  assert.equal(typeof state.compactFieldContext,'function');
  const before=legacy(),packed=state.compactFieldContext(before,'personal'),history=state.createFieldHistory();
  assert.equal(history.entries.length,0);
  const change=state.applyFieldCommand(before,{type:'restore',document:packed,expected:before});state.recordFieldHistory(history,change);
  assert.equal(history.entries.length,1);assert.deepEqual(state.undoFieldHistory(history,change.document).document,before);assert.equal(history.entries.length,0);
  const changed=structuredClone(before);changed.shortcuts[0].slot=[-50,5];
  assert.throws(()=>state.applyFieldCommand(changed,{type:'restore',document:packed,expected:before}),error=>error.code==='field_conflict');
  assert.deepEqual(changed.shortcuts[0].slot,[-50,5]);
});
