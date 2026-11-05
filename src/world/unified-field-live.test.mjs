import test from 'node:test';
import assert from 'node:assert/strict';
import {createFieldDocument} from '../../modules/field/contract.mjs';
import {validateLiveFieldScene,combineFieldScene,combineFieldLayout} from './unified-field-live.mjs';
import {layoutUnifiedField} from './unified-field-layout.mjs';

test('live scenes are bounded detached DTOs and cannot smuggle directory metadata into saved layout',()=>{
  const personal=createFieldDocument(),input={contexts:[{contextId:'live',title:'Общее',x:1000,y:0}],shortcuts:[{shortcutId:'live-a',contextId:'live',slot:[0,0],entity:{kind:'app',id:'app-a'}}]};
  const safe=validateLiveFieldScene(input,personal),combined=combineFieldScene(personal,safe);
  assert.equal(combined.shortcuts.length,1);assert.equal(personal.shortcuts.length,0);input.contexts[0].x=9999;assert.equal(safe.contexts[0].x,1000);
  const bad=structuredClone(input);bad.shortcuts[0].entity.secret='must not persist';assert.throws(()=>validateLiveFieldScene(bad,personal));
  const huge={contexts:input.contexts,shortcuts:Array.from({length:257},(_,i)=>({shortcutId:`s-${i}`,contextId:'live',slot:[i,0],entity:{kind:'app',id:`a-${i}`}}))};assert.throws(()=>validateLiveFieldScene(huge,personal),/field_limit_exceeded/);
});

test('the visual union retains exact world geometry for both personal and live layouts',()=>{
  const make=(id,x)=>({contexts:[{contextId:id,title:id,x,y:0}],shortcuts:[{shortcutId:id,contextId:id,slot:[0,0],entity:{kind:'app',id}}]});
  const personal=make('personal',0),live=make('live',1000),entities=[{entity:personal.shortcuts[0].entity,title:'Personal'},{entity:live.shortcuts[0].entity,title:'Live'}];
  const a=layoutUnifiedField(personal,entities),b=layoutUnifiedField(live,entities),before=structuredClone([a,b]),union=combineFieldLayout(a,b);
  assert.equal(union.nodes.length,2);assert.deepEqual(union.nodes,[a.nodes[0],b.nodes[0]]);assert.ok(union.bounds.left<=a.bounds.left&&union.bounds.right>=b.bounds.right);assert.deepEqual([a,b],before);
});
