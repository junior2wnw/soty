import test from 'node:test';
import assert from 'node:assert/strict';
import {createLiveAppScene} from './live-app-scene.mjs';
import {createPersonalFieldDocument} from './app-field-placement.mjs';
const app=(id,title=id)=>({entity:{kind:'app',id},title,source:'public',description:'Metadata never persists'});
const personal=()=>createPersonalFieldDocument();
test('a new accessible app appears on the same scene while personal geometry stays byte-for-byte equal',()=>{
  const projection=createLiveAppScene(),doc=personal(),before=JSON.stringify(doc);
  const first=projection.update(doc,[app('a')]),second=projection.update(doc,[app('b'),app('a')]);
  assert.equal(second.shortcuts.length,2);assert.deepEqual(second.shortcuts.find(s=>s.entity.id==='a').slot,first.shortcuts[0].slot);
  assert.equal(JSON.stringify(doc),before);assert.equal(JSON.stringify(second).includes('Metadata'),false);assert.equal(JSON.stringify(second).includes('source'),false);
});
test('revoked apps leave the live projection; pinned identities are not duplicated',()=>{
  const doc=personal();doc.shortcuts.push({shortcutId:'mine-a',entity:{kind:'app',id:'a'},contextId:'personal',slot:[63,0]});
  const projection=createLiveAppScene();assert.deepEqual(projection.update(doc,[app('a'),app('b')]).shortcuts.map(s=>s.entity.id),['b']);
  assert.equal(projection.update(doc,[app('a')]).shortcuts.length,0);
});
test('user-controlled IDs cannot collide with the automatic layer namespace',()=>{
  const doc=personal();doc.contexts[0].contextId='soty-live-apps';doc.shortcuts.forEach(s=>{s.contextId='soty-live-apps';});doc.shortcuts[0].shortcutId='live-a';
  const scene=createLiveAppScene().update(doc,[app('a')]);assert.notEqual(scene.contexts[0].contextId,doc.contexts[0].contextId);assert.notEqual(scene.shortcuts[0].shortcutId,doc.shortcuts[0].shortcutId);
});
test('negative control: a metadata refresh alone fails the required app-on-canvas oracle',()=>{
  const doc=personal(),requireApp=scene=>assert.ok(scene.shortcuts.some(s=>s.entity.kind==='app'&&s.entity.id==='a'),'new app must have a visible scene identity');
  assert.throws(()=>requireApp(doc),/new app must have/);requireApp(createLiveAppScene().update(doc,[app('a')]));
});

test('canonical widget coverage removes only the generated launcher and preserves an unplaced app',()=>{
  const doc={schema:'soty.field.v2',contexts:[{contextId:'personal',title:'Personal',x:0,y:0}],shortcuts:[{shortcutId:'placed-widget',entity:{kind:'object',id:'object_fixture'},contextId:'personal',placement:{kind:'free',x:0,y:0,width:320,height:240}}]},before=JSON.stringify(doc),projection=createLiveAppScene();
  const initial=projection.update(doc,[app('a'),app('b')]);
  const scene=projection.update(doc,[app('a'),app('b')],{representedAppIds:new Set(['a'])});
  assert.deepEqual(scene.shortcuts.map(row=>row.entity.id),['b']);assert.deepEqual(scene.shortcuts[0].slot,initial.shortcuts.find(row=>row.entity.id==='b').slot);assert.equal(JSON.stringify(doc),before);
  const dropped={...doc,shortcuts:[]};assert.deepEqual(projection.update(dropped,[app('a'),app('b')],{representedAppIds:new Set()}).shortcuts.map(row=>row.entity.id),['a','b']);
});

test('coverage never removes two explicit canonical app launchers chosen alongside a widget',()=>{
  const doc={schema:'soty.field.v2',contexts:[{contextId:'personal',title:'Personal',x:0,y:0}],shortcuts:[
    {shortcutId:'explicit-one',entity:{kind:'app',id:'a'},contextId:'personal',placement:{kind:'hex',slot:[0,0]}},
    {shortcutId:'explicit-two',entity:{kind:'app',id:'a'},contextId:'personal',placement:{kind:'hex',slot:[1,0]}},
    {shortcutId:'placed-widget',entity:{kind:'object',id:'object_fixture'},contextId:'personal',placement:{kind:'free',x:300,y:0,width:320,height:240}},
  ]},before=JSON.stringify(doc);const scene=createLiveAppScene().update(doc,[app('a'),app('b')],{representedAppIds:new Set(['a'])});assert.deepEqual(scene.shortcuts.map(row=>row.entity.id),['b']);assert.equal(doc.shortcuts.filter(row=>row.entity.kind==='app').length,2);assert.equal(JSON.stringify(doc),before);
});
