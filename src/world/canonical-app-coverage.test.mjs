import test from 'node:test';
import assert from 'node:assert/strict';
import {deriveCanonicalAppCoverage} from './canonical-app-coverage.mjs';
import {createLiveAppScene} from './live-app-scene.mjs';
const appA='app-'+ 'a'.repeat(32),appB='app-'+ 'b'.repeat(32);
const doc=(objects=['object_a'],apps=[])=>({schema:'soty.field.v2',contexts:[{contextId:'personal',title:'Mine',x:0,y:0}],shortcuts:[
 ...objects.map((id,index)=>({shortcutId:'object-'+index,entity:{kind:'object',id},contextId:'personal',placement:{kind:'hex',slot:[index,0]}})),
 ...apps.map((id,index)=>({shortcutId:'app-'+index,entity:{kind:'app',id},contextId:'personal',placement:{kind:'hex',slot:[index,2]}}))]});
const ref=id=>({kind:'app',id});
test('accepted owner batch represents only canonical apps, preserves explicit duplicates, geometry and unplaced launchers',()=>{
 const document=doc(['object_a'],[appA,appA]),before=JSON.stringify(document),representations=new Map([['object_a',[ref(appA)]],['not_placed',[ref(appB)]]]);
 const result=deriveCanonicalAppCoverage({document,representations});assert.equal(result.status,'ready');assert.deepEqual([...result.appIds],[appA]);
 const scene=createLiveAppScene().update(document,[appA,appB].map(id=>({entity:ref(id)})),{representedAppIds:result.appIds});
 assert.deepEqual(scene.shortcuts.map(item=>item.entity.id),[appB]);assert.equal(document.shortcuts.filter(item=>item.entity.id===appA).length,2);assert.equal(JSON.stringify(document),before);
});
test('undefined is durable unknown, declared none is ready, and missing metadata never suppresses an unplaced app',()=>{
 const document=doc(),unknown=deriveCanonicalAppCoverage({document});assert.equal(unknown.status,'unknown');assert.deepEqual(unknown.unknownObjectIds,['object_a']);assert.equal(unknown.appIds.size,0);
 const known=deriveCanonicalAppCoverage({document,representations:new Map([['object_a',[]]])});assert.equal(known.status,'ready');assert.deepEqual(known.unknownObjectIds,[]);
});
test('hidden and locked bindings need only their accepted representation header, without API or content ports',()=>{
 const result=deriveCanonicalAppCoverage({document:doc(['hidden','locked','native']),representations:new Map([['hidden',[ref(appA)]],['locked',[ref(appA)]],['native',[]]])});
 assert.equal(result.status,'ready');assert.deepEqual([...result.appIds],[appA]);
});
test('removal and last canonical ref drop erase coverage despite an older metadata entry',()=>{
 const representations=new Map([['object_a',[ref(appA)]]]);assert.equal(deriveCanonicalAppCoverage({document:doc(),representations}).appIds.has(appA),true);
 assert.equal(deriveCanonicalAppCoverage({document:doc([]),representations}).appIds.size,0);representations.set('object_a',[]);assert.equal(deriveCanonicalAppCoverage({document:doc(),representations}).appIds.size,0);
});
test('generic non-app representations are known but confer no app suppression; malformed refs are rejected',()=>{
 const document=doc(),result=deriveCanonicalAppCoverage({document,representations:new Map([['object_a',[{kind:'person',id:'person_fixture'}]]])});assert.equal(result.status,'ready');assert.equal(result.appIds.size,0);
 for(const value of [null,[{kind:'app',id:appA,url:'https://invalid/'}],Array.from({length:17},()=>ref(appA))])assert.throws(()=>deriveCanonicalAppCoverage({document,representations:new Map([['object_a',value]])}));
});
