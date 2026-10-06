import test from 'node:test';
import assert from 'node:assert/strict';
import {createFieldArtResolver,resolveFieldAppArt} from './field-art.mjs';
import {appArtBindingKey} from './app-art-identity.mjs';
const id='app-'+'1'.repeat(32);
const asset=(key,square=false)=>({key,version:1,alt:'Generic studio sculpture',palette:{base:'#303B34',accent:'#C8D6BF',ink:'#F4F5ED'},focalPoint:{x:.5,y:.4},compactFocalPoint:{x:.55,y:.42},renditions:[160,320,640,960].map(width=>({url:`/app-art/${key}/v001-${'a'.repeat(12)}/cover-${width}.${'b'.repeat(12)}.webp`,width,height:square?width:Math.round(width/1.5)}))});
const base={assets:{hive:asset('hive')},bindings:{[appArtBindingKey(id)]:'hive'}};
const field={schemaVersion:1,profile:'field',aliases:{hive:'field-hive'},bindings:{},assets:{'field-hive':asset('field-hive',true)}};
test('field selects explicit profile artwork, retains full safe metadata and never changes card resolution',()=>{
 const resolve=createFieldArtResolver(base,field),art=resolve({coverKey:'hive',name:'renamed'},{screenWidth:159.4});
 assert.equal(art.profile,'field');assert.equal(art.width,640);assert.equal(art.height,640);assert.equal(art.fallback,false);assert.equal(art.sizes,'160px');assert.equal(art.srcset.split(', ').length,4);assert.equal(art.focalPosition,'50% 40%');assert.equal(art.compactFocalPosition,'55% 42%');assert.ok(Object.isFrozen(art));
 assert.equal(createFieldArtResolver(base,{...field,assets:{}})({coverKey:'hive'}).src,base.assets.hive.renditions[2].url);
});
test('exact identity binding wins over an alias, survives a rename and never guesses from HIVE title',()=>{
 const profile=structuredClone(field);profile.assets['field-private']=asset('field-private',true);profile.bindings[appArtBindingKey(id)]='field-private';
 const resolve=createFieldArtResolver(base,profile);
 assert.equal(resolve({appId:id,coverKey:'hive',name:'different title'}).key,'field-private');
 assert.equal(resolve({name:'HIVE'}).status,'fallback');assert.equal(resolve({name:'hive'}).status,'fallback');
});
test('missing, malformed, inherited and external profile entries fall back to valid native card artwork',()=>{
 for(const bad of ['https://tracker.invalid/cover','../private','__proto__','missing']){
  const profile=structuredClone(field);profile.aliases.hive=bad;const art=createFieldArtResolver(base,profile)('hive');assert.equal(art.profile,'card');assert.equal(art.key,'hive');assert.equal(art.src,base.assets.hive.renditions[2].url);
 }
 const inherited={...field,aliases:Object.create(field.aliases),assets:Object.create(field.assets)};
 assert.equal(createFieldArtResolver(base,inherited)('hive').profile,'card');
 const corrupt=structuredClone(field);corrupt.assets['field-hive'].renditions.forEach(r=>r.url='https://tracker.invalid/collect');assert.equal(createFieldArtResolver(base,corrupt)('hive').profile,'card');
});
test('viewport hints are bounded numerical CSS pixels; fallback is deterministic and leaks no pending data',()=>{
 const resolve=createFieldArtResolver({},{});
 for(const [screenWidth,sizes]of [[-1,'88px'],[99999,'512px'],[NaN,'208px'],['100vw','208px']])assert.equal(resolve({appId:id},{screenWidth}).sizes,sizes);
 const art=resolve({appId:id,name:'PRIVATE_NAME',description:'PRIVATE_DESCRIPTION'});assert.equal(art.alt,'');assert.equal(art.src,null);assert.equal(art.fallback,true);assert.equal(art.focalPosition,'50% 50%');assert.ok(!JSON.stringify(art).includes('PRIVATE_'));
});
test('current accepted card artwork remains available before optional field art is generated',()=>{
 for(const key of ['notes','chess','hive','pulse','tavysh','focus'])assert.equal(resolveFieldAppArt(key).status,'ready');
});
test('decorative glyphs require an explicit accepted field asset and reject inherited or executable icon values',()=>{
 const profile=structuredClone(field);profile.assets['field-hive'].icon='brush';assert.equal(createFieldArtResolver(base,profile)('hive').icon,'brush');
 for(const glyph of ['PRIVATE_NAME','https://tracking.invalid/glyph','<svg onload=alert(1)>',null]){profile.assets['field-hive'].icon=glyph;assert.equal(createFieldArtResolver(base,profile)('hive').icon,undefined);}
 profile.assets['field-hive']=Object.assign(Object.create({icon:'brush'}),asset('field-hive',true));assert.equal(createFieldArtResolver(base,profile)('hive').icon,undefined);
 assert.equal(createFieldArtResolver(base,field)({name:'Canvas'}).icon,undefined);
});
