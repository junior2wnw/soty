import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {FIELD_SCHEMA,FIELD_SCHEMA_V2,FIELD_LIMITS,FIELD_V2_LIMITS,createFieldDocument,createFieldDocumentV2,
 validateFieldDocument,validateAnyFieldDocument,validateFieldDocumentV2,validateFieldPlacement,convertFieldDocumentToV2,fieldShortcutPlacement} from './contract.mjs';
import {validateFieldDocument as frozenV1} from './test/support/contract-v1.mjs';

const legacy=()=>({schema:FIELD_SCHEMA,contexts:[{contextId:'home',title:'Дом',x:320,y:200}],shortcuts:[{shortcutId:'stable_app',entity:{kind:'app',id:'app-source'},contextId:'home',slot:[2,-1]}]});
const free=()=>({kind:'free',x:40,y:20,width:320,height:180});
const make=()=>({schema:FIELD_SCHEMA_V2,contexts:legacy().contexts,shortcuts:[{shortcutId:'stable_app',entity:{kind:'app',id:'app-source'},contextId:'home',placement:free()}]});
const bad=value=>assert.throws(()=>validateFieldDocumentV2(value),error=>error.code==='field_document_invalid');
const hash=value=>createHash('sha256').update(JSON.stringify(value)).digest('hex');

test('v1 bytes/hash/defaults and frozen reader contract remain unchanged; conversion is explicit and retains source identities',()=>{
 const v1=legacy(),before=JSON.stringify(v1),old=frozenV1(v1);assert.equal(JSON.stringify(validateAnyFieldDocument(v1)),JSON.stringify(old));assert.equal(hash(validateAnyFieldDocument(v1)),hash(old));assert.equal(createFieldDocument().schema,FIELD_SCHEMA);
 const converted=convertFieldDocumentToV2(v1);assert.equal(converted.schema,FIELD_SCHEMA_V2);assert.deepEqual(converted.shortcuts[0],{shortcutId:v1.shortcuts[0].shortcutId,entity:v1.shortcuts[0].entity,contextId:'home',placement:{kind:'hex',slot:[2,-1]}});assert.deepEqual(converted.contexts,v1.contexts);assert.equal(JSON.stringify(v1),before);
 assert.deepEqual(validateFieldDocumentV2(converted),converted);assert.throws(()=>validateFieldDocument(converted));assert.throws(()=>frozenV1(converted));assert.deepEqual(createFieldDocumentV2(),{schema:FIELD_SCHEMA_V2,contexts:[],shortcuts:[]});
});

test('v2 free and hex placements detach and normalize without storing two geometry sources',()=>{
 const source=make();source.shortcuts[0].placement.x=-0;source.shortcuts.push({...structuredClone(source.shortcuts[0]),shortcutId:'second_source_view',placement:{kind:'hex',slot:[0,-0]}});
 const result=validateAnyFieldDocument(source);source.shortcuts[0].placement.width=500;source.shortcuts[1].entity.id='changed';
 assert.equal(Object.is(result.shortcuts[0].placement.x,-0),false);assert.equal(result.shortcuts[0].placement.width,320);assert.equal(result.shortcuts[1].entity.id,'app-source');assert.deepEqual(result.shortcuts[1].placement.slot,[0,0]);
 assert.deepEqual(fieldShortcutPlacement(legacy().shortcuts[0]),{kind:'hex',slot:[2,-1]});assert.deepEqual(fieldShortcutPlacement(result.shortcuts[0]),{...free(),x:0});
 const twice=convertFieldDocumentToV2(result);twice.shortcuts[0].placement.width=400;assert.equal(result.shortcuts[0].placement.width,320);
});

test('invalid free extents, coordinates and ambiguous geometry reject at the boundary',()=>{
 const changes=[p=>p.x=NaN,p=>p.y=Infinity,p=>p.x='40',p=>p.width=15.999,p=>p.height=8192.001,p=>p.width=-1,p=>p.height=Infinity,
  p=>p.x=FIELD_LIMITS.coordinate,p=>p.y=-FIELD_LIMITS.coordinate,p=>p.slot=[0,0],p=>p.kind='html',p=>p.opacity=.5,p=>delete p.height];
 for(const change of changes){const value=make();change(value.shortcuts[0].placement);bad(value);}
 for(const change of [v=>v.shortcuts[0].slot=[0,0],v=>v.shortcuts[0].mask='svg',v=>v.shortcuts[0].entity.title='private',v=>v.shortcuts[0].entity.kind='grant',v=>v.shortcuts[0].contextId='absent',v=>v.shortcuts[0].shortcutId=' padded ',v=>v.password='no',v=>v.schema='soty.field.v3']){const value=make();change(value);bad(value);}
 const boundary=free();boundary.width=FIELD_V2_LIMITS.maxExtent;boundary.height=FIELD_V2_LIMITS.minExtent;boundary.x=FIELD_LIMITS.coordinate-boundary.width/2;assert.deepEqual(validateFieldPlacement(boundary),boundary);
});

test('free overlap is intentional; duplicate shortcut IDs and duplicate hex slots still fail',()=>{
 const overlap=make();overlap.shortcuts.push({...structuredClone(overlap.shortcuts[0]),shortcutId:'other'});assert.equal(validateFieldDocumentV2(overlap).shortcuts.length,2);
 overlap.shortcuts[1].shortcutId=overlap.shortcuts[0].shortcutId;bad(overlap);
 const hex=convertFieldDocumentToV2(legacy());hex.shortcuts.push({...structuredClone(hex.shortcuts[0]),shortcutId:'duplicate_hex'});bad(hex);
 hex.shortcuts[1].placement.slot=[3,-1];assert.equal(validateFieldDocumentV2(hex).shortcuts.length,2);
});

test('v2 keeps existing count/UTF8 budgets and no reference alias can mutate the source document',()=>{
 const value=make();value.shortcuts=Array.from({length:256},(_,i)=>({...structuredClone(value.shortcuts[0]),shortcutId:'s_'+i}));assert.equal(validateFieldDocumentV2(value).shortcuts.length,256);
 value.shortcuts.push({...structuredClone(value.shortcuts[0]),shortcutId:'excess'});assert.throws(()=>validateFieldDocumentV2(value),error=>error.code==='field_limit_exceeded');
 const id='界'.repeat(128),large={schema:FIELD_SCHEMA_V2,contexts:[{contextId:id,title:'标题',x:0,y:0}],shortcuts:Array.from({length:256},(_,i)=>({shortcutId:String(i)+'界'.repeat(124),entity:{kind:'app',id},contextId:id,placement:free()}))};
 assert.throws(()=>validateFieldDocumentV2(large),error=>error.code==='field_limit_exceeded');
});

test('object refs belong only to v2; the unchanged v1 kind set and reader reject them',()=>{
 const value=make();value.shortcuts[0].entity={kind:'object',id:'object_instance'};assert.deepEqual(validateFieldDocumentV2(value).shortcuts[0].entity,value.shortcuts[0].entity);
 const old=legacy();old.shortcuts[0].entity=value.shortcuts[0].entity;assert.throws(()=>validateFieldDocument(old));assert.throws(()=>frozenV1(old));
});
