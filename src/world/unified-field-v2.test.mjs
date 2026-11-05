import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {applyFieldCommand,createFieldHistory,recordFieldHistory,undoFieldHistory,compactFieldContext,nextFieldSlot} from './unified-field-state.mjs';
import {layoutUnifiedField,fieldMovePreview,fieldSlotToPoint} from './unified-field-layout.mjs';
import {fieldPresentationClip,fieldPointInPolygon,validateFieldPresentation} from './unified-field-presentation.mjs';
const legacy=()=>({schema:'soty.field.v1',contexts:[{contextId:'home',title:'Личное',x:0,y:0},{contextId:'work',title:'Работа',x:1500,y:250}],shortcuts:[{shortcutId:'app-old',entity:{kind:'app',id:'application'},contextId:'home',slot:[0,0]}]});
const free={kind:'free',x:126.25,y:-69.75,width:640,height:360};
const entities=[{entity:{kind:'app',id:'application'},title:'Приложение'},{entity:{kind:'object',id:'object_native'},title:'Моя записка',presentation:{shape:'rect'}}];
const add=source=>applyFieldCommand(source,{type:'add-shortcut',shortcutId:'native',entity:{kind:'object',id:'object_native'},contextId:'work',placement:free});

test('legacy reads/ordinary commands preserve exact v1 bytes; explicit free placement alone promotes v2',()=>{
  const before=legacy(),bytes=JSON.stringify(before),layout=layoutUnifiedField(before,entities);assert.equal(JSON.stringify(before),bytes);
  const rename=applyFieldCommand(before,{type:'rename-context',contextId:'home',title:'Дом'});assert.equal(rename.document.schema,'soty.field.v1');assert.deepEqual(rename.document.shortcuts,before.shortcuts);
  const added=add(before);assert.equal(added.document.schema,'soty.field.v2');assert.equal(JSON.stringify(before),bytes);
  assert.deepEqual(added.document.shortcuts[0],{shortcutId:'app-old',entity:before.shortcuts[0].entity,contextId:'home',placement:{kind:'hex',slot:[0,0]}});
  assert.deepEqual(layoutUnifiedField(added.document,entities).nodes[0],layout.nodes[0]);
});
test('free dimensions/position have one source, survive context movement and exact undo to literal v1',()=>{
  const before=legacy(),change=add(before),history=createFieldHistory();recordFieldHistory(history,change);
  const layout=layoutUnifiedField(change.document,entities),node=layout.nodes.find(value=>value.shortcutId==='native');
  assert.equal(node.cx,1500+free.x);assert.equal(node.cy,250+free.y);assert.equal(node.width,640);assert.equal(node.height,360);assert.equal(node.x,node.cx-320);assert.equal(node.y,node.cy-180);
  const moved=applyFieldCommand(change.document,{type:'move-context',contextId:'work',x:-500,y:800});
  assert.deepEqual(moved.document.shortcuts,change.document.shortcuts);assert.deepEqual(moved.document.shortcuts[1].placement,free);
  assert.equal(layoutUnifiedField(moved.document,entities).nodes[1].cx,-500+free.x);
  assert.deepEqual(undoFieldHistory(history,change.document).document,before);
});
test('free resize/move may intentionally overlap; automatic hex fill still avoids free content',()=>{
  const before=add(legacy()).document,target={contextId:'home',placement:{kind:'free',x:0,y:0,width:410,height:170}},preview=fieldMovePreview(before,entities,'native',target);
  assert.equal(preview.valid,true);assert.deepEqual(before.shortcuts[1].placement,free);
  const change=applyFieldCommand(before,{type:'place-shortcut',shortcutId:'native',...target});
  const first=layoutUnifiedField(change.document,entities).nodes[0],second=layoutUnifiedField(change.document,entities).nodes[1];assert.equal(first.cx,second.cx);
  const slot=nextFieldSlot(change.document,'home'),point=fieldSlotToPoint(slot);assert.ok(Math.abs(point.x)>205||Math.abs(point.y)>85);
  const compact=compactFieldContext(change.document,'home');assert.deepEqual(compact.shortcuts[1],change.document.shortcuts[1]);
});
test('normalized concave polygon and ellipse honor real empty corners; CSS/HTML/oversized/degenerate masks are refused',()=>{
  const presentation={shape:'polygon',points:[{x:0,y:0},{x:1,y:0},{x:.35,y:.35},{x:0,y:1}]};
  assert.deepEqual(validateFieldPresentation(presentation),presentation);assert.match(fieldPresentationClip(presentation),/^polygon\(/u);
  assert.equal(fieldPointInPolygon({x:.1,y:.1},presentation.points),true);assert.equal(fieldPointInPolygon({x:.8,y:.8},presentation.points),false);
  const document=add(legacy()).document,projected=layoutUnifiedField(document,[entities[0],{...entities[1],presentation}]).nodes[1];
  assert.equal(fieldPointInPolygon({x:projected.x+projected.width*.8,y:projected.y+projected.height*.8},projected.footprint),false);
  for(const mask of [{shape:'custom',css:'url(javascript:)'},{shape:'rect',html:'<script>'},{shape:'polygon',points:[{x:0,y:0},{x:0,y:0},{x:1,y:1}]},{shape:'polygon',points:Array.from({length:129},(_,index)=>({x:index/129,y:index/129}))}])assert.equal(validateFieldPresentation(mask),null);
});
test('a shape-changing directory update never changes persisted placement or content identity',()=>{
  const document=add(legacy()).document,before=JSON.stringify(document),one=layoutUnifiedField(document,entities).nodes[1];
  const next=layoutUnifiedField(document,[entities[0],{...entities[1],title:'Новая аватарка/название',presentation:{shape:'ellipse'}}]).nodes[1];
  assert.equal(next.cx,one.cx);assert.equal(next.cy,one.cy);assert.equal(next.width,one.width);assert.equal(JSON.stringify(document),before);
  assert.equal(fieldPointInPolygon({x:next.x,y:next.y},next.footprint),false);assert.equal(fieldPointInPolygon({x:next.cx,y:next.cy},next.footprint),true);
});
test('negative controls detect ignored free geometry and the original absence of resize/place command',async()=>{
  const url=new URL('./unified-field-layout.mjs',import.meta.url),source=readFileSync(url,'utf8'),needle='const width = free?placement.width:metrics.radius * SQRT3, height = free?placement.height:metrics.radius * 2;';
  const broken=source.replace(needle,'const width=metrics.radius * SQRT3,height=metrics.radius*2;');assert.notEqual(broken,source);
  const absolute=broken.replace(/from '(\.\.?\/[^']+)'/gu,(_match,path)=>`from '${new URL(path,url).href}'`),module=await import(`data:text/javascript;base64,${Buffer.from(absolute).toString('base64')}`);
  const document=add(legacy()).document;assert.throws(()=>assert.equal(module.layoutUnifiedField(document,entities).nodes[1].width,640),error=>error.code==='ERR_ASSERTION');
  const root=fileURLToPath(new URL('../../',import.meta.url));const baseline=execFileSync('git',['show','450204c:src/world/unified-field-state.mjs'],{cwd:root,encoding:'utf8'});
  const stateUrl=new URL('./unified-field-state.mjs',import.meta.url),old=baseline.replace(/from '(\.\.?\/[^']+)'/gu,(_match,path)=>`from '${new URL(path,stateUrl).href}'`),oldModule=await import(`data:text/javascript;base64,${Buffer.from(old).toString('base64')}`);
  assert.throws(()=>oldModule.applyFieldCommand(legacy(),{type:'place-shortcut',shortcutId:'app-old',contextId:'home',placement:free}),error=>error.code==='field_command_invalid');
  assert.equal(applyFieldCommand(legacy(),{type:'place-shortcut',shortcutId:'app-old',contextId:'home',placement:free}).document.schema,'soty.field.v2');
});
