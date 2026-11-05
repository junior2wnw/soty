import test from 'node:test';
import assert from 'node:assert/strict';
import {fieldResizeAnchor,fieldResizeDimensions,fieldPresentationPoints,fieldPointInPolygon} from './unified-field-presentation.mjs';

const near=(a,b)=>assert.ok(Math.abs(a-b)<1e-7,`${a} ≠ ${b}`);
const concave={shape:'polygon',points:[{x:0,y:0},{x:.8,y:0},{x:.8,y:.2},{x:.2,y:.2},{x:.2,y:.8},{x:0,y:.8}]};
test('mask anchors follow hex/ellipse contours and the actual16px rounded corner',()=>{
  const hex=fieldResizeAnchor({shape:'hex'},320,240);near(hex.x,320*5/6);near(hex.y,200);
  const ellipse=fieldResizeAnchor({shape:'ellipse'},320,240);near(((ellipse.x-160)/160)**2+((ellipse.y-120)/120)**2,1);
  for(const[w,h]of[[320,240],[16,16],[20,50],[80,18]]){const p=fieldResizeAnchor({shape:'rect'},w,h),r=Math.min(16,w/2,h/2);near((p.x-(w-r))**2+(p.y-(h-r))**2,r*r);}
});
test('concave outside-centre fallback stays on a real edge and not the empty hull corner',()=>{
  assert.equal(fieldPointInPolygon({x:.5,y:.5},concave.points),false);const p=fieldResizeAnchor(concave,320,240);near(p.x,64);near(p.y,192);assert.equal(fieldPointInPolygon({x:p.x/320,y:p.y/240},concave.points),true);
  assert.equal(fieldPointInPolygon({x:1,y:1},fieldPresentationPoints({shape:'hex'})),false,'original bbox anchor must fail');
});
test('inverse grip mapping follows pointer in both axes including small rounded rect transitions',()=>{
  for(const presentation of [{shape:'rect'},{shape:'hex'},{shape:'ellipse'},concave])for(const[w,h]of[[320,240],[16,16],[20,50],[80,18],[8190,80]])for(const delta of [{x:40,y:30},{x:-6,y:-9},{x:0,y:10},{x:15,y:0}]){
    const before=fieldResizeAnchor(presentation,w,h),size=fieldResizeDimensions(presentation,w,h,delta),after=fieldResizeAnchor(presentation,size.width,size.height);
    assert.ok(size.width>=16&&size.width<=8192&&size.height>=16&&size.height<=8192);
    if(size.width>16&&size.width<8192)near(after.x-before.x,delta.x);
    if(size.height>16&&size.height<8192)near(after.y-before.y,delta.y);
  }
});
test('the same tracking oracle rejects the former unadjusted bbox pointer mapping',()=>{
  const p={shape:'hex'},start=fieldResizeAnchor(p,320,240),wrong=fieldResizeAnchor(p,360,270);
  assert.throws(()=>near(wrong.x-start.x,40));assert.throws(()=>near(wrong.y-start.y,30));
});
test('zero motion does not invent a placement write; bounds and input masks remain intact',()=>{
  const original=JSON.stringify(concave);assert.deepEqual(fieldResizeDimensions(concave,320,240,{x:0,y:0}),{width:320,height:240});fieldResizeDimensions(concave,320,240,{x:20000,y:-20000});assert.equal(JSON.stringify(concave),original);assert.throws(()=>fieldResizeDimensions(concave,320,240,{x:NaN,y:0}),/geometry_invalid/);
});
