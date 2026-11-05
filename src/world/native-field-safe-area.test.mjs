import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {nativeFieldSafeArea} from './native-field-safe-area.mjs';
import {fieldPresentationPoints,fieldPointInPolygon} from './unified-field-presentation.mjs';

const masks=[{shape:'rect'},{shape:'ellipse'},{shape:'hex'},
  {shape:'polygon',points:[{x:.06,y:.04},{x:.94,y:0},{x:1,y:.83},{x:.78,y:.96},{x:0,y:.9}]},
  {shape:'polygon',points:[{x:0,y:0},{x:1,y:0},{x:1,y:1},{x:.7,y:1},{x:.7,y:.28},{x:.3,y:.28},{x:.3,y:1},{x:0,y:1}]},
  {shape:'polygon',points:[{x:.03,y:.1},{x:.14,y:.1},{x:.14,y:.9},{x:.03,y:.9}]},
];
function assertInside(mask,width,height,inset){
  const rect={left:inset.left/width,right:1-inset.right/width,top:inset.top/height,bottom:1-inset.bottom/height};
  assert.ok(rect.right>rect.left&&rect.bottom>rect.top,'A valid mask keeps a positive content viewport');
  for(let x=0;x<=20;x++)for(let y=0;y<=20;y++){
    const point={x:rect.left+(rect.right-rect.left)*x/20,y:rect.top+(rect.bottom-rect.top)*y/20};
    const inside=mask.shape==='ellipse'?((point.x-.5)/.5)**2+((point.y-.5)/.5)**2<=1+1e-10:fieldPointInPolygon(point,fieldPresentationPoints(mask));
    assert.ok(inside,`Content crosses ${mask.shape} at ${point.x},${point.y} for ${width}×${height}`);
  }
}
test('the whole content viewport stays inside rect/ellipse/hex/concave/off-centre masks across resizes',()=>{
  for(const [width,height]of[[16,16],[96,156],[340,300],[800,160],[200,900],[1600,1200]])for(const mask of masks)assertInside(mask,width,height,nativeFieldSafeArea(mask,width,height));
});
test('rect preserves its existing padding and an ellipse gives a useful centered viewport',()=>{
  assert.deepEqual(nativeFieldSafeArea({shape:'rect'},340,300),{top:0,right:0,bottom:0,left:0});
  const area=nativeFieldSafeArea({shape:'ellipse'},340,300);assert.equal(area.left,area.right);assert.equal(area.top,area.bottom);
  assert.ok(340-area.left-area.right>220);assert.ok(300-area.top-area.bottom>190);
});
test('invalid presentation cannot inject styles and resets to the ordinary rect viewport',()=>{
  assert.deepEqual(nativeFieldSafeArea({shape:'polygon',points:[{x:'expression(secret)',y:1}]},340,300),{top:0,right:0,bottom:0,left:0});
});
test('negative controls: old zero inset and a bounding box around a concavity fail the same interior oracle',()=>{
  assert.throws(()=>assertInside(masks[3],340,300,{top:0,right:0,bottom:0,left:0}),/Content crosses/);
  assert.throws(()=>assertInside(masks[4],340,300,{top:8,right:8,bottom:8,left:8}),/Content crosses/);
});
test('media element selectors do not style or hide their provider articles',()=>{
  const css=readFileSync(new URL('./native-field-objects.css',import.meta.url),'utf8');
  const glass=readFileSync(new URL('./field-glass.css',import.meta.url),'utf8');
  function verify(text){
    assert.match(text,/img\.nf-image:not\(\[src\]\)\{display:none\}/,'Only the actual image is hidden before src');
    assert.match(text,/audio\.nf-audio\{[^}]*height:46px/,'Only the audio control is 46px high');
    assert.match(text,/video\.nf-video\{/,'Video sizing applies to the video element');
    for(const selector of text.matchAll(/([^{}]+)\{/gu))for(const value of selector[1].split(',')){
      assert.doesNotMatch(value.trim(),/^\.nf-(?:image|audio|video)(?=[:{\s]|$)/u,'A bare media class also matches its article container');
    }
  }
  verify(css);assert.match(glass,/img\.nf-image\{margin:6px/);
  assert.throws(()=>verify(css.replaceAll('img.nf-image',''+'.nf-image')),/Only the actual image/);
  assert.throws(()=>verify(css.replaceAll('audio.nf-audio',''+'.nf-audio')),/Only the audio control/);
  assert.throws(()=>verify(css.replaceAll('video.nf-video',''+'.nf-video')),/Video sizing/);
});
