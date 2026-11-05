import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import {fieldPresentationPoints,fieldPointInPolygon} from './unified-field-presentation.mjs';

// CPU probes execute the production chamfer and the actual scalar GLSL helper.
// Raster inputs are synthetic supersampled masks. GPU image acceptance remains
// the real-browser before/after A/B, not this arithmetic/source-port check.
const runtime=readFileSync(new URL('../../vendor/optical-glass/src/liquid-glass.js',import.meta.url),'utf8');
function ports(source=runtime){
  const parsed=ts.createSourceFile('liquid-glass.js',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.JS);let method;
  function visit(node){if(ts.isMethodDeclaration(node)&&node.name?.getText(parsed)==='_chamferDistance')method=node;ts.forEachChild(node,visit);}visit(parsed);assert.ok(method);
  const printed=ts.createPrinter().printNode(ts.EmitHint.Unspecified,method,parsed);
  const chamfer=vm.runInNewContext(`({${printed}})._chamferDistance`);
  const body=source.match(/float extendShapeDistance\(float edgeDistance, float outsideDistance\)\s*\{([^}]+)\}/u)?.[1];assert.ok(body,'Actual shader distance continuation exists');
  const extend=new Function('edgeDistance','outsideDistance','max',body);
  assert.match(source,/vec2 nearest = clamp\(p, rect\.xy, rect\.xy \+ rect\.zw\)/);
  assert.match(source,/encodedDistance = sampleShapeAtlas\(nearest, rect, shapeAtlasRect\)/u,'The real SDF sample uses the nearest in-bounds point');
  assert.match(source,/halfTexel = 0\.5 \/ vec2\(textureSize\(u_shapeAtlas, 0\)\)/u);
  assert.match(source,/atlasRect\.xy \+ halfTexel, atlasRect\.xy \+ atlasRect\.zw - halfTexel/u,'LINEAR filtering cannot mix SDF with the transparent packing gutter');
  assert.match(source,/distanceToShape = extendShapeDistance\(edgeDistance, length\(p - nearest\)\)/);
  return{chamfer,extend:(edge,outside)=>extend(edge,outside,Math.max)};
}
function raster(mask,width,height){
  const points=fieldPresentationPoints(mask),alpha=new Uint8ClampedArray(width*height);
  for(let y=0;y<height;y++)for(let x=0;x<width;x++){
    let covered=0;for(let dy=0;dy<4;dy++)for(let dx=0;dx<4;dx++){
      const point={x:(x+(dx+.5)/4)/width,y:(y+(dy+.5)/4)/height};
      if(mask.shape==='ellipse'?((point.x-.5)*2)**2+((point.y-.5)*2)**2<=1:fieldPointInPolygon(point,points))covered++;
    }
    alpha[y*width+x]=Math.round(covered/16*255);
  }
  return alpha;
}
function boundaryOracle(port,alpha,width,height){
  const inside=port.chamfer(alpha,width,height,true),outside=port.chamfer(alpha,width,height,false);
  for(let index=0;index<alpha.length;index++){
    const x=index%width,y=Math.floor(index/width);if(x!==0&&x!==width-1&&y!==0&&y!==height-1)continue;
    const signed=(alpha[index]>=128?-outside[index]:inside[index])+.5-alpha[index]/255;
    if(alpha[index]>=128)assert.ok(outside[index]<=1,'A filled edge sees the immediately adjacent transparent exterior');
    const justOutside=port.extend(signed,.125),farOutside=port.extend(signed,400);
    assert.ok(Math.abs(justOutside-Math.max(signed,0))<.126,'SDF continues at a canvas edge instead of jumping to a constant range');
    assert.ok(justOutside>0,'No custom lens extends opaque content beyond the target bounds');
    assert.ok(farOutside>399,'Shadow distance must grow outside the target, never plateau across a rectangular scissor');
  }
}
test('the actual custom-SDF edge continues beyond rect/ellipse/hex/concave raster bounds at several scales',()=>{
  const port=ports(),masks=[{shape:'rect'},{shape:'ellipse'},{shape:'hex'},{shape:'polygon',points:[{x:0,y:0},{x:1,y:0},{x:1,y:1},{x:.65,y:1},{x:.65,y:.3},{x:.3,y:.3},{x:.3,y:1},{x:0,y:1}]}];
  for(const scale of[.5,1,1.35,2])for(const mask of masks){const width=Math.round(48*scale),height=Math.round(40*scale);boundaryOracle(port,raster(mask,width,height),width,height);}
});
test('the raster oracle includes partial alpha and full canvas fills rather than only masks with an empty border',()=>{
  const port=ports();for(const alpha of[0,64,127,128,192,255])boundaryOracle(port,new Uint8ClampedArray(49).fill(alpha),7,7);
});
test('negative control: the old chamfer misses transparent canvas exterior on a fully filled custom face',()=>{
  const from='distance[index] = inside === sourceInside ? 0\n          : !sourceInside ? Math.min(x + 1, width - x, y + 1, height - y)\n          : infinity;';
  assert.equal(runtime.split(from).length,2);const broken=ports(runtime.replace(from,'distance[index] = inside === sourceInside ? 0 : infinity;'));
  assert.throws(()=>boundaryOracle(broken,new Uint8ClampedArray(64).fill(255),8,8),/filled edge sees/);
});
test('negative control: old zero-outside-atlas sampling is caught by the same edge-continuity oracle',()=>{
  const body='return outsideDistance > 0.0\n        ? max(edgeDistance, 0.0) + outsideDistance\n        : edgeDistance;';
  assert.equal(runtime.split(body).length,2);const broken=ports(runtime.replace(body,'return outsideDistance > 0.0 ? 84.0 : edgeDistance;'));
  assert.throws(()=>boundaryOracle(broken,new Uint8ClampedArray(64).fill(255),8,8),/SDF continues/);
});
test('negative control: an atlas UV clamped to tile edges cannot pass the verified shape-sampling port',()=>{
  const from='vec2 halfTexel = 0.5 / vec2(textureSize(u_shapeAtlas, 0));';assert.equal(runtime.split(from).length,2);
  assert.throws(()=>ports(runtime.replace(from,'vec2 halfTexel = vec2(0.0);')),error=>error.code==='ERR_ASSERTION');
});
