import {validateCanvasMask} from '../../modules/canvas-objects/contract.mjs';
import {FIELD_V2_LIMITS} from '../../modules/field/contract.mjs';
/** Bounded visual metadata only. Never accepts CSS, markup, coordinates or content URLs. */
export function validateFieldPresentation(value) {
  try{return validateCanvasMask(value);}catch{return null;}
}
export function fieldPresentationPoints(presentation) {
  if(presentation?.shape==='polygon')return presentation.points.map(point=>({...point}));
  if(presentation?.shape==='ellipse')return Array.from({length:48},(_,index)=>({x:.5+Math.cos(index*Math.PI/24)*.5,y:.5+Math.sin(index*Math.PI/24)*.5}));
  if(presentation?.shape==='hex')return[{x:.5,y:0},{x:1,y:.25},{x:1,y:.75},{x:.5,y:1},{x:0,y:.75},{x:0,y:.25}];
  return[{x:0,y:0},{x:1,y:0},{x:1,y:1},{x:0,y:1}];
}
export function fieldPresentationClip(presentation) {
  if(presentation?.shape==='rect')return'inset(0 round 16px)';
  if(presentation?.shape==='ellipse')return'ellipse(50% 50% at 50% 50%)';
  return`polygon(${fieldPresentationPoints(presentation).map(point=>`${point.x*100}% ${point.y*100}%`).join(',')})`;
}
/** Ray casting with an inclusive edge test: concave masks keep their real empty corners. */
export function fieldPointInPolygon(point,polygon) {
  let inside=false;
  for(let index=0,prior=polygon.length-1;index<polygon.length;prior=index++){
    const a=polygon[prior],b=polygon[index],cross=(point.x-a.x)*(b.y-a.y)-(point.y-a.y)*(b.x-a.x);
    if(Math.abs(cross)<1e-7&&point.x>=Math.min(a.x,b.x)-1e-7&&point.x<=Math.max(a.x,b.x)+1e-7&&point.y>=Math.min(a.y,b.y)-1e-7&&point.y<=Math.max(a.y,b.y)+1e-7)return true;
    if((a.y>point.y)!==(b.y>point.y)&&point.x<(b.x-a.x)*(point.y-a.y)/(b.y-a.y)+a.x)inside=!inside;
  }
  return inside;
}

const roundCorner=1-Math.SQRT1_2;
const cross=(a,b)=>a.x*b.y-a.y*b.x;
const extent=value=>Math.max(FIELD_V2_LIMITS.minExtent,Math.min(FIELD_V2_LIMITS.maxExtent,value));
/** A local-pixel point on the rendered mask, independent of camera scale. */
export function fieldResizeAnchor(presentation,width,height){
  if(presentation.shape==='rect'){
    const inset=roundCorner*Math.min(16,width/2,height/2);return{x:width-inset,y:height-inset};
  }
  if(presentation.shape==='ellipse')return{x:width*(.5+Math.SQRT1_2/2),y:height*(.5+Math.SQRT1_2/2)};
  const points=fieldPresentationPoints(presentation),direction={x:.5,y:.5};let ray=-1,nearest=null;
  for(let index=0;index<points.length;index++){
    const a=points[index],b=points[(index+1)%points.length],edge={x:b.x-a.x,y:b.y-a.y},relative={x:a.x-.5,y:a.y-.5},denominator=cross(direction,edge);
    if(Math.abs(denominator)>1e-12){
      const t=cross(relative,edge)/denominator,u=cross(relative,direction)/denominator;
      if(t>=0&&t<=1&&u>=0&&u<=1)ray=Math.max(ray,t);
    }
    const length=edge.x**2+edge.y**2;if(!length)continue;
    const t=Math.max(0,Math.min(1,((1-a.x)*edge.x+(1-a.y)*edge.y)/length)),x=a.x+t*edge.x,y=a.y+t*edge.y,distance=(1-x)**2+(1-y)**2;
    if(!nearest||distance<nearest.distance-1e-12||Math.abs(distance-nearest.distance)<1e-12&&y>nearest.y)nearest={x,y,distance};
  }
  // The centre may lie outside a concave mask. Keep its actual contour, never a hull.
  const point=ray>=0?{x:.5+.5*ray,y:.5+.5*ray}:nearest;
  if(!point)throw new TypeError('field_resize_geometry_invalid');
  return{x:point.x*width,y:point.y*height};
}
/** Invert the visible grip position while the original top-left stays fixed. */
export function fieldResizeDimensions(presentation,width,height,delta){
  if(![width,height,delta.x,delta.y].every(Number.isFinite)||width<=0||height<=0)throw new TypeError('field_resize_geometry_invalid');
  if(!delta.x&&!delta.y)return{width,height};
  const anchor=fieldResizeAnchor(presentation,width,height);
  if(presentation.shape==='rect'){
    const x=anchor.x+delta.x,y=anchor.y+delta.y;
    // x=w-d,y=h-d,d=k*min(16,w/2,h/2), with the existing minExtent bound.
    const inset=Math.max(roundCorner*FIELD_V2_LIMITS.minExtent/2,Math.min(roundCorner*16,roundCorner*Math.min(x,y)/(2-roundCorner)));
    return{width:extent(x+inset),height:extent(y+inset)};
  }
  return{width:extent(width+delta.x/Math.max(Number.EPSILON,anchor.x/width)),height:extent(height+delta.y/Math.max(Number.EPSILON,anchor.y/height))};
}
