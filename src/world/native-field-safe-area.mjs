import {validateFieldPresentation,fieldPresentationPoints} from './unified-field-presentation.mjs';

const intersect=(a,b)=>{
  const result=[];let i=0,j=0;
  while(i<a.length&&j<b.length){const left=Math.max(a[i][0],b[j][0]),right=Math.min(a[i][1],b[j][1]);if(right>left)result.push([left,right]);if(a[i][1]<b[j][1])i++;else j++;}
  return result;
};
function scan(points,y){
  const edges=[];
  for(let index=0,prior=points.length-1;index<points.length;prior=index++){
    const a=points[prior],b=points[index];if((a.y>y)!==(b.y>y))edges.push(a.x+(y-a.y)*(b.x-a.x)/(b.y-a.y));
  }
  edges.sort((a,b)=>a-b);const result=[];for(let index=0;index+1<edges.length;index+=2)result.push([edges[index],edges[index+1]]);return result;
}

/** An interior content viewport, never a second object mask or placement. */
export function nativeFieldSafeArea(value,width,height){
  const mask=validateFieldPresentation(value)??{shape:'rect'};
  if(mask.shape==='rect'||!Number.isFinite(width)||!Number.isFinite(height)||width<=0||height<=0)return{top:0,right:0,bottom:0,left:0};
  let box;
  if(mask.shape==='ellipse'){
    const inset=(1-Math.SQRT1_2)/2,gap=Math.min(8,Math.SQRT1_2*width*.08,Math.SQRT1_2*height*.08);
    const left=inset*width+gap,top=inset*height+gap;return{left,right:left,top,bottom:top};
  }else{
    const points=fieldPresentationPoints(mask);
    // Vertices split every change of edge topology; regular cuts improve useful
    // rectangles on diagonal edges without trusting only rectangle corners.
    const cuts=[...new Set([...points.map(point=>point.y),...Array.from({length:25},(_,index)=>index/24)])].sort((a,b)=>a-b);
    const bands=cuts.slice(0,-1).map((top,index)=>{
      const bottom=cuts[index+1],epsilon=Math.min(1e-9,(bottom-top)/4);
      return intersect(scan(points,top+epsilon),scan(points,bottom-epsilon));
    });
    let best=-1;
    for(let first=0;first<bands.length;first++){
      let intervals=[[0,1]];
      for(let last=first;last<bands.length;last++){
        intervals=intersect(intervals,bands[last]);if(!intervals.length)break;
        const top=cuts[first],bottom=cuts[last+1],h=bottom-top;
        for(const[left,right]of intervals){
          const w=right-left,score=w*h*Math.min(1,w*width/168)*Math.min(1,h*height/132);
          const center=(left+right-1)**2+(top+bottom-1)**2;
          if(score>best+1e-12||Math.abs(score-best)<1e-12&&center<(box?.center??Infinity)){best=score;box={left,right,top,bottom,center};}
        }
      }
    }
  }
  if(!box)return{top:height/2,right:width/2,bottom:height/2,left:width/2};
  const gap=Math.min(8,(box.right-box.left)*width*.08,(box.bottom-box.top)*height*.08);
  return{top:box.top*height+gap,right:(1-box.right)*width+gap,bottom:(1-box.bottom)*height+gap,left:box.left*width+gap};
}
