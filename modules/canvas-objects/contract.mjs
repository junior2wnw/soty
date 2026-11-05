import {validateAnyFieldEntity} from '../field/contract.mjs';
export const OBJECT_SCHEMA='soty.canvas-object.v1';
export const OBJECT_LIMITS=Object.freeze({objects:2048,recipients:256,projectionBytes:65536,projectionBatchBytes:1048576,contentBytes:1048576,polygonPoints:128});
export class CanvasObjectError extends Error{constructor(code='canvas_object_invalid',status=400){super(code);this.code=code;this.status=status;}}
export function check(value,code='canvas_object_invalid',status=400){if(!value)throw new CanvasObjectError(code,status);}
export function exact(value,keys,required=keys){
 check(value!==null&&typeof value==='object'&&!Array.isArray(value)&&[Object.prototype,null].includes(Object.getPrototypeOf(value)));
 const descriptors=Object.getOwnPropertyDescriptors(value);
 check(Reflect.ownKeys(descriptors).every(key=>typeof key==='string'&&keys.includes(key)&&descriptors[key].enumerable&&Object.hasOwn(descriptors[key],'value'))&&required.every(key=>Object.hasOwn(descriptors,key)));
}
export function identity(value){check(typeof value==='string'&&/^[A-Za-z0-9_.:-]{1,128}$/u.test(value));return value;}
export function binding(value){exact(value,['providerId','ref']);identity(value.providerId);identity(value.ref);return {providerId:value.providerId,ref:value.ref};}
export function ownerRepresentation(value){
 check(Array.isArray(value)&&value.length<=16,'canvas_provider_metadata_invalid',502);
 try{return value.map(validateAnyFieldEntity);}catch{throw new CanvasObjectError('canvas_provider_metadata_invalid',502);}
}
const cross=(a,b,c)=>(b.x-a.x)*(c.y-a.y)-(b.y-a.y)*(c.x-a.x);
const on=(a,b,c)=>Math.abs(cross(a,b,c))<1e-10&&c.x>=Math.min(a.x,b.x)-1e-10&&c.x<=Math.max(a.x,b.x)+1e-10&&c.y>=Math.min(a.y,b.y)-1e-10&&c.y<=Math.max(a.y,b.y)+1e-10;
function intersects(a,b,c,d){const x=cross(a,b,c),y=cross(a,b,d),z=cross(c,d,a),w=cross(c,d,b);return x*y<0&&z*w<0||on(a,b,c)||on(a,b,d)||on(c,d,a)||on(c,d,b);}
export function validateCanvasMask(value){
 exact(value,['shape','points'],['shape']);check(['hex','rect','ellipse','polygon'].includes(value.shape));
 if(value.shape!=='polygon'){check(value.points===undefined);return {shape:value.shape};}
 check(Array.isArray(value.points)&&value.points.length>=3&&value.points.length<=OBJECT_LIMITS.polygonPoints);
 const seen=new Set(),points=value.points.map(point=>{exact(point,['x','y']);check([point.x,point.y].every(n=>typeof n==='number'&&Number.isFinite(n)&&n>=0&&n<=1));const p={x:point.x===0?0:point.x,y:point.y===0?0:point.y},key=JSON.stringify(p);check(!seen.has(key));seen.add(key);return p;});
 const area=points.reduce((sum,p,i)=>sum+p.x*points[(i+1)%points.length].y-p.y*points[(i+1)%points.length].x,0);check(Math.abs(area)>2e-6);
 for(let i=0;i<points.length;i++)for(let j=i+1;j<points.length;j++)if(j!==i+1&&!(i===0&&j===points.length-1))check(!intersects(points[i],points[(i+1)%points.length],points[j],points[(j+1)%points.length]));
 return {shape:'polygon',points};
}
export function presentation(value){exact(value,['mask','fontSize'],['mask']);const result={mask:validateCanvasMask(value.mask)};if(value.fontSize!==undefined){check(typeof value.fontSize==='number'&&Number.isFinite(value.fontSize)&&value.fontSize>=8&&value.fontSize<=128);result.fontSize=value.fontSize;}return result;}
export function visibility(value){
 exact(value,['audience','accountIds','communityIds']);check(['private','public','restricted'].includes(value.audience));
 const ids=list=>{check(Array.isArray(list)&&list.length<=OBJECT_LIMITS.recipients);const result=list.map(identity);check(new Set(result).size===result.length);return result.sort();};
 const accountIds=ids(value.accountIds),communityIds=ids(value.communityIds);check(value.audience==='restricted'?accountIds.length+communityIds.length>0:accountIds.length+communityIds.length===0);
 return {audience:value.audience,accountIds,communityIds};
}
export function hidden(value){exact(value,['owner','others']);check(typeof value.owner==='boolean'&&typeof value.others==='boolean');return {owner:value.owner,others:value.others};}
export function protection(value){exact(value,['kind']);check(['none','password'].includes(value.kind));return {kind:value.kind};}
export function jsonProjection(value){
 const visit=(item,depth)=>{check(depth<=20,'canvas_projection_invalid',502);if(item===null||typeof item==='boolean'||typeof item==='string')return item;if(typeof item==='number'){check(Number.isFinite(item),'canvas_projection_invalid',502);return item;}
  check(item&&typeof item==='object','canvas_projection_invalid',502);if(Array.isArray(item)){check(item.length<=4096,'canvas_projection_invalid',502);return item.map(child=>visit(child,depth+1));}
  const keys=Reflect.ownKeys(item);check(keys.length<=256,'canvas_projection_invalid',502);exact(item,keys);return Object.fromEntries(keys.map(key=>[key,visit(item[key],depth+1)]));};
 const result=visit(value,0);check(Buffer.byteLength(JSON.stringify(result),'utf8')<=OBJECT_LIMITS.projectionBytes,'canvas_projection_too_large',502);return result;
}

/** Host content reads count binary bytes directly, never JSON-encode media buffers. */
export function boundedContent(value){
 let bytes=0;const add=size=>{bytes+=size;check(bytes<=OBJECT_LIMITS.contentBytes,'canvas_content_too_large',502);};
 const visit=(item,depth)=>{check(depth<=20,'canvas_content_invalid',502);
  if(item instanceof Uint8Array){add(item.byteLength);return Buffer.from(item);}
  if(item===null||typeof item==='boolean'){add(5);return item;}if(typeof item==='string'){add(Buffer.byteLength(item,'utf8'));return item;}
  if(typeof item==='number'){check(Number.isFinite(item),'canvas_content_invalid',502);add(24);return item;}
  check(item&&typeof item==='object','canvas_content_invalid',502);if(Array.isArray(item)){check(item.length<=4096,'canvas_content_invalid',502);return item.map(child=>visit(child,depth+1));}
  const keys=Reflect.ownKeys(item);check(keys.length<=256,'canvas_content_invalid',502);exact(item,keys);return Object.fromEntries(keys.map(key=>{add(Buffer.byteLength(key,'utf8'));return [key,visit(item[key],depth+1)];}));};
 return visit(value,0);
}
