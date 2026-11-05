import {assertApps} from './protocol.mjs';
import {createWidgetSourceAuthority} from './widget-source-authority.mjs';

/** A known legacy canvas ref can use the current PUBLIC named alias without joining discovery. */
export function createPublicCanvasAuthority(options){
 const source=createWidgetSourceAuthority({...options,publicCanvasDefault:true});
 return function withPublicCanvasAuthority(request,callback){
  assertApps(request&&typeof request==='object'&&!Array.isArray(request)&&Object.keys(request).every(key=>['actor','appId'].includes(key))&&Object.hasOwn(request,'appId'),'invalid_arguments');
  return source(request,callback);
 };
}
