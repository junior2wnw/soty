import {validateAnyFieldDocument,FIELD_LIMITS,fieldEntityKey} from '../../modules/field/contract.mjs';

const id=/^page_[a-f0-9]{32}$/u,hash=/^[a-f0-9]{64}$/u;
const fail=code=>{throw Object.assign(new Error(code),{code});};
const check=(value,code='canvas_page_response_invalid')=>{if(!value)fail(code);};
export const parseCanvasPageRoute=value=>typeof value==='string'?/^#space\/(page_[a-f0-9]{32})(?:\?view=public)?$/u.exec(value)?.[1]??null:null;
export function canvasPageLink(pageId,origin){check(id.test(pageId));const url=new URL('/',origin);url.hash='space/'+pageId;return url.href;}

/** A page is a projection of one current field, not another saved field. */
export function createCanvasPageClient({pageId,api,accountId,fetch:fetcher=globalThis.fetch,isCurrent=()=>true,signal}={}){
 check(id.test(pageId)&&typeof isCurrent==='function'&&(!api||typeof api.request==='function'));
 const active=callSignal=>check(!signal?.aborted&&!callSignal?.aborted&&isCurrent(),'canvas_page_cancelled');
 async function request(op,args={},callSignal){
  active(callSignal);let value;const combined=signal&&callSignal?AbortSignal.any([signal,callSignal]):callSignal??signal;
  if(api&&accountId)value=await api.request(op,{expectedAccountId:accountId,pageId,...args},combined?{signal:combined}:undefined);
  else{
   let suffix='';const params=new URLSearchParams();
   if(op==='canvas.pages.read'){if(args.cursor)params.set('cursor',args.cursor);if(args.limit)params.set('limit',String(args.limit));}
   else if(op==='canvas.pages.wait'){suffix='/updates';params.set('after',args.after);params.set('waitMs',String(args.waitMs??15000));}
   else if(op==='canvas.pages.content'){check(typeof args.objectId==='string'&&/^[A-Za-z0-9_-]{1,128}$/u.test(args.objectId));suffix='/objects/'+encodeURIComponent(args.objectId)+'/content';params.set('part',args.part);if(args.index!==undefined)params.set('index',String(args.index));}
   else fail('canvas_page_read_only');
   const response=await fetcher('/api/canvas/pages/'+pageId+suffix+(params.size?'?'+params:''),{credentials:'omit',cache:'no-store',signal:combined,headers:{accept:'application/json'}});
   value=await response.json();if(!response.ok)throw Object.assign(new Error(value?.code??'canvas_page_unavailable'),{code:value?.code??'canvas_page_unavailable',status:response.status});
  }
  active(callSignal);return value;
 }
 function chunk(value){
  check(value?.schema==='soty.canvas-page-view.v1'&&value.pageId===pageId&&hash.test(value.viewRevision)&&Array.isArray(value.items)&&value.items.length<=32
   &&(value.nextCursor===null||typeof value.nextCursor==='string'&&value.nextCursor.length<=768)&&['generic','profile'].includes(value.banner?.kind));
  const field=validateAnyFieldDocument(value.field);check(field.schema==='soty.field.v2'&&field.shortcuts.length===value.items.length);
  for(let index=0;index<value.items.length;index++){const item=value.items[index],shortcut=field.shortcuts[index];check(item?.shortcutId===shortcut.shortcutId&&fieldEntityKey(item.entity)===fieldEntityKey(shortcut.entity)
    &&['object','app'].includes(item.entity.kind)&&item.presentation&&item.protection&&item.view&&(item.canUnlock===undefined||typeof item.canUnlock==='boolean'));}
  return{...value,field};
 }
 async function load(first,{signal:callSignal}={}){
  for(let attempt=0;attempt<2;attempt++){
   try{
    active(callSignal);const initial=chunk(first??await request('canvas.pages.read',{limit:32},callSignal)),items=[],contexts=new Map(),shortcuts=[],seen=new Set(),cursors=new Set();let page=initial;
    for(let count=0;;count++){
     check(count<FIELD_LIMITS.shortcuts&&page.viewRevision===initial.viewRevision&&JSON.stringify(page.banner)===JSON.stringify(initial.banner),'canvas_page_changed');
     for(const context of page.field.contexts){const previous=contexts.get(context.contextId);check(!previous||JSON.stringify(previous)===JSON.stringify(context));contexts.set(context.contextId,context);}
     for(let index=0;index<page.items.length;index++){const item=page.items[index];check(!seen.has(item.shortcutId));seen.add(item.shortcutId);items.push(item);shortcuts.push(page.field.shortcuts[index]);}
     check(items.length<=FIELD_LIMITS.shortcuts);
     if(!page.nextCursor)break;check(!cursors.has(page.nextCursor));cursors.add(page.nextCursor);page=chunk(await request('canvas.pages.read',{cursor:page.nextCursor,limit:32},callSignal));
    }
    active(callSignal);return{...initial,items,field:validateAnyFieldDocument({schema:'soty.field.v2',contexts:[...contexts.values()],shortcuts}),nextCursor:null};
   }catch(error){if(attempt===0&&['canvas_page_cursor_stale','canvas_page_changed'].includes(error?.code)){first=undefined;continue;}throw error;}
  }
 }
 return Object.freeze({load,
  async wait(after,{signal:callSignal}={}){check(hash.test(after));const result=await request('canvas.pages.wait',{after,waitMs:15000},callSignal);check(result?.schema==='soty.canvas-page-update.v1'&&typeof result.changed==='boolean'&&hash.test(result.viewRevision)
    &&result.changed===(result.viewRevision!==after)&&(result.changed?result.view?.viewRevision===result.viewRevision:result.view===null));return result;},
  content(objectId,part,index,{signal:callSignal}={}){return request('canvas.pages.content',{objectId,part,...(index===undefined?{}:{index})},callSignal);},
 });
}
