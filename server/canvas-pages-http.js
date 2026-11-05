import {readCanvasSourceContent} from './canvas-objects-host.js';

const hash=/^[a-f0-9]{64}$/u;
const fail=(code,status=400)=>{throw Object.assign(new Error(code),{code,status});};
const query=(req,allowed)=>{
  if(Object.keys(req.query).some(key=>!allowed.includes(key))||Object.values(req.query).some(value=>typeof value!=='string'))fail('canvas_page_query_invalid');
  return req.query;
};
const integer=(value,min,max)=>{
  if(typeof value!=='string'||!/^\d{1,8}$/u.test(value))fail('canvas_page_query_invalid');
  const number=Number(value);if(!Number.isSafeInteger(number)||number<min||number>max)fail('canvas_page_query_invalid');return number;
};

/** Public reads use current page membership and each Source ACL. Long waits
 * hold only RAM listeners, never a database transaction or an owner identity. */
export function attachCanvasPagesHttp(app,{pages,notes,materials,vault}){
  const waiters=new Set(),scopes=new Map();let closed=false,queued=false;
  const current=request=>{if(closed)fail('canvas_page_unavailable',503);return pages.read(request);};
  const content=(request,input)=>{
    if(closed)fail('canvas_page_unavailable',503);
    if(!['note','metadata','chunk','vault'].includes(input.part)||Object.keys(input).some(key=>!['part','index'].includes(key))
      ||(input.index!==undefined&&(!Number.isSafeInteger(input.index)||input.index<0||input.index>10000)))fail('canvas_content_part_invalid');
    return pages.withPageObjectRead(request,pageContext=>pages.withPageObjectAuthority(pageContext,
      objectContext=>readCanvasSourceContent({notes,materials,vault},objectContext,input)));
  };
  const wait=({pageId,actor,after,waitMs=15000,signal})=>{
    if(!hash.test(after??'')||!Number.isSafeInteger(waitMs)||waitMs<0||waitMs>15000)fail('canvas_page_query_invalid');
    if(waiters.size>=128)fail('canvas_page_wait_busy',429);
    const scope=actor?JSON.stringify([actor.accountId,actor.deviceId]):null;
    if(scope&&(scopes.get(scope)??0)>=4)fail('canvas_page_wait_busy',429);
    if(signal?.aborted)fail('canvas_page_cancelled',499);
    return new Promise((resolve,reject)=>{
      let done=false,timer=null;
      const cleanup=()=>{if(done)return;done=true;if(timer!==null)clearTimeout(timer);waiters.delete(check);signal?.removeEventListener('abort',cancel);
        if(scope){const count=(scopes.get(scope)??1)-1;if(count)scopes.set(scope,count);else scopes.delete(scope);}};
      const cancel=()=>{cleanup();reject(Object.assign(new Error('canvas_page_cancelled'),{code:'canvas_page_cancelled',status:499}));};
      const check=(timeout=false)=>{
        if(done)return;
        try{const value=current({pageId,...(actor?{actor}:{})}),changed=value.viewRevision!==after;
          if(!changed&&!timeout)return;cleanup();resolve({schema:'soty.canvas-page-update.v1',changed,viewRevision:value.viewRevision,view:changed?value:null});
        }catch(reason){cleanup();reject(reason);}
      };
      if(scope)scopes.set(scope,(scopes.get(scope)??0)+1);
      signal?.addEventListener('abort',cancel,{once:true});check(waitMs===0);
      if(!done){waiters.add(check);timer=setTimeout(()=>check(true),waitMs);timer.unref?.();}
    });
  };
  const headers=res=>res.set({'Cache-Control':'no-store','Referrer-Policy':'no-referrer','X-Content-Type-Options':'nosniff','X-Robots-Tag':'noindex, nofollow'});
  const error=(res,reason)=>{
    if(res.destroyed||res.headersSent)return;
    const status=[400,401,403,404,409,429,503].includes(reason?.status)?reason.status:503;
    const code=/^[a-z_]{1,100}$/u.test(reason?.code??'')?reason.code:'canvas_page_unavailable';headers(res);res.status(status).json({ok:false,code});
  };
  const read=req=>{
    if(closed)fail('canvas_page_unavailable',503);
    const args=query(req,['cursor','limit']);
    return current({pageId:req.params.pageId,...(args.cursor?{cursor:args.cursor}:{}),...(args.limit?{limit:integer(args.limit,1,32)}:{})});
  };
  app.get('/api/canvas/pages/:pageId',(req,res)=>{
    try{const value=read(req);headers(res);res.json(value);}catch(reason){error(res,reason);}
  });
  app.get('/api/canvas/pages/:pageId/objects/:objectId/content',(req,res)=>{
    try{
      if(closed)fail('canvas_page_unavailable',503);
      const args=query(req,['part','index']);if(!['note','metadata','chunk','vault'].includes(args.part))fail('canvas_content_part_invalid');
      const input={part:args.part,...(args.index===undefined?{}:{index:integer(args.index,0,10000)})};
      const value=content({pageId:req.params.pageId,objectId:req.params.objectId},input);
      headers(res);res.json(value);
    }catch(reason){error(res,reason);}
  });
  app.get('/api/canvas/pages/:pageId/updates',async(req,res)=>{
    const controller=new AbortController(),aborted=()=>controller.abort();
    try{
      const args=query(req,['after','waitMs']);if(!hash.test(args.after??''))fail('canvas_page_query_invalid');
      const waitMs=args.waitMs===undefined?15000:integer(args.waitMs,0,15000);
      req.on('aborted',aborted);res.on('close',aborted);
      const value=await wait({pageId:req.params.pageId,after:args.after,waitMs,signal:controller.signal});
      if(!res.destroyed){headers(res);res.json(value);}
    }catch(reason){if(!controller.signal.aborted)error(res,reason);}
    finally{req.off('aborted',aborted);res.off('close',aborted);}
  });
  return Object.freeze({
    stats(){return Object.freeze({waiting:waiters.size,scopes:scopes.size});},
    extension:Object.freeze({operations:new Set([...pages.operations,'canvas.pages.read','canvas.pages.content','canvas.pages.wait']),maxResultBytes:2*1024*1024,
      async executeAsync(request){
        if(pages.operations.has(request.op))return pages.execute(request);
        const {actor,op,signal}=request,args=request.args??{};
        if(!actor||args.expectedAccountId!==actor.accountId)fail('canvas_page_account_changed',401);
        const {expectedAccountId,...input}=args;
        const exact=keys=>{if(!input||Object.keys(input).some(key=>!keys.includes(key)))fail('canvas_page_arguments_invalid');};
        if(op==='canvas.pages.read'){exact(['pageId','cursor','limit']);return current({...input,actor});}
        if(op==='canvas.pages.content'){exact(['pageId','objectId','part','index']);const {part,index,...target}=input;
          return content({...target,actor},{part,...(index===undefined?{}:{index})});}
        if(op==='canvas.pages.wait'){exact(['pageId','after','waitMs']);return wait({...input,actor,signal});}
        fail('canvas_page_operation_unsupported');
      }}),
    invalidate(){if(closed||queued||!waiters.size)return;queued=true;queueMicrotask(()=>{queued=false;if(!closed)for(const check of [...waiters])check();});},
    close(){if(closed)return;closed=true;for(const check of [...waiters])check();},
  });
}
