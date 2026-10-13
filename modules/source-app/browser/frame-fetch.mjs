// Public frame locator only. Root still requires the corresponding HttpOnly
// cookie and current actor/target/context; the locator cannot grant access.
export function createSourceFrameFetch({fetch: fetcher = globalThis.fetch, href = globalThis.location?.href,
  shortSessionHandoff = false, view = globalThis.window, parentOrigin = view?.location?.ancestorOrigins?.[0]} = {}) {
  if(typeof fetcher!=='function')throw new TypeError('source_fetch_required');
  if(href===undefined)return fetcher;
  const page=new URL(href),values=page.searchParams.getAll('sotyFrame');
  if(values.length>1||values.length===1&&!/^[A-Za-z0-9_-]{43}$/u.test(values[0]))throw new TypeError('source_frame_invalid');
  // The browser's actual ancestor origin (or a trusted host option), never '*'.
  // A response only wakes a fresh server read; it conveys no Source authority.
  const canRefresh=shortSessionHandoff===true&&view?.parent&&view.parent!==view&&typeof view.addEventListener==='function'
    &&typeof parentOrigin==='string'&&new URL(parentOrigin).origin===parentOrigin;
  if(!values.length&&!canRefresh)return fetcher;
  const frame=values[0];
  let flight=null;
  const refresh=()=>{
    if(flight)return flight;
    const requestId=crypto.randomUUID().replaceAll('-','');
    flight=new Promise(resolve=>{
      let done=false;
      const finish=ready=>{if(done)return;done=true;view.clearTimeout(timeout);view.removeEventListener('message',receive);resolve(ready);};
      const receive=event=>{const data=event.data;
        if(event.source!==view.parent||event.origin!==parentOrigin||!data||data.schema!=='soty.source-frame-refreshed.v1'
          ||data.requestId!==requestId||typeof data.ready!=='boolean'||Object.keys(data).length!==3)return;
        finish(data.ready);};
      const timeout=view.setTimeout(()=>finish(false),15000);view.addEventListener('message',receive);
      view.parent.postMessage({schema:'soty.source-frame-refresh.v1',requestId},parentOrigin);
    }).finally(()=>{flight=null;});return flight;
  };
  return (input,init)=>{
    const url=new URL(typeof input==='string'||input instanceof URL?input:input.url,page);
    if(url.origin!==page.origin||!url.pathname.startsWith('/api/embed/'))return fetcher(input,init);
    const headers=new Headers(init?.headers??(input instanceof Request?input.headers:undefined));
    if(frame&&headers.has('x-soty-frame')&&headers.get('x-soty-frame')!==frame)throw new TypeError('source_frame_invalid');
    if(frame)headers.set('x-soty-frame',frame);
    const options={...init,headers},method=(init?.method??(input instanceof Request?input.method:'GET')).toUpperCase();
    const safe=method==='GET'&&['/api/embed/session-status','/api/embed/context'].includes(url.pathname)
      ||method==='POST'&&['/api/embed/session-continue','/api/embed/query','/api/embed/receipt'].includes(url.pathname);
    const retryInput=canRefresh&&safe&&input instanceof Request?input.clone():input;
    const result=fetcher(input,options);
    if(!canRefresh||!safe)return result;
    return Promise.resolve(result).then(async response=>{
      let needsRefresh=response.status===401||response.status===403;
      if(url.pathname==='/api/embed/session-status'&&response.ok){
        try{needsRefresh=(await response.clone().json()).ready===false;}catch{}
      }
      if(!needsRefresh)return response;
      const signal=init?.signal??(input instanceof Request?input.signal:undefined);signal?.throwIfAborted();
      if(!await refresh())return response;
      signal?.throwIfAborted();
      // Exactly one retry of a read/continuation. Invoke/feedback writes and
      // network-unknown outcomes never enter this path or move their intents.
      return fetcher(retryInput,options);
    });
  };
}
