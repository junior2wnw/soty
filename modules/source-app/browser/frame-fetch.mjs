// Public frame locator only. Root still requires the corresponding HttpOnly
// cookie and current actor/target/context; the locator cannot grant access.
export function createSourceFrameFetch({fetch: fetcher = globalThis.fetch, href = globalThis.location?.href} = {}) {
  if(typeof fetcher!=='function')throw new TypeError('source_fetch_required');
  if(href===undefined)return fetcher;
  const page=new URL(href),values=page.searchParams.getAll('sotyFrame');
  if(values.length>1||values.length===1&&!/^[A-Za-z0-9_-]{43}$/u.test(values[0]))throw new TypeError('source_frame_invalid');
  if(!values.length)return fetcher;
  const frame=values[0];
  return (input,init)=>{
    const url=new URL(typeof input==='string'||input instanceof URL?input:input.url,page);
    if(url.origin!==page.origin||!url.pathname.startsWith('/api/embed/'))return fetcher(input,init);
    const headers=new Headers(init?.headers??(input instanceof Request?input.headers:undefined));
    if(headers.has('x-soty-frame')&&headers.get('x-soty-frame')!==frame)throw new TypeError('source_frame_invalid');
    headers.set('x-soty-frame',frame);
    return fetcher(input,{...init,headers});
  };
}
