// Docker Engine v1.45 all-container inventory. No volume/name/label filters: bind aliases remain visible.
const PAGE_SIZE=8,PAGE_BYTES=4*1024*1024,MAX_CONTAINERS=8192,MAX_PAGES=1025;
const MAX_MOUNTS=65536,MAX_RETAINED_BYTES=16*1024*1024,MAX_MS=60000;
const ID=/^[a-f0-9]{64}$/;
const fail=code=>{throw Object.assign(new Error(code),{code});};
/** Retains only Id and Mounts; incomplete mount summaries are inspected by the caller.
 * Every page uses the existing 4MiB request limit. Unknown/partial inventory is never returned. */
export async function listAllContainerMounts(engine){
 const seen=new Set(),result=[];let cursor,mounts=0,bytes=0;
 const started=performance.now();
 for(let number=0;number<MAX_PAGES;number++){
  if(performance.now()-started>MAX_MS)fail('docker_inventory_limit');
  const query='/containers/json?all=1&limit='+PAGE_SIZE+(cursor?'&filters='+encodeURIComponent(JSON.stringify({before:[cursor]})):'');
  const page=await engine.request('GET',query,undefined,false,PAGE_BYTES);
  if(performance.now()-started>MAX_MS)fail('docker_inventory_limit');
  if(!Array.isArray(page)||page.length>PAGE_SIZE)fail('docker_inventory_invalid');
  if(result.length+page.length>MAX_CONTAINERS)fail('docker_inventory_limit');
  for(const item of page){
   if(!item||typeof item!=='object'||Array.isArray(item)||typeof item.Id!=='string'||!ID.test(item.Id))fail('docker_inventory_invalid');
   if(seen.has(item.Id))fail('docker_inventory_progress');seen.add(item.Id);
   const retained={Id:item.Id};
   if(Object.hasOwn(item,'Mounts')){
    if(item.Mounts!=null&&!Array.isArray(item.Mounts))fail('docker_inventory_invalid');
    if(Array.isArray(item.Mounts)){
     mounts+=item.Mounts.length;if(mounts>MAX_MOUNTS)fail('docker_inventory_limit');
     for(const mount of item.Mounts){
      if(!mount||typeof mount!=='object'||Array.isArray(mount))fail('docker_inventory_invalid');
      for(const field of ['Type','Name','Source','Destination','Driver','Mode','Propagation'])if(mount[field]!==undefined&&typeof mount[field]!=='string')fail('docker_inventory_invalid');
      if(mount.RW!==undefined&&typeof mount.RW!=='boolean')fail('docker_inventory_invalid');
     }
    }
    retained.Mounts=item.Mounts;
   }
   let encoded;try{encoded=JSON.stringify(retained);}catch{fail('docker_inventory_invalid');}
   bytes+=Buffer.byteLength(encoded,'utf8');if(bytes>MAX_RETAINED_BYTES)fail('docker_inventory_limit');
   result.push(retained);
  }
  if(page.length<PAGE_SIZE)return result;
  const next=page[page.length-1].Id;if(next===cursor)fail('docker_inventory_progress');cursor=next;
 }
 fail('docker_inventory_limit');
}
