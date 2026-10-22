import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import path from 'node:path';
import {listAllContainerMounts} from './gpg-container-inventory.mjs';
const id=n=>n.toString(16).padStart(64,'0');
const record=n=>({Id:id(n),Mounts:[],Command:'must be discarded',Names:['private-name-must-not-be-retained'],Labels:{private:'discard'}});
function paged(records,maxPageSize=128){const calls=[];return {calls,request:async(method,query,body,raw,maxBytes)=>{
 calls.push({method,query,body,raw,maxBytes});const url=new URL(query,'http://localhost');
 const limit=Number(url.searchParams.get('limit'));
 assert.equal(method,'GET');assert.equal(url.pathname,'/containers/json');assert.equal(url.searchParams.get('all'),'1');assert.ok([1,2,4,8,16,32,64,128].includes(limit));assert.equal(body,undefined);assert.equal(raw,false);assert.equal(maxBytes,4*1024*1024);
 assert.deepEqual([...url.searchParams.keys()],['all','limit',...(url.searchParams.has('filters')?['filters']:[])]);
 const filters=url.searchParams.has('filters')?JSON.parse(url.searchParams.get('filters')):undefined;
 let start=0;if(filters){assert.deepEqual(Object.keys(filters),['before']);assert.equal(filters.before.length,1);assert.match(filters.before[0],/^[a-f0-9]{64}$/);const at=records.findIndex(x=>x.Id===filters.before[0]);assert.ok(at>=0);start=at+1;}
 const page=records.slice(start,start+limit);
 if(limit>maxPageSize||Buffer.byteLength(JSON.stringify(page))>maxBytes)throw Object.assign(Error('engine_response_limit'),{code:'engine_response_limit'});
 return page;
}};}
test('all pages complete, exact before cursor, page byte cap and only Id+Mounts retained',async()=>{
 const records=Array.from({length:257},(_,i)=>record(i+1));records[200].Mounts=[{Type:'bind',Source:'/synthetic/alias',Destination:'/data',RW:false,Mode:'ro'}];
 const engine=paged(records),all=await listAllContainerMounts(engine);assert.equal(engine.calls.length,3);assert.deepEqual(all.map(x=>x.Id),records.map(x=>x.Id));
 for(const item of all)assert.deepEqual(Object.keys(item),['Id','Mounts']);assert.deepEqual(all[200].Mounts,records[200].Mounts);
 assert.deepEqual(JSON.parse(new URL(engine.calls[1].query,'http://localhost').searchParams.get('filters')),{before:[records[127].Id]});
 assert.equal(JSON.stringify(all).includes('must be discarded'),false);assert.equal(JSON.stringify(all).includes('private-name'),false);
});
test('empty first page completes and a full final page needs the next empty page',async()=>{
 const empty=paged([]);assert.deepEqual(await listAllContainerMounts(empty),[]);assert.equal(empty.calls.length,1);
 const full=paged(Array.from({length:128},(_,i)=>record(i+1)));assert.equal((await listAllContainerMounts(full)).length,128);assert.equal(full.calls.length,2);
});
test('missing, null and incomplete Mounts preserve existing caller inspection fallback',async()=>{
 const engine=paged([{Id:id(1)},{Id:id(2),Mounts:null},{Id:id(3),Mounts:[{Type:'volume',Name:'synthetic'}]}]);
 const rows=await listAllContainerMounts(engine);assert.equal(Object.hasOwn(rows[0],'Mounts'),false);assert.equal(rows[1].Mounts,null);assert.deepEqual(rows[2].Mounts,[{Type:'volume',Name:'synthetic'}]);
});
test('ignored before cursor fails closed rather than returning a partial inventory',async()=>{
 let calls=0;const rows=Array.from({length:128},(_,i)=>record(i+1));await assert.rejects(listAllContainerMounts({request:async()=>{calls++;return rows;}}),{code:'docker_inventory_progress'});assert.equal(calls,2);
});
test('duplicate IDs within a page and on a later page are rejected',async()=>{
 await assert.rejects(listAllContainerMounts({request:async()=>[record(1),record(1)]}),{code:'docker_inventory_progress'});
 let calls=0;await assert.rejects(listAllContainerMounts({request:async()=>++calls===1?Array.from({length:128},(_,i)=>record(i+1)):[record(1)]}),{code:'docker_inventory_progress'});
});
const invalidPages=[null,{},[null],[[]],[{Id:'short'}],[{Id:[id(1)]}],[{Id:id(1),Mounts:'wrong'}],[{Id:id(1),Mounts:[null]}],[{Id:id(1),Mounts:[{Type:'bind',Source:17}]}],[{Id:id(1),Mounts:[{Type:'volume',RW:'false'}]}],Array.from({length:129},(_,i)=>record(i+1))];
for(const [index,page]of invalidPages.entries())test('strict container-list shape refuses malformed page '+index,async()=>{
 await assert.rejects(listAllContainerMounts({request:async()=>page}),{code:'docker_inventory_invalid'});
});
test('per-page lower-layer response limit errors cannot become successful partial results',async()=>{
 let calls=0;await assert.rejects(listAllContainerMounts({request:async()=>{if(++calls===1)return Array.from({length:128},(_,i)=>record(i+1));throw Object.assign(Error('engine_response_limit'),{code:'engine_response_limit'});}}),{code:'engine_response_limit'});assert.equal(calls,9);
});
test('complete inventory bound rejects excess containers without truncation',async()=>{
 const engine=paged(Array.from({length:8193},(_,i)=>record(i+1)));await assert.rejects(listAllContainerMounts(engine),{code:'docker_inventory_limit'});assert.equal(engine.calls.length,65);
});

test('1400 Docker-shaped summaries retain the complete inventory in 11 GETs',async()=>{
 const records=Array.from({length:1400},(_,i)=>({...record(i+1),Image:'synthetic:current',ImageID:'sha256:'+id(9),Created:1700000000,State:'exited',Status:'Exited (0)',Ports:[],HostConfig:{NetworkMode:'none'},NetworkSettings:{Networks:{}}}));
 records[1399].Mounts=[{Type:'bind',Source:'/synthetic/late-alias',Destination:'/data',RW:false}];
 const engine=paged(records),all=await listAllContainerMounts(engine);
 assert.deepEqual(all,records.map(({Id,Mounts})=>({Id,Mounts})));assert.equal(engine.calls.length,11);assert.equal(new URL(engine.calls[0].query,'http://localhost').searchParams.get('limit'),'128');
});

for(const prefix of [0,128])test('real 4MiB response overflow halves pages without advancing the cursor: prefix '+prefix,async()=>{
 const records=Array.from({length:prefix+18},(_,i)=>({...record(i+1),...(i>=prefix?{Labels:{synthetic:'x'.repeat(300000)}}:{})}));
 const engine=paged(records),all=await listAllContainerMounts(engine);
 assert.deepEqual(all,records.map(({Id,Mounts})=>({Id,Mounts})));
 const urls=engine.calls.map(c=>new URL(c.query,'http://localhost'));
 assert.deepEqual(urls.map(u=>Number(u.searchParams.get('limit'))),[...(prefix?[128]:[]),128,64,32,16,8,8,8]);
 const expected=prefix?JSON.stringify({before:[records[prefix-1].Id]}):null;
 for(const url of urls.slice(prefix?1:0,prefix?6:5))assert.equal(url.searchParams.get('filters'),expected);
});

test('fallback reaches page size one with the same 4MiB cap and finishes all pages',async()=>{
 const records=Array.from({length:2},(_,i)=>({...record(i+1),Command:'x'.repeat(2200000)})),engine=paged(records);
 assert.deepEqual(await listAllContainerMounts(engine),records.map(({Id,Mounts})=>({Id,Mounts})));
 assert.deepEqual(engine.calls.map(c=>Number(new URL(c.query,'http://localhost').searchParams.get('limit'))),[128,64,32,16,8,4,2,1,1,1]);
});

test('fallback does not consume successful-page budget at the existing 8192-container edge',async()=>{
 const records=Array.from({length:8192},(_,i)=>record(i+1)),engine=paged(records,8);
 assert.deepEqual(await listAllContainerMounts(engine),records.map(({Id,Mounts})=>({Id,Mounts})));
 assert.equal(engine.calls.length,1029,'1025 successful pages plus four bounded size reductions');
});

test('successful-page cap is still 1025 and all GET attempts are bounded by 1032',async()=>{
 const engine=paged(Array.from({length:1100},(_,i)=>record(i+1)),1);
 await assert.rejects(listAllContainerMounts(engine),{code:'docker_inventory_limit'});assert.equal(engine.calls.length,1032);
});

test('a page exceeding the reduced size is refused without returning partial inventory',async()=>{
 let calls=0;await assert.rejects(listAllContainerMounts({request:async()=>{if(++calls===1)throw Object.assign(Error('too large'),{code:'engine_response_limit'});return Array.from({length:65},(_,i)=>record(i+1));}}),{code:'docker_inventory_invalid'});assert.equal(calls,2);
});

for(const code of ['engine_http_500','engine_response_ambiguous','engine_invalid_json',undefined])test('only exact engine_response_limit permits retry: '+code,async()=>{
 const error=Object.assign(Error('engine_response_limit'),{code});let calls=0;
 await assert.rejects(listAllContainerMounts({request:async()=>{calls++;throw error;}}),e=>e===error);assert.equal(calls,1);
});

test('60-second inventory deadline also applies to fallback attempts',async t=>{
 let now=0;t.mock.method(performance,'now',()=>now);
 for(const overflow of [false,true]){now=0;let calls=0;await assert.rejects(listAllContainerMounts({request:async()=>{calls++;now=60001;if(overflow)throw Object.assign(Error('too large'),{code:'engine_response_limit'});return [];}}),{code:'docker_inventory_limit'});assert.equal(calls,1);}
});
test('mount count and retained-byte bounds reject oversized inventory',async()=>{
 await assert.rejects(listAllContainerMounts(paged([{Id:id(1),Mounts:Array.from({length:65537},()=>({Type:'tmpfs'}))}])),{code:'docker_inventory_limit'});
 const source='/synthetic/'+ 'x'.repeat(480000),records=Array.from({length:40},(_,i)=>({Id:id(i+1),Mounts:[{Type:'bind',Source:source,Destination:'/data'}]}));
 assert.ok(Buffer.byteLength(JSON.stringify(records.slice(0,8)))<4*1024*1024,'each fixture page respects actual lower-layer 4MiB cap');
 await assert.rejects(listAllContainerMounts(paged(records)),{code:'docker_inventory_limit'});
});
// Source port executes actual restore function and actual paginated helper; only the completed
// authentication boundary is supplied so the test reaches target ownership. No GPG or Docker runs.
const source=await readFile(new URL('./gpg-release-backup.mjs',import.meta.url),'utf8');
function span(start,end){const a=source.indexOf(start),b=source.indexOf(end,a);assert.ok(a>=0&&b>a);return source.slice(a,b);}
function restorePort(actions){const constants=span('const ID=','\nfunction processDocker('),fences=span('async function absentContainer(','\nasync function stopOwned('),body=span('export async function restoreIsolated(','\nasync function stdinKey(').replace(/^export /,'');
 const bad=code=>{throw Object.assign(Error(code),{code});};return Function('path','bad','limits','verifyTar','randomBytes','listAllContainerMounts',constants+'\n'+fences+'\n'+body+'\nreturn restoreIsolated;')(path,bad,()=>({}),async()=>{actions.push('AUTH_COMPLETE');return {};},()=>Buffer.alloc(16,4),listAllContainerMounts);
}
for(const mode of ['named-volume','bind-alias','inspect-fallback'])test('actual restore rejects foreign consumer on later inventory page: '+mode,async()=>{
 const actions=[],operationId='2'.repeat(32),image='sha256:'+'1'.repeat(64),volumeName='soty-isolated-restore-'+operationId,foreignId=id(999),mountpoint='/synthetic/owned-restore';
 const mount=mode==='named-volume'?{Type:'volume',Name:volumeName,Source:mountpoint,Destination:'/data',RW:false}:{Type:'bind',Source:mountpoint+'/alias',Destination:'/data',RW:false};
 const records=Array.from({length:128},(_,i)=>record(i+1));records.push({Id:foreignId,...(mode==='inspect-fallback'?{}:{Mounts:[mount]})});const inventory=paged(records);
 let volume,creates=0,foreignInspects=0;const engine={image:async value=>({Id:value}),inspect:async value=>{if(value===foreignId){foreignInspects++;return {Id:value,Mounts:[mount]};}throw Object.assign(Error('absent'),{code:'engine_http_404'});},
 request:async(method,url,body,raw,maxBytes)=>{
  if(method==='GET'&&url.startsWith('/containers/json?'))return inventory.request(method,url,body,raw,maxBytes);
  if(method==='POST'&&url==='/volumes/create'){actions.push('CREATE_VOLUME');volume={...body,Scope:'local',Mountpoint:mountpoint};return volume;}
  if(method==='GET'&&url.startsWith('/volumes/')){if(!volume)throw Object.assign(Error('absent'),{code:'engine_http_404'});return volume;}
  throw Error('unexpected synthetic port');
 },create:async()=>{creates++;throw Error('TEST_STOP_BEFORE_ANY_PROCESS');}};
 await assert.rejects(restorePort(actions)({file:'/synthetic/authenticated.gpg',expectedSha256:'7'.repeat(64),image,operationId,key:Buffer.from('A'.repeat(64))},engine),{code:'restore_incomplete_quarantined'});
 assert.equal(inventory.calls.length,2);assert.equal(creates,0,'late foreign consumer must block extraction CREATE');assert.deepEqual(actions,['AUTH_COMPLETE','CREATE_VOLUME']);assert.equal(foreignInspects,mode==='inspect-fallback'?1:0);
});
