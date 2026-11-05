import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm,readFile,readdir} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {createMonitoringService} from '../server/index.mjs';
import {inspectMonitoringStorage,monitoringSchemaFingerprint} from '../server/schema.mjs';

const a=Object.freeze({accountId:'account-a',deviceId:'device-a'}),b=Object.freeze({accountId:'account-b',deviceId:'device-b'}),viewer=Object.freeze({accountId:'viewer-a',deviceId:'device-view'});
const appA={kind:'app',appId:'app-'+'a'.repeat(32)},appB={kind:'app',appId:'app-'+'b'.repeat(32)},platform={kind:'platform'};
const definitions=[{id:'http.requests',kind:'counter',unit:'count'},{id:'http.duration',kind:'distribution',unit:'ms'},{id:'process.cpu',kind:'gauge',unit:'percent'}];
async function fixture(t,extra={}){
 const root=await mkdtemp(join(tmpdir(),'soty-monitoring-module-')),file=join(root,'monitoring.sqlite');let time=60_000_000,live=true,clockHook=()=>{};const known=new Set([a,b,viewer]);
 const options={databasePath:file,projectId:'soty',mode:'writer',definitions,currentActor:actor=>live&&known.has(actor),withAuthority({actor,scope,right},callback){assert.ok(known.has(actor),'host must reject unissued actor');const owner=scope.kind==='platform'?b:scope.appId===appA.appId?a:b;const permitted=actor===owner||actor===viewer&&scope.appId===appA.appId&&right==='read';if(!permitted)throw Object.assign(new Error('denied'),{code:'denied'});return callback();},clock:()=>{clockHook();return time;},...extra};
 let service=createMonitoringService(options);t.after(async()=>{service.close();await rm(root,{recursive:true,force:true});});
 const call=(op,args={},actor=a,scope=appA)=>service.execute({op,args:{expectedAccountId:actor.accountId,scope,...args},actor});
 return{file,root,options,get service(){return service;},call,setTime(value){time=value;},time:()=>time,setClockHook(fn){clockHook=fn;},setLive(value){live=value;},reopen(mode='writer',changes={}){service.close();service=createMonitoringService({...options,mode,...changes});return service;}};
}
test('actual SQLite: default-on scopes are independent, toggle CAS/idempotency survives reopen',async t=>{
 const f=await fixture(t);assert.equal(f.call('monitoring.describe').config.enabled,true);assert.equal(f.call('monitoring.describe',{},b,appB).config.enabled,true);
 f.service.observe({scope:appA,metricId:'http.requests',value:3});const command={requestId:'toggle-a',expectedRevision:0,enabled:false};const written=f.call('monitoring.config.put',command);assert.equal(written.config.revision,1);assert.equal(f.call('monitoring.config.put',command).replayed,true);
 assert.throws(()=>f.call('monitoring.config.put',{...command,enabled:true}),{code:'monitoring_request_conflict'});assert.throws(()=>f.call('monitoring.config.put',{...command,requestId:'stale'}),{code:'monitoring_revision_conflict'});
 assert.equal(f.service.observe({scope:appA,metricId:'http.requests',value:9}).recorded,false);assert.equal(f.service.observe({scope:appB,metricId:'http.requests',value:8}).recorded,true);
 f.reopen();assert.equal(f.call('monitoring.describe').config.enabled,false);assert.equal(f.call('monitoring.describe',{},b,appB).metrics[0].current,8);assert.equal(f.call('monitoring.describe').metrics[0].current,3);assert.equal(f.call('monitoring.config.put',command).replayed,true);
 const audit=f.call('monitoring.audit.list');assert.equal(audit.entries.length,1);assert.equal(audit.entries[0].operation,'collection.setEnabled');assert.equal(JSON.stringify(audit).includes(a.accountId),false);
});
test('read/configure/admin are separate and foreign/serialized/revoked actors cannot read data',async t=>{
 const f=await fixture(t);f.service.observe({scope:appA,metricId:'http.requests',value:7});const view=f.call('monitoring.describe',{},viewer);assert.equal(view.metrics[0].current,7);assert.deepEqual(view.permissions,{read:true,configure:false,admin:false});
 for(const[op,args]of[['monitoring.config.put',{requestId:'deny',expectedRevision:0,enabled:false}],['monitoring.audit.list',{}]])assert.throws(()=>f.call(op,args,viewer),{code:'monitoring_forbidden'});
 assert.throws(()=>f.call('monitoring.describe',{},b),{code:'monitoring_forbidden'});assert.throws(()=>f.call('monitoring.describe',{},a,platform),{code:'monitoring_forbidden'});assert.throws(()=>f.call('monitoring.describe',{},JSON.parse(JSON.stringify(a))),{code:'monitoring_forbidden'});f.setLive(false);assert.throws(()=>f.call('monitoring.describe'),{code:'monitoring_forbidden'});
});
test('integer counters/weighted means/current gauge and null gaps use real timestamps and units',async t=>{
 const f=await fixture(t),time=f.time();for(const value of[10,30])f.service.observe({scope:appA,metricId:'http.duration',value,observedAt:time});
 f.service.observe({scope:appA,metricId:'process.cpu',value:40,observedAt:time});f.service.observe({scope:appA,metricId:'process.cpu',value:10,observedAt:time-100});f.service.observe({scope:appA,metricId:'http.requests',value:0,observedAt:time});
 const result=f.call('monitoring.query',{metricIds:['http.duration','http.requests'],from:time-60000,to:time+60000,maxPoints:2});assert.deepEqual(result.series[0].points,[{at:time-60000,value:null,samples:0},{at:time,value:20,samples:2}]);assert.equal(result.series[0].unit,'ms');assert.equal(result.series[0].status,'partial');assert.deepEqual(result.series[1].points.map(p=>p.value),[null,0]);assert.equal(f.call('monitoring.describe').metrics[2].current,40);
 f.setTime(time+120000);assert.equal(f.call('monitoring.describe').metrics[0].status,'stale');
});
test('custom business metrics are explicit, immutable and cannot spoof host facts or leak payloads',async t=>{
 const f=await fixture(t);f.call('monitoring.metric.put',{requestId:'define-a',id:'custom.sales',kind:'counter',unit:'count'});
 const args={eventId:'business-event',metricId:'custom.sales',value:4,observedAt:f.time()};assert.equal(f.call('monitoring.metric.record',args).recorded,true);assert.equal(f.call('monitoring.metric.record',args).replayed,true);
 assert.throws(()=>f.call('monitoring.metric.record',{...args,value:5}),{code:'monitoring_request_conflict'});assert.throws(()=>f.call('monitoring.metric.put',{requestId:'different-unit',id:'custom.sales',kind:'gauge',unit:'ms'}),{code:'monitoring_metric_conflict'});
 assert.throws(()=>f.call('monitoring.metric.record',{...args,eventId:'spoof',metricId:'http.requests'}),{code:'monitoring_host_metric_forbidden'});assert.throws(()=>f.call('monitoring.metric.record',{...args,eventId:'body',body:'PRIVATE_SENTINEL'}),{code:'monitoring_invalid_input'});
 assert.throws(()=>f.call('monitoring.metric.record',{...args,eventId:'bad',value:Infinity}),{code:'monitoring_invalid_input'});assert.throws(()=>f.call('monitoring.metric.record',{...args,eventId:'bad',value:0.5}),{code:'monitoring_invalid_input'});
 assert.throws(()=>f.call('monitoring.query',{metricIds:['custom.sales'],from:f.time()-60000,to:f.time()+60000},b,appB),{code:'monitoring_metric_unavailable'});
 const db=new DatabaseSync(f.file,{readOnly:true});try{assert.equal(JSON.stringify(db.prepare('SELECT * FROM monitoring_receipts').all()).includes('PRIVATE_SENTINEL'),false);}finally{db.close();}
});
test('reader-only missing/installed startup cannot create DB or mutate existing module history',async t=>{
 const f=await fixture(t);f.call('monitoring.config.put',{requestId:'before',expectedRevision:0,enabled:false});f.reopen('reader');const before=await readFile(f.file);assert.equal(f.call('monitoring.describe').storage.mode,'reader');assert.equal(f.call('monitoring.describe').permissions.configure,false);assert.throws(()=>f.call('monitoring.config.put',{requestId:'reader-write',expectedRevision:1,enabled:true}),{code:'monitoring_storage_read_only'});assert.equal(f.service.observe({scope:appA,metricId:'http.requests',value:1}).reason,'reader-only');assert.deepEqual(await readFile(f.file),before);
 const nested=join(f.root,'missing','monitoring.sqlite'),entries=await readdir(f.root);const reader=createMonitoringService({...f.options,databasePath:nested,mode:'reader'});try{const description=reader.execute({op:'monitoring.describe',actor:a,args:{expectedAccountId:a.accountId,scope:appA}});assert.equal(description.storage.installed,false);assert.equal(description.metrics[0].current,null);assert.equal(description.metrics[0].status,'unavailable');assert.deepEqual(await readdir(f.root),entries);}finally{reader.close();}
});
test('exact schema rejects unknown version/extra table and reader validates a real independently opened file',async t=>{
 const f=await fixture(t);f.service.close();const db=new DatabaseSync(f.file);assert.equal(inspectMonitoringStorage(db,'soty').schemaVersion,1);assert.match(monitoringSchemaFingerprint(),/^[a-f0-9]{64}$/);db.exec('PRAGMA user_version=99');assert.throws(()=>createMonitoringService({...f.options,mode:'reader'}),{code:'monitoring_storage_format_unsupported'});db.exec('PRAGMA user_version=1;CREATE TABLE arbitrary_extra(value TEXT)');assert.throws(()=>createMonitoringService({...f.options,mode:'writer'}),{code:'monitoring_storage_format_unsupported'});db.close();
});
test('bounded cardinality/retention produces visible missing history, not invented zeros',async t=>{
 const f=await fixture(t,{limits:{maxRows:2,maxScopes:2,maxMetrics:1,retentionMs:60000}}),time=f.time();f.service.observe({scope:appA,metricId:'http.requests',value:1,observedAt:time-60000});f.service.observe({scope:appB,metricId:'http.requests',value:2});f.service.observe({scope:appA,metricId:'http.duration',value:10});
 assert.equal(f.call('monitoring.query',{metricIds:['http.requests'],from:time-60000,to:time+60000}).series[0].points[0].value,null);
 f.call('monitoring.metric.put',{requestId:'one',id:'custom.one',kind:'gauge',unit:'count'});assert.throws(()=>f.call('monitoring.metric.put',{requestId:'two',id:'custom.two',kind:'gauge',unit:'count'}),{code:'monitoring_metric_capacity'});
 f.setTime(time+120000);f.service.observe({scope:appA,metricId:'http.requests',value:0});assert.throws(()=>f.service.observe({scope:appA,metricId:'http.requests',value:2,observedAt:time}),{code:'monitoring_observation_expired'});
 assert.equal(f.call('monitoring.query',{metricIds:['http.requests'],from:time,to:time+180000}).series[0].points[0].value,null);
});
test('identity loss before COMMIT leaves no configuration/audit/receipt success',async t=>{
 const f=await fixture(t);f.setClockHook(()=>f.setLive(false));assert.throws(()=>f.call('monitoring.config.put',{requestId:'lost-identity',expectedRevision:0,enabled:false}),{code:'monitoring_forbidden'});f.setClockHook(()=>{});f.setLive(true);assert.equal(f.call('monitoring.describe').config.revision,0);assert.equal(f.call('monitoring.audit.list').entries.length,0);assert.equal(f.call('monitoring.config.put',{requestId:'lost-identity',expectedRevision:0,enabled:false}).config.revision,1);
});
test('real SQLITE_FULL cannot ACK a changed config; exact original command retries after reopen',async t=>{
 const f=await fixture(t,{limits:{maxBytes:65536}});let accepted=0,failed;
 for(let n=0;n<160;n++){const args={requestId:'capacity-'+n,expectedRevision:accepted,enabled:n%2===0};try{f.call('monitoring.config.put',args);accepted++;}catch(error){failed={error,args};break;}}
 assert.ok(failed,'real SQLite cap must be reached');assert.equal(f.call('monitoring.describe').config.revision,accepted);f.reopen('writer',{limits:{maxBytes:1024*1024}});assert.equal(f.call('monitoring.config.put',failed.args).config.revision,accepted+1);assert.equal(f.call('monitoring.config.put',failed.args).replayed,true);
});
test('uneven host batches exactly match scalar SQLite rows/series and weighted distribution, not mean-of-means',async t=>{
 const f=await fixture(t),now=f.time(),observations=[],raw=new Map();let seed=7;const random=()=>{seed=(seed*1664525+1013904223)>>>0;return seed;};
 for(const[metricId,kind]of[['http.requests','counter'],['http.duration','distribution'],['process.cpu','gauge']]){
  for(const[bucket,count]of[31,1,7,19].entries()){
   const values=Array.from({length:count},(_,index)=>({value:kind==='distribution'?(bucket===1?1000:random()%100/4):kind==='counter'?random()%3:random()%101,at:now-(3-bucket)*60000+index}));
   raw.set(metricId,[...(raw.get(metricId)??[]),...values]);for(const value of values)f.service.observe({scope:appA,metricId,value:value.value,observedAt:value.at});
   // Split the same minute into deliberately uneven groups, then deliver an
   // older group after a newer one: gauge timestamp wins over arrival order.
   const groups=[values.slice(0,1),values.slice(1)].filter(values=>values.length).reverse();
   for(const group of groups){const last=group.at(-1);observations.push({scope:appB,metricId,samples:group.length,sum:group.reduce((sum,item)=>sum+item.value,0),min:Math.min(...group.map(item=>item.value)),max:Math.max(...group.map(item=>item.value)),last:last.value,observedAt:last.at});}
  }
 }
 assert.equal(f.service.observeBatch({observations}).recorded,observations.length);
 const args={metricIds:['http.requests','http.duration','process.cpu'],from:now-180000,to:now+60000,maxPoints:1};
 const actual=f.call('monitoring.query',args).series;assert.deepEqual(actual,f.call('monitoring.query',args,b,appB).series);
 // Independent raw-sample oracle: comparing two paths through the same query
 // alone could let both agree on the same incorrect mean-of-means.
 const durations=raw.get('http.duration');assert.equal(actual[1].points[0].value,durations.reduce((sum,value)=>sum+value.value,0)/durations.length);assert.equal(actual[1].points[0].samples,durations.length);
 assert.equal(actual[0].points[0].value,raw.get('http.requests').reduce((sum,value)=>sum+value.value,0));assert.equal(actual[2].points[0].value,raw.get('process.cpu').at(-1).value);
 const db=new DatabaseSync(f.file,{readOnly:true});try{const rows=key=>db.prepare('SELECT metric_id,bucket_at,samples,sum,min,max,last,last_at FROM monitoring_buckets WHERE scope_key=? ORDER BY metric_id,bucket_at').all('app:'+key);assert.deepEqual(rows(appA.appId),rows(appB.appId));}finally{db.close();}
});
test('batch validates every entry/units/limits and atomically rolls back earlier scope admission',async t=>{
 const f=await fixture(t,{limits:{maxScopes:1}}),entry={scope:appA,metricId:'http.requests',samples:2,sum:3,min:1,max:2,last:2,observedAt:f.time()};
 assert.throws(()=>f.service.observeBatch({observations:[entry,{...entry,sum:100}]}),{code:'monitoring_aggregate_inconsistent'});
 assert.throws(()=>f.service.observeBatch({observations:[entry,{...entry,scope:appB}]}),{code:'monitoring_scope_capacity'});
 assert.equal(f.call('monitoring.describe').metrics[0].current,null,'first entry must roll back with second');
 for(const bad of[{...entry,samples:0},{...entry,last:3},{...entry,sum:Infinity},{...entry,sum:1.5},{...entry,rawURL:'http://private'},{...entry,metricId:'custom.inject'}])assert.throws(()=>f.service.observeBatch({observations:[bad]}));
 assert.throws(()=>f.service.observeBatch({observations:Array(4097).fill(entry)}));assert.throws(()=>f.service.observeBatch({observations:[entry],actor:'spoof'}));
 f.call('monitoring.config.put',{requestId:'off-batch',expectedRevision:0,enabled:false});assert.deepEqual(f.service.observeBatch({observations:[entry]}),{observations:1,recorded:0,disabled:1});f.reopen('reader');assert.equal(f.service.observeBatch({observations:[entry]}).reason,'reader-only');
});
test('collector health is a bounded trusted optional snapshot, never raw exception data or false reader running',async t=>{
 let health={running:true,lastSuccessAt:null,lastFailureAt:null,failures:0,supportedMetricIds:['http.requests']};const f=await fixture(t,{collectorStatus:()=>health});assert.deepEqual(f.call('monitoring.describe').collector,health);
 health={...health,lastFailureAt:f.time(),failures:1};assert.equal(f.call('monitoring.describe').collector.failures,1);health={...health,error:'SECRET_EXCEPTION'};assert.throws(()=>f.call('monitoring.describe'),{code:'monitoring_invalid_input'});
 health={running:true,lastSuccessAt:null,lastFailureAt:null,failures:0,supportedMetricIds:['http.requests']};f.reopen('reader');assert.throws(()=>f.call('monitoring.describe'),{code:'monitoring_collector_invalid'});
});
