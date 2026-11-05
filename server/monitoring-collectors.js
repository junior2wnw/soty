import {availableParallelism} from 'node:os';
import {HOST_METRICS} from '../modules/monitoring/definitions.mjs';
import {observation} from '../modules/monitoring/server/validation.mjs';

const PLATFORM=Object.freeze({kind:'platform'}),MINUTE=60000,MAX_ENTRIES=3072;
const accountId=/^acct_[A-Za-z0-9_-]{32}$/u,appId=/^app-[a-f0-9]{32}$/u;
const configuredError=()=>Object.assign(new Error('monitoring_host_configuration_invalid'),{code:'monitoring_host_configuration_invalid'});
export function readMonitoringConfiguration(env={},input={}){
  if(!input||typeof input!=='object'||Array.isArray(input)||Object.keys(input).some(key=>!['storageMode','platformPolicy'].includes(key)))throw configuredError();
  const storageMode=input.storageMode??env.SOTY_MONITORING_MODE??'reader';
  if(!['reader','writer'].includes(storageMode))throw configuredError();
  const explicit=input.platformPolicy;
  if(explicit&&(!Array.isArray(explicit)&&typeof explicit==='object')&&Object.keys(explicit).some(key=>!['readAccountIds','configureAccountIds','adminAccountIds'].includes(key)))throw configuredError();
  if(explicit!==undefined&&(!explicit||typeof explicit!=='object'||Array.isArray(explicit)))throw configuredError();
  const platformPolicy={};
  for(const right of ['read','configure','admin']){
    const name=right+'AccountIds',value=explicit?.[name]??String(env['SOTY_MONITORING_'+right.toUpperCase()+'_ACCOUNT_IDS']??'').split(',').map(x=>x.trim()).filter(Boolean);
    if(!Array.isArray(value)||value.length>32||value.some(id=>typeof id!=='string'||!accountId.test(id))||new Set(value).size!==value.length)throw configuredError();
    platformPolicy[name]=Object.freeze([...value]);
  }
  return Object.freeze({storageMode,platformPolicy:Object.freeze(platformPolicy)});
}

/** Bounded aggregate buffer; no URL, input, identity, header or request body is retained. */
export function createMonitoringCollectors({module,mode='reader',liveStatus=()=>null,callStatus=()=>null,pushStatus=()=>null,
  connectorStatus=()=>null,capabilityStatus=()=>null,clock=Date.now,monotonic=()=>performance.now(),runtime=process,parallelism=availableParallelism,intervalMs=5000}={}){
  if(!module||typeof module.observeBatch!=='function'||typeof module.collectionEnabled!=='function'||!['reader','writer'].includes(mode)||!Number.isSafeInteger(intervalMs)||intervalMs<1000||intervalMs>60000)throw configuredError();
  let timer=null,closed=false,started=false,flushing=false,failures=0,lastSuccessAt=null,lastFailureAt=null;
  let previousCpu=null,previousTime=null,previousPush=null,previousCapabilities=null;
  const pending=new Map(),appScopes=new WeakMap();
  const definitions=new Map(HOST_METRICS.map(value=>[value.id,value]));
  const supportedMetricIds=Object.freeze(['http.requests','http.errors','http.duration','process.rss','process.cpu','process.uptime',
    'live.scopes','live.waiters','calls.active','calls.sockets','calls.pending','push.dropped','push.failures','connector.online','capabilities.succeeded','capabilities.failed',
    'inference.requests','inference.errors','inference.duration','inference.firstoutput']);
  const failure=()=>{failures++;lastFailureAt=clock();};
  function observe(scope,metricId,value,observedAt=clock()){
    if(closed||mode!=='writer')return;
    try{if(!definitions.has(metricId)||!Number.isSafeInteger(observedAt))throw configuredError();observation(value,definitions.get(metricId));}catch{failure();return;}
    try{if(!module.collectionEnabled(scope))return;}catch{failure();return;}
    const key=(scope.kind==='platform'?'platform':scope.appId)+'\0'+metricId+'\0'+Math.floor(observedAt/MINUTE);
    const old=pending.get(key);
    if(!old&&pending.size>=MAX_ENTRIES){failure();return;}
    if(old){old.samples++;old.sum+=value;old.min=Math.min(old.min,value);old.max=Math.max(old.max,value);if(observedAt>=old.observedAt){old.last=value;old.observedAt=observedAt;}}
    else pending.set(key,{scope,metricId,samples:1,sum:value,min:value,max:value,last:value,observedAt});
  }
  function middleware(_req,res,next){
    if(mode!=='writer'||closed){next();return;}
    const startedAt=monotonic();let finished=false;
    const finish=()=>{if(finished)return;finished=true;res.off('finish',finish);res.off('close',finish);if(closed)return;
      const elapsed=monotonic()-startedAt,error=(!res.writableFinished||res.statusCode>=400)?1:0,at=clock(),target=appScopes.get(res);
      for(const scope of target?[PLATFORM,target]:[PLATFORM]){observe(scope,'http.requests',1,at);observe(scope,'http.errors',error,at);observe(scope,'http.duration',elapsed,at);}
      appScopes.delete(res);
    };
    res.once('finish',finish);res.once('close',finish);next();
  }
  function deltas(values,previous,mapping){
    if(!values||Object.values(values).some(n=>!Number.isSafeInteger(n)||n<0))return previous;
    if(previous)for(const [key,id]of Object.entries(mapping)){if(values[key]>=previous[key])observe(PLATFORM,id,values[key]-previous[key]);else failure();}
    return{...values};
  }
  function sample(){
    if(closed||mode!=='writer')return;
    const at=clock();
    try{const time=monotonic(),cpu=runtime.cpuUsage(),cpus=parallelism();
      observe(PLATFORM,'process.rss',runtime.memoryUsage().rss,at);observe(PLATFORM,'process.uptime',runtime.uptime(),at);
      if(previousCpu&&time>previousTime&&cpus>0)observe(PLATFORM,'process.cpu',100*((cpu.user-previousCpu.user)+(cpu.system-previousCpu.system))/1000/((time-previousTime)*cpus),at);
      previousCpu=cpu;previousTime=time;
    }catch{failure();}
    try{const live=liveStatus();if(live){observe(PLATFORM,'live.scopes',live.scopes,at);observe(PLATFORM,'live.waiters',live.waiting,at);}}catch{failure();}
    try{const calls=callStatus();if(calls){observe(PLATFORM,'calls.active',calls.calls,at);observe(PLATFORM,'calls.sockets',calls.ownedSockets,at);observe(PLATFORM,'calls.pending',calls.pendingSockets,at);}}catch{failure();}
    try{previousPush=deltas(pushStatus()?.counters,previousPush,{intakeDropped:'push.dropped',recipientFailures:'push.failures'});}catch{failure();}
    try{const connectors=connectorStatus();if(connectors)observe(PLATFORM,'connector.online',connectors.online,at);}catch{failure();}
    try{previousCapabilities=deltas(capabilityStatus(),previousCapabilities,{succeeded:'capabilities.succeeded',failed:'capabilities.failed'});}catch{failure();}
  }
  function flushEntries(selected){
    if(closed||mode!=='writer'||flushing)return false;
    const entries=[...pending.entries()].filter(([,value])=>selected(value.scope));if(!entries.length)return true;flushing=true;
    try{const result=module.observeBatch({observations:entries.map(([,value])=>value)});
      if(result.reason==='reader-only')throw configuredError();
      for(const [key]of entries)pending.delete(key);lastSuccessAt=clock();return true;
    }catch{failure();return false;}finally{flushing=false;}
  }
  const flush=()=>flushEntries(()=>true);
  return Object.freeze({middleware,
    tagAppResponse(res,id){if(!closed&&appId.test(id)&&res&&typeof res==='object')appScopes.set(res,Object.freeze({kind:'app',appId:id}));},
    inference(event){if(event?.event!=='inference_request_finished')return;const at=clock();observe(PLATFORM,'inference.requests',1,at);observe(PLATFORM,'inference.errors',event.outcome==='failed'?1:0,at);observe(PLATFORM,'inference.duration',event.durationMs,at);if(Number.isFinite(event.firstOutputMs))observe(PLATFORM,'inference.firstoutput',event.firstOutputMs,at);},
    sample,flush,
    flushScope(scope){if(!flushEntries(target=>target.kind===scope.kind&&(scope.kind==='platform'||target.appId===scope.appId)))throw Object.assign(new Error('monitoring_collection_flush_unconfirmed'),{code:'monitoring_collection_flush_unconfirmed',status:503});},
    start(){if(started||closed||mode!=='writer')return;started=true;sample();flush();timer=setInterval(()=>{try{sample();flush();}catch{failure();}},intervalMs);timer.unref?.();},
    status(scope=PLATFORM){return{running:started&&!closed&&mode==='writer',lastSuccessAt,lastFailureAt,failures,supportedMetricIds:scope.kind==='app'?['http.requests','http.errors','http.duration']:[...supportedMetricIds]};},
    close(){if(closed)return;clearInterval(timer);flush();closed=true;pending.clear();}
  });
}
