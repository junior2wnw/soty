import { createHash, createHmac, randomBytes, timingSafeEqual } from 'node:crypto';

const schema='soty.platform-live.v1',refKey=ref=>`${ref.kind}:${ref.id}`;
const fail=(code,status=400)=>{throw Object.assign(new Error(code),{code,status});};
const exact=(value,keys)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).every(key=>keys.includes(key));
const scopeKey=actor=>createHash('sha256').update(JSON.stringify([actor.accountId,actor.deviceId])).digest('base64url');

/** Content-free hints only. Existing directory reads remain the authority for every displayed object. */
export function createPlatformLive({actorActive,canSee,clock=Date.now,maxScopes=2048,idleMs=300000}={}){
  if(typeof actorActive!=='function'||typeof canSee!=='function')fail('live_configuration_invalid',500);
  const boot=randomBytes(16).toString('hex'),secret=randomBytes(32),scopes=new Map();
  let closed=false,queued=false;const changes=[];
  const sign=body=>createHmac('sha256',secret).update(body).digest('base64url');
  const cursor=scope=>{const body=Buffer.from(JSON.stringify({b:boot,s:scope.key,r:scope.revision})).toString('base64url');return `${body}.${sign(body)}`;};
  const active=actor=>{if(closed)fail('live_closed',503);if(actorActive(actor)!==true)fail('live_authentication_required',401);};
  function parse(value,key){
    if(value===undefined)return null;
    if(typeof value!=='string'||value.length>400||!value.match(/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{43}$/))fail('live_cursor_invalid');
    const [body,mac]=value.split('.');let data;try{data=JSON.parse(Buffer.from(body,'base64url').toString());}catch{fail('live_cursor_invalid');}
    if(!exact(data,['b','s','r'])||typeof data.b!=='string'||!data.b.match(/^[a-f0-9]{32}$/)||data.s!==key||!Number.isSafeInteger(data.r)||data.r<0)fail('live_cursor_invalid');
    // A prior process cannot retain its ephemeral hints. Its caller must reread current ACLs.
    if(data.b!==boot)return {reset:true};
    if(!timingSafeEqual(Buffer.from(mac),Buffer.from(sign(body))))fail('live_cursor_invalid');
    return data;
  }
  function reply(scope,changed,reset=false){active(scope.actor);return {schema,cursor:cursor(scope),changed,reset};}
  function settle(scope,error){for(const waiter of [...scope.waiters]){waiter.cleanup();if(error)waiter.reject(error);else{try{waiter.resolve(reply(scope,true));}catch(reason){waiter.reject(reason);}}}}
  function getScope(actor,key){
    let scope=scopes.get(key);if(scope){scope.actor=actor;scope.usedAt=clock();return scope;}
    for(const [id,value] of scopes)if(!value.waiters.size&&clock()-value.usedAt>idleMs)scopes.delete(id);
    if(scopes.size>=maxScopes){const oldest=[...scopes.values()].filter(value=>!value.waiters.size).sort((a,b)=>a.usedAt-b.usedAt)[0];if(!oldest)fail('live_busy',429);scopes.delete(oldest.key);}
    scope={key,actor,revision:0,seen:new Set(),unresolved:new Map(),waiters:new Set(),usedAt:clock()};scopes.set(key,scope);return scope;
  }
  function visible(actor,ref){try{return canSee(actor,ref)===true;}catch{return null;}}
  function retryUnresolved(scope){
    let changed=false;
    for(const [key,ref] of scope.unresolved){const allowed=visible(scope.actor,ref);if(allowed===null)continue;scope.unresolved.delete(key);if(allowed){scope.seen.add(key);changed=true;}}
    if(changed)scope.revision++;
  }
  function flush(){
    queued=false;const batch=changes.splice(0);
    for(const scope of scopes.values()){
      if(actorActive(scope.actor)!==true){settle(scope,Object.assign(new Error('live_authentication_required'),{code:'live_authentication_required',status:401}));scopes.delete(scope.key);continue;}
      let changed=false;
      for(const change of batch){
        if(change.authorityChanged&&change.accountIds.includes(scope.actor.accountId))for(const key of scope.seen){
          const colon=key.indexOf(':'),allowed=visible(scope.actor,{kind:key.slice(0,colon),id:key.slice(colon+1)});if(allowed!==true)scope.seen.delete(key);
        }
        const ref=change.ref,key=ref&&refKey(ref),wasSeen=key&&scope.seen.has(key),nowSeen=ref&&visible(scope.actor,ref);
        if(nowSeen)scope.seen.add(key);else if(key)scope.seen.delete(key);
        if(nowSeen===null&&key){
          if(!scope.unresolved.has(key)&&scope.unresolved.size>=512){settle(scope,Object.assign(new Error('live_busy'),{code:'live_busy',status:429}));scopes.delete(scope.key);break;}
          scope.unresolved.set(key,ref);
        }
        else if(key)scope.unresolved.delete(key);
        if(change.publicTransition||change.accountIds?.includes(scope.actor.accountId)||wasSeen||nowSeen)changed=true;
      }
      if(changed){scope.revision++;settle(scope);}
    }
  }
  return Object.freeze({
    operations:new Set(['platform.live.wait']),
    async executeAsync({args={},actor,signal}){
      active(actor);if(!exact(args,['expectedAccountId','cursor','waitMs','watch'])||args.expectedAccountId!==actor.accountId)fail('live_account_changed',409);
      const waitMs=args.waitMs??15000;if(!Number.isSafeInteger(waitMs)||waitMs<0||waitMs>15000)fail('live_wait_invalid');
      const watch=args.watch??[];if(!Array.isArray(watch)||watch.length>256||watch.some(ref=>!exact(ref,['kind','id'])||!['app','person','community','object'].includes(ref.kind)||typeof ref.id!=='string'||!ref.id.match(/^[A-Za-z0-9_.:-]{1,180}$/)))fail('live_watch_invalid');
      const key=scopeKey(actor),previous=parse(args.cursor,key),existing=scopes.has(key),scope=getScope(actor,key);
      retryUnresolved(scope);
      for(const ref of watch)if(visible(actor,ref))scope.seen.add(refKey(ref));
      if(scope.seen.size>512)scope.seen=new Set([...scope.seen].slice(-512));
      if(!previous||previous.reset||!existing)return reply(scope,true,true);
      if(previous.r>scope.revision)fail('live_cursor_invalid');
      if(previous.r<scope.revision)return reply(scope,true);
      if(!waitMs)return reply(scope,false);
      if(scope.waiters.size>=4)fail('live_busy',429);
      if(signal?.aborted)fail('live_cancelled',499);
      return new Promise((resolve,reject)=>{
        const cancel=()=>{waiter.cleanup();reject(Object.assign(new Error('live_cancelled'),{code:'live_cancelled',status:499}));};
        const waiter={resolve,reject,cleanup(){scope.waiters.delete(waiter);clearTimeout(waiter.timer);signal?.removeEventListener('abort',cancel);},timer:setTimeout(()=>{waiter.cleanup();try{resolve(reply(scope,false));}catch(error){reject(error);}},waitMs)};
        scope.waiters.add(waiter);signal?.addEventListener('abort',cancel,{once:true});
      });
    },
    publish(change){if(closed)return;if(!change||!Array.isArray(change.accountIds))return;changes.push(change);if(!queued){queued=true;queueMicrotask(flush);}},
    recheck(){for(const scope of scopes.values())if(actorActive(scope.actor)!==true){settle(scope,Object.assign(new Error('live_authentication_required'),{code:'live_authentication_required',status:401}));scopes.delete(scope.key);}},
    stats(){return {scopes:scopes.size,waiting:[...scopes.values()].reduce((sum,scope)=>sum+scope.waiters.size,0)};},
    close(){if(closed)return;closed=true;changes.length=0;for(const scope of scopes.values())settle(scope,Object.assign(new Error('live_closed'),{code:'live_closed',status:503}));scopes.clear();},
  });
}
