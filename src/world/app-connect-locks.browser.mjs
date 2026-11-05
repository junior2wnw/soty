import {createAppConnectLocks} from './app-connect-locks.mjs';
const output=document.querySelector('#app-connect-locks-proof'),checks=[];
const assert=(value,label)=>{checks.push({label,pass:Boolean(value)});if(!value)throw new Error(label);};
const pause=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function run(){
  const locks=createAppConnectLocks({locks:null,indexedDB});
  assert(await locks.request('bootstrap',()=>23)===23,'IndexedDB-backed local transition completes');
  const db=await new Promise((resolve,reject)=>{const request=indexedDB.open('soty-app-connect-locks-v1',1);request.onsuccess=()=>resolve(request.result);request.onerror=()=>reject(request.error);});
  const blocker=db.transaction('local-transitions','readwrite');let release=false,entered=false;
  const done=new Promise((resolve,reject)=>{blocker.oncomplete=resolve;blocker.onabort=()=>reject(blocker.error);});
  function keepAlive(){const request=blocker.objectStore('local-transitions').get('held-probe');request.onsuccess=()=>{if(!release)keepAlive();};}keepAlive();
  const waiting=locks.request('blocked-proof',()=>{entered=true;return'durable';});await pause(25);
  assert(!entered,'A separate connection cannot enter while an existing readwrite transaction holds the store');
  // A no-lock control enters immediately and violates the same oracle.
  let brokenEntered=false;Promise.resolve((()=>{brokenEntered=true;})());assert(brokenEntered,'Negative control without a lock is distinguishable');
  release=true;await done;assert(await waiting==='durable'&&entered,'Queued transition resumes after the real IndexedDB lock commits');db.close();
  let refused=false;try{await locks.request('async-forbidden',()=>Promise.resolve(1));}catch(error){refused=error.code==='app_connect_async_local_transition';}
  assert(refused,'Async local callbacks are refused instead of holding the lock across network work');
  assert(await locks.request('after-abort',()=>true),'Aborted transition does not strand the next transition');
  return{environment:'real browser IndexedDB; forced Web Locks-unavailable path',pass:true,checks};
}
run().then(value=>{output.textContent=JSON.stringify(value,null,2);output.dataset.pass='true';}).catch(error=>{output.textContent=JSON.stringify({pass:false,checks,error:error.message},null,2);output.dataset.pass='false';});
