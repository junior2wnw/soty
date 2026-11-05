const fault=code=>Object.assign(new Error(code),{code});

/** An account-scoped read model. No message body or second durable inbox store.
 * Independent Sources apply independently; one slow Source cannot hide another. */
export function createSocialInbox({api,accountId,isCurrent,directEnabled=()=>true,epochs=()=>({direct:0,community:0,communityRead:0}),onDirect=()=>{},onCommunities=()=>{},onContacts=()=>{},onFailure=()=>{}}={}){
 let dead=false,flight=null,queued=false,scheduled=false;
 const current=()=>!dead&&isCurrent();
 const request=async(op,args={})=>{if(!current())throw fault('inbox_account_changed');const value=await api.request(op,{...(op.startsWith('world.direct.')?{expectedAccountId:accountId}:{}),...args});if(!current())throw fault('inbox_account_changed');return value;};
 async function direct(){
  if(!directEnabled())return[];
  const threads=[],seen=new Set(),cursors=new Set();let cursor=null;
  do{
   const page=await request('world.direct.list',{limit:60,...(cursor?{cursor}:{})});
   if(!page||!Array.isArray(page.threads)||page.threads.length>60||!(page.nextCursor===null||typeof page.nextCursor==='string'&&page.nextCursor.length>0&&page.nextCursor.length<=4096))throw fault('inbox_response_invalid');
   for(const thread of page.threads){
    if(typeof thread?.threadId!=='string'||!thread.threadId||seen.has(thread.threadId)||!Number.isSafeInteger(thread.unreadCount)||thread.unreadCount<0||typeof thread.preferences?.muted!=='boolean')throw fault('inbox_response_invalid');
    seen.add(thread.threadId);threads.push(thread);
   }
   if(threads.length>1000)throw fault('inbox_limit');
   cursor=page.nextCursor;if(cursor){if(cursors.has(cursor))throw fault('inbox_cursor_repeated');cursors.add(cursor);}
  }while(cursor);
  return threads;
 }
 async function pass(){
  const before=epochs(),errors=[];
  const run=async(kind,load,apply)=>{try{const value=await load();if(!current())return;if(apply(value,before)===false)queued=true;}catch(reason){if(current()){errors.push(reason);onFailure(kind,reason);}}};
  await Promise.all([
   run('direct',direct,onDirect),
   run('communities',()=>request('world.community.list'),(value,epoch)=>{if(!Array.isArray(value?.communities))throw fault('inbox_response_invalid');return onCommunities(value.communities,epoch);}),
   run('contacts',()=>request('contacts.list'),value=>{if(!Array.isArray(value?.requests?.incoming))throw fault('inbox_response_invalid');return onContacts(value.requests.incoming.length);}),
  ]);
  if(errors.length)throw errors[0];
 }
 function refresh(){
  if(!current())return Promise.resolve();
  if(flight){queued=true;return flight;}
  const task=(async()=>{let passes=0;do{queued=false;await pass();passes++;}while(queued&&current()&&passes<2);})();
  flight=task.finally(()=>{if(flight===settled)flight=null;if(queued&&current()&&!scheduled){scheduled=true;queueMicrotask(()=>{scheduled=false;if(current())void refresh().catch(()=>{});});}});const settled=flight;return flight;
 }
 return{refresh,dispose(){dead=true;queued=false;},get pending(){return flight!==null;}};
}
