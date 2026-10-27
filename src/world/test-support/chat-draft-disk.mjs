import { readDraftRecord, initializeDraftRecord, writeDraftRecord, selectDraftRecord, acknowledgeDraftRecord, recoverLegacyDraftRecord } from '../chat-draft-record.mjs';
export function createTestChatDraftDisk() {
  const rows=new Map();let chain=Promise.resolve(), failure=false, barrier=null;
  const transaction=fn=>{const work=chain.then(async()=>{if(barrier)await barrier;if(failure)throw Object.assign(Error('Synthetic IDB unavailable'),{code:'attachment_local_storage'});return fn();});chain=work.catch(()=>{});return work;};
  const result=(key,changed)=>{if(changed.record)rows.set(key,structuredClone(changed.record));return {...readDraftRecord(rows.get(key)),...(changed.head?{head:structuredClone(changed.head)}:{}),...(changed.stale?{stale:true}:{})};};
  return {rows,setFailure:value=>{failure=value;},hold:value=>{barrier=value;},
    draftInitialize:(key,legacy,id)=>transaction(()=>result(key,{record:initializeDraftRecord(rows.get(key),legacy,id)})),
    draftRead:key=>transaction(()=>readDraftRecord(rows.get(key))),
    draftList:prefix=>transaction(()=>[...rows].filter(([key])=>key.startsWith(prefix)).map(([key,value])=>({key:'$draft:'+key,...readDraftRecord(value)}))),
    draftWrite:(key,proposal)=>transaction(()=>result(key,writeDraftRecord(rows.get(key),proposal))),
    draftSelect:(key,ids,selected,id,writer)=>transaction(()=>result(key,selectDraftRecord(rows.get(key),ids,selected,id,writer))),
    draftAcknowledge:(key,id)=>transaction(()=>result(key,{record:acknowledgeDraftRecord(rows.get(key),id)})),
    draftRecoverLegacy:(key,candidate,id,isCurrent)=>transaction(()=>result(key,isCurrent&&!isCurrent()?{stale:true}:{record:recoverLegacyDraftRecord(rows.get(key),candidate,id)})),
  };
}
