import { announceChatDraft, subscribeChatDraft } from './attachment-outbox.mjs';

const token = () => `draft_${crypto.randomUUID()}`;
const copy = value => structuredClone(value);
const same = (a,b) => a.text===b.text&&a.replyTo===b.replyTo;

/** Draft heads share the existing account-scoped IndexedDB outbox. */
export function createChatDraftSession({disk,accountId,conversationKey,legacy,legacyCandidates=[],currentLegacy,retainLegacy=false,changed=()=>{}}) {
  const prefix=encodeURIComponent(accountId)+':'+encodeURIComponent(conversationKey)+':', writer=token();
  let value={text:legacy?.text??'',replyTo:legacy?.replyTo??null}, heads=[],selectedId=null, revision=0,
    dirty=retainLegacy&&!!(legacy?.text||legacy?.replyTo), generation=retainLegacy?1:0, error=null, closed=false, running=null, initialized=false, initializing=null;
  let onChange=changed,legacyInFlight=null;const pendingLegacy=[];
  const enqueueLegacy=candidate=>{if(!candidate||!['v1','v2'].includes(candidate.source)||(!candidate.text&&!candidate.replyTo))return;
    const last=pendingLegacy.findLast(item=>item.source===candidate.source);if(last&&same(last,candidate)){
      if(candidate.fresh&&!last.fresh){if(last===legacyInFlight)pendingLegacy.push({...candidate,id:token()});else last.fresh=true;}return;
    }
    for(let i=pendingLegacy.length-1;i>=0;i--)if(pendingLegacy[i].source===candidate.source&&pendingLegacy[i]!==legacyInFlight)pendingLegacy.splice(i,1);
    pendingLegacy.push({...candidate,id:token()});};
  for(const candidate of legacyCandidates)enqueueLegacy(candidate);
  const emit=()=>{if(!closed)onChange(read());};
  function read(){return copy({value,heads,selectedId,dirty,error,generation,pendingLegacy});}
  function receive(snapshot, initial=false) {
    if(snapshot.revision<revision)return;revision=snapshot.revision;heads=snapshot.heads;
    if(dirty) {
      if(initial)selectedId=heads.find(head=>same(head,legacy??{text:'',replyTo:null}))?.id??null;
    } else {
      const normal=heads.filter(head=>!head.writer.startsWith('legacy_recovered_'));let chosen=normal.find(head=>head.id===selectedId);
      if(!chosen&&normal.length===1)chosen=normal[0];
      if(!chosen&&initial)chosen=normal.find(head=>same(head,legacy??{text:'',replyTo:null}))??normal.at(-1);
      if(chosen){selectedId=chosen.id;value={text:chosen.text,replyTo:chosen.replyTo};}
      else if(!normal.length){selectedId=null;value={text:'',replyTo:null};}
    }
    emit();
  }
  function initialize() {
    if(initialized)return Promise.resolve();if(initializing)return initializing;
    initializing=disk.draftInitialize(prefix,legacy,token()).then(snapshot=>{receive(snapshot,true);initialized=true;error=null;})
      .catch(reason=>{error=reason?.code??'attachment_local_storage';dirty=dirty||!!(value.text||value.replyTo);emit();throw reason;})
      .finally(()=>{initializing=null;});return initializing;
  }
  const ready=initialize().then(async()=>{if(pendingLegacy.length&&!await flush())throw Object.assign(Error(error??'attachment_local_storage'),{code:error??'attachment_local_storage'});});
  // A notification only invalidates cached heads; the database owns the result.
  const refresh=()=>initialize().then(()=>disk.draftRead(prefix)).then(snapshot=>receive(snapshot));
  let unsubscribe=subscribeChatDraft(prefix,refresh);
  function flush() {
    if(running)return running.then(saved=>saved&&(dirty||pendingLegacy.length)?flush():saved);
    running=(async()=>{
      try {
        await initialize();
        while(pendingLegacy.length) {
          const item=pendingLegacy[0],bytes=new TextEncoder().encode(JSON.stringify([item.text,item.replyTo]));legacyInFlight=item;
          const digest=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes)),value=>value.toString(16).padStart(2,'0')).join('');
          const result=await disk.draftRecoverLegacy(prefix,{...item,digest},item.id,()=>{const latest=currentLegacy?.(item.source);return latest===undefined||!!latest&&same(latest,item);});
          pendingLegacy.shift();legacyInFlight=null;if(result.stale){const latest=currentLegacy?.(item.source);if(latest)enqueueLegacy({...latest,fresh:true});}
          error=null;receive(result);await announceChatDraft(prefix);
        }
        while(dirty) {
          const at=generation, submitted={...value}, proposal={...submitted,baseId:selectedId,id:token(),writer};
          const result=await disk.draftWrite(prefix,proposal);selectedId=result.head.id;revision=result.revision;heads=result.heads;
          if(at===generation)dirty=false;error=null;emit();await announceChatDraft(prefix);
        }
        return true;
      }catch(reason){legacyInFlight=null;error=reason?.code??'attachment_local_storage';emit();return false;}
    })().finally(()=>{running=null;});return running;
  }
  return {
    ready,read,refresh,isReady:()=>initialized,scope:{accountId,conversationKey},
    edit(text,replyTo) {const next={text,replyTo:replyTo??null};if(same(value,next))return;value=next;generation++;dirty=true;error=null;emit();void flush();},
    flush,hasUnsavedChanges:()=>dirty||pendingLegacy.length>0,
    recoverLegacy(candidate){enqueueLegacy(candidate);emit();void flush();},
    async select(expectedIds,id) {
      if(!await flush())return false;
      try {const result=await disk.draftSelect(prefix,expectedIds,id,token(),writer);selectedId=result.head.id;value={text:result.head.text,replyTo:result.head.replyTo};
        revision=result.revision;heads=result.heads;error=null;emit();await announceChatDraft(prefix);return true;}
      catch(reason){error=reason?.code??'attachment_local_storage';emit();return false;}
    },
    async acknowledge(id) {
      if(!id)return;const result=await disk.draftAcknowledge(prefix,id);receive(result);await announceChatDraft(prefix);
    },
    close(){closed=true;unsubscribe();},
    reopen(callback){onChange=callback;if(closed){closed=false;unsubscribe=subscribeChatDraft(prefix,refresh);}emit();return this;},
  };
}
