import { announceChatDraft, subscribeChatDraft } from './attachment-outbox.mjs';

const token = () => `draft_${crypto.randomUUID()}`;
const copy = value => structuredClone(value);
const same = (a,b) => a.text===b.text&&a.replyTo===b.replyTo;

/** Draft heads share the existing account-scoped IndexedDB outbox. */
export function createChatDraftSession({disk,accountId,conversationKey,legacy,retainLegacy=false,changed=()=>{}}) {
  const prefix=encodeURIComponent(accountId)+':'+encodeURIComponent(conversationKey)+':', writer=token();
  let value={text:legacy?.text??'',replyTo:legacy?.replyTo??null}, heads=[],selectedId=null, revision=0,
    dirty=retainLegacy&&!!(legacy?.text||legacy?.replyTo), generation=retainLegacy?1:0, error=null, closed=false, running=null, initialized=false, initializing=null;
  let onChange=changed;
  const emit=()=>{if(!closed)onChange(read());};
  function read(){return copy({value,heads,selectedId,dirty,error,generation});}
  function receive(snapshot, initial=false) {
    if(snapshot.revision<revision)return;revision=snapshot.revision;heads=snapshot.heads;
    if(dirty) {
      if(initial)selectedId=heads.find(head=>same(head,legacy??{text:'',replyTo:null}))?.id??null;
    } else {
      let chosen=heads.find(head=>head.id===selectedId);
      if(!chosen&&heads.length===1)chosen=heads[0];
      if(!chosen&&initial)chosen=heads.find(head=>same(head,legacy??{text:'',replyTo:null}))??heads.at(-1);
      if(chosen){selectedId=chosen.id;value={text:chosen.text,replyTo:chosen.replyTo};}
      else if(!heads.length){selectedId=null;value={text:'',replyTo:null};}
    }
    emit();
  }
  function initialize() {
    if(initialized)return Promise.resolve();if(initializing)return initializing;
    initializing=disk.draftInitialize(prefix,legacy,token()).then(snapshot=>{receive(snapshot,true);initialized=true;error=null;})
      .catch(reason=>{error=reason?.code??'attachment_local_storage';dirty=dirty||!!(value.text||value.replyTo);emit();throw reason;})
      .finally(()=>{initializing=null;});return initializing;
  }
  const ready=initialize();
  // A notification only invalidates cached heads; the database owns the result.
  const refresh=()=>initialize().then(()=>disk.draftRead(prefix)).then(snapshot=>receive(snapshot));
  let unsubscribe=subscribeChatDraft(prefix,refresh);
  function flush() {
    if(running)return running.then(saved=>saved&&dirty?flush():saved);
    running=(async()=>{
      try {
        await initialize();
        while(dirty) {
          const at=generation, submitted={...value}, proposal={...submitted,baseId:selectedId,id:token(),writer};
          const result=await disk.draftWrite(prefix,proposal);selectedId=result.head.id;revision=result.revision;heads=result.heads;
          if(at===generation)dirty=false;error=null;emit();await announceChatDraft(prefix);
        }
        return true;
      }catch(reason){error=reason?.code??'attachment_local_storage';dirty=true;emit();return false;}
    })().finally(()=>{running=null;});return running;
  }
  return {
    ready,read,refresh,isReady:()=>initialized,scope:{accountId,conversationKey},
    edit(text,replyTo) {const next={text,replyTo:replyTo??null};if(same(value,next))return;value=next;generation++;dirty=true;error=null;emit();void flush();},
    flush,hasUnsavedChanges:()=>dirty,
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
