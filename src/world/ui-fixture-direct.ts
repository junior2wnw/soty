import type { WorldDirectThread, WorldMessage, WorldProfile } from './types';

/** DEV specimen only: visual interaction and reload, never authorization proof.
 * Signed Connect/World/SQLite tests exercise the actual service separately. */
export function createDirectFixture(self: WorldProfile, peer: WorldProfile, enabled: boolean): (op: string, args: Record<string, unknown>) => unknown {
  const relationshipId = 'rel_qa_tim1111111111111111', threadId = 'direct_qa_tim1111111111111111', key = 'soty.qa.direct.v1';
  const thread: WorldDirectThread = { schema:'soty.world-direct.v1',threadId,relationshipId,revision:1,
    peer:{accountId:peer.profileId,label:null,avatarRevision:null},unreadCount:1,preferences:{pinned:false,muted:false},createdAt:Date.now()-60_000 };
  let messages: WorldMessage[] = [{messageId:'qa-direct-first',seq:1,text:'Привет! Соберём идею в Сотах?',author:peer,createdAt:Date.now()-60_000,replyTo:null,removed:false}];
  const failure = (code: string): never => { throw Object.assign(new Error(code),{code}); };
  if(enabled) {
    self.directMessagesVersion=1;
    try { const raw=localStorage.getItem(key);if(raw && raw.length<256_000){const saved=JSON.parse(raw);if(Array.isArray(saved.messages)&&saved.messages.length<=200)messages=saved.messages;
      if(typeof saved.pinned==='boolean')thread.preferences.pinned=saved.pinned;if(typeof saved.muted==='boolean')thread.preferences.muted=saved.muted;} }
    catch { /* Isolated synthetic data only. */ }
  }
  const save=():void=>localStorage.setItem(key,JSON.stringify({messages:messages.slice(-200),...thread.preferences}));
  return (op,args) => {
    if(!op.startsWith('world.direct.'))return undefined;
    if(!enabled)return failure('unsupported_operation');
    if(args.expectedAccountId!==self.profileId)return failure('direct_account_changed');
    if(op==='world.direct.list')return {threads:[thread],nextCursor:null};
    if(op==='world.direct.open') {
      if(args.peerAccountId!==peer.profileId||args.expectedRelationshipId!==relationshipId)return failure('direct_contact_required');
      return {current:thread,receipt:{threadId,relationshipId,requestId:args.requestId},replayed:false};
    }
    if(args.threadId!==threadId||args.expectedRelationshipId!==relationshipId)return failure('direct_not_found');
    if(op==='world.direct.get')return {thread};
    if(op==='world.direct.preferences') {
      for(const field of ['pinned','muted'] as const)if(typeof args[field]==='boolean')thread.preferences[field]=args[field];save();return {thread};
    }
    if(op==='world.direct.chat.list') {
      const filtered=messages.filter(value=>(args.before===undefined||value.seq<Number(args.before))&&(args.after===undefined||value.seq>Number(args.after)));
      const limit=Math.min(60,Number(args.limit)||30);return {messages:args.after===undefined?filtered.slice(-limit):filtered.slice(0,limit),hasMore:filtered.length>limit};
    }
    if(op==='world.direct.chat.send') {
      let message=messages.find(value=>value.messageId===args.clientId);
      if(!message) { if(typeof args.text!=='string'||!args.text.trim()||args.text.length>6000)return failure('invalid_text');
        message={messageId:String(args.clientId),seq:(messages.at(-1)?.seq??0)+1,text:args.text,author:self,createdAt:Date.now(),replyTo:typeof args.replyTo==='string'?args.replyTo:null,removed:false};messages.push(message);save(); }
      return {message};
    }
    if(op==='world.direct.chat.remove') {
      const message=messages.find(value=>value.messageId===args.messageId);if(!message)return failure('message_not_found');
      if(message.author.profileId!==self.profileId)return failure('direct_permission_denied');message.removed=true;message.text='';save();return {removed:true};
    }
    if(op==='world.direct.chat.read') {thread.unreadCount=0;return {unreadCount:0};}
    return failure('unsupported_operation');
  };
}
