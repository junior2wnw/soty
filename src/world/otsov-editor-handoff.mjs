import { EDITOR_HANDOFF_SCHEMA as S, exact, requestId, copyArtifact, validReadback } from '../../modules/otsov-editor-handoff/wire.mjs';

/** Exact current scoped frame only; no URL/project query selector and no DOM clicks. */
export function createOtsovEditorHost({ view, readPeer, assertPeer, makeChannel = () => new view.MessageChannel(), timeoutMs = 30000 }) {
  let disposed = false; const pending = new Set();
  const same = (a,b) => a && b && a.window === b.window && a.origin === b.origin && a.appId === b.appId
    && a.accountId === b.accountId && a.generation === b.generation && a.slot === b.slot;
  return Object.freeze({
    async open(raw, { requestId: originalId, signal, assertCurrent }) {
      const artifact = copyArtifact(raw), peer = readPeer();
      if (disposed || !requestId(originalId) || !peer || peer.appId !== artifact.appId || typeof assertCurrent !== 'function') throw Error('editor_handoff_denied');
      const guard = async () => {
        if (disposed || signal?.aborted || !same(peer,readPeer())) throw Error('editor_handoff_stale');
        await assertCurrent();
        if (await assertPeer(peer,signal) !== true || disposed || signal?.aborted || !same(peer,readPeer())) throw Error('editor_handoff_stale');
      };
      await guard();
      const channel = makeChannel(), nonce = view.crypto.randomUUID(), port = channel.port1;
      return new Promise((resolve,reject) => {
        let finished = false, committing = false;
        const finish = (error,result) => { if(finished)return;finished=true;view.clearTimeout(timer);pending.delete(cancel);signal?.removeEventListener('abort',cancel);port.onmessage=null;port.close();error?reject(error):resolve(result); };
        const cancel = () => { try {port.postMessage({schema:S,kind:'cancel',requestId:nonce});}catch{}finish(Error('editor_handoff_cancelled')); };
        const timer=view.setTimeout(()=>{try{port.postMessage({schema:S,kind:'cancel',requestId:nonce});}catch{}finish(Error('editor_handoff_unknown'));},timeoutMs);
        pending.add(cancel);signal?.addEventListener('abort',cancel,{once:true});
        port.onmessage = event => { const value=event.data;
          if (event.ports?.length || !exact(value,['schema','kind','requestId','data']) || value.schema!==S || value.requestId!==nonce) return;
          if(value.kind==='ready'&&!committing){committing=true;void guard().then(()=>{if(!finished)port.postMessage({schema:S,kind:'commit',requestId:nonce});}).catch(error=>{try{port.postMessage({schema:S,kind:'cancel',requestId:nonce});}catch{}finish(error);});}
          else if(value.kind==='result'&&committing){void guard().then(()=>{if(!validReadback(value.data,artifact))throw Error('editor_readback_invalid');finish(null,{...value.data});}).catch(error=>finish(error));}
          else if(value.kind==='failed')finish(Error('editor_handoff_denied_or_unknown'));
        };
        port.start?.();
        if(signal?.aborted){cancel();return;}
        try{peer.window.postMessage({schema:S,requestId:nonce,artifact},peer.origin,[channel.port2]);}catch(error){finish(error);}
      });
    },
    close(){if(disposed)return;disposed=true;for(const cancel of [...pending])cancel();},
  });
}
