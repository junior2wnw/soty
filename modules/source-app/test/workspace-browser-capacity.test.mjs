import test from 'node:test';
import assert from 'node:assert/strict';
import { createSourceWorkspaceClient } from '../browser/index.mjs';

test('workspace capacity503 is a temporary service failure, with no auth handoff, repeated query or mutation', async () => {
  const view=new EventTarget(),wakes=[],calls=[];
  view.parent={postMessage(message){wakes.push(message);}};view.setTimeout=setTimeout;view.clearTimeout=clearTimeout;
  const client=createSourceWorkspaceClient({view,parentOrigin:'https://root.test',href:'https://app.test/embed?sotyFrame='+'F'.repeat(43),
    fetch:async(path,init)=>{
      calls.push({path,args:JSON.parse(init.body)});
      if(path==='/api/embed/session-continue')return Response.json({schema:'soty.source-session-continuation.v1',ready:true,renewable:true,
        sessionExpiresAt:1900000000000,accessExpiresAt:1900000000000,receiptDigest:'d'.repeat(64)});
      assert.equal(path,'/api/embed/query');
      return Response.json({ok:false,error:{code:'source_workspace_capacity'}},{status:503});
    }});
  await assert.rejects(client.connect({requestId:'capacity-connect-0001'}),{code:'source_workspace_capacity',status:503,effectUnknown:false});
  assert.deepEqual(calls.map(value=>value.path),['/api/embed/session-continue','/api/embed/query']);
  assert.deepEqual(calls[1].args.input,{operation:'soty.current_context'});
  assert.deepEqual(wakes,[]);assert.equal(client.context(),null);client.dispose();
});
