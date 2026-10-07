import test from 'node:test';
import assert from 'node:assert/strict';
import { createSourceFeedbackProcessingClient } from '../browser/processing-client.mjs';
import { digest } from '../server/wire.mjs';

test('processing client freezes canonical original intent; unknown ACK uses exact read-only receipt, never another apply/URL/actor/header',async()=>{
  const calls=[],client=createSourceFeedbackProcessingClient({fetch:async(path,options)=>{
    calls.push({path,options});if(path.endsWith('/invoke'))throw new Error('lost ACK');
    return new Response(JSON.stringify({ok:true,data:{outcome:'committed',data:{jobId:'synthetic'}}}),{headers:{'content-type':'application/json'}});}});
  const input={operation:'feedback.job.grant',purpose:'ocr',budget:{wallMs:1000},ticketId:'synthetic'};
  const intent=await client.intent(input);assert.equal(intent.inputDigest,digest(input));assert.equal(Object.isFrozen(intent.args.input.budget),true);
  await assert.rejects(client.apply(intent));const receipt=await client.receipt(intent);assert.equal(receipt.outcome,'committed');
  assert.deepEqual(calls.map(call=>call.path),['/api/embed/invoke','/api/embed/receipt']);
  const args=JSON.parse(calls[1].options.body);assert.equal(args.requestId,intent.args.requestId);assert.equal(args.input.inputDigest,intent.inputDigest);
  assert.equal(calls[1].options.credentials,'same-origin');assert.equal(calls[1].options.redirect,'error');
  assert.equal('authorization' in calls[1].options.headers,false);
});
test('processing client refuses duplicate/malformed JSON, redirects and oversized private response instead of treating it as permission',async()=>{
  for(const body of ['{"ok":true,"ok":false,"data":{}}','{"ok":true,"data":"'+ 'x'.repeat(70000)+'"}']){
    const client=createSourceFeedbackProcessingClient({fetch:async()=>new Response(body,{headers:{'content-type':'application/json'}})});
    await assert.rejects(client.context(),error=>error.code==='source_feedback_response_invalid');
  }
  const client=createSourceFeedbackProcessingClient({fetch:async()=>new Response('{"ok":true,"data":{}}',{headers:{'content-type':'text/html'}})});
  await assert.rejects(client.context());
});
