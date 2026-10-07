import { parseSourceJson } from '../shared/strict-json.mjs';
import { feedbackFields,SourceFeedbackClientError } from '../shared/feedback-wire.mjs';

export function sourceProcessingCanonical(value){
  if(Array.isArray(value))return '['+value.map(sourceProcessingCanonical).join(',')+']';
  if(value&&typeof value==='object')return '{'+Object.keys(value).sort().map(key=>JSON.stringify(key)+':'+sourceProcessingCanonical(value[key])).join(',')+'}';
  return JSON.stringify(value);
}
const freeze=value=>{if(value&&typeof value==='object'){Object.values(value).forEach(freeze);Object.freeze(value);}return value;};
/** One fixed Source route family, no URL/actor/role/token configuration. Native
 * permissions/retained context are always rechecked by the Source server. */
export function createSourceFeedbackProcessingClient({fetch:fetcher=globalThis.fetch}={}){
  async function call(path,args,signal){
    const body=JSON.stringify(args);if(new TextEncoder().encode(body).byteLength>65536)throw new SourceFeedbackClientError('source_feedback_input_invalid',400);
    const response=await fetcher('/api/embed/'+path,{method:'POST',credentials:'same-origin',redirect:'error',referrerPolicy:'no-referrer',signal,
      headers:{'content-type':'application/json',accept:'application/json'},body});
    if(response.redirected||!/^application\/json(?:;|$)/iu.test(response.headers.get('content-type')||''))throw new SourceFeedbackClientError('source_feedback_response_invalid');
    const chunks=[],reader=response.body.getReader();let count=0;
    try{for(;;){const {done,value}=await reader.read();if(done)break;count+=value.byteLength;if(count>65536)throw new Error('bound');chunks.push(value);}}
    catch{throw new SourceFeedbackClientError('source_feedback_response_invalid');}
    finally{await reader.cancel().catch(()=>{});reader.releaseLock();}
    let value;try{const bytes=new Uint8Array(count);let offset=0;for(const chunk of chunks){bytes.set(chunk,offset);offset+=chunk.length;}
      value=parseSourceJson(new TextDecoder('utf-8',{fatal:true}).decode(bytes));}catch{throw new SourceFeedbackClientError('source_feedback_response_invalid');}
    if(!response.ok)throw new SourceFeedbackClientError(/^[a-z0-9_]{1,128}$/u.test(value?.error?.code??'')?value.error.code:'source_feedback_unknown',response.status);
    try{feedbackFields(value,['ok','data']);if(value.ok!==true)throw new Error();}catch{throw new SourceFeedbackClientError('source_feedback_response_invalid');}
    return value.data;
  }
  const query=(input,signal)=>call('query',{requestId:'processing-'+crypto.randomUUID(),input},signal);
  return Object.freeze({
    context:signal=>query({operation:'feedback.processing.context'},signal),
    ticket:(ticketId,signal)=>query({operation:'feedback.processing.ticket',ticketId},signal),
    status:(jobId,signal)=>query({operation:'feedback.job.status',jobId},signal),
    result:(jobId,signal)=>query({operation:'feedback.job.result',jobId},signal),
    async intent(input){const copied=JSON.parse(JSON.stringify(input));if(!['feedback.processing.consent','feedback.job.grant','feedback.job.revoke'].includes(copied.operation))throw new SourceFeedbackClientError('source_feedback_input_invalid',400);
      const bytes=await crypto.subtle.digest('SHA-256',new TextEncoder().encode(sourceProcessingCanonical(copied)));
      return freeze({args:{requestId:'processing-'+crypto.randomUUID(),input:copied},inputDigest:[...new Uint8Array(bytes)].map(byte=>byte.toString(16).padStart(2,'0')).join('')});},
    apply:(intent,signal)=>call('invoke',intent.args,signal),
    receipt:(intent,signal)=>call('receipt',{requestId:intent.args.requestId,input:{inputDigest:intent.inputDigest}},signal),
  });
}
