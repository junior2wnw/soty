import { createOrdinaryAppStore } from '../../examples/ordinary-app/store.mjs';
import { createOrdinaryAppNativePort } from '../../examples/ordinary-app/native.mjs';
import { createOrdinaryFeedbackJobs } from '../../examples/ordinary-app/feedback-jobs.mjs';
import { createNativeAuthorityRuntime } from '../../server/native-authority.mjs';
import { createFeedbackJobAuthorityProof } from '../../server/feedback-job-authority.mjs';
import { createFeedbackJobEnforcer } from '../../server/feedback-job-enforcer.mjs';
import { createFeedbackProcessorEngine } from '../../server/feedback-job-contract.mjs';
import { digest } from '../../server/wire.mjs';

// Synthetic Source proof SPI, NOT Root/OIDC/browser or OS enforcer evidence.
let bytes='';for await(const part of process.stdin){bytes+=part;if(bytes.length>16000)process.exit(78);}
const input=JSON.parse(bytes);const store=createOrdinaryAppStore({...input.options,key:Buffer.from(input.options.key,'base64'),initialize:false});
const native=createNativeAuthorityRuntime(createOrdinaryAppNativePort({store,resourceId:'selected',incarnationId:'one'}));
const engine=createFeedbackProcessorEngine({ref:input.engineRef,purposes:['ocr'],localOnly:true,synthetic:true,process:async()=>{}});
let calls=0;
const jobs=createOrdinaryFeedbackJobs({store,resourceId:'selected',incarnationId:'one',policy:input.policy,engines:[engine],
  enforcer:createFeedbackJobEnforcer({engine,platform:'linux',maxBudget:input.budget,syntheticTestOnly:true,execute:async()=>{
    calls++;process.stdout.write(JSON.stringify({phase:'executing',synthetic:true})+'\n');await new Promise(resolve=>setTimeout(resolve,input.holdMs));return{kind:'transcript',text:'Synthetic worker result'};}}),
  currentProof:async sessionHash=>{const proof=await native.capture(input.binding),row=store.db.prepare('SELECT expires_at FROM source_sessions WHERE id_hash=?').get(sessionHash);
    return createFeedbackJobAuthorityProof({sessionHash,bindingDigest:digest(input.binding),expiresAt:row.expires_at,
      assertCurrent:()=>native.assertCurrent(proof),withCurrent:action=>native.withCurrent(proof,action)});}});
try{const result=await jobs.process(input.jobId);process.stdout.write(JSON.stringify({phase:'done',outcome:result.outcome,replayed:result.replayed,calls})+'\n');}
catch(error){process.stdout.write(JSON.stringify({phase:'done',outcome:'denied-or-unknown',code:error.code,calls})+'\n');}
finally{jobs.close();native.close();store.close();}
