// Synthetic test-only Native SQL writer. Private fixture input arrives only on
// stdin; no credentials, identities or native paths are returned in output.
import {createOrdinaryAppStore} from '../../examples/ordinary-app/store.mjs';
import {createOrdinaryAppNativePort} from '../../examples/ordinary-app/native.mjs';
import {createNativeAuthorityRuntime} from '../../server/native-authority.mjs';
let chunks=[],bytes=0,store,runtime;
try{
  for await(const part of process.stdin){bytes+=part.length;if(bytes>16384)throw new Error('fixture_input_invalid');chunks.push(part);}
  const input=JSON.parse(Buffer.concat(chunks).toString('utf8'));
  store=createOrdinaryAppStore({...input.options,key:Buffer.from(input.keyBase64,'base64'),initialize:false});
  runtime=createNativeAuthorityRuntime(createOrdinaryAppNativePort({store,resourceId:'selected',incarnationId:'one',allowEmptyGuest:true,allowLinkedLogin:true}));
  const proof=await runtime.capture(input.binding,{headers:{}});
  store.tx(()=>runtime.commitIdentity(proof,input.binding.identity,{createEmptyGuest:true}));
  process.stdout.write('{"linked":true}');
}catch{process.exitCode=1;}finally{runtime?.close();store?.close();chunks=[];}
