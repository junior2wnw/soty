import assert from 'node:assert/strict';
import {createClientWithStorage} from '../../browser/client.mjs';
import {createConnectService} from '../../server/index.mjs';
const origin='https://activation-selection.invalid',projectId='activation-selection',endpoint=origin+'/rpc';
export const password={login:'own-synthetic-login',secret:'Lighthouse-Copper-78-Silverwood'};
export const selection=state=>({accountId:state.accountId,deviceId:state.deviceId,localRevision:state.localRevision});
export async function createActivationFixture(t,{clientFactory=createClientWithStorage}={}){
 const service=createConnectService({databasePath:':memory:',projectId,allowedOrigins:[origin]}),clients=[],operations=[];
 function client(){let state=null;const value=clientFactory({projectId,endpoint,fetch:async(_url,request)=>{const body=JSON.parse(request.body);operations.push(body.op);const reply=await service.handle({...body,origin,peer:'own-synthetic-peer'});return new Response(JSON.stringify(reply),{status:reply.ok?200:400});}},{read:async()=>structuredClone(state),claim:async value=>structuredClone(state??=value),compareAndSwap:async(revision,value,options)=>{assert.equal(state.localRevision,revision);options?.beforeCommit?.();state=structuredClone(value);return structuredClone(state);}});clients.push(value);return value;}
 t.after(()=>{clients.forEach(value=>value.dispose());service.close();});
 const main=client(),seedB=client(),seedC=client(),a=await main.bootstrap('Own A'),b=await seedB.bootstrap('Own B'),c=await seedC.bootstrap('Own C');
 await seedB.setAccountCredential('password',password,b.accountId);
 for(const [seed,account]of[[seedB,b],[seedC,c]]){const request=await main.startEnrollment('Own additional profile');await seed.approveEnrollment(request.requestId,account.accountId);await main.previewEnrollment(request.requestId);await main.finishEnrollment(request.requestId,account.accountId);}
 await main.switchProfile(a.accountId);return{main,a,b,c,operations,newClient:client,seedB};
}

